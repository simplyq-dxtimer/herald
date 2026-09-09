use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use axum::Json;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::auth;
use crate::crypto;
use crate::error::HeraldError;
use crate::queue;
use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct PollParams {
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default = "default_visibility_timeout")]
    pub visibility_timeout: u64,
}

fn default_limit() -> usize {
    10
}

fn default_visibility_timeout() -> u64 {
    300
}

#[derive(Debug, Serialize)]
pub struct MessageResponse {
    pub object: &'static str,
    pub message_id: String,
    pub fingerprint: String,
    pub body: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<serde_json::Value>,
    /// Unix integer seconds since epoch.
    pub received_at: i64,
    pub deliver_count: u32,
    pub encryption: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_version: Option<String>,
}

/// GET /endpoints/{endpoint_name}/messages — poll for messages
pub async fn poll_messages(
    State(state): State<AppState>,
    Path(endpoint_name): Path<String>,
    Query(params): Query<PollParams>,
    req: axum::extract::Request,
) -> Result<impl IntoResponse, HeraldError> {
    let api_key = auth::extract_api_key(&req)?;
    let mut conn = state.redis.clone();
    let account = auth::lookup_account(&mut conn, &api_key).await?;
    let limits = account.tier.limits();

    let limit = params.limit.min(100);
    let visibility_timeout = params.visibility_timeout.clamp(30, 43200);

    let messages = queue::fetch(
        &mut conn,
        &account.customer_id,
        &endpoint_name,
        limit,
        visibility_timeout,
    )
    .await?;

    // Always return a 200 with the list envelope (no 204).
    // has_more is a heuristic: true iff we filled the requested page.
    let has_more = messages.len() == limit && limit > 0;
    let queue_depth = queue::depth(&mut conn, &account.customer_id, &endpoint_name).await?;
    // Surfaced on every poll: without it, messages dying into the DLQ are
    // invisible to a consumer that only ever polls.
    let dlq_depth = queue::dlq_depth(&mut conn, &account.customer_id, &endpoint_name).await?;

    let mut responses = Vec::with_capacity(messages.len());
    for msg in messages {
        // Decrypt body based on per-message encryption label
        let plaintext_body = if msg.encryption == "none" {
            msg.body.clone()
        } else {
            crypto::decrypt(&state.config.service_encryption_key, &msg.body)?
        };
        let body_b64 =
            base64::engine::general_purpose::STANDARD.encode(&plaintext_body);

        // Decrypt headers if tier allows
        let headers = if limits.headers_included {
            msg.headers
                .as_ref()
                .and_then(|h| {
                    base64::engine::general_purpose::STANDARD.decode(h).ok()
                })
                .and_then(|encrypted| {
                    crypto::decrypt(&state.config.service_encryption_key, &encrypted).ok()
                })
                .and_then(|decrypted| String::from_utf8(decrypted).ok())
                .and_then(|json_str| serde_json::from_str(&json_str).ok())
        } else {
            None
        };

        responses.push(MessageResponse {
            object: "message",
            message_id: crypto::wire_message_id(&msg.message_id),
            fingerprint: crypto::wire_fingerprint(&msg.fingerprint),
            body: body_b64,
            headers,
            received_at: queue::nanos_to_seconds(msg.received_at),
            deliver_count: msg.deliver_count,
            encryption: msg.encryption,
            key_version: msg.key_version,
        });
    }

    Ok(Json(json!({
        "object": "list",
        "data": responses,
        "has_more": has_more,
        "queue_depth": queue_depth,
        "dlq_depth": dlq_depth,
    }))
    .into_response())
}

/// POST /endpoints/{endpoint_name}/messages/{message_id}/ack
pub async fn ack_message(
    State(state): State<AppState>,
    Path((endpoint_name, wire_id)): Path<(String, String)>,
    req: axum::extract::Request,
) -> Result<impl IntoResponse, HeraldError> {
    let api_key = auth::extract_api_key(&req)?;
    let raw_id = crypto::parse_wire_message_id(&wire_id)?;
    let mut conn = state.redis.clone();
    let account = auth::lookup_account(&mut conn, &api_key).await?;

    let acked = queue::ack(&mut conn, &account.customer_id, &endpoint_name, &raw_id).await?;

    if !acked {
        return Err(HeraldError::NotFound(format!(
            "message {wire_id} not in flight"
        )));
    }

    Ok(Json(json!({
        "object": "ack_result",
        "message_id": wire_id,
        "acknowledged": true,
    })))
}

#[derive(Debug, Deserialize)]
pub struct BatchAckRequest {
    pub message_ids: Vec<String>,
}

/// POST /endpoints/{endpoint_name}/messages/ack — batch acknowledge
pub async fn batch_ack_messages(
    State(state): State<AppState>,
    Path(endpoint_name): Path<String>,
    req: axum::extract::Request,
) -> Result<impl IntoResponse, HeraldError> {
    // Extract auth first, then body
    let api_key = auth::extract_api_key(&req)?;
    let body = axum::body::to_bytes(req.into_body(), 1024 * 1024)
        .await
        .map_err(|e| HeraldError::BadRequest(e.to_string()))?;
    let batch: BatchAckRequest =
        serde_json::from_slice(&body).map_err(|e| HeraldError::BadRequest(e.to_string()))?;

    let mut conn = state.redis.clone();
    let account = auth::lookup_account(&mut conn, &api_key).await?;

    let mut acknowledged = Vec::new();
    let mut failed = Vec::new();

    for wire_id in &batch.message_ids {
        let raw = match crypto::parse_wire_message_id(wire_id) {
            Ok(r) => r,
            Err(_) => {
                failed.push(wire_id.clone());
                continue;
            }
        };
        match queue::ack(&mut conn, &account.customer_id, &endpoint_name, &raw).await {
            Ok(true) => acknowledged.push(wire_id.clone()),
            _ => failed.push(wire_id.clone()),
        }
    }

    Ok(Json(json!({
        "object": "batch_ack_result",
        "acknowledged": acknowledged,
        "failed": failed,
    })))
}

/// NACK disposition. Open enum: future values may include
/// `delay_requeue`, `discard`, etc.
#[derive(Debug, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NackDisposition {
    #[default]
    Requeue,
    Dlq,
}

#[derive(Debug, Deserialize, Default)]
pub struct NackBody {
    #[serde(default)]
    pub disposition: NackDisposition,
}

/// POST /endpoints/{endpoint_name}/messages/{message_id}/nack
pub async fn nack_message(
    State(state): State<AppState>,
    Path((endpoint_name, wire_id)): Path<(String, String)>,
    req: axum::extract::Request,
) -> Result<impl IntoResponse, HeraldError> {
    let api_key = auth::extract_api_key(&req)?;
    let raw_id = crypto::parse_wire_message_id(&wire_id)?;

    // Body is optional; default to requeue.
    let body_bytes = axum::body::to_bytes(req.into_body(), 64 * 1024)
        .await
        .map_err(|e| HeraldError::BadRequest(e.to_string()))?;
    let body: NackBody = if body_bytes.is_empty() {
        NackBody::default()
    } else {
        serde_json::from_slice(&body_bytes).map_err(|e| {
            HeraldError::BadRequest(format!("invalid nack body: {e}"))
        })?
    };

    let mut conn = state.redis.clone();
    let account = auth::lookup_account(&mut conn, &api_key).await?;

    let permanent = body.disposition == NackDisposition::Dlq;
    let max_retries = 3; // Default, configurable per endpoint later
    let nacked = queue::nack(
        &mut conn,
        &account.customer_id,
        &endpoint_name,
        &raw_id,
        permanent,
        max_retries,
    )
    .await?;

    if !nacked {
        return Err(HeraldError::NotFound(format!(
            "message {wire_id} not in flight"
        )));
    }

    let disposition_str = if permanent { "dlq" } else { "requeue" };
    Ok(Json(json!({
        "object": "nack_result",
        "message_id": wire_id,
        "disposition": disposition_str,
    })))
}

/// POST /endpoints/{endpoint_name}/messages/{message_id}/heartbeat
pub async fn heartbeat(
    State(state): State<AppState>,
    Path((_endpoint_name, wire_id)): Path<(String, String)>,
    Query(params): Query<HeartbeatParams>,
    req: axum::extract::Request,
) -> Result<impl IntoResponse, HeraldError> {
    let api_key = auth::extract_api_key(&req)?;
    let raw_id = crypto::parse_wire_message_id(&wire_id)?;
    let mut conn = state.redis.clone();
    let _account = auth::lookup_account(&mut conn, &api_key).await?;

    let extend = params.extend.unwrap_or(300).clamp(30, 43200);
    let extended = queue::heartbeat(&mut conn, &raw_id, extend).await?;

    if !extended {
        return Err(HeraldError::NotFound(format!(
            "no visibility timeout for {wire_id}"
        )));
    }

    Ok(Json(json!({
        "object": "heartbeat_result",
        "message_id": wire_id,
        "visibility_timeout_extended": true,
        "extended_by": extend,
    })))
}

#[derive(Debug, Deserialize)]
pub struct HeartbeatParams {
    pub extend: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nack_disposition_default_is_requeue() {
        let body: NackBody = serde_json::from_str("{}").unwrap();
        assert_eq!(body.disposition, NackDisposition::Requeue);
    }

    #[test]
    fn test_nack_disposition_parses_dlq() {
        let body: NackBody =
            serde_json::from_str(r#"{"disposition":"dlq"}"#).unwrap();
        assert_eq!(body.disposition, NackDisposition::Dlq);
    }

    #[test]
    fn test_nack_disposition_parses_requeue() {
        let body: NackBody =
            serde_json::from_str(r#"{"disposition":"requeue"}"#).unwrap();
        assert_eq!(body.disposition, NackDisposition::Requeue);
    }

    #[test]
    fn test_nack_disposition_rejects_unknown() {
        assert!(serde_json::from_str::<NackBody>(r#"{"disposition":"foo"}"#).is_err());
    }
}

// =============================================================================
// Dead letter queue
//
// Messages reach the DLQ by nack(disposition=dlq) or by exhausting max_retries.
// Without these routes they were unreachable: the payload stayed in Redis until
// its retention TTL expired, with no way to see why it died or to retry it.
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct DlqListParams {
    #[serde(default)]
    pub offset: isize,
    #[serde(default = "default_dlq_limit")]
    pub limit: isize,
}

fn default_dlq_limit() -> isize {
    50
}

/// GET /endpoints/{endpoint_name}/dlq
///
/// Read-only. Listing the DLQ never replays, purges, or alters delivery counts.
pub async fn list_dlq(
    State(state): State<AppState>,
    Path(endpoint_name): Path<String>,
    Query(params): Query<DlqListParams>,
    req: axum::extract::Request,
) -> Result<impl IntoResponse, HeraldError> {
    let api_key = auth::extract_api_key(&req)?;
    let mut conn = state.redis.clone();
    let account = auth::lookup_account(&mut conn, &api_key).await?;
    let limits = account.tier.limits();

    let limit = params.limit.min(100);
    let entries =
        queue::dlq_list(&mut conn, &account.customer_id, &endpoint_name, params.offset, limit)
            .await?;
    let dlq_depth = queue::dlq_depth(&mut conn, &account.customer_id, &endpoint_name).await?;

    let mut data = Vec::with_capacity(entries.len());
    for entry in entries {
        match entry {
            queue::DlqEntry::Expired(message_id) => {
                data.push(json!({
                    "object": "dlq_entry",
                    "message_id": crypto::wire_message_id(&message_id),
                    "expired": true,
                }));
            }
            queue::DlqEntry::Message(msg) => {
                let plaintext = if msg.encryption == "none" {
                    msg.body.clone()
                } else {
                    crypto::decrypt(&state.config.service_encryption_key, &msg.body)?
                };
                let headers = if limits.headers_included {
                    msg.headers
                        .as_ref()
                        .and_then(|h| base64::engine::general_purpose::STANDARD.decode(h).ok())
                        .and_then(|e| crypto::decrypt(&state.config.service_encryption_key, &e).ok())
                        .and_then(|d| String::from_utf8(d).ok())
                        .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
                } else {
                    None
                };

                data.push(json!({
                    "object": "dlq_entry",
                    "message_id": crypto::wire_message_id(&msg.message_id),
                    "fingerprint": crypto::wire_fingerprint(&msg.fingerprint),
                    "body": base64::engine::general_purpose::STANDARD.encode(&plaintext),
                    "headers": headers,
                    "received_at": queue::nanos_to_seconds(msg.received_at),
                    "deliver_count": msg.deliver_count,
                    "encryption": msg.encryption,
                    "expired": false,
                }));
            }
        }
    }

    Ok(Json(json!({
        "object": "list",
        "data": data,
        "dlq_depth": dlq_depth,
        "has_more": params.offset + (data.len() as isize) < dlq_depth as isize,
    })))
}

/// POST /endpoints/{endpoint_name}/dlq/{message_id}/replay
pub async fn replay_dlq_message(
    State(state): State<AppState>,
    Path((endpoint_name, message_id)): Path<(String, String)>,
    req: axum::extract::Request,
) -> Result<impl IntoResponse, HeraldError> {
    let api_key = auth::extract_api_key(&req)?;
    let mut conn = state.redis.clone();
    let account = auth::lookup_account(&mut conn, &api_key).await?;
    let id = crypto::parse_wire_message_id(&message_id)?;

    let replayed =
        queue::dlq_replay(&mut conn, &account.customer_id, &endpoint_name, &id).await?;
    if !replayed {
        return Err(HeraldError::NotFound(format!(
            "no message {message_id} in the DLQ for {endpoint_name}"
        )));
    }

    Ok(Json(json!({
        "object": "dlq_replay",
        "message_id": message_id,
        "replayed": 1,
    })))
}

/// POST /endpoints/{endpoint_name}/dlq/replay — replay everything.
pub async fn replay_dlq(
    State(state): State<AppState>,
    Path(endpoint_name): Path<String>,
    req: axum::extract::Request,
) -> Result<impl IntoResponse, HeraldError> {
    let api_key = auth::extract_api_key(&req)?;
    let mut conn = state.redis.clone();
    let account = auth::lookup_account(&mut conn, &api_key).await?;

    let replayed =
        queue::dlq_replay_all(&mut conn, &account.customer_id, &endpoint_name).await?;

    Ok(Json(json!({
        "object": "dlq_replay",
        "endpoint": endpoint_name,
        "replayed": replayed,
    })))
}

/// DELETE /endpoints/{endpoint_name}/dlq/{message_id}
pub async fn purge_dlq_message(
    State(state): State<AppState>,
    Path((endpoint_name, message_id)): Path<(String, String)>,
    req: axum::extract::Request,
) -> Result<impl IntoResponse, HeraldError> {
    let api_key = auth::extract_api_key(&req)?;
    let mut conn = state.redis.clone();
    let account = auth::lookup_account(&mut conn, &api_key).await?;
    let id = crypto::parse_wire_message_id(&message_id)?;

    let purged = queue::dlq_purge(&mut conn, &account.customer_id, &endpoint_name, &id).await?;
    if !purged {
        return Err(HeraldError::NotFound(format!(
            "no message {message_id} in the DLQ for {endpoint_name}"
        )));
    }

    Ok(Json(json!({
        "object": "dlq_purge",
        "message_id": message_id,
        "purged": 1,
    })))
}

/// DELETE /endpoints/{endpoint_name}/dlq — purge everything. Irreversible.
pub async fn purge_dlq(
    State(state): State<AppState>,
    Path(endpoint_name): Path<String>,
    req: axum::extract::Request,
) -> Result<impl IntoResponse, HeraldError> {
    let api_key = auth::extract_api_key(&req)?;
    let mut conn = state.redis.clone();
    let account = auth::lookup_account(&mut conn, &api_key).await?;

    let purged = queue::dlq_purge_all(&mut conn, &account.customer_id, &endpoint_name).await?;

    Ok(Json(json!({
        "object": "dlq_purge",
        "endpoint": endpoint_name,
        "purged": purged,
    })))
}
