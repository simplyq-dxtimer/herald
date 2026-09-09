use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use base64::Engine as _;
use serde_json::json;

use crate::auth;
use crate::config::Tier;
use crate::crypto;
use crate::error::HeraldError;
use crate::queue::{self, Message};
use crate::state::AppState;

/// POST /:customer_id/:endpoint_name
/// Inbound webhook ingestion endpoint.
pub async fn ingest_webhook(
    State(state): State<AppState>,
    Path((customer_id, endpoint_name)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, HeraldError> {
    // Validate path components — colons break Redis key structure
    if customer_id.contains(':') || endpoint_name.contains(':') {
        return Err(HeraldError::BadRequest(
            "customer_id and endpoint_name must not contain colons".into(),
        ));
    }

    let mut conn = state.redis.clone();
    let endpoint = format!("{customer_id}/{endpoint_name}");

    // Reject unknown customers before touching any other key. Ingest is
    // unauthenticated by design, so without this an arbitrary POST to
    // /<anything>/<anything> would create a queue and persist a payload for an
    // account that was never registered. The per-customer rate limit does not
    // contain that: the caller picks customer_id, so every invented name gets
    // its own budget. Checked first so a probe costs one EXISTS and writes
    // nothing.
    if !account_exists(&mut conn, &customer_id).await? {
        return Err(HeraldError::NotFound(format!(
            "no endpoint at /{customer_id}/{endpoint_name}"
        )));
    }

    // Check per-customer ingest auth (if configured)
    if let Some(ingest_auth) = auth::load_ingest_auth(
        &mut conn,
        &customer_id,
        &state.config.service_encryption_key,
    )
    .await?
    {
        auth::validate_ingest_auth(&ingest_auth, &headers, &body)?;
    }

    // Look up account tier (for now, default to Free if not found)
    let tier = lookup_tier(&mut conn, &customer_id).await;
    let limits = tier.limits();

    // C003: Check payload size
    if body.len() > limits.max_payload_bytes {
        return Err(HeraldError::PayloadTooLarge {
            size: body.len(),
            limit: limits.max_payload_bytes,
        });
    }

    // C003: Check rate limit
    if !queue::check_rate_limit(&mut conn, &customer_id, limits.max_messages_per_day).await? {
        return Err(HeraldError::RateLimited);
    }

    // C004: Check queue depth
    if !queue::check_queue_depth(&mut conn, &customer_id, &endpoint_name, limits.max_queue_depth)
        .await?
    {
        return Err(HeraldError::QueueFull);
    }

    // C005: Compute fingerprint for deduplication
    let fp = crypto::fingerprint(&body);

    // Check dedup
    if queue::check_dedup(&mut conn, &customer_id, &endpoint_name, &fp).await? {
        tracing::info!(
            fingerprint = %fp,
            endpoint = %endpoint,
            "deduplicated at ingestion"
        );
        return Ok((
            StatusCode::OK,
            Json(json!({
                "object": "ingest_result",
                "fingerprint": crypto::wire_fingerprint(&fp),
                "deduplicated": true,
            })),
        ));
    }

    let received_at = queue::now_nanos();

    // Compute unique message ID
    let message_id = crypto::message_id(&endpoint, received_at, &body);

    // Load per-customer config (encryption mode, retention override)
    let customer_config = auth::load_customer_config(&mut conn, &customer_id).await?;

    // C002: Encrypt body based on customer config
    let (stored_body, encryption_label) = match customer_config.encryption {
        auth::EncryptionMode::Service => (
            crypto::encrypt(&state.config.service_encryption_key, &body)?,
            "service",
        ),
        auth::EncryptionMode::None => (body.to_vec(), "none"),
    };

    // Encrypt headers (always service key — headers may contain auth tokens)
    let headers_json = serialize_headers(&headers);
    let encrypted_headers =
        crypto::encrypt(&state.config.service_encryption_key, headers_json.as_bytes())?;
    let headers_b64 =
        base64::engine::general_purpose::STANDARD.encode(&encrypted_headers);

    let msg = Message {
        message_id: message_id.clone(),
        fingerprint: fp.clone(),
        endpoint: endpoint_name.clone(),
        headers: Some(headers_b64),
        body: stored_body,
        encryption: encryption_label.to_string(),
        key_version: None,
        received_at,
        deliver_count: 0,
    };

    let retention_secs =
        customer_config.effective_retention_secs(limits.retention.as_secs());
    queue::enqueue(&mut conn, &customer_id, &msg, retention_secs).await?;

    Ok((
        StatusCode::OK,
        Json(json!({
            "object": "ingest_result",
            "message_id": crypto::wire_message_id(&message_id),
            "fingerprint": crypto::wire_fingerprint(&fp),
            "received_at": queue::nanos_to_seconds(received_at),
            "received_at_ns": received_at.to_string(),
        })),
    ))
}

/// Serialize relevant request headers to JSON.
fn serialize_headers(headers: &HeaderMap) -> String {
    let mut map = serde_json::Map::new();
    for (name, value) in headers.iter() {
        if let Ok(v) = value.to_str() {
            map.insert(name.to_string(), serde_json::Value::String(v.to_string()));
        }
    }
    serde_json::Value::Object(map).to_string()
}

/// Look up the tier for a customer. Falls back to Free if unknown.
/// Whether `customer_id` has been registered. `register` writes
/// `customer_apikey:<id>` for every account, so its presence is the
/// authoritative existence check.
async fn account_exists(
    conn: &mut redis::aio::MultiplexedConnection,
    customer_id: &str,
) -> Result<bool, HeraldError> {
    let exists: bool = redis::cmd("EXISTS")
        .arg(format!("customer_apikey:{customer_id}"))
        .query_async(conn)
        .await?;
    Ok(exists)
}

async fn lookup_tier(conn: &mut redis::aio::MultiplexedConnection, customer_id: &str) -> Tier {
    let tier_str: Option<String> = redis::cmd("GET")
        .arg(format!("tier:{customer_id}"))
        .query_async(conn)
        .await
        .unwrap_or(None);

    match tier_str.as_deref() {
        Some("standard") => Tier::Standard,
        Some("pro") => Tier::Pro,
        Some("enterprise") => Tier::Enterprise,
        _ => Tier::Free,
    }
}
