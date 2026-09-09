//! Admin subcommands: the management half of Herald.
//!
//! `run` is the delivery path (Herald invokes your agent). These are the other
//! direction — creating accounts, inspecting queues, settling messages — and
//! they exist so an agent can operate Herald without a human editing config.
//!
//! Every command prints JSON on `--json` so callers (notably herald-mcp) parse
//! rather than scrape.

use serde_json::{json, Value};

use crate::client::HeraldClient;
use crate::config::Config;
use crate::error::CliError;

/// Where to reach the server and how to authenticate.
///
/// Resolution order is flag, then environment, then config file. The config
/// file is optional: admin commands must work for an agent that has env vars
/// and no `~/.config/herald/config.yaml`, which is why this does not call
/// `Config::load` fallibly.
pub struct Target {
    pub server: String,
    pub api_key: String,
}

pub fn resolve(
    server: Option<String>,
    api_key: Option<String>,
    config_path: &std::path::PathBuf,
) -> Result<Target, CliError> {
    let cfg = Config::load(config_path).ok();

    let server = server
        .or_else(|| std::env::var("HERALD_SERVER").ok().filter(|s| !s.is_empty()))
        .or_else(|| cfg.as_ref().map(|c| c.server.clone()))
        .ok_or_else(|| {
            CliError::Config(
                "no server: pass --server, set HERALD_SERVER, or add `server:` to the config"
                    .into(),
            )
        })?;

    let api_key = api_key
        .or_else(|| std::env::var("HERALD_API_KEY").ok().filter(|s| !s.is_empty()))
        .or_else(|| cfg.as_ref().map(|c| c.api_key.clone()))
        .ok_or_else(|| {
            CliError::Config(
                "no API key: pass --api-key, set HERALD_API_KEY, or add `api_key:` to the config"
                    .into(),
            )
        })?;

    Ok(Target { server, api_key })
}

/// Server base URL for `register`, which needs no API key.
pub fn resolve_server(
    server: Option<String>,
    config_path: &std::path::PathBuf,
) -> Result<String, CliError> {
    server
        .or_else(|| std::env::var("HERALD_SERVER").ok().filter(|s| !s.is_empty()))
        .or_else(|| Config::load(config_path).ok().map(|c| c.server))
        .ok_or_else(|| {
            CliError::Config("no server: pass --server or set HERALD_SERVER".into())
        })
}

pub async fn register(
    server: String,
    customer_id: String,
    secret: Option<String>,
) -> Result<Value, CliError> {
    let secret = secret
        .or_else(|| std::env::var("HERALD_REGISTER_SECRET").ok().filter(|s| !s.is_empty()));
    HeraldClient::register(&server, &customer_id, secret.as_deref()).await
}

pub async fn depth(t: Target, endpoint: String) -> Result<Value, CliError> {
    let client = HeraldClient::new(&t.server, &t.api_key);
    let depth = client.depth(&endpoint).await?;
    Ok(json!({ "endpoint": endpoint, "queue_depth": depth }))
}

/// Lease messages. This is a *read that writes*: each message returned is moved
/// to the in-flight list, its deliver_count incremented, and a visibility timer
/// started. Anything not ACKed within the timeout is redelivered. Use `depth`
/// when you only want to look.
pub async fn poll(
    t: Target,
    endpoint: String,
    limit: usize,
    visibility_timeout: u64,
) -> Result<Value, CliError> {
    use base64::Engine as _;

    let client = HeraldClient::new(&t.server, &t.api_key);
    let resp = client.poll_full(&endpoint, limit, visibility_timeout).await?;

    let decoded: Vec<Value> = resp
        .data
        .iter()
        .map(|m| {
            let body = base64::engine::general_purpose::STANDARD
                .decode(&m.body)
                .ok()
                .and_then(|b| String::from_utf8(b).ok());
            json!({
                "message_id": m.message_id,
                "fingerprint": m.fingerprint,
                "received_at": m.received_at,
                "deliver_count": m.deliver_count,
                "headers": m.headers,
                // Decoded for readability; `body_base64` stays authoritative
                // for payloads that are not valid UTF-8.
                "body": body,
                "body_base64": m.body,
            })
        })
        .collect();

    Ok(json!({
        "endpoint": endpoint,
        "queue_depth": resp.queue_depth,
        "has_more": resp.has_more,
        "leased": decoded.len(),
        "visibility_timeout": visibility_timeout,
        "data": decoded,
    }))
}

pub async fn ack(
    t: Target,
    endpoint: String,
    message_ids: Vec<String>,
) -> Result<Value, CliError> {
    let client = HeraldClient::new(&t.server, &t.api_key);

    if message_ids.len() == 1 {
        client.ack(&endpoint, &message_ids[0]).await?;
    } else {
        client.batch_ack(&endpoint, &message_ids).await?;
    }
    Ok(json!({ "acked": message_ids }))
}

pub async fn nack(
    t: Target,
    endpoint: String,
    message_id: String,
    dlq: bool,
) -> Result<Value, CliError> {
    let client = HeraldClient::new(&t.server, &t.api_key);
    client.nack(&endpoint, &message_id, dlq).await?;

    let mut out = json!({
        "message_id": message_id,
        "disposition": if dlq { "dlq" } else { "requeue" },
    });
    if dlq {
        // Say so at the call site: the server has no route to read the DLQ
        // back, so this is a one-way door.
        out["warning"] = json!("this server exposes no route to read or drain the DLQ; the message is not retrievable over the API");
    }
    Ok(out)
}

pub async fn heartbeat(
    t: Target,
    endpoint: String,
    message_id: String,
    extend: Option<u64>,
) -> Result<Value, CliError> {
    let client = HeraldClient::new(&t.server, &t.api_key);
    client.heartbeat(&endpoint, &message_id, extend).await?;
    Ok(json!({ "message_id": message_id, "extended_by": extend }))
}

pub async fn account(t: Target, tier: Option<String>) -> Result<Value, CliError> {
    let client = HeraldClient::new(&t.server, &t.api_key);
    match tier {
        Some(tier) => client.set_tier(&tier).await,
        None => client.billing().await,
    }
}

/// Read the dead letter queue. Read-only.
///
/// Bodies come back base64-encoded on the wire; decode them here as `poll`
/// does, keeping `body_base64` authoritative for payloads that are not UTF-8.
pub async fn dlq_list(
    t: Target,
    endpoint: String,
    offset: i64,
    limit: i64,
) -> Result<Value, CliError> {
    use base64::Engine as _;

    let client = HeraldClient::new(&t.server, &t.api_key);
    let mut resp = client.dlq_list(&endpoint, offset, limit).await?;

    if let Some(rows) = resp.get_mut("data").and_then(|d| d.as_array_mut()) {
        for row in rows.iter_mut() {
            let decoded = row
                .get("body")
                .and_then(|b| b.as_str())
                .and_then(|b| base64::engine::general_purpose::STANDARD.decode(b).ok())
                .and_then(|b| String::from_utf8(b).ok());
            if let Some(b64) = row.get("body").cloned() {
                row["body_base64"] = b64;
            }
            row["body"] = match decoded {
                Some(text) => json!(text),
                None => Value::Null,
            };
        }
    }
    resp["endpoint"] = json!(endpoint);
    Ok(resp)
}

/// Move dead messages back onto the main queue. `message_id` None replays all.
///
/// deliver_count is reset server-side, otherwise a replayed message is already
/// at max_retries and dies again on its first failure.
pub async fn dlq_replay(
    t: Target,
    endpoint: String,
    message_id: Option<String>,
) -> Result<Value, CliError> {
    let client = HeraldClient::new(&t.server, &t.api_key);
    client.dlq_replay(&endpoint, message_id.as_deref()).await
}

/// Permanently delete dead messages. `message_id` None purges the whole DLQ.
pub async fn dlq_purge(
    t: Target,
    endpoint: String,
    message_id: Option<String>,
) -> Result<Value, CliError> {
    let client = HeraldClient::new(&t.server, &t.api_key);
    client.dlq_purge(&endpoint, message_id.as_deref()).await
}
