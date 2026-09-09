//! Stripe billing integration for Herald tiers.
//!
//! Stripe handles: receipts, failed payment emails, dunning, retries.
//! Herald handles: tier upgrade on checkout.session.completed,
//! tier downgrade on customer.subscription.deleted.

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use hmac::{Hmac, Mac};
use serde::Deserialize;
use serde_json::json;
use sha2::Sha256;

use crate::auth;
use crate::config::Tier;
use crate::error::HeraldError;
use crate::state::AppState;

// =============================================================================
// Stripe Price Constants (created via API, live in Stripe)
// =============================================================================

// New dedicated Herald Stripe account (separate from personal)
pub const PRICE_STANDARD: &str = "price_1THyznBWDEkTlwUGAizP6VIB"; // $9/mo
pub const PRICE_PRO: &str = "price_1THyzoBWDEkTlwUGsbVMwX1u"; // $29/mo
pub const PRICE_ENTERPRISE: &str = "price_1THyzoBWDEkTlwUGCoBk8JgK"; // $99/mo

fn tier_to_price(tier: &str) -> Option<&'static str> {
    match tier {
        "standard" => Some(PRICE_STANDARD),
        "pro" => Some(PRICE_PRO),
        "enterprise" => Some(PRICE_ENTERPRISE),
        _ => None,
    }
}

fn tier_price_label(tier: &str) -> &'static str {
    match tier {
        "standard" => "$9/mo",
        "pro" => "$29/mo",
        "enterprise" => "$99/mo",
        _ => "free",
    }
}

// =============================================================================
// Tier Upgrade (Redis — used by both Stripe webhook and API)
// =============================================================================

/// Upgrade a customer's tier in Redis. Updates both tier:{customer_id}
/// and the account JSON in apikey:{api_key}.
pub async fn upgrade_tier(
    conn: &mut redis::aio::MultiplexedConnection,
    customer_id: &str,
    new_tier: Tier,
) -> Result<(), HeraldError> {
    let tier_str = match new_tier {
        Tier::Free => "free",
        Tier::Standard => "standard",
        Tier::Pro => "pro",
        Tier::Enterprise => "enterprise",
    };

    // Update tier key
    let _: () = redis::cmd("SET")
        .arg(format!("tier:{customer_id}"))
        .arg(tier_str)
        .query_async(conn)
        .await?;

    // Also update account JSON if api key exists
    let api_key: Option<String> = redis::cmd("GET")
        .arg(format!("customer_apikey:{customer_id}"))
        .query_async(conn)
        .await?;

    if let Some(key) = api_key {
        let account_json = serde_json::json!({
            "customer_id": customer_id,
            "tier": tier_str,
        });
        let _: () = redis::cmd("SET")
            .arg(format!("apikey:{key}"))
            .arg(account_json.to_string())
            .query_async(conn)
            .await?;
    }

    tracing::info!(
        customer_id = %customer_id,
        tier = %tier_str,
        "tier updated"
    );

    Ok(())
}

fn parse_tier(s: &str) -> Result<Tier, HeraldError> {
    match s {
        "free" => Ok(Tier::Free),
        "standard" => Ok(Tier::Standard),
        "pro" => Ok(Tier::Pro),
        "enterprise" => Ok(Tier::Enterprise),
        _ => Err(HeraldError::BadRequest(format!("invalid tier: {s}"))),
    }
}

// =============================================================================
// GET /account/billing — current tier + upgrade options
// =============================================================================

pub async fn get_billing(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, HeraldError> {
    let key = auth::extract_bearer_from_headers(&headers)?;
    let mut conn = state.redis.clone();
    let account = auth::lookup_account(&mut conn, &key).await?;

    let current_tier = match account.tier {
        Tier::Free => "free",
        Tier::Standard => "standard",
        Tier::Pro => "pro",
        Tier::Enterprise => "enterprise",
    };

    // A self-hosted deployment has no Stripe key, which is a configuration
    // state and not a failure. Report the account and its limits, with no
    // upgrade options to offer, instead of a 500 that makes /account/billing
    // unusable for every self-hoster.
    let Some(stripe_key) = state.config.stripe_api_key.as_ref() else {
        let limits = account.tier.limits();
        return Ok(Json(json!({
            "customer_id": account.customer_id,
            "tier": current_tier,
            "billing": "not_configured",
            "upgrade_options": [],
            "limits": {
                "max_endpoints": limits.max_endpoints,
                "max_messages_per_day": limits.max_messages_per_day,
                "burst_per_minute": limits.burst_per_minute,
                "max_queue_depth": limits.max_queue_depth,
                "max_payload_bytes": limits.max_payload_bytes,
                "retention_seconds": limits.retention.as_secs(),
                "websocket_allowed": limits.websocket_allowed,
                "headers_included": limits.headers_included,
            },
        })));
    };

    // Build upgrade options (only tiers above current)
    let all_tiers = ["standard", "pro", "enterprise"];
    let current_idx = match current_tier {
        "standard" => 1,
        "pro" => 2,
        "enterprise" => 3,
        _ => 0,
    };

    let mut upgrade_options = Vec::new();
    for (i, tier) in all_tiers.iter().enumerate() {
        if i + 1 > current_idx {
            if let Some(price_id) = tier_to_price(tier) {
                // Create Stripe Checkout session
                match create_checkout_session(
                    stripe_key,
                    &state.config.base_url,
                    &account.customer_id,
                    tier,
                    price_id,
                )
                .await
                {
                    Ok(url) => {
                        upgrade_options.push(json!({
                            "tier": tier,
                            "price": tier_price_label(tier),
                            "checkout_url": url,
                        }));
                    }
                    Err(e) => {
                        tracing::warn!(tier = tier, error = %e, "failed to create checkout session");
                    }
                }
            }
        }
    }

    Ok(Json(json!({
        "customer_id": account.customer_id,
        "tier": current_tier,
        "upgrade_options": upgrade_options,
    })))
}

// =============================================================================
// POST /account/tier — API-based tier change (for Signet/admin)
// =============================================================================

#[derive(Deserialize)]
pub struct TierChangeRequest {
    pub tier: String,
}

pub async fn set_tier(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, HeraldError> {
    let key = auth::extract_bearer_from_headers(&headers)?;
    let mut conn = state.redis.clone();
    let account = auth::lookup_account(&mut conn, &key).await?;

    let req: TierChangeRequest = serde_json::from_slice(&body)
        .map_err(|e| HeraldError::BadRequest(format!("invalid request: {e}")))?;

    let new_tier = parse_tier(&req.tier)?;
    upgrade_tier(&mut conn, &account.customer_id, new_tier).await?;

    Ok(Json(json!({
        "customer_id": account.customer_id,
        "tier": req.tier,
        "updated": true,
    })))
}

// =============================================================================
// POST /stripe/webhook — Stripe event handler
// =============================================================================

pub async fn stripe_webhook(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, HeraldError> {
    let webhook_secret = state.config.stripe_webhook_secret.as_ref().ok_or_else(|| {
        HeraldError::Internal("stripe webhook secret not configured".into())
    })?;

    // Verify Stripe signature
    let sig_header = headers
        .get("stripe-signature")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| HeraldError::Unauthorized("missing stripe-signature".into()))?;

    verify_stripe_signature(&body, sig_header, webhook_secret)?;

    // Parse event
    let event: serde_json::Value = serde_json::from_slice(&body)
        .map_err(|e| HeraldError::BadRequest(format!("invalid event JSON: {e}")))?;

    let event_type = event["type"].as_str().unwrap_or("");
    let mut conn = state.redis.clone();

    match event_type {
        "checkout.session.completed" => {
            let session = &event["data"]["object"];
            let customer_id = session["client_reference_id"]
                .as_str()
                .ok_or_else(|| HeraldError::BadRequest("missing client_reference_id".into()))?;
            let tier = session["metadata"]["herald_tier"]
                .as_str()
                .ok_or_else(|| HeraldError::BadRequest("missing herald_tier metadata".into()))?;

            let new_tier = parse_tier(tier)?;
            upgrade_tier(&mut conn, customer_id, new_tier).await?;
            tracing::info!(customer_id = customer_id, tier = tier, "stripe checkout completed — tier upgraded");
        }
        "customer.subscription.deleted" => {
            // Subscription cancelled (failed payments, manual cancel, etc.)
            // Downgrade to Free
            let subscription = &event["data"]["object"];
            if let Some(customer_id) = subscription["metadata"]["herald_customer_id"].as_str() {
                upgrade_tier(&mut conn, customer_id, Tier::Free).await?;
                tracing::info!(customer_id = customer_id, "subscription deleted — downgraded to free");
            }
        }
        _ => {
            tracing::debug!(event_type = event_type, "unhandled stripe event");
        }
    }

    Ok(StatusCode::OK)
}

// =============================================================================
// Stripe Helpers
// =============================================================================

/// Create a Stripe Checkout session for upgrading to a paid tier.
async fn create_checkout_session(
    api_key: &str,
    base_url: &str,
    customer_id: &str,
    tier: &str,
    price_id: &str,
) -> Result<String, HeraldError> {
    let client = reqwest::Client::new();

    let params = [
        ("mode", "subscription"),
        ("client_reference_id", customer_id),
        ("line_items[0][price]", price_id),
        ("line_items[0][quantity]", "1"),
        ("metadata[herald_tier]", tier),
        ("metadata[herald_customer_id]", customer_id),
        ("success_url", &format!("{base_url}/docs?upgraded={tier}")),
        ("cancel_url", &format!("{base_url}/docs")),
    ];

    let resp = client
        .post("https://api.stripe.com/v1/checkout/sessions")
        .basic_auth(api_key, None::<&str>)
        .form(&params)
        .send()
        .await
        .map_err(|e| HeraldError::Internal(format!("stripe request failed: {e}")))?;

    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| HeraldError::Internal(format!("stripe response parse failed: {e}")))?;

    body["url"]
        .as_str()
        .map(|s| s.to_string())
        .ok_or_else(|| {
            let err = body["error"]["message"].as_str().unwrap_or("unknown error");
            HeraldError::Internal(format!("stripe checkout failed: {err}"))
        })
}

/// Verify Stripe webhook signature (v1 scheme).
fn verify_stripe_signature(
    payload: &[u8],
    sig_header: &str,
    secret: &str,
) -> Result<(), HeraldError> {
    // Parse sig header: t=timestamp,v1=signature
    let mut timestamp = None;
    let mut signatures = Vec::new();

    for part in sig_header.split(',') {
        if let Some(ts) = part.strip_prefix("t=") {
            timestamp = Some(ts);
        } else if let Some(sig) = part.strip_prefix("v1=") {
            signatures.push(sig);
        }
    }

    let ts = timestamp.ok_or_else(|| {
        HeraldError::Unauthorized("missing timestamp in stripe signature".into())
    })?;

    if signatures.is_empty() {
        return Err(HeraldError::Unauthorized("missing v1 signature".into()));
    }

    // Compute expected signature: HMAC-SHA256(secret, "timestamp.payload")
    let signed_payload = format!("{ts}.{}", String::from_utf8_lossy(payload));
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
        .map_err(|e| HeraldError::Internal(format!("hmac init: {e}")))?;
    mac.update(signed_payload.as_bytes());
    let expected = hex::encode(mac.finalize().into_bytes());

    // Check if any v1 signature matches
    if signatures.iter().any(|s| *s == expected) {
        Ok(())
    } else {
        Err(HeraldError::Unauthorized("stripe signature mismatch".into()))
    }
}
