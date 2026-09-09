mod auth;
mod billing;
mod config;
mod crypto;
mod error;
mod queue;
mod routes;
mod state;

use std::time::Duration;

use axum::routing::{get, post};
use axum::Router;
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;
use tracing_subscriber::EnvFilter;

use crate::config::Config;
use crate::state::AppState;

#[tokio::main]
async fn main() {
    // Initialize structured logging
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .json()
        .init();

    let config = Config::from_env();

    tracing::info!(
        listen_addr = %config.listen_addr,
        admin_listen_addr = ?config.admin_listen_addr,
        redis_url = %config.redis_url,
        "starting herald-server"
    );

    // Connect to Redis
    let redis_client =
        redis::Client::open(config.redis_url.as_str()).expect("invalid Redis URL");
    let redis_conn = redis_client
        .get_multiplexed_async_connection()
        .await
        .expect("failed to connect to Redis");

    let state = AppState {
        redis: redis_conn,
        config: config.clone(),
    };

    // Start the reaper background task
    let reaper_state = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            interval.tick().await;
            let mut conn = reaper_state.redis.clone();
            if let Err(e) = queue::reap_expired(&mut conn, 3).await {
                tracing::error!(error = %e, "reaper error");
            }
        }
    });

    let listener = TcpListener::bind(&config.listen_addr)
        .await
        .expect("failed to bind");

    match config.admin_listen_addr.as_deref() {
        // Split mode: the ingest surface is exposed on `listen_addr` (safe to
        // publish to the internet), while registration, billing and the agent
        // API are only reachable on `admin_listen_addr` — bind that to a
        // private interface (loopback, a VPN address, a private subnet).
        Some(admin_addr) => {
            let admin_listener = TcpListener::bind(admin_addr)
                .await
                .expect("failed to bind admin listener");

            let public_app = finalize(ingest_routes(), state.clone());
            let admin_app = finalize(management_routes(), state);

            tracing::info!(addr = %config.listen_addr, "herald-server ingest listener");
            tracing::info!(addr = %admin_addr, "herald-server admin listener");

            tokio::select! {
                r = axum::serve(listener, public_app) => r.expect("ingest server error"),
                r = axum::serve(admin_listener, admin_app) => r.expect("admin server error"),
            }
        }
        // Single-listener mode (default): every route on one address.
        None => {
            let app = finalize(ingest_routes().merge(management_routes()), state);

            tracing::info!(addr = %config.listen_addr, "herald-server listening");

            axum::serve(listener, app).await.expect("server error");
        }
    }
}

/// Routes that inbound providers call. No authentication by design — anyone
/// holding the endpoint URL may POST to it, so this surface is safe to expose
/// publicly.
fn ingest_routes() -> Router<AppState> {
    Router::new()
        // Inbound webhook ingestion (no auth — providers POST freely)
        .route(
            "/{customer_id}/{endpoint_name}",
            post(routes::ingest::ingest_webhook),
        )
        // Stripe delivers signed callbacks from the public internet
        .route("/stripe/webhook", post(billing::stripe_webhook))
}

/// Routes that mint or consume credentials, or that read queued payloads back
/// out. API-key authenticated, but keep them off the public listener when a
/// private interface is available.
fn management_routes() -> Router<AppState> {
    Router::new()
        // Account registration (no auth — returns API key)
        .route("/register", post(routes::register::register))
        // Billing and tier management
        .route("/account/billing", get(billing::get_billing))
        .route("/account/tier", post(billing::set_tier))
        // Agent polling and management (auth required) — resource-oriented paths
        .route(
            "/endpoints/{endpoint_name}/messages",
            get(routes::agent::poll_messages),
        )
        .route(
            "/endpoints/{endpoint_name}/messages/ack",
            post(routes::agent::batch_ack_messages),
        )
        .route(
            "/endpoints/{endpoint_name}/messages/{message_id}/ack",
            post(routes::agent::ack_message),
        )
        .route(
            "/endpoints/{endpoint_name}/messages/{message_id}/nack",
            post(routes::agent::nack_message),
        )
        .route(
            "/endpoints/{endpoint_name}/messages/{message_id}/heartbeat",
            post(routes::agent::heartbeat),
        )
        // WebSocket streaming
        .route(
            "/endpoints/{endpoint_name}/stream",
            get(routes::websocket::websocket_handler),
        )
}

/// Attach `/health` and shared middleware. Health lives here rather than in
/// either group so both listeners answer probes in split mode.
fn finalize(router: Router<AppState>, state: AppState) -> Router {
    router
        .route("/health", get(health_check))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn health_check() -> &'static str {
    "ok"
}
