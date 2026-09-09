use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Config {
    pub redis_url: String,
    pub listen_addr: String,
    /// If set, `listen_addr` serves only the ingest surface (provider webhooks
    /// plus `/health`) and registration, billing and the agent API move to this
    /// address instead. Bind it to a private interface so queued payloads are
    /// not readable from the public internet.
    /// Unset = every route on `listen_addr` (single-listener behavior).
    pub admin_listen_addr: Option<String>,
    pub service_encryption_key: [u8; 32],
    /// If set, POST /register requires this as a Bearer token.
    /// Unset = open registration (hosted service behavior).
    pub register_secret: Option<String>,
    /// Stripe API key for billing. Unset = billing disabled.
    /// Read from STRIPE_API_KEY env var. Never logged or serialized.
    pub stripe_api_key: Option<String>,
    /// Stripe webhook signing secret. Required for webhook verification.
    pub stripe_webhook_secret: Option<String>,
    /// Base URL for Stripe checkout success/cancel redirects.
    pub base_url: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    Free,
    Standard,
    Pro,
    Enterprise,
}

#[derive(Debug, Clone)]
pub struct TierLimits {
    pub max_endpoints: u32,
    pub max_messages_per_day: u64,
    pub burst_per_minute: u64,
    pub max_queue_depth: u64,
    pub max_payload_bytes: usize,
    pub retention: Duration,
    pub websocket_allowed: bool,
    pub headers_included: bool,
}

impl Tier {
    pub fn limits(&self) -> TierLimits {
        match self {
            Tier::Free => TierLimits {
                max_endpoints: 1,
                max_messages_per_day: 100,
                burst_per_minute: 10,
                max_queue_depth: 100,
                max_payload_bytes: 64 * 1024,
                retention: Duration::from_secs(7 * 24 * 3600),
                websocket_allowed: false,
                headers_included: false,
            },
            Tier::Standard => TierLimits {
                max_endpoints: 10,
                max_messages_per_day: 10_000,
                burst_per_minute: 100,
                max_queue_depth: 10_000,
                max_payload_bytes: 1024 * 1024,
                retention: Duration::from_secs(30 * 24 * 3600),
                websocket_allowed: true,
                headers_included: true,
            },
            Tier::Pro => TierLimits {
                max_endpoints: u32::MAX,
                max_messages_per_day: 500_000,
                burst_per_minute: 5_000,
                max_queue_depth: 100_000,
                max_payload_bytes: 10 * 1024 * 1024,
                retention: Duration::from_secs(90 * 24 * 3600),
                websocket_allowed: true,
                headers_included: true,
            },
            Tier::Enterprise => TierLimits {
                max_endpoints: u32::MAX,
                max_messages_per_day: u64::MAX,
                burst_per_minute: u64::MAX,
                max_queue_depth: u64::MAX,
                max_payload_bytes: 100 * 1024 * 1024,
                retention: Duration::from_secs(365 * 24 * 3600),
                websocket_allowed: true,
                headers_included: true,
            },
        }
    }
}

impl Config {
    pub fn from_env() -> Self {
        let redis_url =
            std::env::var("HERALD_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
        let listen_addr =
            std::env::var("HERALD_LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());

        let admin_listen_addr = std::env::var("HERALD_ADMIN_LISTEN_ADDR")
            .ok()
            .filter(|s| !s.is_empty());

        let key_hex = std::env::var("HERALD_ENCRYPTION_KEY").unwrap_or_else(|_| {
            if std::env::var("HERALD_DEV_MODE").is_ok() {
                tracing::warn!("HERALD_ENCRYPTION_KEY not set, generating ephemeral key (dev mode)");
                let mut key = [0u8; 32];
                use rand::RngCore;
                rand::thread_rng().fill_bytes(&mut key);
                hex::encode(key)
            } else {
                panic!(
                    "HERALD_ENCRYPTION_KEY must be set (64 hex chars). \
                     Set HERALD_DEV_MODE=1 to generate an ephemeral key for development."
                );
            }
        });

        let key_bytes = hex::decode(&key_hex).expect("HERALD_ENCRYPTION_KEY must be valid hex");
        let mut service_encryption_key = [0u8; 32];
        service_encryption_key.copy_from_slice(&key_bytes[..32]);

        let register_secret = std::env::var("HERALD_REGISTER_SECRET").ok()
            .filter(|s| !s.is_empty());

        let stripe_api_key = std::env::var("STRIPE_API_KEY").ok()
            .filter(|s| !s.is_empty());

        let stripe_webhook_secret = std::env::var("STRIPE_WEBHOOK_SECRET").ok()
            .filter(|s| !s.is_empty());

        let base_url = std::env::var("HERALD_BASE_URL")
            .unwrap_or_else(|_| "https://proxy.herald.tools".to_string());

        Config {
            redis_url,
            listen_addr,
            admin_listen_addr,
            service_encryption_key,
            register_secret,
            stripe_api_key,
            stripe_webhook_secret,
            base_url,
        }
    }
}
