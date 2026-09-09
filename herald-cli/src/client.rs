use serde::{Deserialize, Serialize};

use crate::error::CliError;

/// Herald API client for polling, ACK, and NACK operations.
pub struct HeraldClient {
    base_url: String,
    api_key: String,
    http: reqwest::Client,
}

#[derive(Debug, Deserialize)]
pub struct PollResponse {
    #[serde(default)]
    pub data: Vec<QueueMessage>,
    #[serde(default)]
    pub has_more: bool,
    #[serde(default)]
    pub queue_depth: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct QueueMessage {
    /// Wire-format message id (e.g. `msg_<hex>`). Treat as opaque.
    pub message_id: String,
    /// Wire-format fingerprint (e.g. `fp_<hex>`). Treat as opaque.
    pub fingerprint: String,
    pub body: String,
    pub headers: Option<serde_json::Value>,
    /// Unix integer seconds since epoch.
    pub received_at: i64,
    pub deliver_count: u32,
    pub encryption: String,
    pub key_version: Option<String>,
}

#[derive(Debug, Serialize)]
struct BatchAckRequest {
    message_ids: Vec<String>,
}

#[derive(Debug, Serialize)]
struct NackBody {
    disposition: &'static str,
}

impl HeraldClient {
    pub fn new(base_url: &str, api_key: &str) -> Self {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("failed to build HTTP client");

        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key: api_key.to_string(),
            http,
        }
    }

    /// Poll for messages from an endpoint.
    pub async fn poll(
        &self,
        endpoint: &str,
        limit: usize,
        visibility_timeout: u64,
    ) -> Result<Vec<QueueMessage>, CliError> {
        Ok(self.poll_full(endpoint, limit, visibility_timeout).await?.data)
    }

    /// Poll, keeping the whole envelope. `queue_depth` and `has_more` are only
    /// available here; `poll` discards them.
    pub async fn poll_full(
        &self,
        endpoint: &str,
        limit: usize,
        visibility_timeout: u64,
    ) -> Result<PollResponse, CliError> {
        let url = format!(
            "{}/endpoints/{}/messages?limit={}&visibility_timeout={}",
            self.base_url, endpoint, limit, visibility_timeout
        );

        let resp = self
            .http
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .send()
            .await
            .map_err(|e| CliError::Http(e.to_string()))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(CliError::Http(format!("{status}: {body}")));
        }

        resp.json().await.map_err(|e| CliError::Http(e.to_string()))
    }

    /// Queue depth without leasing anything. `limit=0` makes the server skip
    /// its fetch loop entirely — no RPOPLPUSH, no visibility timer, no
    /// deliver_count increment — while still reporting depth. Never implement
    /// this by polling and discarding: that would lease and redeliver every
    /// message you looked at.
    pub async fn depth(&self, endpoint: &str) -> Result<u64, CliError> {
        Ok(self.poll_full(endpoint, 0, 30).await?.queue_depth)
    }

    /// Acknowledge several messages in one request.
    pub async fn batch_ack(
        &self,
        endpoint: &str,
        message_ids: &[String],
    ) -> Result<(), CliError> {
        let url = format!("{}/endpoints/{}/messages/ack", self.base_url, endpoint);
        let resp = self
            .http
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .json(&BatchAckRequest { message_ids: message_ids.to_vec() })
            .send()
            .await
            .map_err(|e| CliError::Http(e.to_string()))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(CliError::Http(format!("batch ack failed ({status}): {body}")));
        }
        Ok(())
    }

    /// Extend the visibility timeout on an in-flight message.
    pub async fn heartbeat(
        &self,
        endpoint: &str,
        message_id: &str,
        extend: Option<u64>,
    ) -> Result<(), CliError> {
        let mut url = format!(
            "{}/endpoints/{}/messages/{}/heartbeat",
            self.base_url, endpoint, message_id
        );
        if let Some(secs) = extend {
            url.push_str(&format!("?extend={secs}"));
        }
        let resp = self
            .http
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .send()
            .await
            .map_err(|e| CliError::Http(e.to_string()))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(CliError::Http(format!("heartbeat failed ({status}): {body}")));
        }
        Ok(())
    }

    /// Current account and billing state.
    pub async fn billing(&self) -> Result<serde_json::Value, CliError> {
        let url = format!("{}/account/billing", self.base_url);
        let resp = self
            .http
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .send()
            .await
            .map_err(|e| CliError::Http(e.to_string()))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(CliError::Http(format!("billing failed ({status}): {body}")));
        }
        resp.json().await.map_err(|e| CliError::Http(e.to_string()))
    }

    /// Change the account tier.
    pub async fn set_tier(&self, tier: &str) -> Result<serde_json::Value, CliError> {
        let url = format!("{}/account/tier", self.base_url);
        let resp = self
            .http
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .json(&serde_json::json!({ "tier": tier }))
            .send()
            .await
            .map_err(|e| CliError::Http(e.to_string()))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(CliError::Http(format!("set tier failed ({status}): {body}")));
        }
        resp.json().await.map_err(|e| CliError::Http(e.to_string()))
    }

    /// Create an account. Unlike every other call this authenticates with the
    /// server's registration secret, not an API key, so it takes no `self`.
    pub async fn register(
        base_url: &str,
        customer_id: &str,
        secret: Option<&str>,
    ) -> Result<serde_json::Value, CliError> {
        let url = format!("{}/register", base_url.trim_end_matches('/'));
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| CliError::Http(e.to_string()))?;

        let mut req = http.post(&url).json(&serde_json::json!({
            "customer_id": customer_id,
        }));
        if let Some(secret) = secret {
            req = req.header("Authorization", format!("Bearer {secret}"));
        }

        let resp = req.send().await.map_err(|e| CliError::Http(e.to_string()))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            let hint = if status == reqwest::StatusCode::UNAUTHORIZED {
                " (this server gates /register — pass --secret or set HERALD_REGISTER_SECRET)"
            } else {
                ""
            };
            return Err(CliError::Http(format!("register failed ({status}): {body}{hint}")));
        }
        resp.json().await.map_err(|e| CliError::Http(e.to_string()))
    }

    /// Acknowledge a processed message. `message_id` should be the wire-format
    /// id returned by `poll` (e.g. `msg_<hex>`).
    pub async fn ack(&self, endpoint: &str, message_id: &str) -> Result<(), CliError> {
        let url = format!(
            "{}/endpoints/{}/messages/{}/ack",
            self.base_url, endpoint, message_id
        );

        let resp = self
            .http
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .send()
            .await
            .map_err(|e| CliError::Http(e.to_string()))?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(CliError::Http(format!("ack failed: {body}")));
        }

        Ok(())
    }

    /// Negative-acknowledge a message. `permanent: true` routes to the DLQ;
    /// `false` requeues for another attempt.
    pub async fn nack(
        &self,
        endpoint: &str,
        message_id: &str,
        permanent: bool,
    ) -> Result<(), CliError> {
        let url = format!(
            "{}/endpoints/{}/messages/{}/nack",
            self.base_url, endpoint, message_id
        );

        let body = NackBody {
            disposition: if permanent { "dlq" } else { "requeue" },
        };

        let resp = self
            .http
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .json(&body)
            .send()
            .await
            .map_err(|e| CliError::Http(e.to_string()))?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(CliError::Http(format!("nack failed: {body}")));
        }

        Ok(())
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn api_key(&self) -> &str {
        &self.api_key
    }
}
