use std::time::Duration;

use reqwest::Client;
use rocky_core::config::RetryConfig;
use rocky_core::redacted::RedactedString;
use rocky_observe::events::{ErrorClass, PipelineEvent, global_event_bus};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use thiserror::Error;
use tracing::{debug, warn};

#[derive(Debug, Error)]
pub enum FivetranError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("API error ({code}): {message}")]
    Api { code: String, message: String },

    #[error("unexpected response format: {0}")]
    UnexpectedResponse(String),

    #[error("rate limited — retry after backoff")]
    RateLimited,

    #[error("run-level retry budget exhausted (limit {limit}); aborting remaining retries")]
    RetryBudgetExhausted { limit: u32 },
}

/// Async Fivetran REST client with Basic Auth and configurable retry.
pub struct FivetranClient {
    client: Client,
    base_url: String,
    api_key: RedactedString,
    api_secret: RedactedString,
    retry: RetryConfig,
    /// Shared retry budget across the run (§P2.7). Unbounded by default.
    retry_budget: rocky_core::retry_budget::RetryBudget,
}

/// Standard Fivetran API envelope.
#[derive(Debug, Deserialize)]
pub struct ApiResponse<T> {
    pub code: String,
    pub data: Option<T>,
    pub message: Option<String>,
}

/// Paginated list response.
#[derive(Debug, Deserialize)]
pub struct PaginatedData<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

impl FivetranClient {
    pub fn new(api_key: String, api_secret: String) -> Self {
        Self::with_retry(api_key, api_secret, RetryConfig::default())
    }

    pub fn with_retry(api_key: String, api_secret: String, retry: RetryConfig) -> Self {
        let retry_budget =
            rocky_core::retry_budget::RetryBudget::from_config(retry.max_retries_per_run);
        FivetranClient {
            client: Client::new(),
            base_url: "https://api.fivetran.com".to_string(),
            api_key: RedactedString::new(api_key),
            api_secret: RedactedString::new(api_secret),
            retry,
            retry_budget,
        }
    }

    /// Override the run-level [`RetryBudget`](rocky_core::retry_budget::RetryBudget).
    /// Used by `rocky run` when multiple adapters should share one budget.
    #[must_use]
    pub fn with_retry_budget(mut self, budget: rocky_core::retry_budget::RetryBudget) -> Self {
        self.retry_budget = budget;
        self
    }

    /// Creates a client pointing at a custom base URL (for testing with wiremock).
    #[cfg(any(test, feature = "test-support"))]
    pub fn with_base_url(api_key: String, api_secret: String, base_url: String) -> Self {
        FivetranClient {
            client: Client::new(),
            base_url,
            api_key: RedactedString::new(api_key),
            api_secret: RedactedString::new(api_secret),
            retry: RetryConfig {
                max_retries: 0,
                ..Default::default()
            },
            retry_budget: rocky_core::retry_budget::RetryBudget::unbounded(),
        }
    }

    /// Helper: try to consume one retry slot; if exhausted, return `Err(RetryBudgetExhausted)`.
    /// Roadmap §P2.7.
    fn check_retry_budget(&self) -> Result<(), FivetranError> {
        if self.retry_budget.try_consume() {
            Ok(())
        } else {
            let limit = self.retry_budget.total().unwrap_or(0);
            warn!(budget_limit = limit, "retry budget exhausted for this run");
            Err(FivetranError::RetryBudgetExhausted { limit })
        }
    }

    /// §P2.8 retry-event emission. Every retry-about-to-fire point in `get` /
    /// `get_single_page` should call this so event-bus subscribers see
    /// structured attempt count + classification instead of free-form text.
    fn emit_retry_event(&self, attempt: u32, reason: &str, class: ErrorClass) {
        global_event_bus().emit(
            PipelineEvent::new("http_retry")
                .with_error(reason.to_string())
                .with_attempt(attempt + 1, self.retry.max_retries)
                .with_error_class(class),
        );
    }

    /// GET request with automatic response envelope unwrapping and retry on transient errors.
    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T, FivetranError> {
        let url = format!("{}{}", self.base_url, path);

        for attempt in 0..=self.retry.max_retries {
            debug!(path, attempt, "GET");

            let resp = self
                .client
                .get(&url)
                .basic_auth(self.api_key.expose(), Some(self.api_secret.expose()))
                .send()
                .await;

            let resp = match resp {
                Ok(r) => r,
                Err(e)
                    if attempt < self.retry.max_retries && (e.is_connect() || e.is_timeout()) =>
                {
                    self.check_retry_budget()?;
                    let class = if e.is_timeout() {
                        ErrorClass::Timeout
                    } else {
                        ErrorClass::Transient
                    };
                    self.emit_retry_event(attempt, &e.to_string(), class);
                    let backoff = retry_backoff(&self.retry, attempt);
                    warn!(attempt = attempt + 1, backoff_ms = backoff.as_millis() as u64, error = %e, "transient HTTP error, retrying");
                    tokio::time::sleep(backoff).await;
                    continue;
                }
                Err(e) => return Err(e.into()),
            };

            if resp.status() == 429 {
                if attempt < self.retry.max_retries {
                    self.check_retry_budget()?;
                    self.emit_retry_event(attempt, "HTTP 429", ErrorClass::RateLimit);
                    let backoff = retry_backoff(&self.retry, attempt);
                    warn!(
                        attempt = attempt + 1,
                        backoff_ms = backoff.as_millis() as u64,
                        "rate limited, retrying"
                    );
                    tokio::time::sleep(backoff).await;
                    continue;
                }
                return Err(FivetranError::RateLimited);
            }

            if resp.status().is_server_error() && attempt < self.retry.max_retries {
                self.check_retry_budget()?;
                let status = resp.status().as_u16();
                self.emit_retry_event(
                    attempt,
                    &format!("HTTP {status}"),
                    ErrorClass::Transient,
                );
                let backoff = retry_backoff(&self.retry, attempt);
                warn!(
                    attempt = attempt + 1,
                    status,
                    backoff_ms = backoff.as_millis() as u64,
                    "server error, retrying"
                );
                tokio::time::sleep(backoff).await;
                continue;
            }

            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(FivetranError::Api {
                    code: status.to_string(),
                    message: body,
                });
            }

            let envelope: ApiResponse<T> = resp.json().await.map_err(|e| {
                FivetranError::UnexpectedResponse(format!("failed to parse response: {e}"))
            })?;

            if envelope.code != "Success" {
                return Err(FivetranError::Api {
                    code: envelope.code,
                    message: envelope.message.unwrap_or_default(),
                });
            }

            return envelope
                .data
                .ok_or_else(|| FivetranError::UnexpectedResponse("response has no data".into()));
        }

        unreachable!("retry loop should always return")
    }

    /// GET paginated list, following cursors to completion.
    /// Each page request retries independently on transient errors.
    pub async fn get_all_pages<T: DeserializeOwned>(
        &self,
        path: &str,
    ) -> Result<Vec<T>, FivetranError> {
        let mut all_items = Vec::new();
        let mut cursor: Option<String> = None;

        loop {
            let url = match &cursor {
                Some(c) => format!("{}{}?cursor={}&limit=100", self.base_url, path, c),
                None => format!("{}{}?limit=100", self.base_url, path),
            };

            let page: PaginatedData<T> =
                self.get_single_page(&url, path, cursor.as_deref()).await?;

            let has_next = page.next_cursor.is_some();
            cursor = page.next_cursor;
            all_items.extend(page.items);

            if !has_next {
                break;
            }
        }

        Ok(all_items)
    }

    /// Single paginated page fetch with retry logic.
    async fn get_single_page<T: DeserializeOwned>(
        &self,
        url: &str,
        path: &str,
        cursor: Option<&str>,
    ) -> Result<PaginatedData<T>, FivetranError> {
        for attempt in 0..=self.retry.max_retries {
            debug!(path, cursor, attempt, "GET (paginated)");

            let resp = self
                .client
                .get(url)
                .basic_auth(self.api_key.expose(), Some(self.api_secret.expose()))
                .send()
                .await;

            let resp = match resp {
                Ok(r) => r,
                Err(e)
                    if attempt < self.retry.max_retries && (e.is_connect() || e.is_timeout()) =>
                {
                    self.check_retry_budget()?;
                    let class = if e.is_timeout() {
                        ErrorClass::Timeout
                    } else {
                        ErrorClass::Transient
                    };
                    self.emit_retry_event(attempt, &e.to_string(), class);
                    let backoff = retry_backoff(&self.retry, attempt);
                    warn!(attempt = attempt + 1, backoff_ms = backoff.as_millis() as u64, error = %e, "transient HTTP error, retrying page");
                    tokio::time::sleep(backoff).await;
                    continue;
                }
                Err(e) => return Err(e.into()),
            };

            if resp.status() == 429 {
                if attempt < self.retry.max_retries {
                    self.check_retry_budget()?;
                    self.emit_retry_event(attempt, "HTTP 429", ErrorClass::RateLimit);
                    let backoff = retry_backoff(&self.retry, attempt);
                    warn!(
                        attempt = attempt + 1,
                        backoff_ms = backoff.as_millis() as u64,
                        "rate limited, retrying page"
                    );
                    tokio::time::sleep(backoff).await;
                    continue;
                }
                return Err(FivetranError::RateLimited);
            }

            if resp.status().is_server_error() && attempt < self.retry.max_retries {
                self.check_retry_budget()?;
                let status = resp.status().as_u16();
                self.emit_retry_event(
                    attempt,
                    &format!("HTTP {status}"),
                    ErrorClass::Transient,
                );
                let backoff = retry_backoff(&self.retry, attempt);
                warn!(
                    attempt = attempt + 1,
                    status,
                    backoff_ms = backoff.as_millis() as u64,
                    "server error, retrying page"
                );
                tokio::time::sleep(backoff).await;
                continue;
            }

            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(FivetranError::Api {
                    code: status.to_string(),
                    message: body,
                });
            }

            let envelope: ApiResponse<PaginatedData<T>> = resp.json().await.map_err(|e| {
                FivetranError::UnexpectedResponse(format!("failed to parse response: {e}"))
            })?;

            if envelope.code != "Success" {
                return Err(FivetranError::Api {
                    code: envelope.code,
                    message: envelope.message.unwrap_or_default(),
                });
            }

            return envelope.data.ok_or_else(|| {
                FivetranError::UnexpectedResponse("paginated response has no data".into())
            });
        }

        unreachable!("retry loop should always return")
    }
}

/// Computes backoff from RetryConfig with exponential growth, cap, and optional jitter.
fn retry_backoff(cfg: &RetryConfig, attempt: u32) -> Duration {
    let base = (cfg.initial_backoff_ms as f64) * cfg.backoff_multiplier.powi(attempt as i32);
    let capped = base.min(cfg.max_backoff_ms as f64) as u64;

    let ms = if cfg.jitter {
        // Use subsecond nanos as cheap jitter source (no rand dependency needed)
        let jitter_range = capped / 4; // +-25% jitter
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos() as u64;
        let jitter = nanos % (jitter_range.max(1));
        capped
            .saturating_sub(jitter_range / 2)
            .saturating_add(jitter)
    } else {
        capped
    };

    Duration::from_millis(ms)
}

impl std::fmt::Debug for FivetranClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FivetranClient")
            .field("base_url", &self.base_url)
            .field("api_key", &"***")
            .field("api_secret", &"***")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_debug_hides_secrets() {
        let client = FivetranClient::new("my_api_key_value".into(), "my_secret_value".into());
        let debug = format!("{client:?}");
        assert!(!debug.contains("my_api_key_value"));
        assert!(!debug.contains("my_secret_value"));
        assert!(debug.contains("***"));
    }

    #[test]
    fn test_api_response_deserialize() {
        let json = r#"{"code": "Success", "data": {"value": 42}, "message": null}"#;
        let resp: ApiResponse<serde_json::Value> = serde_json::from_str(json).unwrap();
        assert_eq!(resp.code, "Success");
        assert!(resp.data.is_some());
    }

    #[test]
    fn test_paginated_response_deserialize() {
        let json = r#"{
            "code": "Success",
            "data": {
                "items": [{"id": "1"}, {"id": "2"}],
                "next_cursor": "abc123"
            }
        }"#;
        let resp: ApiResponse<PaginatedData<serde_json::Value>> =
            serde_json::from_str(json).unwrap();
        let data = resp.data.unwrap();
        assert_eq!(data.items.len(), 2);
        assert_eq!(data.next_cursor, Some("abc123".to_string()));
    }

    #[test]
    fn test_paginated_last_page() {
        let json = r#"{
            "code": "Success",
            "data": {
                "items": [{"id": "3"}],
                "next_cursor": null
            }
        }"#;
        let resp: ApiResponse<PaginatedData<serde_json::Value>> =
            serde_json::from_str(json).unwrap();
        let data = resp.data.unwrap();
        assert_eq!(data.items.len(), 1);
        assert!(data.next_cursor.is_none());
    }
}
