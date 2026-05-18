use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use reqwest::{Client, Response};

use crate::ratelimit;

/// Percent-encode a single user-supplied URL path component.
///
/// Uses RFC 3986 unreserved chars minus `.` — `-`, `_`, `~`, and ASCII
/// alphanumerics pass through; `.` stays encoded so a hostile `..` cannot
/// climb out of the parent path. Every other byte (`/`, `?`, `#`, `%`,
/// spaces, etc.) is percent-encoded. Always apply this when interpolating
/// caller-controlled values (connector IDs, group IDs, table names) into a
/// URL path; path-traversal and query-injection bugs in source-adapter
/// HTTP clients begin and end here.
pub(crate) fn encode_path_segment(segment: &str) -> String {
    const PATH_SAFE: &AsciiSet = &NON_ALPHANUMERIC.remove(b'-').remove(b'_').remove(b'~');
    utf8_percent_encode(segment, PATH_SAFE).to_string()
}

/// Hard cap on the size of an upstream error body that we surface in
/// [`FivetranError::Api`]. Fivetran's documented errors are short
/// structured messages; truncating defends logs against an unexpectedly
/// large or attacker-shaped body if the upstream response ever changes.
const MAX_ERROR_BODY_BYTES: usize = 1024;

/// Truncate `body` to at most [`MAX_ERROR_BODY_BYTES`] bytes on a UTF-8
/// char boundary, appending `…(truncated)` when shortened.
fn truncate_error_body(body: &str) -> String {
    if body.len() <= MAX_ERROR_BODY_BYTES {
        return body.to_string();
    }
    let mut end = MAX_ERROR_BODY_BYTES;
    while end > 0 && !body.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…(truncated)", &body[..end])
}

/// Build the shared `reqwest::Client` used for every Fivetran API call.
/// `Client::new()` previously left both connect and request timeouts
/// unset — a stalled connection would block `rocky run` forever. The
/// 10s/120s pair matches the per-adapter `timeout_secs` default and the
/// other engine adapters' bounds.
fn build_http_client() -> Client {
    Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(120))
        .build()
        .expect("reqwest client builder is infallible with these options")
}
use rocky_core::config::RetryConfig;
use rocky_core::redacted::RedactedString;
use rocky_observe::events::{ErrorClass, PipelineEvent, global_event_bus, record_span_event};
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
    /// Short hex hash of the API key — used as the per-host coordination
    /// file name and as the `account_id` attribute on rate-limit OTLP
    /// span events. Computed once at construction so we never re-hash
    /// the credential on a hot path.
    account_id_hash: String,
    /// Directory holding the per-account shared rate-limit state file
    /// (FR-B). One file per account: `{ratelimit_dir}/{hash}.json`.
    ratelimit_dir: PathBuf,
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
        let account_id_hash = ratelimit::hash_account_id(&api_key);
        FivetranClient {
            client: build_http_client(),
            base_url: "https://api.fivetran.com".to_string(),
            api_key: RedactedString::new(api_key),
            api_secret: RedactedString::new(api_secret),
            retry,
            retry_budget,
            account_id_hash,
            ratelimit_dir: ratelimit::default_ratelimit_dir(),
        }
    }

    /// Override the run-level [`RetryBudget`](rocky_core::retry_budget::RetryBudget).
    /// Used by `rocky run` when multiple adapters should share one budget.
    #[must_use]
    pub fn with_retry_budget(mut self, budget: rocky_core::retry_budget::RetryBudget) -> Self {
        self.retry_budget = budget;
        self
    }

    /// Override the per-host rate-limit state directory (FR-B). Tests
    /// pass a per-test [`tempfile::tempdir()`] so concurrent runs don't
    /// collide on the default `${TMPDIR}/rocky-fivetran-ratelimit/`
    /// location. Production callers should leave the default in place
    /// so every `rocky-cli` process on the host shares a single file.
    #[must_use]
    pub fn with_ratelimit_dir(mut self, dir: PathBuf) -> Self {
        self.ratelimit_dir = dir;
        self
    }

    /// Resolve the per-account state file under [`Self::ratelimit_dir`].
    fn ratelimit_path(&self) -> PathBuf {
        ratelimit::account_state_path(&self.ratelimit_dir, &self.account_id_hash)
    }

    /// Block on any active shared rate-limit window before sending a
    /// request. Fail-open — see [`ratelimit::pre_request_wait`].
    async fn observe_shared_budget(&self) {
        ratelimit::pre_request_wait(&self.ratelimit_path()).await;
    }

    /// Record a new shared rate-limit window so other rocky-cli
    /// processes on the host observe the same backoff. Called when we
    /// receive a 429 / 503 with `Retry-After`. Also emits the
    /// `fivetran.rate_limit_observed` OTLP span event.
    ///
    /// `sleep_for` is the effective wait the caller is about to
    /// perform (already `max(header, fixed_backoff)` when `source =
    /// Header`). `header_value` is the unmodified value parsed from
    /// the upstream header (None for `source = Fallback`). Splitting
    /// the two lets dashboards see "upstream said 100ms but we slept
    /// 1s because backoff dominated" without conflating the signals.
    fn publish_rate_limit_observation(
        &self,
        sleep_for: Duration,
        header_value: Option<Duration>,
        source: RateLimitSource,
        status: u16,
    ) {
        let wake_at = SystemTime::now() + sleep_for;
        ratelimit::record_wake_at(&self.ratelimit_path(), wake_at);

        let to_ms = |d: Duration| -> u64 {
            d.as_millis()
                .min(u128::from(u64::MAX))
                .try_into()
                .unwrap_or(u64::MAX)
        };
        let mut evt = PipelineEvent::new("fivetran.rate_limit_observed")
            .with_error_class(ErrorClass::RateLimit)
            .with_metadata("retry_after_ms", to_ms(sleep_for))
            .with_metadata("source", source.as_str())
            .with_metadata("account_id", self.account_id_hash.clone())
            .with_metadata("status", u64::from(status));
        if let Some(h) = header_value {
            evt = evt.with_metadata("header_retry_after_ms", to_ms(h));
        }
        record_span_event(&evt);
        global_event_bus().emit(evt);
    }

    /// Creates a client pointing at a custom base URL (for testing with wiremock).
    #[cfg(any(test, feature = "test-support"))]
    pub fn with_base_url(api_key: String, api_secret: String, base_url: String) -> Self {
        let account_id_hash = ratelimit::hash_account_id(&api_key);
        FivetranClient {
            client: build_http_client(),
            base_url,
            api_key: RedactedString::new(api_key),
            api_secret: RedactedString::new(api_secret),
            retry: RetryConfig {
                max_retries: 0,
                ..Default::default()
            },
            retry_budget: rocky_core::retry_budget::RetryBudget::unbounded(),
            account_id_hash,
            // Tests that need an isolated shared-budget file should
            // chain `.with_ratelimit_dir(...)` after construction.
            ratelimit_dir: ratelimit::default_ratelimit_dir(),
        }
    }

    /// Test-only variant of [`Self::with_base_url`] that lets the
    /// caller supply a [`RetryConfig`] — used by FR-B's Retry-After
    /// integration tests where the first attempt must be retried so
    /// the post-`Retry-After` sleep is observable.
    #[cfg(any(test, feature = "test-support"))]
    pub fn with_base_url_and_retry(
        api_key: String,
        api_secret: String,
        base_url: String,
        retry: RetryConfig,
    ) -> Self {
        let account_id_hash = ratelimit::hash_account_id(&api_key);
        FivetranClient {
            client: build_http_client(),
            base_url,
            api_key: RedactedString::new(api_key),
            api_secret: RedactedString::new(api_secret),
            retry,
            retry_budget: rocky_core::retry_budget::RetryBudget::unbounded(),
            account_id_hash,
            ratelimit_dir: ratelimit::default_ratelimit_dir(),
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
        let evt = PipelineEvent::new("http_retry")
            .with_error(reason.to_string())
            .with_attempt(attempt + 1, self.retry.max_retries)
            .with_error_class(class);
        record_span_event(&evt);
        global_event_bus().emit(evt);
    }

    /// GET request with automatic response envelope unwrapping and retry on transient errors.
    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T, FivetranError> {
        let url = format!("{}{}", self.base_url, path);

        for attempt in 0..=self.retry.max_retries {
            debug!(path, attempt, "GET");

            // FR-B: observe any active shared backoff window before
            // sending — a peer rocky-cli that just hit a 429 may have
            // recorded a wake_at we should honor.
            self.observe_shared_budget().await;

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
                    let sleep_for = self.compute_throttle_sleep(&resp, attempt, 429);
                    warn!(
                        attempt = attempt + 1,
                        backoff_ms = sleep_for.as_millis() as u64,
                        "rate limited, retrying"
                    );
                    tokio::time::sleep(sleep_for).await;
                    continue;
                }
                return Err(FivetranError::RateLimited);
            }

            if resp.status().is_server_error() && attempt < self.retry.max_retries {
                self.check_retry_budget()?;
                let status = resp.status().as_u16();
                self.emit_retry_event(attempt, &format!("HTTP {status}"), ErrorClass::Transient);
                // 503 Service Unavailable carries `Retry-After` per
                // RFC 9110 §10.2.3 — honor it the same way as 429.
                let sleep_for = self.compute_throttle_sleep(&resp, attempt, status);
                warn!(
                    attempt = attempt + 1,
                    status,
                    backoff_ms = sleep_for.as_millis() as u64,
                    "server error, retrying"
                );
                tokio::time::sleep(sleep_for).await;
                continue;
            }

            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(FivetranError::Api {
                    code: status.to_string(),
                    message: truncate_error_body(&body),
                });
            }

            let envelope: ApiResponse<T> = resp.json().await.map_err(|e| {
                FivetranError::UnexpectedResponse(format!("failed to parse response: {e}"))
            })?;

            if envelope.code != "Success" {
                return Err(FivetranError::Api {
                    code: envelope.code,
                    message: truncate_error_body(&envelope.message.unwrap_or_default()),
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

            // FR-B: observe shared backoff window before issuing.
            self.observe_shared_budget().await;

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
                    let sleep_for = self.compute_throttle_sleep(&resp, attempt, 429);
                    warn!(
                        attempt = attempt + 1,
                        backoff_ms = sleep_for.as_millis() as u64,
                        "rate limited, retrying page"
                    );
                    tokio::time::sleep(sleep_for).await;
                    continue;
                }
                return Err(FivetranError::RateLimited);
            }

            if resp.status().is_server_error() && attempt < self.retry.max_retries {
                self.check_retry_budget()?;
                let status = resp.status().as_u16();
                self.emit_retry_event(attempt, &format!("HTTP {status}"), ErrorClass::Transient);
                let sleep_for = self.compute_throttle_sleep(&resp, attempt, status);
                warn!(
                    attempt = attempt + 1,
                    status,
                    backoff_ms = sleep_for.as_millis() as u64,
                    "server error, retrying page"
                );
                tokio::time::sleep(sleep_for).await;
                continue;
            }

            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(FivetranError::Api {
                    code: status.to_string(),
                    message: truncate_error_body(&body),
                });
            }

            let envelope: ApiResponse<PaginatedData<T>> = resp.json().await.map_err(|e| {
                FivetranError::UnexpectedResponse(format!("failed to parse response: {e}"))
            })?;

            if envelope.code != "Success" {
                return Err(FivetranError::Api {
                    code: envelope.code,
                    message: truncate_error_body(&envelope.message.unwrap_or_default()),
                });
            }

            return envelope.data.ok_or_else(|| {
                FivetranError::UnexpectedResponse("paginated response has no data".into())
            });
        }

        unreachable!("retry loop should always return")
    }

    /// Compute the sleep duration after a throttle-class response (429
    /// or 503). When the upstream returned a `Retry-After` header we
    /// take `max(retry_after, fixed_backoff)` per the FR-B contract,
    /// publish the shared rate-limit window so peer rocky-cli processes
    /// observe the same wake-up, and emit the `fivetran.rate_limit_observed`
    /// span event sourced as `"header"`. Without a header we use the
    /// existing per-attempt exponential backoff and tag the event as
    /// `"fallback"` so dashboards can spot adapters that aren't getting
    /// the upstream signal.
    fn compute_throttle_sleep(&self, resp: &Response, attempt: u32, status: u16) -> Duration {
        let backoff = retry_backoff(&self.retry, attempt);
        match parse_retry_after(resp) {
            Some(retry_after) => {
                let sleep_for = retry_after.max(backoff);
                self.publish_rate_limit_observation(
                    sleep_for,
                    Some(retry_after),
                    RateLimitSource::Header,
                    status,
                );
                sleep_for
            }
            None => {
                self.publish_rate_limit_observation(
                    backoff,
                    None,
                    RateLimitSource::Fallback,
                    status,
                );
                backoff
            }
        }
    }
}

/// Tag for the `source` field of the `fivetran.rate_limit_observed`
/// span event — distinguishes upstream-signalled throttles (`header`)
/// from local fixed-backoff fallbacks (`fallback`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RateLimitSource {
    /// The wait duration came from a `Retry-After` response header.
    Header,
    /// The upstream omitted `Retry-After`; we used the local fixed
    /// exponential-backoff schedule.
    Fallback,
}

impl RateLimitSource {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            RateLimitSource::Header => "header",
            RateLimitSource::Fallback => "fallback",
        }
    }
}

/// Parse a `Retry-After` header per RFC 9110 §10.2.3. The header may
/// be either a non-negative integer count of seconds or an HTTP-date
/// (RFC 7231 §7.1.1.1). Returns `None` when the header is absent,
/// unparseable, or refers to a past instant.
fn parse_retry_after(resp: &Response) -> Option<Duration> {
    let raw = resp.headers().get(reqwest::header::RETRY_AFTER)?;
    parse_retry_after_value(raw.to_str().ok()?, SystemTime::now())
}

/// Pure-string variant of [`parse_retry_after`], exposed so unit tests
/// can exercise the parser without an HTTP response in hand. `now`
/// fixes the reference for HTTP-date arithmetic.
fn parse_retry_after_value(raw: &str, now: SystemTime) -> Option<Duration> {
    let s = raw.trim();
    if s.is_empty() {
        return None;
    }

    // 1) Integer seconds form. RFC 9110 says non-negative; treat
    //    negative or overflowing values as "no useful signal" rather
    //    than guessing.
    if let Ok(secs) = s.parse::<u64>() {
        return Some(Duration::from_secs(secs));
    }

    // 2) HTTP-date form. `httpdate::parse_http_date` accepts all three
    //    formats RFC 7231 lists (IMF-fixdate, RFC 850, asctime). A
    //    date already in the past collapses to `None` so we don't
    //    immediately retry but also don't sleep for a negative amount.
    let target = httpdate::parse_http_date(s).ok()?;
    target.duration_since(now).ok()
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
    fn test_encode_path_segment_alphanumeric_passthrough() {
        // Plain alphanumeric IDs are unchanged.
        assert_eq!(encode_path_segment("conn_abc123"), "conn_abc123");
        assert_eq!(encode_path_segment("group_xyz"), "group_xyz");
    }

    #[test]
    fn test_encode_path_segment_unsafe_chars() {
        // Path-traversal: `..` is fully encoded so the path component cannot
        // climb out of `/v1/connectors/`.
        assert_eq!(encode_path_segment(".."), "%2E%2E");
        // Path separator must not survive — otherwise `attacker/admin` could
        // forge `/v1/connectors/attacker/admin` and hit a different endpoint.
        assert_eq!(encode_path_segment("a/b"), "a%2Fb");
        // Query-string injection: `?` and `#` are encoded.
        assert_eq!(encode_path_segment("a?b"), "a%3Fb");
        assert_eq!(encode_path_segment("a#b"), "a%23b");
        // Pre-encoded payloads round-trip the literal `%` so the server sees
        // the original byte rather than a re-decoded escape.
        assert_eq!(encode_path_segment("%2F"), "%252F");
        // Spaces and other ASCII whitespace must be encoded.
        assert_eq!(encode_path_segment("a b"), "a%20b");
    }

    #[test]
    fn test_encode_path_segment_unreserved_passthrough() {
        // RFC 3986 unreserved chars (minus `.`) survive untouched so that
        // ordinary identifiers (`my_id-1~v2`) don't become noise URLs.
        assert_eq!(encode_path_segment("my_id-1~v2"), "my_id-1~v2");
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

    /// `Retry-After: 30` → 30 seconds. The trivial integer-seconds
    /// form is what Fivetran sends today; HTTP-date form is documented
    /// in the spec and parsed below.
    #[test]
    fn parse_retry_after_integer_seconds() {
        let now = SystemTime::now();
        let parsed = parse_retry_after_value("30", now).expect("integer-seconds must parse");
        assert_eq!(parsed, Duration::from_secs(30));
    }

    /// `Retry-After: 0` is valid — caller should fall through to
    /// `max(retry_after, current_backoff)` and end up using the
    /// fixed backoff for this attempt. The parser itself returns 0.
    #[test]
    fn parse_retry_after_zero_seconds() {
        let now = SystemTime::now();
        let parsed = parse_retry_after_value("0", now).expect("zero must parse");
        assert_eq!(parsed, Duration::from_secs(0));
    }

    /// HTTP-date form (RFC 7231 IMF-fixdate). Reference instant is
    /// pinned so the test is deterministic regardless of wall-clock.
    #[test]
    fn parse_retry_after_http_date_future() {
        // 100 seconds after 2026-01-01T00:00:00Z.
        let now = httpdate::parse_http_date("Thu, 01 Jan 2026 00:00:00 GMT").unwrap();
        let parsed = parse_retry_after_value("Thu, 01 Jan 2026 00:01:40 GMT", now)
            .expect("future HTTP-date must parse");
        assert_eq!(parsed, Duration::from_secs(100));
    }

    /// HTTP-date already in the past collapses to `None` so the
    /// caller falls back to the fixed schedule rather than sleeping
    /// for an invalid (negative) duration.
    #[test]
    fn parse_retry_after_http_date_past_is_none() {
        let now = httpdate::parse_http_date("Thu, 01 Jan 2026 00:01:00 GMT").unwrap();
        let parsed = parse_retry_after_value("Thu, 01 Jan 2026 00:00:00 GMT", now);
        assert!(parsed.is_none(), "past date should collapse to None");
    }

    #[test]
    fn parse_retry_after_empty_is_none() {
        assert!(parse_retry_after_value("", SystemTime::now()).is_none());
        assert!(parse_retry_after_value("   ", SystemTime::now()).is_none());
    }

    #[test]
    fn parse_retry_after_garbage_is_none() {
        assert!(parse_retry_after_value("soonish", SystemTime::now()).is_none());
        assert!(parse_retry_after_value("-5", SystemTime::now()).is_none());
    }

    #[test]
    fn rate_limit_source_string_shape() {
        // The wire form is consumed by dashboards / log greps — keep
        // it stable across refactors.
        assert_eq!(RateLimitSource::Header.as_str(), "header");
        assert_eq!(RateLimitSource::Fallback.as_str(), "fallback");
    }
}
