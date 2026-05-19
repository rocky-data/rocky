use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use chrono::Utc;
use futures::stream::{self, StreamExt};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use reqwest::{Client, Response};
use tokio::sync::Mutex;

use crate::circuit_breaker::{AlwaysClosed, CircuitState, FailureKind, FivetranCircuitBreaker};
use crate::connector as ft_connector;
use crate::envelope::{
    FivetranColumnConfig, FivetranConnectorStatus, FivetranConnectorSummary, FivetranDestination,
    FivetranSchemaConfig, FivetranSchemaEntry, FivetranStateEnvelope, FivetranTableConfig,
};
use crate::ratelimit::{self, FileBudget, RatelimitBudget};
use crate::schema as ft_schema;
use crate::stampede::{self, NoLock, StampedeLock};
use crate::state_cache::{
    self, FivetranStateCache, NoCache, WriteOutcome, observability as cache_obs,
};

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

    #[error("Fivetran circuit breaker open for account; no HTTP attempted")]
    CircuitOpen,

    #[error(
        "emit-fivetran-state: all {total} connector(s) returned missing schema_config; no healthy connectors found"
    )]
    NoHealthyConnectors { total: usize },
}

/// Wire-decode shape for `GET /v1/destinations/{id}`. The envelope
/// projects only a subset; this struct captures the same surface but
/// stays narrow (no `JsonSchema`, no public re-export) so the public
/// envelope contract isn't coupled to the upstream wire format.
#[derive(Debug, Clone, Deserialize)]
struct DestinationWire {
    pub id: String,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub time_zone: Option<String>,
    #[serde(default)]
    pub service: Option<String>,
    #[serde(default)]
    pub setup_status: Option<String>,
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
    /// (FR-B Phase 1 file backend). Retained even when a Valkey budget
    /// is wired so that fail-open of the Valkey path can degrade to
    /// the per-host file backend without losing scope.
    ratelimit_dir: PathBuf,
    /// In-process envelope memoization for FR-C. Keyed by
    /// `destination_id` so a single client servicing multiple
    /// destinations stays correct, even though the common case is one
    /// destination per client. Wrapped in [`Arc`] so a [`FivetranClient`]
    /// can be cloned (or shared via `Arc<FivetranClient>`) without
    /// duplicating the cache.
    ///
    /// The cache is the FIRST layer in the three-layer envelope cache:
    /// (1) in-process memo (this field) → (2) cross-process
    /// [`FivetranStateCache`](crate::state_cache::FivetranStateCache)
    /// backend → (3) live Fivetran HTTP. Both layers are bypassed when
    /// `fetch_envelope(_, refresh = true)` is called.
    envelope_cache: Arc<Mutex<BTreeMap<String, FivetranStateEnvelope>>>,
    /// Optional persistent state cache (FR-A). When `Some`, layer (2)
    /// of the three-layer cache; on miss the client falls through to
    /// the HTTP path, then write-backs to this layer + the in-process
    /// memo on success. When `None` the cache layer is transparent.
    state_cache: Arc<dyn FivetranStateCache>,
    /// Cross-pod rate-limit budget (FR-B Phase 2). Defaults to a
    /// [`FileBudget`] rooted at [`Self::ratelimit_dir`] so a client
    /// constructed without explicit wiring behaves exactly like a
    /// pre-Phase-2 client.
    budget: Arc<dyn RatelimitBudget>,
    /// Distributed cache-stampede lock (Layer 1). Defaults to
    /// [`NoLock`] so callers without coordination configured always
    /// take the leader path.
    stampede: Arc<dyn StampedeLock>,
    /// Per-account circuit breaker (Layer 3). Defaults to
    /// [`AlwaysClosed`] so callers without coordination configured
    /// never short-circuit.
    circuit: Arc<dyn FivetranCircuitBreaker>,
    /// Lock TTL passed to [`StampedeLock::acquire`]. 0 means use the
    /// crate default.
    stampede_lock_ttl: Duration,
    /// Follower-poll timeout for the cache-poll loop.
    stampede_poll_timeout: Duration,
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
        let ratelimit_dir = ratelimit::default_ratelimit_dir();
        FivetranClient {
            client: build_http_client(),
            base_url: "https://api.fivetran.com".to_string(),
            api_key: RedactedString::new(api_key),
            api_secret: RedactedString::new(api_secret),
            retry,
            retry_budget,
            account_id_hash,
            ratelimit_dir: ratelimit_dir.clone(),
            envelope_cache: Arc::new(Mutex::new(BTreeMap::new())),
            state_cache: Arc::new(NoCache),
            budget: Arc::new(FileBudget::new(ratelimit_dir)),
            stampede: Arc::new(NoLock),
            circuit: Arc::new(AlwaysClosed),
            stampede_lock_ttl: stampede::DEFAULT_LOCK_TTL,
            stampede_poll_timeout: stampede::DEFAULT_POLL_TIMEOUT,
        }
    }

    /// Override the run-level [`RetryBudget`](rocky_core::retry_budget::RetryBudget).
    /// Used by `rocky run` when multiple adapters should share one budget.
    #[must_use]
    pub fn with_retry_budget(mut self, budget: rocky_core::retry_budget::RetryBudget) -> Self {
        self.retry_budget = budget;
        self
    }

    /// Override the per-host rate-limit state directory (FR-B Phase 1
    /// file backend). Tests pass a per-test [`tempfile::tempdir()`]
    /// so concurrent runs don't collide on the default
    /// `${TMPDIR}/rocky-fivetran-ratelimit/` location. Production
    /// callers should leave the default in place so every `rocky-cli`
    /// process on the host shares a single file.
    ///
    /// Also re-wires the default file-backed [`RatelimitBudget`] to
    /// point at the same directory — so a test that overrides the
    /// dir without explicitly setting a Valkey budget still sees the
    /// per-test isolation.
    #[must_use]
    pub fn with_ratelimit_dir(mut self, dir: PathBuf) -> Self {
        self.ratelimit_dir = dir.clone();
        // Only replace the budget if it's still the default file
        // backend; an explicitly-wired Valkey budget should survive
        // a ratelimit-dir override (callers don't expect that to
        // clobber their cross-pod store).
        if self.budget.backend() == "file" {
            self.budget = Arc::new(FileBudget::new(dir));
        }
        self
    }

    /// Override the cross-pod rate-limit budget (FR-B Phase 2). Default
    /// is a [`FileBudget`] rooted at [`Self::ratelimit_dir`]; production
    /// callers swap in a [`ValkeyBudget`](crate::ratelimit_valkey::ValkeyBudget)
    /// to share the wake_at window across hosts.
    #[must_use]
    pub fn with_ratelimit_budget(mut self, budget: Arc<dyn RatelimitBudget>) -> Self {
        self.budget = budget;
        self
    }

    /// Override the cache-stampede lock (Layer 1). Default is
    /// [`NoLock`]; production callers swap in
    /// [`ValkeyLock`](crate::stampede::ValkeyLock) to elect a single
    /// leader on cold-start herds.
    #[must_use]
    pub fn with_stampede_lock(mut self, lock: Arc<dyn StampedeLock>) -> Self {
        self.stampede = lock;
        self
    }

    /// Override the per-account circuit breaker (Layer 3). Default
    /// is [`AlwaysClosed`]; production callers swap in
    /// [`ValkeyCircuit`](crate::circuit_breaker::ValkeyCircuit) to
    /// short-circuit during Fivetran outages.
    #[must_use]
    pub fn with_circuit_breaker(mut self, cb: Arc<dyn FivetranCircuitBreaker>) -> Self {
        self.circuit = cb;
        self
    }

    /// Override the stampede lock TTL (Layer 1). Caps how long a
    /// leader can hold the lock before followers are allowed to elect
    /// a new leader.
    #[must_use]
    pub fn with_stampede_lock_ttl(mut self, ttl: Duration) -> Self {
        self.stampede_lock_ttl = ttl;
        self
    }

    /// Override the cap on the follower-poll loop (Layer 1).
    #[must_use]
    pub fn with_stampede_poll_timeout(mut self, timeout: Duration) -> Self {
        self.stampede_poll_timeout = timeout;
        self
    }

    /// Block on any active shared rate-limit window before sending a
    /// request. Fail-open — see [`ratelimit::pre_request_wait_budget`].
    async fn observe_shared_budget(&self) {
        ratelimit::pre_request_wait_budget(self.budget.as_ref(), &self.account_id_hash).await;
    }

    /// Record a new shared rate-limit window so other rocky-cli
    /// processes (on the host AND across the cluster, when the
    /// Valkey-backed budget is wired) observe the same backoff.
    /// Called when we receive a 429 / 503 with `Retry-After`. Also
    /// emits the `fivetran.rate_limit_observed` OTLP span event.
    ///
    /// `sleep_for` is the effective wait the caller is about to
    /// perform (already `max(header, fixed_backoff)` when `source =
    /// Header`). `header_value` is the unmodified value parsed from
    /// the upstream header (None for `source = Fallback`). Splitting
    /// the two lets dashboards see "upstream said 100ms but we slept
    /// 1s because backoff dominated" without conflating the signals.
    ///
    /// The write goes through the configured [`RatelimitBudget`] —
    /// `FileBudget` for the default Phase-1 per-host behavior or
    /// `ValkeyBudget` (Phase 2) for the cross-pod variant. Errors
    /// are fail-open: a Valkey outage degrades to the next request
    /// re-discovering the throttle on its own.
    async fn publish_rate_limit_observation(
        &self,
        sleep_for: Duration,
        header_value: Option<Duration>,
        source: RateLimitSource,
        status: u16,
    ) {
        let wake_at = SystemTime::now() + sleep_for;
        // Dispatch the budget write via the trait so a Valkey budget
        // shares the wake_at across pods. Awaited inline so a
        // short-lived CLI invocation can't exit before the persist
        // lands — matches the pre-refactor file-backend's synchronous
        // contract.
        ratelimit::record_wake_at_budget(self.budget.as_ref(), &self.account_id_hash, wake_at)
            .await;

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
            .with_metadata("ratelimit_backend", self.budget.backend())
            .with_metadata("status", u64::from(status));
        if let Some(h) = header_value {
            evt = evt.with_metadata("header_retry_after_ms", to_ms(h));
        }
        record_span_event(&evt);
        global_event_bus().emit(evt);
    }

    /// Override the persistent state cache (FR-A). Defaults to
    /// [`NoCache`] so a client constructed without explicit wiring
    /// behaves exactly like a pre-FR-A client. Production callers
    /// thread the configured backend via
    /// [`state_cache::build_state_cache`].
    #[must_use]
    pub fn with_state_cache(mut self, cache: Arc<dyn FivetranStateCache>) -> Self {
        self.state_cache = cache;
        self
    }

    /// Creates a client pointing at a custom base URL (for testing with wiremock).
    #[cfg(any(test, feature = "test-support"))]
    pub fn with_base_url(api_key: String, api_secret: String, base_url: String) -> Self {
        let account_id_hash = ratelimit::hash_account_id(&api_key);
        let ratelimit_dir = ratelimit::default_ratelimit_dir();
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
            ratelimit_dir: ratelimit_dir.clone(),
            envelope_cache: Arc::new(Mutex::new(BTreeMap::new())),
            state_cache: Arc::new(NoCache),
            budget: Arc::new(FileBudget::new(ratelimit_dir)),
            stampede: Arc::new(NoLock),
            circuit: Arc::new(AlwaysClosed),
            stampede_lock_ttl: stampede::DEFAULT_LOCK_TTL,
            stampede_poll_timeout: stampede::DEFAULT_POLL_TIMEOUT,
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
        let ratelimit_dir = ratelimit::default_ratelimit_dir();
        FivetranClient {
            client: build_http_client(),
            base_url,
            api_key: RedactedString::new(api_key),
            api_secret: RedactedString::new(api_secret),
            retry,
            retry_budget: rocky_core::retry_budget::RetryBudget::unbounded(),
            account_id_hash,
            ratelimit_dir: ratelimit_dir.clone(),
            envelope_cache: Arc::new(Mutex::new(BTreeMap::new())),
            state_cache: Arc::new(NoCache),
            budget: Arc::new(FileBudget::new(ratelimit_dir)),
            stampede: Arc::new(NoLock),
            circuit: Arc::new(AlwaysClosed),
            stampede_lock_ttl: stampede::DEFAULT_LOCK_TTL,
            stampede_poll_timeout: stampede::DEFAULT_POLL_TIMEOUT,
        }
    }

    /// Build the canonical cache key for `destination_id` under this
    /// client's account hash. Exposed via `pub(crate)` so test helpers
    /// inside the crate can validate cache plumbing without re-deriving
    /// the encoding.
    pub(crate) fn cache_key(&self, destination_id: &str) -> String {
        state_cache::cache_key(&self.account_id_hash, destination_id)
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
                    let sleep_for = self.compute_throttle_sleep(&resp, attempt, 429).await;
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
                let sleep_for = self.compute_throttle_sleep(&resp, attempt, status).await;
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
                    let sleep_for = self.compute_throttle_sleep(&resp, attempt, 429).await;
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
                let sleep_for = self.compute_throttle_sleep(&resp, attempt, status).await;
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
    async fn compute_throttle_sleep(&self, resp: &Response, attempt: u32, status: u16) -> Duration {
        let backoff = retry_backoff(&self.retry, attempt);
        match parse_retry_after(resp) {
            Some(retry_after) => {
                let sleep_for = retry_after.max(backoff);
                self.publish_rate_limit_observation(
                    sleep_for,
                    Some(retry_after),
                    RateLimitSource::Header,
                    status,
                )
                .await;
                sleep_for
            }
            None => {
                self.publish_rate_limit_observation(
                    backoff,
                    None,
                    RateLimitSource::Fallback,
                    status,
                )
                .await;
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

impl FivetranClient {
    /// Fetch the per-destination metadata block — `GET /v1/destinations/{id}`.
    ///
    /// Returns the same shape the envelope's
    /// [`FivetranDestination`] block projects. Net-new HTTP path in
    /// PR-1 — earlier Rocky versions discovered destinations purely
    /// indirectly via `groups/{id}/connectors`.
    pub async fn get_destination(
        &self,
        destination_id: &str,
    ) -> Result<FivetranDestination, FivetranError> {
        let path = format!("/v1/destinations/{}", encode_path_segment(destination_id));
        let wire: DestinationWire = self.get(&path).await?;
        Ok(FivetranDestination {
            id: wire.id,
            region: wire.region,
            time_zone: wire.time_zone,
            service: wire.service,
            setup_status: wire.setup_status,
        })
    }

    /// Fetch (or return memoized) canonical envelope for `destination_id`.
    ///
    /// Five-layer walk:
    ///
    ///   L0. In-process memo (this client's [`Self::envelope_cache`])
    ///       — `refresh = false`, key present → return clone.
    ///   L2. Cross-process [`FivetranStateCache`] backend (FR-A) —
    ///       `refresh = false`, primary miss → attempt
    ///       `state_cache.read(<account_hash>/<destination_id>)`.
    ///       On hit: hydrate the in-process memo + return.
    ///   L1. Cache-stampede lock (NEW, Layer 1) — on cache miss the
    ///       client tries to acquire `<key>:lock NX EX <ttl>`. The
    ///       leader proceeds to the HTTP path; followers wait on the
    ///       cache key with bounded polling until the leader's write
    ///       becomes visible.
    ///   L4. Per-account circuit breaker (NEW, Layer 3) — checked
    ///       BEFORE every HTTP attempt. `Open` short-circuits with
    ///       [`FivetranError::CircuitOpen`]; `HalfOpen`/`Closed`
    ///       proceed. After the HTTP path the outcome is recorded so
    ///       the breaker transitions on its own.
    ///   L3. Cross-pod rate-limit budget (already shared via the
    ///       `RatelimitBudget` trait inside [`get`]).
    ///   L5. Retry-After + exponential backoff (already inside
    ///       [`get`]).
    ///
    /// `refresh = true` skips L0, L2, and L1 on read; the breaker
    /// (L4) is still consulted so an `Open` circuit short-circuits
    /// even on `--no-cache` refreshes.
    ///
    /// Fail-open posture:
    /// - L1 lock backend unreachable → fall through to direct HTTP
    ///   (no stampede protection, but no block).
    /// - L4 breaker read fails → treat as `Closed` (don't refuse
    ///   live traffic because coordination is down).
    pub async fn fetch_envelope(
        &self,
        destination_id: &str,
        refresh: bool,
    ) -> Result<FivetranStateEnvelope, FivetranError> {
        let cache_key = self.cache_key(destination_id);
        let backend = self.state_cache.backend();

        // (L0) In-process memo — only consulted on non-refresh.
        if !refresh
            && let Some(cached) = self
                .envelope_cache
                .lock()
                .await
                .get(destination_id)
                .cloned()
        {
            return Ok(cached);
        }

        // (L2) Cross-process state cache — only consulted on
        // non-refresh AND when the configured backend isn't NoCache.
        if !refresh && backend != "none" {
            match self.state_cache.read(&cache_key).await {
                Ok(Some(env)) => {
                    cache_obs::emit_cache_hit(backend, &cache_key, &env);
                    self.envelope_cache
                        .lock()
                        .await
                        .insert(destination_id.to_string(), env.clone());
                    return Ok(env);
                }
                Ok(None) => cache_obs::emit_cache_miss(backend, &cache_key, "no-entry"),
                Err(e) => warn!(
                    backend,
                    key = cache_key.as_str(),
                    error = %e,
                    "fivetran state cache read failed; falling through to API"
                ),
            }
        } else if refresh && backend != "none" {
            cache_obs::emit_cache_miss(backend, &cache_key, "refresh-forced");
        }

        // (L1) Stampede protection — only for non-refresh callers.
        // `refresh = true` means "I explicitly want fresh data";
        // waiting on another leader's TTL window would violate that
        // intent.
        if !refresh && self.stampede.backend() != "none" {
            match self
                .stampede
                .acquire(&cache_key, self.stampede_lock_ttl)
                .await
            {
                Ok(Some(_guard)) => {
                    // Leader path: fall through to the HTTP fetch
                    // below; the guard is held across the HTTP
                    // call + cache write-back so followers can't
                    // observe the half-state.
                    record_span_event(
                        &PipelineEvent::new("fivetran.stampede_acquired")
                            .with_metadata("key", cache_key.clone())
                            .with_metadata("ttl_seconds", self.stampede_lock_ttl.as_secs())
                            .with_metadata("backend", self.stampede.backend()),
                    );
                    let env = self
                        .fetch_envelope_from_api_with_circuit(destination_id, &cache_key, backend)
                        .await?;
                    // Hold _guard alive until here; releases on drop
                    // at function-end via the LockGuard contract.
                    drop(_guard);
                    return Ok(env);
                }
                Ok(None) => {
                    // Follower path: poll the cache for the leader's
                    // write.
                    let started_at = std::time::Instant::now();
                    match self
                        .poll_cache_until_present(&cache_key, self.stampede_poll_timeout)
                        .await
                    {
                        Some(env) => {
                            record_span_event(
                                &PipelineEvent::new("fivetran.stampede_polled")
                                    .with_metadata("key", cache_key.clone())
                                    .with_metadata(
                                        "waited_ms",
                                        started_at.elapsed().as_millis().min(u128::from(u64::MAX))
                                            as u64,
                                    )
                                    .with_metadata("outcome", "cache_hit"),
                            );
                            self.envelope_cache
                                .lock()
                                .await
                                .insert(destination_id.to_string(), env.clone());
                            return Ok(env);
                        }
                        None => {
                            record_span_event(
                                &PipelineEvent::new("fivetran.stampede_polled")
                                    .with_metadata("key", cache_key.clone())
                                    .with_metadata(
                                        "waited_ms",
                                        started_at.elapsed().as_millis().min(u128::from(u64::MAX))
                                            as u64,
                                    )
                                    .with_metadata("outcome", "timeout_direct_fetch"),
                            );
                            // Follower timed out — fall through to
                            // a direct HTTP fetch (no protection).
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        backend = self.stampede.backend(),
                        "stampede lock unavailable; falling through to direct fetch"
                    );
                    record_span_event(
                        &PipelineEvent::new("fivetran.stampede_unavailable")
                            .with_metadata("backend", self.stampede.backend())
                            .with_error(e.to_string()),
                    );
                }
            }
        }

        // Direct fetch path: either the stampede backend was NoLock,
        // the leader returned, the follower timed out, or the lock
        // backend failed. All routes converge here and through the
        // circuit-protected API path.
        self.fetch_envelope_from_api_with_circuit(destination_id, &cache_key, backend)
            .await
    }

    /// HTTP fetch wrapped in the circuit-breaker check + record
    /// transitions. Used both by the stampede leader path and the
    /// non-stampede / fall-through path.
    async fn fetch_envelope_from_api_with_circuit(
        &self,
        destination_id: &str,
        cache_key: &str,
        cache_backend: &str,
    ) -> Result<FivetranStateEnvelope, FivetranError> {
        // (L4) Circuit-breaker check.
        let circuit_state = match self.circuit.state(&self.account_id_hash).await {
            Ok(s) => s,
            Err(e) => {
                // Fail-open — never refuse traffic because the
                // coordination store is down.
                warn!(
                    error = %e,
                    backend = self.circuit.backend(),
                    "circuit breaker state read failed; treating as Closed"
                );
                CircuitState::Closed
            }
        };
        if circuit_state == CircuitState::Open {
            record_span_event(
                &PipelineEvent::new("fivetran.circuit_state_change")
                    .with_metadata("account_id", self.account_id_hash.clone())
                    .with_metadata("from_state", "open")
                    .with_metadata("to_state", "open")
                    .with_metadata("reason", "request_short_circuited")
                    .with_metadata("backend", self.circuit.backend()),
            );
            return Err(FivetranError::CircuitOpen);
        }

        let api_outcome = self
            .fetch_envelope_from_api(destination_id, cache_key, cache_backend)
            .await;

        // Record success / failure to drive future transitions.
        match &api_outcome {
            Ok(_) => {
                let prev_state = circuit_state;
                if let Err(e) = self.circuit.record_success(&self.account_id_hash).await {
                    warn!(error = %e, "circuit record_success failed; fail-open");
                } else if prev_state == CircuitState::HalfOpen {
                    record_span_event(
                        &PipelineEvent::new("fivetran.circuit_state_change")
                            .with_metadata("account_id", self.account_id_hash.clone())
                            .with_metadata("from_state", "half_open")
                            .with_metadata("to_state", "closed")
                            .with_metadata("reason", "probe_success")
                            .with_metadata("backend", self.circuit.backend()),
                    );
                }
            }
            Err(e) if is_remote_failure(e) => {
                if let Err(rec_err) = self
                    .circuit
                    .record_failure(&self.account_id_hash, FailureKind::Remote)
                    .await
                {
                    warn!(error = %rec_err, "circuit record_failure failed; fail-open");
                } else {
                    // Re-read state so the event reflects the actual
                    // transition (if any). The breaker may have
                    // tripped from Closed→Open or HalfOpen→Open, or
                    // it may still be Closed if the consecutive
                    // failure count is below the threshold.
                    let new_state = self
                        .circuit
                        .state(&self.account_id_hash)
                        .await
                        .unwrap_or(circuit_state);
                    if new_state != circuit_state {
                        record_span_event(
                            &PipelineEvent::new("fivetran.circuit_state_change")
                                .with_metadata("account_id", self.account_id_hash.clone())
                                .with_metadata("from_state", circuit_state.as_str())
                                .with_metadata("to_state", new_state.as_str())
                                .with_metadata("reason", "remote_failure")
                                .with_metadata("backend", self.circuit.backend()),
                        );
                    }
                }
            }
            Err(_) => {
                // Local error — record as Local so backends with
                // telemetry can count them without affecting the
                // state machine.
                let _ = self
                    .circuit
                    .record_failure(&self.account_id_hash, FailureKind::Local)
                    .await;
            }
        }

        api_outcome
    }

    /// Internal: do the actual HTTP fetch + cache write-back. Split
    /// out from [`Self::fetch_envelope`] so the stampede leader path
    /// and the fall-through direct path share one implementation.
    async fn fetch_envelope_from_api(
        &self,
        destination_id: &str,
        cache_key: &str,
        cache_backend: &str,
    ) -> Result<FivetranStateEnvelope, FivetranError> {
        let destination = self.get_destination(destination_id).await?;
        let connectors = ft_connector::list_connectors(self, destination_id).await?;

        // Fan-out schema fetches in parallel — matches the existing
        // discover adapter's concurrency. 10 is the same constant.
        let connector_count = connectors.len();
        let schema_results: Vec<(String, Result<ft_schema::SchemaConfig, FivetranError>)> =
            stream::iter(connectors.iter().cloned())
                .map(|conn| async move {
                    let result = ft_schema::get_schema_config(self, &conn.id).await;
                    (conn.id, result)
                })
                .buffer_unordered(10)
                .collect()
                .await;
        // Tolerate per-connector 404s from `connectors/{id}/schemas` —
        // a connector in `incomplete`/`broken`/paused-pre-schema state
        // returns 404 here as normal upstream signal, not a failure.
        // Skip the offending connector (it still appears in the
        // `connectors` summary list with its status fields), WARN with
        // its id, and abort only when every connector 404s (which
        // would indicate a credential or quota problem upstream).
        let total_schema_fetches = schema_results.len();
        let mut schemas: BTreeMap<String, FivetranSchemaConfig> = BTreeMap::new();
        let mut skipped_count: usize = 0;
        for (conn_id, result) in schema_results {
            match result {
                Ok(cfg) => {
                    schemas.insert(conn_id, project_schema_config(cfg));
                }
                Err(e) if is_missing_schema_config(&e) => {
                    skipped_count += 1;
                    tracing::warn!(
                        target: "rocky_fivetran::emit",
                        connector_id = %conn_id,
                        reason = %e,
                        "emit-fivetran-state: skipping connector (schema_config not available); continuing"
                    );
                }
                Err(e) => return Err(e),
            }
        }
        if total_schema_fetches > 0 && schemas.is_empty() {
            return Err(FivetranError::NoHealthyConnectors {
                total: total_schema_fetches,
            });
        }
        tracing::info!(
            target: "rocky_fivetran::emit",
            written = schemas.len(),
            skipped = skipped_count,
            total = total_schema_fetches,
            "emit-fivetran-state: envelope built ({} connector schemas, {} skipped due to missing schema_config)",
            schemas.len(),
            skipped_count
        );

        let summaries: Vec<FivetranConnectorSummary> = connectors
            .into_iter()
            .map(project_connector_summary)
            .collect();

        let envelope =
            FivetranStateEnvelope::from_parts(Utc::now(), destination, summaries, schemas);

        tracing::debug!(
            destination_id,
            connectors = connector_count,
            "fivetran envelope fetched"
        );

        // Write-back to L2 cache — best-effort.
        if cache_backend != "none" {
            match self.state_cache.write(cache_key, &envelope).await {
                Ok(WriteOutcome::Written) => {
                    cache_obs::emit_cache_write(cache_backend, cache_key, &envelope)
                }
                Ok(WriteOutcome::SkippedNoChange) => {
                    cache_obs::emit_cache_write_skipped(cache_backend, cache_key, "hash-match")
                }
                Err(e) => cache_obs::emit_cache_write_failed(cache_backend, cache_key, &e),
            }
        }

        // Write-back to L0 in-process memo.
        self.envelope_cache
            .lock()
            .await
            .insert(destination_id.to_string(), envelope.clone());
        Ok(envelope)
    }

    /// Follower-side cache poll. Returns the envelope when the
    /// configured [`FivetranStateCache`] backend has the key, or
    /// `None` on timeout. Exponential backoff: 100ms → 200ms → ... →
    /// 2s, total budget = `timeout`.
    async fn poll_cache_until_present(
        &self,
        cache_key: &str,
        timeout: Duration,
    ) -> Option<FivetranStateEnvelope> {
        if self.state_cache.backend() == "none" {
            // No cache → nothing to poll.
            return None;
        }
        let deadline = std::time::Instant::now() + timeout;
        let mut attempt: u32 = 0;
        loop {
            if std::time::Instant::now() >= deadline {
                return None;
            }
            match self.state_cache.read(cache_key).await {
                Ok(Some(env)) => return Some(env),
                Ok(None) => {} // continue polling
                Err(e) => {
                    warn!(error = %e, "cache poll read failed; falling through");
                    return None;
                }
            }
            let backoff = stampede::poll_backoff(attempt);
            attempt = attempt.saturating_add(1);
            // Don't oversleep past the deadline.
            let now = std::time::Instant::now();
            let remaining = deadline.saturating_duration_since(now);
            let sleep_for = backoff.min(remaining);
            if sleep_for.is_zero() {
                return None;
            }
            tokio::time::sleep(sleep_for).await;
        }
    }
}

/// Classify a [`FivetranError`] as a remote (Fivetran-side) failure.
/// Only remote failures trip the circuit breaker; local errors
/// (parsing, serialization) do not.
///
/// 429 errors are intentionally *not* counted as remote failures —
/// rate limiting is a soft signal that Fivetran is responsive but
/// busy, and `RateLimited` only surfaces from [`Self::get`] when the
/// retry budget is exhausted. The shared [`RatelimitBudget`] (L3) is
/// the right tool for 429 propagation; tripping the breaker on every
/// 429-storm would refuse traffic for an unrelated reason.
fn is_remote_failure(err: &FivetranError) -> bool {
    match err {
        FivetranError::Http(e) => {
            // Connect refused, TLS error, timeout — all remote
            // (network) failures that the breaker should count.
            e.is_connect() || e.is_timeout() || e.is_request()
        }
        FivetranError::Api { code, .. } => {
            // 5xx status codes — surfaced when retries are
            // exhausted on a server error.
            code.starts_with('5')
        }
        FivetranError::RetryBudgetExhausted { .. } => true,
        FivetranError::RateLimited => false, // see doc comment above
        FivetranError::UnexpectedResponse(_) => false, // local decode error
        FivetranError::CircuitOpen => false,
        // Per-account upstream state, not a transport failure — don't
        // trip the breaker. A retry on the next discover cycle won't
        // help unless the operator fixes the connectors upstream.
        FivetranError::NoHealthyConnectors { .. } => false,
    }
}

/// Recognise the missing-schema-config 404 shape from
/// `GET /v1/connectors/{id}/schemas`. Fivetran returns 404 with a
/// `NotFound_SchemaConfig` payload for connectors in `incomplete`,
/// `broken`, or paused-pre-schema states — normal upstream state,
/// not a failure condition. The HTTP non-success path in
/// [`FivetranClient::get`] sets `code` to the status display
/// (e.g. `"404 Not Found"`) so a leading-digit match captures the
/// whole 404 family from this endpoint.
fn is_missing_schema_config(err: &FivetranError) -> bool {
    match err {
        FivetranError::Api { code, .. } => code.starts_with("404"),
        _ => false,
    }
}

/// Project a wire-decoded [`ft_connector::Connector`] into the envelope
/// shape. Falls back to `schema` for the `name` field when the upstream
/// payload omits it — `connectors/{id}` and `groups/{id}/connectors`
/// disagree on whether `connector_name` is returned, so we accept both.
fn project_connector_summary(conn: ft_connector::Connector) -> FivetranConnectorSummary {
    let name = conn.name.clone().unwrap_or_else(|| conn.schema.clone());
    FivetranConnectorSummary {
        id: conn.id,
        name,
        schema: conn.schema,
        service: conn.service,
        status: FivetranConnectorStatus {
            setup_state: conn.status.setup_state,
            sync_state: conn.status.sync_state,
        },
        paused: conn.paused,
        succeeded_at: conn.succeeded_at,
        failed_at: conn.failed_at,
        group_id: Some(conn.group_id),
    }
}

/// Project the wire-decoded schema config into the envelope shape,
/// promoting the inner [`HashMap`](std::collections::HashMap) layers to
/// [`BTreeMap`] for hash stability.
fn project_schema_config(cfg: ft_schema::SchemaConfig) -> FivetranSchemaConfig {
    let mut schemas: BTreeMap<String, FivetranSchemaEntry> = BTreeMap::new();
    for (schema_key, entry) in cfg.schemas {
        let mut tables: BTreeMap<String, FivetranTableConfig> = BTreeMap::new();
        for (table_key, table) in entry.tables {
            let mut columns: BTreeMap<String, FivetranColumnConfig> = BTreeMap::new();
            for (col_key, col) in table.columns {
                columns.insert(
                    col_key,
                    FivetranColumnConfig {
                        enabled: col.enabled,
                        hashed: col.hashed,
                    },
                );
            }
            tables.insert(
                table_key,
                FivetranTableConfig {
                    enabled: table.enabled,
                    name_in_destination: table.name_in_destination,
                    sync_mode: table.sync_mode,
                    columns,
                },
            );
        }
        schemas.insert(
            schema_key,
            FivetranSchemaEntry {
                enabled: entry.enabled,
                name_in_destination: entry.name_in_destination,
                tables,
            },
        );
    }
    FivetranSchemaConfig { schemas }
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
