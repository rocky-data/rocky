//! Per-account circuit breaker for the Fivetran adapter (Layer 3).
//!
//! Trips after N consecutive remote failures so a Fivetran outage
//! short-circuits subsequent HTTP attempts with a typed
//! [`FivetranError::CircuitOpen`](crate::client::FivetranError::CircuitOpen)
//! error instead of burning the org's rate-limit budget on requests
//! that are guaranteed to fail. State is coordinated across processes
//! via Valkey so a 10-pod fleet trips one breaker for the whole org.
//!
//! ## Trait
//!
//! Three operations:
//!
//! - [`FivetranCircuitBreaker::state`] — read the current state
//!   (`Closed`/`HalfOpen`/`Open`). The wiring in `client.rs` calls
//!   this before every HTTP attempt and short-circuits on `Open`.
//! - [`FivetranCircuitBreaker::record_success`] — called after a
//!   successful HTTP response. Resets the failure counter; in
//!   `HalfOpen` it transitions back to `Closed`.
//! - [`FivetranCircuitBreaker::record_failure`] — called after a
//!   remote failure (5xx, exhausted retries, transport error).
//!   Increments the counter; on hitting `failure_threshold` (or on
//!   *any* failure in `HalfOpen`) trips the breaker into `Open`.
//!
//! ## Failure classification
//!
//! Only *remote* failures count toward tripping the breaker — local
//! errors (JSON parse, serialization) don't indicate Fivetran is
//! struggling. The [`FailureKind`] enum tags the call so backends
//! can record telemetry about *why* a breaker tripped without
//! depending on the upstream error type.
//!
//! ## Backends
//!
//! - [`AlwaysClosed`] — no-op; `state` always returns `Closed`,
//!   record_* are no-ops. Default when `[adapter.fivetran.circuit_breaker]`
//!   is absent or `backend = "none"`.
//! - `valkey::ValkeyCircuit` — shared state machine in Valkey,
//!   behind the `valkey` Cargo feature.
//!
//! ## Concurrency / atomicity
//!
//! The Valkey backend persists state via a read-modify-write
//! sequence rather than a server-side Lua transaction. Two
//! concurrent `record_failure` calls can read the same prior
//! counter value, both bump it, and both write the same `n+1`,
//! losing one increment. The practical effect is that the breaker
//! trips at *approximately* `failure_threshold` consecutive
//! failures — under sustained contention the actual trip point
//! may be 1-2 above the configured threshold. The breaker still
//! converges to `Open` quickly during a real outage; the
//! looseness only matters at the threshold boundary, where
//! tripping one request later is acceptable given the fail-open
//! posture upstream.
//!
//! If a deployment needs exact threshold semantics, route through
//! [`AlwaysClosed`] (no breaker) and rely on the cross-pod
//! rate-limit budget + retry budget; both are stricter coordination
//! layers.
//!
//! ## Fail-open
//!
//! Every operation on the breaker is permitted to fail in the
//! coordination layer. The client wiring traps errors from `state`
//! by treating them as `Closed` — never refuse live traffic because
//! the breaker store is unreachable.

use std::time::Duration;

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Errors surfaced by [`FivetranCircuitBreaker`] backends.
#[derive(Debug, Error)]
pub enum CircuitError {
    /// Valkey / Redis transport failure.
    #[error("circuit valkey: {0}")]
    Valkey(String),
    /// Misconfigured backend.
    #[error("circuit config: {0}")]
    Config(String),
}

/// Discriminator for what kind of failure tripped the breaker. Only
/// `Remote` actually counts toward `failure_threshold`; the other
/// variants are recorded for telemetry but don't move the counter.
///
/// The split matters because the breaker exists to protect the
/// Fivetran org's rate budget when *Fivetran* is degraded — a local
/// JSON deserialization bug isn't useful evidence either way.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FailureKind {
    /// 5xx, 429-storm, connect-refused, request timeout, retry-budget
    /// exhausted. Counts toward `failure_threshold`.
    Remote,
    /// Local error (JSON parse, serialization, missing field). Does
    /// NOT count toward the threshold.
    Local,
}

/// The three states a circuit breaker can be in.
///
/// Lifecycle:
///
/// ```text
///                +-------------+
///       success  |   Closed    |  N failures within window
///        +-------+             +------+
///        |       +-------------+      |
///        v                            v
///   +-------------+              +-------------+
///   |  HalfOpen   |<--cooldown---|     Open    |
///   +-------------+   elapses    +-------------+
///        |                            ^
///        | failure                    |
///        +----------------------------+
/// ```
///
/// `HalfOpen` permits a single probe through; success closes the
/// breaker, failure re-opens with exponentially extended cooldown.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CircuitState {
    /// Normal operation; requests pass through unconditionally.
    Closed,
    /// One probe permitted; outcome determines next transition.
    HalfOpen,
    /// All requests short-circuit with `CircuitOpen` until cooldown
    /// elapses.
    Open,
}

impl CircuitState {
    /// Stable wire-form string used in OTLP span attributes and log
    /// messages. Pinned so dashboards filter on a known tag set.
    pub fn as_str(self) -> &'static str {
        match self {
            CircuitState::Closed => "closed",
            CircuitState::HalfOpen => "half_open",
            CircuitState::Open => "open",
        }
    }
}

/// Per-account circuit breaker.
#[async_trait]
pub trait FivetranCircuitBreaker: Send + Sync + std::fmt::Debug {
    /// Current breaker state for `account_hash`. Fail-open: backend
    /// errors are not propagated — the caller treats `Err(_)` the
    /// same as `Ok(Closed)`.
    async fn state(&self, account_hash: &str) -> Result<CircuitState, CircuitError>;

    /// Record a successful HTTP outcome. In `HalfOpen` this closes
    /// the breaker; in `Closed` it resets the consecutive-failure
    /// counter.
    async fn record_success(&self, account_hash: &str) -> Result<(), CircuitError>;

    /// Record a failed HTTP outcome. `Local` failures are recorded
    /// for telemetry but don't move the state machine.
    async fn record_failure(
        &self,
        account_hash: &str,
        kind: FailureKind,
    ) -> Result<(), CircuitError>;

    /// Stable backend tag for OTLP span attributes.
    fn backend(&self) -> &'static str;

    /// Initial cooldown (in whole seconds) the breaker uses when
    /// tripping from `Closed → Open`. Surfaced on
    /// [`FivetranError::CircuitOpen`](crate::client::FivetranError::CircuitOpen)
    /// so callers downstream of the adapter can derive a `retry_after`
    /// hint without re-parsing config. Implementations back this with
    /// [`CircuitConfig::cooldown`]; the default returns 300 (matching
    /// [`CircuitConfig::default`]) so out-of-tree backends don't break.
    fn cooldown_seconds(&self) -> u64 {
        CircuitConfig::default().cooldown.as_secs()
    }
}

/// No-op breaker — always reports `Closed`, ignores record_*. Default
/// when no breaker backend is configured.
#[derive(Debug, Default, Clone, Copy)]
pub struct AlwaysClosed;

#[async_trait]
impl FivetranCircuitBreaker for AlwaysClosed {
    async fn state(&self, _account_hash: &str) -> Result<CircuitState, CircuitError> {
        Ok(CircuitState::Closed)
    }
    async fn record_success(&self, _account_hash: &str) -> Result<(), CircuitError> {
        Ok(())
    }
    async fn record_failure(
        &self,
        _account_hash: &str,
        _kind: FailureKind,
    ) -> Result<(), CircuitError> {
        Ok(())
    }
    fn backend(&self) -> &'static str {
        "none"
    }
}

/// Tunables for a circuit breaker. Defaults match the FR plan.
#[derive(Debug, Clone, Copy)]
pub struct CircuitConfig {
    /// Consecutive failures required to trip `Closed → Open`.
    pub failure_threshold: u32,
    /// Failure-counting window. Failures older than this are dropped.
    pub window: Duration,
    /// Initial cooldown before `Open → HalfOpen`.
    pub cooldown: Duration,
    /// Upper bound on the exponentially-extended cooldown after
    /// repeated `HalfOpen` failures.
    pub cooldown_max: Duration,
}

impl Default for CircuitConfig {
    fn default() -> Self {
        CircuitConfig {
            failure_threshold: 5,
            window: Duration::from_secs(60),
            cooldown: Duration::from_secs(300),
            cooldown_max: Duration::from_secs(3600),
        }
    }
}

impl CircuitConfig {
    /// Build a config from the user-facing TOML knobs, applying the
    /// defaults from [`Self::default`] for any unset field.
    pub fn from_options(
        failure_threshold: Option<u32>,
        window_seconds: Option<u64>,
        cooldown_seconds: Option<u64>,
        cooldown_max_seconds: Option<u64>,
    ) -> Self {
        let d = CircuitConfig::default();
        CircuitConfig {
            failure_threshold: failure_threshold.unwrap_or(d.failure_threshold).max(1),
            window: window_seconds.map(Duration::from_secs).unwrap_or(d.window),
            cooldown: cooldown_seconds
                .map(Duration::from_secs)
                .unwrap_or(d.cooldown),
            cooldown_max: cooldown_max_seconds
                .map(Duration::from_secs)
                .unwrap_or(d.cooldown_max),
        }
    }
}

/// Construct the circuit-breaker backend described by `config`.
/// Returns the [`Arc<dyn FivetranCircuitBreaker>`]; defaults to
/// [`AlwaysClosed`] when `config` is `None`.
///
/// # Errors
///
/// - [`CircuitError::Config`] — required field missing.
/// - [`CircuitError::Valkey`] — the Redis client refused the URL.
pub fn build_circuit_breaker(
    config: Option<&rocky_core::config::FivetranCircuitBreakerConfig>,
) -> Result<std::sync::Arc<dyn FivetranCircuitBreaker>, CircuitError> {
    use rocky_core::config::FivetranCircuitBreakerBackend;
    let Some(cfg) = config else {
        return Ok(std::sync::Arc::new(AlwaysClosed));
    };
    // The tunables are only consumed by the Valkey branch; suppress
    // the unused-variable lint when the `valkey` feature is off.
    let _resolved = CircuitConfig::from_options(
        cfg.failure_threshold,
        cfg.window_seconds,
        cfg.cooldown_seconds,
        cfg.cooldown_max_seconds,
    );
    #[cfg(feature = "valkey")]
    let resolved = _resolved;
    match cfg.backend {
        FivetranCircuitBreakerBackend::None => Ok(std::sync::Arc::new(AlwaysClosed)),
        #[cfg(feature = "valkey")]
        FivetranCircuitBreakerBackend::Valkey => {
            let url = cfg.valkey_url.as_ref().ok_or_else(|| {
                CircuitError::Config(
                    "backend = \"valkey\" requires `valkey_url` in [adapter.<name>.circuit_breaker]"
                        .into(),
                )
            })?;
            let cb = crate::circuit_breaker::valkey::ValkeyCircuit::from_url(url, resolved)?;
            Ok(std::sync::Arc::new(cb))
        }
        #[cfg(not(feature = "valkey"))]
        FivetranCircuitBreakerBackend::Valkey => Err(CircuitError::Config(
            "backend = \"valkey\" requires building rocky-fivetran with the `valkey` feature"
                .into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn always_closed_never_trips() {
        let cb = AlwaysClosed;
        assert_eq!(cb.state("acct").await.unwrap(), CircuitState::Closed);
        cb.record_failure("acct", FailureKind::Remote)
            .await
            .unwrap();
        cb.record_failure("acct", FailureKind::Remote)
            .await
            .unwrap();
        cb.record_failure("acct", FailureKind::Remote)
            .await
            .unwrap();
        assert_eq!(cb.state("acct").await.unwrap(), CircuitState::Closed);
    }

    #[test]
    fn circuit_state_as_str_pinned() {
        assert_eq!(CircuitState::Closed.as_str(), "closed");
        assert_eq!(CircuitState::HalfOpen.as_str(), "half_open");
        assert_eq!(CircuitState::Open.as_str(), "open");
    }

    #[test]
    fn config_defaults_match_plan() {
        let d = CircuitConfig::default();
        assert_eq!(d.failure_threshold, 5);
        assert_eq!(d.window, Duration::from_secs(60));
        assert_eq!(d.cooldown, Duration::from_secs(300));
        assert_eq!(d.cooldown_max, Duration::from_secs(3600));
    }

    #[test]
    fn config_from_options_overrides_only_set_fields() {
        let c = CircuitConfig::from_options(Some(10), None, Some(120), None);
        assert_eq!(c.failure_threshold, 10);
        assert_eq!(c.window, Duration::from_secs(60));
        assert_eq!(c.cooldown, Duration::from_secs(120));
        assert_eq!(c.cooldown_max, Duration::from_secs(3600));
    }
}

/// In-memory breaker used for unit tests. Thread-safe single-process
/// reference implementation of the state machine the Valkey backend
/// runs server-side. Exposed at the crate boundary because the
/// `client.rs` layered-fetch tests need it.
#[cfg(any(test, feature = "test-support"))]
pub mod test_support {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    use async_trait::async_trait;

    use super::{CircuitConfig, CircuitError, CircuitState, FailureKind, FivetranCircuitBreaker};

    #[derive(Debug)]
    struct AccountState {
        state: CircuitState,
        consecutive_failures: u32,
        last_failure: Option<Instant>,
        opened_at: Option<Instant>,
        current_cooldown: Duration,
    }

    impl AccountState {
        fn new(initial_cooldown: Duration) -> Self {
            AccountState {
                state: CircuitState::Closed,
                consecutive_failures: 0,
                last_failure: None,
                opened_at: None,
                current_cooldown: initial_cooldown,
            }
        }
    }

    /// In-memory circuit breaker for tests.
    #[derive(Debug)]
    pub struct InMemoryCircuit {
        config: CircuitConfig,
        accounts: Mutex<HashMap<String, AccountState>>,
    }

    impl InMemoryCircuit {
        pub fn new(config: CircuitConfig) -> Self {
            InMemoryCircuit {
                config,
                accounts: Mutex::new(HashMap::new()),
            }
        }

        fn now(&self) -> Instant {
            Instant::now()
        }
    }

    #[async_trait]
    impl FivetranCircuitBreaker for InMemoryCircuit {
        async fn state(&self, account_hash: &str) -> Result<CircuitState, CircuitError> {
            let now = self.now();
            let mut accounts = self.accounts.lock().unwrap();
            let acct = accounts
                .entry(account_hash.to_string())
                .or_insert_with(|| AccountState::new(self.config.cooldown));
            // Transition Open → HalfOpen on elapsed cooldown.
            if acct.state == CircuitState::Open
                && let Some(opened) = acct.opened_at
                && now.duration_since(opened) >= acct.current_cooldown
            {
                acct.state = CircuitState::HalfOpen;
            }
            Ok(acct.state)
        }

        async fn record_success(&self, account_hash: &str) -> Result<(), CircuitError> {
            let mut accounts = self.accounts.lock().unwrap();
            let acct = accounts
                .entry(account_hash.to_string())
                .or_insert_with(|| AccountState::new(self.config.cooldown));
            acct.state = CircuitState::Closed;
            acct.consecutive_failures = 0;
            acct.last_failure = None;
            acct.opened_at = None;
            acct.current_cooldown = self.config.cooldown;
            Ok(())
        }

        async fn record_failure(
            &self,
            account_hash: &str,
            kind: FailureKind,
        ) -> Result<(), CircuitError> {
            // Local failures don't move the state machine.
            if kind == FailureKind::Local {
                return Ok(());
            }
            let now = self.now();
            let mut accounts = self.accounts.lock().unwrap();
            let acct = accounts
                .entry(account_hash.to_string())
                .or_insert_with(|| AccountState::new(self.config.cooldown));
            // HalfOpen failure → Open with extended cooldown.
            if acct.state == CircuitState::HalfOpen {
                acct.state = CircuitState::Open;
                acct.opened_at = Some(now);
                let extended = acct.current_cooldown.saturating_mul(2);
                acct.current_cooldown = extended.min(self.config.cooldown_max);
                return Ok(());
            }
            // Closed: bump counter; if a window has elapsed since the
            // last failure, reset to 1.
            if let Some(prev) = acct.last_failure
                && now.duration_since(prev) > self.config.window
            {
                acct.consecutive_failures = 0;
            }
            acct.consecutive_failures = acct.consecutive_failures.saturating_add(1);
            acct.last_failure = Some(now);
            if acct.consecutive_failures >= self.config.failure_threshold {
                acct.state = CircuitState::Open;
                acct.opened_at = Some(now);
                acct.current_cooldown = self.config.cooldown;
            }
            Ok(())
        }

        fn backend(&self) -> &'static str {
            "in_memory"
        }

        fn cooldown_seconds(&self) -> u64 {
            self.config.cooldown.as_secs()
        }
    }
}

#[cfg(feature = "valkey")]
pub mod valkey {
    //! Valkey-backed [`FivetranCircuitBreaker`] implementation.
    //!
    //! State for each account lives at
    //! `<KEY_PREFIX><account_hash>` as a JSON blob holding the state
    //! machine fields. Persistence is a read-modify-write `GET` +
    //! `SET EX` rather than a server-side Lua transaction — see the
    //! parent module's "Concurrency / atomicity" section for the
    //! looseness this implies at the threshold boundary.

    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use async_trait::async_trait;
    use serde::{Deserialize, Serialize};
    use tokio::sync::OnceCell;
    use tracing::debug;

    use super::{CircuitConfig, CircuitError, CircuitState, FailureKind, FivetranCircuitBreaker};

    /// Redis key prefix for every Fivetran circuit breaker entry.
    const KEY_PREFIX: &str = "fivetran-circuit:";

    /// Wire shape of the per-account breaker state. Stored as JSON
    /// in Valkey under [`KEY_PREFIX`].
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct WireState {
        state: String, // "closed" | "half_open" | "open"
        consecutive_failures: u32,
        last_failure_epoch_ms: u64, // 0 = none
        opened_at_epoch_ms: u64,    // 0 = none
        current_cooldown_secs: u64,
    }

    impl WireState {
        fn fresh(cooldown: Duration) -> Self {
            WireState {
                state: "closed".into(),
                consecutive_failures: 0,
                last_failure_epoch_ms: 0,
                opened_at_epoch_ms: 0,
                current_cooldown_secs: cooldown.as_secs(),
            }
        }

        fn parsed_state(&self) -> CircuitState {
            match self.state.as_str() {
                "open" => CircuitState::Open,
                "half_open" => CircuitState::HalfOpen,
                _ => CircuitState::Closed,
            }
        }
    }

    pub struct ValkeyCircuit {
        client: redis::Client,
        conn: OnceCell<redis::aio::ConnectionManager>,
        config: CircuitConfig,
    }

    impl std::fmt::Debug for ValkeyCircuit {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("ValkeyCircuit")
                .field("config", &self.config)
                .field("connected", &self.conn.initialized())
                .finish_non_exhaustive()
        }
    }

    impl ValkeyCircuit {
        /// Open a Valkey / Redis client for `url`. Sync + non-blocking;
        /// connect deferred to first call.
        pub fn from_url(url: &str, config: CircuitConfig) -> Result<Self, CircuitError> {
            let client =
                redis::Client::open(url).map_err(|e| CircuitError::Valkey(e.to_string()))?;
            Ok(ValkeyCircuit {
                client,
                conn: OnceCell::new(),
                config,
            })
        }

        async fn get_conn(&self) -> Result<redis::aio::ConnectionManager, CircuitError> {
            let conn = self
                .conn
                .get_or_try_init(|| async {
                    redis::aio::ConnectionManager::new(self.client.clone())
                        .await
                        .map_err(|e| CircuitError::Valkey(e.to_string()))
                })
                .await?;
            Ok(conn.clone())
        }

        fn prefixed(account_hash: &str) -> String {
            format!("{KEY_PREFIX}{account_hash}")
        }

        async fn load_or_init(
            &self,
            mut conn: redis::aio::ConnectionManager,
            key: &str,
        ) -> Result<WireState, CircuitError> {
            use redis::AsyncCommands;
            let raw: Option<String> = conn
                .get(key)
                .await
                .map_err(|e| CircuitError::Valkey(e.to_string()))?;
            match raw {
                Some(s) => serde_json::from_str(&s)
                    .map_err(|e| CircuitError::Valkey(format!("decode breaker state failed: {e}"))),
                None => Ok(WireState::fresh(self.config.cooldown)),
            }
        }

        async fn persist(
            &self,
            mut conn: redis::aio::ConnectionManager,
            key: &str,
            state: &WireState,
        ) -> Result<(), CircuitError> {
            use redis::AsyncCommands;
            let json = serde_json::to_string(state)
                .map_err(|e| CircuitError::Valkey(format!("encode breaker state failed: {e}")))?;
            // TTL = current_cooldown_secs * 2 (plus the window), so an
            // abandoned breaker entry disappears within a bounded time.
            let ttl = state
                .current_cooldown_secs
                .saturating_mul(2)
                .max(self.config.window.as_secs())
                .max(60);
            conn.set_ex::<&str, &str, ()>(key, &json, ttl)
                .await
                .map_err(|e| CircuitError::Valkey(e.to_string()))?;
            Ok(())
        }

        fn now_ms() -> u64 {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis().min(u64::MAX as u128) as u64)
                .unwrap_or(0)
        }
    }

    #[async_trait]
    impl FivetranCircuitBreaker for ValkeyCircuit {
        async fn state(&self, account_hash: &str) -> Result<CircuitState, CircuitError> {
            let conn = self.get_conn().await?;
            let key = Self::prefixed(account_hash);
            let mut wire = self.load_or_init(conn.clone(), &key).await?;
            // Maybe transition Open → HalfOpen on elapsed cooldown.
            if wire.parsed_state() == CircuitState::Open && wire.opened_at_epoch_ms > 0 {
                let now_ms = Self::now_ms();
                let elapsed = now_ms.saturating_sub(wire.opened_at_epoch_ms);
                if elapsed >= wire.current_cooldown_secs.saturating_mul(1_000) {
                    wire.state = "half_open".into();
                    self.persist(conn, &key, &wire).await?;
                    return Ok(CircuitState::HalfOpen);
                }
            }
            Ok(wire.parsed_state())
        }

        async fn record_success(&self, account_hash: &str) -> Result<(), CircuitError> {
            let conn = self.get_conn().await?;
            let key = Self::prefixed(account_hash);
            let mut wire = self.load_or_init(conn.clone(), &key).await?;
            wire.state = "closed".into();
            wire.consecutive_failures = 0;
            wire.last_failure_epoch_ms = 0;
            wire.opened_at_epoch_ms = 0;
            wire.current_cooldown_secs = self.config.cooldown.as_secs();
            self.persist(conn, &key, &wire).await?;
            debug!(account_hash, "circuit closed via record_success");
            Ok(())
        }

        async fn record_failure(
            &self,
            account_hash: &str,
            kind: FailureKind,
        ) -> Result<(), CircuitError> {
            if kind == FailureKind::Local {
                return Ok(());
            }
            let conn = self.get_conn().await?;
            let key = Self::prefixed(account_hash);
            let mut wire = self.load_or_init(conn.clone(), &key).await?;
            let now_ms = Self::now_ms();
            let current_state = wire.parsed_state();
            // HalfOpen failure → Open with extended cooldown.
            if current_state == CircuitState::HalfOpen {
                wire.state = "open".into();
                wire.opened_at_epoch_ms = now_ms;
                let extended = wire.current_cooldown_secs.saturating_mul(2);
                wire.current_cooldown_secs = extended.min(self.config.cooldown_max.as_secs());
            } else {
                // Closed: bump counter; reset if window elapsed since
                // the last failure.
                if wire.last_failure_epoch_ms > 0
                    && now_ms.saturating_sub(wire.last_failure_epoch_ms)
                        > self.config.window.as_millis().min(u64::MAX as u128) as u64
                {
                    wire.consecutive_failures = 0;
                }
                wire.consecutive_failures = wire.consecutive_failures.saturating_add(1);
                wire.last_failure_epoch_ms = now_ms;
                if wire.consecutive_failures >= self.config.failure_threshold {
                    wire.state = "open".into();
                    wire.opened_at_epoch_ms = now_ms;
                    wire.current_cooldown_secs = self.config.cooldown.as_secs();
                }
            }
            self.persist(conn, &key, &wire).await?;
            Ok(())
        }

        fn backend(&self) -> &'static str {
            "valkey"
        }

        fn cooldown_seconds(&self) -> u64 {
            self.config.cooldown.as_secs()
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn prefix_is_applied() {
            assert_eq!(ValkeyCircuit::prefixed("acct"), "fivetran-circuit:acct");
        }

        #[test]
        fn from_url_is_sync_and_doesnt_connect() {
            let cb = ValkeyCircuit::from_url("redis://127.0.0.1:1/", CircuitConfig::default());
            assert!(cb.is_ok());
        }

        /// Live integration test.
        #[tokio::test]
        async fn live_valkey_trip_and_recover() {
            let url = match std::env::var("ROCKY_TEST_VALKEY_URL") {
                Ok(u) => u,
                Err(_) => {
                    eprintln!("skipping live_valkey_trip_and_recover — set ROCKY_TEST_VALKEY_URL");
                    return;
                }
            };

            let config = CircuitConfig {
                failure_threshold: 3,
                window: Duration::from_secs(60),
                cooldown: Duration::from_secs(1),
                cooldown_max: Duration::from_secs(10),
            };
            let cb = ValkeyCircuit::from_url(&url, config).expect("connect");
            let hash = format!("ROCKY_TEST_trip_{}", std::process::id());

            // Trip the breaker.
            for _ in 0..3 {
                cb.record_failure(&hash, FailureKind::Remote)
                    .await
                    .expect("record_failure");
            }
            assert_eq!(cb.state(&hash).await.unwrap(), CircuitState::Open);

            // Wait > cooldown then re-check; should transition to
            // HalfOpen.
            tokio::time::sleep(Duration::from_millis(1_100)).await;
            assert_eq!(cb.state(&hash).await.unwrap(), CircuitState::HalfOpen);

            // Probe success closes the breaker.
            cb.record_success(&hash).await.expect("ok");
            assert_eq!(cb.state(&hash).await.unwrap(), CircuitState::Closed);
        }
    }
}

#[cfg(feature = "valkey")]
pub use valkey::ValkeyCircuit;
