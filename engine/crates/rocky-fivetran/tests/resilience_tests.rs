//! Resilience-layer integration tests.
//!
//! Covers the three new defensive layers shipped on top of FR-A / FR-B
//! Phase 1:
//!
//! - Layer 1: cache stampede protection
//! - Layer 2: FR-B Phase 2 cross-pod budget (file-backend parity here;
//!   live Valkey behavior is unit-tested in `src/ratelimit_valkey.rs`)
//! - Layer 3: per-account circuit breaker (in-memory `InMemoryCircuit`
//!   stand-in for the Valkey state machine)
//!
//! The Valkey-backed implementations live behind the `valkey` Cargo
//! feature and are exercised separately by their crate-local unit
//! tests; here we use the in-memory + file-backed implementations so
//! the assertion lives in the default CI build.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use rocky_fivetran::circuit_breaker::test_support::InMemoryCircuit;
use rocky_fivetran::circuit_breaker::{
    CircuitConfig, CircuitError, CircuitState, FailureKind, FivetranCircuitBreaker,
};
use rocky_fivetran::client::{FivetranClient, FivetranError};
use rocky_fivetran::ratelimit::{BudgetError, FileBudget, RatelimitBudget, build_ratelimit_budget};
use rocky_fivetran::stampede::{LockError, LockGuard, NoLock, StampedeLock, build_stampede_lock};
use tokio::sync::Mutex;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// =============================================================================
// Layer 3 — circuit breaker state machine via InMemoryCircuit
// =============================================================================

#[tokio::test]
async fn circuit_trips_after_threshold_consecutive_failures() {
    let cb = InMemoryCircuit::new(CircuitConfig {
        failure_threshold: 3,
        window: Duration::from_secs(60),
        cooldown: Duration::from_secs(60),
        cooldown_max: Duration::from_secs(600),
    });

    // First two failures keep the circuit Closed.
    cb.record_failure("acct", FailureKind::Remote)
        .await
        .unwrap();
    assert_eq!(cb.state("acct").await.unwrap(), CircuitState::Closed);
    cb.record_failure("acct", FailureKind::Remote)
        .await
        .unwrap();
    assert_eq!(cb.state("acct").await.unwrap(), CircuitState::Closed);

    // Third failure trips it.
    cb.record_failure("acct", FailureKind::Remote)
        .await
        .unwrap();
    assert_eq!(cb.state("acct").await.unwrap(), CircuitState::Open);
}

#[tokio::test]
async fn circuit_local_failures_do_not_trip() {
    let cb = InMemoryCircuit::new(CircuitConfig {
        failure_threshold: 2,
        ..CircuitConfig::default()
    });
    for _ in 0..10 {
        cb.record_failure("acct", FailureKind::Local).await.unwrap();
    }
    assert_eq!(cb.state("acct").await.unwrap(), CircuitState::Closed);
}

#[tokio::test]
async fn circuit_cooldown_transitions_to_half_open() {
    let cb = InMemoryCircuit::new(CircuitConfig {
        failure_threshold: 1,
        window: Duration::from_secs(60),
        cooldown: Duration::from_millis(50),
        cooldown_max: Duration::from_secs(600),
    });
    cb.record_failure("acct", FailureKind::Remote)
        .await
        .unwrap();
    assert_eq!(cb.state("acct").await.unwrap(), CircuitState::Open);

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(cb.state("acct").await.unwrap(), CircuitState::HalfOpen);
}

#[tokio::test]
async fn circuit_half_open_success_closes_breaker() {
    let cb = InMemoryCircuit::new(CircuitConfig {
        failure_threshold: 1,
        cooldown: Duration::from_millis(20),
        ..CircuitConfig::default()
    });
    cb.record_failure("acct", FailureKind::Remote)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(cb.state("acct").await.unwrap(), CircuitState::HalfOpen);

    cb.record_success("acct").await.unwrap();
    assert_eq!(cb.state("acct").await.unwrap(), CircuitState::Closed);
}

#[tokio::test]
async fn circuit_half_open_failure_reopens_with_extended_cooldown() {
    let cb = InMemoryCircuit::new(CircuitConfig {
        failure_threshold: 1,
        cooldown: Duration::from_millis(40),
        cooldown_max: Duration::from_millis(200),
        ..CircuitConfig::default()
    });
    cb.record_failure("acct", FailureKind::Remote)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(cb.state("acct").await.unwrap(), CircuitState::HalfOpen);

    // A failure in HalfOpen pushes back to Open with extended
    // cooldown (40ms → 80ms).
    cb.record_failure("acct", FailureKind::Remote)
        .await
        .unwrap();
    assert_eq!(cb.state("acct").await.unwrap(), CircuitState::Open);

    // After the original 40ms it must NOT yet be HalfOpen.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(cb.state("acct").await.unwrap(), CircuitState::Open);

    // After 80ms total it should be HalfOpen again.
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert_eq!(cb.state("acct").await.unwrap(), CircuitState::HalfOpen);
}

#[tokio::test]
async fn circuit_cooldown_caps_at_cooldown_max() {
    let cb = InMemoryCircuit::new(CircuitConfig {
        failure_threshold: 1,
        cooldown: Duration::from_millis(20),
        cooldown_max: Duration::from_millis(50),
        ..CircuitConfig::default()
    });
    // Drive multiple half-open → open transitions; the cooldown
    // doubles each time but must not exceed cooldown_max.
    cb.record_failure("acct", FailureKind::Remote)
        .await
        .unwrap();
    for _ in 0..6 {
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(cb.state("acct").await.unwrap(), CircuitState::HalfOpen);
        cb.record_failure("acct", FailureKind::Remote)
            .await
            .unwrap();
        assert_eq!(cb.state("acct").await.unwrap(), CircuitState::Open);
    }
}

// =============================================================================
// Layer 1 — stampede + cache-poll behavior with HTTP wiremock + in-memory cache
// =============================================================================

/// In-memory `FivetranStateCache` for client_layered_tests: tracks
/// reads and writes via shared atomic counters so the test can prove
/// who actually hit the cache vs the wire.
#[derive(Debug)]
struct CountingCache {
    inner: Mutex<Option<rocky_fivetran::envelope::FivetranStateEnvelope>>,
    reads: AtomicU32,
    writes: AtomicU32,
}

impl CountingCache {
    fn new() -> Self {
        CountingCache {
            inner: Mutex::new(None),
            reads: AtomicU32::new(0),
            writes: AtomicU32::new(0),
        }
    }
}

#[async_trait]
impl rocky_fivetran::state_cache::FivetranStateCache for CountingCache {
    async fn read(
        &self,
        _key: &str,
    ) -> Result<
        Option<rocky_fivetran::envelope::FivetranStateEnvelope>,
        rocky_fivetran::state_cache::CacheError,
    > {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok(self.inner.lock().await.clone())
    }

    async fn write(
        &self,
        _key: &str,
        envelope: &rocky_fivetran::envelope::FivetranStateEnvelope,
    ) -> Result<rocky_fivetran::state_cache::WriteOutcome, rocky_fivetran::state_cache::CacheError>
    {
        self.writes.fetch_add(1, Ordering::SeqCst);
        *self.inner.lock().await = Some(envelope.clone());
        Ok(rocky_fivetran::state_cache::WriteOutcome::Written)
    }

    fn backend(&self) -> &'static str {
        // Surface as a non-"none" backend so the cache pathway in
        // fetch_envelope actually exercises read/write.
        "in_memory_counting"
    }
}

/// In-memory leader-elect stampede: the first caller to acquire the
/// key wins; subsequent callers get `None`. Tracks acquire-count so a
/// test can prove only one leader was elected. Release is a no-op
/// for the test — once "held", the key stays held for the duration
/// of the test (which is exactly what we want when proving 10
/// concurrent callers see only 1 leader).
#[derive(Debug)]
struct InMemoryStampede {
    held: std::sync::Mutex<std::collections::HashSet<String>>,
    acquired: AtomicU32,
}

impl InMemoryStampede {
    fn new() -> Self {
        InMemoryStampede {
            held: std::sync::Mutex::new(std::collections::HashSet::new()),
            acquired: AtomicU32::new(0),
        }
    }
}

#[async_trait]
impl StampedeLock for InMemoryStampede {
    async fn acquire(&self, key: &str, _ttl: Duration) -> Result<Option<LockGuard>, LockError> {
        let inserted = self.held.lock().unwrap().insert(key.to_string());
        if !inserted {
            return Ok(None);
        }
        self.acquired.fetch_add(1, Ordering::SeqCst);
        Ok(Some(LockGuard::new(|| {
            // No-op release; the test asserts on `acquired` count
            // and the cache for "exactly one leader observed."
        })))
    }

    fn backend(&self) -> &'static str {
        "in_memory_stampede"
    }
}

/// Mount one set of endpoints with `expect(1)` so wiremock fails the
/// test if the API is hit more than once across N concurrent clients.
async fn mount_layered_envelope_endpoints_once(server: &MockServer, dest_id: &str) {
    Mock::given(method("GET"))
        .and(path(format!("/v1/destinations/{dest_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "code": "Success",
            "data": {
                "id": dest_id,
                "region": "us-east-1",
                "time_zone": "UTC",
                "service": "snowflake",
                "setup_status": "connected"
            }
        })))
        .expect(1)
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!("/v1/groups/{dest_id}/connectors")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "code": "Success",
            "data": {
                "items": [
                    {
                        "id": "conn_a",
                        "group_id": dest_id,
                        "service": "shopify",
                        "schema": "src__acme__na__shopify",
                        "connector_name": "Acme Shopify",
                        "status": { "setup_state": "connected", "sync_state": "scheduled" },
                        "paused": false,
                        "succeeded_at": "2026-05-18T10:00:00Z",
                        "failed_at": null
                    }
                ],
                "next_cursor": null
            }
        })))
        .expect(1)
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/connectors/conn_a/schemas"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "code": "Success",
            "data": {
                "schemas": {
                    "src__acme__na__shopify": {
                        "enabled": true,
                        "tables": {
                            "orders": {
                                "enabled": true,
                                "sync_mode": "SOFT_DELETE",
                                "columns": {
                                    "id": { "enabled": true, "hashed": false }
                                }
                            }
                        }
                    }
                }
            }
        })))
        .expect(1)
        .mount(server)
        .await;
}

/// Layered fetch test: 10 concurrent `fetch_envelope` callers share
/// one stampede lock and one cache, and the wire is hit exactly once.
///
/// Because each `FivetranClient` carries its own in-process memo,
/// concurrent calls on DIFFERENT clients (one per pod) still race
/// the L0 layer — exactly the cold-start herd this PR addresses.
#[tokio::test]
async fn layered_concurrent_callers_collapse_to_one_http_call() {
    let server = MockServer::start().await;
    mount_layered_envelope_endpoints_once(&server, "dest_layered").await;

    // Shared coordination layers across all simulated pods. Keep a
    // typed handle to `CountingCache` so the assertions below can
    // reach the AtomicU32 counters; clone an `Arc<dyn>` to the
    // client builder via separate variables.
    let counting = Arc::new(CountingCache::new());
    let stampede_inner = Arc::new(InMemoryStampede::new());

    // Build 10 distinct clients (each represents a "pod") all pointing
    // at the same coordination state — so L0 never short-circuits.
    let mut tasks = Vec::new();
    for _ in 0..10 {
        let cache: Arc<dyn rocky_fivetran::state_cache::FivetranStateCache> = counting.clone();
        let stampede: Arc<dyn StampedeLock> = stampede_inner.clone();
        let base = server.uri();
        tasks.push(tokio::spawn(async move {
            let client = FivetranClient::with_base_url(
                "shared-api-key-for-stampede-test".into(),
                "shared-secret".into(),
                base,
            )
            .with_state_cache(cache)
            .with_stampede_lock(stampede)
            // Tighten the follower poll timeout so the test doesn't
            // wait the default 30s if something goes sideways.
            .with_stampede_poll_timeout(Duration::from_secs(5))
            .with_stampede_lock_ttl(Duration::from_secs(10));
            client.fetch_envelope("dest_layered", false).await
        }));
    }

    let mut envelopes = Vec::new();
    for t in tasks {
        envelopes.push(t.await.unwrap().expect("fetch must succeed"));
    }

    // All callers see the same envelope.
    let first = envelopes.first().unwrap().clone();
    for env in &envelopes {
        assert_eq!(env.destination.id, first.destination.id);
    }

    // wiremock's `.expect(1)` on each endpoint already asserts the
    // wire was hit exactly once on server drop. Pin the additional
    // invariants explicitly:
    // - Exactly one leader wrote the cache (the rest were followers
    //   who polled the cache).
    let writes = counting.writes.load(Ordering::SeqCst);
    assert_eq!(
        writes, 1,
        "exactly one leader must write the cache; observed {writes}"
    );
    // - Followers polled the cache more than once (at least the
    //   initial L2 miss + one poll-loop iteration each).
    let reads = counting.reads.load(Ordering::SeqCst);
    assert!(reads > 1, "followers must poll the cache; reads = {reads}");
    // - Exactly one stampede leader was elected.
    let acquired = stampede_inner.acquired.load(Ordering::SeqCst);
    assert_eq!(
        acquired, 1,
        "exactly one stampede leader must be elected; observed {acquired}"
    );

    drop(server);
}

/// Refresh=true bypasses the stampede protection — caller explicitly
/// wants fresh data, so waiting for another leader's TTL window
/// would violate intent.
#[tokio::test]
async fn refresh_true_bypasses_stampede() {
    let server = MockServer::start().await;
    // Both calls below run with refresh=true → each must hit the wire
    // independently. expect(2) per endpoint.
    Mock::given(method("GET"))
        .and(path("/v1/destinations/dest_refresh"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "code": "Success",
            "data": {
                "id": "dest_refresh", "region": null, "time_zone": null,
                "service": null, "setup_status": null
            }
        })))
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/groups/dest_refresh/connectors"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "code": "Success",
            "data": { "items": [], "next_cursor": null }
        })))
        .expect(2)
        .mount(&server)
        .await;

    let stampede: Arc<dyn StampedeLock> = Arc::new(InMemoryStampede::new());
    let client = FivetranClient::with_base_url("k".into(), "s".into(), server.uri())
        .with_stampede_lock(stampede.clone());

    let _ = client.fetch_envelope("dest_refresh", true).await.unwrap();
    let _ = client.fetch_envelope("dest_refresh", true).await.unwrap();
    drop(server);
}

// =============================================================================
// Circuit-open path short-circuits HTTP
// =============================================================================

/// When the breaker is Open, `fetch_envelope` must return
/// `FivetranError::CircuitOpen` immediately without touching the
/// wire. wiremock's `expect(0)` enforces no-HTTP.
#[tokio::test]
async fn circuit_open_short_circuits_http() {
    let server = MockServer::start().await;
    // No endpoint may be hit — `expect(0)` on a wildcard mount.
    Mock::given(method("GET"))
        .and(wiremock::matchers::path_regex(".*"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;

    let cb = Arc::new(InMemoryCircuit::new(CircuitConfig {
        failure_threshold: 1,
        cooldown: Duration::from_secs(60),
        ..CircuitConfig::default()
    }));
    // Pre-trip the breaker for the account hash this client uses.
    let api_key = "circuit_test_key";
    let account_hash = rocky_fivetran::ratelimit::hash_account_id(api_key);
    cb.record_failure(&account_hash, FailureKind::Remote)
        .await
        .unwrap();
    assert_eq!(
        cb.state(&account_hash).await.unwrap(),
        CircuitState::Open,
        "breaker must be Open before the fetch attempt"
    );

    let client = FivetranClient::with_base_url(api_key.into(), "s".into(), server.uri())
        .with_circuit_breaker(cb);

    let err = client
        .fetch_envelope("dest_circuit", false)
        .await
        .expect_err("CircuitOpen must short-circuit");
    assert!(
        matches!(
            err,
            FivetranError::CircuitOpen {
                cooldown_seconds: 60
            }
        ),
        "expected CircuitOpen with 60s cooldown, got {err:?}"
    );
    drop(server);
}

// =============================================================================
// Sustained 429-storm trips the shared breaker (FR rate-limit-self-healing)
// =============================================================================

/// A sustained 429 storm must trip the shared circuit breaker. Each
/// envelope-fetch hits 429 and surfaces `RateLimited`; under the
/// post-FR classification `is_remote_failure(RateLimited) == true`, so
/// each call votes `Remote` to the breaker. After `failure_threshold`
/// consecutive votes the breaker transitions `Closed → Open`, and the
/// next call short-circuits with [`FivetranError::CircuitOpen`]
/// carrying the configured cooldown.
#[tokio::test]
async fn rate_limit_storm_trips_breaker() {
    let server = MockServer::start().await;
    // Every endpoint returns 429 — fetch_envelope's first internal
    // call surfaces RateLimited on attempt 0 (max_retries=0).
    Mock::given(method("GET"))
        .and(wiremock::matchers::path_regex(".*"))
        .respond_with(ResponseTemplate::new(429))
        .mount(&server)
        .await;

    let cb = Arc::new(InMemoryCircuit::new(CircuitConfig {
        failure_threshold: 5,
        window: Duration::from_secs(60),
        cooldown: Duration::from_secs(300),
        cooldown_max: Duration::from_secs(3600),
    }));
    let api_key = "storm_test_key";
    let account_hash = rocky_fivetran::ratelimit::hash_account_id(api_key);

    let retry = rocky_core::config::RetryConfig {
        max_retries: 0,
        ..rocky_core::config::RetryConfig::default()
    };
    let client =
        FivetranClient::with_base_url_and_retry(api_key.into(), "s".into(), server.uri(), retry)
            .with_circuit_breaker(cb.clone());

    // Five envelope-fetches against a 429-only wiremock — each
    // surfaces RateLimited and votes Remote.
    for i in 0..5u32 {
        let err = client
            .fetch_envelope("dest_storm", false)
            .await
            .expect_err("429 storm should surface RateLimited");
        assert!(
            matches!(err, FivetranError::RateLimited),
            "call {i}: expected RateLimited, got {err:?}"
        );
    }
    assert_eq!(
        cb.state(&account_hash).await.unwrap(),
        CircuitState::Open,
        "breaker must be Open after {} consecutive 429-storm votes",
        5
    );

    // Sixth call short-circuits with CircuitOpen + the configured cooldown.
    let err = client
        .fetch_envelope("dest_storm", false)
        .await
        .expect_err("CircuitOpen must short-circuit after trip");
    assert!(
        matches!(
            err,
            FivetranError::CircuitOpen {
                cooldown_seconds: 300
            }
        ),
        "expected CircuitOpen {{ cooldown_seconds: 300 }}, got {err:?}"
    );
    drop(server);
}

/// A 429 burst below the breaker's `failure_threshold` must NOT trip
/// it. Regression for the FR's "single 429 isn't a service failure"
/// property — the post-retry classification only acts on *sustained*
/// storms (≥ `failure_threshold` votes within `window`).
#[tokio::test]
async fn rate_limit_below_threshold_does_not_trip_breaker() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(wiremock::matchers::path_regex(".*"))
        .respond_with(ResponseTemplate::new(429))
        .mount(&server)
        .await;

    let cb = Arc::new(InMemoryCircuit::new(CircuitConfig {
        failure_threshold: 5,
        window: Duration::from_secs(60),
        cooldown: Duration::from_secs(300),
        cooldown_max: Duration::from_secs(3600),
    }));
    let api_key = "below_threshold_key";
    let account_hash = rocky_fivetran::ratelimit::hash_account_id(api_key);

    let retry = rocky_core::config::RetryConfig {
        max_retries: 0,
        ..rocky_core::config::RetryConfig::default()
    };
    let client =
        FivetranClient::with_base_url_and_retry(api_key.into(), "s".into(), server.uri(), retry)
            .with_circuit_breaker(cb.clone());

    // Four 429-surfacing calls (one below threshold=5). The breaker
    // counts them but does not trip.
    for i in 0..4u32 {
        let err = client
            .fetch_envelope("dest_below", false)
            .await
            .expect_err("429 should surface RateLimited");
        assert!(
            matches!(err, FivetranError::RateLimited),
            "call {i}: expected RateLimited, got {err:?}"
        );
    }
    assert_eq!(
        cb.state(&account_hash).await.unwrap(),
        CircuitState::Closed,
        "breaker must stay Closed below failure_threshold"
    );
    drop(server);
}

// =============================================================================
// Layer 2 — FR-B Phase 2 budget trait dispatch (file backend parity)
// =============================================================================

#[tokio::test]
async fn ratelimit_budget_factory_defaults_to_file() {
    let budget = build_ratelimit_budget(None).expect("no config → FileBudget");
    assert_eq!(budget.backend(), "file");
}

#[tokio::test]
async fn ratelimit_budget_factory_resolves_file_backend() {
    use rocky_core::config::{FivetranRatelimitBackend, FivetranRatelimitConfig};
    let cfg = FivetranRatelimitConfig {
        backend: FivetranRatelimitBackend::File,
        valkey_url: None,
        max_wake_seconds: None,
    };
    let budget = build_ratelimit_budget(Some(&cfg)).expect("file backend");
    assert_eq!(budget.backend(), "file");
}

#[tokio::test]
#[cfg_attr(feature = "valkey", ignore)] // covered by valkey unit tests
async fn ratelimit_budget_valkey_requires_feature_when_disabled() {
    use rocky_core::config::{FivetranRatelimitBackend, FivetranRatelimitConfig};
    let cfg = FivetranRatelimitConfig {
        backend: FivetranRatelimitBackend::Valkey,
        valkey_url: Some("redis://localhost:6379/".into()),
        max_wake_seconds: None,
    };
    let err = build_ratelimit_budget(Some(&cfg)).expect_err("valkey requested but feature absent");
    assert!(matches!(err, BudgetError::Config(_)));
}

#[tokio::test]
async fn stampede_factory_defaults_to_no_lock() {
    let (lock, tunables) = build_stampede_lock(None).expect("no config → NoLock");
    assert_eq!(lock.backend(), "none");
    assert!(tunables.lock_ttl > Duration::ZERO);
    assert!(tunables.poll_timeout > Duration::ZERO);
}

#[tokio::test]
async fn stampede_factory_resolves_none_backend() {
    use rocky_core::config::{FivetranStampedeBackend, FivetranStampedeConfig};
    let cfg = FivetranStampedeConfig {
        backend: FivetranStampedeBackend::None,
        valkey_url: None,
        lock_ttl_seconds: Some(120),
        poll_timeout_seconds: Some(45),
    };
    let (lock, tunables) = build_stampede_lock(Some(&cfg)).expect("none backend");
    assert_eq!(lock.backend(), "none");
    assert_eq!(tunables.lock_ttl, Duration::from_secs(120));
    assert_eq!(tunables.poll_timeout, Duration::from_secs(45));
}

#[tokio::test]
async fn circuit_factory_defaults_to_always_closed() {
    let cb = rocky_fivetran::circuit_breaker::build_circuit_breaker(None)
        .expect("no config → AlwaysClosed");
    assert_eq!(cb.backend(), "none");
    assert_eq!(cb.state("anything").await.unwrap(), CircuitState::Closed);
}

#[tokio::test]
async fn budget_file_backend_isolation() {
    // Two FileBudgets with distinct dirs do NOT share state. Each
    // simulated "host" gets its own dir, matching the Phase-1
    // per-host contract.
    let tmp_a = tempfile::tempdir().unwrap();
    let tmp_b = tempfile::tempdir().unwrap();
    let budget_a: Box<dyn RatelimitBudget> = Box::new(FileBudget::new(tmp_a.path().to_path_buf()));
    let budget_b: Box<dyn RatelimitBudget> = Box::new(FileBudget::new(tmp_b.path().to_path_buf()));

    let acct = "abc123";
    let later = std::time::SystemTime::now() + Duration::from_secs(60);
    budget_a.set_wake_at(acct, later).await.unwrap();

    let from_a = budget_a.wake_at(acct).await.unwrap();
    let from_b = budget_b.wake_at(acct).await.unwrap();
    assert!(from_a.is_some(), "budget_a should observe its own write");
    assert!(
        from_b.is_none(),
        "budget_b is isolated; should NOT see budget_a's write"
    );
}

// =============================================================================
// Stampede lock-guard ordering
// =============================================================================

/// The default `NoLock` backend always returns a guard — every caller
/// is a leader. Used as the smoke test that the trait dispatch through
/// the client is wired correctly.
#[tokio::test]
async fn nolock_grants_every_caller_as_leader() {
    let lock = NoLock;
    let g1 = lock
        .acquire("k", Duration::from_secs(60))
        .await
        .unwrap()
        .expect("nolock grants");
    let g2 = lock
        .acquire("k", Duration::from_secs(60))
        .await
        .unwrap()
        .expect("nolock grants again");
    drop(g1);
    drop(g2);
}

#[tokio::test]
async fn stampede_factory_threads_ttl_to_client() {
    // Confirms that the stampede tunables flow into the client's
    // builder. Regression guard: a refactor that dropped the
    // `with_stampede_lock_ttl(...)` wiring would silently regress
    // the Layer 1 SLA.
    use rocky_core::config::{FivetranStampedeBackend, FivetranStampedeConfig};
    let cfg = FivetranStampedeConfig {
        backend: FivetranStampedeBackend::None,
        valkey_url: None,
        lock_ttl_seconds: Some(45),
        poll_timeout_seconds: Some(12),
    };
    let (lock, tunables) = build_stampede_lock(Some(&cfg)).unwrap();
    let _client = FivetranClient::with_base_url("k".into(), "s".into(), "http://localhost".into())
        .with_stampede_lock(lock)
        .with_stampede_lock_ttl(tunables.lock_ttl)
        .with_stampede_poll_timeout(tunables.poll_timeout);
    // No assertion shape here beyond "compiles + builds without
    // panicking"; the layered tests above prove the runtime behavior.
}

#[tokio::test]
async fn circuit_error_classified_local_does_not_trip() {
    // Smoke: only Remote failures move the state machine.
    let cb = InMemoryCircuit::new(CircuitConfig {
        failure_threshold: 1,
        ..CircuitConfig::default()
    });
    cb.record_failure("acct", FailureKind::Local).await.unwrap();
    assert_eq!(cb.state("acct").await.unwrap(), CircuitState::Closed);
    cb.record_failure("acct", FailureKind::Remote)
        .await
        .unwrap();
    assert_eq!(cb.state("acct").await.unwrap(), CircuitState::Open);
}

// Touch the CircuitError type so it isn't unused-imported.
#[allow(dead_code)]
fn _circuit_error_imported(_e: &CircuitError) {}
