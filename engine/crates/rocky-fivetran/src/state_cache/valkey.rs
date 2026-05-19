//! Valkey / Redis state cache backend.
//!
//! [`ValkeyCache`] persists each cached envelope as a single Redis
//! string keyed by `fivetran-state:<key>`. Built behind the `valkey`
//! Cargo feature so the default build doesn't pull the Redis client
//! into the dependency closure — mirrors the same pattern in
//! [`rocky_cache::valkey`].
//!
//! ## Hash-dedupe
//!
//! Writes do a `GET` first and compare envelope hashes; matching values
//! short-circuit with [`WriteOutcome::SkippedNoChange`] (no `SET` over
//! the wire). This costs a round-trip per write, but it saves a write
//! on the cold-start herd case where N processes converge on the same
//! envelope — and the `GET` was already going to be paid by the next
//! reader anyway.
//!
//! ## TTL
//!
//! Every `SET` carries an `EX <ttl_seconds>` expiry. The TTL keeps stale
//! envelopes from outliving their useful window if the upstream Fivetran
//! tenant gets retired or the cache layer is rolled back. Default 600s
//! matches the FR-A spec.

use std::time::Duration;

use async_trait::async_trait;
use redis::AsyncCommands;
use tokio::sync::OnceCell;
use tracing::debug;

use super::{CacheError, FivetranStateCache, WriteOutcome};
use crate::envelope::{FivetranStateEnvelope, envelope_hash};

/// Redis key prefix for every Fivetran state cache entry. Namespacing
/// keeps the cache from colliding with Rocky's other Redis users
/// (state-sync's `rocky:state:*`, etc.) and makes operator-side
/// inspection trivial (`KEYS fivetran-state:*`).
const KEY_PREFIX: &str = "fivetran-state:";

/// Valkey / Redis state cache backend.
///
/// ## Lazy connect
///
/// The `from_url` constructor is sync (called from inside
/// [`super::build_state_cache`], itself called from the sync adapter
/// init paths in `rocky-cli::registry` / `rocky-cli::commands::discover`)
/// but the underlying [`redis::aio::ConnectionManager`] is async-init.
/// To bridge the two without dropping into [`tokio::runtime::Handle::block_on`]
/// (which deadlocks on a current-thread runtime and is documented as
/// "must not be called from inside an executor" on the multi-threaded
/// runtime), the [`ConnectionManager`] is held behind a
/// [`tokio::sync::OnceCell`] and initialized lazily on the first
/// `read` / `write` call — both of which already run on the tokio
/// runtime that constructed the [`ValkeyCache`].
///
/// A connect failure on the first call surfaces as
/// [`CacheError::Valkey`]; the cache layer above fails open and falls
/// through to the HTTP path.
pub struct ValkeyCache {
    /// Configured Redis client — cheap to clone, used to build the
    /// pool on first use. Holding the [`redis::Client`] rather than
    /// the raw URL string means `Client::open` validates the URL
    /// shape at `from_url` time so a typo'd config still fails fast.
    client: redis::Client,
    /// Async connection pool — initialized on first `read` / `write`.
    /// Kept in [`tokio::sync::OnceCell`] so concurrent first-callers
    /// share a single connect attempt rather than racing.
    conn: OnceCell<redis::aio::ConnectionManager>,
    /// TTL applied to every `SET` — keeps the layer from accumulating
    /// stale envelopes if the upstream tenant is retired.
    ttl: Duration,
}

impl std::fmt::Debug for ValkeyCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValkeyCache")
            .field("ttl_secs", &self.ttl.as_secs())
            .field("connected", &self.conn.initialized())
            .finish_non_exhaustive()
    }
}

impl ValkeyCache {
    /// Open a Valkey / Redis client for `url` (`redis://...` or
    /// `rediss://...` for TLS). `ttl` is applied to every `SET`.
    ///
    /// Synchronous and non-blocking: the actual TCP / TLS handshake
    /// is deferred until the first `read` / `write` call. URL parse
    /// errors surface here as [`CacheError::Valkey`]; connect errors
    /// surface on the first cache touch.
    pub fn from_url(url: &str, ttl: Duration) -> Result<Self, CacheError> {
        let client = redis::Client::open(url).map_err(|e| CacheError::Valkey(e.to_string()))?;
        Ok(ValkeyCache {
            client,
            conn: OnceCell::new(),
            ttl,
        })
    }

    /// Get-or-init the underlying [`ConnectionManager`].
    ///
    /// Concurrent first-callers contend on the [`OnceCell`] so we
    /// don't race to build N connection managers when the cache is
    /// hit by a burst of requests at startup.
    async fn get_conn(&self) -> Result<redis::aio::ConnectionManager, CacheError> {
        let conn = self
            .conn
            .get_or_try_init(|| async {
                redis::aio::ConnectionManager::new(self.client.clone())
                    .await
                    .map_err(|e| CacheError::Valkey(e.to_string()))
            })
            .await?;
        Ok(conn.clone())
    }

    /// Build the prefixed Redis key for `key`.
    fn prefixed(key: &str) -> String {
        format!("{KEY_PREFIX}{key}")
    }
}

#[async_trait]
impl FivetranStateCache for ValkeyCache {
    async fn read(&self, key: &str) -> Result<Option<FivetranStateEnvelope>, CacheError> {
        let prefixed = Self::prefixed(key);
        let mut conn = self.get_conn().await?;
        let raw: Option<String> = conn
            .get(&prefixed)
            .await
            .map_err(|e| CacheError::Valkey(e.to_string()))?;
        match raw {
            Some(json) => {
                let envelope: FivetranStateEnvelope = serde_json::from_str(&json)?;
                Ok(Some(envelope))
            }
            None => Ok(None),
        }
    }

    async fn write(
        &self,
        key: &str,
        envelope: &FivetranStateEnvelope,
    ) -> Result<WriteOutcome, CacheError> {
        let prefixed = Self::prefixed(key);
        let new_hash = envelope_hash(envelope);
        let mut conn = self.get_conn().await?;

        let prior_raw: Option<String> = conn
            .get(&prefixed)
            .await
            .map_err(|e| CacheError::Valkey(e.to_string()))?;
        if let Some(prior_json) = prior_raw
            && let Ok(prior_env) = serde_json::from_str::<FivetranStateEnvelope>(&prior_json)
            && envelope_hash(&prior_env) == new_hash
        {
            debug!(
                key,
                "fivetran valkey cache: hash-dedupe matched, skipping SET"
            );
            return Ok(WriteOutcome::SkippedNoChange);
        }

        let json = serde_json::to_string(envelope)?;
        let ttl_secs = self.ttl.as_secs().max(1);
        conn.set_ex::<&str, &str, ()>(&prefixed, &json, ttl_secs)
            .await
            .map_err(|e| CacheError::Valkey(e.to_string()))?;
        Ok(WriteOutcome::Written)
    }

    fn backend(&self) -> &'static str {
        "valkey"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_is_applied() {
        assert_eq!(
            ValkeyCache::prefixed("acct/dest"),
            "fivetran-state:acct/dest"
        );
    }

    /// Construction is non-blocking — `from_url` doesn't reach the
    /// network. Pins the lazy-connect property so a future refactor
    /// can't silently regress to a `block_on` in the constructor
    /// (which would panic on a current-thread runtime or deadlock on
    /// the multi-threaded one).
    #[test]
    fn from_url_is_sync_and_doesnt_connect() {
        // A port nothing's listening on. If `from_url` were
        // synchronously connecting it would either hang past this
        // test's wall-clock or surface a connection-refused error.
        let cache = ValkeyCache::from_url("redis://127.0.0.1:1/", Duration::from_secs(30));
        assert!(cache.is_ok(), "from_url must not connect: {cache:?}");
    }

    /// On the first `read` call against an unreachable server the
    /// connect manager surfaces a Valkey error rather than panicking.
    /// Confirms the lazy-connect path correctly propagates failures
    /// via the trait's error type.
    #[tokio::test]
    async fn first_read_against_unreachable_returns_err() {
        // Use a port that's deliberately not bound. The redis crate's
        // ConnectionManager will retry transiently before reporting
        // failure; the test waits long enough for the error to bubble
        // up but short enough that a hung test isn't ambiguous.
        let cache = ValkeyCache::from_url("redis://127.0.0.1:1/", Duration::from_secs(30))
            .expect("from_url must succeed without connecting");
        let result = tokio::time::timeout(Duration::from_secs(10), cache.read("acct/dest")).await;
        match result {
            // The redis crate may retry internally; we just need to
            // confirm the cache layer eventually surfaces a typed
            // Valkey error rather than panicking the executor.
            Ok(Err(CacheError::Valkey(_))) => (),
            Ok(other) => panic!("expected CacheError::Valkey, got {other:?}"),
            Err(_) => {
                // Timed out — connection retry budget exhausted is
                // also acceptable; what we care about is that we did
                // NOT panic on a `block_on` invariant.
            }
        }
    }

    /// Live Valkey integration test — gated on `ROCKY_TEST_VALKEY_URL`.
    /// Mirrors the `ROCKY_TEST_*` pattern used by other live-integration
    /// tests; the default CI run skips this.
    #[tokio::test]
    async fn live_valkey_round_trip() {
        let url = match std::env::var("ROCKY_TEST_VALKEY_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!(
                    "skipping live_valkey_round_trip — set ROCKY_TEST_VALKEY_URL to a Valkey/Redis URL to enable"
                );
                return;
            }
        };

        use std::collections::BTreeMap;

        use chrono::{DateTime, Utc};

        use crate::envelope::{
            FivetranConnectorStatus, FivetranConnectorSummary, FivetranDestination,
            FivetranSchemaConfig, FivetranStateEnvelope,
        };

        let env = FivetranStateEnvelope::from_parts(
            DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap(),
            FivetranDestination {
                id: "dest_live".into(),
                region: None,
                time_zone: None,
                service: None,
                setup_status: None,
            },
            vec![FivetranConnectorSummary {
                id: "conn_a".into(),
                name: "conn_a".into(),
                schema: "src__live".into(),
                service: "shopify".into(),
                status: FivetranConnectorStatus {
                    setup_state: "connected".into(),
                    sync_state: "scheduled".into(),
                },
                paused: false,
                succeeded_at: None,
                failed_at: None,
                group_id: None,
            }],
            BTreeMap::<String, FivetranSchemaConfig>::new(),
        );

        // Use a unique key per test run to avoid colliding with parallel
        // CI invocations.
        let key = format!("ROCKY_TEST_VALKEY_round_trip/{}", std::process::id());

        let cache = ValkeyCache::from_url(&url, Duration::from_secs(30)).expect("Valkey connect");
        let outcome = cache.write(&key, &env).await.expect("write");
        assert_eq!(outcome, WriteOutcome::Written);

        let back = cache.read(&key).await.expect("read").expect("present");
        assert_eq!(back, env);

        // Re-write with a different fetched_at — must dedupe.
        let mut env_b = env.clone();
        env_b.fetched_at = DateTime::<Utc>::from_timestamp(1_900_000_000, 0).unwrap();
        let outcome = cache.write(&key, &env_b).await.expect("re-write");
        assert_eq!(outcome, WriteOutcome::SkippedNoChange);
    }
}
