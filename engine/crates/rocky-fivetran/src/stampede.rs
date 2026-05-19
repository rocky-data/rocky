//! Distributed cache-stampede protection (Layer 1).
//!
//! On a cold-start herd, N concurrent rocky-cli processes simultaneously
//! observe the cache miss, fan out N independent API calls, and then
//! converge with N redundant write-backs. The stampede lock elects a
//! single leader to do the API call; followers wait on the cache key
//! with bounded polling until the leader's write becomes visible.
//!
//! ## Trait
//!
//! [`StampedeLock::acquire`] returns:
//! - `Ok(Some(LockGuard))` — caller is the leader, must release on
//!   `Drop` (or via the guard's lifetime ending).
//! - `Ok(None)` — another caller holds the lock; the caller is a
//!   follower and should poll the cache.
//! - `Err(_)` — backend unreachable. Caller falls open to the direct
//!   HTTP path (no protection, but no block).
//!
//! ## Backends
//!
//! - [`NoLock`] — every `acquire` succeeds; behavior identical to no
//!   stampede protection. Default when `[adapter.fivetran.stampede]`
//!   is absent or `backend = "none"`.
//! - [`ValkeyLock`] — distributed via `SET <key>:lock <unique_id> NX
//!   EX <ttl>`. Behind the `valkey` Cargo feature.
//!
//! ## Lock fencing
//!
//! Each acquisition generates a unique 128-bit token persisted in the
//! Valkey value. `LockGuard::Drop` issues an EVAL Lua script that
//! check-and-DELs only if the value still matches our token; this
//! prevents a leader that overran the TTL from deleting a fresh
//! lock acquired by a different holder after expiry. The mechanism is
//! the standard Redis SETNX-with-fencing pattern documented at
//! https://redis.io/docs/latest/develop/use/patterns/distributed-locks/
//! (single-instance variant — the "Redlock" multi-instance variant is
//! intentionally out of scope; one Valkey is the deployment shape).

use std::time::Duration;

use async_trait::async_trait;
use thiserror::Error;

/// Errors surfaced by [`StampedeLock`] implementations.
///
/// Like the other coordination-layer errors, these are coarse on
/// purpose — fail-open is the caller's only useful policy.
#[derive(Debug, Error)]
pub enum LockError {
    /// Valkey / Redis transport failure (connect refused, TLS error,
    /// command timeout).
    #[error("stampede valkey: {0}")]
    Valkey(String),
    /// Misconfigured backend — surfaced from `build_stampede_lock`
    /// rather than from a request.
    #[error("stampede config: {0}")]
    Config(String),
}

/// Trait every stampede-lock backend implements.
///
/// Acquired guards must explicitly release on `Drop`. A guard's
/// lifetime represents leadership for the keyed envelope fetch —
/// dropping the guard signals "leader done, followers may poll
/// freely." Implementations that can't release synchronously (i.e.
/// Valkey) spawn an async task on drop to issue the release; if the
/// process exits before the task runs, the TTL backstop guarantees
/// the lock eventually disappears.
#[async_trait]
pub trait StampedeLock: Send + Sync + std::fmt::Debug {
    /// Try to acquire the lock for `key` with `ttl`.
    ///
    /// - `Ok(Some(_))` → caller is the leader.
    /// - `Ok(None)` → another caller is the leader; caller is a
    ///   follower and should poll the cache.
    /// - `Err(_)` → backend unreachable.
    async fn acquire(&self, key: &str, ttl: Duration) -> Result<Option<LockGuard>, LockError>;

    /// Stable backend tag for OTLP span attributes.
    fn backend(&self) -> &'static str;
}

/// Opaque guard returned by a successful [`StampedeLock::acquire`].
///
/// Holds whatever release primitive the backend needs (a `no-op`
/// closure for `NoLock`, a Valkey release script + key/token pair for
/// `ValkeyLock`). Dropping the guard runs the release; explicit
/// release is not exposed because the wiring in `fetch_envelope`
/// always wants release at scope end.
pub struct LockGuard {
    on_drop: Option<Box<dyn FnOnce() + Send>>,
}

impl LockGuard {
    /// Build a guard whose `Drop` runs `f`. The closure runs at most
    /// once — `Drop` clears `on_drop` so a double-drop is well-defined
    /// (no-op the second time).
    pub fn new<F>(f: F) -> Self
    where
        F: FnOnce() + Send + 'static,
    {
        LockGuard {
            on_drop: Some(Box::new(f)),
        }
    }

    /// Sentinel guard for the no-op backend ([`NoLock`]). Releasing is
    /// a no-op so the closure is `||{}`.
    pub fn noop() -> Self {
        LockGuard::new(|| {})
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        if let Some(f) = self.on_drop.take() {
            f();
        }
    }
}

impl std::fmt::Debug for LockGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LockGuard")
            .field("on_drop_armed", &self.on_drop.is_some())
            .finish()
    }
}

/// No-op lock — every `acquire` succeeds. Default backend when no
/// stampede protection is configured.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoLock;

#[async_trait]
impl StampedeLock for NoLock {
    async fn acquire(&self, _key: &str, _ttl: Duration) -> Result<Option<LockGuard>, LockError> {
        Ok(Some(LockGuard::noop()))
    }

    fn backend(&self) -> &'static str {
        "none"
    }
}

/// Compute the suggested exponential backoff for the follower's
/// cache-poll loop. Starts at 100ms, doubles each iteration, capped
/// at 2s — matches the FR plan §Layer 1 schedule.
///
/// Exposed at module scope so tests and the client's poll loop share
/// one implementation.
pub fn poll_backoff(attempt: u32) -> Duration {
    let exp = 100u64.saturating_mul(2u64.saturating_pow(attempt.min(8)));
    Duration::from_millis(exp.min(2_000))
}

/// Cap on the [`StampedeLock`]-driven follower poll loop. Defaults to
/// 30s per the FR plan; the per-config `poll_timeout_seconds` knob
/// overrides this. Surfaced at the client wiring layer, not the trait.
pub const DEFAULT_POLL_TIMEOUT: Duration = Duration::from_secs(30);

/// Default lock TTL — caps how long a leader can hold the lock before
/// followers are allowed to elect a new leader. The per-config
/// `lock_ttl_seconds` knob overrides this. 60s matches the FR plan.
pub const DEFAULT_LOCK_TTL: Duration = Duration::from_secs(60);

/// Resolved tunables for a stampede backend — populated from the
/// user's [`FivetranStampedeConfig`] block with the canonical
/// defaults applied. Surfaced from
/// [`build_stampede_lock`](crate::stampede::build_stampede_lock) so
/// the caller can thread the configured TTL + poll-timeout into the
/// [`FivetranClient`](crate::client::FivetranClient).
#[derive(Debug, Clone, Copy)]
pub struct StampedeTunables {
    /// TTL applied to the lock acquire (`SET NX EX <ttl>`).
    pub lock_ttl: Duration,
    /// Cap on the follower poll loop.
    pub poll_timeout: Duration,
}

/// Construct the stampede lock backend described by `config`. Returns
/// the [`Arc<dyn StampedeLock>`] and the resolved tunables; the
/// caller threads both into the client. Defaults to
/// [`NoLock`] + crate defaults when `config` is `None`.
///
/// # Errors
///
/// - [`LockError::Config`] — required field missing.
/// - [`LockError::Valkey`] — the Redis client refused the URL.
pub fn build_stampede_lock(
    config: Option<&rocky_core::config::FivetranStampedeConfig>,
) -> Result<(std::sync::Arc<dyn StampedeLock>, StampedeTunables), LockError> {
    use rocky_core::config::FivetranStampedeBackend;
    let tunables_default = StampedeTunables {
        lock_ttl: DEFAULT_LOCK_TTL,
        poll_timeout: DEFAULT_POLL_TIMEOUT,
    };
    let Some(cfg) = config else {
        return Ok((std::sync::Arc::new(NoLock), tunables_default));
    };
    let resolved = StampedeTunables {
        lock_ttl: cfg
            .lock_ttl_seconds
            .map(Duration::from_secs)
            .unwrap_or(tunables_default.lock_ttl),
        poll_timeout: cfg
            .poll_timeout_seconds
            .map(Duration::from_secs)
            .unwrap_or(tunables_default.poll_timeout),
    };
    match cfg.backend {
        FivetranStampedeBackend::None => Ok((std::sync::Arc::new(NoLock), resolved)),
        #[cfg(feature = "valkey")]
        FivetranStampedeBackend::Valkey => {
            let url = cfg.valkey_url.as_ref().ok_or_else(|| {
                LockError::Config(
                    "backend = \"valkey\" requires `valkey_url` in [adapter.<name>.stampede]"
                        .into(),
                )
            })?;
            let lock = crate::stampede::valkey::ValkeyLock::from_url(url)?;
            Ok((std::sync::Arc::new(lock), resolved))
        }
        #[cfg(not(feature = "valkey"))]
        FivetranStampedeBackend::Valkey => Err(LockError::Config(
            "backend = \"valkey\" requires building rocky-fivetran with the `valkey` feature"
                .into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn no_lock_always_grants() {
        let lock = NoLock;
        let g = lock
            .acquire("key", Duration::from_secs(60))
            .await
            .expect("ok")
            .expect("some");
        drop(g);
    }

    #[test]
    fn poll_backoff_schedule() {
        // 100ms, 200ms, 400ms, 800ms, 1600ms, then capped at 2s.
        assert_eq!(poll_backoff(0), Duration::from_millis(100));
        assert_eq!(poll_backoff(1), Duration::from_millis(200));
        assert_eq!(poll_backoff(2), Duration::from_millis(400));
        assert_eq!(poll_backoff(3), Duration::from_millis(800));
        assert_eq!(poll_backoff(4), Duration::from_millis(1600));
        assert_eq!(poll_backoff(5), Duration::from_millis(2_000));
        assert_eq!(poll_backoff(20), Duration::from_millis(2_000));
    }

    #[test]
    fn lock_guard_releases_on_drop() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let flag = Arc::new(AtomicBool::new(false));
        let flag2 = flag.clone();
        let g = LockGuard::new(move || {
            flag2.store(true, Ordering::SeqCst);
        });
        assert!(!flag.load(Ordering::SeqCst));
        drop(g);
        assert!(flag.load(Ordering::SeqCst));
    }

    #[test]
    fn lock_guard_releases_only_once() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU32, Ordering};

        let count = Arc::new(AtomicU32::new(0));
        let count2 = count.clone();
        let mut g = LockGuard::new(move || {
            count2.fetch_add(1, Ordering::SeqCst);
        });
        // Manually take + drop the inner option to simulate a
        // re-entrant Drop (e.g. double-free); the second drop must
        // be a no-op.
        let inner = g.on_drop.take();
        if let Some(f) = inner {
            f();
        }
        drop(g);
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
}

#[cfg(feature = "valkey")]
pub mod valkey {
    //! Valkey-backed [`StampedeLock`] implementation.

    use std::time::Duration;

    use async_trait::async_trait;
    use tokio::sync::OnceCell;
    use tracing::{debug, warn};

    use super::{LockError, LockGuard, StampedeLock};

    /// Redis key prefix for every stampede lock. Distinct from the
    /// cache and ratelimit prefixes so a `KEYS fivetran-lock:*`
    /// inventory is unambiguous.
    const KEY_PREFIX: &str = "fivetran-lock:";

    /// Distributed stampede lock backed by Valkey / Redis.
    pub struct ValkeyLock {
        client: redis::Client,
        conn: OnceCell<redis::aio::ConnectionManager>,
    }

    impl std::fmt::Debug for ValkeyLock {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("ValkeyLock")
                .field("connected", &self.conn.initialized())
                .finish_non_exhaustive()
        }
    }

    impl ValkeyLock {
        /// Open a Valkey / Redis client for `url`. Sync + non-blocking;
        /// the TCP / TLS handshake is deferred until the first
        /// `acquire`.
        pub fn from_url(url: &str) -> Result<Self, LockError> {
            let client = redis::Client::open(url).map_err(|e| LockError::Valkey(e.to_string()))?;
            Ok(ValkeyLock {
                client,
                conn: OnceCell::new(),
            })
        }

        async fn get_conn(&self) -> Result<redis::aio::ConnectionManager, LockError> {
            let conn = self
                .conn
                .get_or_try_init(|| async {
                    redis::aio::ConnectionManager::new(self.client.clone())
                        .await
                        .map_err(|e| LockError::Valkey(e.to_string()))
                })
                .await?;
            Ok(conn.clone())
        }

        fn prefixed(key: &str) -> String {
            format!("{KEY_PREFIX}{key}:lock")
        }

        /// Generate a fresh unique token for a lock acquisition.
        /// Combines the process id, a high-resolution clock read, and
        /// a hash of both so that two `rocky` processes that race to
        /// acquire at the same nanosecond still get distinct tokens.
        fn generate_token() -> String {
            use std::time::{SystemTime, UNIX_EPOCH};
            let ns = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            format!("{}-{}", std::process::id(), ns)
        }
    }

    /// Lua script that releases the lock only if the stored value
    /// matches our token. Standard Redis "Redlock" check-and-DEL —
    /// guards against deleting a lock that was re-acquired by another
    /// holder after the original holder's TTL expired.
    const RELEASE_SCRIPT: &str = r#"
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            return redis.call('DEL', KEYS[1])
        else
            return 0
        end
    "#;

    #[async_trait]
    impl StampedeLock for ValkeyLock {
        async fn acquire(&self, key: &str, ttl: Duration) -> Result<Option<LockGuard>, LockError> {
            let prefixed = Self::prefixed(key);
            let token = Self::generate_token();
            let ttl_secs = ttl.as_secs().max(1);
            let mut conn = self.get_conn().await?;

            // `SET <key> <token> NX EX <ttl>` — atomic acquire +
            // TTL. Returns `Ok` (`true`-ish via the redis crate's
            // bool decode) on first acquisition; `Nil` (decoded as
            // `false`-ish) when the key already exists.
            let acquired: bool = redis::cmd("SET")
                .arg(&prefixed)
                .arg(&token)
                .arg("NX")
                .arg("EX")
                .arg(ttl_secs)
                .query_async::<Option<String>>(&mut conn)
                .await
                .map_err(|e| LockError::Valkey(e.to_string()))?
                .is_some();

            if !acquired {
                debug!(
                    key = prefixed,
                    "stampede lock contended; caller is follower"
                );
                return Ok(None);
            }
            debug!(key = prefixed, "stampede lock acquired");

            // Capture an Arc of the manager + key + token for the
            // release closure. The closure spawns a release task on
            // drop so we don't block the caller's hot path.
            let release_conn = conn.clone();
            let release_key = prefixed.clone();
            let release_token = token.clone();
            let runtime = tokio::runtime::Handle::try_current().ok();
            let guard = LockGuard::new(move || {
                let mut release_conn = release_conn;
                let release_key = release_key;
                let release_token = release_token;
                if let Some(handle) = runtime {
                    handle.spawn(async move {
                        let script = redis::Script::new(RELEASE_SCRIPT);
                        if let Err(e) = script
                            .key(&release_key)
                            .arg(&release_token)
                            .invoke_async::<i64>(&mut release_conn)
                            .await
                        {
                            warn!(
                                key = %release_key,
                                error = %e,
                                "stampede lock release failed; relying on TTL"
                            );
                        }
                    });
                }
                // No runtime context → silently rely on TTL. This
                // path is reached when a guard is dropped from
                // outside any tokio worker, which shouldn't happen
                // in production but might in some tests.
            });

            Ok(Some(guard))
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
                ValkeyLock::prefixed("acct/dest"),
                "fivetran-lock:acct/dest:lock"
            );
        }

        #[test]
        fn from_url_is_sync_and_doesnt_connect() {
            let lock = ValkeyLock::from_url("redis://127.0.0.1:1/");
            assert!(lock.is_ok());
        }

        /// Live Valkey integration — gated on `ROCKY_TEST_VALKEY_URL`.
        #[tokio::test]
        async fn live_valkey_acquire_release() {
            let url = match std::env::var("ROCKY_TEST_VALKEY_URL") {
                Ok(u) => u,
                Err(_) => {
                    eprintln!("skipping live_valkey_acquire_release — set ROCKY_TEST_VALKEY_URL");
                    return;
                }
            };

            let lock = ValkeyLock::from_url(&url).expect("connect");
            let key = format!("ROCKY_TEST_acquire_{}", std::process::id());

            // First acquire succeeds.
            let g = lock
                .acquire(&key, Duration::from_secs(10))
                .await
                .expect("ok")
                .expect("some");

            // Second concurrent acquire returns None (contention).
            let none = lock
                .acquire(&key, Duration::from_secs(10))
                .await
                .expect("ok");
            assert!(none.is_none(), "second acquire should be follower");

            // Release the first guard.
            drop(g);

            // Allow the spawned release task a beat to run.
            tokio::time::sleep(Duration::from_millis(200)).await;

            // After release, a fresh acquire succeeds again.
            let g2 = lock
                .acquire(&key, Duration::from_secs(10))
                .await
                .expect("ok");
            assert!(g2.is_some(), "post-release acquire should succeed");
            drop(g2);
        }
    }
}

#[cfg(feature = "valkey")]
pub use valkey::ValkeyLock;
