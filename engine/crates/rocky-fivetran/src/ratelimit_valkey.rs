//! Valkey-backed cross-pod rate-limit budget (FR-B Phase 2).
//!
//! Sits alongside the file-backed [`FileBudget`](crate::ratelimit::FileBudget)
//! and implements the same [`RatelimitBudget`] trait so the rest of
//! the adapter can dispatch without caring which backend was wired.
//!
//! ## Key shape
//!
//! Every account's wake-at lives at
//! `<KEY_PREFIX><account_hash>` — namespaced so cluster operators can
//! prefix-scan (`KEYS fivetran-ratelimit:*`) without colliding with
//! other Rocky Redis users. The stored value is the `wake_at` epoch
//! milliseconds as a decimal string; the parser tolerates whitespace
//! so a future change to embed JSON stays backward-compatible.
//!
//! ## Concurrent writes
//!
//! Naive `SET` allows a stale T1 to land after a fresh T2 and
//! regress `wake_at`. To match the file backend's "later wins"
//! semantics across pods, [`ValkeyBudget::set_wake_at`] uses an EVAL
//! Lua script that runs atomically on the Valkey server:
//!
//! ```text
//! local current = tonumber(redis.call('GET', KEYS[1])) or 0
//! local incoming = tonumber(ARGV[1])
//! if incoming > current then
//!     redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
//! end
//! return 1
//! ```
//!
//! The `EX` cap (`max_wake_seconds`, default 600s) ensures an
//! abandoned wake_at value disappears on its own rather than
//! persisting forever in the cluster.
//!
//! ## Fail-open
//!
//! Every operation surfaces [`BudgetError::Valkey`] on transport
//! failure. The caller in [`client.rs`](crate::client) traps these,
//! logs at `warn!`, and falls back to per-host [`FileBudget`].

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use redis::AsyncCommands;
use tokio::sync::OnceCell;
use tracing::debug;

use crate::ratelimit::{BudgetError, RatelimitBudget};

/// Redis key prefix for every Fivetran rate-limit entry. Mirrors the
/// `fivetran-state:` namespace used by [`ValkeyCache`](crate::state_cache::valkey::ValkeyCache).
const KEY_PREFIX: &str = "fivetran-ratelimit:";

/// Cross-pod shared rate-limit budget backed by Valkey / Redis.
///
/// Gated behind the `valkey` Cargo feature — same posture as the
/// state-cache Valkey backend so the default build doesn't pull the
/// Redis client transitively.
pub struct ValkeyBudget {
    client: redis::Client,
    conn: OnceCell<redis::aio::ConnectionManager>,
    /// Cap on the Valkey `EX <seconds>` set on each write. Bounds how
    /// long a stale wake_at lives after a crashed peer.
    max_wake: Duration,
}

impl std::fmt::Debug for ValkeyBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValkeyBudget")
            .field("max_wake_secs", &self.max_wake.as_secs())
            .field("connected", &self.conn.initialized())
            .finish_non_exhaustive()
    }
}

impl ValkeyBudget {
    /// Open a Valkey / Redis client for `url`. Synchronous and
    /// non-blocking — the TCP / TLS handshake is deferred until the
    /// first request. URL parse errors surface here as
    /// [`BudgetError::Valkey`]; connect errors surface on the first
    /// budget read / write.
    pub fn from_url(url: &str, max_wake: Duration) -> Result<Self, BudgetError> {
        let client = redis::Client::open(url).map_err(|e| BudgetError::Valkey(e.to_string()))?;
        Ok(ValkeyBudget {
            client,
            conn: OnceCell::new(),
            max_wake,
        })
    }

    async fn get_conn(&self) -> Result<redis::aio::ConnectionManager, BudgetError> {
        let conn = self
            .conn
            .get_or_try_init(|| async {
                redis::aio::ConnectionManager::new(self.client.clone())
                    .await
                    .map_err(|e| BudgetError::Valkey(e.to_string()))
            })
            .await?;
        Ok(conn.clone())
    }

    fn prefixed(account_hash: &str) -> String {
        format!("{KEY_PREFIX}{account_hash}")
    }
}

#[async_trait]
impl RatelimitBudget for ValkeyBudget {
    async fn wake_at(&self, account_hash: &str) -> Result<Option<SystemTime>, BudgetError> {
        let mut conn = self.get_conn().await?;
        let key = Self::prefixed(account_hash);
        let raw: Option<String> = conn
            .get(&key)
            .await
            .map_err(|e| BudgetError::Valkey(e.to_string()))?;
        match raw.and_then(|s| s.trim().parse::<u64>().ok()) {
            Some(ms) => Ok(UNIX_EPOCH.checked_add(Duration::from_millis(ms))),
            None => Ok(None),
        }
    }

    async fn set_wake_at(
        &self,
        account_hash: &str,
        wake_at: SystemTime,
    ) -> Result<(), BudgetError> {
        let ms = match wake_at.duration_since(UNIX_EPOCH) {
            Ok(d) => d.as_millis().min(u64::MAX as u128) as u64,
            Err(_) => return Ok(()), // pre-epoch — ignore (matches file backend)
        };
        let key = Self::prefixed(account_hash);
        let ttl_secs = self.max_wake.as_secs().max(1);
        let mut conn = self.get_conn().await?;

        // Atomic max-merge via Lua EVAL so concurrent writers don't
        // regress wake_at.
        const MAX_MERGE_SCRIPT: &str = r#"
            local current = tonumber(redis.call('GET', KEYS[1])) or 0
            local incoming = tonumber(ARGV[1])
            if incoming > current then
                redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
            end
            return 1
        "#;
        let _: i64 = redis::Script::new(MAX_MERGE_SCRIPT)
            .key(&key)
            .arg(ms)
            .arg(ttl_secs)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| BudgetError::Valkey(e.to_string()))?;
        debug!(key, wake_at_epoch_ms = ms, "valkey ratelimit set");
        Ok(())
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
            ValkeyBudget::prefixed("abc123"),
            "fivetran-ratelimit:abc123"
        );
    }

    #[test]
    fn from_url_is_sync_and_doesnt_connect() {
        let budget = ValkeyBudget::from_url("redis://127.0.0.1:1/", Duration::from_secs(60));
        assert!(budget.is_ok(), "from_url must not connect: {budget:?}");
    }

    /// Live Valkey integration test — gated on `ROCKY_TEST_VALKEY_URL`.
    /// Mirrors the `ROCKY_TEST_*` pattern used by the other live
    /// integration tests.
    #[tokio::test]
    async fn live_valkey_max_merge() {
        let url = match std::env::var("ROCKY_TEST_VALKEY_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!(
                    "skipping live_valkey_max_merge — set ROCKY_TEST_VALKEY_URL to a Valkey/Redis URL to enable"
                );
                return;
            }
        };

        let budget =
            ValkeyBudget::from_url(&url, Duration::from_secs(60)).expect("from_url must succeed");
        let hash = format!("ROCKY_TEST_max_merge_{}", std::process::id());

        let later = SystemTime::now() + Duration::from_secs(60);
        let earlier = SystemTime::now() + Duration::from_secs(10);

        budget.set_wake_at(&hash, later).await.expect("set later");
        // Writing an earlier value must not regress the wake_at.
        budget
            .set_wake_at(&hash, earlier)
            .await
            .expect("set earlier");

        let observed = budget.wake_at(&hash).await.expect("read").expect("present");
        let delta = if observed > later {
            observed.duration_since(later).unwrap()
        } else {
            later.duration_since(observed).unwrap()
        };
        assert!(
            delta < Duration::from_secs(2),
            "expected later wake_at to win, got delta {delta:?}"
        );
    }
}
