//! Tiered (primary + secondary) state cache composition.
//!
//! [`TieredCache`] wraps two backends and gives the client a single
//! `FivetranStateCache` surface that reads from the primary first,
//! falls through to the secondary on miss, and write-backs to the
//! primary on a secondary hit so a subsequent read hits primary fast.
//!
//! The canonical configuration is `primary = Valkey`, `secondary =
//! ObjectStore`:
//!
//! - **Primary (Valkey)** — sub-millisecond reads, the hot path for
//!   sensors and sync detection that need fresh-ish data fast. Bounded
//!   by TTL; can be transiently unavailable.
//! - **Secondary (ObjectStore)** — durable, cross-process, survives
//!   primary outages and pod restarts. Writes here are guaranteed.
//!
//! ## Read path
//!
//! 1. `primary.read(key)` → on `Some` return.
//! 2. On `None`: `secondary.read(key)`.
//! 3. On secondary `Some`: best-effort write-back to primary so the
//!    next read is fast. Errors from the write-back are logged at
//!    `warn!` and not propagated — the secondary value is still
//!    correct to return.
//! 4. On both `None`: return `None`.
//!
//! ## Write path
//!
//! 1. `secondary.write(key, &env)` — durable layer first. Propagates
//!    on failure (the caller can fail open, but we can't pretend a
//!    secondary write succeeded when it didn't).
//! 2. `primary.write(key, &env)` — best-effort, errors are logged at
//!    `warn!` and not propagated.
//!
//! If the secondary returns `SkippedNoChange` (the bytes are
//! identical), the primary write still goes through unconditionally —
//! the primary may have evicted the entry under TTL, and we want a
//! fresh entry there for the next read.

use std::sync::Arc;

use async_trait::async_trait;
use tracing::warn;

use super::{CacheError, FivetranStateCache, WriteOutcome};
use crate::envelope::FivetranStateEnvelope;

/// Composing wrapper around two [`FivetranStateCache`] backends.
#[derive(Debug, Clone)]
pub struct TieredCache {
    /// Fast / volatile layer (typically Valkey). Reads tried first;
    /// writes happen after the secondary persists.
    primary: Arc<dyn FivetranStateCache>,
    /// Durable / slow layer (typically object_store). Reads on
    /// primary miss; writes always succeed here first.
    secondary: Arc<dyn FivetranStateCache>,
}

impl TieredCache {
    /// Compose two backends. `primary` is queried first on read; both
    /// receive writes (secondary first).
    pub fn new(
        primary: Arc<dyn FivetranStateCache>,
        secondary: Arc<dyn FivetranStateCache>,
    ) -> Self {
        TieredCache { primary, secondary }
    }
}

#[async_trait]
impl FivetranStateCache for TieredCache {
    async fn read(&self, key: &str) -> Result<Option<FivetranStateEnvelope>, CacheError> {
        match self.primary.read(key).await {
            Ok(Some(env)) => Ok(Some(env)),
            Ok(None) => {
                let from_secondary = self.secondary.read(key).await?;
                if let Some(ref env) = from_secondary
                    && let Err(e) = self.primary.write(key, env).await
                {
                    warn!(
                        key,
                        backend = self.primary.backend(),
                        error = %e,
                        "fivetran tiered cache: secondary hit but primary write-back failed (ignored)"
                    );
                }
                Ok(from_secondary)
            }
            Err(primary_err) => {
                // Primary errored — fall through to secondary rather
                // than failing the whole read. This is what the
                // fail-open semantics give us: a wedged primary
                // doesn't take the durable layer down with it.
                warn!(
                    key,
                    backend = self.primary.backend(),
                    error = %primary_err,
                    "fivetran tiered cache: primary read errored; falling through to secondary"
                );
                self.secondary.read(key).await
            }
        }
    }

    async fn write(
        &self,
        key: &str,
        envelope: &FivetranStateEnvelope,
    ) -> Result<WriteOutcome, CacheError> {
        let secondary_outcome = self.secondary.write(key, envelope).await?;

        // Primary write is best-effort — a wedged primary must not
        // block a durable secondary write that already happened.
        if let Err(e) = self.primary.write(key, envelope).await {
            warn!(
                key,
                backend = self.primary.backend(),
                error = %e,
                "fivetran tiered cache: primary write failed (secondary persisted; ignoring)"
            );
        }

        Ok(secondary_outcome)
    }

    fn backend(&self) -> &'static str {
        "tiered"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::{BTreeMap, HashMap};
    use std::sync::Mutex;

    use chrono::{DateTime, Utc};

    use crate::envelope::{
        FivetranConnectorStatus, FivetranConnectorSummary, FivetranDestination,
        FivetranSchemaConfig, FivetranStateEnvelope,
    };

    /// In-memory mock backend for composition tests. Records every
    /// read/write so assertions can observe the dispatch order.
    #[derive(Debug, Default)]
    struct MockCache {
        store: Mutex<HashMap<String, FivetranStateEnvelope>>,
        reads: Mutex<Vec<String>>,
        writes: Mutex<Vec<String>>,
        tag: &'static str,
    }

    impl MockCache {
        fn new(tag: &'static str) -> Self {
            MockCache {
                store: Mutex::new(HashMap::new()),
                reads: Mutex::new(Vec::new()),
                writes: Mutex::new(Vec::new()),
                tag,
            }
        }

        fn insert(&self, key: &str, env: FivetranStateEnvelope) {
            self.store.lock().unwrap().insert(key.to_string(), env);
        }

        fn read_count(&self) -> usize {
            self.reads.lock().unwrap().len()
        }

        fn write_count(&self) -> usize {
            self.writes.lock().unwrap().len()
        }
    }

    #[async_trait]
    impl FivetranStateCache for MockCache {
        async fn read(&self, key: &str) -> Result<Option<FivetranStateEnvelope>, CacheError> {
            self.reads.lock().unwrap().push(key.to_string());
            Ok(self.store.lock().unwrap().get(key).cloned())
        }

        async fn write(
            &self,
            key: &str,
            envelope: &FivetranStateEnvelope,
        ) -> Result<WriteOutcome, CacheError> {
            self.writes.lock().unwrap().push(key.to_string());
            self.store
                .lock()
                .unwrap()
                .insert(key.to_string(), envelope.clone());
            Ok(WriteOutcome::Written)
        }

        fn backend(&self) -> &'static str {
            self.tag
        }
    }

    fn sample_envelope() -> FivetranStateEnvelope {
        FivetranStateEnvelope::from_parts(
            DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap(),
            FivetranDestination {
                id: "dest_x".into(),
                region: None,
                time_zone: None,
                service: None,
                setup_status: None,
            },
            vec![FivetranConnectorSummary {
                id: "conn_a".into(),
                name: "conn_a".into(),
                schema: "src__a__b__shopify".into(),
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
        )
    }

    #[tokio::test]
    async fn primary_hit_short_circuits_secondary() {
        let primary = Arc::new(MockCache::new("primary"));
        let secondary = Arc::new(MockCache::new("secondary"));
        primary.insert("k", sample_envelope());

        let tiered = TieredCache::new(primary.clone(), secondary.clone());
        let env = tiered.read("k").await.unwrap().unwrap();
        assert_eq!(env, sample_envelope());

        assert_eq!(primary.read_count(), 1, "primary read once");
        assert_eq!(
            secondary.read_count(),
            0,
            "secondary must NOT be touched on primary hit"
        );
    }

    #[tokio::test]
    async fn primary_miss_falls_through_to_secondary() {
        let primary = Arc::new(MockCache::new("primary"));
        let secondary = Arc::new(MockCache::new("secondary"));
        secondary.insert("k", sample_envelope());

        let tiered = TieredCache::new(primary.clone(), secondary.clone());
        let env = tiered.read("k").await.unwrap().unwrap();
        assert_eq!(env, sample_envelope());

        assert_eq!(primary.read_count(), 1);
        assert_eq!(secondary.read_count(), 1);
    }

    #[tokio::test]
    async fn secondary_hit_triggers_write_back_to_primary() {
        let primary = Arc::new(MockCache::new("primary"));
        let secondary = Arc::new(MockCache::new("secondary"));
        secondary.insert("k", sample_envelope());

        let tiered = TieredCache::new(primary.clone(), secondary.clone());
        tiered.read("k").await.unwrap();

        // The write-back lands on the primary so a subsequent read is
        // fast — pin the count so a refactor can't silently remove it.
        assert_eq!(primary.write_count(), 1);
        assert!(
            primary.store.lock().unwrap().contains_key("k"),
            "write-back must populate primary"
        );
    }

    #[tokio::test]
    async fn both_miss_returns_none() {
        let primary = Arc::new(MockCache::new("primary"));
        let secondary = Arc::new(MockCache::new("secondary"));

        let tiered = TieredCache::new(primary, secondary);
        assert!(tiered.read("nope").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn write_goes_to_both_layers() {
        let primary = Arc::new(MockCache::new("primary"));
        let secondary = Arc::new(MockCache::new("secondary"));

        let tiered = TieredCache::new(primary.clone(), secondary.clone());
        tiered.write("k", &sample_envelope()).await.unwrap();

        assert_eq!(primary.write_count(), 1);
        assert_eq!(secondary.write_count(), 1);
    }

    /// A `MockCache` variant whose `write` returns
    /// `SkippedNoChange` so tests can pin the `TieredCache`
    /// outcome-propagation contract.
    #[derive(Debug, Default)]
    struct SkipWriteCache {
        writes: Mutex<Vec<String>>,
        tag: &'static str,
    }

    impl SkipWriteCache {
        fn new(tag: &'static str) -> Self {
            SkipWriteCache {
                writes: Mutex::new(Vec::new()),
                tag,
            }
        }

        fn write_count(&self) -> usize {
            self.writes.lock().unwrap().len()
        }
    }

    #[async_trait]
    impl FivetranStateCache for SkipWriteCache {
        async fn read(&self, _key: &str) -> Result<Option<FivetranStateEnvelope>, CacheError> {
            Ok(None)
        }

        async fn write(
            &self,
            key: &str,
            _envelope: &FivetranStateEnvelope,
        ) -> Result<WriteOutcome, CacheError> {
            self.writes.lock().unwrap().push(key.to_string());
            Ok(WriteOutcome::SkippedNoChange)
        }

        fn backend(&self) -> &'static str {
            self.tag
        }
    }

    /// Pin the outcome-propagation contract: when secondary returns
    /// `SkippedNoChange` (hash-dedupe hit) the tiered wrapper propagates
    /// that outcome to the caller, but primary still receives the write
    /// unconditionally (it may have evicted the entry under TTL).
    #[tokio::test]
    async fn write_propagates_secondary_skipped_outcome_but_still_writes_primary() {
        let primary = Arc::new(MockCache::new("primary"));
        let secondary = Arc::new(SkipWriteCache::new("secondary"));

        let tiered = TieredCache::new(primary.clone(), secondary.clone());
        let outcome = tiered.write("k", &sample_envelope()).await.unwrap();

        assert_eq!(
            outcome,
            WriteOutcome::SkippedNoChange,
            "tiered must propagate secondary's SkippedNoChange"
        );
        assert_eq!(secondary.write_count(), 1);
        assert_eq!(
            primary.write_count(),
            1,
            "primary must receive the write even when secondary deduped"
        );
    }

    #[tokio::test]
    async fn backend_tag_is_tiered() {
        let primary = Arc::new(MockCache::new("primary"));
        let secondary = Arc::new(MockCache::new("secondary"));
        let tiered = TieredCache::new(primary, secondary);
        assert_eq!(tiered.backend(), "tiered");
    }
}
