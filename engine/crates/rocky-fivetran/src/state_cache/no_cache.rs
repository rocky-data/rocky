//! No-op cache backend.
//!
//! [`NoCache`] is the default when `[adapter.<name>.cache]` is absent or
//! has `backend = "none"`. It's the trivial implementation of the trait
//! and exists for two reasons:
//!
//! 1. Lets [`FivetranClient::with_state_cache`](crate::client::FivetranClient::with_state_cache)
//!    take an `Arc<dyn FivetranStateCache>` rather than an `Option`,
//!    so the cache-aware code path is the same regardless of config.
//! 2. Makes the absent-config behavior explicit: a [`NoCache`] backed
//!    client surfaces `backend = "none"` in OTLP attrs so dashboards
//!    can distinguish "no cache configured" from "cache backend errored
//!    and we fell open."
//!
//! [`NoCache::write`] returns [`WriteOutcome::Written`] rather than
//! `SkippedNoChange` so a wrapper that interprets `SkippedNoChange` as
//! "no useful work happened" doesn't mistake the no-op for a no-op-due-to-dedupe.

use async_trait::async_trait;

use super::{CacheError, FivetranStateCache, WriteOutcome};
use crate::envelope::FivetranStateEnvelope;

/// No-op cache backend.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoCache;

#[async_trait]
impl FivetranStateCache for NoCache {
    async fn read(&self, _key: &str) -> Result<Option<FivetranStateEnvelope>, CacheError> {
        Ok(None)
    }

    async fn write(
        &self,
        _key: &str,
        _envelope: &FivetranStateEnvelope,
    ) -> Result<WriteOutcome, CacheError> {
        Ok(WriteOutcome::Written)
    }

    fn backend(&self) -> &'static str {
        "none"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;

    use chrono::{DateTime, Utc};

    use crate::envelope::{
        FivetranConnectorStatus, FivetranConnectorSummary, FivetranDestination,
        FivetranSchemaConfig, FivetranStateEnvelope,
    };

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
                schema: "src__x__y__shopify".into(),
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
    async fn read_always_returns_none() {
        let cache = NoCache;
        assert!(cache.read("any/key").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn write_returns_written_not_skipped() {
        // The wrapper interprets SkippedNoChange as "hash match" — the
        // no-op backend must NOT use that variant or downstream dashboards
        // would misread the no-cache case as a hash-dedupe hit.
        let cache = NoCache;
        let outcome = cache.write("any/key", &sample_envelope()).await.unwrap();
        assert_eq!(outcome, WriteOutcome::Written);
    }

    #[test]
    fn backend_tag_is_none() {
        assert_eq!(NoCache.backend(), "none");
    }
}
