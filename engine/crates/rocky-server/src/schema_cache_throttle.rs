//! Per-document throttle for schema-cache info logs in the LSP path.
//!
//! The CLI emits a single info log per process when `load_source_schemas_from_cache`
//! returns at least one entry. That pattern would turn into per-keystroke spam in
//! the LSP: `did_change` fires a recompile on a 300 ms debounce, and each recompile
//! loads `source_schemas` from the cache. So the LSP needs a throttle keyed on
//! `(document_uri, cache_version)`.
//!
//! PR 1b (this PR) has no write tap, so there are no cache-version bumps — the
//! throttle collapses to "have I logged for this document this session?" A
//! `Mutex<HashSet<Uri>>` is enough. PR 2 will add a cache-version counter:
//! when the write tap bumps it, this module's [`ThrottleState::mark_logged`]
//! becomes "did I log for `(uri, version)` already?" — pass the new version in
//! as a second key. No external dep today (no `dashmap`): a `tokio::sync::Mutex`
//! keeps the footprint minimal.
//!
//! Put here rather than inline in `lsp.rs` so both LSP recompile sites
//! (`RockyLsp::recompile` at initialize + `did_save`, and the `did_change`
//! spawn_blocking path) share one map. Tested in isolation; behaviour is
//! purely state-machine so doesn't need a live LSP to exercise.

use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::Mutex;

/// Shared throttle for per-document schema-cache info logs.
///
/// Cheap to clone — it's just an `Arc` over the inner mutex. Each
/// `RockyLsp`/`ServerState` holds one.
#[derive(Clone, Default)]
pub(crate) struct SchemaCacheThrottle {
    logged: Arc<Mutex<HashSet<String>>>,
}

impl SchemaCacheThrottle {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Record that we have emitted the info log for `key`. Returns `true`
    /// when this is the first time the key has been seen (i.e. the caller
    /// should emit the log), `false` when we've already logged for it.
    ///
    /// `key` is the `document_uri` for PR 1b. PR 2's write tap will extend
    /// it to `"<uri>@<cache_version>"` so the log re-fires when the cache
    /// is updated.
    pub(crate) async fn mark_logged(&self, key: &str) -> bool {
        let mut set = self.logged.lock().await;
        set.insert(key.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn first_call_returns_true() {
        let t = SchemaCacheThrottle::new();
        assert!(t.mark_logged("file:///tmp/a.rocky").await);
    }

    #[tokio::test]
    async fn repeated_same_key_returns_false() {
        let t = SchemaCacheThrottle::new();
        assert!(t.mark_logged("file:///tmp/a.rocky").await);
        assert!(!t.mark_logged("file:///tmp/a.rocky").await);
        assert!(!t.mark_logged("file:///tmp/a.rocky").await);
    }

    #[tokio::test]
    async fn distinct_keys_each_log_once() {
        let t = SchemaCacheThrottle::new();
        assert!(t.mark_logged("file:///a").await);
        assert!(t.mark_logged("file:///b").await);
        // Repeats for both become no-ops.
        assert!(!t.mark_logged("file:///a").await);
        assert!(!t.mark_logged("file:///b").await);
    }

    #[tokio::test]
    async fn version_bump_pattern_re_fires() {
        // PR 2 will encode cache_version in the key. Confirm the shape
        // today so the migration is a string-format change, not a
        // semantics change.
        let t = SchemaCacheThrottle::new();
        assert!(t.mark_logged("file:///a@v1").await);
        assert!(!t.mark_logged("file:///a@v1").await);
        // New cache version -> the key is different -> log fires again.
        assert!(t.mark_logged("file:///a@v2").await);
    }
}
