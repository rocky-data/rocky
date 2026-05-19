//! Pluggable state cache backends for the Fivetran adapter (FR-A).
//!
//! The cache layer sits between the in-process [`FivetranClient`] envelope
//! memoization (shipped in PR-1) and the upstream Fivetran REST API. When
//! a fresh `rocky run` boots, its [`FivetranClient`] starts cold — the
//! in-process memo is empty — so without a persistent cache every process
//! would re-discover the same destination from scratch. The cache layer
//! lets the second process find a fresh envelope from the first process's
//! discover without hitting the API.
//!
//! ## Cache key
//!
//! All backends share the same key derivation: `<account_hash>/<destination_id>`.
//! `account_hash` reuses [`ratelimit::hash_account_id`](crate::ratelimit::hash_account_id)
//! so the per-host rate-limit budget (FR-B), the multi-destination
//! emit-to-path (FR-C), and this cache layer all scope identically.
//! Destination IDs are name-shaped tokens (not globally-unique UUIDs);
//! the account hash prevents two orgs sharing a cache backend from
//! colliding on the same name.
//!
//! ## Hash-dedupe
//!
//! Writes call [`envelope_hash`](crate::envelope::envelope_hash) on the
//! new envelope and compare with the prior cached value's hash. When the
//! hashes match the write becomes a no-op ([`WriteOutcome::SkippedNoChange`]),
//! which (a) keeps downstream `stat(2)` watchers stable and (b) cuts S3
//! PUT volume on cold-start herds where many processes try to write the
//! same envelope. The hash explicitly excludes `fetched_at` (see the
//! envelope module) so two captures of the same upstream state at
//! different wall-clock instants dedupe correctly.
//!
//! ## Fail-open policy
//!
//! The client wires cache reads / writes around the HTTP path
//! (see [`FivetranClient::fetch_envelope`](crate::client::FivetranClient::fetch_envelope)).
//! Cache errors do NOT propagate — a misbehaving cache must never block
//! a live API fetch. Backends return their error type; the wiring logs
//! at `warn!` and falls through to HTTP.
//!
//! ## Backends
//!
//! - [`no_cache::NoCache`] — sentinel; reads always miss, writes always
//!   succeed without persisting. Used when `[adapter.fivetran.cache]` is
//!   absent or `backend = "none"`.
//! - [`file::FileCache`] — `<root>/<account_hash>/<destination_id>.json`
//!   with atomic tmp+rename + hash-dedupe.
//! - [`object_store_backend::ObjectStoreCache`] — S3 / GCS / Azure / local
//!   filesystem via the `object_store` crate. Single-part PUTs with
//!   ETag-MD5 hash-dedupe.
//! - [`valkey::ValkeyCache`] — Redis / Valkey, gated by the `valkey`
//!   Cargo feature.
//! - [`tiered::TieredCache`] — primary + secondary composition.

use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;

use crate::envelope::FivetranStateEnvelope;

pub mod file;
pub mod no_cache;
pub mod object_store_backend;
pub mod observability;
pub mod tiered;
#[cfg(feature = "valkey")]
pub mod valkey;

pub use file::FileCache;
pub use no_cache::NoCache;
pub use object_store_backend::ObjectStoreCache;
pub use tiered::TieredCache;
#[cfg(feature = "valkey")]
pub use valkey::ValkeyCache;

/// Outcome of a successful cache write.
///
/// `SkippedNoChange` is the hash-dedupe signal — the new envelope hashes
/// identically to the prior cached value, so the backend left the prior
/// bytes alone. Distinguishing the two outcomes lets the
/// [`observability`] layer emit different span events
/// (`fivetran.cache_write` vs `fivetran.cache_write_skipped`) so dashboards
/// can prove cold-start herds aren't generating redundant PUTs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteOutcome {
    /// The backend persisted the new envelope.
    Written,
    /// The new envelope hashed identically to the cached value; the
    /// backend left the on-cache bytes alone.
    SkippedNoChange,
}

/// Error type for cache backend operations.
///
/// Variants are intentionally coarse — the caller's only useful handling
/// is "log + fail open." Finer-grained distinctions live in the inner
/// error types when callers need to introspect.
#[derive(Debug, Error)]
pub enum CacheError {
    /// Local filesystem I/O failure ([`FileCache`]).
    #[error("cache I/O: {0}")]
    Io(#[from] std::io::Error),

    /// Object-store backend failure ([`ObjectStoreCache`]).
    #[error("cache object-store: {0}")]
    ObjectStore(#[from] object_store::Error),

    /// Object-store URL parsing failure (e.g. unsupported scheme).
    #[error("cache object-store URL: {0}")]
    ObjectStoreUrl(String),

    /// Valkey / Redis client failure ([`ValkeyCache`]).
    #[error("cache valkey: {0}")]
    Valkey(String),

    /// JSON serialize / deserialize failure (cache value codec).
    #[error("cache serialize: {0}")]
    Serialize(#[from] serde_json::Error),

    /// Misconfigured backend — missing required field, etc. Surfaced at
    /// config-load time before any cache I/O is attempted.
    #[error("cache config: {0}")]
    Config(String),
}

/// Trait every Fivetran state cache backend implements.
///
/// Implementations must be `Send + Sync + Debug` so a single
/// `Arc<dyn FivetranStateCache>` can be cheaply cloned and threaded
/// through async tasks. The trait is intentionally narrow — only the
/// behavior `FivetranClient` exercises is on the public surface; richer
/// operations (e.g. cache invalidation by prefix, multi-get) are deferred
/// until a real caller asks for them.
#[async_trait]
pub trait FivetranStateCache: Send + Sync + std::fmt::Debug {
    /// Look up the envelope keyed by `key`.
    ///
    /// Returns `Ok(None)` on a clean miss (the key doesn't exist).
    /// Returns `Err(_)` on transport / decoding failure — the caller
    /// fails open and falls through to the HTTP path.
    async fn read(&self, key: &str) -> Result<Option<FivetranStateEnvelope>, CacheError>;

    /// Persist `envelope` keyed by `key`.
    ///
    /// Returns [`WriteOutcome::SkippedNoChange`] when the backend's
    /// prior cached value hashes identically to `envelope` — see the
    /// module-level "Hash-dedupe" note. Returns [`WriteOutcome::Written`]
    /// on a fresh write. Returns `Err(_)` on transport failure.
    async fn write(
        &self,
        key: &str,
        envelope: &FivetranStateEnvelope,
    ) -> Result<WriteOutcome, CacheError>;

    /// Short tag identifying the backend ("none", "file", "object_store",
    /// "valkey", "tiered"). Surfaced as the `backend` attribute on every
    /// `fivetran.cache_*` OTLP span event so dashboards can filter by
    /// backend without parsing log messages.
    fn backend(&self) -> &'static str;
}

/// Construct the cache backend described by `config`.
///
/// Returns an [`Arc<dyn FivetranStateCache>`] so the caller can clone the
/// backend cheaply (the in-flight HTTP client holds one ref; tests hold
/// another). [`NoCache`] is the default when `config.backend == None`.
///
/// # Errors
///
/// - [`CacheError::Config`] — a backend's required field is missing
///   (e.g. `object_store_url` for `backend = "object_store"`). The
///   validation in [`FivetranCacheConfig::validate`](rocky_core::config::FivetranCacheConfig::validate)
///   should catch this at `load_rocky_config` time, but this is the
///   second-chance fallback.
/// - [`CacheError::ObjectStoreUrl`] — the configured URL didn't parse.
/// - [`CacheError::Io`] — `[file] file_root` couldn't be created.
/// - [`CacheError::Valkey`] — the Redis client refused the URL.
pub fn build_state_cache(
    config: &rocky_core::config::FivetranCacheConfig,
) -> Result<Arc<dyn FivetranStateCache>, CacheError> {
    use rocky_core::config::FivetranCacheBackend;

    match config.backend {
        FivetranCacheBackend::None => Ok(Arc::new(NoCache)),
        FivetranCacheBackend::File => {
            let root = config.file_root.as_ref().ok_or_else(|| {
                CacheError::Config(
                    "backend = \"file\" requires `file_root` in [adapter.<name>.cache]".into(),
                )
            })?;
            Ok(Arc::new(FileCache::new(root.into())?))
        }
        FivetranCacheBackend::ObjectStore => {
            let url = config.object_store_url.as_ref().ok_or_else(|| {
                CacheError::Config(
                    "backend = \"object_store\" requires `object_store_url` in [adapter.<name>.cache]"
                        .into(),
                )
            })?;
            Ok(Arc::new(ObjectStoreCache::from_url(url)?))
        }
        #[cfg(feature = "valkey")]
        FivetranCacheBackend::Valkey => {
            let url = config.valkey_url.as_ref().ok_or_else(|| {
                CacheError::Config(
                    "backend = \"valkey\" requires `valkey_url` in [adapter.<name>.cache]".into(),
                )
            })?;
            let ttl = std::time::Duration::from_secs(
                config
                    .valkey_ttl_seconds
                    .unwrap_or(DEFAULT_VALKEY_TTL_SECONDS),
            );
            Ok(Arc::new(ValkeyCache::from_url(url, ttl)?))
        }
        #[cfg(not(feature = "valkey"))]
        FivetranCacheBackend::Valkey => Err(CacheError::Config(
            "backend = \"valkey\" requires building rocky-fivetran with the `valkey` feature"
                .into(),
        )),
        FivetranCacheBackend::Tiered => {
            #[cfg(feature = "valkey")]
            {
                let valkey_url = config.valkey_url.as_ref().ok_or_else(|| {
                    CacheError::Config(
                        "backend = \"tiered\" requires `valkey_url` in [adapter.<name>.cache]"
                            .into(),
                    )
                })?;
                let object_store_url = config.object_store_url.as_ref().ok_or_else(|| {
                    CacheError::Config(
                        "backend = \"tiered\" requires `object_store_url` in [adapter.<name>.cache]"
                            .into(),
                    )
                })?;
                let ttl = std::time::Duration::from_secs(
                    config
                        .valkey_ttl_seconds
                        .unwrap_or(DEFAULT_VALKEY_TTL_SECONDS),
                );
                let primary: Arc<dyn FivetranStateCache> =
                    Arc::new(ValkeyCache::from_url(valkey_url, ttl)?);
                let secondary: Arc<dyn FivetranStateCache> =
                    Arc::new(ObjectStoreCache::from_url(object_store_url)?);
                Ok(Arc::new(TieredCache::new(primary, secondary)))
            }
            #[cfg(not(feature = "valkey"))]
            {
                Err(CacheError::Config(
                    "backend = \"tiered\" requires building rocky-fivetran with the `valkey` feature"
                        .into(),
                ))
            }
        }
    }
}

/// Default TTL for the Valkey backend when the config omits
/// `valkey_ttl_seconds`. Matches the upstream FR-A spec.
pub const DEFAULT_VALKEY_TTL_SECONDS: u64 = 600;

/// Build the canonical cache key for a `(account_hash, destination_id)`
/// pair. Exposed so callers that derive their own key (tests, the client)
/// can stay aligned with whatever scheme the backends expect.
pub fn cache_key(account_hash: &str, destination_id: &str) -> String {
    format!("{account_hash}/{destination_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_key_format_is_stable() {
        // The wire form ends up as both filesystem path components and
        // object-store keys — pin it so refactors can't silently shift
        // the encoding.
        assert_eq!(cache_key("abc123", "dest_xyz"), "abc123/dest_xyz");
    }

    #[test]
    fn build_state_cache_none_default() {
        let cfg = rocky_core::config::FivetranCacheConfig::default();
        let cache = build_state_cache(&cfg).expect("None backend always succeeds");
        assert_eq!(cache.backend(), "none");
    }
}
