//! Object-store state cache backend.
//!
//! [`ObjectStoreCache`] persists each cached envelope as a single object
//! at `<prefix>/<key>.json` in an S3 / GCS / Azure / local-filesystem
//! bucket — anything the `object_store` crate can target via
//! [`object_store::parse_url`]. Credentials come from the SDK's default
//! provider chain (`AWS_*` env vars, `GOOGLE_APPLICATION_CREDENTIALS`,
//! IAM role on the host, etc.) — Rocky deliberately doesn't reinvent
//! that surface.
//!
//! ## Size cap — single-part PUT only
//!
//! S3 multi-part uploads produce an ETag that is NOT the MD5 of the
//! object body — it's `MD5(MD5_of_part_1 || MD5_of_part_2 || ...)-<N>`.
//! That makes ETag-based dedupe unsafe for multi-part objects.
//! [`MAX_ENVELOPE_BYTES`] caps the JSON payload at the 5 MB threshold
//! below which S3 always uses a single-part PUT (and where the ETag is
//! the simple MD5). Real envelopes for a 57-connector tenant land
//! around 60-120 KB, so the cap is comfortably above the steady-state
//! size and a defense in depth rather than a tight bound.
//!
//! ## Hash-dedupe via HEAD + ETag
//!
//! Before writing, the backend issues a HEAD against the target key.
//! If the response carries an `ETag` matching `md5_hex(new_bytes)` the
//! write becomes a no-op ([`WriteOutcome::SkippedNoChange`]) and no PUT
//! goes over the wire. On `NotFound` (no prior object) the backend
//! falls through to an unconditional PUT. ETag-string quoting handling:
//! S3 ships ETags as `"abc123..."` (with literal double-quote
//! characters); the comparison strips those before hashing.
//!
//! ## Fail-open
//!
//! The trait contract is "errors propagate; caller fails open" — see
//! the module-level note on the state-cache mod.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use md5::{Digest, Md5};
use object_store::{ObjectStore, path::Path as ObjectPath};
use tracing::debug;
use url::Url;

use super::{CacheError, FivetranStateCache, WriteOutcome};
use crate::envelope::FivetranStateEnvelope;

/// Upper bound on serialized envelope size we'll write through the
/// object-store backend. Past this we'd fall into multi-part PUT
/// territory where the ETag is no longer a plain MD5; see the
/// module-level "Size cap" note.
pub const MAX_ENVELOPE_BYTES: usize = 5 * 1024 * 1024;

/// Object-store state cache backend.
pub struct ObjectStoreCache {
    /// Underlying `object_store` instance — kept behind `Arc` so the
    /// backend can be cloned cheaply (the trait surface is
    /// `Arc<dyn FivetranStateCache>`).
    store: Arc<dyn ObjectStore>,
    /// Static key prefix inside the bucket. `parse_url` interprets the
    /// URL path as the prefix (e.g. `s3://my-bucket/rocky/fv/` →
    /// prefix `rocky/fv`).
    prefix: ObjectPath,
    /// URL scheme tag for `Debug` output and log messages — keeps the
    /// raw bucket / credentials out of any log line.
    scheme: String,
}

impl std::fmt::Debug for ObjectStoreCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStoreCache")
            .field("scheme", &self.scheme)
            .field("prefix", &self.prefix.to_string())
            .finish_non_exhaustive()
    }
}

impl ObjectStoreCache {
    /// Construct an [`ObjectStoreCache`] from a URL the `object_store`
    /// crate understands.
    ///
    /// Supported schemes:
    /// - `s3://bucket/prefix` — AWS S3
    /// - `gs://bucket/prefix` — Google Cloud Storage
    /// - `az://container/prefix` — Azure Blob Storage
    /// - `file:///path/prefix` — local filesystem (for tests and dev)
    pub fn from_url(url: &str) -> Result<Self, CacheError> {
        let parsed =
            Url::parse(url).map_err(|e| CacheError::ObjectStoreUrl(format!("{url}: {e}")))?;
        let scheme = parsed.scheme().to_string();
        let (store, prefix) = object_store::parse_url(&parsed)
            .map_err(|e| CacheError::ObjectStoreUrl(format!("{url}: {e}")))?;
        Ok(ObjectStoreCache {
            store: Arc::from(store),
            prefix,
            scheme,
        })
    }

    /// Construct directly from a parsed [`ObjectStore`] + prefix.
    /// Test-only / advanced — production paths come through
    /// [`Self::from_url`].
    ///
    /// `#[doc(hidden)]` because the public surface is the URL-based
    /// constructor; this one exists for integration tests that bring
    /// their own `LocalFileSystem` instance.
    #[doc(hidden)]
    pub fn from_parts(store: Arc<dyn ObjectStore>, prefix: ObjectPath, scheme: &str) -> Self {
        ObjectStoreCache {
            store,
            prefix,
            scheme: scheme.to_string(),
        }
    }

    /// Resolve the per-key object path: `<prefix>/<key>.json`.
    fn object_path(&self, key: &str) -> ObjectPath {
        let mut p = self.prefix.clone();
        for segment in key.split('/') {
            // `Path::child` re-encodes each segment, so we don't have
            // to escape `account_hash/destination_id` ourselves.
            p = p.child(segment);
        }
        // We can't use `set_extension` on `object_store::Path`. Build a
        // new Path from the stringified form to add `.json`.
        let with_suffix = format!("{p}.json");
        ObjectPath::from(with_suffix)
    }
}

/// Compute the lowercase-hex MD5 of `bytes` — matches the
/// single-part-PUT ETag S3 + S3-compatible stores produce, modulo the
/// surrounding quotes that the ETag header carries.
fn md5_hex(bytes: &[u8]) -> String {
    let digest = Md5::digest(bytes);
    let mut out = String::with_capacity(32);
    for b in digest.iter() {
        use std::fmt::Write;
        // The `Result` is infallible for `String::write_fmt`.
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// Strip the optional ETag wrapping. S3 ships ETags as `"abc..."` —
/// literal `"` characters at both ends; some S3-compatibles also strip
/// the quotes; the prefix `W/` indicates a "weak" ETag (RFC 7232 §2.3)
/// which we don't see in practice from S3 but is parseable.
fn normalize_etag(etag: &str) -> &str {
    etag.trim_start_matches("W/").trim_matches('"')
}

#[async_trait]
impl FivetranStateCache for ObjectStoreCache {
    async fn read(&self, key: &str) -> Result<Option<FivetranStateEnvelope>, CacheError> {
        let path = self.object_path(key);
        let result = match self.store.get(&path).await {
            Ok(r) => r,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(CacheError::ObjectStore(e)),
        };
        let bytes = result.bytes().await?;
        let envelope: FivetranStateEnvelope = serde_json::from_slice(&bytes)?;
        Ok(Some(envelope))
    }

    async fn write(
        &self,
        key: &str,
        envelope: &FivetranStateEnvelope,
    ) -> Result<WriteOutcome, CacheError> {
        let bytes = serde_json::to_vec(envelope)?;
        if bytes.len() > MAX_ENVELOPE_BYTES {
            return Err(CacheError::Config(format!(
                "fivetran envelope is {} bytes; object-store backend caps single-part PUT at {} \
                 (multi-part S3 ETags aren't simple MD5 and would break hash-dedupe)",
                bytes.len(),
                MAX_ENVELOPE_BYTES
            )));
        }

        let new_md5 = md5_hex(&bytes);
        let path = self.object_path(key);

        // HEAD-then-conditional-PUT dedupe: if the existing object's
        // ETag matches our locally-computed MD5, the bytes are
        // identical and we skip the PUT. Any HEAD failure other than
        // `NotFound` propagates — the caller fails open.
        match self.store.head(&path).await {
            Ok(meta) => {
                if let Some(etag) = meta.e_tag.as_deref()
                    && normalize_etag(etag).eq_ignore_ascii_case(&new_md5)
                {
                    debug!(
                        key,
                        path = %path,
                        "fivetran object_store cache: hash-dedupe matched ETag, skipping PUT"
                    );
                    return Ok(WriteOutcome::SkippedNoChange);
                }
            }
            Err(object_store::Error::NotFound { .. }) => {
                // No prior object — fall through to unconditional PUT.
            }
            Err(e) => return Err(CacheError::ObjectStore(e)),
        }

        self.store.put(&path, Bytes::from(bytes).into()).await?;
        Ok(WriteOutcome::Written)
    }

    fn backend(&self) -> &'static str {
        "object_store"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn md5_hex_round_trip() {
        // The MD5 of "hello" is a well-known fixture.
        assert_eq!(md5_hex(b"hello"), "5d41402abc4b2a76b9719d911017c592");
        // The empty-input MD5 — sanity check the empty payload path.
        assert_eq!(md5_hex(b""), "d41d8cd98f00b204e9800998ecf8427e");
    }

    #[test]
    fn etag_strip_quoting() {
        assert_eq!(normalize_etag("\"abc123\""), "abc123");
        // Already unquoted — pass through.
        assert_eq!(normalize_etag("abc123"), "abc123");
        // Weak ETag prefix.
        assert_eq!(normalize_etag("W/\"abc123\""), "abc123");
        // Empty.
        assert_eq!(normalize_etag(""), "");
    }

    #[test]
    fn max_envelope_bytes_is_below_s3_multipart_threshold() {
        // S3 single-part PUT goes up to 5 GiB. Multi-part *upload*
        // becomes a separate API (`CreateMultipartUpload`...) once you
        // exceed the part-size threshold (default 5 MiB). Past 5 MiB
        // the ETag stops being a simple MD5 of the body. Pin the
        // boundary so a refactor can't silently raise it past the safe
        // dedupe ceiling.
        assert_eq!(MAX_ENVELOPE_BYTES, 5 * 1024 * 1024);
    }

    #[test]
    fn from_url_rejects_unparseable() {
        let err = ObjectStoreCache::from_url("not a url").unwrap_err();
        assert!(matches!(err, CacheError::ObjectStoreUrl(_)));
    }

    #[test]
    fn from_url_accepts_file_scheme() {
        let tmp = tempfile::tempdir().unwrap();
        // `parse_url` accepts `file://` for local filesystem-backed tests.
        let url = format!("file://{}/", tmp.path().display());
        let cache = ObjectStoreCache::from_url(&url).expect("file:// URL must parse");
        assert_eq!(cache.backend(), "object_store");
    }

    #[test]
    fn object_path_segments_with_slashes() {
        let tmp = tempfile::tempdir().unwrap();
        let url = format!("file://{}/prefix/", tmp.path().display());
        let cache = ObjectStoreCache::from_url(&url).unwrap();
        // The key contains a `/` from `<account_hash>/<destination_id>` —
        // both segments must show up in the object path.
        let path = cache.object_path("acct_hash/dest_xyz");
        let s = path.to_string();
        assert!(s.ends_with("acct_hash/dest_xyz.json"), "got {s}");
    }
}
