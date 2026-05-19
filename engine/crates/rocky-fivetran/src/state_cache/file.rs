//! Local-filesystem state cache backend.
//!
//! [`FileCache`] persists each cached envelope as a single JSON file
//! under `<root>/<key>.json`. The key contains a `/` segment
//! (`<account_hash>/<destination_id>`) so a single root naturally
//! organizes per-account subdirectories — no flat-namespace risk of
//! two orgs colliding on the same destination_id.
//!
//! ## Atomicity
//!
//! Writes go via tmp+rename with `fsync` on the tmp file so a process
//! crash mid-write doesn't leave a half-rendered JSON for the next
//! reader. The tmp lives next to the target so the rename stays on the
//! same filesystem (cross-fs `rename(2)` would silently degrade to
//! copy+unlink and lose atomicity).
//!
//! ## Hash-dedupe
//!
//! Before writing the tmp, the backend reads any existing target,
//! deserializes, and compares the envelope hash. When the hashes match
//! it returns [`WriteOutcome::SkippedNoChange`] without touching the
//! filesystem — both the bytes and the mtime stay stable, so downstream
//! `stat(2)` watchers don't fire on cold-start herds. Read failures
//! during dedupe fall through to "no prior value; write unconditionally"
//! rather than propagating, on the same fail-open principle as the rest
//! of the cache layer.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tracing::debug;

use super::{CacheError, FivetranStateCache, WriteOutcome};
use crate::envelope::{FivetranStateEnvelope, envelope_hash};

/// Local-filesystem state cache backend.
#[derive(Debug, Clone)]
pub struct FileCache {
    /// Root directory under which `<key>.json` files are laid out.
    root: PathBuf,
}

impl FileCache {
    /// Construct a [`FileCache`] rooted at `root`.
    ///
    /// The directory is created lazily on first write — `new` doesn't
    /// touch the filesystem so a buggy config doesn't fail Rocky's
    /// startup on a directory we may never actually write to.
    pub fn new(root: PathBuf) -> Result<Self, CacheError> {
        Ok(FileCache { root })
    }

    /// Resolve the on-disk path for `key`. `<root>/<key>.json` with all
    /// the parent directories created as needed.
    fn path_for(&self, key: &str) -> PathBuf {
        let mut p = self.root.clone();
        // `key` is a forward-slash-separated logical path
        // (`<account_hash>/<destination_id>`). Splitting on `/` and
        // re-joining with `Path::push` is the portable way to turn
        // that into a real path on Windows + Unix.
        for segment in key.split('/') {
            p.push(segment);
        }
        p.set_extension("json");
        p
    }

    /// Best-effort: try to read and deserialize the prior cached value
    /// at `path`. Failures collapse to `None` so [`Self::write`] falls
    /// through to "no prior value; write unconditionally" rather than
    /// propagating a read error from the dedupe path.
    async fn read_prior(path: &Path) -> Option<FivetranStateEnvelope> {
        let bytes = match fs::read(path).await {
            Ok(b) => b,
            Err(e) => {
                if e.kind() != std::io::ErrorKind::NotFound {
                    debug!(path = %path.display(), error = %e, "fivetran filecache: prior-read failed during dedupe (treating as miss)");
                }
                return None;
            }
        };
        match serde_json::from_slice(&bytes) {
            Ok(env) => Some(env),
            Err(e) => {
                debug!(path = %path.display(), error = %e, "fivetran filecache: prior-deserialize failed during dedupe (treating as miss)");
                None
            }
        }
    }
}

#[async_trait]
impl FivetranStateCache for FileCache {
    async fn read(&self, key: &str) -> Result<Option<FivetranStateEnvelope>, CacheError> {
        let path = self.path_for(key);
        let bytes = match fs::read(&path).await {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(CacheError::Io(e)),
        };
        let envelope: FivetranStateEnvelope = serde_json::from_slice(&bytes)?;
        Ok(Some(envelope))
    }

    async fn write(
        &self,
        key: &str,
        envelope: &FivetranStateEnvelope,
    ) -> Result<WriteOutcome, CacheError> {
        let path = self.path_for(key);

        // Hash-dedupe: if the prior cached value hashes identically,
        // skip the write so mtime + bytes stay stable.
        if let Some(prior) = Self::read_prior(&path).await
            && envelope_hash(&prior) == envelope_hash(envelope)
        {
            return Ok(WriteOutcome::SkippedNoChange);
        }

        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).await?;
        }

        let tmp = path.with_extension("json.tmp");
        let bytes = serde_json::to_vec(envelope)?;

        {
            // Drop the file handle before the rename so the fsync
            // ordering is well-defined on all platforms.
            let mut f = fs::File::create(&tmp).await?;
            f.write_all(&bytes).await?;
            f.flush().await?;
            f.sync_all().await?;
        }

        // Atomic on Unix, best-effort atomic on Windows (filesystem-dependent).
        // We tolerate failure to remove a half-written tmp on the unhappy
        // path — the next write will overwrite it.
        if let Err(e) = fs::rename(&tmp, &path).await {
            let _ = fs::remove_file(&tmp).await;
            return Err(CacheError::Io(e));
        }

        Ok(WriteOutcome::Written)
    }

    fn backend(&self) -> &'static str {
        "file"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;
    use std::time::Duration;

    use chrono::{DateTime, Utc};

    use crate::envelope::{
        FivetranConnectorStatus, FivetranConnectorSummary, FivetranDestination,
        FivetranSchemaConfig, FivetranStateEnvelope,
    };

    fn sample_envelope_with_id(id: &str) -> FivetranStateEnvelope {
        FivetranStateEnvelope::from_parts(
            DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap(),
            FivetranDestination {
                id: id.into(),
                region: Some("us-east-1".into()),
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
    async fn read_returns_none_when_key_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = FileCache::new(tmp.path().to_path_buf()).unwrap();
        let value = cache.read("missing/key").await.unwrap();
        assert!(value.is_none());
    }

    #[tokio::test]
    async fn write_then_read_round_trip() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = FileCache::new(tmp.path().to_path_buf()).unwrap();
        let env = sample_envelope_with_id("dest_a");
        let outcome = cache.write("acct/dest_a", &env).await.unwrap();
        assert_eq!(outcome, WriteOutcome::Written);

        let back = cache.read("acct/dest_a").await.unwrap().unwrap();
        assert_eq!(back, env);
    }

    #[tokio::test]
    async fn double_write_of_same_envelope_returns_skipped() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = FileCache::new(tmp.path().to_path_buf()).unwrap();
        let env = sample_envelope_with_id("dest_a");
        cache.write("acct/dest_a", &env).await.unwrap();

        // Sleep so any second-granularity mtime can move if we did
        // actually rewrite the file.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let path = cache.path_for("acct/dest_a");
        let mtime_before = std::fs::metadata(&path).unwrap().modified().unwrap();

        // Different fetched_at but same upstream state — hash excludes
        // fetched_at, so the dedupe must short-circuit.
        let mut env_b = env.clone();
        env_b.fetched_at = DateTime::<Utc>::from_timestamp(1_900_000_000, 0).unwrap();
        let outcome = cache.write("acct/dest_a", &env_b).await.unwrap();
        assert_eq!(outcome, WriteOutcome::SkippedNoChange);

        let mtime_after = std::fs::metadata(&path).unwrap().modified().unwrap();
        assert_eq!(
            mtime_before, mtime_after,
            "dedupe must leave the on-disk mtime unchanged"
        );
    }

    #[tokio::test]
    async fn write_with_changed_envelope_rewrites() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = FileCache::new(tmp.path().to_path_buf()).unwrap();
        let env_a = sample_envelope_with_id("dest_a");
        cache.write("acct/dest_a", &env_a).await.unwrap();

        // Different destination metadata => different envelope_hash =>
        // actual write.
        let mut env_b = env_a.clone();
        env_b.destination.region = Some("eu-west-1".into());
        let outcome = cache.write("acct/dest_a", &env_b).await.unwrap();
        assert_eq!(outcome, WriteOutcome::Written);

        let back = cache.read("acct/dest_a").await.unwrap().unwrap();
        assert_eq!(back.destination.region, Some("eu-west-1".into()));
    }

    #[tokio::test]
    async fn write_creates_nested_directories() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = FileCache::new(tmp.path().to_path_buf()).unwrap();
        let env = sample_envelope_with_id("dest_a");
        cache
            .write("deep/nested/account_hash/dest_a", &env)
            .await
            .unwrap();

        let expected = tmp
            .path()
            .join("deep")
            .join("nested")
            .join("account_hash")
            .join("dest_a.json");
        assert!(expected.exists(), "nested write must create parent dirs");
    }

    #[tokio::test]
    async fn write_leaves_no_dangling_tmp_on_success() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = FileCache::new(tmp.path().to_path_buf()).unwrap();
        let env = sample_envelope_with_id("dest_a");
        cache.write("acct/dest_a", &env).await.unwrap();

        // No `.json.tmp` siblings — the rename consumed the tmp.
        let mut entries = fs::read_dir(tmp.path().join("acct")).await.unwrap();
        while let Some(entry) = entries.next_entry().await.unwrap() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            assert!(
                !name_str.ends_with(".tmp"),
                "tmp file left dangling: {name_str}"
            );
        }
    }

    #[tokio::test]
    async fn read_of_corrupt_file_returns_err() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = FileCache::new(tmp.path().to_path_buf()).unwrap();
        let path = cache.path_for("acct/dest_x");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"this is not JSON").unwrap();

        let result = cache.read("acct/dest_x").await;
        assert!(matches!(result, Err(CacheError::Serialize(_))));
    }

    #[test]
    fn backend_tag_is_file() {
        let cache = FileCache::new(PathBuf::from("/nowhere")).unwrap();
        assert_eq!(cache.backend(), "file");
    }
}
