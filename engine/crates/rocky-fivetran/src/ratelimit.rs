//! Per-host shared Fivetran rate-limit budget (FR-B).
//!
//! Several `rocky` processes can run concurrently on the same host (e.g.
//! a sensor fan-out triggering several `rocky run` invocations within
//! seconds). Each process talks to the same Fivetran org and shares the
//! same per-org rate-limit budget upstream, but, without coordination,
//! each one independently re-discovers throttling by issuing a request
//! and getting a 429 back. Once one process has been told to back off
//! by Fivetran's `Retry-After` header, the others should observe the
//! same wake-up window before issuing fresh requests.
//!
//! The coordination layer is a per-account JSON file in a shared
//! directory (`${TMPDIR}/rocky-fivetran-ratelimit/<hash>.json`) that
//! holds a single `wake_at_epoch_ms` field. Writes are advisory-locked
//! via `fs4`; reads are lock-free (last-write-wins is good enough for
//! a coordination layer — see fail-open semantics below).
//!
//! ## Fail-open semantics
//!
//! Every I/O error in this module is treated as "no shared state, fall
//! through to the normal request path." A bug or permission glitch in
//! the coordination file MUST NOT block live Fivetran traffic — the
//! worst-case fallback is the per-process retry/backoff loop that has
//! shipped since v0, which already handles 429 itself.
//!
//! ## Account scoping
//!
//! The path is keyed by a short SHA-256 truncation of the Fivetran API
//! key. Two distinct orgs sharing the same host get distinct files;
//! the same org on the same host shares one file regardless of the
//! `rocky-cli` invocation. Hashing prevents the raw key from ever
//! landing on disk.

use std::fs::{File, OpenOptions, create_dir_all};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use fs4::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tracing::{debug, trace, warn};

/// Default subdirectory under `TMPDIR` for the shared rate-limit state.
const RATELIMIT_DIR_NAME: &str = "rocky-fivetran-ratelimit";

/// Hard cap on how long [`pre_request_wait`] will block in a single call.
/// Defends against a buggy or stale `wake_at` value parking a request
/// indefinitely.
const MAX_OBSERVED_WAIT: Duration = Duration::from_secs(300);

/// JSON envelope persisted to the per-account state file. Only a single
/// field today, but the wrapper lets future fields (e.g. last-429 ts,
/// request counters) be added compatibly with `#[serde(default)]`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct WakeAtRecord {
    wake_at_epoch_ms: u64,
}

/// Hash a credential to a short stable identifier suitable for
/// filenames and OTLP attributes. Returns 16 hex chars (8 bytes of
/// SHA-256), which is enough to make collisions vanishingly unlikely
/// on a single host while keeping the path short.
///
/// Same input always produces the same output, so two `rocky run`
/// processes that share an org key share the same coordination file.
pub fn hash_account_id(api_key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(api_key.as_bytes());
    let digest = hasher.finalize();
    let mut out = String::with_capacity(16);
    for byte in &digest[..8] {
        use std::fmt::Write;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Resolve the directory that holds per-account rate-limit state files.
/// Honors `TMPDIR` on platforms that set it, falls back to
/// [`std::env::temp_dir`] elsewhere. The directory is created lazily
/// — callers don't need to `mkdir -p` it themselves.
pub fn default_ratelimit_dir() -> PathBuf {
    std::env::temp_dir().join(RATELIMIT_DIR_NAME)
}

/// Build the per-account JSON path under `dir`. Caller is expected to
/// have passed `hash_account_id(api_key)` as `account_hash`.
pub fn account_state_path(dir: &Path, account_hash: &str) -> PathBuf {
    dir.join(format!("{account_hash}.json"))
}

/// Read the wake-up timestamp from `path`. Returns `None` on any I/O,
/// JSON-parse, or sanity-check failure — fail-open is the contract.
fn read_wake_at(path: &Path) -> Option<SystemTime> {
    let mut buf = String::new();
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) => {
            trace!(path = %path.display(), error = %e, "ratelimit state missing/unreadable; no shared budget");
            return None;
        }
    };
    if let Err(e) = file.read_to_string(&mut buf) {
        warn!(path = %path.display(), error = %e, "ratelimit state read failed; ignoring");
        return None;
    }
    let record: WakeAtRecord = match serde_json::from_str(&buf) {
        Ok(r) => r,
        Err(e) => {
            warn!(path = %path.display(), error = %e, "ratelimit state corrupt; ignoring");
            return None;
        }
    };
    UNIX_EPOCH.checked_add(Duration::from_millis(record.wake_at_epoch_ms))
}

/// Write `wake_at` to `path` atomically and under an advisory lock.
/// Errors are logged and swallowed — a failed coordination write must
/// not surface to the caller.
fn write_wake_at(path: &Path, wake_at: SystemTime) {
    let dir = match path.parent() {
        Some(d) => d,
        None => {
            warn!(path = %path.display(), "ratelimit state path has no parent; skipping write");
            return;
        }
    };
    if let Err(e) = create_dir_all(dir) {
        warn!(dir = %dir.display(), error = %e, "ratelimit dir mkdir failed; skipping write");
        return;
    }

    let epoch_ms = match wake_at.duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis().min(u64::MAX as u128) as u64,
        Err(_) => {
            warn!("ratelimit wake_at predates UNIX_EPOCH; skipping write");
            return;
        }
    };

    let record = WakeAtRecord {
        wake_at_epoch_ms: epoch_ms,
    };
    let payload = match serde_json::to_vec(&record) {
        Ok(b) => b,
        Err(e) => {
            warn!(error = %e, "ratelimit record serialize failed; skipping write");
            return;
        }
    };

    // tmp + rename keeps readers that race with us from observing a
    // half-written file. The advisory lock on the temp handle prevents
    // two writers from both renaming over the same target.
    let tmp_path = path.with_extension("json.tmp");
    let tmp = match OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp_path)
    {
        Ok(f) => f,
        Err(e) => {
            warn!(tmp_path = %tmp_path.display(), error = %e, "ratelimit tmp open failed; skipping write");
            return;
        }
    };
    // Use explicit-trait form so we resolve to fs4 1.x's `FileExt::lock`
    // rather than the std `File::lock` that was stabilised in Rust
    // 1.89 — the workspace MSRV is 1.88, so we can't rely on the std
    // method existing on every build host.
    if let Err(e) = FileExt::lock(&tmp) {
        warn!(tmp_path = %tmp_path.display(), error = %e, "ratelimit tmp lock failed; skipping write");
        let _ = std::fs::remove_file(&tmp_path);
        return;
    }
    {
        let mut handle = &tmp;
        if let Err(e) = handle.write_all(&payload) {
            warn!(tmp_path = %tmp_path.display(), error = %e, "ratelimit tmp write failed; skipping write");
            let _ = FileExt::unlock(&tmp);
            let _ = std::fs::remove_file(&tmp_path);
            return;
        }
        if let Err(e) = handle.flush() {
            warn!(tmp_path = %tmp_path.display(), error = %e, "ratelimit tmp flush failed; skipping write");
            let _ = FileExt::unlock(&tmp);
            let _ = std::fs::remove_file(&tmp_path);
            return;
        }
    }
    if let Err(e) = std::fs::rename(&tmp_path, path) {
        warn!(path = %path.display(), error = %e, "ratelimit rename failed; leaving tmp in place");
        let _ = FileExt::unlock(&tmp);
        let _ = std::fs::remove_file(&tmp_path);
        return;
    }
    let _ = FileExt::unlock(&tmp);
    debug!(path = %path.display(), wake_at_epoch_ms = epoch_ms, "ratelimit wake_at persisted");
}

/// Async pre-request wait. If the shared state file holds a future
/// `wake_at`, sleep until it elapses; otherwise return immediately.
///
/// Capped at [`MAX_OBSERVED_WAIT`] so a stale file (or a buggy peer
/// that wrote a giant value) can't park forever.
///
/// Errors fall through silently (fail-open) — see module-level docs.
pub async fn pre_request_wait(path: &Path) {
    let Some(wake_at) = read_wake_at(path) else {
        return;
    };
    let now = SystemTime::now();
    let Ok(remaining) = wake_at.duration_since(now) else {
        // Already in the past.
        return;
    };
    if remaining.is_zero() {
        return;
    }
    let bounded = remaining.min(MAX_OBSERVED_WAIT);
    debug!(
        path = %path.display(),
        wait_ms = bounded.as_millis() as u64,
        "observing shared rate-limit budget"
    );
    tokio::time::sleep(bounded).await;
}

/// Persist a new wake-up window. Idempotent against re-writes that
/// would shorten the window: the larger of the existing and new
/// `wake_at` wins, so a 30s observation followed by a 10s observation
/// keeps the 30s window in place.
pub fn record_wake_at(path: &Path, wake_at: SystemTime) {
    let effective = match read_wake_at(path) {
        Some(existing) if existing > wake_at => existing,
        _ => wake_at,
    };
    write_wake_at(path, effective);
}

/// Default cap on the Valkey backend's `EXPIRE` for an abandoned
/// `wake_at` value (10 minutes). Matches the upstream throttle window
/// typical for Fivetran's documented Retry-After values; long enough
/// to bridge a 429 burst, short enough that a stale value disappears
/// on its own.
pub const DEFAULT_MAX_WAKE_SECONDS: u64 = 600;

/// Errors surfaced by [`RatelimitBudget`] implementations.
///
/// Coarse-grained on purpose — the budget is fail-open at every call
/// site, so the caller's only useful response is `warn!` + fall back
/// to a permissive default.
#[derive(Debug, Error)]
pub enum BudgetError {
    /// Local filesystem I/O error (file backend).
    #[error("ratelimit I/O: {0}")]
    Io(#[from] std::io::Error),
    /// JSON serialize / deserialize failure on the persisted state.
    #[error("ratelimit serialize: {0}")]
    Serialize(#[from] serde_json::Error),
    /// Valkey / Redis transport failure.
    #[error("ratelimit valkey: {0}")]
    Valkey(String),
    /// Misconfigured backend — surfaced from `build_ratelimit_budget`
    /// rather than from a request.
    #[error("ratelimit config: {0}")]
    Config(String),
}

/// Cross-process shared rate-limit budget.
///
/// `wake_at` is the upper-bound SystemTime that every observer (every
/// rocky-cli process that shares the budget) should refuse to issue a
/// fresh Fivetran request before. Implementations are responsible for
/// deduping concurrent writes such that the *larger* of the existing
/// and new `wake_at` wins — a brief 100ms `Retry-After` should never
/// shorten an in-flight 30s window.
#[async_trait]
pub trait RatelimitBudget: Send + Sync + std::fmt::Debug {
    /// Read the current wake-at for `account_hash`. Returns `Ok(None)`
    /// on a clean miss; `Err(_)` on transport failure.
    async fn wake_at(&self, account_hash: &str) -> Result<Option<SystemTime>, BudgetError>;
    /// Persist `wake_at` for `account_hash`, max-merging with any
    /// existing value (later wins).
    async fn set_wake_at(&self, account_hash: &str, wake_at: SystemTime)
    -> Result<(), BudgetError>;
    /// Stable backend tag for log + OTLP attributes.
    fn backend(&self) -> &'static str;
}

/// Per-host file-backed budget. Wraps the original FR-B Phase 1 file
/// code in a [`RatelimitBudget`] trait object so the cross-pod
/// Valkey backend (FR-B Phase 2) can sit alongside it.
#[derive(Debug, Clone)]
pub struct FileBudget {
    dir: PathBuf,
}

impl FileBudget {
    /// Construct a budget rooted at `dir`. The directory is created
    /// lazily on first write.
    pub fn new(dir: PathBuf) -> Self {
        FileBudget { dir }
    }

    /// Default location under `${TMPDIR}/rocky-fivetran-ratelimit/`.
    pub fn default_dir() -> Self {
        FileBudget::new(default_ratelimit_dir())
    }

    fn path(&self, account_hash: &str) -> PathBuf {
        account_state_path(&self.dir, account_hash)
    }
}

#[async_trait]
impl RatelimitBudget for FileBudget {
    async fn wake_at(&self, account_hash: &str) -> Result<Option<SystemTime>, BudgetError> {
        // The original file backend is sync + fail-open; lift its
        // result into the async trait surface without losing the
        // fail-open contract — a missing / corrupt file still maps
        // to `Ok(None)`. The sync read runs on the blocking pool so a
        // slow filesystem (e.g. a networked TMPDIR) can't park the
        // executor worker this request shares with every other future.
        let path = self.path(account_hash);
        match tokio::task::spawn_blocking(move || read_wake_at(&path)).await {
            Ok(wake_at) => Ok(wake_at),
            Err(e) => {
                warn!(error = %e, "ratelimit read task failed; fail-open");
                Ok(None)
            }
        }
    }

    async fn set_wake_at(
        &self,
        account_hash: &str,
        wake_at: SystemTime,
    ) -> Result<(), BudgetError> {
        // `record_wake_at` is sync + fail-open and takes a *blocking*
        // advisory file lock (`fs4`) that can wait on a peer process —
        // it must run on the blocking pool, not an executor worker.
        let path = self.path(account_hash);
        if let Err(e) = tokio::task::spawn_blocking(move || record_wake_at(&path, wake_at)).await {
            warn!(error = %e, "ratelimit write task failed; fail-open");
        }
        Ok(())
    }

    fn backend(&self) -> &'static str {
        "file"
    }
}

/// Async pre-request wait against any [`RatelimitBudget`].
///
/// Returns immediately on a missing entry, a past `wake_at`, or any
/// backend error (fail-open). Capped at [`MAX_OBSERVED_WAIT`] so a
/// stale value can't park forever.
pub async fn pre_request_wait_budget(budget: &dyn RatelimitBudget, account_hash: &str) {
    let wake_at = match budget.wake_at(account_hash).await {
        Ok(Some(w)) => w,
        Ok(None) => return,
        Err(e) => {
            trace!(error = %e, "ratelimit budget read failed; fail-open");
            return;
        }
    };
    let now = SystemTime::now();
    let Ok(remaining) = wake_at.duration_since(now) else {
        return;
    };
    if remaining.is_zero() {
        return;
    }
    let bounded = remaining.min(MAX_OBSERVED_WAIT);
    debug!(
        wait_ms = bounded.as_millis() as u64,
        backend = budget.backend(),
        "observing shared rate-limit budget"
    );
    tokio::time::sleep(bounded).await;
}

/// Persist a wake-at via any [`RatelimitBudget`]. Backend errors are
/// logged at `warn!` and swallowed (fail-open).
pub async fn record_wake_at_budget(
    budget: &dyn RatelimitBudget,
    account_hash: &str,
    wake_at: SystemTime,
) {
    if let Err(e) = budget.set_wake_at(account_hash, wake_at).await {
        warn!(
            error = %e,
            backend = budget.backend(),
            "ratelimit budget write failed; fail-open"
        );
    }
}

/// Construct the rate-limit budget backend described by `config`.
/// Returns an [`Arc<dyn RatelimitBudget>`] so the caller can clone
/// cheaply. Defaults to [`FileBudget::default_dir`] when `config` is
/// `None`.
///
/// # Errors
///
/// - [`BudgetError::Config`] — the configured backend is missing a
///   required field (e.g. `valkey_url` for `backend = "valkey"`).
/// - [`BudgetError::Valkey`] — the Redis client refused the URL.
pub fn build_ratelimit_budget(
    config: Option<&rocky_core::config::FivetranRatelimitConfig>,
) -> Result<std::sync::Arc<dyn RatelimitBudget>, BudgetError> {
    use rocky_core::config::FivetranRatelimitBackend;
    let Some(cfg) = config else {
        return Ok(std::sync::Arc::new(FileBudget::default_dir()));
    };
    match cfg.backend {
        FivetranRatelimitBackend::File => Ok(std::sync::Arc::new(FileBudget::default_dir())),
        #[cfg(feature = "valkey")]
        FivetranRatelimitBackend::Valkey => {
            let url = cfg.valkey_url.as_ref().ok_or_else(|| {
                BudgetError::Config(
                    "backend = \"valkey\" requires `valkey_url` in [adapter.<name>.ratelimit]"
                        .into(),
                )
            })?;
            let max_wake =
                Duration::from_secs(cfg.max_wake_seconds.unwrap_or(DEFAULT_MAX_WAKE_SECONDS));
            let budget = crate::ratelimit_valkey::ValkeyBudget::from_url(url, max_wake)?;
            Ok(std::sync::Arc::new(budget))
        }
        #[cfg(not(feature = "valkey"))]
        FivetranRatelimitBackend::Valkey => Err(BudgetError::Config(
            "backend = \"valkey\" requires building rocky-fivetran with the `valkey` feature"
                .into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn hash_account_id_is_stable() {
        assert_eq!(hash_account_id("key-1"), hash_account_id("key-1"));
        assert_ne!(hash_account_id("key-1"), hash_account_id("key-2"));
        assert_eq!(hash_account_id("key-1").len(), 16);
    }

    #[test]
    fn hash_account_id_is_hex() {
        let h = hash_account_id("anything");
        assert!(h.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn read_wake_at_missing_file_is_none() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("does-not-exist.json");
        assert!(read_wake_at(&path).is_none());
    }

    #[test]
    fn read_wake_at_corrupt_file_is_none() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("corrupt.json");
        std::fs::write(&path, "not-json").unwrap();
        assert!(read_wake_at(&path).is_none());
    }

    #[test]
    fn write_then_read_round_trips() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("acct.json");
        let target = SystemTime::now() + Duration::from_secs(30);
        write_wake_at(&path, target);
        let read = read_wake_at(&path).expect("should read");
        // Round-trip is millisecond-accurate.
        let delta = if read > target {
            read.duration_since(target).unwrap()
        } else {
            target.duration_since(read).unwrap()
        };
        assert!(delta < Duration::from_millis(2));
    }

    #[test]
    fn record_wake_at_keeps_later_value() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("acct.json");
        let later = SystemTime::now() + Duration::from_secs(60);
        let earlier = SystemTime::now() + Duration::from_secs(10);
        record_wake_at(&path, later);
        record_wake_at(&path, earlier);
        let observed = read_wake_at(&path).unwrap();
        // The earlier write should not have shortened the window.
        let delta = if observed > later {
            observed.duration_since(later).unwrap()
        } else {
            later.duration_since(observed).unwrap()
        };
        assert!(
            delta < Duration::from_millis(2),
            "expected later wake_at to win"
        );
    }

    // The "returns quickly" bounds below are 2s, not tens of ms: they only
    // need to distinguish "returned without observing a wait" from "slept
    // toward a future wake_at" (proven at 250ms granularity by
    // `pre_request_wait_blocks_until_future_wake_at`). A tight bound races
    // scheduling + filesystem stalls on a loaded CI runner.

    #[tokio::test]
    async fn pre_request_wait_returns_quickly_on_missing_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("missing.json");
        let start = std::time::Instant::now();
        pre_request_wait(&path).await;
        assert!(start.elapsed() < Duration::from_secs(2));
    }

    #[tokio::test]
    async fn pre_request_wait_returns_quickly_for_past_wake_at() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("past.json");
        let past = SystemTime::now() - Duration::from_secs(10);
        write_wake_at(&path, past);
        let start = std::time::Instant::now();
        pre_request_wait(&path).await;
        assert!(start.elapsed() < Duration::from_secs(2));
    }

    #[tokio::test]
    async fn pre_request_wait_blocks_until_future_wake_at() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("future.json");
        let wait = Duration::from_millis(250);
        write_wake_at(&path, SystemTime::now() + wait);
        let start = std::time::Instant::now();
        pre_request_wait(&path).await;
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(200),
            "expected at least 200ms wait, got {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_millis(1500),
            "expected wait to complete promptly, got {elapsed:?}"
        );
    }
}
