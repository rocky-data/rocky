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

use fs4::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
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
    // 1.89 — the workspace MSRV is 1.85, so we can't rely on the std
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

    #[tokio::test]
    async fn pre_request_wait_returns_quickly_on_missing_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("missing.json");
        let start = std::time::Instant::now();
        pre_request_wait(&path).await;
        assert!(start.elapsed() < Duration::from_millis(50));
    }

    #[tokio::test]
    async fn pre_request_wait_returns_quickly_for_past_wake_at() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("past.json");
        let past = SystemTime::now() - Duration::from_secs(10);
        write_wake_at(&path, past);
        let start = std::time::Instant::now();
        pre_request_wait(&path).await;
        assert!(start.elapsed() < Duration::from_millis(50));
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
