//! Remote state persistence.
//!
//! Pulls / pushes the redb state file across runs so ephemeral environments
//! (EKS pods, CI jobs) can resume watermarks and anomaly history. Four
//! backends: local (no-op), S3, GCS, Valkey, or Tiered (Valkey + S3).
//!
//! S3 and GCS use the shared [`ObjectStoreProvider`][crate::object_store::ObjectStoreProvider]
//! so credential resolution follows the standard AWS SDK / GCP ADC chains.

use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use thiserror::Error;
use tracing::{Instrument, debug, info, info_span, warn};

use crate::circuit_breaker::TransitionOutcome;
use crate::config::{
    ConcurrencyControl, RetryConfig, StateBackend, StateConfig, StateUploadFailureMode,
};
use crate::object_store::{
    Generation, ObjectStoreError, ObjectStoreProvider, PutIfMatchOutcome, RemoteVersion,
};
use crate::redacted::RedactedString;
use crate::retry::compute_backoff;
use crate::retry_budget::RetryBudget;
use crate::state::StateStore;

#[derive(Debug, Error)]
pub enum StateSyncError {
    #[error("S3 download failed: {0}")]
    S3Download(String),

    #[error("S3 upload failed: {0}")]
    S3Upload(String),

    #[error("GCS download failed: {0}")]
    GcsDownload(String),

    #[error("GCS upload failed: {0}")]
    GcsUpload(String),

    #[error("Valkey error: {0}")]
    Valkey(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("state store error: {0}")]
    State(#[from] crate::state::StateError),

    #[error("state backend '{0}' requires {1} to be configured")]
    MissingConfig(String, String),

    #[error("object store error: {0}")]
    ObjectStore(#[from] ObjectStoreError),

    #[error("state transfer timed out after {0:?}")]
    Timeout(Duration),

    #[error(
        "state backend circuit breaker tripped after {consecutive_failures} consecutive transient failures"
    )]
    CircuitOpen { consecutive_failures: u32 },

    #[error("state retry budget exhausted (limit {limit}); aborting remaining retries")]
    RetryBudgetExhausted { limit: u32 },

    #[error("ledger-seam transition failed: {0}")]
    /// A [`LedgerSeamSession`] caller's transition closure failed for a
    /// domain reason (authorization refused, proof no longer holds, execution
    /// error). The session aborts WITHOUT publishing — the remote winner is
    /// untouched — and the caller re-wraps this into its own error context.
    SeamTransition(String),

    #[error(
        "state compare-and-swap conflict on '{key}': another writer committed since this run \
         downloaded state; refusing to overwrite the winner (fail-closed)"
    )]
    CasConflict { key: String },

    #[error(
        "publish conflict on environment {env:?}: expected head {}, found {}; another publish \
         moved it. Re-read the environment and publish again",
        .expected.as_deref().unwrap_or("<none: create>"),
        .found.as_deref().unwrap_or("<none>")
    )]
    /// A [`publish_pointers`] attempt found a different environment head than
    /// the caller expected — on the first attempt or on a replay after a CAS
    /// conflict. A semantic refusal: the seam aborts without uploading and
    /// puts the remote winner back locally. Retrying cannot change it.
    PublishConflict {
        env: String,
        expected: Option<String>,
        found: Option<String>,
    },

    #[error(
        "ledger-seam compare-and-swap conflict on shared state blob '{key}' after {attempts} \
         attempts: concurrent writers kept advancing it; preserving the remote winner \
         (fail-closed)"
    )]
    LedgerSeamConflict { key: String, attempts: u32 },

    #[error(
        "concurrency_control = \"cas\" is set, but the '{backend}' state store does not honour \
         conditional writes: {reason}. Refusing to run with a compare-and-swap setting the store \
         would silently ignore. Fix the store, or set concurrency_control = \"off\" and serialize \
         writers so only one writes this state at a time"
    )]
    /// `cas` was set explicitly and the startup probe proved the store ignores
    /// or rejects conditional writes.
    CasUnsupported { backend: String, reason: String },

    #[error(
        "refusing an unconditional state upload: '{marker}' exists, so other writers of this \
         state use compare-and-swap, and an unconditional upload could overwrite their committed \
         state. Fix: set [state] concurrency_control = \"cas\" (or remove the explicit \"off\") \
         on this writer. Only if every writer of this state is deliberately moving to \"off\", \
         delete the marker on purpose"
    )]
    /// An unconditional upload found the `cas-required` marker beside the
    /// state object.
    CasRequired { marker: String },

    #[error(
        "refusing to publish environment pointers through the '{backend}' state store: it does \
         not use compare-and-swap here, so two concurrent publishes could both report success \
         and one would be lost without an error. Fix: set [state] concurrency_control = \"cas\" \
         on a store that honours conditional writes (s3, gcs, or tiered), or publish against a \
         local state store"
    )]
    /// [`publish_pointers`] on a remote backend whose effective
    /// concurrency control is `off`. A lost publish is silent there, so it is
    /// refused before any download.
    PublishRequiresCas { backend: String },
}

/// State file name within the configured prefix.
const STATE_FILE: &str = "state.redb";
const DEFAULT_S3_PREFIX: &str = "rocky/state/";
const DEFAULT_GCS_PREFIX: &str = "rocky/state/";
const DEFAULT_VALKEY_PREFIX: &str = "rocky:state:";

/// Derive the remote object key (within the configured prefix) for a local
/// state file.
///
/// The local file name and the remote key are intentionally decoupled: the
/// global local file `<models>/.rocky-state.redb` maps to the fixed remote key
/// `state.redb`. When per-namespace state-file namespacing is on, the local
/// file lives under a `.rocky-state/` directory as `<namespace>.redb`; this
/// returns `<namespace>.redb` so each namespace round-trips to a distinct
/// remote object instead of every pipeline clobbering the same `state.redb`
/// (silent cross-pod state loss).
///
/// The gate is the **parent directory name**, not the local file stem: the
/// legacy global file's stem is `.rocky-state`, which must keep mapping to the
/// unchanged `state.redb` key so the namespacing-OFF path is byte-identical on
/// the wire.
fn remote_state_key(local_path: &Path) -> String {
    let is_namespaced = local_path
        .parent()
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        == Some(crate::state::STATE_NAMESPACE_DIR);
    if is_namespaced && let Some(name) = local_path.file_name().and_then(|n| n.to_str()) {
        return name.to_string();
    }
    STATE_FILE.to_string()
}

/// Schema-version path segment for remote state keys (e.g. `"v9"`).
///
/// Every remote state key is qualified by the engine's **schema** version so
/// two engine versions that disagree on the redb schema never read or write
/// each other's state through a shared tiered / object / Valkey backend. This
/// is the durable fix for the rolling-upgrade strand: during a schema-changing
/// bump, an old-binary pod resolves its keys under `vN` while already-upgraded
/// pods use `vN+1`, so they never collide.
///
/// Qualifying by **schema** version, not binary version or hash, is
/// deliberate: a patch bump that leaves the redb schema unchanged keeps the
/// same segment and so keeps sharing state (no fleet-wide watermark reset).
/// Only a schema-changing bump shifts the segment. The new version then finds
/// no object under its own key, so the download reads the newest older key
/// instead and restores it (#1955, see [`carry_forward_versions`]). The first
/// upload writes the current key; the older key is never written or deleted.
/// Only when no older key exists down to the floor does the run start fresh.
fn schema_version_segment() -> String {
    format!("v{}", crate::state::current_schema_version())
}

/// Oldest schema version a carry-forward will read (#1955).
///
/// v22 is the last schema version that added a table, so a store written at
/// v22 or later carries every table this binary reads, and the normal
/// read-write open migrates it in place by re-stamping the version. A store
/// older than that is not carried forward; the key stays absent and the
/// download is a fresh start, as before. Release 1.74.0 jumped from v23 to
/// v30, so the floor covers every key a supported upgrade can leave behind.
const CARRY_FORWARD_FLOOR_SCHEMA_VERSION: u32 = 22;

/// Older schema versions to probe, newest first, when the current-version key
/// is absent (#1955).
///
/// The search runs DOWN from `current - 1` to
/// [`CARRY_FORWARD_FLOOR_SCHEMA_VERSION`] inclusive and never up: a newer key
/// was written by a newer binary, and reading it would be a downgrade.
fn carry_forward_versions() -> Vec<u32> {
    carry_forward_versions_below(crate::state::current_schema_version())
}

/// [`carry_forward_versions`] for an explicit `current` version (unit-testable
/// without changing the compiled-in schema version).
fn carry_forward_versions_below(current: u32) -> Vec<u32> {
    (CARRY_FORWARD_FLOOR_SCHEMA_VERSION..current)
        .rev()
        .collect()
}

/// Whether a download leg may restore an older schema version's key when the
/// current-version key is absent (#1955).
///
/// Only the Valkey read takes this flag: `Yes` for the `valkey` backend, `No`
/// for the Valkey cache leg of `tiered`, where a miss falls through to the
/// durable S3 leg, which does the carry-forward itself. The object-store read
/// is always a durable leg (`s3`, `gcs`, the S3 leg of `tiered`), so it always
/// carries forward.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CarryForward {
    Yes,
    No,
}

/// Schema-qualified object-store key: `v9/state.redb` (under the configured
/// `<prefix>`). The version segment is a path component so the full object
/// path reads `<prefix>/v9/state.redb`.
fn object_store_state_key(remote_key: &str) -> String {
    object_store_state_key_at(crate::state::current_schema_version(), remote_key)
}

/// [`object_store_state_key`] for an explicit schema version. Only the
/// carry-forward READ path passes an older version; every write uses the
/// current one.
fn object_store_state_key_at(version: u32, remote_key: &str) -> String {
    format!("v{version}/{remote_key}")
}

/// Schema-qualified Valkey key: `<prefix>v9:state.redb` (e.g.
/// `rocky:state:v9:state.redb`). The version segment is colon-delimited to
/// match the Valkey prefix convention.
fn valkey_state_key(prefix: &str, remote_key: &str) -> String {
    valkey_state_key_at(prefix, crate::state::current_schema_version(), remote_key)
}

/// [`valkey_state_key`] for an explicit schema version. Only the
/// carry-forward READ path passes an older version.
fn valkey_state_key_at(prefix: &str, version: u32, remote_key: &str) -> String {
    format!("{prefix}v{version}:{remote_key}")
}

/// Build an `ObjectStoreProvider` rooted at `<scheme>://<bucket>/<prefix>`.
fn cloud_provider(
    scheme: &str,
    bucket: &str,
    prefix: &str,
) -> Result<ObjectStoreProvider, StateSyncError> {
    // Test seam: when a test has installed an in-memory provider via
    // `test_support::install`, route every object-store construction to it so
    // the real `upload_state → strip → dispatch → upload_to_object_store` path
    // can be exercised end-to-end without a live cloud client. Production
    // builds never compile this branch — the `test-support` feature exists for
    // test targets only and the `rocky` binary never enables it.
    #[cfg(any(test, feature = "test-support"))]
    if let Some(provider) = test_support::current_override() {
        return Ok(provider);
    }

    // The process-global override (installed by the cross-pod test harness) is
    // consulted AFTER the thread-local one, so a test that needs a
    // this-thread-only provider can still shadow a harness-installed global.
    // Unlike the thread-local seam, this one survives OS-thread hops (e.g.
    // rocky-cli's `block_on_state_sync` dedicated runtime thread).
    #[cfg(any(test, feature = "test-support"))]
    if let Some(provider) = test_support::current_global_override() {
        return Ok(provider);
    }

    let trimmed_prefix = prefix.trim_end_matches('/');
    let uri = if trimmed_prefix.is_empty() {
        format!("{scheme}://{bucket}")
    } else {
        format!("{scheme}://{bucket}/{trimmed_prefix}")
    };
    Ok(ObjectStoreProvider::from_uri(&uri)?)
}

/// Object-store handle for the **durable tier** of a remote `[state]`
/// backend, or `None` when no durable object tier exists (`local`,
/// `valkey`-only).
///
/// The single place the backend enum maps to a marker-capable tier for
/// [`crate::freeze_marker`]: `tiered` resolves to its S3 leg — Valkey is
/// bypassed entirely, so a stale cached blob can never shadow a durable
/// freeze marker. Bucket resolution mirrors the state download/upload
/// dispatch (`MissingConfig` when the required bucket is absent), and
/// construction goes through [`cloud_provider`] so the test-override seams
/// intercept it exactly like every other state transfer.
///
/// # Errors
///
/// [`StateSyncError::MissingConfig`] when the backend has a durable tier but
/// its bucket is not configured; provider-construction failures propagate.
pub fn durable_tier_provider(
    config: &StateConfig,
) -> Result<Option<ObjectStoreProvider>, StateSyncError> {
    match config.backend {
        StateBackend::S3 | StateBackend::Tiered => {
            let bucket = config.s3_bucket.as_deref().ok_or_else(|| {
                StateSyncError::MissingConfig("s3".into(), "state.s3_bucket".into())
            })?;
            let prefix = config.s3_prefix.as_deref().unwrap_or(DEFAULT_S3_PREFIX);
            Ok(Some(cloud_provider("s3", bucket, prefix)?))
        }
        StateBackend::Gcs => {
            let bucket = config.gcs_bucket.as_deref().ok_or_else(|| {
                StateSyncError::MissingConfig("gcs".into(), "state.gcs_bucket".into())
            })?;
            let prefix = config.gcs_prefix.as_deref().unwrap_or(DEFAULT_GCS_PREFIX);
            Ok(Some(cloud_provider("gs", bucket, prefix)?))
        }
        StateBackend::Local | StateBackend::Valkey => Ok(None),
    }
}

/// Test-only support for injecting an in-memory object store into the upload
/// dispatch path. Lets a test drive the real
/// `upload_state → strip → dispatch → upload_to_object_store → cloud_provider`
/// chain against [`ObjectStoreProvider::in_memory`] instead of a live cloud
/// client, so it observes the *actual* remote key the upload path computes
/// (the round-trip-via-`provider.upload_file` test cannot, because it
/// precomputes the key and skips dispatch entirely).
///
/// Two override seams coexist:
/// - the **thread-local** [`install`] used by this file's unit tests, and
/// - the **process-global** [`install_global`] used by integration tests
///   (re-exported via [`super::remote_testing`] under the `test-support`
///   feature), which is visible from every OS thread and is consulted only
///   when no thread-local override is installed.
#[cfg(any(test, feature = "test-support"))]
mod test_support {
    use super::ObjectStoreProvider;
    use std::cell::{Cell, RefCell};
    use std::collections::HashMap;
    use std::sync::{Mutex, MutexGuard, PoisonError};

    thread_local! {
        static PROVIDER_OVERRIDE: RefCell<Option<ObjectStoreProvider>> =
            const { RefCell::new(None) };
        /// One-shot fault: make the next object-store existence probe error.
        static OBJECT_STORE_EXISTS_FAULT: Cell<bool> = const { Cell::new(false) };
        /// One-shot fault: make the next object-store generation probe error.
        static OBJECT_STORE_HEAD_FAULT: Cell<bool> = const { Cell::new(false) };
        /// One-shot fault: make the next Valkey download report a MISS.
        static VALKEY_MISS_FAULT: Cell<bool> = const { Cell::new(false) };
        /// In-process stand-in for a Valkey peer, so the tiered coherent-cache
        /// paths (GET / SET / DEL) can be driven without a live server.
        static VALKEY_FAKE: RefCell<Option<HashMap<String, Vec<u8>>>> =
            const { RefCell::new(None) };
        /// One-shot fault: make the next coherent-cache SET fail.
        static VALKEY_SET_FAULT: Cell<bool> = const { Cell::new(false) };
        /// One-shot fault: make the next coherent-cache DEL fail.
        static VALKEY_DEL_FAULT: Cell<bool> = const { Cell::new(false) };
    }

    /// Arm a one-shot fault so the next `probe_exists` returns `Err`. Consumed
    /// (cleared) on read so it can't leak into a later leg/test on this thread.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn arm_object_store_exists_fault() {
        OBJECT_STORE_EXISTS_FAULT.with(|c| c.set(true));
    }

    /// Read-and-clear the object-store existence fault.
    pub(super) fn take_object_store_exists_fault() -> bool {
        OBJECT_STORE_EXISTS_FAULT.with(|c| c.replace(false))
    }

    /// Arm a one-shot fault so the next `probe_generation` returns `Err`. Kept
    /// distinct from the existence fault: the tiered coherent read probes the
    /// generation and then may still delegate to the durable leg's existence
    /// probe, and a test must be able to fault exactly one of them.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn arm_object_store_head_fault() {
        OBJECT_STORE_HEAD_FAULT.with(|c| c.set(true));
    }

    /// Read-and-clear the object-store generation-probe fault.
    pub(super) fn take_object_store_head_fault() -> bool {
        OBJECT_STORE_HEAD_FAULT.with(|c| c.replace(false))
    }

    /// Arm a one-shot fault so the next `download_from_valkey` reports a MISS
    /// (`Absent`) without touching a live Valkey peer.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn arm_valkey_miss_fault() {
        VALKEY_MISS_FAULT.with(|c| c.set(true));
    }

    /// Read-and-clear the Valkey-miss fault.
    pub(super) fn take_valkey_miss_fault() -> bool {
        VALKEY_MISS_FAULT.with(|c| c.replace(false))
    }

    /// Install an empty in-process Valkey stand-in on this thread. While it is
    /// installed the coherent-cache GET / SET / DEL helpers resolve against it
    /// instead of a live peer (and never resolve `state.valkey_url`).
    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn install_fake_valkey() {
        VALKEY_FAKE.with(|cell| *cell.borrow_mut() = Some(HashMap::new()));
    }

    /// Whether the in-process Valkey stand-in is installed on this thread.
    pub(super) fn fake_valkey_installed() -> bool {
        VALKEY_FAKE.with(|cell| cell.borrow().is_some())
    }

    /// Read a key from the stand-in (`None` = cache miss).
    pub(super) fn fake_valkey_get(key: &str) -> Option<Vec<u8>> {
        VALKEY_FAKE.with(|cell| cell.borrow().as_ref()?.get(key).cloned())
    }

    /// Write a key to the stand-in. `Err(())` when the one-shot SET fault is
    /// armed — the caller maps it to the same error shape a live peer would
    /// produce, and (critically) the key is left UNWRITTEN.
    pub(super) fn fake_valkey_set(key: &str, value: Vec<u8>) -> Result<(), ()> {
        if VALKEY_SET_FAULT.with(|c| c.replace(false)) {
            return Err(());
        }
        VALKEY_FAKE.with(|cell| {
            if let Some(map) = cell.borrow_mut().as_mut() {
                map.insert(key.to_string(), value);
            }
        });
        Ok(())
    }

    /// Delete a key from the stand-in. `Err(())` when the one-shot DEL fault is
    /// armed, leaving the entry in place — the state a test needs to prove that
    /// a *surviving* stale entry is still rejected on read.
    pub(super) fn fake_valkey_del(key: &str) -> Result<(), ()> {
        if VALKEY_DEL_FAULT.with(|c| c.replace(false)) {
            return Err(());
        }
        VALKEY_FAKE.with(|cell| {
            if let Some(map) = cell.borrow_mut().as_mut() {
                map.remove(key);
            }
        });
        Ok(())
    }

    /// Arm a one-shot coherent-cache SET failure.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn arm_valkey_set_fault() {
        VALKEY_SET_FAULT.with(|c| c.set(true));
    }

    /// Arm a one-shot coherent-cache DEL failure.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn arm_valkey_del_fault() {
        VALKEY_DEL_FAULT.with(|c| c.set(true));
    }

    /// Install `provider` as the next provider [`super::cloud_provider`] hands
    /// out on this thread. Returns a clone so the caller retains a handle to
    /// assert on after the upload (the in-memory store is `Arc`-backed, so the
    /// clone shares storage with the installed copy).
    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn install(provider: ObjectStoreProvider) -> ObjectStoreProvider {
        let handle = provider.clone();
        PROVIDER_OVERRIDE.with(|cell| *cell.borrow_mut() = Some(provider));
        handle
    }

    /// Clone the installed override, if any. Returns a clone (rather than
    /// removing it) so every object-store construction on this thread — e.g.
    /// both the Valkey and S3 legs of a tiered upload — sees the same
    /// `Arc`-backed store until the test calls [`clear`].
    pub(super) fn current_override() -> Option<ObjectStoreProvider> {
        PROVIDER_OVERRIDE.with(|cell| cell.borrow().clone())
    }

    /// Clear any installed override and armed faults so nothing leaks into a
    /// later test on the same worker thread.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn clear() {
        PROVIDER_OVERRIDE.with(|cell| *cell.borrow_mut() = None);
        OBJECT_STORE_EXISTS_FAULT.with(|c| c.set(false));
        OBJECT_STORE_HEAD_FAULT.with(|c| c.set(false));
        VALKEY_MISS_FAULT.with(|c| c.set(false));
        VALKEY_FAKE.with(|cell| *cell.borrow_mut() = None);
        VALKEY_SET_FAULT.with(|c| c.set(false));
        VALKEY_DEL_FAULT.with(|c| c.set(false));
    }

    /// Process-global provider override, consulted by
    /// [`super::cloud_provider`] AFTER the thread-local one.
    static GLOBAL_PROVIDER_OVERRIDE: Mutex<Option<ObjectStoreProvider>> = Mutex::new(None);

    /// Serializes tests that install the global override (see
    /// [`serial_guard`]).
    static GLOBAL_OVERRIDE_SERIAL: Mutex<()> = Mutex::new(());

    fn lock_global() -> MutexGuard<'static, Option<ObjectStoreProvider>> {
        GLOBAL_PROVIDER_OVERRIDE
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Install `provider` as the PROCESS-GLOBAL provider every
    /// [`super::cloud_provider`] call resolves to (unless a thread-local
    /// override shadows it). Returns a guard that clears the override on drop.
    ///
    /// Unlike the thread-local [`install`], the global override is visible
    /// from every OS thread — including the dedicated runtime threads
    /// rocky-cli's `block_on_state_sync` hops to — so integration tests can
    /// drive the real download/upload paths end to end.
    ///
    /// The override is process-wide shared state: tests that use it must hold
    /// [`serial_guard`] for their whole duration or they will clobber each
    /// other's provider.
    pub fn install_global(provider: ObjectStoreProvider) -> GlobalOverrideGuard {
        *lock_global() = Some(provider);
        GlobalOverrideGuard { _priv: () }
    }

    /// RAII guard returned by [`install_global`]; clears the process-global
    /// override on drop so nothing leaks into a later test.
    #[derive(Debug)]
    pub struct GlobalOverrideGuard {
        _priv: (),
    }

    impl Drop for GlobalOverrideGuard {
        fn drop(&mut self) {
            *lock_global() = None;
        }
    }

    /// Serialize tests that use [`install_global`]: the override is
    /// process-global, so two concurrently-running tests in one test binary
    /// would clobber each other's provider. Hold the returned guard for the
    /// duration of the test. Lock poisoning (a panicking test) is deliberately
    /// ignored — the next test proceeds with a clean override either way,
    /// because the panicking test's [`GlobalOverrideGuard`] already cleared it.
    pub fn serial_guard() -> SerialGuard {
        SerialGuard(
            GLOBAL_OVERRIDE_SERIAL
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        )
    }

    /// Guard returned by [`serial_guard`].
    ///
    /// Deliberately a newtype (not a bare `MutexGuard`) because tests hold it
    /// across `.await` points for their WHOLE duration — that is the entire
    /// point of the serialization — and that is sound here: contenders are
    /// other tests' dedicated OS threads blocking in `lock()`, never tasks on
    /// this test's runtime, so no async task is starved and no deadlock is
    /// possible. A bare `MutexGuard` would trip `clippy::await_holding_lock`
    /// in every consuming test file for a pattern that is intentional.
    #[derive(Debug)]
    pub struct SerialGuard(#[allow(dead_code)] MutexGuard<'static, ()>);

    /// Clone the process-global override, if any. Like [`current_override`],
    /// cloning (rather than taking) keeps every object-store construction —
    /// e.g. both legs of a tiered transfer — on the same `Arc`-backed store.
    pub(super) fn current_global_override() -> Option<ObjectStoreProvider> {
        lock_global().clone()
    }
}

/// Public remote-state testing seams, compiled only with the `test-support`
/// cargo feature (or for the crate's own tests). The `rocky` binary never
/// enables the feature, so none of this exists in a release build.
///
/// Integration tests (`rocky-core/tests/`, `rocky-cli/tests/`) cannot reach
/// the crate-private `test_support` seam, and its thread-local override is
/// invisible to code that hops OS threads. This module re-exports the
/// process-global override so out-of-crate tests can inject a shared
/// in-memory (or fault-decorated — see [`crate::fault_store`]) store into the
/// real state-sync decision paths. See [`crate::test_harness`] for the
/// ready-made cross-pod harness built on top of it.
#[cfg(any(test, feature = "test-support"))]
pub mod remote_testing {
    pub use super::test_support::{GlobalOverrideGuard, SerialGuard, install_global, serial_guard};
}

/// Resolve the per-transfer wall-clock budget from `StateConfig`.
fn transfer_timeout(config: &StateConfig) -> Duration {
    Duration::from_secs(config.transfer_timeout_seconds)
}

/// Outcome of a remote-state download leg.
///
/// The distinction is load-bearing for the **tiered** backend: a Valkey
/// [`Absent`][DownloadOutcome::Absent] (cache miss) must fall through to the
/// durable S3 tier, whereas a [`Restored`][DownloadOutcome::Restored] (the leg
/// wrote the local file) short-circuits. Deciding hit-vs-miss on an explicit
/// return value — rather than the old `local_path.exists()` heuristic — is what
/// stops a **pre-existing stale local file** from masquerading as a Valkey hit
/// and starving the S3 fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DownloadOutcome {
    /// The leg found remote state and wrote it to the local path.
    Restored,
    /// No remote state existed for this key (a legit fresh start).
    Absent,
}

/// Whether a run may trust its local state ledger to back a governed decision.
///
/// Returned by [`download_state`] on success. The variants carry the one
/// distinction every downstream governance gate needs — *authoritative vs.
/// genuinely-empty vs. don't-know*:
///
/// - a download **failure stays `Err(StateSyncError)`** — it is never mapped to
///   `Ok(Indeterminate)`. [`Indeterminate`][StateAuthority::Indeterminate] is
///   **caller-synthesized**: it exists only when a caller explicitly elects to
///   continue past a download `Err`. Keeping failure as `Err` means every
///   fail-closed `download_state(...)?` seam keeps bailing unchanged — a caller
///   must make an *explicit* choice to proceed on non-authoritative state.
#[must_use = "the download's authority decides whether the local ledger may back a governed decision"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateAuthority {
    /// The remote object existed and was restored (or the backend is Local,
    /// whose on-disk file IS the authority). The ledger reflects durable truth.
    Authoritative,
    /// No remote object existed for this key — a genuine fresh start. The
    /// ledger is trustworthy AND may bootstrap an empty ledger.
    FreshStart,
    /// A download / existence-check failure was elected-past by a caller. The
    /// ledger is NON-authoritative: an active freeze / exhausted budget
    /// recorded elsewhere may be invisible. Fail-closed.
    Indeterminate,
}

impl StateAuthority {
    /// The ledger may back a governed decision: the download restored the
    /// authoritative remote or proved a genuine fresh start.
    /// [`Indeterminate`][StateAuthority::Indeterminate] is NOT usable.
    #[must_use]
    pub fn is_usable(self) -> bool {
        matches!(self, Self::Authoritative | Self::FreshStart)
    }
}

/// How [`RemoteStateSession::finalize`] treats a failed terminal upload.
///
/// The split is by governance class, not by backend: a governed run's terminal
/// state writes (run record, verify-after custody, idempotency stamp) are part
/// of the audited decision trail, so losing the upload that carries them must
/// fail the run even when the operator left the liveness-friendly default
/// `on_upload_failure = "skip"` in place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinalizeDurability {
    /// Force `on_upload_failure = Fail` for the terminal upload, regardless of
    /// the configured mode. Used by governed runs: a lost terminal upload is a
    /// lost audit/custody mutation, so the run must exit nonzero.
    Durable,
    /// Honor the configured `[state] on_upload_failure` as-is: the default
    /// `skip` warns-and-continues (state re-derives next run), an explicit
    /// `fail` propagates. Used by ungoverned runs — a successful warehouse run
    /// must not turn nonzero on a failed state PUT unless the operator asked
    /// for exactly that.
    ConfigDefault,
}

/// The lifecycle owner for one run's remote `[state]` interaction.
///
/// Replaces the inline download/periodic/terminal-upload seams with a single
/// object that owns the whole `acquire → (periodic) → finalize | abandon`
/// lifecycle. Constructed from an **owned [`StateConfig`] snapshot** — never a
/// config path — so no seam can be redirected by a `rocky.toml` swap timed
/// after the caller's gate (execute-from-owned by construction).
///
/// # Lifecycle
///
/// - [`acquire`][Self::acquire] — download-before-read. Resolves and records
///   the [`StateAuthority`]. A download *failure* is recorded as
///   [`Indeterminate`][StateAuthority::Indeterminate] (with the error retained
///   for [`require_synced`][Self::require_synced] and the caller's fail-closed
///   messaging) and returned as `Ok` — the session is the electing caller the
///   [`download_state`] contract requires; `Err` is reserved for internal
///   misuse (double-acquire).
/// - [`start_periodic_uploader`][Self::start_periodic_uploader] /
///   [`stop_periodic`][Self::stop_periodic] — the owned mid-run uploader;
///   `stop_periodic` aborts **and joins** the task (the RD-004
///   abort-without-await fix).
/// - [`finalize`][Self::finalize] — one-shot upload-after, positioned by the
///   caller AFTER the run's terminal state writes so they ride the upload.
///   Suppressed uploads (forward-incompat recreate, indeterminate download)
///   return `Ok` without touching the remote.
/// - [`abandon`][Self::abandon] — deliberate no-upload consumption for
///   error/interrupted exits (which never uploaded, and must not start to).
///
/// # What the session does NOT own
///
/// - **The `StateStore`.** A hard lock-ordering constraint: [`download_state`]
///   internally takes the same advisory writer lock `StateStore::open` takes
///   (around its merge + atomic publish), so a session-held store would
///   contend with the download's publish. Callers open/close their store
///   around the session as today.
/// - **The `StateStore` merge lock** — see above.
///
/// # Held only under CAS
///
/// - **A `base` generation** is captured at [`acquire`][Self::acquire] and held
///   ONLY under `concurrency_control = "cas"` on a backend with a durable
///   object tier: it is the durable object's generation at download time, which
///   [`finalize`][Self::finalize] CAS-commits against so a run that lost a
///   cross-pod race fail-closes instead of erasing the winner. On `tiered` the
///   base always comes from the durable tier, never from the Valkey cache —
///   the cache only ever answers a read after proving it holds that same
///   generation. `None` on the default `off` path (an unconditional upload,
///   byte-identical to pre-CAS).
///
/// # Drop tripwire
///
/// Dropping a session that was neither finalized nor abandoned is a bug in the
/// calling run path: it `warn!`s, `debug_assert!`s, and aborts (without
/// joining — `Drop` cannot await) any live periodic task so a leaked handle
/// cannot outlive the run. The tripwire is a diagnostic + resource net, NOT a
/// durability guarantee — a panic between mutation and `finalize` still skips
/// the upload.
///
/// The one deliberate drop is `rocky run --watch` stopping an iteration on a
/// signal. It runs the iteration inside an armed [`DroppedSessionSink`], so the
/// session hands itself over and the watch loop settles its terminal upload
/// instead of tripping (#1603).
#[derive(Debug)]
pub struct RemoteStateSession {
    /// Owned config snapshot — never a path (execute-from-owned).
    cfg: StateConfig,
    /// The local ledger this session syncs.
    state_path: PathBuf,
    /// Guards double-acquire (the one internal-misuse `Err`).
    acquired: bool,
    /// Starts `Indeterminate` (fail-closed until `acquire` resolves it).
    authority: StateAuthority,
    /// The download failure `acquire` elected past, verbatim — powers
    /// `require_synced`'s message and the caller's fail-closed bail text.
    last_download_error: Option<String>,
    durability: FinalizeDurability,
    /// `Some(reason)` once an upload suppression is recorded; first reason
    /// wins. A suppressed `finalize` performs no upload.
    suppress_reason: Option<&'static str>,
    /// One-shot: set by `finalize`/`abandon`; arms the Drop tripwire while
    /// false.
    finalized: bool,
    /// The owned mid-run periodic uploader, when started.
    periodic: Option<tokio::task::JoinHandle<()>>,
    /// Cooperative-drain signal for the periodic uploader. `stop_periodic` fires
    /// it and joins, so an in-flight tick's `spawn_blocking` snapshot runs to
    /// completion (releasing its `Arc<StateStore>` upgrade and cleaning up its
    /// scratch guard) BEFORE the task exits — a plain `abort()` would instead
    /// detach the blocking closure, leaking the scratch it recreates and holding
    /// the upgrade past the run tail's `Arc::try_unwrap`.
    periodic_shutdown: Option<Arc<tokio::sync::Notify>>,
    /// The durable object's generation captured at `acquire`, held ONLY under
    /// `concurrency_control = "cas"` on a backend with a durable object tier.
    /// `finalize` CAS-commits against it. `None` on the `off` path, on the
    /// tier-less backends, or when the object was absent (bootstrap →
    /// create-if-absent).
    base: Option<Generation>,
    /// `[cache.schemas] replicate` — whether `schema_cache` travels with the
    /// replicated state (#1620). Carried explicitly rather than re-derived,
    /// so the acquire, periodic and finalize legs of one session can never
    /// disagree about the posture.
    replicate_schema_cache: bool,
}

/// Whether Rocky performs compare-and-swap state writes on `backend` at all.
///
/// The single source of truth for CAS backend capability. The runtime write
/// path gates on it through [`cas_effective`], and `rocky doctor`'s
/// `state_concurrency` check derives its verdict from the same call, so a
/// backend that gains or loses conditional-write support changes both together.
/// Re-deriving this list anywhere else is how a diagnostic ends up vouching for
/// protection that is not there — or, just as harmful, warning about a
/// deployment that is in fact protected.
///
/// `local` is `false` because it performs no remote write to condition; callers
/// that care about the single-writer-local case handle it before asking.
#[must_use]
pub fn cas_supported_on(backend: StateBackend) -> bool {
    match backend {
        // A conditional-write object tier the upload can compare against.
        StateBackend::S3 | StateBackend::Gcs => true,
        // The durable S3 leg carries the generation, and the Valkey tier is
        // kept coherent with it (see [`tiered_cas_download`] and
        // [`dispatch_upload_cas`]) — a cached copy is only usable once its
        // stored generation is confirmed to still be the durable object's.
        StateBackend::Tiered => true,
        // No generation to condition on: `cas` falls back to an unconditional
        // upload (and warns once at `acquire`).
        StateBackend::Valkey => false,
        // No remote write at all — the on-disk file IS the state.
        StateBackend::Local => false,
    }
}

/// Whether this `[state]` configuration *actually* performs compare-and-swap
/// state writes: `cas` requested (explicitly, or as the backend default when
/// `concurrency_control` is unset — see [`requested_concurrency_control`])
/// AND [`cas_supported_on`] the configured backend.
///
/// "Effective" rather than "configured" is the distinction that matters — a
/// `cas` request on a backend without conditional writes silently downgrades to
/// an unconditional upload, so the request alone proves nothing about whether
/// the deployment is protected.
///
/// This is the *static* answer. The write paths resolve the config through
/// [`resolve_concurrency_control`] first, which also consults the startup
/// conditional-write probe and pins the outcome into the config they carry, so
/// for them this predicate reads the probed result.
#[must_use]
pub fn cas_effective(cfg: &StateConfig) -> bool {
    requested_concurrency_control(cfg).0 == ConcurrencyControl::Cas && cas_supported_on(cfg.backend)
}

/// Where a writer's concurrency mode came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConcurrencySource {
    /// `concurrency_control` is set in `[state]`.
    Explicit,
    /// `concurrency_control` is unset; the backend default applies.
    Defaulted,
}

impl std::fmt::Display for ConcurrencySource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConcurrencySource::Explicit => write!(f, "explicit"),
            ConcurrencySource::Defaulted => write!(f, "default"),
        }
    }
}

/// The mode an unset `concurrency_control` defaults to on `backend`: `cas`
/// wherever Rocky performs compare-and-swap writes ([`cas_supported_on`]),
/// `off` elsewhere. Derived from the capability predicate, never re-listed, so
/// the default can never ask for a protection the backend cannot give.
///
/// `valkey` defaults to `off` because its upload is a plain `SET` with no
/// compare token — there is no conditional put to default to.
#[must_use]
pub fn default_concurrency_control(backend: StateBackend) -> ConcurrencyControl {
    if cas_supported_on(backend) {
        ConcurrencyControl::Cas
    } else {
        ConcurrencyControl::Off
    }
}

/// The requested mode and its source, before the startup probe: the explicit
/// setting when present, else [`default_concurrency_control`].
#[must_use]
pub fn requested_concurrency_control(cfg: &StateConfig) -> (ConcurrencyControl, ConcurrencySource) {
    match cfg.concurrency_control {
        Some(mode) => (mode, ConcurrencySource::Explicit),
        None => (
            default_concurrency_control(cfg.backend),
            ConcurrencySource::Defaulted,
        ),
    }
}

/// Result of the startup conditional-write probe ([`probe_conditional_writes`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CasProbeOutcome {
    /// The store honoured create-if-absent and if-match preconditions.
    Supported,
    /// The store accepted a write a precondition should have refused, or
    /// rejected conditional writes while accepting unconditional ones. The
    /// string says which.
    Unsupported(String),
    /// The probe could not finish (transport, permission, timeout), so it
    /// says nothing about support either way.
    Inconclusive(String),
}

impl std::fmt::Display for CasProbeOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CasProbeOutcome::Supported => write!(f, "supported"),
            CasProbeOutcome::Unsupported(why) => write!(f, "unsupported ({why})"),
            CasProbeOutcome::Inconclusive(why) => write!(f, "inconclusive ({why})"),
        }
    }
}

/// A writer's concurrency mode after the startup probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedConcurrency {
    /// The mode the writer runs with.
    pub mode: ConcurrencyControl,
    /// Whether the mode was set explicitly or defaulted.
    pub source: ConcurrencySource,
    /// The probe outcome, or `None` when no probe ran (`off` requested, or a
    /// backend without a conditional-write tier).
    pub probe: Option<CasProbeOutcome>,
}

/// Resolve `cfg`'s concurrency mode for a writer, probing the store once per
/// process when `cas` is requested on a backend that supports it.
///
/// | requested            | probe          | result                      |
/// |----------------------|----------------|-----------------------------|
/// | `off` (explicit)     | not run        | `off`                       |
/// | unset, no CAS tier   | not run        | `off`                       |
/// | `cas`, no CAS tier   | not run        | `off` (existing downgrade)  |
/// | `cas` / unset        | supported      | `cas`                       |
/// | unset                | unsupported    | `off`, with a warning       |
/// | `cas` (explicit)     | unsupported    | [`StateSyncError::CasUnsupported`] |
/// | `cas` / unset        | inconclusive   | `cas`, with a warning       |
///
/// An inconclusive probe keeps `cas`: falling back to an unconditional upload
/// on a guess would reopen the lost update this default exists to close. That
/// is not protection either: on a store that IGNORES conditional headers, a
/// `cas` write commits as a silent overwrite. A store that REJECTS them fails
/// the write instead. So an inconclusive probe means "unconfirmed", and the
/// writer warns; `rocky doctor` reports it as a Warning.
///
/// # Errors
///
/// [`StateSyncError::CasUnsupported`] when `cas` is set explicitly and the
/// probe proves the store does not honour conditional writes.
pub async fn resolve_concurrency_control(
    cfg: &StateConfig,
) -> Result<ResolvedConcurrency, StateSyncError> {
    let (requested, source) = requested_concurrency_control(cfg);
    if requested == ConcurrencyControl::Off || !cas_supported_on(cfg.backend) {
        if requested == ConcurrencyControl::Cas {
            warn!(
                backend = %cfg.backend,
                "concurrency_control = cas needs a durable object tier (s3, gcs, or \
                 tiered) and this backend has none; using an unconditional state upload \
                 (auto-downgraded to off)"
            );
        }
        return Ok(ResolvedConcurrency {
            mode: ConcurrencyControl::Off,
            source,
            probe: None,
        });
    }
    let probe = cached_probe_conditional_writes(cfg).await;
    let mode = match (&probe, source) {
        (CasProbeOutcome::Supported, _) => ConcurrencyControl::Cas,
        (CasProbeOutcome::Unsupported(why), ConcurrencySource::Explicit) => {
            return Err(StateSyncError::CasUnsupported {
                backend: cfg.backend.to_string(),
                reason: why.clone(),
            });
        }
        (CasProbeOutcome::Unsupported(why), ConcurrencySource::Defaulted) => {
            warn!(
                backend = %cfg.backend,
                reason = %why,
                "the state store does not honour conditional writes; concurrency_control is \
                 unset, so falling back to off (unconditional, last-writer-wins uploads). \
                 Concurrent writers sharing this state can overwrite each other. Set \
                 concurrency_control = \"off\" to silence this, or move [state] to a store \
                 that supports conditional writes"
            );
            ConcurrencyControl::Off
        }
        (CasProbeOutcome::Inconclusive(why), _) => {
            warn!(
                backend = %cfg.backend,
                reason = %why,
                "could not confirm the state store honours conditional writes; keeping \
                 concurrency_control = cas (never falling back to unconditional uploads on an \
                 inconclusive probe)"
            );
            ConcurrencyControl::Cas
        }
    };
    Ok(ResolvedConcurrency {
        mode,
        source,
        probe: Some(probe),
    })
}

/// `cfg` with its concurrency mode pinned to the resolved one, so every
/// downstream gate ([`cas_effective`], the tiered read arm) reads the probed
/// result rather than the request.
async fn resolved_state_config(cfg: &StateConfig) -> Result<StateConfig, StateSyncError> {
    let resolved = resolve_concurrency_control(cfg).await?;
    Ok(StateConfig {
        concurrency_control: Some(resolved.mode),
        ..cfg.clone()
    })
}

/// Process-wide probe cache: one probe per store location per process. Only
/// definitive outcomes are cached; an inconclusive probe is retried next time.
static CAS_PROBE_CACHE: std::sync::Mutex<
    Option<std::collections::HashMap<String, CasProbeOutcome>>,
> = std::sync::Mutex::new(None);

/// Cache key for the probe: the durable tier's scheme, bucket and prefix, plus
/// the endpoint and region the object-store client reads from the
/// environment. The same bucket name on a different endpoint (an
/// S3-compatible store beside AWS) is a different store with its own verdict.
fn cas_probe_cache_key(cfg: &StateConfig) -> String {
    let env = |names: &[&str]| -> String {
        names
            .iter()
            .map(|name| format!("{name}={}", std::env::var(name).unwrap_or_default()))
            .collect::<Vec<_>>()
            .join(",")
    };
    match cfg.backend {
        StateBackend::Gcs => format!(
            "gs://{}/{} [{}]",
            cfg.gcs_bucket.as_deref().unwrap_or_default(),
            cfg.gcs_prefix.as_deref().unwrap_or(DEFAULT_GCS_PREFIX),
            env(&["GOOGLE_BASE_URL", "GOOGLE_CLOUD_STORAGE_BASE_URL"])
        ),
        StateBackend::S3 | StateBackend::Tiered | StateBackend::Local | StateBackend::Valkey => {
            format!(
                "s3://{}/{} [{}]",
                cfg.s3_bucket.as_deref().unwrap_or_default(),
                cfg.s3_prefix.as_deref().unwrap_or(DEFAULT_S3_PREFIX),
                env(&[
                    "AWS_ENDPOINT_URL",
                    "AWS_ENDPOINT",
                    "AWS_REGION",
                    "AWS_DEFAULT_REGION",
                    // Changes how a conditional put is sent, so the verdict.
                    "AWS_CONDITIONAL_PUT",
                ])
            )
        }
    }
}

/// Whether the probe cache may be used. Under the test seams every test
/// installs its own in-memory store at the same bucket name, so a cached
/// outcome would leak from one test's store into another's.
fn cas_probe_cache_enabled() -> bool {
    #[cfg(any(test, feature = "test-support"))]
    if test_support::current_override().is_some()
        || test_support::current_global_override().is_some()
    {
        return false;
    }
    true
}

async fn cached_probe_conditional_writes(cfg: &StateConfig) -> CasProbeOutcome {
    let key = cas_probe_cache_key(cfg);
    let use_cache = cas_probe_cache_enabled();
    if use_cache
        && let Some(hit) = CAS_PROBE_CACHE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(|m| m.get(&key).cloned())
    {
        return hit;
    }
    let (outcome, cacheable) = probe_conditional_writes_inner(cfg).await;
    if use_cache && cacheable {
        CAS_PROBE_CACHE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_or_insert_with(Default::default)
            .insert(key, outcome.clone());
    }
    outcome
}

/// Probe whether the durable tier of `cfg` honours conditional writes.
///
/// Some S3-compatible stores (older MinIO, some gateways) accept the
/// `If-None-Match` / `If-Match` headers and ignore them, which turns every
/// compare-and-swap into a silent unconditional overwrite. The probe proves
/// the preconditions are enforced on a throwaway object under its own unique
/// key — never the shared state object:
///
/// 1. create-if-absent the probe key → must commit;
/// 2. create-if-absent again → must be refused;
/// 3. if-match the generation from step 1 → must commit;
/// 4. if-match the now-stale generation from step 1 → must be refused.
///
/// Then it deletes the key, best-effort. Four small PUTs and a DELETE, run
/// once per process per store location (see [`resolve_concurrency_control`]).
/// A conditional PUT that errors where an unconditional PUT to the same key
/// succeeds means conditional writes are rejected — `Unsupported`; anything
/// else that stops the probe is `Inconclusive`. Bounded by
/// `transfer_timeout_seconds`.
///
/// Returns `Unsupported` without I/O for a backend with no durable object tier.
pub async fn probe_conditional_writes(cfg: &StateConfig) -> CasProbeOutcome {
    probe_conditional_writes_inner(cfg).await.0
}

/// The probe plus whether its verdict may be cached for the process. Only a
/// verdict that rests on the store's own behaviour is cacheable: `Supported`,
/// and `Unsupported` because a precondition was IGNORED. "Conditional write
/// rejected, unconditional accepted" could still be two transport blips in a
/// row, so it is re-probed next time rather than pinned for the process.
async fn probe_conditional_writes_inner(cfg: &StateConfig) -> (CasProbeOutcome, bool) {
    let provider = match durable_tier_provider(cfg) {
        Ok(Some(provider)) => provider,
        Ok(None) => {
            return (
                CasProbeOutcome::Unsupported(format!(
                    "the '{}' backend has no conditional-write object tier",
                    cfg.backend
                )),
                true,
            );
        }
        Err(e) => return (CasProbeOutcome::Inconclusive(e.to_string()), false),
    };
    let key = cas_probe_key();
    let timeout = transfer_timeout(cfg);
    let span = info_span!("state.cas_probe", backend = %cfg.backend);
    let (outcome, cacheable) = with_transfer_timeout(timeout, async {
        Ok(cas_probe_steps(&provider, &key).await)
    })
    .instrument(span)
    .await
    .unwrap_or_else(|e| (CasProbeOutcome::Inconclusive(e.to_string()), false));
    match with_transfer_timeout(timeout, async { Ok(provider.delete(&key).await?) }).await {
        Ok(()) => {}
        Err(e) => debug!(error = %e, key = %key, "conditional-write probe cleanup failed"),
    }
    info!(outcome = %outcome, "state conditional-write probe finished");
    let cacheable = cacheable && !matches!(outcome, CasProbeOutcome::Inconclusive(_));
    (outcome, cacheable)
}

/// Unique key for one conditional-write probe, under `cas-probe/` at the
/// configured prefix (never the state object, never a doctor probe name).
fn cas_probe_key() -> String {
    let doctor_style = probe_key();
    let unique = doctor_style
        .strip_prefix("doctor-probe-")
        .unwrap_or(&doctor_style);
    format!("cas-probe/{unique}")
}

/// Returns the outcome and whether it is cacheable (see
/// [`probe_conditional_writes_inner`]).
async fn cas_probe_steps(provider: &ObjectStoreProvider, key: &str) -> (CasProbeOutcome, bool) {
    // One conditional put, retried once on a non-precondition error so a
    // single transport blip is not read as "conditional writes rejected"
    // (that verdict is cached for the process). Also reports whether it
    // retried: a failed first attempt may still have landed, and a retry after
    // one that did shows up as a precondition outcome. The steps below treat
    // that as inconclusive where it could be mistaken for a store verdict.
    async fn put_cond(
        provider: &ObjectStoreProvider,
        key: &str,
        data: &'static [u8],
        expected: Option<&Generation>,
    ) -> (Result<PutIfMatchOutcome, ObjectStoreError>, bool) {
        match provider
            .put_if_match(key, Bytes::from_static(data), expected)
            .await
        {
            Err(_) => (
                provider
                    .put_if_match(key, Bytes::from_static(data), expected)
                    .await,
                true,
            ),
            outcome => (outcome, false),
        }
    }

    // A conditional write failed twice with a non-precondition error. If the
    // same key accepts an unconditional write, the store rejects conditional
    // writes specifically; otherwise the probe learned nothing.
    async fn classify(
        provider: &ObjectStoreProvider,
        key: &str,
        step: &str,
        err: ObjectStoreError,
    ) -> (CasProbeOutcome, bool) {
        let outcome = match provider
            .put(key, Bytes::from_static(b"rocky cas probe (unconditional)"))
            .await
        {
            Ok(()) => CasProbeOutcome::Unsupported(format!(
                "the store rejected a conditional write ({step}: {err}) but accepted an \
                 unconditional one"
            )),
            Err(_) => CasProbeOutcome::Inconclusive(format!("{step}: {err}")),
        };
        (outcome, false)
    }

    let first = match put_cond(provider, key, b"rocky cas probe 1", None).await.0 {
        Ok(PutIfMatchOutcome::Committed(generation)) => generation,
        Ok(PutIfMatchOutcome::Conflict) => {
            return (
                CasProbeOutcome::Inconclusive("the unique probe key already existed".to_string()),
                false,
            );
        }
        Err(e) => return classify(provider, key, "create-if-absent", e).await,
    };
    // A retried create that found the object is the expected refusal either
    // way: the object exists since step 1.
    match put_cond(provider, key, b"rocky cas probe 2", None).await.0 {
        Ok(PutIfMatchOutcome::Conflict) => {}
        Ok(PutIfMatchOutcome::Committed(_)) => {
            return (
                CasProbeOutcome::Unsupported(
                    "the store accepted a create-if-absent write over an existing object \
                     (If-None-Match ignored)"
                        .to_string(),
                ),
                true,
            );
        }
        Err(e) => {
            return classify(provider, key, "create-if-absent on an existing object", e).await;
        }
    }
    if first.e_tag.is_none() && first.version.is_none() {
        return (
            CasProbeOutcome::Unsupported(
                "the store returned no ETag or version for a write, so there is nothing to \
                 compare-and-swap against"
                    .to_string(),
            ),
            true,
        );
    }
    match put_cond(provider, key, b"rocky cas probe 3", Some(&first)).await {
        (Ok(PutIfMatchOutcome::Committed(_)), _) => {}
        // The failed first attempt may have committed, moving the generation
        // on, and then the retry's refusal is the store working correctly.
        // Not a verdict either way, so not cached.
        (Ok(PutIfMatchOutcome::Conflict), true) => {
            return (
                CasProbeOutcome::Inconclusive(
                    "an if-match write carrying the current generation failed, and its \
                     retry was refused; the first attempt may have landed"
                        .to_string(),
                ),
                false,
            );
        }
        (Ok(PutIfMatchOutcome::Conflict), false) => {
            return (
                CasProbeOutcome::Unsupported(
                    "the store refused an if-match write carrying the object's current \
                     generation"
                        .to_string(),
                ),
                true,
            );
        }
        (Err(e), _) => return classify(provider, key, "if-match", e).await,
    }
    match put_cond(provider, key, b"rocky cas probe 4", Some(&first))
        .await
        .0
    {
        Ok(PutIfMatchOutcome::Conflict) => (CasProbeOutcome::Supported, true),
        Ok(PutIfMatchOutcome::Committed(_)) => (
            CasProbeOutcome::Unsupported(
                "the store accepted an if-match write carrying a stale generation (If-Match \
                 ignored)"
                    .to_string(),
            ),
            true,
        ),
        Err(e) => classify(provider, key, "if-match with a stale generation", e).await,
    }
}

/// Suffix of the `cas-required` marker, a sibling of the state object.
const CAS_REQUIRED_MARKER_SUFFIX: &str = ".cas-required";

/// Object key of the `cas-required` marker for a state object:
/// `v9/state.redb.cas-required` (under the configured prefix). One marker per
/// state object, so a namespaced state file gets its own.
fn cas_required_marker_key(remote_key: &str) -> String {
    format!(
        "{}{CAS_REQUIRED_MARKER_SUFFIX}",
        object_store_state_key(remote_key)
    )
}

/// Marker keys this process already knows exist (written or seen by a CAS
/// upload). Skips the create on every later upload of the same object.
static CAS_MARKER_KNOWN: std::sync::Mutex<Option<std::collections::HashSet<String>>> =
    std::sync::Mutex::new(None);

/// Create the `cas-required` marker beside the state object, once.
///
/// Called by every compare-and-swap upload BEFORE its conditional put, so the
/// shared object is never CAS-committed without the marker in place. The write
/// is create-if-absent on its own key — like a freeze marker it never touches
/// the state object and cannot conflict with another writer's state. A marker
/// that already exists is success.
async fn ensure_cas_required_marker(
    provider: &ObjectStoreProvider,
    remote_key: &str,
) -> Result<(), StateSyncError> {
    let key = cas_required_marker_key(remote_key);
    let cache_key = format!(
        "{}://{}/{}",
        provider.scheme(),
        provider.bucket(),
        provider.absolute_key(&key)
    );
    let use_cache = cas_probe_cache_enabled();
    if use_cache
        && CAS_MARKER_KNOWN
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_some_and(|known| known.contains(&cache_key))
    {
        return Ok(());
    }
    let body = serde_json::json!({
        "marker": "cas-required",
        "state_object": object_store_state_key(remote_key),
        "created_at": chrono::Utc::now().to_rfc3339(),
        "created_by": format!("rocky {}", env!("CARGO_PKG_VERSION")),
        "meaning": "Writers of this state object use compare-and-swap. A writer with \
                    concurrency_control = \"off\" refuses to upload while this marker exists. \
                    Delete it only on purpose, after every writer is deliberately on \"off\".",
    });
    let created = provider
        .put_if_not_exists(&key, Bytes::from(body.to_string()))
        .await?;
    if created == crate::object_store::PutIfNotExistsOutcome::Created {
        info!(marker = %key, "created the cas-required marker beside the state object");
    }
    if use_cache {
        CAS_MARKER_KNOWN
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_or_insert_with(Default::default)
            .insert(cache_key);
    }
    Ok(())
}

/// Refuse an unconditional upload of the state object when its `cas-required`
/// marker exists. Reads are never gated; only this write path is.
///
/// Fail-closed: if the marker's existence cannot be determined, the upload is
/// refused too (as a transport error, subject to `on_upload_failure`), because
/// an unconditional put that guessed wrong would erase a CAS writer's commit.
async fn refuse_unconditional_upload_if_cas_required(
    config: &StateConfig,
    remote_key: &str,
) -> Result<(), StateSyncError> {
    let Some(provider) = durable_tier_provider(config)? else {
        return Ok(());
    };
    if let Some(key) = find_cas_required_marker(&provider, remote_key).await? {
        let marker = format!(
            "{}://{}/{}",
            provider.scheme(),
            provider.bucket(),
            provider.absolute_key(&key)
        );
        return Err(StateSyncError::CasRequired { marker });
    }
    Ok(())
}

/// The `cas-required` marker keys that bind a writer of `remote_key`, newest
/// first: the current schema version's, then each older version's down to the
/// carry-forward floor.
///
/// The marker lives under its schema-version prefix, and a schema bump does not
/// move it. Without the older keys, the first writer after a bump would find no
/// marker under the new prefix, so an `off` writer would upload unguarded
/// until the first CAS commit at the new version recreated it. A marker at an
/// older version means the fleet that carried this state forward was on `cas`,
/// so it binds the new version too. Deleting the marker on purpose therefore
/// means deleting it at every version that has one.
fn cas_required_marker_keys(remote_key: &str) -> Vec<String> {
    std::iter::once(cas_required_marker_key(remote_key))
        .chain(carry_forward_versions().into_iter().map(|version| {
            format!(
                "{}{CAS_REQUIRED_MARKER_SUFFIX}",
                object_store_state_key_at(version, remote_key)
            )
        }))
        .collect()
}

/// The newest `cas-required` marker that exists among
/// [`cas_required_marker_keys`], or `None`. The existence checks run
/// concurrently, so the scan costs one round trip of the shared transfer
/// timeout rather than one per schema version. Any check that fails is an
/// error (fail-closed, as before).
async fn find_cas_required_marker(
    provider: &ObjectStoreProvider,
    remote_key: &str,
) -> Result<Option<String>, StateSyncError> {
    let keys = cas_required_marker_keys(remote_key);
    let found = futures::future::try_join_all(keys.iter().map(|key| provider.exists(key))).await?;
    Ok(keys
        .into_iter()
        .zip(found)
        .find_map(|(key, exists)| exists.then_some(key)))
}

/// Startup half of the marker rule: a writer whose resolved mode is `off`
/// refuses BEFORE it does any work when the marker already exists, so a run
/// does not reach its warehouse writes only to have its state upload refused
/// at the end (its watermarks would then never be published, and the next run
/// could re-load the same rows). The upload-time check in [`dispatch_upload`]
/// still covers a marker that appears mid-run.
///
/// A marker check that cannot complete here is warned and left to the
/// upload-time check, which fails closed — startup must not turn a transient
/// HEAD error into a refusal the download path would have tolerated.
async fn refuse_off_writer_at_start(
    cfg: &StateConfig,
    state_path: &Path,
) -> Result<(), StateSyncError> {
    if matches!(cfg.backend, StateBackend::Local) || cas_effective(cfg) {
        return Ok(());
    }
    match with_transfer_timeout(
        transfer_timeout(cfg),
        refuse_unconditional_upload_if_cas_required(cfg, &remote_state_key(state_path)),
    )
    .await
    {
        Ok(()) => Ok(()),
        Err(e @ StateSyncError::CasRequired { .. }) => Err(e),
        Err(e) => {
            warn!(
                error = %e,
                "could not check for the cas-required marker at startup; the state upload \
                 re-checks it and refuses if it cannot"
            );
            Ok(())
        }
    }
}

/// Whether the `cas-required` marker exists for the state object backing
/// `state_path`, at the current schema version or an older one down to the
/// carry-forward floor (see [`cas_required_marker_keys`]). `Ok(None)` for a
/// backend with no durable object tier, which never carries a marker. Used by
/// `rocky doctor`.
///
/// # Errors
///
/// Propagates provider construction and existence-check failures.
pub async fn cas_required_marker_present(
    cfg: &StateConfig,
    state_path: &Path,
) -> Result<Option<bool>, StateSyncError> {
    let Some(provider) = durable_tier_provider(cfg)? else {
        return Ok(None);
    };
    let found = find_cas_required_marker(&provider, &remote_state_key(state_path)).await?;
    Ok(Some(found.is_some()))
}

/// The marker's object key (relative to the configured prefix) for the state
/// object backing `state_path`, for messages.
#[must_use]
pub fn cas_required_marker_relative_key(state_path: &Path) -> String {
    cas_required_marker_key(&remote_state_key(state_path))
}

/// Boxed full-transition attempt used by [`LedgerSeamSession`].
///
/// The attempt borrows the freshly downloaded [`StateStore`] and the
/// generation captured by that same download. Borrowing (rather than owning)
/// the store prevents a successful attempt from returning it in `T`; the
/// session drops the store before it performs the conditional upload. The
/// future shape lets a seam refresh external proofs inside every attempt,
/// after the fresh shared `state.redb` blob is installed and before it mutates
/// the ledger.
pub type LedgerSeamAttempt<'a, T> =
    Pin<Box<dyn std::future::Future<Output = Result<T, StateSyncError>> + Send + 'a>>;

/// A generation-owning session for a replayable, single-seam ledger
/// transition against the shared remote `state.redb` blob.
///
/// Unlike [`RemoteStateSession`], which owns one run's acquire/finalize base,
/// this session retries a seam transition on a fresh winner. Under effective
/// CAS, every attempt is ordered as:
///
/// 1. download the shared blob and its generation together;
/// 2. open the freshly published store;
/// 3. run the caller's complete transition (dynamic authorization, external
///    proof refresh, and ledger mutation all belong inside this closure);
/// 4. drop the store;
/// 5. conditionally upload against that attempt's generation.
///
/// The return value comes from the CAS-winning attempt. A conflict discards
/// that attempt's value; the next fresh download replaces its stale local
/// mutation before replaying the whole transition, up to three total attempts.
/// Exhaustion returns
/// [`StateSyncError::LedgerSeamConflict`] and never falls back to an
/// unconditional upload.
///
/// When CAS is not effective this is deliberately the legacy half-seam shape:
/// Local performs one transition and no remote I/O; a remote backend downloads
/// once, performs one transition, and unconditionally uploads with
/// `on_upload_failure = "fail"`. On either path a failed transition or upload
/// puts the local file back to the remote copy, so rows that never committed
/// are not left readable locally.
#[derive(Debug, Clone)]
pub struct LedgerSeamSession {
    cfg: StateConfig,
    state_path: PathBuf,
    /// `[cache.schemas] replicate` — whether `schema_cache` travels with the
    /// replicated state (#1620). Carried explicitly rather than re-derived,
    /// so a session cannot disagree with the config it was built from.
    replicate_schema_cache: bool,
}

const LEDGER_SEAM_MAX_ATTEMPTS: u32 = 3;

impl LedgerSeamSession {
    /// Build a session from an owned state-config snapshot and local ledger
    /// path. Performs no I/O until [`execute`][Self::execute].
    #[must_use]
    pub fn new(cfg: &StateConfig, state_path: &Path, replicate_schema_cache: bool) -> Self {
        Self {
            cfg: cfg.clone(),
            state_path: state_path.to_path_buf(),
            replicate_schema_cache,
        }
    }

    /// Execute a full typed ledger transition and return the winning attempt's
    /// result.
    ///
    /// `attempt` receives the store produced by the current attempt's download
    /// and the generation captured by that same download. It must re-run every
    /// dynamic authorization check and refresh every external proof that the
    /// blob generation does not cover before applying its ledger mutation.
    /// Create-once side effects that cannot be replayed (for example freeze
    /// markers) stay outside this closure.
    ///
    /// # Errors
    ///
    /// Propagates download, store, transition, and upload failures. Three
    /// consecutive CAS conflicts return
    /// [`StateSyncError::LedgerSeamConflict`]; the remote winner is preserved
    /// and no unconditional upload is attempted.
    pub async fn execute<T, F>(&self, mut attempt: F) -> Result<T, StateSyncError>
    where
        T: Send,
        F: for<'a> FnMut(&'a StateStore, Option<&'a Generation>) -> LedgerSeamAttempt<'a, T> + Send,
    {
        if matches!(self.cfg.backend, StateBackend::Local) {
            let store = StateStore::open(&self.state_path)?;
            let result = attempt(&store, None).await;
            drop(store);
            return result;
        }

        // Same resolution as `RemoteStateSession::acquire`: the backend default
        // for an unset mode, confirmed by the startup probe, pinned for every
        // attempt of this transition.
        let cfg = resolved_state_config(&self.cfg).await?;
        refuse_off_writer_at_start(&cfg, &self.state_path).await?;
        let this = Self {
            cfg,
            state_path: self.state_path.clone(),
            replicate_schema_cache: self.replicate_schema_cache,
        };
        this.execute_resolved(attempt).await
    }

    async fn execute_resolved<T, F>(&self, mut attempt: F) -> Result<T, StateSyncError>
    where
        T: Send,
        F: for<'a> FnMut(&'a StateStore, Option<&'a Generation>) -> LedgerSeamAttempt<'a, T> + Send,
    {
        if !cas_effective(&self.cfg) {
            let _authority =
                download_state(&self.cfg, &self.state_path, self.replicate_schema_cache).await?;
            let store = StateStore::open(&self.state_path)?;
            let result = attempt(&store, None).await;
            drop(store);
            // The same local-visibility rule as the CAS loop below: a
            // transition that never reached the remote must not stay readable
            // in the local file.
            let output = match result {
                Ok(output) => output,
                Err(e) => {
                    self.restore_remote_winner("failed ledger-seam transition")
                        .await;
                    return Err(e);
                }
            };
            let upload_cfg = StateConfig {
                on_upload_failure: StateUploadFailureMode::Fail,
                ..self.cfg.clone()
            };
            if let Err(e) =
                upload_state(&upload_cfg, &self.state_path, self.replicate_schema_cache).await
            {
                self.restore_remote_winner("failed ledger-seam upload")
                    .await;
                return Err(e);
            }
            return Ok(output);
        }

        let upload_cfg = StateConfig {
            on_upload_failure: StateUploadFailureMode::Fail,
            ..self.cfg.clone()
        };
        for attempt_index in 0..LEDGER_SEAM_MAX_ATTEMPTS {
            let (_authority, base) = download_state_with_generation(
                &self.cfg,
                &self.state_path,
                self.replicate_schema_cache,
            )
            .await?;
            let store = StateStore::open(&self.state_path)?;
            let result = attempt(&store, base.as_ref()).await;
            drop(store);
            let output = match result {
                Ok(output) => output,
                Err(e) => {
                    // The attempt mutated the freshly downloaded LOCAL file
                    // before failing (e.g. a post-transition fence refusal
                    // after rows were written). Read-only consumers such as
                    // audit, brief, and `restore plan` open the local file
                    // without downloading, so a transition that never
                    // committed must not remain locally visible — restore the
                    // remote winner before propagating.
                    self.restore_remote_winner("failed ledger-seam transition")
                        .await;
                    return Err(e);
                }
            };

            match upload_state_cas(
                &upload_cfg,
                &self.state_path,
                base.as_ref(),
                self.replicate_schema_cache,
            )
            .await
            {
                Ok(()) => return Ok(output),
                Err(StateSyncError::CasConflict { key })
                    if attempt_index + 1 < LEDGER_SEAM_MAX_ATTEMPTS =>
                {
                    let backoff = ledger_seam_conflict_backoff(attempt_index);
                    warn!(
                        key,
                        attempt = attempt_index + 1,
                        max_attempts = LEDGER_SEAM_MAX_ATTEMPTS,
                        backoff_ms = backoff.as_millis() as u64,
                        "shared state blob changed during ledger-seam transition; \
                         replaying the full transition on the winner"
                    );
                    tokio::time::sleep(backoff).await;
                }
                Err(StateSyncError::CasConflict { key }) => {
                    // Same local-visibility rule at conflict exhaustion.
                    self.restore_remote_winner("ledger-seam conflict exhaustion")
                        .await;
                    return Err(StateSyncError::LedgerSeamConflict {
                        key,
                        attempts: LEDGER_SEAM_MAX_ATTEMPTS,
                    });
                }
                Err(e) => {
                    // A non-conflict upload failure (transport) also leaves
                    // the attempt's mutations local-only — same rule.
                    self.restore_remote_winner("failed ledger-seam upload")
                        .await;
                    return Err(e);
                }
            }
        }

        unreachable!("ledger-seam attempt loop always returns within the for body")
    }
}

impl LedgerSeamSession {
    /// Re-establish committed truth locally after a terminal seam failure,
    /// closing the recovery window: the attempt's uncommitted local file is
    /// QUARANTINED FIRST (an atomic rename — after this instant no
    /// path-opening reader can observe rows that never committed), and only
    /// then is the remote winner re-downloaded into a fresh file. On a
    /// successful restore the quarantined copy is deleted; if the download
    /// fails, it is kept for forensics and readers see absence (fail-closed
    /// — audit, brief, and `restore plan` error on a missing store instead
    /// of planning against ghosts; the next command's start-download
    /// recreates the file). Failures are logged, never masked — the
    /// caller's original error is what propagates.
    async fn restore_remote_winner(&self, context: &str) {
        let quarantine = self
            .state_path
            .with_extension(format!("redb.unpublished-{}", std::process::id()));
        let quarantined = match std::fs::rename(&self.state_path, &quarantine) {
            Ok(()) => true,
            Err(rename_error) => {
                warn!(
                    rename_error = %rename_error,
                    context,
                    "could not quarantine the uncommitted local ledger before \
                     restoring the winner; a concurrent reader may observe \
                     uncommitted rows until the re-download completes"
                );
                false
            }
        };
        match download_state(&self.cfg, &self.state_path, self.replicate_schema_cache).await {
            Ok(_) => {
                if quarantined && let Err(remove_error) = std::fs::remove_file(&quarantine) {
                    warn!(
                        remove_error = %remove_error,
                        quarantine = %quarantine.display(),
                        "restored the remote winner but could not delete the \
                         quarantined uncommitted copy"
                    );
                }
            }
            Err(error) => {
                warn!(
                    error = %error,
                    context,
                    quarantined_to = %quarantine.display(),
                    quarantined,
                    "failed to restore the remote ledger winner after a terminal \
                     ledger-seam failure; the uncommitted local file stays \
                     quarantined aside (fail-closed: readers see absence, not \
                     ghosts)"
                );
            }
        }
    }
}

fn ledger_seam_conflict_backoff(attempt: u32) -> Duration {
    let cfg = RetryConfig {
        max_retries: LEDGER_SEAM_MAX_ATTEMPTS - 1,
        initial_backoff_ms: 20,
        max_backoff_ms: 80,
        backoff_multiplier: 2.0,
        jitter: true,
        ..RetryConfig::default()
    };
    Duration::from_millis(compute_backoff(&cfg, attempt))
}

impl RemoteStateSession {
    /// Build a session over an owned snapshot of `cfg` for the ledger at
    /// `state_path`. Performs no I/O; call [`acquire`][Self::acquire] before
    /// the first state read.
    pub fn new(
        cfg: &StateConfig,
        state_path: &Path,
        durability: FinalizeDurability,
        replicate_schema_cache: bool,
    ) -> Self {
        Self {
            cfg: cfg.clone(),
            state_path: state_path.to_path_buf(),
            acquired: false,
            authority: StateAuthority::Indeterminate,
            last_download_error: None,
            durability,
            suppress_reason: None,
            finalized: false,
            periodic: None,
            periodic_shutdown: None,
            base: None,
            replicate_schema_cache,
        }
    }

    /// Whether this session performs compare-and-swap state writes — see
    /// [`cas_effective`], which this delegates to so the write path and the
    /// `rocky doctor` diagnostic cannot disagree.
    fn cas_enabled(&self) -> bool {
        cas_effective(&self.cfg)
    }

    /// Download-before-read: resolve, record, and return the ledger's
    /// [`StateAuthority`].
    ///
    /// - [`StateBackend::Local`] is a **zero-I/O** `Authoritative` no-op (the
    ///   on-disk file IS the single source of truth) — the only skip.
    /// - A successful [`download_state`] records and returns its authority.
    /// - A download **failure** records
    ///   [`Indeterminate`][StateAuthority::Indeterminate] plus the error
    ///   string and returns `Ok(Indeterminate)` — the session IS the electing
    ///   caller that [`download_state`]'s failure-stays-`Err` contract
    ///   requires; the election past the failure (bail, `--assume-fresh-state`,
    ///   degraded continue) stays with the caller, which reads the retained
    ///   error via [`last_download_error`][Self::last_download_error].
    ///
    /// # Errors
    ///
    /// Never `Err` for a download failure. `Err` is reserved for internal
    /// misuse (calling `acquire` twice on one session), for
    /// [`StateSyncError::CasUnsupported`] (`concurrency_control = "cas"` set
    /// explicitly on a store the startup probe proves ignores conditional
    /// writes — see [`resolve_concurrency_control`]), and for
    /// [`StateSyncError::CasRequired`] (the mode resolved to `off` while the
    /// `cas-required` marker exists — refused before any work, not at the
    /// end-of-run upload).
    pub async fn acquire(&mut self) -> Result<StateAuthority, StateSyncError> {
        if self.acquired {
            return Err(StateSyncError::Io(std::io::Error::other(
                "RemoteStateSession::acquire called twice — the session is one-shot per run \
                 (internal misuse)",
            )));
        }
        self.acquired = true;

        // Local backend: the on-disk redb file is the single source of truth —
        // nothing to download, nothing to fail. Zero I/O by construction
        // (`download_state`'s Local arm is also a no-op, but skipping the call
        // keeps the invariant self-evident and cheap).
        if matches!(self.cfg.backend, StateBackend::Local) {
            self.authority = StateAuthority::Authoritative;
            return Ok(self.authority);
        }

        // Resolve the concurrency mode once, before the download decides
        // whether to capture a base generation: an unset mode becomes the
        // backend default, confirmed by the startup conditional-write probe.
        // The resolved mode is pinned into the session's config so acquire,
        // checkpoint, periodic and finalize all agree. An explicit `cas` the
        // store cannot honour is an error, not a silent downgrade.
        self.cfg = resolved_state_config(&self.cfg).await?;
        refuse_off_writer_at_start(&self.cfg, &self.state_path).await?;

        // Under CAS on a backend with a durable object tier, capture the durable
        // object's generation as this run's base so `finalize` can conditionally
        // commit against it. Tier-less backends fall back to the plain download
        // (and warn once if `cas` was configured but unsupported here).
        let result = if self.cas_enabled() {
            download_state_with_generation(&self.cfg, &self.state_path, self.replicate_schema_cache)
                .await
                .map(|(authority, base)| {
                    self.base = base;
                    authority
                })
        } else {
            // (An explicit `cas` on a tier-less backend already warned in
            // `resolve_concurrency_control`.)
            download_state(&self.cfg, &self.state_path, self.replicate_schema_cache).await
        };

        match result {
            Ok(authority) => {
                self.authority = authority;
                Ok(authority)
            }
            Err(e) => {
                self.last_download_error = Some(e.to_string());
                self.authority = StateAuthority::Indeterminate;
                Ok(StateAuthority::Indeterminate)
            }
        }
    }

    /// The recorded [`StateAuthority`] (fail-closed `Indeterminate` before
    /// [`acquire`][Self::acquire] resolves it). The return type is itself
    /// `#[must_use]`.
    pub fn authority(&self) -> StateAuthority {
        self.authority
    }

    /// The download failure [`acquire`][Self::acquire] elected past, if any.
    /// Callers use it to source fail-closed bail messages from the actual
    /// transport error rather than a generic placeholder.
    #[must_use]
    pub fn last_download_error(&self) -> Option<&str> {
        self.last_download_error.as_deref()
    }

    /// Fail-closed guard: `Err` **iff** the recorded authority is
    /// [`Indeterminate`][StateAuthority::Indeterminate], carrying the retained
    /// download error so the operator sees the root cause, not just the
    /// refusal.
    ///
    /// # Errors
    ///
    /// Returns `Err` when the ledger cannot back a governed decision — the
    /// download failed (or `acquire` has not run) and no explicit election
    /// (`assume_fresh_start`) has cleared it.
    pub fn require_synced(&self) -> Result<(), StateSyncError> {
        if self.authority != StateAuthority::Indeterminate {
            return Ok(());
        }
        let cause = self
            .last_download_error
            .as_deref()
            .unwrap_or("no download was attempted");
        Err(StateSyncError::Io(std::io::Error::other(format!(
            "remote state download failed ({cause}); the local ledger is non-authoritative \
             and cannot back this run (fail-closed)"
        ))))
    }

    /// The audited operator election: flip a recorded
    /// [`Indeterminate`][StateAuthority::Indeterminate] to
    /// [`FreshStart`][StateAuthority::FreshStart] (`--assume-fresh-state`).
    /// The caller emits the structured audit warn — the session only records
    /// the elected authority. A no-op when the authority is already usable.
    pub fn assume_fresh_start(&mut self) {
        if self.authority == StateAuthority::Indeterminate {
            self.authority = StateAuthority::FreshStart;
        }
    }

    /// Publish the pre-copy recovery plan without consuming this session.
    ///
    /// Returns `true` only when the checkpoint is durable (the local backend
    /// needs no upload). A configured `skip` transport failure returns `false`:
    /// losing the local ledger then also loses this recovery evidence. Strict
    /// or governed runs propagate failures before any warehouse write.
    ///
    /// Call before starting the periodic uploader. Under CAS, only a committed
    /// upload advances this session's base; a conflict is never refreshed or
    /// swallowed. The snapshot uses the same filtering and remote key as the
    /// terminal upload, while the live store remains open.
    pub async fn publish_checkpoint(
        &mut self,
        store: &StateStore,
        durability: FinalizeDurability,
    ) -> Result<bool, StateSyncError> {
        if !self.acquired || self.periodic.is_some() {
            return Err(StateSyncError::Io(std::io::Error::other(
                "publish_checkpoint requires an acquired session before periodic uploads start",
            )));
        }
        let cfg = match (self.durability, durability) {
            (FinalizeDurability::Durable, _) | (_, FinalizeDurability::Durable) => StateConfig {
                on_upload_failure: StateUploadFailureMode::Fail,
                ..self.cfg.clone()
            },
            (FinalizeDurability::ConfigDefault, FinalizeDurability::ConfigDefault) => {
                self.cfg.clone()
            }
        };
        let suppression = self.suppress_reason.or_else(|| {
            if !self.authority.is_usable() {
                Some("non-authoritative state download")
            } else if store.was_recreated_for_forward_incompat() {
                Some("forward-incompatible state recreation")
            } else {
                None
            }
        });
        if let Some(reason) = suppression {
            let err = StateSyncError::Io(std::io::Error::other(format!(
                "recovery checkpoint publication suppressed: {reason}"
            )));
            apply_upload_failure_policy(&cfg, Err(err))?;
            return Ok(false);
        }
        if matches!(cfg.backend, StateBackend::Local) {
            return Ok(true);
        }

        let scratch = ScratchGuard::new();
        let excluded = crate::state::local_only_table_names(self.replicate_schema_cache);
        if let Err(err) = store.snapshot_to_excluding(scratch.path(), excluded) {
            apply_upload_failure_policy(&cfg, Err(err.into()))?;
            return Ok(false);
        }
        let remote_key = remote_state_key(&self.state_path);
        let result = if self.cas_enabled() {
            match dispatch_upload_cas(&cfg, scratch.path(), &remote_key, self.base.as_ref()).await {
                Ok(PutIfMatchOutcome::Committed(generation)) => {
                    self.base = Some(generation);
                    Ok(())
                }
                Ok(PutIfMatchOutcome::Conflict) => {
                    Err(StateSyncError::CasConflict { key: remote_key })
                }
                Err(err) => Err(err),
            }
        } else {
            dispatch_upload(&cfg, scratch.path(), &remote_key).await
        };
        match result {
            Ok(()) => Ok(true),
            Err(err) => {
                apply_upload_failure_policy(&cfg, Err(err))?;
                Ok(false)
            }
        }
    }

    /// Start the owned mid-run periodic uploader over a **`Weak`** handle to the
    /// live [`StateStore`], dirty-gated on its [`write_epoch`][StateStore::write_epoch].
    ///
    /// Each tick, if the store's epoch has advanced since the last upload, it
    /// exports a torn-read-free [`snapshot_to_excluding`][StateStore::snapshot_to_excluding]
    /// (on the blocking pool) and uploads it via [`upload_state_snapshot`]; a
    /// clean tick (epoch unchanged) costs zero I/O.
    ///
    /// The handle is [`Weak`], never a strong [`Arc`][std::sync::Arc] clone: the
    /// run tail recovers the owned store with `Arc::try_unwrap`, which a lingering
    /// strong clone here would block — silently dropping the owned store and
    /// no-op'ing every terminal write. With `Weak`, `try_unwrap` is never held
    /// up; a tick that races the run tail's `drop` simply fails to `upgrade` and
    /// exits.
    ///
    /// The caller applies its own spawn gating (estimated duration, suppression)
    /// — the session only owns the task so [`stop_periodic`][Self::stop_periodic]
    /// / [`finalize`][Self::finalize] can drain **and join** it. A no-op when a
    /// periodic task is already running.
    ///
    /// Shutdown is **cooperative**, not an `abort()`: an idle tick (parked on the
    /// cadence sleep) breaks instantly, but a tick already inside its
    /// `spawn_blocking` snapshot runs that snapshot to completion. `abort()`
    /// would drop the `.await` and detach the blocking closure, which would then
    /// (1) recreate the scratch file its [`ScratchGuard`] just removed on
    /// cancellation (a leak) and (2) hold its `Arc<StateStore>` upgrade past the
    /// run tail's `Arc::try_unwrap`, silently no-op'ing every terminal write.
    pub fn start_periodic_uploader(&mut self, store: Weak<StateStore>, cadence: Duration) {
        if self.periodic.is_some() {
            return;
        }
        if self.cas_enabled() {
            // Under CAS the mid-run periodic uploader is disabled: a correct
            // periodic CAS write must advance a base shared with `finalize` and
            // stop-without-refresh on conflict (ADR-CONCURRENCY §D2) — a
            // follow-up. Finalize-only CAS against the acquire base is correct
            // precisely because no mid-run upload bumps the remote generation.
            debug!(
                "periodic state uploader disabled under concurrency_control = cas \
                 (finalize-only CAS)"
            );
            return;
        }
        let cfg = self.cfg.clone();
        let path = self.state_path.clone();
        // Same posture as the terminal upload legs — a mid-run tick must not
        // strip a table the end-of-run upload keeps, or the remote's schema
        // cache would flip between ticks (#1620).
        let local_only = crate::state::local_only_table_names(self.replicate_schema_cache);
        // Derive the remote key ONCE from the REAL state path (namespace-correct)
        // — never from the scratch temp path, which would resolve to the legacy
        // `state.redb` key and clobber namespaced state (mirrors the invariant in
        // `upload_state_with_excluded_tables`).
        let remote_key = remote_state_key(&path);
        let shutdown = Arc::new(tokio::sync::Notify::new());
        self.periodic_shutdown = Some(Arc::clone(&shutdown));
        self.periodic = Some(tokio::spawn(async move {
            let mut last_uploaded_epoch: u64 = 0;
            loop {
                // Cooperative wait: a shutdown signal breaks the loop instead of
                // sleeping out the cadence. `notify_one` sets a permit even with
                // no waiter parked, so a signal that arrives mid-tick is consumed
                // by the next iteration's `notified()`.
                tokio::select! {
                    _ = tokio::time::sleep(cadence) => {}
                    _ = shutdown.notified() => break,
                }

                // The run tail dropped the owned store → nothing left to sync.
                let Some(store) = store.upgrade() else {
                    break;
                };

                // Capture the epoch BEFORE the read snapshot: recording an epoch
                // <= the snapshot's content is a harmless re-upload next tick;
                // recording > content would permanently skip a real change.
                let epoch = store.write_epoch();
                if epoch == last_uploaded_epoch {
                    // Clean tick — no mutation since the last upload; zero I/O.
                    continue;
                }

                // Build the consistent snapshot on the blocking pool (the redb
                // export is fsync-bearing). Cooperative shutdown lets this
                // `.await` finish rather than being cancelled, so the scratch
                // guard's cleanup and the `Arc` release both happen in-task.
                let scratch = ScratchGuard::new();
                let scratch_path = scratch.path().to_path_buf();
                let snapshot = tokio::task::spawn_blocking(move || {
                    store.snapshot_to_excluding(&scratch_path, local_only)
                })
                .await;

                match snapshot {
                    Ok(Ok(())) => {
                        match upload_state_snapshot(&cfg, scratch.path(), &remote_key).await {
                            Ok(()) => last_uploaded_epoch = epoch,
                            Err(e) => warn!(error = %e, "periodic state sync upload failed"),
                        }
                    }
                    Ok(Err(e)) => warn!(error = %e, "periodic state snapshot failed"),
                    Err(e) => warn!(error = %e, "periodic state snapshot task panicked"),
                }
                // `scratch` drops here → the temp snapshot file is removed.
            }
        }));
    }

    /// Cooperatively DRAIN and JOIN the periodic uploader (the RD-004
    /// stop-before-terminal-upload fix, hardened for the snapshot path): signal
    /// shutdown, then await the task so it exits after any in-flight tick's
    /// snapshot + upload complete — never mid-`put`, never leaking a detached
    /// blocking closure. Idempotent — a no-op when no periodic task is running.
    pub async fn stop_periodic(&mut self) {
        if let Some(shutdown) = self.periodic_shutdown.take() {
            shutdown.notify_one();
        }
        if let Some(handle) = self.periodic.take() {
            // Cooperative exit is the expected outcome; a JoinError panics-variant
            // is surfaced by the task's own panic hook, not here.
            let _ = handle.await;
        }
    }

    /// Record an upload suppression: [`finalize`][Self::finalize] will skip
    /// the terminal upload (and return `Ok`). First recorded reason wins.
    ///
    /// Used for the two no-clobber cases: a store recreated after a
    /// forward-incompatible schema mismatch (uploading the downgraded state
    /// would overwrite newer shared state), and an
    /// [`Indeterminate`][StateAuthority::Indeterminate] download (pushing
    /// local state over a remote this run could not read is a blind
    /// last-writer-wins).
    pub fn set_suppress_upload(&mut self, reason: &'static str) {
        self.suppress_reason.get_or_insert(reason);
    }

    /// One-shot terminal upload. Stops (aborts + joins) the periodic uploader,
    /// then — unless suppressed — uploads via [`upload_state`]:
    ///
    /// - [`FinalizeDurability::Durable`]: `on_upload_failure` is **forced to
    ///   `Fail`** (same forcing as the apply path's fail-closed ledger
    ///   upload), so a lost governed terminal upload errs regardless of the
    ///   configured liveness default.
    /// - [`FinalizeDurability::ConfigDefault`]: the configured mode is honored
    ///   — `skip` swallows-with-warn inside [`upload_state`], an explicit
    ///   `fail` propagates.
    ///
    /// Callers position this AFTER the run's terminal state writes so the run
    /// record / custody rows / idempotency stamp ride the upload.
    ///
    /// # Errors
    ///
    /// Propagates the upload failure per the durability policy above. The
    /// session counts as consumed either way (no Drop tripwire).
    pub async fn finalize(mut self) -> Result<(), StateSyncError> {
        self.stop_periodic().await;
        // Deliberately consumed even when the upload below fails: the caller
        // made the terminal decision and handles the Err; the tripwire exists
        // for *forgotten* sessions.
        self.finalized = true;
        if let Some(reason) = self.suppress_reason {
            info!(
                reason,
                outcome = "suppressed",
                "skipping end-of-run state upload"
            );
            return Ok(());
        }
        terminal_upload(
            &self.cfg,
            self.durability,
            &self.state_path,
            self.base.as_ref(),
            self.replicate_schema_cache,
        )
        .await
    }

    /// Deliberate no-upload consumption for error/interrupted exits: stops
    /// (aborts + joins) the periodic uploader and disarms the Drop tripwire
    /// without touching the remote. Error paths never uploaded — the run's
    /// terminal state writes did not happen, so there is nothing durable to
    /// persist.
    pub async fn abandon(mut self, reason: &str) {
        self.stop_periodic().await;
        self.finalized = true;
        debug!(
            reason,
            "remote-state session abandoned without a terminal upload"
        );
    }

    // -----------------------------------------------------------------------
    // Half-seams — download-XOR-upload lifecycle shapes (WP-01 PR-B §1)
    // -----------------------------------------------------------------------

    /// Half-seam download for the pre-gate reads of the ledger seams (gc
    /// apply, restore apply, the governed-apply pre-gate sync): pull the
    /// authoritative remote ledger before a gate reads it.
    ///
    /// There is deliberately no upload counterpart. Every ledger-seam publish
    /// goes through [`LedgerSeamSession`], which downloads again inside each
    /// attempt and, under effective CAS, publishes conditionally (#1242). An
    /// unconditional "upload the local file" seam would let a stale local
    /// ledger overwrite a CAS-committed winner.
    ///
    /// A *lifecycle shape*, not a session: no `acquire`/`finalize` pairing, no
    /// Drop tripwire. [`StateBackend::Local`] is a zero-I/O
    /// [`Authoritative`][StateAuthority::Authoritative] no-op (mirroring
    /// [`acquire`][Self::acquire]); otherwise this delegates to
    /// [`download_state`] and propagates its result UNCHANGED — these seams
    /// stay fail-closed `?`-bail and never synthesize
    /// [`Indeterminate`][StateAuthority::Indeterminate].
    ///
    /// # Errors
    ///
    /// Propagates [`download_state`]'s failure verbatim (the caller attaches
    /// its seam-specific fail-closed context).
    pub async fn download_only(
        cfg: &StateConfig,
        state_path: &Path,
        replicate_schema_cache: bool,
    ) -> Result<StateAuthority, StateSyncError> {
        if matches!(cfg.backend, StateBackend::Local) {
            return Ok(StateAuthority::Authoritative);
        }
        download_state(cfg, state_path, replicate_schema_cache).await
    }
}

impl RemoteStateSession {
    /// Move everything settlement needs out of `self` and mark it consumed, so
    /// the `Drop` that called this neither trips nor aborts the periodic task
    /// the settler is about to join.
    fn take_for_settlement(&mut self) -> DroppedSession {
        self.finalized = true;
        DroppedSession {
            cfg: std::mem::take(&mut self.cfg),
            state_path: std::mem::take(&mut self.state_path),
            acquired: self.acquired,
            authority: self.authority,
            durability: self.durability,
            suppress_reason: self.suppress_reason,
            periodic: self.periodic.take(),
            periodic_shutdown: self.periodic_shutdown.take(),
            base: self.base.take(),
            replicate_schema_cache: self.replicate_schema_cache,
        }
    }
}

/// How long [`DroppedSession::settle`] waits for the dropped run's last
/// `StateStore` writer to close before it gives up on the upload.
///
/// The writers a dropped run can leave behind are detached `spawn_blocking`
/// ledger commits (deferred watermarks, progress checkpoints), which finish in
/// milliseconds. The bound is for a wedged one: the settler must not hang the
/// shutdown it is part of.
pub const DROPPED_SESSION_WRITER_WAIT: Duration = Duration::from_secs(30);

/// Poll interval for [`DROPPED_SESSION_WRITER_WAIT`].
const DROPPED_SESSION_WRITER_POLL: Duration = Duration::from_millis(25);

tokio::task_local! {
    /// Where a [`RemoteStateSession`] dropped without `finalize`/`abandon`
    /// hands itself, when the code that dropped it installed a sink and armed
    /// it. See [`DroppedSessionSink`].
    static DROPPED_SESSIONS: DroppedSessionSink;
}

/// Catches the [`RemoteStateSession`]s a deliberately dropped run future still
/// held, so their terminal upload runs instead of being skipped (#1603).
///
/// `rocky run --watch` stops an iteration on a signal by dropping its future.
/// A session inside it never reaches `finalize`/`abandon`, and `Drop` cannot
/// await an upload. So the watch loop runs the iteration inside
/// [`scope`][Self::scope], [`arm`][Self::arm]s the sink just before it drops
/// the future, and then [`settle_all`][Self::settle_all]s what was handed over:
///
/// ```text
///   signal ──▶ sink.arm() ──▶ drop(iteration)
///                                 │
///                                 └─▶ RemoteStateSession::drop
///                                        armed sink in scope? ──yes──▶ hand over
///                                                             └─no───▶ tripwire
///   sink.settle_all() ──▶ join periodic ──▶ wait for writers ──▶ terminal upload
/// ```
///
/// The sink is armed only for that deliberate drop. A session dropped while
/// the run is still being polled — an early `?` return, a panic — is the
/// calling-path bug the tripwire exists for, and it still trips: the sink
/// never turns an error path into an upload.
#[derive(Clone, Default)]
pub struct DroppedSessionSink {
    inner: Arc<DroppedSessionSinkInner>,
}

#[derive(Default)]
struct DroppedSessionSinkInner {
    armed: std::sync::atomic::AtomicBool,
    sessions: std::sync::Mutex<Vec<DroppedSession>>,
}

impl std::fmt::Debug for DroppedSessionSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DroppedSessionSink")
            .field(
                "armed",
                &self.inner.armed.load(std::sync::atomic::Ordering::SeqCst),
            )
            .finish_non_exhaustive()
    }
}

impl DroppedSessionSink {
    /// A fresh, unarmed sink.
    pub fn new() -> Self {
        Self::default()
    }

    /// Run `future` with this sink installed. Sessions created inside it, on
    /// this task, hand themselves here when dropped unsettled while the sink
    /// is armed — including when `future` itself is dropped, because tokio
    /// drops a scoped future with its task-local still set.
    pub fn scope<F: std::future::Future>(
        &self,
        future: F,
    ) -> tokio::task::futures::TaskLocalFuture<DroppedSessionSink, F> {
        DROPPED_SESSIONS.scope(self.clone(), future)
    }

    /// From now on an unsettled session dropped in scope is handed over, not
    /// tripped. Call this immediately before deliberately dropping the scoped
    /// future.
    pub fn arm(&self) {
        self.inner
            .armed
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Accept `session` if armed. `false` means the caller must trip.
    fn accept(&self, session: &mut RemoteStateSession) -> bool {
        if !self.inner.armed.load(std::sync::atomic::Ordering::SeqCst) {
            return false;
        }
        let dropped = session.take_for_settlement();
        self.inner
            .sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(dropped);
        true
    }

    /// Settle every session handed over so far. Each is attempted even when an
    /// earlier one fails; the first failure is returned.
    ///
    /// # Errors
    ///
    /// The first [`DroppedSession::settle`] failure — a terminal upload that
    /// the session's durability says must not be lost.
    pub async fn settle_all(&self) -> Result<usize, StateSyncError> {
        let sessions = std::mem::take(
            &mut *self
                .inner
                .sessions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        let count = sessions.len();
        let mut first_error = None;
        for session in sessions {
            if let Err(e) = session.settle().await {
                warn!(error = %e, "settling an interrupted run's state session failed");
                first_error.get_or_insert(e);
            }
        }
        match first_error {
            Some(e) => Err(e),
            None => Ok(count),
        }
    }

    /// Whether any session waiting to be settled will attempt a remote
    /// upload — the only case where settling can take noticeable time.
    pub fn owes_upload(&self) -> bool {
        self.inner
            .sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .any(DroppedSession::owes_upload)
    }

    /// How many sessions are waiting to be settled.
    pub fn pending(&self) -> usize {
        self.inner
            .sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }
}

/// A [`RemoteStateSession`] whose run future was dropped before it reached
/// `finalize` or `abandon`, waiting for [`settle`][Self::settle].
#[derive(Debug)]
pub struct DroppedSession {
    cfg: StateConfig,
    state_path: PathBuf,
    acquired: bool,
    authority: StateAuthority,
    durability: FinalizeDurability,
    suppress_reason: Option<&'static str>,
    periodic: Option<tokio::task::JoinHandle<()>>,
    periodic_shutdown: Option<Arc<tokio::sync::Notify>>,
    base: Option<Generation>,
    replicate_schema_cache: bool,
}

impl DroppedSession {
    /// Finish what the dropped run owed its remote state: the terminal upload,
    /// under the same rules [`RemoteStateSession::finalize`] applies.
    ///
    /// 1. Drain and join the periodic uploader, as `finalize` does.
    /// 2. Skip the upload — `Ok`, nothing to push — when the session never
    ///    downloaded (pushing local state over a remote it never read is a
    ///    blind overwrite), when an upload suppression was recorded, when the
    ///    download was not authoritative, or on the local backend.
    /// 3. Wait, bounded by [`DROPPED_SESSION_WRITER_WAIT`], for the advisory
    ///    writer lock every `StateStore` writer holds, and hold it through the
    ///    upload. A detached ledger commit from the dropped run is therefore
    ///    finished, never torn, when the file is read.
    /// 4. Upload exactly as `finalize` would: CAS against the base captured at
    ///    acquire when CAS is effective, `Durable` forcing
    ///    `on_upload_failure = "fail"`.
    ///
    /// The uploaded ledger is what the run had committed when it was dropped —
    /// the same kind of mid-run snapshot the periodic uploader ships, and what
    /// an interrupted replication run already publishes as its checkpoint.
    ///
    /// # Errors
    ///
    /// A lost upload, or a writer that never let go, as the durability policy
    /// decides: `Durable` (governed) always errs, `ConfigDefault` errs only
    /// under `on_upload_failure = "fail"`. A CAS conflict always errs.
    pub async fn settle(mut self) -> Result<(), StateSyncError> {
        if let Some(shutdown) = self.periodic_shutdown.take() {
            shutdown.notify_one();
        }
        if let Some(handle) = self.periodic.take() {
            let _ = handle.await;
        }

        if let Some(reason) = self.no_upload_reason() {
            info!(
                reason,
                outcome = "skipped",
                "interrupted run: no terminal state upload owed"
            );
            return Ok(());
        }

        let _writer = match wait_for_writer_lock(&self.state_path).await {
            Ok(lock) => lock,
            Err(e) => return apply_upload_failure_policy(&self.effective_cfg(), Err(e)),
        };
        info!(
            state_path = %self.state_path.display(),
            "interrupted run: performing its terminal state upload"
        );
        // A CAS conflict stays fail-closed even under `skip`, as everywhere
        // else (`apply_upload_failure_policy`). Here it can have an innocent
        // cause — the dropped run committed its own interrupted checkpoint
        // without recording the new generation — but it cannot be told apart
        // from a lost cross-pod race, and overwriting a winner is the worse
        // error. The cost is a non-zero exit, never lost or clobbered state.
        terminal_upload(
            &self.cfg,
            self.durability,
            &self.state_path,
            self.base.as_ref(),
            self.replicate_schema_cache,
        )
        .await
    }

    /// Why this session owes no terminal upload, if it owes none: it never
    /// downloaded (pushing local state over a remote it never read is a
    /// blind overwrite), an upload suppression was recorded, the download
    /// was not authoritative, or the backend is local.
    fn no_upload_reason(&self) -> Option<&'static str> {
        if !self.acquired {
            Some("the session never downloaded state")
        } else if let Some(reason) = self.suppress_reason {
            Some(reason)
        } else if !self.authority.is_usable() {
            Some("non-authoritative state download")
        } else if matches!(self.cfg.backend, StateBackend::Local) {
            Some("local backend")
        } else {
            None
        }
    }

    /// Whether [`settle`][Self::settle] will attempt a remote upload.
    pub fn owes_upload(&self) -> bool {
        self.no_upload_reason().is_none()
    }

    /// The config the upload runs under: `Durable` forces
    /// `on_upload_failure = "fail"`.
    fn effective_cfg(&self) -> StateConfig {
        match self.durability {
            FinalizeDurability::Durable => StateConfig {
                on_upload_failure: StateUploadFailureMode::Fail,
                ..self.cfg.clone()
            },
            FinalizeDurability::ConfigDefault => self.cfg.clone(),
        }
    }
}

/// Take the state file's advisory writer lock, retrying while another writer
/// holds it, up to [`DROPPED_SESSION_WRITER_WAIT`].
async fn wait_for_writer_lock(
    state_path: &Path,
) -> Result<crate::state::StateWriterLock, StateSyncError> {
    let deadline = tokio::time::Instant::now() + DROPPED_SESSION_WRITER_WAIT;
    loop {
        match crate::state::try_acquire_writer_lock(state_path) {
            Ok(lock) => return Ok(lock),
            Err(crate::state::StateError::LockHeldByOther { .. })
                if tokio::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(DROPPED_SESSION_WRITER_POLL).await;
            }
            Err(e) => {
                return Err(StateSyncError::Io(std::io::Error::other(format!(
                    "the interrupted run's state store at {} was never released, so its \
                     terminal upload could not read a settled ledger: {e}",
                    state_path.display()
                ))));
            }
        }
    }
}

/// The terminal upload body shared by [`RemoteStateSession::finalize`] and
/// [`DroppedSession::settle`], so the two cannot drift.
///
/// CAS terminal commit when CAS is effective: conditionally upload against the
/// base captured at acquire. A lost cross-pod race surfaces `CasConflict`
/// (fail-closed, never swallowed). `Durable` forces
/// `on_upload_failure = Fail` for the transport-error case on both legs.
async fn terminal_upload(
    cfg: &StateConfig,
    durability: FinalizeDurability,
    state_path: &Path,
    base: Option<&Generation>,
    replicate_schema_cache: bool,
) -> Result<(), StateSyncError> {
    let effective = match durability {
        FinalizeDurability::Durable => StateConfig {
            on_upload_failure: StateUploadFailureMode::Fail,
            ..cfg.clone()
        },
        FinalizeDurability::ConfigDefault => cfg.clone(),
    };
    if cas_effective(cfg) {
        return upload_state_cas(&effective, state_path, base, replicate_schema_cache).await;
    }
    upload_state(&effective, state_path, replicate_schema_cache).await
}

impl Drop for RemoteStateSession {
    fn drop(&mut self) {
        if self.finalized {
            return;
        }
        // A deliberate drop under an armed sink (`rocky run --watch` stopping
        // an iteration on a signal): hand the session over so the watch loop
        // performs its terminal upload. Not a bug, so no tripwire (#1603).
        if DROPPED_SESSIONS
            .try_with(|sink| sink.accept(self))
            .unwrap_or(false)
        {
            debug!("RemoteStateSession handed to the dropped-session sink for settlement");
            return;
        }
        // Resource net first (the debug_assert below panics in debug builds):
        // a leaked periodic handle must not outlive the run. Signal cooperative
        // shutdown, then `abort()` as the hard net — `Drop` cannot await a join,
        // so this cannot guarantee the in-tick drain the normal
        // `stop_periodic`/`finalize` paths provide; it is the leaked-session bug
        // path, where the resource net wins over the drain guarantee.
        if let Some(shutdown) = self.periodic_shutdown.take() {
            shutdown.notify_one();
        }
        if let Some(handle) = self.periodic.take() {
            handle.abort();
        }
        warn!(
            state_path = %self.state_path.display(),
            "RemoteStateSession dropped without finalize/abandon — the terminal state upload \
             was skipped (tripwire; this is a bug in the calling run path)"
        );
        debug_assert!(
            false,
            "RemoteStateSession dropped without finalize/abandon (state_path: {})",
            self.state_path.display()
        );
    }
}

/// Downloads state from remote storage to a local file before a run.
///
/// Returns the typed [`StateAuthority`] of the local ledger after the download:
///
/// - the remote object existed and was restored ⇒ [`StateAuthority::Authoritative`];
/// - no remote object existed (a genuine fresh start) ⇒ [`StateAuthority::FreshStart`]
///   — **non-fatal** by design;
/// - the **Local** backend ⇒ [`StateAuthority::Authoritative`] (the on-disk
///   file is the single source of truth — see the explicit arm below).
///
/// A real download/existence-check *failure* is **propagated** as `Err` so the
/// caller can fail closed — see [`download_from_object_store`]. It is never
/// mapped to `Ok(StateAuthority::Indeterminate)`: that variant is synthesized
/// only by a caller that explicitly elects to continue past an `Err`, so every
/// fail-closed `download_state(...)?` seam keeps bailing unchanged.
///
/// # Local-only-preserving merge (findings 6 + 7)
///
/// A remote download REPLACES the replicated tables wholesale: a
/// [`DownloadOutcome::Restored`] overwrites the file, and a
/// [`DownloadOutcome::Absent`] would otherwise leave a *stale* local file
/// authoritative. Neither may be allowed to disturb the machine-local tables in
/// [`crate::state::LOCAL_ONLY_TABLE_NAMES`] (`jobs`, `schema_cache`), which are
/// stripped from the remote copy on upload and so never travel back down:
///
/// - **Restored** — the local file is replaced by the remote (which carries the
///   authoritative replicated tables but *no* local-only tables). The local-only
///   tables are snapshotted **before** the download and spliced back in
///   afterwards, so `jobs` / `schema_cache` survive a download that replaces the
///   replicated tables.
/// - **Carried forward** (#1955) — the current-version key is absent but an
///   older schema version's key exists (newest first, down to
///   [`CARRY_FORWARD_FLOOR_SCHEMA_VERSION`]). The leg restores it and reports
///   [`DownloadOutcome::Restored`], so it merges exactly like a restore. It
///   captures no CAS generation: the first upload writes the current key with
///   create-if-absent. The Valkey cache leg of `tiered` never carries forward.
/// - **Absent** — no remote object exists for this key, nor for any older
///   schema version down to the floor. For a REMOTE backend the
///   replicated tables must become **fresh** (empty) — a switch to an empty
///   prefix must not keep stale watermarks (finding 6) — while the local-only
///   tables are preserved. The pre-download snapshot already holds *only* the
///   local-only tables, so promoting it to the authoritative file both clears
///   the replicated tables and keeps `jobs` / `schema_cache`.
///
/// The **Local** backend performs no transfer, so there is nothing to replace
/// and nothing to preserve — it delegates straight to [`download_state_inner`].
///
/// ## Crash safety + fail-closed (finding 5)
///
/// The remote content is downloaded into a STAGING file beside `local_path`; the
/// local-only tables are merged into that staging file, and only then is the
/// COMPLETE merged db published to `local_path` with a single atomic `rename`.
/// So `local_path` is never left holding remote content *without* its local-only
/// tables — a crash before the rename leaves the prior local file fully intact.
/// A merge / publish failure is **fail-closed** (propagated as `Err` after a
/// bounded retry on transient redb open contention).
///
/// ## Publish serialization (finding B)
///
/// The download itself runs **without** any lock (it is network I/O). The
/// snapshot-of-local-only + merge + atomic publish then run **while holding the
/// same advisory writer lock [`StateStore::open`] takes**
/// ([`crate::state::try_acquire_writer_lock`]), and the lock is released before
/// this returns so the caller's `StateStore::open` can re-acquire it. This
/// serializes the publish with any concurrent StateStore writer and with another
/// download's publish on the same namespace, so a late `rename` can never clobber
/// a live writer's file. Crucially, the local-only tables are re-read from the
/// CURRENT local file *under the lock*, so a concurrent run's just-committed
/// `jobs` are captured rather than lost.
pub async fn download_state(
    config: &StateConfig,
    local_path: &Path,
    replicate_schema_cache: bool,
) -> Result<StateAuthority, StateSyncError> {
    download_state_impl(config, local_path, None, replicate_schema_cache).await
}

/// Download-before-read that ALSO captures the remote object's [`Generation`]
/// for a compare-and-swap writer (`[state] concurrency_control = "cas"`).
///
/// The captured generation is the *base* the writer CAS-commits against at
/// [`RemoteStateSession::finalize`]. `Ok((_, None))` means the object was
/// absent (a bootstrap first-write, which CASes with `PutMode::Create`) or the
/// backend surfaced no version metadata. Behaviour is otherwise identical to
/// [`download_state`] — same staging, publish-lock, and local-only merge.
pub async fn download_state_with_generation(
    config: &StateConfig,
    local_path: &Path,
    replicate_schema_cache: bool,
) -> Result<(StateAuthority, Option<Generation>), StateSyncError> {
    let mut generation = None;
    let authority = download_state_impl(
        config,
        local_path,
        Some(&mut generation),
        replicate_schema_cache,
    )
    .await?;
    Ok((authority, generation))
}

async fn download_state_impl(
    config: &StateConfig,
    local_path: &Path,
    mut gen_sink: Option<&mut Option<Generation>>,
    replicate_schema_cache: bool,
) -> Result<StateAuthority, StateSyncError> {
    let remote_key = remote_state_key(local_path);

    // Local backend never replaces the file — nothing to stage, merge, or lock.
    //
    // Local is `Authoritative`, NOT `FreshStart`: the inner dispatch reports
    // `Absent` for Local (there is no remote object to restore), but a naive
    // outcome map would mislabel that as a bootstrappable fresh start. There is
    // no remote to be "fresh" against — the on-disk redb file IS the single
    // source of truth and must never be treated as an empty ledger to
    // bootstrap over.
    if matches!(config.backend, StateBackend::Local) {
        download_state_inner(config, local_path, &remote_key, gen_sink.take()).await?;
        return Ok(StateAuthority::Authoritative);
    }

    // 1. Download the remote into a STAGING file (no lock). `local_path` is left
    //    untouched; `Absent` leaves the staging file unwritten.
    let scratch_dir = sibling_scratch_dir(local_path);
    let staged = unique_scratch_path(&scratch_dir, "download");
    let outcome = match download_state_inner(config, &staged, &remote_key, gen_sink.take()).await {
        Ok(outcome) => outcome,
        Err(e) => {
            let _ = std::fs::remove_file(&staged);
            return Err(e);
        }
    };

    // 2. Acquire the StateStore writer lock so the snapshot + merge + publish
    //    serialize with concurrent writers/publishes (finding B). Released on
    //    drop, before this function returns.
    let lock = match acquire_publish_lock(local_path).await {
        Ok(lock) => lock,
        Err(e) => {
            let _ = std::fs::remove_file(&staged);
            return Err(e);
        }
    };

    // 3. Under the lock: re-read the CURRENT local-only tables, merge them into
    //    the candidate db, and publish atomically.
    let result = publish_merged(&staged, local_path, outcome, replicate_schema_cache).await;

    drop(lock);
    let _ = std::fs::remove_file(&staged);
    result?;
    Ok(match outcome {
        DownloadOutcome::Restored => StateAuthority::Authoritative,
        DownloadOutcome::Absent => StateAuthority::FreshStart,
    })
}

/// Acquire the StateStore writer lock for `local_path` (the same lock
/// `StateStore::open` takes), retrying briefly on contention before failing
/// closed. Serializes the download's atomic publish with StateStore writers and
/// with a concurrent download (finding B).
async fn acquire_publish_lock(
    local_path: &Path,
) -> Result<crate::state::StateWriterLock, StateSyncError> {
    const MAX_ATTEMPTS: u32 = 5;
    let mut last: Option<crate::state::StateError> = None;
    for attempt in 1..=MAX_ATTEMPTS {
        match crate::state::try_acquire_writer_lock(local_path) {
            Ok(lock) => return Ok(lock),
            Err(e) => {
                last = Some(e);
                if attempt < MAX_ATTEMPTS {
                    tokio::time::sleep(Duration::from_millis(20 * u64::from(attempt))).await;
                }
            }
        }
    }
    Err(StateSyncError::Io(std::io::Error::other(format!(
        "could not acquire the state writer lock to publish downloaded state \
         (another writer holds it): {}",
        last.expect("retry loop runs at least once")
    ))))
}

/// Build the merged candidate db and publish it to `local_path` with a single
/// atomic `rename` — MUST be called while holding the writer lock (finding B).
///
/// The local-only tables are read from the CURRENT `local_path` here (not a
/// pre-download snapshot), so a concurrent run's just-committed rows are captured.
/// Fail-closed on any merge/publish error (finding 5b).
async fn publish_merged(
    staged: &Path,
    local_path: &Path,
    outcome: DownloadOutcome,
    replicate_schema_cache: bool,
) -> Result<(), StateSyncError> {
    let local_exists = local_path.exists();
    // The set to take FROM THE LOCAL FILE when a remote copy exists. Under
    // `[cache.schemas] replicate = true` the schema cache is not in it, so the
    // remote's cache survives into the published file instead of being
    // overwritten by this machine's (#1620).
    let tables = crate::state::local_only_table_names(replicate_schema_cache);
    match outcome {
        DownloadOutcome::Restored => {
            // `staged` holds the remote replicated tables. Overwrite its
            // local-only tables from the current local file, or EMPTY them when
            // there is none — a pre-this-patch remote snapshot can still carry
            // another pod's `jobs` rows, which must NOT land locally (finding 6a).
            if local_exists {
                redb_op_retry(|| copy_named_tables(local_path, staged, tables)).await?;
            } else {
                redb_op_retry(|| clear_named_tables(staged, tables)).await?;
            }
            publish_atomically(staged, local_path)
        }
        DownloadOutcome::Absent if local_exists => {
            // No remote object: the replicated tables must reset to fresh/empty
            // (finding 6) while the local-only tables are preserved. Build a fresh
            // db carrying ONLY the current local-only tables (`staged` was not
            // written by an `Absent` download, so `copy_named_tables` creates it
            // with just those tables → replicated tables are empty on next open).
            //
            // This arm deliberately keeps the FULL local-only set even when
            // replicating. There is no remote schema cache to inherit here, and
            // the replicating set would leave `schema_cache` out of the rebuilt
            // file — so opting into replication would DELETE this machine's warm
            // cache every time the remote object is missing. Replication is
            // about sharing the cache, never about discarding it (#1620).
            redb_op_retry(|| {
                copy_named_tables(local_path, staged, crate::state::LOCAL_ONLY_TABLE_NAMES)
            })
            .await?;
            publish_atomically(staged, local_path)
        }
        DownloadOutcome::Absent => Ok(()),
    }
}

/// Directory that should host the sibling scratch file for `local_path`, so an
/// atomic `rename` publish stays on the same filesystem. Falls back to the
/// current directory when `local_path` has no parent component.
fn sibling_scratch_dir(local_path: &Path) -> PathBuf {
    local_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."))
}

/// A process-unique scratch path in `dir` (PID + nanos + a monotonic sequence,
/// so two scratch files taken in the same nanosecond never collide).
fn unique_scratch_path(dir: &Path, tag: &str) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    dir.join(format!(
        ".rocky-state-{tag}-{}-{}-{}.redb",
        std::process::id(),
        nanos,
        seq
    ))
}

/// Bounded async retry for a synchronous redb file operation. Retries a small,
/// fixed number of times (dominated in practice by transient open contention — a
/// concurrent handle briefly holding the file's advisory lock), sleeping briefly
/// between attempts, then propagates the last error. **Fail-closed: never
/// converts an error to `Ok`.**
async fn redb_op_retry<T>(
    mut op: impl FnMut() -> Result<T, StateSyncError>,
) -> Result<T, StateSyncError> {
    const MAX_ATTEMPTS: u32 = 5;
    let mut last: Option<StateSyncError> = None;
    for attempt in 1..=MAX_ATTEMPTS {
        match op() {
            Ok(v) => return Ok(v),
            Err(e) => {
                last = Some(e);
                if attempt < MAX_ATTEMPTS {
                    tokio::time::sleep(Duration::from_millis(15 * u64::from(attempt))).await;
                }
            }
        }
    }
    Err(last.expect("retry loop runs at least once"))
}

/// Publish `src` to `dst` with a single atomic `rename` (same-filesystem by
/// construction — `src` is a sibling of `dst`). Fail-closed.
fn publish_atomically(src: &Path, dst: &Path) -> Result<(), StateSyncError> {
    std::fs::rename(src, dst).map_err(StateSyncError::Io)
}

/// Empty the named tables in the redb at `dst` — every row removed, the tables
/// themselves KEPT, and created if the remote object lacked them — leaving
/// every other table untouched. Used to scrub any remote-carried local-only
/// rows on a `Restored` download with no prior local file (finding 6a).
///
/// Kept, not dropped: a read-only open judges a store by its table set and
/// never creates a table in a store that holds anything
/// (`StateStore::init_db_read_only`), so a cold pod's first `GET` — before its
/// first `rocky run` — must find these tables present and empty, not missing.
/// Every table in [`crate::state::LOCAL_ONLY_TABLE_NAMES`] is a
/// `TableDefinition<&str, &[u8]>`, which is what the open below assumes.
fn clear_named_tables(dst: &Path, tables: &[&str]) -> Result<(), StateSyncError> {
    let io = |ctx: &str, e: &dyn std::fmt::Display| {
        StateSyncError::Io(std::io::Error::other(format!("{ctx}: {e}")))
    };
    let db =
        redb::Database::create(dst).map_err(|e| io("redb create dst for local-only clear", &e))?;
    let txn = db
        .begin_write()
        .map_err(|e| io("redb begin_write for local-only clear", &e))?;
    for name in tables {
        let def: redb::TableDefinition<&str, &[u8]> = redb::TableDefinition::new(name);
        let mut table = txn
            .open_table(def)
            .map_err(|e| io("redb open_table for local-only clear", &e))?;
        table
            .retain(|_, _| false)
            .map_err(|e| io("redb retain for local-only clear", &e))?;
    }
    txn.commit()
        .map_err(|e| io("redb commit for local-only clear", &e))?;
    drop(db);
    Ok(())
}

/// Copy the `tables` from the redb at `src` into the redb at `dst`, replacing
/// exactly those tables in `dst` and leaving every OTHER table in `dst`
/// untouched. `dst` is created if it does not exist.
///
/// Every table in [`crate::state::LOCAL_ONLY_TABLE_NAMES`] is a
/// `TableDefinition<&str, &[u8]>` (opaque serialized blobs), so the copy is a
/// faithful, bit-exact key/value replay over that shape. A table absent from
/// `src` leaves the corresponding table in `dst` present and EMPTY — never
/// absent: a read-only open judges a store by its table set and creates
/// nothing (`StateStore::init_db_read_only`), so the published file must carry
/// every table. If a future local-only table used a different key/value type,
/// opening it here would surface a redb type error rather than silently
/// mis-copying.
fn copy_named_tables(src: &Path, dst: &Path, tables: &[&str]) -> Result<(), StateSyncError> {
    // `iter()` lives on the `ReadableTable` trait — scope it locally.
    use redb::ReadableTable;

    let io = |ctx: &str, e: &dyn std::fmt::Display| {
        StateSyncError::Io(std::io::Error::other(format!("{ctx}: {e}")))
    };

    // `src` and `dst` are independent redb databases, so a read transaction on
    // one and a write transaction on the other coexist freely — stream each
    // table's rows straight across without an intermediate buffer.
    let src_db =
        redb::Database::open(src).map_err(|e| io("redb open src for local-only copy", &e))?;
    let read = src_db
        .begin_read()
        .map_err(|e| io("redb begin_read for local-only copy", &e))?;
    let dst_db =
        redb::Database::create(dst).map_err(|e| io("redb create dst for local-only copy", &e))?;
    let txn = dst_db
        .begin_write()
        .map_err(|e| io("redb begin_write for local-only copy", &e))?;
    for name in tables {
        let def: redb::TableDefinition<&str, &[u8]> = redb::TableDefinition::new(name);
        // Replace, don't merge: drop any existing copy of this table in `dst`
        // first, so a stale row can never survive.
        txn.delete_table(def)
            .map_err(|e| io("redb delete_table (dst) for local-only copy", &e))?;
        match read.open_table(def) {
            Ok(src_table) => {
                let mut dst_table = txn
                    .open_table(def)
                    .map_err(|e| io("redb open_table (dst) for local-only copy", &e))?;
                let iter = src_table
                    .iter()
                    .map_err(|e| io("redb iter for local-only copy", &e))?;
                for entry in iter {
                    let (k, v) = entry.map_err(|e| io("redb entry for local-only copy", &e))?;
                    dst_table
                        .insert(k.value(), v.value())
                        .map_err(|e| io("redb insert (dst) for local-only copy", &e))?;
                }
                // `dst_table` (the write borrow on `txn`) is dropped at the end
                // of the iteration, so `txn.commit()` has no outstanding borrow.
            }
            // A local-only table absent from `src` is legitimate: the
            // destination table is recreated EMPTY here, not left absent, so a
            // read-only open of the published file finds every table.
            Err(redb::TableError::TableDoesNotExist(_)) => {
                let _empty = txn
                    .open_table(def)
                    .map_err(|e| io("redb open_table (dst, empty) for local-only copy", &e))?;
            }
            Err(e) => return Err(io("redb open_table (src) for local-only copy", &e)),
        }
    }
    txn.commit()
        .map_err(|e| io("redb commit (dst) for local-only copy", &e))?;
    drop(dst_db);
    drop(read);
    drop(src_db);
    Ok(())
}

/// [`download_state`] retaining the hit/miss [`DownloadOutcome`] the tiered
/// backend needs to decide whether to fall through to its durable tier.
///
/// `dest_path` is where the downloaded bytes are written (a staging file, not
/// necessarily `local_path`), and `remote_key` is the object key derived ONCE
/// from the real local path by [`download_state`] — threaded explicitly (like
/// the upload path) so writing to a differently-named staging file does not
/// disturb namespaced key resolution.
async fn download_state_inner(
    config: &StateConfig,
    dest_path: &Path,
    remote_key: &str,
    gen_sink: Option<&mut Option<Generation>>,
) -> Result<DownloadOutcome, StateSyncError> {
    match config.backend {
        StateBackend::Local => {
            debug!("State backend: local (no sync needed)");
            Ok(DownloadOutcome::Absent)
        }
        StateBackend::S3 => {
            let bucket = config.s3_bucket.as_deref().ok_or_else(|| {
                StateSyncError::MissingConfig("s3".into(), "state.s3_bucket".into())
            })?;
            let prefix = config.s3_prefix.as_deref().unwrap_or(DEFAULT_S3_PREFIX);
            download_from_object_store(
                "s3",
                bucket,
                prefix,
                dest_path,
                remote_key,
                transfer_timeout(config),
                gen_sink,
            )
            .await
        }
        StateBackend::Gcs => {
            let bucket = config.gcs_bucket.as_deref().ok_or_else(|| {
                StateSyncError::MissingConfig("gcs".into(), "state.gcs_bucket".into())
            })?;
            let prefix = config.gcs_prefix.as_deref().unwrap_or(DEFAULT_GCS_PREFIX);
            download_from_object_store(
                "gs",
                bucket,
                prefix,
                dest_path,
                remote_key,
                transfer_timeout(config),
                gen_sink,
            )
            .await
        }
        StateBackend::Valkey => {
            download_from_valkey(config, dest_path, remote_key, CarryForward::Yes).await
        }
        // Under `cas` the tiered read is generation-validated against the
        // durable tier — the Valkey copy may only short-circuit when it proves
        // it holds the object's CURRENT generation.
        StateBackend::Tiered
            if requested_concurrency_control(config).0 == ConcurrencyControl::Cas =>
        {
            tiered_cas_download(config, dest_path, remote_key, gen_sink).await
        }
        StateBackend::Tiered => {
            // Try Valkey first (fast), fall back to S3 (durable).
            //
            // KNOWN LIMITATION — this is the `concurrency_control = off` tiered
            // read, where a genuine Valkey HIT short-circuits the durable S3
            // tier below. `off` writes carry no generation, so there is nothing
            // to validate a cached copy against: if state was written to S3
            // while Valkey holds a stale snapshot, a read through this path can
            // see the stale copy. Set `concurrency_control = "cas"` for the
            // coherent tiered read (the arm above) — that is the fix, and it is
            // opt-in precisely so `off` stays byte-identical to pre-CAS
            // releases.
            info!("State backend: tiered (Valkey → S3 fallback)");
            let valkey_config = StateConfig {
                backend: StateBackend::Valkey,
                ..config.clone()
            };
            let s3_config = StateConfig {
                backend: StateBackend::S3,
                ..config.clone()
            };

            // Only a genuine Valkey HIT (the leg wrote the file) short-circuits.
            // A MISS (`Absent`) or an error both fall through to the durable S3
            // tier. Crucially this no longer consults `dest_path.exists()`: a
            // stale local file left by a previous run must NOT be mistaken for a
            // fresh Valkey hit (which would skip the S3 fallback and resume from
            // stale state).
            //
            // `gen_sink` is deliberately dropped on both legs: an `off` tiered
            // read captures no base, so nothing can later mistake `None` for
            // "the object was absent" — `cas_enabled()` never routes a base
            // capture through this arm.
            //
            // The cache leg reads ONLY the current key (`CarryForward::No`).
            // An older Valkey copy is never trusted over the durable tier: a
            // miss falls through to S3, whose leg does the carry-forward.
            match download_from_valkey(&valkey_config, dest_path, remote_key, CarryForward::No)
                .await
            {
                Ok(DownloadOutcome::Restored) => {
                    debug!("State restored from Valkey");
                    Ok(DownloadOutcome::Restored)
                }
                Ok(DownloadOutcome::Absent) => {
                    debug!("Valkey miss, trying S3");
                    Box::pin(download_state_inner(
                        &s3_config, dest_path, remote_key, None,
                    ))
                    .await
                }
                Err(e) => {
                    debug!(error = %e, "Valkey error, trying S3");
                    Box::pin(download_state_inner(
                        &s3_config, dest_path, remote_key, None,
                    ))
                    .await
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tiered cache coherence (ADR-CONCURRENCY D5)
// ---------------------------------------------------------------------------
//
// The tiered backend caches the state blob in Valkey in front of a durable S3
// object. Under `concurrency_control = "cas"` the durable object decides what
// committed for THIS write path, and the cache is a read accelerator that must
// never be able to answer with something the durable tier disagrees with.
//
// Scope, stated up front: this covers every write of the shared blob — the
// end-of-run upload (`RemoteStateSession::finalize`) and every ledger seam
// (`rocky policy`, `rocky gc`, `rocky restore`, and `rocky apply`'s governed
// rule decision + verify-after custody), which all publish through
// [`LedgerSeamSession`] (#1242). Under effective CAS none of them writes the
// shared blob unconditionally; the behaviour is identical on `s3`/`gcs`.
//
// The mechanism is a FRESHNESS-CHECKED cache entry: the generation the durable
// CAS committed at is framed into the cached value itself, in one atomic
// Valkey `SET`. A reader reads the durable object's current generation (a
// cheap `HEAD`) and accepts the cached blob only on an exact match.
//
// # Trust boundary (a named assumption, not a verified property)
//
// The framed generation is NOT authenticated. The check proves **freshness,
// not provenance**: it establishes that the entry corresponds to the durable
// object's current generation, not that Rocky wrote it. Anyone who can write
// the cache key can pair the current generation with an arbitrary body and be
// believed.
//
// Rocky trusts the configured state tiers as infrastructure, and that
// assumption is not new or specific to the cache: nothing authenticates the
// durable object either, so whoever can write the S3 object owns the state
// outright. Under `off` the cached blob is served with no check at all, so
// `cas` strictly tightens this trust rather than introducing it. Authenticating
// the payload would need a body digest committed to durable storage — a
// separate design, not something the generation header can be made to do.
//
// Two consequences worth stating explicitly, because they are what make the
// protocol robust rather than merely careful:
//
// - **Invalidation is hygiene, not a correctness dependency.** A `DEL` that
//   fails (or a process that dies before issuing one) leaves an entry whose
//   framed generation is, by construction, older than the durable object's —
//   so the next read rejects it anyway. Every crash point between the durable
//   commit and the cache populate lands on "cache holds an older generation",
//   which is the safe direction.
// - **A single key, not a sidecar.** Storing the generation in a second key
//   would let two writers interleave blob and generation writes and leave a
//   STALE blob paired with the CURRENT generation — an entry that validates
//   and is wrong. One value, one `SET`, no pairing hazard.

/// Magic + framing version for a coherent tiered cache entry.
///
/// Bumping the trailing digit retires every previously written entry for free:
/// an unrecognised magic reads as a cache miss, so the framing can evolve
/// without a migration step.
const COHERENT_CACHE_MAGIC: &[u8; 8] = b"RKYSGEN1";

/// Valkey key for the generation-validated tiered cache entry.
///
/// Deliberately DISJOINT from [`valkey_state_key`]'s raw-blob key. The coherent
/// entry carries a framed header, and a pod still on `concurrency_control =
/// "off"` reads the raw key and would interpret a frame as redb bytes. Separate
/// keys let a fleet roll `off → cas` one pod at a time without either side
/// mis-reading the other's value.
///
/// The `cas:` discriminator sits BEFORE the schema-version segment, which is
/// what makes the two namespaces provably disjoint: `valkey_state_key` yields
/// `<prefix>v9:<key>` and this yields `<prefix>cas:v9:<key>`, so a collision
/// would require the version segment to be the literal `cas` — impossible, it
/// is always `v` followed by digits. Placing the discriminator *after* the
/// segment would not be safe: a namespaced state file whose name began `cas:`
/// would then collide with another namespace's coherent key.
fn valkey_coherent_key(prefix: &str, remote_key: &str) -> String {
    format!("{prefix}cas:{}:{remote_key}", schema_version_segment())
}

/// Frame `blob` together with the durable generation it was committed at.
///
/// Layout: `MAGIC(8) || u32-le header length || header JSON || state bytes`.
///
/// # Errors
///
/// Only for an un-encodable generation or a header beyond `u32` — both
/// impossible in practice; they are surfaced rather than panicked on.
fn frame_coherent_cache_entry(
    generation: &Generation,
    blob: &[u8],
) -> Result<Vec<u8>, StateSyncError> {
    let header = serde_json::to_vec(generation).map_err(|e| {
        StateSyncError::Valkey(format!("failed to encode tiered cache generation: {e}"))
    })?;
    let header_len = u32::try_from(header.len())
        .map_err(|_| StateSyncError::Valkey("tiered cache generation header exceeds u32".into()))?;
    let mut framed = Vec::with_capacity(
        COHERENT_CACHE_MAGIC.len() + size_of::<u32>() + header.len() + blob.len(),
    );
    framed.extend_from_slice(COHERENT_CACHE_MAGIC);
    framed.extend_from_slice(&header_len.to_le_bytes());
    framed.extend_from_slice(&header);
    framed.extend_from_slice(blob);
    Ok(framed)
}

/// Parse a framed cache entry into `(generation, state bytes)`.
///
/// TOTAL and panic-free over arbitrary input. The value is externally supplied
/// — another pod, an older binary, an operator poking at Valkey — so every
/// malformed shape (wrong magic, truncated length, over-long header, invalid
/// JSON) reads as `None`, i.e. a plain cache miss. It is never a panic and
/// never an error that could fail a run: the durable tier is right there.
fn parse_coherent_cache_entry(value: &[u8]) -> Option<(Generation, &[u8])> {
    let rest = value.strip_prefix(COHERENT_CACHE_MAGIC.as_slice())?;
    let (len_bytes, rest) = rest.split_at_checked(size_of::<u32>())?;
    let header_len = u32::from_le_bytes(len_bytes.try_into().ok()?) as usize;
    let (header, blob) = rest.split_at_checked(header_len)?;
    let generation = serde_json::from_slice::<Generation>(header).ok()?;
    Some((generation, blob))
}

/// The Valkey peer URL from `[state] valkey_url`.
fn valkey_url(config: &StateConfig) -> Result<String, StateSyncError> {
    Ok(config
        .valkey_url
        .as_ref()
        .map(RedactedString::expose)
        .ok_or_else(|| StateSyncError::MissingConfig("valkey".into(), "state.valkey_url".into()))?
        .to_string())
}

/// The configured Valkey key prefix (or the default).
fn valkey_key_prefix(config: &StateConfig) -> &str {
    config
        .valkey_prefix
        .as_deref()
        .unwrap_or(DEFAULT_VALKEY_PREFIX)
}

/// Run one blocking Valkey command under the shared transfer-timeout budget.
///
/// The `redis` crate's sync client blocks its thread, so — exactly as
/// [`download_from_valkey`] does — the work is offloaded to the blocking pool
/// and capped by `transfer_timeout_seconds` so a dead peer cannot stall a run.
async fn valkey_exec<T, F>(config: &StateConfig, op: F) -> Result<T, StateSyncError>
where
    F: FnOnce(&mut redis::Connection) -> Result<T, StateSyncError> + Send + 'static,
    T: Send + 'static,
{
    let url = valkey_url(config)?;
    with_transfer_timeout(transfer_timeout(config), async move {
        let join = tokio::task::spawn_blocking(move || {
            let client =
                redis::Client::open(url).map_err(|e| StateSyncError::Valkey(e.to_string()))?;
            let mut conn = client
                .get_connection()
                .map_err(|e| StateSyncError::Valkey(e.to_string()))?;
            op(&mut conn)
        })
        .await;
        match join {
            Ok(inner) => inner,
            Err(e) => Err(StateSyncError::Valkey(format!(
                "valkey worker task failed: {e}"
            ))),
        }
    })
    .await
}

/// `GET` the coherent cache entry (`None` = miss).
async fn coherent_cache_get(
    config: &StateConfig,
    key: String,
) -> Result<Option<Vec<u8>>, StateSyncError> {
    #[cfg(any(test, feature = "test-support"))]
    if test_support::fake_valkey_installed() {
        return Ok(test_support::fake_valkey_get(&key));
    }
    valkey_exec(config, move |conn| {
        redis::cmd("GET")
            .arg(&key)
            .query::<Option<Vec<u8>>>(conn)
            .map_err(|e| StateSyncError::Valkey(e.to_string()))
    })
    .await
}

/// `SET` the coherent cache entry.
async fn coherent_cache_set(
    config: &StateConfig,
    key: String,
    value: Vec<u8>,
) -> Result<(), StateSyncError> {
    #[cfg(any(test, feature = "test-support"))]
    if test_support::fake_valkey_installed() {
        return test_support::fake_valkey_set(&key, value)
            .map_err(|()| StateSyncError::Valkey("injected cache SET failure (test seam)".into()));
    }
    valkey_exec(config, move |conn| {
        redis::cmd("SET")
            .arg(&key)
            .arg(value)
            .query::<()>(conn)
            .map_err(|e| StateSyncError::Valkey(e.to_string()))
    })
    .await
}

/// `DEL` the coherent cache entry.
async fn coherent_cache_del(config: &StateConfig, key: String) -> Result<(), StateSyncError> {
    #[cfg(any(test, feature = "test-support"))]
    if test_support::fake_valkey_installed() {
        return test_support::fake_valkey_del(&key)
            .map_err(|()| StateSyncError::Valkey("injected cache DEL failure (test seam)".into()));
    }
    valkey_exec(config, move |conn| {
        redis::cmd("DEL")
            .arg(&key)
            .query::<()>(conn)
            .map_err(|e| StateSyncError::Valkey(e.to_string()))
    })
    .await
}

/// Drop the coherent cache entry. **Best-effort by design** — see the module
/// note above: a surviving entry carries a superseded generation and is
/// rejected on read, so a failed `DEL` costs a wasted round-trip, never
/// correctness.
async fn invalidate_coherent_cache(config: &StateConfig, remote_key: &str) {
    let key = valkey_coherent_key(valkey_key_prefix(config), remote_key);
    match coherent_cache_del(config, key.clone()).await {
        Ok(()) => debug!(key = %key, "tiered cache entry invalidated"),
        Err(e) => warn!(
            error = %e,
            key = %key,
            outcome = "invalidate_failed",
            "could not invalidate the tiered state cache entry; it carries a superseded \
             generation and will be rejected on read"
        ),
    }
}

/// Publish the just-committed state as the tiered cache entry, tagged with the
/// generation the durable compare-and-swap returned.
///
/// Best-effort on failure, deliberately: the durable write has ALREADY
/// committed, so failing the run over a cache miss would report failure for
/// state that is safely persisted. What it must not do is leave a half-known
/// entry hittable, so any failure invalidates.
async fn populate_coherent_cache(
    config: &StateConfig,
    remote_key: &str,
    committed: &Generation,
    blob: &[u8],
) {
    let key = valkey_coherent_key(valkey_key_prefix(config), remote_key);
    let framed = match frame_coherent_cache_entry(committed, blob) {
        Ok(framed) => framed,
        Err(e) => {
            warn!(error = %e, "could not frame the tiered state cache entry");
            invalidate_coherent_cache(config, remote_key).await;
            return;
        }
    };
    match coherent_cache_set(config, key.clone(), framed).await {
        Ok(()) => debug!(key = %key, "tiered cache populated with the committed generation"),
        Err(e) => {
            warn!(
                error = %e,
                key = %key,
                outcome = "cache_populate_failed",
                "tiered state cache populate failed after a durable commit; invalidating so \
                 no stale entry can be served (the run's state IS committed)"
            );
            invalidate_coherent_cache(config, remote_key).await;
        }
    }
}

/// The durable tier's CURRENT generation for `remote_key` — the only thing a
/// cached entry may be validated against.
async fn durable_generation(
    config: &StateConfig,
    remote_key: &str,
) -> Result<RemoteVersion, StateSyncError> {
    let bucket = config
        .s3_bucket
        .as_deref()
        .ok_or_else(|| StateSyncError::MissingConfig("tiered".into(), "state.s3_bucket".into()))?;
    let prefix = config.s3_prefix.as_deref().unwrap_or(DEFAULT_S3_PREFIX);
    let provider = cloud_provider("s3", bucket, prefix)?;
    let key = object_store_state_key(remote_key);
    with_transfer_timeout(transfer_timeout(config), probe_generation(&provider, &key)).await
}

/// Generation probe for the durable tier, isolated behind a test seam (mirrors
/// [`probe_exists`]). Production behaviour is exactly
/// `provider.remote_generation(key)`.
async fn probe_generation(
    provider: &ObjectStoreProvider,
    key: &str,
) -> Result<RemoteVersion, StateSyncError> {
    #[cfg(any(test, feature = "test-support"))]
    if test_support::take_object_store_head_fault() {
        return Err(StateSyncError::S3Download(
            "injected generation-probe failure (test seam)".into(),
        ));
    }
    provider
        .remote_generation(key)
        .await
        .map_err(StateSyncError::from)
}

/// Read the coherent cache entry and write it to `dest_path` **only** if its
/// framed generation equals `durable`.
///
/// Returns `Ok(true)` when the cache served the read. Every other outcome —
/// miss, unparseable value, generation mismatch — is `Ok(false)`, which the
/// caller resolves by reading the durable tier. A definitively-stale entry is
/// invalidated on the way out (hygiene; the mismatch check already made it
/// unusable).
///
/// # Errors
///
/// Transport failures reaching the cache propagate so the caller can log them;
/// the caller treats them exactly like a miss.
async fn read_coherent_cache(
    config: &StateConfig,
    remote_key: &str,
    durable: &Generation,
    dest_path: &Path,
) -> Result<bool, StateSyncError> {
    let key = valkey_coherent_key(valkey_key_prefix(config), remote_key);
    let Some(value) = coherent_cache_get(config, key.clone()).await? else {
        debug!(key = %key, "tiered cache miss; reading the durable tier");
        return Ok(false);
    };
    let Some((cached, blob)) = parse_coherent_cache_entry(&value) else {
        warn!(
            key = %key,
            outcome = "cache_unparseable",
            "tiered cache entry is not a coherent frame; ignoring it and reading the durable tier"
        );
        invalidate_coherent_cache(config, remote_key).await;
        return Ok(false);
    };
    if &cached != durable {
        debug!(
            key = %key,
            outcome = "cache_stale",
            "tiered cache entry carries a superseded generation; reading the durable tier"
        );
        invalidate_coherent_cache(config, remote_key).await;
        return Ok(false);
    }
    tokio::fs::write(dest_path, blob).await?;
    info!(
        size = blob.len(),
        outcome = "ok",
        "state restored from the tiered cache (generation validated against the durable tier)"
    );
    Ok(true)
}

/// The `concurrency_control = "cas"` tiered read (ADR-CONCURRENCY D5).
///
/// ```text
/// HEAD durable object ──► Present(G) ──► cache GET ──► frame.generation == G ──► serve from cache
///        │                    │              │                  │
///        │                    │              │                  └─ mismatch ─┐
///        │                    │              └─ miss / unparseable ──────────┤
///        │                    ├─ Absent / PresentUnversioned ────────────────┤
///        └─ Err ──────────────────────────────────────────────────────────────┤
///                                                                            ▼
///                                                          read the DURABLE S3 tier
/// ```
///
/// One invariant carries the whole design: **the cache short-circuits only on a
/// proven generation match; every other path reads the durable tier.** So the
/// worst case is the cost of the plain `s3` backend, and no interleaving of
/// *conforming* writers — writers that publish through
/// [`populate_coherent_cache`], which only ever pairs a generation with the
/// bytes committed at it — leaves a reader observing a cache entry newer-looking
/// than durable truth. It is a freshness check, not an authentication one: a
/// hand-crafted entry pairing the current generation with a different body would
/// be believed (see the trust boundary in this module's header).
///
/// The base generation this run CAS-commits against comes from the durable
/// tier either way: from the validated `HEAD` on a cache hit, or from the same
/// `GET` that fetched the bytes on a fall-through. It is never derived from the
/// cache.
///
/// Cost: one metadata `HEAD` on every tiered read, and a second one on the
/// fall-through (the durable leg runs its own existence probe). A `HEAD` is
/// bytes; the ledger it avoids downloading is megabytes, which is the trade the
/// cache exists to make.
async fn tiered_cas_download(
    config: &StateConfig,
    dest_path: &Path,
    remote_key: &str,
    mut gen_sink: Option<&mut Option<Generation>>,
) -> Result<DownloadOutcome, StateSyncError> {
    info!("State backend: tiered (generation-validated cache → S3)");

    match durable_generation(config, remote_key).await {
        Ok(RemoteVersion::Present(durable)) => {
            match read_coherent_cache(config, remote_key, &durable, dest_path).await {
                Ok(true) => {
                    if let Some(sink) = gen_sink.as_deref_mut() {
                        *sink = Some(durable);
                    }
                    return Ok(DownloadOutcome::Restored);
                }
                Ok(false) => {}
                Err(e) => warn!(
                    error = %e,
                    "tiered cache read failed; reading the durable tier"
                ),
            }
        }
        // Nothing durable exists, so nothing can validate a cached entry —
        // the cache is skipped entirely rather than trusted unchecked.
        Ok(RemoteVersion::Absent) => {
            debug!("durable tier has no state object; the tiered cache cannot be validated")
        }
        Ok(RemoteVersion::PresentUnversioned) => warn!(
            outcome = "unversioned",
            "durable state object carries no ETag/version; the tiered cache cannot be \
             validated, reading the durable tier"
        ),
        Err(e) => warn!(
            error = %e,
            "durable generation probe failed; reading the durable tier"
        ),
    }

    let s3_config = StateConfig {
        backend: StateBackend::S3,
        ..config.clone()
    };
    Box::pin(download_state_inner(
        &s3_config, dest_path, remote_key, gen_sink,
    ))
    .await
}

/// Uploads state from a local file to remote storage after a run.
///
/// Applies `config.retry` to transient failures and `config.on_upload_failure`
/// to the final result — by default (`Skip`) a post-retry failure is logged
/// and reported back as `Ok` so the run continues in degraded mode, matching
/// the de-facto behaviour of existing callers that `warn + continue` on upload
/// errors. Set `on_upload_failure = "fail"` for strict environments that must
/// treat state durability as a hard requirement.
///
/// Tables listed in [`crate::state::LOCAL_ONLY_TABLE_NAMES`] are filtered out
/// of the remote copy — the schema cache is one of them, so fresh clones don't
/// inherit another machine's stale types.
///
/// `replicate_schema_cache` is `[cache.schemas] replicate`. When it is `true`
/// the schema cache is NOT stripped, so it travels with the rest of the state
/// (#1620). It defaults to `false`, and at `false` the uploaded bytes are
/// identical to what this function produced before the setting was wired.
pub async fn upload_state(
    config: &StateConfig,
    local_path: &Path,
    replicate_schema_cache: bool,
) -> Result<(), StateSyncError> {
    upload_state_with_excluded_tables(
        config,
        local_path,
        crate::state::local_only_table_names(replicate_schema_cache),
    )
    .await
}

/// End-of-run state upload, suppressed when the local store was recreated after
/// a forward-incompatible schema mismatch.
///
/// This is the **no-clobber** half of the mixed-version safety invariant. When
/// `recreated_for_forward_incompat` is true, the local `state.redb` was
/// bootstrapped fresh under `on_schema_mismatch = recreate` because the on-disk
/// state was written by a *newer* binary — so the local state is a downgrade.
/// Uploading it back would overwrite the newer shared state that
/// already-upgraded pods depend on, so the upload is deliberately skipped.
/// Otherwise the local state is uploaded via [`upload_state`].
///
/// The open-time half of the invariant (refusing to *run* against
/// forward-incompatible on-disk state, or recreating under policy) lives in
/// [`crate::state`]'s `open_with_policy`. The periodic mid-run uploader in the
/// CLI is gated by the same flag at spawn time (it is simply not started when
/// the store was recreated), so both upload paths honor the suppression.
pub async fn upload_state_unless_recreated(
    recreated_for_forward_incompat: bool,
    config: &StateConfig,
    local_path: &Path,
    replicate_schema_cache: bool,
) -> Result<(), StateSyncError> {
    if recreated_for_forward_incompat {
        info!(
            outcome = "skipped_forward_incompat_recreate",
            "skipping end-of-run state upload: local state was recreated after a \
             forward-incompatible schema mismatch (on_schema_mismatch = recreate); \
             leaving the newer shared state intact"
        );
        return Ok(());
    }
    upload_state(config, local_path, replicate_schema_cache).await
}

/// Variant of [`upload_state`] that lets the caller override the list of
/// redb tables stripped from the remote copy.
///
/// When `excluded_tables` is empty the local file is uploaded as-is (no
/// temp-file copy, no overhead). Otherwise the local file is copied to a
/// temp path, the listed tables are deleted from the copy, and the copy is
/// uploaded. The local `state.redb` is never modified.
///
/// Errors from the temp-copy step (I/O, redb open, delete_table, commit) are
/// surfaced as `StateSyncError::Io` or the underlying error string wrapped
/// into a transient upload error — they share the same failure-mode policy
/// as any other transient upload error (retry then skip/fail per
/// `on_upload_failure`).
pub async fn upload_state_with_excluded_tables(
    config: &StateConfig,
    local_path: &Path,
    excluded_tables: &[&str],
) -> Result<(), StateSyncError> {
    if !local_path.exists() {
        debug!("No local state file to upload");
        return Ok(());
    }
    // Derive the remote object key ONCE from the original local path, before
    // any strip/scratch copy. The strip path writes the filtered copy under
    // `std::env::temp_dir()`, whose parent is `/tmp` — re-deriving the key
    // from the scratch path would always yield the legacy `state.redb`, so a
    // namespaced upload would silently clobber the shared object while the
    // download read the (never-written) namespaced key. Computing it here and
    // threading it down keeps the key tied to the namespace regardless of
    // which physical file reaches the upload leaf.
    let remote_key = remote_state_key(local_path);

    if excluded_tables.is_empty() || matches!(config.backend, StateBackend::Local) {
        // Fast path: nothing to strip or local backend is a no-op anyway.
        let result = dispatch_upload(config, local_path, &remote_key).await;
        return apply_upload_failure_policy(config, result);
    }

    // Filter path: copy, strip, upload the copy.
    let scratch = match strip_local_only_tables(local_path, excluded_tables) {
        Ok(path) => path,
        Err(e) => {
            warn!(
                error = %e,
                outcome = "filter_failed",
                "failed to build replicate-filtered state copy; refusing to upload unfiltered \
                 local-only data (fail-closed, finding 6b)"
            );
            // Finding 6(b): NEVER fall back to uploading the unfiltered local
            // file — that would leak the local-only tables (another pod's `jobs`,
            // this machine's `schema_cache`) into the shared remote snapshot.
            // Surface the filter failure as an upload error subject to
            // `on_upload_failure`: `Skip` skips this upload in degraded mode (no
            // unfiltered upload happens), `Fail` aborts. Either way, no
            // unfiltered local-only data is ever uploaded.
            return apply_upload_failure_policy(config, Err(e));
        }
    };
    let result = dispatch_upload(config, &scratch, &remote_key).await;
    // Scratch files live under `std::env::temp_dir()`; best-effort cleanup
    // on success or failure. A leaked scratch DB on this run is a small
    // cost the OS cleans up at reboot.
    let _ = std::fs::remove_file(&scratch);
    apply_upload_failure_policy(config, result)
}

/// Upload an already-built, already-filtered state **snapshot** to the remote.
///
/// Unlike [`upload_state`], this performs NO local-only table stripping — the
/// snapshot was produced by [`StateStore::snapshot_to_excluding`], which already
/// omitted the excluded tables under a single MVCC read (torn-read-free). The
/// caller passes the schema-namespaced `remote_key`, derived ONCE from the real
/// state path — NEVER re-derived from the scratch temp path, which would resolve
/// to the legacy `state.redb` key and clobber namespaced state (the same
/// invariant [`upload_state_with_excluded_tables`] documents). The configured
/// `on_upload_failure` policy applies exactly as in [`upload_state`].
pub async fn upload_state_snapshot(
    config: &StateConfig,
    snapshot_path: &Path,
    remote_key: &str,
) -> Result<(), StateSyncError> {
    if !snapshot_path.exists() {
        debug!("No state snapshot to upload");
        return Ok(());
    }
    let result = dispatch_upload(config, snapshot_path, remote_key).await;
    apply_upload_failure_policy(config, result)
}

/// A temp snapshot path removed on drop, so an aborted-mid-tick periodic upload
/// leaks neither a scratch snapshot file nor a late upload.
struct ScratchGuard {
    path: PathBuf,
}

impl ScratchGuard {
    /// Reserve a unique scratch path under the system temp dir. The file itself
    /// is created by [`StateStore::snapshot_to_excluding`]; this owns its name
    /// and its cleanup.
    fn new() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "rocky-state-snapshot-{}-{}.redb",
            std::process::id(),
            nanos
        ));
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for ScratchGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Build a temp redb copy of `local_path` with `excluded_tables` removed.
///
/// Returns the path of the temp copy. Caller is responsible for deleting
/// it. Used by the replicate-filtered upload path. Kept small and
/// synchronous — the schema cache is the only local-only table today,
/// its footprint is bounded by TTL, and the round-trip file copy is
/// proportional to total state size (order of single-digit megabytes for
/// a typical project).
fn strip_local_only_tables(
    local_path: &Path,
    excluded_tables: &[&str],
) -> Result<PathBuf, StateSyncError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    // A process-unique sequence disambiguates two uploads that observe the same
    // `nanos` on a coarse clock — the scratch file is read back and uploaded, so
    // a collision would publish another upload's bytes. The name carries no
    // contract; it is removed after the upload.
    static SCRATCH_SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let sequence = SCRATCH_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let scratch = std::env::temp_dir().join(format!(
        "rocky-state-upload-{}-{}-{}.redb",
        std::process::id(),
        nanos,
        sequence
    ));
    std::fs::copy(local_path, &scratch)?;

    // Open the copy and drop the excluded tables. `delete_table` returns
    // `Ok(false)` when the table doesn't exist, which is the right
    // behaviour: an older copy that never had `schema_cache` just no-ops
    // and uploads cleanly.
    let db = redb::Database::create(&scratch).map_err(|e| {
        StateSyncError::Io(std::io::Error::other(format!(
            "redb open for state filter failed: {e}"
        )))
    })?;
    let txn = db.begin_write().map_err(|e| {
        StateSyncError::Io(std::io::Error::other(format!(
            "redb begin_write for state filter failed: {e}"
        )))
    })?;
    for name in excluded_tables {
        let def: redb::TableDefinition<&str, &[u8]> = redb::TableDefinition::new(name);
        // Finding C: a genuinely-absent table is a legitimate no-op — the v2 API
        // returns `Ok(false)` for it (an older copy that never had `schema_cache`
        // uploads cleanly). But a REAL `delete_table` error (e.g. a table with an
        // incompatible definition on an otherwise-valid redb) must FAIL the
        // filter — swallowing it would commit an UNFILTERED scratch and upload
        // this pod's local-only rows (`jobs` / `schema_cache`) to shared remote
        // state. `TableDoesNotExist` (if a redb version surfaces it as an error
        // rather than `Ok(false)`) stays non-fatal; every other error propagates.
        match txn.delete_table(def) {
            Ok(_) => {}
            Err(redb::TableError::TableDoesNotExist(_)) => {}
            Err(e) => {
                return Err(StateSyncError::Io(std::io::Error::other(format!(
                    "redb delete_table('{name}') for state filter failed: {e}"
                ))));
            }
        }
    }
    txn.commit().map_err(|e| {
        StateSyncError::Io(std::io::Error::other(format!(
            "redb commit for state filter failed: {e}"
        )))
    })?;
    // Drop the db handle so the OS fully releases any mmap before the
    // scratch file is opened by the upload path.
    drop(db);
    Ok(scratch)
}

/// Internal upload dispatch — runs the raw upload (with retry) without
/// applying the `on_upload_failure` policy. Tiered recursion uses this
/// directly so the skip/fail decision is evaluated exactly once at the
/// outermost `upload_state` call, not per-leg.
///
/// `remote_key` is the object key derived from the *original* local path by
/// [`upload_state_with_excluded_tables`]. It is threaded explicitly (rather
/// than re-derived from `local_path`) because the filter path passes a scratch
/// copy under `std::env::temp_dir()` whose parent would resolve back to the
/// legacy `state.redb` key — see the comment at the top of
/// [`upload_state_with_excluded_tables`].
async fn dispatch_upload(
    config: &StateConfig,
    local_path: &Path,
    remote_key: &str,
) -> Result<(), StateSyncError> {
    // Every unconditional upload of the shared state object passes here, so
    // this is the one place the `cas-required` marker is enforced (#1228).
    // Checked once, before any leg — a refused tiered upload must not reach
    // Valkey either.
    if !matches!(config.backend, StateBackend::Local) {
        with_transfer_timeout(
            transfer_timeout(config),
            refuse_unconditional_upload_if_cas_required(config, remote_key),
        )
        .await?;
    }
    dispatch_upload_legs(config, local_path, remote_key).await
}

/// The per-backend unconditional upload, without the `cas-required` check
/// [`dispatch_upload`] performs once up front. Tiered recursion uses this so
/// the check is not repeated per leg.
async fn dispatch_upload_legs(
    config: &StateConfig,
    local_path: &Path,
    remote_key: &str,
) -> Result<(), StateSyncError> {
    match config.backend {
        StateBackend::Local => {
            debug!("State backend: local (no sync needed)");
            Ok(())
        }
        StateBackend::S3 => {
            let bucket = config.s3_bucket.as_deref().ok_or_else(|| {
                StateSyncError::MissingConfig("s3".into(), "state.s3_bucket".into())
            })?;
            let prefix = config.s3_prefix.as_deref().unwrap_or(DEFAULT_S3_PREFIX);
            upload_to_object_store(
                "s3",
                bucket,
                prefix,
                local_path,
                remote_key,
                transfer_timeout(config),
                &config.retry,
            )
            .await
        }
        StateBackend::Gcs => {
            let bucket = config.gcs_bucket.as_deref().ok_or_else(|| {
                StateSyncError::MissingConfig("gcs".into(), "state.gcs_bucket".into())
            })?;
            let prefix = config.gcs_prefix.as_deref().unwrap_or(DEFAULT_GCS_PREFIX);
            upload_to_object_store(
                "gs",
                bucket,
                prefix,
                local_path,
                remote_key,
                transfer_timeout(config),
                &config.retry,
            )
            .await
        }
        StateBackend::Valkey => upload_to_valkey(config, local_path, remote_key).await,
        StateBackend::Tiered => {
            // Write to both Valkey (fast) and S3 (durable)
            info!("State backend: tiered (uploading to Valkey + S3)");
            let valkey_config = StateConfig {
                backend: StateBackend::Valkey,
                ..config.clone()
            };
            let s3_config = StateConfig {
                backend: StateBackend::S3,
                ..config.clone()
            };

            // Valkey first (fast, best-effort). Recurse into the *inner*
            // dispatch so `on_upload_failure` is not applied here — the
            // outer `upload_state` owns that decision for the tiered leg
            // as a whole. The same `remote_key` flows to both legs so a
            // namespaced upload lands at `<ns>.redb` on Valkey and S3 alike.
            //
            // KNOWN LIMITATION — this is the `concurrency_control = off` tiered
            // upload, where a Valkey failure here is SWALLOWED (only the durable
            // S3 leg below is required) even under `on_upload_failure = Fail`.
            // Combined with the `off` read-side short-circuit (a Valkey HIT skips
            // S3), a stale cache entry can survive and shadow durably-written
            // state on the next read. `off` writes carry no generation, so there
            // is nothing an invalidation could be made reliable against. Set
            // `concurrency_control = "cas"` for the coherent tiered write
            // (`dispatch_upload_cas`), which commits to S3 first and then
            // populates-or-invalidates a generation-tagged cache entry.
            if let Err(e) =
                Box::pin(dispatch_upload_legs(&valkey_config, local_path, remote_key)).await
            {
                warn!(error = %e, "Valkey upload failed (non-fatal, S3 is durable)");
            }

            // S3 second (durable, required)
            Box::pin(dispatch_upload_legs(&s3_config, local_path, remote_key)).await
        }
    }
}

/// End-of-run compare-and-swap upload for `concurrency_control = "cas"` on a
/// backend with a durable object tier (`s3` / `gcs` / `tiered`).
///
/// Strips local-only tables exactly as [`upload_state`] does, then conditionally
/// uploads against `base` (the generation captured at
/// [`download_state_with_generation`]). On `tiered` the conditional upload goes
/// to the durable S3 leg and [`dispatch_upload_cas`] keeps the Valkey tier
/// coherent with its outcome:
/// - `Committed` → `Ok(())`.
/// - `Conflict` → [`StateSyncError::CasConflict`] — another writer committed
///   since this run's download; fail-closed, and NEVER swallowed by
///   `on_upload_failure` (a lost cross-pod race is not a transient upload blip).
/// - A genuine transport error is subject to `on_upload_failure` exactly as
///   [`upload_state`].
///
/// `base = None` (the object was absent at acquire) maps to a create-if-absent
/// write, so a concurrent bootstrap writer also conflicts rather than
/// blind-overwriting.
async fn upload_state_cas(
    config: &StateConfig,
    local_path: &Path,
    base: Option<&Generation>,
    replicate_schema_cache: bool,
) -> Result<(), StateSyncError> {
    if !local_path.exists() {
        debug!("No local state file to upload");
        return Ok(());
    }
    let remote_key = remote_state_key(local_path);
    // Same posture as the non-CAS upload leg. Wiring one and not the other
    // would make `[cache.schemas] replicate = true` a no-op for every project
    // on `concurrency_control = "cas"`, silently (#1620).
    let excluded = crate::state::local_only_table_names(replicate_schema_cache);

    // Resolve the file to upload: a stripped copy when there are local-only
    // tables, else the file as-is. Same fail-closed filter contract as
    // `upload_state_with_excluded_tables` — never upload the unfiltered file.
    let (upload_path, scratch) = if excluded.is_empty() {
        (local_path.to_path_buf(), None)
    } else {
        match strip_local_only_tables(local_path, excluded) {
            Ok(p) => (p.clone(), Some(p)),
            Err(e) => {
                warn!(
                    error = %e,
                    outcome = "filter_failed",
                    "failed to build replicate-filtered state copy; refusing to upload \
                     unfiltered local-only data (fail-closed)"
                );
                return apply_upload_failure_policy(config, Err(e));
            }
        }
    };

    let outcome = dispatch_upload_cas(config, &upload_path, &remote_key, base).await;
    if let Some(scratch) = scratch {
        let _ = std::fs::remove_file(&scratch);
    }
    match outcome {
        Ok(PutIfMatchOutcome::Committed(_)) => Ok(()),
        Ok(PutIfMatchOutcome::Conflict) => Err(StateSyncError::CasConflict { key: remote_key }),
        // A transport / backend error is not a conflict — apply the configured
        // liveness policy (Durable forces Fail upstream).
        Err(e) => apply_upload_failure_policy(config, Err(e)),
    }
}

/// CAS upload dispatch for the backends with a durable object tier. `local` and
/// `valkey` never reach here — the session downgrades `cas` to an unconditional
/// [`upload_state`] on those.
///
/// On `tiered` this is the D5 write half: **the durable S3 leg is the commit**,
/// and the Valkey tier is populated with the committed generation only after
/// that commit succeeds — or invalidated on conflict, on a transport error, or
/// on a cache write that fails. The Valkey leg is never load-bearing for
/// durability, so it cannot turn a committed run into a failed one.
async fn dispatch_upload_cas(
    config: &StateConfig,
    local_path: &Path,
    remote_key: &str,
    base: Option<&Generation>,
) -> Result<PutIfMatchOutcome, StateSyncError> {
    // The `cas-required` marker goes in BEFORE the conditional put, so the
    // shared object is never CAS-committed without it. A failure to create it
    // is a transport failure of this upload (subject to `on_upload_failure`),
    // never a reason to fall back to an unconditional write.
    if let Some(provider) = durable_tier_provider(config)?
        && let Err(e) = with_transfer_timeout(
            transfer_timeout(config),
            ensure_cas_required_marker(&provider, remote_key),
        )
        .await
    {
        // No commit was attempted, but keep the tiered rule that a failed
        // upload leaves nothing cached for this key.
        if matches!(config.backend, StateBackend::Tiered) {
            invalidate_coherent_cache(config, remote_key).await;
        }
        return Err(e);
    }
    match config.backend {
        StateBackend::Tiered => {
            let bucket = config.s3_bucket.as_deref().ok_or_else(|| {
                StateSyncError::MissingConfig("tiered".into(), "state.s3_bucket".into())
            })?;
            let prefix = config.s3_prefix.as_deref().unwrap_or(DEFAULT_S3_PREFIX);
            // Read the payload ONCE and commit exactly those bytes, so the
            // durable object and the cache entry that claims its generation are
            // provably the same content — re-reading the file for the cache
            // would open a window for them to diverge.
            let data = Bytes::from(tokio::fs::read(local_path).await?);
            let outcome = upload_bytes_to_object_store_cas(
                "s3",
                bucket,
                prefix,
                data.clone(),
                remote_key,
                transfer_timeout(config),
                base,
            )
            .await;
            match &outcome {
                Ok(PutIfMatchOutcome::Committed(committed)) => {
                    populate_coherent_cache(config, remote_key, committed, &data).await;
                }
                // Conflict: we are not the winner, so anything this pod cached
                // is not what committed. Transport error: the durable outcome is
                // UNKNOWN, so no generation can be claimed. Both invalidate and
                // populate nothing.
                Ok(PutIfMatchOutcome::Conflict) | Err(_) => {
                    invalidate_coherent_cache(config, remote_key).await;
                }
            }
            outcome
        }
        StateBackend::S3 => {
            let bucket = config.s3_bucket.as_deref().ok_or_else(|| {
                StateSyncError::MissingConfig("s3".into(), "state.s3_bucket".into())
            })?;
            let prefix = config.s3_prefix.as_deref().unwrap_or(DEFAULT_S3_PREFIX);
            upload_to_object_store_cas(
                "s3",
                bucket,
                prefix,
                local_path,
                remote_key,
                transfer_timeout(config),
                base,
            )
            .await
        }
        StateBackend::Gcs => {
            let bucket = config.gcs_bucket.as_deref().ok_or_else(|| {
                StateSyncError::MissingConfig("gcs".into(), "state.gcs_bucket".into())
            })?;
            let prefix = config.gcs_prefix.as_deref().unwrap_or(DEFAULT_GCS_PREFIX);
            upload_to_object_store_cas(
                "gs",
                bucket,
                prefix,
                local_path,
                remote_key,
                transfer_timeout(config),
                base,
            )
            .await
        }
        // Unreachable in practice — the session downgrades `cas` to an
        // unconditional upload on the tier-less backends. Listed explicitly
        // rather than behind a `_ =>` so a future backend variant has to be
        // classified here instead of silently landing on "refuse".
        StateBackend::Local | StateBackend::Valkey => Err(StateSyncError::MissingConfig(
            config.backend.to_string(),
            "a durable object tier (s3/gcs/tiered) for concurrency_control = cas".into(),
        )),
    }
}

/// Apply the `on_upload_failure` policy to a terminal upload result. `Skip`
/// converts Err → Ok with a structured warn; `Fail` propagates Err unchanged.
fn apply_upload_failure_policy(
    config: &StateConfig,
    result: Result<(), StateSyncError>,
) -> Result<(), StateSyncError> {
    match result {
        Ok(()) => Ok(()),
        // A CAS conflict ("another writer won the race") is a hard fail-closed
        // regardless of `on_upload_failure` — `Skip` must never convert it to a
        // silent success that erases the winner. (The CAS path returns this
        // directly today; this guard keeps the invariant if it ever routes here.)
        Err(e @ StateSyncError::CasConflict { .. }) => Err(e),
        Err(e @ StateSyncError::LedgerSeamConflict { .. }) => Err(e),
        // Same for the cas-required refusal: `Skip` would turn "this writer
        // must not overwrite CAS writers" into a silent success that also
        // drops this run's state.
        Err(e @ StateSyncError::CasRequired { .. }) => Err(e),
        Err(e) => match config.on_upload_failure {
            StateUploadFailureMode::Skip => {
                warn!(
                    error = %e,
                    outcome = "skipped_after_failure",
                    "state upload failed after retries; continuing in degraded mode \
                     (next run's discover will re-derive state)"
                );
                Ok(())
            }
            StateUploadFailureMode::Fail => Err(e),
        },
    }
}

/// Existence probe for a remote state object, isolated behind a test seam.
///
/// Production behaviour is exactly `provider.exists(key)`. Under `#[cfg(test)]`
/// a fault can be armed (see [`test_support::arm_object_store_exists_fault`]) so
/// a test can drive the failure arm of [`download_from_object_store`]
/// deterministically without a live/flaky cloud endpoint — the in-memory
/// provider never errors on `exists`, so this is the only in-crate way to prove
/// the fail-closed propagation is RED before the fix and GREEN after it.
async fn probe_exists(provider: &ObjectStoreProvider, key: &str) -> Result<bool, StateSyncError> {
    #[cfg(any(test, feature = "test-support"))]
    if test_support::take_object_store_exists_fault() {
        return Err(StateSyncError::S3Download(
            "injected existence-check failure (test seam)".into(),
        ));
    }
    provider.exists(key).await.map_err(StateSyncError::from)
}

/// Download `STATE_FILE` from an object store rooted at `<scheme>://<bucket>/<prefix>`.
///
/// Returns [`DownloadOutcome::Restored`] when the object existed and was written
/// locally, and [`DownloadOutcome::Absent`] when no object exists (a legit fresh
/// start — non-fatal by design).
///
/// A *failed* existence check (network / auth / backend error) is **propagated
/// as `Err`**, never swallowed. Previously this arm logged and returned `Ok`,
/// which made a genuine download failure indistinguishable from an
/// authoritative "no remote state yet": the caller (`rocky run`) would then
/// treat a possibly-populated remote ledger as empty and lose its fail-closed
/// signal. The absent case (`Ok(false)`) still returns `Absent` — a real fresh
/// start must stay non-fatal.
async fn download_from_object_store(
    scheme: &str,
    bucket: &str,
    prefix: &str,
    dest_path: &Path,
    remote_key: &str,
    timeout: Duration,
    // When `Some`, capture the downloaded object's [`Generation`] into the sink
    // from the SAME GET that fetched the bytes (never a separate HEAD — that
    // would open a TOCTOU between the version read and the bytes the writer
    // mutates). `None` (every non-CAS caller) uses the plain download.
    //
    // The sink is AUTHORITATIVE over its prior contents: an absent object
    // clears it rather than leaving whatever was there. Callers seed it with
    // `None` today, so this is observably identical — but it means a base can
    // never survive a leg that proved the object is gone, which would let a
    // `PutMode::Update` fire against a key that should take `Create`.
    gen_sink: Option<&mut Option<Generation>>,
) -> Result<DownloadOutcome, StateSyncError> {
    let provider = cloud_provider(scheme, bucket, prefix)?;
    // Qualify the object key by schema version so a v7 pod and a v9 pod never
    // collide on the same object — `<prefix>/v9/state.redb`, not
    // `<prefix>/state.redb`. `remote_key` is threaded from `download_state` so a
    // staging `dest_path` does not change which object we read.
    let key = object_store_state_key(remote_key);
    let span = info_span!(
        "state.download",
        backend = %provider.scheme(),
        bucket = %provider.bucket(),
    );
    async {
        info!(
            uri = format!("{scheme}://{bucket}/{prefix}{key}"),
            local = %dest_path.display(),
            "downloading state from object store"
        );

        with_transfer_timeout(timeout, async {
            match probe_exists(&provider, &key).await {
                Ok(true) => {
                    match gen_sink {
                        Some(sink) => {
                            *sink = provider
                                .download_file_capturing_version(&key, dest_path)
                                .await?;
                        }
                        None => {
                            provider.download_file(&key, dest_path).await?;
                        }
                    }
                    info!(
                        size = dest_path.metadata().map(|m| m.len()).unwrap_or(0),
                        outcome = "ok",
                        "state restored from object store"
                    );
                    Ok(DownloadOutcome::Restored)
                }
                Ok(false) => {
                    // Clear the sink: there is no object at the CURRENT key, so
                    // there is no base. Leaving a stale one would send
                    // `finalize` down `PutMode::Update` for a key whose only
                    // correct write is `Create`. This holds for a carried-forward
                    // restore too: the older key's generation belongs to a
                    // different object, so the first upload must still CAS
                    // against "current key absent".
                    if let Some(sink) = gen_sink {
                        *sink = None;
                    }
                    // An object store is always a durable leg (`s3`, `gcs`, or
                    // the S3 leg of `tiered`), so it always carries forward.
                    if let Some(version) =
                            carry_forward_from_object_store(&provider, remote_key, dest_path)
                                .await?
                    {
                        info!(
                            size = dest_path.metadata().map(|m| m.len()).unwrap_or(0),
                            outcome = "carried_forward",
                            carried_forward_from = format!("v{version}"),
                            current = %schema_version_segment(),
                            "no state at the current schema version; restored the newest older \
                             version's state. The next upload writes the current key; the older \
                             key is left untouched"
                        );
                        return Ok(DownloadOutcome::Restored);
                    }
                    info!(outcome = "absent", "No existing state in object store — starting fresh");
                    Ok(DownloadOutcome::Absent)
                }
                // Fail closed: a real existence-check failure is NOT an
                // authoritative "no remote state". Propagate so the caller can
                // mark its local ledger non-authoritative instead of silently
                // starting fresh over a populated remote.
                Err(e) => {
                    warn!(error = %e, outcome = "error", "state existence check failed; propagating (fail-closed)");
                    Err(e)
                }
            }
        })
        .await
    }
    .instrument(span)
    .await
}

/// Carry-forward read for the object-store path (#1955): probe the older
/// schema versions' keys newest first and download the first that exists.
///
/// Returns the version restored, or `None` when no older key exists down to
/// [`CARRY_FORWARD_FLOOR_SCHEMA_VERSION`]. Read-only: the older key is never
/// written or deleted. The download captures no generation, because the
/// caller's CAS base must stay "current key absent".
///
/// A failed probe is propagated (fail-closed), exactly like the current-key
/// probe: an unreadable older key is not proof of a fresh start.
async fn carry_forward_from_object_store(
    provider: &ObjectStoreProvider,
    remote_key: &str,
    dest_path: &Path,
) -> Result<Option<u32>, StateSyncError> {
    for version in carry_forward_versions() {
        let key = object_store_state_key_at(version, remote_key);
        match probe_exists(provider, &key).await {
            Ok(true) => {
                provider.download_file(&key, dest_path).await?;
                return Ok(Some(version));
            }
            Ok(false) => debug!(key = %key, "no state at older schema version"),
            Err(e) => {
                warn!(
                    error = %e,
                    key = %key,
                    outcome = "error",
                    "older-version state existence check failed; propagating (fail-closed)"
                );
                return Err(e);
            }
        }
    }
    Ok(None)
}

/// Upload `STATE_FILE` to an object store rooted at `<scheme>://<bucket>/<prefix>`.
///
/// Wraps the put in a retry loop driven by `retry` (shared-shape
/// [`RetryConfig`]) and a call-local circuit breaker + retry budget. The
/// outer `with_transfer_timeout` still caps total wall-clock, so retries
/// share — not extend — the configured transfer budget.
async fn upload_to_object_store(
    scheme: &str,
    bucket: &str,
    prefix: &str,
    local_path: &Path,
    key: &str,
    timeout: Duration,
    retry: &RetryConfig,
) -> Result<(), StateSyncError> {
    let provider = cloud_provider(scheme, bucket, prefix)?;
    // Mirror `download_from_object_store`: qualify the object key by schema
    // version so the upload lands at the same `<prefix>/v9/state.redb` the
    // download reads, and never over a different version's object.
    let key = object_store_state_key(key);
    let size_bytes = local_path.metadata().map(|m| m.len()).unwrap_or(0);
    let span = info_span!(
        "state.upload",
        backend = %provider.scheme(),
        bucket = %provider.bucket(),
        size_bytes,
    );
    async {
        info!(
            uri = format!("{scheme}://{bucket}/{prefix}{key}"),
            "uploading state to object store"
        );
        let retries = with_transfer_timeout(timeout, async {
            retry_transient(retry, "state.upload.object_store", || async {
                provider
                    .upload_file(local_path, &key)
                    .await
                    .map_err(StateSyncError::from)
            })
            .await
        })
        .await?;
        info!(
            bytes = size_bytes,
            retries,
            outcome = "ok",
            "state upload complete"
        );
        Ok(())
    }
    .instrument(span)
    .await
}

/// Compare-and-swap variant of [`upload_to_object_store`]: conditionally upload
/// `local_path` against `base`, returning the [`PutIfMatchOutcome`].
///
/// A `Conflict` is returned as `Ok(Conflict)` (never an `Err`), so the caller
/// maps it to a fail-closed refusal. The `object_store` client applies its own
/// transient-retry and, on S3, `retry_on_conflict` already absorbs the
/// transient 409 — so a surfaced conflict is a genuine stale base, and the
/// state-level `retry_transient` (which carries no return value) is not used on
/// this path.
async fn upload_to_object_store_cas(
    scheme: &str,
    bucket: &str,
    prefix: &str,
    local_path: &Path,
    key: &str,
    timeout: Duration,
    base: Option<&Generation>,
) -> Result<PutIfMatchOutcome, StateSyncError> {
    let provider = cloud_provider(scheme, bucket, prefix)?;
    let key = object_store_state_key(key);
    let size_bytes = local_path.metadata().map(|m| m.len()).unwrap_or(0);
    let span = info_span!(
        "state.upload.cas",
        backend = %provider.scheme(),
        bucket = %provider.bucket(),
        size_bytes,
    );
    async {
        info!(
            uri = format!("{scheme}://{bucket}/{prefix}{key}"),
            conditional = base.is_some(),
            "CAS-uploading state to object store"
        );
        let outcome = with_transfer_timeout(timeout, async {
            provider
                .upload_file_if_match(local_path, &key, base)
                .await
                .map_err(StateSyncError::from)
        })
        .await?;
        info!(
            bytes = size_bytes,
            committed = matches!(outcome, PutIfMatchOutcome::Committed(_)),
            outcome = "ok",
            "CAS state upload complete"
        );
        Ok(outcome)
    }
    .instrument(span)
    .await
}

/// Byte-payload compare-and-swap upload — the tiered path's durable commit.
///
/// Separate from [`upload_to_object_store_cas`] (which uploads from a path) on
/// purpose: the tiered write needs the EXACT bytes it committed in hand
/// afterwards, to publish them as the cache entry tagged with the generation
/// this commit returned. Reading the file twice would let the durable object
/// and the cache entry claiming its generation drift apart.
///
/// A `Conflict` is `Ok(Conflict)`, never an `Err` — the caller maps it to a
/// fail-closed refusal, and a backend error stays an error.
async fn upload_bytes_to_object_store_cas(
    scheme: &str,
    bucket: &str,
    prefix: &str,
    data: Bytes,
    key: &str,
    timeout: Duration,
    base: Option<&Generation>,
) -> Result<PutIfMatchOutcome, StateSyncError> {
    let provider = cloud_provider(scheme, bucket, prefix)?;
    let key = object_store_state_key(key);
    let size_bytes = data.len() as u64;
    let span = info_span!(
        "state.upload.cas",
        backend = %provider.scheme(),
        bucket = %provider.bucket(),
        size_bytes,
    );
    async {
        info!(
            uri = format!("{scheme}://{bucket}/{prefix}{key}"),
            conditional = base.is_some(),
            "CAS-uploading state to the durable tier"
        );
        let outcome = with_transfer_timeout(timeout, async {
            provider
                .put_if_match(&key, data, base)
                .await
                .map_err(StateSyncError::from)
        })
        .await?;
        info!(
            bytes = size_bytes,
            committed = matches!(outcome, PutIfMatchOutcome::Committed(_)),
            outcome = "ok",
            "CAS state upload complete"
        );
        Ok(outcome)
    }
    .instrument(span)
    .await
}

/// Wrap a state-transfer future with the configured timeout budget. On elapse
/// the returned error is `StateSyncError::Timeout` — distinct from the
/// per-request timeout the client raises, so callers can tell the two apart.
///
/// Emits a structured `tracing::warn!` on elapse so operators can diagnose
/// hung transfers from log output alone. Span fields (`backend`, `bucket`,
/// `size_bytes`) propagate via the enclosing `state.{upload,download}` span.
async fn with_transfer_timeout<F, T>(timeout: Duration, fut: F) -> Result<T, StateSyncError>
where
    F: std::future::Future<Output = Result<T, StateSyncError>>,
{
    match tokio::time::timeout(timeout, fut).await {
        Ok(result) => result,
        Err(_) => {
            warn!(
                duration_ms = timeout.as_millis() as u64,
                outcome = "timeout",
                "state transfer exceeded timeout budget"
            );
            Err(StateSyncError::Timeout(timeout))
        }
    }
}

/// Download state from Valkey/Redis.
///
/// The `redis` crate's sync client blocks the current thread — a dead Valkey
/// peer would otherwise stall the tokio runtime indefinitely and no outer
/// `tokio::time::timeout` could rescue it. We offload the blocking work to a
/// dedicated thread via `spawn_blocking` and gate it with the same
/// `transfer_timeout_seconds` budget the object-store paths use.
async fn download_from_valkey(
    config: &StateConfig,
    dest_path: &Path,
    remote_key: &str,
    carry_forward: CarryForward,
) -> Result<DownloadOutcome, StateSyncError> {
    // Test seam: force a Valkey MISS without a live Valkey peer, so the tiered
    // fall-through-to-S3 path (and the "stale local file is not a hit" fix) can
    // be exercised deterministically. See [`test_support::arm_valkey_miss_fault`].
    #[cfg(any(test, feature = "test-support"))]
    if test_support::take_valkey_miss_fault() {
        return Ok(DownloadOutcome::Absent);
    }
    let prefix = config
        .valkey_prefix
        .as_deref()
        .unwrap_or(DEFAULT_VALKEY_PREFIX)
        .to_string();
    // Qualify by schema version: `rocky:state:v9:state.redb`, not
    // `rocky:state:state.redb`, so a v7 reader and a v9 writer never share a key.
    // `remote_key` is threaded from `download_state` so a staging `dest_path`
    // does not change which key we read.
    //
    // The current key comes first. Under carry-forward (#1955) the older
    // versions' keys follow, newest first; the first hit wins. They are only
    // ever read here, never written or deleted.
    let current = crate::state::current_schema_version();
    let mut candidates = vec![(current, valkey_state_key(&prefix, remote_key))];
    if carry_forward == CarryForward::Yes {
        candidates.extend(
            carry_forward_versions()
                .into_iter()
                .map(|v| (v, valkey_state_key_at(&prefix, v, remote_key))),
        );
    }
    let timeout = transfer_timeout(config);

    let span = info_span!("state.download", backend = "valkey");
    async move {
        info!(key = %candidates[0].1, "downloading state from Valkey");
        let hit = with_transfer_timeout(timeout, valkey_get_first(config, candidates)).await?;
        match hit {
            Some((version, key, bytes)) => {
                tokio::fs::write(dest_path, &bytes).await?;
                if version == current {
                    info!(
                        size = bytes.len(),
                        outcome = "ok",
                        "state restored from Valkey"
                    );
                } else {
                    info!(
                        size = bytes.len(),
                        key = %key,
                        outcome = "carried_forward",
                        carried_forward_from = format!("v{version}"),
                        current = %schema_version_segment(),
                        "no state at the current schema version; restored the newest older \
                         version's state from Valkey. The next upload writes the current key; \
                         the older key is left untouched"
                    );
                }
                Ok(DownloadOutcome::Restored)
            }
            None => {
                info!(
                    outcome = "absent",
                    "No existing state in Valkey — starting fresh"
                );
                Ok(DownloadOutcome::Absent)
            }
        }
    }
    .instrument(span)
    .await
}

/// `GET` each `(version, key)` candidate in order and return the first that
/// holds a value, or `None` when every key is absent. One connection serves
/// the whole probe.
///
/// The redis sync client blocks its thread, so the work runs on the blocking
/// pool; the caller caps it with `transfer_timeout_seconds`.
async fn valkey_get_first(
    config: &StateConfig,
    candidates: Vec<(u32, String)>,
) -> Result<Option<(u32, String, Vec<u8>)>, StateSyncError> {
    #[cfg(any(test, feature = "test-support"))]
    if test_support::fake_valkey_installed() {
        return Ok(candidates.into_iter().find_map(|(version, key)| {
            test_support::fake_valkey_get(&key).map(|bytes| (version, key, bytes))
        }));
    }
    let url = config
        .valkey_url
        .as_ref()
        .map(RedactedString::expose)
        .ok_or_else(|| StateSyncError::MissingConfig("valkey".into(), "state.valkey_url".into()))?
        .to_string();
    let join = tokio::task::spawn_blocking(
        move || -> Result<Option<(u32, String, Vec<u8>)>, StateSyncError> {
            let client =
                redis::Client::open(url).map_err(|e| StateSyncError::Valkey(e.to_string()))?;
            let mut conn = client
                .get_connection()
                .map_err(|e| StateSyncError::Valkey(e.to_string()))?;
            for (version, key) in candidates {
                let data: Option<Vec<u8>> = redis::cmd("GET")
                    .arg(&key)
                    .query(&mut conn)
                    .map_err(|e| StateSyncError::Valkey(e.to_string()))?;
                if let Some(bytes) = data {
                    return Ok(Some((version, key, bytes)));
                }
                debug!(key = %key, "no state in Valkey at this key");
            }
            Ok(None)
        },
    )
    .await;
    match join {
        Ok(inner) => inner,
        Err(e) => Err(StateSyncError::Valkey(format!(
            "valkey worker task failed: {e}"
        ))),
    }
}

/// Upload state to Valkey/Redis.
///
/// Mirrors [`download_from_valkey`]: offloads the blocking redis SET via
/// `spawn_blocking` and wraps the handle with `with_transfer_timeout` so a
/// hung Valkey peer cannot stall the run past the configured budget.
async fn upload_to_valkey(
    config: &StateConfig,
    local_path: &Path,
    remote_key: &str,
) -> Result<(), StateSyncError> {
    let url = config
        .valkey_url
        .as_ref()
        .map(RedactedString::expose)
        .ok_or_else(|| StateSyncError::MissingConfig("valkey".into(), "state.valkey_url".into()))?
        .to_string();
    let prefix = config
        .valkey_prefix
        .as_deref()
        .unwrap_or(DEFAULT_VALKEY_PREFIX)
        .to_string();
    // Mirror `download_from_valkey`: qualify by schema version so the upload
    // key matches the download key (`rocky:state:v9:<remote_key>`).
    let key = valkey_state_key(&prefix, remote_key);
    let data = std::fs::read(local_path)?;
    let size_bytes = data.len() as u64;
    let timeout = transfer_timeout(config);
    let retry = &config.retry;

    let span = info_span!("state.upload", backend = "valkey", size_bytes);
    async move {
        info!(key = %key, size = size_bytes, "uploading state to Valkey");
        let retries = with_transfer_timeout(timeout, async {
            retry_transient(retry, "state.upload.valkey", || {
                let url = url.clone();
                let key = key.clone();
                let data = data.clone();
                async move {
                    let join =
                        tokio::task::spawn_blocking(move || -> Result<(), StateSyncError> {
                            let client = redis::Client::open(url)
                                .map_err(|e| StateSyncError::Valkey(e.to_string()))?;
                            let mut conn = client
                                .get_connection()
                                .map_err(|e| StateSyncError::Valkey(e.to_string()))?;

                            redis::cmd("SET")
                                .arg(&key)
                                .arg(data)
                                .query::<()>(&mut conn)
                                .map_err(|e| StateSyncError::Valkey(e.to_string()))?;
                            Ok(())
                        })
                        .await;
                    match join {
                        Ok(inner) => inner,
                        Err(e) => Err(StateSyncError::Valkey(format!(
                            "valkey worker task failed: {e}"
                        ))),
                    }
                }
            })
            .await
        })
        .await?;
        info!(
            bytes = size_bytes,
            retries,
            outcome = "ok",
            "state upload complete"
        );
        Ok(())
    }
    .instrument(span)
    .await
}

/// Round-trip RW probe against the configured state backend.
///
/// Writes a short-lived marker to a **distinct key** (never the real
/// `state.redb`), reads it back, and deletes it. Used by `rocky doctor`
/// to verify a state backend is actually reachable and writable — not
/// merely configured. Honours `transfer_timeout_seconds` as an outer
/// wall-clock cap; no retries — probes should produce a single-pass
/// pass/fail signal, not resilient writes.
///
/// For `tiered` both legs (Valkey + S3) must pass — either one failing
/// fails the probe. For `local` this is a no-op returning `Ok`.
pub async fn probe_state_backend(config: &StateConfig) -> Result<(), StateSyncError> {
    match config.backend {
        StateBackend::Local => Ok(()),
        StateBackend::S3 => {
            let bucket = config.s3_bucket.as_deref().ok_or_else(|| {
                StateSyncError::MissingConfig("s3".into(), "state.s3_bucket".into())
            })?;
            let prefix = config.s3_prefix.as_deref().unwrap_or(DEFAULT_S3_PREFIX);
            probe_object_store("s3", bucket, prefix, transfer_timeout(config)).await
        }
        StateBackend::Gcs => {
            let bucket = config.gcs_bucket.as_deref().ok_or_else(|| {
                StateSyncError::MissingConfig("gcs".into(), "state.gcs_bucket".into())
            })?;
            let prefix = config.gcs_prefix.as_deref().unwrap_or(DEFAULT_GCS_PREFIX);
            probe_object_store("gs", bucket, prefix, transfer_timeout(config)).await
        }
        StateBackend::Valkey => probe_valkey(config).await,
        StateBackend::Tiered => {
            let valkey_config = StateConfig {
                backend: StateBackend::Valkey,
                ..config.clone()
            };
            let s3_config = StateConfig {
                backend: StateBackend::S3,
                ..config.clone()
            };
            Box::pin(probe_state_backend(&valkey_config)).await?;
            Box::pin(probe_state_backend(&s3_config)).await
        }
    }
}

/// Build a per-call probe key under the configured prefix. PID separates
/// concurrent doctor invocations; a process-local sequence separates calls
/// inside one process, so uniqueness never depends on clock resolution.
fn probe_key() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    probe_key_at(nanos)
}

/// Format a probe key for an already-read `nanos`. Split out so a test can
/// exhibit two calls landing in the same clock tick.
fn probe_key_at(nanos: u128) -> String {
    // A process-unique sequence disambiguates two probes that observe the same
    // `nanos` on a coarse clock — the probe writes the key, reads it back and
    // compares the bytes, so a collision would let one probe read another's
    // write. The name carries no contract; it is deleted, best-effort, after a
    // successful round-trip.
    static PROBE_SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let sequence = PROBE_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!(
        "doctor-probe-{}-{}-{}.marker",
        std::process::id(),
        nanos,
        sequence
    )
}

async fn probe_object_store(
    scheme: &str,
    bucket: &str,
    prefix: &str,
    timeout: Duration,
) -> Result<(), StateSyncError> {
    let provider = cloud_provider(scheme, bucket, prefix)?;
    let key = probe_key();
    let data = Bytes::from_static(b"rocky doctor probe");
    let span = info_span!(
        "state.probe",
        backend = %provider.scheme(),
        bucket = %provider.bucket(),
    );
    async move {
        with_transfer_timeout(timeout, async {
            provider.put(&key, data.clone()).await?;
            let got = provider.get(&key).await?;
            if got != data {
                // Reuse the backend-specific Upload variant so the message
                // carries the scheme; the probe-vs-real-upload distinction
                // is captured by the `state.probe` span name above.
                let err = format!("probe content mismatch (wrote {} bytes, read {} bytes)", data.len(), got.len());
                return match scheme {
                    "s3" => Err(StateSyncError::S3Upload(err)),
                    "gs" => Err(StateSyncError::GcsUpload(err)),
                    _ => Err(StateSyncError::S3Upload(err)),
                };
            }
            // Best-effort cleanup — a stale probe object is a small cost
            // (< 20 bytes, lifecycle rules clean up eventually); surfacing
            // a delete failure on an otherwise successful probe would
            // mask the real signal (RW works).
            if let Err(e) = provider.delete(&key).await {
                warn!(error = %e, key = %key, outcome = "probe_cleanup_failed", "state probe cleanup failed (object will remain until lifecycle rule cleans it up)");
            }
            info!(outcome = "ok", "state backend probe succeeded");
            Ok(())
        })
        .await
    }
    .instrument(span)
    .await
}

async fn probe_valkey(config: &StateConfig) -> Result<(), StateSyncError> {
    let url = config
        .valkey_url
        .as_ref()
        .map(RedactedString::expose)
        .ok_or_else(|| StateSyncError::MissingConfig("valkey".into(), "state.valkey_url".into()))?
        .to_string();
    let prefix = config
        .valkey_prefix
        .as_deref()
        .unwrap_or(DEFAULT_VALKEY_PREFIX)
        .to_string();
    let key = format!("{prefix}{}", probe_key());
    let timeout = transfer_timeout(config);

    let span = info_span!("state.probe", backend = "valkey");
    async move {
        with_transfer_timeout(timeout, async move {
            let join = tokio::task::spawn_blocking(move || -> Result<(), StateSyncError> {
                let client =
                    redis::Client::open(url).map_err(|e| StateSyncError::Valkey(e.to_string()))?;
                let mut conn = client
                    .get_connection()
                    .map_err(|e| StateSyncError::Valkey(e.to_string()))?;
                redis::cmd("SET")
                    .arg(&key)
                    .arg("rocky doctor probe")
                    .query::<()>(&mut conn)
                    .map_err(|e| StateSyncError::Valkey(e.to_string()))?;
                let val: Option<String> = redis::cmd("GET")
                    .arg(&key)
                    .query(&mut conn)
                    .map_err(|e| StateSyncError::Valkey(e.to_string()))?;
                if val.as_deref() != Some("rocky doctor probe") {
                    return Err(StateSyncError::Valkey(format!(
                        "probe value mismatch (got {val:?})"
                    )));
                }
                // Best-effort cleanup — same rationale as the object-store path.
                let _ = redis::cmd("DEL").arg(&key).query::<()>(&mut conn);
                Ok(())
            })
            .await;
            match join {
                Ok(inner) => inner,
                Err(e) => Err(StateSyncError::Valkey(format!(
                    "valkey worker task failed: {e}"
                ))),
            }
        })
        .await?;
        info!(outcome = "ok", "state backend probe succeeded");
        Ok(())
    }
    .instrument(span)
    .await
}

/// Drive `op` through the shared retry + circuit-breaker + budget policy.
///
/// Returns the number of retries consumed by the successful attempt (0 when
/// the first try wins) so the caller can stamp `retries` on the terminal
/// span event. On permanent failure returns the underlying
/// [`StateSyncError`] — or [`StateSyncError::CircuitOpen`] /
/// [`StateSyncError::RetryBudgetExhausted`] when the abort happens inside
/// this helper rather than at the transport layer.
///
/// The circuit breaker and retry budget are built per-call from `cfg`. This
/// keeps state-sync's lifecycle simple (each upload/download is independent)
/// and mirrors the `[adapter.databricks.retry]` shape end-to-end for
/// operational parity. Cross-call breaker state could be wired in later via
/// a state-sync context struct, but no caller needs that today.
async fn retry_transient<F, Fut>(
    cfg: &RetryConfig,
    op_name: &str,
    mut op: F,
) -> Result<u32, StateSyncError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(), StateSyncError>>,
{
    let breaker = cfg.build_circuit_breaker();
    let budget = RetryBudget::from_config(cfg.max_retries_per_run);

    if let Err(e) = breaker.check() {
        return Err(StateSyncError::CircuitOpen {
            consecutive_failures: e.consecutive_failures,
        });
    }

    for attempt in 0..=cfg.max_retries {
        match op().await {
            Ok(()) => {
                if breaker.record_success() == TransitionOutcome::Recovered {
                    info!(
                        op = op_name,
                        outcome = "recovered",
                        "state backend circuit breaker recovered"
                    );
                }
                return Ok(attempt);
            }
            Err(err) if is_transient(&err) => {
                if breaker.record_failure(&err.to_string()) == TransitionOutcome::Tripped {
                    warn!(
                        op = op_name,
                        outcome = "circuit_open",
                        error = %err,
                        "state backend circuit breaker tripped"
                    );
                }
                if attempt < cfg.max_retries {
                    if !budget.try_consume() {
                        let limit = budget.total().unwrap_or(0);
                        warn!(
                            op = op_name,
                            attempt = attempt + 1,
                            budget_limit = limit,
                            error = %err,
                            outcome = "budget_exhausted",
                            "state retry budget exhausted; aborting further retries"
                        );
                        return Err(StateSyncError::RetryBudgetExhausted { limit });
                    }
                    let backoff_ms = compute_backoff(cfg, attempt);
                    warn!(
                        op = op_name,
                        attempt = attempt + 1,
                        max_retries = cfg.max_retries,
                        backoff_ms,
                        error = %err,
                        outcome = "retry",
                        "state transient error, retrying"
                    );
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    continue;
                }
                warn!(
                    op = op_name,
                    attempts = attempt + 1,
                    error = %err,
                    outcome = "transient_exhausted",
                    "state transient retries exhausted"
                );
                return Err(err);
            }
            Err(err) => return Err(err),
        }
    }

    unreachable!("retry loop always returns within the for body")
}

/// Classify a state-sync error for retry decisions.
///
/// Network/SDK-level errors and timeouts are treated as transient — a
/// fresh attempt might clear a single flake. Config errors and local disk
/// I/O are permanent; retrying them wastes budget. The breaker/budget
/// sentinels are already terminal by construction and must not re-enter
/// the retry loop.
fn is_transient(err: &StateSyncError) -> bool {
    match err {
        StateSyncError::S3Download(_)
        | StateSyncError::S3Upload(_)
        | StateSyncError::GcsDownload(_)
        | StateSyncError::GcsUpload(_)
        | StateSyncError::Valkey(_)
        | StateSyncError::Timeout(_) => true,
        StateSyncError::ObjectStore(ObjectStoreError::Backend(_)) => true,
        StateSyncError::ObjectStore(
            ObjectStoreError::InvalidUri(..)
            | ObjectStoreError::UnsupportedScheme(_)
            | ObjectStoreError::Io(_),
        )
        | StateSyncError::Io(_)
        | StateSyncError::State(_)
        | StateSyncError::MissingConfig(..)
        | StateSyncError::CircuitOpen { .. }
        | StateSyncError::RetryBudgetExhausted { .. }
        // A CAS conflict is definitive (another writer won) — retrying against
        // the same stale base would just conflict again and burn the budget.
        // Fail closed immediately.
        | StateSyncError::CasConflict { .. }
        | StateSyncError::LedgerSeamConflict { .. }
        // A seam-transition failure is a domain refusal from the caller's
        // closure, not a transport fault — retrying cannot change it.
        | StateSyncError::SeamTransition(_)
        // A publish conflict is a semantic refusal: the head moved.
        | StateSyncError::PublishConflict { .. }
        // Configuration refusals: retrying cannot change the store or the
        // marker.
        | StateSyncError::CasUnsupported { .. }
        | StateSyncError::CasRequired { .. }
        | StateSyncError::PublishRequiresCas { .. } => false,
    }
}

/// Hook run inside each [`publish_pointers`] attempt, after the local
/// transaction and before the conditional upload. Test seam only.
type PublishAttemptHook =
    Arc<dyn Fn(u32) -> Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>;

/// Map a local publish error to the seam's error type. A head conflict
/// becomes the typed [`StateSyncError::PublishConflict`], so it travels the
/// seam's failure path (restore the remote winner, no upload) as itself.
fn publish_error(e: crate::state::StateError) -> StateSyncError {
    match e {
        crate::state::StateError::PublishConflict {
            env,
            expected,
            found,
        } => StateSyncError::PublishConflict {
            env,
            expected,
            found,
        },
        other => StateSyncError::State(other),
    }
}

/// Publish environment pointers against the shared state (RV1-P2).
///
/// Runs [`StateStore::publish_pointers`] as one [`LedgerSeamSession`]
/// transition: download the blob with its generation, run the local
/// transaction, upload with compare-and-swap. When the blob moved, the WHOLE
/// transaction replays on the winner (up to three attempts):
///
/// ```text
///   blob moved by an unrelated run finalize ──▶ replay, same head ──▶ Ok
///   blob moved by another publish           ──▶ replay, new head  ──▶ PublishConflict
/// ```
///
/// On the Local backend it is one local transaction, no remote I/O; the
/// local writer lock serializes publishes. On a remote backend the effective
/// concurrency control must be `cas`: under `off` the seam is one download and
/// one unconditional upload, so two concurrent publishes could both succeed
/// and one would be lost silently. That case is refused up front.
///
/// # Errors
///
/// [`StateSyncError::PublishRequiresCas`] on a remote backend without
/// effective CAS (nothing is downloaded or uploaded);
/// [`StateSyncError::PublishConflict`] when the head is not the expected
/// one; [`StateSyncError::State`] wrapping
/// [`crate::state::StateError::Environment`] for a refused request; plus
/// every seam error ([`StateSyncError::LedgerSeamConflict`], transport).
pub async fn publish_pointers(
    session: &LedgerSeamSession,
    request: &crate::environments::PublishRequest,
) -> Result<crate::environments::PublishRecord, StateSyncError> {
    require_publish_cas(session).await?;
    publish_pointers_with_hook(session, request, None).await
}

/// Refuse a publish over a remote backend without effective CAS: there the
/// seam is one download and one unconditional upload, so a concurrent
/// publish could be lost silently.
async fn require_publish_cas(session: &LedgerSeamSession) -> Result<(), StateSyncError> {
    if !matches!(session.cfg.backend, StateBackend::Local) {
        // The same resolution `execute` performs: the backend default for an
        // unset mode, confirmed by the startup probe.
        let cfg = resolved_state_config(&session.cfg).await?;
        if !cas_effective(&cfg) {
            return Err(StateSyncError::PublishRequiresCas {
                backend: cfg.backend.to_string(),
            });
        }
    }
    Ok(())
}

/// Start a table publish against the shared state (RV1-P3): run
/// [`StateStore::begin_table_publish`] as one [`LedgerSeamSession`]
/// transition, with the same CAS guard and replay rules as
/// [`publish_pointers`]. A head another publish moved is a
/// [`StateSyncError::PublishConflict`]; nothing is written.
///
/// # Errors
///
/// As [`publish_pointers`], plus
/// [`crate::environments::EnvironmentError::PublishInProgress`] wrapped in
/// [`StateSyncError::State`].
pub async fn begin_table_publish(
    session: &LedgerSeamSession,
    request: &crate::environments::PublishRequest,
    take_over: bool,
    check: &(dyn Fn(&crate::environments::EnvPointer) -> Result<(), String> + Sync),
) -> Result<crate::environments::PublishRecord, StateSyncError> {
    require_publish_cas(session).await?;
    session
        .execute(move |store, _base| {
            // The store call is synchronous; the future only carries its
            // result, so it borrows nothing from the caller.
            let result = store
                .begin_table_publish(request, take_over, check)
                .map_err(publish_error);
            Box::pin(async move { result })
        })
        .await
}

/// Record a table publish's outcome against the shared state (RV1-P3): run
/// [`StateStore::finish_table_publish`] as one [`LedgerSeamSession`]
/// transition. It replays like [`publish_pointers`]; the environment is
/// marked `publishing`, so no other publish can move the head in between.
///
/// # Errors
///
/// As [`StateStore::finish_table_publish`], plus every seam error.
pub async fn finish_table_publish(
    session: &LedgerSeamSession,
    env: &crate::environments::EnvironmentName,
    started: &str,
    moves: &[crate::environments::TableMove],
    principal: &crate::config::PrincipalRef,
) -> Result<crate::environments::PublishRecord, StateSyncError> {
    require_publish_cas(session).await?;
    session
        .execute(move |store, _base| {
            let result = store
                .finish_table_publish(env, started, moves, principal)
                .map_err(publish_error);
            Box::pin(async move { result })
        })
        .await
}

/// The publish transition WITHOUT the CAS guard. Tests use it to show what
/// the guard prevents.
async fn publish_pointers_with_hook(
    session: &LedgerSeamSession,
    request: &crate::environments::PublishRequest,
    hook: Option<PublishAttemptHook>,
) -> Result<crate::environments::PublishRecord, StateSyncError> {
    let attempt_number = Arc::new(std::sync::atomic::AtomicU32::new(0));
    session
        .execute(move |store, _base| {
            let request = request.clone();
            let hook = hook.clone();
            let n = attempt_number.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            Box::pin(async move {
                let result = store.publish_pointers(&request);
                if let Some(hook) = hook {
                    hook(n).await;
                }
                result.map_err(publish_error)
            })
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// WP-01 PR-B (2b) — the `rocky load` remote-key collision mechanism (ADR
    /// §3): `remote_state_key` keys on the parent *directory name*, so load's
    /// legacy `<config_dir>/.rocky_state` and the canonical
    /// `models/.rocky-state.redb` both map to the SAME remote object
    /// `state.redb`. Syncing load's legacy path as-is would therefore clobber
    /// the pipeline's canonical remote ledger on upload (and overwrite load's
    /// local state on download) — which is why `run_load` is unified onto the
    /// canonical threaded state path instead of syncing its legacy file.
    #[test]
    fn legacy_load_path_collides_with_canonical_remote_key() {
        let legacy = remote_state_key(Path::new("/proj/.rocky_state"));
        let canonical = remote_state_key(Path::new("/proj/models/.rocky-state.redb"));
        assert_eq!(legacy, "state.redb");
        assert_eq!(canonical, "state.redb");
        assert_eq!(
            legacy, canonical,
            "legacy load path and canonical state path map to ONE remote object — \
             the collision that forces load onto the canonical path"
        );
    }

    #[tokio::test]
    async fn test_local_download_noop() {
        let config = StateConfig::default();
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        assert!(download_state(&config, &path, false).await.is_ok());
    }

    #[tokio::test]
    async fn test_local_upload_noop() {
        let config = StateConfig::default();
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        assert!(upload_state(&config, &path, false).await.is_ok());
    }

    #[tokio::test]
    async fn test_s3_missing_bucket() {
        let config = StateConfig {
            backend: StateBackend::S3,
            s3_bucket: None,
            ..Default::default()
        };
        let dir = TempDir::new().unwrap();
        let result = download_state(&config, &dir.path().join("state.redb"), false).await;
        assert!(matches!(result, Err(StateSyncError::MissingConfig(..))));
    }

    #[tokio::test]
    async fn test_gcs_missing_bucket() {
        let config = StateConfig {
            backend: StateBackend::Gcs,
            gcs_bucket: None,
            ..Default::default()
        };
        let dir = TempDir::new().unwrap();
        let result = download_state(&config, &dir.path().join("state.redb"), false).await;
        assert!(matches!(result, Err(StateSyncError::MissingConfig(..))));
    }

    #[tokio::test]
    async fn test_valkey_missing_url() {
        let config = StateConfig {
            backend: StateBackend::Valkey,
            valkey_url: None,
            ..Default::default()
        };
        let dir = TempDir::new().unwrap();
        let result = download_state(&config, &dir.path().join("state.redb"), false).await;
        assert!(matches!(result, Err(StateSyncError::MissingConfig(..))));
    }

    #[test]
    fn test_state_backend_display_includes_gcs() {
        assert_eq!(StateBackend::Gcs.to_string(), "gcs");
    }

    /// `durable_tier_provider` maps each backend to its marker-capable tier:
    /// s3/gcs directly, tiered to its S3 leg (never Valkey), and local /
    /// valkey-only to `None`. A durable backend with no bucket is
    /// `MissingConfig`, mirroring the transfer dispatch.
    #[test]
    fn durable_tier_provider_maps_backends() {
        // Route provider construction to an in-memory store so no cloud
        // client (or credentials) is needed.
        let _handle = test_support::install(ObjectStoreProvider::in_memory());

        let s3 = StateConfig {
            backend: StateBackend::S3,
            s3_bucket: Some("bucket".into()),
            ..Default::default()
        };
        assert!(durable_tier_provider(&s3).unwrap().is_some());

        let gcs = StateConfig {
            backend: StateBackend::Gcs,
            gcs_bucket: Some("bucket".into()),
            ..Default::default()
        };
        assert!(durable_tier_provider(&gcs).unwrap().is_some());

        let tiered = StateConfig {
            backend: StateBackend::Tiered,
            s3_bucket: Some("bucket".into()),
            ..Default::default()
        };
        assert!(
            durable_tier_provider(&tiered).unwrap().is_some(),
            "tiered maps to its durable S3 leg"
        );

        assert!(
            durable_tier_provider(&StateConfig::default())
                .unwrap()
                .is_none(),
            "local has no durable object tier"
        );
        let valkey = StateConfig {
            backend: StateBackend::Valkey,
            ..Default::default()
        };
        assert!(
            durable_tier_provider(&valkey).unwrap().is_none(),
            "valkey-only has no durable object tier"
        );

        let s3_no_bucket = StateConfig {
            backend: StateBackend::S3,
            ..Default::default()
        };
        assert!(matches!(
            durable_tier_provider(&s3_no_bucket),
            Err(StateSyncError::MissingConfig(..))
        ));

        test_support::clear();
    }

    // R8 (part 1) — remote key derivation. The legacy global file maps to the
    // unchanged `state.redb` key (namespacing OFF is byte-identical on the
    // wire); namespaced files under `.rocky-state/` map to distinct
    // `<namespace>.redb` keys so pipelines don't clobber a shared object.
    #[test]
    fn test_remote_state_key_legacy_vs_namespaced() {
        // Legacy global file (stem is `.rocky-state`) -> fixed `state.redb`.
        let legacy = std::path::Path::new("models/.rocky-state.redb");
        assert_eq!(remote_state_key(legacy), "state.redb");

        // A bare custom --state-path also keeps the fixed key (not under a
        // `.rocky-state/` parent) — OFF/override paths never gain a key.
        let custom = std::path::Path::new("/var/data/my-state.redb");
        assert_eq!(remote_state_key(custom), "state.redb");

        // Namespaced files -> distinct per-namespace keys.
        let acme = std::path::Path::new("models/.rocky-state/acme.redb");
        let globex = std::path::Path::new("models/.rocky-state/globex.redb");
        assert_eq!(remote_state_key(acme), "acme.redb");
        assert_eq!(remote_state_key(globex), "globex.redb");
        assert_ne!(remote_state_key(acme), remote_state_key(globex));
    }

    // Schema-version qualification: remote keys carry the engine's *schema*
    // version so two engine versions with different schemas never share a key.
    #[test]
    fn schema_version_segment_matches_current_schema() {
        assert_eq!(
            schema_version_segment(),
            format!("v{}", crate::state::current_schema_version())
        );
    }

    #[test]
    fn object_store_key_is_schema_qualified() {
        let seg = schema_version_segment();
        // Global file → `v9/state.redb`; namespaced → `v9/acme.redb`.
        assert_eq!(
            object_store_state_key("state.redb"),
            format!("{seg}/state.redb")
        );
        assert_eq!(
            object_store_state_key("acme.redb"),
            format!("{seg}/acme.redb")
        );
    }

    #[test]
    fn valkey_key_is_schema_qualified() {
        let seg = schema_version_segment();
        // Default prefix already ends in `:`, so the result is
        // `rocky:state:v9:state.redb` — matching the FR's wire format.
        assert_eq!(
            valkey_state_key(DEFAULT_VALKEY_PREFIX, "state.redb"),
            format!("rocky:state:{seg}:state.redb")
        );
    }

    // Two engine versions with different schema versions must never resolve to
    // the same remote key. We can't change `CURRENT_SCHEMA_VERSION` at runtime,
    // so assert the version segment is load-bearing in the composed key: a key
    // built with a different segment differs at the version position only.
    #[test]
    fn distinct_schema_versions_yield_distinct_keys() {
        let here = object_store_state_key("state.redb");
        let other = format!("v{}/state.redb", crate::state::current_schema_version() + 1);
        assert_ne!(here, other);
        // The local file stem maps to the same trailing object regardless of
        // version — only the version segment changes — so a patch bump (same
        // schema version) keeps sharing state.
        assert!(here.ends_with("/state.redb") && other.ends_with("/state.redb"));
    }

    // R8 (part 2) — no clobber. Two namespaced files round-trip through a real
    // object store at distinct keys and do not overwrite each other.
    #[tokio::test]
    async fn test_remote_keys_round_trip_without_clobber() {
        let dir = TempDir::new().unwrap();
        let provider = crate::object_store::ObjectStoreProvider::in_memory();

        // Two namespaced local files with distinct contents.
        let ns_dir = dir.path().join(crate::state::STATE_NAMESPACE_DIR);
        std::fs::create_dir_all(&ns_dir).unwrap();
        let a_local = ns_dir.join("ns_a.redb");
        let b_local = ns_dir.join("ns_b.redb");
        std::fs::write(&a_local, b"AAAA").unwrap();
        std::fs::write(&b_local, b"BBBB").unwrap();

        let key_a = remote_state_key(&a_local);
        let key_b = remote_state_key(&b_local);
        assert_ne!(key_a, key_b);

        // Upload both; the second must not overwrite the first.
        provider.upload_file(&a_local, &key_a).await.unwrap();
        provider.upload_file(&b_local, &key_b).await.unwrap();

        // Download each back into fresh local paths and assert contents are
        // intact (no clobber).
        let a_back = dir.path().join("a_back.redb");
        let b_back = dir.path().join("b_back.redb");
        provider.download_file(&key_a, &a_back).await.unwrap();
        provider.download_file(&key_b, &b_back).await.unwrap();
        assert_eq!(std::fs::read(&a_back).unwrap(), b"AAAA");
        assert_eq!(std::fs::read(&b_back).unwrap(), b"BBBB");
    }

    // Regression: the *upload* dispatch must land a namespaced state file at
    // its `<ns>.redb` remote key, NOT the legacy shared `state.redb`. The
    // strip path copies the file to a scratch under `std::env::temp_dir()`;
    // before the fix the leaf re-derived the key from that scratch's parent
    // (`/tmp`) and always wrote `state.redb`, so every namespace clobbered the
    // shared object on upload while download read the never-written namespaced
    // key (perpetual cold start). This drives the *real*
    // `upload_state → strip → dispatch → upload_to_object_store → cloud_provider`
    // chain via an injected in-memory store — it does NOT precompute the key
    // and call `provider.upload_file` directly the way
    // `test_remote_keys_round_trip_without_clobber` does, so it exercises the
    // path that was actually broken.
    //
    // The seed writes a real redb with a schema_cache entry (a local-only
    // table), so the strip path genuinely runs and the scratch copy is taken —
    // a non-redb seed would make strip fail, fall back to the unfiltered local
    // path, and the test would pass against the *unfixed* code.
    async fn assert_namespaced_upload_key(backend: StateBackend) {
        // Defensive: clear any override a prior test on this worker thread may
        // have left armed (the thread-local persists across tests on reuse).
        test_support::clear();
        let dir = TempDir::new().unwrap();
        let ns_dir = dir.path().join(crate::state::STATE_NAMESPACE_DIR);
        std::fs::create_dir_all(&ns_dir).unwrap();
        let local = ns_dir.join("acme.redb");
        seed_watermark_and_cache(&local);

        let label = backend.to_string();
        let provider = test_support::install(ObjectStoreProvider::in_memory());

        let config = StateConfig {
            backend,
            s3_bucket: Some("test-bucket".into()),
            gcs_bucket: Some("test-bucket".into()),
            ..Default::default()
        };

        // Default upload path: strips the schema cache into a /tmp scratch,
        // then dispatches. The remote key must still be derived from `local`.
        upload_state(&config, &local, false).await.unwrap();

        test_support::clear();

        // The namespaced object exists at the schema-qualified key
        // `v<N>/acme.redb` (the version segment keeps different-schema engines
        // from sharing the object)...
        let qualified = object_store_state_key("acme.redb");
        assert!(
            provider.exists(&qualified).await.unwrap(),
            "{label} upload must land at the schema-qualified namespaced key `{qualified}`"
        );
        // ...and neither the *unqualified* namespaced key nor the legacy shared
        // key was written (the version segment is load-bearing; the clobber is
        // avoided).
        assert!(
            !provider.exists("acme.redb").await.unwrap(),
            "{label} upload must NOT write the unqualified namespaced key `acme.redb`"
        );
        assert!(
            !provider.exists("state.redb").await.unwrap(),
            "{label} upload must NOT write the shared legacy `state.redb` for a namespaced file"
        );
    }

    #[tokio::test]
    async fn test_namespaced_upload_lands_at_ns_key_s3() {
        assert_namespaced_upload_key(StateBackend::S3).await;
    }

    #[tokio::test]
    async fn test_namespaced_upload_lands_at_ns_key_gcs() {
        assert_namespaced_upload_key(StateBackend::Gcs).await;
    }

    #[test]
    fn test_default_state_transfer_timeout() {
        let config = StateConfig::default();
        assert_eq!(config.transfer_timeout_seconds, 300);
    }

    #[tokio::test]
    async fn test_probe_state_backend_local_is_noop() {
        let config = StateConfig::default();
        assert!(matches!(config.backend, StateBackend::Local));
        probe_state_backend(&config)
            .await
            .expect("Local backend probe should be a no-op");
    }

    #[tokio::test]
    async fn test_probe_state_backend_s3_missing_bucket_fails_fast() {
        let config = StateConfig {
            backend: StateBackend::S3,
            s3_bucket: None,
            ..Default::default()
        };
        let err = probe_state_backend(&config)
            .await
            .expect_err("missing bucket should error");
        assert!(matches!(err, StateSyncError::MissingConfig(..)));
    }

    #[tokio::test]
    async fn test_probe_state_backend_valkey_missing_url_fails_fast() {
        let config = StateConfig {
            backend: StateBackend::Valkey,
            valkey_url: None,
            ..Default::default()
        };
        let err = probe_state_backend(&config)
            .await
            .expect_err("missing URL should error");
        assert!(matches!(err, StateSyncError::MissingConfig(..)));
    }

    #[test]
    fn test_probe_key_is_unique_across_calls() {
        let a = probe_key();
        let b = probe_key();
        assert_ne!(a, b, "probe_key should produce unique values per call");
        assert!(a.starts_with("doctor-probe-"));
        assert!(a.ends_with(".marker"));
    }

    #[test]
    fn test_probe_key_is_unique_within_one_clock_tick() {
        // Two calls that read the same `nanos` — what a coarse realtime clock
        // hands back for back-to-back calls. PID + nanos alone would collide.
        let nanos = 1_700_000_000_000_000_000u128;
        let a = probe_key_at(nanos);
        let b = probe_key_at(nanos);
        assert_ne!(
            a, b,
            "probe_key must not depend on clock resolution for uniqueness"
        );
    }

    #[test]
    fn test_default_on_upload_failure_is_skip() {
        let config = StateConfig::default();
        assert_eq!(config.on_upload_failure, StateUploadFailureMode::Skip);
    }

    #[test]
    fn test_is_transient_network_and_timeout() {
        assert!(is_transient(&StateSyncError::S3Upload("boom".into())));
        assert!(is_transient(&StateSyncError::S3Download("boom".into())));
        assert!(is_transient(&StateSyncError::GcsUpload("boom".into())));
        assert!(is_transient(&StateSyncError::GcsDownload("boom".into())));
        assert!(is_transient(&StateSyncError::Valkey("boom".into())));
        assert!(is_transient(&StateSyncError::Timeout(Duration::from_secs(
            1
        ))));
    }

    #[test]
    fn test_is_transient_permanent_errors_not_retried() {
        assert!(!is_transient(&StateSyncError::MissingConfig(
            "s3".into(),
            "bucket".into()
        )));
        assert!(!is_transient(&StateSyncError::CircuitOpen {
            consecutive_failures: 5
        }));
        assert!(!is_transient(&StateSyncError::RetryBudgetExhausted {
            limit: 3
        }));
    }

    #[tokio::test]
    async fn test_retry_transient_succeeds_after_two_transient_failures() {
        let cfg = RetryConfig {
            max_retries: 3,
            initial_backoff_ms: 1,
            max_backoff_ms: 5,
            backoff_multiplier: 2.0,
            jitter: false,
            ..RetryConfig::default()
        };
        let attempts = std::sync::atomic::AtomicU32::new(0);
        let result = retry_transient(&cfg, "test", || {
            let n = attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async move {
                if n < 2 {
                    Err(StateSyncError::S3Upload(format!("flake {n}")))
                } else {
                    Ok(())
                }
            }
        })
        .await;
        assert_eq!(result.unwrap(), 2, "should have taken 2 retries");
    }

    #[tokio::test]
    async fn test_retry_transient_gives_up_after_max_retries() {
        let cfg = RetryConfig {
            max_retries: 2,
            initial_backoff_ms: 1,
            max_backoff_ms: 5,
            backoff_multiplier: 2.0,
            jitter: false,
            ..RetryConfig::default()
        };
        let result = retry_transient(&cfg, "test", || async {
            Err(StateSyncError::S3Upload("always fails".into()))
        })
        .await;
        assert!(matches!(result, Err(StateSyncError::S3Upload(_))));
    }

    #[tokio::test]
    async fn test_retry_transient_does_not_retry_permanent_errors() {
        let cfg = RetryConfig {
            max_retries: 3,
            initial_backoff_ms: 1,
            max_backoff_ms: 5,
            backoff_multiplier: 2.0,
            jitter: false,
            ..RetryConfig::default()
        };
        let attempts = std::sync::atomic::AtomicU32::new(0);
        let result = retry_transient(&cfg, "test", || {
            attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async { Err(StateSyncError::MissingConfig("s3".into(), "bucket".into())) }
        })
        .await;
        assert!(matches!(result, Err(StateSyncError::MissingConfig(..))));
        assert_eq!(
            attempts.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "permanent errors should not be retried"
        );
    }

    #[tokio::test]
    async fn test_retry_budget_exhaustion_aborts_early() {
        let cfg = RetryConfig {
            max_retries: 5,
            initial_backoff_ms: 1,
            max_backoff_ms: 5,
            backoff_multiplier: 2.0,
            jitter: false,
            max_retries_per_run: Some(1),
            ..RetryConfig::default()
        };
        let attempts = std::sync::atomic::AtomicU32::new(0);
        let result = retry_transient(&cfg, "test", || {
            attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async { Err(StateSyncError::S3Upload("always flakes".into())) }
        })
        .await;
        // With budget=1: first attempt fails → consume retry slot → one
        // retry attempt → fails → budget exhausted → return
        // RetryBudgetExhausted. Total 2 calls to op() before we error.
        assert!(matches!(
            result,
            Err(StateSyncError::RetryBudgetExhausted { limit: 1 })
        ));
        assert_eq!(
            attempts.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "budget should abort after first retry",
        );
    }

    // -----------------------------------------------------------------------
    // strip_local_only_tables
    // -----------------------------------------------------------------------

    use crate::schema_cache::{SchemaCacheEntry, StoredColumn, schema_cache_key};
    use crate::state::{LOCAL_ONLY_TABLE_NAMES, StateStore};

    fn seed_watermark_and_cache(path: &Path) {
        let store = StateStore::open(path).expect("open state store for test seed");
        let now = chrono::Utc::now();
        store
            .set_watermark(
                "cat.sch.tbl",
                &rocky_ir::WatermarkState {
                    last_value: now,
                    updated_at: now,
                },
            )
            .unwrap();
        let key = schema_cache_key("cat", "staging", "orders");
        store
            .write_schema_cache_entry(
                &key,
                &SchemaCacheEntry {
                    columns: vec![StoredColumn {
                        name: "id".into(),
                        data_type: "BIGINT".into(),
                        nullable: false,
                    }],
                    cached_at: now,
                },
            )
            .unwrap();
        drop(store);
    }

    #[test]
    fn strip_local_only_tables_drops_schema_cache_preserves_watermarks() {
        let dir = TempDir::new().unwrap();
        let src = dir.path().join("state.redb");
        seed_watermark_and_cache(&src);

        let filtered =
            strip_local_only_tables(&src, LOCAL_ONLY_TABLE_NAMES).expect("filter should succeed");

        // Open the filtered DB and verify: watermark is preserved, schema
        // cache is empty.
        let filtered_store = StateStore::open(&filtered).unwrap();
        assert!(
            filtered_store
                .get_watermark("cat.sch.tbl")
                .unwrap()
                .is_some(),
            "watermark should survive the filter"
        );
        assert!(
            filtered_store.list_schema_cache().unwrap().is_empty(),
            "schema cache should be stripped"
        );
        drop(filtered_store);

        // Source is untouched — the schema_cache entry is still there.
        let src_store = StateStore::open(&src).unwrap();
        assert_eq!(src_store.list_schema_cache().unwrap().len(), 1);

        let _ = std::fs::remove_file(&filtered);
    }

    #[tokio::test]
    async fn upload_state_with_empty_excluded_fast_paths_local() {
        let dir = TempDir::new().unwrap();
        let src = dir.path().join("state.redb");
        seed_watermark_and_cache(&src);

        // Empty exclude list + local backend = no-op; just proves the
        // fast-path guard doesn't touch the file.
        let config = StateConfig::default();
        let result = upload_state_with_excluded_tables(&config, &src, &[]).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn upload_state_default_honours_local_only_list_on_local_backend() {
        // Local backend is a no-op for actual upload, but the fast-path
        // branch should still route through the default-excluded-list
        // wrapper without error.
        let dir = TempDir::new().unwrap();
        let src = dir.path().join("state.redb");
        seed_watermark_and_cache(&src);

        let config = StateConfig::default();
        assert!(upload_state(&config, &src, false).await.is_ok());
    }

    #[tokio::test]
    async fn upload_state_unless_recreated_suppresses_upload_when_recreated() {
        // Cross-pod tests hold this guard while exercising the same filtered
        // upload scratch-database path through a process-global provider.
        let _serial = test_support::serial_guard();

        // The "no-clobber" half of the mixed-version safety invariant: when the
        // local store was recreated after a forward-incompatible schema
        // mismatch, the end-of-run upload must NOT run — otherwise a downgraded
        // pod overwrites the newer shared state.
        //
        // We prove the upload is *not attempted* (not just that a bool flipped)
        // by pointing the config at a real upload backend that hard-errors the
        // moment dispatch is reached — S3 with no bucket, under the `Fail`
        // policy so the error propagates instead of being warn-swallowed:
        //   recreated = true  → Ok                       (upload skipped, broken backend never touched)
        //   recreated = false → Err(MissingConfig "s3")  (upload attempted, hit the broken S3 dispatch)
        // Deleting the suppression guard would make the `true` case also reach
        // the broken backend and fail — i.e. this test goes red, which is the
        // regression we want to catch.
        let dir = TempDir::new().unwrap();
        let src = dir.path().join("state.redb");
        // The local state file must exist, else `upload_state` early-returns Ok
        // and the `false` case would look "skipped" too.
        seed_watermark_and_cache(&src);

        let config = StateConfig {
            backend: StateBackend::S3,
            s3_bucket: None, // deliberately misconfigured → dispatch yields MissingConfig
            on_upload_failure: StateUploadFailureMode::Fail, // propagate, don't warn-swallow
            ..StateConfig::default()
        };

        // Recreated store: the upload is skipped entirely, so the broken
        // backend is never reached.
        assert!(
            upload_state_unless_recreated(true, &config, &src, false)
                .await
                .is_ok(),
            "a store recreated for forward-incompat must skip the end-of-run upload"
        );

        // Normal store: the upload runs and hits the (deliberately broken) S3
        // dispatch — proving the gate let a real upload attempt through.
        let err = upload_state_unless_recreated(false, &config, &src, false)
            .await
            .expect_err("an un-suppressed upload must actually attempt the backend");
        assert!(
            matches!(&err, StateSyncError::MissingConfig(backend, _) if backend == "s3"),
            "expected the S3 dispatch to be reached (MissingConfig for the bucket); got {err:?}"
        );
    }

    // -----------------------------------------------------------------------
    // S2 — a failed download must NOT be treated as authoritative-empty
    // -----------------------------------------------------------------------

    /// S2 (a): an injected existence-check failure makes `download_state` return
    /// `Err`, not `Ok`. Pre-fix the `Err(e)` arm logged and returned `Ok(())`
    /// ("non-fatal, starting fresh"), hiding a real download failure from the
    /// caller's fail-closed machinery. The in-memory provider never errors on
    /// `exists`, so we drive the arm through the `probe_exists` test seam — this
    /// is the only in-crate way to make the assertion RED before the fix.
    #[tokio::test]
    async fn download_existence_failure_propagates_not_fresh_start() {
        test_support::clear();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");

        // Route object-store construction to an in-memory provider (so no live
        // S3 client is built), then arm the existence-probe fault.
        let _provider = test_support::install(ObjectStoreProvider::in_memory());
        test_support::arm_object_store_exists_fault();

        let config = StateConfig {
            backend: StateBackend::S3,
            s3_bucket: Some("bucket".into()),
            ..Default::default()
        };
        let result = download_state(&config, &local, false).await;
        test_support::clear();

        assert!(
            matches!(result, Err(StateSyncError::S3Download(_))),
            "a failed existence check must propagate as Err (fail-closed), not a silent \
             fresh-start Ok; got {result:?}"
        );
        assert!(
            !local.exists(),
            "no local state file should be written when the download failed"
        );
    }

    /// S2 (a'): the absent case stays non-fatal. An in-memory provider with no
    /// object at the key returns `Ok(())` — a legit fresh start must not be
    /// turned into an error by the fail-closed change above.
    #[tokio::test]
    async fn download_absent_object_is_non_fatal_fresh_start() {
        test_support::clear();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        let _provider = test_support::install(ObjectStoreProvider::in_memory());

        let config = StateConfig {
            backend: StateBackend::S3,
            s3_bucket: Some("bucket".into()),
            ..Default::default()
        };
        let result = download_state(&config, &local, false).await;
        test_support::clear();

        assert!(
            matches!(result, Ok(StateAuthority::FreshStart)),
            "an absent remote object must be a fresh start (Ok(FreshStart)); got {result:?}"
        );
        assert!(
            !local.exists(),
            "nothing to restore, so no local file is written"
        );
    }

    // -----------------------------------------------------------------------
    // PR-A (RD-001) — typed StateAuthority across the download boundary
    // -----------------------------------------------------------------------

    /// An existing remote object restores and maps to `Ok(Authoritative)`.
    #[tokio::test]
    async fn download_maps_restored_to_authoritative() {
        test_support::clear();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        let remote_bytes = build_remote_object_bytes(dir.path(), "remote.fresh");
        let provider = test_support::install(ObjectStoreProvider::in_memory());
        let key = object_store_state_key(&remote_state_key(&local));
        provider.put(&key, Bytes::from(remote_bytes)).await.unwrap();

        let config = StateConfig {
            backend: StateBackend::S3,
            s3_bucket: Some("bucket".into()),
            ..Default::default()
        };
        let result = download_state(&config, &local, false).await;
        test_support::clear();
        assert!(
            matches!(result, Ok(StateAuthority::Authoritative)),
            "a restored remote object must map to Ok(Authoritative); got {result:?}"
        );
    }

    /// A genuinely-absent remote object maps to `Ok(FreshStart)`.
    #[tokio::test]
    async fn download_maps_absent_to_fresh_start() {
        test_support::clear();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        let _provider = test_support::install(ObjectStoreProvider::in_memory());

        let config = StateConfig {
            backend: StateBackend::S3,
            s3_bucket: Some("bucket".into()),
            ..Default::default()
        };
        let result = download_state(&config, &local, false).await;
        test_support::clear();
        assert!(
            matches!(result, Ok(StateAuthority::FreshStart)),
            "an absent remote object must map to Ok(FreshStart); got {result:?}"
        );
    }

    /// The F7 safe-standalone property: a download failure stays `Err` — it is
    /// NEVER collapsed into `Ok(Indeterminate)`. If it were, every fail-closed
    /// `download_state(...)?` seam would have its `?` succeed on a failed
    /// download and silently proceed on stale local state.
    #[tokio::test]
    async fn download_failure_stays_err_never_indeterminate() {
        test_support::clear();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        let _provider = test_support::install(ObjectStoreProvider::in_memory());
        test_support::arm_object_store_exists_fault();

        let config = StateConfig {
            backend: StateBackend::S3,
            s3_bucket: Some("bucket".into()),
            ..Default::default()
        };
        let result = download_state(&config, &local, false).await;
        test_support::clear();
        assert!(
            matches!(result, Err(StateSyncError::S3Download(_))),
            "a failed download must stay Err — never Ok(Indeterminate); got {result:?}"
        );
    }

    /// The Local backend maps to `Authoritative`, NOT `FreshStart`: the on-disk
    /// redb file IS the authority — there is no remote to be "fresh" against,
    /// and it must never be treated as a bootstrappable empty ledger.
    #[tokio::test]
    async fn local_backend_is_authoritative_not_fresh_start() {
        let config = StateConfig::default();
        let dir = TempDir::new().unwrap();
        let result = download_state(&config, &dir.path().join("state.redb"), false).await;
        assert!(
            matches!(result, Ok(StateAuthority::Authoritative)),
            "the Local backend's on-disk file is the authority; got {result:?}"
        );
    }

    /// S2 (b): a Valkey MISS with a *stale* local file present must fall through
    /// to the durable S3 tier, not short-circuit. Pre-fix the tiered dispatch
    /// matched `Ok(()) if local_path.exists()` and counted the stale file as a
    /// Valkey hit, starving the S3 fallback. Post-fix hit/miss is an explicit
    /// `DownloadOutcome`, so the S3 tier restores authoritative state over the
    /// stale bytes.
    #[tokio::test]
    async fn tiered_valkey_miss_with_stale_local_falls_through_to_s3() {
        test_support::clear();
        let dir = TempDir::new().unwrap();
        // Legacy global file → remote key `state.redb`.
        let local = dir.path().join(".rocky-state.redb");
        // A STALE local file left by a previous run — the exact bait the old
        // `local_path.exists()` heuristic mistook for a Valkey hit.
        std::fs::write(&local, b"STALE").unwrap();

        // The durable S3 tier (in-memory) holds authoritative state at the
        // schema-qualified key.
        let provider = test_support::install(ObjectStoreProvider::in_memory());
        let key = object_store_state_key(&remote_state_key(&local));
        provider
            .put(&key, Bytes::from_static(b"FRESH-FROM-S3"))
            .await
            .unwrap();

        // Force the Valkey leg to MISS (no live peer needed).
        test_support::arm_valkey_miss_fault();

        let config = StateConfig {
            backend: StateBackend::Tiered,
            s3_bucket: Some("bucket".into()),
            // valkey_url intentionally None: the miss fault short-circuits
            // before URL resolution, proving the fall-through independently.
            ..Default::default()
        };
        let outcome = download_state_inner(&config, &local, &remote_state_key(&local), None)
            .await
            .unwrap();
        test_support::clear();

        assert_eq!(
            outcome,
            DownloadOutcome::Restored,
            "the S3 tier must have restored state after the Valkey miss"
        );
        assert_eq!(
            std::fs::read(&local).unwrap(),
            b"FRESH-FROM-S3",
            "the stale local file must be replaced by the S3 tier's content — proving the \
             tiered dispatch fell through instead of treating the stale file as a Valkey hit"
        );
    }

    /// S2 (b'): with both tiers absent (Valkey miss + empty S3), the tiered
    /// download resolves to `Absent` — a real fresh start — and writes no local
    /// file. Complements (b): the fall-through does not manufacture a spurious
    /// `Restored` when there is genuinely nothing to restore.
    #[tokio::test]
    async fn tiered_both_tiers_absent_stays_absent() {
        test_support::clear();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");

        // In-memory S3 tier installed but EMPTY (no object at the key).
        let _provider = test_support::install(ObjectStoreProvider::in_memory());
        test_support::arm_valkey_miss_fault();

        let config = StateConfig {
            backend: StateBackend::Tiered,
            s3_bucket: Some("bucket".into()),
            ..Default::default()
        };
        let outcome = download_state_inner(&config, &local, &remote_state_key(&local), None)
            .await
            .unwrap();
        test_support::clear();

        assert_eq!(
            outcome,
            DownloadOutcome::Absent,
            "both tiers absent ⇒ Absent (a real fresh start)"
        );
        assert!(
            !local.exists(),
            "nothing to restore, so no local file is written"
        );
    }

    // -----------------------------------------------------------------------
    // Findings 6 + 7 — local-only-preserving merge on download
    // -----------------------------------------------------------------------

    /// Seed `path` as a local state store with a STALE replicated watermark plus
    /// the two LOCAL-ONLY tables populated (schema_cache + a jobs record), i.e.
    /// exactly the machine-local state a run-start download must not wipe.
    fn seed_local_with_local_only(path: &Path, replicated_wm: &str, job_id: &str) {
        let now = chrono::Utc::now();
        let store = StateStore::open(path).expect("open local seed store");
        store
            .set_watermark(
                replicated_wm,
                &rocky_ir::WatermarkState {
                    last_value: now,
                    updated_at: now,
                },
            )
            .unwrap();
        store
            .write_schema_cache_entry(
                &schema_cache_key("cat", "staging", "orders"),
                &SchemaCacheEntry {
                    columns: vec![StoredColumn {
                        name: "id".into(),
                        data_type: "BIGINT".into(),
                        nullable: false,
                    }],
                    cached_at: now,
                },
            )
            .unwrap();
        store
            .record_job(&crate::state::PersistedJob {
                job_id: job_id.to_string(),
                kind: "run".into(),
                state: "running".into(),
                submitted_at: now.to_rfc3339(),
                started_at: None,
                finished_at: None,
                principal: None,
                error: None,
                result: None,
                redaction_version: Some(crate::state::CURRENT_REDACTION_VERSION),
            })
            .unwrap();
        drop(store);
    }

    /// Build the bytes of a REMOTE state object: a valid store carrying an
    /// authoritative replicated watermark with the local-only tables STRIPPED —
    /// exactly what `upload_state` leaves on the wire.
    fn build_remote_object_bytes(dir: &Path, replicated_wm: &str) -> Vec<u8> {
        let now = chrono::Utc::now();
        let seed = dir.join("remote_seed.redb");
        {
            let store = StateStore::open(&seed).unwrap();
            store
                .set_watermark(
                    replicated_wm,
                    &rocky_ir::WatermarkState {
                        last_value: now,
                        updated_at: now,
                    },
                )
                .unwrap();
            drop(store);
        }
        let stripped = strip_local_only_tables(&seed, LOCAL_ONLY_TABLE_NAMES).unwrap();
        let bytes = std::fs::read(&stripped).unwrap();
        let _ = std::fs::remove_file(&stripped);
        let _ = std::fs::remove_file(&seed);
        bytes
    }

    /// Finding 7 (Restored): a download that wholesale-replaces the replicated
    /// tables must PRESERVE the local-only tables (jobs, schema_cache), and
    /// finding 6 (Restored half): the stale local replicated watermark is
    /// replaced by the remote's authoritative one.
    #[tokio::test]
    async fn download_restored_replaces_replicated_keeps_local_only() {
        test_support::clear();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");

        seed_local_with_local_only(&local, "local.stale", "job-local-1");
        let remote_bytes = build_remote_object_bytes(dir.path(), "remote.fresh");

        let provider = test_support::install(ObjectStoreProvider::in_memory());
        let key = object_store_state_key(&remote_state_key(&local));
        provider.put(&key, Bytes::from(remote_bytes)).await.unwrap();

        let config = StateConfig {
            backend: StateBackend::S3,
            s3_bucket: Some("bucket".into()),
            ..Default::default()
        };
        let authority = download_state(&config, &local, false)
            .await
            .expect("restored download should succeed");
        test_support::clear();
        assert_eq!(
            authority,
            StateAuthority::Authoritative,
            "a restored remote object maps to Authoritative"
        );

        let store = StateStore::open(&local).unwrap();
        assert!(
            store.get_watermark("remote.fresh").unwrap().is_some(),
            "the remote's replicated watermark must be restored"
        );
        assert!(
            store.get_watermark("local.stale").unwrap().is_none(),
            "the stale local replicated watermark must be replaced by the remote copy"
        );
        assert_eq!(
            store.list_schema_cache().unwrap().len(),
            1,
            "local-only schema_cache must survive a download that replaces the replicated tables"
        );
        let jobs = store.list_jobs().unwrap();
        assert_eq!(
            jobs.len(),
            1,
            "local-only jobs must survive a download that replaces the replicated tables"
        );
        assert_eq!(jobs[0].job_id, "job-local-1");
    }

    /// Finding 6 (Absent): switching to an EMPTY remote prefix must reset the
    /// replicated tables to fresh (no stale watermark survives) — while finding
    /// 7 (Absent half) preserves the local-only tables.
    #[tokio::test]
    async fn download_absent_clears_replicated_keeps_local_only() {
        test_support::clear();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");

        seed_local_with_local_only(&local, "local.stale", "job-local-1");

        // In-memory provider installed but EMPTY → the download resolves Absent.
        let _provider = test_support::install(ObjectStoreProvider::in_memory());
        let config = StateConfig {
            backend: StateBackend::S3,
            s3_bucket: Some("bucket".into()),
            ..Default::default()
        };
        let authority = download_state(&config, &local, false)
            .await
            .expect("an absent remote is a fresh start");
        test_support::clear();
        assert_eq!(
            authority,
            StateAuthority::FreshStart,
            "an absent remote object maps to FreshStart"
        );

        let store = StateStore::open(&local).unwrap();
        assert!(
            store.get_watermark("local.stale").unwrap().is_none(),
            "an Absent download on a remote backend must reset the replicated tables to fresh — \
             a stale watermark must NOT keep the run from re-reading source rows (finding 6)"
        );
        assert_eq!(
            store.list_schema_cache().unwrap().len(),
            1,
            "Absent must keep the local-only schema_cache (finding 7)"
        );
        let jobs = store.list_jobs().unwrap();
        assert_eq!(
            jobs.len(),
            1,
            "Absent must keep the local-only jobs (finding 7)"
        );
        assert_eq!(jobs[0].job_id, "job-local-1");
    }

    /// The copy primitive itself: it replaces ONLY the named (local-only) tables
    /// in `dst`, leaves every replicated table in `dst` untouched, drags NO
    /// replicated table across from `src`, and drops a stale `dst` row.
    #[test]
    fn copy_named_tables_replaces_only_named_tables() {
        let dir = TempDir::new().unwrap();
        let src = dir.path().join("src.redb");
        let dst = dir.path().join("dst.redb");
        let now = chrono::Utc::now();

        // src: a replicated watermark + the local-only tables populated.
        seed_local_with_local_only(&src, "src.replicated", "j-src");

        // dst: a DIFFERENT replicated watermark + a STALE local-only entry.
        {
            let store = StateStore::open(&dst).unwrap();
            store
                .set_watermark(
                    "dst.replicated",
                    &rocky_ir::WatermarkState {
                        last_value: now,
                        updated_at: now,
                    },
                )
                .unwrap();
            store
                .write_schema_cache_entry(
                    &schema_cache_key("stale", "stale", "stale"),
                    &SchemaCacheEntry {
                        columns: vec![],
                        cached_at: now,
                    },
                )
                .unwrap();
            drop(store);
        }

        copy_named_tables(&src, &dst, LOCAL_ONLY_TABLE_NAMES).unwrap();

        let store = StateStore::open(&dst).unwrap();
        // dst's replicated table is untouched.
        assert!(
            store.get_watermark("dst.replicated").unwrap().is_some(),
            "copy must not touch replicated tables in dst"
        );
        // src's replicated table did NOT leak across (copy is scoped to local-only).
        assert!(
            store.get_watermark("src.replicated").unwrap().is_none(),
            "copy must not drag a replicated table across from src"
        );
        // dst's local-only tables were REPLACED by src's (stale row gone).
        let sc = store.list_schema_cache().unwrap();
        assert_eq!(
            sc.len(),
            1,
            "the stale dst schema_cache entry must be replaced"
        );
        assert_eq!(sc[0].0, schema_cache_key("cat", "staging", "orders"));
        let jobs = store.list_jobs().unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].job_id, "j-src");
    }

    /// Finding 5 — the mechanism the fail-closed seam uploads rely on: a broken
    /// upload (S3 with no bucket → `MissingConfig`) is SWALLOWED under the
    /// default `Skip` policy but PROPAGATES under `Fail`. The freeze / gc-tombstone
    /// seams force `Fail` so a state that commits locally but never reaches the
    /// remote can never be reported as success.
    #[tokio::test]
    async fn upload_state_skip_swallows_but_fail_propagates() {
        let dir = TempDir::new().unwrap();
        let src = dir.path().join("state.redb");
        seed_watermark_and_cache(&src);

        let skip = StateConfig {
            backend: StateBackend::S3,
            s3_bucket: None,
            on_upload_failure: StateUploadFailureMode::Skip,
            ..Default::default()
        };
        assert!(
            upload_state(&skip, &src, false).await.is_ok(),
            "the default Skip policy must swallow an upload failure (degraded mode)"
        );

        let fail = StateConfig {
            backend: StateBackend::S3,
            s3_bucket: None,
            on_upload_failure: StateUploadFailureMode::Fail,
            ..Default::default()
        };
        assert!(
            matches!(
                upload_state(&fail, &src, false).await,
                Err(StateSyncError::MissingConfig(..))
            ),
            "the Fail policy the seams force must propagate the upload failure (abort)"
        );
    }

    /// Finding 5(b) — fail-closed preserve. A PRESENT but unreadable local file
    /// (not a redb) cannot be snapshotted, so the local-only tables cannot be
    /// preserved. The download must FAIL CLOSED (Err) rather than silently
    /// proceed and discard `jobs` / `schema_cache`; and the prior local file must
    /// be left untouched (never replaced by remote-only content).
    #[tokio::test]
    async fn download_snapshot_failure_fails_closed() {
        test_support::clear();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        std::fs::write(&local, b"not a redb file").unwrap();

        // A remote object exists, so the download itself would otherwise succeed.
        let remote_bytes = build_remote_object_bytes(dir.path(), "remote.fresh");
        let provider = test_support::install(ObjectStoreProvider::in_memory());
        let key = object_store_state_key(&remote_state_key(&local));
        provider.put(&key, Bytes::from(remote_bytes)).await.unwrap();

        let config = StateConfig {
            backend: StateBackend::S3,
            s3_bucket: Some("bucket".into()),
            ..Default::default()
        };
        let result = download_state(&config, &local, false).await;
        test_support::clear();

        assert!(
            result.is_err(),
            "an unreadable local file must fail the local-only snapshot CLOSED, never silently \
             discard jobs / schema_cache"
        );
        assert_eq!(
            std::fs::read(&local).unwrap(),
            b"not a redb file",
            "the prior local file must be left intact on a fail-closed download (never replaced \
             by remote-only content)"
        );
    }

    /// Finding 6(a) — on a `Restored` download with NO prior local file, the
    /// remote's local-only rows must be SCRUBBED, not trusted. A pre-this-patch
    /// remote snapshot (uploaded when only `schema_cache` was local-only) can
    /// carry another pod's `jobs` rows; those must never land locally.
    #[tokio::test]
    async fn download_restored_no_local_file_scrubs_remote_local_only() {
        test_support::clear();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        assert!(!local.exists(), "precondition: no prior local file");

        // Remote object carrying replicated + LOCAL-ONLY rows (UNstripped, as a
        // pre-patch upload would have left them).
        let remote_seed = dir.path().join("remote_seed.redb");
        seed_local_with_local_only(&remote_seed, "remote.fresh", "job-from-other-pod");
        let remote_bytes = std::fs::read(&remote_seed).unwrap();
        let provider = test_support::install(ObjectStoreProvider::in_memory());
        let key = object_store_state_key(&remote_state_key(&local));
        provider.put(&key, Bytes::from(remote_bytes)).await.unwrap();

        let config = StateConfig {
            backend: StateBackend::S3,
            s3_bucket: Some("bucket".into()),
            ..Default::default()
        };
        let authority = download_state(&config, &local, false)
            .await
            .expect("restored download should succeed");
        test_support::clear();
        assert_eq!(authority, StateAuthority::Authoritative);

        // A cold pod's first reads come BEFORE its first run. The scrub must
        // leave the local-only tables present and empty — not dropped — so a
        // read-only open (every `GET` on `rocky serve`) finds every table and
        // takes no write transaction. Byte identity is the evidence: a
        // committed redb transaction advances the on-disk transaction id.
        let before = std::fs::read(&local).unwrap();
        {
            let ro = StateStore::open_read_only(&local)
                .expect("a cold-restored store opens read-only without a write");
            assert!(
                ro.list_jobs().unwrap().is_empty(),
                "the scrubbed jobs table is present and empty on the read-only path"
            );
        }
        assert!(
            before == std::fs::read(&local).unwrap(),
            "a read-only open of a cold-restored store wrote: the scrub dropped a local-only \
             table instead of emptying it"
        );

        let store = StateStore::open(&local).unwrap();
        assert!(
            store.get_watermark("remote.fresh").unwrap().is_some(),
            "the remote's replicated watermark must be present"
        );
        assert!(
            store.list_jobs().unwrap().is_empty(),
            "a foreign pod's jobs carried by a stale remote must be scrubbed when there is no \
             prior local file (finding 6a)"
        );
        assert!(
            store.list_schema_cache().unwrap().is_empty(),
            "a stale remote's schema_cache must be scrubbed when there is no prior local file"
        );
    }

    /// Finding 6(b) — a local-only filter failure must NEVER fall back to
    /// uploading the unfiltered local file: under `Fail` the upload aborts, and
    /// NO object is written (the local-only rows never leak to the remote).
    #[tokio::test]
    async fn upload_filter_failure_does_not_upload_unfiltered() {
        test_support::clear();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        // Garbage: `strip_local_only_tables` copies it, then fails to open it as
        // a redb → the filter fails.
        std::fs::write(&local, b"not a redb file").unwrap();

        let provider = test_support::install(ObjectStoreProvider::in_memory());
        let config = StateConfig {
            backend: StateBackend::S3,
            s3_bucket: Some("bucket".into()),
            on_upload_failure: StateUploadFailureMode::Fail,
            ..Default::default()
        };
        let result = upload_state(&config, &local, false).await;
        let key = object_store_state_key(&remote_state_key(&local));
        let uploaded = provider.exists(&key).await.unwrap();
        test_support::clear();

        assert!(
            result.is_err(),
            "a local-only filter failure must abort under Fail, not upload unfiltered"
        );
        assert!(
            !uploaded,
            "no state object may be uploaded when the local-only filter fails (no leak)"
        );
    }

    /// Finding B: a download must not publish over a LIVE writer. While a
    /// `StateStore` holds the writer lock, `download_state` downloads to staging
    /// but cannot acquire the lock to publish, so it fails closed and leaves the
    /// live writer's file intact. Once the lock frees, a fresh download applies
    /// the remote and preserves the local-only tables.
    #[tokio::test]
    async fn download_publish_fails_closed_when_writer_lock_held() {
        test_support::clear();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        seed_local_with_local_only(&local, "local.wm", "job-1");

        let remote_bytes = build_remote_object_bytes(dir.path(), "remote.fresh");
        let provider = test_support::install(ObjectStoreProvider::in_memory());
        let key = object_store_state_key(&remote_state_key(&local));
        provider.put(&key, Bytes::from(remote_bytes)).await.unwrap();

        let config = StateConfig {
            backend: StateBackend::S3,
            s3_bucket: Some("bucket".into()),
            ..Default::default()
        };

        // Hold the writer lock, as a live `rocky run` would during execution.
        let held = StateStore::open(&local).unwrap();

        let result = download_state(&config, &local, false).await;
        assert!(
            result.is_err(),
            "the publish must fail closed while another writer holds the lock (finding B)"
        );
        assert!(
            held.get_watermark("local.wm").unwrap().is_some(),
            "a concurrent download must not clobber the live writer's state"
        );

        // Release the lock; a fresh download now applies the remote + preserves
        // the local-only `jobs`.
        drop(held);
        let authority = download_state(&config, &local, false)
            .await
            .expect("download should succeed once the writer lock is free");
        test_support::clear();
        assert_eq!(authority, StateAuthority::Authoritative);

        let store = StateStore::open(&local).unwrap();
        assert!(
            store.get_watermark("remote.fresh").unwrap().is_some(),
            "the remote replicated watermark is applied after the lock frees"
        );
        assert_eq!(
            store.list_jobs().unwrap().len(),
            1,
            "local-only jobs preserved"
        );
    }

    /// Finding C: a GENUINE `delete_table` error must FAIL the filter — not be
    /// swallowed and let an UNFILTERED scratch upload this pod's local-only rows.
    /// A `schema_cache` created as a MULTIMAP table makes the strip's regular
    /// `delete_table` error (`TableIsMultimap`) rather than the benign
    /// missing-table `Ok(false)`. `strip_local_only_tables` propagates it, and the
    /// upload therefore aborts under `Fail` with nothing written.
    #[tokio::test]
    async fn upload_delete_table_error_fails_closed_no_unfiltered_upload() {
        test_support::clear();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");

        // A valid redb whose `schema_cache` is a MULTIMAP table, so the strip's
        // regular `delete_table("schema_cache")` returns a genuine error instead
        // of `Ok(false)`.
        {
            let db = redb::Database::create(&local).unwrap();
            let txn = db.begin_write().unwrap();
            {
                let def: redb::MultimapTableDefinition<&str, &[u8]> =
                    redb::MultimapTableDefinition::new("schema_cache");
                let mut t = txn.open_multimap_table(def).unwrap();
                t.insert("k", b"v".as_slice()).unwrap();
            }
            txn.commit().unwrap();
        }

        // Direct: the filter propagates the delete_table error.
        assert!(
            strip_local_only_tables(&local, LOCAL_ONLY_TABLE_NAMES).is_err(),
            "a genuine delete_table error must fail the filter (finding C)"
        );

        // End-to-end: the upload aborts under Fail and writes NO object.
        let provider = test_support::install(ObjectStoreProvider::in_memory());
        let config = StateConfig {
            backend: StateBackend::S3,
            s3_bucket: Some("bucket".into()),
            on_upload_failure: StateUploadFailureMode::Fail,
            ..Default::default()
        };
        let result = upload_state(&config, &local, false).await;
        let key = object_store_state_key(&remote_state_key(&local));
        let uploaded = provider.exists(&key).await.unwrap();
        test_support::clear();

        assert!(
            result.is_err(),
            "the upload must abort on a delete_table filter error under Fail (finding C)"
        );
        assert!(
            !uploaded,
            "no unfiltered state object may be uploaded when the filter fails (finding C)"
        );
    }

    /// Finding 3: the budget-burn decision PAIR (a plain rule-decision row + a
    /// verify-after custody Deny for the same plan) must survive the remote
    /// round-trip, so a later agent action on another pod burns the autonomy
    /// budget. Pod A records the pair and uploads; pod B downloads a fresh copy;
    /// `budget_failures_in_window` counts the failed plan.
    #[tokio::test]
    async fn budget_pair_survives_remote_round_trip_and_burns_budget() {
        use crate::config::{PolicyCapability, PolicyEffect, PolicyPrincipal};
        use crate::state::PolicyDecisionRecord;

        test_support::clear();
        let _provider = test_support::install(ObjectStoreProvider::in_memory());
        let now = chrono::Utc::now();

        // Pod A: a failed governed apply — the plain rule decision (winning rule
        // 0) plus the verify-after custody Deny, both keyed to `plan-1`.
        let dir_a = TempDir::new().unwrap();
        let pod_a = dir_a.path().join(".rocky-state.redb");
        {
            let store = StateStore::open(&pod_a).unwrap();
            store
                .record_policy_decision(&PolicyDecisionRecord {
                    seq: 0,
                    keys_recorded: false,
                    fail_closed: false,
                    models: Vec::new(),
                    timestamp: now,
                    plan_id: "plan-1".into(),
                    principal: PolicyPrincipal::Agent,
                    capability: PolicyCapability::Apply,
                    model: "m".into(),
                    effect: PolicyEffect::Allow,
                    rule_id: Some(0),
                    reason: "plain rule decision".into(),
                    verify_after: vec![],
                    auto_apply: None,
                    principal_ref: None,
                })
                .unwrap();
            store
                .record_policy_decision(&PolicyDecisionRecord {
                    seq: 0,
                    keys_recorded: false,
                    fail_closed: false,
                    models: Vec::new(),
                    timestamp: now,
                    plan_id: "plan-1".into(),
                    principal: PolicyPrincipal::Agent,
                    capability: PolicyCapability::Apply,
                    model: "*".into(),
                    effect: PolicyEffect::Deny,
                    rule_id: None,
                    reason: "verify_after FAILED".into(),
                    verify_after: vec!["row_count".into()],
                    auto_apply: None,
                    principal_ref: None,
                })
                .unwrap();
            drop(store);
        }

        let config = StateConfig {
            backend: StateBackend::S3,
            s3_bucket: Some("bucket".into()),
            ..Default::default()
        };
        upload_state(&config, &pod_a, false).await.unwrap();

        // Pod B: a fresh pod downloads the remote state.
        let dir_b = TempDir::new().unwrap();
        let pod_b = dir_b.path().join(".rocky-state.redb");
        assert_eq!(
            download_state(&config, &pod_b, false).await.unwrap(),
            StateAuthority::Authoritative
        );
        test_support::clear();

        let store = StateStore::open(&pod_b).unwrap();
        let decisions = store.list_policy_decisions().unwrap();
        let burned = crate::policy::budget_failures_in_window(
            &decisions,
            0,
            chrono::Duration::hours(24),
            now + chrono::Duration::seconds(1),
        );
        assert_eq!(
            burned, 1,
            "the rule-decision + verify-custody pair must survive the remote round-trip so the \
             failed apply burns rule 0's budget (finding 3)"
        );
    }

    // -----------------------------------------------------------------------
    // WP-01 PR-B (2a) — RemoteStateSession lifecycle state machine
    // -----------------------------------------------------------------------

    /// Install a fault-counting in-memory provider (thread-local) and return
    /// its control handle, so a session test can both arm faults and prove
    /// call counts (e.g. the Local backend's zero-I/O contract).
    fn install_counting_provider() -> crate::fault_store::FaultHandle {
        let (store, faults) = crate::fault_store::FaultingStore::wrap(std::sync::Arc::new(
            object_store::memory::InMemory::new(),
        ));
        test_support::install(ObjectStoreProvider::from_store(store, "s3", "bucket", ""));
        faults
    }

    /// Like [`install_counting_provider`] but PROCESS-GLOBAL, so the counting
    /// store is visible from the periodic uploader's own tokio worker thread
    /// (the thread-local [`test_support::install`] only covers the test thread).
    /// Hold the returned guard for the test's duration; pair with
    /// [`test_support::serial_guard`] (the global override is shared state).
    fn install_counting_provider_global() -> (
        crate::fault_store::FaultHandle,
        test_support::GlobalOverrideGuard,
    ) {
        let (store, faults) = crate::fault_store::FaultingStore::wrap(std::sync::Arc::new(
            object_store::memory::InMemory::new(),
        ));
        let guard = test_support::install_global(ObjectStoreProvider::from_store(
            store, "s3", "bucket", "",
        ));
        (faults, guard)
    }

    /// Seed a real (empty) redb state file at `path` so the upload path has a
    /// file to strip/dispatch.
    fn seed_state_file(path: &Path) {
        let store = StateStore::open(path).expect("seed session state file");
        drop(store);
    }

    #[tokio::test]
    async fn session_checkpoint_publishes_live_plan_and_advances_cas_base_before_finalize() {
        test_support::clear();
        let provider = ObjectStoreProvider::in_memory();
        let _guard = test_support::install(provider.clone());
        let dir = TempDir::new().unwrap();
        let local = dir
            .path()
            .join(crate::state::STATE_NAMESPACE_DIR)
            .join("recovery.redb");
        std::fs::create_dir_all(local.parent().unwrap()).unwrap();
        let mut cfg = s3_session_config(StateUploadFailureMode::Fail);
        cfg.concurrency_control = Some(ConcurrencyControl::Cas);
        let mut session =
            RemoteStateSession::new(&cfg, &local, FinalizeDurability::ConfigDefault, false);
        assert_eq!(session.acquire().await.unwrap(), StateAuthority::FreshStart);
        let store = StateStore::open(&local).unwrap();
        store
            .write_schema_cache_entry(
                "local-cache",
                &crate::schema_cache::SchemaCacheEntry {
                    columns: vec![],
                    cached_at: chrono::Utc::now(),
                },
            )
            .unwrap();
        let recovery = crate::state::WatermarkRecoveryTable {
            source: rocky_ir::TableRef {
                catalog: "source".into(),
                schema: "raw".into(),
                table: "orders".into(),
            },
            target: rocky_ir::TableRef {
                catalog: "wh".into(),
                schema: "raw".into(),
                table: "orders".into(),
            },
            timestamp_column: "synced_at".into(),
            prior_watermark: Some(chrono::Utc::now()),
        };
        store
            .init_run_progress_with_recovery(
                "planned",
                &["wh.raw.orders".into()],
                None,
                std::slice::from_ref(&recovery),
            )
            .unwrap();
        assert!(
            session
                .publish_checkpoint(&store, FinalizeDurability::ConfigDefault)
                .await
                .unwrap()
        );
        assert!(
            session.base.is_some(),
            "a committed bootstrap publication must advance the held base"
        );
        assert!(
            provider
                .exists(&object_store_state_key("recovery.redb"))
                .await
                .unwrap()
        );
        assert!(
            !provider
                .exists(&object_store_state_key("state.redb"))
                .await
                .unwrap()
        );

        // A fresh pod can recover the plan even if this process dies before
        // its first periodic tick or final upload, while the live store is open.
        let other_dir = TempDir::new().unwrap();
        let restored = other_dir
            .path()
            .join(crate::state::STATE_NAMESPACE_DIR)
            .join("recovery.redb");
        std::fs::create_dir_all(restored.parent().unwrap()).unwrap();
        assert_eq!(
            download_state(&cfg, &restored, false).await.unwrap(),
            StateAuthority::Authoritative
        );
        let restored_store = StateStore::open(&restored).unwrap();
        assert!(
            restored_store
                .read_schema_cache_entry("local-cache")
                .unwrap()
                .is_none(),
            "pre-copy publication must preserve local-only filtering"
        );
        let progress = restored_store.get_run_progress("planned").unwrap().unwrap();
        assert_eq!(progress.watermark_recovery_tables, Some(vec![recovery]));
        assert!(!progress.watermarks_confirmed);
        drop(restored_store);

        store
            .batch_set_watermarks_and_confirm_runs(&[], &["planned"])
            .unwrap();
        drop(store);
        session
            .finalize()
            .await
            .expect("terminal CAS must use the publication's committed base");
        assert_eq!(
            download_state(&cfg, &restored, false).await.unwrap(),
            StateAuthority::Authoritative
        );
        assert!(
            StateStore::open(&restored)
                .unwrap()
                .get_run_progress("planned")
                .unwrap()
                .unwrap()
                .watermarks_confirmed
        );
        test_support::clear();
    }

    #[tokio::test]
    async fn session_checkpoint_cas_conflict_never_advances_or_refreshes_base_under_skip() {
        test_support::clear();
        let faults = install_counting_provider();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        let mut cfg = s3_session_config(StateUploadFailureMode::Skip);
        cfg.concurrency_control = Some(ConcurrencyControl::Cas);
        let mut stale =
            RemoteStateSession::new(&cfg, &local, FinalizeDurability::ConfigDefault, false);
        assert_eq!(stale.acquire().await.unwrap(), StateAuthority::FreshStart);

        let racer_dir = TempDir::new().unwrap();
        let racer_path = racer_dir.path().join(".rocky-state.redb");
        let mut racer =
            RemoteStateSession::new(&cfg, &racer_path, FinalizeDurability::ConfigDefault, false);
        assert_eq!(racer.acquire().await.unwrap(), StateAuthority::FreshStart);
        seed_state_file(&racer_path);
        racer.finalize().await.unwrap();

        let store = StateStore::open(&local).unwrap();
        store
            .init_run_progress_with_recovery("stale-plan", &[], None, &[])
            .unwrap();
        let get_count = faults.count(crate::fault_store::FaultOp::Get);
        let err = stale
            .publish_checkpoint(&store, FinalizeDurability::ConfigDefault)
            .await
            .unwrap_err();
        assert!(matches!(err, StateSyncError::CasConflict { .. }));
        assert!(
            stale.base.is_none(),
            "a conflict cannot advance the acquire base"
        );
        assert_eq!(
            faults.count(crate::fault_store::FaultOp::Get),
            get_count,
            "a conflict cannot refresh state"
        );
        drop(store);
        stale.abandon("conflict").await;
        test_support::clear();
    }

    #[tokio::test]
    async fn session_checkpoint_transport_failure_distinguishes_skip_strict_and_governed() {
        test_support::clear();
        let faults = install_counting_provider();
        faults.arm(
            crate::fault_store::FaultOp::Put,
            crate::fault_store::FaultMode::FailAll,
        );
        for (mode, durability, should_fail) in [
            (
                StateUploadFailureMode::Skip,
                FinalizeDurability::ConfigDefault,
                false,
            ),
            (
                StateUploadFailureMode::Fail,
                FinalizeDurability::ConfigDefault,
                true,
            ),
            (
                StateUploadFailureMode::Skip,
                FinalizeDurability::Durable,
                true,
            ),
        ] {
            let dir = TempDir::new().unwrap();
            let local = dir.path().join(".rocky-state.redb");
            let mut cfg = s3_session_config(mode);
            cfg.concurrency_control = Some(ConcurrencyControl::Cas);
            let mut session = RemoteStateSession::new(&cfg, &local, durability, false);
            assert_eq!(session.acquire().await.unwrap(), StateAuthority::FreshStart);
            let store = StateStore::open(&local).unwrap();
            store
                .init_run_progress_with_recovery("plan", &[], None, &[])
                .unwrap();
            let result = session.publish_checkpoint(&store, durability).await;
            if should_fail {
                assert!(result.unwrap_err().to_string().contains("injected fault"));
            } else {
                assert!(
                    !result.unwrap(),
                    "skipped publication is not durable evidence"
                );
            }
            assert!(
                session.base.is_none(),
                "transport failure cannot advance the CAS base"
            );
            drop(store);
            session.abandon("publication failed").await;
        }
        test_support::clear();
    }

    #[tokio::test]
    async fn session_checkpoint_suppression_does_not_publish_and_governed_refuses_it() {
        test_support::clear();
        let faults = install_counting_provider();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        let mut session = RemoteStateSession::new(
            &s3_session_config(StateUploadFailureMode::Skip),
            &local,
            FinalizeDurability::ConfigDefault,
            false,
        );
        assert_eq!(session.acquire().await.unwrap(), StateAuthority::FreshStart);
        let store = StateStore::open(&local).unwrap();
        session.set_suppress_upload("forward-incompatible recreation");
        assert!(
            !session
                .publish_checkpoint(&store, FinalizeDurability::ConfigDefault)
                .await
                .unwrap()
        );
        assert!(
            session
                .publish_checkpoint(&store, FinalizeDurability::Durable)
                .await
                .unwrap_err()
                .to_string()
                .contains("publication suppressed")
        );
        assert_eq!(faults.count(crate::fault_store::FaultOp::Put), 0);
        drop(store);
        session.abandon("suppressed").await;
        test_support::clear();
    }

    #[tokio::test]
    async fn session_checkpoint_non_authoritative_download_never_publishes() {
        test_support::clear();
        let faults = install_counting_provider();
        test_support::arm_object_store_exists_fault();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        let mut session = RemoteStateSession::new(
            &s3_session_config(StateUploadFailureMode::Skip),
            &local,
            FinalizeDurability::ConfigDefault,
            false,
        );
        assert_eq!(
            session.acquire().await.unwrap(),
            StateAuthority::Indeterminate
        );
        let store = StateStore::open(&local).unwrap();
        assert!(
            !session
                .publish_checkpoint(&store, FinalizeDurability::ConfigDefault)
                .await
                .unwrap()
        );
        assert!(
            session
                .publish_checkpoint(&store, FinalizeDurability::Durable)
                .await
                .is_err()
        );
        assert_eq!(faults.count(crate::fault_store::FaultOp::Put), 0);
        drop(store);
        session.abandon("non-authoritative state").await;
        test_support::clear();
    }

    #[tokio::test]
    async fn session_checkpoint_local_is_durable_and_publication_precedes_periodic_uploads() {
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        let store = Arc::new(StateStore::open(&local).unwrap());
        let mut session = RemoteStateSession::new(
            &StateConfig::default(),
            &local,
            FinalizeDurability::Durable,
            false,
        );
        assert!(
            session
                .publish_checkpoint(&store, FinalizeDurability::Durable)
                .await
                .is_err()
        );
        assert_eq!(
            session.acquire().await.unwrap(),
            StateAuthority::Authoritative
        );
        assert!(
            session
                .publish_checkpoint(&store, FinalizeDurability::Durable)
                .await
                .unwrap()
        );
        session.start_periodic_uploader(Arc::downgrade(&store), Duration::from_secs(60));
        assert!(
            session
                .publish_checkpoint(&store, FinalizeDurability::Durable)
                .await
                .is_err()
        );
        session.abandon("test teardown").await;
    }

    fn s3_session_config(on_upload_failure: StateUploadFailureMode) -> StateConfig {
        StateConfig {
            backend: StateBackend::S3,
            s3_bucket: Some("bucket".into()),
            on_upload_failure,
            // No retries: the armed-fault tests assert on the FIRST terminal
            // attempt, and backoff sleep would only slow the suite down.
            retry: RetryConfig {
                max_retries: 0,
                ..RetryConfig::default()
            },
            // Explicit `off`: these tests pin the unconditional-upload
            // lifecycle. The backend default (`cas` on s3) is covered by the
            // #1228 default-resolution tests.
            concurrency_control: Some(ConcurrencyControl::Off),
            ..Default::default()
        }
    }

    /// The Local backend acquires as a zero-I/O `Authoritative` no-op — the
    /// installed fault-counting provider proves not a single object-store call
    /// is made across the whole acquire→finalize lifecycle.
    #[tokio::test]
    async fn session_local_acquire_and_finalize_are_zero_io() {
        test_support::clear();
        let faults = install_counting_provider();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");

        let mut session = RemoteStateSession::new(
            &StateConfig::default(),
            &local,
            FinalizeDurability::ConfigDefault,
            false,
        );
        let authority = session.acquire().await.expect("Local acquire");
        assert_eq!(authority, StateAuthority::Authoritative);
        assert_eq!(session.authority(), StateAuthority::Authoritative);
        session.require_synced().expect("Local ledger is usable");
        session.finalize().await.expect("Local finalize is a no-op");
        test_support::clear();

        for op in [
            crate::fault_store::FaultOp::Get,
            crate::fault_store::FaultOp::Put,
            crate::fault_store::FaultOp::Head,
            crate::fault_store::FaultOp::List,
        ] {
            assert_eq!(
                faults.count(op),
                0,
                "Local backend must be zero-I/O ({op:?} was called)"
            );
        }
    }

    /// A download failure is recorded — NOT propagated: `acquire` returns
    /// `Ok(Indeterminate)`, retains the error string, and `require_synced`
    /// fails closed carrying it. This is the session electing past the
    /// `download_state` `Err` (the PR-B Indeterminate-as-a-value migration);
    /// the callee itself still returns `Err`.
    #[tokio::test]
    async fn session_acquire_failure_is_ok_indeterminate_and_require_synced_errs() {
        test_support::clear();
        let _provider = test_support::install(ObjectStoreProvider::in_memory());
        test_support::arm_object_store_exists_fault();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");

        let mut session = RemoteStateSession::new(
            &s3_session_config(StateUploadFailureMode::Skip),
            &local,
            FinalizeDurability::ConfigDefault,
            false,
        );
        let authority = session.acquire().await.expect(
            "a download failure must be recorded as Ok(Indeterminate), never propagated as Err",
        );
        assert_eq!(authority, StateAuthority::Indeterminate);
        let cause = session
            .last_download_error()
            .expect("the elected-past download error is retained")
            .to_string();
        assert!(
            cause.contains("injected existence-check failure"),
            "the retained error must be the transport error verbatim; got: {cause}"
        );
        let err = session
            .require_synced()
            .expect_err("Indeterminate must fail require_synced");
        assert!(
            err.to_string().contains("injected existence-check failure"),
            "require_synced must carry the stored download error; got: {err}"
        );
        session.abandon("test teardown").await;
        test_support::clear();
    }

    /// `assume_fresh_start` is the audited operator election: it flips a
    /// recorded `Indeterminate` to `FreshStart`, after which `require_synced`
    /// passes and the upload is no longer authority-suppressed.
    #[tokio::test]
    async fn session_assume_fresh_start_flips_indeterminate() {
        test_support::clear();
        let _provider = test_support::install(ObjectStoreProvider::in_memory());
        test_support::arm_object_store_exists_fault();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");

        let mut session = RemoteStateSession::new(
            &s3_session_config(StateUploadFailureMode::Skip),
            &local,
            FinalizeDurability::ConfigDefault,
            false,
        );
        assert_eq!(
            session.acquire().await.unwrap(),
            StateAuthority::Indeterminate
        );
        session.assume_fresh_start();
        assert_eq!(session.authority(), StateAuthority::FreshStart);
        session
            .require_synced()
            .expect("an elected fresh start is usable");
        session.abandon("test teardown").await;
        test_support::clear();
    }

    /// Double-acquire is the one internal-misuse `Err` — the session is
    /// one-shot per run.
    #[tokio::test]
    async fn session_double_acquire_is_internal_misuse() {
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        let mut session = RemoteStateSession::new(
            &StateConfig::default(),
            &local,
            FinalizeDurability::ConfigDefault,
            false,
        );
        let _ = session.acquire().await.expect("first acquire");
        let err = session
            .acquire()
            .await
            .expect_err("a second acquire must err (one-shot)");
        assert!(err.to_string().contains("called twice"), "got: {err}");
        session.abandon("test teardown").await;
    }

    /// Dropping a session that was neither finalized nor abandoned trips the
    /// tripwire (`debug_assert!` — tests run with debug assertions on).
    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "dropped without finalize/abandon")]
    fn session_drop_unfinalized_trips_tripwire() {
        let session = RemoteStateSession::new(
            &StateConfig::default(),
            Path::new("/nonexistent/.rocky-state.redb"),
            FinalizeDurability::ConfigDefault,
            false,
        );
        drop(session);
    }

    /// Drop `session` the way `rocky run --watch` drops an interrupted
    /// iteration: owned by a future running inside `sink`'s scope, polled
    /// once so it is genuinely in flight, then dropped. Arms the sink first
    /// when `arm` is set.
    async fn drop_inside_scope(sink: &DroppedSessionSink, session: RemoteStateSession, arm: bool) {
        let mut iteration = Box::pin(sink.scope(async move {
            let _held = session;
            std::future::pending::<()>().await;
        }));
        tokio::select! {
            biased;
            () = &mut iteration => unreachable!("the iteration never completes"),
            () = std::future::ready(()) => {}
        }
        if arm {
            sink.arm();
        }
        drop(iteration);
    }

    /// #1603. A session dropped with its run future under an armed sink is
    /// handed over instead of tripping, and settling it performs the terminal
    /// upload the dropped run skipped — carrying what the run had committed.
    #[tokio::test]
    async fn a_session_dropped_under_an_armed_sink_settles_its_terminal_upload() {
        test_support::clear();
        let provider = ObjectStoreProvider::in_memory();
        let _guard = test_support::install(provider.clone());
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        let cfg = s3_session_config(StateUploadFailureMode::Fail);

        let mut session = RemoteStateSession::new(&cfg, &local, FinalizeDurability::Durable, false);
        assert_eq!(session.acquire().await.unwrap(), StateAuthority::FreshStart);
        // What the run committed before the signal landed.
        {
            let store = StateStore::open(&local).unwrap();
            store
                .init_run_progress("interrupted-run", &["wh.raw.orders".into()], None)
                .unwrap();
        }

        let sink = DroppedSessionSink::new();
        drop_inside_scope(&sink, session, true).await;
        assert_eq!(sink.pending(), 1, "the dropped session was handed over");

        assert_eq!(sink.settle_all().await.unwrap(), 1);
        assert_eq!(sink.pending(), 0);

        let restored = TempDir::new().unwrap();
        let restored_path = restored.path().join(".rocky-state.redb");
        assert_eq!(
            download_state(&cfg, &restored_path, false).await.unwrap(),
            StateAuthority::Authoritative,
            "settlement must have uploaded the ledger the dropped run skipped"
        );
        let store = StateStore::open(&restored_path).unwrap();
        assert!(
            store.get_run_progress("interrupted-run").unwrap().is_some(),
            "the uploaded ledger carries what the run committed before the drop"
        );
        test_support::clear();
    }

    /// The sink only catches the deliberate drop. Unarmed — the run future
    /// dropped a session while still being polled, an early `?` — it is the
    /// calling-path bug the tripwire exists for, and it still trips.
    #[tokio::test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "dropped without finalize/abandon")]
    async fn an_unarmed_sink_still_trips_the_tripwire() {
        let session = RemoteStateSession::new(
            &StateConfig::default(),
            Path::new("/nonexistent/.rocky-state.redb"),
            FinalizeDurability::ConfigDefault,
            false,
        );
        let sink = DroppedSessionSink::new();
        drop_inside_scope(&sink, session, false).await;
    }

    /// Fail closed: a governed (`Durable`) dropped session whose terminal
    /// upload fails makes settlement fail, even under the configured `skip`.
    #[tokio::test]
    async fn settling_a_durable_dropped_session_fails_closed_on_a_lost_upload() {
        test_support::clear();
        let faults = install_counting_provider();
        faults.arm(
            crate::fault_store::FaultOp::Put,
            crate::fault_store::FaultMode::FailAll,
        );
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        seed_state_file(&local);
        let skip_cfg = s3_session_config(StateUploadFailureMode::Skip);

        let mut session =
            RemoteStateSession::new(&skip_cfg, &local, FinalizeDurability::Durable, false);
        let _ = session.acquire().await.unwrap();
        let sink = DroppedSessionSink::new();
        drop_inside_scope(&sink, session, true).await;
        let err = sink
            .settle_all()
            .await
            .expect_err("a governed session must not lose its upload behind an Ok");
        assert!(err.to_string().contains("injected fault"), "got: {err}");
        test_support::clear();
    }

    /// A CAS conflict at settlement fails closed even under `skip`, the same
    /// invariant every other upload keeps: a dropped session never overwrites
    /// a remote that moved after it downloaded.
    #[tokio::test]
    async fn a_cas_conflict_at_settlement_fails_closed_even_under_skip() {
        test_support::clear();
        let _faults = install_counting_provider();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        seed_state_file(&local);
        let cfg = StateConfig {
            concurrency_control: Some(ConcurrencyControl::Cas),
            ..s3_session_config(StateUploadFailureMode::Skip)
        };
        let mut first =
            RemoteStateSession::new(&cfg, &local, FinalizeDurability::ConfigDefault, false);
        let _ = first.acquire().await.unwrap();
        first.finalize().await.unwrap();

        let mut dropped =
            RemoteStateSession::new(&cfg, &local, FinalizeDurability::ConfigDefault, false);
        let _ = dropped.acquire().await.unwrap();
        // The remote advances after the dropped session captured its base.
        let mut racer =
            RemoteStateSession::new(&cfg, &local, FinalizeDurability::ConfigDefault, false);
        let _ = racer.acquire().await.unwrap();
        racer.finalize().await.unwrap();

        let sink = DroppedSessionSink::new();
        drop_inside_scope(&sink, dropped, true).await;
        let settled = sink.settle_all().await;
        assert!(
            matches!(settled, Err(StateSyncError::CasConflict { .. })),
            "a conflict must fail closed, not be skipped: {settled:?}"
        );
        test_support::clear();
    }

    /// The no-clobber rules `finalize` honours hold for a dropped session
    /// too: a recorded suppression or a non-authoritative download uploads
    /// nothing.
    #[tokio::test]
    async fn a_suppressed_dropped_session_settles_without_uploading() {
        test_support::clear();
        let faults = install_counting_provider();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        seed_state_file(&local);

        let mut suppressed = RemoteStateSession::new(
            &s3_session_config(StateUploadFailureMode::Fail),
            &local,
            FinalizeDurability::Durable,
            false,
        );
        let _ = suppressed.acquire().await.unwrap();
        suppressed.set_suppress_upload("forward-incompat recreate");
        let mut indeterminate = RemoteStateSession::new(
            &s3_session_config(StateUploadFailureMode::Fail),
            &local,
            FinalizeDurability::Durable,
            false,
        );
        let _ = indeterminate.acquire().await.unwrap();
        indeterminate.authority = StateAuthority::Indeterminate;

        let sink = DroppedSessionSink::new();
        drop_inside_scope(&sink, suppressed, true).await;
        drop_inside_scope(&sink, indeterminate, true).await;
        assert_eq!(sink.settle_all().await.unwrap(), 2);
        test_support::clear();
        assert_eq!(
            faults.count(crate::fault_store::FaultOp::Put),
            0,
            "a suppressed or non-authoritative session must not touch the remote"
        );
    }

    /// Settlement waits for the dropped run's last `StateStore` writer — a
    /// detached ledger commit — to close before it reads the file, so the
    /// upload never ships a half-written ledger.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn settlement_waits_for_the_last_writer_before_uploading() {
        let _serial = test_support::serial_guard();
        test_support::clear();
        let (faults, _global) = install_counting_provider_global();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        seed_state_file(&local);

        let mut session = RemoteStateSession::new(
            &s3_session_config(StateUploadFailureMode::Fail),
            &local,
            FinalizeDurability::Durable,
            false,
        );
        let _ = session.acquire().await.unwrap();

        // A writer the dropped run left behind, still open.
        let writer = StateStore::open(&local).unwrap();
        let sink = DroppedSessionSink::new();
        drop_inside_scope(&sink, session, true).await;

        let settle = tokio::spawn({
            let sink = sink.clone();
            async move { sink.settle_all().await }
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            faults.count(crate::fault_store::FaultOp::Put),
            0,
            "no upload while a writer still holds the store"
        );
        drop(writer);
        settle
            .await
            .unwrap()
            .expect("settles once the writer closes");
        assert_eq!(faults.count(crate::fault_store::FaultOp::Put), 1);
    }

    /// `abandon` disarms the tripwire — a deliberate no-upload consumption
    /// must not panic on drop.
    #[tokio::test]
    async fn session_abandon_disarms_tripwire() {
        test_support::clear();
        let faults = install_counting_provider();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        seed_state_file(&local);

        let mut session = RemoteStateSession::new(
            &s3_session_config(StateUploadFailureMode::Fail),
            &local,
            FinalizeDurability::Durable,
            false,
        );
        let _ = session.acquire().await.unwrap();
        session.abandon("error-path exit (test)").await;
        test_support::clear();
        assert_eq!(
            faults.count(crate::fault_store::FaultOp::Put),
            0,
            "abandon must never upload"
        );
    }

    /// A suppressed finalize performs no upload and returns Ok — even under
    /// `Durable` (suppression is the deliberate no-clobber election, checked
    /// before durability).
    #[tokio::test]
    async fn session_suppressed_finalize_skips_upload() {
        test_support::clear();
        let faults = install_counting_provider();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        seed_state_file(&local);

        let mut session = RemoteStateSession::new(
            &s3_session_config(StateUploadFailureMode::Fail),
            &local,
            FinalizeDurability::Durable,
            false,
        );
        let _ = session.acquire().await.unwrap();
        session.set_suppress_upload("forward-incompat recreate");
        // First reason wins; a second suppression must not replace it.
        session.set_suppress_upload("indeterminate download");
        session
            .finalize()
            .await
            .expect("a suppressed finalize is Ok without uploading");
        test_support::clear();
        assert_eq!(
            faults.count(crate::fault_store::FaultOp::Put),
            0,
            "a suppressed finalize must not touch the remote"
        );
    }

    /// `Durable` forces `on_upload_failure = Fail`: an armed terminal-Put
    /// fault errs the finalize even though the config says `skip` (the
    /// governed fail-closed split). `ConfigDefault` under the same `skip`
    /// config swallows the same fault (warn + Ok — the liveness contract).
    #[tokio::test]
    async fn session_durable_forces_fail_while_config_default_skip_swallows() {
        test_support::clear();
        let faults = install_counting_provider();
        faults.arm(
            crate::fault_store::FaultOp::Put,
            crate::fault_store::FaultMode::FailAll,
        );
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        seed_state_file(&local);
        let skip_cfg = s3_session_config(StateUploadFailureMode::Skip);

        // Durable + configured skip ⇒ the forced Fail propagates the Put fault.
        let mut durable =
            RemoteStateSession::new(&skip_cfg, &local, FinalizeDurability::Durable, false);
        let _ = durable.acquire().await.unwrap();
        let err = durable
            .finalize()
            .await
            .expect_err("Durable must force on_upload_failure=Fail over the configured skip");
        assert!(
            err.to_string().contains("injected fault"),
            "the propagated error must be the upload failure; got: {err}"
        );

        // ConfigDefault + configured skip ⇒ the same fault is swallowed (Ok).
        let mut config_default =
            RemoteStateSession::new(&skip_cfg, &local, FinalizeDurability::ConfigDefault, false);
        let _ = config_default.acquire().await.unwrap();
        config_default
            .finalize()
            .await
            .expect("ConfigDefault must honor the configured skip (warn-and-Ok)");
        test_support::clear();
    }

    /// CAS finalize fail-closes when the remote object advanced since acquire:
    /// pod B captures its base, a racer commits and bumps the generation, and
    /// B's terminal CAS upload conflicts instead of blind-overwriting the
    /// winner. (Exercised against the InMemory conditional-put backend; the S3
    /// wire path is covered by `tests/state_sync_s3_live.rs`.)
    #[tokio::test]
    async fn session_cas_finalize_conflicts_when_remote_advanced() {
        test_support::clear();
        let _faults = install_counting_provider();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        seed_state_file(&local);

        let cfg = StateConfig {
            backend: StateBackend::S3,
            s3_bucket: Some("bucket".into()),
            concurrency_control: Some(ConcurrencyControl::Cas),
            retry: RetryConfig {
                max_retries: 0,
                ..RetryConfig::default()
            },
            ..Default::default()
        };

        // Bootstrap: first run creates the remote object (base None → Create).
        let mut first =
            RemoteStateSession::new(&cfg, &local, FinalizeDurability::ConfigDefault, false);
        let _ = first.acquire().await.unwrap();
        first
            .finalize()
            .await
            .expect("first CAS finalize creates the object");

        // Pod B acquires and captures the current generation as its base.
        let mut pod_b =
            RemoteStateSession::new(&cfg, &local, FinalizeDurability::ConfigDefault, false);
        let _ = pod_b.acquire().await.unwrap();

        // A racer commits in between, advancing the remote generation.
        let mut racer =
            RemoteStateSession::new(&cfg, &local, FinalizeDurability::ConfigDefault, false);
        let _ = racer.acquire().await.unwrap();
        racer
            .finalize()
            .await
            .expect("racer commits and advances the remote generation");

        // Pod B's terminal CAS now conflicts (stale base) — fail-closed, not a
        // silent overwrite of the racer's committed state.
        let err = pod_b
            .finalize()
            .await
            .expect_err("pod B lost the cross-pod race → CasConflict");
        assert!(
            matches!(err, StateSyncError::CasConflict { .. }),
            "expected CasConflict, got: {err:?}"
        );
        test_support::clear();
    }

    /// Explicit `off` routes an unconditional upload even against an
    /// already-present object — byte-identical to pre-CAS: two sequential runs
    /// both finalize successfully, the second overwriting without a base.
    #[tokio::test]
    async fn session_off_default_overwrites_unconditionally() {
        test_support::clear();
        let _faults = install_counting_provider();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        seed_state_file(&local);
        // s3_session_config pins concurrency_control = off explicitly.
        let cfg = s3_session_config(StateUploadFailureMode::Fail);

        for _ in 0..2 {
            let mut s =
                RemoteStateSession::new(&cfg, &local, FinalizeDurability::ConfigDefault, false);
            let _ = s.acquire().await.unwrap();
            s.finalize()
                .await
                .expect("off finalize is an unconditional put (no conflict)");
        }
        test_support::clear();
    }

    // -----------------------------------------------------------------------
    // #1228 — replayable ledger-seam CAS session
    // -----------------------------------------------------------------------

    fn seam_policy_record(plan_id: &str) -> crate::state::PolicyDecisionRecord {
        crate::state::PolicyDecisionRecord {
            seq: 0,
            keys_recorded: false,
            fail_closed: false,
            models: Vec::new(),
            timestamp: chrono::Utc::now(),
            plan_id: plan_id.to_string(),
            principal: crate::config::PolicyPrincipal::Agent,
            capability: crate::config::PolicyCapability::Apply,
            model: "any".to_string(),
            effect: crate::config::PolicyEffect::Deny,
            rule_id: None,
            reason: plan_id.to_string(),
            verify_after: Vec::new(),
            auto_apply: None,
            principal_ref: None,
        }
    }

    /// A run commits after the seam's fresh download but before its upload.
    /// The first seam attempt therefore loses; the second must re-download the
    /// winner, replay the exact freeze record, and return its own result. A
    /// stale-delta or stale-generation implementation loses one of the rows.
    #[tokio::test]
    async fn ledger_seam_retry_preserves_run_winner_and_returns_winning_result() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let _serial = test_support::serial_guard();
        test_support::clear();
        let mut harness = crate::test_harness::CrossPodHarness::new_s3_like();
        harness.pod_a.cfg.concurrency_control = Some(ConcurrencyControl::Cas);
        harness.pod_b.cfg.concurrency_control = Some(ConcurrencyControl::Cas);

        seed_state_file(&harness.pod_a.state_path);
        let mut bootstrap = RemoteStateSession::new(
            &harness.pod_a.cfg,
            &harness.pod_a.state_path,
            FinalizeDurability::Durable,
            false,
        );
        let _authority = bootstrap.acquire().await.unwrap();
        bootstrap.finalize().await.unwrap();

        // The run captures the same base the seam's first attempt will read,
        // then stages its winner row locally.
        let mut run_winner = RemoteStateSession::new(
            &harness.pod_a.cfg,
            &harness.pod_a.state_path,
            FinalizeDurability::Durable,
            false,
        );
        let _authority = run_winner.acquire().await.unwrap();
        let winner_record = seam_policy_record("run-winner");
        {
            let store = harness.open_store(&harness.pod_a);
            store.record_policy_decision(&winner_record).unwrap();
        }

        let run_winner = Arc::new(tokio::sync::Mutex::new(Some(run_winner)));
        let attempts = Arc::new(AtomicUsize::new(0));
        let freeze_record = seam_policy_record("freeze-record");
        let session = LedgerSeamSession::new(&harness.pod_b.cfg, &harness.pod_b.state_path, false);
        let winning_attempt = session
            .execute({
                let run_winner = Arc::clone(&run_winner);
                let attempts = Arc::clone(&attempts);
                let freeze_record = freeze_record.clone();
                move |store, fresh_base| {
                    let run_winner = Arc::clone(&run_winner);
                    let attempts = Arc::clone(&attempts);
                    let freeze_record = freeze_record.clone();
                    let has_fresh_base = fresh_base.is_some();
                    Box::pin(async move {
                        assert!(
                            has_fresh_base,
                            "the blob already exists, so every attempt must own its generation"
                        );
                        store.record_policy_decision(&freeze_record)?;
                        let attempt_number = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                        if attempt_number == 1 {
                            let winner = run_winner.lock().await.take().ok_or_else(|| {
                                StateSyncError::Io(std::io::Error::other(
                                    "run winner was already committed",
                                ))
                            })?;
                            winner.finalize().await?;
                        }
                        Ok(attempt_number)
                    })
                }
            })
            .await
            .expect("the replayed seam transition must win its second attempt");

        assert_eq!(
            winning_attempt, 2,
            "the helper must return T from the CAS-winning replay"
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 2);

        let verify_dir = TempDir::new().unwrap();
        let verify_path = verify_dir.path().join(".rocky-state.redb");
        let _authority = download_state(&harness.pod_a.cfg, &verify_path, false)
            .await
            .unwrap();
        let decisions = StateStore::open(&verify_path)
            .unwrap()
            .list_policy_decisions()
            .unwrap();
        assert!(decisions.iter().any(|row| row.plan_id == "run-winner"));
        assert!(decisions.iter().any(|row| row.plan_id == "freeze-record"));
        test_support::clear();
    }

    /// Three consecutive semantic conflicts exhaust the seam budget. The
    /// typed error must escape even when the configured liveness policy says
    /// `skip`, the pre-existing remote winner must remain byte-semantically
    /// intact, and no unconditional state-blob put may be issued.
    #[tokio::test]
    async fn ledger_seam_exhaustion_preserves_winner_without_unconditional_upload() {
        use crate::fault_store::PutKind;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let _serial = test_support::serial_guard();
        test_support::clear();
        let mut harness = crate::test_harness::CrossPodHarness::new_s3_like();
        harness.pod_a.cfg.concurrency_control = Some(ConcurrencyControl::Cas);
        harness.pod_b.cfg.concurrency_control = Some(ConcurrencyControl::Cas);
        harness.pod_b.cfg.on_upload_failure = StateUploadFailureMode::Skip;

        seed_state_file(&harness.pod_a.state_path);
        let winner_record = seam_policy_record("remote-winner");
        let mut bootstrap = RemoteStateSession::new(
            &harness.pod_a.cfg,
            &harness.pod_a.state_path,
            FinalizeDurability::Durable,
            false,
        );
        let _authority = bootstrap.acquire().await.unwrap();
        {
            let store = harness.open_store(&harness.pod_a);
            store.record_policy_decision(&winner_record).unwrap();
        }
        bootstrap.finalize().await.unwrap();

        let object_key = format!("v{}/state.redb", crate::state::current_schema_version());
        harness
            .faults
            .arm_precondition_failures(&object_key, LEDGER_SEAM_MAX_ATTEMPTS);
        let loser_record = seam_policy_record("losing-freeze");
        let attempts = Arc::new(AtomicUsize::new(0));
        let session = LedgerSeamSession::new(&harness.pod_b.cfg, &harness.pod_b.state_path, false);
        let err = session
            .execute({
                let attempts = Arc::clone(&attempts);
                move |store, _fresh_base| {
                    let loser_record = loser_record.clone();
                    let attempts = Arc::clone(&attempts);
                    Box::pin(async move {
                        store.record_policy_decision(&loser_record)?;
                        attempts.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                }
            })
            .await
            .expect_err("three conflicts must fail closed");
        assert!(
            matches!(
                err,
                StateSyncError::LedgerSeamConflict {
                    attempts: LEDGER_SEAM_MAX_ATTEMPTS,
                    ..
                }
            ),
            "expected the typed exhausted-conflict error, got {err:?}"
        );
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            LEDGER_SEAM_MAX_ATTEMPTS as usize
        );
        assert_eq!(
            harness.faults.put_count(&object_key, PutKind::Update),
            u64::from(LEDGER_SEAM_MAX_ATTEMPTS),
            "exactly three conditional attempts are allowed"
        );
        assert_eq!(
            harness
                .faults
                .put_count(&object_key, PutKind::Unconditional),
            0,
            "CAS exhaustion must never fall back to an unconditional upload"
        );

        harness.faults.clear();
        let verify_dir = TempDir::new().unwrap();
        let verify_path = verify_dir.path().join(".rocky-state.redb");
        let _authority = download_state(&harness.pod_a.cfg, &verify_path, false)
            .await
            .unwrap();
        let decisions = StateStore::open(&verify_path)
            .unwrap()
            .list_policy_decisions()
            .unwrap();
        assert!(decisions.iter().any(|row| row.plan_id == "remote-winner"));
        assert!(
            !decisions.iter().any(|row| row.plan_id == "losing-freeze"),
            "the losing local transition must never overwrite the remote winner"
        );
        test_support::clear();
    }

    /// `concurrency_control = "off"` retains the legacy half-seam: one fresh
    /// download, one transition with no generation, and one forced-Fail
    /// unconditional upload that preserves the downloaded winner.
    #[tokio::test]
    async fn ledger_seam_off_retains_legacy_unconditional_shape() {
        use crate::fault_store::PutKind;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let _serial = test_support::serial_guard();
        test_support::clear();
        let mut harness = crate::test_harness::CrossPodHarness::new_s3_like();
        // Explicit `off`: an unset mode now defaults to `cas` on s3 (#1228).
        harness.pod_a.cfg.concurrency_control = Some(ConcurrencyControl::Off);
        harness.pod_b.cfg.concurrency_control = Some(ConcurrencyControl::Off);
        let winner_record = seam_policy_record("off-winner");
        {
            let store = harness.open_store(&harness.pod_a);
            store.record_policy_decision(&winner_record).unwrap();
        }
        harness.upload(&harness.pod_a).await.unwrap();

        let calls = Arc::new(AtomicUsize::new(0));
        let seam_record = seam_policy_record("off-seam");
        let session = LedgerSeamSession::new(&harness.pod_b.cfg, &harness.pod_b.state_path, false);
        let output = session
            .execute({
                let calls = Arc::clone(&calls);
                move |store, fresh_base| {
                    assert!(
                        fresh_base.is_none(),
                        "the off path must not introduce generation ownership"
                    );
                    let calls = Arc::clone(&calls);
                    let seam_record = seam_record.clone();
                    Box::pin(async move {
                        store.record_policy_decision(&seam_record)?;
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok("legacy-output")
                    })
                }
            })
            .await
            .unwrap();
        assert_eq!(output, "legacy-output");
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let object_key = format!("v{}/state.redb", crate::state::current_schema_version());
        assert_eq!(
            harness
                .faults
                .put_count(&object_key, PutKind::Unconditional),
            2,
            "the seed plus the seam must both use the legacy unconditional put"
        );
        let decisions = StateStore::open(&harness.pod_b.state_path)
            .unwrap()
            .list_policy_decisions()
            .unwrap();
        assert!(decisions.iter().any(|row| row.plan_id == "off-winner"));
        assert!(decisions.iter().any(|row| row.plan_id == "off-seam"));
        test_support::clear();
    }

    /// `concurrency_control = "off"`: a transition that fails after writing
    /// rows must not leave them readable in the local file. The rows never
    /// reached the remote, so the local file is put back to the remote copy,
    /// exactly as the CAS path does.
    #[tokio::test]
    async fn ledger_seam_off_failed_transition_restores_the_remote_copy() {
        let _serial = test_support::serial_guard();
        test_support::clear();
        let mut harness = crate::test_harness::CrossPodHarness::new_s3_like();
        // Explicit `off`: an unset mode defaults to `cas` on s3 (#1228).
        harness.pod_a.cfg.concurrency_control = Some(ConcurrencyControl::Off);
        harness.pod_b.cfg.concurrency_control = Some(ConcurrencyControl::Off);
        {
            let store = harness.open_store(&harness.pod_a);
            store
                .record_policy_decision(&seam_policy_record("off-remote"))
                .unwrap();
        }
        harness.upload(&harness.pod_a).await.unwrap();

        let session = LedgerSeamSession::new(&harness.pod_b.cfg, &harness.pod_b.state_path, false);
        let err = session
            .execute(|store, _| {
                Box::pin(async move {
                    store.record_policy_decision(&seam_policy_record("off-unpublished"))?;
                    Err::<(), _>(StateSyncError::SeamTransition("refused".into()))
                })
            })
            .await
            .expect_err("the failed transition must propagate");
        assert!(matches!(err, StateSyncError::SeamTransition(_)), "{err:?}");

        let decisions = StateStore::open(&harness.pod_b.state_path)
            .unwrap()
            .list_policy_decisions()
            .unwrap();
        assert!(decisions.iter().any(|row| row.plan_id == "off-remote"));
        assert!(
            !decisions.iter().any(|row| row.plan_id == "off-unpublished"),
            "a row that never reached the remote must not stay in the local file"
        );
        test_support::clear();
    }

    /// Local remains a one-transition, zero-remote-I/O path.
    #[tokio::test]
    async fn ledger_seam_local_is_legacy_zero_io() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        test_support::clear();
        let faults = install_counting_provider();
        let dir = TempDir::new().unwrap();
        let state_path = dir.path().join(".rocky-state.redb");
        let calls = Arc::new(AtomicUsize::new(0));
        let record = seam_policy_record("local-seam");
        let session = LedgerSeamSession::new(&StateConfig::default(), &state_path, false);
        let output = session
            .execute({
                let calls = Arc::clone(&calls);
                move |store, fresh_base| {
                    assert!(fresh_base.is_none());
                    let calls = Arc::clone(&calls);
                    let record = record.clone();
                    Box::pin(async move {
                        store.record_policy_decision(&record)?;
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(7_u8)
                    })
                }
            })
            .await
            .unwrap();
        assert_eq!(output, 7);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        for op in [
            crate::fault_store::FaultOp::Get,
            crate::fault_store::FaultOp::Put,
            crate::fault_store::FaultOp::Head,
            crate::fault_store::FaultOp::List,
        ] {
            assert_eq!(faults.count(op), 0, "Local must not issue {op:?}");
        }
        test_support::clear();
    }

    // -----------------------------------------------------------------------
    // Tiered cache coherence (ADR-CONCURRENCY D5)
    // -----------------------------------------------------------------------

    /// A `tiered` + `cas` config wired to the in-memory durable tier. No
    /// `valkey_url`: the in-process cache stand-in short-circuits ahead of URL
    /// resolution, so a coherent-cache test never needs a live peer.
    fn tiered_cas_config() -> StateConfig {
        StateConfig {
            backend: StateBackend::Tiered,
            s3_bucket: Some("bucket".into()),
            concurrency_control: Some(ConcurrencyControl::Cas),
            retry: RetryConfig {
                max_retries: 0,
                ..RetryConfig::default()
            },
            ..Default::default()
        }
    }

    /// Install both tiers a coherent-cache test needs: the fault-counting
    /// in-memory durable store and the in-process Valkey stand-in.
    fn install_tiered_backends() -> (crate::fault_store::FaultHandle, ObjectStoreProvider) {
        let faults = install_counting_provider();
        test_support::install_fake_valkey();
        let provider = test_support::current_override().expect("provider override installed");
        (faults, provider)
    }

    /// Seed the durable tier with `bytes` and return its generation.
    async fn seed_durable(
        provider: &ObjectStoreProvider,
        object_key: &str,
        bytes: &[u8],
    ) -> Generation {
        provider
            .put(object_key, Bytes::copy_from_slice(bytes))
            .await
            .unwrap();
        match provider.remote_generation(object_key).await.unwrap() {
            RemoteVersion::Present(generation) => generation,
            other => panic!("the in-memory tier must surface a generation, got {other:?}"),
        }
    }

    fn bogus_generation() -> Generation {
        Generation {
            e_tag: Some("\"superseded\"".into()),
            version: None,
        }
    }

    #[test]
    fn coherent_cache_frame_round_trips() {
        let generation = Generation {
            e_tag: Some("\"abc123\"".into()),
            version: Some("42".into()),
        };
        let framed = frame_coherent_cache_entry(&generation, b"redb-bytes").unwrap();
        let (parsed, blob) = parse_coherent_cache_entry(&framed).expect("frame round-trips");
        assert_eq!(parsed, generation);
        assert_eq!(blob, b"redb-bytes");

        // An empty payload is a legitimate frame, not a parse failure.
        let empty = frame_coherent_cache_entry(&generation, b"").unwrap();
        let (_, blob) = parse_coherent_cache_entry(&empty).expect("empty payload round-trips");
        assert!(blob.is_empty());
    }

    /// The cached value is externally supplied, so the parser must be TOTAL:
    /// every malformed shape reads as a plain miss — never a panic, never an
    /// error that could fail a run. A legacy raw-redb value (no magic) is one
    /// of those shapes.
    #[test]
    fn coherent_cache_parse_is_total_over_malformed_input() {
        let malformed: [&[u8]; 8] = [
            b"",
            b"RKY",
            b"NOTMAGIC-and-more",
            // A legacy raw blob written by an `off` pod.
            b"redb\x00\x00\x00\x00garbage",
            // Magic only — no length word.
            b"RKYSGEN1",
            // Truncated length word.
            b"RKYSGEN1\x01\x00",
            // Header length far beyond the value.
            b"RKYSGEN1\xff\xff\xff\xff",
            // Well-formed length, invalid JSON header.
            b"RKYSGEN1\x03\x00\x00\x00{{{",
        ];
        for value in malformed {
            assert!(
                parse_coherent_cache_entry(value).is_none(),
                "a malformed cache value must read as a miss, got a parse for {value:?}"
            );
        }
    }

    /// The fast path still works: a cache entry whose framed generation matches
    /// the durable object's is served from the cache, and the durable object is
    /// never GET. The base still comes from the durable tier's generation.
    #[tokio::test]
    async fn tiered_cas_validated_cache_hit_serves_without_a_durable_get() {
        test_support::clear();
        let (faults, provider) = install_tiered_backends();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        let remote_key = remote_state_key(&local);
        let object_key = object_store_state_key(&remote_key);
        let durable = seed_durable(&provider, &object_key, b"DURABLE").await;

        // Distinguishable cache payload, framed with the CURRENT generation, so
        // "the cache served this read" is unambiguous.
        let cfg = tiered_cas_config();
        let cache_key = valkey_coherent_key(valkey_key_prefix(&cfg), &remote_key);
        test_support::fake_valkey_set(
            &cache_key,
            frame_coherent_cache_entry(&durable, b"FROM-CACHE").unwrap(),
        )
        .unwrap();

        let gets_before = faults.count(crate::fault_store::FaultOp::Get);
        let mut base = None;
        let outcome = download_state_inner(&cfg, &local, &remote_key, Some(&mut base))
            .await
            .unwrap();
        test_support::clear();

        assert_eq!(outcome, DownloadOutcome::Restored);
        assert_eq!(
            std::fs::read(&local).unwrap(),
            b"FROM-CACHE",
            "a generation-validated cache entry must serve the read"
        );
        assert_eq!(
            base,
            Some(durable),
            "the CAS base must be the durable tier's generation, never a cache-local value"
        );
        assert_eq!(
            faults.count(crate::fault_store::FaultOp::Get),
            gets_before,
            "a validated hit must not download the durable object — that is why tiered exists"
        );
    }

    /// When the durable generation cannot be READ, there is nothing to validate
    /// against — so the cache is not consulted at all, even though the entry
    /// present here would have matched. "Cannot check" resolves to "read the
    /// durable tier", never to "trust it".
    #[tokio::test]
    async fn tiered_cas_generation_probe_failure_never_serves_the_cache() {
        test_support::clear();
        let (_faults, provider) = install_tiered_backends();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        let remote_key = remote_state_key(&local);
        let object_key = object_store_state_key(&remote_key);
        let durable = seed_durable(&provider, &object_key, b"DURABLE").await;

        let cfg = tiered_cas_config();
        let cache_key = valkey_coherent_key(valkey_key_prefix(&cfg), &remote_key);
        // A cache entry that WOULD validate — the probe failure is the only
        // reason it must not be used.
        test_support::fake_valkey_set(
            &cache_key,
            frame_coherent_cache_entry(&durable, b"FROM-CACHE").unwrap(),
        )
        .unwrap();
        // One-shot, and distinct from the existence-probe fault, so the durable
        // leg's own probe still succeeds and the fall-through is observable.
        test_support::arm_object_store_head_fault();

        let mut base = None;
        let outcome = download_state_inner(&cfg, &local, &remote_key, Some(&mut base))
            .await
            .unwrap();
        test_support::clear();

        assert_eq!(outcome, DownloadOutcome::Restored);
        assert_eq!(
            std::fs::read(&local).unwrap(),
            b"DURABLE",
            "an unvalidatable cache entry must not be served, however well it would have matched"
        );
        assert_eq!(
            base,
            Some(durable),
            "the fall-through still captures the durable generation as the base"
        );
    }

    /// A cache entry whose framed generation no longer matches the durable
    /// object is rejected: the durable bytes win, and the stale entry is
    /// invalidated on the way out.
    #[tokio::test]
    async fn tiered_cas_stale_cache_generation_is_rejected() {
        test_support::clear();
        let (_faults, provider) = install_tiered_backends();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        let remote_key = remote_state_key(&local);
        let object_key = object_store_state_key(&remote_key);
        let durable = seed_durable(&provider, &object_key, b"DURABLE").await;

        let cfg = tiered_cas_config();
        let cache_key = valkey_coherent_key(valkey_key_prefix(&cfg), &remote_key);
        test_support::fake_valkey_set(
            &cache_key,
            frame_coherent_cache_entry(&bogus_generation(), b"STALE").unwrap(),
        )
        .unwrap();

        let mut base = None;
        let outcome = download_state_inner(&cfg, &local, &remote_key, Some(&mut base))
            .await
            .unwrap();
        let survived = test_support::fake_valkey_get(&cache_key);
        test_support::clear();

        assert_eq!(outcome, DownloadOutcome::Restored);
        assert_eq!(
            std::fs::read(&local).unwrap(),
            b"DURABLE",
            "a superseded cache generation must lose to durable truth"
        );
        assert_eq!(base, Some(durable));
        assert!(
            survived.is_none(),
            "a definitively-stale entry should be invalidated once observed"
        );
    }

    /// A cache entry with NO durable object behind it cannot be validated
    /// against anything, so it is skipped entirely — the read resolves to a
    /// genuine fresh start with a `None` base, which maps to create-if-absent.
    #[tokio::test]
    async fn tiered_cas_cache_entry_without_a_durable_object_is_rejected() {
        test_support::clear();
        let (_faults, _provider) = install_tiered_backends();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        let remote_key = remote_state_key(&local);

        let cfg = tiered_cas_config();
        let cache_key = valkey_coherent_key(valkey_key_prefix(&cfg), &remote_key);
        test_support::fake_valkey_set(
            &cache_key,
            frame_coherent_cache_entry(&bogus_generation(), b"ORPHANED").unwrap(),
        )
        .unwrap();

        let mut base = Some(bogus_generation());
        let outcome = download_state_inner(&cfg, &local, &remote_key, Some(&mut base))
            .await
            .unwrap();
        test_support::clear();

        assert_eq!(
            outcome,
            DownloadOutcome::Absent,
            "no durable object ⇒ a real fresh start, not an unvalidatable cache hit"
        );
        assert!(
            !local.exists(),
            "the orphaned cache entry must not be written locally"
        );
        assert_eq!(
            base, None,
            "an absent durable object leaves no base ⇒ PutMode::Create"
        );
    }

    /// A cache write that fails after a durable commit must INVALIDATE, so the
    /// next read falls through to the durable tier instead of hitting a stale
    /// entry. This is the pre-fix silent-staleness path, closed.
    #[tokio::test]
    async fn tiered_cas_cache_write_failure_invalidates_the_stale_entry() {
        test_support::clear();
        let (_faults, _provider) = install_tiered_backends();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        seed_state_file(&local);
        let cfg = tiered_cas_config();
        let remote_key = remote_state_key(&local);
        let cache_key = valkey_coherent_key(valkey_key_prefix(&cfg), &remote_key);

        // A stale entry left over from an earlier write — the bait the pre-fix
        // read path would have served.
        test_support::fake_valkey_set(
            &cache_key,
            frame_coherent_cache_entry(&bogus_generation(), b"STALE").unwrap(),
        )
        .unwrap();

        let mut session = RemoteStateSession::new(&cfg, &local, FinalizeDurability::Durable, false);
        let _ = session.acquire().await.unwrap();
        test_support::arm_valkey_set_fault();
        session
            .finalize()
            .await
            .expect("a cache-populate failure must NOT fail an already-durable commit");

        let survived = test_support::fake_valkey_get(&cache_key);
        let mut base = None;
        let outcome = download_state_inner(&cfg, &local, &remote_key, Some(&mut base))
            .await
            .unwrap();
        test_support::clear();

        assert!(
            survived.is_none(),
            "a failed cache populate must invalidate, leaving nothing hittable"
        );
        assert_eq!(outcome, DownloadOutcome::Restored);
        assert!(
            base.is_some(),
            "the fall-through read must capture the durable generation as the base"
        );
    }

    /// The harder case: the cache populate AND the invalidation both fail, so a
    /// stale entry genuinely SURVIVES. It is still rejected, because the entry
    /// carries its own generation — invalidation is hygiene, not the mechanism.
    #[tokio::test]
    async fn tiered_cas_surviving_stale_entry_is_still_rejected() {
        test_support::clear();
        let (_faults, provider) = install_tiered_backends();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        seed_state_file(&local);
        let cfg = tiered_cas_config();
        let remote_key = remote_state_key(&local);
        let object_key = object_store_state_key(&remote_key);
        let cache_key = valkey_coherent_key(valkey_key_prefix(&cfg), &remote_key);

        test_support::fake_valkey_set(
            &cache_key,
            frame_coherent_cache_entry(&bogus_generation(), b"STALE").unwrap(),
        )
        .unwrap();

        let mut session = RemoteStateSession::new(&cfg, &local, FinalizeDurability::Durable, false);
        let _ = session.acquire().await.unwrap();
        test_support::arm_valkey_set_fault();
        test_support::arm_valkey_del_fault();
        session.finalize().await.expect("the durable commit stands");

        assert!(
            test_support::fake_valkey_get(&cache_key).is_some(),
            "this test is only meaningful while the stale entry survives an armed DEL failure"
        );

        // Read into a scratch path so the assertion is on what the tiered read
        // produced, not on the pre-existing local ledger.
        let restored = dir.path().join("restored.redb");
        let mut base = None;
        let outcome = download_state_inner(&cfg, &restored, &remote_key, Some(&mut base))
            .await
            .unwrap();
        let durable_bytes = provider.get(&object_key).await.unwrap();
        test_support::clear();

        assert_eq!(outcome, DownloadOutcome::Restored);
        let served = std::fs::read(&restored).unwrap();
        assert_ne!(
            served, b"STALE",
            "a surviving stale entry must never be served — the frame carries its own \
             generation, so invalidation is hygiene rather than the mechanism"
        );
        assert_eq!(
            served,
            durable_bytes.as_ref(),
            "the read must resolve to durable truth"
        );
        assert!(
            base.is_some(),
            "the fall-through read captures the durable generation as the base"
        );
    }

    /// Two writers from a shared base: exactly one commits, the loser gets a
    /// fail-closed `CasConflict`, the durable generation does not advance past
    /// the winner, and the loser leaves nothing cached.
    #[tokio::test]
    async fn tiered_cas_loser_fail_closes_without_overwriting_the_winner() {
        test_support::clear();
        let (_faults, provider) = install_tiered_backends();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        seed_state_file(&local);
        let cfg = tiered_cas_config();
        let remote_key = remote_state_key(&local);
        let object_key = object_store_state_key(&remote_key);
        let cache_key = valkey_coherent_key(valkey_key_prefix(&cfg), &remote_key);

        // Bootstrap so both racers share a non-None base.
        let mut first =
            RemoteStateSession::new(&cfg, &local, FinalizeDurability::ConfigDefault, false);
        let _ = first.acquire().await.unwrap();
        first.finalize().await.expect("bootstrap commit");

        let mut loser =
            RemoteStateSession::new(&cfg, &local, FinalizeDurability::ConfigDefault, false);
        let _ = loser.acquire().await.unwrap();
        let mut winner =
            RemoteStateSession::new(&cfg, &local, FinalizeDurability::ConfigDefault, false);
        let _ = winner.acquire().await.unwrap();
        winner.finalize().await.expect("the winner commits");

        let after_winner = provider.remote_generation(&object_key).await.unwrap();
        let err = loser
            .finalize()
            .await
            .expect_err("the loser must fail closed, not overwrite the winner");
        let after_loser = provider.remote_generation(&object_key).await.unwrap();
        let cached = test_support::fake_valkey_get(&cache_key);
        test_support::clear();

        assert!(
            matches!(err, StateSyncError::CasConflict { .. }),
            "expected CasConflict, got: {err:?}"
        );
        assert_eq!(
            after_winner, after_loser,
            "the durable generation must not advance — the loser wrote nothing"
        );
        assert!(
            cached.is_none(),
            "a conflicted writer must invalidate, never leave its own view cached"
        );
    }

    /// Bootstrap race: two first-ever writers both carry `base = None`. One
    /// creates, the other CONFLICTS — `None` never degrades to an
    /// unconditional put.
    #[tokio::test]
    async fn tiered_cas_bootstrap_race_conflicts_rather_than_overwriting() {
        test_support::clear();
        let (_faults, provider) = install_tiered_backends();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        seed_state_file(&local);
        let cfg = tiered_cas_config();
        let object_key = object_store_state_key(&remote_state_key(&local));

        let mut first =
            RemoteStateSession::new(&cfg, &local, FinalizeDurability::ConfigDefault, false);
        let mut second =
            RemoteStateSession::new(&cfg, &local, FinalizeDurability::ConfigDefault, false);
        assert_eq!(
            first.acquire().await.unwrap(),
            StateAuthority::FreshStart,
            "both bootstrap writers see an absent durable object"
        );
        assert_eq!(second.acquire().await.unwrap(), StateAuthority::FreshStart);

        first.finalize().await.expect("the first Create commits");
        let after_first = provider.remote_generation(&object_key).await.unwrap();
        let err = second
            .finalize()
            .await
            .expect_err("the second bootstrap writer must conflict");
        let after_second = provider.remote_generation(&object_key).await.unwrap();
        test_support::clear();

        assert!(
            matches!(err, StateSyncError::CasConflict { .. }),
            "expected CasConflict on the bootstrap race, got: {err:?}"
        );
        assert_eq!(
            after_first, after_second,
            "the losing bootstrap writer must not have written"
        );
    }

    /// A backend/transport error is an `Err`, never a `Conflict` — and it
    /// invalidates, because the durable outcome is unknown and no generation
    /// can be claimed for a cache entry.
    #[tokio::test]
    async fn tiered_cas_backend_error_is_err_not_conflict() {
        test_support::clear();
        let (faults, _provider) = install_tiered_backends();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        seed_state_file(&local);
        let cfg = tiered_cas_config();
        let remote_key = remote_state_key(&local);
        let cache_key = valkey_coherent_key(valkey_key_prefix(&cfg), &remote_key);
        test_support::fake_valkey_set(
            &cache_key,
            frame_coherent_cache_entry(&bogus_generation(), b"STALE").unwrap(),
        )
        .unwrap();

        let mut session = RemoteStateSession::new(&cfg, &local, FinalizeDurability::Durable, false);
        let _ = session.acquire().await.unwrap();
        faults.arm(
            crate::fault_store::FaultOp::Put,
            crate::fault_store::FaultMode::FailAll,
        );
        let err = session
            .finalize()
            .await
            .expect_err("a Durable finalize must propagate the transport failure");
        let survived = test_support::fake_valkey_get(&cache_key);
        test_support::clear();

        assert!(
            !matches!(err, StateSyncError::CasConflict { .. }),
            "a transport error must NOT be reported as a CAS conflict; got: {err:?}"
        );
        assert!(
            survived.is_none(),
            "an unknown durable outcome must leave nothing cached"
        );
    }

    /// `off` on `tiered` is untouched: the coherent cache key is never written,
    /// and the durable write stays the unconditional put it has always been.
    #[tokio::test]
    async fn tiered_off_never_touches_the_coherent_cache_key() {
        test_support::clear();
        let (_faults, provider) = install_tiered_backends();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        seed_state_file(&local);
        let cfg = StateConfig {
            concurrency_control: Some(ConcurrencyControl::Off),
            ..tiered_cas_config()
        };
        let remote_key = remote_state_key(&local);
        let object_key = object_store_state_key(&remote_key);
        let cache_key = valkey_coherent_key(valkey_key_prefix(&cfg), &remote_key);

        for _ in 0..2 {
            let mut session =
                RemoteStateSession::new(&cfg, &local, FinalizeDurability::ConfigDefault, false);
            let _ = session.acquire().await.unwrap();
            session
                .finalize()
                .await
                .expect("off on tiered is an unconditional put — never a conflict");
        }
        let cached = test_support::fake_valkey_get(&cache_key);
        let durable = provider.exists(&object_key).await.unwrap();
        test_support::clear();

        assert!(
            cached.is_none(),
            "the `off` path must not write the coherent cache key (mixed-fleet safety)"
        );
        assert!(durable, "the `off` durable write is unchanged");
    }

    /// Enabling `cas` on `tiered` also disables the mid-run periodic uploader,
    /// exactly as it does on `s3`: finalize-only CAS is correct precisely
    /// because no mid-run upload bumps the durable generation.
    #[tokio::test]
    async fn tiered_cas_disables_the_periodic_uploader() {
        test_support::clear();
        let (_faults, _provider) = install_tiered_backends();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        seed_state_file(&local);

        // `off` first: once the CAS commit below creates the cas-required
        // marker, an `off` session on this state is refused at acquire (#1228).
        let off_cfg = StateConfig {
            concurrency_control: Some(ConcurrencyControl::Off),
            ..tiered_cas_config()
        };
        let mut off =
            RemoteStateSession::new(&off_cfg, &local, FinalizeDurability::ConfigDefault, false);
        let _ = off.acquire().await.unwrap();
        off.start_periodic_uploader(Weak::<StateStore>::new(), Duration::from_secs(3600));
        assert!(
            off.periodic.is_some(),
            "off on tiered keeps the mid-run uploader"
        );
        off.stop_periodic().await;
        off.abandon("test").await;

        let mut cas = RemoteStateSession::new(
            &tiered_cas_config(),
            &local,
            FinalizeDurability::ConfigDefault,
            false,
        );
        let _ = cas.acquire().await.unwrap();
        cas.start_periodic_uploader(Weak::<StateStore>::new(), Duration::from_secs(3600));
        assert!(
            cas.periodic.is_none(),
            "cas on tiered must not start the mid-run uploader"
        );
        cas.finalize().await.expect("terminal CAS commit");
        test_support::clear();
    }

    /// `stop_periodic` aborts AND joins, and is idempotent; `finalize` stops
    /// the uploader before its terminal upload.
    #[tokio::test]
    async fn session_stop_periodic_is_idempotent_and_finalize_stops_it() {
        test_support::clear();
        let faults = install_counting_provider();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        seed_state_file(&local);

        let mut session = RemoteStateSession::new(
            &s3_session_config(StateUploadFailureMode::Fail),
            &local,
            FinalizeDurability::ConfigDefault,
            false,
        );
        let _ = session.acquire().await.unwrap();
        // A cadence far beyond the test's lifetime: the loop must never fire,
        // so the only Put observed below is the terminal finalize upload. A
        // dangling Weak is fine — the loop never reaches an upgrade.
        session.start_periodic_uploader(Weak::<StateStore>::new(), Duration::from_secs(3600));
        // Idempotent no-op while one is running.
        session.start_periodic_uploader(Weak::<StateStore>::new(), Duration::from_secs(3600));
        session.stop_periodic().await;
        session.stop_periodic().await; // idempotent after stop
        session.start_periodic_uploader(Weak::<StateStore>::new(), Duration::from_secs(3600));
        session
            .finalize()
            .await
            .expect("finalize stops the periodic task and uploads");
        test_support::clear();
        assert_eq!(
            faults.count(crate::fault_store::FaultOp::Put),
            1,
            "exactly the terminal upload — the periodic loop must never have fired"
        );
    }

    fn wm_now() -> rocky_ir::WatermarkState {
        let now = chrono::Utc::now();
        rocky_ir::WatermarkState {
            last_value: now,
            updated_at: now,
        }
    }

    async fn poll_until<F: Fn() -> bool>(f: F, budget: Duration) -> bool {
        let deadline = std::time::Instant::now() + budget;
        while std::time::Instant::now() < deadline {
            if f() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
        f()
    }

    /// The dirty-gated periodic uploader uploads when the store's write epoch
    /// advances and SKIPS clean ticks (zero I/O when nothing changed) — the
    /// `FaultHandle` put-counter is the instrument.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn periodic_uploader_uploads_on_dirty_epoch_and_skips_clean_ticks() {
        let _serial = test_support::serial_guard();
        let (faults, _override) = install_counting_provider_global();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");

        let store = Arc::new(StateStore::open(&local).unwrap());
        // Dirty the store so the first tick has a change to replicate.
        store.set_watermark("c.s.t", &wm_now()).unwrap();

        let mut session = RemoteStateSession::new(
            &s3_session_config(StateUploadFailureMode::Fail),
            &local,
            FinalizeDurability::ConfigDefault,
            false,
        );
        let _ = session.acquire().await.unwrap();
        session.start_periodic_uploader(Arc::downgrade(&store), Duration::from_millis(40));

        // First dirty tick uploads.
        let uploaded = poll_until(
            || faults.count(crate::fault_store::FaultOp::Put) >= 1,
            Duration::from_secs(10),
        )
        .await;
        let after_first = faults.count(crate::fault_store::FaultOp::Put);

        // No new writes → subsequent ticks are clean and skip (zero new Puts).
        tokio::time::sleep(Duration::from_millis(300)).await;
        let clean_skipped = faults.count(crate::fault_store::FaultOp::Put) == after_first;

        // A fresh write re-dirties the epoch → the uploader picks it up.
        store.set_watermark("c.s.u", &wm_now()).unwrap();
        let re_uploaded = poll_until(
            || faults.count(crate::fault_store::FaultOp::Put) > after_first,
            Duration::from_secs(10),
        )
        .await;

        // Consume the session BEFORE asserting: a failing assertion must not
        // leave the session unconsumed, or its Drop tripwire fires during unwind
        // (a destructor panic on top of the assert's panic) and aborts the
        // process with truncated output — turning a clean, retryable FAIL into an
        // un-debuggable SIGABRT.
        session.abandon("test complete").await;

        assert!(uploaded, "a dirty tick must upload the snapshot");
        assert!(
            clean_skipped,
            "clean ticks must skip — the dirty-gate spends zero I/O when the epoch is unchanged"
        );
        assert!(re_uploaded, "a new epoch bump must trigger another upload");
    }

    /// Cooperative drain: after `stop_periodic` no late upload happens and no
    /// scratch snapshot file is leaked — the in-flight tick's `spawn_blocking`
    /// runs to completion in-task rather than being detached by an `abort()`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stopping_the_uploader_drains_without_scratch_leak_or_late_upload() {
        let _serial = test_support::serial_guard();
        let (faults, _override) = install_counting_provider_global();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");

        let store = Arc::new(StateStore::open(&local).unwrap());
        store.set_watermark("c.s.t", &wm_now()).unwrap();

        let mut session = RemoteStateSession::new(
            &s3_session_config(StateUploadFailureMode::Fail),
            &local,
            FinalizeDurability::ConfigDefault,
            false,
        );
        let _ = session.acquire().await.unwrap();
        session.start_periodic_uploader(Arc::downgrade(&store), Duration::from_millis(30));

        // Let at least one upload happen so a tick has genuinely run.
        let uploaded = poll_until(
            || faults.count(crate::fault_store::FaultOp::Put) >= 1,
            Duration::from_secs(10),
        )
        .await;

        // Cooperative drain + join.
        session.stop_periodic().await;
        let after_stop = faults.count(crate::fault_store::FaultOp::Put);

        // No further upload after the drain returns.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let no_late_upload = faults.count(crate::fault_store::FaultOp::Put) == after_stop;

        // No scratch snapshot leaked for this process (the ScratchGuard removed
        // every temp file in-task, uncancelled).
        let pid_prefix = format!("rocky-state-snapshot-{}-", std::process::id());
        let leaked: Vec<String> = std::fs::read_dir(std::env::temp_dir())
            .expect("read temp dir")
            .filter_map(Result::ok)
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with(&pid_prefix))
            .collect();

        // Consume the session BEFORE asserting (clean FAIL, never a Drop-tripwire
        // double-panic SIGABRT on assertion failure — see the uploads/skips test).
        session.abandon("test complete").await;

        assert!(
            uploaded,
            "the uploader must run at least one tick before stop"
        );
        assert!(
            no_late_upload,
            "no late upload may fire after a cooperative stop"
        );
        assert!(
            leaked.is_empty(),
            "cooperative drain must leave no scratch snapshot behind, found: {leaked:?}"
        );
    }

    /// The `Weak`-handle safety net (mirrors run.rs's serial tail): with the
    /// periodic uploader wired and having run at least one tick, the run tail's
    /// `Arc::try_unwrap` still recovers the owned store — a strong `Arc` clone
    /// held by the uploader would have blocked it, leaving `None` and silently
    /// no-op'ing every terminal write. After the drain, `try_unwrap` succeeds and
    /// a terminal write persists.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn terminal_writes_persist_with_periodic_wired() {
        let _serial = test_support::serial_guard();
        let (faults, _override) = install_counting_provider_global();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");

        let shared_state = Arc::new(StateStore::open(&local).unwrap());
        shared_state.set_watermark("c.s.t", &wm_now()).unwrap();

        let mut session = RemoteStateSession::new(
            &s3_session_config(StateUploadFailureMode::Fail),
            &local,
            FinalizeDurability::ConfigDefault,
            false,
        );
        let _ = session.acquire().await.unwrap();
        session.start_periodic_uploader(Arc::downgrade(&shared_state), Duration::from_millis(20));

        // Ensure a tick actually ran (so the uploader genuinely upgraded the
        // Weak at least once — the case a strong clone would have made fatal).
        let ticked = poll_until(
            || faults.count(crate::fault_store::FaultOp::Put) >= 1,
            Duration::from_secs(10),
        )
        .await;

        // Run tail: `abandon` drains the periodic (cooperative stop + join), and
        // consuming the session here — BEFORE the recovery assertions — keeps a
        // failure a clean FAIL rather than a Drop-tripwire double-panic SIGABRT.
        session.abandon("test complete").await;

        // The Weak periodic handle never blocked the run tail's try_unwrap.
        let recovered = Arc::try_unwrap(shared_state).ok();
        let terminal_persisted = if let Some(store) = recovered.as_ref() {
            // The recovered store is real (not a no-op `None`): a terminal write
            // persists. `store` drops at the end of scope, freeing the writer
            // lock — the drop-before-finalize discipline.
            store.set_watermark("c.s.terminal", &wm_now()).unwrap();
            store.get_watermark("c.s.terminal").unwrap().is_some()
        } else {
            false
        };

        assert!(
            ticked,
            "the uploader must run a tick so the Weak is upgraded"
        );
        assert!(
            recovered.is_some(),
            "Arc::try_unwrap must succeed — the Weak periodic handle never blocks the run tail"
        );
        assert!(
            terminal_persisted,
            "the terminal write must persist on the recovered owned store"
        );
    }

    // -----------------------------------------------------------------------
    // #1955 — carry the newest older schema version's state forward
    // -----------------------------------------------------------------------

    /// A real redb ledger holding one policy decision, stamped at `version`,
    /// as an older binary would have uploaded it.
    fn older_ledger_bytes(dir: &Path, version: u32, plan_id: &str) -> Vec<u8> {
        let path = dir.join(format!("seed-v{version}-{plan_id}.redb"));
        {
            let store = StateStore::open(&path).unwrap();
            store
                .record_policy_decision(&seam_policy_record(plan_id))
                .unwrap();
        }
        crate::state::force_schema_version(&path, &version.to_string());
        std::fs::read(&path).unwrap()
    }

    fn ledger_plan_ids(local: &Path) -> Vec<String> {
        let store = StateStore::open(local).unwrap();
        store
            .list_policy_decisions()
            .unwrap()
            .into_iter()
            .map(|r| r.plan_id)
            .collect()
    }

    fn current_version() -> u32 {
        crate::state::current_schema_version()
    }

    #[test]
    fn carry_forward_probes_newest_first_down_to_the_floor() {
        assert_eq!(
            carry_forward_versions_below(30),
            vec![29, 28, 27, 26, 25, 24, 23, 22],
            "probe runs from current - 1 DOWN to v22 inclusive"
        );
        assert_eq!(carry_forward_versions_below(23), vec![22]);
        assert!(
            carry_forward_versions_below(22).is_empty(),
            "at the floor there is nothing older to probe"
        );
        assert!(
            carry_forward_versions_below(5).is_empty(),
            "below the floor nothing is ever probed"
        );

        let live = carry_forward_versions();
        assert_eq!(live, carry_forward_versions_below(current_version()));
        assert!(
            live.iter()
                .all(|v| (CARRY_FORWARD_FLOOR_SCHEMA_VERSION..current_version()).contains(v)),
            "never probe the current version, a newer one, or one below v22: {live:?}"
        );
        assert!(
            live.windows(2).all(|w| w[0] > w[1]),
            "strictly newest first"
        );
    }

    #[test]
    fn versioned_key_builders_match_the_current_key_shape() {
        let v = current_version();
        assert_eq!(
            object_store_state_key_at(v, "state.redb"),
            object_store_state_key("state.redb")
        );
        assert_eq!(object_store_state_key_at(22, "acme.redb"), "v22/acme.redb");
        assert_eq!(
            valkey_state_key_at(DEFAULT_VALKEY_PREFIX, v, "state.redb"),
            valkey_state_key(DEFAULT_VALKEY_PREFIX, "state.redb")
        );
        assert_eq!(
            valkey_state_key_at(DEFAULT_VALKEY_PREFIX, 22, "state.redb"),
            "rocky:state:v22:state.redb"
        );
    }

    /// Absent current key + present older keys: the download restores the
    /// NEWEST older key (not an even older one), the restored store migrates in
    /// place on open, the older key is never written, and the next upload
    /// creates the current key.
    #[tokio::test]
    async fn object_store_carry_forward_restores_newest_older_key() {
        test_support::clear();
        let faults = install_counting_provider();
        let provider = test_support::current_override().unwrap();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        let remote_key = remote_state_key(&local);

        let prev = current_version() - 1;
        let prev_key = object_store_state_key_at(prev, &remote_key);
        let older_key = object_store_state_key_at(CARRY_FORWARD_FLOOR_SCHEMA_VERSION, &remote_key);
        let current_key = object_store_state_key(&remote_key);
        let prev_bytes = older_ledger_bytes(dir.path(), prev, "kept-decision");
        provider
            .put(&prev_key, Bytes::from(prev_bytes.clone()))
            .await
            .unwrap();
        provider
            .put(
                &older_key,
                Bytes::from(older_ledger_bytes(
                    dir.path(),
                    CARRY_FORWARD_FLOOR_SCHEMA_VERSION,
                    "too-old-decision",
                )),
            )
            .await
            .unwrap();
        let seeded_puts = faults.count(crate::fault_store::FaultOp::Put);

        let cfg = s3_session_config(StateUploadFailureMode::Fail);
        let authority = download_state(&cfg, &local, false).await.unwrap();
        assert_eq!(
            authority,
            StateAuthority::Authoritative,
            "a carried-forward restore is a real restore, not a fresh start"
        );
        assert_eq!(
            ledger_plan_ids(&local),
            vec!["kept-decision".to_string()],
            "the ledger must come from the NEWEST older key"
        );
        assert_eq!(
            crate::state::StateStore::peek_schema_version(&local).unwrap(),
            Some(current_version()),
            "the read-write open migrates the carried-forward store in place"
        );
        assert_eq!(
            faults.count(crate::fault_store::FaultOp::Put),
            seeded_puts,
            "a download never writes to the remote"
        );
        assert!(
            !provider.exists(&current_key).await.unwrap(),
            "the download alone does not create the current key"
        );

        upload_state(&cfg, &local, false).await.unwrap();
        assert!(
            provider.exists(&current_key).await.unwrap(),
            "the next upload writes the CURRENT key"
        );
        assert_eq!(
            provider.get(&prev_key).await.unwrap().to_vec(),
            prev_bytes,
            "the older key is never written or deleted"
        );
        assert_eq!(
            faults.put_count(&prev_key, crate::fault_store::PutKind::Unconditional)
                + faults.put_count(&prev_key, crate::fault_store::PutKind::Create)
                + faults.put_count(&prev_key, crate::fault_store::PutKind::Update),
            1,
            "only the test's own seed ever wrote the older key"
        );
        test_support::clear();
    }

    /// The current key wins whenever it exists: no carry-forward probe runs.
    #[tokio::test]
    async fn object_store_current_key_wins_over_older_keys() {
        test_support::clear();
        let faults = install_counting_provider();
        let provider = test_support::current_override().unwrap();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        let remote_key = remote_state_key(&local);
        provider
            .put(
                &object_store_state_key(&remote_key),
                Bytes::from(older_ledger_bytes(dir.path(), current_version(), "current")),
            )
            .await
            .unwrap();
        provider
            .put(
                &object_store_state_key_at(current_version() - 1, &remote_key),
                Bytes::from(older_ledger_bytes(
                    dir.path(),
                    current_version() - 1,
                    "older",
                )),
            )
            .await
            .unwrap();
        let heads_before = faults.count(crate::fault_store::FaultOp::Head);

        let cfg = s3_session_config(StateUploadFailureMode::Fail);
        let _ = download_state(&cfg, &local, false).await.unwrap();
        assert_eq!(ledger_plan_ids(&local), vec!["current".to_string()]);
        assert_eq!(
            faults.count(crate::fault_store::FaultOp::Head) - heads_before,
            1,
            "a present current key costs exactly one existence probe"
        );
        test_support::clear();
    }

    /// Nothing below v22 is ever read: a key only at v21 leaves the download a
    /// fresh start, after exactly one probe per version from current down to
    /// the floor — and no GET at all.
    #[tokio::test]
    async fn object_store_carry_forward_never_reads_below_the_floor() {
        test_support::clear();
        let faults = install_counting_provider();
        let provider = test_support::current_override().unwrap();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        let remote_key = remote_state_key(&local);
        let below = CARRY_FORWARD_FLOOR_SCHEMA_VERSION - 1;
        provider
            .put(
                &object_store_state_key_at(below, &remote_key),
                Bytes::from(older_ledger_bytes(dir.path(), below, "below-floor")),
            )
            .await
            .unwrap();
        let heads_before = faults.count(crate::fault_store::FaultOp::Head);
        let gets_before = faults.count(crate::fault_store::FaultOp::Get);

        let cfg = s3_session_config(StateUploadFailureMode::Fail);
        let authority = download_state(&cfg, &local, false).await.unwrap();
        assert_eq!(authority, StateAuthority::FreshStart);
        assert!(!local.exists(), "nothing restored, nothing written");
        assert_eq!(
            faults.count(crate::fault_store::FaultOp::Head) - heads_before,
            1 + carry_forward_versions().len() as u64,
            "one probe for the current key plus one per version down to v22, no more"
        );
        assert_eq!(
            faults.count(crate::fault_store::FaultOp::Get),
            gets_before,
            "no object was downloaded"
        );
        test_support::clear();
    }

    /// A failed older-key probe fails closed, exactly like the current-key
    /// probe: an unreadable older key is not proof of a fresh start.
    #[tokio::test]
    async fn object_store_carry_forward_probe_failure_fails_closed() {
        test_support::clear();
        let faults = install_counting_provider();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        // Head #1 is the current key; #2 is the first older key.
        let nth = faults.count(crate::fault_store::FaultOp::Head) + 2;
        faults.arm(
            crate::fault_store::FaultOp::Head,
            crate::fault_store::FaultMode::FailNth(nth),
        );
        let cfg = s3_session_config(StateUploadFailureMode::Fail);
        let result = download_state(&cfg, &local, false).await;
        test_support::clear();
        assert!(
            result.is_err(),
            "an older-key probe failure must propagate, got {result:?}"
        );
    }

    /// Under CAS a carried-forward restore captures NO base, so the first
    /// upload CASes against "current key absent" (`PutMode::Create`) — never an
    /// update against the older object's generation — and never touches the
    /// older key.
    #[tokio::test]
    async fn cas_carry_forward_first_upload_creates_the_current_key() {
        test_support::clear();
        let faults = install_counting_provider();
        let provider = test_support::current_override().unwrap();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        let remote_key = remote_state_key(&local);
        let prev_key = object_store_state_key_at(current_version() - 1, &remote_key);
        let current_key = object_store_state_key(&remote_key);
        let prev_bytes = older_ledger_bytes(dir.path(), current_version() - 1, "cas-kept");
        provider
            .put(&prev_key, Bytes::from(prev_bytes.clone()))
            .await
            .unwrap();

        let cfg = StateConfig {
            concurrency_control: Some(ConcurrencyControl::Cas),
            ..s3_session_config(StateUploadFailureMode::Fail)
        };
        let mut session = RemoteStateSession::new(&cfg, &local, FinalizeDurability::Durable, false);
        let authority = session.acquire().await.unwrap();
        // Capture before asserting: a failed assert must not drop an
        // unfinalized session (its Drop tripwire would abort the test binary).
        let base_after_acquire = session.base.clone();
        let plan_ids = ledger_plan_ids(&local);
        let finalized = session.finalize().await;

        assert_eq!(authority, StateAuthority::Authoritative);
        assert!(
            base_after_acquire.is_none(),
            "a carried-forward restore must not hand the older object's generation to CAS"
        );
        assert_eq!(plan_ids, vec!["cas-kept".to_string()]);
        finalized.expect("first CAS upload commits");
        assert_eq!(
            faults.put_count(&current_key, crate::fault_store::PutKind::Create),
            1,
            "the first upload after a carry-forward is create-if-absent on the current key"
        );
        assert_eq!(
            faults.put_count(&current_key, crate::fault_store::PutKind::Update),
            0
        );
        assert_eq!(
            faults.put_count(&prev_key, crate::fault_store::PutKind::Create)
                + faults.put_count(&prev_key, crate::fault_store::PutKind::Update),
            0,
            "no conditional write ever targets the older key"
        );
        assert_eq!(
            provider.get(&prev_key).await.unwrap().to_vec(),
            prev_bytes,
            "the older key is untouched"
        );
        test_support::clear();
    }

    /// Tiered `cas`: the durable S3 leg carries forward, with no base.
    #[tokio::test]
    async fn tiered_cas_durable_leg_carries_forward_without_a_base() {
        test_support::clear();
        let (_faults, provider) = install_tiered_backends();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        let remote_key = remote_state_key(&local);
        provider
            .put(
                &object_store_state_key_at(current_version() - 1, &remote_key),
                Bytes::from(older_ledger_bytes(
                    dir.path(),
                    current_version() - 1,
                    "tiered-kept",
                )),
            )
            .await
            .unwrap();

        let (authority, base) = download_state_with_generation(&tiered_cas_config(), &local, false)
            .await
            .unwrap();
        test_support::clear();
        assert_eq!(authority, StateAuthority::Authoritative);
        assert!(base.is_none(), "carry-forward leaves the CAS base empty");
        assert_eq!(ledger_plan_ids(&local), vec!["tiered-kept".to_string()]);
    }

    /// Tiered `off`: the Valkey cache leg never carries forward. With an older
    /// copy ONLY in the cache and nothing durable, the download is a fresh
    /// start.
    #[tokio::test]
    async fn tiered_cache_leg_does_not_carry_forward() {
        test_support::clear();
        let _provider = test_support::install(ObjectStoreProvider::in_memory());
        test_support::install_fake_valkey();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        let remote_key = remote_state_key(&local);
        test_support::fake_valkey_set(
            &valkey_state_key_at(DEFAULT_VALKEY_PREFIX, current_version() - 1, &remote_key),
            b"OLDER-CACHE-COPY".to_vec(),
        )
        .unwrap();

        let cfg = StateConfig {
            backend: StateBackend::Tiered,
            s3_bucket: Some("bucket".into()),
            // Explicit `off`: tiered now defaults to `cas` (#1228), which
            // reads the durable leg only. This test pins the cache-leg path.
            concurrency_control: Some(ConcurrencyControl::Off),
            ..Default::default()
        };
        let outcome = download_state_inner(&cfg, &local, &remote_key, None)
            .await
            .unwrap();
        test_support::clear();
        assert_eq!(
            outcome,
            DownloadOutcome::Absent,
            "an older Valkey cache copy must never be carried forward"
        );
        assert!(!local.exists());
    }

    /// Tiered `off`: a cache miss falls through to S3, and the S3 leg carries
    /// forward — the durable older copy wins over an older cache copy.
    #[tokio::test]
    async fn tiered_off_durable_leg_carries_forward() {
        test_support::clear();
        let provider = test_support::install(ObjectStoreProvider::in_memory());
        test_support::install_fake_valkey();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        let remote_key = remote_state_key(&local);
        let prev = current_version() - 1;
        test_support::fake_valkey_set(
            &valkey_state_key_at(DEFAULT_VALKEY_PREFIX, prev, &remote_key),
            b"OLDER-CACHE-COPY".to_vec(),
        )
        .unwrap();
        provider
            .put(
                &object_store_state_key_at(prev, &remote_key),
                Bytes::from_static(b"OLDER-DURABLE-COPY"),
            )
            .await
            .unwrap();

        let cfg = StateConfig {
            backend: StateBackend::Tiered,
            s3_bucket: Some("bucket".into()),
            // Explicit `off`: tiered now defaults to `cas` (#1228), which
            // reads the durable leg only. This test pins the cache-leg path.
            concurrency_control: Some(ConcurrencyControl::Off),
            ..Default::default()
        };
        let outcome = download_state_inner(&cfg, &local, &remote_key, None)
            .await
            .unwrap();
        test_support::clear();
        assert_eq!(outcome, DownloadOutcome::Restored);
        assert_eq!(std::fs::read(&local).unwrap(), b"OLDER-DURABLE-COPY");
    }

    /// Valkey backend: absent current key, present older keys → restore the
    /// newest older one; the older keys are left as they were.
    #[tokio::test]
    async fn valkey_carry_forward_restores_newest_older_key() {
        test_support::clear();
        test_support::install_fake_valkey();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        let remote_key = remote_state_key(&local);
        let prev = current_version() - 1;
        let prev_key = valkey_state_key_at(DEFAULT_VALKEY_PREFIX, prev, &remote_key);
        let floor_key = valkey_state_key_at(
            DEFAULT_VALKEY_PREFIX,
            CARRY_FORWARD_FLOOR_SCHEMA_VERSION,
            &remote_key,
        );
        let prev_bytes = older_ledger_bytes(dir.path(), prev, "valkey-kept");
        test_support::fake_valkey_set(&prev_key, prev_bytes.clone()).unwrap();
        test_support::fake_valkey_set(
            &floor_key,
            older_ledger_bytes(dir.path(), CARRY_FORWARD_FLOOR_SCHEMA_VERSION, "too-old"),
        )
        .unwrap();

        let cfg = StateConfig {
            backend: StateBackend::Valkey,
            ..Default::default()
        };
        let authority = download_state(&cfg, &local, false).await.unwrap();
        let prev_after = test_support::fake_valkey_get(&prev_key);
        let current_after =
            test_support::fake_valkey_get(&valkey_state_key(DEFAULT_VALKEY_PREFIX, &remote_key));
        test_support::clear();

        assert_eq!(authority, StateAuthority::Authoritative);
        assert_eq!(ledger_plan_ids(&local), vec!["valkey-kept".to_string()]);
        assert_eq!(prev_after, Some(prev_bytes), "the older key is untouched");
        assert!(current_after.is_none(), "a download never writes Valkey");
    }

    /// Valkey backend: nothing below v22 is read.
    #[tokio::test]
    async fn valkey_carry_forward_never_reads_below_the_floor() {
        test_support::clear();
        test_support::install_fake_valkey();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        let remote_key = remote_state_key(&local);
        test_support::fake_valkey_set(
            &valkey_state_key_at(
                DEFAULT_VALKEY_PREFIX,
                CARRY_FORWARD_FLOOR_SCHEMA_VERSION - 1,
                &remote_key,
            ),
            b"BELOW-FLOOR".to_vec(),
        )
        .unwrap();
        let cfg = StateConfig {
            backend: StateBackend::Valkey,
            ..Default::default()
        };
        let authority = download_state(&cfg, &local, false).await.unwrap();
        test_support::clear();
        assert_eq!(authority, StateAuthority::FreshStart);
        assert!(!local.exists());
    }

    // -----------------------------------------------------------------------
    // #1228 — backend-default concurrency, startup probe, cas-required marker
    // -----------------------------------------------------------------------

    /// A fault-counting in-memory provider (thread-local) plus a handle on it.
    fn install_probe_provider() -> (crate::fault_store::FaultHandle, ObjectStoreProvider) {
        let (store, faults) = crate::fault_store::FaultingStore::wrap(std::sync::Arc::new(
            object_store::memory::InMemory::new(),
        ));
        let provider =
            test_support::install(ObjectStoreProvider::from_store(store, "s3", "bucket", ""));
        (faults, provider)
    }

    /// `s3` with `concurrency_control` left unset.
    fn s3_unset_config() -> StateConfig {
        StateConfig {
            backend: StateBackend::S3,
            s3_bucket: Some("bucket".into()),
            on_upload_failure: StateUploadFailureMode::Fail,
            retry: RetryConfig {
                max_retries: 0,
                ..RetryConfig::default()
            },
            ..Default::default()
        }
    }

    fn with_mode(cfg: &StateConfig, mode: ConcurrencyControl) -> StateConfig {
        StateConfig {
            concurrency_control: Some(mode),
            ..cfg.clone()
        }
    }

    async fn run_session(cfg: &StateConfig, local: &Path) -> Result<(), StateSyncError> {
        let mut session =
            RemoteStateSession::new(cfg, local, FinalizeDurability::ConfigDefault, false);
        if let Err(e) = session.acquire().await {
            session.abandon("test: acquire failed").await;
            return Err(e);
        }
        session.finalize().await
    }

    #[test]
    fn unset_concurrency_control_defaults_per_backend() {
        use ConcurrencyControl::{Cas, Off};
        for (backend, expected) in [
            (StateBackend::S3, Cas),
            (StateBackend::Gcs, Cas),
            (StateBackend::Tiered, Cas),
            (StateBackend::Local, Off),
            // A plain `SET`, no conditional put — see `upload_to_valkey`.
            (StateBackend::Valkey, Off),
        ] {
            assert_eq!(default_concurrency_control(backend), expected, "{backend}");
            let cfg = StateConfig {
                backend,
                ..Default::default()
            };
            assert_eq!(
                requested_concurrency_control(&cfg),
                (expected, ConcurrencySource::Defaulted),
                "{backend}"
            );
            assert_eq!(cas_effective(&cfg), expected == Cas, "{backend}");
        }
        // `off` stays an explicit opt-out.
        let explicit_off = with_mode(&s3_unset_config(), Off);
        assert_eq!(
            requested_concurrency_control(&explicit_off),
            (Off, ConcurrencySource::Explicit)
        );
        assert!(!cas_effective(&explicit_off));
    }

    #[test]
    fn unset_concurrency_control_parses_as_none_and_explicit_values_round_trip() {
        let unset: StateConfig = toml::from_str("backend = \"s3\"\n").unwrap();
        assert_eq!(unset.concurrency_control, None);
        let off: StateConfig =
            toml::from_str("backend = \"s3\"\nconcurrency_control = \"off\"\n").unwrap();
        assert_eq!(off.concurrency_control, Some(ConcurrencyControl::Off));
    }

    #[tokio::test]
    async fn probe_reports_supported_and_leaves_nothing_behind() {
        test_support::clear();
        let (faults, provider) = install_probe_provider();
        let outcome = probe_conditional_writes(&s3_unset_config()).await;
        assert_eq!(outcome, CasProbeOutcome::Supported);
        // Cheap and self-cleaning: four conditional puts, and the probe object
        // is gone afterwards. It never touched the state object.
        assert_eq!(faults.count(crate::fault_store::FaultOp::Put), 4);
        assert!(
            provider.list("").await.unwrap().is_empty(),
            "probe must delete its object"
        );
        let state_key = object_store_state_key(STATE_FILE);
        for kind in [
            crate::fault_store::PutKind::Unconditional,
            crate::fault_store::PutKind::Create,
            crate::fault_store::PutKind::Update,
        ] {
            assert_eq!(faults.put_count(&state_key, kind), 0);
        }
        test_support::clear();
    }

    /// Older S3-compatible stores accept the conditional headers and ignore
    /// them. That must read as `Unsupported`, never `Supported`.
    #[tokio::test]
    async fn probe_reports_unsupported_when_the_store_ignores_preconditions() {
        test_support::clear();
        let (faults, _provider) = install_probe_provider();
        faults.ignore_conditional_writes(true);
        let outcome = probe_conditional_writes(&s3_unset_config()).await;
        assert!(
            matches!(&outcome, CasProbeOutcome::Unsupported(why) if why.contains("If-None-Match")),
            "got {outcome:?}"
        );
        test_support::clear();
    }

    /// One transport blip on a conditional put is retried, not read as
    /// "conditional writes rejected".
    #[tokio::test]
    async fn probe_single_transient_error_is_not_unsupported() {
        test_support::clear();
        let (faults, _provider) = install_probe_provider();
        faults.arm(
            crate::fault_store::FaultOp::Put,
            crate::fault_store::FaultMode::FailNext(1),
        );
        assert_eq!(
            probe_conditional_writes(&s3_unset_config()).await,
            CasProbeOutcome::Supported
        );
        test_support::clear();
    }

    #[tokio::test]
    async fn defaulted_mode_on_a_supporting_store_resolves_to_cas() {
        test_support::clear();
        let (_faults, _provider) = install_probe_provider();
        let resolved = resolve_concurrency_control(&s3_unset_config())
            .await
            .unwrap();
        assert_eq!(resolved.mode, ConcurrencyControl::Cas);
        assert_eq!(resolved.source, ConcurrencySource::Defaulted);
        assert_eq!(resolved.probe, Some(CasProbeOutcome::Supported));
        test_support::clear();
    }

    /// Defaulted `cas` on a store that ignores conditional writes falls back
    /// to `off` (with a warning) — the run proceeds unconditionally.
    #[tokio::test]
    async fn defaulted_mode_falls_back_to_off_when_the_probe_says_unsupported() {
        test_support::clear();
        let (faults, provider) = install_probe_provider();
        faults.ignore_conditional_writes(true);
        let cfg = s3_unset_config();
        let resolved = resolve_concurrency_control(&cfg).await.unwrap();
        assert_eq!(resolved.mode, ConcurrencyControl::Off);
        assert!(matches!(
            resolved.probe,
            Some(CasProbeOutcome::Unsupported(_))
        ));

        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        seed_state_file(&local);
        run_session(&cfg, &local)
            .await
            .expect("defaulted fallback to off still uploads");
        let marker = cas_required_marker_key(&remote_state_key(&local));
        assert!(
            !provider.exists(&marker).await.unwrap(),
            "an off writer never writes the cas-required marker"
        );
        test_support::clear();
    }

    /// Explicit `cas` on a store that ignores conditional writes is an error,
    /// on both the run session and the ledger seam — never a silent
    /// downgrade.
    #[tokio::test]
    async fn explicit_cas_errors_when_the_probe_says_unsupported() {
        test_support::clear();
        let (faults, provider) = install_probe_provider();
        faults.ignore_conditional_writes(true);
        let cfg = with_mode(&s3_unset_config(), ConcurrencyControl::Cas);

        let err = resolve_concurrency_control(&cfg).await.unwrap_err();
        assert!(
            matches!(err, StateSyncError::CasUnsupported { .. }),
            "{err:?}"
        );
        assert!(err.to_string().contains("concurrency_control = \"off\""));

        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        seed_state_file(&local);
        let err = run_session(&cfg, &local).await.unwrap_err();
        assert!(
            matches!(err, StateSyncError::CasUnsupported { .. }),
            "{err:?}"
        );

        let seam = LedgerSeamSession::new(&cfg, &local, false);
        let ran = std::sync::atomic::AtomicBool::new(false);
        let err = seam
            .execute(|_store, _base| {
                ran.store(true, std::sync::atomic::Ordering::SeqCst);
                Box::pin(async { Ok(()) })
            })
            .await
            .unwrap_err();
        assert!(
            matches!(err, StateSyncError::CasUnsupported { .. }),
            "{err:?}"
        );
        assert!(!ran.load(std::sync::atomic::Ordering::SeqCst));
        let state_key = object_store_state_key(STATE_FILE);
        assert!(
            !provider.exists(&state_key).await.unwrap(),
            "nothing may be written"
        );
        test_support::clear();
    }

    /// An inconclusive probe keeps the defaulted `cas`: falling back to
    /// unconditional uploads on a guess would reopen the lost update.
    #[tokio::test]
    async fn inconclusive_probe_keeps_defaulted_cas() {
        test_support::clear();
        let (faults, _provider) = install_probe_provider();
        faults.arm(
            crate::fault_store::FaultOp::Put,
            crate::fault_store::FaultMode::FailAll,
        );
        let resolved = resolve_concurrency_control(&s3_unset_config())
            .await
            .unwrap();
        assert_eq!(resolved.mode, ConcurrencyControl::Cas);
        assert!(matches!(
            resolved.probe,
            Some(CasProbeOutcome::Inconclusive(_))
        ));
        test_support::clear();
    }

    /// With no setting at all, `s3` now commits by compare-and-swap: a stale
    /// writer conflicts instead of erasing the winner, the state object is
    /// never written unconditionally, and the first CAS upload creates the
    /// `cas-required` marker exactly once.
    #[tokio::test]
    async fn defaulted_s3_session_commits_by_cas_and_writes_the_marker_once() {
        test_support::clear();
        let (faults, provider) = install_probe_provider();
        let cfg = s3_unset_config();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        seed_state_file(&local);
        let state_key = object_store_state_key(&remote_state_key(&local));
        let marker = cas_required_marker_key(&remote_state_key(&local));

        run_session(&cfg, &local).await.expect("first run commits");
        let first_marker = provider.get(&marker).await.expect("marker created");
        assert!(String::from_utf8_lossy(&first_marker).contains("cas-required"));

        // A stale writer: acquires, then loses to a racer.
        let mut stale =
            RemoteStateSession::new(&cfg, &local, FinalizeDurability::ConfigDefault, false);
        let _ = stale.acquire().await.unwrap();
        run_session(&cfg, &local).await.expect("racer commits");
        let err = stale.finalize().await.unwrap_err();
        assert!(matches!(err, StateSyncError::CasConflict { .. }), "{err:?}");

        assert_eq!(
            provider.get(&marker).await.unwrap(),
            first_marker,
            "the marker is create-once: later CAS uploads never rewrite it"
        );
        assert_eq!(
            faults.put_count(&state_key, crate::fault_store::PutKind::Unconditional),
            0,
            "a defaulted s3 writer never uploads the state object unconditionally"
        );
        assert_eq!(
            faults.put_count(&marker, crate::fault_store::PutKind::Unconditional),
            0
        );
        test_support::clear();
    }

    /// The marker goes in BEFORE the conditional put: if it cannot be created,
    /// the state object is not committed at all.
    #[tokio::test]
    async fn marker_failure_blocks_the_cas_commit() {
        test_support::clear();
        let (faults, provider) = install_probe_provider();
        let cfg = s3_unset_config();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        seed_state_file(&local);
        let state_key = object_store_state_key(&remote_state_key(&local));
        let marker = cas_required_marker_key(&remote_state_key(&local));
        faults.arm_precondition_failures(marker.clone(), 1);

        let err = run_session(&cfg, &local).await.unwrap_err();
        assert!(
            !matches!(err, StateSyncError::CasConflict { .. }),
            "{err:?}"
        );
        assert!(!provider.exists(&state_key).await.unwrap());
        test_support::clear();
    }

    /// #1228's core: once a CAS writer has marked the state object, an
    /// explicit-`off` writer refuses its unconditional upload — even under
    /// `on_upload_failure = "skip"`, which must not swallow the refusal — and
    /// the committed state is untouched. Reads are unaffected.
    #[tokio::test]
    async fn explicit_off_writer_refuses_when_the_marker_is_present() {
        test_support::clear();
        let (faults, provider) = install_probe_provider();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        seed_state_file(&local);
        let state_key = object_store_state_key(&remote_state_key(&local));

        run_session(&s3_unset_config(), &local)
            .await
            .expect("CAS writer commits and creates the marker");
        let committed = provider.get(&state_key).await.unwrap();

        for on_upload_failure in [StateUploadFailureMode::Fail, StateUploadFailureMode::Skip] {
            let off = StateConfig {
                on_upload_failure,
                ..with_mode(&s3_unset_config(), ConcurrencyControl::Off)
            };
            let err = run_session(&off, &local).await.unwrap_err();
            assert!(
                matches!(err, StateSyncError::CasRequired { .. }),
                "{on_upload_failure:?}: {err:?}"
            );
            let message = err.to_string();
            assert!(
                message.contains("concurrency_control = \"cas\"")
                    && message.contains(".cas-required")
                    && message.contains("delete the marker on purpose"),
                "the refusal must name both fixes: {message}"
            );
            let err = upload_state(&off, &local, false).await.unwrap_err();
            assert!(matches!(err, StateSyncError::CasRequired { .. }), "{err:?}");
            // Reads are unaffected.
            let _ = download_state(&off, &local, false)
                .await
                .expect("an off writer can still read");
        }
        assert_eq!(provider.get(&state_key).await.unwrap(), committed);
        assert_eq!(
            faults.put_count(&state_key, crate::fault_store::PutKind::Unconditional),
            0,
            "the refused writer never reached the state object"
        );
        test_support::clear();
    }

    /// P3-6: the marker sits under its schema-version prefix and a bump does
    /// not move it. A marker left at an older version by a CAS fleet still
    /// binds an explicit-`off` writer at the current version, at start and at
    /// upload.
    #[tokio::test]
    async fn an_older_version_marker_refuses_an_off_writer_after_a_bump() {
        test_support::clear();
        let (faults, provider) = install_probe_provider();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        seed_state_file(&local);
        let remote = remote_state_key(&local);
        let previous = current_version() - 1;
        let old_marker = format!(
            "{}{CAS_REQUIRED_MARKER_SUFFIX}",
            object_store_state_key_at(previous, &remote)
        );
        provider
            .put(&old_marker, Bytes::from_static(b"{}"))
            .await
            .unwrap();
        assert!(
            !provider
                .exists(&cas_required_marker_key(&remote))
                .await
                .unwrap(),
            "PRECONDITION: no marker at the current version"
        );

        let off = StateConfig {
            on_upload_failure: StateUploadFailureMode::Fail,
            ..with_mode(&s3_unset_config(), ConcurrencyControl::Off)
        };
        let mut session =
            RemoteStateSession::new(&off, &local, FinalizeDurability::ConfigDefault, false);
        let err = session.acquire().await.unwrap_err();
        session.abandon("test: refused at acquire").await;
        match &err {
            StateSyncError::CasRequired { marker } => {
                assert!(marker.contains(&format!("v{previous}/")), "{marker}");
            }
            other => panic!("expected CasRequired, got {other:?}"),
        }
        let err = upload_state(&off, &local, false).await.unwrap_err();
        assert!(matches!(err, StateSyncError::CasRequired { .. }), "{err:?}");
        let state_key = object_store_state_key(&remote);
        assert_eq!(
            faults.put_count(&state_key, crate::fault_store::PutKind::Unconditional),
            0,
            "the refused writer never reached the state object"
        );
        assert_eq!(
            cas_required_marker_present(&off, &local).await.unwrap(),
            Some(true),
            "doctor sees the older marker too"
        );
        test_support::clear();
    }

    /// Without the marker, explicit `off` keeps uploading unconditionally —
    /// the opt-out still works for a fleet that never ran `cas`.
    #[tokio::test]
    async fn explicit_off_writer_uploads_when_no_marker_exists() {
        test_support::clear();
        let (faults, _provider) = install_probe_provider();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        seed_state_file(&local);
        let off = with_mode(&s3_unset_config(), ConcurrencyControl::Off);
        run_session(&off, &local).await.unwrap();
        run_session(&off, &local).await.unwrap();
        let state_key = object_store_state_key(&remote_state_key(&local));
        assert_eq!(
            faults.put_count(&state_key, crate::fault_store::PutKind::Unconditional),
            2
        );
        test_support::clear();
    }

    /// The marker check runs once, before any leg: a refused tiered `off`
    /// upload writes nothing.
    #[tokio::test]
    async fn refused_tiered_off_upload_writes_neither_leg() {
        test_support::clear();
        let (_faults, provider) = install_tiered_backends();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        seed_state_file(&local);
        let marker = cas_required_marker_key(&remote_state_key(&local));
        provider
            .put(&marker, Bytes::from_static(b"{}"))
            .await
            .unwrap();
        let off = StateConfig {
            on_upload_failure: StateUploadFailureMode::Fail,
            ..with_mode(&tiered_cas_config(), ConcurrencyControl::Off)
        };
        let err = upload_state(&off, &local, false).await.unwrap_err();
        assert!(matches!(err, StateSyncError::CasRequired { .. }), "{err:?}");
        assert!(
            !provider
                .exists(&object_store_state_key(&remote_state_key(&local)))
                .await
                .unwrap(),
            "the durable leg must not be written"
        );
        test_support::clear();
    }

    /// Review finding #1: an `off` writer that finds the marker refuses at
    /// `acquire`, before any work — not at the end-of-run upload, after its
    /// warehouse writes. The ledger seam refuses before running its
    /// transition.
    #[tokio::test]
    async fn off_writer_refuses_at_acquire_when_the_marker_is_present() {
        test_support::clear();
        let (_faults, provider) = install_probe_provider();
        let dir = TempDir::new().unwrap();
        let local = dir.path().join(".rocky-state.redb");
        seed_state_file(&local);
        run_session(&s3_unset_config(), &local)
            .await
            .expect("CAS writer commits and creates the marker");
        let committed = provider
            .get(&object_store_state_key(&remote_state_key(&local)))
            .await
            .unwrap();

        let off = with_mode(&s3_unset_config(), ConcurrencyControl::Off);
        let mut session =
            RemoteStateSession::new(&off, &local, FinalizeDurability::ConfigDefault, false);
        let err = session.acquire().await.unwrap_err();
        session.abandon("test: refused at acquire").await;
        assert!(matches!(err, StateSyncError::CasRequired { .. }), "{err:?}");

        let seam = LedgerSeamSession::new(&off, &local, false);
        let ran = std::sync::atomic::AtomicBool::new(false);
        let err = seam
            .execute(|_store, _base| {
                ran.store(true, std::sync::atomic::Ordering::SeqCst);
                Box::pin(async { Ok(()) })
            })
            .await
            .unwrap_err();
        assert!(matches!(err, StateSyncError::CasRequired { .. }), "{err:?}");
        assert!(
            !ran.load(std::sync::atomic::Ordering::SeqCst),
            "the seam transition must not run"
        );
        assert_eq!(
            provider
                .get(&object_store_state_key(&remote_state_key(&local)))
                .await
                .unwrap(),
            committed
        );
        test_support::clear();
    }

    /// Review finding #4: "conditional write rejected, unconditional
    /// accepted" may be two transport blips in a row, so that verdict is not
    /// cached for the process. A verdict from an IGNORED precondition is.
    #[tokio::test]
    async fn only_store_behaviour_verdicts_are_cacheable() {
        test_support::clear();
        let (faults, _provider) = install_probe_provider();
        // Both tries of the first conditional put fail; the unconditional
        // fallback then succeeds.
        faults.arm(
            crate::fault_store::FaultOp::Put,
            crate::fault_store::FaultMode::FailNext(2),
        );
        let (outcome, cacheable) = probe_conditional_writes_inner(&s3_unset_config()).await;
        assert!(
            matches!(outcome, CasProbeOutcome::Unsupported(_)),
            "{outcome:?}"
        );
        assert!(
            !cacheable,
            "a rejection verdict must be re-probed, not pinned"
        );
        test_support::clear();

        let (faults, _provider) = install_probe_provider();
        faults.ignore_conditional_writes(true);
        let (outcome, cacheable) = probe_conditional_writes_inner(&s3_unset_config()).await;
        assert!(
            matches!(outcome, CasProbeOutcome::Unsupported(_)),
            "{outcome:?}"
        );
        assert!(cacheable);
        test_support::clear();

        let (_faults, _provider) = install_probe_provider();
        assert_eq!(
            probe_conditional_writes_inner(&s3_unset_config()).await,
            (CasProbeOutcome::Supported, true)
        );
        test_support::clear();
    }

    /// P3-7: step 3's first attempt lands but its response is lost, so its
    /// retry carries a generation the write itself just moved on and is
    /// refused. That is the store working, not "conditional writes
    /// unsupported": the verdict is inconclusive and not cached.
    #[tokio::test]
    async fn a_conflict_after_a_retried_if_match_is_inconclusive() {
        test_support::clear();
        let (faults, _provider) = install_probe_provider();
        // Puts: 1 create, 2 create-again, 3 if-match (lands, then errors).
        faults.land_then_fail_nth_put(3);
        let (outcome, cacheable) = probe_conditional_writes_inner(&s3_unset_config()).await;
        assert!(
            matches!(outcome, CasProbeOutcome::Inconclusive(_)),
            "{outcome:?}"
        );
        assert!(!cacheable, "an inconclusive probe is never cached");
        test_support::clear();
    }

    /// P3-7: the probe cache key names the endpoint, so the same bucket name
    /// on another S3-compatible endpoint is probed on its own.
    #[test]
    fn the_probe_cache_key_names_the_endpoint_and_region() {
        let key = cas_probe_cache_key(&s3_unset_config());
        assert!(
            key.contains("AWS_ENDPOINT_URL=") && key.contains("AWS_REGION="),
            "{key}"
        );
    }

    /// The probe object lives under its own `cas-probe/` key.
    #[test]
    fn cas_probe_key_is_its_own_namespace() {
        let key = cas_probe_key();
        assert!(
            key.starts_with("cas-probe/") && !key.contains("doctor"),
            "{key}"
        );
        assert_ne!(cas_probe_key(), key);
    }
}

/// RV1-P2: environment publishes through the ledger seam — the cross-pod
/// race, the lost-update negative control, a run finalize racing a publish,
/// and the carry-forward across the v31 -> v32 bump.
#[cfg(test)]
mod environment_publish_tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use tempfile::TempDir;

    use super::*;
    use crate::config::PrincipalRef;
    use crate::environments::{
        EnvironmentName, EnvironmentRecord, PublishRecord, PublishRequest, PublishSource,
    };
    use crate::state::{OutputVersion, run_with_output_versions};
    use crate::test_harness::CrossPodHarness;

    fn delta(v: u64) -> Option<OutputVersion> {
        Some(OutputVersion::DeltaObserved {
            table: "c.s.t".into(),
            version: v,
        })
    }

    fn req(env: &str, expected: Option<&str>, sources: &[(&str, &str)]) -> PublishRequest {
        PublishRequest {
            environment: EnvironmentName::parse(env).unwrap(),
            expected_head: expected.map(str::to_string),
            sources: sources
                .iter()
                .map(|(model, run_id)| PublishSource {
                    model: (*model).to_string(),
                    run_id: (*run_id).to_string(),
                })
                .collect(),
            principal: PrincipalRef::unnamed(),
            plan_id: None,
        }
    }

    fn record_seed_runs(store: &StateStore) {
        store
            .record_run(&run_with_output_versions(
                "r1",
                &[("orders", delta(1)), ("customers", delta(1))],
            ))
            .unwrap();
        store
            .record_run(&run_with_output_versions("r2", &[("orders", delta(2))]))
            .unwrap();
        store
            .record_run(&run_with_output_versions("r3", &[("customers", delta(3))]))
            .unwrap();
    }

    /// Hook that counts attempts and, on attempt 1 only, waits at `barrier`
    /// — after the local transaction read (and moved) the head, before the
    /// upload. Both racers therefore read the same head and the same blob
    /// generation before either uploads.
    fn barrier_hook(
        barrier: Arc<tokio::sync::Barrier>,
        attempts: Arc<AtomicU32>,
    ) -> PublishAttemptHook {
        Arc::new(move |n| {
            attempts.fetch_add(1, Ordering::SeqCst);
            let barrier = Arc::clone(&barrier);
            Box::pin(async move {
                if n == 1 {
                    barrier.wait().await;
                }
            })
        })
    }

    /// Two pods with `cc`, the seed runs on the remote, and `staging#1`
    /// published (orders from r1) by pod A.
    async fn bootstrapped(cc: ConcurrencyControl) -> CrossPodHarness {
        let mut h = CrossPodHarness::new_s3_like();
        h.pod_a.cfg.concurrency_control = Some(cc);
        h.pod_b.cfg.concurrency_control = Some(cc);
        drop(StateStore::open(&h.pod_a.state_path).unwrap());
        let mut seed = RemoteStateSession::new(
            &h.pod_a.cfg,
            &h.pod_a.state_path,
            FinalizeDurability::Durable,
            false,
        );
        let _ = seed.acquire().await.unwrap();
        record_seed_runs(&h.open_store(&h.pod_a));
        seed.finalize().await.unwrap();
        // The unguarded path, so the `off` negative control can bootstrap.
        let first = publish_pointers_with_hook(
            &LedgerSeamSession::new(&h.pod_a.cfg, &h.pod_a.state_path, false),
            &req("staging", None, &[("orders", "r1")]),
            None,
        )
        .await
        .unwrap();
        assert_eq!(first.publish_id, "staging#1");
        h
    }

    /// The remote head and history, read through a fresh download.
    async fn remote_view(h: &CrossPodHarness) -> (EnvironmentRecord, Vec<PublishRecord>, bool) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(".rocky-state.redb");
        let _ = download_state(&h.pod_a.cfg, &path, false).await.unwrap();
        let store = StateStore::open(&path).unwrap();
        let env = EnvironmentName::parse("staging").unwrap();
        let head = store
            .get_environment(&env)
            .unwrap()
            .expect("staging exists");
        let history = store.publish_history(&env).unwrap();
        let has_r9 = store.get_run("r9").unwrap().is_some();
        (head, history, has_r9)
    }

    /// Race two publishes from the same head, one per pod, with the barrier.
    async fn race(
        h: &CrossPodHarness,
        a: PublishRequest,
        b: PublishRequest,
    ) -> [(Result<PublishRecord, StateSyncError>, u32); 2] {
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let (att_a, att_b) = (Arc::new(AtomicU32::new(0)), Arc::new(AtomicU32::new(0)));
        let sa = LedgerSeamSession::new(&h.pod_a.cfg, &h.pod_a.state_path, false);
        let sb = LedgerSeamSession::new(&h.pod_b.cfg, &h.pod_b.state_path, false);
        let (ra, rb) = tokio::join!(
            publish_pointers_with_hook(
                &sa,
                &a,
                Some(barrier_hook(Arc::clone(&barrier), Arc::clone(&att_a)))
            ),
            publish_pointers_with_hook(
                &sb,
                &b,
                Some(barrier_hook(Arc::clone(&barrier), Arc::clone(&att_b)))
            ),
        );
        [
            (ra, att_a.load(Ordering::SeqCst)),
            (rb, att_b.load(Ordering::SeqCst)),
        ]
    }

    /// RV1-P2 exit test. Two pods publish from head `staging#1` at once.
    /// Exactly one wins; the loser's CAS upload conflicts, its replay reads
    /// the winner's head, and it is refused with the typed `PublishConflict`
    /// on attempt 2. The remote holds the winner's pointers and a gap-free
    /// history.
    #[tokio::test]
    async fn concurrent_publishes_from_one_head_one_wins_one_conflicts() {
        let _serial = test_support::serial_guard();
        test_support::clear();
        let h = bootstrapped(ConcurrencyControl::Cas).await;
        let results = race(
            &h,
            req("staging", Some("staging#1"), &[("orders", "r2")]),
            req("staging", Some("staging#1"), &[("customers", "r3")]),
        )
        .await;

        let wins: Vec<_> = results.iter().filter(|(r, _)| r.is_ok()).collect();
        let losses: Vec<_> = results.iter().filter(|(r, _)| r.is_err()).collect();
        assert_eq!(wins.len(), 1, "exactly one publish wins: {results:?}");
        assert_eq!(losses.len(), 1);
        let (Ok(winner), win_attempts) = wins[0] else {
            unreachable!()
        };
        let (Err(loss), lose_attempts) = losses[0] else {
            unreachable!()
        };
        match loss {
            StateSyncError::PublishConflict {
                env,
                expected,
                found,
            } => {
                assert_eq!(env, "staging");
                assert_eq!(expected.as_deref(), Some("staging#1"));
                assert_eq!(found.as_deref(), Some("staging#2"));
            }
            other => panic!("expected PublishConflict, got {other:?}"),
        }
        assert_eq!(*win_attempts, 1);
        assert_eq!(*lose_attempts, 2, "the loser replays once and is refused");

        let (head, history, _) = remote_view(&h).await;
        assert_eq!(head.head_publish_id, "staging#2");
        // Either racer can win. The head is staging#1's pointers with the
        // winner's merged over them — and nothing of the loser's.
        let mut expected = history[0].to.clone();
        expected.extend(winner.to.clone());
        assert_eq!(head.pointers, expected, "remote head = staging#1 + winner");
        let seqs: Vec<u64> = history.iter().map(|r| r.seq).collect();
        assert_eq!(seqs, vec![1, 2]);
        assert_eq!(history[1].prior_publish_id.as_deref(), Some("staging#1"));
        assert_eq!(&history[1], winner);

        // The loser's LOCAL store holds the remote winner, not its own
        // refused transaction: same head, same history.
        let loser_pod = if results[0].0.is_err() {
            &h.pod_a
        } else {
            &h.pod_b
        };
        let local = h.open_store(loser_pod);
        let staging = EnvironmentName::parse("staging").unwrap();
        assert_eq!(
            local.get_environment(&staging).unwrap().as_ref(),
            Some(&head)
        );
        assert_eq!(local.publish_history(&staging).unwrap(), history);
        test_support::clear();
    }

    /// The guard: a remote backend with `concurrency_control = "off"` refuses
    /// a publish with the typed error before any download or upload, because
    /// a lost publish would be silent there (see the negative control below).
    /// Mutation: drop the guard and the publish succeeds.
    #[tokio::test]
    async fn publish_through_a_remote_backend_without_cas_is_refused() {
        let _serial = test_support::serial_guard();
        test_support::clear();
        let h = bootstrapped(ConcurrencyControl::Off).await;
        let (gets, puts) = (
            h.faults.count(crate::fault_store::FaultOp::Get),
            h.faults.put_count_outside("cas-probe/"),
        );
        let err = publish_pointers(
            &LedgerSeamSession::new(&h.pod_b.cfg, &h.pod_b.state_path, false),
            &req("staging", Some("staging#1"), &[("orders", "r2")]),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(&err, StateSyncError::PublishRequiresCas { backend } if backend == "s3"),
            "{err:?}"
        );
        assert!(!err.to_string().is_empty());
        assert_eq!(h.faults.count(crate::fault_store::FaultOp::Get), gets);
        assert_eq!(
            h.faults.put_count_outside("cas-probe/"),
            puts,
            "nothing uploaded"
        );
        let (head, _, _) = remote_view(&h).await;
        assert_eq!(head.head_publish_id, "staging#1");
        test_support::clear();
    }

    /// A refused publish through the seam (a model the run did not execute,
    /// and an existing history row) uploads nothing and leaves the local
    /// store equal to the remote.
    #[tokio::test]
    async fn a_refused_publish_through_the_seam_uploads_nothing() {
        let _serial = test_support::serial_guard();
        test_support::clear();
        let h = bootstrapped(ConcurrencyControl::Cas).await;
        let session = LedgerSeamSession::new(&h.pod_b.cfg, &h.pod_b.state_path, false);
        let staging = EnvironmentName::parse("staging").unwrap();

        let puts = h.faults.put_count_outside("cas-probe/");
        let err = publish_pointers(
            &session,
            &req("staging", Some("staging#1"), &[("customers", "r2")]),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(
                &err,
                StateSyncError::State(crate::state::StateError::Environment(
                    crate::environments::EnvironmentError::Refused { .. }
                ))
            ),
            "{err:?}"
        );
        assert_eq!(
            h.faults.put_count_outside("cas-probe/"),
            puts,
            "Refused uploads nothing"
        );
        let local = h.open_store(&h.pod_b);
        assert_eq!(
            local
                .get_environment(&staging)
                .unwrap()
                .unwrap()
                .head_publish_id,
            "staging#1"
        );
        drop(local);

        // Plant an orphan history row at seq 2 in the remote blob.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(".rocky-state.redb");
        let _ = download_state(&h.pod_a.cfg, &path, false).await.unwrap();
        crate::state::insert_orphan_history_row(
            &path,
            &crate::environments::history_key(&staging, 2),
        );
        let key = "v32/state.redb";
        assert!(h.provider.exists(key).await.unwrap());
        h.provider
            .put(key, Bytes::from(std::fs::read(&path).unwrap()))
            .await
            .unwrap();

        let puts = h.faults.put_count_outside("cas-probe/");
        let err = publish_pointers(
            &session,
            &req("staging", Some("staging#1"), &[("orders", "r2")]),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(
                &err,
                StateSyncError::State(crate::state::StateError::Environment(
                    crate::environments::EnvironmentError::HistoryRowExists { .. }
                ))
            ),
            "{err:?}"
        );
        assert_eq!(
            h.faults.put_count_outside("cas-probe/"),
            puts,
            "HistoryRowExists uploads nothing"
        );
        let local = h.open_store(&h.pod_b);
        assert_eq!(
            local
                .get_environment(&staging)
                .unwrap()
                .unwrap()
                .head_publish_id,
            "staging#1"
        );
        test_support::clear();
    }

    /// Negative control: the same race with `concurrency_control = "off"`,
    /// through the UNGUARDED inner path (the public `publish_pointers` now
    /// refuses `off`). Both publishes report success and the remote keeps only
    /// one: an update is lost. The in-store head check alone does not stop the
    /// blob-level loss, which is why the guard exists. It also proves the race
    /// above can fail, so its pass is evidence.
    #[tokio::test]
    async fn concurrent_publishes_without_cas_lose_an_update() {
        let _serial = test_support::serial_guard();
        test_support::clear();
        let h = bootstrapped(ConcurrencyControl::Off).await;
        let results = race(
            &h,
            req("staging", Some("staging#1"), &[("orders", "r2")]),
            req("staging", Some("staging#1"), &[("customers", "r3")]),
        )
        .await;
        for (r, attempts) in &results {
            let record = r
                .as_ref()
                .expect("without CAS both publishes report success");
            assert_eq!(record.publish_id, "staging#2", "both claim seq 2");
            assert_eq!(*attempts, 1);
        }
        let (head, history, _) = remote_view(&h).await;
        assert_eq!(history.len(), 2, "one of the two seq-2 rows is gone");
        let orders_moved = head.pointers["orders"].run_id == "r2";
        let customers_moved = head.pointers.contains_key("customers");
        assert!(
            orders_moved != customers_moved,
            "exactly one publish survived; the other was lost: {head:?}"
        );
        test_support::clear();
    }

    /// An unrelated run finalize moves the blob between the publish's
    /// download and its upload. The publish's CAS conflicts, the replay finds
    /// the same head, and the publish SUCCEEDS on attempt 2. The run row
    /// survives.
    #[tokio::test]
    async fn a_run_finalize_racing_a_publish_replays_and_both_survive() {
        let _serial = test_support::serial_guard();
        test_support::clear();
        let h = bootstrapped(ConcurrencyControl::Cas).await;

        let mut run = RemoteStateSession::new(
            &h.pod_a.cfg,
            &h.pod_a.state_path,
            FinalizeDurability::Durable,
            false,
        );
        let _ = run.acquire().await.unwrap();
        h.open_store(&h.pod_a)
            .record_run(&run_with_output_versions("r9", &[("orders", delta(9))]))
            .unwrap();
        let run = Arc::new(tokio::sync::Mutex::new(Some(run)));

        let attempts = Arc::new(AtomicU32::new(0));
        let hook: PublishAttemptHook = {
            let run = Arc::clone(&run);
            let attempts = Arc::clone(&attempts);
            Arc::new(move |n| {
                attempts.fetch_add(1, Ordering::SeqCst);
                let run = Arc::clone(&run);
                Box::pin(async move {
                    if n == 1 {
                        let session = run.lock().await.take().expect("finalize once");
                        session.finalize().await.expect("the run finalizes first");
                    }
                })
            })
        };
        let record = publish_pointers_with_hook(
            &LedgerSeamSession::new(&h.pod_b.cfg, &h.pod_b.state_path, false),
            &req("staging", Some("staging#1"), &[("customers", "r3")]),
            Some(hook),
        )
        .await
        .expect("the replay finds the same head and wins");
        assert_eq!(record.publish_id, "staging#2");
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            2,
            "one CAS conflict, one replay"
        );

        let (head, history, has_r9) = remote_view(&h).await;
        assert!(has_r9, "the run row survives the publish");
        assert_eq!(head.head_publish_id, "staging#2");
        assert_eq!(history.len(), 2);
        test_support::clear();
    }

    /// Local backend: one store, no remote. Two sequential publishes from the
    /// same head: the second is a `PublishConflict` through the seam too.
    #[tokio::test]
    async fn local_backend_second_publish_from_the_same_head_conflicts() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(".rocky-state.redb");
        record_seed_runs(&StateStore::open(&path).unwrap());
        let session = LedgerSeamSession::new(&StateConfig::default(), &path, false);
        publish_pointers(&session, &req("staging", None, &[("orders", "r1")]))
            .await
            .unwrap();
        publish_pointers(
            &session,
            &req("staging", Some("staging#1"), &[("orders", "r2")]),
        )
        .await
        .unwrap();
        let err = publish_pointers(
            &session,
            &req("staging", Some("staging#1"), &[("customers", "r3")]),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(&err, StateSyncError::PublishConflict { found: Some(f), .. } if f == "staging#2"),
            "{err:?}"
        );
    }

    /// The bytes of a store a v31 binary wrote, holding run r1 (and r2, r3).
    fn v31_state_bytes(dir: &Path) -> Vec<u8> {
        let path = dir.join("seed-v31.redb");
        record_seed_runs(&StateStore::open(&path).unwrap());
        crate::state::force_pre_v32_store(&path);
        std::fs::read(&path).unwrap()
    }

    /// Carry-forward across the bump. Only `v31/state.redb` exists. The v32
    /// download restores it (Authoritative, stamped 32, run kept); a publish
    /// creates `v32/state.redb` with create-if-absent; the v31 key stays
    /// byte-identical.
    #[tokio::test]
    async fn publish_after_the_bump_carries_v31_forward_and_never_writes_it() {
        use crate::fault_store::PutKind;
        let _serial = test_support::serial_guard();
        test_support::clear();
        let mut h = CrossPodHarness::new_s3_like();
        h.pod_a.cfg.concurrency_control = Some(ConcurrencyControl::Cas);
        assert_eq!(crate::state::current_schema_version(), 32);
        let seed_dir = TempDir::new().unwrap();
        let v31 = v31_state_bytes(seed_dir.path());
        let (v31_key, v32_key) = ("v31/state.redb", "v32/state.redb");
        h.provider
            .put(v31_key, Bytes::from(v31.clone()))
            .await
            .unwrap();

        let authority = h.download(&h.pod_a).await.unwrap();
        assert_eq!(authority, StateAuthority::Authoritative);
        // The first read-write open migrates the carried-forward store.
        assert!(h.open_store(&h.pod_a).get_run("r1").unwrap().is_some());
        assert_eq!(
            StateStore::peek_schema_version(&h.pod_a.state_path).unwrap(),
            Some(32)
        );
        assert!(!h.provider.exists(v32_key).await.unwrap());

        let record = publish_pointers(
            &LedgerSeamSession::new(&h.pod_a.cfg, &h.pod_a.state_path, false),
            &req("staging", None, &[("orders", "r1")]),
        )
        .await
        .unwrap();
        assert_eq!(record.publish_id, "staging#1");
        assert!(h.provider.exists(v32_key).await.unwrap());
        assert_eq!(h.faults.put_count(v32_key, PutKind::Create), 1);
        assert_eq!(h.faults.put_count(v32_key, PutKind::Unconditional), 0);
        assert_eq!(
            h.provider.get(v31_key).await.unwrap().to_vec(),
            v31,
            "the v31 key is never written"
        );
        let (head, _, _) = remote_view(&h).await;
        assert_eq!(head.head_publish_id, "staging#1");
        test_support::clear();
    }

    /// The FIRST publish right after the bump, from two pods at once. Both
    /// carry forward with no base and both try create-if-absent; one create
    /// is refused, and the loser's replay downloads the winner's `v32` key and
    /// finds the environment, so it is a `PublishConflict`.
    #[tokio::test]
    async fn concurrent_first_publish_after_the_bump_one_conflicts() {
        let _serial = test_support::serial_guard();
        test_support::clear();
        let mut h = CrossPodHarness::new_s3_like();
        h.pod_a.cfg.concurrency_control = Some(ConcurrencyControl::Cas);
        h.pod_b.cfg.concurrency_control = Some(ConcurrencyControl::Cas);
        let seed_dir = TempDir::new().unwrap();
        let v31 = v31_state_bytes(seed_dir.path());
        h.provider
            .put("v31/state.redb", Bytes::from(v31.clone()))
            .await
            .unwrap();

        let results = race(
            &h,
            req("staging", None, &[("orders", "r1")]),
            req("staging", None, &[("orders", "r2")]),
        )
        .await;
        let wins = results.iter().filter(|(r, _)| r.is_ok()).count();
        assert_eq!(wins, 1, "{results:?}");
        let (loss, attempts) = results.iter().find(|(r, _)| r.is_err()).unwrap();
        assert!(
            matches!(
                loss,
                Err(StateSyncError::PublishConflict { expected: None, found: Some(f), .. })
                    if f == "staging#1"
            ),
            "{loss:?}"
        );
        assert_eq!(*attempts, 2);
        let (head, history, _) = remote_view(&h).await;
        assert_eq!(head.head_publish_id, "staging#1");
        assert_eq!(history.len(), 1);
        assert_eq!(
            h.provider.get("v31/state.redb").await.unwrap().to_vec(),
            v31
        );
        test_support::clear();
    }
}
