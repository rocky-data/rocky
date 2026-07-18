//! Remote state persistence.
//!
//! Pulls / pushes the redb state file across runs so ephemeral environments
//! (EKS pods, CI jobs) can resume watermarks and anomaly history. Four
//! backends: local (no-op), S3, GCS, Valkey, or Tiered (Valkey + S3).
//!
//! S3 and GCS use the shared [`ObjectStoreProvider`][crate::object_store::ObjectStoreProvider]
//! so credential resolution follows the standard AWS SDK / GCP ADC chains.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use thiserror::Error;
use tracing::{Instrument, debug, info, info_span, warn};

use crate::circuit_breaker::TransitionOutcome;
use crate::config::{RetryConfig, StateBackend, StateConfig, StateUploadFailureMode};
use crate::object_store::{ObjectStoreError, ObjectStoreProvider};
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
/// Only a schema-changing bump shifts the segment, after which the new version
/// finds no state under its key and bootstraps once via the existing
/// incremental-bootstrap path.
fn schema_version_segment() -> String {
    format!("v{}", crate::state::current_schema_version())
}

/// Schema-qualified object-store key: `v9/state.redb` (under the configured
/// `<prefix>`). The version segment is a path component so the full object
/// path reads `<prefix>/v9/state.redb`.
fn object_store_state_key(remote_key: &str) -> String {
    format!("{}/{remote_key}", schema_version_segment())
}

/// Schema-qualified Valkey key: `<prefix>v9:state.redb` (e.g.
/// `rocky:state:v9:state.redb`). The version segment is colon-delimited to
/// match the Valkey prefix convention.
fn valkey_state_key(prefix: &str, remote_key: &str) -> String {
    format!("{prefix}{}:{remote_key}", schema_version_segment())
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
    use std::sync::{Mutex, MutexGuard, PoisonError};

    thread_local! {
        static PROVIDER_OVERRIDE: RefCell<Option<ObjectStoreProvider>> =
            const { RefCell::new(None) };
        /// One-shot fault: make the next object-store existence probe error.
        static OBJECT_STORE_EXISTS_FAULT: Cell<bool> = const { Cell::new(false) };
        /// One-shot fault: make the next Valkey download report a MISS.
        static VALKEY_MISS_FAULT: Cell<bool> = const { Cell::new(false) };
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
        VALKEY_MISS_FAULT.with(|c| c.set(false));
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
/// - **A `base` generation.** Deliberately absent this stage: the observed
///   remote generation (the field the CAS PR reads and CAS-writes at
///   `finalize`) is the concurrency PR's forward extension of this struct, not
///   part of the lifecycle spine.
///
/// # Drop tripwire
///
/// Dropping a session that was neither finalized nor abandoned is a bug in the
/// calling run path: it `warn!`s, `debug_assert!`s, and aborts (without
/// joining — `Drop` cannot await) any live periodic task so a leaked handle
/// cannot outlive the run. The tripwire is a diagnostic + resource net, NOT a
/// durability guarantee — a panic between mutation and `finalize` still skips
/// the upload.
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
}

impl RemoteStateSession {
    /// Build a session over an owned snapshot of `cfg` for the ledger at
    /// `state_path`. Performs no I/O; call [`acquire`][Self::acquire] before
    /// the first state read.
    pub fn new(cfg: &StateConfig, state_path: &Path, durability: FinalizeDurability) -> Self {
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
        }
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
    /// misuse: calling `acquire` twice on one session.
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

        match download_state(&self.cfg, &self.state_path).await {
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
        let cfg = self.cfg.clone();
        let path = self.state_path.clone();
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
                    store.snapshot_to_excluding(&scratch_path, crate::state::LOCAL_ONLY_TABLE_NAMES)
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
        match self.durability {
            FinalizeDurability::Durable => {
                let durable_cfg = StateConfig {
                    on_upload_failure: StateUploadFailureMode::Fail,
                    ..self.cfg.clone()
                };
                upload_state(&durable_cfg, &self.state_path).await
            }
            FinalizeDurability::ConfigDefault => upload_state(&self.cfg, &self.state_path).await,
        }
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

    /// Half-seam download for the single-record ledger seams (policy freeze,
    /// gc apply, restore apply, the governed-apply pre-gate sync): pull the
    /// authoritative remote ledger before the seam reads it.
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
    ) -> Result<StateAuthority, StateSyncError> {
        if matches!(cfg.backend, StateBackend::Local) {
            return Ok(StateAuthority::Authoritative);
        }
        download_state(cfg, state_path).await
    }

    /// Half-seam upload for the single-record ledger seams: push the local
    /// ledger to the remote backend with `on_upload_failure` **forced to
    /// [`Fail`][StateUploadFailureMode::Fail]**, regardless of the configured
    /// liveness default — a ledger mutation (freeze row, gc tombstone,
    /// restore custody, budget pair) that commits locally but never reaches
    /// the remote would be silently reverted by the next run's
    /// start-download while the command reported success.
    ///
    /// [`StateBackend::Local`] is a no-op. `reason` names the seam in the
    /// upload's structured log line; error context stays with the caller.
    ///
    /// # Errors
    ///
    /// Propagates the upload failure (never swallowed — the forced `Fail`
    /// disables the configured `skip` liveness contract for this seam).
    pub async fn upload_only_fail_closed(
        cfg: &StateConfig,
        state_path: &Path,
        reason: &str,
    ) -> Result<(), StateSyncError> {
        if matches!(cfg.backend, StateBackend::Local) {
            return Ok(());
        }
        debug!(reason, "fail-closed ledger upload (half-seam)");
        let upload_cfg = StateConfig {
            on_upload_failure: StateUploadFailureMode::Fail,
            ..cfg.clone()
        };
        upload_state(&upload_cfg, state_path).await
    }
}

impl Drop for RemoteStateSession {
    fn drop(&mut self) {
        if self.finalized {
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
/// - **Absent** — no remote object exists for this key. For a REMOTE backend the
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
        download_state_inner(config, local_path, &remote_key).await?;
        return Ok(StateAuthority::Authoritative);
    }

    // 1. Download the remote into a STAGING file (no lock). `local_path` is left
    //    untouched; `Absent` leaves the staging file unwritten.
    let scratch_dir = sibling_scratch_dir(local_path);
    let staged = unique_scratch_path(&scratch_dir, "download");
    let outcome = match download_state_inner(config, &staged, &remote_key).await {
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
    let result = publish_merged(&staged, local_path, outcome).await;

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
) -> Result<(), StateSyncError> {
    let local_exists = local_path.exists();
    let tables = crate::state::LOCAL_ONLY_TABLE_NAMES;
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
            redb_op_retry(|| copy_named_tables(local_path, staged, tables)).await?;
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

/// Delete the named tables from the redb at `dst` (leaving every other table
/// untouched), so they reappear empty on the next `StateStore::open`. Used to
/// scrub any remote-carried local-only rows on a `Restored` download with no
/// prior local file (finding 6a).
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
        txn.delete_table(def)
            .map_err(|e| io("redb delete_table for local-only clear", &e))?;
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
/// `src` clears the corresponding table in `dst` (delete + no re-insert). If a
/// future local-only table used a different key/value type, opening it here
/// would surface a redb type error rather than silently mis-copying.
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
            // A local-only table absent from `src` is a legitimate no-op — the
            // destination table stays deleted (cleared) and is recreated empty
            // on the next `StateStore::open`.
            Err(redb::TableError::TableDoesNotExist(_)) => {}
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
            )
            .await
        }
        StateBackend::Valkey => download_from_valkey(config, dest_path, remote_key).await,
        StateBackend::Tiered => {
            // Try Valkey first (fast), fall back to S3 (durable).
            //
            // KNOWN LIMITATION — tiered cache coherence (finding 3): on the READ
            // side a genuine Valkey HIT short-circuits the durable S3 tier below.
            // If a freeze/decision was written to S3 but Valkey holds a STALE
            // cached snapshot (or one written before the freeze), a policy gate
            // reading through this path can see the stale Valkey copy and MISS
            // the freeze. This is NOT fixed here; a correct fix needs
            // cache-invalidation-on-write (bust/refresh the Valkey key when a
            // governance ledger is written) or reading the durable tier directly
            // for policy gates. Tracked as follow-up.
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
            match Box::pin(download_state_inner(&valkey_config, dest_path, remote_key)).await {
                Ok(DownloadOutcome::Restored) => {
                    debug!("State restored from Valkey");
                    Ok(DownloadOutcome::Restored)
                }
                Ok(DownloadOutcome::Absent) => {
                    debug!("Valkey miss, trying S3");
                    Box::pin(download_state_inner(&s3_config, dest_path, remote_key)).await
                }
                Err(e) => {
                    debug!(error = %e, "Valkey error, trying S3");
                    Box::pin(download_state_inner(&s3_config, dest_path, remote_key)).await
                }
            }
        }
    }
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
/// Tables listed in [`crate::state::LOCAL_ONLY_TABLE_NAMES`] are filtered
/// out of the remote copy by default — the schema cache is wired here so
/// fresh clones don't inherit another machine's stale types. Use
/// [`upload_state_with_excluded_tables`] to override the default when
/// `[cache.schemas] replicate = true` is configured.
pub async fn upload_state(config: &StateConfig, local_path: &Path) -> Result<(), StateSyncError> {
    upload_state_with_excluded_tables(config, local_path, crate::state::LOCAL_ONLY_TABLE_NAMES)
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
    upload_state(config, local_path).await
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
    let scratch = std::env::temp_dir().join(format!(
        "rocky-state-upload-{}-{}.redb",
        std::process::id(),
        nanos
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
            // KNOWN LIMITATION — tiered cache coherence (finding 3): a Valkey
            // upload failure here is SWALLOWED (only the durable S3 leg below is
            // required), even under `on_upload_failure = Fail`. Combined with the
            // read-side short-circuit (a Valkey HIT skips S3 — see
            // `download_state_inner`'s Tiered arm), this means a freeze can be
            // durably written to S3 while a STALE Valkey cache entry survives and
            // shadows it on the next policy-gate read. This is NOT fixed here; a
            // correct fix needs cache-invalidation-on-write (bust/refresh the
            // Valkey key when a governance ledger is written) or reading the
            // durable tier directly for policy gates. Tracked as follow-up.
            if let Err(e) = Box::pin(dispatch_upload(&valkey_config, local_path, remote_key)).await
            {
                warn!(error = %e, "Valkey upload failed (non-fatal, S3 is durable)");
            }

            // S3 second (durable, required)
            Box::pin(dispatch_upload(&s3_config, local_path, remote_key)).await
        }
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
                    provider.download_file(&key, dest_path).await?;
                    info!(
                        size = dest_path.metadata().map(|m| m.len()).unwrap_or(0),
                        outcome = "ok",
                        "state restored from object store"
                    );
                    Ok(DownloadOutcome::Restored)
                }
                Ok(false) => {
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
) -> Result<DownloadOutcome, StateSyncError> {
    // Test seam: force a Valkey MISS without a live Valkey peer, so the tiered
    // fall-through-to-S3 path (and the "stale local file is not a hit" fix) can
    // be exercised deterministically. See [`test_support::arm_valkey_miss_fault`].
    #[cfg(any(test, feature = "test-support"))]
    if test_support::take_valkey_miss_fault() {
        return Ok(DownloadOutcome::Absent);
    }
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
    // Qualify by schema version: `rocky:state:v9:state.redb`, not
    // `rocky:state:state.redb`, so a v7 reader and a v9 writer never share a key.
    // `remote_key` is threaded from `download_state` so a staging `dest_path`
    // does not change which key we read.
    let key = valkey_state_key(&prefix, remote_key);
    let local_path_owned = dest_path.to_path_buf();
    let timeout = transfer_timeout(config);

    let span = info_span!("state.download", backend = "valkey");
    async move {
        info!(key = %key, "downloading state from Valkey");
        with_transfer_timeout(timeout, async move {
            let key_for_task = key.clone();
            let local_for_task = local_path_owned.clone();
            let join =
                tokio::task::spawn_blocking(move || -> Result<DownloadOutcome, StateSyncError> {
                    let client = redis::Client::open(url)
                        .map_err(|e| StateSyncError::Valkey(e.to_string()))?;
                    let mut conn = client
                        .get_connection()
                        .map_err(|e| StateSyncError::Valkey(e.to_string()))?;

                    let data: Option<Vec<u8>> = redis::cmd("GET")
                        .arg(&key_for_task)
                        .query(&mut conn)
                        .map_err(|e| StateSyncError::Valkey(e.to_string()))?;

                    match data {
                        Some(bytes) => {
                            std::fs::write(&local_for_task, bytes)?;
                            let size = std::fs::metadata(&local_for_task)
                                .map(|m| m.len())
                                .unwrap_or(0);
                            info!(size, outcome = "ok", "state restored from Valkey");
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
    .instrument(span)
    .await
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

/// Build a per-call probe key under the configured prefix. Uses PID +
/// nanosecond epoch so concurrent doctor invocations don't collide.
fn probe_key() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("doctor-probe-{}-{}.marker", std::process::id(), nanos)
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
        | StateSyncError::MissingConfig(..)
        | StateSyncError::CircuitOpen { .. }
        | StateSyncError::RetryBudgetExhausted { .. } => false,
    }
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
        assert!(download_state(&config, &path).await.is_ok());
    }

    #[tokio::test]
    async fn test_local_upload_noop() {
        let config = StateConfig::default();
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        assert!(upload_state(&config, &path).await.is_ok());
    }

    #[tokio::test]
    async fn test_s3_missing_bucket() {
        let config = StateConfig {
            backend: StateBackend::S3,
            s3_bucket: None,
            ..Default::default()
        };
        let dir = TempDir::new().unwrap();
        let result = download_state(&config, &dir.path().join("state.redb")).await;
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
        let result = download_state(&config, &dir.path().join("state.redb")).await;
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
        let result = download_state(&config, &dir.path().join("state.redb")).await;
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
        upload_state(&config, &local).await.unwrap();

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
        assert!(upload_state(&config, &src).await.is_ok());
    }

    #[tokio::test]
    async fn upload_state_unless_recreated_suppresses_upload_when_recreated() {
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
            upload_state_unless_recreated(true, &config, &src)
                .await
                .is_ok(),
            "a store recreated for forward-incompat must skip the end-of-run upload"
        );

        // Normal store: the upload runs and hits the (deliberately broken) S3
        // dispatch — proving the gate let a real upload attempt through.
        let err = upload_state_unless_recreated(false, &config, &src)
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
        let result = download_state(&config, &local).await;
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
        let result = download_state(&config, &local).await;
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
        let result = download_state(&config, &local).await;
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
        let result = download_state(&config, &local).await;
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
        let result = download_state(&config, &local).await;
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
        let result = download_state(&config, &dir.path().join("state.redb")).await;
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
        let outcome = download_state_inner(&config, &local, &remote_state_key(&local))
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
        let outcome = download_state_inner(&config, &local, &remote_state_key(&local))
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
        let authority = download_state(&config, &local)
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
        let authority = download_state(&config, &local)
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
            upload_state(&skip, &src).await.is_ok(),
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
                upload_state(&fail, &src).await,
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
        let result = download_state(&config, &local).await;
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
        let authority = download_state(&config, &local)
            .await
            .expect("restored download should succeed");
        test_support::clear();
        assert_eq!(authority, StateAuthority::Authoritative);

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
        let result = upload_state(&config, &local).await;
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

        let result = download_state(&config, &local).await;
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
        let authority = download_state(&config, &local)
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
        let result = upload_state(&config, &local).await;
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
                })
                .unwrap();
            store
                .record_policy_decision(&PolicyDecisionRecord {
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
                })
                .unwrap();
            drop(store);
        }

        let config = StateConfig {
            backend: StateBackend::S3,
            s3_bucket: Some("bucket".into()),
            ..Default::default()
        };
        upload_state(&config, &pod_a).await.unwrap();

        // Pod B: a fresh pod downloads the remote state.
        let dir_b = TempDir::new().unwrap();
        let pod_b = dir_b.path().join(".rocky-state.redb");
        assert_eq!(
            download_state(&config, &pod_b).await.unwrap(),
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
    fn install_counting_provider_global()
    -> (crate::fault_store::FaultHandle, test_support::GlobalOverrideGuard) {
        let (store, faults) = crate::fault_store::FaultingStore::wrap(std::sync::Arc::new(
            object_store::memory::InMemory::new(),
        ));
        let guard =
            test_support::install_global(ObjectStoreProvider::from_store(store, "s3", "bucket", ""));
        (faults, guard)
    }

    /// Seed a real (empty) redb state file at `path` so the upload path has a
    /// file to strip/dispatch.
    fn seed_state_file(path: &Path) {
        let store = StateStore::open(path).expect("seed session state file");
        drop(store);
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
        );
        drop(session);
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
        let mut durable = RemoteStateSession::new(&skip_cfg, &local, FinalizeDurability::Durable);
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
            RemoteStateSession::new(&skip_cfg, &local, FinalizeDurability::ConfigDefault);
        let _ = config_default.acquire().await.unwrap();
        config_default
            .finalize()
            .await
            .expect("ConfigDefault must honor the configured skip (warn-and-Ok)");
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
        );
        let _ = session.acquire().await.unwrap();
        session.start_periodic_uploader(Arc::downgrade(&store), Duration::from_millis(40));

        // First dirty tick uploads.
        let uploaded = poll_until(
            || faults.count(crate::fault_store::FaultOp::Put) >= 1,
            Duration::from_secs(5),
        )
        .await;
        assert!(uploaded, "a dirty tick must upload the snapshot");
        let after_first = faults.count(crate::fault_store::FaultOp::Put);

        // No new writes → subsequent ticks are clean and skip (zero new Puts).
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            faults.count(crate::fault_store::FaultOp::Put),
            after_first,
            "clean ticks must skip — the dirty-gate spends zero I/O when the epoch is unchanged"
        );

        // A fresh write re-dirties the epoch → the uploader picks it up.
        store.set_watermark("c.s.u", &wm_now()).unwrap();
        let re_uploaded = poll_until(
            || faults.count(crate::fault_store::FaultOp::Put) > after_first,
            Duration::from_secs(5),
        )
        .await;
        assert!(re_uploaded, "a new epoch bump must trigger another upload");

        session.abandon("test complete").await;
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
        );
        let _ = session.acquire().await.unwrap();
        session.start_periodic_uploader(Arc::downgrade(&store), Duration::from_millis(30));

        // Let at least one upload happen so a tick has genuinely run.
        let uploaded = poll_until(
            || faults.count(crate::fault_store::FaultOp::Put) >= 1,
            Duration::from_secs(5),
        )
        .await;
        assert!(
            uploaded,
            "the uploader must run at least one tick before stop"
        );

        // Cooperative drain + join.
        session.stop_periodic().await;
        let after_stop = faults.count(crate::fault_store::FaultOp::Put);

        // No further upload after the drain returns.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            faults.count(crate::fault_store::FaultOp::Put),
            after_stop,
            "no late upload may fire after a cooperative stop"
        );

        // No scratch snapshot leaked for this process (the ScratchGuard removed
        // every temp file in-task, uncancelled).
        let pid_prefix = format!("rocky-state-snapshot-{}-", std::process::id());
        let leaked: Vec<String> = std::fs::read_dir(std::env::temp_dir())
            .expect("read temp dir")
            .filter_map(Result::ok)
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with(&pid_prefix))
            .collect();
        assert!(
            leaked.is_empty(),
            "cooperative drain must leave no scratch snapshot behind, found: {leaked:?}"
        );

        session.abandon("test complete").await;
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
        );
        let _ = session.acquire().await.unwrap();
        session.start_periodic_uploader(Arc::downgrade(&shared_state), Duration::from_millis(20));

        // Ensure a tick actually ran (so the uploader genuinely upgraded the
        // Weak at least once — the case a strong clone would have made fatal).
        let ticked = poll_until(
            || faults.count(crate::fault_store::FaultOp::Put) >= 1,
            Duration::from_secs(5),
        )
        .await;
        assert!(
            ticked,
            "the uploader must run a tick so the Weak is upgraded"
        );

        // Run tail: drain the periodic, then recover the owned store.
        session.stop_periodic().await;
        let recovered = Arc::try_unwrap(shared_state).ok();
        assert!(
            recovered.is_some(),
            "Arc::try_unwrap must succeed — the Weak periodic handle never blocks the run tail"
        );
        let store = recovered.unwrap();

        // The recovered store is real (not a no-op `None`): a terminal write
        // persists.
        store.set_watermark("c.s.terminal", &wm_now()).unwrap();
        assert!(
            store.get_watermark("c.s.terminal").unwrap().is_some(),
            "the terminal write must persist on the recovered owned store"
        );

        // Drop-before-finalize: dropping the owned store frees the writer lock.
        drop(store);
        session.abandon("test complete").await;
    }
}
