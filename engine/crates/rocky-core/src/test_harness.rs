//! Deterministic cross-pod remote-state test harness.
//!
//! Compiled only for the crate's own tests or under the `test-support`
//! feature — the `rocky` binary never enables either, so nothing here can
//! reach a production build.
//!
//! [`CrossPodHarness`] simulates two ephemeral pods (isolated temp dirs, each
//! with its own local state file and `StateConfig`) sharing ONE remote store:
//! a [`FaultingStore`]-decorated in-memory backend masquerading as S3,
//! installed as the process-global provider override so the REAL
//! [`state_sync::download_state`] / [`state_sync::upload_state`] decision
//! paths run against it — from any thread, without credentials, byte-for-byte
//! deterministic.
//!
//! The override is process-global, so tests that build a harness MUST hold
//! [`crate::state_sync::remote_testing::serial_guard`] for their whole
//! duration.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::config::{StateBackend, StateConfig, StateUploadFailureMode};
use crate::fault_store::{FaultHandle, FaultingStore};
use crate::object_store::ObjectStoreProvider;
use crate::state::StateStore;
use crate::state_sync::remote_testing::{GlobalOverrideGuard, install_global};
use crate::state_sync::{self, StateSyncError};

/// One simulated ephemeral pod: an isolated temp dir, a pod-local state file
/// path inside it, and a `StateConfig` pointing at the harness's shared
/// remote.
#[derive(Debug)]
pub struct Pod {
    /// Backing temp dir — owned so it lives (and is cleaned up) with the pod.
    pub dir: tempfile::TempDir,
    /// The pod-local state file path (`<dir>/.rocky-state.redb`).
    pub state_path: PathBuf,
    /// The pod's state config (remote backend resolving to the shared store).
    pub cfg: StateConfig,
}

impl Pod {
    fn s3_like() -> Self {
        let dir = tempfile::tempdir().expect("create pod temp dir");
        let state_path = dir.path().join(".rocky-state.redb");
        let cfg = StateConfig {
            backend: StateBackend::S3,
            s3_bucket: Some("test".into()),
            // A test rig must fail loudly: the production default (`Skip`)
            // swallows exhausted-retry upload failures into `Ok`.
            on_upload_failure: StateUploadFailureMode::Fail,
            ..StateConfig::default()
        };
        Self {
            dir,
            state_path,
            cfg,
        }
    }
}

/// Two pods sharing one fault-injectable in-memory "S3".
///
/// Dropping the harness drops the held [`GlobalOverrideGuard`], clearing the
/// process-global provider override.
#[derive(Debug)]
pub struct CrossPodHarness {
    /// The shared provider both pods' state sync resolves to. Handy for
    /// direct object-level assertions (`exists` / `get` / `list`).
    pub provider: ObjectStoreProvider,
    /// Fault/counter control for the shared store (see [`FaultingStore`]).
    pub faults: FaultHandle,
    pub pod_a: Pod,
    pub pod_b: Pod,
    _guard: GlobalOverrideGuard,
}

impl CrossPodHarness {
    /// Build a two-pod harness over a shared fault-injecting in-memory store
    /// masquerading as S3 (`backend = "s3"`, bucket `"test"`), and install it
    /// as the process-global provider override.
    ///
    /// Callers sharing a test binary must hold
    /// [`crate::state_sync::remote_testing::serial_guard`] for the duration
    /// of the test — the override is process-global.
    pub fn new_s3_like() -> Self {
        let (store, faults) = FaultingStore::wrap(Arc::new(object_store::memory::InMemory::new()));
        let provider = ObjectStoreProvider::from_store(store, "s3", "test", "");
        let guard = install_global(provider.clone());
        Self {
            provider,
            faults,
            pod_a: Pod::s3_like(),
            pod_b: Pod::s3_like(),
            _guard: guard,
        }
    }

    /// Run the real [`state_sync::download_state`] for `pod` (staging file,
    /// writer-lock publish serialization, local-only merge — the whole path).
    pub async fn download(&self, pod: &Pod) -> Result<(), StateSyncError> {
        state_sync::download_state(&pod.cfg, &pod.state_path).await
    }

    /// Run the real [`state_sync::upload_state`] for `pod` (local-only strip,
    /// dispatch, schema-qualified remote key derivation — the whole path).
    pub async fn upload(&self, pod: &Pod) -> Result<(), StateSyncError> {
        state_sync::upload_state(&pod.cfg, &pod.state_path).await
    }

    /// Open `pod`'s local state store — acquiring the advisory writer lock,
    /// exactly like a live `rocky run` does. Drop the store to release it.
    pub fn open_store(&self, pod: &Pod) -> StateStore {
        StateStore::open(&pod.state_path).expect("open pod state store")
    }
}

/// Run `f` while a [`StateStore`] is held open on `state_path` — holding the
/// same advisory writer lock a live `rocky run` holds during execution — then
/// release the lock and return `f`'s output.
///
/// The store is opened (and the lock acquired) BEFORE `f` is polled, so a
/// concurrent download/publish driven from inside `f` is guaranteed to
/// contend with the held writer.
pub async fn with_held_writer<T>(state_path: &Path, f: impl Future<Output = T>) -> T {
    let held = StateStore::open(state_path).expect("open held-writer state store");
    let out = f.await;
    drop(held);
    out
}
