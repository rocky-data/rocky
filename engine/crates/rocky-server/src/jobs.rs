//! In-process job bookkeeping for the `rocky serve` HTTP job model.
//!
//! Two small pieces, held on [`crate::state::ServerState`] and driven by the
//! job handlers in `rocky_cli::api`:
//!
//! - [`MutationPermit`] — the app-level single-mutating-job guard. A second
//!   `run`/`apply` submission while one holds the permit is rejected with
//!   `409 mutation_in_progress` (carrying the holder's `job_id`) rather than
//!   colliding on the redb flock. `plan` is non-mutating and never takes it.
//!   This is best-effort by design: the permit lives in memory and does **not**
//!   survive a sidecar restart, at which point the redb advisory lock is the
//!   correctness backstop (see the mutation-serialization design).
//!
//! - [`JobRegistry`] — a fast in-memory cache of [`PersistedJob`] records so
//!   `GET /api/v1/jobs/{id}` answers without touching redb on the hot path.
//!   The durable copy lives in the `jobs` state table; the registry is
//!   repopulated lazily (per id) from redb after a restart.
//!
//! There is no queue, cron, or scheduler here on purpose — one mutating job at
//! a time is the whole contract; the embedder orchestrates.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::RwLock;

use rocky_core::state::PersistedJob;

/// The single-mutating-job permit, keyed conceptually by the sidecar's one
/// resolved state path (one sidecar binds one project config, so a single slot
/// is the correct granularity — cross-project isolation is one-sidecar-per-tenant,
/// not namespacing inside one sidecar).
///
/// [`try_acquire`](Self::try_acquire) hands out a [`PermitGuard`]; the slot is
/// freed when that guard is dropped (i.e. when the job's background task ends).
#[derive(Clone, Default)]
pub struct MutationPermit {
    /// `Some(job_id)` while a mutating job holds the permit; `None` when free.
    slot: Arc<Mutex<Option<String>>>,
}

impl MutationPermit {
    /// Create an unheld permit.
    pub fn new() -> Self {
        Self::default()
    }

    /// Try to take the permit for `job_id`.
    ///
    /// # Errors
    ///
    /// Returns the `job_id` of the job **currently holding** the permit when it
    /// is already taken — the handler surfaces that as the
    /// `409 mutation_in_progress` `running_job_id`.
    pub fn try_acquire(&self, job_id: &str) -> Result<PermitGuard, String> {
        let mut slot = self.slot.lock().expect("mutation permit mutex poisoned");
        if let Some(held) = slot.as_ref() {
            return Err(held.clone());
        }
        *slot = Some(job_id.to_string());
        Ok(PermitGuard {
            slot: Arc::clone(&self.slot),
        })
    }

    /// The `job_id` currently holding the permit, if any.
    pub fn running_job(&self) -> Option<String> {
        self.slot
            .lock()
            .expect("mutation permit mutex poisoned")
            .clone()
    }
}

/// RAII release for [`MutationPermit`]. Dropping it frees the slot.
///
/// Held for the lifetime of a mutating job's background task; the task moves the
/// guard in and it drops when the task returns, releasing the permit.
#[derive(Debug)]
pub struct PermitGuard {
    slot: Arc<Mutex<Option<String>>>,
}

impl Drop for PermitGuard {
    fn drop(&mut self) {
        *self.slot.lock().expect("mutation permit mutex poisoned") = None;
    }
}

/// In-memory cache of job records fronting the durable `jobs` state table.
#[derive(Clone, Default)]
pub struct JobRegistry {
    inner: Arc<RwLock<HashMap<String, PersistedJob>>>,
}

impl JobRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or replace a record.
    pub async fn upsert(&self, job: PersistedJob) {
        self.inner.write().await.insert(job.job_id.clone(), job);
    }

    /// Fetch a record by `job_id`.
    pub async fn get(&self, job_id: &str) -> Option<PersistedJob> {
        self.inner.read().await.get(job_id).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permit_admits_one_and_reports_holder() {
        let permit = MutationPermit::new();
        let guard = permit.try_acquire("job-a").expect("first acquire succeeds");
        assert_eq!(permit.running_job().as_deref(), Some("job-a"));

        // A second acquire is rejected with the current holder's id.
        let err = permit
            .try_acquire("job-b")
            .expect_err("second acquire is rejected");
        assert_eq!(err, "job-a");

        // Releasing the guard frees the slot for the next mutating job.
        drop(guard);
        assert_eq!(permit.running_job(), None);
        let _g2 = permit
            .try_acquire("job-b")
            .expect("acquire after release succeeds");
        assert_eq!(permit.running_job().as_deref(), Some("job-b"));
    }

    #[tokio::test]
    async fn registry_upsert_and_get() {
        let reg = JobRegistry::new();
        assert!(reg.get("j").await.is_none());
        reg.upsert(PersistedJob {
            job_id: "j".to_string(),
            kind: "run".to_string(),
            state: "running".to_string(),
            submitted_at: "t".to_string(),
            started_at: None,
            finished_at: None,
            principal: None,
            error: None,
            result: None,
        })
        .await;
        assert_eq!(reg.get("j").await.unwrap().state, "running");

        // Upsert replaces under the same key.
        let mut done = reg.get("j").await.unwrap();
        done.state = "succeeded".to_string();
        reg.upsert(done).await;
        assert_eq!(reg.get("j").await.unwrap().state, "succeeded");
    }
}
