//! The scheduler's jobs-model spawner.
//!
//! Scheduler-launched runs go through the SAME persisted jobs model as
//! `POST /api/v1/jobs/run` (F11 §4.3): this [`Spawner`] records a durable
//! [`PersistedJob`] before and after each run, so `GET /api/v1/jobs/{id}`
//! reports scheduler runs like API-submitted ones and a killed-and-restarted
//! server reports honest status (the startup sweep marks a stranded `running`
//! job `failed`).
//!
//! Execution itself delegates to a drain-aware [`SubprocessSpawner`] — the run's
//! full record (materializations, checks, drift) is written by the child into
//! `run_history`, joined to this job by `submission_id`. To make that join
//! trivial, the **job id IS the `submission_id`**.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rocky_core::schedule::{Drain, RunOutcome, SpawnRequest, Spawner, SubprocessSpawner};
use rocky_core::state::PersistedJob;
use rocky_server::state::ServerState;

use crate::api::{job_state_str, persist_job, state_path_for};
use crate::output::JobState;

/// The `principal` attributed to scheduler-launched jobs. Advisory attribution
/// only (like every principal under the v1 auth ceiling, F11 §4.4).
pub(crate) const SCHEDULER_PRINCIPAL: &str = "scheduler";

/// A [`Spawner`] that brackets each run with a durable [`PersistedJob`] and
/// delegates execution to a drain-aware [`SubprocessSpawner`].
pub struct JobsModelSpawner {
    inner: SubprocessSpawner,
    state: Arc<ServerState>,
    state_path: PathBuf,
}

impl JobsModelSpawner {
    /// Construct the spawner. `drain`/`drain_timeout` are handed to the inner
    /// [`SubprocessSpawner`] so a server shutdown bounds a running child's wait.
    pub fn new(state: Arc<ServerState>, drain: Drain, drain_timeout: Duration) -> Self {
        let state_path = state_path_for(&state);
        Self {
            inner: SubprocessSpawner::with_drain(drain, drain_timeout),
            state,
            state_path,
        }
    }

    /// Upsert the in-memory registry and best-effort persist to redb. Persistence
    /// under lock contention is non-fatal — the in-memory registry is
    /// authoritative for the live session and embedders reconcile via `/runs`.
    async fn record(&self, job: PersistedJob) {
        self.state.jobs.upsert(job.clone()).await;
        if let Err(e) = persist_job(self.state_path.clone(), job.clone()).await {
            tracing::warn!(error = %e, job_id = %job.job_id,
                "could not persist scheduler job record; in-memory only until it settles");
        }
    }
}

#[async_trait]
impl Spawner for JobsModelSpawner {
    async fn run(&self, request: &SpawnRequest) -> RunOutcome {
        let started = chrono::Utc::now().to_rfc3339();
        // The job id IS the submission id: `GET /api/v1/jobs/{submission_id}`
        // and the run's `run_history` entry share one key.
        let mut record = PersistedJob {
            job_id: request.submission_id.clone(),
            kind: "run".to_string(),
            state: job_state_str(JobState::Running).to_string(),
            submitted_at: started.clone(),
            started_at: Some(started),
            finished_at: None,
            principal: Some(SCHEDULER_PRINCIPAL.to_string()),
            error: None,
            result: None,
        };
        // Record `running` BEFORE spawning so a crash mid-run still reports honest
        // status on restart. The reconciler has already released the state store
        // (it closes around every spawn), so this open never self-contends.
        self.record(record.clone()).await;

        let outcome = self.inner.run(request).await;

        // Binary success/failure for the job model; the precise outcome
        // (partial exit 2 vs failure) lives on the run-history record.
        let (state, error) = if outcome.exit_code == 0 {
            (JobState::Succeeded, None)
        } else {
            (
                JobState::Failed,
                Some(format!("run exited with code {}", outcome.exit_code)),
            )
        };
        record.state = job_state_str(state).to_string();
        record.finished_at = Some(chrono::Utc::now().to_rfc3339());
        record.error = error;
        self.record(record).await;

        outcome
    }
}
