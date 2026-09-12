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
        // The tick has released the store, and its gate permit with it, for
        // the child's window (see `PhaseStore::close`), so taking the gate here
        // waits only for the server's reads, never for the tick itself.
        if let Err(e) = persist_job(&self.state, self.state_path.clone(), job.clone()).await {
            tracing::warn!(error = %e, job_id = %job.job_id,
                "could not persist scheduler job record; in-memory only until it settles");
        }
    }
}

/// Apply a finished run's outcome to its job record.
///
/// Pure, and extracted for the same reason [`crate::api::scrub_job_outcome`]
/// is: `run` delegates to a concrete `SubprocessSpawner` which spawns
/// `current_exe` — the test harness under `cargo test` — so nothing inside
/// `run` can be reached by a unit test. Without a seam, the fact that THIS
/// writer stamps `redaction_version` would rest on my having noticed it,
/// which is how it came to be missing in the first place.
///
/// The stamp matters more here than the scrub. An unstamped record reads as
/// pre-redaction forever, so `GET /api/v1/jobs/{id}` withholds its error on
/// every scheduler-launched run — a silent loss of diagnostics that looks
/// exactly like the legacy rule working as designed.
fn finish_scheduler_job(mut record: PersistedJob, exit_code: i32) -> PersistedJob {
    // Binary success/failure for the job model; the precise outcome
    // (partial exit 2 vs failure) lives on the run-history record.
    let (state, error) = if exit_code == 0 {
        (JobState::Succeeded, None)
    } else {
        (
            JobState::Failed,
            Some(format!("run exited with code {exit_code}")),
        )
    };
    record.state = job_state_str(state).to_string();
    record.finished_at = Some(chrono::Utc::now().to_rfc3339());
    // Through the same seam as the API's spawn path. This message is a fixed
    // template with an exit code, so nothing can be in it today — routed
    // anyway, because "this particular string is safe" is an argument that
    // stops being true when someone edits the string.
    let (_, error, version) = crate::api::scrub_job_outcome(None, error);
    record.error = error;
    record.redaction_version = Some(version);
    record
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
            // The SECOND writer of a job record. Without this stamp every
            // scheduler-run job would read as pre-redaction forever, and
            // `GET /api/v1/jobs/{id}` would withhold its error for a record
            // this binary wrote (#1897).
            redaction_version: Some(rocky_core::state::CURRENT_REDACTION_VERSION),
        };
        // Record `running` BEFORE spawning so a crash mid-run still reports honest
        // status on restart. The reconciler has already released the state store
        // (it closes around every spawn), so this open never self-contends.
        self.record(record.clone()).await;

        let outcome = self.inner.run(request).await;

        record = finish_scheduler_job(record, outcome.exit_code);
        self.record(record).await;

        outcome
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn running_record() -> PersistedJob {
        PersistedJob {
            job_id: "sched-1".to_string(),
            kind: "run".to_string(),
            state: job_state_str(JobState::Running).to_string(),
            submitted_at: "2026-09-12T00:00:00Z".to_string(),
            started_at: Some("2026-09-12T00:00:00Z".to_string()),
            finished_at: None,
            principal: Some(SCHEDULER_PRINCIPAL.to_string()),
            error: None,
            result: None,
            redaction_version: Some(rocky_core::state::CURRENT_REDACTION_VERSION),
        }
    }

    /// #1897. The writer nobody was looking for stamps its records.
    ///
    /// Requested by a reviewer, and the distinction is the point: the API
    /// tests prove the READER honours a stamp on a record built by hand. They
    /// say nothing about whether THIS writer applies one. Unstamped, every
    /// scheduler-launched job would report as pre-redaction forever.
    #[test]
    fn a_failed_scheduler_job_is_stamped_and_returns_its_error() {
        let done = finish_scheduler_job(running_record(), 1);

        assert_eq!(
            done.redaction_version,
            Some(rocky_core::state::CURRENT_REDACTION_VERSION),
            "an unstamped record reads as pre-redaction forever"
        );
        assert!(
            !done.redaction_is_legacy(),
            "a record this binary just wrote must not read as legacy"
        );
        assert_eq!(
            done.error.as_deref(),
            Some("run exited with code 1"),
            "the error must survive the scrub — it carries no config value"
        );
        assert_eq!(done.state, "failed");
    }

    /// A clean run stamps too. The stamp is not conditional on there being
    /// an error to protect.
    #[test]
    fn a_succeeding_scheduler_job_is_stamped_as_well() {
        let done = finish_scheduler_job(running_record(), 0);
        assert!(!done.redaction_is_legacy());
        assert_eq!(done.error, None);
        assert_eq!(done.state, "succeeded");
    }
}
