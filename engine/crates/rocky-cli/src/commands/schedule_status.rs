//! Backing read for `GET /api/v1/schedule` — a scheduler status snapshot.
//!
//! A pure read of config + stored cursors + claims + the tick lock. It
//! deliberately does **not** evaluate demand: `rocky tick --dry-run` is the
//! evaluation, and duplicating it here would mean a status request could do
//! history scans and cron searches per pipeline, and could disagree with the
//! reconciler about what is due.
//!
//! The scheduled set comes from [`resolve_schedules_detailed`], the
//! reconciler's own resolution — not from iterating `config.pipelines` and
//! filtering on `schedule()`. Those two disagree: the latter includes load
//! pipelines, which the reconciler refuses to schedule, so it would report a
//! pipeline as scheduled that can never fire.

use std::path::Path;

use chrono::{DateTime, Utc};
use rocky_core::config::{RockyConfig, load_rocky_config};
use rocky_core::schedule::{
    Catchup, ClaimState, LOCK_TAKEOVER_AFTER, Throttle, next_projected_fire, parse_claim_key,
    probe_tick_lock, resolve_schedules_detailed,
};
use rocky_core::state::StateStore;

use crate::commands::tick::build_member_budgets;
use crate::output::{
    ScheduleClaimStatus, ScheduleLockStatus, SchedulePipelineStatus, ScheduleStatusCounts,
    ScheduleStatusOutput, ScheduleThrottleKind, ScheduleThrottleStatus, TickLockState,
};

/// Why a schedule-status read failed.
#[derive(Debug, thiserror::Error)]
pub enum ScheduleStatusError {
    /// `rocky.toml` could not be read or parsed. Reported distinctly from a
    /// state-store failure so the caller can answer with the right status: a
    /// config the engine cannot read is not a transient busy condition, and
    /// answering `200` with an empty pipeline list would claim "nothing is
    /// scheduled" when the truth is "we cannot tell".
    #[error("{0}")]
    ConfigInvalid(String),

    /// The state store could not be opened or read. Distinct from
    /// `ConfigInvalid` because a locked store is retryable.
    #[error(transparent)]
    State(#[from] anyhow::Error),
}

/// Build the scheduler status snapshot.
pub fn schedule_status_output(
    config_path: &Path,
    state_path: &Path,
    rocky_dir: &Path,
    now: DateTime<Utc>,
) -> Result<ScheduleStatusOutput, ScheduleStatusError> {
    let config = load_rocky_config(config_path)
        .map_err(|e| ScheduleStatusError::ConfigInvalid(e.to_string()))?;

    let tick_lock = probe_lock(rocky_dir);

    // An ABSENT state file is normal (no tick has run yet) and yields empty
    // cursors; a file that EXISTS but cannot be opened is a real fault and must
    // not be flattened into "nothing scheduled has ever run".
    let store = match StateStore::open_read_only(state_path) {
        Ok(store) => Some(store),
        Err(e) => {
            if state_path.exists() {
                return Err(ScheduleStatusError::State(e.into()));
            }
            None
        }
    };

    let claims_by_pipeline = collect_claims(store.as_ref())?;
    let pipelines = build_pipelines(
        &config,
        config_path,
        store.as_ref(),
        &claims_by_pipeline,
        now,
    );
    let counts = count(&pipelines, now);

    Ok(ScheduleStatusOutput {
        now,
        timezone: config.schedule.timezone.clone(),
        tick_lock,
        pipelines,
        counts,
    })
}

fn probe_lock(rocky_dir: &Path) -> ScheduleLockStatus {
    match probe_tick_lock(rocky_dir) {
        Ok(Some(probe)) => {
            let age_secs = probe.heartbeat_age.map(|d| d.as_secs());
            let state = if !probe.held {
                TickLockState::Free
            } else if probe
                .heartbeat_age
                .is_some_and(|age| age > LOCK_TAKEOVER_AFTER)
            {
                TickLockState::Wedged
            } else {
                TickLockState::Held
            };
            ScheduleLockStatus {
                state,
                heartbeat_age_seconds: age_secs,
            }
        }
        // No lock file, or the probe itself failed. Both mean "we observed no
        // reconciler"; neither is grounds for failing the whole request, since
        // the cursors below are the load-bearing signal.
        _ => ScheduleLockStatus {
            state: TickLockState::Never,
            heartbeat_age_seconds: None,
        },
    }
}

type ClaimsByPipeline = std::collections::BTreeMap<String, Vec<ScheduleClaimStatus>>;

fn collect_claims(store: Option<&StateStore>) -> Result<ClaimsByPipeline, ScheduleStatusError> {
    let mut out: ClaimsByPipeline = ClaimsByPipeline::new();
    let Some(store) = store else {
        return Ok(out);
    };
    for (key, claim) in store.list_schedule_claims().map_err(anyhow::Error::from)? {
        // A key the parser cannot interpret is skipped rather than guessed at,
        // matching the orphan sweep.
        let Some((pipeline, source, logical_ts)) = parse_claim_key(&key) else {
            continue;
        };
        let (state, outcome) = match &claim.state {
            ClaimState::Submitted => ("submitted", None),
            ClaimState::Released => ("released", None),
            ClaimState::Completed { outcome } => {
                ("completed", Some(format!("{outcome:?}").to_lowercase()))
            }
        };
        out.entry(pipeline).or_default().push(ScheduleClaimStatus {
            source: source.as_str().to_string(),
            state: state.to_string(),
            outcome,
            submission_id: claim.submission_id.clone(),
            logical_ts,
            attempts: claim.attempts,
        });
    }
    // Newest occurrence first. Webhook claims carry no instant, so they sort
    // last rather than being dropped.
    for claims in out.values_mut() {
        claims.sort_by_key(|c| std::cmp::Reverse(c.logical_ts));
    }
    Ok(out)
}

fn build_pipelines(
    config: &RockyConfig,
    config_path: &Path,
    store: Option<&StateStore>,
    claims: &ClaimsByPipeline,
    now: DateTime<Utc>,
) -> Vec<SchedulePipelineStatus> {
    let member_budgets = build_member_budgets(config, config_path, None);
    let mut out = Vec::new();

    for (name, resolved) in resolve_schedules_detailed(config, None, &member_budgets) {
        let cursor = store
            .and_then(|s| s.get_schedule_state(&name).ok().flatten())
            .unwrap_or_default();

        let claims_here = claims.get(&name).cloned().unwrap_or_default();
        let throttle = match cursor.standing_throttle(now) {
            Throttle::Clear => None,
            Throttle::FailureBackoff { resume_at } => Some(ScheduleThrottleStatus {
                kind: ScheduleThrottleKind::FailureBackoff,
                resume_at,
            }),
            Throttle::PartialBackoff { resume_at } => Some(ScheduleThrottleStatus {
                kind: ScheduleThrottleKind::PartialBackoff,
                resume_at,
            }),
        };

        let common = |config_error: Option<String>,
                      enabled: bool,
                      cron: Option<String>,
                      after: Vec<String>,
                      freshness_budget_seconds: Option<u64>,
                      catchup: String,
                      awaiting_first_anchor: bool,
                      next_fire_at: Option<DateTime<Utc>>| {
            SchedulePipelineStatus {
                pipeline: name.clone(),
                enabled,
                cron,
                after,
                freshness_budget_seconds,
                catchup,
                last_evaluated_at: parse_ts(cursor.last_evaluated_at.as_deref()),
                last_fire_logical_ts: cursor.fire_anchor(),
                last_attempt_at: parse_ts(cursor.last_attempt_at.as_deref()),
                last_attempt_outcome: cursor.last_attempt_outcome.clone(),
                consecutive_failures: cursor.consecutive_failures,
                awaiting_first_anchor,
                next_fire_at,
                throttle: throttle.clone(),
                claims: claims_here.clone(),
                config_error,
            }
        };

        out.push(match resolved {
            Ok(schedule) => {
                let has_cron = schedule.cron.is_some();
                common(
                    None,
                    schedule.enabled,
                    schedule.cron.as_ref().map(|(expr, _)| expr.clone()),
                    schedule.after.clone(),
                    schedule
                        .freshness_budget
                        .map(|d| d.num_seconds().max(0) as u64),
                    catchup_str(schedule.catchup).to_string(),
                    has_cron && cursor.fire_anchor().is_none(),
                    next_projected_fire(&schedule, &cursor, now),
                )
            }
            // An unresolvable schedule reports its reason instead of vanishing:
            // the pipeline IS configured, it just cannot fire, and silence is
            // the least actionable way to say so.
            Err(e) => common(
                Some(e.to_string()),
                false,
                None,
                Vec::new(),
                None,
                "unknown".to_string(),
                false,
                None,
            ),
        });
    }
    out
}

fn count(pipelines: &[SchedulePipelineStatus], now: DateTime<Utc>) -> ScheduleStatusCounts {
    ScheduleStatusCounts {
        scheduled: pipelines.len(),
        enabled: pipelines.iter().filter(|p| p.enabled).count(),
        throttled: pipelines.iter().filter(|p| p.throttle.is_some()).count(),
        in_flight: pipelines
            .iter()
            .filter(|p| p.claims.iter().any(|c| c.state == "submitted"))
            .count(),
        overdue: pipelines
            .iter()
            .filter(|p| p.next_fire_at.is_some_and(|at| at < now))
            .count(),
        config_errors: pipelines
            .iter()
            .filter(|p| p.config_error.is_some())
            .count(),
    }
}

fn catchup_str(catchup: Catchup) -> &'static str {
    match catchup {
        Catchup::Latest => "latest",
        Catchup::Skip => "skip",
    }
}

fn parse_ts(raw: Option<&str>) -> Option<DateTime<Utc>> {
    raw.and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc))
}
