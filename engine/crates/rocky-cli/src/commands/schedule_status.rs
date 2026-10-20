//! Backing read for `GET /api/v1/schedule` — a scheduler status snapshot.
//!
//! A pure read of config + stored cursors + claims + the tick lock. It
//! deliberately does **not** evaluate demand: `rocky tick --dry-run` is the
//! evaluation, and duplicating it here could make the status disagree with
//! the reconciler about what is due.
//!
//! It does read run history for one thing: delivery state. When any product
//! declares a `deliver_by` deadline, it reads, per pipeline, the pipeline's
//! newest production run that built a deadlined model. For a pipeline that
//! delivers such a product it
//! also reads the product's latest delivery and the pipeline's last 20
//! production runs for the duration estimate. Each of those history reads
//! walks the ordering key of every recorded run (cheap), and decodes full
//! records only until it finds what it needs. The search for the newest
//! deadlined build decodes at most [`DELIVERY_LOOKUP_WINDOW`] runs per
//! pipeline, so full-record decoding is `O(pipelines × window)`; the key walk
//! stays `O(pipelines × runs)`. A deadlined build older than the window reads
//! as no delivery. With no product declaring `deliver_by`, it reads no run
//! history at all.
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
    Catchup, ClaimState, LOCK_TAKEOVER_AFTER, ResolvedSchedule, ScheduleStateRecord, Throttle,
    next_projected_fire, parse_claim_key, probe_tick_lock, resolve_schedules_detailed,
};
use rocky_core::state::StateStore;

use crate::commands::tick::build_member_budgets;
use crate::output::{
    DeliveryStatus, EstimateBasis, ScheduleClaimStatus, ScheduleLockStatus, SchedulePipelineStatus,
    ScheduleStatusCounts, ScheduleStatusOutput, ScheduleThrottleKind, ScheduleThrottleStatus,
    TickLockState,
};

/// Why a schedule-status read failed.
#[derive(thiserror::Error)]
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

/// `Debug` prints the rendered `Display` text. A derived `Debug` would print
/// the plaintext of every field and wrapped error (#1919).
impl std::fmt::Debug for ScheduleStatusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        rocky_core::secret_registry::fmt_rendered_debug(f, "ScheduleStatusError", self)
    }
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
    //
    // The existence check runs BEFORE the open, not in its error branch:
    // redb's read-only open still goes through `Database::create`, so opening
    // an absent path MATERIALIZES the state file — a status read that writes,
    // on the one endpoint documented side-effect free. (The check-then-open
    // race is benign: a run starting concurrently creates the real file and
    // the next status call reads it.)
    let store = if !state_path.exists() {
        None
    } else {
        match StateStore::open_read_only_or_empty(state_path) {
            Ok(store) => Some(store),
            Err(e) => return Err(ScheduleStatusError::State(e.into())),
        }
    };

    let claims_by_pipeline = collect_claims(store.as_ref())?;
    // The project root every product reader shares: the config's directory.
    let project_root = crate::commands::product::product_command_root(config_path);
    let deadlines = crate::commands::product::product_deadlines_in(&project_root);
    let pipelines = build_pipelines(
        &config,
        config_path,
        store.as_ref(),
        &claims_by_pipeline,
        &deadlines,
        now,
    )?;
    let counts = count(&pipelines, now);

    Ok(ScheduleStatusOutput {
        now,
        // Resolved `${VAR}` values print as `${NAME}` (#1919).
        timezone: rocky_core::secret_registry::render_placeholders(&config.schedule.timezone),
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

/// How many of the newest recorded runs the delivery lookup decodes per
/// pipeline.
///
/// A window, not a history search: a pipeline whose newest deadlined build is
/// older than this many runs (of any pipeline) reads as delivering nothing.
/// It stops a pipeline that never builds a deadlined model from decoding the
/// whole, never-pruned history on every request.
const DELIVERY_LOOKUP_WINDOW: usize = 500;

/// The delivery state of the most urgent product deadline `pipeline`
/// delivers.
///
/// The pipeline delivers the products whose output models its newest
/// production run that built any deadlined model built. With several, the one
/// whose next deadline comes first wins. `None` without a store, or when no
/// production run of the pipeline, within the newest [`DELIVERY_LOOKUP_WINDOW`]
/// runs, built a deadlined model.
///
/// # Errors
///
/// A run-history read failure is an error, never `None`: `None` says the
/// pipeline delivers no product with a deadline, which an unread history
/// cannot show.
fn pipeline_delivery(
    pipeline: &str,
    store: Option<&StateStore>,
    deadlines: &[rocky_core::delivery::ProductDeadline],
    now: DateTime<Utc>,
) -> Result<Option<DeliveryStatus>, ScheduleStatusError> {
    use rocky_core::state::UnrecordedScope;

    let Some(store) = store else {
        return Ok(None);
    };
    if deadlines.is_empty() {
        return Ok(None);
    }
    // The newest production run of this pipeline that built any deadlined
    // model. Not simply the newest run: a `--model` run, or a run that failed
    // before the output model, would otherwise make the pipeline read as
    // delivering nothing.
    let latest = store
        .list_runs_matching_within(1, DELIVERY_LOOKUP_WINDOW, |run| {
            run.pipeline.as_deref() == Some(pipeline)
                && run.counts_as_production(UnrecordedScope::Exclude)
                && run.models_executed.iter().any(|execution| {
                    deadlines
                        .iter()
                        .any(|deadline| deadline.output_model == execution.rocky_model_name())
                })
        })
        .map_err(anyhow::Error::from)?;
    let Some(run) = latest.first() else {
        return Ok(None);
    };
    let built: std::collections::BTreeSet<&str> = run
        .models_executed
        .iter()
        .map(rocky_core::state::ModelExecution::rocky_model_name)
        .collect();
    let delivered: Vec<_> = deadlines
        .iter()
        .filter(|deadline| built.contains(deadline.output_model.as_str()))
        .collect();
    if delivered.is_empty() {
        return Ok(None);
    }
    let estimate = store
        .get_pipeline_duration_estimate(pipeline, UnrecordedScope::Exclude)
        .map_err(anyhow::Error::from)?;
    let mut most_urgent: Option<DeliveryStatus> = None;
    for deadline in delivered {
        let status = DeliveryStatus::judge(
            deadline,
            Some(store),
            &estimate,
            EstimateBasis::PipelineRun,
            now,
        )
        .map_err(anyhow::Error::from)?;
        let sooner = match &most_urgent {
            None => true,
            Some(current) => match (status.next_due_at, current.next_due_at) {
                (Some(candidate), Some(held)) => candidate < held,
                (Some(_), None) => true,
                (None, Some(_) | None) => false,
            },
        };
        if sooner {
            most_urgent = Some(status);
        }
    }
    Ok(most_urgent)
}

fn build_pipelines(
    config: &RockyConfig,
    config_path: &Path,
    store: Option<&StateStore>,
    claims: &ClaimsByPipeline,
    deadlines: &[rocky_core::delivery::ProductDeadline],
    now: DateTime<Utc>,
) -> Result<Vec<SchedulePipelineStatus>, ScheduleStatusError> {
    let member_budgets = build_member_budgets(config, config_path, None);
    let mut out = Vec::new();

    for (name, resolved) in resolve_schedules_detailed(config, None, &member_budgets) {
        let cursor = store
            .and_then(|s| s.get_schedule_state(&name).ok().flatten())
            .unwrap_or_default();

        let claims_here = claims.get(&name).cloned().unwrap_or_default();
        let delivery = pipeline_delivery(&name, store, deadlines, now)?;

        let common = |config_error: Option<String>,
                      enabled: bool,
                      cron: Option<String>,
                      after: Vec<String>,
                      freshness_budget_seconds: Option<u64>,
                      catchup: String,
                      awaiting_first_anchor: bool,
                      next_fire_at: Option<DateTime<Utc>>,
                      throttle: Option<ScheduleThrottleStatus>| {
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
                throttle,
                claims: claims_here.clone(),
                config_error,
                delivery: delivery.clone(),
            }
        };

        out.push(match resolved {
            Ok(schedule) => {
                let has_cron = schedule.cron.is_some();
                let throttle = reportable_throttle(&schedule, &cursor, now);
                common(
                    None,
                    schedule.enabled,
                    schedule
                        .cron
                        .as_ref()
                        .map(|(expr, _)| rocky_core::secret_registry::render_placeholders(expr)),
                    schedule
                        .after
                        .iter()
                        .map(|a| rocky_core::secret_registry::render_placeholders(a))
                        .collect(),
                    schedule
                        .freshness_budget
                        .map(|d| d.num_seconds().max(0) as u64),
                    catchup_str(schedule.catchup).to_string(),
                    has_cron && cursor.fire_anchor().is_none(),
                    next_projected_fire(&schedule, &cursor, now),
                    throttle,
                )
            }
            // An unresolvable schedule reports its reason instead of vanishing:
            // the pipeline IS configured, it just cannot fire, and silence is
            // the least actionable way to say so.
            Err(e) => common(
                Some(rocky_core::secret_registry::render_placeholders(
                    &e.to_string(),
                )),
                false,
                None,
                Vec::new(),
                None,
                "unknown".to_string(),
                false,
                None,
                None,
            ),
        });
    }
    Ok(out)
}

/// The throttle to report, matching only what the reconciler actually honors.
///
/// The failure/partial backoff ladder applies to **standing** demands
/// (`after` / `freshness`) alone. The reconciler never throttles a cron
/// occurrence — `ScheduleStateRecord::standing_throttle`'s own contract says a
/// cron demand must not be passed through it, because suppressing an occurrence
/// would drop a legitimate fire. So a cron-only pipeline reports no throttle
/// even when its cursor carries a failed last attempt; reporting one would
/// signal a suppression window that does not exist.
fn reportable_throttle(
    schedule: &ResolvedSchedule,
    cursor: &ScheduleStateRecord,
    now: DateTime<Utc>,
) -> Option<ScheduleThrottleStatus> {
    let is_standing = !schedule.after.is_empty() || schedule.freshness_budget.is_some();
    if !is_standing {
        return None;
    }
    match cursor.standing_throttle(now) {
        Throttle::Clear => None,
        Throttle::FailureBackoff { resume_at } => Some(ScheduleThrottleStatus {
            kind: ScheduleThrottleKind::FailureBackoff,
            resume_at,
        }),
        Throttle::PartialBackoff { resume_at } => Some(ScheduleThrottleStatus {
            kind: ScheduleThrottleKind::PartialBackoff,
            resume_at,
        }),
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn schedule(after: Vec<&str>, freshness: bool) -> ResolvedSchedule {
        ResolvedSchedule {
            pipeline: "p".to_string(),
            enabled: true,
            // The tz is inferred from the field type — the throttle helper never
            // reads it, so this avoids naming `chrono_tz` directly.
            cron: Some(("* * * * *".to_string(), "UTC".parse().unwrap())),
            after: after.into_iter().map(String::from).collect(),
            freshness_budget: freshness.then(|| chrono::Duration::seconds(3600)),
            catchup: Catchup::Latest,
            retry_max: 0,
            timeout: None,
        }
    }

    /// A cursor whose last attempt failed — enough to trip `standing_throttle`
    /// into a FailureBackoff.
    fn failed_cursor(now: DateTime<Utc>) -> ScheduleStateRecord {
        ScheduleStateRecord {
            last_attempt_at: Some(now.to_rfc3339()),
            last_attempt_outcome: Some("failure".to_string()),
            consecutive_failures: 1,
            ..Default::default()
        }
    }

    /// The bug this guards: a cron-only pipeline that failed must NOT report a
    /// throttle, because the reconciler never applies the backoff ladder to
    /// cron — reporting one signals a suppression window that does not exist.
    #[test]
    fn cron_only_never_reports_a_throttle() {
        let now = Utc.with_ymd_and_hms(2026, 5, 1, 12, 0, 0).unwrap();
        assert!(reportable_throttle(&schedule(vec![], false), &failed_cursor(now), now).is_none());
    }

    #[test]
    fn standing_demands_do_report_a_throttle() {
        let now = Utc.with_ymd_and_hms(2026, 5, 1, 12, 0, 0).unwrap();
        // `after` makes it standing.
        assert!(
            reportable_throttle(&schedule(vec!["upstream"], false), &failed_cursor(now), now)
                .is_some()
        );
        // freshness makes it standing.
        assert!(reportable_throttle(&schedule(vec![], true), &failed_cursor(now), now).is_some());
    }

    #[test]
    fn a_clear_standing_cursor_reports_no_throttle() {
        let now = Utc.with_ymd_and_hms(2026, 5, 1, 12, 0, 0).unwrap();
        // No last attempt → Throttle::Clear even though it is standing.
        let fresh = ScheduleStateRecord::default();
        assert!(reportable_throttle(&schedule(vec!["upstream"], false), &fresh, now).is_none());
    }
}

#[cfg(test)]
mod side_effect_free_tests {
    use super::*;

    /// The #1330 review's confirmed defect: redb's read-only open goes through
    /// `Database::create`, so a status read of a FRESH project materialized
    /// the state file — a write, on the one endpoint documented side-effect
    /// free. The existence guard must keep the read genuinely read-only.
    #[test]
    fn a_status_read_of_a_fresh_project_creates_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let config_path = tmp.path().join("rocky.toml");
        std::fs::write(
            &config_path,
            "[adapter]\ntype = \"duckdb\"\npath = \"x.duckdb\"\n\n\
             [pipeline.p]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
             [pipeline.p.target.governance]\nauto_create_schemas = true\n",
        )
        .unwrap();
        let state_path = tmp.path().join("state.redb");
        let rocky_dir = tmp.path().join(".rocky");

        let out = schedule_status_output(&config_path, &state_path, &rocky_dir, chrono::Utc::now())
            .expect("a fresh project must yield an empty snapshot");
        assert!(
            !state_path.exists(),
            "the read-only status must not materialize the state file"
        );
        drop(out);
    }
}

#[cfg(test)]
mod delivery_state_tests {
    use super::*;
    use crate::commands::product::delivery_tests::{
        at, seed_delivery, seed_five_on_time, spec_with_deadline, write,
    };

    /// A project scheduling pipeline `revenue`, whose product declares a
    /// 06:00 deadline, with the state store open for seeding.
    fn project() -> (
        tempfile::TempDir,
        std::path::PathBuf,
        std::path::PathBuf,
        StateStore,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(
            &root.join("products/revenue_daily.toml"),
            &spec_with_deadline("0 6 * * *"),
        );
        let config_path = root.join("rocky.toml");
        write(
            &config_path,
            "[adapter]\ntype = \"duckdb\"\npath = \"x.duckdb\"\n\n\
             [pipeline.revenue]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
             [pipeline.revenue.target.governance]\nauto_create_schemas = true\n\n\
             [pipeline.revenue.schedule]\ncron = \"0 5 * * *\"\ntimezone = \"UTC\"\n",
        );
        let state_path = root.join("state.redb");
        let store = StateStore::open(&state_path).unwrap();
        (dir, config_path, state_path, store)
    }

    /// The `delivery` object of pipeline `revenue` in the status JSON.
    fn delivery_json(
        config_path: &Path,
        state_path: &Path,
        now: DateTime<Utc>,
    ) -> serde_json::Value {
        let rocky_dir = config_path.parent().unwrap().join(".rocky");
        let output =
            schedule_status_output(config_path, state_path, &rocky_dir, now).expect("status");
        let json = serde_json::to_value(&output).unwrap();
        assert_eq!(json["pipelines"][0]["pipeline"], "revenue");
        json["pipelines"][0]["delivery"].clone()
    }

    #[test]
    fn schedule_status_reports_an_on_time_delivery() {
        let (dir, config_path, state_path, store) = project();
        seed_five_on_time(&store, dir.path());
        drop(store);

        let delivery = delivery_json(&config_path, &state_path, at(5, 12, 0));
        assert_eq!(delivery["product"], "revenue_daily");
        assert_eq!(delivery["on_time"], true);
        assert_eq!(delivery["late_by_seconds"], 0);
        assert_eq!(delivery["next_due_at"], "2026-05-06T06:00:00Z");
        assert_eq!(delivery["at_risk"], false);
        assert_eq!(delivery["estimate"]["basis"], "pipeline_run");
        assert_eq!(delivery["estimate"]["state"], "known");
        assert_eq!(delivery["estimate"]["n"], 5);
        assert_eq!(delivery["estimate"]["window"], 20);
        assert_eq!(delivery["estimate"]["p50_ms"], 600_000);
        assert_eq!(delivery["estimate"]["p90_ms"], 600_000);
    }

    #[test]
    fn schedule_status_reports_a_late_delivery() {
        let (dir, config_path, state_path, store) = project();
        seed_five_on_time(&store, dir.path());
        seed_delivery(&store, dir.path(), "run-6", at(6, 7, 20), at(6, 7, 30));
        drop(store);

        let delivery = delivery_json(&config_path, &state_path, at(6, 12, 0));
        assert_eq!(delivery["on_time"], false);
        assert_eq!(delivery["late_by_seconds"], 90 * 60);
    }

    #[test]
    fn schedule_status_reports_an_at_risk_delivery() {
        let (dir, config_path, state_path, store) = project();
        seed_five_on_time(&store, dir.path());
        drop(store);

        let delivery = delivery_json(&config_path, &state_path, at(6, 5, 55));
        assert_eq!(delivery["at_risk"], true);
    }

    #[test]
    fn schedule_status_gives_no_risk_verdict_below_five_runs() {
        let (dir, config_path, state_path, store) = project();
        seed_delivery(&store, dir.path(), "run-1", at(1, 5, 0), at(1, 5, 10));
        drop(store);

        let delivery = delivery_json(&config_path, &state_path, at(2, 5, 55));
        assert_eq!(delivery["estimate"]["state"], "unknown");
        assert!(delivery["at_risk"].is_null(), "{delivery}");
    }

    #[test]
    fn a_newer_run_without_the_output_model_keeps_the_delivery() {
        let (dir, config_path, state_path, store) = project();
        seed_five_on_time(&store, dir.path());
        // A newer production run of the same pipeline that built another
        // model only, as `--model other` would.
        let other: rocky_core::state::RunRecord = serde_json::from_value(serde_json::json!({
            "run_id": "run-other",
            "started_at": at(5, 9, 0),
            "finished_at": at(5, 9, 5),
            "status": rocky_core::state::RunStatus::Success,
            "models_executed": [{
                "model_name": "other",
                "started_at": at(5, 9, 0),
                "finished_at": at(5, 9, 5),
                "duration_ms": 300_000,
                "rows_affected": 1,
                "status": "success",
                "sql_hash": "h",
                "bytes_scanned": null,
                "bytes_written": null,
            }],
            "trigger": rocky_core::state::RunTrigger::Manual,
            "config_hash": "c",
            "pipeline": "revenue",
            "run_scope": rocky_core::state::RunScope::Production,
        }))
        .unwrap();
        store.record_run(&other).unwrap();
        drop(store);

        let delivery = delivery_json(&config_path, &state_path, at(5, 12, 0));
        assert_eq!(delivery["product"], "revenue_daily", "{delivery}");
        assert_eq!(delivery["on_time"], true);
    }

    #[test]
    fn a_pipeline_that_delivers_no_product_reports_no_delivery() {
        let (_dir, config_path, state_path, store) = project();
        drop(store);
        let delivery = delivery_json(&config_path, &state_path, at(2, 5, 55));
        assert!(delivery.is_null(), "never ran: no delivery, got {delivery}");
    }
}

/// `Debug` prints a resolved `${VAR}` value as `${NAME}` (#1919).
#[cfg(test)]
mod rendered_debug_1919 {
    use super::*;

    #[test]
    fn schedule_status_error_debug_renders_a_resolved_value() {
        const SECRET: &str = "sched-stat-1919-9d10";
        rocky_core::secret_registry::register_substitution("RV_SCHED_STATUS_DBG", SECRET);
        let err = ScheduleStatusError::State(anyhow::anyhow!("open {SECRET} failed"));
        let debug = format!("{err:?}");
        assert!(!debug.contains(SECRET), "Debug leaks: {debug}");
        assert!(debug.contains("${RV_SCHED_STATUS_DBG}"), "{debug}");
    }
}
