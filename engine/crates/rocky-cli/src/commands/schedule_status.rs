//! Backing read for `GET /api/v1/schedule` — a scheduler status snapshot.
//!
//! A pure read of config + stored cursors + claims + the tick lock. It
//! deliberately does **not** evaluate demand: `rocky tick --dry-run` is the
//! evaluation, and duplicating it here could make the status disagree with
//! the reconciler about what is due.
//!
//! It does read more for one thing: delivery state. When any product
//! declares a `deliver_by` deadline, it finds which scheduled pipelines build
//! each deadlined output model. For a transformation pipeline it loads the
//! models its `models` glob selects, the way `rocky run` selects them: that
//! parses model files on every request. When that answer is incomplete (not
//! a transformation pipeline, an absent models directory, or a model that
//! fails to load), it also reads the pipeline's newest production run that
//! built a deadlined model. For a pipeline that delivers such a product it
//! also reads the product's latest delivery and the pipeline's last 20
//! production runs for the duration estimate. Each of those history reads
//! walks the ordering key of every recorded run (cheap), and decodes full
//! records only until it finds what it needs. The search for the newest
//! deadlined build decodes at most [`DELIVERY_LOOKUP_WINDOW`] runs per
//! pipeline, so full-record decoding is `O(pipelines × window)`; the key walk
//! stays `O(pipelines × runs)`. For the latest start, it also reads the
//! duration estimate of each pipeline upstream, along `after`, of one that
//! delivers such a product. With no product declaring `deliver_by`, it loads
//! no model and reads no run history.
//!
//! The scheduled set comes from [`resolve_schedules_detailed`], the
//! reconciler's own resolution — not from iterating `config.pipelines` and
//! filtering on `schedule()`. Those two disagree: the latter includes load
//! pipelines, which the reconciler refuses to schedule, so it would report a
//! pipeline as scheduled that can never fire.

use std::path::Path;

use chrono::{DateTime, Utc};
use rocky_core::config::{RockyConfig, load_rocky_config};
use rocky_core::delivery::{ChainNode, DirectDeadline, latest_starts};
use rocky_core::schedule::{
    Catchup, ClaimState, LOCK_TAKEOVER_AFTER, ResolvedSchedule, ScheduleStateRecord, Throttle,
    next_projected_fire, parse_claim_key, probe_tick_lock, resolve_schedules_detailed,
};
use rocky_core::state::StateStore;

use crate::commands::tick::build_member_budgets;
use crate::output::{
    CronContext, DeliveryStatus, EstimateBasis, LatestStartStatus, ScheduleClaimStatus,
    ScheduleLockStatus, SchedulePipelineStatus, ScheduleStatusCounts, ScheduleStatusOutput,
    ScheduleThrottleKind, ScheduleThrottleStatus, TickLockState,
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

/// How many of the newest recorded runs the history lookup decodes per
/// pipeline.
///
/// A window, not a history search: when config cannot say which deadlined
/// models a pipeline builds, a deadlined build older than this many runs (of
/// any pipeline) is not found. It stops a pipeline that never builds a
/// deadlined model from decoding the whole, never-pruned history on every
/// request.
const DELIVERY_LOOKUP_WINDOW: usize = 500;

/// Which deadlined products a scheduled pipeline builds.
struct Served<'a> {
    /// The deadlines whose output model the pipeline builds.
    deadlines: Vec<&'a rocky_core::delivery::ProductDeadline>,
    /// Whether `deadlines` may be incomplete: the pipeline's models could
    /// not all be read, and neither those read nor run history show it
    /// building a deadlined model.
    unreadable: bool,
}

/// The deadlines `pipeline` builds.
///
/// For a transformation pipeline whose models directory exists, the models
/// come from its `models` glob, located and filtered exactly as `rocky run`
/// does ([`crate::models_loader::locate_models_dir`],
/// [`crate::models_loader::resolved_models_glob`]). That is what the next
/// scheduled run builds, whether or not it has ever built it before, and
/// however old its last build is. When every model loads, that answer is
/// complete and no history is read.
///
/// Otherwise config alone cannot answer: another pipeline type, an absent
/// models directory, a directory that cannot be located, or a model that
/// fails to load. Then the deadlines found in config (if any) are joined with
/// those the newest production run that built a deadlined model built, within
/// the newest [`DELIVERY_LOOKUP_WINDOW`] runs. If both are empty and a model
/// failed to load, `unreadable` is set.
///
/// # Errors
///
/// A run-history read failure is an error: an unread history cannot show
/// that the pipeline builds nothing.
fn served_deadlines<'a>(
    config: &RockyConfig,
    config_path: &Path,
    pipeline: &str,
    store: Option<&StateStore>,
    deadlines: &'a [rocky_core::delivery::ProductDeadline],
) -> Result<Served<'a>, ScheduleStatusError> {
    use rocky_core::state::UnrecordedScope;

    if deadlines.is_empty() {
        return Ok(Served {
            deadlines: Vec::new(),
            unreadable: false,
        });
    }
    // `Some(models, complete)` for a transformation pipeline whose models
    // directory exists.
    let from_config = match config.pipelines.get(pipeline) {
        Some(rocky_core::config::PipelineConfig::Transformation(t)) => {
            match crate::models_loader::locate_models_dir(&t.models, config_path) {
                Ok(crate::models_loader::ModelsDir::Present(dir)) => {
                    let glob = crate::models_loader::resolved_models_glob(&t.models, config_path);
                    let (models, errors) =
                        crate::models_loader::load_project_models_matching_partial(
                            &dir, &glob, None,
                        );
                    for e in &errors {
                        tracing::warn!(
                            pipeline,
                            error = %format!("{e:#}"),
                            "schedule status: a model could not be loaded; its deadline may be missed"
                        );
                    }
                    let names: std::collections::BTreeSet<String> =
                        models.into_iter().map(|m| m.config.name).collect();
                    Some((names, errors.is_empty()))
                }
                Ok(crate::models_loader::ModelsDir::Absent(_)) => None,
                Err(e) => {
                    tracing::warn!(
                        pipeline,
                        error = %format!("{e:#}"),
                        "schedule status: the models directory could not be located"
                    );
                    Some((std::collections::BTreeSet::new(), false))
                }
            }
        }
        Some(_) | None => None,
    };
    let mut served: Vec<&rocky_core::delivery::ProductDeadline> = match &from_config {
        Some((names, _)) => deadlines
            .iter()
            .filter(|deadline| names.contains(&deadline.output_model))
            .collect(),
        None => Vec::new(),
    };
    let complete = from_config.as_ref().is_some_and(|(_, complete)| *complete);
    if complete {
        return Ok(Served {
            deadlines: served,
            unreadable: false,
        });
    }
    if let Some(store) = store {
        // The newest production run of this pipeline that built any
        // deadlined model. Not simply the newest run: a `--model` run, or a
        // run that failed before the output model, would otherwise make the
        // pipeline read as delivering nothing.
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
        if let Some(run) = latest.first() {
            let built: std::collections::BTreeSet<&str> = run
                .models_executed
                .iter()
                .map(rocky_core::state::ModelExecution::rocky_model_name)
                .collect();
            for deadline in deadlines {
                if built.contains(deadline.output_model.as_str())
                    && !served.iter().any(|held| std::ptr::eq(*held, deadline))
                {
                    served.push(deadline);
                }
            }
        }
    }
    // A transformation pipeline with a model that did not load, and nothing
    // found: whether it builds a deadlined model is unknown.
    let unreadable = served.is_empty() && from_config.is_some();
    Ok(Served {
        deadlines: served,
        unreadable,
    })
}

/// The delivery state of the most urgent of `served`: the one whose next
/// deadline comes first. `None` when `served` is empty.
///
/// Without a store the estimate is empty, so `unknown`.
///
/// # Errors
///
/// A run-history read failure is an error, never `None`.
fn pipeline_delivery(
    pipeline: &str,
    store: Option<&StateStore>,
    served: &[&rocky_core::delivery::ProductDeadline],
    now: DateTime<Utc>,
) -> Result<Option<DeliveryStatus>, ScheduleStatusError> {
    use rocky_core::state::UnrecordedScope;

    if served.is_empty() {
        return Ok(None);
    }
    let estimate = match store {
        Some(store) => store
            .get_pipeline_duration_estimate(pipeline, UnrecordedScope::Exclude)
            .map_err(anyhow::Error::from)?,
        None => rocky_core::delivery::estimate_durations(&[]),
    };
    let mut most_urgent: Option<DeliveryStatus> = None;
    for deadline in served {
        let status =
            DeliveryStatus::judge(deadline, store, &estimate, EstimateBasis::PipelineRun, now)
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
    let scheduled = resolve_schedules_detailed(config, None, &member_budgets);

    // Which deadlines each scheduled pipeline builds, before any status: an
    // unreadable pipeline only matters when some deadline has no builder.
    let mut served: std::collections::BTreeMap<String, Served<'_>> =
        std::collections::BTreeMap::new();
    for name in scheduled.keys() {
        served.insert(
            name.clone(),
            served_deadlines(config, config_path, name, store, deadlines)?,
        );
    }
    let every_deadline_has_a_builder = deadlines.iter().all(|deadline| {
        served
            .values()
            .any(|s| s.deadlines.iter().any(|held| std::ptr::eq(*held, deadline)))
    });

    let mut out = Vec::new();
    // The `after` graph and each pipeline's own deadline, for the latest
    // start derived after the loop.
    let mut chain: std::collections::BTreeMap<String, ChainNode> =
        std::collections::BTreeMap::new();

    for (name, resolved) in scheduled {
        let cursor = store
            .and_then(|s| s.get_schedule_state(&name).ok().flatten())
            .unwrap_or_default();

        let claims_here = claims.get(&name).cloned().unwrap_or_default();
        let served_here = served.remove(&name).unwrap_or(Served {
            deadlines: Vec::new(),
            unreadable: false,
        });
        let delivery = pipeline_delivery(&name, store, &served_here.deadlines, now)?;
        chain.insert(
            name.clone(),
            ChainNode {
                after: resolved
                    .as_ref()
                    .map(|schedule| schedule.after.clone())
                    .unwrap_or_default(),
                p90_ms: delivery.as_ref().and_then(|d| d.estimate.p90_ms),
                deadline: delivery.as_ref().map(|d| DirectDeadline {
                    product: d.product.clone(),
                    due_at: d.next_due_at,
                }),
                serves_unknown: served_here.unreadable && !every_deadline_has_a_builder,
            },
        );

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
                latest_start: None,
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
    attach_latest_starts(&mut out, chain, store, now)?;
    Ok(out)
}

/// Derive each pipeline's latest start from the deadlines it feeds and
/// judge its cron against it. See [`rocky_core::delivery::latest_starts`].
///
/// A pipeline that delivers a product already carries its p90 in
/// `delivery`. Only the other pipelines that feed a deadline need their own
/// estimate read, so a project with no deadline reads no run history here.
/// Without a store every p90 is unknown, so every latest start is too.
fn attach_latest_starts(
    pipelines: &mut [SchedulePipelineStatus],
    mut chain: std::collections::BTreeMap<String, ChainNode>,
    store: Option<&StateStore>,
    now: DateTime<Utc>,
) -> Result<(), ScheduleStatusError> {
    // Walk `after` upstream from every pipeline that serves a deadline, or
    // may.
    let mut feeding: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut stack: Vec<String> = chain
        .iter()
        .filter(|(_, node)| node.deadline.is_some() || node.serves_unknown)
        .map(|(name, _)| name.clone())
        .collect();
    while let Some(name) = stack.pop() {
        if !feeding.insert(name.clone()) {
            continue;
        }
        if let Some(node) = chain.get(&name) {
            stack.extend(node.after.iter().cloned());
        }
    }
    if let Some(store) = store {
        for name in &feeding {
            let Some(node) = chain.get_mut(name) else {
                continue;
            };
            if node.deadline.is_some() {
                continue;
            }
            node.p90_ms = store
                .get_pipeline_duration_estimate(name, rocky_core::state::UnrecordedScope::Exclude)
                .map_err(anyhow::Error::from)?
                .band
                .map(|band| band.p90_ms);
        }
    }
    let mut starts = latest_starts(&chain);
    for pipeline in pipelines {
        if let Some(latest) = starts.remove(&pipeline.pipeline) {
            pipeline.latest_start = Some(LatestStartStatus::from_latest_start(
                latest,
                CronContext {
                    now,
                    next_fire_at: pipeline.next_fire_at,
                    enabled: pipeline.enabled,
                    has_after: !pipeline.after.is_empty(),
                },
            ));
        }
    }
    Ok(())
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

    /// A production run of `pipeline` that built `model`, stamped as
    /// `rocky run` stamps it.
    fn seed_run(
        store: &StateStore,
        root: &Path,
        pipeline: &str,
        model: &str,
        started: DateTime<Utc>,
        finished: DateTime<Utc>,
    ) {
        let mut record: rocky_core::state::RunRecord = serde_json::from_value(serde_json::json!({
            "run_id": format!("{pipeline}-{}", started.to_rfc3339()),
            "started_at": started,
            "finished_at": finished,
            "status": rocky_core::state::RunStatus::Success,
            "models_executed": [{
                "model_name": model,
                "started_at": started,
                "finished_at": finished,
                "duration_ms": (finished - started).num_milliseconds(),
                "rows_affected": 1,
                "status": "success",
                "sql_hash": "h",
                "bytes_scanned": null,
                "bytes_written": null,
            }],
            "trigger": rocky_core::state::RunTrigger::Schedule,
            "config_hash": "c",
            "pipeline": pipeline,
            "run_scope": rocky_core::state::RunScope::Production,
        }))
        .unwrap();
        rocky_core::delivery::stamp_delivery_timing(
            &mut record,
            store,
            &crate::commands::product::product_deadlines_in(root),
        )
        .unwrap();
        store.record_run(&record).unwrap();
    }

    /// Three scheduled pipelines: `raw` on cron `raw_cron`, and `revenue`
    /// and `costs`, which both run `after` it. `revenue` delivers
    /// `revenue_daily` (due 06:00), `costs` delivers `costs_daily` (due
    /// 04:00). On each day it ran, `raw` took 20 min, `costs` 30 min and
    /// `revenue` 10 min. `costs` ran on `costs_days` only. The `raw` cursor
    /// says its last fire was day 5 at the cron's slot, so `next_fire_at`
    /// is day 6 at that slot.
    fn chain_project(
        raw_cron: &str,
        raw_slot: (u32, u32),
        costs_days: std::ops::RangeInclusive<u32>,
    ) -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(
            &root.join("products/revenue_daily.toml"),
            &spec_with_deadline("0 6 * * *"),
        );
        write(
            &root.join("products/costs_daily.toml"),
            &spec_with_deadline("0 4 * * *").replace("revenue_daily", "costs_daily"),
        );
        let mut toml = String::from("[adapter]\ntype = \"duckdb\"\npath = \"x.duckdb\"\n");
        for (name, schedule) in [
            ("raw", format!("cron = \"{raw_cron}\"\ntimezone = \"UTC\"")),
            ("revenue", "after = [\"raw\"]".to_string()),
            ("costs", "after = [\"raw\"]".to_string()),
        ] {
            toml.push_str(&format!(
                "\n[pipeline.{name}]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
                 [pipeline.{name}.target.governance]\nauto_create_schemas = true\n\n\
                 [pipeline.{name}.schedule]\n{schedule}\n"
            ));
        }
        let config_path = root.join("rocky.toml");
        write(&config_path, &toml);
        let state_path = root.join("state.redb");
        let store = StateStore::open(&state_path).unwrap();
        for day in 1..=5 {
            seed_run(
                &store,
                root,
                "raw",
                "raw_orders",
                at(day, 3, 0),
                at(day, 3, 20),
            );
            if costs_days.contains(&day) {
                seed_run(
                    &store,
                    root,
                    "costs",
                    "costs_daily",
                    at(day, 3, 20),
                    at(day, 3, 50),
                );
            }
            seed_run(
                &store,
                root,
                "revenue",
                "revenue_daily",
                at(day, 5, 0),
                at(day, 5, 10),
            );
        }
        let (hour, minute) = raw_slot;
        store
            .put_schedule_state(
                "raw",
                &ScheduleStateRecord {
                    last_fire_logical_ts: Some(at(5, hour, minute).to_rfc3339()),
                    ..ScheduleStateRecord::default()
                },
            )
            .unwrap();
        drop(store);
        (dir, config_path, state_path)
    }

    /// The status JSON of each pipeline, keyed by name, read at day 5 noon.
    fn chain_status(
        config_path: &Path,
        state_path: &Path,
    ) -> std::collections::BTreeMap<String, serde_json::Value> {
        let rocky_dir = config_path.parent().unwrap().join(".rocky");
        let output = schedule_status_output(config_path, state_path, &rocky_dir, at(5, 12, 0))
            .expect("status");
        serde_json::to_value(&output).unwrap()["pipelines"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| (p["pipeline"].as_str().unwrap().to_string(), p.clone()))
            .collect()
    }

    /// `raw` feeds two deadlines. Through `revenue` it must start by
    /// 06:00 - 10 min - 20 min = 05:30; through `costs` by
    /// 04:00 - 30 min - 20 min = 03:10. The earlier one wins. A 03:00 cron
    /// is early enough: no warning.
    #[test]
    fn latest_start_takes_the_earliest_of_two_downstream_deadlines() {
        let (_dir, config_path, state_path) = chain_project("0 3 * * *", (3, 0), 1..=5);
        let status = chain_status(&config_path, &state_path);

        let raw = &status["raw"];
        assert_eq!(raw["next_fire_at"], "2026-05-06T03:00:00Z", "{raw}");
        let latest = &raw["latest_start"];
        assert_eq!(latest["state"], "known", "{latest}");
        assert_eq!(latest["at"], "2026-05-06T03:10:00Z");
        assert_eq!(latest["product"], "costs_daily");
        assert_eq!(latest["due_at"], "2026-05-06T04:00:00Z");
        assert_eq!(latest["cron_too_late"], false);
        assert!(latest.get("warning").is_none(), "{latest}");

        assert_eq!(
            status["revenue"]["latest_start"]["at"],
            "2026-05-06T05:50:00Z"
        );
        assert_eq!(
            status["costs"]["latest_start"]["at"],
            "2026-05-06T03:30:00Z"
        );
        // `revenue` and `costs` have no cron, so nothing to judge.
        assert_eq!(status["costs"]["latest_start"]["cron_too_late"], false);
    }

    /// A 03:15 cron fires after the 03:10 latest start: warned, and the
    /// cron itself is reported unchanged.
    #[test]
    fn a_cron_later_than_the_latest_start_is_warned() {
        let (_dir, config_path, state_path) = chain_project("15 3 * * *", (3, 15), 1..=5);
        let status = chain_status(&config_path, &state_path);

        let raw = &status["raw"];
        assert_eq!(raw["cron"], "15 3 * * *", "the cron is never moved");
        assert_eq!(raw["next_fire_at"], "2026-05-06T03:15:00Z");
        let latest = &raw["latest_start"];
        assert_eq!(latest["at"], "2026-05-06T03:10:00Z", "{latest}");
        assert_eq!(latest["cron_too_late"], true);
        let warning = latest["warning"].as_str().expect("a warning");
        assert!(
            warning.contains("the cron starts too late to meet the deadline"),
            "{warning}"
        );
        assert!(warning.contains("costs_daily"), "{warning}");
    }

    /// `costs` ran once, so its p90 is unknown. `raw` feeds it, so `raw`'s
    /// latest start is unknown too: no time and no warning, even with a
    /// cron that a known estimate would flag. `revenue` is on another chain
    /// and stays known.
    #[test]
    fn an_unknown_estimate_on_the_chain_gives_no_latest_start() {
        let (_dir, config_path, state_path) = chain_project("15 3 * * *", (3, 15), 5..=5);
        let status = chain_status(&config_path, &state_path);

        let latest = &status["raw"]["latest_start"];
        assert_eq!(latest["state"], "unknown", "{latest}");
        assert!(latest.get("at").is_none(), "never a guess: {latest}");
        assert_eq!(latest["missing_estimate"], serde_json::json!(["costs"]));
        assert_eq!(latest["cron_too_late"], false);
        assert!(latest.get("warning").is_none(), "{latest}");

        assert_eq!(status["costs"]["latest_start"]["state"], "unknown");
        assert_eq!(status["revenue"]["latest_start"]["state"], "known");
    }

    #[test]
    fn a_pipeline_that_delivers_no_product_reports_no_delivery() {
        let (_dir, config_path, state_path, store) = project();
        drop(store);
        let delivery = delivery_json(&config_path, &state_path, at(2, 5, 55));
        assert!(delivery.is_null(), "never ran: no delivery, got {delivery}");
    }

    /// Two pipelines with their own model directories: `raw` (cron
    /// `0 3 * * *`, builds `raw_orders`) and `revenue` (after `raw`, builds
    /// `revenue_daily`, due 06:00). `extra_revenue_file` adds one more file
    /// under `models/revenue/`. The state store is open for seeding.
    fn config_project(
        extra_revenue_file: Option<(&str, &str)>,
    ) -> (
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
        for (sub, model) in [("raw", "raw_orders"), ("revenue", "revenue_daily")] {
            let base = root.join("models").join(sub);
            write(&base.join(format!("{model}.sql")), "SELECT 1 AS id\n");
            write(
                &base.join(format!("{model}.toml")),
                &format!(
                    "depends_on = []\n\n[target]\ncatalog = \"p\"\nschema = \"s\"\n\
                     table = \"{model}\"\n"
                ),
            );
        }
        if let Some((file, text)) = extra_revenue_file {
            write(&root.join("models/revenue").join(file), text);
        }
        let mut toml = String::from("[adapter]\ntype = \"duckdb\"\npath = \"x.duckdb\"\n");
        for (name, schedule) in [
            ("raw", "cron = \"0 3 * * *\"\ntimezone = \"UTC\""),
            ("revenue", "after = [\"raw\"]"),
        ] {
            toml.push_str(&format!(
                "\n[pipeline.{name}]\ntype = \"transformation\"\nmodels = \"models/{name}/**\"\n\n\
                 [pipeline.{name}.target.governance]\nauto_create_schemas = true\n\n\
                 [pipeline.{name}.schedule]\n{schedule}\n"
            ));
        }
        let config_path = root.join("rocky.toml");
        write(&config_path, &toml);
        let state_path = root.join("state.redb");
        let store = StateStore::open(&state_path).unwrap();
        store
            .put_schedule_state(
                "raw",
                &ScheduleStateRecord {
                    last_fire_logical_ts: Some(at(5, 3, 0).to_rfc3339()),
                    ..ScheduleStateRecord::default()
                },
            )
            .unwrap();
        (dir, config_path, state_path, store)
    }

    /// `revenue` builds `revenue_daily` by config, but no run in history
    /// built it: its five production runs built another model only, as when
    /// the output model is new or fails before it is built. The mapping
    /// comes from config, so the deadline still reaches `raw`: 06:00 - 10m -
    /// 20m = 05:30. Before, history alone mapped it, and both pipelines had
    /// no `latest_start` at all.
    #[test]
    fn a_pipeline_that_never_built_the_product_still_feeds_its_deadline() {
        let (dir, config_path, state_path, store) = config_project(None);
        for day in 1..=5 {
            seed_run(
                &store,
                dir.path(),
                "raw",
                "raw_orders",
                at(day, 3, 0),
                at(day, 3, 20),
            );
            seed_run(
                &store,
                dir.path(),
                "revenue",
                "stg_only",
                at(day, 3, 20),
                at(day, 3, 30),
            );
        }
        drop(store);
        let status = chain_status(&config_path, &state_path);

        assert_eq!(
            status["revenue"]["delivery"]["product"], "revenue_daily",
            "{}",
            status["revenue"]
        );
        let revenue = &status["revenue"]["latest_start"];
        assert_eq!(revenue["state"], "known", "{revenue}");
        assert_eq!(revenue["at"], "2026-05-06T05:50:00Z");
        let raw = &status["raw"]["latest_start"];
        assert_eq!(raw["state"], "known", "{raw}");
        assert_eq!(raw["at"], "2026-05-06T05:30:00Z");
        assert_eq!(raw["cron_check"], "on_time");
    }

    /// The newest build of `revenue_daily` is more than 500 runs old: other
    /// pipelines ran since. History alone no longer finds it; config does.
    #[test]
    fn a_build_older_than_the_history_window_still_feeds_its_deadline() {
        let (dir, config_path, state_path, store) = config_project(None);
        for day in 1..=5 {
            seed_run(
                &store,
                dir.path(),
                "revenue",
                "revenue_daily",
                at(day, 3, 20),
                at(day, 3, 30),
            );
        }
        for i in 0..=DELIVERY_LOOKUP_WINDOW as i64 {
            let start = at(5, 4, 0) + chrono::Duration::seconds(i);
            seed_run(
                &store,
                dir.path(),
                "raw",
                "raw_orders",
                start,
                start + chrono::Duration::minutes(20),
            );
        }
        drop(store);
        let status = chain_status(&config_path, &state_path);

        let revenue = &status["revenue"]["latest_start"];
        assert_eq!(revenue["state"], "known", "{}", status["revenue"]);
        assert_eq!(revenue["product"], "revenue_daily");
        assert_eq!(status["raw"]["latest_start"]["state"], "known");
    }

    /// A brand-new project: no state file at all. The deadline still maps by
    /// config, and with no runs every latest start is `unknown` with the
    /// reason `missing_estimate`, never absent.
    #[test]
    fn a_new_product_with_no_history_reads_unknown_not_absent() {
        let (_dir, config_path, state_path, store) = config_project(None);
        drop(store);
        std::fs::remove_file(&state_path).unwrap();
        let status = chain_status(&config_path, &state_path);

        for name in ["raw", "revenue"] {
            let latest = &status[name]["latest_start"];
            assert_eq!(latest["state"], "unknown", "{name}: {latest}");
            assert_eq!(latest["reasons"], serde_json::json!(["missing_estimate"]));
        }
        assert_eq!(
            status["raw"]["latest_start"]["missing_estimate"],
            serde_json::json!(["raw", "revenue"])
        );
    }

    /// `revenue_daily` has no builder that config or history can show:
    /// `revenue` has a model that fails to load, and its output model is
    /// not in a file that loads. Its latest start, and `raw`'s, are
    /// `unknown` with `models_unreadable`, not absent.
    #[test]
    fn a_pipeline_with_an_unloadable_model_reads_models_unreadable() {
        let (dir, config_path, state_path, store) =
            config_project(Some(("broken.toml", "this is not = = toml")));
        std::fs::remove_file(dir.path().join("models/revenue/revenue_daily.sql")).unwrap();
        std::fs::remove_file(dir.path().join("models/revenue/revenue_daily.toml")).unwrap();
        write(&dir.path().join("models/revenue/broken.sql"), "SELECT 1\n");
        drop(store);
        let status = chain_status(&config_path, &state_path);

        for name in ["raw", "revenue"] {
            let latest = &status[name]["latest_start"];
            assert_eq!(latest["state"], "unknown", "{name}: {latest}");
            let reasons = latest["reasons"].as_array().expect("reasons");
            assert!(
                reasons.contains(&serde_json::json!("models_unreadable")),
                "{name}: {latest}"
            );
        }
    }
}

/// The cron verdict against a latest start. Pure: no store, no clock.
#[cfg(test)]
mod cron_check_tests {
    use crate::output::{CronCheck, CronContext, LatestStartStatus};
    use chrono::{DateTime, TimeZone, Utc};
    use rocky_core::delivery::{LatestStart, UnknownReason};

    fn t(hour: u32, minute: u32, second: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 5, 6, hour, minute, second)
            .unwrap()
    }

    /// Latest start 03:10 for `costs_daily`, due 04:00.
    fn known() -> LatestStart {
        LatestStart::Known {
            at: t(3, 10, 0),
            product: "costs_daily".to_string(),
            due_at: t(4, 0, 0),
        }
    }

    fn ctx(fire: Option<DateTime<Utc>>, enabled: bool, has_after: bool) -> CronContext {
        CronContext {
            now: t(1, 0, 0),
            next_fire_at: fire,
            enabled,
            has_after,
        }
    }

    /// The docs say "later than `at`": a fire exactly at `at` is in time.
    #[test]
    fn a_fire_exactly_at_the_latest_start_is_on_time() {
        let status =
            LatestStartStatus::from_latest_start(known(), ctx(Some(t(3, 10, 0)), true, false));
        assert_eq!(status.cron_check, CronCheck::OnTime);
        assert!(!status.cron_too_late);
        assert!(status.warning.is_none() && status.note.is_none());

        let status =
            LatestStartStatus::from_latest_start(known(), ctx(Some(t(3, 10, 1)), true, false));
        assert_eq!(status.cron_check, CronCheck::TooLate);
        assert!(status.cron_too_late);
        let warning = status.warning.expect("a warning");
        assert!(
            warning.contains("the cron starts too late to meet the deadline"),
            "{warning}"
        );
        assert!(warning.contains("costs_daily"), "{warning}");
    }

    /// With `after` as well as a cron, a late cron is not `cron_too_late`:
    /// the `after` trigger is relied on, and `note` says so.
    #[test]
    fn a_late_cron_with_after_relies_on_after() {
        let status =
            LatestStartStatus::from_latest_start(known(), ctx(Some(t(3, 15, 0)), true, true));
        assert_eq!(status.cron_check, CronCheck::ReliesOnAfter);
        assert!(!status.cron_too_late);
        assert!(status.warning.is_none());
        let note = status.note.expect("a note");
        assert!(
            note.contains("the cron alone would start too late"),
            "{note}"
        );
        assert!(note.contains("`after` trigger is relied on"), "{note}");

        // An early cron with `after` is simply on time.
        let status =
            LatestStartStatus::from_latest_start(known(), ctx(Some(t(3, 0, 0)), true, true));
        assert_eq!(status.cron_check, CronCheck::OnTime);
        assert!(status.note.is_none());
    }

    /// A fire before `now` is a missed slot: `overdue`, with a warning,
    /// known or not. A fire exactly at `now` is due, not overdue, as in
    /// `counts.overdue`.
    #[test]
    fn a_missed_cron_slot_is_overdue() {
        let now = t(1, 0, 0);
        let missed = Some(t(0, 59, 59));
        let status = LatestStartStatus::from_latest_start(known(), ctx(missed, true, false));
        assert_eq!(status.cron_check, CronCheck::Overdue);
        assert!(!status.cron_too_late);
        let warning = status.warning.expect("a warning");
        assert!(warning.contains("the cron is overdue"), "{warning}");

        let unknown = LatestStart::Unknown {
            reasons: vec![UnknownReason::MissingEstimate],
            missing_estimate: vec!["raw".to_string()],
        };
        let status = LatestStartStatus::from_latest_start(unknown, ctx(missed, true, false));
        assert_eq!(status.cron_check, CronCheck::Overdue);
        assert!(status.warning.is_some());

        let status = LatestStartStatus::from_latest_start(known(), ctx(Some(now), true, false));
        assert_eq!(status.cron_check, CronCheck::OnTime);
    }

    /// A disabled schedule never fires: `disabled`, with a warning, and no
    /// cron verdict.
    #[test]
    fn a_disabled_schedule_is_reported_as_disabled() {
        let status = LatestStartStatus::from_latest_start(known(), ctx(None, false, false));
        assert_eq!(status.cron_check, CronCheck::Disabled);
        assert!(!status.cron_too_late);
        let warning = status.warning.expect("a warning");
        assert!(warning.contains("disabled"), "{warning}");
    }

    /// An unknown latest start carries its reasons and judges no cron.
    #[test]
    fn an_unknown_latest_start_carries_its_reasons() {
        let unknown = LatestStart::Unknown {
            reasons: vec![UnknownReason::Cycle, UnknownReason::Overflow],
            missing_estimate: Vec::new(),
        };
        let status =
            LatestStartStatus::from_latest_start(unknown, ctx(Some(t(23, 0, 0)), true, false));
        assert_eq!(status.cron_check, CronCheck::NotJudged);
        assert!(!status.cron_too_late);
        assert!(status.warning.is_none() && status.at.is_none());
        let json = serde_json::to_value(&status).unwrap();
        assert_eq!(json["reasons"], serde_json::json!(["cycle", "overflow"]));
        assert_eq!(json["cron_check"], "not_judged");
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
