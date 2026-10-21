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
//! models its `models` glob selects, with the same root, models directory and
//! glob as `rocky run`, leaving out ephemeral models: that parses model files
//! on every request. It does not compile them. When the models directory is
//! absent or cannot be located, or a model fails to load, `rocky run` builds
//! nothing, so the pipeline serves no deadline; the newest production run
//! that built a deadlined model is read only to flag it. For any other
//! pipeline type, that run is what maps it. For a pipeline that delivers such a product it
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
    AfterTrigger, CronCheck, CronContext, DeliveryStatus, EstimateBasis, LatestStartState,
    LatestStartStatus, ScheduleClaimStatus, ScheduleLockStatus, SchedulePipelineStatus,
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
    /// `Some` when the pipeline is a transformation pipeline whose models
    /// `rocky run` cannot build: then `deadlines` is empty.
    unreadable: Option<Unreadable<'a>>,
}

/// A transformation pipeline whose next `rocky run` builds nothing: its
/// models directory is absent or cannot be located, or a model fails to
/// load.
struct Unreadable<'a> {
    /// Why, in a few words, for the warning.
    cause: &'static str,
    /// The deadlines whose output model the pipeline would build: matched
    /// among the models that did load, or built by its newest production
    /// run that built a deadlined model. Never served while unreadable.
    claimed: Vec<&'a rocky_core::delivery::ProductDeadline>,
}

/// The deadlines `pipeline` builds.
///
/// For a transformation pipeline, the models come from its `models` glob,
/// with the same root, models directory and glob as `rocky run`
/// ([`crate::models_loader::locate_models_dir`],
/// [`crate::models_loader::resolved_models_glob`]). Ephemeral models are left
/// out: `rocky run` never builds one. The models are loaded, not compiled,
/// so a compile error is not seen here. When every model loads, that answer
/// is complete and no history is read: it is what the next scheduled run
/// builds, whether or not it built it before, and however old its last
/// build is.
///
/// When the models directory is absent, cannot be located, or a model fails
/// to load, `rocky run` builds nothing for the pipeline. Then it serves no
/// deadline, and `unreadable` lists the deadlines it would build: those
/// matched among the models that loaded, and those its newest production run
/// that built a deadlined model built (within the newest
/// [`DELIVERY_LOOKUP_WINDOW`] runs). History never makes it serve one.
///
/// For any other pipeline type, the deadlines are those its newest
/// production run that built a deadlined model built, within the same
/// window.
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
    if deadlines.is_empty() {
        return Ok(Served {
            deadlines: Vec::new(),
            unreadable: None,
        });
    }
    let Some(rocky_core::config::PipelineConfig::Transformation(t)) =
        config.pipelines.get(pipeline)
    else {
        return Ok(Served {
            deadlines: built_by_history(pipeline, store, deadlines)?,
            unreadable: None,
        });
    };
    // The models `rocky run` would build, and why it builds nothing when
    // it would.
    let (names, cause) = match crate::models_loader::locate_models_dir(&t.models, config_path) {
        Ok(crate::models_loader::ModelsDir::Present(dir)) => {
            let glob = crate::models_loader::resolved_models_glob(&t.models, config_path);
            let (models, errors) =
                crate::models_loader::load_project_models_matching_partial(&dir, &glob, None);
            for e in &errors {
                tracing::warn!(
                    pipeline,
                    error = %format!("{e:#}"),
                    "schedule status: a model could not be loaded; rocky run builds nothing"
                );
            }
            let names: std::collections::BTreeSet<String> = models
                .into_iter()
                // `rocky run` never builds an ephemeral model: compile
                // inlines it into each consumer.
                .filter(|m| {
                    !matches!(
                        m.config.strategy,
                        rocky_core::models::StrategyConfig::Ephemeral
                    )
                })
                .map(|m| m.config.name)
                .collect();
            let cause = (!errors.is_empty()).then_some("a model fails to load");
            (names, cause)
        }
        Ok(crate::models_loader::ModelsDir::Absent(_)) => (
            std::collections::BTreeSet::new(),
            Some("the models directory does not exist"),
        ),
        Err(e) => {
            tracing::warn!(
                pipeline,
                error = %format!("{e:#}"),
                "schedule status: the models directory could not be located"
            );
            (
                std::collections::BTreeSet::new(),
                Some("the models directory cannot be located"),
            )
        }
    };
    let mut matched: Vec<&rocky_core::delivery::ProductDeadline> = deadlines
        .iter()
        .filter(|deadline| names.contains(&deadline.output_model))
        .collect();
    let Some(cause) = cause else {
        return Ok(Served {
            deadlines: matched,
            unreadable: None,
        });
    };
    for deadline in built_by_history(pipeline, store, deadlines)? {
        if !matched.iter().any(|held| std::ptr::eq(*held, deadline)) {
            matched.push(deadline);
        }
    }
    Ok(Served {
        deadlines: Vec::new(),
        unreadable: Some(Unreadable {
            cause,
            claimed: matched,
        }),
    })
}

/// The deadlines whose output model the newest production run of
/// `pipeline` that built any deadlined model built, within the newest
/// [`DELIVERY_LOOKUP_WINDOW`] runs. Empty without a store.
///
/// Not simply the newest run: a `--model` run, or a run that failed before
/// the output model, would otherwise make the pipeline read as delivering
/// nothing.
///
/// # Errors
///
/// A run-history read failure is an error.
fn built_by_history<'a>(
    pipeline: &str,
    store: Option<&StateStore>,
    deadlines: &'a [rocky_core::delivery::ProductDeadline],
) -> Result<Vec<&'a rocky_core::delivery::ProductDeadline>, ScheduleStatusError> {
    use rocky_core::state::UnrecordedScope;

    let Some(store) = store else {
        return Ok(Vec::new());
    };
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
        return Ok(Vec::new());
    };
    let built: std::collections::BTreeSet<&str> = run
        .models_executed
        .iter()
        .map(rocky_core::state::ModelExecution::rocky_model_name)
        .collect();
    Ok(deadlines
        .iter()
        .filter(|deadline| built.contains(deadline.output_model.as_str()))
        .collect())
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

    // Which deadlines each scheduled pipeline builds, before any status.
    let mut served: std::collections::BTreeMap<String, Served<'_>> =
        std::collections::BTreeMap::new();
    for name in scheduled.keys() {
        served.insert(
            name.clone(),
            served_deadlines(config, config_path, name, store, deadlines)?,
        );
    }
    // The warning of each unreadable pipeline: each counts as feeding a
    // deadline.
    let mut unreadable_warnings: std::collections::BTreeMap<String, String> =
        std::collections::BTreeMap::new();

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
            unreadable: None,
        });
        let delivery = pipeline_delivery(&name, store, &served_here.deadlines, now)?;
        // Fail closed: an unreadable pipeline always may serve a deadline.
        // Neither its models (some failed to load, or none could be read)
        // nor its history (a window, possibly empty) can show that it builds
        // no deadlined model, and another pipeline's build of a deadlined
        // model does not show it either.
        let serves_unknown = match &served_here.unreadable {
            None => false,
            Some(unreadable) if !unreadable.claimed.is_empty() => {
                let products: Vec<String> = unreadable
                    .claimed
                    .iter()
                    .map(|deadline| format!("`{}`", deadline.product))
                    .collect();
                unreadable_warnings.insert(
                    name.clone(),
                    format!(
                        "{}, so `rocky run` builds nothing for this pipeline; config or run \
                         history shows it building the output model of {}",
                        unreadable.cause,
                        products.join(", ")
                    ),
                );
                true
            }
            Some(unreadable) => {
                unreadable_warnings.insert(
                    name.clone(),
                    format!(
                        "{}, so `rocky run` builds nothing for this pipeline; a product \
                         declares `deliver_by`, and neither config nor run history can show \
                         that this pipeline does not build its output model",
                        unreadable.cause
                    ),
                );
                true
            }
        };
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
                serves_unknown,
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
    attach_latest_starts(&mut out, chain, unreadable_warnings, store, now)?;
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
    mut unreadable_warnings: std::collections::BTreeMap<String, String>,
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
    let starts = latest_starts(&chain);
    let fires: std::collections::BTreeMap<String, Fire> = pipelines
        .iter()
        .map(|p| {
            (
                p.pipeline.clone(),
                Fire {
                    has_cron: p.cron.is_some(),
                    next_fire_at: p.next_fire_at,
                    enabled: p.enabled,
                },
            )
        })
        .collect();
    let mut judge = Judge {
        starts: &starts,
        chain: &chain,
        fires: &fires,
        now,
        memo: std::collections::BTreeMap::new(),
        in_progress: std::collections::BTreeSet::new(),
    };
    for pipeline in pipelines {
        let Some(mut status) = judge.status(&pipeline.pipeline) else {
            continue;
        };
        if let Some(text) = unreadable_warnings.remove(&pipeline.pipeline) {
            status.warning = Some(match status.warning.take() {
                Some(cron) => format!("{text}; {cron}"),
                None => text,
            });
        }
        pipeline.latest_start = Some(status);
    }
    Ok(())
}

/// A scheduled pipeline's cron, as [`Judge`] reads it.
#[derive(Debug, Clone, Copy)]
struct Fire {
    /// Whether the pipeline has a cron at all. A cron awaiting its first
    /// anchor has one, with no `next_fire_at`.
    has_cron: bool,
    /// The projected next cron fire.
    next_fire_at: Option<DateTime<Utc>>,
    /// Whether the schedule is enabled and resolves.
    enabled: bool,
}

/// Judges each pipeline's cron against its latest start, upstreams first:
/// a late cron, or a pipeline with no cron and an `after` trigger, relies on
/// `after` only when every `after` upstream is itself judged in time.
struct Judge<'a> {
    starts: &'a std::collections::BTreeMap<String, rocky_core::delivery::LatestStart>,
    chain: &'a std::collections::BTreeMap<String, ChainNode>,
    /// Each scheduled pipeline's cron and whether it is enabled.
    fires: &'a std::collections::BTreeMap<String, Fire>,
    now: DateTime<Utc>,
    memo: std::collections::BTreeMap<String, LatestStartStatus>,
    in_progress: std::collections::BTreeSet<String>,
}

impl Judge<'_> {
    /// The latest start of `name` with its cron judged. `None` when it feeds
    /// no deadline, or on a cycle back to itself.
    fn status(&mut self, name: &str) -> Option<LatestStartStatus> {
        if let Some(done) = self.memo.get(name) {
            return Some(done.clone());
        }
        let latest = self.starts.get(name)?.clone();
        if !self.in_progress.insert(name.to_string()) {
            return None;
        }
        let upstreams: Vec<String> = self
            .chain
            .get(name)
            .map(|node| node.after.clone())
            .unwrap_or_default();
        let after = if upstreams.is_empty() {
            AfterTrigger::Absent
        } else {
            // An upstream is in time when it has a known latest start and a
            // cron judged in time, itself or through its own `after`. It is
            // known late when its `cron_check` is `too_late`, `overdue` or
            // `disabled`. Anything else (an unknown latest start, a
            // `not_judged` cron, no deadline or a cycle) cannot be judged.
            let mut untimely = Vec::new();
            let mut any_late = false;
            for upstream in upstreams {
                let status = self.status(&upstream);
                let in_time = status.as_ref().is_some_and(|status| {
                    status.state == LatestStartState::Known
                        && matches!(
                            status.cron_check,
                            CronCheck::OnTime | CronCheck::ReliesOnAfter
                        )
                });
                if in_time {
                    continue;
                }
                any_late |= status.is_some_and(|status| {
                    matches!(
                        status.cron_check,
                        CronCheck::TooLate | CronCheck::Overdue | CronCheck::Disabled
                    )
                });
                untimely.push(upstream);
            }
            if untimely.is_empty() {
                AfterTrigger::UpstreamsInTime
            } else if any_late {
                AfterTrigger::UpstreamsLate(untimely)
            } else {
                AfterTrigger::UpstreamsNotJudged(untimely)
            }
        };
        self.in_progress.remove(name);
        let fire = self.fires.get(name).copied().unwrap_or(Fire {
            has_cron: false,
            next_fire_at: None,
            enabled: false,
        });
        let status = LatestStartStatus::from_latest_start(
            latest,
            &CronContext {
                now: self.now,
                has_cron: fire.has_cron,
                next_fire_at: fire.next_fire_at,
                enabled: fire.enabled,
                after,
            },
        );
        self.memo.insert(name.to_string(), status.clone());
        Some(status)
    }
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

    /// Write model `name` (a `.sql` and a `.toml`) into `dir`. `extra_toml`
    /// is appended to the sidecar.
    fn write_model(dir: &Path, name: &str, extra_toml: &str) {
        write(&dir.join(format!("{name}.sql")), "SELECT 1 AS id\n");
        write(
            &dir.join(format!("{name}.toml")),
            &format!(
                "depends_on = []\n{extra_toml}\n[target]\ncatalog = \"p\"\nschema = \"s\"\n\
                 table = \"{name}\"\n"
            ),
        );
    }

    /// A project scheduling pipeline `revenue`, which builds model
    /// `revenue_daily`, whose product declares a 06:00 deadline, with the
    /// state store open for seeding.
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
        write_model(&root.join("models"), "revenue_daily", "");
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
        for (sub, model) in [
            ("raw", "raw_orders"),
            ("revenue", "revenue_daily"),
            ("costs", "costs_daily"),
        ] {
            write_model(&root.join("models").join(sub), model, "");
        }
        let mut toml = String::from("[adapter]\ntype = \"duckdb\"\npath = \"x.duckdb\"\n");
        for (name, schedule) in [
            ("raw", format!("cron = \"{raw_cron}\"\ntimezone = \"UTC\"")),
            ("revenue", "after = [\"raw\"]".to_string()),
            ("costs", "after = [\"raw\"]".to_string()),
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
        // `revenue` and `costs` have no cron: only `after` starts them, and
        // `raw` is on time, so they rely on it, with no warning.
        for name in ["revenue", "costs"] {
            let latest = &status[name]["latest_start"];
            assert_eq!(latest["cron_check"], "relies_on_after", "{name}: {latest}");
            assert_eq!(latest["cron_too_late"], false);
            assert!(latest.get("warning").is_none(), "{name}: {latest}");
        }
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

        // `revenue` and `costs` run only `after` `raw`, which is late, so
        // they are late too: warned, naming `raw`. They have no cron, so
        // `cron_too_late` stays false.
        for name in ["revenue", "costs"] {
            let latest = &status[name]["latest_start"];
            assert_eq!(latest["cron_check"], "too_late", "{name}: {latest}");
            assert_eq!(latest["cron_too_late"], false, "{name}: {latest}");
            let warning = latest["warning"].as_str().expect("a warning");
            assert!(warning.contains("has no cron"), "{warning}");
            assert!(warning.contains("`raw`"), "{warning}");
        }
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

    /// `revenue` has no cron and runs only `after` `raw`, whose latest start
    /// is unknown (`costs` ran once). Nothing is known to be late, so
    /// `revenue` is `not_judged`, with no warning and a note naming `raw`.
    /// Before, it read `too_late` with a warning.
    #[test]
    fn an_after_only_pipeline_below_an_unknown_upstream_is_not_judged() {
        let (_dir, config_path, state_path) = chain_project("0 3 * * *", (3, 0), 5..=5);
        let status = chain_status(&config_path, &state_path);
        assert_eq!(status["raw"]["latest_start"]["state"], "unknown");

        let latest = &status["revenue"]["latest_start"];
        assert_eq!(latest["state"], "known", "{latest}");
        assert_eq!(latest["cron_check"], "not_judged", "{latest}");
        assert_eq!(latest["cron_too_late"], false, "{latest}");
        assert!(latest.get("warning").is_none(), "{latest}");
        let note = latest["note"].as_str().expect("a note");
        assert!(note.contains("has no cron"), "{note}");
        assert!(note.contains("`raw` cannot be judged"), "{note}");
    }

    #[test]
    fn a_pipeline_that_delivers_no_product_reports_no_delivery() {
        let (dir, config_path, state_path, store) = project();
        drop(store);
        let models = dir.path().join("models");
        std::fs::remove_dir_all(&models).unwrap();
        write_model(&models, "other", "");
        let delivery = delivery_json(&config_path, &state_path, at(2, 5, 55));
        assert!(
            delivery.is_null(),
            "builds no output model: no delivery, got {delivery}"
        );
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
            write_model(&root.join("models").join(sub), model, "");
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

    /// Five production runs of `raw` (`raw_orders`, 03:00-03:20) and of
    /// `revenue` (`revenue_daily`, 03:20-03:30), on days 1 to 5.
    fn seed_old_history(store: &StateStore, root: &Path) {
        for day in 1..=5 {
            seed_run(
                store,
                root,
                "raw",
                "raw_orders",
                at(day, 3, 0),
                at(day, 3, 20),
            );
            seed_run(
                store,
                root,
                "revenue",
                "revenue_daily",
                at(day, 3, 20),
                at(day, 3, 30),
            );
        }
    }

    /// `revenue` built `revenue_daily` before, but `rocky run` builds
    /// nothing for it now. History never makes it serve the deadline: it
    /// has no `delivery`, and its latest start, and `raw`'s, are `unknown`
    /// with `models_unreadable`, and it carries a warning naming the
    /// product and `cause`.
    fn assert_models_unreadable(
        status: &std::collections::BTreeMap<String, serde_json::Value>,
        cause: &str,
    ) {
        let revenue = &status["revenue"];
        assert!(revenue["delivery"].is_null(), "{revenue}");
        let latest = &revenue["latest_start"];
        assert_eq!(latest["state"], "unknown", "{latest}");
        assert_eq!(latest["reasons"], serde_json::json!(["models_unreadable"]));
        assert!(latest.get("at").is_none(), "{latest}");
        let warning = latest["warning"].as_str().expect("a warning");
        assert!(warning.contains(cause), "{warning}");
        assert!(warning.contains("`revenue_daily`"), "{warning}");
        assert!(warning.contains("`rocky run` builds nothing"), "{warning}");
        let raw = &status["raw"]["latest_start"];
        assert_eq!(raw["state"], "unknown", "{raw}");
        assert_eq!(raw["reasons"], serde_json::json!(["models_unreadable"]));
    }

    #[test]
    fn an_absent_models_dir_with_old_history_reads_models_unreadable() {
        let (dir, config_path, state_path, store) = config_project(None);
        seed_old_history(&store, dir.path());
        drop(store);
        std::fs::remove_dir_all(dir.path().join("models/revenue")).unwrap();
        let status = chain_status(&config_path, &state_path);
        assert_models_unreadable(&status, "the models directory does not exist");
    }

    /// The output model still loads, but a sibling does not: `rocky run`
    /// builds nothing, so the config match is not served either.
    #[test]
    fn one_unloadable_model_with_old_history_reads_models_unreadable() {
        let (dir, config_path, state_path, store) =
            config_project(Some(("broken.toml", "this is not = = toml")));
        write(&dir.path().join("models/revenue/broken.sql"), "SELECT 1\n");
        seed_old_history(&store, dir.path());
        drop(store);
        let status = chain_status(&config_path, &state_path);
        assert_models_unreadable(&status, "a model fails to load");
    }

    /// The models directory is a link out of the project root: `rocky run`
    /// refuses it, so status does too.
    #[cfg(unix)]
    #[test]
    fn an_unlocatable_models_dir_with_old_history_reads_models_unreadable() {
        let (dir, config_path, state_path, store) = config_project(None);
        seed_old_history(&store, dir.path());
        drop(store);
        let outside = tempfile::tempdir().unwrap();
        write_model(outside.path(), "revenue_daily", "");
        let linked = dir.path().join("models/revenue");
        std::fs::remove_dir_all(&linked).unwrap();
        std::os::unix::fs::symlink(outside.path(), &linked).unwrap();
        let status = chain_status(&config_path, &state_path);
        assert_models_unreadable(&status, "the models directory cannot be located");
    }

    /// `rocky run` never builds an ephemeral model, so a pipeline whose
    /// output model is ephemeral does not serve its deadline.
    #[test]
    fn an_ephemeral_output_model_is_not_served() {
        let (dir, config_path, state_path, store) = config_project(None);
        write_model(
            &dir.path().join("models/revenue"),
            "revenue_daily",
            "\n[strategy]\ntype = \"ephemeral\"\n",
        );
        drop(store);
        let status = chain_status(&config_path, &state_path);
        let revenue = &status["revenue"];
        assert!(revenue["delivery"].is_null(), "{revenue}");
        assert!(revenue["latest_start"].is_null(), "{revenue}");
        assert!(status["raw"]["latest_start"].is_null(), "{}", status["raw"]);
    }

    /// `chain_project`, with a `55 5 * * *` cron on `revenue` as well as
    /// `after = ["raw"]`. `revenue`'s latest start is 06:00 - 10m = 05:50,
    /// so its next fire at 05:55 is late on its own.
    fn revenue_cron_status(
        raw_cron: &str,
        raw_slot: (u32, u32),
        costs_days: std::ops::RangeInclusive<u32>,
    ) -> serde_json::Value {
        let (_dir, config_path, state_path) = chain_project(raw_cron, raw_slot, costs_days);
        let toml = std::fs::read_to_string(&config_path).unwrap();
        let block = "[pipeline.revenue.schedule]\nafter = [\"raw\"]\n";
        assert!(toml.contains(block), "fixture layout changed");
        write(
            &config_path,
            &toml.replacen(
                block,
                &format!("{block}cron = \"55 5 * * *\"\ntimezone = \"UTC\"\n"),
                1,
            ),
        );
        let store = StateStore::open(&state_path).unwrap();
        store
            .put_schedule_state(
                "revenue",
                &ScheduleStateRecord {
                    last_fire_logical_ts: Some(at(5, 5, 55).to_rfc3339()),
                    ..ScheduleStateRecord::default()
                },
            )
            .unwrap();
        drop(store);
        let status = chain_status(&config_path, &state_path);
        assert_eq!(status["revenue"]["next_fire_at"], "2026-05-06T05:55:00Z");
        status["revenue"]["latest_start"].clone()
    }

    /// `raw` fires at 03:00, before its 03:10 latest start: `after` is
    /// relied on.
    #[test]
    fn a_late_cron_after_an_on_time_upstream_relies_on_after() {
        let latest = revenue_cron_status("0 3 * * *", (3, 0), 1..=5);
        assert_eq!(latest["at"], "2026-05-06T05:50:00Z", "{latest}");
        assert_eq!(latest["cron_check"], "relies_on_after");
        assert_eq!(latest["cron_too_late"], false);
        assert!(latest.get("warning").is_none(), "{latest}");
    }

    /// `raw` fires at 03:15, after its 03:10 latest start: it is
    /// `too_late`, so `after` cannot be relied on.
    #[test]
    fn a_late_cron_after_a_late_upstream_is_too_late() {
        let latest = revenue_cron_status("15 3 * * *", (3, 15), 1..=5);
        assert_eq!(latest["cron_check"], "too_late", "{latest}");
        assert_eq!(latest["cron_too_late"], true);
        assert!(latest["warning"].is_string(), "{latest}");
    }

    /// `raw`'s latest start is unknown (`costs` ran once), so `after`
    /// cannot be relied on. `revenue`'s own cron is known late, so it stays
    /// `too_late`, unlike a pipeline with no cron below the same upstream.
    #[test]
    fn a_late_cron_after_an_unknown_upstream_is_too_late() {
        let latest = revenue_cron_status("0 3 * * *", (3, 0), 5..=5);
        assert_eq!(latest["state"], "known", "{latest}");
        assert_eq!(latest["cron_check"], "too_late");
        assert_eq!(latest["cron_too_late"], true);
        let warning = latest["warning"].as_str().expect("a warning");
        assert!(
            warning.contains("the `after` trigger cannot be relied on: `raw`"),
            "{warning}"
        );
        assert!(latest.get("note").is_none(), "{latest}");
    }

    /// Three pipelines in a line: `raw` on cron `raw_cron` (20 min), `mid`
    /// with no cron, `after = ["raw"]` (10 min), and `revenue` on cron
    /// `55 5 * * *` and `after = ["mid"]` (10 min), which delivers
    /// `revenue_daily`, due 06:00. Latest starts: `revenue` 05:50, `mid`
    /// 05:40, `raw` 05:20. `revenue`'s 05:55 cron is late on its own. Its
    /// fire anchor is set only when `revenue_anchored`.
    fn line_status(
        raw_cron: &str,
        raw_slot: (u32, u32),
        revenue_anchored: bool,
        raw_extra: &str,
    ) -> std::collections::BTreeMap<String, serde_json::Value> {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(
            &root.join("products/revenue_daily.toml"),
            &spec_with_deadline("0 6 * * *"),
        );
        for (sub, model) in [
            ("raw", "raw_orders"),
            ("mid", "mid_orders"),
            ("revenue", "revenue_daily"),
        ] {
            write_model(&root.join("models").join(sub), model, "");
        }
        let mut toml = String::from("[adapter]\ntype = \"duckdb\"\npath = \"x.duckdb\"\n");
        for (name, schedule) in [
            (
                "raw",
                format!("cron = \"{raw_cron}\"\ntimezone = \"UTC\"{raw_extra}"),
            ),
            ("mid", "after = [\"raw\"]".to_string()),
            (
                "revenue",
                "after = [\"mid\"]\ncron = \"55 5 * * *\"\ntimezone = \"UTC\"".to_string(),
            ),
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
        for day in 1..=5 {
            for (pipeline, model, start, finish) in [
                ("raw", "raw_orders", at(day, 3, 0), at(day, 3, 20)),
                ("mid", "mid_orders", at(day, 3, 20), at(day, 3, 30)),
                ("revenue", "revenue_daily", at(day, 5, 0), at(day, 5, 10)),
            ] {
                seed_run(&store, root, pipeline, model, start, finish);
            }
        }
        let (hour, minute) = raw_slot;
        let mut anchors = vec![("raw", at(5, hour, minute))];
        if revenue_anchored {
            anchors.push(("revenue", at(5, 5, 55)));
        }
        for (pipeline, anchor) in anchors {
            store
                .put_schedule_state(
                    pipeline,
                    &ScheduleStateRecord {
                        last_fire_logical_ts: Some(anchor.to_rfc3339()),
                        ..ScheduleStateRecord::default()
                    },
                )
                .unwrap();
        }
        drop(store);
        let status = chain_status(&config_path, &state_path);
        for (name, at_) in [
            ("raw", "2026-05-06T05:20:00Z"),
            ("mid", "2026-05-06T05:40:00Z"),
            ("revenue", "2026-05-06T05:50:00Z"),
        ] {
            assert_eq!(
                status[name]["latest_start"]["at"], at_,
                "{name}: {}",
                status[name]
            );
        }
        status
    }

    /// The scheduler starts an after-only pipeline once its upstreams
    /// succeed. A 03:00 `raw` is on time, so `mid` relies on `after`, and so
    /// does `revenue` below it, though its own cron is late: no warning.
    /// Before, `mid` read `not_judged`, so `revenue` read `too_late`.
    #[test]
    fn an_after_only_pipeline_below_an_in_time_root_relies_on_after() {
        let status = line_status("0 3 * * *", (3, 0), true, "");
        assert_eq!(status["raw"]["latest_start"]["cron_check"], "on_time");
        for name in ["mid", "revenue"] {
            let latest = &status[name]["latest_start"];
            assert_eq!(latest["cron_check"], "relies_on_after", "{name}: {latest}");
            assert_eq!(latest["cron_too_late"], false, "{name}: {latest}");
            assert!(latest.get("warning").is_none(), "{name}: {latest}");
        }
        let note = status["mid"]["latest_start"]["note"]
            .as_str()
            .expect("a note");
        assert!(note.contains("has no cron"), "{note}");
    }

    /// A 05:30 `raw` fires after its 05:20 latest start: `mid`, which only
    /// runs `after` it, is late too, and `revenue` below `mid` cannot rely
    /// on `after`. Each warning names the upstream that is not in time.
    #[test]
    fn an_after_only_pipeline_below_a_late_root_is_too_late() {
        let status = line_status("30 5 * * *", (5, 30), true, "");
        assert_eq!(status["raw"]["latest_start"]["cron_check"], "too_late");

        let mid = &status["mid"]["latest_start"];
        assert_eq!(mid["cron_check"], "too_late", "{mid}");
        assert_eq!(mid["cron_too_late"], false, "no cron to be late: {mid}");
        let warning = mid["warning"].as_str().expect("a warning");
        assert!(warning.contains("has no cron"), "{warning}");
        assert!(warning.contains("`raw`"), "{warning}");

        let revenue = &status["revenue"]["latest_start"];
        assert_eq!(revenue["cron_check"], "too_late", "{revenue}");
        assert_eq!(revenue["cron_too_late"], true, "{revenue}");
        let warning = revenue["warning"].as_str().expect("a warning");
        assert!(
            warning.contains("the `after` trigger cannot be relied on: `mid`"),
            "{warning}"
        );
    }

    /// A disabled `raw` will not start: `mid`, which runs only `after` it,
    /// is `too_late`, and `revenue` cannot rely on `after` either.
    #[test]
    fn an_after_only_pipeline_below_a_disabled_root_is_too_late() {
        let status = line_status("0 3 * * *", (3, 0), true, "\nenabled = false");
        assert_eq!(status["raw"]["latest_start"]["cron_check"], "disabled");
        let mid = &status["mid"]["latest_start"];
        assert_eq!(mid["cron_check"], "too_late", "{mid}");
        let warning = mid["warning"].as_str().expect("a warning");
        assert!(warning.contains("not in time: `raw`;"), "{warning}");
        let revenue = &status["revenue"]["latest_start"];
        assert_eq!(revenue["cron_check"], "too_late", "{revenue}");
    }

    /// `revenue` has a cron that has not fired yet: no fire anchor, so no
    /// `next_fire_at`. It is `not_judged`, not judged by its `after` as a
    /// pipeline with no cron would be, though its upstreams are in time.
    #[test]
    fn a_cron_awaiting_its_first_anchor_is_not_judged() {
        let status = line_status("0 3 * * *", (3, 0), false, "");
        let revenue = &status["revenue"];
        assert_eq!(revenue["awaiting_first_anchor"], true, "{revenue}");
        assert!(revenue["next_fire_at"].is_null(), "{revenue}");
        let latest = &revenue["latest_start"];
        assert_eq!(latest["cron_check"], "not_judged", "{latest}");
        assert_eq!(latest["cron_too_late"], false);
        assert!(latest.get("warning").is_none() && latest.get("note").is_none());
        assert_eq!(
            status["mid"]["latest_start"]["cron_check"],
            "relies_on_after"
        );
    }

    /// A pipeline with neither a cron nor `after` runs by hand: nothing
    /// starts it, so nothing to judge.
    #[test]
    fn a_manual_pipeline_is_not_judged() {
        let (dir, config_path, state_path, store) = project();
        seed_five_on_time(&store, dir.path());
        drop(store);
        let toml = std::fs::read_to_string(&config_path).unwrap();
        let block = "cron = \"0 5 * * *\"\ntimezone = \"UTC\"\n";
        assert!(toml.contains(block), "fixture layout changed");
        write(&config_path, &toml.replacen(block, "enabled = true\n", 1));
        let status = chain_status(&config_path, &state_path);
        let revenue = &status["revenue"];
        assert!(revenue["cron"].is_null(), "{revenue}");
        assert!(
            revenue["after"]
                .as_array()
                .is_none_or(std::vec::Vec::is_empty),
            "{revenue}"
        );
        let latest = &revenue["latest_start"];
        assert_eq!(latest["state"], "known", "{latest}");
        assert_eq!(latest["cron_check"], "not_judged");
        assert_eq!(latest["cron_too_late"], false);
        assert!(latest.get("warning").is_none() && latest.get("note").is_none());
    }

    /// Two unreadable pipelines: `revenue`, whose history built
    /// `revenue_daily`, and `other`, with no history at all. History alone
    /// placed the deadline at `revenue`, which then counted as placed, so
    /// `other` read clean. Neither can be ruled out: both are flagged.
    #[test]
    fn every_unreadable_pipeline_is_flagged() {
        let (dir, config_path, state_path, store) = config_project(None);
        seed_old_history(&store, dir.path());
        drop(store);
        std::fs::remove_dir_all(dir.path().join("models/revenue")).unwrap();
        let mut toml = std::fs::read_to_string(&config_path).unwrap();
        toml.push_str(
            "\n[pipeline.other]\ntype = \"transformation\"\nmodels = \"models/other/**\"\n\n\
             [pipeline.other.target.governance]\nauto_create_schemas = true\n\n\
             [pipeline.other.schedule]\ncron = \"0 4 * * *\"\ntimezone = \"UTC\"\n",
        );
        write(&config_path, &toml);
        let status = chain_status(&config_path, &state_path);

        assert_models_unreadable(&status, "the models directory does not exist");
        let other = &status["other"];
        assert!(other["delivery"].is_null(), "{other}");
        let latest = &other["latest_start"];
        assert_eq!(latest["state"], "unknown", "{other}");
        // `other` has no runs either, so `missing_estimate` too.
        assert!(
            latest["reasons"]
                .as_array()
                .expect("reasons")
                .contains(&serde_json::json!("models_unreadable")),
            "{latest}"
        );
        let warning = latest["warning"].as_str().expect("a warning");
        assert!(
            warning.contains("the models directory does not exist"),
            "{warning}"
        );
        assert!(warning.contains("`rocky run` builds nothing"), "{warning}");
    }
}

/// The cron verdict against a latest start. Pure: no store, no clock.
#[cfg(test)]
mod cron_check_tests {
    use crate::output::{AfterTrigger, CronCheck, CronContext, LatestStartStatus};
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

    fn ctx(fire: Option<DateTime<Utc>>, enabled: bool, after: AfterTrigger) -> CronContext {
        CronContext {
            now: t(1, 0, 0),
            has_cron: true,
            next_fire_at: fire,
            enabled,
            after,
        }
    }

    /// An overdue cron whose latest start has already passed is too late as
    /// well: the missed slot can no longer meet the deadline.
    #[test]
    fn an_overdue_cron_past_its_latest_start_is_too_late() {
        let status = LatestStartStatus::from_latest_start(
            known(),
            &CronContext {
                now: t(3, 30, 0),
                has_cron: true,
                next_fire_at: Some(t(3, 0, 0)),
                enabled: true,
                after: AfterTrigger::Absent,
            },
        );
        assert_eq!(status.cron_check, CronCheck::Overdue);
        assert!(status.cron_too_late);
        assert!(status.warning.is_some());
    }

    /// An overdue cron whose latest start is still ahead is not too late:
    /// a catch-up fire now can still meet it.
    #[test]
    fn an_overdue_cron_before_its_latest_start_is_not_too_late() {
        let status = LatestStartStatus::from_latest_start(
            known(),
            &CronContext {
                now: t(3, 5, 0),
                has_cron: true,
                next_fire_at: Some(t(3, 0, 0)),
                enabled: true,
                after: AfterTrigger::Absent,
            },
        );
        assert_eq!(status.cron_check, CronCheck::Overdue);
        assert!(!status.cron_too_late);
        assert!(status.warning.is_some());
    }

    /// A pipeline with no cron, below an upstream that is known late, is
    /// `too_late` with a warning naming it; one below an upstream that
    /// cannot be judged is `not_judged`, with a note naming it.
    #[test]
    fn an_after_only_pipeline_is_judged_by_its_upstreams() {
        let no_cron = |after| CronContext {
            now: t(1, 0, 0),
            has_cron: false,
            next_fire_at: None,
            enabled: true,
            after,
        };
        let late = LatestStartStatus::from_latest_start(
            known(),
            &no_cron(AfterTrigger::UpstreamsLate(vec!["raw".to_string()])),
        );
        assert_eq!(late.cron_check, CronCheck::TooLate);
        assert!(!late.cron_too_late);
        let warning = late.warning.expect("a warning");
        assert!(warning.contains("`raw`"), "{warning}");
        assert!(late.note.is_none());

        let unjudged = LatestStartStatus::from_latest_start(
            known(),
            &no_cron(AfterTrigger::UpstreamsNotJudged(vec!["raw".to_string()])),
        );
        assert_eq!(unjudged.cron_check, CronCheck::NotJudged);
        assert!(!unjudged.cron_too_late);
        assert!(unjudged.warning.is_none());
        let note = unjudged.note.expect("a note");
        assert!(note.contains("`raw` cannot be judged"), "{note}");
    }

    /// A pipeline with no cron whose own latest start is unknown still reads
    /// `too_late` below an upstream known late (a disabled one, say), and
    /// still carries the note below an upstream that cannot be judged.
    #[test]
    fn an_after_only_pipeline_with_an_unknown_latest_start_keeps_its_upstream_verdict() {
        let unknown = || LatestStart::Unknown {
            reasons: vec![UnknownReason::MissingEstimate],
            missing_estimate: vec!["mid".to_string()],
        };
        let no_cron = |after| CronContext {
            now: t(1, 0, 0),
            has_cron: false,
            next_fire_at: None,
            enabled: true,
            after,
        };
        let late = LatestStartStatus::from_latest_start(
            unknown(),
            &no_cron(AfterTrigger::UpstreamsLate(vec!["raw".to_string()])),
        );
        assert_eq!(late.cron_check, CronCheck::TooLate);
        assert!(!late.cron_too_late);
        let warning = late.warning.expect("a warning");
        assert!(warning.contains("not in time: `raw`;"), "{warning}");

        let unjudged = LatestStartStatus::from_latest_start(
            unknown(),
            &no_cron(AfterTrigger::UpstreamsNotJudged(vec!["raw".to_string()])),
        );
        assert_eq!(unjudged.cron_check, CronCheck::NotJudged);
        assert!(unjudged.warning.is_none());
        let note = unjudged.note.expect("a note");
        assert!(note.contains("`raw` cannot be judged"), "{note}");
    }

    /// A late cron whose `after` upstream cannot be judged stays
    /// `too_late`: the cron is known late, and `after` cannot be relied on.
    #[test]
    fn a_late_cron_with_an_unjudged_upstream_is_too_late() {
        let status = LatestStartStatus::from_latest_start(
            known(),
            &ctx(
                Some(t(3, 15, 0)),
                true,
                AfterTrigger::UpstreamsNotJudged(vec!["raw".to_string()]),
            ),
        );
        assert_eq!(status.cron_check, CronCheck::TooLate);
        assert!(status.cron_too_late);
        let warning = status.warning.expect("a warning");
        assert!(
            warning.contains("the `after` trigger cannot be relied on: `raw`"),
            "{warning}"
        );
        assert!(status.note.is_none());
    }

    /// A late cron with `after` whose upstreams are not all judged in time
    /// is `too_late`, not `relies_on_after`.
    #[test]
    fn a_late_cron_with_an_untimely_upstream_is_too_late() {
        let status = LatestStartStatus::from_latest_start(
            known(),
            &ctx(
                Some(t(3, 15, 0)),
                true,
                AfterTrigger::UpstreamsLate(vec!["raw".to_string()]),
            ),
        );
        assert_eq!(status.cron_check, CronCheck::TooLate);
        assert!(status.cron_too_late);
        let warning = status.warning.expect("a warning");
        assert!(
            warning.contains("the `after` trigger cannot be relied on: `raw`"),
            "{warning}"
        );
        assert!(status.note.is_none());
    }

    /// The docs say "later than `at`": a fire exactly at `at` is in time.
    #[test]
    fn a_fire_exactly_at_the_latest_start_is_on_time() {
        let status = LatestStartStatus::from_latest_start(
            known(),
            &ctx(Some(t(3, 10, 0)), true, AfterTrigger::Absent),
        );
        assert_eq!(status.cron_check, CronCheck::OnTime);
        assert!(!status.cron_too_late);
        assert!(status.warning.is_none() && status.note.is_none());

        let status = LatestStartStatus::from_latest_start(
            known(),
            &ctx(Some(t(3, 10, 1)), true, AfterTrigger::Absent),
        );
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
        let status = LatestStartStatus::from_latest_start(
            known(),
            &ctx(Some(t(3, 15, 0)), true, AfterTrigger::UpstreamsInTime),
        );
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
        let status = LatestStartStatus::from_latest_start(
            known(),
            &ctx(Some(t(3, 0, 0)), true, AfterTrigger::UpstreamsInTime),
        );
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
        let status =
            LatestStartStatus::from_latest_start(known(), &ctx(missed, true, AfterTrigger::Absent));
        assert_eq!(status.cron_check, CronCheck::Overdue);
        assert!(!status.cron_too_late);
        let warning = status.warning.expect("a warning");
        assert!(warning.contains("the cron is overdue"), "{warning}");

        let unknown = LatestStart::Unknown {
            reasons: vec![UnknownReason::MissingEstimate],
            missing_estimate: vec!["raw".to_string()],
        };
        let status =
            LatestStartStatus::from_latest_start(unknown, &ctx(missed, true, AfterTrigger::Absent));
        assert_eq!(status.cron_check, CronCheck::Overdue);
        assert!(status.warning.is_some());

        let status = LatestStartStatus::from_latest_start(
            known(),
            &ctx(Some(now), true, AfterTrigger::Absent),
        );
        assert_eq!(status.cron_check, CronCheck::OnTime);
    }

    /// A disabled schedule never fires: `disabled`, with a warning, and no
    /// cron verdict.
    #[test]
    fn a_disabled_schedule_is_reported_as_disabled() {
        let status =
            LatestStartStatus::from_latest_start(known(), &ctx(None, false, AfterTrigger::Absent));
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
        let status = LatestStartStatus::from_latest_start(
            unknown,
            &ctx(Some(t(23, 0, 0)), true, AfterTrigger::Absent),
        );
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
