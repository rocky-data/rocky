//! `rocky tick` — one demand-reconciliation pass (experimental).
//!
//! A tick evaluates every scheduled pipeline's standing demand once (`cron`,
//! `after`, `freshness`), runs what is due through child `rocky run` processes,
//! records scheduler state, and emits a typed [`TickOutput`]. There is no
//! daemon: the tick comes from an external timer (cron, systemd, CI). The
//! reconciler core lives in `rocky-core` and reads no wall clock — this wrapper
//! is the only place the clock is consulted (via `--now`, else `Utc::now()`).
//!
//! This layer is deliberately thin: it loads config + state, resolves the
//! per-pipeline member-model freshness budgets the core cannot load itself
//! (model files live behind the CLI), calls
//! [`tick_once`](rocky_core::schedule::tick_once), and maps the reconciler's
//! [`TickReport`] onto the wire [`TickOutput`]. Exit codes mirror `rocky run`:
//! `0` nothing-due-or-all-succeeded, `2` at least one executed run failed or was
//! partial, `1` tick infrastructure error (config invalid, state unopenable).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};

use rocky_core::config::{RockyConfig, TransformationPipelineConfig, load_rocky_config};
use rocky_core::schedule::demand::{SkipReason, SourceSkip};
use rocky_core::schedule::{
    DemandKind, Drain, SubprocessSpawner, TerminalOutcome, TickOptions, TickReport, TickSkipReason,
    tick_once,
};

use crate::commands::run::PartialFailure;
use crate::output::{
    ExecutedRunOutput, PipelineDemandStatus, SkippedDemandOutput, SourceEvaluation, TickCounts,
    TickOutput, print_json,
};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Run one demand-reconciliation tick.
///
/// `now_arg` is the injected evaluation instant (`--now`); `None` reads the wall
/// clock — the sole clock read in the whole reconciler. A `PartialFailure`
/// sentinel is returned (after the JSON is emitted) when any executed run failed
/// or was partial, so `rocky/src/main.rs` maps it to exit 2; a genuine infra
/// fault returns an ordinary error mapped to exit 1.
///
/// # Errors
///
/// Returns an error (exit 1) when the config cannot be loaded, the selected
/// pipeline does not exist, the state store cannot be opened, or the tick itself
/// faults (state I/O, lock I/O, an `after` cycle). Every such case runs nothing
/// — fail closed.
pub async fn run_tick(
    config_path: &Path,
    state_path: &Path,
    dry_run: bool,
    pipeline: Option<String>,
    now_arg: Option<String>,
    json: bool,
) -> Result<()> {
    // An invalid `--now` is an input error ⇒ exit 1, nothing runs.
    let now_arg = match now_arg {
        Some(raw) => Some(
            DateTime::parse_from_rfc3339(&raw)
                .map(|dt| dt.with_timezone(&Utc))
                .with_context(|| format!("invalid --now '{raw}': expected an RFC3339 timestamp"))?,
        ),
        None => None,
    };

    // Config parse error ⇒ exit 1, nothing runs.
    let config = load_rocky_config(config_path)
        .with_context(|| format!("failed to load config from {}", config_path.display()))?;
    if let Some(name) = pipeline.as_deref() {
        crate::registry::resolve_pipeline(&config, Some(name))?;
    }

    // The only wall-clock read in the reconciler. Everything downstream takes
    // this instant as a parameter.
    let now = now_arg.unwrap_or_else(Utc::now);

    // State open error ⇒ exit 1, nothing runs. Create the parent directory so a
    // first-ever tick on a fresh project can open (and create) the store.
    if let Some(parent) = state_path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create state directory {}", parent.display()))?;
    }
    // The reconciler opens the store itself (it releases the flock around each
    // child spawn), so the CLI passes the resolved path. Absolutize it (lexical,
    // no I/O) so the child — forwarded this exact path via `--state-path` — opens
    // the same file regardless of any cwd difference.
    let state_path = std::path::absolute(state_path).unwrap_or_else(|_| state_path.to_path_buf());

    // Per-pipeline member-model freshness budgets (F3). The reconciler core
    // loads no model files; the CLI resolves each freshness-scheduled
    // transformation pipeline's members and passes the tightest budgets in. A
    // pipeline with no entry falls back to the project `[freshness]` default —
    // the exact pre-wiring behavior.
    let member_budgets = build_member_budgets(&config, config_path, pipeline.as_deref());

    // The `.rocky` directory holding the tick lock is anchored to the config
    // file's directory (the project root), NOT the process cwd — two ticks
    // launched from different cwds must contend on the same lock. It is also
    // where the P3 webhook spool will live.
    let rocky_dir = config_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .join(".rocky");

    let opts = TickOptions {
        dry_run,
        pipeline_filter: pipeline,
        config_path: config_path.to_path_buf(),
        rocky_dir,
        // Connected orchestrator→engine tracing is still blocked on the CHILD
        // side: `extract_remote_context` has no call site at the `rocky run`
        // entry point, so a `TRACEPARENT` handed to the child is not read and
        // its run trace stands alone. (`serve --scheduler` does emit a
        // `scheduler.tick` span; `rocky tick` is one-shot and has no loop to
        // anchor one to.)
        traceparent: None,
        member_budgets,
        state_path: state_path.clone(),
        // `rocky tick` is one-shot: there is no resident loop to drain, so the
        // reconciler evaluates every scheduled pipeline. Serve mode supplies a
        // real drain (see `commands::scheduler`).
        drain: Drain::default(),
    };

    let spawner = SubprocessSpawner::new();
    let start = Instant::now();
    // Any TickError (state fault, lock I/O fault, `after` cycle) is an infra
    // error ⇒ exit 1 with nothing run. Lock contention with a live `rocky run`
    // is NOT an error — the reconciler reports it as a `state_busy` skip, exit 0.
    let report = tick_once(&config, &state_path, now, &spawner, &opts)
        .await
        .context("tick failed")?;
    let duration_ms = start.elapsed().as_millis() as u64;

    let output = build_tick_output(&report, now, dry_run, duration_ms);

    if json {
        print_json(&output)?;
    } else {
        render_text(&output);
    }

    // Exit 2 when any executed run failed or was partial (mirrors `rocky run`).
    // The JSON has already been emitted, so a wrapper parsing stdout still sees
    // the full report before the process exits non-zero.
    let unhealthy = output.counts.failed + output.counts.partial;
    if unhealthy > 0 {
        let run_id = output
            .executed
            .iter()
            .find(|e| e.outcome != "success")
            .map(|e| e.submission_id.clone())
            .unwrap_or_default();
        return Err(PartialFailure {
            count: unhealthy,
            run_id,
        }
        .into());
    }

    Ok(())
}

/// Resolve each freshness-scheduled transformation pipeline's member-model
/// budgets (`max_lag_seconds`), keyed by pipeline name.
///
/// Only transformation pipelines have member models, and only `freshness = true`
/// pipelines consult the budget — so nothing else is loaded. A pipeline whose
/// models cannot be loaded (missing dir, read fault) is simply omitted, so it
/// falls back to the project default rather than aborting the tick; the
/// `V043`-class validation warning is the pre-tick signal for that gap.
pub(crate) fn build_member_budgets(
    config: &RockyConfig,
    config_path: &Path,
    filter: Option<&str>,
) -> BTreeMap<String, Vec<u64>> {
    let mut budgets = BTreeMap::new();
    for (name, pipeline) in &config.pipelines {
        if let Some(f) = filter
            && name.as_str() != f
        {
            continue;
        }
        let Some(schedule) = pipeline.schedule() else {
            continue;
        };
        if !schedule.freshness {
            continue;
        }
        let Some(tx) = pipeline.as_transformation() else {
            continue;
        };
        match member_max_lags(tx, config_path) {
            Ok(lags) if !lags.is_empty() => {
                budgets.insert(name.clone(), lags);
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(
                    pipeline = %name,
                    error = %e,
                    "could not load member models for the freshness budget; \
                     falling back to the project default"
                );
            }
        }
    }
    budgets
}

/// The `max_lag_seconds` of every member model that declares its own freshness
/// block. Models without one inherit the project default, which is already the
/// reconciler's fallback — so only own-declared budgets count as member lags.
fn member_max_lags(tx: &TransformationPipelineConfig, config_path: &Path) -> Result<Vec<u64>> {
    let Some(models_dir) = resolve_models_dir(&tx.models, config_path)? else {
        return Ok(Vec::new());
    };
    let models = crate::models_loader::load_project_models(&models_dir)?;
    Ok(models
        .iter()
        .filter_map(|m| m.config.freshness.as_ref().map(|f| f.max_lag_seconds))
        .collect())
}

/// Resolve a transformation pipeline's `models` glob to its base directory,
/// confined to the project root. Returns `None` when the directory does not
/// exist. Mirrors `scope.rs`'s containment check: a `models = "../../etc"`
/// escape must never read outside the project tree.
fn resolve_models_dir(models_glob: &str, config_path: &Path) -> Result<Option<PathBuf>> {
    // `Path::new("rocky.toml").parent()` is `Some("")`, not `None`, and an empty
    // path fails to canonicalize — normalize it to the cwd so a relative default
    // config (the common case) still resolves its models.
    let project_root = config_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let models_base = models_glob
        .split(&['*', '?', '['][..])
        .next()
        .unwrap_or("models");
    let models_dir = project_root.join(models_base.trim_end_matches('/'));
    if !models_dir.exists() {
        return Ok(None);
    }
    // Confine to the project root. Both sides canonicalized so intra-project
    // symlinks resolve before the prefix check (macOS `/tmp` is itself a
    // symlink, so asymmetric resolution would false-reject).
    let canonical_root = project_root.canonicalize().with_context(|| {
        format!(
            "project root '{}' could not be resolved",
            project_root.display()
        )
    })?;
    if let Ok(canonical_models) = models_dir.canonicalize()
        && !canonical_models.starts_with(&canonical_root)
    {
        anyhow::bail!(
            "models directory '{}' resolves outside the project root '{}'",
            canonical_models.display(),
            canonical_root.display(),
        );
    }
    Ok(Some(models_dir))
}

/// Map the reconciler's [`TickReport`] onto the wire [`TickOutput`].
///
/// Pure: no I/O, no clock — every value comes from `report`, `now`, `dry_run`,
/// or `duration_ms`, so the mapping is unit-testable without running a tick.
fn build_tick_output(
    report: &TickReport,
    now: DateTime<Utc>,
    dry_run: bool,
    duration_ms: u64,
) -> TickOutput {
    let evaluated: Vec<PipelineDemandStatus> = report
        .evaluated
        .iter()
        .map(|ev| {
            let mut sources: Vec<SourceEvaluation> = ev.skips.iter().map(map_source_skip).collect();
            let (due, due_source, logical_ts) = match &ev.due {
                Some(d) => {
                    sources.push(SourceEvaluation {
                        source: demand_kind_str(d.source).to_string(),
                        reason: "due".to_string(),
                        resume_at: None,
                        missed: None,
                    });
                    (
                        true,
                        Some(demand_kind_str(d.source).to_string()),
                        Some(d.logical_ts),
                    )
                }
                None => (false, None, None),
            };
            PipelineDemandStatus {
                pipeline: ev.pipeline.clone(),
                due,
                due_source,
                logical_ts,
                sources,
            }
        })
        .collect();

    let executed: Vec<ExecutedRunOutput> = report
        .executed
        .iter()
        .map(|e| ExecutedRunOutput {
            pipeline: e.pipeline.clone(),
            source: demand_kind_str(e.source).to_string(),
            logical_ts: e.logical_ts,
            submission_id: e.submission_id.clone(),
            exit_code: e.exit_code,
            outcome: terminal_outcome_str(e.outcome).to_string(),
            attempts: e.attempts,
        })
        .collect();

    let mut skipped: Vec<SkippedDemandOutput> = report
        .skipped
        .iter()
        .map(|s| {
            let (reason, resume_at, missed) = map_tick_skip_reason(&s.reason);
            SkippedDemandOutput {
                pipeline: Some(s.pipeline.clone()),
                source: s.source.map(|k| demand_kind_str(k).to_string()),
                reason: reason.to_string(),
                resume_at,
                missed,
            }
        })
        .collect();

    // A whole-tick skip (a live reconciler already held the lock) is a
    // pipeline-less `tick_in_progress` entry, preserving the flat skip-reason
    // contract rather than a parallel top-level flag.
    if report.skipped_whole_tick {
        skipped.push(SkippedDemandOutput {
            pipeline: None,
            source: None,
            reason: "tick_in_progress".to_string(),
            resume_at: None,
            missed: None,
        });
    }

    // The store was held by another `rocky` process. When it could not be opened
    // at all, no per-demand skip exists yet, so synthesize a pipeline-less
    // `state_busy` entry; a mid-tick reopen contention already pushed a
    // pipeline-scoped `state_busy` skip, so don't duplicate it.
    if report.state_busy && !skipped.iter().any(|s| s.reason == "state_busy") {
        skipped.push(SkippedDemandOutput {
            pipeline: None,
            source: None,
            reason: "state_busy".to_string(),
            resume_at: None,
            missed: None,
        });
    }

    let succeeded = executed.iter().filter(|e| e.outcome == "success").count();
    let partial = executed.iter().filter(|e| e.outcome == "partial").count();
    let failed = executed.iter().filter(|e| e.outcome == "failure").count();
    let due = evaluated.iter().filter(|p| p.due).count();

    let counts = TickCounts {
        evaluated: evaluated.len(),
        due,
        executed: executed.len(),
        succeeded,
        partial,
        failed,
        skipped: skipped.len(),
    };

    TickOutput {
        version: VERSION.to_string(),
        command: "tick".to_string(),
        dry_run,
        now,
        evaluated,
        executed,
        skipped,
        tick_in_progress: report.skipped_whole_tick,
        lock_overridden: report.lock_overridden,
        counts,
        duration_ms,
    }
}

/// Map a per-source [`SourceSkip`] onto the wire [`SourceEvaluation`].
fn map_source_skip(skip: &SourceSkip) -> SourceEvaluation {
    let (reason, resume_at, missed) = match &skip.reason {
        SkipReason::NotDue => ("not_due", None, None),
        SkipReason::Disabled => ("disabled", None, None),
        SkipReason::CatchupSkipped { missed } => ("catchup_skipped", None, Some(*missed)),
        SkipReason::FailureBackoff { resume_at } => ("failure_backoff", Some(*resume_at), None),
        SkipReason::PartialBackoff { resume_at } => ("partial_backoff", Some(*resume_at), None),
        SkipReason::Superseded => ("superseded", None, None),
        SkipReason::HistoryError => ("history_unavailable", None, None),
    };
    SourceEvaluation {
        source: demand_kind_str(skip.source).to_string(),
        reason: reason.to_string(),
        resume_at,
        missed,
    }
}

/// Map a [`TickSkipReason`] onto its wire string plus any structured detail.
fn map_tick_skip_reason(
    reason: &TickSkipReason,
) -> (&'static str, Option<DateTime<Utc>>, Option<u32>) {
    match reason {
        TickSkipReason::NotDue => ("not_due", None, None),
        TickSkipReason::Disabled => ("disabled", None, None),
        TickSkipReason::InFlight => ("in_flight", None, None),
        TickSkipReason::CatchupSkipped { missed } => ("catchup_skipped", None, Some(*missed)),
        TickSkipReason::FailureBackoff { resume_at } => ("failure_backoff", Some(*resume_at), None),
        TickSkipReason::PartialBackoff { resume_at } => ("partial_backoff", Some(*resume_at), None),
        TickSkipReason::Dedup => ("dedup", None, None),
        TickSkipReason::HistoryUnavailable => ("history_unavailable", None, None),
        TickSkipReason::StateBusy => ("state_busy", None, None),
    }
}

/// Stable wire string for a demand source.
fn demand_kind_str(kind: DemandKind) -> &'static str {
    match kind {
        DemandKind::Cron => "cron",
        DemandKind::After => "after",
        DemandKind::Freshness => "freshness",
        DemandKind::Webhook => "webhook",
    }
}

/// Stable wire string for a terminal run outcome.
fn terminal_outcome_str(outcome: TerminalOutcome) -> &'static str {
    match outcome {
        TerminalOutcome::Success => "success",
        TerminalOutcome::Partial => "partial",
        TerminalOutcome::Failure => "failure",
    }
}

/// Human-readable one-shot summary for `--output text`.
fn render_text(o: &TickOutput) {
    let mode = if o.dry_run { " (dry-run)" } else { "" };
    println!(
        "tick{mode}: {} evaluated, {} due, {} executed, {} skipped",
        o.counts.evaluated, o.counts.due, o.counts.executed, o.counts.skipped
    );
    if o.tick_in_progress {
        println!("  skipped: another reconciler holds the tick lock (tick_in_progress)");
        return;
    }
    if o.lock_overridden {
        println!("  note: proceeded via wedge override (prior reconciler's lock heartbeat stale)");
    }
    for e in &o.executed {
        println!(
            "  ran {} ({}) → {} (exit {}, attempts {})",
            e.pipeline, e.source, e.outcome, e.exit_code, e.attempts
        );
    }
    for s in &o.skipped {
        let who = s.pipeline.as_deref().unwrap_or("<tick>");
        let src = s
            .source
            .as_deref()
            .map(|x| format!(" {x}"))
            .unwrap_or_default();
        let resume = s
            .resume_at
            .map(|t| format!(" (resume {})", t.to_rfc3339()))
            .unwrap_or_default();
        println!("  skipped {who}{src}: {}{resume}", s.reason);
    }
    if o.counts.failed + o.counts.partial > 0 {
        println!(
            "  {} run(s) failed or were partial — exit 2",
            o.counts.failed + o.counts.partial
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_core::schedule::demand::EvaluatedPipeline;
    use rocky_core::schedule::{Demand, ExecutedDemand, SkippedDemand};

    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[tokio::test]
    async fn unknown_pipeline_filter_fails_before_state_open() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("rocky.toml");
        let state_path = dir.path().join("state.redb");
        std::fs::write(
            &config_path,
            r#"
[adapter.db]
type = "duckdb"

[pipeline.raw]
type = "transformation"
[pipeline.raw.target]
adapter = "db"
[pipeline.raw.schedule]
cron = "0 3 * * *"
"#,
        )
        .unwrap();

        let error = run_tick(
            &config_path,
            &state_path,
            true,
            Some("typo".to_string()),
            Some("2026-07-18T03:00:00Z".to_string()),
            false,
        )
        .await
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("pipeline 'typo' not found in config")
        );
        assert!(!state_path.exists());
    }

    #[test]
    fn maps_executed_skipped_and_counts() {
        let report = TickReport {
            evaluated: vec![
                EvaluatedPipeline {
                    pipeline: "raw".to_string(),
                    due: Some(Demand {
                        pipeline: "raw".to_string(),
                        source: DemandKind::Cron,
                        logical_ts: ts("2026-05-02T03:00:00Z"),
                    }),
                    anchor_init: None,
                    catchup_advance: None,
                    skips: vec![],
                },
                EvaluatedPipeline {
                    pipeline: "staging".to_string(),
                    due: None,
                    anchor_init: None,
                    catchup_advance: None,
                    skips: vec![SourceSkip {
                        source: DemandKind::After,
                        reason: SkipReason::NotDue,
                    }],
                },
            ],
            executed: vec![ExecutedDemand {
                pipeline: "raw".to_string(),
                source: DemandKind::Cron,
                logical_ts: ts("2026-05-02T03:00:00Z"),
                submission_id: "sub-1".to_string(),
                exit_code: 0,
                outcome: TerminalOutcome::Success,
                attempts: 1,
            }],
            skipped: vec![SkippedDemand {
                pipeline: "staging".to_string(),
                source: Some(DemandKind::After),
                reason: TickSkipReason::NotDue,
            }],
            skipped_whole_tick: false,
            lock_overridden: false,
            state_busy: false,
            drained: false,
        };

        let out = build_tick_output(&report, ts("2026-05-02T03:05:00Z"), false, 42);

        assert_eq!(out.command, "tick");
        assert_eq!(out.counts.evaluated, 2);
        assert_eq!(out.counts.due, 1);
        assert_eq!(out.counts.executed, 1);
        assert_eq!(out.counts.succeeded, 1);
        assert_eq!(out.counts.skipped, 1);
        assert!(!out.tick_in_progress);

        let raw = out.evaluated.iter().find(|p| p.pipeline == "raw").unwrap();
        assert!(raw.due);
        assert_eq!(raw.due_source.as_deref(), Some("cron"));
        assert!(
            raw.sources
                .iter()
                .any(|s| s.reason == "due" && s.source == "cron")
        );

        assert_eq!(out.executed[0].submission_id, "sub-1");
        assert_eq!(out.executed[0].outcome, "success");
        assert_eq!(out.skipped[0].reason, "not_due");
        assert_eq!(out.skipped[0].pipeline.as_deref(), Some("staging"));
    }

    #[test]
    fn whole_tick_skip_becomes_pipeline_less_entry() {
        let report = TickReport {
            skipped_whole_tick: true,
            ..Default::default()
        };
        let out = build_tick_output(&report, ts("2026-05-02T03:05:00Z"), false, 1);
        assert!(out.tick_in_progress);
        let entry = out
            .skipped
            .iter()
            .find(|s| s.reason == "tick_in_progress")
            .expect("a tick_in_progress skip entry");
        assert!(entry.pipeline.is_none());
        assert_eq!(out.counts.executed, 0);
    }

    #[test]
    fn history_unavailable_and_backoff_reasons_surface() {
        let report = TickReport {
            skipped: vec![
                SkippedDemand {
                    pipeline: "a".to_string(),
                    source: Some(DemandKind::After),
                    reason: TickSkipReason::HistoryUnavailable,
                },
                SkippedDemand {
                    pipeline: "b".to_string(),
                    source: Some(DemandKind::Freshness),
                    reason: TickSkipReason::FailureBackoff {
                        resume_at: ts("2026-05-02T04:00:00Z"),
                    },
                },
            ],
            ..Default::default()
        };
        let out = build_tick_output(&report, ts("2026-05-02T03:05:00Z"), true, 3);
        let a = out
            .skipped
            .iter()
            .find(|s| s.pipeline.as_deref() == Some("a"))
            .unwrap();
        assert_eq!(a.reason, "history_unavailable");
        let b = out
            .skipped
            .iter()
            .find(|s| s.pipeline.as_deref() == Some("b"))
            .unwrap();
        assert_eq!(b.reason, "failure_backoff");
        assert_eq!(b.resume_at, Some(ts("2026-05-02T04:00:00Z")));
    }
}
