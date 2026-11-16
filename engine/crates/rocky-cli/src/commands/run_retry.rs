//! `rocky run --retry-failed [<RUN_ID>]`: pick the models to rerun after a
//! failed transformation run.
//!
//! This re-selects models. It writes no checkpoint and reads none: it is not
//! `--resume`, which stays replication-only. The selection goes to the same
//! multi-model path as `--select`.

use std::collections::BTreeSet;
use std::path::Path;

use anyhow::{Context, Result};
use rocky_core::state::{RunRecord, RunStatus, StateStore, UnrecordedScope};

use crate::registry;

/// The models a retry reruns, and the pipeline they belong to.
#[derive(Debug)]
pub struct RetrySelection {
    /// The transformation pipeline the retried run belongs to.
    pub pipeline: String,
    /// The run being retried.
    pub run_id: String,
    /// The models to rerun: every current model that did not succeed with its
    /// current SQL in that run.
    pub models: BTreeSet<String>,
}

/// Resolve the run to retry and the models to rerun.
///
/// With no `run_id`, the run is the newest record of `pipeline` under the same
/// `branch` (`None` is production) that is not a skipped-idempotent or
/// in-flight stub. It must be a failure.
///
/// A model is kept out of the selection only when that run holds a `success`
/// row for it whose `sql_hash` equals the hash of the SQL the model generates
/// now. The hash is recomputed from the model as loaded from disk, so a model
/// whose generated SQL depends on warehouse state (an incremental model's
/// insert, a typed merge), on `--var`, or on a Snowflake warehouse name never
/// matches and is rerun. The selection is then closed over the model graph:
/// every transitive dependent of a selected model is rerun too, because it
/// must read the rebuilt output.
///
/// Every doubt rebuilds; nothing is skipped on a guess:
/// - A model the failed run skipped as unchanged or reused has no `success`
///   row, so it is rebuilt.
/// - A retry of a failed retry looks only at that retry's own rows, so a
///   model built by the first run is rebuilt again.
///
/// The run history comes from the local state store, so a remote `[state]`
/// backend is refused.
pub fn resolve_retry_failed(
    config_path: &Path,
    state_path: &Path,
    pipeline_arg: Option<&str>,
    models_dir: Option<&Path>,
    run_id: Option<&str>,
    branch: Option<&str>,
) -> Result<RetrySelection> {
    let config = rocky_core::config::load_rocky_config(config_path)?;
    anyhow::ensure!(
        matches!(
            config.state.backend,
            rocky_core::config::StateBackend::Local
        ),
        "--retry-failed reads run history from the local state store; with a remote [state] \
         backend run the failed models with --select"
    );
    let (pipeline_name, pipeline) = registry::resolve_pipeline(&config, pipeline_arg)?;
    let rocky_core::config::PipelineConfig::Transformation(transformation) = pipeline else {
        anyhow::bail!(
            "--retry-failed reruns a transformation pipeline, but pipeline '{pipeline_name}' is {}",
            pipeline.pipeline_type_str()
        );
    };

    let store = StateStore::open_read_only_or_empty(state_path)
        .with_context(|| format!("failed to read the state store at {}", state_path.display()))?;
    // A production retry never picks a `--shadow` run, and a run with no
    // recorded scope is left out rather than guessed at.
    let in_scope = |run: &RunRecord| {
        run.pipeline.as_deref() == Some(pipeline_name)
            && run.rocky_branch.as_deref() == branch
            && (branch.is_some() || run.counts_as_production(UnrecordedScope::Exclude))
    };
    let scope = || match branch {
        Some(b) => format!("pipeline '{pipeline_name}' on branch '{b}'"),
        None => format!("pipeline '{pipeline_name}'"),
    };
    let run = match run_id {
        Some(id) => {
            let run = store
                .get_run(id)?
                .with_context(|| format!("run '{id}' not found in the state store"))?;
            anyhow::ensure!(
                in_scope(&run),
                "run '{id}' is not a production run of {}: it belongs to pipeline {:?} on branch {:?} \
                 (scope {:?})",
                scope(),
                run.pipeline,
                run.rocky_branch,
                run.production_scope()
            );
            run
        }
        None => store
            .list_runs_matching(1, |run| {
                in_scope(run)
                    && !matches!(
                        run.status,
                        RunStatus::SkippedIdempotent | RunStatus::SkippedInFlight
                    )
            })?
            .into_iter()
            .next()
            .with_context(|| format!("no run of {} found to retry", scope()))?,
    };
    anyhow::ensure!(
        matches!(run.status, RunStatus::Failure | RunStatus::PartialFailure),
        "nothing to retry: run '{}' has status {:?}, not a failure",
        run.run_id,
        run.status
    );

    let (dir, glob) = match models_dir {
        Some(dir) => (dir.to_path_buf(), None),
        None => {
            let dir =
                match crate::models_loader::locate_models_dir(&transformation.models, config_path)?
                {
                    crate::models_loader::ModelsDir::Present(dir)
                    | crate::models_loader::ModelsDir::Absent(dir) => dir,
                };
            let glob =
                crate::models_loader::resolved_models_glob(&transformation.models, config_path);
            (dir, Some(glob))
        }
    };
    anyhow::ensure!(
        dir.exists(),
        "models directory '{}' not found (required for --retry-failed)",
        dir.display()
    );
    let models = match glob.as_deref() {
        Some(glob) => crate::models_loader::load_project_models_matching(&dir, glob, None)?,
        None => crate::models_loader::load_project_models(&dir, None)?,
    };

    // The dialect comes from the adapter's config alone: no adapter is built
    // and nothing connects before the run's own policy gate.
    let adapter = config
        .adapters
        .get(&transformation.target.adapter)
        .with_context(|| {
            format!(
                "adapter '{}' of pipeline '{pipeline_name}' is not configured",
                transformation.target.adapter
            )
        })?;
    let dialect = super::plan::dialect_for_adapter(adapter);

    let changed: BTreeSet<String> = models
        .iter()
        .filter(|m| {
            !matches!(
                m.config.strategy,
                rocky_core::models::StrategyConfig::Ephemeral
            )
        })
        .filter(|m| {
            let Ok(statements) = rocky_core::sql_gen::generate_transformation_sql_with_warehouse(
                &m.to_model_ir(),
                dialect.as_ref(),
                None,
            ) else {
                return true;
            };
            let current = crate::output::sql_fingerprint(&statements);
            !run.models_executed.iter().any(|done| {
                let same_model = match done.output_target {
                    Some(_) => done.rocky_model_name() == m.config.name,
                    None => {
                        done.model_name == m.config.name || done.model_name == m.config.target.table
                    }
                };
                same_model && done.status == "success" && done.sql_hash == current
            })
        })
        .map(|m| m.config.name.clone())
        .collect();

    // Close over the graph: a model downstream of a rebuilt one must rebuild.
    let selected = if changed.is_empty() {
        changed
    } else {
        let closure = crate::selection::SelectionArgs {
            select: changed.iter().map(|name| format!("{name}+")).collect(),
            exclude: Vec::new(),
            state_ref: None,
            state_working_tree: false,
            required_model: None,
            run_vars: Default::default(),
        };
        crate::selection::resolve_buildable_in_dir(
            &closure,
            &dir,
            glob.as_deref(),
            &crate::selection::StateContext {
                config_path,
                state_path,
                cache_ttl_override: None,
            },
        )?
    };
    anyhow::ensure!(
        !selected.is_empty(),
        "nothing to retry: every model of pipeline '{pipeline_name}' already succeeded in run '{}'",
        run.run_id
    );
    Ok(RetrySelection {
        pipeline: pipeline_name.to_string(),
        run_id: run.run_id,
        models: selected,
    })
}
