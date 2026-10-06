//! `rocky freshness` — check source and model freshness against the warehouse.
//!
//! dbt's `source freshness`, for Rocky's transformation pipelines. Two inputs:
//!
//! - **Sources.** Every `[[pipeline.<name>.sources]]` entry with a
//!   `freshness` block. Rocky reads `MAX(loaded_at_field)` (narrowed by the
//!   optional `filter`) and grades its age against `warn_after` /
//!   `error_after`.
//! - **Models.** Every model with a `[freshness]` block. With a
//!   `time_column`, Rocky reads `MAX(time_column)` from the model's target
//!   table. Without one, it reads the model's last successful build from the
//!   state store. The model's one TTL (`max_lag_seconds`) is the warn
//!   threshold, or the error threshold under `severity = "error"`.
//!
//! ```text
//!   age = now - max_loaded_at
//!   age > error_after  -> error          (exit 1)
//!   age > warn_after   -> warn           (exit 0)
//!   otherwise          -> pass
//!   query/config fails -> runtime_error  (exit 1)
//!   no rows            -> worst configured threshold (never loaded)
//! ```
//!
//! Read-only: nothing is written to the warehouse or the state store.

use std::path::Path;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::Serialize;

use rocky_core::config::{PipelineConfig, RockyConfig, TransformationPipelineConfig};
use rocky_core::source_freshness::{
    FreshnessStatus, FreshnessThresholds, PipelineSourceConfig, evaluate,
    generate_max_loaded_at_sql, parse_loaded_at_cell_for,
};
use rocky_core::traits::WarehouseAdapter;

use crate::output::print_json;
use crate::registry::AdapterRegistry;

/// JSON output for `rocky freshness`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct FreshnessOutput {
    pub version: String,
    pub command: String,
    /// The instant every age was measured against.
    pub checked_at: DateTime<Utc>,
    /// One entry per declared source freshness block.
    pub sources: Vec<FreshnessCheckResult>,
    /// One entry per model `[freshness]` block.
    pub models: Vec<FreshnessCheckResult>,
    pub summary: FreshnessSummary,
}

/// Counts per status across `sources` and `models`.
#[derive(Debug, Default, Serialize, JsonSchema, PartialEq, Eq)]
pub struct FreshnessSummary {
    pub pass: usize,
    pub warn: usize,
    pub error: usize,
    pub runtime_error: usize,
}

/// One freshness check.
#[derive(Debug, Serialize, JsonSchema)]
pub struct FreshnessCheckResult {
    /// Source `schema.table` (or `catalog.schema.table`), or the model name.
    pub name: String,
    /// The pipeline that declares the source or loads the model.
    pub pipeline: String,
    /// The table the measurement was taken from (`catalog.schema.table`).
    pub table: String,
    /// The column read with `MAX(..)`. `None` when the measurement is the
    /// model's last successful build from the state store.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub loaded_at_field: Option<String>,
    /// Where `max_loaded_at` came from: `warehouse` or `state_store`.
    pub measured_from: String,
    /// The newest load time seen. `None` when the table is empty, every value
    /// is NULL, the model was never built, or the check could not run.
    pub max_loaded_at: Option<DateTime<Utc>>,
    /// `checked_at - max_loaded_at`, in seconds. Negative when the newest
    /// load time is in the future (clock skew).
    pub age_seconds: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warn_after_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_after_seconds: Option<u64>,
    /// `pass`, `warn`, `error`, or `runtime_error`.
    pub status: FreshnessStatus,
    /// Why the status is what it is, when that is not just the age.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl FreshnessOutput {
    /// True when any check is `error` or `runtime_error`.
    pub fn has_failures(&self) -> bool {
        self.summary.error > 0 || self.summary.runtime_error > 0
    }
}

/// Execute `rocky freshness`.
///
/// Prints the report, then fails (exit 1) when any check is `error` or
/// `runtime_error`. `warn` alone exits 0.
pub async fn run_freshness(
    config_path: &Path,
    state_path: &Path,
    pipeline: Option<&str>,
    output_json: bool,
) -> Result<()> {
    let cfg = rocky_core::config::load_rocky_config(config_path)
        .with_context(|| format!("failed to load config from {}", config_path.display()))?;
    let output = freshness_output(&cfg, config_path, state_path, pipeline, Utc::now()).await?;

    if output_json {
        print_json(&output)?;
    } else {
        render_text(&output);
    }

    if output.has_failures() {
        anyhow::bail!(
            "freshness check failed: {} error, {} runtime_error",
            output.summary.error,
            output.summary.runtime_error
        );
    }
    Ok(())
}

/// Build the report with `now` injected (the clock seam the tests use).
pub async fn freshness_output(
    cfg: &RockyConfig,
    config_path: &Path,
    state_path: &Path,
    pipeline: Option<&str>,
    now: DateTime<Utc>,
) -> Result<FreshnessOutput> {
    let pipelines: Vec<(&str, &TransformationPipelineConfig)> = match pipeline {
        Some(name) => {
            let p = cfg
                .pipelines
                .get(name)
                .with_context(|| format!("pipeline '{name}' not found in config"))?;
            let tx = p.as_transformation().with_context(|| {
                format!(
                    "pipeline '{name}' is type '{}', but `rocky freshness` checks transformation \
                     pipelines (replication pipelines enforce freshness through \
                     `[checks.freshness]` during `rocky run`)",
                    p.pipeline_type_str()
                )
            })?;
            vec![(name, tx)]
        }
        None => cfg
            .pipelines
            .iter()
            .filter_map(|(n, p)| match p {
                PipelineConfig::Transformation(tx) => Some((n.as_str(), tx.as_ref())),
                _ => None,
            })
            .collect(),
    };

    let registry = AdapterRegistry::from_config(cfg)?;
    let mut sources = Vec::new();
    let mut models = Vec::new();
    // Two pipelines may load the same models directory; check each model once.
    let mut seen_models: std::collections::HashSet<String> = std::collections::HashSet::new();

    for (name, tx) in pipelines {
        let declared: Vec<&PipelineSourceConfig> = tx
            .sources
            .iter()
            .filter(|s| s.freshness.is_some())
            .collect();
        let mut model_list = load_models_with_freshness(cfg, tx, config_path)?;
        model_list.retain(|m| seen_models.insert(m.config.name.clone()));
        if declared.is_empty() && model_list.is_empty() {
            continue;
        }
        let default_adapter = registry
            .warehouse_adapter(&tx.target.adapter)
            .with_context(|| format!("pipeline '{name}': no warehouse adapter"))?;

        for source in declared {
            sources.push(check_source(default_adapter.as_ref(), name, source, now).await);
        }

        let mut store: Option<rocky_core::state::StateStore> = None;
        for model in &model_list {
            let adapter = match &model.config.adapter {
                Some(a) => match registry.warehouse_adapter(a) {
                    Ok(a) => a,
                    Err(e) => {
                        models.push(model_runtime_error(name, model, format!("{e:#}")));
                        continue;
                    }
                },
                None => default_adapter.clone(),
            };
            let freshness = model
                .config
                .freshness
                .as_ref()
                .expect("filtered to freshness");
            let mut fallback_reason = None;
            if freshness.time_column.is_some() {
                let result = check_model_column(adapter.as_ref(), name, model, now).await;
                // An inherited `time_column` (from `_defaults.toml` or the
                // project `[freshness]`) was not written for this model and may
                // not exist on it. Fall back to the last successful build
                // rather than report a runtime error the model never asked for.
                if result.status != FreshnessStatus::RuntimeError || freshness.declared_in_sidecar {
                    models.push(result);
                    continue;
                }
                fallback_reason = result.message;
            }
            if store.is_none() {
                store = Some(
                    rocky_core::state::StateStore::open_read_only_or_empty(state_path)
                        .with_context(|| {
                            format!("failed to open state store {}", state_path.display())
                        })?,
                );
            }
            let mut result =
                check_model_state(store.as_ref().expect("opened above"), name, model, now);
            if let Some(reason) = fallback_reason {
                result.message = Some(format!(
                    "inherited time_column could not be read ({reason}); measured the last \
                     successful build instead{}",
                    result
                        .message
                        .as_deref()
                        .map(|m| format!(": {m}"))
                        .unwrap_or_default()
                ));
            }
            models.push(result);
        }
    }

    let mut summary = FreshnessSummary::default();
    for r in sources.iter().chain(models.iter()) {
        match r.status {
            FreshnessStatus::Pass => summary.pass += 1,
            FreshnessStatus::Warn => summary.warn += 1,
            FreshnessStatus::Error => summary.error += 1,
            FreshnessStatus::RuntimeError => summary.runtime_error += 1,
        }
    }

    Ok(FreshnessOutput {
        version: env!("CARGO_PKG_VERSION").to_string(),
        command: "freshness".to_string(),
        checked_at: now,
        sources,
        models,
        summary,
    })
}

fn load_models_with_freshness(
    cfg: &RockyConfig,
    tx: &TransformationPipelineConfig,
    config_path: &Path,
) -> Result<Vec<rocky_core::models::Model>> {
    let Some(dir) = crate::models_loader::resolve_models_dir(&tx.models, config_path)? else {
        return Ok(Vec::new());
    };
    let glob = crate::models_loader::resolved_models_glob(&tx.models, config_path);
    let mut models =
        crate::models_loader::load_project_models_matching(&dir, &glob, Some(&cfg.freshness))?;
    // An ephemeral model has no table to measure: its consumers inline it.
    models.retain(|m| {
        m.config.freshness.is_some()
            && !matches!(
                m.config.strategy,
                rocky_core::models::StrategyConfig::Ephemeral
            )
    });
    models.sort_by(|a, b| a.config.name.cmp(&b.config.name));
    Ok(models)
}

/// One `MAX(field)` read: `(Some(max) | None for no rows, message)`.
async fn read_max(
    adapter: &dyn WarehouseAdapter,
    catalog: &str,
    schema: &str,
    table: &str,
    field: &str,
    filter: Option<&str>,
) -> Result<(Option<DateTime<Utc>>, Option<String>), String> {
    let sql = generate_max_loaded_at_sql(catalog, schema, table, field, filter, adapter.dialect())?;
    let result = adapter
        .execute_query(&sql)
        .await
        .map_err(|e| format!("the freshness query failed: {e}"))?;
    let row = result
        .rows
        .first()
        .ok_or_else(|| "the freshness query returned no rows".to_string())?;
    let count = rocky_core::checks::cell_as_u64(row.first());
    let cell = row
        .get(1)
        .ok_or_else(|| "the freshness query returned a row without its MAX cell".to_string())?;
    if cell.is_null() {
        let why = match count {
            Some(0) => "no rows to measure: the table is empty (or `filter` matched none)",
            _ => "every value of the column is NULL",
        };
        return Ok((None, Some(why.to_string())));
    }
    let text = match cell {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    parse_loaded_at_cell_for(adapter.dialect().name(), &text)
        .map(|ts| (Some(ts), None))
        .ok_or_else(|| {
            format!("could not read {text} as a timestamp; use a DATE or TIMESTAMP column")
        })
}

fn qualified(catalog: &str, schema: &str, table: &str) -> String {
    if catalog.is_empty() {
        format!("{schema}.{table}")
    } else {
        format!("{catalog}.{schema}.{table}")
    }
}

async fn check_source(
    adapter: &dyn WarehouseAdapter,
    pipeline: &str,
    source: &PipelineSourceConfig,
    now: DateTime<Utc>,
) -> FreshnessCheckResult {
    let freshness = source
        .freshness
        .as_ref()
        .expect("filtered to declared freshness");
    let mut result = FreshnessCheckResult {
        name: source.full_name(),
        pipeline: pipeline.to_string(),
        table: qualified(&source.catalog, &source.schema, &source.table),
        loaded_at_field: Some(freshness.loaded_at_field.clone()),
        measured_from: "warehouse".to_string(),
        max_loaded_at: None,
        age_seconds: None,
        warn_after_seconds: None,
        error_after_seconds: None,
        status: FreshnessStatus::RuntimeError,
        message: None,
    };
    let thresholds = match freshness.validate() {
        Ok(t) => t,
        Err(problems) => {
            result.message = Some(format!(
                "invalid freshness config (E050): {}",
                problems
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join("; ")
            ));
            return result;
        }
    };
    result.warn_after_seconds = thresholds.warn_after_seconds;
    result.error_after_seconds = thresholds.error_after_seconds;
    let read = read_max(
        adapter,
        &source.catalog,
        &source.schema,
        &source.table,
        &freshness.loaded_at_field,
        freshness.filter.as_deref(),
    )
    .await;
    grade(&mut result, read, now, &thresholds);
    result
}

fn model_shell(pipeline: &str, model: &rocky_core::models::Model) -> FreshnessCheckResult {
    let t = &model.config.target;
    let thresholds = model
        .config
        .freshness
        .as_ref()
        .map(FreshnessThresholds::from_model)
        .unwrap_or_default();
    FreshnessCheckResult {
        name: model.config.name.clone(),
        pipeline: pipeline.to_string(),
        table: qualified(&t.catalog, &t.schema, &t.table),
        loaded_at_field: model
            .config
            .freshness
            .as_ref()
            .and_then(|f| f.time_column.clone()),
        measured_from: "warehouse".to_string(),
        max_loaded_at: None,
        age_seconds: None,
        warn_after_seconds: thresholds.warn_after_seconds,
        error_after_seconds: thresholds.error_after_seconds,
        status: FreshnessStatus::RuntimeError,
        message: None,
    }
}

fn model_runtime_error(
    pipeline: &str,
    model: &rocky_core::models::Model,
    message: String,
) -> FreshnessCheckResult {
    let mut r = model_shell(pipeline, model);
    r.message = Some(message);
    r
}

async fn check_model_column(
    adapter: &dyn WarehouseAdapter,
    pipeline: &str,
    model: &rocky_core::models::Model,
    now: DateTime<Utc>,
) -> FreshnessCheckResult {
    let freshness = model
        .config
        .freshness
        .as_ref()
        .expect("filtered to freshness");
    let column = freshness.time_column.as_deref().expect("checked by caller");
    let thresholds = FreshnessThresholds::from_model(freshness);
    let t = &model.config.target;
    let mut result = model_shell(pipeline, model);
    let read = read_max(adapter, &t.catalog, &t.schema, &t.table, column, None).await;
    grade(&mut result, read, now, &thresholds);
    result
}

/// Without a `time_column`, a model is as fresh as its last successful build.
/// The newest successful build of a model by a production run (#2201).
///
/// Run history keys an execution by the model name, or by the target table
/// (the last asset-key component), so every key is searched. A shadow or
/// branch run built somewhere other than the production target, so its
/// build never makes the production model fresh. A run recorded before
/// runs carried a scope counts, as in the other reporting readers.
fn last_production_build<'a>(
    store: &rocky_core::state::StateStore,
    keys: impl IntoIterator<Item = &'a str>,
) -> Result<Option<DateTime<Utc>>, rocky_core::state::StateError> {
    let mut last_built: Option<DateTime<Utc>> = None;
    for key in keys {
        let history = store.get_model_history_matching(key, 50, |r| {
            r.counts_as_production(rocky_core::state::UnrecordedScope::Count)
        })?;
        let newest = history
            .iter()
            .filter(|e| e.status == "success")
            .map(|e| e.finished_at)
            .max();
        last_built = last_built.max(newest);
    }
    Ok(last_built)
}

fn check_model_state(
    store: &rocky_core::state::StateStore,
    pipeline: &str,
    model: &rocky_core::models::Model,
    now: DateTime<Utc>,
) -> FreshnessCheckResult {
    let freshness = model
        .config
        .freshness
        .as_ref()
        .expect("filtered to freshness");
    let thresholds = FreshnessThresholds::from_model(freshness);
    let mut result = model_shell(pipeline, model);
    result.measured_from = "state_store".to_string();
    result.loaded_at_field = None;
    let last_built = match last_production_build(
        store,
        [
            model.config.name.as_str(),
            model.config.target.table.as_str(),
        ],
    ) {
        Ok(t) => t,
        Err(e) => {
            result.message = Some(format!("could not read run history: {e}"));
            return result;
        }
    };
    let read = Ok((
        last_built,
        last_built
            .is_none()
            .then(|| "no successful build recorded in the state store".to_string()),
    ));
    grade(&mut result, read, now, &thresholds);
    result
}

fn grade(
    result: &mut FreshnessCheckResult,
    read: Result<(Option<DateTime<Utc>>, Option<String>), String>,
    now: DateTime<Utc>,
    thresholds: &FreshnessThresholds,
) {
    match read {
        Ok((max, message)) => {
            let (status, age) = evaluate(max, now, thresholds);
            result.max_loaded_at = max;
            result.age_seconds = age;
            result.status = status;
            result.message = message.or_else(|| {
                age.filter(|a| *a < 0).map(|_| {
                    "the newest load time is in the future: clock skew, a future-dated row, or a \
                         local-time column read as UTC"
                        .to_string()
                })
            });
        }
        Err(message) => {
            result.status = FreshnessStatus::RuntimeError;
            result.message = Some(message);
        }
    }
}

fn status_label(s: FreshnessStatus) -> &'static str {
    match s {
        FreshnessStatus::Pass => "PASS",
        FreshnessStatus::Warn => "WARN",
        FreshnessStatus::Error => "ERROR",
        FreshnessStatus::RuntimeError => "RUNTIME_ERROR",
    }
}

fn render_text(output: &FreshnessOutput) {
    if output.sources.is_empty() && output.models.is_empty() {
        println!(
            "No freshness declared. Add a `freshness` block to a `[[pipeline.<name>.sources]]` \
             entry or a model `[freshness]` block."
        );
        return;
    }
    for (title, rows) in [("Sources", &output.sources), ("Models", &output.models)] {
        if rows.is_empty() {
            continue;
        }
        println!("{title}:");
        for r in rows {
            let age = r
                .age_seconds
                .map_or_else(|| "-".to_string(), |a| format!("{a}s"));
            println!(
                "  {:<14} {:<40} age {:<10}{}",
                status_label(r.status),
                r.name,
                age,
                r.message
                    .as_deref()
                    .map(|m| format!("  ({m})"))
                    .unwrap_or_default()
            );
        }
    }
    let s = &output.summary;
    println!(
        "\n{} pass, {} warn, {} error, {} runtime_error",
        s.pass, s.warn, s.error, s.runtime_error
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_core::state::{RunRecord, RunScope, StateStore};

    fn run(id: &str, at: DateTime<Utc>, scope: Option<RunScope>) -> RunRecord {
        serde_json::from_value(serde_json::json!({
            "run_id": id,
            "started_at": at,
            "finished_at": at,
            "status": "Success",
            "models_executed": [{
                "model_name": "orders",
                "started_at": at,
                "finished_at": at,
                "duration_ms": 1,
                "rows_affected": null,
                "status": "success",
                "sql_hash": id,
            }],
            "trigger": "Manual",
            "config_hash": "h",
            "hostname": "test",
            "rocky_version": "0.0.0-test",
            "run_scope": scope,
        }))
        .expect("run record")
    }

    /// #2201: a newer shadow or branch build does not make the production
    /// model fresh; the newest production build does.
    #[test]
    fn last_build_ignores_shadow_and_branch_runs() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("state.redb")).unwrap();
        let t0 = Utc::now() - chrono::Duration::hours(5);
        store
            .record_run(&run("prod", t0, Some(RunScope::Production)))
            .unwrap();
        store
            .record_run(&run(
                "shadow",
                t0 + chrono::Duration::hours(1),
                Some(RunScope::Shadow { schema: None }),
            ))
            .unwrap();
        store
            .record_run(&run(
                "branch",
                t0 + chrono::Duration::hours(2),
                Some(RunScope::Branch { name: "b".into() }),
            ))
            .unwrap();
        assert_eq!(
            last_production_build(&store, ["orders"]).unwrap(),
            Some(t0)
        );

        // A run recorded before runs carried a scope still counts.
        let legacy = t0 + chrono::Duration::hours(3);
        store.record_run(&run("legacy", legacy, None)).unwrap();
        assert_eq!(
            last_production_build(&store, ["orders"]).unwrap(),
            Some(legacy)
        );
    }
}
