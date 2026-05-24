//! `rocky preview rows` — sample result rows for a model (or one of its
//! CTEs).
//!
//! Compiles the project, takes the target model's compiled SELECT (macros
//! expanded), masks any classification-tagged columns inline so the preview
//! matches what the materialized target would expose, wraps the query in a
//! `LIMIT`, and executes it against the pipeline's configured adapter via
//! [`WarehouseAdapter::execute_query`].
//!
//! **Scope (Phase 1):** models with an executable `SELECT` body (raw-SQL and
//! `.rocky` DSL models, in any pipeline type). Time-interval models and
//! non-`SELECT` bodies are rejected; a config-only replication model with no
//! SQL file surfaces as `model_not_found`. Warehouse (non-DuckDB) execution is
//! gated behind `--allow-warehouse`. The preview reads *materialized* upstream
//! state — if an
//! upstream table doesn't exist yet, the adapter error is surfaced as
//! `upstream_not_materialized`.
//!
//! Failure modes emit a `{"error_kind": ...}` envelope to stdout (parsed by
//! the VS Code extension) and exit non-zero. See [`err`].

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Instant;

use anyhow::{Result, anyhow};

use rocky_compiler::compile::{self, CompilerConfig};
use rocky_compiler::diagnostic::Severity;
use rocky_core::config::PipelineConfig;
use rocky_core::masking::inline_mask_expr;
use rocky_core::models::StrategyConfig;
use rocky_ir::MaskStrategy;

use crate::output::{PreviewRowsOutput, print_json};
use crate::registry::{self, AdapterRegistry};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Execute `rocky preview rows`.
#[allow(clippy::too_many_arguments)]
pub async fn run_preview_rows(
    config_path: &Path,
    _state_path: &Path,
    model: &str,
    cte: Option<&str>,
    limit: u32,
    allow_warehouse: bool,
    pipeline_name: Option<&str>,
    models_dir: &Path,
    output_json: bool,
) -> Result<()> {
    let start = Instant::now();

    // Validate identifiers up-front — never interpolate unvalidated input.
    rocky_sql::validation::validate_identifier(model).map_err(|_| {
        err(
            output_json,
            "invalid_model_name",
            &format!("invalid model name '{model}'"),
            None,
        )
    })?;
    if let Some(c) = cte {
        rocky_sql::validation::validate_identifier(c).map_err(|_| {
            err(
                output_json,
                "invalid_cte_name",
                &format!("invalid CTE name '{c}'"),
                None,
            )
        })?;
    }

    let rocky_cfg = rocky_core::config::load_rocky_config(config_path).map_err(|e| {
        err(
            output_json,
            "config_error",
            &format!("failed to load config from {}: {e}", config_path.display()),
            None,
        )
    })?;

    let registry = AdapterRegistry::from_config(&rocky_cfg)
        .map_err(|e| err(output_json, "config_error", &format!("{e}"), None))?;

    let (_pname, pipeline) = registry::resolve_pipeline(&rocky_cfg, pipeline_name)
        .map_err(|e| err(output_json, "pipeline_error", &format!("{e}"), None))?;

    let adapter_name = pipeline_target_adapter(pipeline);
    let adapter_type = rocky_cfg
        .adapters
        .get(&adapter_name)
        .map(|c| c.adapter_type.clone())
        .unwrap_or_else(|| "unknown".to_string());

    // Gate (before compile, so it can't fail on missing warehouse creds):
    // remote execution requires an explicit opt-in.
    if !is_local_adapter(&adapter_type) && !allow_warehouse {
        return Err(err(
            output_json,
            "warehouse_gated",
            &format!(
                "preview would execute against '{adapter_name}' ({adapter_type}); pass --allow-warehouse to run against a remote warehouse"
            ),
            Some(serde_json::json!({ "adapter_kind": adapter_type })),
        ));
    }

    let compiler_cfg = CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        contracts_dir: None,
        source_schemas: std::collections::HashMap::new(),
        source_column_info: std::collections::HashMap::new(),
        mask: rocky_cfg.mask.clone(),
        allow_unmasked: rocky_cfg.classifications.allow_unmasked.clone(),
        project_freshness_default: rocky_cfg.freshness.has_default(),
    };
    let result = compile::compile(&compiler_cfg).map_err(|e| {
        err(
            output_json,
            "compile_error",
            &format!("compile failed: {e}"),
            None,
        )
    })?;

    let Some(m) = result
        .project
        .models
        .iter()
        .find(|m| m.config.name == model)
    else {
        return Err(err(
            output_json,
            "model_not_found",
            &format!("model '{model}' not found in pipeline"),
            None,
        ));
    };

    // Guards.
    if matches!(m.config.strategy, StrategyConfig::TimeInterval { .. }) {
        return Err(err(
            output_json,
            "unsupported_model_kind",
            &format!(
                "model '{model}' is a time-interval model; preview rows can't resolve partition placeholders"
            ),
            None,
        ));
    }
    let model_has_errors = m.sql.trim().is_empty()
        || result
            .diagnostics
            .iter()
            .any(|d| d.model == model && d.severity == Severity::Error);
    if model_has_errors {
        return Err(err(
            output_json,
            "compile_error",
            &format!("model '{model}' has compile errors; fix them before previewing"),
            None,
        ));
    }

    // Expand Rocky macros (model.sql may contain unexpanded macro calls).
    let macros_dir = models_dir.join("../macros");
    let macro_defs = if macros_dir.is_dir() {
        rocky_core::macros::load_macros_from_dir(&macros_dir).map_err(|e| {
            err(
                output_json,
                "compile_error",
                &format!("macro load failed: {e}"),
                None,
            )
        })?
    } else {
        Vec::new()
    };
    let expanded = rocky_core::macros::expand_macros(&m.sql, &macro_defs).map_err(|e| {
        err(
            output_json,
            "compile_error",
            &format!("macro expansion failed: {e}"),
            None,
        )
    })?;

    if !rocky_sql::parser::is_single_select(&expanded) {
        return Err(err(
            output_json,
            "unsupported_model_kind",
            &format!("model '{model}' did not compile to a single SELECT statement"),
            None,
        ));
    }

    // Resolve which output columns must be masked (workspace-default env).
    let masks = rocky_cfg.resolve_mask_for_env(None);
    let mut masked: BTreeMap<String, MaskStrategy> = BTreeMap::new();
    for (col, tag) in &m.config.classification {
        if let Some(&strat) = masks.get(tag) {
            if strat != MaskStrategy::None {
                masked.insert(col.clone(), strat);
            }
        }
    }

    // CTE side-door: a CTE's intermediate columns carry no classification, so
    // previewing one could leak a value the model output would mask. Refuse.
    if cte.is_some() && !masked.is_empty() {
        return Err(err(
            output_json,
            "cte_masking_unverified",
            &format!(
                "model '{model}' has masked columns; CTE preview is disabled to avoid leaking pre-mask values"
            ),
            None,
        ));
    }

    let inner = expanded.trim().trim_end_matches(';').trim();
    let final_sql = if let Some(cte_name) = cte {
        let isolated = rocky_sql::parser::isolate_cte(&expanded, cte_name)
            .map_err(|e| err(output_json, "cte_error", &format!("{e}"), None))?;
        wrap_with_limit(isolated.trim().trim_end_matches(';').trim(), limit)
    } else if masked.is_empty() {
        wrap_with_limit(inner, limit)
    } else {
        let ordered: Vec<String> = result
            .type_check
            .typed_models
            .get(model)
            .map(|cols| cols.iter().map(|c| c.name.clone()).collect())
            .unwrap_or_default();
        build_masking_projection(inner, &ordered, &masked, &adapter_type, limit).map_err(|cols| {
            err(
                output_json,
                "unmaskable_column",
                &format!(
                    "can't safely mask column(s) {cols:?} for adapter '{adapter_type}'; preview refused to avoid leaking unmasked values"
                ),
                Some(serde_json::json!({ "columns": cols })),
            )
        })?
    };

    // Build the adapter and execute (after the gate + compile, so warehouse
    // credential construction only happens once the run is authorized).
    let adapter = registry
        .warehouse_adapter(&adapter_name)
        .map_err(|e| err(output_json, "adapter_error", &format!("{e}"), None))?;
    adapter.ping().await.map_err(|e| {
        err(
            output_json,
            "connection_error",
            &format!("failed to connect to {adapter_name}: {e}"),
            None,
        )
    })?;
    let query_result = match adapter.execute_query(&final_sql).await {
        Ok(r) => r,
        Err(e) => {
            let msg = e.to_string();
            let kind = if looks_like_missing_relation(&msg) {
                "upstream_not_materialized"
            } else {
                "execution_error"
            };
            let human = if kind == "upstream_not_materialized" {
                format!(
                    "a referenced table doesn't exist yet — run upstream models first (`rocky run`). ({msg})"
                )
            } else {
                format!("query failed: {msg}")
            };
            return Err(err(output_json, kind, &human, None));
        }
    };

    let row_count = query_result.rows.len();
    let out = PreviewRowsOutput {
        version: VERSION.to_string(),
        command: "preview-rows".to_string(),
        model: model.to_string(),
        cte: cte.map(str::to_string),
        columns: query_result.columns.clone(),
        rows: query_result.rows.clone(),
        row_count,
        limit_applied: limit,
        truncated: row_count as u64 >= u64::from(limit),
        executed_sql: final_sql,
        adapter_kind: adapter_type,
        duration_ms: start.elapsed().as_millis() as u64,
    };

    if output_json {
        print_json(&out)?;
    } else {
        print_table(&out);
    }
    Ok(())
}

/// DuckDB is the only "local" adapter — it needs no credentials and incurs no
/// warehouse cost, so it's never gated.
fn is_local_adapter(adapter_type: &str) -> bool {
    adapter_type.eq_ignore_ascii_case("duckdb")
}

/// Target adapter name for any pipeline variant (preview reads the model's
/// compiled SQL, so the pipeline type doesn't matter — only its adapter does).
fn pipeline_target_adapter(pipeline: &PipelineConfig) -> String {
    match pipeline {
        PipelineConfig::Replication(r) => r.target.adapter.clone(),
        PipelineConfig::Transformation(t) => t.target.adapter.clone(),
        PipelineConfig::Quality(q) => q.target.adapter.clone(),
        PipelineConfig::Snapshot(s) => s.target.adapter.clone(),
        PipelineConfig::Load(l) => l.target.adapter.clone(),
    }
}

/// Wrap an arbitrary `SELECT` (or `WITH … SELECT`) so it always returns at
/// most `limit` rows, regardless of any inner `LIMIT`/`ORDER BY`.
fn wrap_with_limit(inner: &str, limit: u32) -> String {
    format!("SELECT * FROM ({inner}) AS rocky_preview LIMIT {limit}")
}

/// Build a `LIMIT`-bounded masking projection over a model's ordered output
/// columns, replacing classified columns with their inline mask expression.
///
/// Returns `Err(columns)` listing the columns that can't be safely masked —
/// either the output schema couldn't be enumerated, a column name isn't a
/// simple identifier, a masked column is absent from the schema, or the
/// adapter has no mask expression for the strategy. The caller turns this into
/// a fail-safe `unmaskable_column` refusal rather than risk leaking.
fn build_masking_projection(
    inner: &str,
    ordered_cols: &[String],
    masked: &BTreeMap<String, MaskStrategy>,
    adapter_type: &str,
    limit: u32,
) -> Result<String, Vec<String>> {
    if ordered_cols.is_empty() {
        // Can't enumerate the output — refuse rather than `SELECT *` and leak.
        let mut cols: Vec<String> = masked.keys().cloned().collect();
        cols.sort();
        return Err(cols);
    }

    let mut unmaskable: Vec<String> = Vec::new();
    let mut items: Vec<String> = Vec::with_capacity(ordered_cols.len());
    for col in ordered_cols {
        if rocky_sql::validation::validate_identifier(col).is_err() {
            // A non-simple identifier can't be interpolated safely.
            if masked.contains_key(col) {
                unmaskable.push(col.clone());
            }
            // Even untagged, we can't build a faithful projection — bail.
            unmaskable.push(col.clone());
            continue;
        }
        match masked.get(col) {
            Some(&strat) => match inline_mask_expr(strat, col, adapter_type) {
                Some(expr) => items.push(format!("{expr} AS {col}")),
                None => unmaskable.push(col.clone()),
            },
            None => items.push(col.clone()),
        }
    }
    // Every masked column must have appeared in the enumerated schema.
    for mcol in masked.keys() {
        if !ordered_cols.iter().any(|c| c == mcol) {
            unmaskable.push(mcol.clone());
        }
    }

    if !unmaskable.is_empty() {
        unmaskable.sort();
        unmaskable.dedup();
        return Err(unmaskable);
    }

    Ok(format!(
        "SELECT {} FROM ({inner}) AS rocky_src LIMIT {limit}",
        items.join(", ")
    ))
}

/// Heuristic: does an adapter error look like a missing upstream table?
fn looks_like_missing_relation(msg: &str) -> bool {
    let m = msg.to_lowercase();
    m.contains("does not exist")
        || m.contains("no such table")
        || m.contains("cannot be resolved")
        || m.contains("not found")
        || (m.contains("table") && m.contains("exist"))
}

/// Emit a structured error envelope and return a non-zero-exit error.
///
/// In `--output json` mode prints `{"error_kind","message",...extra}` to stdout
/// (the VS Code extension reads this even on non-zero exit); always returns an
/// `anyhow::Error` so the process exits non-zero with the human message on
/// stderr.
fn err(
    output_json: bool,
    kind: &str,
    message: &str,
    extra: Option<serde_json::Value>,
) -> anyhow::Error {
    if output_json {
        let mut obj = serde_json::Map::new();
        obj.insert(
            "error_kind".to_string(),
            serde_json::Value::String(kind.to_string()),
        );
        obj.insert(
            "message".to_string(),
            serde_json::Value::String(message.to_string()),
        );
        if let Some(serde_json::Value::Object(extra_map)) = extra {
            for (k, v) in extra_map {
                obj.insert(k, v);
            }
        }
        println!("{}", serde_json::Value::Object(obj));
    }
    anyhow!("{message}")
}

/// Minimal column-aligned table for the human (`--output table`) path.
fn print_table(out: &PreviewRowsOutput) {
    println!("{}", out.columns.join(" | "));
    for row in &out.rows {
        let cells: Vec<String> = row
            .iter()
            .map(|v| match v {
                serde_json::Value::Null => "NULL".to_string(),
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            })
            .collect();
        println!("{}", cells.join(" | "));
    }
    let suffix = if out.truncated { " (truncated)" } else { "" };
    println!("({} rows{suffix})", out.row_count);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_local_adapter() {
        assert!(is_local_adapter("duckdb"));
        assert!(is_local_adapter("DuckDB"));
        assert!(!is_local_adapter("databricks"));
        assert!(!is_local_adapter("snowflake"));
    }

    #[test]
    fn test_wrap_with_limit() {
        assert_eq!(
            wrap_with_limit("SELECT a FROM t", 100),
            "SELECT * FROM (SELECT a FROM t) AS rocky_preview LIMIT 100"
        );
    }

    #[test]
    fn test_masking_projection_masks_tagged_column() {
        let masked = BTreeMap::from([("email".to_string(), MaskStrategy::Hash)]);
        let cols = vec!["id".to_string(), "email".to_string()];
        let sql = build_masking_projection("SELECT id, email FROM u", &cols, &masked, "duckdb", 50)
            .unwrap();
        assert!(sql.contains("id,"));
        assert!(sql.contains("sha256(CAST(email AS VARCHAR)) AS email"));
        assert!(sql.ends_with("AS rocky_src LIMIT 50"));
    }

    #[test]
    fn test_masking_projection_passes_untagged() {
        let masked = BTreeMap::from([("email".to_string(), MaskStrategy::Redact)]);
        let cols = vec!["id".to_string(), "email".to_string(), "name".to_string()];
        let sql =
            build_masking_projection("SELECT * FROM u", &cols, &masked, "databricks", 10).unwrap();
        // Untagged columns pass through verbatim.
        assert!(sql.contains("SELECT id, '***' AS email, name FROM"));
    }

    #[test]
    fn test_masking_projection_empty_schema_refuses() {
        let masked = BTreeMap::from([("email".to_string(), MaskStrategy::Hash)]);
        let err =
            build_masking_projection("SELECT * FROM u", &[], &masked, "duckdb", 10).unwrap_err();
        assert_eq!(err, vec!["email".to_string()]);
    }

    #[test]
    fn test_masking_projection_unsupported_dialect_refuses() {
        let masked = BTreeMap::from([("email".to_string(), MaskStrategy::Hash)]);
        let cols = vec!["id".to_string(), "email".to_string()];
        // BigQuery has no inline Hash expression yet → fail-safe refusal.
        let err =
            build_masking_projection("SELECT id, email FROM u", &cols, &masked, "bigquery", 10)
                .unwrap_err();
        assert_eq!(err, vec!["email".to_string()]);
    }

    #[test]
    fn test_masking_projection_missing_masked_column_refuses() {
        // A masked column that the typed schema doesn't contain → refuse.
        let masked = BTreeMap::from([("ssn".to_string(), MaskStrategy::Hash)]);
        let cols = vec!["id".to_string(), "email".to_string()];
        let err = build_masking_projection("SELECT id, email FROM u", &cols, &masked, "duckdb", 10)
            .unwrap_err();
        assert_eq!(err, vec!["ssn".to_string()]);
    }

    #[test]
    fn test_looks_like_missing_relation() {
        assert!(looks_like_missing_relation(
            "Catalog Error: Table with name stg_orders does not exist!"
        ));
        assert!(looks_like_missing_relation("no such table: foo"));
        assert!(!looks_like_missing_relation("syntax error near SELECT"));
    }
}
