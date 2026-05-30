//! `rocky ai-contract <model>` — AI-drafted data contracts from observed data.
//!
//! Profiles a model's target table per column (DuckDB only this release), then
//! asks the LLM to propose a `.contract.toml` grounded in that profile. The
//! proposal is compile-verified against the model before it's accepted, so the
//! reported contract is one `rocky compile` will accept.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use rocky_ai::client::{AiConfig, DEFAULT_MAX_TOKENS, LlmClient};
use rocky_ai::contract::{ColumnProfile, TableProfile, contract_type_name, draft_contract};
use rocky_compiler::compile::{CompileResult, CompilerConfig, compile};
use rocky_compiler::types::TypedColumn;
use rocky_core::redacted::RedactedString;
use rocky_core::traits::WarehouseAdapter;

use crate::output::{AiContractColumnProfile, AiContractOutput, print_json};

const VERSION: &str = env!("CARGO_PKG_VERSION");
const ANTHROPIC_API_KEY_VAR: &str = "ANTHROPIC_API_KEY";

/// Distinct-count ceiling under which a column's domain is treated as
/// low-cardinality and surfaced as evidence.
const LOW_CARDINALITY_CAP: u64 = 25;
/// Maximum number of domain values fetched for a low-cardinality column.
const DOMAIN_FETCH_LIMIT: u64 = LOW_CARDINALITY_CAP;

/// Create an LLM client from environment + project config. Mirrors the helper
/// in `commands/ai.rs` — reads `[ai] max_tokens` when the config loads, falls
/// back to the default otherwise.
fn make_client(config_path: &Path) -> Result<LlmClient> {
    let api_key = std::env::var(ANTHROPIC_API_KEY_VAR)
        .context("ANTHROPIC_API_KEY not set. Set it to use `rocky ai-contract`.")?;

    let max_tokens = rocky_core::config::load_rocky_config(config_path)
        .map(|cfg| cfg.ai.max_tokens)
        .unwrap_or(DEFAULT_MAX_TOKENS);

    let config = AiConfig {
        provider: "anthropic".to_string(),
        model: "claude-sonnet-4-6".to_string(),
        api_key: RedactedString::new(api_key),
        default_format: "rocky".to_string(),
        max_attempts: 3,
        max_tokens,
    };

    LlmClient::new(config).map_err(|e| anyhow::anyhow!("{e}"))
}

/// Compile the project to obtain typed model schemas. Source schemas come from
/// the warm cache (degrading to empty on a cold cache), matching the AI
/// commands' grounding approach.
pub(crate) fn compile_project(
    config_path: &Path,
    state_path: &Path,
    models_dir: &str,
    cache_ttl_override: Option<u64>,
) -> Result<CompileResult> {
    let source_schemas = match rocky_core::config::load_rocky_config(config_path) {
        Ok(cfg) => {
            let schema_cfg = cfg.cache.schemas.with_ttl_override(cache_ttl_override);
            crate::source_schemas::load_cached_source_schemas(&schema_cfg, state_path)
        }
        Err(_) => std::collections::HashMap::new(),
    };

    let config = CompilerConfig {
        models_dir: PathBuf::from(models_dir),
        contracts_dir: None,
        source_schemas,
        source_column_info: std::collections::HashMap::new(),
        ..Default::default()
    };
    compile(&config).map_err(|e| anyhow::anyhow!("{e}"))
}

/// A validated, DuckDB-runnable table reference plus the adapter to run it on.
/// Ported from the rocky-mcp grounding helper: identifiers are validated via
/// `rocky_sql::validation` before composing the ref so SQL is never built from
/// raw input.
pub(crate) struct PreparedTable {
    pub(crate) adapter: std::sync::Arc<dyn WarehouseAdapter>,
    pub(crate) table_ref: String,
    /// Set when the model's declared target wasn't materialized and we fell
    /// back to a source table (only when `FallbackPolicy::SourceFallback` is
    /// requested). Carries the declared-target FQN that was missing so the
    /// caller can surface "this is a source preview, not the model output" in
    /// its output. `None` means we profiled the declared target directly.
    pub(crate) fell_back_from: Option<String>,
}

/// Either a runnable table query or the reason a non-DuckDB adapter blocks
/// profiling this release.
pub(crate) enum PreparedKind {
    Ready(PreparedTable),
    Unavailable(String),
}

/// How `prepare_table_query` should react when the model's declared target is
/// not materialized in the warehouse.
///
/// `Strict` (the ai-contract caller) refuses with a clear message — a contract
/// drafted from source data would be semantically wrong for a non-passthrough
/// model. `SourceFallback` (the `rocky profile` caller) tries the model's
/// declared sources so the agentic authoring loop and replication-pipeline
/// demos can still get observed data, surfacing the fallback via
/// `PreparedTable::fell_back_from`.
pub(crate) enum FallbackPolicy {
    Strict,
    SourceFallback,
}

/// The target adapter resolved from config, plus whether it's DuckDB.
struct ResolvedTarget {
    registry: crate::registry::AdapterRegistry,
    target_adapter: String,
    adapter_type: String,
}

/// Resolve the pipeline's target adapter and its type from config. Pure of any
/// model lookup so the DuckDB-only gate is unit-testable without a compile.
fn resolve_target(config_path: &Path) -> Result<ResolvedTarget> {
    let cfg = rocky_core::config::load_rocky_config(config_path)?;
    let registry = crate::registry::AdapterRegistry::from_config(&cfg)?;
    let (_, pipeline) = crate::registry::resolve_pipeline(&cfg, None)?;
    let target_adapter = pipeline.target_adapter().to_string();
    let adapter_type = registry
        .adapter_config(&target_adapter)
        .map(|a| a.adapter_type.clone())
        .unwrap_or_default();
    Ok(ResolvedTarget {
        registry,
        target_adapter,
        adapter_type,
    })
}

/// The DuckDB-only refusal message for a given target adapter type. Returns
/// `None` when the adapter is DuckDB (no refusal).
fn duckdb_only_refusal(adapter_type: &str) -> Option<String> {
    if adapter_type == "duckdb" {
        None
    } else {
        Some(format!(
            "`rocky ai-contract` is DuckDB-only in this release; the target adapter is '{adapter_type}'. \
             Warehouse support is deferred."
        ))
    }
}

/// Resolve a model's target table into a validated table ref + warehouse
/// adapter — but only on DuckDB. Other adapters return the typed "unavailable"
/// reason so the command refuses cleanly with a clear message.
///
/// When `policy` is [`FallbackPolicy::SourceFallback`] and the model's declared
/// target isn't materialized, this probes the model's declared sources (or
/// SQL-extracted FROM tables when sidecars don't declare any) and returns the
/// first one that exists, with [`PreparedTable::fell_back_from`] set so the
/// caller can label the result.
pub(crate) async fn prepare_table_query(
    config_path: &Path,
    compile_result: &CompileResult,
    model_name: &str,
    policy: FallbackPolicy,
) -> Result<PreparedKind> {
    let resolved = resolve_target(config_path)?;
    if let Some(reason) = duckdb_only_refusal(&resolved.adapter_type) {
        return Ok(PreparedKind::Unavailable(reason));
    }
    let registry = resolved.registry;
    let target_adapter = resolved.target_adapter;

    let model = compile_result
        .project
        .models
        .iter()
        .find(|m| m.config.name == model_name)
        .ok_or_else(|| anyhow::anyhow!("model '{model_name}' not found in project"))?;
    let t = &model.config.target;

    // DuckDB has no catalog level — emit a two-part `schema.table` name built
    // from validated identifiers only.
    let schema = rocky_sql::validation::validate_identifier(&t.schema)
        .map_err(|e| anyhow::anyhow!("invalid schema identifier: {e}"))?;
    let table = rocky_sql::validation::validate_identifier(&t.table)
        .map_err(|e| anyhow::anyhow!("invalid table identifier: {e}"))?;
    if !t.catalog.is_empty() {
        rocky_sql::validation::validate_identifier(&t.catalog)
            .map_err(|e| anyhow::anyhow!("invalid catalog identifier: {e}"))?;
    }
    let target_ref = format!("{schema}.{table}");

    let adapter = registry.warehouse_adapter(&target_adapter)?;

    // Probe the model's declared target. We use `describe_table` rather than
    // information_schema so the existence check goes through the adapter trait
    // (works for any future warehouse), and is cheap even on cold tables.
    let target_table_ref = rocky_ir::TableRef {
        catalog: t.catalog.clone(),
        schema: t.schema.clone(),
        table: t.table.clone(),
    };
    if adapter.describe_table(&target_table_ref).await.is_ok() {
        return Ok(PreparedKind::Ready(PreparedTable {
            adapter,
            table_ref: target_ref,
            fell_back_from: None,
        }));
    }

    // Target isn't materialized. Strict callers (ai-contract) refuse here —
    // drafting a contract from source data would be semantically wrong.
    match policy {
        FallbackPolicy::Strict => Ok(PreparedKind::Unavailable(format!(
            "model '{model_name}' target '{target_ref}' is not materialized. \
             Run `rocky run` to materialize the model first."
        ))),
        FallbackPolicy::SourceFallback => {
            match first_existing_source(adapter.as_ref(), model).await {
                Some(source_ref) => Ok(PreparedKind::Ready(PreparedTable {
                    adapter,
                    table_ref: source_ref,
                    fell_back_from: Some(target_ref),
                })),
                None => Ok(PreparedKind::Unavailable(format!(
                    "model '{model_name}' has no profile-able table: target \
                     '{target_ref}' is not materialized and no source table \
                     could be resolved. Run `rocky run` to materialize the \
                     model first."
                ))),
            }
        }
    }
}

/// Find the first source table for `model` that actually exists in the
/// warehouse. Tries explicit `[[sources]]` sidecar declarations first, then
/// falls back to FROM clauses parsed out of the model's SQL — useful when the
/// sidecar doesn't list sources (the common shape in the playground POCs).
/// Returns the DuckDB-style two-part `schema.table` ref, since this release
/// is DuckDB-only.
async fn first_existing_source(
    adapter: &dyn WarehouseAdapter,
    model: &rocky_core::models::Model,
) -> Option<String> {
    for src in &model.config.sources {
        let r = rocky_ir::TableRef {
            catalog: src.catalog.clone(),
            schema: src.schema.clone(),
            table: src.table.clone(),
        };
        if adapter.describe_table(&r).await.is_ok() {
            return Some(format!("{}.{}", src.schema, src.table));
        }
    }
    // SQL-extracted fallback. `referenced_tables` returns lowercased
    // `schema.table` or three-part names; unqualified names (no dot) can't be
    // resolved without a default schema, so we skip them.
    let refs = rocky_sql::lineage::referenced_tables(&model.sql).ok()?;
    for name in refs {
        let parts: Vec<&str> = name.split('.').collect();
        let table_ref = match parts.as_slice() {
            [s, t] => rocky_ir::TableRef {
                catalog: String::new(),
                schema: (*s).to_string(),
                table: (*t).to_string(),
            },
            [c, s, t] => rocky_ir::TableRef {
                catalog: (*c).to_string(),
                schema: (*s).to_string(),
                table: (*t).to_string(),
            },
            _ => continue,
        };
        if adapter.describe_table(&table_ref).await.is_ok() {
            return Some(format!("{}.{}", table_ref.schema, table_ref.table));
        }
    }
    None
}

/// Read a `serde_json::Value` cell as a `u64`, tolerating string-encoded ints.
fn as_u64(v: &serde_json::Value) -> u64 {
    match v {
        serde_json::Value::Number(n) => n.as_u64().unwrap_or(0),
        serde_json::Value::String(s) => s.parse().unwrap_or(0),
        _ => 0,
    }
}

/// Read a `serde_json::Value` cell as a display string, mapping SQL NULL to
/// `None`.
fn str_cell(v: Option<&serde_json::Value>) -> Option<String> {
    match v {
        Some(serde_json::Value::Null) | None => None,
        Some(serde_json::Value::String(s)) => Some(s.clone()),
        Some(other) => Some(other.to_string()),
    }
}

/// Profile one column.
///
/// By default this issues only aggregate **statistics** queries
/// (count / non-null / distinct count) — no raw cell values leave the machine.
/// When `with_data` is `true` the caller has opted into sending observed cell
/// values to the LLM, so it additionally captures `MIN`/`MAX` and (for
/// low-cardinality columns) the observed domain values.
///
/// SQL is built from the already-validated `table_ref` and a freshly-validated
/// column identifier — never from raw input.
pub(crate) async fn profile_column(
    adapter: &dyn WarehouseAdapter,
    table_ref: &str,
    typed_col: &TypedColumn,
    with_data: bool,
) -> Result<ColumnProfile> {
    let col = rocky_sql::validation::validate_identifier(&typed_col.name)
        .map_err(|e| anyhow::anyhow!("invalid column identifier: {e}"))?;

    // Aggregate STATISTICS only — counts, not row values. These are computed
    // on every path. MIN/MAX (which are real cell values) are only selected
    // when the caller opted into `--with-data`.
    let agg_sql = if with_data {
        format!(
            "SELECT COUNT(*) AS n, COUNT({col}) AS non_null, COUNT(DISTINCT {col}) AS distinct_n, \
             CAST(MIN({col}) AS VARCHAR) AS min_v, CAST(MAX({col}) AS VARCHAR) AS max_v \
             FROM {table_ref}"
        )
    } else {
        format!(
            "SELECT COUNT(*) AS n, COUNT({col}) AS non_null, COUNT(DISTINCT {col}) AS distinct_n \
             FROM {table_ref}"
        )
    };
    let qr = adapter.execute_query(&agg_sql).await.map_err(|e| {
        anyhow::anyhow!("profile query failed for column '{}': {e}", typed_col.name)
    })?;
    let row = qr.rows.first().ok_or_else(|| {
        anyhow::anyhow!("profile query returned no rows for '{}'", typed_col.name)
    })?;

    let total = row.first().map(as_u64).unwrap_or(0);
    let non_null = row.get(1).map(as_u64).unwrap_or(0);
    let distinct = row.get(2).map(as_u64).unwrap_or(0);
    let nulls = total.saturating_sub(non_null);
    let null_rate = if total == 0 {
        0.0
    } else {
        nulls as f64 / total as f64
    };
    let (min, max) = if with_data {
        (str_cell(row.get(3)), str_cell(row.get(4)))
    } else {
        (None, None)
    };

    // For low-cardinality columns, fetch the observed domain as evidence —
    // only when `--with-data` opted into shipping raw cell values.
    let observed_values = if with_data && distinct > 0 && distinct <= LOW_CARDINALITY_CAP {
        let domain_sql = format!(
            "SELECT DISTINCT CAST({col} AS VARCHAR) AS v FROM {table_ref} \
             WHERE {col} IS NOT NULL ORDER BY v LIMIT {DOMAIN_FETCH_LIMIT}"
        );
        let dr = adapter.execute_query(&domain_sql).await.map_err(|e| {
            anyhow::anyhow!("domain query failed for column '{}': {e}", typed_col.name)
        })?;
        dr.rows.iter().filter_map(|r| str_cell(r.first())).collect()
    } else {
        Vec::new()
    };

    Ok(ColumnProfile {
        name: typed_col.name.clone(),
        type_name: contract_type_name(typed_col),
        rows: total,
        nulls,
        null_rate,
        distinct,
        observed_values,
        min,
        max,
    })
}

/// Execute `rocky ai-contract <model>` — AI-draft a data contract from the
/// observed profile of a model's target table.
#[allow(clippy::too_many_arguments)]
pub async fn run_ai_contract(
    config_path: &Path,
    state_path: &Path,
    models_dir: &str,
    model_name: &str,
    save: bool,
    with_data: bool,
    output_json: bool,
    cache_ttl_override: Option<u64>,
) -> Result<()> {
    let client = make_client(config_path)?;
    let compile_result = compile_project(config_path, state_path, models_dir, cache_ttl_override)?;

    // The model's inferred output schema — the basis for compile-verification.
    let inferred_schema: Vec<TypedColumn> = compile_result
        .type_check
        .typed_models
        .get(model_name)
        .cloned()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "model '{model_name}' not found in compiled project (or has no inferred schema)"
            )
        })?;

    // Resolve the target table — refuses non-DuckDB adapters with a clear
    // message before any LLM tokens are spent. `Strict` policy: refuse if the
    // model isn't materialized rather than drafting a contract from source
    // data (which would be semantically wrong for non-passthrough models).
    let prepared = match prepare_table_query(
        config_path,
        &compile_result,
        model_name,
        FallbackPolicy::Strict,
    )
    .await?
    {
        PreparedKind::Ready(p) => p,
        PreparedKind::Unavailable(reason) => {
            anyhow::bail!(reason);
        }
    };

    // Egress notice: `rocky ai-contract` sends an LLM prompt to
    // api.anthropic.com. By default that prompt carries only aggregate
    // STATISTICS (row/null/distinct counts) — no raw cell values. `--with-data`
    // opts into additionally sending observed MIN/MAX and low-cardinality
    // domain samples, which ARE real cell values; name that explicitly on
    // stderr so it's never silent.
    if with_data {
        eprintln!(
            "ai-contract: --with-data enabled — will send observed min/max and \
             low-cardinality domain samples for {} column(s) of model '{}' to \
             api.anthropic.com (alongside schema + aggregate statistics).",
            inferred_schema.len(),
            model_name,
        );
    } else {
        eprintln!(
            "ai-contract: sending schema + aggregate statistics (row/null/distinct \
             counts) for {} column(s) of model '{}' to api.anthropic.com. No raw \
             cell values are sent. Pass --with-data to also include observed \
             min/max and domain samples.",
            inferred_schema.len(),
            model_name,
        );
    }

    // Profile each column of the model's inferred schema. The profile reads
    // the full target table (no sampling): a sampled `null_rate` of 0 would
    // not justify proposing `nullable = false`, so soundness requires the
    // exact count. By default only aggregate statistics are computed; raw cell
    // values (min/max/domain) are gated behind `--with-data`.
    let mut profiles = Vec::with_capacity(inferred_schema.len());
    for col in &inferred_schema {
        let p = profile_column(
            prepared.adapter.as_ref(),
            &prepared.table_ref,
            col,
            with_data,
        )
        .await?;
        profiles.push(p);
    }

    let table_profile = TableProfile {
        model: model_name.to_string(),
        columns: profiles,
    };

    // Draft the contract via the LLM, compile-verified against the model.
    let drafted = draft_contract(&table_profile, &inferred_schema, &client, 3)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    // Optionally persist as `<model>.contract.toml` next to the models dir.
    let saved_path = if save {
        let path = PathBuf::from(models_dir).join(format!("{model_name}.contract.toml"));
        std::fs::write(&path, &drafted.toml)
            .with_context(|| format!("writing {}", path.display()))?;
        Some(path.display().to_string())
    } else {
        None
    };

    let profile_out: Vec<AiContractColumnProfile> = table_profile
        .columns
        .iter()
        .map(|c| AiContractColumnProfile {
            name: c.name.clone(),
            type_name: c.type_name.clone(),
            rows: c.rows,
            nulls: c.nulls,
            null_rate: c.null_rate,
            distinct: c.distinct,
            observed_values: c.observed_values.clone(),
            min: c.min.clone(),
            max: c.max.clone(),
        })
        .collect();

    if output_json {
        let output = AiContractOutput {
            version: VERSION.to_string(),
            command: "ai_contract".to_string(),
            model: model_name.to_string(),
            attempts: drafted.attempts,
            contract_toml: drafted.toml.clone(),
            saved_path: saved_path.clone(),
            profile: profile_out,
        };
        print_json(&output)?;
    } else {
        println!("Drafted contract for model: {model_name}");
        println!("Attempts: {}", drafted.attempts);
        if let Some(path) = &saved_path {
            println!("Wrote: {path}");
        }
        println!();
        println!("{}", drafted.toml);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn as_u64_handles_number_and_string() {
        assert_eq!(as_u64(&serde_json::json!(42)), 42);
        assert_eq!(as_u64(&serde_json::json!("17")), 17);
        assert_eq!(as_u64(&serde_json::json!(null)), 0);
        assert_eq!(as_u64(&serde_json::json!("not-a-number")), 0);
    }

    #[test]
    fn str_cell_maps_null_to_none() {
        assert_eq!(
            str_cell(Some(&serde_json::json!("x"))),
            Some("x".to_string())
        );
        assert_eq!(str_cell(Some(&serde_json::Value::Null)), None);
        assert_eq!(str_cell(None), None);
        assert_eq!(str_cell(Some(&serde_json::json!(3))), Some("3".to_string()));
    }

    /// The DuckDB-only gate must refuse any non-DuckDB target with a clear,
    /// adapter-naming message — and a DuckDB target must NOT be refused.
    #[test]
    fn duckdb_only_gate_refuses_non_duckdb_adapter() {
        let snowflake = duckdb_only_refusal("snowflake").expect("snowflake must be refused");
        assert!(
            snowflake.contains("DuckDB-only"),
            "expected DuckDB-only refusal, got: {snowflake}"
        );
        assert!(
            snowflake.contains("snowflake"),
            "refusal should name the adapter: {snowflake}"
        );

        assert!(
            duckdb_only_refusal("databricks").is_some(),
            "databricks must be refused"
        );
        assert!(
            duckdb_only_refusal("duckdb").is_none(),
            "duckdb must not be refused"
        );
    }

    /// `resolve_target` reads the pipeline's target adapter type so the gate
    /// can fire before any compile or LLM call.
    #[test]
    fn resolve_target_reports_duckdb_type() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("rocky.toml");
        std::fs::write(
            &config_path,
            r#"
[adapter.warehouse]
type = "duckdb"
path = "warehouse.duckdb"

[pipeline.main]
type = "transformation"

[pipeline.main.target]
adapter = "warehouse"
"#,
        )
        .unwrap();
        let resolved = resolve_target(&config_path).expect("config should resolve");
        assert_eq!(resolved.adapter_type, "duckdb");
        assert_eq!(resolved.target_adapter, "warehouse");
        assert!(duckdb_only_refusal(&resolved.adapter_type).is_none());
    }

    // ── Egress safety: no raw cell values without --with-data (H2) ────────

    use std::sync::Mutex;

    use rocky_compiler::types::RockyType;
    use rocky_core::traits::{
        AdapterError, AdapterResult, QueryResult, SqlDialect, WarehouseAdapter,
    };
    use rocky_ir::{ColumnInfo, TableRef};

    /// A `WarehouseAdapter` that records every SQL string passed to
    /// `execute_query` and replies with a fixed three-column count row. Only
    /// `execute_query` is exercised by `profile_column`; the other trait
    /// methods are never called on this path and panic if they are.
    struct RecordingAdapter {
        queries: Mutex<Vec<String>>,
    }

    impl RecordingAdapter {
        fn new() -> Self {
            Self {
                queries: Mutex::new(Vec::new()),
            }
        }
        fn recorded(&self) -> Vec<String> {
            self.queries.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl WarehouseAdapter for RecordingAdapter {
        fn dialect(&self) -> &dyn SqlDialect {
            unimplemented!("profile_column never calls dialect()")
        }
        async fn execute_statement(&self, _sql: &str) -> AdapterResult<()> {
            unimplemented!("profile_column never calls execute_statement()")
        }
        async fn execute_query(&self, sql: &str) -> AdapterResult<QueryResult> {
            self.queries.lock().unwrap().push(sql.to_string());
            // n=100, non_null=100, distinct=3 — distinct is low enough that the
            // with-data path WOULD fire the domain query, so the no-data path's
            // absence of it is meaningful.
            Ok(QueryResult {
                columns: vec!["n".into(), "non_null".into(), "distinct_n".into()],
                rows: vec![vec![
                    serde_json::json!(100),
                    serde_json::json!(100),
                    serde_json::json!(3),
                ]],
            })
        }
        async fn describe_table(&self, _table: &TableRef) -> AdapterResult<Vec<ColumnInfo>> {
            Err(AdapterError::msg("not used"))
        }
    }

    fn col(name: &str) -> TypedColumn {
        TypedColumn {
            name: name.to_string(),
            data_type: RockyType::String,
            nullable: true,
        }
    }

    /// Without `--with-data`, `profile_column` must issue ONLY the statistics
    /// aggregate — no `MIN`/`MAX`, no `DISTINCT ... LIMIT` domain query — and
    /// the resulting profile must carry no cell values.
    #[tokio::test]
    async fn profile_column_without_data_sends_no_values() {
        let adapter = RecordingAdapter::new();
        let profile = profile_column(&adapter, "main.orders", &col("status"), false)
            .await
            .expect("profile should succeed");

        let queries = adapter.recorded();
        assert_eq!(queries.len(), 1, "exactly one statistics query expected");
        let q = &queries[0];
        assert!(
            !q.contains("MIN(") && !q.contains("MAX("),
            "no-data path must not select MIN/MAX: {q}"
        );
        assert!(
            !q.to_uppercase().contains("DISTINCT CAST"),
            "no-data path must not issue the domain-values query: {q}"
        );
        assert!(
            q.contains("COUNT(DISTINCT"),
            "distinct COUNT is a statistic and is fine: {q}"
        );

        assert!(profile.observed_values.is_empty(), "no domain values");
        assert!(profile.min.is_none(), "no min value");
        assert!(profile.max.is_none(), "no max value");
        assert_eq!(profile.rows, 100);
        assert_eq!(profile.distinct, 3);

        // And the assembled prompt evidence carries no value lines.
        let table = TableProfile {
            model: "orders".to_string(),
            columns: vec![profile],
        };
        let evidence = rocky_ai::contract::render_profile_evidence(&table);
        assert!(
            !evidence.contains("min="),
            "prompt must omit min: {evidence}"
        );
        assert!(
            !evidence.contains("max="),
            "prompt must omit max: {evidence}"
        );
        assert!(
            !evidence.contains("observed_domain="),
            "prompt must omit observed domain: {evidence}"
        );
    }

    /// With `--with-data`, the domain query fires for a low-cardinality column
    /// (sanity that the opt-in still works).
    #[tokio::test]
    async fn profile_column_with_data_issues_value_queries() {
        // A custom adapter that answers both the agg (5 cols) and domain query.
        struct WithDataAdapter {
            queries: Mutex<Vec<String>>,
        }
        #[async_trait::async_trait]
        impl WarehouseAdapter for WithDataAdapter {
            fn dialect(&self) -> &dyn SqlDialect {
                unimplemented!()
            }
            async fn execute_statement(&self, _sql: &str) -> AdapterResult<()> {
                unimplemented!()
            }
            async fn execute_query(&self, sql: &str) -> AdapterResult<QueryResult> {
                self.queries.lock().unwrap().push(sql.to_string());
                if sql.to_uppercase().contains("DISTINCT CAST") {
                    Ok(QueryResult {
                        columns: vec!["v".into()],
                        rows: vec![vec![serde_json::json!("active")]],
                    })
                } else {
                    Ok(QueryResult {
                        columns: vec![
                            "n".into(),
                            "non_null".into(),
                            "distinct_n".into(),
                            "min_v".into(),
                            "max_v".into(),
                        ],
                        rows: vec![vec![
                            serde_json::json!(10),
                            serde_json::json!(10),
                            serde_json::json!(2),
                            serde_json::json!("active"),
                            serde_json::json!("inactive"),
                        ]],
                    })
                }
            }
            async fn describe_table(&self, _t: &TableRef) -> AdapterResult<Vec<ColumnInfo>> {
                Err(AdapterError::msg("not used"))
            }
        }

        let adapter = WithDataAdapter {
            queries: Mutex::new(Vec::new()),
        };
        let profile = profile_column(&adapter, "main.orders", &col("status"), true)
            .await
            .expect("profile should succeed");

        assert_eq!(profile.min.as_deref(), Some("active"));
        assert_eq!(profile.max.as_deref(), Some("inactive"));
        assert_eq!(profile.observed_values, vec!["active".to_string()]);
        let recorded = adapter.queries.lock().unwrap().clone();
        assert!(
            recorded.iter().any(|q| q.contains("MIN(")),
            "with-data path must select MIN/MAX"
        );
        assert!(
            recorded
                .iter()
                .any(|q| q.to_uppercase().contains("DISTINCT CAST")),
            "with-data path must issue the domain query"
        );
    }

    /// Live end-to-end: profiles a real DuckDB table and drafts a
    /// compile-verified contract. Gated on `ANTHROPIC_API_KEY` + `#[ignore]`
    /// per the repo convention for LLM commands. Run with:
    /// `cargo test -p rocky-cli ai_contract -- --ignored --nocapture`.
    #[tokio::test]
    #[ignore = "requires ANTHROPIC_API_KEY and network access"]
    async fn live_drafts_contract_for_duckdb_model() {
        if std::env::var(ANTHROPIC_API_KEY_VAR).is_err() {
            eprintln!("skipping: ANTHROPIC_API_KEY not set");
            return;
        }

        // Build a tiny self-contained DuckDB project: a seeded source +
        // a transformation model whose target table we profile.
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let db_path = root.join("warehouse.duckdb");
        std::fs::write(
            root.join("rocky.toml"),
            format!(
                r#"
[adapter.warehouse]
type = "duckdb"
path = "{}"

[pipeline.main]
type = "transformation"

[pipeline.main.target]
adapter = "warehouse"
"#,
                db_path.display()
            ),
        )
        .unwrap();

        let models_dir = root.join("models");
        std::fs::create_dir_all(&models_dir).unwrap();
        std::fs::write(
            models_dir.join("orders.sql"),
            "SELECT * FROM main.raw_orders",
        )
        .unwrap();
        std::fs::write(
            models_dir.join("orders.toml"),
            r#"
[target]
schema = "main"
table = "orders"

[strategy]
type = "full_refresh"
"#,
        )
        .unwrap();

        // Seed both the source the model reads (`raw_orders`) and the target
        // the profiler reads (`orders`) via the warehouse adapter. (A full
        // `rocky run` is out of scope for this gate; we only need the physical
        // tables to exist with data.)
        let config_path = root.join("rocky.toml");
        let cfg = rocky_core::config::load_rocky_config(&config_path).unwrap();
        let registry = crate::registry::AdapterRegistry::from_config(&cfg).unwrap();
        let adapter = registry.warehouse_adapter("warehouse").unwrap();
        for stmt in [
            "CREATE SCHEMA IF NOT EXISTS main",
            "CREATE TABLE main.raw_orders (id BIGINT, status VARCHAR)",
            "INSERT INTO main.raw_orders VALUES (1,'completed'),(2,'completed'),(3,'pending')",
            "CREATE TABLE main.orders AS SELECT * FROM main.raw_orders",
        ] {
            adapter.execute_statement(stmt).await.unwrap();
        }

        let state_path = root.join("state.redb");
        let result = run_ai_contract(
            &config_path,
            &state_path,
            models_dir.to_str().unwrap(),
            "orders",
            true, // save
            true, // with_data (exercise the values-included path)
            true, // output_json
            None, // cache_ttl
        )
        .await;
        assert!(result.is_ok(), "ai-contract failed: {result:?}");
        assert!(
            root.join("models/orders.contract.toml").exists(),
            "contract file should be written with --save"
        );
    }
}
