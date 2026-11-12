//! `rocky ai-contract <model>` — AI-drafted data contracts from observed data.
//!
//! Profiles a model's target table per column on the configured warehouse, then
//! asks the LLM to propose a `.contract.toml` grounded in that profile. The
//! proposal is compile-verified against the model before it's accepted, so the
//! reported contract is one `rocky compile` will accept.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use rocky_ai::client::{AiConfig, DEFAULT_MAX_TOKENS, LlmClient};
use rocky_ai::contract::{ColumnProfile, TableProfile, contract_type_name, draft_contract};
use rocky_ai::residency::AiResidency;
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
/// back to the default when there is no `rocky.toml`, and REFUSES a
/// `rocky.toml` that is present and does not load (#1680).
///
/// Credential-TOLERANT here, deliberately, even though `rocky ai-contract` DOES
/// connect: the warehouse leg has its own strict load below
/// (`resolve_target` → `load_rocky_config(config_path)?`), which is where the
/// credentials have to resolve. This read only picks a token ceiling, so
/// failing it on an unset `${VAR}` would refuse a command the strict leg would
/// have accepted.
///
/// The prompt carries statistics (and, with `--with-data`, values) of the
/// model's table in the target warehouse of the project's pipeline, so the
/// client is built for that data: V062 refuses here, before the warehouse is
/// profiled and before any request is sent, when it may not go to the `[ai]`
/// endpoint's region.
fn make_client(config_path: &Path, model_name: &str) -> Result<LlmClient> {
    // Config first, API key second — same reasoning as `commands::ai`.
    let project = rocky_core::config::load_optional_project_config(Some(config_path))
        .with_context(|| format!("failed to load config from {}", config_path.display()))?;
    let ai_section = project.as_ref().map(|cfg| &cfg.ai);
    let max_tokens = ai_section.map_or(DEFAULT_MAX_TOKENS, |ai| ai.max_tokens);
    let base_url =
        rocky_ai::client::resolve_base_url(ai_section.and_then(|ai| ai.base_url.clone()));

    let api_key = std::env::var(ANTHROPIC_API_KEY_VAR)
        .context("ANTHROPIC_API_KEY not set. Set it to use `rocky ai-contract`.")?;

    let config = AiConfig {
        provider: "anthropic".to_string(),
        model: "claude-sonnet-4-6".to_string(),
        api_key: RedactedString::new(api_key),
        default_format: "rocky".to_string(),
        max_attempts: 3,
        max_tokens,
        base_url,
    };

    let data = profile_data_regions(project.as_ref(), model_name);
    let residency = AiResidency::for_data(project.as_ref(), &data);
    LlmClient::new(config, residency).map_err(|e| anyhow::anyhow!("{e}"))
}

/// Where the profiled table of `model_name` lives: the target of the
/// pipeline the profile runs on (`resolve_pipeline(cfg, None)`, as
/// `resolve_target` picks it), and its source too for a replication or
/// snapshot pipeline, whose target holds data copied from the source. When
/// no single pipeline resolves, the region is unknown.
pub fn profile_data_regions(
    project: Option<&rocky_core::config::RockyConfig>,
    model_name: &str,
) -> Vec<rocky_core::config::AiDataRegion> {
    let Some(cfg) = project else {
        return Vec::new();
    };
    let data = format!("statistics of the table of model `{model_name}`");
    let Ok((_, pipeline)) = crate::registry::resolve_pipeline(cfg, None) else {
        return vec![rocky_core::config::AiDataRegion { data, region: None }];
    };
    let mut regions = vec![pipeline.target_region()];
    if let Some(source) = pipeline.source_region() {
        regions.push(source);
    }
    regions
        .into_iter()
        .map(|region| rocky_core::config::AiDataRegion {
            data: data.clone(),
            region: region.map(str::to_string),
        })
        .collect()
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
    compile_project_with_live(
        config_path,
        state_path,
        models_dir,
        cache_ttl_override,
        &HashMap::new(),
    )
}

/// [`compile_project`], with `live` source schemas laid over the cached ones.
///
/// `live` is what [`describe_leaf_sources`] read from the warehouse. A live
/// entry replaces the cached entry of the same `schema.table`: the warehouse
/// is the fresher answer. A leaf with no live entry keeps its cached schema,
/// and a leaf with neither stays `Unknown`.
pub(crate) fn compile_project_with_live(
    config_path: &Path,
    state_path: &Path,
    models_dir: &str,
    cache_ttl_override: Option<u64>,
    live: &HashMap<String, Vec<TypedColumn>>,
) -> Result<CompileResult> {
    // Present-but-unloadable `rocky.toml` refuses (#1625); absent still gives
    // a cold map.
    let mut source_schemas = crate::source_schemas::load_project_source_schemas(
        config_path,
        state_path,
        cache_ttl_override,
    )?;
    overlay_live_schemas(&mut source_schemas, live);

    let config = CompilerConfig {
        models_dir: PathBuf::from(models_dir),
        contracts_dir: None,
        source_schemas,
        // Each model's warehouse, so a cast's column type matches
        // `rocky compile` (#2342).
        target_dialects: super::compile::target_dialects_at(config_path),
        ..Default::default()
    };
    compile(&config).map_err(|e| anyhow::anyhow!("{e}"))
}

/// Leaf source schemas read live from the warehouse for one compile.
#[derive(Debug, Default)]
pub struct LiveLeafSchemas {
    /// `schema.table` (as the model SQL spells it, lowercased) to the columns
    /// the warehouse reported, typed by the same mapper the schema cache uses.
    pub schemas: HashMap<String, Vec<TypedColumn>>,
    /// Leaf reads Rocky could not describe, one entry each. Their columns stay
    /// `Unknown` (or keep a cached schema when there is one).
    pub unresolved: Vec<String>,
}

/// The tables the project's models read that no model writes: the leaves
/// whose column types only the warehouse (or the schema cache) can supply.
///
/// Reads come from `rocky_sql::lineage::referenced_tables`, which lowercases
/// them. A one-part name is a model reference (or unresolvable without a
/// default schema) and is skipped. A two- or three-part name that matches a
/// model's own `schema.table` target is that model, not a leaf, and is
/// skipped too, the same rule as the `E045` missing-table check.
///
/// Only the reads of the models named in `scope` count (see
/// [`live_describe_scope`]); every model's target still counts as written.
pub fn leaf_source_reads(
    models: &[rocky_core::models::Model],
    scope: &HashSet<String>,
) -> BTreeSet<String> {
    let written: HashSet<String> = models
        .iter()
        .map(|m| {
            format!(
                "{}.{}",
                m.config.target.schema.to_lowercase(),
                m.config.target.table.to_lowercase()
            )
        })
        .collect();
    let mut leaves = BTreeSet::new();
    for model in models.iter().filter(|m| scope.contains(&m.config.name)) {
        let Ok(reads) = rocky_sql::lineage::referenced_tables(&model.sql) else {
            continue;
        };
        for read in reads {
            let parts: Vec<&str> = read.split('.').collect();
            let two_part = match parts.as_slice() {
                [s, t] | [_, s, t] => format!("{s}.{t}"),
                _ => continue,
            };
            if written.contains(&two_part) {
                continue;
            }
            leaves.insert(read);
        }
    }
    leaves
}

/// Describe every leaf source of `models` through `adapter` and type the
/// columns it reports.
///
/// The schema cache is filled only by a replication `rocky run` and by
/// `rocky discover --with-schemas`, so in a transformation-only project every
/// leaf column typechecks as `Unknown`. The contract validator then compares
/// no declared type (`I003`) and can prove no column non-null (`E012`), and a
/// wrong draft goes through. A command that already holds a warehouse
/// connection reads the leaves' real schemas here instead.
///
/// Nothing is written: the result lives for one compile. A leaf whose
/// describe fails, or comes back with no columns, is reported in
/// [`LiveLeafSchemas::unresolved`] and stays `Unknown`. Rocky never guesses
/// a leaf's type.
///
/// `nullable` is what the adapter reports. The cache stores the same field
/// from the same adapters, so `E012` places no new trust in it.
pub async fn describe_leaf_sources(
    adapter: &dyn WarehouseAdapter,
    models: &[rocky_core::models::Model],
    scope: &HashSet<String>,
) -> LiveLeafSchemas {
    let mut out = LiveLeafSchemas::default();
    for read in leaf_source_reads(models, scope) {
        let parts: Vec<&str> = read.split('.').collect();
        let (catalog, schema, table) = match parts.as_slice() {
            [s, t] => ("", *s, *t),
            [c, s, t] => (*c, *s, *t),
            _ => continue,
        };
        let table_ref = rocky_ir::TableRef {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
        };
        match adapter.describe_table(&table_ref).await {
            Ok(columns) if !columns.is_empty() => {
                let typed = columns
                    .into_iter()
                    .map(|c| TypedColumn {
                        data_type: rocky_compiler::compile::default_type_mapper(&c.data_type),
                        name: c.name,
                        nullable: c.nullable,
                    })
                    .collect();
                out.schemas.insert(read, typed);
            }
            Ok(_) => out
                .unresolved
                .push(format!("{read}: the warehouse reported no columns")),
            Err(e) => out.unresolved.push(format!("{read}: {e}")),
        }
    }
    out
}

/// The models whose leaf reads a live describe for `model_name` may type:
/// the model itself and every model upstream of it, each kept only when every
/// warehouse it runs on is the default pipeline's target adapter, the one the
/// describe runs on.
///
/// A live entry replaces the cached entry for every model that reads the same
/// table, so a leaf read by a model on another warehouse would be typed by the
/// wrong warehouse. A model whose warehouses are unknown, or include another
/// adapter, is left out and keeps its cached schema. A config that does not
/// load gives an empty scope: no live describe at all.
pub fn live_describe_scope(
    config_path: &Path,
    project: &rocky_compiler::project::Project,
    model_name: &str,
) -> HashSet<String> {
    let Ok(cfg) = rocky_core::config::load_rocky_config(config_path) else {
        return HashSet::new();
    };
    let Ok((_, pipeline)) = crate::registry::resolve_pipeline(&cfg, None) else {
        return HashSet::new();
    };
    let Some(adapter) = cfg.adapters.get(pipeline.target_adapter()) else {
        return HashSet::new();
    };
    let targets = rocky_compiler::model_targets::ModelTargets::resolve(&cfg, config_path);
    upstream_kept(project, model_name, |name| {
        let on = targets.for_model(name);
        !on.is_empty() && on.iter().all(|a| std::ptr::eq(*a, adapter))
    })
}

/// `model_name` and every model upstream of it, through the resolved DAG,
/// that `keep` accepts. The walk goes through a model `keep` refuses, so a
/// model above it can still be kept.
fn upstream_kept(
    project: &rocky_compiler::project::Project,
    model_name: &str,
    keep: impl Fn(&str) -> bool,
) -> HashSet<String> {
    let deps: HashMap<&str, &[String]> = project
        .dag_nodes
        .iter()
        .map(|n| (n.name.as_str(), n.depends_on.as_slice()))
        .collect();
    let mut upstream = HashSet::new();
    let mut stack = vec![model_name.to_string()];
    while let Some(name) = stack.pop() {
        if !upstream.insert(name.clone()) {
            continue;
        }
        if let Some(parents) = deps.get(name.as_str()) {
            stack.extend(parents.iter().cloned());
        }
    }
    upstream.retain(|name| keep(name));
    upstream
}

/// Lay `live` over `cached`: a live entry replaces the cached entry of the
/// same key, compared without regard to ASCII case so a cached
/// `Seeds.Orders` does not survive beside a live `seeds.orders`.
pub fn overlay_live_schemas(
    cached: &mut HashMap<String, Vec<TypedColumn>>,
    live: &HashMap<String, Vec<TypedColumn>>,
) {
    if live.is_empty() {
        return;
    }
    cached.retain(|key, _| !live.keys().any(|l| l.eq_ignore_ascii_case(key)));
    for (key, columns) in live {
        cached.insert(key.clone(), columns.clone());
    }
}

/// A validated table reference plus the adapter to run it on.
///
/// `table_ref` is SQL the profiler interpolates into `FROM`: the adapter's
/// dialect builds it from `table` and validates every segment first, so SQL
/// is never built from raw input and each warehouse gets its own quoting.
pub(crate) struct PreparedTable {
    pub(crate) adapter: std::sync::Arc<dyn WarehouseAdapter>,
    /// The structured coordinates of the profiled table.
    pub(crate) table: rocky_ir::TableRef,
    /// The dialect-built SQL reference for `table`.
    pub(crate) table_ref: String,
    /// Set when the model's declared target wasn't materialized and we fell
    /// back to a source table (only when `FallbackPolicy::SourceFallback` is
    /// requested). Carries the declared-target name that was missing so the
    /// caller can surface "this is a source preview, not the model output" in
    /// its output. `None` means we profiled the declared target directly.
    pub(crate) fell_back_from: Option<String>,
}

/// Either a runnable table query or the reason profiling cannot run (for
/// example, the model's target is not materialized).
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

/// The target adapter resolved from config.
struct ResolvedTarget {
    registry: crate::registry::AdapterRegistry,
    target_adapter: String,
    adapter_type: String,
}

/// Resolve the pipeline's target adapter and its type from config. Pure of any
/// model lookup so it is unit-testable without a compile.
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

/// The dotted display name of a table: `catalog.schema.table`, or
/// `schema.table` when there is no catalog. Unquoted, for humans and JSON
/// output — never interpolated into SQL.
pub(crate) fn display_table_name(t: &rocky_ir::TableRef) -> String {
    if t.catalog.is_empty() {
        format!("{}.{}", t.schema, t.table)
    } else {
        format!("{}.{}.{}", t.catalog, t.schema, t.table)
    }
}

/// The SQL reference for `t`, built by the adapter's dialect — the same
/// `format_table_ref` `rocky run` writes the table with. The dialect
/// validates every segment, so a hostile name is refused here.
fn dialect_table_ref(adapter: &dyn WarehouseAdapter, t: &rocky_ir::TableRef) -> Result<String> {
    adapter
        .dialect()
        .format_table_ref(&t.catalog, &t.schema, &t.table)
        .map_err(|e| anyhow::anyhow!("invalid table reference '{}': {e}", display_table_name(t)))
}

/// Whether `sql_ref` resolves on the warehouse, by a zero-row query through
/// the exact ref the profile will use. `describe_table` is not enough: an
/// adapter may build its own differently-quoted ref for it (Snowflake's
/// describe quotes, while a source's grounding ref does not), so a describe
/// can pass for a ref that then fails to query, or fail for one that works.
/// Zero rows, so it reads no data.
async fn ref_resolves(adapter: &dyn WarehouseAdapter, sql_ref: &str) -> bool {
    let sql = adapter
        .dialect()
        .select_limited("1 AS probe", &format!("FROM {sql_ref}"), 0);
    adapter.execute_query(&sql).await.is_ok()
}

/// The dialect's grounding ref for a table Rocky did not create (a source):
/// see [`first_existing_source`] for why it differs from [`dialect_table_ref`].
fn grounding_table_ref(adapter: &dyn WarehouseAdapter, t: &rocky_ir::TableRef) -> Result<String> {
    let parts: Vec<&str> = if t.catalog.is_empty() {
        vec![&t.schema, &t.table]
    } else {
        vec![&t.catalog, &t.schema, &t.table]
    };
    adapter
        .dialect()
        .ground_table_ref(&parts)
        .map_err(|e| anyhow::anyhow!("invalid table reference '{}': {e}", display_table_name(t)))
}

/// Resolve a model's target table into a validated table ref + warehouse
/// adapter, on any configured warehouse. The ref comes from the adapter's
/// dialect, so quoting and catalog handling match what `rocky run` writes.
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
    let registry = resolved.registry;
    let target_adapter = resolved.target_adapter;

    let model = compile_result
        .project
        .models
        .iter()
        .find(|m| m.config.name == model_name)
        .ok_or_else(|| anyhow::anyhow!("model '{model_name}' not found in project"))?;
    let t = &model.config.target;
    let target = rocky_ir::TableRef {
        catalog: t.catalog.clone(),
        schema: t.schema.clone(),
        table: t.table.clone(),
    };

    let adapter = registry.warehouse_adapter(&target_adapter)?;
    let target_name = display_table_name(&target);
    // Validates every segment through the dialect before any query runs. A
    // name this dialect cannot address (an empty BigQuery project, a catalog
    // on ClickHouse) makes the profile unavailable, not an error.
    let target_ref = match dialect_table_ref(adapter.as_ref(), &target) {
        Ok(r) => r,
        Err(e) => {
            return Ok(PreparedKind::Unavailable(format!(
                "model '{model_name}' target '{target_name}' cannot be profiled on the \
                 '{}' adapter: {e:#}",
                resolved.adapter_type
            )));
        }
    };

    // Probe the model's declared target with the ref the profile will query,
    // so "exists" means "the profile query resolves".
    if ref_resolves(adapter.as_ref(), &target_ref).await {
        return Ok(PreparedKind::Ready(PreparedTable {
            adapter,
            table: target,
            table_ref: target_ref,
            fell_back_from: None,
        }));
    }

    // Target isn't materialized. Strict callers (ai-contract) refuse here —
    // drafting a contract from source data would be semantically wrong.
    match policy {
        FallbackPolicy::Strict => Ok(PreparedKind::Unavailable(format!(
            "model '{model_name}' target '{target_name}' is not materialized \
             on the '{}' adapter. Run `rocky run` to materialize the model first.",
            resolved.adapter_type
        ))),
        FallbackPolicy::SourceFallback => {
            match first_existing_source(adapter.as_ref(), model).await {
                Some((table, table_ref)) => Ok(PreparedKind::Ready(PreparedTable {
                    adapter,
                    table,
                    table_ref,
                    fell_back_from: Some(target_name),
                })),
                None => Ok(PreparedKind::Unavailable(format!(
                    "model '{model_name}' has no profile-able table: target \
                     '{target_name}' is not materialized and no source table \
                     could be resolved. Run `rocky run` to materialize the \
                     model first."
                ))),
            }
        }
    }
}

/// Find the first source table for `model` that actually exists in the
/// warehouse, with its SQL reference. Tries explicit `[[sources]]` sidecar
/// declarations first, then falls back to FROM clauses parsed out of the
/// model's SQL — useful when the sidecar doesn't list sources (the common
/// shape in the playground POCs).
///
/// A source gets the dialect's `ground_table_ref`, not the `format_table_ref`
/// the target gets. Rocky did not create a source table, so its ref must
/// resolve the way the model's own SQL names it: `ground_table_ref` leaves it
/// unquoted on the dialects that fold case (Snowflake), where a quoted
/// lowercase name would miss an uppercase table. The SQL parser also
/// lowercases the names it returns. A candidate whose name the dialect
/// refuses is skipped.
async fn first_existing_source(
    adapter: &dyn WarehouseAdapter,
    model: &rocky_core::models::Model,
) -> Option<(rocky_ir::TableRef, String)> {
    for src in &model.config.sources {
        let r = rocky_ir::TableRef {
            catalog: src.catalog.clone(),
            schema: src.schema.clone(),
            table: src.table.clone(),
        };
        let Ok(sql_ref) = grounding_table_ref(adapter, &r) else {
            continue;
        };
        if ref_resolves(adapter, &sql_ref).await {
            return Some((r, sql_ref));
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
        let Ok(sql_ref) = grounding_table_ref(adapter, &table_ref) else {
            continue;
        };
        if ref_resolves(adapter, &sql_ref).await {
            return Some((table_ref, sql_ref));
        }
    }
    None
}

/// Read a count cell as a `u64`, tolerating string-encoded and integral
/// float values (`"17"`, `17.0`, `"17.0"`), which some warehouses return.
///
/// Anything else is an error, never 0: a row count read as 0 would show an
/// empty table, and a null count that comes out 0 would back a draft
/// `nullable = false` the data does not support.
fn as_u64(v: &serde_json::Value) -> Option<u64> {
    let integral = |f: f64| (f.is_finite() && f >= 0.0 && f.fract() == 0.0).then_some(f as u64);
    match v {
        serde_json::Value::Number(n) => n.as_u64().or_else(|| n.as_f64().and_then(integral)),
        serde_json::Value::String(s) => {
            let s = s.trim();
            s.parse()
                .ok()
                .or_else(|| s.parse::<f64>().ok().and_then(integral))
        }
        _ => None,
    }
}

/// Read count cell `i` of a profile row, or say which one did not parse.
fn count_cell(row: &[serde_json::Value], i: usize, what: &str, column: &str) -> Result<u64> {
    let cell = row.get(i);
    cell.and_then(as_u64).ok_or_else(|| {
        anyhow::anyhow!(
            "profile query for column '{column}' returned an unreadable {what}: {}",
            cell.map_or_else(|| "nothing".to_string(), ToString::to_string)
        )
    })
}

/// Whether a profile may read raw cell values.
///
/// An enum, not a `bool`, so every caller names its egress choice: a stray
/// `true` would send real values to the LLM.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CellValues {
    /// Aggregate counts only. No raw cell value is read. The MCP
    /// `ai_contract` tool and `rocky ai-contract` without `--with-data`.
    Withheld,
    /// Also read `MIN`/`MAX` and a low-cardinality column's value list.
    /// `rocky profile` and `rocky ai-contract --with-data`.
    Included,
}

/// Read a `serde_json::Value` cell as a display string, mapping SQL NULL to
/// `None`.
pub(crate) fn str_cell(v: Option<&serde_json::Value>) -> Option<String> {
    match v {
        Some(serde_json::Value::Null) | None => None,
        Some(serde_json::Value::String(s)) => Some(s.clone()),
        Some(other) => Some(other.to_string()),
    }
}

/// Profile one column.
///
/// With [`CellValues::Withheld`] this issues only aggregate **statistics**
/// queries (count / non-null / distinct count) — no raw cell values leave the
/// machine. With [`CellValues::Included`] the caller has opted into reading
/// observed cell values, so it additionally captures `MIN`/`MAX` and (for
/// low-cardinality columns) the observed domain values.
///
/// SQL is built from the already-validated `table_ref` and a freshly-validated
/// column identifier — never from raw input. `table_ref` must be a reference
/// the adapter's dialect built (`format_table_ref` or `ground_table_ref`).
///
/// # Cost
///
/// Each query scans the whole column: no sampling, because a sampled null
/// count of 0 would not justify `nullable = false`. On a metered warehouse
/// (Snowflake, BigQuery, Databricks) a profile of a large table costs money.
pub async fn profile_column(
    adapter: &dyn WarehouseAdapter,
    table_ref: &str,
    typed_col: &TypedColumn,
    values: CellValues,
) -> Result<ColumnProfile> {
    let with_data = values == CellValues::Included;
    let col = rocky_sql::validation::validate_identifier(&typed_col.name)
        .map_err(|e| anyhow::anyhow!("invalid column identifier: {e}"))?;

    // Aggregate STATISTICS only — counts, not row values. These are computed
    // on every path. MIN/MAX (which are real cell values) are only selected
    // when the caller opted into `--with-data`.
    let agg_sql = if with_data {
        // The dialect's text type: a bare `VARCHAR` is `VARCHAR(30)` in a
        // T-SQL `CAST` and not a type at all on BigQuery.
        let text = adapter.dialect().string_type_name();
        format!(
            "SELECT COUNT(*) AS n, COUNT({col}) AS non_null, COUNT(DISTINCT {col}) AS distinct_n, \
             CAST(MIN({col}) AS {text}) AS min_v, CAST(MAX({col}) AS {text}) AS max_v \
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

    let total = count_cell(row, 0, "row count", &typed_col.name)?;
    let non_null = count_cell(row, 1, "non-null count", &typed_col.name)?;
    let distinct = count_cell(row, 2, "distinct count", &typed_col.name)?;
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
        let dialect = adapter.dialect();
        let domain_sql = dialect.select_limited(
            &format!(
                "DISTINCT CAST({col} AS {}) AS v",
                dialect.string_type_name()
            ),
            &format!("FROM {table_ref} WHERE {col} IS NOT NULL ORDER BY v"),
            DOMAIN_FETCH_LIMIT,
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
    let client = make_client(config_path, model_name)?;
    let compile_result = compile_project(config_path, state_path, models_dir, cache_ttl_override)?;
    if !compile_result
        .type_check
        .typed_models
        .contains_key(model_name)
    {
        anyhow::bail!(
            "model '{model_name}' not found in compiled project (or has no inferred schema)"
        );
    }

    // Resolve the target table before any LLM tokens are spent. `Strict`
    // policy: refuse if the model isn't materialized rather than drafting a
    // contract from source data (which would be semantically wrong for
    // non-passthrough models).
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

    // Type the leaf sources from the warehouse this command is connected to,
    // then compile again. Without this a transformation-only project has an
    // empty schema cache, every leaf column is `Unknown`, and the validator
    // checks no declared type and proves no column non-null.
    let scope = live_describe_scope(config_path, &compile_result.project, model_name);
    let live = describe_leaf_sources(
        prepared.adapter.as_ref(),
        &compile_result.project.models,
        &scope,
    )
    .await;
    let compile_result = if live.schemas.is_empty() {
        compile_result
    } else {
        compile_project_with_live(
            config_path,
            state_path,
            models_dir,
            cache_ttl_override,
            &live.schemas,
        )?
    };

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
            if with_data {
                CellValues::Included
            } else {
                CellValues::Withheld
            },
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
            unverified_types: drafted.unverified_types.clone(),
            unmatched_columns: drafted.unmatched_columns.clone(),
            unproven_non_null: drafted.unproven_non_null.clone(),
            unresolved_sources: live.unresolved.clone(),
        };
        print_json(&output)?;
    } else {
        println!("Drafted contract for model: {model_name}");
        println!("Attempts: {}", drafted.attempts);
        if let Some(path) = &saved_path {
            println!("Wrote: {path}");
        }
        // Said out loud, because the draft otherwise reads as verified.
        if !drafted.unmatched_columns.is_empty() {
            println!();
            println!(
                "{} declared column(s) are NOT produced by the model:",
                drafted.unmatched_columns.len()
            );
            for message in &drafted.unmatched_columns {
                println!("  - {message}");
            }
        }
        if !live.unresolved.is_empty() {
            println!();
            println!(
                "{} source table(s) could not be described, so their columns stay untyped:",
                live.unresolved.len()
            );
            for message in &live.unresolved {
                println!("  - {message}");
            }
        }
        if !drafted.unverified_types.is_empty() {
            println!();
            println!(
                "{} declared type(s) were NOT checked — Rocky could not work out the \
                 column's own type, so it compared nothing:",
                drafted.unverified_types.len()
            );
            for message in &drafted.unverified_types {
                println!("  - {message}");
            }
        }
        if !drafted.unproven_non_null.is_empty() {
            println!();
            println!(
                "{} column(s) had no NULLs, but Rocky cannot prove them non-null, so \
                 the draft leaves `nullable` unset:",
                drafted.unproven_non_null.len()
            );
            for message in &drafted.unproven_non_null {
                println!("  - {message}");
            }
        }
        println!();
        println!("{}", drafted.toml);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The `rocky ai-contract` / MCP `ai_contract` profile: its statistics
    /// are in the region of the project's pipeline, source and target, and
    /// V062 judges both against `[ai] region`.
    #[test]
    fn profile_data_regions_cover_the_pipeline_source_and_target() {
        let tmp = tempfile::tempdir().unwrap();
        let load = |extra: &str| {
            let path = tmp.path().join("rocky.toml");
            std::fs::write(
                &path,
                format!(
                    "[adapter]\ntype = \"duckdb\"\npath = \"x.duckdb\"\n\n\
                     [pipeline.p.source]\nregion = \"eu\"\n\n\
                     [pipeline.p.source.schema_pattern]\nprefix = \"src__\"\n\
                     separator = \"__\"\ncomponents = [\"source\"]\n\n\
                     [pipeline.p.target]\ncatalog_template = \"wh\"\n\
                     schema_template = \"raw__{{source}}\"\nregion = \"eu\"\n{extra}"
                ),
            )
            .unwrap();
            rocky_core::config::load_optional_project_config(Some(&path))
                .expect("loads")
                .expect("present")
        };

        let cfg = load("\n[ai]\nregion = \"us\"\n");
        let data = profile_data_regions(Some(&cfg), "orders");
        assert_eq!(data.len(), 2, "{data:?}");
        assert!(data.iter().all(|d| d.region.as_deref() == Some("eu")));
        let err = rocky_core::config::check_ai_residency(Some(&cfg), &data)
            .expect_err("EU statistics to a US endpoint");
        assert!(err.to_string().starts_with("V062"), "{err}");

        let same = load("\n[ai]\nregion = \"eu\"\n");
        let data = profile_data_regions(Some(&same), "orders");
        assert!(rocky_core::config::check_ai_residency(Some(&same), &data).is_ok());
    }

    #[test]
    fn as_u64_handles_number_and_string() {
        assert_eq!(as_u64(&serde_json::json!(42)), Some(42));
        assert_eq!(as_u64(&serde_json::json!("17")), Some(17));
        assert_eq!(as_u64(&serde_json::json!(17.0)), Some(17));
        assert_eq!(as_u64(&serde_json::json!(" 17.0 ")), Some(17));
        // Never a silent 0: an unreadable count is an error upstream.
        assert_eq!(as_u64(&serde_json::json!(null)), None);
        assert_eq!(as_u64(&serde_json::json!("not-a-number")), None);
        assert_eq!(as_u64(&serde_json::json!(1.5)), None);
        assert_eq!(as_u64(&serde_json::json!(-3)), None);
        let row = vec![serde_json::json!(10), serde_json::json!("x")];
        assert_eq!(count_cell(&row, 0, "row count", "c").unwrap(), 10);
        let err = count_cell(&row, 1, "non-null count", "c").unwrap_err();
        assert!(
            err.to_string().contains("unreadable non-null count"),
            "{err}"
        );
        assert!(count_cell(&row, 2, "distinct count", "c").is_err());
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

    /// A stub warehouse on the Trino dialect, which double-quotes every
    /// segment in `format_table_ref` and leaves `ground_table_ref` unquoted.
    /// Only a query naming a table in `existing` by its unquoted ref works.
    struct QuotingWarehouse {
        dialect: rocky_trino::TrinoDialect,
        existing: Vec<(String, String, String)>,
    }

    #[async_trait::async_trait]
    impl WarehouseAdapter for QuotingWarehouse {
        fn dialect(&self) -> &dyn SqlDialect {
            &self.dialect
        }
        async fn execute_statement(&self, _sql: &str) -> AdapterResult<()> {
            unimplemented!("not used")
        }
        /// Answers the zero-row probe only when it names an existing table
        /// by its unquoted ref, the way a case-folding warehouse resolves a
        /// source; a quoted ref never resolves here.
        async fn execute_query(&self, sql: &str) -> AdapterResult<QueryResult> {
            let hit = self
                .existing
                .iter()
                .any(|(c, s, t)| sql.contains(&format!("FROM {c}.{s}.{t} ")));
            if hit {
                Ok(QueryResult {
                    columns: vec!["probe".into()],
                    rows: Vec::new(),
                })
            } else {
                Err(AdapterError::msg("no such table"))
            }
        }
        async fn describe_table(&self, _t: &TableRef) -> AdapterResult<Vec<ColumnInfo>> {
            // Succeeds for everything: the probe must not trust it.
            Ok(Vec::new())
        }
    }

    fn quoting_warehouse(existing: &[(&str, &str, &str)]) -> QuotingWarehouse {
        QuotingWarehouse {
            dialect: rocky_trino::TrinoDialect,
            existing: existing
                .iter()
                .map(|(c, s, t)| (c.to_string(), s.to_string(), t.to_string()))
                .collect(),
        }
    }

    fn table(catalog: &str, schema: &str, name: &str) -> TableRef {
        TableRef {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: name.to_string(),
        }
    }

    /// The profiled table's SQL ref comes from the adapter's dialect, not a
    /// hardcoded two-part `schema.table`: a quoting dialect quotes it, the
    /// catalog is kept, and a hostile segment is refused before any SQL.
    #[test]
    fn table_ref_is_built_by_the_dialect() {
        let wh = quoting_warehouse(&[]);
        let t = table("lake", "staging", "orders");
        assert_eq!(
            dialect_table_ref(&wh, &t).unwrap(),
            r#""lake"."staging"."orders""#
        );
        assert_eq!(display_table_name(&t), "lake.staging.orders");
        assert_eq!(
            display_table_name(&table("", "staging", "orders")),
            "staging.orders"
        );
        let hostile = table("lake", "staging", "orders; DROP TABLE x");
        assert!(dialect_table_ref(&wh, &hostile).is_err());
    }

    /// The source fallback gives a declared `[[sources]]` entry and a name
    /// parsed from the SQL the dialect's unquoted grounding ref, not the
    /// quoted target ref. Tables that do not exist are skipped.
    #[tokio::test]
    async fn source_fallback_refs_go_through_the_dialect() {
        let dir = tempfile::tempdir().unwrap();
        let sql = dir.path().join("m.sql");
        let toml = dir.path().join("m.toml");
        std::fs::write(&sql, "SELECT id FROM lake.raw.orders").unwrap();
        std::fs::write(
            &toml,
            "[target]\ncatalog = \"lake\"\nschema = \"staging\"\n\n\
             [[sources]]\ncatalog = \"lake\"\nschema = \"raw\"\ntable = \"declared\"\n",
        )
        .unwrap();
        let model = rocky_core::models::load_model_pair(&sql, &toml, None).unwrap();

        // The declared source exists: it wins, with the unquoted grounding ref.
        let wh = quoting_warehouse(&[("lake", "raw", "declared"), ("lake", "raw", "orders")]);
        let (t, sql_ref) = first_existing_source(&wh, &model).await.unwrap();
        assert_eq!(t, table("lake", "raw", "declared"));
        assert_eq!(sql_ref, "lake.raw.declared");

        // Only the SQL-referenced table exists: `ground_table_ref`, unquoted.
        let wh = quoting_warehouse(&[("lake", "raw", "orders")]);
        let (t, sql_ref) = first_existing_source(&wh, &model).await.unwrap();
        assert_eq!(t, table("lake", "raw", "orders"));
        assert_eq!(sql_ref, "lake.raw.orders");

        // Nothing exists.
        assert!(
            first_existing_source(&quoting_warehouse(&[]), &model)
                .await
                .is_none()
        );
    }

    /// `resolve_target` reads the pipeline's target adapter and its type from
    /// config, before any compile or LLM call.
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
        let profile = profile_column(
            &adapter,
            "main.orders",
            &col("status"),
            CellValues::Withheld,
        )
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
        let evidence = rocky_ai::contract::render_profile_evidence(&table, &[]);
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
            // The domain query takes its row limit and text type from the
            // dialect (`TOP` on SQL Server, `LIMIT` here).
            fn dialect(&self) -> &dyn SqlDialect {
                &rocky_databricks::dialect::DatabricksSqlDialect
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
        let profile = profile_column(
            &adapter,
            "main.orders",
            &col("status"),
            CellValues::Included,
        )
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

    /// A transformation project on DuckDB with a cold schema cache: the leaf
    /// `raw.orders` is untyped from the cache alone, so the validator checks
    /// no drafted type (`I003`) and proves no column non-null. Read live from
    /// the warehouse, the same compile types every column, keeps the NOT
    /// NULL proof, and refuses a wrong drafted type with `E011`. Nothing is
    /// written to the state store.
    #[tokio::test]
    async fn live_leaf_schemas_type_a_cold_cache_compile() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let db_path = root.join("w.duckdb");
        std::fs::write(
            root.join("rocky.toml"),
            format!(
                "[adapter]\ntype = \"duckdb\"\npath = \"{}\"\n\n\
                 [pipeline.t]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
                 [pipeline.t.target.governance]\nauto_create_schemas = true\n",
                db_path.display()
            ),
        )
        .unwrap();
        let models_dir = root.join("models");
        std::fs::create_dir_all(&models_dir).unwrap();
        std::fs::write(
            models_dir.join("stg.sql"),
            "SELECT id, LOWER(status) AS status, note FROM raw.orders",
        )
        .unwrap();
        std::fs::write(
            models_dir.join("stg.toml"),
            "[target]\ncatalog = \"w\"\nschema = \"main\"\ntable = \"stg\"\n",
        )
        .unwrap();
        // A second model reads the first by its qualified target: that read
        // is a model, not a leaf, and is never described.
        std::fs::write(models_dir.join("downstream.sql"), "SELECT id FROM main.stg").unwrap();
        std::fs::write(
            models_dir.join("downstream.toml"),
            "depends_on = [\"stg\"]\n[target]\ncatalog = \"w\"\nschema = \"main\"\ntable = \"downstream\"\n",
        )
        .unwrap();

        let config_path = root.join("rocky.toml");
        let cfg = rocky_core::config::load_rocky_config(&config_path).unwrap();
        let registry = crate::registry::AdapterRegistry::from_config(&cfg).unwrap();
        let adapter = registry.warehouse_adapter("default").unwrap();
        for stmt in [
            "CREATE SCHEMA raw",
            "CREATE TABLE raw.orders (id BIGINT NOT NULL, status VARCHAR NOT NULL, note VARCHAR)",
            "INSERT INTO raw.orders VALUES (1, 'A', NULL)",
        ] {
            adapter.execute_statement(stmt).await.unwrap();
        }
        let state_path = root.join("state.redb");
        let models = models_dir.to_str().unwrap();

        let cold = compile_project(&config_path, &state_path, models, None).unwrap();
        let cold_stg = &cold.type_check.typed_models["stg"];
        assert!(
            cold_stg
                .iter()
                .all(|c| c.data_type == rocky_compiler::types::RockyType::Unknown && c.nullable),
            "a cold cache types nothing: {cold_stg:?}"
        );
        let scope = live_describe_scope(&config_path, &cold.project, "downstream");
        assert_eq!(
            scope,
            HashSet::from(["downstream".to_string(), "stg".to_string()]),
            "the drafted model and its upstream chain, all on the one warehouse"
        );
        assert_eq!(
            live_describe_scope(&config_path, &cold.project, "stg"),
            HashSet::from(["stg".to_string()]),
            "a downstream model is not in the scope"
        );
        assert_eq!(
            leaf_source_reads(&cold.project.models, &scope),
            BTreeSet::from(["raw.orders".to_string()]),
            "`main.stg` is a model's target, not a leaf"
        );

        let live = describe_leaf_sources(adapter.as_ref(), &cold.project.models, &scope).await;
        assert!(live.unresolved.is_empty(), "{:?}", live.unresolved);
        let warm =
            compile_project_with_live(&config_path, &state_path, models, None, &live.schemas)
                .unwrap();
        let stg = &warm.type_check.typed_models["stg"];
        let get = |name: &str| stg.iter().find(|c| c.name == name).unwrap();
        use rocky_compiler::types::RockyType;
        assert_eq!(
            (&get("id").data_type, get("id").nullable),
            (&RockyType::Int64, false)
        );
        assert_eq!(
            (&get("status").data_type, get("status").nullable),
            (&RockyType::String, false)
        );
        assert_eq!(
            (&get("note").data_type, get("note").nullable),
            (&RockyType::String, true)
        );

        // The validator now checks the drafted type instead of passing it.
        let wrong: rocky_compiler::contracts::CompilerContract =
            toml::from_str("[[columns]]\nname = \"id\"\ntype = \"String\"\n").unwrap();
        let cold_diags = rocky_compiler::contracts::validate_contract("stg", cold_stg, &wrong);
        assert!(cold_diags.iter().all(|d| !d.is_error()), "{cold_diags:?}");
        let warm_diags = rocky_compiler::contracts::validate_contract("stg", stg, &wrong);
        assert!(
            warm_diags.iter().any(|d| &*d.code == "E011"),
            "{warm_diags:?}"
        );
        assert!(!state_path.exists(), "the live read persists nothing");
    }

    /// A leaf the warehouse does not have is reported and stays untyped;
    /// the compile does not fail.
    #[tokio::test]
    async fn an_undescribable_leaf_is_reported_not_guessed() {
        let dir = tempfile::tempdir().unwrap();
        let models_dir = dir.path().join("models");
        std::fs::create_dir_all(&models_dir).unwrap();
        std::fs::write(models_dir.join("m.sql"), "SELECT a FROM nowhere.missing").unwrap();
        std::fs::write(
            models_dir.join("m.toml"),
            "[target]\ncatalog = \"w\"\nschema = \"main\"\ntable = \"m\"\n",
        )
        .unwrap();
        let config_path = dir.path().join("rocky.toml");
        let compiled = compile_project(
            &config_path,
            &dir.path().join("state.redb"),
            models_dir.to_str().unwrap(),
            None,
        )
        .unwrap();
        let adapter = rocky_duckdb::adapter::DuckDbWarehouseAdapter::in_memory().unwrap();
        let live = describe_leaf_sources(
            &adapter,
            &compiled.project.models,
            &HashSet::from(["m".to_string()]),
        )
        .await;
        assert!(live.schemas.is_empty());
        assert_eq!(live.unresolved.len(), 1);
        assert!(
            live.unresolved[0].starts_with("nowhere.missing: "),
            "{:?}",
            live.unresolved
        );
    }

    /// The scope follows the DAG upstream only, and leaves out a model the
    /// filter refuses (one on another warehouse) without cutting off the
    /// models above it. A model outside the scope adds no leaf.
    #[test]
    fn the_live_scope_is_the_upstream_chain_on_the_one_warehouse() {
        let dir = tempfile::tempdir().unwrap();
        let models_dir = dir.path().join("models");
        std::fs::create_dir_all(&models_dir).unwrap();
        for (name, sql, deps) in [
            ("base", "SELECT a FROM raw.base", ""),
            (
                "mid",
                "SELECT a FROM raw.elsewhere JOIN base USING (a)",
                "\"base\"",
            ),
            ("top", "SELECT a FROM mid", "\"mid\""),
            ("side", "SELECT a FROM raw.side", ""),
        ] {
            std::fs::write(models_dir.join(format!("{name}.sql")), sql).unwrap();
            std::fs::write(
                models_dir.join(format!("{name}.toml")),
                format!(
                    "depends_on = [{deps}]\n[target]\ncatalog = \"w\"\nschema = \"main\"\ntable = \"{name}\"\n"
                ),
            )
            .unwrap();
        }
        let compiled = compile_project(
            &dir.path().join("rocky.toml"),
            &dir.path().join("state.redb"),
            models_dir.to_str().unwrap(),
            None,
        )
        .unwrap();
        let scope = upstream_kept(&compiled.project, "top", |name| name != "mid");
        assert_eq!(
            scope,
            HashSet::from(["top".to_string(), "base".to_string()])
        );
        assert_eq!(
            leaf_source_reads(&compiled.project.models, &scope),
            BTreeSet::from(["raw.base".to_string()]),
            "`raw.elsewhere` is read only by the refused model, `raw.side` by none in scope"
        );
    }

    #[test]
    fn a_live_entry_replaces_a_cached_one_whatever_its_case() {
        use rocky_compiler::types::RockyType;
        let col = |ty| {
            vec![TypedColumn {
                name: "id".into(),
                data_type: ty,
                nullable: true,
            }]
        };
        let mut cached = HashMap::from([
            ("Raw.Orders".to_string(), col(RockyType::String)),
            ("raw.other".to_string(), col(RockyType::Date)),
        ]);
        let live = HashMap::from([("raw.orders".to_string(), col(RockyType::Int64))]);
        overlay_live_schemas(&mut cached, &live);
        assert_eq!(cached.len(), 2);
        assert_eq!(cached["raw.orders"][0].data_type, RockyType::Int64);
        assert_eq!(cached["raw.other"][0].data_type, RockyType::Date);
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

    // ------------------------------------------------------------------
    // #1680 — the two config reads `rocky ai-contract` makes offline.
    // ------------------------------------------------------------------

    /// Parses as TOML, fails a validator: `fivetran` is discovery-only and
    /// needs `kind = "discovery"`. Present-and-broken, never absent.
    const BROKEN_CONFIG_1680: &str =
        "[adapter.ft]\ntype = \"fivetran\"\napi_key = \"k\"\napi_secret = \"s\"\n";

    /// Loads under the credential-TOLERANT loader (#1536), refuses under the
    /// strict one: `${...}` is unset and sits in an adapter connection field.
    const UNSET_CREDENTIAL_CONFIG_1680: &str = "[adapters.wh]\ntype = \"databricks\"\n\
         host = \"${ROCKY_T_1680_UNSET}\"\n";

    /// The converted caller (#1667): a present-but-unloadable `rocky.toml`
    /// refuses the grounding compile, naming the file.
    #[test]
    fn ai_contract_compile_project_refuses_a_present_but_unloadable_config() {
        let tmp = tempfile::tempdir().unwrap();
        let models_dir = tmp.path().join("models");
        std::fs::create_dir_all(&models_dir).unwrap();
        std::fs::write(models_dir.join("m.sql"), "SELECT 1 AS id").unwrap();
        std::fs::write(
            models_dir.join("m.toml"),
            "name = \"m\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
             [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"m\"\n",
        )
        .unwrap();
        let cfg = tmp.path().join("rocky.toml");
        std::fs::write(&cfg, BROKEN_CONFIG_1680).unwrap();

        // `let ... else` rather than `expect_err`: `CompileResult` is not `Debug`.
        let Err(err) = compile_project(
            &cfg,
            &tmp.path().join("state.redb"),
            models_dir.to_str().unwrap(),
            None,
        ) else {
            panic!("a present but unloadable rocky.toml must refuse the ai-contract compile");
        };
        let rendered = format!("{err:#}");
        assert!(
            rendered.contains("failed to load config from") && rendered.contains("rocky.toml"),
            "the refusal must name the config file, got: {rendered}"
        );
    }

    /// Honest-failure guard for the converted caller.
    #[test]
    fn ai_contract_compile_project_still_runs_without_any_config() {
        let tmp = tempfile::tempdir().unwrap();
        let models_dir = tmp.path().join("models");
        std::fs::create_dir_all(&models_dir).unwrap();
        std::fs::write(models_dir.join("m.sql"), "SELECT 1 AS id").unwrap();
        std::fs::write(
            models_dir.join("m.toml"),
            "name = \"m\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
             [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"m\"\n",
        )
        .unwrap();
        let cfg = tmp.path().join("rocky.toml");
        assert!(!cfg.exists());

        compile_project(
            &cfg,
            &tmp.path().join("state.redb"),
            models_dir.to_str().unwrap(),
            None,
        )
        .expect("a missing rocky.toml must not refuse the ai-contract compile");
    }

    /// The `[ai]` secondary read refuses a present-but-unloadable config.
    #[test]
    fn ai_contract_make_client_refuses_a_present_but_unloadable_config() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = tmp.path().join("rocky.toml");
        std::fs::write(&cfg, BROKEN_CONFIG_1680).unwrap();

        let Err(err) = make_client(&cfg, "m") else {
            panic!("a present but unloadable rocky.toml must refuse the ai-contract client");
        };
        let rendered = format!("{err:#}");
        assert!(
            rendered.contains("failed to load config from") && rendered.contains("rocky.toml"),
            "the refusal must name the config file, got: {rendered}"
        );
    }

    /// Honest-failure, case (b): a VALID config whose adapter connection field
    /// holds an unset `${VAR}` must not refuse this read. `rocky ai-contract`
    /// DOES connect to the warehouse, but the strict load that has to resolve
    /// those credentials is `resolve_target`, further down — failing here would
    /// refuse a command the strict leg would have accepted.
    #[test]
    fn ai_contract_make_client_tolerates_an_unset_credential_var() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = tmp.path().join("rocky.toml");
        std::fs::write(&cfg, UNSET_CREDENTIAL_CONFIG_1680).unwrap();

        assert!(
            rocky_core::config::load_rocky_config(&cfg).is_err(),
            "fixture must fail the STRICT loader, or this proves nothing"
        );
        match make_client(&cfg, "m") {
            Ok(_) => {}
            Err(e) => {
                let rendered = format!("{e:#}");
                assert!(
                    rendered.contains("ANTHROPIC_API_KEY"),
                    "an unset credential var must not refuse the client: {rendered}"
                );
            }
        }
    }
}
