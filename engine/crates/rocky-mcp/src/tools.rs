//! The Rocky MCP server: tool definitions, the `ServerHandler` impl, and the
//! projection helpers that map Rocky's typed `*Output` cores into the lite,
//! schemars-1.x result types in [`crate::result_types`].

use std::path::{Path, PathBuf};

use rmcp::ErrorData as McpError;
use rmcp::RoleServer;
use rmcp::handler::server::router::prompt::PromptRouter;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
// `GetPromptRequestParams`, `PaginatedRequestParams`, and `ListPromptsResult`
// are referenced unqualified in the code the `#[prompt_handler]` macro emits,
// so they must be in scope here even though this module names none of them.
use rmcp::model::{
    GetPromptRequestParams, GetPromptResult, Implementation, ListPromptsResult,
    PaginatedRequestParams, PromptMessage, PromptMessageRole, ProtocolVersion, ServerCapabilities,
    ServerInfo,
};
use rmcp::service::RequestContext;
use rmcp::{
    Json, ServerHandler, prompt, prompt_handler, prompt_router, tool, tool_handler, tool_router,
};

use rocky_cli::commands;
use rocky_compiler::compile::{self, CompileResult as CompilerResult, CompilerConfig};

use crate::result_types::*;

/// The server's `instructions` — the agent-authoring workflow. Sourced from
/// the single canonical skill so the MCP guidance never drifts from the
/// `rocky-ai-workflow` skill. Path is relative to this source file:
/// `crates/rocky-mcp/src` → repo root is four `..` segments.
const INSTRUCTIONS: &str = include_str!("../../../../.claude/skills/rocky-ai-workflow/SKILL.md");

/// Stateless Rocky MCP server. Holds only the project locators; every tool
/// call recompiles from the current on-disk files (correctness over a warm
/// cache — caching is a deferred optimization).
#[derive(Clone)]
pub struct RockyMcpServer {
    config_path: PathBuf,
    models_dir: PathBuf,
    root: PathBuf,
    tool_router: ToolRouter<Self>,
    prompt_router: PromptRouter<Self>,
}

// ---------------------------------------------------------------------------
// Tool input parameter structs (schemars 1.x — rmcp's `Parameters<T>` bound).
// ---------------------------------------------------------------------------

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct CompileArgs {
    /// Optional single-model filter; compile-checks the whole project either
    /// way but scopes the returned diagnostics to this model when set.
    #[serde(default)]
    pub model: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct PlanPreviewArgs {
    /// Optional single-model filter. When unset, previews every model.
    #[serde(default)]
    pub model: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct LineageArgs {
    /// The focal model.
    pub model: String,
    /// When set, scope lineage to this column (column-level trace).
    #[serde(default)]
    pub column: Option<String>,
    /// When `true`, trace downstream consumers instead of upstream sources.
    #[serde(default)]
    pub downstream: bool,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ListArgs {
    /// What to list: `"models"`, `"pipelines"`, `"adapters"`, or `"sources"`.
    pub kind: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct InspectSchemaArgs {}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct SampleRowsArgs {
    /// What to sample: a compiled model name, OR a qualified `schema.table`
    /// (or `catalog.schema.table`) reference to a raw source table. A dotted
    /// reference resolves directly against the warehouse and needs no compiled
    /// model, so it also works at cold start (a project with zero models yet).
    pub model: String,
    /// Random-sample percentage (1–100). Omit to return the first rows
    /// deterministically — the right default for small tables, where a low
    /// percentage sample can return zero rows.
    #[serde(default)]
    pub percent: Option<u32>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ProfileColumnArgs {
    /// What to profile: a compiled model name, OR a qualified `schema.table`
    /// (or `catalog.schema.table`) reference to a raw source table.
    pub model: String,
    /// The column to profile.
    pub column: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ProposeArgs {
    /// Single model to materialize. When unset, the plan covers every model.
    #[serde(default)]
    pub model: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct BreakingChangeArgs {
    /// Git ref to compare the working tree against. Defaults to `"HEAD"`.
    #[serde(default = "default_base_ref")]
    pub base: String,
}

fn default_base_ref() -> String {
    "HEAD".to_string()
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct DependentsArgs {
    /// The focal model whose downstream consumers to resolve.
    pub model: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct CatalogArgs {}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct HistoryArgs {
    /// When set, return that model's execution history instead of the
    /// project-level run summary.
    #[serde(default)]
    pub model: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct MetricsArgs {
    /// The model whose quality metrics to read.
    pub model: String,
    /// When set, also return a per-run trend for this single column.
    #[serde(default)]
    pub column: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct OptimizeArgs {
    /// Substring filter on model name. When unset, analyses every model with
    /// run history.
    #[serde(default)]
    pub model: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct SuggestFreshnessBlockArgs {
    /// The model the `[freshness]` block is for (used in the prompt context).
    pub model: String,
    /// Candidate temporal columns (timestamp/date) the block's `time_column`
    /// may be chosen from — typically the model's date/timestamp columns.
    pub temporal_columns: Vec<String>,
    /// The model's current sidecar `.toml` text, so the draft does not
    /// duplicate or conflict with an existing block. Optional.
    #[serde(default)]
    pub current_sidecar: Option<String>,
}

// ---------------------------------------------------------------------------
// Prompt argument structs (schemars 1.x — rmcp's `Parameters<T>` bound).
// MCP prompt arguments are string-typed on the wire; `Serialize` is part of
// the prompt-macro contract (mirrors rmcp's own examples).
// ---------------------------------------------------------------------------

#[derive(Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct BuildModelArgs {
    /// What the user wants to build — the model's purpose in their own words
    /// (e.g. "daily completed-orders revenue by region"). The prompt threads
    /// this intent through Rocky's authoring loop.
    pub intent: String,
}

// ---------------------------------------------------------------------------
// Caps for the data-grounding tools.
// ---------------------------------------------------------------------------

const SAMPLE_MAX_ROWS: usize = 50;
const SAMPLE_MAX_BYTES: usize = 16 * 1024;
const CELL_MAX_CHARS: usize = 256;
/// Max distinct values `profile_column` lists in `top_values`; above this the
/// column is treated as high-cardinality and the value list is omitted.
const PROFILE_TOP_VALUES_MAX: usize = 25;

#[tool_router(router = tool_router)]
impl RockyMcpServer {
    /// Build a server rooted at `config_path`'s directory; the models
    /// directory is `<config-dir>/models` (the CLI's top-level convention).
    pub fn new(config_path: PathBuf) -> Self {
        let root = config_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        let models_dir = root.join("models");
        Self {
            config_path,
            models_dir,
            root,
            tool_router: Self::tool_router(),
            prompt_router: Self::prompt_router(),
        }
    }

    fn state_path(&self) -> PathBuf {
        rocky_core::state::resolve_state_path(None, &self.models_dir).path
    }

    /// Path to the project's `data/seed.sql`, if it exists. The playground
    /// convention is `<project>/models/` + `<project>/data/seed.sql`, so the
    /// parent of the models dir is the place to look.
    fn seed_file(&self) -> Option<PathBuf> {
        let p = self
            .models_dir
            .parent()
            .unwrap_or(Path::new("."))
            .join("data")
            .join("seed.sql");
        p.is_file().then_some(p)
    }

    /// Compile the project in-process, returning the raw compiler result for
    /// the lineage / inspect tools. Source schemas come from the warm schema
    /// cache when one exists, degrading to empty on a cold cache (typecheck
    /// then falls back to `Unknown` for source-leaf columns — the same
    /// behaviour as `rocky compile` without a warm cache).
    fn compile_full(&self) -> anyhow::Result<CompilerResult> {
        let source_schemas = self.load_source_schemas();
        let config = CompilerConfig {
            models_dir: self.models_dir.clone(),
            contracts_dir: None,
            source_schemas,
            source_column_info: std::collections::HashMap::new(),
            ..Default::default()
        };
        compile::compile(&config).map_err(|e| anyhow::anyhow!("compile failed: {e}"))
    }

    /// Load typed source schemas from the persisted schema cache, honouring
    /// `[cache.schemas]`. Returns an empty map on a cold cache / missing
    /// config / disabled cache — the typecheck degrades to `Unknown`.
    fn load_source_schemas(
        &self,
    ) -> std::collections::HashMap<String, Vec<rocky_compiler::types::TypedColumn>> {
        use rocky_compiler::schema_cache::load_source_schemas_from_cache;
        use rocky_core::state::StateStore;

        let Ok(cfg) = rocky_core::config::load_rocky_config(&self.config_path) else {
            return std::collections::HashMap::new();
        };
        if !cfg.cache.schemas.enabled {
            return std::collections::HashMap::new();
        }
        let Ok(store) = StateStore::open_read_only(&self.state_path()) else {
            return std::collections::HashMap::new();
        };
        load_source_schemas_from_cache(&store, chrono::Utc::now(), cfg.cache.schemas.ttl())
            .unwrap_or_default()
    }

    /// Classify the semantic breaking changes between the working tree (HEAD
    /// of the on-disk files) and the models at `base_ref`. Reuses the exact
    /// compile + classify path `rocky review` runs: compile HEAD with the warm
    /// source-schema cache, `extract_base_compile` the base ref, lower both to
    /// `ProjectIr`, and `diff_project_ir`.
    ///
    /// On any step that prevents the gate from running (HEAD or base fails to
    /// compile — typically because the project isn't a git repo), returns a
    /// result with `skipped_reason` set and zeroed counts so the caller can
    /// distinguish "clean diff" from "gate didn't run".
    fn compute_breaking_change(&self, base_ref: &str) -> BreakingChangeResult {
        let source_schemas = self.load_source_schemas();

        let config = CompilerConfig {
            models_dir: self.models_dir.clone(),
            contracts_dir: None,
            source_schemas: source_schemas.clone(),
            source_column_info: std::collections::HashMap::new(),
            ..Default::default()
        };
        let head_compile = match compile::compile(&config) {
            Ok(r) => r,
            Err(e) => {
                return BreakingChangeResult {
                    skipped_reason: Some(format!("HEAD compile failed: {e}")),
                    ..Default::default()
                };
            }
        };

        let base_compile =
            match commands::extract_base_compile(base_ref, &self.models_dir, source_schemas) {
                Ok(r) => r,
                Err(reason) => {
                    return BreakingChangeResult {
                        skipped_reason: Some(format!("base ref '{base_ref}': {reason}")),
                        ..Default::default()
                    };
                }
            };

        let base_ir = commands::project_ir_from_compile(&base_compile);
        let head_ir = commands::project_ir_from_compile(&head_compile);
        let findings = rocky_core::breaking_change::diff_project_ir(&base_ir, &head_ir);

        let breaking_count = findings.iter().filter(|f| f.is_breaking()).count();
        let lite = findings.iter().map(breaking_finding_lite).collect();
        BreakingChangeResult {
            has_breaking: breaking_count > 0,
            breaking_count,
            findings: lite,
            skipped_reason: None,
        }
    }

    // -------------------------- MUST tools ---------------------------------

    #[tool(
        description = "Type-check the Rocky project and return diagnostics (errors/warnings) \
         plus model count. Always reflects the current on-disk models. Read diagnostics' \
         code/span/suggestion and fix against them — this is the fast feedback loop."
    )]
    async fn compile(
        &self,
        params: Parameters<CompileArgs>,
    ) -> Result<Json<CompileResult>, String> {
        let model = params.0.model.as_deref();
        // `--with-seed` hard-fails when `data/seed.sql` is absent, so opt in
        // only when the project actually ships a seed (the playground does);
        // otherwise rely on the warm schema cache / cold-cache degradation.
        let with_seed = self.seed_file().is_some();
        let output = commands::compile_output(
            Some(&self.config_path),
            &self.state_path(),
            &self.models_dir,
            None,
            model,
            false,
            None,
            with_seed,
            None,
        )
        .map_err(|e| format!("{e:#}"))?;
        Ok(Json(project_compile_result(&output)))
    }

    #[tool(
        description = "Preview the exact SQL Rocky would execute for the project's \
         transformation models (offline, no warehouse connection). Read it to confirm the \
         generated SQL matches intent before proposing a materialization."
    )]
    async fn plan_preview(
        &self,
        params: Parameters<PlanPreviewArgs>,
    ) -> Result<Json<PlanPreviewResult>, String> {
        let model = params.0.model.as_deref();
        let output =
            commands::plan_preview_output(Some(&self.config_path), &self.models_dir, model, None)
                .map_err(|e| format!("{e:#}"))?;
        let statements = output
            .statements
            .into_iter()
            .map(|s| PlannedStatementLite {
                purpose: s.purpose,
                target: s.target,
                sql: s.sql,
            })
            .collect();
        Ok(Json(PlanPreviewResult { statements }))
    }

    #[tool(
        description = "Explore column-level lineage for a model. Without `column`, returns the \
         model's columns plus upstream/downstream models and the model-level edge set. With \
         `column`, returns the column trace; set `downstream` to trace consumers instead of sources."
    )]
    async fn lineage(
        &self,
        params: Parameters<LineageArgs>,
    ) -> Result<Json<LineageResult>, String> {
        let args = params.0;
        let result = self.compile_full().map_err(|e| format!("{e:#}"))?;

        if let Some(column) = args.column.as_deref() {
            let out =
                commands::column_lineage_output(&result, &args.model, column, args.downstream)
                    .map_err(|e| format!("{e:#}"))?;
            let edges = out.trace.iter().map(edge_lite).collect();
            Ok(Json(LineageResult {
                model: out.model,
                column: Some(out.column),
                direction: Some(out.direction),
                columns: vec![],
                upstream: vec![],
                downstream: vec![],
                edges,
            }))
        } else {
            let out =
                commands::lineage_output(&result, &args.model).map_err(|e| format!("{e:#}"))?;
            let edges = out.edges.iter().map(edge_lite).collect();
            let columns = out.columns.into_iter().map(|c| c.name).collect();
            Ok(Json(LineageResult {
                model: out.model,
                column: None,
                direction: None,
                columns,
                upstream: out.upstream,
                downstream: out.downstream,
                edges,
            }))
        }
    }

    #[tool(
        description = "Run the project's DuckDB-backed local tests (contracts + assertions) and \
         return pass/fail counts plus per-failure detail. Use after writing or changing a model."
    )]
    async fn test(&self) -> Result<Json<TestResult>, String> {
        let output =
            commands::test_output(&self.models_dir, None, None).map_err(|e| format!("{e:#}"))?;
        let failures = output
            .failures
            .into_iter()
            .map(|f| TestFailureLite {
                name: f.name,
                error: f.error,
            })
            .collect();
        Ok(Json(TestResult {
            total: output.total,
            passed: output.passed,
            failures,
        }))
    }

    #[tool(
        description = "List project entities. `kind` is one of: models, pipelines, adapters, sources."
    )]
    async fn list(&self, params: Parameters<ListArgs>) -> Result<Json<ListResult>, String> {
        let kind = params.0.kind;
        let entries = match kind.as_str() {
            "models" => {
                let out =
                    commands::list_models_output(&self.models_dir).map_err(|e| format!("{e:#}"))?;
                out.models
                    .into_iter()
                    .map(|m| ListEntry {
                        name: m.name,
                        target: Some(m.target),
                        strategy: Some(m.strategy),
                        depends_on: m.depends_on,
                        ..Default::default()
                    })
                    .collect()
            }
            "pipelines" => {
                let out = commands::list_pipelines_output(&self.config_path)
                    .map_err(|e| format!("{e:#}"))?;
                out.pipelines
                    .into_iter()
                    .map(|p| ListEntry {
                        name: p.name,
                        pipeline_type: Some(p.pipeline_type),
                        target_adapter: Some(p.target_adapter),
                        depends_on: p.depends_on,
                        ..Default::default()
                    })
                    .collect()
            }
            "adapters" => {
                let out = commands::list_adapters_output(&self.config_path)
                    .map_err(|e| format!("{e:#}"))?;
                out.adapters
                    .into_iter()
                    .map(|a| ListEntry {
                        name: a.name,
                        adapter_type: Some(a.adapter_type),
                        host: a.host,
                        ..Default::default()
                    })
                    .collect()
            }
            "sources" => {
                let out = commands::list_sources_output(&self.config_path)
                    .map_err(|e| format!("{e:#}"))?;
                out.sources
                    .into_iter()
                    .map(|s| ListEntry {
                        name: s.pipeline,
                        adapter: Some(s.adapter),
                        catalog: s.catalog,
                        ..Default::default()
                    })
                    .collect()
            }
            other => {
                return Err(format!(
                    "unknown kind '{other}' — expected models, pipelines, adapters, or sources"
                ));
            }
        };
        Ok(Json(ListResult { kind, entries }))
    }

    #[tool(
        description = "Return the typed columns of every model and source table in the project. \
         Use this to learn what's available to select from and the upstream types — never guess \
         column names."
    )]
    async fn inspect_schema(
        &self,
        _params: Parameters<InspectSchemaArgs>,
    ) -> Result<Json<InspectSchemaResult>, String> {
        let to_entries = |buckets: Vec<(String, Vec<rocky_compiler::types::TypedColumn>)>| {
            buckets
                .into_iter()
                .map(|(name, cols)| SchemaEntry {
                    name,
                    columns: cols
                        .into_iter()
                        .map(|c| ColumnLite {
                            name: c.name,
                            data_type: c.data_type.to_string(),
                            nullable: c.nullable,
                        })
                        .collect(),
                })
                .collect::<Vec<_>>()
        };

        // Compile to learn the project's models. Tolerate a models-less project
        // (cold start) — there, the source discovery below is the whole point.
        let (models, mut sources, model_targets) = match self.compile_full() {
            Ok(result) => {
                let (model_schemas, source_tables) = commands::build_schema_context(&result);
                let targets: std::collections::HashSet<String> = result
                    .project
                    .models
                    .iter()
                    .map(|m| format!("{}.{}", m.config.target.schema, m.config.target.table))
                    .collect();
                (
                    to_entries(model_schemas),
                    to_entries(source_tables),
                    targets,
                )
            }
            Err(e) if e.to_string().contains("no models found") => {
                (Vec::new(), Vec::new(), std::collections::HashSet::new())
            }
            Err(e) => return Err(format!("{e:#}")),
        };

        // Surface the physical warehouse tables so an agent can ground a raw
        // source the project never declared — and at cold start, before any
        // model exists. Skip a table that is a model's target or is already
        // reported as a compile-derived source. Best-effort across warehouses:
        // the discovery query degrades to an empty list on any error.
        if let Ok(Some(adapter)) = self.warehouse_adapter() {
            let seen: std::collections::HashSet<String> =
                sources.iter().map(|s| s.name.clone()).collect();
            for entry in discover_source_tables(adapter.as_ref()).await {
                if model_targets.contains(&entry.name) || seen.contains(&entry.name) {
                    continue;
                }
                sources.push(entry);
            }
        }

        Ok(Json(InspectSchemaResult { models, sources }))
    }

    #[tool(
        description = "Classify the semantic breaking changes between the working-tree models \
         and the models at a base git ref (default HEAD). Reuses the exact compile + typed-IR \
         classifier that `rocky review` and the branch-promote gate run. Self-check blast radius \
         BEFORE propose. Returns {has_breaking, breaking_count, findings:[{change, severity, \
         model, column?, message}]}. When the gate can't run (non-git project, or either side \
         fails to compile), `skipped_reason` is set and the counts are zero."
    )]
    async fn breaking_change(
        &self,
        params: Parameters<BreakingChangeArgs>,
    ) -> Result<Json<BreakingChangeResult>, String> {
        let base = params.0.base;
        Ok(Json(self.compute_breaking_change(&base)))
    }

    #[tool(
        description = "List the downstream models that depend on a given model (the reverse of \
         `lineage`). For each dependent, returns the focal model's columns it reads via \
         `via_columns`. Use to gauge the blast radius of changing a model before editing it."
    )]
    async fn dependents(
        &self,
        params: Parameters<DependentsArgs>,
    ) -> Result<Json<DependentsResult>, String> {
        let model = params.0.model;
        let result = self.compile_full().map_err(|e| format!("{e:#}"))?;

        // Assert the focal model exists in the semantic graph — same
        // not-found contract as `lineage_output`.
        let schema = result
            .semantic_graph
            .model_schema(&model)
            .ok_or_else(|| format!("model '{model}' not found"))?;

        // Downstream model names come straight from the model schema; the
        // per-dependent `via_columns` are the focal model's columns that feed
        // each dependent, collected from the column-level edge set (the
        // reverse direction of the `lineage` edge filter).
        let mut dependents: Vec<DependentEntry> = schema
            .downstream
            .iter()
            .map(|dep| {
                let mut via_columns: Vec<String> = result
                    .semantic_graph
                    .edges
                    .iter()
                    .filter(|e| *e.source.model == *model && *e.target.model == **dep)
                    .map(|e| e.source.column.to_string())
                    .collect();
                via_columns.sort();
                via_columns.dedup();
                DependentEntry {
                    model: dep.clone(),
                    via_columns,
                }
            })
            .collect();
        dependents.sort_by(|a, b| a.model.cmp(&b.model));

        Ok(Json(DependentsResult { model, dependents }))
    }

    #[tool(
        description = "Return the project-wide asset catalog in one call: every model and source \
         with its typed columns and upstream/downstream model lists. Use to orient on the whole \
         project at once. For the column-level edge trace of a single model use `lineage`; for \
         typed columns alone use `inspect_schema`; for one model's consumers use `dependents`."
    )]
    async fn catalog(
        &self,
        _params: Parameters<CatalogArgs>,
    ) -> Result<Json<CatalogResult>, String> {
        let output = commands::compute_catalog_output(
            &self.config_path,
            &self.state_path(),
            &self.models_dir,
            None,
        )
        .map_err(|e| format!("{e:#}"))?;
        Ok(Json(catalog_result(output)))
    }

    #[tool(
        description = "Read run history from the state store. Without `model`, returns the recent \
         project-level runs (id, status, trigger, duration). With `model`, returns that model's \
         executions (duration, rows, status, sql_hash) newest-first. Grounds proposals in \
         operational reality — is this model flaky, slow, recently changed? Empty when nothing has \
         been run yet."
    )]
    async fn history(
        &self,
        params: Parameters<HistoryArgs>,
    ) -> Result<Json<HistoryResult>, String> {
        let state_path = self.state_path();
        match params.0.model {
            Some(model) => {
                let out = commands::model_history_output(&state_path, &model, None, false, 20)
                    .map_err(|e| format!("{e:#}"))?;
                let executions = out
                    .executions
                    .into_iter()
                    .map(|e| ModelExecutionLite {
                        started_at: e.started_at.to_rfc3339(),
                        duration_ms: e.duration_ms,
                        rows_affected: e.rows_affected,
                        status: e.status,
                        sql_hash: e.sql_hash,
                    })
                    .collect();
                Ok(Json(HistoryResult {
                    model: Some(out.model),
                    runs: vec![],
                    executions,
                }))
            }
            None => {
                let out = commands::history_runs_output(&state_path, None, false)
                    .map_err(|e| format!("{e:#}"))?;
                let runs = out
                    .runs
                    .into_iter()
                    .map(|r| RunHistoryLite {
                        run_id: r.run_id,
                        started_at: r.started_at.to_rfc3339(),
                        status: r.status,
                        trigger: r.trigger,
                        models_executed: r.models_executed,
                        duration_ms: r.duration_ms,
                    })
                    .collect();
                Ok(Json(HistoryResult {
                    model: None,
                    runs,
                    executions: vec![],
                }))
            }
        }
    }

    #[tool(
        description = "Read a model's quality-metric snapshots from the state store: row count, \
         freshness lag, and per-column null rates over recent runs, plus derived freshness / \
         null-rate alerts. Pass `column` to also get that column's per-run trend. `message` is set \
         (and snapshots empty) when the model has no recorded metrics yet."
    )]
    async fn metrics(
        &self,
        params: Parameters<MetricsArgs>,
    ) -> Result<Json<MetricsResult>, String> {
        let args = params.0;
        let out = commands::metrics_output(
            &self.state_path(),
            &args.model,
            true,
            args.column.as_deref(),
            true,
        )
        .map_err(|e| format!("{e:#}"))?;

        let snapshots = out
            .snapshots
            .into_iter()
            .map(|s| MetricsSnapshotLite {
                run_id: s.run_id,
                timestamp: s.timestamp.to_rfc3339(),
                row_count: s.row_count,
                freshness_lag_seconds: s.freshness_lag_seconds,
                null_rates: s
                    .null_rates
                    .into_iter()
                    .map(|(column, null_rate)| ColumnNullRateLite { column, null_rate })
                    .collect(),
            })
            .collect();
        let alerts = out
            .alerts
            .into_iter()
            .map(|a| MetricsAlertLite {
                kind: a.kind,
                severity: a.severity,
                message: a.message,
                column: a.column,
            })
            .collect();
        Ok(Json(MetricsResult {
            model: out.model,
            snapshots,
            alerts,
            message: out.message,
        }))
    }

    #[tool(
        description = "Cost-model materialization recommendations from run history + the on-disk \
         DAG: for each model, the current vs recommended strategy, projected monthly savings, and \
         the reasoning. Use to reason about materialization with Rocky's cost model rather than \
         guessing. `message` is set (and recommendations empty) when there's no run history yet."
    )]
    async fn optimize(
        &self,
        params: Parameters<OptimizeArgs>,
    ) -> Result<Json<OptimizeResult>, String> {
        let out = commands::optimize_output(
            &self.state_path(),
            Some(&self.models_dir),
            params.0.model.as_deref(),
        )
        .map_err(|e| format!("{e:#}"))?;
        let recommendations = out
            .recommendations
            .into_iter()
            .map(|r| OptimizeRecommendationLite {
                model_name: r.model_name,
                current_strategy: r.current_strategy,
                recommended_strategy: r.recommended_strategy,
                estimated_monthly_savings: r.estimated_monthly_savings,
                reasoning: r.reasoning,
                downstream_references: r.downstream_references,
            })
            .collect();
        Ok(Json(OptimizeResult {
            recommendations,
            message: out.message,
        }))
    }

    #[tool(
        description = "Draft a `[freshness]` TOML block for a model with temporal columns (the \
         W005 fix): an LLM picks a sensible `expected_lag_seconds` TTL and a `time_column` from \
         the supplied candidates. Returns the ready-to-paste block directly (NOT a TextEdit); the \
         caller appends it to the model's sidecar. Requires ANTHROPIC_API_KEY in the server \
         environment — without it, `freshness_block` is null and `message` explains why."
    )]
    async fn suggest_freshness_block(
        &self,
        params: Parameters<SuggestFreshnessBlockArgs>,
    ) -> Result<Json<SuggestFreshnessBlockResult>, String> {
        let args = params.0;

        // Gate on the API key the same way the LSP's freshness arm does;
        // degrade to a null block + message rather than erroring.
        let api_key = match std::env::var(rocky_ai::client::AI_API_KEY_ENV) {
            Ok(v) if !v.is_empty() => v,
            _ => {
                return Ok(Json(SuggestFreshnessBlockResult {
                    freshness_block: None,
                    message: Some(format!(
                        "{} not set in the server environment",
                        rocky_ai::client::AI_API_KEY_ENV
                    )),
                }));
            }
        };

        let sidecar_text = args.current_sidecar.unwrap_or_default();
        let (system_prompt, user_prompt) = rocky_ai::prompt::build_freshness_fix_prompt(
            &args.model,
            &args.temporal_columns,
            &sidecar_text,
        );

        // Mirror the LSP's AiConfig: anthropic / sonnet / TOML / single attempt.
        let ai_config = rocky_ai::client::AiConfig {
            provider: "anthropic".to_string(),
            model: "claude-sonnet-4-6".to_string(),
            api_key: rocky_core::redacted::RedactedString::new(api_key),
            default_format: "toml".to_string(),
            max_attempts: 1,
            max_tokens: rocky_ai::client::DEFAULT_MAX_TOKENS,
        };
        let client = rocky_ai::client::LlmClient::new(ai_config)
            .map_err(|e| format!("AI client init failed: {e}"))?;
        let response = client
            .generate(&system_prompt, &user_prompt, None)
            .await
            .map_err(|e| format!("AI request failed: {e}"))?;

        let extracted = rocky_ai::generate::extract_code(&response.content);
        let snippet = extracted.trim();
        if snippet.is_empty() {
            return Ok(Json(SuggestFreshnessBlockResult {
                freshness_block: None,
                message: Some("AI response did not contain a TOML code block".to_string()),
            }));
        }

        Ok(Json(SuggestFreshnessBlockResult {
            freshness_block: Some(snippet.to_string()),
            message: None,
        }))
    }

    // ------------------------- SHOULD tools --------------------------------

    #[tool(
        description = "Sample real rows from a model's target table OR a qualified `schema.table` \
         source reference. Look at literal values, units, and null patterns the schema can't tell \
         you. Omit `percent` to get the first rows (the right default for small tables); set 1–100 \
         for a random-percentage sample. Capped at 50 rows / 16 KB; long cells truncated. Requires \
         live warehouse credentials in the target adapter (rocky.toml)."
    )]
    async fn sample_rows(
        &self,
        params: Parameters<SampleRowsArgs>,
    ) -> Result<Json<SampleRowsResult>, String> {
        let args = params.0;

        let prepared = self
            .prepare_table_query(&args.model)
            .await
            .map_err(|e| format!("{e:#}"))?;

        // Build: SELECT * FROM <ref> [tablesample] LIMIT n. The ref is built
        // only from validated identifiers; never `format!`'d from raw input.
        // With no `percent`, return the first rows deterministically — a low
        // percentage sample returns ~0 rows on a small table, which is the most
        // common grounding case. `percent`, when given, is a clamped integer.
        let sample = args
            .percent
            .and_then(|p| prepared.dialect_tablesample(p.clamp(1, 100)))
            .map(|s| format!(" {s}"))
            .unwrap_or_default();
        let sql = format!(
            "SELECT * FROM {}{} LIMIT {}",
            prepared.table_ref, sample, SAMPLE_MAX_ROWS
        );

        let qr = query_grounding(prepared.adapter.as_ref(), &sql)
            .await
            .map_err(|e| format!("sample query failed: {e}"))?;

        let columns = qr.columns.clone();
        let mut rows: Vec<Vec<String>> = Vec::new();
        let mut truncated = qr.rows.len() > SAMPLE_MAX_ROWS;
        let mut bytes = 0usize;
        for row in qr.rows.into_iter().take(SAMPLE_MAX_ROWS) {
            let cells: Vec<String> = row.into_iter().map(render_cell).collect();
            bytes += cells.iter().map(String::len).sum::<usize>();
            if bytes > SAMPLE_MAX_BYTES {
                truncated = true;
                break;
            }
            rows.push(cells);
        }

        Ok(Json(SampleRowsResult {
            unavailable: false,
            reason: None,
            columns,
            rows,
            truncated,
        }))
    }

    #[tool(
        description = "Profile one column of a model's target table OR a qualified `schema.table` \
         source: row count, nulls, null rate, distinct count, min, max — and, for a \
         low-cardinality column (≤25 distinct), the distinct values with their counts \
         (`top_values`), which surfaces exact literals (e.g. a status string) that min/max hide. \
         Requires live warehouse credentials in the target adapter (rocky.toml)."
    )]
    async fn profile_column(
        &self,
        params: Parameters<ProfileColumnArgs>,
    ) -> Result<Json<ProfileColumnResult>, String> {
        let args = params.0;

        let prepared = self
            .prepare_table_query(&args.model)
            .await
            .map_err(|e| format!("{e:#}"))?;

        let col = rocky_sql::validation::validate_identifier(&args.column)
            .map_err(|e| format!("invalid column identifier: {e}"))?;

        // Cast to the dialect's string type — `VARCHAR` everywhere except
        // BigQuery, where it is `STRING` (BigQuery rejects `CAST(... AS VARCHAR)`).
        let string_type = prepared.adapter.dialect().string_type_name();
        let sql = format!(
            "SELECT COUNT(*) AS n, COUNT({col}) AS non_null, COUNT(DISTINCT {col}) AS distinct_n, \
             CAST(MIN({col}) AS {string_type}) AS min_v, \
             CAST(MAX({col}) AS {string_type}) AS max_v \
             FROM {}",
            prepared.table_ref
        );

        let qr = query_grounding(prepared.adapter.as_ref(), &sql)
            .await
            .map_err(|e| format!("profile query failed: {e}"))?;
        let row = qr
            .rows
            .first()
            .ok_or_else(|| "profile query returned no rows".to_string())?;

        let as_u64 = |v: &serde_json::Value| -> u64 {
            match v {
                serde_json::Value::Number(n) => n.as_u64().unwrap_or(0),
                serde_json::Value::String(s) => s.parse().unwrap_or(0),
                _ => 0,
            }
        };
        let total = row.first().map(as_u64).unwrap_or(0);
        let non_null = row.get(1).map(as_u64).unwrap_or(0);
        let distinct = row.get(2).map(as_u64).unwrap_or(0);
        let nulls = total.saturating_sub(non_null);
        let null_rate = if total == 0 {
            0.0
        } else {
            nulls as f64 / total as f64
        };
        let str_cell = |v: Option<&serde_json::Value>| -> Option<String> {
            match v {
                Some(serde_json::Value::Null) | None => None,
                Some(serde_json::Value::String(s)) => Some(s.clone()),
                Some(other) => Some(other.to_string()),
            }
        };

        // For a low-cardinality column, surface the distinct values + their
        // counts — what `min`/`max` alone can't reveal (e.g. that `status`
        // holds 'COMPLETE', not 'completed'). One extra grouped query, run only
        // when the cardinality makes it cheap.
        let top_values = if distinct > 0 && distinct <= PROFILE_TOP_VALUES_MAX as u64 {
            let q = format!(
                "SELECT CAST({col} AS {string_type}) AS v, COUNT(*) AS c FROM {} \
                 GROUP BY {col} ORDER BY c DESC, v LIMIT {}",
                prepared.table_ref, PROFILE_TOP_VALUES_MAX
            );
            match query_grounding(prepared.adapter.as_ref(), &q).await {
                Ok(r) => r
                    .rows
                    .into_iter()
                    .map(|row| ValueCount {
                        value: str_cell(row.first()),
                        count: row.get(1).map(as_u64).unwrap_or(0),
                    })
                    .collect(),
                Err(_) => Vec::new(),
            }
        } else {
            Vec::new()
        };

        Ok(Json(ProfileColumnResult {
            unavailable: false,
            reason: None,
            rows: total,
            nulls,
            null_rate,
            distinct,
            min: str_cell(row.get(3)),
            max: str_cell(row.get(4)),
            top_values,
        }))
    }

    #[tool(
        description = "Propose materializing the model(s) as an AI-AUTHORED plan. This does NOT \
         execute anything. It records a plan that a human must review and approve \
         (`rocky review <plan_id> --approve`) before `rocky apply <plan_id>` will run it. Surface \
         the plan_id and the review/apply path to the user; never approve on their behalf."
    )]
    async fn propose(
        &self,
        params: Parameters<ProposeArgs>,
    ) -> Result<Json<ProposeResult>, String> {
        let result = self.compile_full().map_err(|e| format!("{e:#}"))?;
        if result.project.models.is_empty() {
            return Err("project has no compiled models to propose".to_string());
        }
        let models: Vec<String> = result
            .project
            .models
            .iter()
            .map(|m| m.config.name.clone())
            .collect();

        // If a model filter was given, assert it exists so we don't write a
        // plan that applies to nothing.
        if let Some(model) = params.0.model.as_deref()
            && !models.iter().any(|m| m == model)
        {
            return Err(format!("model '{model}' not found in project"));
        }

        let run_plan = build_ai_run_plan(params.0.model.clone(), &result);
        let plan_id = rocky_cli::plan_store::write_plan(
            &self.root,
            rocky_cli::plan_store::PlanKind::AiAuthored,
            &run_plan,
        )
        .map_err(|e| format!("failed to write AI-authored plan: {e:#}"))?;

        Ok(Json(ProposeResult { plan_id, models }))
    }

    /// Resolve the project's target warehouse adapter from `rocky.toml`.
    ///
    /// Returns the configured target adapter for the resolved pipeline — any
    /// warehouse (DuckDB, Snowflake, BigQuery, Databricks, Trino). The data
    /// grounding tools (`sample_rows`, `profile_column`, and `inspect_schema`'s
    /// source discovery) reach the live warehouse through it. Kept as
    /// `Result<Option<...>>` so `inspect_schema`'s `if let Ok(Some(_))`
    /// graceful-degradation path survives a resolution failure.
    fn warehouse_adapter(
        &self,
    ) -> anyhow::Result<Option<std::sync::Arc<dyn rocky_core::traits::WarehouseAdapter>>> {
        let cfg = rocky_core::config::load_rocky_config(&self.config_path)?;
        let registry = commands_adapter_registry(&cfg)?;
        let (_, pipeline) = rocky_cli::registry::resolve_pipeline(&cfg, None)?;
        let target_adapter = pipeline.target_adapter().to_string();
        Ok(Some(registry.warehouse_adapter(&target_adapter)?))
    }

    /// Resolve a grounding-tool target into a runnable, validated table ref plus
    /// the warehouse adapter.
    ///
    /// The target is either a **compiled model name** (resolved to its target
    /// table, which requires the models to compile) or a **qualified
    /// `schema.table` / `catalog.schema.table` source reference** (any dotted
    /// name — resolved directly with no compile, so it reaches raw sources the
    /// project never declared and works at cold start, before any model exists).
    async fn prepare_table_query(&self, target: &str) -> anyhow::Result<Prepared> {
        let adapter = self
            .warehouse_adapter()?
            .ok_or_else(|| anyhow::anyhow!("could not resolve the target warehouse adapter"))?;

        let table_ref = if target.contains('.') {
            // Qualified raw reference — validate each segment and use it as-is.
            // No compile required: this is how an agent grounds a source before
            // (or without) authoring any model.
            let parts: Vec<&str> = target.split('.').collect();
            if !(2..=3).contains(&parts.len()) {
                return Err(anyhow::anyhow!(
                    "table reference '{target}' must be `schema.table` or \
                     `catalog.schema.table`"
                ));
            }
            for part in &parts {
                rocky_sql::validation::validate_identifier(part)
                    .map_err(|e| anyhow::anyhow!("invalid identifier '{part}': {e}"))?;
            }
            parts.join(".")
        } else {
            // Bare name — resolve the model's target coordinates by compiling
            // the models dir. Emit `catalog.schema.table` when the target
            // carries a catalog (Snowflake/BigQuery/Databricks); DuckDB has no
            // catalog level so it stays a two-part `schema.table` name.
            let result = self.compile_full()?;
            let model = result
                .project
                .models
                .iter()
                .find(|m| m.config.name == target)
                .ok_or_else(|| anyhow::anyhow!("model '{target}' not found in project"))?;
            let t = &model.config.target;
            qualify_table_ref(&t.catalog, &t.schema, &t.table)?
        };

        Ok(Prepared { adapter, table_ref })
    }
}

// `prompt_router`'s `router` arg takes a string ident (unlike `tool_router`);
// the default generated fn is already named `prompt_router`, so no arg needed.
#[prompt_router]
impl RockyMcpServer {
    /// The actionable, intent-parameterized form of the server `instructions`
    /// (the `rocky-ai-workflow` skill). Walks a connected agent through
    /// Rocky's authoring loop for one concrete model, ending at *propose* —
    /// the human runs `rocky review --approve` + `rocky apply`.
    #[prompt(
        name = "build_model",
        description = "Guide the authoring of one Rocky model from a plain-language intent: \
         inspect schema -> sample rows -> profile columns -> write SQL -> compile-loop -> \
         plan preview -> propose. Stops at the human approval gate."
    )]
    async fn build_model(
        &self,
        Parameters(args): Parameters<BuildModelArgs>,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<GetPromptResult, McpError> {
        let intent = args.intent.trim();
        let messages = vec![
            PromptMessage::new_text(
                PromptMessageRole::Assistant,
                "I'll author this Rocky model SQL-first, grounding every decision in the \
                 real data, and stop at a proposed plan for you to review and apply. \
                 The substrate trusts my edits because the compiler checked them and you \
                 sign off the invariants — never because they merely compiled.",
            ),
            PromptMessage::new_text(
                PromptMessageRole::User,
                format!(
                    "Build a Rocky model for this intent:\n\n  {intent}\n\n\
                     Follow Rocky's authoring loop, using the MCP tools at each step:\n\n\
                     1. inspect_schema — read every existing model and source table with its \
                     typed columns. Never guess column names; select only what's actually there.\n\
                     2. sample_rows — look at real rows before writing any filter or cast. The \
                     schema tells you a column exists; it does not tell you its literal values, \
                     its units, or its null rate.\n\
                     3. profile_column — for any column you filter, cast, or aggregate on, \
                     check distinct values, null rate, and domain.\n\
                     4. Write the model as raw SQL (models/<name>.sql + a <name>.toml sidecar \
                     for strategy + target). SQL is first-class in Rocky — do NOT reach for the \
                     .rocky DSL unless the user explicitly asks. Keep it minimal and readable.\n\
                     5. compile — type-check and read the diagnostics. Each carries a code, a \
                     span, and often a suggestion. Fix against the diagnostic and recompile; \
                     loop until clean. The compiler is your fast feedback loop — lean on it \
                     instead of reasoning about correctness in your head.\n\
                     6. plan_preview — read the exact SQL Rocky would execute and confirm it \
                     matches the intent before proposing.\n\
                     7. Encode what you learned while sampling as a contract (required/protected \
                     columns) or a check (assertion), not just a WHERE clause — that moves the \
                     invariant into the typed substrate so the compiler enforces it on every \
                     future run.\n\
                     8. propose — generate the materialization plan. It is recorded as an \
                     AI-authored plan with a plan_id.\n\n\
                     RECONCILE DISCIPLINE (the step that separates a model that compiles from a \
                     model that is correct): check literal values and units against the sampled \
                     data, not just the schema. A `WHERE status = 'completed'` that returns zero \
                     rows because the data actually holds 'COMPLETE' compiles perfectly and is \
                     wrong. Confirm dollars-vs-cents and UTC-vs-local from real rows.\n\n\
                     STOP at propose. Never apply an AI-authored change directly — a bare apply \
                     is refused by design. Surface the plan_id and the review report clearly, \
                     then the human runs `rocky review <plan-id> --approve` to sign off the \
                     invariants and `rocky apply <plan-id>` to execute. Do not approve on the \
                     user's behalf unless they explicitly tell you to."
                ),
            ),
        ];

        Ok(GetPromptResult::new(messages)
            .with_description(format!("Rocky model-authoring loop for: {intent}")))
    }
}

#[tool_handler(router = self.tool_router)]
#[prompt_handler(router = self.prompt_router)]
impl ServerHandler for RockyMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_prompts()
                .build(),
        )
        .with_server_info(Implementation::from_build_env())
        .with_protocol_version(ProtocolVersion::V_2024_11_05)
        .with_instructions(INSTRUCTIONS.to_string())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// A validated, runnable table reference plus the warehouse adapter to run it on.
struct Prepared {
    adapter: std::sync::Arc<dyn rocky_core::traits::WarehouseAdapter>,
    table_ref: String,
}

impl Prepared {
    fn dialect_tablesample(&self, percent: u32) -> Option<String> {
        self.adapter.dialect().tablesample_clause(percent)
    }
}

/// Build a validated, dialect-agnostic table reference from a model's target
/// coordinates. Emits `catalog.schema.table` when a catalog is set
/// (Snowflake/BigQuery/Databricks), or `schema.table` otherwise (DuckDB has no
/// catalog level). Each segment is validated as a SQL identifier.
fn qualify_table_ref(catalog: &str, schema: &str, table: &str) -> anyhow::Result<String> {
    let schema = rocky_sql::validation::validate_identifier(schema)
        .map_err(|e| anyhow::anyhow!("invalid schema identifier: {e}"))?;
    let table = rocky_sql::validation::validate_identifier(table)
        .map_err(|e| anyhow::anyhow!("invalid table identifier: {e}"))?;
    if catalog.is_empty() {
        Ok(format!("{schema}.{table}"))
    } else {
        let catalog = rocky_sql::validation::validate_identifier(catalog)
            .map_err(|e| anyhow::anyhow!("invalid catalog identifier: {e}"))?;
        Ok(format!("{catalog}.{schema}.{table}"))
    }
}

/// Run a grounding query, preferring the columnar Arrow path and falling back
/// to the row-based JSON path.
///
/// `fetch_arrow_batch` is implemented on DuckDB / BigQuery / Databricks / Trino;
/// Snowflake inherits the trait default that errors before running any SQL, so
/// it always falls back to [`WarehouseAdapter::execute_query`]. A genuine SQL
/// error on an Arrow-capable adapter re-surfaces with its real message via the
/// `execute_query` arm — nothing is swallowed, just one extra round-trip on a
/// real failure. The inner conversion `Err` (an unformattable Arrow type) also
/// falls back rather than hard-erroring.
async fn query_grounding(
    adapter: &dyn rocky_core::traits::WarehouseAdapter,
    sql: &str,
) -> rocky_core::traits::AdapterResult<rocky_core::traits::QueryResult> {
    if let Ok(batch) = adapter.fetch_arrow_batch(sql).await
        && let Ok(qr) = record_batch_to_query_result(&batch)
    {
        return Ok(qr);
    }
    adapter.execute_query(sql).await
}

/// Convert an Arrow [`RecordBatch`](arrow::record_batch::RecordBatch) into the
/// row-based [`QueryResult`](rocky_core::traits::QueryResult) the grounding
/// tools consume.
///
/// Each cell renders to text via `arrow`'s `ArrayFormatter`, EXCEPT SQL NULL:
/// the default `FormatOptions` renders NULL as the empty string, which would be
/// indistinguishable from an empty value, so NULL is emitted as
/// `serde_json::Value::Null` explicitly (checked via `Array::is_null`). All
/// other cells become `Value::String`, matching the JSON path's effective shape
/// for the grounding tools (which render every cell to a display string and
/// parse aggregates back out of strings).
fn record_batch_to_query_result(
    batch: &arrow::record_batch::RecordBatch,
) -> Result<rocky_core::traits::QueryResult, arrow::error::ArrowError> {
    use arrow::util::display::{ArrayFormatter, FormatOptions};

    let schema = batch.schema();
    let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

    let options = FormatOptions::default();
    // One formatter per column, built once, then indexed per row.
    let formatters: Vec<ArrayFormatter> = batch
        .columns()
        .iter()
        .map(|col| ArrayFormatter::try_new(col.as_ref(), &options))
        .collect::<Result<_, _>>()?;

    let mut rows: Vec<Vec<serde_json::Value>> = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let mut cells: Vec<serde_json::Value> = Vec::with_capacity(batch.num_columns());
        for (col_idx, fmt) in formatters.iter().enumerate() {
            let cell = if batch.column(col_idx).is_null(row) {
                serde_json::Value::Null
            } else {
                serde_json::Value::String(fmt.value(row).to_string())
            };
            cells.push(cell);
        }
        rows.push(cells);
    }

    Ok(rocky_core::traits::QueryResult { columns, rows })
}

/// Build the `AdapterRegistry` from the loaded config. Thin wrapper so the
/// call site reads clearly; the registry constructor lives in rocky-cli.
fn commands_adapter_registry(
    cfg: &rocky_core::config::RockyConfig,
) -> anyhow::Result<rocky_cli::registry::AdapterRegistry> {
    rocky_cli::registry::AdapterRegistry::from_config(cfg)
}

/// Project a `CompileOutput` into the trimmed [`CompileResult`].
fn project_compile_result(output: &rocky_cli::output::CompileOutput) -> CompileResult {
    use rocky_compiler::diagnostic::Severity;
    let error_count = output
        .diagnostics
        .iter()
        .filter(|d| d.severity == Severity::Error)
        .count();
    let warning_count = output
        .diagnostics
        .iter()
        .filter(|d| d.severity == Severity::Warning)
        .count();
    let diagnostics = output
        .diagnostics
        .iter()
        .map(|d| DiagnosticLite {
            code: d.code.to_string(),
            severity: format!("{:?}", d.severity),
            model: d.model.clone(),
            message: d.message.to_string(),
            suggestion: d.suggestion.clone(),
            span: d
                .span
                .as_ref()
                .map(|s| format!("{}:{}:{}", s.file, s.line, s.col)),
        })
        .collect();
    CompileResult {
        has_errors: output.has_errors,
        error_count,
        warning_count,
        model_count: output.models,
        diagnostics,
    }
}

/// Project a `CatalogOutput` into the lite [`CatalogResult`], dropping the
/// (token-heavy) column-level edge set in favour of the per-asset
/// upstream/downstream model lists plus the aggregate counts. Agents that
/// need the edge trace use the `lineage` tool.
fn catalog_result(output: rocky_cli::output::CatalogOutput) -> CatalogResult {
    use rocky_cli::output::AssetKind;
    let assets = output
        .assets
        .into_iter()
        .map(|a| {
            let kind = match a.kind {
                AssetKind::Source => "source",
                AssetKind::Model => "model",
                AssetKind::View => "view",
                AssetKind::MaterializedView => "materialized_view",
            }
            .to_string();
            let columns = a
                .columns
                .into_iter()
                .map(|c| CatalogColumnLite {
                    name: c.name,
                    data_type: c.data_type,
                    nullable: c.nullable,
                })
                .collect();
            CatalogAssetLite {
                fqn: a.fqn,
                model_name: a.model_name,
                kind,
                columns,
                upstream_models: a.upstream_models,
                downstream_models: a.downstream_models,
                intent: a.intent,
            }
        })
        .collect();
    CatalogResult {
        project_name: output.project_name,
        assets,
        asset_count: output.stats.asset_count,
        column_count: output.stats.column_count,
        edge_count: output.stats.edge_count,
    }
}

/// Project a borrowed `LineageEdgeRecord` into the lite edge shape.
fn edge_lite(e: &rocky_cli::output::LineageEdgeRecord) -> LineageEdgeLite {
    LineageEdgeLite {
        source_model: e.source.model.clone(),
        source_column: e.source.column.clone(),
        target_model: e.target.model.clone(),
        target_column: e.target.column.clone(),
        transform: e.transform.clone(),
    }
}

/// Project a `BreakingFinding` into the lite, schemars-1.x shape.
///
/// `change` is the snake_case `kind` discriminant of the tagged
/// [`rocky_core::breaking_change::BreakingChange`] enum (e.g.
/// `"column_dropped"`); `model` and the optional `column` are pulled from the
/// variant; `message` is the debug rendering of the change, matching the
/// human-readable line `rocky review` emits.
fn breaking_finding_lite(f: &rocky_core::breaking_change::BreakingFinding) -> BreakingFindingLite {
    use rocky_core::breaking_change::BreakingSeverity;
    let severity = match f.severity {
        BreakingSeverity::Breaking => "breaking",
        BreakingSeverity::Warning => "warning",
        BreakingSeverity::Info => "info",
    }
    .to_string();

    // The enum is `#[serde(tag = "kind", rename_all = "snake_case")]`, so the
    // serialized object carries the discriminant under `kind` and the variant
    // fields (incl. `model` and, where present, `column`) at the top level.
    let value = serde_json::to_value(&f.change).unwrap_or_default();
    let change = value
        .get("kind")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();
    let model = value
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let column = value
        .get("column")
        .and_then(|v| v.as_str())
        .map(str::to_string);

    BreakingFindingLite {
        change,
        severity,
        model,
        column,
        message: format!("{:?}", f.change),
    }
}

/// Discover the physical tables in the DuckDB warehouse as schema-qualified
/// source entries (best-effort — returns empty on any query error). Excludes
/// the system schemas. Lets `inspect_schema` show an agent the raw sources the
/// project never declared, including at cold start.
async fn discover_source_tables(
    adapter: &dyn rocky_core::traits::WarehouseAdapter,
) -> Vec<SchemaEntry> {
    let sql = "SELECT table_schema, table_name, column_name, data_type, is_nullable \
               FROM information_schema.columns \
               WHERE table_schema NOT IN ('information_schema', 'pg_catalog') \
               ORDER BY table_schema, table_name, ordinal_position";
    let Ok(qr) = adapter.execute_query(sql).await else {
        return Vec::new();
    };
    let cell = |v: Option<&serde_json::Value>| -> String {
        match v {
            Some(serde_json::Value::String(s)) => s.clone(),
            Some(serde_json::Value::Null) | None => String::new(),
            Some(other) => other.to_string(),
        }
    };
    // Group columns under their `schema.table`, preserving first-seen order.
    let mut order: Vec<String> = Vec::new();
    let mut columns: std::collections::HashMap<String, Vec<ColumnLite>> =
        std::collections::HashMap::new();
    for row in qr.rows {
        let schema = cell(row.first());
        let table = cell(row.get(1));
        if schema.is_empty() || table.is_empty() {
            continue;
        }
        let key = format!("{schema}.{table}");
        if !columns.contains_key(&key) {
            order.push(key.clone());
        }
        columns.entry(key).or_default().push(ColumnLite {
            name: cell(row.get(2)),
            data_type: cell(row.get(3)),
            nullable: !cell(row.get(4)).eq_ignore_ascii_case("NO"),
        });
    }
    order
        .into_iter()
        .map(|name| {
            let cols = columns.remove(&name).unwrap_or_default();
            SchemaEntry {
                name,
                columns: cols,
            }
        })
        .collect()
}

/// Render one query cell as a display string, truncating long values.
fn render_cell(v: serde_json::Value) -> String {
    let s = match v {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::String(s) => s,
        other => other.to_string(),
    };
    if s.chars().count() > CELL_MAX_CHARS {
        let mut out: String = s.chars().take(CELL_MAX_CHARS).collect();
        out.push('…');
        out
    } else {
        s
    }
}

/// Build an AI-authored `RunPlan` for the given model filter. Constructed
/// inline (the `rocky plan` builder is private + entangled with discovery);
/// every field is set explicitly so a future field addition is a compile error
/// rather than a silent default.
fn build_ai_run_plan(model: Option<String>, result: &CompilerResult) -> rocky_cli::output::RunPlan {
    let models: Vec<String> = result
        .project
        .models
        .iter()
        .map(|m| m.config.name.clone())
        .collect();
    let execution_layers: Vec<Vec<String>> = result.project.layers.clone();
    rocky_cli::output::RunPlan {
        filter: None,
        pipeline: None,
        model,
        branch: None,
        partition: None,
        partition_from: None,
        partition_to: None,
        latest: false,
        missing: false,
        lookback: None,
        parallel: 1,
        run_all: false,
        env: None,
        models_dir: None,
        resume: None,
        resume_latest: false,
        shadow: false,
        shadow_suffix: None,
        shadow_schema: None,
        dag: false,
        idempotency_key: None,
        governance_override: None,
        models,
        execution_layers,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_cell_passes_short_strings_through() {
        assert_eq!(
            render_cell(serde_json::Value::String("hello".into())),
            "hello"
        );
        assert_eq!(render_cell(serde_json::Value::Null), "NULL");
        assert_eq!(render_cell(serde_json::json!(42)), "42");
    }

    #[test]
    fn render_cell_truncates_long_strings_with_ellipsis() {
        let long = "a".repeat(CELL_MAX_CHARS + 100);
        let out = render_cell(serde_json::Value::String(long));
        // Truncated to the cap plus a single ellipsis char.
        assert_eq!(out.chars().count(), CELL_MAX_CHARS + 1);
        assert!(out.ends_with('…'));
    }

    #[test]
    fn caps_are_within_spec() {
        // Hard caps from the tool spec.
        assert_eq!(SAMPLE_MAX_ROWS, 50);
        assert_eq!(SAMPLE_MAX_BYTES, 16 * 1024);
        assert_eq!(CELL_MAX_CHARS, 256);
    }

    #[test]
    fn server_resolves_models_dir_beside_config() {
        let server = RockyMcpServer::new(PathBuf::from("/tmp/proj/rocky.toml"));
        assert_eq!(server.models_dir, PathBuf::from("/tmp/proj/models"));
        assert_eq!(server.root, PathBuf::from("/tmp/proj"));
    }

    #[test]
    fn breaking_finding_lite_projects_column_scoped_change() {
        use rocky_core::breaking_change::{BreakingChange, BreakingFinding, BreakingSeverity};
        let finding = BreakingFinding {
            change: BreakingChange::ColumnDropped {
                model: "c.s.orders".to_string(),
                column: "legacy_flag".to_string(),
                data_type: "String".to_string(),
            },
            severity: BreakingSeverity::Breaking,
        };
        let lite = breaking_finding_lite(&finding);
        assert_eq!(lite.change, "column_dropped");
        assert_eq!(lite.severity, "breaking");
        assert_eq!(lite.model, "c.s.orders");
        assert_eq!(lite.column.as_deref(), Some("legacy_flag"));
        assert!(lite.message.contains("ColumnDropped"));
    }

    #[test]
    fn qualify_table_ref_emits_catalog_when_present() {
        // Catalog set (Snowflake/BigQuery/Databricks) → three-part name.
        assert_eq!(
            qualify_table_ref("analytics", "raw", "orders").unwrap(),
            "analytics.raw.orders"
        );
        // No catalog (DuckDB) → two-part name.
        assert_eq!(
            qualify_table_ref("", "raw", "orders").unwrap(),
            "raw.orders"
        );
    }

    #[test]
    fn qualify_table_ref_rejects_bad_identifier() {
        assert!(qualify_table_ref("", "raw", "orders; DROP TABLE x").is_err());
        assert!(qualify_table_ref("a.b", "raw", "orders").is_err());
    }

    #[test]
    fn record_batch_to_query_result_renders_null_as_json_null() {
        use std::sync::Arc;

        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;

        // A 2-row batch where row 1 is NULL in both columns. The default
        // `FormatOptions` renders NULL as "", so the converter MUST emit
        // `Value::Null` for those cells (checked via `is_null`), not "".
        let schema = Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
        ]));
        let ints = Int64Array::from(vec![Some(42), None]);
        let strs = StringArray::from(vec![Some("hello"), None]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(ints), Arc::new(strs)]).unwrap();

        let qr = record_batch_to_query_result(&batch).unwrap();
        assert_eq!(qr.columns, vec!["n".to_string(), "s".to_string()]);
        assert_eq!(qr.rows.len(), 2);
        // Row 0: non-null cells render to strings.
        assert_eq!(qr.rows[0][0], serde_json::Value::String("42".to_string()));
        assert_eq!(
            qr.rows[0][1],
            serde_json::Value::String("hello".to_string())
        );
        // Row 1: SQL NULL → JSON null, NOT the empty string.
        assert_eq!(qr.rows[1][0], serde_json::Value::Null);
        assert_eq!(qr.rows[1][1], serde_json::Value::Null);
    }

    #[test]
    fn breaking_finding_lite_omits_column_for_model_scoped_change() {
        use rocky_core::breaking_change::{BreakingChange, BreakingFinding, BreakingSeverity};
        let finding = BreakingFinding {
            change: BreakingChange::ModelRemoved {
                model: "c.s.orders".to_string(),
            },
            severity: BreakingSeverity::Breaking,
        };
        let lite = breaking_finding_lite(&finding);
        assert_eq!(lite.change, "model_removed");
        assert_eq!(lite.model, "c.s.orders");
        assert_eq!(lite.column, None);
    }
}
