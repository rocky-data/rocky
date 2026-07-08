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

use crate::error::{ToolError, ToolResult};
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
    /// Optional portability target dialect — one of `"databricks"`,
    /// `"snowflake"`, `"bigquery"`, or `"duckdb"`. When set, the P001
    /// dialect-divergence lint runs against it on demand: each model's SQL is
    /// checked for constructs that won't port to the named dialect, surfaced as
    /// P001 diagnostics. When absent, behaviour is unchanged — the lint runs
    /// only if `rocky.toml` declares `[portability] target_dialect`.
    #[serde(default)]
    pub target_dialect: Option<String>,
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
pub struct DraftModelArgs {
    /// The model name. Becomes `models/<name>.sql` + a `models/<name>.toml`
    /// sidecar. Must be a bare identifier — no path separators, no `..`, no
    /// extension, not absolute. A name that would escape the models directory
    /// is refused with an `invalid_argument` error.
    pub name: String,
    /// The model's SQL body, written verbatim to `models/<name>.sql`. Raw SQL is
    /// first-class in Rocky — write real SQL grounded in the sampled data.
    pub sql: String,
    /// A plain-language statement of what the model is for, persisted to the
    /// sidecar's `intent` field (surfaced by `catalog` and lineage). Ground it
    /// in the intent you were given; it is the reviewer's context for the draft.
    pub intent: String,
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

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct DraftContractArgs {
    /// The model to draft a `.contract.toml` for. Its target table must be
    /// materialized in the warehouse (run the model first) — the contract is
    /// grounded in the table's observed per-column profile.
    pub model: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct GenerateTestsArgs {
    /// The model to draft test assertions for, from its intent + schema + SQL.
    pub model: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ExplainModelArgs {
    /// The model to draft an intent description for, from its SQL, output
    /// schema, and upstream dependencies.
    pub model: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct GovernancePreviewArgs {
    /// Optional environment name (mirrors `rocky plan --env <name>`). When
    /// set, masking policies resolve `[mask.<env>]` overrides on top of the
    /// workspace `[mask]` defaults. Classification + retention previews are
    /// env-invariant.
    #[serde(default)]
    pub env: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct DriftPreviewArgs {
    /// The source table to compare — a qualified `schema.table` (or
    /// `catalog.schema.table`) reference. Both tables are `DESCRIBE`d and
    /// their warehouse-reported types compared.
    pub source_table: String,
    /// The target table to compare against — a qualified `schema.table` (or
    /// `catalog.schema.table`) reference.
    pub target_table: String,
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

/// No-argument prompt args for the project-wide trajectories
/// (`find_untested_models`, `summarize_project`). MCP prompts must declare a
/// `Parameters<T>` type even when they take no input.
#[derive(Debug, Default, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct NoArgs {}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct ScopedModelArgs {
    /// Optional single-model scope. When set, the trajectory focuses on this
    /// model; when omitted, it sweeps the whole project.
    #[serde(default)]
    pub model: Option<String>,
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
         code/span/suggestion and fix against them — this is the fast feedback loop. Pass \
         `target_dialect` (databricks/snowflake/bigquery/duckdb) to additionally run the P001 \
         portability lint on demand: SQL that won't port to that dialect surfaces as P001."
    )]
    async fn compile(&self, params: Parameters<CompileArgs>) -> ToolResult<CompileResult> {
        let args = params.0;
        let model = args.model.as_deref();
        // On-demand portability lint: parse the requested dialect (case-
        // insensitive, matching the `Dialect` serde vocabulary). When absent,
        // pass `None` so the lint stays driven solely by `[portability]` in
        // rocky.toml — i.e. behaviour is unchanged.
        let target_dialect = match args.target_dialect.as_deref() {
            Some(d) => Some(parse_target_dialect(d)?),
            None => None,
        };
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
            target_dialect,
            with_seed,
            None,
        )
        .map_err(|e| ToolError::compile_failed(format!("{e:#}")))?;
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
    ) -> ToolResult<PlanPreviewResult> {
        let model = params.0.model.as_deref();
        let output =
            commands::plan_preview_output(Some(&self.config_path), &self.models_dir, model, None)
                .map_err(|e| ToolError::compile_failed(format!("{e:#}")))?;
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
    async fn lineage(&self, params: Parameters<LineageArgs>) -> ToolResult<LineageResult> {
        let args = params.0;
        let result = self
            .compile_full()
            .map_err(|e| ToolError::compile_failed(format!("{e:#}")))?;

        if let Some(column) = args.column.as_deref() {
            let out =
                commands::column_lineage_output(&result, &args.model, column, args.downstream)
                    .map_err(|_| ToolError::model_not_found(&args.model))?;
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
            let out = commands::lineage_output(&result, &args.model)
                .map_err(|_| ToolError::model_not_found(&args.model))?;
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
    async fn test(&self) -> ToolResult<TestResult> {
        let output = commands::test_output(&self.models_dir, None, None).map_err(|e| {
            ToolError::internal(
                format!("{e:#}"),
                "The local test runner could not execute; confirm the project compiles (the \
                 `compile` tool) and any `data/seed.sql` the tests need is present.",
            )
        })?;
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
    async fn list(&self, params: Parameters<ListArgs>) -> ToolResult<ListResult> {
        let kind = params.0.kind;
        let entries = match kind.as_str() {
            "models" => {
                let out = commands::list_models_output(&self.models_dir)
                    .map_err(|e| ToolError::compile_failed(format!("{e:#}")))?;
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
                    .map_err(|e| ToolError::config_invalid(format!("{e:#}")))?;
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
                    .map_err(|e| ToolError::config_invalid(format!("{e:#}")))?;
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
                    .map_err(|e| ToolError::config_invalid(format!("{e:#}")))?;
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
                return Err(ToolError::invalid_argument(
                    format!("unknown kind '{other}'"),
                    "Pass one of: models, pipelines, adapters, sources.",
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
    ) -> ToolResult<InspectSchemaResult> {
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
            Err(e) => return Err(ToolError::compile_failed(format!("{e:#}"))),
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
    ) -> ToolResult<BreakingChangeResult> {
        let base = params.0.base;
        Ok(Json(self.compute_breaking_change(&base)))
    }

    #[tool(
        description = "List the downstream models that depend on a given model (the reverse of \
         `lineage`). For each dependent, returns the focal model's columns it reads via \
         `via_columns`. Use to gauge the blast radius of changing a model before editing it."
    )]
    async fn dependents(&self, params: Parameters<DependentsArgs>) -> ToolResult<DependentsResult> {
        let model = params.0.model;
        let result = self
            .compile_full()
            .map_err(|e| ToolError::compile_failed(format!("{e:#}")))?;

        // Assert the focal model exists in the semantic graph — same
        // not-found contract as `lineage_output`.
        let schema = result
            .semantic_graph
            .model_schema(&model)
            .ok_or_else(|| ToolError::model_not_found(&model))?;

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
    async fn catalog(&self, _params: Parameters<CatalogArgs>) -> ToolResult<CatalogResult> {
        let output = commands::compute_catalog_output(
            &self.config_path,
            &self.state_path(),
            &self.models_dir,
            None,
        )
        .map_err(|e| ToolError::compile_failed(format!("{e:#}")))?;
        Ok(Json(catalog_result(output)))
    }

    #[tool(
        description = "Read run history from the state store. Without `model`, returns the recent \
         project-level runs (id, status, trigger, duration). With `model`, returns that model's \
         executions (duration, rows, status, sql_hash) newest-first. Grounds proposals in \
         operational reality — is this model flaky, slow, recently changed? Empty when nothing has \
         been run yet."
    )]
    async fn history(&self, params: Parameters<HistoryArgs>) -> ToolResult<HistoryResult> {
        let state_path = self.state_path();
        match params.0.model {
            Some(model) => {
                let out = commands::model_history_output(&state_path, &model, None, false, 20)
                    .map_err(|e| {
                        ToolError::internal(
                            format!("{e:#}"),
                            "Could not read the run history from the state store; ensure the \
                             project has been run at least once (history is empty, not an error, \
                             before the first run).",
                        )
                    })?;
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
                let out = commands::history_runs_output(&state_path, None, false).map_err(|e| {
                    ToolError::internal(
                        format!("{e:#}"),
                        "Could not read the run history from the state store; ensure the \
                             project has been run at least once (history is empty, not an error, \
                             before the first run).",
                    )
                })?;
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
    async fn metrics(&self, params: Parameters<MetricsArgs>) -> ToolResult<MetricsResult> {
        let args = params.0;
        let out = commands::metrics_output(
            &self.state_path(),
            &args.model,
            true,
            args.column.as_deref(),
            true,
        )
        .map_err(|e| {
            ToolError::internal(
                format!("{e:#}"),
                "Could not read quality metrics from the state store; ensure the project has been \
                 run at least once (a model with no recorded metrics returns an empty result with \
                 a `message`, not an error).",
            )
        })?;

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
    async fn optimize(&self, params: Parameters<OptimizeArgs>) -> ToolResult<OptimizeResult> {
        let out = commands::optimize_output(
            &self.state_path(),
            Some(&self.models_dir),
            params.0.model.as_deref(),
        )
        .map_err(|e| {
            ToolError::internal(
                format!("{e:#}"),
                "Could not compute optimization recommendations; ensure the project compiles and \
                 has run history (no history returns an empty result with a `message`, not an \
                 error).",
            )
        })?;
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
    ) -> ToolResult<SuggestFreshnessBlockResult> {
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
            .map_err(|e| ToolError::ai_error(format!("AI client init failed: {e}")))?;
        let response = client
            .generate(&system_prompt, &user_prompt, None)
            .await
            .map_err(|e| ToolError::ai_error(format!("AI request failed: {e}")))?;

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

    // ------------------------- generator tools -----------------------------
    // These wrap the existing `rocky-ai` generators (the CLI's `rocky ai-*`
    // commands). Each is an LLM/BYOK tool gated on ANTHROPIC_API_KEY, exactly
    // like `suggest_freshness_block`. They return DRAFTS — the agent then runs
    // `compile` / `propose` to act on them; nothing here mutates the warehouse
    // or applies anything.

    #[tool(
        description = "Draft a `.contract.toml` for a model from the aggregate per-column \
         profile of its target table (the `rocky ai-contract` generator). An LLM proposes \
         required/protected columns and per-column types; the draft is compile-verified against \
         the model's inferred schema before it is returned. Returns the contract TOML as a DRAFT \
         — save it next to the model and run `compile` to enforce it; it mutates nothing. The \
         model's target table must be materialized. Egress: only aggregate STATISTICS \
         (row/null/distinct counts) are sent to the LLM — no raw cell values. Requires \
         ANTHROPIC_API_KEY in the server environment — without it (or when the target isn't \
         reachable), `contract_toml` is null and `message` explains why."
    )]
    async fn draft_contract(
        &self,
        params: Parameters<DraftContractArgs>,
    ) -> ToolResult<DraftContractResult> {
        let model_name = params.0.model;

        let client = match self.make_ai_client() {
            Ok(Some(c)) => c,
            Ok(None) => {
                return Ok(Json(DraftContractResult {
                    model: model_name,
                    message: Some(format!(
                        "{} not set in the server environment",
                        rocky_ai::client::AI_API_KEY_ENV
                    )),
                    ..Default::default()
                }));
            }
            Err(e) => return Err(ToolError::ai_error(format!("AI client init failed: {e}"))),
        };

        // The model's inferred output schema — the basis for compile-verifying
        // the drafted contract.
        let compiled = self
            .compile_full()
            .map_err(|e| ToolError::compile_failed(format!("{e:#}")))?;
        let inferred_schema: Vec<rocky_compiler::types::TypedColumn> = compiled
            .type_check
            .typed_models
            .get(&model_name)
            .cloned()
            .ok_or_else(|| ToolError::model_not_found(&model_name))?;

        // Profile each column against the live target table.
        let profile = match self
            .profile_table_columns(&model_name, &inferred_schema)
            .await
        {
            Ok(p) => p,
            Err(e) => {
                return Ok(Json(DraftContractResult {
                    model: model_name,
                    message: Some(format!("could not profile the target table: {e:#}")),
                    ..Default::default()
                }));
            }
        };

        let drafted = rocky_ai::contract::draft_contract(&profile, &inferred_schema, &client, 3)
            .await
            .map_err(|e| ToolError::ai_error(format!("contract draft failed: {e}")))?;

        Ok(Json(DraftContractResult {
            model: model_name,
            contract_toml: Some(drafted.toml),
            attempts: Some(drafted.attempts),
            message: None,
        }))
    }

    #[tool(
        description = "Draft test assertions for a model from its intent, schema, and SQL (the \
         `rocky ai-test` generator). An LLM proposes SQL assertions that each return 0 rows when \
         the invariant holds (not-null, grain uniqueness, value ranges, referential integrity). \
         Returns the assertions as DRAFTS — write each into the project's `tests/` directory \
         (`<model>_<name>.sql`) and run them via the `test` tool; it mutates nothing. Requires \
         ANTHROPIC_API_KEY in the server environment — without it, `assertions` is empty and \
         `message` explains why."
    )]
    async fn generate_tests(
        &self,
        params: Parameters<GenerateTestsArgs>,
    ) -> ToolResult<GenerateTestsResult> {
        let model_name = params.0.model;

        let client = match self.make_ai_client() {
            Ok(Some(c)) => c,
            Ok(None) => {
                return Ok(Json(GenerateTestsResult {
                    model: model_name,
                    message: Some(format!(
                        "{} not set in the server environment",
                        rocky_ai::client::AI_API_KEY_ENV
                    )),
                    ..Default::default()
                }));
            }
            Err(e) => return Err(ToolError::ai_error(format!("AI client init failed: {e}"))),
        };

        let (compiled, model) = self.compile_and_find_model(&model_name)?;
        let assertions = rocky_ai::testgen::generate_tests(&model, &compiled, &client)
            .await
            .map_err(|e| ToolError::ai_error(format!("test generation failed: {e}")))?;

        let assertions = assertions
            .into_iter()
            .map(|a| TestAssertionLite {
                name: a.name,
                sql: a.sql,
                description: a.description,
            })
            .collect();

        Ok(Json(GenerateTestsResult {
            model: model_name,
            assertions,
            message: None,
        }))
    }

    #[tool(
        description = "Draft an intent description for a model from its SQL, output schema, and \
         upstream dependencies (the `rocky ai-explain` generator). An LLM writes a 2-3 sentence \
         business-logic summary (grain, key filters/joins/aggregations). Returns the description \
         as a DRAFT — save it to the model's sidecar as `intent = \"...\"` if useful; it mutates \
         nothing. Requires ANTHROPIC_API_KEY in the server environment — without it, `intent` is \
         null and `message` explains why."
    )]
    async fn explain_model(
        &self,
        params: Parameters<ExplainModelArgs>,
    ) -> ToolResult<ExplainModelResult> {
        let model_name = params.0.model;

        let client = match self.make_ai_client() {
            Ok(Some(c)) => c,
            Ok(None) => {
                return Ok(Json(ExplainModelResult {
                    model: model_name,
                    message: Some(format!(
                        "{} not set in the server environment",
                        rocky_ai::client::AI_API_KEY_ENV
                    )),
                    ..Default::default()
                }));
            }
            Err(e) => return Err(ToolError::ai_error(format!("AI client init failed: {e}"))),
        };

        let (compiled, model) = self.compile_and_find_model(&model_name)?;
        let intent = rocky_ai::explain::explain_model(&model, &compiled, &client)
            .await
            .map_err(|e| ToolError::ai_error(format!("explain failed: {e}")))?;

        Ok(Json(ExplainModelResult {
            model: model_name,
            intent: Some(intent),
            message: None,
        }))
    }

    /// Build an [`LlmClient`](rocky_ai::client::LlmClient) for the generator
    /// tools, BYOK via `ANTHROPIC_API_KEY`. Returns `Ok(None)` when the key is
    /// unset so each tool degrades to a null draft + explanatory message (the
    /// same graceful no-op as `suggest_freshness_block`). `[ai] max_tokens`
    /// from `rocky.toml` is honoured when the config loads.
    fn make_ai_client(&self) -> anyhow::Result<Option<rocky_ai::client::LlmClient>> {
        let api_key = match std::env::var(rocky_ai::client::AI_API_KEY_ENV) {
            Ok(v) if !v.is_empty() => v,
            _ => return Ok(None),
        };
        let max_tokens = rocky_core::config::load_rocky_config(&self.config_path)
            .map(|cfg| cfg.ai.max_tokens)
            .unwrap_or(rocky_ai::client::DEFAULT_MAX_TOKENS);
        let ai_config = rocky_ai::client::AiConfig {
            provider: "anthropic".to_string(),
            model: "claude-sonnet-4-6".to_string(),
            api_key: rocky_core::redacted::RedactedString::new(api_key),
            default_format: "rocky".to_string(),
            max_attempts: 3,
            max_tokens,
        };
        rocky_ai::client::LlmClient::new(ai_config)
            .map(Some)
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    /// Compile the project and resolve `model_name` to its loaded
    /// [`Model`](rocky_core::models::Model). The generators that read source +
    /// intent (`generate_tests`, `explain_model`) need both the compile result
    /// and the owned model.
    fn compile_and_find_model(
        &self,
        model_name: &str,
    ) -> Result<(CompilerResult, rocky_core::models::Model), Json<ToolError>> {
        let compiled = self
            .compile_full()
            .map_err(|e| ToolError::compile_failed(format!("{e:#}")))?;
        let model = compiled
            .project
            .models
            .iter()
            .find(|m| m.config.name == model_name)
            .cloned()
            .ok_or_else(|| ToolError::model_not_found(model_name))?;
        Ok((compiled, model))
    }

    /// Profile each column of a model's target table into a
    /// [`TableProfile`](rocky_ai::contract::TableProfile) for `draft_contract`.
    ///
    /// Reuses the grounding path (`prepare_table_query` + `query_grounding`), so
    /// it works on any configured warehouse, not just DuckDB.
    ///
    /// # Egress
    ///
    /// Issues **aggregate statistics only** — `COUNT(*)`, `COUNT(col)`,
    /// `COUNT(DISTINCT col)` — and never selects `MIN`/`MAX` or a domain sample.
    /// No raw cell value leaves the machine; the prompt the LLM sees carries
    /// counts, not data. This mirrors the default of the `rocky ai-contract`
    /// generator this tool wraps (whose `--with-data` opt-in, which would send
    /// observed min/max and low-cardinality samples, is intentionally NOT
    /// exposed over MCP). `null_rate` + `distinct` are enough to draft the
    /// nullable / required / protected constraints; `min`/`max`/`observed_values`
    /// are left empty. SQL is built only from validated identifiers.
    async fn profile_table_columns(
        &self,
        model_name: &str,
        schema: &[rocky_compiler::types::TypedColumn],
    ) -> anyhow::Result<rocky_ai::contract::TableProfile> {
        let prepared = self.prepare_table_query(model_name).await?;

        let mut columns = Vec::with_capacity(schema.len());
        for typed_col in schema {
            let col = rocky_sql::validation::validate_identifier(&typed_col.name)
                .map_err(|e| anyhow::anyhow!("invalid column identifier: {e}"))?;
            // Statistics only — counts, never raw cell values. No MIN/MAX, no
            // domain query, so nothing observable from the table's contents
            // reaches the LLM prompt.
            let agg_sql = column_stats_sql(&prepared.table_ref, col);
            let qr = query_grounding(prepared.adapter.as_ref(), &agg_sql)
                .await
                .map_err(|e| {
                    anyhow::anyhow!("profile query failed for column '{}': {e}", typed_col.name)
                })?;
            let row = qr.rows.first().ok_or_else(|| {
                anyhow::anyhow!("profile query returned no rows for '{}'", typed_col.name)
            })?;

            let total = row.first().map(json_as_u64).unwrap_or(0);
            let non_null = row.get(1).map(json_as_u64).unwrap_or(0);
            let distinct = row.get(2).map(json_as_u64).unwrap_or(0);
            let nulls = total.saturating_sub(non_null);
            let null_rate = if total == 0 {
                0.0
            } else {
                nulls as f64 / total as f64
            };

            columns.push(rocky_ai::contract::ColumnProfile {
                name: typed_col.name.clone(),
                type_name: rocky_ai::contract::contract_type_name(typed_col),
                rows: total,
                nulls,
                null_rate,
                distinct,
                // Raw cell values are never sent over MCP (see # Egress).
                observed_values: Vec::new(),
                min: None,
                max: None,
            });
        }

        Ok(rocky_ai::contract::TableProfile {
            model: model_name.to_string(),
            columns,
        })
    }

    // ----------------- governance + drift preview tools --------------------
    // These let an agent see the full enforcement picture in-loop. Both are
    // read-only DRY-RUNs — neither applies anything. `governance_preview` is
    // offline (compile + sidecar read, the same core `rocky plan` uses);
    // `drift_preview` hits the configured warehouse via the same adapter path
    // as the grounding tools.

    #[tool(
        description = "Preview the pre-apply governance actions a subsequent `rocky run` would \
         reconcile: classification tags, masking policies, and retention policies declared across \
         the project's model sidecars. This is the same control-plane work `rocky plan` previews \
         — a DRY-RUN computed offline from the compiled models + their `[classification]` / `mask` \
         / `retention` config. It performs NO warehouse I/O and applies nothing. Empty action \
         lists mean the project declares no governance for that surface. Pass `env` to resolve \
         `[mask.<env>]` overrides (classification + retention are env-invariant). Use this to \
         confirm a model's PII / masking / retention is wired before proposing — encode an \
         invariant as governance, not just a WHERE clause."
    )]
    async fn governance_preview(
        &self,
        params: Parameters<GovernancePreviewArgs>,
    ) -> ToolResult<GovernancePreviewResult> {
        let env = params.0.env;

        let cfg = rocky_core::config::load_rocky_config(&self.config_path)
            .map_err(|e| ToolError::config_invalid(format!("could not load rocky.toml: {e:#}")))?;
        // Resolve the active pipeline's target adapter type — the same input
        // `rocky plan` feeds `populate_governance_actions` so retention's
        // `warehouse_preview` renders the warehouse-native form. This is the
        // ONLY thing the adapter type drives; classification + masking don't
        // touch it, and retention already degrades to `None` on an unknown
        // type. So a pipeline that won't resolve must not fail this offline
        // tool — degrade to "" and the preview still reports every declared
        // action, just without the warehouse-native retention rendering.
        let adapter_type = rocky_cli::registry::resolve_pipeline(&cfg, None)
            .ok()
            .and_then(|(_, pipeline)| {
                cfg.adapters
                    .get(pipeline.target_adapter())
                    .map(|a| a.adapter_type.clone())
            })
            .unwrap_or_default();

        // Reuse the exact offline governance-preview core `rocky plan` uses —
        // it compiles the models dir and reads each sidecar's governance
        // config, populating a PlanOutput. No discovery, no adapter call.
        let mut output = rocky_cli::output::PlanOutput::new(String::new());
        output.env = env.clone();
        commands::populate_governance_actions(
            &cfg,
            &self.models_dir,
            env.as_deref(),
            &adapter_type,
            &mut output,
        )
        .map_err(|e| ToolError::compile_failed(format!("governance preview failed: {e:#}")))?;

        Ok(Json(GovernancePreviewResult {
            env,
            classification_actions: output
                .classification_actions
                .into_iter()
                .map(|a| ClassificationActionLite {
                    model: a.model,
                    column: a.column,
                    tag: a.tag,
                })
                .collect(),
            mask_actions: output
                .mask_actions
                .into_iter()
                .map(|a| MaskActionLite {
                    model: a.model,
                    column: a.column,
                    tag: a.tag,
                    resolved_strategy: a.resolved_strategy,
                })
                .collect(),
            retention_actions: output
                .retention_actions
                .into_iter()
                .map(|a| RetentionActionLite {
                    model: a.model,
                    duration_days: a.duration_days,
                    warehouse_preview: a.warehouse_preview,
                })
                .collect(),
        }))
    }

    #[tool(
        description = "Preview source-vs-target schema drift between two warehouse tables — the \
         same apples-to-apples comparison `rocky run` performs before an incremental load. Both \
         tables are `DESCRIBE`d and their warehouse-reported column types compared via the engine's \
         drift detector. Read-only: it applies nothing. Pass `source_table` and `target_table` as \
         qualified `schema.table` (or `catalog.schema.table`) references. Returns drifted columns \
         (type changed), added columns (in source, missing from target — a run would ADD COLUMN), \
         and the action the runtime would take (`ignore` / `add_columns` / `alter_column_types` / \
         `drop_and_recreate`). When the target doesn't exist yet, `target_exists` is false and the \
         lists are empty. Hits the configured warehouse — requires live credentials."
    )]
    async fn drift_preview(
        &self,
        params: Parameters<DriftPreviewArgs>,
    ) -> ToolResult<DriftPreviewResult> {
        let args = params.0;

        let adapter = self
            .warehouse_adapter()
            .map_err(|e| {
                ToolError::warehouse_error(
                    format!("could not resolve the warehouse adapter: {e:#}"),
                    "Check the [adapter] block in rocky.toml and that the target warehouse's \
                     credentials are set in the server environment.",
                )
            })?
            .ok_or_else(|| {
                ToolError::warehouse_error(
                    "could not resolve the target warehouse adapter",
                    "Check the [adapter] block in rocky.toml and that the target warehouse's \
                     credentials are set in the server environment.",
                )
            })?;

        let source_ref = parse_table_ref(&args.source_table).ok_or_else(|| {
            ToolError::invalid_argument(
                format!("invalid source_table reference '{}'", args.source_table),
                "Pass a qualified `schema.table` or `catalog.schema.table` reference.",
            )
        })?;
        let target_ref = parse_table_ref(&args.target_table).ok_or_else(|| {
            ToolError::invalid_argument(
                format!("invalid target_table reference '{}'", args.target_table),
                "Pass a qualified `schema.table` or `catalog.schema.table` reference.",
            )
        })?;

        // DESCRIBE both tables. A failed describe on the TARGET means it is not
        // materialized yet (the first run would create it) — that's a clean
        // "no drift, target absent" answer, not an error. A failed describe on
        // the SOURCE is a genuine error (you asked to compare against a table
        // that isn't there).
        let source_cols = adapter.describe_table(&source_ref).await.map_err(|e| {
            ToolError::warehouse_error(
                format!(
                    "could not describe source_table '{}': {e}",
                    args.source_table
                ),
                "Confirm the source table exists and the target adapter's credentials can read it.",
            )
        })?;
        // Most adapters `Err` on a missing table, but some report an empty
        // column set instead; treat an empty source as not-found rather than
        // letting it produce a vacuously "no drift" answer that would lie.
        if source_cols.is_empty() {
            return Err(ToolError::warehouse_error(
                format!(
                    "source_table '{}' has no columns (table not found or empty schema)",
                    args.source_table
                ),
                "Confirm the source table exists and is not empty.",
            ));
        }
        let target_cols = adapter
            .describe_table(&target_ref)
            .await
            .unwrap_or_default();
        let target_exists = !target_cols.is_empty();

        if !target_exists {
            return Ok(Json(DriftPreviewResult {
                source_table: args.source_table,
                target_table: args.target_table,
                target_exists: false,
                action: drift_action_wire_name(&rocky_ir::DriftAction::Ignore).to_string(),
                ..Default::default()
            }));
        }

        let result = rocky_core::drift::detect_drift(
            &target_ref,
            &source_cols,
            &target_cols,
            adapter.dialect(),
        );

        // `detect_drift` returns `DriftAction::Ignore` whenever there are no
        // type-changed columns — INCLUDING the added-columns-only case. But
        // `rocky run` does NOT ignore that case: its `else if
        // !added_columns.is_empty()` branch (commands/run.rs) issues
        // `ALTER TABLE ADD COLUMN` and reports the action as `add_columns`.
        // Mirror the runtime's emitted action here so the preview doesn't tell
        // an agent "no action" for a run that would actually ALTER the target.
        let action =
            if result.action == rocky_ir::DriftAction::Ignore && !result.added_columns.is_empty() {
                "add_columns".to_string()
            } else {
                drift_action_wire_name(&result.action).to_string()
            };

        Ok(Json(DriftPreviewResult {
            source_table: args.source_table,
            target_table: args.target_table,
            target_exists: true,
            drifted_columns: result
                .drifted_columns
                .into_iter()
                .map(|c| DriftedColumnLite {
                    name: c.name,
                    source_type: c.source_type,
                    target_type: c.target_type,
                })
                .collect(),
            added_columns: result.added_columns.into_iter().map(|c| c.name).collect(),
            action,
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
    ) -> ToolResult<SampleRowsResult> {
        let args = params.0;

        let prepared = self.prepare_table_query(&args.model).await.map_err(|e| {
            ToolError::warehouse_error(
                format!("{e:#}"),
                "Confirm the model name or `schema.table` reference exists and the target \
                     adapter in rocky.toml has live warehouse credentials.",
            )
        })?;

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
            .map_err(|e| {
                ToolError::warehouse_error(
                    format!("sample query failed: {e}"),
                    "Confirm the table is materialized and the target adapter's credentials can \
                     read it.",
                )
            })?;

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
    ) -> ToolResult<ProfileColumnResult> {
        let args = params.0;

        let prepared = self.prepare_table_query(&args.model).await.map_err(|e| {
            ToolError::warehouse_error(
                format!("{e:#}"),
                "Confirm the model name or `schema.table` reference exists and the target \
                     adapter in rocky.toml has live warehouse credentials.",
            )
        })?;

        let col = rocky_sql::validation::validate_identifier(&args.column).map_err(|e| {
            ToolError::invalid_argument(
                format!("invalid column identifier: {e}"),
                "Pass a valid column name (letters, digits, and underscores); verify it with \
                 `inspect_schema`.",
            )
        })?;

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
            .map_err(|e| {
                ToolError::warehouse_error(
                    format!("profile query failed: {e}"),
                    "Confirm the table is materialized and the target adapter's credentials can \
                     read it.",
                )
            })?;
        let row = qr.rows.first().ok_or_else(|| {
            ToolError::warehouse_error(
                "profile query returned no rows",
                "Confirm the target table is materialized and non-empty.",
            )
        })?;

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

    /// Validate a draft model `name` and resolve its `models/<name>.sql` +
    /// sidecar paths, refusing any name that could escape the models directory.
    ///
    /// Mirrors the import-dbt `safe_join_under` path guard (the traversal fix
    /// that hardened untrusted `model-paths`): reject an absolute name or any
    /// path-traversal component syntactically, and — when a target path already
    /// exists — canonicalize it and confirm it stays under the models directory,
    /// catching a pre-existing symlink that would redirect the write. A draft
    /// name is a bare identifier, so a separator, `..`, or extension is refused.
    fn resolve_draft_paths(&self, name: &str) -> Result<DraftPaths, Json<ToolError>> {
        use std::path::Component;

        let bad = |msg: String| {
            ToolError::invalid_argument(
                msg,
                "Pass a bare model name — a single identifier like \"completed_revenue\" — so it \
                 maps to exactly one models/<name>.sql draft under the project.",
            )
        };

        let stem = name.trim();
        if stem.is_empty() {
            return Err(bad("model name is empty".to_string()));
        }
        // A draft name is a single path segment with no extension: reject
        // separators, `..`, and `.` up front (syntactic, no filesystem access).
        if stem.contains('/') || stem.contains('\\') || stem.contains('.') {
            return Err(bad(format!(
                "model name '{stem}' must be a bare identifier: no path separators, '..', or \
                 extension (it becomes models/<name>.sql)"
            )));
        }
        // Belt-and-braces: the name must be exactly one normal path component.
        let mut comps = Path::new(stem).components();
        if !matches!(comps.next(), Some(Component::Normal(_))) || comps.next().is_some() {
            return Err(bad(format!(
                "model name '{stem}' is not a single path segment"
            )));
        }

        let sql_path = self.models_dir.join(format!("{stem}.sql"));
        let sidecar_path = self.models_dir.join(format!("{stem}.toml"));

        // Symlink defense-in-depth: if a target already exists, confirm it
        // resolves under the (canonicalized) models directory before we write
        // through it. A not-yet-existing path passed the syntactic check above.
        for p in [&sql_path, &sidecar_path] {
            if p.exists() {
                let base = self.models_dir.canonicalize().map_err(|e| {
                    bad(format!("failed to canonicalize the models directory: {e}"))
                })?;
                let canon = p
                    .canonicalize()
                    .map_err(|e| bad(format!("failed to canonicalize {}: {e}", p.display())))?;
                if !canon.starts_with(&base) {
                    return Err(bad(format!(
                        "draft path {} resolves outside the models directory",
                        p.display()
                    )));
                }
            }
        }

        Ok(DraftPaths {
            stem: stem.to_string(),
            sql_path,
            sidecar_path,
        })
    }

    #[tool(
        description = "Draft a Rocky transformation model into the project working tree and \
         compile it in the SAME call — the safe write path for an agent. Writes the SQL to \
         models/<name>.sql plus a sidecar carrying the intent, then compiles and returns the \
         diagnostics, so you get the type-check WITH the write (no separate round-trip). It does \
         NOT run, apply, or touch the warehouse; a draft is inert until you `propose` it and a \
         human reviews it. Path-gated to the models directory (a name with separators/`..` is \
         refused) and policy-aware: authoring into a governed scope returns a structured \
         policy_denied / policy_review_required error, and a denied draft is not left on disk. \
         Use this instead of raw file writes so your edits flow through compile feedback + policy."
    )]
    async fn draft_model(
        &self,
        params: Parameters<DraftModelArgs>,
    ) -> ToolResult<DraftModelResult> {
        let args = params.0;
        let paths = self.resolve_draft_paths(&args.name)?;

        // A cold project may not have a models/ directory yet.
        std::fs::create_dir_all(&self.models_dir).map_err(|e| {
            ToolError::internal(
                format!("failed to create the models directory: {e}"),
                "Ensure the project directory is writable so drafts can be written.",
            )
        })?;

        // Snapshot prior on-disk state so a policy DENY (or a write failure) rolls
        // back to leave NO new artifact — a draft the policy plane refuses must
        // not linger on disk (mirrors the propose gate's deny → no plan written).
        let prior_sql = std::fs::read(&paths.sql_path).ok();
        let prior_sidecar = std::fs::read(&paths.sidecar_path).ok();
        let rollback = || {
            restore_or_remove(&paths.sql_path, prior_sql.as_deref());
            restore_or_remove(&paths.sidecar_path, prior_sidecar.as_deref());
        };

        // Write the draft: the SQL body verbatim + a minimal sidecar that carries
        // the intent. Target/strategy resolve from the project's conventions
        // (rocky.toml pipeline + _defaults.toml), exactly as a hand-authored bare
        // model — the draft tool never invents a target the agent didn't ask for.
        if let Err(e) = std::fs::write(&paths.sql_path, ensure_trailing_newline(&args.sql)) {
            rollback();
            return Err(ToolError::internal(
                format!(
                    "failed to write draft SQL to {}: {e}",
                    paths.sql_path.display()
                ),
                "Ensure the models directory is writable.",
            ));
        }
        if let Err(e) = std::fs::write(
            &paths.sidecar_path,
            draft_sidecar(&paths.stem, args.intent.trim()),
        ) {
            rollback();
            return Err(ToolError::internal(
                format!(
                    "failed to write draft sidecar to {}: {e}",
                    paths.sidecar_path.display()
                ),
                "Ensure the models directory is writable.",
            ));
        }

        // Compile immediately — the agent gets the type-check with the write.
        // Scope the returned diagnostics to the drafted model (the whole project
        // is still checked, so a fatal error anywhere surfaces).
        let with_seed = self.seed_file().is_some();
        let output = match commands::compile_output(
            Some(&self.config_path),
            &self.state_path(),
            &self.models_dir,
            None,
            Some(&paths.stem),
            false,
            None,
            with_seed,
            None,
        ) {
            Ok(o) => o,
            Err(e) => {
                rollback();
                return Err(ToolError::compile_failed(format!("{e:#}")));
            }
        };
        let compiled = project_compile_result(&output);

        // A draft is a `propose`-class authorship. Map the drafted model to the
        // `propose` capability and consult the SAME agent-policy plane the
        // propose/apply gates use (the shared `evaluate_apply_policy`) — so an
        // agent authoring into a governed scope gets a structured verdict WITH
        // the write, not later at apply. Absent a `[policy]` block this resolves
        // to `NotConfigured` and behaviour is byte-identical to no policy plane.
        let state_path = self.state_path();
        let touched: std::collections::BTreeMap<String, rocky_core::config::PolicyCapability> =
            std::iter::once((
                paths.stem.clone(),
                rocky_core::config::PolicyCapability::Propose,
            ))
            .collect();
        // A draft has no plan; the decision is recorded against a draft-scoped id
        // so the audit ledger stays honest about what it is.
        let decision_id = format!("draft:{}", paths.stem);
        let gate = rocky_cli::commands::evaluate_apply_policy(
            &self.config_path,
            &decision_id,
            rocky_core::config::PolicyPrincipal::Agent,
            &touched,
            &self.models_dir,
            &state_path,
        );

        match gate {
            rocky_cli::commands::PolicyGate::NotConfigured
            | rocky_cli::commands::PolicyGate::Allow => Ok(Json(DraftModelResult {
                model: paths.stem.clone(),
                sql_path: rel_display(&self.root, &paths.sql_path),
                sidecar_path: rel_display(&self.root, &paths.sidecar_path),
                has_errors: compiled.has_errors,
                error_count: compiled.error_count,
                warning_count: compiled.warning_count,
                diagnostics: compiled.diagnostics,
                next_steps: DRAFT_NEXT_STEPS.to_string(),
            })),
            rocky_cli::commands::PolicyGate::RequireReview {
                model,
                rule_id,
                reason,
            } => {
                // Mirrors the propose gate's require_review: the draft is the
                // reviewable artifact, so it PERSISTS; the structured signal
                // routes the agent to human review before it takes the change
                // further in this governed scope.
                let named = rule_id.map(|r| format!(" (rule {r})")).unwrap_or_default();
                Err(ToolError::policy_review_required(
                    format!(
                        "policy requires human review before authoring in this scope: model \
                         '{model}'{named} — {reason}. The draft was written to {} for a human to \
                         review.",
                        rel_display(&self.root, &paths.sql_path)
                    ),
                    "A human must review this draft before it goes further; do not plan, propose, \
                     or apply it in this governed scope on your own."
                        .to_string(),
                    rule_id.map(|r| r.to_string()),
                ))
            }
            rocky_cli::commands::PolicyGate::Deny {
                model,
                rule_id,
                reason,
            } => {
                // A deny cannot be satisfied by review — roll the draft back so
                // NO artifact lingers on disk (the decision is already in the
                // ledger), consistent with the propose gate's deny semantics.
                rollback();
                let named = rule_id.map(|r| format!(" (rule {r})")).unwrap_or_default();
                Err(ToolError::policy_denied(
                    format!(
                        "policy denies authoring this model: '{model}'{named} — {reason}. A deny \
                         cannot be satisfied by human review, so the draft was not kept."
                    ),
                    "Re-scope the draft — author it under a different, ungoverned name, or drop \
                     it. A denied authorship cannot be applied even after review."
                        .to_string(),
                    rule_id.map(|r| r.to_string()),
                ))
            }
        }
    }

    #[tool(
        description = "Propose materializing the model(s) as an AI-AUTHORED plan. This does NOT \
         execute anything. It records a plan that a human must review and approve \
         (`rocky review <plan_id> --approve`) before `rocky apply <plan_id>` will run it. Surface \
         the plan_id and the review/apply path to the user; never approve on their behalf."
    )]
    async fn propose(&self, params: Parameters<ProposeArgs>) -> ToolResult<ProposeResult> {
        let result = self
            .compile_full()
            .map_err(|e| ToolError::compile_failed(format!("{e:#}")))?;
        if result.project.models.is_empty() {
            return Err(ToolError::empty_project(
                "project has no compiled models to propose",
            ));
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
            return Err(ToolError::model_not_found(model));
        }

        let run_plan = build_ai_run_plan(params.0.model.clone(), &result);

        // The `propose` tool is the sole MCP writer of plans; it always authors
        // an AI-authored plan and therefore always acts as the `agent`
        // principal. Embed the propose-time change-classification so the
        // reviewed capabilities bind to the plan_id (a creds-free / non-git
        // project fails closed — every model classified breaking).
        let state_path = self.state_path();
        let capabilities = rocky_cli::commands::compute_embedded_capabilities(
            &self.config_path,
            &self.models_dir,
            "main",
            Some(&state_path),
        );

        // Consult the agent-policy plane before persisting — the same per-model
        // evaluation `rocky apply` runs — so an over-eager agent receives a
        // structured, parseable verdict at propose time instead of a plan a
        // later apply silently refuses. Absent a `[policy]` block this resolves
        // to `NotConfigured` and behaviour is byte-identical to before the plane.
        let touched = capabilities.touched(&run_plan.models);
        // The deterministic id the plan will carry if written — recorded in the
        // audit ledger (and named in a review message) even when a deny refuses
        // to persist the plan.
        let plan_id = rocky_cli::plan_store::governed_plan_id(
            &rocky_cli::plan_store::PlanKind::AiAuthored,
            &run_plan,
            &capabilities,
        )
        .map_err(|e| {
            ToolError::internal(
                format!("failed to compute plan id: {e:#}"),
                "Retry the propose; if it persists, verify the project compiles cleanly.",
            )
        })?;
        let gate = rocky_cli::commands::evaluate_apply_policy(
            &self.config_path,
            &plan_id,
            rocky_core::config::PolicyPrincipal::Agent,
            &touched,
            &self.models_dir,
            &state_path,
        );

        let write_plan = || {
            rocky_cli::plan_store::write_plan_governed(
                &self.root,
                rocky_cli::plan_store::PlanKind::AiAuthored,
                &run_plan,
                rocky_core::config::PolicyPrincipal::Agent,
                capabilities,
            )
            .map_err(|e| {
                ToolError::internal(
                    format!("failed to write AI-authored plan: {e:#}"),
                    "Ensure the project directory is writable so the plan store can persist the \
                     plan.",
                )
            })
        };

        match gate {
            rocky_cli::commands::PolicyGate::NotConfigured
            | rocky_cli::commands::PolicyGate::Allow => {
                let plan_id = write_plan()?;
                Ok(Json(ProposeResult { plan_id, models }))
            }
            rocky_cli::commands::PolicyGate::RequireReview {
                model,
                rule_id,
                reason,
            } => {
                // Headed to human review — persist the plan so a reviewer can
                // approve it, then return a structured signal the agent parses.
                let plan_id = write_plan()?;
                let named = rule_id.map(|r| format!(" (rule {r})")).unwrap_or_default();
                Err(ToolError::policy_review_required(
                    format!(
                        "policy requires human review before this change can apply: \
                         model '{model}'{named} — {reason}. The plan was recorded as {plan_id}."
                    ),
                    format!(
                        "A human must run `rocky review {plan_id} --approve` then \
                         `rocky apply {plan_id}`; never approve on the user's behalf."
                    ),
                    rule_id.map(|r| r.to_string()),
                ))
            }
            rocky_cli::commands::PolicyGate::Deny {
                model,
                rule_id,
                reason,
            } => {
                // A deny cannot be satisfied by review — do NOT persist the
                // plan; the decision is already recorded in the audit ledger.
                let named = rule_id.map(|r| format!(" (rule {r})")).unwrap_or_default();
                Err(ToolError::policy_denied(
                    format!(
                        "policy denies proposing this change: model '{model}'{named} — {reason}. \
                         A deny cannot be satisfied by human review, so no plan was recorded."
                    ),
                    "Re-scope the change so it no longer touches the denied model — propose to a \
                     branch, or drop that model from the change. A denied change cannot be applied \
                     even after review."
                        .to_string(),
                    rule_id.map(|r| r.to_string()),
                ))
            }
        }
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

        let dialect = adapter.dialect();
        let table_ref = if target.contains('.') {
            // Qualified raw reference — validate each segment and quote it
            // dialect-correctly. No compile required: this is how an agent
            // grounds a source before (or without) authoring any model. The
            // dialect decides validation + quoting (e.g. BigQuery allows a
            // hyphenated project segment and backtick-quotes the ref).
            let parts: Vec<&str> = target.split('.').collect();
            dialect
                .ground_table_ref(&parts)
                .map_err(|e| anyhow::anyhow!("invalid table reference '{target}': {e}"))?
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
            let parts: Vec<&str> = if t.catalog.is_empty() {
                vec![&t.schema, &t.table]
            } else {
                vec![&t.catalog, &t.schema, &t.table]
            };
            dialect
                .ground_table_ref(&parts)
                .map_err(|e| anyhow::anyhow!("invalid model target reference: {e}"))?
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

    /// Sweep the project for models with no declarative tests and draft tests
    /// for them. Orchestrates the read-only catalog + generator tools and stops
    /// at *propose* — never applies.
    #[prompt(
        name = "find_untested_models",
        description = "Find models with no declarative tests and draft tests for them: catalog \
         -> identify untested models -> generate_tests / draft_contract -> propose. Stops at the \
         human approval gate."
    )]
    async fn find_untested_models(
        &self,
        Parameters(_args): Parameters<NoArgs>,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<GetPromptResult, McpError> {
        let messages = vec![
            PromptMessage::new_text(
                PromptMessageRole::Assistant,
                "I'll find the models that carry no declarative tests, draft tests grounded in \
                 their real data, and stop at a proposed plan for you to review and apply. A model \
                 that compiles is not the same as a model that is checked — tests are what make \
                 the substrate trust a future run.",
            ),
            PromptMessage::new_text(
                PromptMessageRole::User,
                "Find the untested models in this Rocky project and draft tests for them, using \
                 the MCP tools at each step:\n\n\
                 1. catalog — enumerate every model with its declared tests, checks, and \
                 contract. Treat a model with no checks, no contract, and no test files as \
                 untested. Prioritise leaf/marts models and anything carrying a primary key or a \
                 grain you can name.\n\
                 2. For each untested model, ground before you assert: sample_rows to see real \
                 values, and profile_column on any key, status, or amount column to learn its null \
                 rate, distinct count, and domain. The schema says a column exists; only the data \
                 tells you whether it is unique, non-null, or bounded.\n\
                 3. generate_tests — draft SQL assertions (not-null, grain uniqueness, value \
                 ranges, referential integrity) from what you observed. For invariants better \
                 expressed as required/protected columns, draft_contract instead. Both return \
                 DRAFTS — write each assertion into the project's tests/ directory and the \
                 contract next to its model.\n\
                 4. compile — type-check after writing, and run the new tests via the `test` tool. \
                 Fix against any diagnostic and re-run until clean.\n\
                 5. propose — generate the plan that records the new tests/contracts. It is an \
                 AI-authored plan with a plan_id.\n\n\
                 RECONCILE DISCIPLINE: a test that asserts the wrong invariant passes and is still \
                 wrong. Confirm the grain, the not-null columns, and the value domain against the \
                 sampled data before you encode them — do not assume `id` is unique or `status` is \
                 non-null without checking.\n\n\
                 STOP at propose. Never apply an AI-authored change directly — a bare apply is \
                 refused by design. Surface the plan_id and the review report, then the human runs \
                 `rocky review <plan-id> --approve` and `rocky apply <plan-id>`. Do not approve on \
                 the user's behalf unless they explicitly tell you to.",
            ),
        ];

        Ok(GetPromptResult::new(messages).with_description(
            "Find untested Rocky models and draft tests, stopping at the approval gate",
        ))
    }

    /// Add uniqueness + not-null tests to a model's primary-key / unique
    /// columns. Inspects the schema, identifies the key columns, drafts tests,
    /// and stops at *propose*.
    #[prompt(
        name = "add_tests_to_pks",
        description = "Add uniqueness + not-null tests to a model's primary-key / unique columns: \
         inspect_schema -> identify key columns -> generate_tests for uniqueness + not-null -> \
         propose. Stops at the human approval gate."
    )]
    async fn add_tests_to_pks(
        &self,
        Parameters(args): Parameters<ScopedModelArgs>,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<GetPromptResult, McpError> {
        let scope = match args.model.as_deref().map(str::trim) {
            Some(m) if !m.is_empty() => format!("the model `{m}`"),
            _ => "every model".to_string(),
        };
        let messages = vec![
            PromptMessage::new_text(
                PromptMessageRole::Assistant,
                "I'll identify the primary-key and unique columns, draft uniqueness and not-null \
                 tests grounded in the real data, and stop at a proposed plan for you to review \
                 and apply. A declared key is a claim; the data is what proves it.",
            ),
            PromptMessage::new_text(
                PromptMessageRole::User,
                format!(
                    "Add uniqueness + not-null tests to the key columns of {scope} in this Rocky \
                     project, using the MCP tools at each step:\n\n\
                     1. inspect_schema — read the typed columns. Identify the primary-key / unique \
                     / grain columns: an explicit key in the sidecar, an `id`-shaped column, or \
                     the columns that define the model's grain.\n\
                     2. profile_column — for each candidate key column, confirm it is actually \
                     unique (distinct count == row count) and non-null before you assert it. A \
                     column named `id` that has duplicates or nulls is not a key, and a test that \
                     claims it is will fail on the next run — find that out now, from the data.\n\
                     3. generate_tests — draft a uniqueness assertion and a not-null assertion for \
                     each confirmed key column (each returns 0 rows when the invariant holds). \
                     These are DRAFTS — write each into the project's tests/ directory as \
                     `<model>_<name>.sql`.\n\
                     4. compile, then run the new tests via the `test` tool. Loop until clean.\n\
                     5. propose — generate the plan recording the new tests. It is an AI-authored \
                     plan with a plan_id.\n\n\
                     RECONCILE DISCIPLINE: only assert uniqueness/not-null on columns the profile \
                     actually shows to be unique/non-null. Encoding a wrong key invariant is worse \
                     than none — it green-lights a future run that should have failed.\n\n\
                     STOP at propose. Never apply an AI-authored change directly — a bare apply is \
                     refused by design. Surface the plan_id and the review report, then the human \
                     runs `rocky review <plan-id> --approve` and `rocky apply <plan-id>`. Do not \
                     approve on the user's behalf unless they explicitly tell you to."
                ),
            ),
        ];

        Ok(GetPromptResult::new(messages).with_description(format!(
            "Add uniqueness + not-null tests to the keys of {scope}"
        )))
    }

    /// Produce a structured, read-only summary of the project from the catalog
    /// and lineage. No edits, no propose — purely informational.
    #[prompt(
        name = "summarize_project",
        description = "Produce a structured, read-only summary of the Rocky project: catalog + \
         lineage -> grouped overview of models, their grain, governance, tests, and DAG shape. \
         Read-only — no edits, no propose."
    )]
    async fn summarize_project(
        &self,
        Parameters(_args): Parameters<NoArgs>,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<GetPromptResult, McpError> {
        let messages = vec![
            PromptMessage::new_text(
                PromptMessageRole::Assistant,
                "I'll summarize this Rocky project from the catalog and lineage. This is a \
                 read-only orientation — I will not edit, propose, or apply anything.",
            ),
            PromptMessage::new_text(
                PromptMessageRole::User,
                "Summarize this Rocky project, using only the read-only MCP tools:\n\n\
                 1. catalog — enumerate every model with its target table, materialization \
                 strategy, declared tests/checks, contract, and governance (classification / mask \
                 / retention).\n\
                 2. lineage — for the key models (sources, marts/leaf models), trace upstream \
                 dependencies to understand the DAG shape and how data flows.\n\
                 3. Group the result into a structured summary: sources and raw inputs; \
                 intermediate transforms; marts / leaf outputs. For each, note its grain (one row \
                 per what?), its materialization strategy, whether it carries tests / a contract / \
                 governance, and its place in the DAG.\n\
                 4. Call out gaps an owner would care about: untested leaf models, PII columns \
                 with no mask, models with no contract, or long undocumented dependency chains. \
                 Frame these as observations, not actions.\n\n\
                 This is purely informational — do NOT write SQL, draft tests, propose a plan, or \
                 apply anything. If the user then wants to act on a gap, the find_untested_models \
                 or build_model trajectory is the next step.",
            ),
        ];

        Ok(GetPromptResult::new(messages)
            .with_description("Read-only structured summary of the Rocky project"))
    }

    /// Diagnose and fix failing declarative tests: run the tests, ground each
    /// failure with profile_column, propose a fix. Stops at *propose*.
    #[prompt(
        name = "fix_failing_test",
        description = "Diagnose and fix failing declarative tests: run `test` -> for each failure \
         profile_column the implicated columns to ground the cause -> propose a fix. Stops at the \
         human approval gate."
    )]
    async fn fix_failing_test(
        &self,
        Parameters(args): Parameters<ScopedModelArgs>,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<GetPromptResult, McpError> {
        let scope = match args.model.as_deref().map(str::trim) {
            Some(m) if !m.is_empty() => format!("the model `{m}`"),
            _ => "the project".to_string(),
        };
        let messages = vec![
            PromptMessage::new_text(
                PromptMessageRole::Assistant,
                "I'll run the tests, ground each failure in the real data before changing \
                 anything, and stop at a proposed fix for you to review and apply. A failing test \
                 is a signal — I will find out whether the test is wrong or the data is wrong \
                 before I touch either.",
            ),
            PromptMessage::new_text(
                PromptMessageRole::User,
                format!(
                    "Diagnose and fix the failing tests in {scope}, using the MCP tools at each \
                     step:\n\n\
                     1. test — run the declarative tests and read which assertions fail, on which \
                     model, and the failing-row count. Each failure names the invariant it \
                     checks.\n\
                     2. For each failure, ground the cause before deciding the fix: profile_column \
                     the implicated columns (the ones the assertion references) to see their \
                     actual null rate, distinct count, and value domain, and sample_rows to look \
                     at offending rows. The failure tells you WHAT broke; the data tells you \
                     WHY.\n\
                     3. Decide which side is wrong. Either the model SQL is wrong (it produces \
                     duplicates / nulls / out-of-domain values it shouldn't) — fix the SQL — or \
                     the test encodes an invariant the data was never meant to hold — fix the \
                     assertion. Do not weaken a test just to make it pass; that hides the \
                     defect.\n\
                     4. compile, then re-run the `test` tool. Loop until the failure is genuinely \
                     resolved, not silenced.\n\
                     5. propose — generate the plan recording the fix. It is an AI-authored plan \
                     with a plan_id.\n\n\
                     RECONCILE DISCIPLINE: the whole point is to check the data, not just the \
                     schema. A uniqueness test failing because the grain is actually composite \
                     (two columns, not one) is a real finding you can only see in the rows.\n\n\
                     STOP at propose. Never apply an AI-authored change directly — a bare apply is \
                     refused by design. Surface the plan_id and the review report, then the human \
                     runs `rocky review <plan-id> --approve` and `rocky apply <plan-id>`. Do not \
                     approve on the user's behalf unless they explicitly tell you to."
                ),
            ),
        ];

        Ok(GetPromptResult::new(messages)
            .with_description(format!("Diagnose and fix failing tests in {scope}")))
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

/// Build the per-column **statistics** query for `draft_contract`'s profiler.
///
/// Aggregate counts only — `COUNT(*)`, `COUNT(col)`, `COUNT(DISTINCT col)`.
/// Deliberately selects no `MIN`/`MAX` and issues no domain query, so no raw
/// cell value can reach the LLM prompt (the egress contract the MCP
/// `draft_contract` tool upholds — see `profile_table_columns`). `table_ref`
/// and `col` are already validated by the caller.
fn column_stats_sql(table_ref: &str, col: &str) -> String {
    format!(
        "SELECT COUNT(*) AS n, COUNT({col}) AS non_null, COUNT(DISTINCT {col}) AS distinct_n \
         FROM {table_ref}"
    )
}

/// Parse a qualified `schema.table` / `catalog.schema.table` reference into a
/// [`TableRef`](rocky_ir::TableRef) for `drift_preview`'s `describe_table`
/// calls.
///
/// Mirrors `commands/profile.rs::observed_column_types`: a two-part name has an
/// empty catalog (DuckDB / catalog-less dialects), a three-part name carries
/// one. Any other arity is rejected (returns `None`). Segments are not
/// validated here — `describe_table` is parameter-safe (the adapter quotes the
/// ref); a bad name surfaces as a describe error, not SQL injection.
fn parse_table_ref(reference: &str) -> Option<rocky_ir::TableRef> {
    let parts: Vec<&str> = reference.split('.').collect();
    match parts.as_slice() {
        [schema, table] => Some(rocky_ir::TableRef {
            catalog: String::new(),
            schema: (*schema).to_string(),
            table: (*table).to_string(),
        }),
        [catalog, schema, table] => Some(rocky_ir::TableRef {
            catalog: (*catalog).to_string(),
            schema: (*schema).to_string(),
            table: (*table).to_string(),
        }),
        _ => None,
    }
}

/// Stable wire name for a [`DriftAction`](rocky_ir::DriftAction) in a
/// `drift_preview` result — snake_case, matching the strings `rocky run`
/// emits in `DriftActionOutput.action`.
fn drift_action_wire_name(action: &rocky_ir::DriftAction) -> &'static str {
    match action {
        rocky_ir::DriftAction::DropAndRecreate => "drop_and_recreate",
        rocky_ir::DriftAction::AlterColumnTypes => "alter_column_types",
        rocky_ir::DriftAction::Ignore => "ignore",
    }
}

/// Read a `serde_json::Value` grounding cell as a `u64`, tolerating the
/// string-encoded integers some adapters return.
fn json_as_u64(v: &serde_json::Value) -> u64 {
    match v {
        serde_json::Value::Number(n) => n.as_u64().unwrap_or(0),
        serde_json::Value::String(s) => s.parse().unwrap_or(0),
        _ => 0,
    }
}

/// Parse a `target_dialect` tool argument into the engine's [`Dialect`].
///
/// Accepts the `Dialect` serde vocabulary case-insensitively
/// (`databricks`/`snowflake`/`bigquery`/`duckdb`). An unrecognised value is a
/// caller error returned as an [`InvalidArgument`](crate::error::ToolErrorCode)
/// envelope naming the accepted values, rather than silently ignoring the
/// request.
fn parse_target_dialect(raw: &str) -> Result<rocky_sql::transpile::Dialect, rmcp::Json<ToolError>> {
    use rocky_sql::transpile::Dialect;
    match raw.trim().to_ascii_lowercase().as_str() {
        "databricks" => Ok(Dialect::Databricks),
        "snowflake" => Ok(Dialect::Snowflake),
        "bigquery" => Ok(Dialect::BigQuery),
        "duckdb" => Ok(Dialect::DuckDB),
        other => Err(ToolError::invalid_argument(
            format!("unknown target_dialect '{other}'"),
            "Pass one of: databricks, snowflake, bigquery, duckdb.",
        )),
    }
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

/// The authoring-loop reminder every successful `draft_model` response carries.
/// A draft is written and compiled, never applied — this restates the flow so
/// the agent never mistakes a written draft for a materialized change.
const DRAFT_NEXT_STEPS: &str = "This is a draft — Rocky has NOT applied it or touched the \
     warehouse. Continue the authoring loop: fix any error diagnostics above and re-draft (or \
     `compile`) until it is clean, `plan_preview` to read the SQL Rocky would run, then `propose` \
     to record an AI-authored plan for a human to `rocky review <plan_id> --approve` and \
     `rocky apply`. Never apply a draft directly.";

/// The validated on-disk targets a draft writes to.
struct DraftPaths {
    /// The model name (bare file stem).
    stem: String,
    /// Absolute path of `models/<stem>.sql`.
    sql_path: PathBuf,
    /// Absolute path of `models/<stem>.toml`.
    sidecar_path: PathBuf,
}

/// Restore `path` to its snapshotted `prior` bytes, or remove it when it had no
/// prior content. The rollback primitive for a policy-denied (or failed) draft:
/// a freshly written draft is removed entirely; a re-draft over an existing
/// model is restored to the model's prior content, so a deny never corrupts nor
/// leaves a new artifact.
fn restore_or_remove(path: &Path, prior: Option<&[u8]>) {
    match prior {
        Some(bytes) => {
            let _ = std::fs::write(path, bytes);
        }
        None => {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// Ensure the drafted SQL ends in exactly one trailing newline (POSIX text
/// file), without disturbing a body that already does.
fn ensure_trailing_newline(sql: &str) -> String {
    let trimmed = sql.trim_end_matches('\n');
    format!("{trimmed}\n")
}

/// Build the draft sidecar TOML: `name` (matching the file stem so the L001
/// name lint stays quiet) plus the `intent`, both TOML-escaped. Target and
/// strategy are intentionally omitted — they resolve from the project's
/// conventions, keeping the draft tool from inventing routing the agent never
/// asked for.
fn draft_sidecar(stem: &str, intent: &str) -> String {
    let header = "# Draft authored via the Rocky MCP `draft_model` tool. Target and strategy \
                  resolve\n# from the project's conventions (rocky.toml pipeline + \
                  _defaults.toml).\n";
    if intent.is_empty() {
        format!("{header}name = {}\n", toml_basic_string(stem))
    } else {
        format!(
            "{header}name = {}\nintent = {}\n",
            toml_basic_string(stem),
            toml_basic_string(intent)
        )
    }
}

/// Render `s` as a TOML basic string (double-quoted, with the escapes TOML
/// requires) so an arbitrary intent embeds safely in the sidecar.
fn toml_basic_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04X}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Display `path` relative to the project `root` with forward slashes, falling
/// back to the absolute path when it is not under the root.
fn rel_display(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .components()
        .map(|c| c.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
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
    fn parse_target_dialect_accepts_known_values_case_insensitively() {
        use rocky_sql::transpile::Dialect;
        // `Json<ToolError>` is not `Debug`, so match rather than `.expect()`.
        let ok = |s: &str| match parse_target_dialect(s) {
            Ok(d) => d,
            Err(_) => panic!("'{s}' should parse to a known dialect"),
        };
        assert_eq!(ok("bigquery"), Dialect::BigQuery);
        assert_eq!(ok("BigQuery"), Dialect::BigQuery);
        assert_eq!(ok(" snowflake "), Dialect::Snowflake);
        assert_eq!(ok("DATABRICKS"), Dialect::Databricks);
        assert_eq!(ok("duckdb"), Dialect::DuckDB);
    }

    #[test]
    fn parse_target_dialect_rejects_unknown_value() {
        let err = parse_target_dialect("redshift").expect_err("unknown dialect must error");
        // The failure is the structured envelope: an invalid_argument code, the
        // offending value in the message, and the accepted set in the hint.
        assert_eq!(err.0.code, crate::error::ToolErrorCode::InvalidArgument);
        assert!(
            err.0.message.contains("redshift"),
            "message should name the input: {:?}",
            err.0
        );
        assert!(
            err.0.remediation_hint.contains("bigquery"),
            "hint should list the accepted values: {:?}",
            err.0
        );
    }

    /// Egress contract: the `draft_contract` profiler issues STATISTICS only —
    /// it must never select raw cell values (`MIN`/`MAX`) nor a domain sample,
    /// matching the default of the `rocky ai-contract` generator it wraps.
    #[test]
    fn column_stats_sql_sends_no_raw_cell_values() {
        let sql = column_stats_sql("out.orders", "status");
        assert!(
            sql.contains("COUNT(DISTINCT status)"),
            "distinct COUNT is a statistic and is expected: {sql}"
        );
        let upper = sql.to_uppercase();
        assert!(
            !upper.contains("MIN(") && !upper.contains("MAX("),
            "statistics-only query must not select MIN/MAX: {sql}"
        );
        assert!(
            !upper.contains("DISTINCT CAST"),
            "statistics-only query must not issue the domain-values query: {sql}"
        );
    }

    #[test]
    fn json_as_u64_handles_null_number_and_string() {
        assert_eq!(json_as_u64(&serde_json::json!(42)), 42);
        assert_eq!(json_as_u64(&serde_json::json!("17")), 17);
        assert_eq!(json_as_u64(&serde_json::json!(null)), 0);
        assert_eq!(json_as_u64(&serde_json::json!("nope")), 0);
    }

    #[test]
    fn server_resolves_models_dir_beside_config() {
        let server = RockyMcpServer::new(PathBuf::from("/tmp/proj/rocky.toml"));
        assert_eq!(server.models_dir, PathBuf::from("/tmp/proj/models"));
        assert_eq!(server.root, PathBuf::from("/tmp/proj"));
    }

    #[test]
    fn resolve_draft_paths_accepts_a_bare_name_and_refuses_traversal() {
        let server = RockyMcpServer::new(PathBuf::from("/tmp/proj/rocky.toml"));
        let Ok(ok) = server.resolve_draft_paths("completed_revenue") else {
            panic!("a bare name should resolve");
        };
        assert_eq!(ok.stem, "completed_revenue");
        assert_eq!(
            ok.sql_path,
            PathBuf::from("/tmp/proj/models/completed_revenue.sql")
        );
        assert_eq!(
            ok.sidecar_path,
            PathBuf::from("/tmp/proj/models/completed_revenue.toml")
        );
        for bad in [
            "../evil",
            "/etc/passwd",
            "sub/model",
            "..\\win",
            "revenue.sql",
            "..",
            "",
        ] {
            assert!(
                server.resolve_draft_paths(bad).is_err(),
                "name '{bad}' must be refused as a path-escape / non-bare name"
            );
        }
    }

    #[test]
    fn draft_sidecar_toml_escapes_the_intent() {
        let sidecar = draft_sidecar("orders", "revenue for \"COMPLETE\" orders\nline two");
        assert!(sidecar.contains("name = \"orders\""));
        // Quotes and newlines in the intent are TOML-escaped so an arbitrary
        // intent embeds as a valid TOML basic string.
        assert!(sidecar.contains("intent = \"revenue for \\\"COMPLETE\\\" orders\\nline two\""));
        // An empty intent omits the key entirely (still a valid sidecar).
        let empty = draft_sidecar("orders", "");
        assert!(empty.contains("name = \"orders\""));
        assert!(
            !empty.contains("intent ="),
            "empty intent omits the intent key"
        );
    }

    #[test]
    fn ensure_trailing_newline_normalizes() {
        assert_eq!(ensure_trailing_newline("SELECT 1"), "SELECT 1\n");
        assert_eq!(ensure_trailing_newline("SELECT 1\n"), "SELECT 1\n");
        assert_eq!(ensure_trailing_newline("SELECT 1\n\n"), "SELECT 1\n");
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
    fn ground_table_ref_default_emits_unquoted_segments() {
        use rocky_core::traits::SqlDialect;
        use rocky_duckdb::dialect::DuckDbSqlDialect;
        // The grounding path routes a parsed table ref through the target
        // dialect's `ground_table_ref`. The default (DuckDB/Snowflake/
        // Databricks) joins validated segments unquoted — Snowflake relies on
        // this to fold to its default uppercase casing rather than locking in
        // a case-sensitive quoted name.
        let d = DuckDbSqlDialect;
        // Three-part name (catalog.schema.table).
        assert_eq!(
            d.ground_table_ref(&["analytics", "raw", "orders"]).unwrap(),
            "analytics.raw.orders"
        );
        // Two-part name (schema.table).
        assert_eq!(
            d.ground_table_ref(&["raw", "orders"]).unwrap(),
            "raw.orders"
        );
    }

    #[test]
    fn ground_table_ref_default_rejects_bad_identifier_and_arity() {
        use rocky_core::traits::SqlDialect;
        use rocky_duckdb::dialect::DuckDbSqlDialect;
        let d = DuckDbSqlDialect;
        // Injection in any segment is rejected.
        assert!(
            d.ground_table_ref(&["raw", "orders; DROP TABLE x"])
                .is_err()
        );
        // A four-part ref (or a single bare name) is out of range.
        assert!(d.ground_table_ref(&["a", "b", "c", "d"]).is_err());
        assert!(d.ground_table_ref(&["orders"]).is_err());
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
