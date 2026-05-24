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
    /// The model whose target table to sample. Must be a compiled model.
    pub model: String,
    /// Sample percentage (1–100). Defaults to 10.
    #[serde(default)]
    pub percent: Option<u32>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ProfileColumnArgs {
    /// The model whose target table to profile.
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
        let output = commands::test_output(&self.models_dir, None).map_err(|e| format!("{e:#}"))?;
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
        let result = self.compile_full().map_err(|e| format!("{e:#}"))?;
        let (model_schemas, source_tables) = commands::build_schema_context(&result);
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
        Ok(Json(InspectSchemaResult {
            models: to_entries(model_schemas),
            sources: to_entries(source_tables),
        }))
    }

    // ------------------------- SHOULD tools --------------------------------

    #[tool(
        description = "Sample real rows from a model's target table (DuckDB only). Look at \
         literal values, units, and null patterns the schema can't tell you. Capped at 50 rows / \
         16 KB; long cells truncated. Returns {unavailable:true} on non-DuckDB adapters."
    )]
    async fn sample_rows(
        &self,
        params: Parameters<SampleRowsArgs>,
    ) -> Result<Json<SampleRowsResult>, String> {
        let args = params.0;
        let percent = args.percent.unwrap_or(10).clamp(1, 100);

        let prepared = match self.prepare_table_query(&args.model).await {
            Ok(PreparedTable::Ready(p)) => p,
            Ok(PreparedTable::Unavailable(reason)) => {
                return Ok(Json(SampleRowsResult {
                    unavailable: true,
                    reason: Some(reason),
                    ..Default::default()
                }));
            }
            Err(e) => return Err(format!("{e:#}")),
        };

        // Build: SELECT * FROM <ref> <tablesample> LIMIT n. The table ref is
        // built only from validated identifiers; never `format!`'d from raw
        // input. `percent` is a clamped integer.
        let sample = prepared
            .dialect_tablesample(percent)
            .map(|s| format!(" {s}"))
            .unwrap_or_default();
        let sql = format!(
            "SELECT * FROM {}{} LIMIT {}",
            prepared.table_ref, sample, SAMPLE_MAX_ROWS
        );

        let qr = prepared
            .adapter
            .execute_query(&sql)
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
        description = "Profile one column of a model's target table in a single aggregate query \
         (DuckDB only): row count, nulls, null rate, distinct count, min, max. Returns \
         {unavailable:true} on non-DuckDB adapters."
    )]
    async fn profile_column(
        &self,
        params: Parameters<ProfileColumnArgs>,
    ) -> Result<Json<ProfileColumnResult>, String> {
        let args = params.0;

        let prepared = match self.prepare_table_query(&args.model).await {
            Ok(PreparedTable::Ready(p)) => p,
            Ok(PreparedTable::Unavailable(reason)) => {
                return Ok(Json(ProfileColumnResult {
                    unavailable: true,
                    reason: Some(reason),
                    ..Default::default()
                }));
            }
            Err(e) => return Err(format!("{e:#}")),
        };

        let col = rocky_sql::validation::validate_identifier(&args.column)
            .map_err(|e| format!("invalid column identifier: {e}"))?;

        let sql = format!(
            "SELECT COUNT(*) AS n, COUNT({col}) AS non_null, COUNT(DISTINCT {col}) AS distinct_n, \
             CAST(MIN({col}) AS VARCHAR) AS min_v, CAST(MAX({col}) AS VARCHAR) AS max_v \
             FROM {}",
            prepared.table_ref
        );

        let qr = prepared
            .adapter
            .execute_query(&sql)
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

        Ok(Json(ProfileColumnResult {
            unavailable: false,
            reason: None,
            rows: total,
            nulls,
            null_rate,
            distinct,
            min: str_cell(row.get(3)),
            max: str_cell(row.get(4)),
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

    /// Resolve a model's target table into a runnable, validated table ref +
    /// the warehouse adapter — but only on DuckDB. Other adapters return the
    /// typed "unavailable" result so the grounding tools degrade cleanly.
    async fn prepare_table_query(&self, model_name: &str) -> anyhow::Result<PreparedTable> {
        let cfg = rocky_core::config::load_rocky_config(&self.config_path)?;
        let registry = commands_adapter_registry(&cfg)?;
        let (_, pipeline) = rocky_cli::registry::resolve_pipeline(&cfg, None)?;
        let target_adapter = pipeline.target_adapter().to_string();

        let adapter_type = registry
            .adapter_config(&target_adapter)
            .map(|a| a.adapter_type.clone())
            .unwrap_or_default();
        if adapter_type != "duckdb" {
            return Ok(PreparedTable::Unavailable(
                "sample_rows/profile_column are DuckDB-only in this release".to_string(),
            ));
        }

        // Resolve the model's target coordinates by loading the models dir.
        let result = self.compile_full()?;
        let model = result
            .project
            .models
            .iter()
            .find(|m| m.config.name == model_name)
            .ok_or_else(|| anyhow::anyhow!("model '{model_name}' not found in project"))?;
        let t = &model.config.target;

        // Validate every identifier before composing the ref. DuckDB has no
        // catalog level, so we emit a two-part `schema.table` name.
        let schema = rocky_sql::validation::validate_identifier(&t.schema)
            .map_err(|e| anyhow::anyhow!("invalid schema identifier: {e}"))?;
        let table = rocky_sql::validation::validate_identifier(&t.table)
            .map_err(|e| anyhow::anyhow!("invalid table identifier: {e}"))?;
        if !t.catalog.is_empty() {
            rocky_sql::validation::validate_identifier(&t.catalog)
                .map_err(|e| anyhow::anyhow!("invalid catalog identifier: {e}"))?;
        }
        let table_ref = format!("{schema}.{table}");

        let adapter = registry.warehouse_adapter(&target_adapter)?;
        Ok(PreparedTable::Ready(Prepared { adapter, table_ref }))
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

/// A validated, DuckDB-runnable table reference plus the adapter to run it on.
struct Prepared {
    adapter: std::sync::Arc<dyn rocky_core::traits::WarehouseAdapter>,
    table_ref: String,
}

impl Prepared {
    fn dialect_tablesample(&self, percent: u32) -> Option<String> {
        self.adapter.dialect().tablesample_clause(percent)
    }
}

/// Either a runnable table query or the reason a non-DuckDB adapter makes the
/// data-grounding tool unavailable.
enum PreparedTable {
    Ready(Prepared),
    Unavailable(String),
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
}
