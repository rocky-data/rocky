//! Lite, schemars-1.x result projections returned by the MCP tools.
//!
//! These deliberately mirror only the fields an agent needs, dropping
//! token-heavy payloads (`expanded_sql`, full `models_detail`, etc.). They
//! derive schemars 1.x (rmcp's `Json<T>` bound) and are built from Rocky's
//! 0.8-deriving `*Output` types at the tool boundary — see the module note in
//! `lib.rs` for why the two cannot be shared.

use schemars::JsonSchema;
use serde::Serialize;

/// One compiler diagnostic, projected from `rocky_compiler::diagnostic::Diagnostic`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct DiagnosticLite {
    /// Diagnostic code, e.g. `"E001"`, `"W003"`, `"P001"`.
    pub code: String,
    /// `"Error"`, `"Warning"`, or `"Info"`.
    pub severity: String,
    /// Model the diagnostic is attached to.
    pub model: String,
    /// Human-readable message.
    pub message: String,
    /// Suggested fix, when the diagnostic carries one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggestion: Option<String>,
    /// `file:line:col` source location, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span: Option<String>,
}

/// Trimmed `rocky compile` result. Drops `expanded_sql` and the full
/// `models_detail` (token-heavy) in favour of counts + diagnostics.
#[derive(Debug, Serialize, JsonSchema)]
pub struct CompileResult {
    /// Whether compilation produced any error-severity diagnostics.
    pub has_errors: bool,
    /// Count of error-severity diagnostics.
    pub error_count: usize,
    /// Count of warning-severity diagnostics.
    pub warning_count: usize,
    /// Number of models in the project.
    pub model_count: usize,
    /// All diagnostics (errors + warnings + info).
    pub diagnostics: Vec<DiagnosticLite>,
}

/// One statement in a `plan_preview` result.
#[derive(Debug, Serialize, JsonSchema)]
pub struct PlannedStatementLite {
    /// What the statement does, e.g. `"full_refresh"`, `"incremental"`.
    pub purpose: String,
    /// Fully-qualified target the statement writes to.
    pub target: String,
    /// The generated SQL.
    pub sql: String,
}

/// `plan_preview` result — the SQL the runner would emit for the model(s).
#[derive(Debug, Serialize, JsonSchema)]
pub struct PlanPreviewResult {
    /// Generated statements, in execution order.
    pub statements: Vec<PlannedStatementLite>,
}

/// One lineage edge (column → column with a transform label).
#[derive(Debug, Serialize, JsonSchema)]
pub struct LineageEdgeLite {
    pub source_model: String,
    pub source_column: String,
    pub target_model: String,
    pub target_column: String,
    pub transform: String,
}

/// `lineage` result. When `column` is set, `trace` holds the column-level
/// trace and `direction` is `"upstream"`/`"downstream"`; otherwise
/// `columns` + `upstream`/`downstream` model lists + model-level `edges`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct LineageResult {
    /// Focal model.
    pub model: String,
    /// Focal column, when the query was column-scoped.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
    /// `"upstream"` / `"downstream"` for a column-scoped trace.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub direction: Option<String>,
    /// Column names of the focal model (model-level query only).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<String>,
    /// Upstream model names (model-level query only).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub upstream: Vec<String>,
    /// Downstream model names (model-level query only).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub downstream: Vec<String>,
    /// Lineage edges: the model-level edge set, or the column trace.
    pub edges: Vec<LineageEdgeLite>,
}

/// One failing test in a `test` result.
#[derive(Debug, Serialize, JsonSchema)]
pub struct TestFailureLite {
    pub name: String,
    pub error: String,
}

/// `test` result — DuckDB-backed local assertions.
#[derive(Debug, Serialize, JsonSchema)]
pub struct TestResult {
    pub total: usize,
    pub passed: usize,
    pub failures: Vec<TestFailureLite>,
}

/// One row in a `list` result. Each `kind` populates a distinct subset of
/// fields (absent fields are omitted) — no field is overloaded across kinds:
///
/// - **models**: `name`, `target`, `strategy`, `depends_on`.
/// - **pipelines**: `name`, `pipeline_type`, `target_adapter`, `depends_on`.
/// - **adapters**: `name`, `adapter_type`, `host`.
/// - **sources**: `name` (the pipeline), `adapter`, `catalog`.
#[derive(Debug, Default, Serialize, JsonSchema)]
pub struct ListEntry {
    /// Primary identifier: the model / pipeline / adapter name, or — for
    /// `sources` — the owning pipeline name.
    pub name: String,
    /// (models) Fully-qualified target table `catalog.schema.table`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    /// (models) Materialization strategy, e.g. `"full_refresh"`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strategy: Option<String>,
    /// (pipelines) Pipeline type, e.g. `"replication"`, `"transformation"`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pipeline_type: Option<String>,
    /// (pipelines) The pipeline's target adapter name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_adapter: Option<String>,
    /// (adapters) Adapter type, e.g. `"duckdb"`, `"snowflake"`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub adapter_type: Option<String>,
    /// (adapters) Connection host, when configured.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    /// (sources) Source adapter name for this replication pipeline.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub adapter: Option<String>,
    /// (sources) Source catalog, when configured.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog: Option<String>,
    /// (models / pipelines) Declared upstream dependencies.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<String>,
}

/// `list` result.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ListResult {
    /// What was listed: `"models"`, `"pipelines"`, `"adapters"`, `"sources"`.
    pub kind: String,
    pub entries: Vec<ListEntry>,
}

/// One typed column in an `inspect_schema` result.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ColumnLite {
    pub name: String,
    /// Rocky type, e.g. `"Int64"`, `"String"`. `"Unknown"` when unresolved.
    pub data_type: String,
    pub nullable: bool,
}

/// A model or source table with its typed columns.
#[derive(Debug, Serialize, JsonSchema)]
pub struct SchemaEntry {
    pub name: String,
    pub columns: Vec<ColumnLite>,
}

/// `inspect_schema` result — the typed columns of every model + source.
#[derive(Debug, Serialize, JsonSchema)]
pub struct InspectSchemaResult {
    pub models: Vec<SchemaEntry>,
    pub sources: Vec<SchemaEntry>,
}

/// `sample_rows` result — a capped sample of real rows.
///
/// On a non-DuckDB adapter, `unavailable` is `true`, `reason` explains why,
/// and the data fields are empty. (A single concrete schema is required:
/// rmcp's output-schema derivation rejects an untyped union.)
#[derive(Debug, Default, Serialize, JsonSchema)]
pub struct SampleRowsResult {
    /// `true` when the tool could not run (e.g. non-DuckDB adapter).
    #[serde(default, skip_serializing_if = "is_false")]
    pub unavailable: bool,
    /// Why the tool is unavailable, when `unavailable` is `true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Column names, in result order.
    pub columns: Vec<String>,
    /// Sampled rows; each cell is rendered as a string (truncated at 256
    /// chars). Capped at 50 rows and ~16 KB serialized.
    pub rows: Vec<Vec<String>>,
    /// `true` when the cap (rows or bytes) clipped the result.
    pub truncated: bool,
}

/// `profile_column` result — one-pass aggregate stats for a column.
///
/// On a non-DuckDB adapter, `unavailable` is `true` and `reason` explains why.
#[derive(Debug, Default, Serialize, JsonSchema)]
pub struct ProfileColumnResult {
    /// `true` when the tool could not run (e.g. non-DuckDB adapter).
    #[serde(default, skip_serializing_if = "is_false")]
    pub unavailable: bool,
    /// Why the tool is unavailable, when `unavailable` is `true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub rows: u64,
    pub nulls: u64,
    pub null_rate: f64,
    pub distinct: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<String>,
}

/// serde `skip_serializing_if` predicate for `bool` fields.
fn is_false(b: &bool) -> bool {
    !*b
}

/// `propose` result — the AI-authored plan id awaiting human review.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ProposeResult {
    /// 64-char blake3 plan id. Apply is gated: run
    /// `rocky review <plan_id> --approve` then `rocky apply <plan_id>`.
    pub plan_id: String,
    /// The models this plan would materialize.
    pub models: Vec<String>,
}
