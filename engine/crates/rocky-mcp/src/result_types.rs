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
/// `unavailable` / `reason` are reserved for the case where the tool could not
/// run (e.g. the target adapter has no live credentials); the data fields are
/// then empty. (A single concrete schema is required: rmcp's output-schema
/// derivation rejects an untyped union.)
#[derive(Debug, Default, Serialize, JsonSchema)]
pub struct SampleRowsResult {
    /// `true` when the tool could not run (reserved for future degradation
    /// paths; the fields below carry the sample when `false`).
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
/// `unavailable` / `reason` are reserved for the case where the tool could not
/// run (e.g. the target adapter has no live credentials).
#[derive(Debug, Default, Serialize, JsonSchema)]
pub struct ProfileColumnResult {
    /// `true` when the tool could not run (reserved for future degradation
    /// paths).
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
    /// For a low-cardinality column (≤25 distinct), the distinct values with
    /// their row counts — surfaces exact literals that `min`/`max` hide (e.g.
    /// `status` = 'COMPLETE', not 'completed'). Empty for high-cardinality
    /// columns, and omitted from JSON when empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub top_values: Vec<ValueCount>,
}

/// One distinct value of a column and how many rows carry it — the entries of
/// `ProfileColumnResult::top_values`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ValueCount {
    /// The value rendered as text; omitted (JSON null) for the SQL NULL group.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    /// Number of rows carrying this value.
    pub count: u64,
}

/// serde `skip_serializing_if` predicate for `bool` fields.
fn is_false(b: &bool) -> bool {
    !*b
}

/// One classified breaking-change finding, projected from
/// `rocky_core::breaking_change::BreakingFinding`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct BreakingFindingLite {
    /// The change kind discriminant, e.g. `"column_dropped"`,
    /// `"column_type_changed"`, `"model_removed"`.
    pub change: String,
    /// `"breaking"`, `"warning"`, or `"info"`.
    pub severity: String,
    /// Externally-visible target name of the affected model
    /// (`catalog.schema.table`).
    pub model: String,
    /// Affected column, for column-scoped changes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
    /// Human-readable description of the change.
    pub message: String,
}

/// `breaking_change` result — the semantic delta between the working-tree
/// models and the models at a base git ref.
///
/// When the gate could not run (the base ref or HEAD failed to compile, or the
/// models directory is missing — typically because the project is not a git
/// repo), `skipped_reason` is set, `has_breaking` is `false`, `breaking_count`
/// is `0`, and `findings` is empty. A clean diff and a skipped gate are
/// therefore distinguishable: check `skipped_reason`.
#[derive(Debug, Default, Serialize, JsonSchema)]
pub struct BreakingChangeResult {
    /// `true` when at least one finding is `breaking`-severity.
    pub has_breaking: bool,
    /// Count of `breaking`-severity findings.
    pub breaking_count: usize,
    /// All classified findings (breaking + warning + info).
    pub findings: Vec<BreakingFindingLite>,
    /// Why the breaking-change gate was skipped, when it could not run.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skipped_reason: Option<String>,
}

/// One downstream consumer of a model in a `dependents` result.
#[derive(Debug, Serialize, JsonSchema)]
pub struct DependentEntry {
    /// The downstream model that depends on the focal model.
    pub model: String,
    /// Columns of the focal model that this dependent reads. Empty when the
    /// dependency is model-level only (no column-resolved edges).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub via_columns: Vec<String>,
}

/// `dependents` result — the reverse of `lineage`: downstream models that
/// consume the focal model, with the focal columns each reads.
#[derive(Debug, Serialize, JsonSchema)]
pub struct DependentsResult {
    /// The focal model whose consumers were resolved.
    pub model: String,
    /// Downstream consumers, sorted by model name.
    pub dependents: Vec<DependentEntry>,
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

/// One column on a `catalog` asset.
#[derive(Debug, Serialize, JsonSchema)]
pub struct CatalogColumnLite {
    pub name: String,
    /// Declared or inferred type, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_type: Option<String>,
    /// Whether the column accepts nulls, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nullable: Option<bool>,
}

/// One asset (model or source) in a `catalog` result.
#[derive(Debug, Serialize, JsonSchema)]
pub struct CatalogAssetLite {
    /// Fully-qualified target (`catalog.schema.table`) when resolvable,
    /// otherwise the model name.
    pub fqn: String,
    /// Model / source name as it appears in lineage edges.
    pub model_name: String,
    /// `"model"`, `"source"`, `"view"`, or `"materialized_view"`.
    pub kind: String,
    pub columns: Vec<CatalogColumnLite>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub upstream_models: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub downstream_models: Vec<String>,
    /// Natural-language description from the model's sidecar, when set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub intent: Option<String>,
}

/// `catalog` result — the project-wide asset inventory in one call: every
/// model and source with its typed columns and the upstream/downstream model
/// lists. Column-level edges are intentionally dropped (token-heavy); use
/// `lineage` for the column-level trace of one model and `inspect_schema` for
/// typed columns alone.
#[derive(Debug, Serialize, JsonSchema)]
pub struct CatalogResult {
    /// Pipeline the catalog was built for (first in declaration order).
    pub project_name: String,
    pub assets: Vec<CatalogAssetLite>,
    pub asset_count: usize,
    pub column_count: usize,
    pub edge_count: usize,
}

/// One project run in a model-unscoped `history` result.
#[derive(Debug, Serialize, JsonSchema)]
pub struct RunHistoryLite {
    pub run_id: String,
    /// RFC 3339 start timestamp.
    pub started_at: String,
    /// `"Success"`, `"Failed"`, `"Partial"`, etc.
    pub status: String,
    /// `"Manual"`, `"Scheduled"`, etc.
    pub trigger: String,
    /// Number of models executed in the run.
    pub models_executed: usize,
    pub duration_ms: u64,
}

/// One model execution in a model-scoped `history` result.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ModelExecutionLite {
    /// RFC 3339 start timestamp.
    pub started_at: String,
    pub duration_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_affected: Option<u64>,
    pub status: String,
    /// Hash of the executed SQL — distinguishes a re-run from a changed model.
    pub sql_hash: String,
}

/// `history` result — recent runs from the state store. When `model` is set the
/// query is model-scoped and `executions` is populated; otherwise `runs` holds
/// the project-level run summary. Both empty means no recorded history.
#[derive(Debug, Default, Serialize, JsonSchema)]
pub struct HistoryResult {
    /// Set when the query was scoped to a single model.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Recent project runs (model-unscoped query).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub runs: Vec<RunHistoryLite>,
    /// Per-model executions, newest first (model-scoped query).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub executions: Vec<ModelExecutionLite>,
}

/// One column's null rate in a `metrics` snapshot.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ColumnNullRateLite {
    pub column: String,
    /// Fraction of rows that were null (0.0–1.0).
    pub null_rate: f64,
}

/// One quality snapshot in a `metrics` result.
#[derive(Debug, Serialize, JsonSchema)]
pub struct MetricsSnapshotLite {
    pub run_id: String,
    /// RFC 3339 snapshot timestamp.
    pub timestamp: String,
    pub row_count: u64,
    /// Seconds since the data was last fresh, when tracked.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub freshness_lag_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub null_rates: Vec<ColumnNullRateLite>,
}

/// One quality alert in a `metrics` result.
#[derive(Debug, Serialize, JsonSchema)]
pub struct MetricsAlertLite {
    /// `"freshness"` or `"null_rate"`.
    pub kind: String,
    /// `"warning"` or `"critical"`.
    pub severity: String,
    pub message: String,
    /// Affected column, for column-scoped alerts.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
}

/// `metrics` result — quality snapshots (row count, freshness, per-column null
/// rates) plus derived alerts for a model. `message` is set instead of
/// snapshots when the model has no recorded quality metrics yet.
#[derive(Debug, Default, Serialize, JsonSchema)]
pub struct MetricsResult {
    pub model: String,
    pub snapshots: Vec<MetricsSnapshotLite>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub alerts: Vec<MetricsAlertLite>,
    /// Why there are no snapshots, when the model has no metrics yet.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// One materialization-strategy recommendation in an `optimize` result.
#[derive(Debug, Serialize, JsonSchema)]
pub struct OptimizeRecommendationLite {
    pub model_name: String,
    /// Current strategy assumed by the cost model.
    pub current_strategy: String,
    /// Strategy the cost model recommends.
    pub recommended_strategy: String,
    /// Projected monthly savings (USD) from switching.
    pub estimated_monthly_savings: f64,
    /// Why the strategy is recommended.
    pub reasoning: String,
    /// How many models depend on this one (weights the recommendation).
    pub downstream_references: u64,
}

/// `optimize` result — cost-model-driven materialization recommendations,
/// derived from run history + the on-disk DAG. `message` is set instead of
/// recommendations when there is no run history to analyse.
#[derive(Debug, Default, Serialize, JsonSchema)]
pub struct OptimizeResult {
    pub recommendations: Vec<OptimizeRecommendationLite>,
    /// Why there are no recommendations, when run history is absent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// `suggest_freshness_block` result — an LLM-drafted `[freshness]` TOML block
/// for a model with temporal columns (the W005 fix).
///
/// `freshness_block` is the ready-to-paste TOML when drafting succeeds;
/// `message` explains why no block was produced (the API key is unset, or the
/// model returned no fenced block) so the caller can distinguish a real draft
/// from a graceful no-op rather than seeing an error.
#[derive(Debug, Default, Serialize, JsonSchema)]
pub struct SuggestFreshnessBlockResult {
    /// The drafted `[freshness]` TOML block, when one was produced.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub freshness_block: Option<String>,
    /// Why no block was produced (key unset / no fenced block), when
    /// `freshness_block` is `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// `draft_contract` result — an LLM-drafted `.contract.toml` grounded in the
/// observed profile of a model's target table, compile-verified against the
/// model's inferred schema before it is returned.
///
/// `contract_toml` is the ready-to-save contract when drafting succeeds; it is
/// a DRAFT — the caller writes it next to the model and runs `compile` to
/// enforce it. `message` explains why no contract was produced (the API key is
/// unset, the target isn't materialized, or the warehouse isn't reachable) so a
/// graceful no-op is distinguishable from an error.
#[derive(Debug, Default, Serialize, JsonSchema)]
pub struct DraftContractResult {
    /// The model the contract was drafted for.
    pub model: String,
    /// The drafted `.contract.toml`, when one was produced.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contract_toml: Option<String>,
    /// Number of LLM attempts the compile-verify loop took, when it ran.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempts: Option<usize>,
    /// Why no contract was produced (key unset / target unavailable), when
    /// `contract_toml` is `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// One generated test assertion in a `generate_tests` result.
#[derive(Debug, Serialize, JsonSchema)]
pub struct TestAssertionLite {
    /// Short assertion name (also the suggested test filename stem).
    pub name: String,
    /// The assertion SQL — returns 0 rows when the invariant holds.
    pub sql: String,
    /// What the assertion checks, in plain language.
    pub description: String,
}

/// `generate_tests` result — LLM-drafted test assertions for a model, derived
/// from its intent + schema + source code.
///
/// `assertions` are DRAFTS the caller writes into the project's `tests/`
/// directory (each becomes a `<model>_<name>.sql` file) and runs via the `test`
/// tool. `message` explains why no assertions were produced (the API key is
/// unset, or the model wasn't found) so a no-op is distinguishable from an
/// error.
#[derive(Debug, Default, Serialize, JsonSchema)]
pub struct GenerateTestsResult {
    /// The model the tests were drafted for.
    pub model: String,
    /// The drafted assertions, when any were produced.
    pub assertions: Vec<TestAssertionLite>,
    /// Why no assertions were produced (key unset / model missing), when
    /// `assertions` is empty.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// `explain_model` result — an LLM-drafted intent description for a model,
/// derived from its source code, schema, and upstream dependencies.
///
/// `intent` is the drafted 2-3 sentence description when drafting succeeds; the
/// caller can save it to the model's sidecar as `intent = "..."`. `message`
/// explains why no description was produced (the API key is unset, or the model
/// wasn't found) so a no-op is distinguishable from an error.
#[derive(Debug, Default, Serialize, JsonSchema)]
pub struct ExplainModelResult {
    /// The model the description was drafted for.
    pub model: String,
    /// The drafted intent description, when one was produced.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub intent: Option<String>,
    /// Why no description was produced (key unset / model missing), when
    /// `intent` is `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

// ---------------------------------------------------------------------------
// Governance + drift preview (read-only, dry-run).
// ---------------------------------------------------------------------------

/// One classification-tag application in a `governance_preview` result —
/// projected from `rocky_cli::output::ClassificationAction`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ClassificationActionLite {
    /// Model the tag would be applied to.
    pub model: String,
    /// Column the tag would be applied to.
    pub column: String,
    /// Classification tag (e.g. `"pii"`, `"confidential"`).
    pub tag: String,
}

/// One masking-policy application in a `governance_preview` result —
/// projected from `rocky_cli::output::MaskAction`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct MaskActionLite {
    /// Model the mask would be applied to.
    pub model: String,
    /// Column the mask would be applied to.
    pub column: String,
    /// Classification tag the mask resolves against.
    pub tag: String,
    /// Resolved strategy wire name (`"hash"`, `"redact"`, `"partial"`,
    /// `"none"`) for the active environment.
    pub resolved_strategy: String,
}

/// One retention-policy application in a `governance_preview` result —
/// projected from `rocky_cli::output::RetentionAction`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct RetentionActionLite {
    /// Model the retention policy would be applied to.
    pub model: String,
    /// Retention duration in days, parsed from the sidecar (`"90d"` → 90,
    /// `"1y"` → 365).
    pub duration_days: u32,
    /// Warehouse-native preview of the policy SQL / TBLPROPERTIES on the
    /// active adapter, when the adapter has a first-class retention knob.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warehouse_preview: Option<String>,
}

/// `governance_preview` result — the pre-apply enforcement picture a
/// subsequent `rocky run [--env <name>]` would reconcile: classification
/// tags, masking policies, and retention policies declared across the
/// project's model sidecars.
///
/// This is a DRY-RUN preview built offline (compile + sidecar read); it
/// performs no warehouse I/O and applies nothing. Empty action lists mean
/// the project declares no governance for that surface. There is no
/// grants diff here — pre-apply governance covers classification / masking
/// / retention only; grant reconciliation surfaces at `rocky run` time.
#[derive(Debug, Default, Serialize, JsonSchema)]
pub struct GovernancePreviewResult {
    /// The active environment the preview resolved masks against, when an
    /// `env` was supplied (`[mask.<env>]` overrides on top of `[mask]`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env: Option<String>,
    /// Classification tags the reconciler would apply.
    pub classification_actions: Vec<ClassificationActionLite>,
    /// Masking policies the reconciler would apply (only tags that resolve
    /// to a strategy for the active environment).
    pub mask_actions: Vec<MaskActionLite>,
    /// Retention policies the reconciler would apply.
    pub retention_actions: Vec<RetentionActionLite>,
}

/// One drifted column in a `drift_preview` result — a column present in
/// both tables whose warehouse-reported type differs between source and
/// target.
#[derive(Debug, Serialize, JsonSchema)]
pub struct DriftedColumnLite {
    /// Column name.
    pub name: String,
    /// The source table's reported type for this column.
    pub source_type: String,
    /// The target table's reported type for this column.
    pub target_type: String,
}

/// `drift_preview` result — source-vs-target schema drift between two
/// warehouse tables, computed via [`rocky_core::drift::detect_drift`] over
/// the columns each table reports through `DESCRIBE`.
///
/// Read-only: it `DESCRIBE`s both tables and compares them, applying
/// nothing. This is the same apples-to-apples comparison `rocky run`
/// performs before an incremental load (both sides are warehouse-reported
/// type strings, so there is no inferred-vs-observed type mismatch).
/// `target_exists` is `false` when the target has not been materialized yet
/// (the first `rocky run` would create it via CTAS) — the drift lists are
/// then empty. `added_columns` are present in the source but missing from
/// the target (a `rocky run` would `ALTER TABLE ADD COLUMN`);
/// `drifted_columns` carry a type change; `action` is the wire name of the
/// action the runtime would take (`ignore` / `add_columns` /
/// `alter_column_types` / `drop_and_recreate`). `add_columns` covers the
/// added-columns-only case: [`detect_drift`](rocky_core::drift::detect_drift)
/// returns [`DriftAction::Ignore`](rocky_ir::DriftAction::Ignore) there
/// (no type change), but a `rocky run` still issues `ALTER TABLE ADD COLUMN`,
/// so the preview reports `add_columns` to match.
#[derive(Debug, Default, Serialize, JsonSchema)]
pub struct DriftPreviewResult {
    /// The source table the drift was checked against.
    pub source_table: String,
    /// The target table the drift was checked against.
    pub target_table: String,
    /// Whether the target table exists in the warehouse. When `false`, the
    /// target has not been materialized yet and the drift lists are empty.
    pub target_exists: bool,
    /// Columns present in both tables whose types differ.
    pub drifted_columns: Vec<DriftedColumnLite>,
    /// Columns present in the source but missing from the target table.
    pub added_columns: Vec<String>,
    /// Wire name of the action the runtime would take: `"ignore"`,
    /// `"add_columns"`, `"alter_column_types"`, or `"drop_and_recreate"`.
    pub action: String,
}
