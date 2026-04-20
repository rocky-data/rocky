use std::collections::{BTreeMap, HashMap};
use std::hash::{DefaultHasher, Hasher};

use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use rocky_compiler::compile::PhaseTimings;
use rocky_compiler::diagnostic::Diagnostic;
use rocky_observe::metrics::MetricsSnapshot;
use schemars::JsonSchema;
use serde::Serialize;

use rocky_core::checks::CheckResult;

/// Compute a stable 16-char hex fingerprint for a sequence of SQL statements.
///
/// Used by [`MaterializationMetadata::sql_hash`] so downstream orchestrators
/// (Dagster) can detect "what changed?" between runs without diffing full
/// SQL bodies. The hash is computed over the joined statements separated by
/// `;\n` so reordering or whitespace changes inside a single statement still
/// fingerprint differently.
///
/// Uses [`std::hash::DefaultHasher`] (currently SipHash-1-3) which is
/// stable within a single Rust release. Cross-release stability is not
/// guaranteed — fingerprints should not be persisted across Rocky CLI
/// upgrades. For run-to-run "did this change since last run?" comparisons
/// inside a single deployment, it's a good fit.
///
/// Implementation note (§P4.3): statements are fed into the hasher
/// incrementally instead of via `statements.join(";\n")` — the concatenated
/// string used to allocate an O(total_chars) intermediate buffer per call.
/// The byte sequence fed to the hasher is preserved (each statement's bytes
/// followed by `;\n` between adjacent entries, with str's `0xff`
/// end-marker) so the emitted fingerprint is bit-for-bit identical to the
/// join-then-hash version — existing persisted `sql_hash` values in state
/// stores stay comparable.
pub fn sql_fingerprint(statements: &[String]) -> String {
    let mut hasher = DefaultHasher::new();
    for (i, stmt) in statements.iter().enumerate() {
        if i > 0 {
            hasher.write(b";\n");
        }
        hasher.write(stmt.as_bytes());
    }
    // Match `str::hash`'s terminator byte so the stream matches
    // `statements.join(";\n").hash(&mut hasher)` exactly.
    hasher.write_u8(0xff);
    format!("{:016x}", hasher.finish())
}

/// JSON output for `rocky discover`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct DiscoverOutput {
    pub version: String,
    pub command: String,
    pub sources: Vec<SourceOutput>,
    /// Pipeline-level data quality check configuration. Present when the
    /// pipeline declares a `[checks]` block in `rocky.toml`. Downstream
    /// orchestrators (e.g. Dagster) consume this to attach asset-level
    /// freshness policies and check expectations without re-reading
    /// `rocky.toml` themselves.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checks: Option<ChecksConfigOutput>,
    /// Tables filtered out of `sources` because they were reported by the
    /// discovery adapter but do not exist in the source warehouse. Same
    /// shape as `RunOutput.excluded_tables` so consumers can use one
    /// parser. Empty when nothing was filtered.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub excluded_tables: Vec<ExcludedTableOutput>,
}

/// Pipeline-level checks configuration projected into the discover output.
///
/// This is intentionally a thin projection of `rocky_core::config::ChecksConfig`
/// — only the fields downstream orchestrators currently consume are exposed.
/// Add fields as new integrations need them.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ChecksConfigOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub freshness: Option<FreshnessConfigOutput>,
}

/// Freshness check configuration projected into the discover output.
///
/// Per-schema `overrides` from `rocky_core::config::FreshnessConfig` are
/// intentionally not exposed yet — the override-key semantics need to be
/// nailed down before integrations can rely on them.
#[derive(Debug, Serialize, JsonSchema)]
pub struct FreshnessConfigOutput {
    pub threshold_seconds: u64,
}

/// A discovered source (connector, schema, integration — terminology varies by adapter).
#[derive(Debug, Serialize, JsonSchema)]
pub struct SourceOutput {
    pub id: String,
    /// Schema pattern components parsed from the source schema name.
    /// Keys are the component names from the schema pattern config.
    /// e.g., {"tenant": "acme", "regions": ["us_west"], "source": "shopify"}
    pub components: IndexMap<String, serde_json::Value>,
    pub source_type: String,
    pub last_sync_at: Option<DateTime<Utc>>,
    pub tables: Vec<TableOutput>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct TableOutput {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_count: Option<u64>,
}

/// JSON output for `rocky run`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct RunOutput {
    pub version: String,
    pub command: String,
    /// Pipeline type that was executed (e.g., "replication").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pipeline_type: Option<String>,
    pub filter: String,
    pub duration_ms: u64,
    pub tables_copied: usize,
    pub tables_failed: usize,
    #[serde(skip_serializing_if = "is_zero")]
    pub tables_skipped: usize,
    /// Tables that the discovery adapter reported as enabled but that do not
    /// exist in the source warehouse (e.g. Fivetran has them configured but
    /// hasn't synced them, or they carry the `do_not_alter__` broken-table
    /// marker). Surfaced so orchestrators can flag the gap in their UIs
    /// instead of silently dropping the assets.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub excluded_tables: Vec<ExcludedTableOutput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resumed_from: Option<String>,
    /// True when running in shadow mode (targets rewritten).
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub shadow: bool,
    pub materializations: Vec<MaterializationOutput>,
    pub check_results: Vec<TableCheckOutput>,
    /// Row-quarantine outcomes — one entry per table the quality
    /// pipeline quarantined. Empty for runs that did not use
    /// `[pipeline.x.checks.quarantine]`.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub quarantine: Vec<QuarantineOutput>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub anomalies: Vec<AnomalyOutput>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<TableErrorOutput>,
    pub execution: ExecutionSummary,
    pub metrics: Option<MetricsSnapshot>,
    pub permissions: PermissionSummary,
    pub drift: DriftSummary,
    /// Per-model partition execution summaries, present only when the run
    /// touched one or more `time_interval` models. Empty for runs that
    /// didn't execute any partitioned models.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub partition_summaries: Vec<PartitionSummary>,
    /// `true` when the run was cancelled by a SIGINT (Ctrl-C). Surfaced so
    /// orchestrators can distinguish "user interrupted" from "run failed".
    /// Tables that hadn't reached `Success` or `Failed` at interrupt time
    /// are recorded as `TableStatus::Interrupted` in the state store.
    /// Always serialised (even when `false`) so consumers don't have to
    /// treat its absence specially.
    pub interrupted: bool,
    /// Aggregate cost attribution across every materialization in this
    /// run (per-model entries + run totals). `None` for DuckDB-only
    /// pipelines or when no materializations produced a cost number.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost_summary: Option<RunCostSummary>,
    /// Budget breaches detected at end of run. Empty when no
    /// `[budget]` block is configured or all configured limits were
    /// respected. Each breach is also emitted as a `budget_breach`
    /// [`rocky_observe::events::PipelineEvent`] and fires the
    /// `on_budget_breach` hook so subscribers see them live.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub budget_breaches: Vec<BudgetBreachOutput>,
}

/// Per-model cost attribution entry inside [`RunCostSummary`].
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct ModelCostEntry {
    /// Asset key path for the model this entry covers.
    pub asset_key: Vec<String>,
    pub duration_ms: u64,
    /// Observed cost for this materialization. `None` when the adapter
    /// type isn't billed or the formula couldn't be computed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost_usd: Option<f64>,
}

/// Aggregate cost attribution for a `rocky run` invocation.
///
/// The run finalizer walks every [`MaterializationOutput`] in
/// [`RunOutput::materializations`], computes a per-model dollar cost
/// using [`rocky_core::cost::compute_observed_cost_usd`], and stuffs the
/// totals here. Consumers (Dagster, custom dashboards) can then read
/// either the per-model breakdown or the run total without re-deriving
/// the cost model themselves.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct RunCostSummary {
    /// Sum of every per-model `cost_usd` that produced a number.
    /// `None` when no materialization produced a cost.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_cost_usd: Option<f64>,
    /// Wall-clock time summed across every materialization. Separate
    /// from [`RunOutput::duration_ms`] (which includes setup /
    /// governance overhead) so budget enforcement can target
    /// model-compute time specifically.
    pub total_duration_ms: u64,
    /// Per-model cost attribution, ordered as in
    /// [`RunOutput::materializations`].
    pub per_model: Vec<ModelCostEntry>,
    /// Adapter type the cost formula was parameterised against, for
    /// audit. Values mirror `AdapterConfig.type`: "databricks",
    /// "snowflake", "bigquery", "duckdb".
    pub adapter_type: String,
}

/// One budget breach surfaced on [`RunOutput::budget_breaches`].
///
/// Kept as a CLI-side struct (rather than re-using
/// [`rocky_core::config::BudgetBreach`]) so the JSON schema lives
/// alongside the other `rocky run` output types. The fields mirror
/// `BudgetBreach` one-to-one.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct BudgetBreachOutput {
    /// Which limit was breached: `"max_usd"` or `"max_duration_ms"`.
    pub limit_type: String,
    pub limit: f64,
    pub actual: f64,
}

fn is_zero(v: &usize) -> bool {
    *v == 0
}

/// Summary of execution parallelism and throughput.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ExecutionSummary {
    pub concurrency: usize,
    pub tables_processed: usize,
    pub tables_failed: usize,
    /// Whether adaptive concurrency (AIMD throttle) was enabled for this run.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub adaptive_concurrency: bool,
    /// Final concurrency level at end of run (may differ from initial if
    /// adaptive concurrency adjusted it). Only present when adaptive
    /// concurrency is enabled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_concurrency: Option<usize>,
    /// Number of rate-limit signals (HTTP 429 / UC_REQUEST_LIMIT_EXCEEDED)
    /// that triggered concurrency reduction. Only present when adaptive
    /// concurrency is enabled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limits_detected: Option<u64>,
}

/// Row count anomaly detected by historical baseline comparison.
#[derive(Debug, Serialize, JsonSchema)]
pub struct AnomalyOutput {
    pub table: String,
    pub current_count: u64,
    pub baseline_avg: f64,
    pub deviation_pct: f64,
    pub reason: String,
}

/// Error from a table that failed during parallel processing.
#[derive(Debug, Serialize, JsonSchema)]
pub struct TableErrorOutput {
    pub asset_key: Vec<String>,
    pub error: String,
}

/// A table that the discovery adapter reported but that is missing from
/// the source warehouse, so the run skipped it. Tracked separately from
/// `errors` because it is not a runtime failure — the row never made it
/// past the pre-flight existence check.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ExcludedTableOutput {
    /// Dagster-style asset key path (`[source_type, ...components, table]`).
    pub asset_key: Vec<String>,
    /// Source schema the table was expected to live in.
    pub source_schema: String,
    /// Bare table name as reported by the discovery adapter.
    pub table_name: String,
    /// Why the table was excluded. Currently always
    /// `"missing_from_source"` but kept as a free-form field so future
    /// causes (disabled, sync_paused, ...) can be added without a
    /// schema break.
    pub reason: String,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct MaterializationOutput {
    pub asset_key: Vec<String>,
    pub rows_copied: Option<u64>,
    pub duration_ms: u64,
    pub metadata: MaterializationMetadata,
    /// Partition window this materialization targeted, present only when
    /// the model's strategy is `time_interval`. `None` for unpartitioned
    /// strategies (full_refresh, incremental, merge).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition: Option<PartitionInfo>,
    /// Observed dollar cost of this materialization, computed post-hoc
    /// from the adapter-appropriate formula (duration × DBU rate for
    /// Databricks/Snowflake, bytes × $/TB for BigQuery, zero for
    /// DuckDB). `None` when the warehouse type isn't billed or the
    /// adapter didn't report the inputs the formula needs. Populated by
    /// the run finalizer via
    /// `rocky_core::cost::compute_observed_cost_usd`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost_usd: Option<f64>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct MaterializationMetadata {
    pub strategy: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub watermark: Option<DateTime<Utc>>,
    /// Fully-qualified target table identifier in `catalog.schema.table`
    /// format. Useful for click-through links to the warehouse UI from
    /// the Dagster asset detail page. Always set when the materialization
    /// targets a known table.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_table_full_name: Option<String>,
    /// Short hash (16 hex chars) of the generated SQL string. Lets
    /// orchestrators detect "what changed?" between runs without
    /// diffing full SQL bodies. Computed via `xxh3_64` of the
    /// canonical SQL the engine sent to the warehouse.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql_hash: Option<String>,
    /// Number of columns in the materialized table's typed schema.
    /// Populated for derived models (where the compiler resolved a
    /// typed schema); `None` for source-replication tables that
    /// inherit their schema from upstream.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column_count: Option<u64>,
    /// Compile time in milliseconds for the model that produced this
    /// materialization. Populated only for derived models. Mirrors
    /// the relevant slice of `PhaseTimings.total_ms`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compile_time_ms: Option<u64>,
}

/// Partition window information for a single `time_interval`
/// materialization.
///
/// `key` is the canonical Rocky partition key (e.g. `"2026-04-07"` for
/// daily; `"2026-04-07T13"` for hourly). `start` / `end` are the half-open
/// `[start, end)` window the SQL substituted for `@start_date` /
/// `@end_date`. `batched_with` lists any additional partition keys that
/// were merged into this batch when `batch_size > 1` — empty for the
/// default one-partition-per-statement case.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct PartitionInfo {
    pub key: String,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub batched_with: Vec<String>,
}

/// Per-model summary of `time_interval` partition execution.
///
/// One entry per partitioned model touched by the run. Lets dagster-rocky
/// (and other orchestrators) display per-model partition stats without
/// re-counting the per-partition `MaterializationOutput.partition` entries.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct PartitionSummary {
    pub model: String,
    pub partitions_planned: usize,
    pub partitions_succeeded: usize,
    pub partitions_failed: usize,
    /// Partitions that were already `Computed` in the state store and
    /// skipped by the runtime (currently always 0; reserved for the
    /// `--missing` change-detection optimization).
    #[serde(skip_serializing_if = "is_zero")]
    pub partitions_skipped: usize,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct TableCheckOutput {
    pub asset_key: Vec<String>,
    pub checks: Vec<CheckResult>,
}

/// Row-quarantine outcome for a single table processed by the quality
/// pipeline. Emitted when `[pipeline.x.checks.quarantine]` is enabled
/// and the table has at least one error-severity row-level assertion
/// that lowers to a boolean predicate.
///
/// Row counts are reported when the warehouse adapter supplies them.
/// Adapters that cannot count rows written by a `CREATE OR REPLACE
/// TABLE` leave the counts as `None`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct QuarantineOutput {
    /// Dagster-style asset key path
    /// (`[catalog, schema, table]`) of the source table the quarantine
    /// acted on.
    pub asset_key: Vec<String>,
    /// Quarantine mode that was applied. One of `"split"`, `"tag"`,
    /// `"drop"` (matches [`rocky_core::config::QuarantineMode`]).
    pub mode: String,
    /// Fully-qualified `catalog.schema.table` name of the `__valid`
    /// output table. Empty for `mode = "tag"` (source is rewritten in
    /// place).
    #[serde(skip_serializing_if = "String::is_empty")]
    pub valid_table: String,
    /// Fully-qualified name of the `__quarantine` output table. Empty
    /// for `mode = "drop"` (failing rows discarded) and `mode = "tag"`.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub quarantine_table: String,
    /// Number of rows in the `__valid` output, when the adapter can
    /// report it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub valid_rows: Option<u64>,
    /// Number of rows in the `__quarantine` output, when the adapter
    /// can report it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quarantined_rows: Option<u64>,
    /// `true` when every quarantine statement executed successfully.
    /// `false` means a partial failure — inspect `error` for details.
    pub ok: bool,
    /// Error message from the first failing statement, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct PermissionSummary {
    pub grants_added: usize,
    pub grants_revoked: usize,
    pub catalogs_created: usize,
    pub schemas_created: usize,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct DriftSummary {
    pub tables_checked: usize,
    pub tables_drifted: usize,
    pub actions_taken: Vec<DriftActionOutput>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct DriftActionOutput {
    pub table: String,
    pub action: String,
    pub reason: String,
}

/// JSON output for `rocky plan`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct PlanOutput {
    pub version: String,
    pub command: String,
    pub filter: String,
    pub statements: Vec<PlannedStatement>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct PlannedStatement {
    pub purpose: String,
    pub target: String,
    pub sql: String,
}

/// JSON output for `rocky drift`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct DriftOutput {
    pub version: String,
    pub command: String,
    pub drift: DriftSummary,
}

/// JSON output for `rocky compile`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct CompileOutput {
    pub version: String,
    pub command: String,
    pub models: usize,
    pub execution_layers: usize,
    pub diagnostics: Vec<Diagnostic>,
    pub has_errors: bool,
    pub compile_timings: PhaseTimings,
    /// Per-model details extracted from each model's TOML frontmatter.
    /// Empty when the project has no models. Downstream consumers
    /// (`dagster-rocky` to surface per-model freshness policies and
    /// partition-keyed assets) iterate this list rather than re-loading
    /// model frontmatter themselves.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub models_detail: Vec<ModelDetail>,
    /// Expanded SQL for each model after macro substitution. Only
    /// populated when `--expand-macros` is passed. Keys are model names,
    /// values are the SQL after all `@macro()` calls have been replaced.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub expanded_sql: HashMap<String, String>,
}

/// Per-model summary projected from `rocky_core::models::ModelConfig`.
///
/// Intentionally excludes fields that change run-to-run (timings,
/// diagnostics) — those live on the run-level outputs. This is the
/// stable, declarative shape of one compiled model, suitable for
/// orchestrators that build asset definitions ahead of execution.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ModelDetail {
    pub name: String,
    /// Materialization strategy as the wire-shape `StrategyConfig`
    /// (`{"type": "...", ...}`).
    pub strategy: rocky_core::models::StrategyConfig,
    /// Target table coordinates.
    pub target: rocky_core::models::TargetConfig,
    /// Per-model freshness expectation, when declared in the model's
    /// TOML frontmatter. `None` when not configured.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub freshness: Option<rocky_core::models::ModelFreshnessConfig>,
    /// How the contract was discovered: `"auto"` (sibling `.contract.toml`),
    /// `"explicit"` (via `--contracts` flag), or absent when no contract.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contract_source: Option<String>,
    /// When the model uses `full_refresh` and has columns that look monotonic,
    /// this hint suggests switching to incremental materialization. `None`
    /// when the model already uses an incremental strategy or no candidates
    /// were found.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub incrementality_hint: Option<rocky_compiler::incrementality::IncrementalityHint>,
    /// DAG-propagated cost estimate for this model. Populated at compile
    /// time using heuristic cardinality propagation (no warehouse round-trip).
    /// `None` when no upstream table statistics are available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost_hint: Option<CostHint>,
    /// Names of models this model directly depends on. Derived from the
    /// model's TOML `depends_on` list or auto-resolved from SQL table
    /// references. Empty when the model has no upstream dependencies.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<String>,
}

/// Heuristic cost estimate derived from DAG-aware cardinality propagation.
///
/// These numbers are directional — useful for comparing models within a
/// project and surfacing expensive operations in the LSP, but not precise
/// enough to substitute for a warehouse EXPLAIN. Use `rocky estimate` for
/// warehouse-backed estimates.
#[derive(Debug, Serialize, JsonSchema)]
pub struct CostHint {
    /// Estimated number of output rows.
    pub estimated_rows: u64,
    /// Estimated total output bytes.
    pub estimated_bytes: u64,
    /// Estimated compute cost in USD.
    pub estimated_cost_usd: f64,
    /// Confidence level: `"low"`, `"medium"`, or `"high"`.
    pub confidence: String,
}

impl CompileOutput {
    pub fn new(
        models: usize,
        execution_layers: usize,
        diagnostics: Vec<Diagnostic>,
        has_errors: bool,
        compile_timings: PhaseTimings,
    ) -> Self {
        CompileOutput {
            version: VERSION.to_string(),
            command: "compile".to_string(),
            models,
            execution_layers,
            diagnostics,
            has_errors,
            compile_timings,
            models_detail: vec![],
            expanded_sql: HashMap::new(),
        }
    }

    /// Attach per-model details and return self.
    #[must_use]
    pub fn with_models_detail(mut self, details: Vec<ModelDetail>) -> Self {
        self.models_detail = details;
        self
    }

    /// Attach expanded SQL (post-macro-expansion) for each model.
    #[must_use]
    pub fn with_expanded_sql(mut self, expanded: HashMap<String, String>) -> Self {
        self.expanded_sql = expanded;
        self
    }
}

/// JSON output for `rocky test`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct TestOutput {
    pub version: String,
    pub command: String,
    pub total: usize,
    pub passed: usize,
    pub failed: usize,
    pub failures: Vec<TestFailure>,
    /// Results from declarative `[[tests]]` in model sidecars.
    /// Present only when `--declarative` is used.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub declarative: Option<DeclarativeTestSummary>,
}

/// One failed test, mirroring the (name, error) tuple in
/// `rocky_engine::test_runner::TestResult::failures` but with named fields
/// because schemars/JSON Schema can't represent positional tuples cleanly.
#[derive(Debug, Serialize, JsonSchema)]
pub struct TestFailure {
    pub name: String,
    pub error: String,
}

/// Summary of declarative test execution (from `[[tests]]` in model sidecars).
#[derive(Debug, Serialize, JsonSchema)]
pub struct DeclarativeTestSummary {
    pub total: usize,
    pub passed: usize,
    pub failed: usize,
    pub warned: usize,
    pub errored: usize,
    pub results: Vec<DeclarativeTestResult>,
}

/// Result of a single declarative test assertion.
#[derive(Debug, Serialize, JsonSchema)]
pub struct DeclarativeTestResult {
    /// Model name the test belongs to.
    pub model: String,
    /// Fully-qualified target table (catalog.schema.table).
    pub table: String,
    /// Test type (e.g., "not_null", "unique", "row_count_range").
    pub test_type: String,
    /// Column under test, if applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
    /// "pass", "fail", or "error".
    pub status: String,
    /// Severity declared in the sidecar ("error" or "warning").
    pub severity: String,
    /// Human-readable detail (e.g., "3 NULL rows found" or execution error).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// The SQL that was executed.
    pub sql: String,
}

impl TestOutput {
    pub fn new(total: usize, passed: usize, failures: Vec<TestFailure>) -> Self {
        TestOutput {
            version: VERSION.to_string(),
            command: "test".to_string(),
            total,
            passed,
            failed: failures.len(),
            failures,
            declarative: None,
        }
    }
}

/// JSON output for `rocky ci`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct CiOutput {
    pub version: String,
    pub command: String,
    pub compile_ok: bool,
    pub tests_ok: bool,
    pub models_compiled: usize,
    pub tests_passed: usize,
    pub tests_failed: usize,
    pub exit_code: i32,
    pub diagnostics: Vec<Diagnostic>,
    pub failures: Vec<TestFailure>,
}

impl CiOutput {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        compile_ok: bool,
        tests_ok: bool,
        models_compiled: usize,
        tests_passed: usize,
        tests_failed: usize,
        exit_code: i32,
        diagnostics: Vec<Diagnostic>,
        failures: Vec<TestFailure>,
    ) -> Self {
        CiOutput {
            version: VERSION.to_string(),
            command: "ci".to_string(),
            compile_ok,
            tests_ok,
            models_compiled,
            tests_passed,
            tests_failed,
            exit_code,
            diagnostics,
            failures,
        }
    }
}

/// JSON output for `rocky ci-diff`.
///
/// Reports which models changed between two git refs, with optional
/// column-level structural diffs when compilation succeeds on both sides.
#[derive(Debug, Serialize, JsonSchema)]
pub struct CiDiffOutput {
    pub version: String,
    pub command: String,
    /// Git ref used as the comparison base (e.g. `main`).
    pub base_ref: String,
    /// Git ref for the incoming changes (typically `HEAD`).
    pub head_ref: String,
    /// Aggregate counts of changed models.
    pub summary: rocky_core::ci_diff::DiffSummary,
    /// Per-model diff results.
    pub models: Vec<rocky_core::ci_diff::DiffResult>,
    /// Pre-rendered Markdown suitable for posting as a GitHub PR comment.
    pub markdown: String,
}

impl CiDiffOutput {
    pub fn new(
        base_ref: String,
        head_ref: String,
        summary: rocky_core::ci_diff::DiffSummary,
        models: Vec<rocky_core::ci_diff::DiffResult>,
    ) -> Self {
        let markdown = rocky_core::ci_diff::format_diff_markdown(&models);
        CiDiffOutput {
            version: VERSION.to_string(),
            command: "ci-diff".to_string(),
            base_ref,
            head_ref,
            summary,
            models,
            markdown,
        }
    }
}

/// JSON output for `rocky optimize`.
///
/// `recommendations` is empty when no run history exists; `message` is
/// populated in that case to explain why.
#[derive(Debug, Serialize, JsonSchema)]
pub struct OptimizeOutput {
    pub version: String,
    pub command: String,
    pub recommendations: Vec<OptimizeRecommendation>,
    pub total_models_analyzed: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Hint pointing users to `rocky compile --output json` for
    /// inferred incrementality recommendations on `full_refresh` models.
    /// Only populated when the optimize command detects that compile-time
    /// analysis could provide additional optimization opportunities.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub incrementality_note: Option<String>,
}

/// One materialization-strategy recommendation. Mirrors
/// `rocky_core::optimize::MaterializationCost` but lives in the CLI
/// crate so we don't have to derive JsonSchema across the workspace.
#[derive(Debug, Serialize, JsonSchema)]
pub struct OptimizeRecommendation {
    pub model_name: String,
    pub current_strategy: String,
    pub recommended_strategy: String,
    pub estimated_monthly_savings: f64,
    pub reasoning: String,
}

impl OptimizeOutput {
    pub fn new(recommendations: Vec<OptimizeRecommendation>) -> Self {
        let count = recommendations.len();
        OptimizeOutput {
            version: VERSION.to_string(),
            command: "optimize".to_string(),
            recommendations,
            total_models_analyzed: count,
            message: None,
            incrementality_note: Some(
                "Run `rocky compile --output json` for inferred incrementality \
                 hints on full_refresh models"
                    .to_string(),
            ),
        }
    }

    pub fn empty(message: impl Into<String>) -> Self {
        OptimizeOutput {
            version: VERSION.to_string(),
            command: "optimize".to_string(),
            recommendations: vec![],
            total_models_analyzed: 0,
            message: Some(message.into()),
            incrementality_note: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Estimate
// ---------------------------------------------------------------------------

/// JSON output for `rocky estimate`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct EstimateOutput {
    pub version: String,
    pub command: String,
    pub estimates: Vec<ModelEstimate>,
    pub total_models: usize,
    /// Total estimated cost in USD across all models. `None` when no
    /// individual model produced a cost estimate.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_estimated_cost_usd: Option<f64>,
}

/// Cost estimate for a single model.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ModelEstimate {
    pub model_name: String,
    /// Estimated bytes that would be scanned (if available).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_bytes_scanned: Option<u64>,
    /// Estimated number of rows (if available).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_rows: Option<u64>,
    /// Estimated compute cost in USD (derived from warehouse pricing model).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_cost_usd: Option<f64>,
    /// Raw EXPLAIN output from the warehouse.
    pub raw_explain: String,
}

impl EstimateOutput {
    pub fn new(estimates: Vec<ModelEstimate>) -> Self {
        let count = estimates.len();
        let total_cost: f64 = estimates.iter().filter_map(|e| e.estimated_cost_usd).sum();
        let total_cost = if total_cost > 0.0 {
            Some(total_cost)
        } else {
            None
        };
        EstimateOutput {
            version: VERSION.to_string(),
            command: "estimate".to_string(),
            estimates,
            total_models: count,
            total_estimated_cost_usd: total_cost,
        }
    }
}

/// JSON output for `rocky history` (all runs view).
///
/// When invoked with `--model <name>`, the dispatch returns
/// `ModelHistoryOutput` instead. The two shapes share the version/command
/// header but differ in their primary collection field, which is why they
/// are separate types rather than one enum.
#[derive(Debug, Serialize, JsonSchema)]
pub struct HistoryOutput {
    pub version: String,
    pub command: String,
    pub runs: Vec<RunHistoryRecord>,
    pub count: usize,
}

/// JSON output for `rocky history --model <name>`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ModelHistoryOutput {
    pub version: String,
    pub command: String,
    pub model: String,
    pub executions: Vec<ModelExecutionRecord>,
    pub count: usize,
}

/// One run from the state store, mirroring `rocky_core::state::RunRecord`
/// with serializable field types (no enums or non-JSON-friendly Rust types).
#[derive(Debug, Serialize, JsonSchema)]
pub struct RunHistoryRecord {
    pub run_id: String,
    pub started_at: DateTime<Utc>,
    pub status: String,
    pub trigger: String,
    pub models_executed: usize,
    /// Duration in milliseconds (`finished_at - started_at`).
    pub duration_ms: u64,
    /// Per-model execution details for this run.
    pub models: Vec<RunModelRecord>,
}

/// Per-model execution record embedded in [`RunHistoryRecord`].
#[derive(Debug, Serialize, JsonSchema)]
pub struct RunModelRecord {
    pub model_name: String,
    pub duration_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_affected: Option<u64>,
    pub status: String,
}

/// One model execution from the state store, mirroring
/// `rocky_core::state::ModelExecution`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ModelExecutionRecord {
    pub started_at: DateTime<Utc>,
    pub duration_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_affected: Option<u64>,
    pub status: String,
    pub sql_hash: String,
}

/// JSON output for `rocky metrics`.
///
/// All optional fields are present-or-absent depending on the flags
/// (`--trend`, `--alerts`, `--column`). The empty case (no snapshots
/// available) sets `message` and leaves the collections empty.
#[derive(Debug, Serialize, JsonSchema)]
pub struct MetricsOutput {
    pub version: String,
    pub command: String,
    pub model: String,
    pub snapshots: Vec<MetricsSnapshotEntry>,
    pub count: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub alerts: Vec<MetricsAlert>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub column_trend: Vec<ColumnTrendPoint>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct MetricsSnapshotEntry {
    pub run_id: String,
    pub timestamp: DateTime<Utc>,
    pub row_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub freshness_lag_seconds: Option<u64>,
    pub null_rates: IndexMap<String, f64>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct MetricsAlert {
    #[serde(rename = "type")]
    pub kind: String,
    pub severity: String,
    pub message: String,
    pub run_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ColumnTrendPoint {
    pub run_id: String,
    pub timestamp: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub null_rate: Option<f64>,
    pub row_count: u64,
}

/// JSON output for `rocky ai "<intent>"`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct AiGenerateOutput {
    pub version: String,
    pub command: String,
    pub intent: String,
    pub format: String,
    pub name: String,
    pub source: String,
    pub attempts: usize,
}

/// JSON output for `rocky ai sync`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct AiSyncOutput {
    pub version: String,
    pub command: String,
    pub proposals: Vec<AiSyncProposal>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct AiSyncProposal {
    pub model: String,
    pub intent: String,
    pub diff: String,
    pub proposed_source: String,
}

/// JSON output for `rocky ai explain`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct AiExplainOutput {
    pub version: String,
    pub command: String,
    pub explanations: Vec<AiExplanation>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct AiExplanation {
    pub model: String,
    pub intent: String,
    pub saved: bool,
}

/// JSON output for `rocky ai test`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct AiTestOutput {
    pub version: String,
    pub command: String,
    pub results: Vec<AiTestModelResult>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct AiTestModelResult {
    pub model: String,
    pub tests: Vec<AiTestAssertion>,
    pub saved: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct AiTestAssertion {
    pub name: String,
    pub description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
}

/// JSON output for `rocky lineage <model>` (model lineage shape).
///
/// When invoked with `--column <col>` (or `model.column` syntax), the
/// dispatch returns `ColumnLineageOutput` instead — the two shapes share
/// version/command/model headers but otherwise differ enough to keep
/// separate.
#[derive(Debug, Serialize, JsonSchema)]
pub struct LineageOutput {
    pub version: String,
    pub command: String,
    pub model: String,
    pub columns: Vec<LineageColumnDef>,
    pub upstream: Vec<String>,
    pub downstream: Vec<String>,
    pub edges: Vec<LineageEdgeRecord>,
}

/// JSON output for `rocky lineage <model> --column <col>`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ColumnLineageOutput {
    pub version: String,
    pub command: String,
    pub model: String,
    pub column: String,
    /// Direction of the trace walk: `"upstream"` (producers) or `"downstream"`
    /// (consumers). Defaults to upstream when `--column` is set without
    /// direction flags, matching pre-Arc-1 behaviour.
    pub direction: String,
    pub trace: Vec<LineageEdgeRecord>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct LineageColumnDef {
    pub name: String,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct LineageEdgeRecord {
    pub source: LineageQualifiedColumn,
    pub target: LineageQualifiedColumn,
    /// Transform kind: "direct", "cast", "expression", etc. Stringified
    /// from `rocky_sql::lineage::TransformKind` to avoid pulling schemars
    /// into rocky-sql.
    pub transform: String,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct LineageQualifiedColumn {
    pub model: String,
    pub column: String,
}

/// JSON output for `rocky state show`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct StateOutput {
    pub version: String,
    pub command: String,
    pub watermarks: Vec<WatermarkEntry>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct WatermarkEntry {
    pub table: String,
    pub last_value: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// JSON output for `rocky list pipelines`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ListPipelinesOutput {
    pub version: String,
    pub command: String,
    pub pipelines: Vec<ListPipelineEntry>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ListPipelineEntry {
    pub name: String,
    pub pipeline_type: String,
    pub target_adapter: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_adapter: Option<String>,
    pub depends_on: Vec<String>,
    pub concurrency: String,
}

impl ListPipelinesOutput {
    pub fn new(pipelines: Vec<ListPipelineEntry>) -> Self {
        Self {
            version: VERSION.to_string(),
            command: "list_pipelines".to_string(),
            pipelines,
        }
    }
}

/// JSON output for `rocky list adapters`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ListAdaptersOutput {
    pub version: String,
    pub command: String,
    pub adapters: Vec<ListAdapterEntry>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ListAdapterEntry {
    pub name: String,
    pub adapter_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
}

impl ListAdaptersOutput {
    pub fn new(adapters: Vec<ListAdapterEntry>) -> Self {
        Self {
            version: VERSION.to_string(),
            command: "list_adapters".to_string(),
            adapters,
        }
    }
}

/// JSON output for `rocky list models`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ListModelsOutput {
    pub version: String,
    pub command: String,
    pub models: Vec<ListModelEntry>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ListModelEntry {
    pub name: String,
    pub target: String,
    pub strategy: String,
    pub depends_on: Vec<String>,
    pub has_contract: bool,
}

impl ListModelsOutput {
    pub fn new(models: Vec<ListModelEntry>) -> Self {
        Self {
            version: VERSION.to_string(),
            command: "list_models".to_string(),
            models,
        }
    }
}

/// JSON output for `rocky list sources`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ListSourcesOutput {
    pub version: String,
    pub command: String,
    pub sources: Vec<ListSourceEntry>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ListSourceEntry {
    pub pipeline: String,
    pub adapter: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_prefix: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub discovery_adapter: Option<String>,
    pub components: Vec<String>,
}

impl ListSourcesOutput {
    pub fn new(sources: Vec<ListSourceEntry>) -> Self {
        Self {
            version: VERSION.to_string(),
            command: "list_sources".to_string(),
            sources,
        }
    }
}

/// JSON output for `rocky compare`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct CompareOutput {
    pub version: String,
    pub command: String,
    pub filter: String,
    pub tables_compared: usize,
    pub tables_passed: usize,
    pub tables_warned: usize,
    pub tables_failed: usize,
    pub results: Vec<TableCompareResult>,
    pub overall_verdict: String,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct TableCompareResult {
    pub production_table: String,
    pub shadow_table: String,
    pub row_count_match: bool,
    pub production_count: u64,
    pub shadow_count: u64,
    pub row_count_diff_pct: f64,
    pub schema_match: bool,
    pub schema_diffs: Vec<String>,
    pub verdict: String,
}

/// JSON output for `rocky compact`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct CompactOutput {
    pub version: String,
    pub command: String,
    pub model: String,
    pub dry_run: bool,
    pub target_size_mb: u64,
    pub statements: Vec<NamedStatement>,
}

/// Named SQL statement (purpose + sql), reused by compact and archive.
#[derive(Debug, Serialize, JsonSchema)]
pub struct NamedStatement {
    pub purpose: String,
    pub sql: String,
}

/// JSON output for `rocky archive`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ArchiveOutput {
    pub version: String,
    pub command: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub older_than: String,
    pub older_than_days: u64,
    pub dry_run: bool,
    pub statements: Vec<NamedStatement>,
}

/// JSON output for `rocky compact --measure-dedup` (Layer 0 storage experiment).
///
/// Measures cross-table partition dedup across all Rocky-managed tables in
/// a project. The `raw` and `semantic` numbers are both **partition-level
/// lower bounds** on byte-level dedup — two partitions that share 99% of
/// their row groups but differ on a single row will appear fully distinct
/// here. The optional `calibration` block (populated by `--calibrate-bytes`)
/// hashes actual row-group bytes on a sampled subset of tables to give a
/// sharper, more expensive second number.
///
/// The measurement is **engine-scoped**: warehouse-native `HASH()` is not
/// portable across engines (Databricks ≠ Snowflake ≠ DuckDB), so the
/// `engine` field must be carried with any published result.
#[derive(Debug, Serialize, JsonSchema)]
pub struct CompactDedupOutput {
    pub version: String,
    pub command: String,
    /// Warehouse engine identifier (`"databricks"`, `"snowflake"`, `"duckdb"`).
    /// Measurements are not comparable across engines because `HASH()` is
    /// not portable.
    pub engine: String,
    /// Project name from `rocky.toml`.
    pub project: String,
    /// Whether the measurement was scoped to Rocky-managed tables
    /// (`"managed"`) or included all warehouse tables (`"all"`).
    pub scope: String,
    pub tables_scanned: usize,
    pub partitions_scanned: usize,
    /// Dedup computed over all columns (what a byte-level chunk store
    /// would actually deduplicate).
    pub raw: DedupSummary,
    /// Dedup computed after excluding Rocky-owned metadata columns. The
    /// gap between `raw` and `semantic` is "dedup we'd unlock if we
    /// ignored Rocky's own per-row metadata."
    pub semantic: DedupSummary,
    /// Columns excluded from the `semantic` hash.
    pub excluded_columns: Vec<String>,
    /// Top-N table pairs by shared partition count (from the `semantic`
    /// measurement).
    pub top_dedup_pairs: Vec<DedupPair>,
    /// Per-table breakdown showing which tables contribute most to the
    /// duplicate set.
    pub per_table: Vec<TableDedupContribution>,
    /// Byte-level calibration on a sampled subset of tables. Present only
    /// when `--calibrate-bytes` was passed. Produces a sharper estimate
    /// at the cost of pulling raw rows and hashing them locally.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub calibration: Option<ByteCalibration>,
    pub measured_at: DateTime<Utc>,
    pub duration_ms: u64,
}

/// Summary of a single dedup measurement (raw or semantic).
///
/// `dedup_ratio` is a **lower bound** on true byte-level dedup — see the
/// docstring on [`CompactDedupOutput`].
#[derive(Debug, Serialize, JsonSchema)]
pub struct DedupSummary {
    pub total_rows: u64,
    pub unique_partitions: usize,
    pub duplicate_partitions: usize,
    /// 0.0..=1.0. Partition-level lower bound on byte-level dedup.
    pub dedup_ratio: f64,
    /// 0.0..=100.0. Estimated storage savings if duplicate partitions
    /// were collapsed to a single copy.
    pub estimated_savings_pct: f64,
}

/// A pair of tables that share duplicate partitions.
#[derive(Debug, Serialize, JsonSchema)]
pub struct DedupPair {
    pub table_a: String,
    pub table_b: String,
    pub shared_partitions: usize,
    pub shared_rows: u64,
}

/// Per-table contribution to the overall duplicate set.
#[derive(Debug, Serialize, JsonSchema)]
pub struct TableDedupContribution {
    pub table: String,
    pub partitions: usize,
    pub partitions_shared_with_others: usize,
    /// 0.0..=100.0. Share of this table's partitions that are duplicates
    /// of some other table's partitions.
    pub contribution_pct: f64,
}

/// Byte-level calibration block populated by `--calibrate-bytes`.
///
/// Samples a handful of tables (default: 3), pulls their rows via
/// `SELECT *`, hashes each row group with blake3 in-process, and
/// computes byte-level dedup on the sample. The `lower_bound_multiplier`
/// is the ratio `byte_dedup_pct / partition_dedup_pct` — it tells you
/// how much of a floor the cheap partition-level measurement actually is.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ByteCalibration {
    /// Which tables were hashed at byte level.
    pub tables_sampled: Vec<String>,
    /// Partition-level `SUM(HASH())` dedup recomputed on the sample
    /// (so it's directly comparable to `byte_dedup_pct`).
    pub partition_dedup_pct: f64,
    /// Byte-level blake3 dedup on the same sample.
    pub byte_dedup_pct: f64,
    /// `byte_dedup_pct / partition_dedup_pct`. A value >1.0 means the
    /// cheap partition-level measurement is genuinely underestimating.
    pub lower_bound_multiplier: f64,
}

/// JSON output for `rocky profile-storage`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ProfileStorageOutput {
    pub version: String,
    pub command: String,
    pub model: String,
    pub profile_sql: String,
    pub recommendations: Vec<EncodingRecommendationOutput>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct EncodingRecommendationOutput {
    pub column: String,
    pub data_type: String,
    pub estimated_cardinality: String,
    pub recommended_encoding: String,
    pub reasoning: String,
}

/// JSON output for `rocky import-dbt`.
///
/// The `report` field is the per-model migration report from
/// `rocky_compiler::import::report::generate_report`. We hold it as
/// `serde_json::Value` (typed as `any` in JSON Schema) to avoid pulling
/// `JsonSchema` derives all the way through `rocky-compiler::import`. The
/// downstream Pydantic/TS bindings will see it as a free-form object.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ImportDbtOutput {
    pub version: String,
    pub command: String,
    pub import_method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dbt_version: Option<String>,
    pub imported: usize,
    pub warnings: usize,
    pub failed: usize,
    pub sources_found: usize,
    pub sources_mapped: usize,
    pub tests_found: usize,
    pub tests_converted: usize,
    pub tests_converted_custom: usize,
    pub tests_skipped: usize,
    pub macros_detected: usize,
    pub imported_models: Vec<String>,
    pub warning_details: Vec<ImportDbtWarning>,
    pub failed_details: Vec<ImportDbtFailure>,
    /// Free-form per-model migration report payload.
    pub report: serde_json::Value,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ImportDbtWarning {
    pub model: String,
    pub category: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggestion: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ImportDbtFailure {
    pub name: String,
    pub reason: String,
}

/// JSON output for `rocky hooks list`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct HooksListOutput {
    pub hooks: Vec<HookEntry>,
    pub total: usize,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct HookEntry {
    pub event: String,
    pub command: String,
    pub timeout_ms: u64,
    pub on_failure: String,
    pub env_keys: Vec<String>,
}

/// JSON output for `rocky validate-migration`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ValidateMigrationOutput {
    pub version: String,
    pub command: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dbt_version: Option<String>,
    pub models_imported: usize,
    pub models_failed: usize,
    pub total_tests: usize,
    pub total_contracts: usize,
    pub total_warnings: usize,
    pub validations: Vec<ModelValidationOutput>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ModelValidationOutput {
    pub model: String,
    pub present_in_dbt: bool,
    pub present_in_rocky: bool,
    pub compile_ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dbt_description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rocky_intent: Option<String>,
    pub test_count: usize,
    pub contracts_generated: usize,
    pub warnings: Vec<String>,
}

/// JSON output for `rocky test-adapter`.
///
/// Mirrors `rocky_adapter_sdk::conformance::ConformanceResult` with the
/// status/category enums stringified, so we don't have to add `JsonSchema`
/// to the SDK crate.
#[derive(Debug, Serialize, JsonSchema)]
pub struct TestAdapterOutput {
    pub adapter: String,
    pub sdk_version: String,
    pub tests_run: usize,
    pub tests_passed: usize,
    pub tests_failed: usize,
    pub tests_skipped: usize,
    pub results: Vec<TestAdapterTestResult>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct TestAdapterTestResult {
    pub name: String,
    /// Stringified `TestCategory`: "connection", "ddl", "dml", "query",
    /// "types", "dialect", "governance", "discovery", "batch_checks".
    pub category: String,
    /// Stringified `TestStatus`: "passed", "failed", "skipped".
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    pub duration_ms: u64,
}

/// JSON output for `rocky hooks test <event>`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct HooksTestOutput {
    pub event: String,
    /// One of "no_hooks", "continue", "abort".
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Free-form Debug rendering of the hook result, when applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,
}

// ---------------------------------------------------------------------------
// rocky validate
// ---------------------------------------------------------------------------

/// JSON output for `rocky validate`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ValidateOutput {
    pub version: String,
    pub command: String,
    pub valid: bool,
    pub adapters: Vec<ValidateAdapterStatus>,
    pub pipelines: Vec<ValidatePipelineStatus>,
    pub models: ValidateModelsStatus,
    pub messages: Vec<ValidateMessage>,
}

/// Status of a single adapter in the validate output.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ValidateAdapterStatus {
    pub name: String,
    pub adapter_type: String,
    pub ok: bool,
}

/// Status of a single pipeline in the validate output.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ValidatePipelineStatus {
    pub name: String,
    pub pipeline_type: String,
    pub strategy: String,
    pub catalog_template: String,
    pub schema_template: String,
    pub ok: bool,
}

/// Status of the models directory in the validate output.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ValidateModelsStatus {
    pub found: bool,
    pub count: usize,
    pub dag_valid: bool,
}

/// A structured diagnostic message from `rocky validate`.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct ValidateMessage {
    /// One of "ok", "warn", "error", "lint".
    pub severity: String,
    /// Machine-readable code (e.g. "V001", "L001").
    pub code: String,
    pub message: String,
    /// File path associated with this message, if applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    /// Config field path associated with this message, if applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
}

impl Default for ValidateOutput {
    fn default() -> Self {
        Self::new()
    }
}

impl ValidateOutput {
    pub fn new() -> Self {
        ValidateOutput {
            version: VERSION.to_string(),
            command: "validate".to_string(),
            valid: true,
            adapters: vec![],
            pipelines: vec![],
            models: ValidateModelsStatus {
                found: false,
                count: 0,
                dag_valid: false,
            },
            messages: vec![],
        }
    }

    /// Push a message and downgrade `valid` to false if severity is "error".
    pub fn push(&mut self, msg: ValidateMessage) {
        if msg.severity == "error" {
            self.valid = false;
        }
        self.messages.push(msg);
    }
}

/// JSON output for `rocky seed`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct SeedOutput {
    pub version: String,
    pub command: String,
    pub seeds_dir: String,
    pub tables_loaded: usize,
    pub tables_failed: usize,
    pub tables: Vec<SeedTableOutput>,
    pub duration_ms: u64,
}

/// A single seed table result within `SeedOutput`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct SeedTableOutput {
    pub name: String,
    pub target: String,
    pub rows: usize,
    pub columns: usize,
    pub duration_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

// --- Load output ---

/// JSON output for `rocky load`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct LoadOutput {
    pub version: String,
    pub command: String,
    pub source_dir: String,
    pub format: String,
    pub files_loaded: usize,
    pub files_failed: usize,
    pub total_rows: u64,
    pub total_bytes: u64,
    pub files: Vec<LoadFileOutput>,
    pub duration_ms: u64,
}

/// A single file result within `LoadOutput`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct LoadFileOutput {
    pub file: String,
    pub target: String,
    pub rows_loaded: u64,
    pub bytes_read: u64,
    pub duration_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

// --- Snapshot output ---

/// Output for `rocky snapshot --output json`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct SnapshotOutput {
    pub version: String,
    pub command: String,
    pub pipeline: String,
    pub source: String,
    pub target: String,
    pub dry_run: bool,
    pub steps_total: usize,
    pub steps_ok: usize,
    pub steps: Vec<SnapshotStepOutput>,
    pub duration_ms: u64,
}

/// A single SQL step within a snapshot run.
#[derive(Debug, Serialize, JsonSchema)]
pub struct SnapshotStepOutput {
    pub step: String,
    pub sql: String,
    pub status: String,
    pub duration_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

// --- Docs output ---

/// Output for `rocky docs --output json`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct DocsOutput {
    pub version: String,
    pub command: String,
    pub output_path: String,
    pub models_count: usize,
    pub pipelines_count: usize,
    pub duration_ms: u64,
}

const VERSION: &str = env!("CARGO_PKG_VERSION");

impl DiscoverOutput {
    pub fn new(sources: Vec<SourceOutput>) -> Self {
        DiscoverOutput {
            version: VERSION.to_string(),
            command: "discover".to_string(),
            sources,
            checks: None,
            excluded_tables: vec![],
        }
    }

    /// Attach the projected pipeline checks config and return self.
    #[must_use]
    pub fn with_checks(mut self, checks: Option<ChecksConfigOutput>) -> Self {
        self.checks = checks;
        self
    }

    /// Attach the list of tables filtered out of `sources` and return self.
    #[must_use]
    pub fn with_excluded_tables(mut self, excluded: Vec<ExcludedTableOutput>) -> Self {
        self.excluded_tables = excluded;
        self
    }
}

impl ChecksConfigOutput {
    /// Project the engine `ChecksConfig` into its CLI output shape.
    /// Returns `None` when nothing freshness-related is configured (the only
    /// field currently surfaced); add more conditions here as fields are added.
    pub fn from_engine(cfg: &rocky_core::config::ChecksConfig) -> Option<Self> {
        let freshness = cfg.freshness.as_ref().map(|f| FreshnessConfigOutput {
            threshold_seconds: f.threshold_seconds,
        });
        freshness.as_ref()?;
        Some(ChecksConfigOutput { freshness })
    }
}

impl RunOutput {
    pub fn new(filter: String, duration_ms: u64, concurrency: usize) -> Self {
        RunOutput {
            version: VERSION.to_string(),
            command: "run".to_string(),
            pipeline_type: Some("replication".to_string()),
            filter,
            duration_ms,
            tables_copied: 0,
            tables_failed: 0,
            tables_skipped: 0,
            excluded_tables: vec![],
            resumed_from: None,
            shadow: false,
            materializations: vec![],
            check_results: vec![],
            quarantine: vec![],
            anomalies: vec![],
            errors: vec![],
            execution: ExecutionSummary {
                concurrency,
                tables_processed: 0,
                tables_failed: 0,
                adaptive_concurrency: false,
                final_concurrency: None,
                rate_limits_detected: None,
            },
            metrics: None,
            permissions: PermissionSummary {
                grants_added: 0,
                grants_revoked: 0,
                catalogs_created: 0,
                schemas_created: 0,
            },
            drift: DriftSummary {
                tables_checked: 0,
                tables_drifted: 0,
                actions_taken: vec![],
            },
            partition_summaries: vec![],
            interrupted: false,
            cost_summary: None,
            budget_breaches: vec![],
        }
    }

    /// Walk every [`MaterializationOutput`] already pushed onto this
    /// run, populate per-model `cost_usd` via the adapter-specific
    /// formula, and aggregate the totals into [`RunCostSummary`].
    ///
    /// Always safe to call — no-op for unbilled adapters (fivetran,
    /// airbyte), zero-cost for DuckDB, etc. Split from
    /// [`RunOutput::check_and_record_budget`] so the interrupt path
    /// can surface per-model cost without firing budget breach events
    /// on partial work.
    pub fn populate_cost_summary(
        &mut self,
        adapter_type: &str,
        cost_cfg: &rocky_core::config::CostSection,
    ) {
        let Some(wh) = rocky_core::cost::WarehouseType::from_adapter_type(adapter_type) else {
            return;
        };

        let dbu_per_hour =
            rocky_core::cost::warehouse_size_to_dbu_per_hour(&cost_cfg.warehouse_size);
        let cost_per_dbu = cost_cfg.compute_cost_per_dbu;

        let mut per_model = Vec::with_capacity(self.materializations.len());
        let mut any_cost_computed = false;
        let mut total_cost = 0.0;
        let mut total_duration_ms: u64 = 0;

        for mat in &mut self.materializations {
            // `bytes_scanned` is not yet plumbed into `MaterializationOutput`
            // from the adapter layer — a later wave will wire
            // Databricks's `result.manifest.total_byte_count` /
            // Snowflake's `statistics.queryLoad` / BigQuery's
            // `totalBytesProcessed` through. Until then BigQuery cost
            // stays `None`; Databricks/Snowflake/DuckDB compute from
            // `duration_ms` alone.
            let cost = rocky_core::cost::compute_observed_cost_usd(
                wh,
                None,
                mat.duration_ms,
                dbu_per_hour,
                cost_per_dbu,
            );
            mat.cost_usd = cost;
            if let Some(c) = cost {
                any_cost_computed = true;
                total_cost += c;
            }
            total_duration_ms = total_duration_ms.saturating_add(mat.duration_ms);
            per_model.push(ModelCostEntry {
                asset_key: mat.asset_key.clone(),
                duration_ms: mat.duration_ms,
                cost_usd: cost,
            });
        }

        let total_cost_usd = if any_cost_computed {
            Some(total_cost)
        } else {
            None
        };

        self.cost_summary = Some(RunCostSummary {
            total_cost_usd,
            total_duration_ms,
            per_model,
            adapter_type: adapter_type.to_string(),
        });
    }

    /// Run the [`rocky_core::config::BudgetConfig`] check against the
    /// already-populated [`RunCostSummary`] and [`Self::duration_ms`].
    ///
    /// For each breached dimension, emits a `budget_breach`
    /// [`rocky_observe::events::PipelineEvent`] on the global event bus
    /// (so hook subscribers and Dagster see breaches live) and records
    /// the breach on [`Self::budget_breaches`] (so JSON consumers can
    /// read it synchronously).
    ///
    /// Returns `Err` when `budget_cfg.on_breach = "error"` and any
    /// configured limit was exceeded. In every other case — including
    /// `on_breach = "warn"` with a breach — returns `Ok(())` so the
    /// event fires but the run still succeeds.
    ///
    /// No-op when [`rocky_core::config::BudgetConfig::is_unset`].
    pub fn check_and_record_budget(
        &mut self,
        budget_cfg: &rocky_core::config::BudgetConfig,
        run_id: Option<&str>,
    ) -> anyhow::Result<()> {
        if budget_cfg.is_unset() {
            return Ok(());
        }

        let observed_cost = self.cost_summary.as_ref().and_then(|s| s.total_cost_usd);
        let breaches = budget_cfg.check_breaches(observed_cost, self.duration_ms);
        if breaches.is_empty() {
            return Ok(());
        }

        let bus = rocky_observe::events::global_event_bus();
        for b in &breaches {
            let mut evt = rocky_observe::events::PipelineEvent::new("budget_breach")
                .with_metadata("limit_type", serde_json::json!(b.limit_type.as_str()))
                .with_metadata("limit", serde_json::json!(b.limit))
                .with_metadata("actual", serde_json::json!(b.actual));
            if let Some(rid) = run_id {
                evt = evt.with_run_id(rid);
            }
            bus.emit(evt);
        }

        self.budget_breaches = breaches
            .iter()
            .map(|b| BudgetBreachOutput {
                limit_type: b.limit_type.as_str().to_string(),
                limit: b.limit,
                actual: b.actual,
            })
            .collect();

        match budget_cfg.on_breach {
            rocky_core::config::BudgetBreachAction::Warn => Ok(()),
            rocky_core::config::BudgetBreachAction::Error => {
                let joined = breaches
                    .iter()
                    .map(|b| {
                        format!(
                            "{}: actual {} > limit {}",
                            b.limit_type.as_str(),
                            b.actual,
                            b.limit
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("; ");
                Err(anyhow::anyhow!("budget exceeded — {joined}"))
            }
        }
    }
}

impl PlanOutput {
    pub fn new(filter: String) -> Self {
        PlanOutput {
            version: VERSION.to_string(),
            command: "plan".to_string(),
            filter,
            statements: vec![],
        }
    }
}

impl StateOutput {
    pub fn new(watermarks: Vec<WatermarkEntry>) -> Self {
        StateOutput {
            version: VERSION.to_string(),
            command: "state".to_string(),
            watermarks,
        }
    }
}

// ---------------------------------------------------------------------------
// rocky dag
// ---------------------------------------------------------------------------

/// JSON output for `rocky dag`.
///
/// Projects the engine's internal [`UnifiedDag`] into an enriched,
/// orchestrator-friendly shape: every pipeline stage becomes a node with
/// its target table coordinates, materialization strategy, freshness SLA,
/// partition shape, and direct upstream dependencies.
///
/// Consumers (dagster-rocky) can build a complete, connected asset graph
/// from a single `rocky dag --output json` call.
///
/// [`UnifiedDag`]: rocky_core::unified_dag::UnifiedDag
#[derive(Debug, Serialize, JsonSchema)]
pub struct DagOutput {
    pub version: String,
    pub command: String,
    /// Every stage in the pipeline as an enriched DAG node.
    pub nodes: Vec<DagNodeOutput>,
    /// Directed edges between nodes (from → to).
    pub edges: Vec<DagEdgeOutput>,
    /// Topologically-sorted execution layers. Nodes within the same
    /// layer have no mutual dependencies and can execute in parallel.
    pub execution_layers: Vec<Vec<String>>,
    /// Summary counts for the DAG.
    pub summary: DagSummaryOutput,
    /// Column-level lineage edges across all models. Only populated
    /// when `--column-lineage` is passed; empty otherwise.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub column_lineage: Vec<LineageEdgeRecord>,
}

/// One node in the enriched DAG, projected for orchestrators.
///
/// Cross-references the engine's internal `UnifiedNode` with model
/// configs, seeds, and pipeline configs to attach the metadata that
/// orchestrators need (target, strategy, freshness, partition shape).
#[derive(Debug, Serialize, JsonSchema)]
pub struct DagNodeOutput {
    /// Unique identifier: `{kind}:{name}` (e.g. `transformation:stg_orders`).
    pub id: String,
    /// Node kind: `source`, `load`, `transformation`, `quality`,
    /// `snapshot`, `seed`, `test`, `replication`.
    pub kind: String,
    /// Human-readable label (usually the pipeline or model name).
    pub label: String,
    /// Pipeline name from `rocky.toml`, if applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pipeline: Option<String>,
    /// Target table coordinates. Present for transformation, seed,
    /// load, and snapshot nodes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<rocky_core::models::TargetConfig>,
    /// Materialization strategy. Present for transformation nodes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strategy: Option<rocky_core::models::StrategyConfig>,
    /// Per-model freshness expectation from the model sidecar.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub freshness: Option<rocky_core::models::ModelFreshnessConfig>,
    /// Partition shape for time-interval models. `None` for
    /// unpartitioned strategies.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_shape: Option<PartitionShapeOutput>,
    /// Upstream node IDs (derived from DAG edges).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<String>,
}

/// Partition shape metadata for time-interval nodes.
#[derive(Debug, Serialize, JsonSchema)]
pub struct PartitionShapeOutput {
    /// Time granularity: `"daily"`, `"hourly"`, `"monthly"`, `"yearly"`.
    pub granularity: String,
    /// First partition key, if declared in the model sidecar.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_partition: Option<String>,
}

/// One directed edge in the DAG output.
#[derive(Debug, Serialize, JsonSchema)]
pub struct DagEdgeOutput {
    /// Upstream node ID.
    pub from: String,
    /// Downstream node ID.
    pub to: String,
    /// Semantic classification: `"data"`, `"check"`, `"governance"`.
    pub edge_type: String,
}

/// Summary counts for the DAG.
#[derive(Debug, Serialize, JsonSchema)]
pub struct DagSummaryOutput {
    pub total_nodes: usize,
    pub total_edges: usize,
    pub execution_layers: usize,
    pub counts_by_kind: BTreeMap<String, usize>,
}

/// Output of `rocky run --dag`: per-node execution results plus aggregate counts.
#[derive(Debug, Serialize, JsonSchema)]
pub struct DagRunOutput {
    pub version: String,
    pub command: String,
    /// Total nodes across all layers.
    pub total_nodes: usize,
    /// Number of execution layers (Kahn topological depth).
    pub total_layers: usize,
    /// Nodes that completed successfully.
    pub completed: usize,
    /// Nodes whose dispatcher returned an error.
    pub failed: usize,
    /// Nodes skipped because an upstream failed (or because the dispatcher
    /// declined to handle them, e.g. Test nodes).
    pub skipped: usize,
    /// Wall-clock duration of the entire DAG execution.
    pub duration_ms: u64,
    /// Per-node execution records, sorted by (layer, id).
    pub nodes: Vec<DagRunNodeOutput>,
}

/// Per-node record in a [`DagRunOutput`].
#[derive(Debug, Serialize, JsonSchema)]
pub struct DagRunNodeOutput {
    /// Node identifier (e.g. `transformation:stg_orders`).
    pub id: String,
    /// Node kind: `source`, `replication`, `transformation`, `quality`,
    /// `snapshot`, `load`, `seed`, `test`.
    pub kind: String,
    /// Human-readable label (usually the pipeline or model name).
    pub label: String,
    /// Layer index (0-based; nodes in the same layer ran concurrently).
    pub layer: usize,
    /// Status: `pending`, `running`, `completed`, `failed`, `skipped`.
    pub status: String,
    /// Wall-clock duration of this node's execution.
    pub duration_ms: u64,
    /// Failure reason. Present iff `status = "failed"` (or `"skipped"` due to
    /// upstream failure).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Prints output as JSON or formatted table.
pub fn print_json<T: Serialize>(output: &T) -> anyhow::Result<()> {
    let json = serde_json::to_string_pretty(output)?;
    println!("{json}");
    Ok(())
}

#[cfg(test)]
mod cost_finalize_tests {
    use super::*;
    use rocky_core::config::{BudgetBreachAction, BudgetConfig, CostSection};

    fn mat(asset_key: &[&str], duration_ms: u64) -> MaterializationOutput {
        MaterializationOutput {
            asset_key: asset_key.iter().map(|s| (*s).to_string()).collect(),
            rows_copied: None,
            duration_ms,
            metadata: MaterializationMetadata {
                strategy: "full_refresh".to_string(),
                watermark: None,
                target_table_full_name: None,
                sql_hash: None,
                column_count: None,
                compile_time_ms: None,
            },
            partition: None,
            cost_usd: None,
        }
    }

    #[test]
    fn populate_cost_summary_no_op_on_unbilled_adapter() {
        let mut out = RunOutput::new(String::new(), 1000, 1);
        out.materializations.push(mat(&["a", "b", "c"], 1000));
        out.populate_cost_summary("fivetran", &CostSection::default());
        assert!(out.cost_summary.is_none(), "unbilled adapters skip cost");
        assert!(out.materializations[0].cost_usd.is_none());
    }

    #[test]
    fn populate_cost_summary_duckdb_is_zero() {
        let mut out = RunOutput::new(String::new(), 1000, 1);
        out.materializations.push(mat(&["a", "b"], 2000));
        out.populate_cost_summary("duckdb", &CostSection::default());
        let summary = out.cost_summary.as_ref().unwrap();
        assert_eq!(summary.adapter_type, "duckdb");
        assert_eq!(summary.total_duration_ms, 2000);
        assert_eq!(summary.total_cost_usd, Some(0.0));
        assert_eq!(summary.per_model.len(), 1);
        assert_eq!(summary.per_model[0].cost_usd, Some(0.0));
        assert_eq!(out.materializations[0].cost_usd, Some(0.0));
    }

    #[test]
    fn populate_cost_summary_databricks_uses_duration() {
        let mut out = RunOutput::new(String::new(), 3_600_000, 1);
        out.materializations.push(mat(&["orders"], 1_800_000));
        out.materializations.push(mat(&["customers"], 1_800_000));
        let cost_cfg = CostSection {
            storage_cost_per_gb_month: 0.023,
            compute_cost_per_dbu: 0.40,
            warehouse_size: "Medium".to_string(),
            min_history_runs: 5,
        };
        out.populate_cost_summary("databricks", &cost_cfg);

        let summary = out.cost_summary.as_ref().unwrap();
        // 1 hr total on Medium (24 DBU/hr) at $0.40/DBU = $9.60.
        let total = summary.total_cost_usd.unwrap();
        assert!((total - 9.60).abs() < 1.0e-6, "total cost = {total}");
        assert_eq!(summary.per_model.len(), 2);
    }

    #[test]
    fn check_and_record_budget_noop_when_unset() {
        let mut out = RunOutput::new(String::new(), 0, 1);
        let budget = BudgetConfig::default();
        assert!(out.check_and_record_budget(&budget, None).is_ok());
        assert!(out.budget_breaches.is_empty());
    }

    #[test]
    fn check_and_record_budget_warn_flags_but_succeeds() {
        let mut out = RunOutput::new(String::new(), 7_200_000, 1);
        out.materializations.push(mat(&["huge"], 7_200_000));
        out.populate_cost_summary(
            "databricks",
            &CostSection {
                storage_cost_per_gb_month: 0.023,
                compute_cost_per_dbu: 0.40,
                warehouse_size: "Medium".to_string(),
                min_history_runs: 5,
            },
        );
        let budget = BudgetConfig {
            max_usd: Some(5.0),
            max_duration_ms: Some(60_000),
            on_breach: BudgetBreachAction::Warn,
        };
        assert!(out.check_and_record_budget(&budget, Some("run-1")).is_ok());
        assert_eq!(out.budget_breaches.len(), 2);
    }

    #[test]
    fn check_and_record_budget_error_returns_err() {
        let mut out = RunOutput::new(String::new(), 120_000, 1);
        out.materializations.push(mat(&["over"], 120_000));
        out.populate_cost_summary("duckdb", &CostSection::default());
        let budget = BudgetConfig {
            max_usd: None,
            max_duration_ms: Some(60_000),
            on_breach: BudgetBreachAction::Error,
        };
        let result = out.check_and_record_budget(&budget, None);
        assert!(result.is_err(), "error mode must return Err on breach");
        assert_eq!(out.budget_breaches.len(), 1);
        assert_eq!(out.budget_breaches[0].limit_type, "max_duration_ms");
    }
}

#[cfg(test)]
mod sql_fingerprint_tests {
    use super::sql_fingerprint;
    use std::hash::{DefaultHasher, Hash, Hasher};

    /// Reference implementation: the pre-§P4.3 `statements.join(";\n").hash(…)`
    /// path. Used to prove the streaming version produces bit-identical output
    /// (critical — `sql_hash` values are persisted in state and compared
    /// across runs, so any drift would invalidate history).
    fn reference_fingerprint(statements: &[String]) -> String {
        let joined = statements.join(";\n");
        let mut hasher = DefaultHasher::new();
        joined.hash(&mut hasher);
        format!("{:016x}", hasher.finish())
    }

    #[test]
    fn fingerprint_matches_join_then_hash_for_empty_input() {
        let stmts: Vec<String> = vec![];
        assert_eq!(sql_fingerprint(&stmts), reference_fingerprint(&stmts));
    }

    #[test]
    fn fingerprint_matches_join_then_hash_for_single_stmt() {
        let stmts = vec!["SELECT 1".to_string()];
        assert_eq!(sql_fingerprint(&stmts), reference_fingerprint(&stmts));
    }

    #[test]
    fn fingerprint_matches_join_then_hash_for_multiple_stmts() {
        let stmts = vec![
            "CREATE TABLE t(x INT)".to_string(),
            "INSERT INTO t VALUES(1)".to_string(),
            "UPDATE t SET x = 2 WHERE x = 1".to_string(),
            "DELETE FROM t".to_string(),
        ];
        assert_eq!(sql_fingerprint(&stmts), reference_fingerprint(&stmts));
    }

    #[test]
    fn fingerprint_matches_for_empty_strings_in_slice() {
        let stmts = vec!["".to_string(), "SELECT 1".to_string(), "".to_string()];
        assert_eq!(sql_fingerprint(&stmts), reference_fingerprint(&stmts));
    }

    #[test]
    fn fingerprint_order_sensitive() {
        let a = vec!["SELECT 1".to_string(), "SELECT 2".to_string()];
        let b = vec!["SELECT 2".to_string(), "SELECT 1".to_string()];
        assert_ne!(sql_fingerprint(&a), sql_fingerprint(&b));
    }

    #[test]
    fn fingerprint_preserves_output_width() {
        let stmts = vec!["SELECT 1".to_string()];
        assert_eq!(sql_fingerprint(&stmts).len(), 16);
    }
}

/// JSON output for `rocky branch create` and `rocky branch show`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct BranchOutput {
    pub version: String,
    pub command: String,
    pub branch: BranchEntry,
}

/// JSON output for `rocky branch list`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct BranchListOutput {
    pub version: String,
    pub command: String,
    pub branches: Vec<BranchEntry>,
    pub total: usize,
}

/// A single branch record in JSON output.
#[derive(Debug, Serialize, JsonSchema)]
pub struct BranchEntry {
    pub name: String,
    pub schema_prefix: String,
    pub created_by: String,
    pub created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// JSON output for `rocky branch delete`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct BranchDeleteOutput {
    pub version: String,
    pub command: String,
    pub name: String,
    /// Whether a record was actually removed (false if the branch didn't exist).
    pub removed: bool,
}

/// JSON output for `rocky replay <run_id|latest>`.
///
/// Inspection-only surface over the state store's [`RunRecord`]: shows every
/// model that ran, with the SQL hash, row counts, bytes, and timings
/// captured at the time. Re-execution (with pinned inputs + content-addressed
/// writes) is deferred to a follow-up when the Arc-1 storage path arrives.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ReplayOutput {
    pub version: String,
    pub command: String,
    pub run_id: String,
    pub status: String,
    pub trigger: String,
    pub started_at: String,
    pub finished_at: String,
    pub config_hash: String,
    pub models: Vec<ReplayModelOutput>,
}

/// A single model execution inside a replayed run.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ReplayModelOutput {
    pub model_name: String,
    pub status: String,
    pub started_at: String,
    pub finished_at: String,
    pub duration_ms: u64,
    pub sql_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_affected: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_scanned: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_written: Option<u64>,
}
