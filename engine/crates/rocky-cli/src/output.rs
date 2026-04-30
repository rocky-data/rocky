use std::collections::{BTreeMap, HashMap};
use std::hash::{DefaultHasher, Hasher};

use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use rocky_compiler::compile::PhaseTimings;
use rocky_compiler::diagnostic::Diagnostic;
use rocky_observe::metrics::MetricsSnapshot;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

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

/// Stable 16-char hex fingerprint of a `rocky.toml` file's raw bytes.
///
/// Stored on [`rocky_core::state::RunRecord::config_hash`] so consumers can
/// detect "did the config change between these two runs?" without diffing
/// full TOML bodies. Uses the same [`DefaultHasher`] (SipHash) as
/// [`sql_fingerprint`] — intra-release-stable, not cross-release-stable.
///
/// Returns `"unknown"` on read failure — record persistence should never
/// fail because the config file became unreadable between load and finalize.
pub fn config_fingerprint(config_path: &std::path::Path) -> String {
    match std::fs::read(config_path) {
        Ok(bytes) => {
            let mut hasher = DefaultHasher::new();
            hasher.write(&bytes);
            format!("{:016x}", hasher.finish())
        }
        Err(_) => "unknown".to_string(),
    }
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
    /// Sources the discovery adapter attempted to fetch metadata for and
    /// failed (transient HTTP error, timeout, rate-limit budget exhausted,
    /// auth blip). Their absence from `sources` does NOT mean they were
    /// removed upstream — consumers diffing against a prior run must treat
    /// failed sources as "unknown state, do not delete." Empty when
    /// discovery completed cleanly. See FR-014.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failed_sources: Vec<FailedSourceOutput>,
    /// Number of schema-cache entries written by this invocation.
    ///
    /// Populated by `rocky discover --with-schemas` — the explicit
    /// warm-up path for the schema cache. Zero — and omitted from the
    /// wire format — when `--with-schemas` isn't set, so fixtures
    /// captured without the flag stay byte-stable.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub schemas_cached: usize,
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
    /// Adapter-namespaced metadata surfaced by the discovery adapter.
    ///
    /// Keys are conventionally prefixed with the adapter kind (e.g.
    /// `fivetran.service`, `fivetran.connector_id`,
    /// `fivetran.custom_reports`, `fivetran.custom_tables`,
    /// `fivetran.schema_prefix`) so entries from different adapters don't
    /// collide when an orchestrator folds them into an asset graph. Values
    /// are opaque `serde_json::Value` — Rocky relays service-specific
    /// payloads rather than modelling them.
    ///
    /// Empty for adapters that haven't opted in; the field is skipped
    /// from the wire format in that case so existing fixtures stay
    /// byte-stable. Populated per-adapter in the discover command — see
    /// [`rocky_core::source::DiscoveredConnector::metadata`].
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub metadata: IndexMap<String, serde_json::Value>,
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
    /// Terminal status of the run. `success` / `partial_failure` / `failure`
    /// match the lifecycle semantics callers already understand; the two
    /// `skipped_*` variants short-circuit via the idempotency key (see
    /// `idempotency_key` below). Always populated — non-skipped runs derive
    /// this field from `tables_failed` / materialization counts, so JSON
    /// consumers no longer need to re-derive status from counts themselves.
    pub status: rocky_core::state::RunStatus,
    /// Prior run whose idempotency key deflected this call, or the run
    /// currently holding the in-flight claim. Populated only when `status`
    /// is `skipped_idempotent` or `skipped_in_flight`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skipped_by_run_id: Option<String>,
    /// The `--idempotency-key` value this run was invoked with, echoed back
    /// for operator cross-reference in logs and `rocky history`. `None` for
    /// runs that didn't pass the flag.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
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
    /// Sum of every per-model `bytes_scanned` that produced a number.
    /// `None` when no materialization reported a byte count (the
    /// non-BigQuery adapters today, which still inherit the default
    /// stub on `WarehouseAdapter::execute_statement_with_stats`).
    /// Surfaced so consumers — and the `[budget]` `max_bytes_scanned`
    /// gate — can read scan volume without re-walking
    /// [`RunOutput::materializations`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_bytes_scanned: Option<u64>,
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
    /// Which limit was breached: `"max_usd"`, `"max_duration_ms"`, or
    /// `"max_bytes_scanned"`.
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

/// A source the discovery adapter attempted to fetch metadata for and failed.
///
/// Surfaced on `DiscoverOutput.failed_sources` so downstream consumers can
/// distinguish a transient fetch failure from a deletion when diffing
/// successive discover snapshots (FR-014).
#[derive(Debug, Serialize, JsonSchema)]
pub struct FailedSourceOutput {
    /// Adapter-side identifier for the source (e.g. Fivetran connector_id).
    pub id: String,
    /// Source schema name the adapter would have written into.
    pub schema: String,
    /// Adapter type (`"fivetran"`, `"airbyte"`, `"iceberg"`, ...).
    pub source_type: String,
    /// Coarse error class so consumers can branch without parsing the
    /// `message`. One of `"transient"` / `"timeout"` / `"rate_limit"` /
    /// `"auth"` / `"unknown"`.
    pub error_class: String,
    /// Human-readable error from the adapter — for logs / debugging only.
    /// Don't pattern-match on this; use `error_class` for branching.
    pub message: String,
}

impl FailedSourceOutput {
    /// Project a `rocky_core` `FailedSource` into the wire shape.
    pub fn from_engine(failed: rocky_core::source::FailedSource) -> Self {
        Self {
            id: failed.id,
            schema: failed.schema,
            source_type: failed.source_type,
            error_class: failed.error_class.to_string(),
            message: failed.message,
        }
    }
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct MaterializationOutput {
    pub asset_key: Vec<String>,
    pub rows_copied: Option<u64>,
    pub duration_ms: u64,
    /// Wall-clock timestamp captured at the moment the engine began
    /// executing this model. Used by `RunOutput::to_run_record` to
    /// build accurate per-model windows on the persisted
    /// `ModelExecution` — replaces the prior lossy reconstruction
    /// (`finished_at - duration`) that mis-ordered parallel runs.
    /// `finished_at` is derived as `started_at + duration_ms`; keeping
    /// one source of truth avoids drift between the two.
    pub started_at: DateTime<Utc>,
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
    /// Adapter-reported bytes figure used for cost accounting, summed
    /// across all statements executed for this materialization. This
    /// is the *billing-relevant* number per adapter, not literal scan
    /// volume — so anyone comparing this to a warehouse console should
    /// know which column lines up.
    ///
    /// - **BigQuery:** `statistics.query.totalBytesBilled` (with the
    ///   10 MB per-query minimum already applied) — matches the
    ///   BigQuery console's "Bytes billed" field, **not** "Bytes
    ///   processed". Fed straight into
    ///   [`rocky_core::cost::compute_observed_cost_usd`] to produce
    ///   `cost_usd`.
    /// - **Databricks:** when populated, byte count from the
    ///   statement-execution manifest (`total_byte_count`); `None`
    ///   today until the manifest plumbing lands.
    /// - **Snowflake:** `None` — deferred by design (QUERY_HISTORY
    ///   round-trip cost; Snowflake cost is duration × DBU, not
    ///   bytes-driven).
    /// - **DuckDB:** `None` — no billed-bytes concept.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_scanned: Option<u64>,
    /// Adapter-reported bytes-written figure, summed across all
    /// statements. Currently `None` on every adapter — BigQuery doesn't
    /// expose a bytes-written figure for query jobs, and the Databricks
    /// / Snowflake paths haven't wired it yet. Reserved so future waves
    /// can populate it without a schema break.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_written: Option<u64>,
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
///
/// `statements` enumerates the warehouse SQL Rocky would emit. The three
/// `*_actions` collections are a parallel view of the control-plane
/// governance work `rocky run` would do *after* a successful DAG — the
/// classification / masking / retention reconcile pass. These never show
/// up as SQL; they fire through [`rocky_core::traits::GovernanceAdapter`]
/// methods (e.g. `apply_column_tags`, `apply_masking_policy`,
/// `apply_retention_policy`). Projects without any `[classification]`,
/// `[mask]`, or `retention` config get empty lists — the fields
/// `skip_serializing_if = Vec::is_empty`, so JSON consumers written
/// against the pre-Wave A shape are byte-stable.
#[derive(Debug, Serialize, JsonSchema)]
pub struct PlanOutput {
    pub version: String,
    pub command: String,
    pub filter: String,
    /// Environment name passed via `--env <name>`. Propagates to
    /// `mask_actions` so the preview resolves `[mask.<env>]` overrides
    /// on top of the workspace `[mask]` defaults. `None` when the flag
    /// is absent — preview resolves against defaults only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env: Option<String>,
    pub statements: Vec<PlannedStatement>,
    /// Column-tag applications the governance reconciler would issue
    /// via `apply_column_tags`. One row per `(model, column, tag)`
    /// triple declared in a model sidecar's `[classification]` block.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub classification_actions: Vec<ClassificationAction>,
    /// Masking-policy applications the governance reconciler would
    /// issue via `apply_masking_policy`. One row per `(model, column,
    /// tag)` where the tag resolves to a strategy for the active env.
    /// Unresolved tags are intentionally omitted — `rocky compliance`
    /// is the diagnostic surface for that gap.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mask_actions: Vec<MaskAction>,
    /// Retention-policy applications the governance reconciler would
    /// issue via `apply_retention_policy`. One row per model whose
    /// sidecar declares `retention = "<N>[dy]"`. `warehouse_preview`
    /// shows the warehouse-native SQL that the current adapter would
    /// compile the policy to (Databricks / Snowflake); `None` on
    /// warehouses without a first-class retention knob.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub retention_actions: Vec<RetentionAction>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct PlannedStatement {
    pub purpose: String,
    pub target: String,
    pub sql: String,
}

/// Classification-tag application row in `PlanOutput.classification_actions`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ClassificationAction {
    /// Model name the action targets.
    pub model: String,
    /// Column name the tag will be applied to.
    pub column: String,
    /// Free-form classification tag (e.g. `"pii"`, `"confidential"`).
    pub tag: String,
}

/// Masking-policy application row in `PlanOutput.mask_actions`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct MaskAction {
    /// Model name the action targets.
    pub model: String,
    /// Column name the mask will be applied to.
    pub column: String,
    /// Classification tag the mask is resolved against.
    pub tag: String,
    /// Wire name of the resolved strategy (`"hash"`, `"redact"`,
    /// `"partial"`, `"none"`). Matches `MaskStrategy::as_str`.
    pub resolved_strategy: String,
}

/// Retention-policy application row in `PlanOutput.retention_actions`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct RetentionAction {
    /// Model name the action targets.
    pub model: String,
    /// Retention duration parsed from the sidecar (`"90d"` → 90,
    /// `"1y"` → 365). Flat day count — no leap-year semantics.
    pub duration_days: u32,
    /// Warehouse-native preview of the SQL / TBLPROPERTIES Rocky would
    /// issue for this model on the active adapter. `None` on warehouses
    /// that don't support a first-class retention knob (BigQuery,
    /// DuckDB).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warehouse_preview: Option<String>,
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
    /// Projected per-run compute cost (USD). Populated from
    /// `rocky_core::optimize::MaterializationCost::compute_cost_per_run`
    /// so Dagster's `checks.py` can surface it as metadata without
    /// re-deriving from config.
    pub compute_cost_per_run: f64,
    /// Projected monthly storage cost (USD).
    pub storage_cost_per_month: f64,
    /// How many downstream models depend on this one. Drives whether
    /// the recommendation favours table materialisation (many
    /// consumers) vs a view.
    pub downstream_references: u64,
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
///
/// The governance audit fields (`triggering_identity`, `session_source`,
/// `git_commit`, `git_branch`, `idempotency_key`, `target_catalog`,
/// `hostname`, `rocky_version`) are only populated in the
/// `rocky history --audit` shape; the default shape continues to emit
/// just the existing 7 fields. The `#[serde(skip_serializing_if = "…")]`
/// attributes keep the default payload byte-identical to schema v5
/// unless `--audit` flips the fields on.
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

    // --- Governance audit trail (populated only with `--audit`) ---
    /// Resolved caller identity (Unix `$USER` / Windows `$USERNAME`).
    /// `None` in CI / container contexts where no human caller is
    /// discernible.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub triggering_identity: Option<String>,
    /// Session origin — `"cli"`, `"dagster"`, `"lsp"`, or `"http_api"`.
    /// Emitted as the lowercase variant string so JSON consumers can
    /// match on it without knowing the Rust enum shape.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_source: Option<String>,
    /// `git rev-parse HEAD` at claim time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git_commit: Option<String>,
    /// `git symbolic-ref --short HEAD` at claim time, or `None` on
    /// detached HEAD.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git_branch: Option<String>,
    /// The `--idempotency-key` value supplied to `rocky run`, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    /// Best-available target catalog — the `catalog_template` for
    /// replication pipelines, the literal `target.catalog` for
    /// transformation/quality/snapshot/load.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_catalog: Option<String>,
    /// Machine hostname that produced the run.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
    /// `CARGO_PKG_VERSION` of the `rocky` binary, or `"<pre-audit>"`
    /// on schema-v5 rows that predate the audit trail.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rocky_version: Option<String>,
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

/// JSON output for `rocky lineage-diff <base_ref>`.
///
/// Combines the structural per-column diff produced by `rocky ci-diff`
/// (added/removed/type-changed columns between two git refs) with the
/// downstream blast-radius from `rocky lineage --downstream` (consumers
/// of each changed column, traced through HEAD's semantic graph).
///
/// The markdown payload is rendered server-side and ready to drop into a
/// PR comment — answers "what does this PR change downstream?" in one
/// command.
///
/// Trace direction is fixed to **downstream from HEAD only** in v1.
/// Removed columns therefore report an empty consumer set (the column
/// no longer exists on HEAD's compile, so its downstream reach can't be
/// walked); the structural diff still surfaces the removal.
#[derive(Debug, Serialize, JsonSchema)]
pub struct LineageDiffOutput {
    pub version: String,
    pub command: String,
    /// Git ref used as the comparison base (e.g. `main`).
    pub base_ref: String,
    /// Git ref for the incoming changes (typically `HEAD`).
    pub head_ref: String,
    pub summary: rocky_core::ci_diff::DiffSummary,
    /// Per-changed-model entries, each with per-column downstream traces.
    pub results: Vec<LineageDiffResult>,
    /// Pre-rendered Markdown suitable for posting as a GitHub PR comment.
    pub markdown: String,
}

/// One model's worth of structural + lineage diff.
#[derive(Debug, Serialize, JsonSchema)]
pub struct LineageDiffResult {
    pub model_name: String,
    pub status: rocky_core::ci_diff::ModelDiffStatus,
    pub column_changes: Vec<LineageColumnChange>,
}

/// Per-column structural change augmented with downstream consumers.
#[derive(Debug, Serialize, JsonSchema)]
pub struct LineageColumnChange {
    pub column_name: String,
    pub change_type: rocky_core::ci_diff::ColumnChangeType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub old_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_type: Option<String>,
    /// Columns reached by walking the lineage graph downstream from
    /// `(model_name, column_name)` on HEAD's compile. Empty when the
    /// column no longer exists on HEAD (e.g. for removed columns) or
    /// when the trace finds no consumers.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub downstream_consumers: Vec<LineageQualifiedColumn>,
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

/// JSON output for `rocky state clear-schema-cache`.
///
/// `dry_run = true` reports what *would* be deleted without touching
/// redb; `dry_run = false` deletes the entries. `entries_deleted` is
/// the actual removed count (or would-be-removed count in dry-run).
#[derive(Debug, Serialize, JsonSchema)]
pub struct ClearSchemaCacheOutput {
    pub version: String,
    pub command: String,
    /// Number of entries removed (or that would be removed in dry-run
    /// mode). Zero when the cache was already empty.
    pub entries_deleted: usize,
    /// `true` when `--dry-run` was set; the cache is left untouched.
    pub dry_run: bool,
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
            failed_sources: vec![],
            schemas_cached: 0,
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

    /// Attach the list of sources whose discovery fetch failed and return self.
    #[must_use]
    pub fn with_failed_sources(mut self, failed: Vec<FailedSourceOutput>) -> Self {
        self.failed_sources = failed;
        self
    }

    /// Record the number of schema-cache entries written by
    /// `rocky discover --with-schemas` and return self.
    ///
    /// Zero is the canonical "flag absent" value and is skipped from the
    /// wire format (see the `#[serde(skip_serializing_if = "is_zero")]`
    /// attribute on the field).
    #[must_use]
    pub fn with_schemas_cached(mut self, schemas_cached: usize) -> Self {
        self.schemas_cached = schemas_cached;
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

/// Governance audit-trail values threaded into
/// [`RunOutput::to_run_record`] at run finalise time. Mirrors the 8
/// audit fields on [`rocky_core::state::RunRecord`] (§1.4).
///
/// Populated once per invocation by
/// `rocky_cli::commands::run_audit::AuditContext::detect` and then
/// re-used for every `persist_run_record` call site (model-only
/// happy path, interrupted, replication main exit). Re-collecting at
/// every call site would stamp the interrupted exit path with a
/// different `hostname` than the initial claim on a machine that
/// changed identity mid-run — unlikely, but trivially prevented by
/// caching the bundle up front.
#[derive(Debug, Clone)]
pub struct RunRecordAudit {
    pub triggering_identity: Option<String>,
    pub session_source: rocky_core::state::SessionSource,
    pub git_commit: Option<String>,
    pub git_branch: Option<String>,
    pub idempotency_key: Option<String>,
    pub target_catalog: Option<String>,
    pub hostname: String,
    pub rocky_version: String,
}

impl RunRecordAudit {
    /// Test-only constructor — every field set to a sentinel that
    /// makes any field confusion in a unit test obvious. Never used
    /// outside `#[cfg(test)]` call sites, so the `_test` suffix is an
    /// API tripwire rather than an invariant.
    #[cfg(test)]
    pub fn test_sentinels() -> Self {
        Self {
            triggering_identity: None,
            session_source: rocky_core::state::SessionSource::Cli,
            git_commit: None,
            git_branch: None,
            idempotency_key: None,
            target_catalog: None,
            hostname: "output-test-host".to_string(),
            rocky_version: "0.0.0-test".to_string(),
        }
    }
}

impl RunOutput {
    pub fn new(filter: String, duration_ms: u64, concurrency: usize) -> Self {
        RunOutput {
            version: VERSION.to_string(),
            command: "run".to_string(),
            status: rocky_core::state::RunStatus::Success,
            skipped_by_run_id: None,
            idempotency_key: None,
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
        let mut any_bytes_reported = false;
        let mut total_bytes: u64 = 0;

        for mat in &mut self.materializations {
            // BigQuery: `mat.bytes_scanned` is populated by the adapter
            // from `statistics.query.totalBytesBilled` (Arc 2 wave 3).
            // Databricks / Snowflake / DuckDB still inherit the default
            // [`rocky_core::traits::WarehouseAdapter::execute_statement_with_stats`]
            // stub which returns `None`, so those adapters continue to
            // compute cost from `duration_ms` alone; wiring their native
            // stats (Databricks `result.manifest.total_byte_count`,
            // Snowflake `statistics.queryLoad`) is a follow-up wave.
            let cost = rocky_core::cost::compute_observed_cost_usd(
                wh,
                mat.bytes_scanned,
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
            if let Some(b) = mat.bytes_scanned {
                any_bytes_reported = true;
                total_bytes = total_bytes.saturating_add(b);
            }
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
        let total_bytes_scanned = if any_bytes_reported {
            Some(total_bytes)
        } else {
            None
        };

        self.cost_summary = Some(RunCostSummary {
            total_cost_usd,
            total_duration_ms,
            total_bytes_scanned,
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
        let observed_bytes = self
            .cost_summary
            .as_ref()
            .and_then(|s| s.total_bytes_scanned);
        let breaches = budget_cfg.check_breaches(observed_cost, self.duration_ms, observed_bytes);
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

    /// Build a [`rocky_core::state::RunRecord`] from the finalised output,
    /// ready for [`rocky_core::state::StateStore::record_run`].
    ///
    /// Each [`MaterializationOutput`] becomes a success-status
    /// [`rocky_core::state::ModelExecution`]; each
    /// [`TableErrorOutput`] becomes a failure-status entry. Per-model
    /// windows use `MaterializationOutput::started_at` (stamped at
    /// execution time) and derive `finished_at` as
    /// `started_at + duration_ms` — parallel runs preserve their real
    /// ordering on the stored `RunRecord`.
    ///
    /// `model_name` is derived from the last element of
    /// [`MaterializationOutput::asset_key`] to match how `rocky cost` /
    /// `rocky history` reference models.
    ///
    /// # Audit context
    ///
    /// Every `RunRecord` stamps the governance audit trail (schema v6,
    /// §1.4) from an
    /// [`rocky_core::state::RunRecordAudit`] bundle. Callers collect
    /// the fields once at claim time via
    /// `rocky_cli::commands::run_audit::AuditContext::detect`.
    #[allow(clippy::too_many_arguments)]
    pub fn to_run_record(
        &self,
        run_id: &str,
        started_at: DateTime<Utc>,
        finished_at: DateTime<Utc>,
        config_hash: String,
        trigger: rocky_core::state::RunTrigger,
        status: rocky_core::state::RunStatus,
        audit: RunRecordAudit,
    ) -> rocky_core::state::RunRecord {
        let mut models = Vec::with_capacity(self.materializations.len() + self.errors.len());

        for mat in &self.materializations {
            let duration_ms = mat.duration_ms;
            let model_started = mat.started_at;
            let model_finished = model_started
                + chrono::Duration::milliseconds(duration_ms.min(i64::MAX as u64) as i64);
            let model_name = mat
                .asset_key
                .last()
                .cloned()
                .unwrap_or_else(|| "<unknown>".to_string());
            models.push(rocky_core::state::ModelExecution {
                model_name,
                started_at: model_started,
                finished_at: model_finished,
                duration_ms,
                rows_affected: mat.rows_copied,
                status: "success".to_string(),
                sql_hash: mat.metadata.sql_hash.clone().unwrap_or_default(),
                bytes_scanned: mat.bytes_scanned,
                bytes_written: mat.bytes_written,
            });
        }

        for err in &self.errors {
            let model_name = err
                .asset_key
                .last()
                .cloned()
                .unwrap_or_else(|| "<unknown>".to_string());
            models.push(rocky_core::state::ModelExecution {
                model_name,
                started_at,
                finished_at,
                duration_ms: 0,
                rows_affected: None,
                status: "failed".to_string(),
                sql_hash: String::new(),
                bytes_scanned: None,
                bytes_written: None,
            });
        }

        rocky_core::state::RunRecord {
            run_id: run_id.to_string(),
            started_at,
            finished_at,
            status,
            models_executed: models,
            trigger,
            config_hash,
            triggering_identity: audit.triggering_identity,
            session_source: audit.session_source,
            git_commit: audit.git_commit,
            git_branch: audit.git_branch,
            idempotency_key: audit.idempotency_key,
            target_catalog: audit.target_catalog,
            hostname: audit.hostname,
            rocky_version: audit.rocky_version,
        }
    }

    /// Derive the [`rocky_core::state::RunStatus`] from the output's
    /// end-state counters. Interrupt (`self.interrupted`) with no
    /// materializations → `Failure`; with at least one → `PartialFailure`.
    /// Same rule for failed-table counts. Clean completion → `Success`.
    pub fn derive_run_status(&self) -> rocky_core::state::RunStatus {
        let has_progress = self.tables_copied > 0;
        let has_problem = self.interrupted || self.tables_failed > 0;
        match (has_progress, has_problem) {
            (_, false) => rocky_core::state::RunStatus::Success,
            (true, true) => rocky_core::state::RunStatus::PartialFailure,
            (false, true) => rocky_core::state::RunStatus::Failure,
        }
    }
}

impl PlanOutput {
    pub fn new(filter: String) -> Self {
        PlanOutput {
            version: VERSION.to_string(),
            command: "plan".to_string(),
            filter,
            env: None,
            statements: vec![],
            classification_actions: vec![],
            mask_actions: vec![],
            retention_actions: vec![],
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

impl ClearSchemaCacheOutput {
    pub fn new(entries_deleted: usize, dry_run: bool) -> Self {
        ClearSchemaCacheOutput {
            version: VERSION.to_string(),
            command: "state-clear-schema-cache".to_string(),
            entries_deleted,
            dry_run,
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
            started_at: Utc::now(),
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
            bytes_scanned: None,
            bytes_written: None,
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
    fn populate_cost_summary_bigquery_uses_bytes_scanned() {
        // Arc 2 wave 3: when the BigQuery adapter populates
        // `bytes_scanned` from `statistics.query.totalBytesBilled`,
        // `populate_cost_summary` must produce a real dollar figure
        // rather than the `None` it returned before the plumbing landed.
        // 1 TB billed at $6.25/TB → $6.25.
        let mut out = RunOutput::new(String::new(), 1_000, 1);
        let mut m = mat(&["orders"], 1_000);
        m.bytes_scanned = Some(1_000_000_000_000); // 1 TB.
        out.materializations.push(m);
        out.populate_cost_summary("bigquery", &CostSection::default());
        let summary = out
            .cost_summary
            .as_ref()
            .expect("BigQuery is a billed warehouse");
        let total = summary.total_cost_usd.expect("bytes_scanned drives cost");
        assert!((total - 6.25).abs() < 1.0e-6, "total cost = {total}");
        assert_eq!(out.materializations[0].cost_usd, Some(6.25));
    }

    #[test]
    fn populate_cost_summary_bigquery_none_when_bytes_missing() {
        // When the adapter doesn't report bytes (e.g. a DDL that
        // BigQuery returned without `statistics`), the cost column
        // stays `None` — don't silently fall back to a duration-based
        // figure that would be off by orders of magnitude for a
        // bytes-priced warehouse.
        let mut out = RunOutput::new(String::new(), 1_000, 1);
        out.materializations.push(mat(&["orders"], 1_000));
        out.populate_cost_summary("bigquery", &CostSection::default());
        let summary = out.cost_summary.as_ref().unwrap();
        assert!(summary.total_cost_usd.is_none());
        assert!(out.materializations[0].cost_usd.is_none());
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
            max_bytes_scanned: None,
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
            max_bytes_scanned: None,
            on_breach: BudgetBreachAction::Error,
        };
        let result = out.check_and_record_budget(&budget, None);
        assert!(result.is_err(), "error mode must return Err on breach");
        assert_eq!(out.budget_breaches.len(), 1);
        assert_eq!(out.budget_breaches[0].limit_type, "max_duration_ms");
    }

    #[test]
    fn check_and_record_budget_trips_on_max_bytes_scanned() {
        // Synthetic run: two BigQuery-style materializations each
        // reporting 1_000_000 bytes scanned. With
        // `max_bytes_scanned = 1_000_000`, total of 2_000_000 must
        // trip the breach and (because `on_breach = "error"`) return
        // `Err`.
        let mut out = RunOutput::new(String::new(), 1_000, 1);
        let mut m1 = mat(&["a"], 500);
        m1.bytes_scanned = Some(1_000_000);
        let mut m2 = mat(&["b"], 500);
        m2.bytes_scanned = Some(1_000_000);
        out.materializations.push(m1);
        out.materializations.push(m2);
        out.populate_cost_summary("bigquery", &CostSection::default());
        assert_eq!(
            out.cost_summary
                .as_ref()
                .and_then(|s| s.total_bytes_scanned),
            Some(2_000_000),
        );

        let budget = BudgetConfig {
            max_usd: None,
            max_duration_ms: None,
            max_bytes_scanned: Some(1_000_000),
            on_breach: BudgetBreachAction::Error,
        };
        let result = out.check_and_record_budget(&budget, Some("run-bytes"));
        assert!(result.is_err(), "scan-volume breach must return Err");
        assert_eq!(out.budget_breaches.len(), 1);
        assert_eq!(out.budget_breaches[0].limit_type, "max_bytes_scanned");
        assert!((out.budget_breaches[0].limit - 1_000_000.0).abs() < f64::EPSILON);
        assert!((out.budget_breaches[0].actual - 2_000_000.0).abs() < f64::EPSILON);
    }

    #[test]
    fn check_and_record_budget_no_breach_when_max_bytes_scanned_unset() {
        // Same synthetic 2 MB run, but `max_bytes_scanned` defaults to
        // `None` — no breach regardless of scan total.
        let mut out = RunOutput::new(String::new(), 1_000, 1);
        let mut m = mat(&["a"], 500);
        m.bytes_scanned = Some(2_000_000);
        out.materializations.push(m);
        out.populate_cost_summary("bigquery", &CostSection::default());

        let budget = BudgetConfig::default();
        assert!(out.check_and_record_budget(&budget, None).is_ok());
        assert!(out.budget_breaches.is_empty());
    }
}

#[cfg(test)]
mod run_record_tests {
    use super::*;
    use rocky_core::state::{RunStatus, RunTrigger};

    fn mat(
        asset_key: &[&str],
        duration_ms: u64,
        sql_hash: Option<&str>,
        started_at: DateTime<Utc>,
    ) -> MaterializationOutput {
        MaterializationOutput {
            asset_key: asset_key.iter().map(|s| (*s).to_string()).collect(),
            rows_copied: Some(42),
            duration_ms,
            started_at,
            metadata: MaterializationMetadata {
                strategy: "full_refresh".to_string(),
                watermark: None,
                target_table_full_name: None,
                sql_hash: sql_hash.map(str::to_string),
                column_count: None,
                compile_time_ms: None,
            },
            partition: None,
            cost_usd: None,
            bytes_scanned: None,
            bytes_written: None,
        }
    }

    fn fixed_start() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-04-21T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn to_run_record_uses_per_model_started_at() {
        let mut out = RunOutput::new(String::new(), 5_000, 1);
        out.tables_copied = 2;
        let run_started = fixed_start();
        // Simulate parallel execution: orders starts at +0s (runs
        // 1.5s), customers starts at +0.5s (runs 0.8s). Under the old
        // lossy reconstruction both would share the same finished_at
        // and have started_at rebuilt from the wrong end.
        let orders_start = run_started;
        let customers_start = run_started + chrono::Duration::milliseconds(500);
        out.materializations
            .push(mat(&["s", "orders"], 1_500, Some("abc123"), orders_start));
        out.materializations
            .push(mat(&["s", "customers"], 800, None, customers_start));

        let run_finished = run_started + chrono::Duration::seconds(5);
        let record = out.to_run_record(
            "run-test-1",
            run_started,
            run_finished,
            "cfghash".to_string(),
            RunTrigger::Manual,
            RunStatus::Success,
            RunRecordAudit::test_sentinels(),
        );

        assert_eq!(record.run_id, "run-test-1");
        assert_eq!(record.config_hash, "cfghash");
        assert_eq!(record.models_executed.len(), 2);

        let orders = &record.models_executed[0];
        assert_eq!(orders.model_name, "orders");
        assert_eq!(orders.status, "success");
        assert_eq!(orders.duration_ms, 1_500);
        assert_eq!(orders.rows_affected, Some(42));
        assert_eq!(orders.sql_hash, "abc123");
        // Each model's started_at mirrors the MaterializationOutput's;
        // finished_at is derived as started_at + duration (preserves
        // parallel overlap rather than collapsing to run.finished_at).
        assert_eq!(orders.started_at, orders_start);
        assert_eq!(
            orders.finished_at,
            orders_start + chrono::Duration::milliseconds(1_500)
        );

        let customers = &record.models_executed[1];
        assert_eq!(customers.model_name, "customers");
        assert_eq!(customers.sql_hash, "", "sql_hash none → empty string");
        assert_eq!(customers.started_at, customers_start);
        assert_eq!(
            customers.finished_at,
            customers_start + chrono::Duration::milliseconds(800)
        );
    }

    #[test]
    fn to_run_record_maps_errors_to_failed_entries() {
        let mut out = RunOutput::new(String::new(), 2_000, 1);
        out.tables_copied = 1;
        out.tables_failed = 1;
        let run_started = fixed_start();
        out.materializations
            .push(mat(&["s", "orders"], 1_000, Some("ok"), run_started));
        out.errors.push(TableErrorOutput {
            asset_key: vec!["s".to_string(), "broken".to_string()],
            error: "connection refused".to_string(),
        });

        let started = fixed_start();
        let finished = started + chrono::Duration::seconds(2);
        let record = out.to_run_record(
            "run-test-2",
            started,
            finished,
            String::new(),
            RunTrigger::Manual,
            out.derive_run_status(),
            RunRecordAudit::test_sentinels(),
        );

        assert_eq!(record.models_executed.len(), 2);
        let failed = record
            .models_executed
            .iter()
            .find(|m| m.status == "failed")
            .expect("failed entry present");
        assert_eq!(failed.model_name, "broken");
        assert_eq!(failed.duration_ms, 0);
        assert!(matches!(record.status, RunStatus::PartialFailure));
    }

    #[test]
    fn derive_run_status_clean_success() {
        let mut out = RunOutput::new(String::new(), 0, 1);
        out.tables_copied = 3;
        assert!(matches!(out.derive_run_status(), RunStatus::Success));
    }

    #[test]
    fn derive_run_status_interrupted_with_progress_is_partial() {
        let mut out = RunOutput::new(String::new(), 0, 1);
        out.tables_copied = 2;
        out.interrupted = true;
        assert!(matches!(out.derive_run_status(), RunStatus::PartialFailure));
    }

    #[test]
    fn derive_run_status_interrupted_no_progress_is_failure() {
        let mut out = RunOutput::new(String::new(), 0, 1);
        out.interrupted = true;
        assert!(matches!(out.derive_run_status(), RunStatus::Failure));
    }

    #[test]
    fn derive_run_status_failed_tables_no_progress_is_failure() {
        let mut out = RunOutput::new(String::new(), 0, 1);
        out.tables_failed = 2;
        assert!(matches!(out.derive_run_status(), RunStatus::Failure));
    }

    #[test]
    fn config_fingerprint_stable_for_same_bytes() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), b"[adapter]\ntype = \"duckdb\"\n").unwrap();
        let h1 = config_fingerprint(tmp.path());
        let h2 = config_fingerprint(tmp.path());
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 16, "16 hex chars");
        assert_ne!(h1, "unknown");
    }

    #[test]
    fn config_fingerprint_differs_when_bytes_differ() {
        let tmp1 = tempfile::NamedTempFile::new().unwrap();
        let tmp2 = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp1.path(), b"one").unwrap();
        std::fs::write(tmp2.path(), b"two").unwrap();
        assert_ne!(
            config_fingerprint(tmp1.path()),
            config_fingerprint(tmp2.path())
        );
    }

    #[test]
    fn config_fingerprint_missing_file_returns_unknown() {
        assert_eq!(
            config_fingerprint(std::path::Path::new("/does/not/exist.toml")),
            "unknown"
        );
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
    /// Adapter-reported bytes figure used for cost accounting. This is
    /// the *billing-relevant* number per adapter, not literal scan
    /// volume:
    ///
    /// - **BigQuery:** `totalBytesBilled` — includes the 10 MB
    ///   per-query minimum floor; matches the BigQuery console's
    ///   "Bytes billed" field, **not** "Bytes processed".
    /// - **Databricks:** when populated, byte count from the
    ///   statement-execution manifest (`total_byte_count`); `None`
    ///   today until the manifest plumbing lands.
    /// - **Snowflake:** `None` — deferred by design (QUERY_HISTORY
    ///   round-trip cost; Snowflake cost is duration × DBU, not
    ///   bytes-driven).
    /// - **DuckDB:** `None` — no billed-bytes concept.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_scanned: Option<u64>,
    /// Adapter-reported bytes-written figure. Currently `None` on
    /// every adapter — BigQuery doesn't expose a bytes-written figure
    /// for query jobs, and the Databricks / Snowflake paths haven't
    /// wired it yet.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_written: Option<u64>,
}

/// JSON output for `rocky trace <run_id|latest>`.
///
/// Sibling to [`ReplayOutput`] but with offset-relative timings so
/// downstream consumers (Dagster asset Gantt, custom dashboards) can
/// render the run as a timeline without re-deriving the run start.
#[derive(Debug, Serialize, JsonSchema)]
pub struct TraceOutput {
    pub version: String,
    pub command: String,
    pub run_id: String,
    pub status: String,
    pub trigger: String,
    pub started_at: String,
    pub finished_at: String,
    pub run_duration_ms: u64,
    /// Number of concurrent lanes the scheduler used during this run.
    /// `1` for fully sequential pipelines, `>1` when the DAG had
    /// independent models that the executor materialized in parallel.
    pub lane_count: usize,
    pub models: Vec<TraceModelEntry>,
}

/// One model execution entry inside [`TraceOutput`]. `start_offset_ms`
/// is the wall-clock offset from the run start; `lane` identifies the
/// concurrency lane for Gantt rendering (entries on the same lane
/// never overlap in time).
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct TraceModelEntry {
    pub model_name: String,
    pub status: String,
    pub start_offset_ms: i64,
    pub duration_ms: u64,
    pub sql_hash: String,
    /// Greedy first-fit concurrency lane. Populated by the renderer;
    /// deserializing clients don't need to supply it.
    #[serde(default)]
    pub lane: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_affected: Option<u64>,
    /// Adapter-reported bytes figure used for cost accounting. This is
    /// the *billing-relevant* number per adapter, not literal scan
    /// volume:
    ///
    /// - **BigQuery:** `totalBytesBilled` — includes the 10 MB
    ///   per-query minimum floor; matches the BigQuery console's
    ///   "Bytes billed" field, **not** "Bytes processed".
    /// - **Databricks:** when populated, byte count from the
    ///   statement-execution manifest (`total_byte_count`); `None`
    ///   today until the manifest plumbing lands.
    /// - **Snowflake:** `None` — deferred by design (QUERY_HISTORY
    ///   round-trip cost; Snowflake cost is duration × DBU, not
    ///   bytes-driven).
    /// - **DuckDB:** `None` — no billed-bytes concept.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_scanned: Option<u64>,
    /// Adapter-reported bytes-written figure. Currently `None` on
    /// every adapter — BigQuery doesn't expose a bytes-written figure
    /// for query jobs, and the Databricks / Snowflake paths haven't
    /// wired it yet.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_written: Option<u64>,
}

/// JSON output for `rocky cost <run_id|latest>`.
///
/// Trust-system Arc 2 wave 2: historical per-run cost attribution read
/// from the embedded state store's [`rocky_core::state::RunRecord`].
/// Re-derives per-model cost via [`rocky_core::cost::compute_observed_cost_usd`]
/// — the same formula [`RunOutput::populate_cost_summary`] applies at
/// the end of a live run. The per-model and per-run totals make the
/// "what did my last run cost?" question answerable from the recorded
/// run alone, without re-materialising tables.
///
/// # Adapter type resolution
///
/// The `RunRecord` only carries `config_hash`, not the adapter type.
/// The command loads `rocky.toml` to resolve the billed-warehouse type.
/// When the config can't be loaded (working-dir mismatch, missing file,
/// parse error), the output degrades gracefully: `adapter_type` stays
/// `None` and `cost_usd` is `None` on every model, but durations and
/// byte counts are still populated from the stored record.
///
/// # BigQuery note
///
/// Because [`rocky_core::state::ModelExecution::bytes_scanned`] is
/// persisted, this command can return a real cost figure for BigQuery
/// runs even though the live `rocky run` path currently reports
/// `cost_usd: None` for BQ (adapter bytes-scanned plumbing is a
/// follow-up wave).
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct CostOutput {
    pub version: String,
    pub command: String,
    pub run_id: String,
    pub status: String,
    pub trigger: String,
    pub started_at: String,
    pub finished_at: String,
    pub duration_ms: u64,
    /// Adapter type the cost formula was parameterised against, for
    /// audit. Mirrors `AdapterConfig.type`. `None` when the config
    /// couldn't be loaded or the adapter isn't a billed warehouse.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub adapter_type: Option<String>,
    /// Sum of every per-model `cost_usd` that produced a number.
    /// `None` when no model produced a cost.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_cost_usd: Option<f64>,
    /// Wall-clock time summed across every model execution.
    pub total_duration_ms: u64,
    /// Sum of per-model `bytes_scanned`. `None` when no model reported
    /// bytes scanned.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_bytes_scanned: Option<u64>,
    /// Sum of per-model `bytes_written`. `None` when no model reported
    /// bytes written.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_bytes_written: Option<u64>,
    pub per_model: Vec<PerModelCostHistorical>,
}

/// A single model's cost attribution inside [`CostOutput`].
///
/// Distinct from [`ModelCostEntry`] (which lives on
/// [`RunCostSummary`]) because the historical surface carries the
/// richer fields the state store actually persists: model name (not
/// asset-key vector), row/byte counts, and the recorded per-model
/// status.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct PerModelCostHistorical {
    pub model_name: String,
    pub status: String,
    pub duration_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_affected: Option<u64>,
    /// Adapter-reported bytes figure used for cost accounting. This is
    /// the *billing-relevant* number per adapter, not literal scan
    /// volume:
    ///
    /// - **BigQuery:** `totalBytesBilled` — includes the 10 MB
    ///   per-query minimum floor; matches the BigQuery console's
    ///   "Bytes billed" field, **not** "Bytes processed".
    /// - **Databricks:** when populated, byte count from the
    ///   statement-execution manifest (`total_byte_count`); `None`
    ///   today until the manifest plumbing lands.
    /// - **Snowflake:** `None` — deferred by design (QUERY_HISTORY
    ///   round-trip cost; Snowflake cost is duration × DBU, not
    ///   bytes-driven).
    /// - **DuckDB:** `None` — no billed-bytes concept.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_scanned: Option<u64>,
    /// Adapter-reported bytes-written figure. Currently `None` on
    /// every adapter — BigQuery doesn't expose a bytes-written figure
    /// for query jobs, and the Databricks / Snowflake paths haven't
    /// wired it yet.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_written: Option<u64>,
    /// Observed cost for this execution. `None` when the adapter
    /// isn't a billed warehouse, the config couldn't be loaded, or
    /// the formula inputs were unavailable (e.g. BigQuery without
    /// `bytes_scanned`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost_usd: Option<f64>,
}

// ---------------------------------------------------------------------------
// `rocky compliance` — governance compliance rollup (Wave B)
// ---------------------------------------------------------------------------

/// JSON output for `rocky compliance`.
///
/// A thin rollup over Wave A governance: classification sidecars
/// (`[classification]` per model) + project-level `[mask]` / `[mask.<env>]`
/// policy. Answers the question **"are all classified columns masked
/// wherever policy says they should be?"** without making any warehouse
/// calls — purely a static resolver over `rocky.toml` + model sidecars.
///
/// Consumers (CI gates, dagster) dispatch on the top-level `command` field
/// (`"compliance"`).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ComplianceOutput {
    /// `CARGO_PKG_VERSION` at the time this payload was emitted.
    pub version: String,
    /// Always `"compliance"`. Consumers key dispatch off this field.
    pub command: String,
    /// Aggregate tallies over the full `per_column` / `exceptions` lists.
    pub summary: ComplianceSummary,
    /// One entry per `(model, column)` pair carrying a classification tag.
    /// When `--exceptions-only` is set, this list is filtered to only the
    /// pairs whose `envs` contain at least one exception.
    pub per_column: Vec<ColumnClassificationStatus>,
    /// The unmasked-where-expected list. An exception fires when a
    /// classification tag has no resolved masking strategy **and** the tag
    /// is not on the project-level `[classifications] allow_unmasked`
    /// advisory list.
    pub exceptions: Vec<ComplianceException>,
}

/// Aggregate counters for a compliance report.
///
/// All three counters are over `(model, column, env)` triples — so a
/// single classified column evaluated across three envs contributes up to
/// three to `total_classified`. This matches the granularity of
/// `total_exceptions` and `total_masked`, keeping the invariant
/// `total_classified = total_masked + total_exceptions + <allow_listed
/// unresolved>` (the third term is not counted in either bucket, so it
/// surfaces implicitly as the gap).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ComplianceSummary {
    /// Total `(model, column, env)` triples across every classified
    /// column in the project, expanded across the env enumeration.
    pub total_classified: u64,
    /// Triples where a masking strategy successfully resolved
    /// (`enforced = true`). `MaskStrategy::None` ("explicit identity —
    /// no masking") counts as masked here because the project has
    /// deliberately opted out, which is a conscious policy decision
    /// rather than an enforcement gap.
    pub total_masked: u64,
    /// Triples where no masking strategy resolved and the classification
    /// tag is **not** on the `allow_unmasked` advisory list. Matches the
    /// length of [`ComplianceOutput::exceptions`].
    pub total_exceptions: u64,
}

/// Per-`(model, column)` report: the classification tag and the resolved
/// masking status across every evaluated environment.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ColumnClassificationStatus {
    /// Model name — matches `ModelConfig::name` (the `.toml` sidecar key
    /// or the `.sql`/`.rocky` filename stem).
    pub model: String,
    /// Column name — the key under the model's `[classification]` block.
    pub column: String,
    /// Free-form classification tag (e.g., `"pii"`, `"confidential"`).
    /// The engine does not enum-constrain these — projects coin tags as
    /// needed.
    pub classification: String,
    /// One entry per evaluated environment. When `--env X` is set, this
    /// is a single-element list labeled `"X"`. When unset, it expands
    /// across the union of the defaults (labeled `"default"`) and every
    /// named `[mask.<env>]` override block.
    pub envs: Vec<EnvMaskingStatus>,
}

/// Masking status for one `(model, column, env)` triple.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct EnvMaskingStatus {
    /// Environment label. `"default"` when the row came from the
    /// unscoped `[mask]` block; otherwise the name of the matching
    /// `[mask.<env>]` override, or the raw `--env` value.
    pub env: String,
    /// Resolved masking strategy. One of `"hash"`, `"redact"`,
    /// `"partial"`, `"none"`, or `"unresolved"` when no strategy
    /// applies to this classification tag in this env.
    pub masking_strategy: String,
    /// `true` iff `masking_strategy != "unresolved"`. Allow-listed tags
    /// (on `[classifications] allow_unmasked`) still report
    /// `enforced = false` — the allow list only suppresses the
    /// [`ComplianceException`] emission, not the underlying fact that
    /// no strategy resolved.
    pub enforced: bool,
}

/// A single compliance exception — a classified column with no resolved
/// masking strategy in some environment, and whose classification tag is
/// **not** on the advisory allow list.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ComplianceException {
    pub model: String,
    pub column: String,
    pub env: String,
    /// Human-readable explanation. Always of the shape
    /// `"no masking strategy resolves for classification tag '<tag>'"`
    /// in v1; future variants (e.g., adapter-specific violations) may
    /// widen this.
    pub reason: String,
}

// ---------------------------------------------------------------------------
// Retention status (Wave C-2)
// ---------------------------------------------------------------------------

/// JSON output for `rocky retention-status`.
///
/// Reports which models declare a `retention = "<N>[dy]"` sidecar value and
/// — when `--drift` is set in a future wave — whether the warehouse's
/// current retention matches. Today `warehouse_days` is always `None`
/// because the probe is deferred to v2; the schema is stable so v2 can
/// populate the field without a JSON shape break.
#[derive(Debug, Serialize, JsonSchema)]
pub struct RetentionStatusOutput {
    pub version: String,
    pub command: String,
    pub models: Vec<ModelRetentionStatus>,
}

/// Per-model retention declaration + (eventually) warehouse-observed value.
///
/// - `configured_days` is `None` when the model's sidecar has no
///   `retention` key.
/// - `warehouse_days` is populated by the (v2) `--drift` probe via
///   `SHOW TBLPROPERTIES` on Databricks or `SHOW PARAMETERS ... FOR
///   TABLE` on Snowflake. Always `None` in v1.
/// - `in_sync` is `true` iff `configured_days == warehouse_days`, or
///   both are `None`. A model with no configured retention is never
///   flagged as out-of-sync.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ModelRetentionStatus {
    pub model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub configured_days: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warehouse_days: Option<u32>,
    pub in_sync: bool,
}

impl RetentionStatusOutput {
    pub fn new(models: Vec<ModelRetentionStatus>) -> Self {
        RetentionStatusOutput {
            version: VERSION.to_string(),
            command: "retention-status".to_string(),
            models,
        }
    }
}

// ---------------------------------------------------------------------------
// `rocky preview` — PR preview workflow (Arc 1 ∩ Arc 2 user-facing surface)
//
// Three subcommands compose into a single PR comment:
//   * `preview create` — pruned re-run on a per-PR branch with copy-from-base
//                        for unchanged upstream
//   * `preview diff`   — structural + sampled row-level diff vs. base
//   * `preview cost`   — per-model bytes/duration/USD delta vs. base
//
// The shapes below are stable wire contracts: changing them requires
// regenerating dagster Pydantic + vscode TypeScript bindings via
// `just codegen` (see `engine/CLAUDE.md`).
// ---------------------------------------------------------------------------

/// JSON output for `rocky preview create`.
///
/// Records the prune-and-copy decision plus the run summary for the branch
/// execution. The `prune_set` lists models that re-executed; the `copy_set`
/// lists models pre-populated from the base schema instead of being
/// re-run. `skipped_set` is the empty-cost residue — neither changed
/// nor downstream of a change.
#[derive(Debug, Serialize, JsonSchema)]
pub struct PreviewCreateOutput {
    pub version: String,
    pub command: String,
    /// Branch name registered in the state store. Mirrors the `name`
    /// from `rocky branch create`.
    pub branch_name: String,
    /// Schema prefix the branch run wrote into (e.g. `branch__fix-price`).
    pub branch_schema: String,
    /// Git ref the change set was computed against. Mirrors `--base`.
    pub base_ref: String,
    /// Git SHA the diff was computed from (typically `HEAD`).
    pub head_ref: String,
    /// Models that re-executed against the branch.
    pub prune_set: Vec<PreviewPrunedModel>,
    /// Models pre-populated from the base schema instead of re-running.
    /// Empty in Phase 1 if no models could be safely copied.
    pub copy_set: Vec<PreviewCopiedModel>,
    /// Models neither in the prune set nor copied — the column-level
    /// pruner determined they are unaffected and not depended on by any
    /// pruned model. Names only; rationale lives in the diff layer.
    pub skipped_set: Vec<String>,
    /// `RunRecord` id for the branch execution. Empty when no models
    /// re-executed (a no-op preview where every changed model pruned
    /// to zero downstream).
    pub run_id: String,
    /// `succeeded`, `partial`, or `failed` — mirrors `RunRecord.status`.
    pub run_status: String,
    pub duration_ms: u64,
}

/// One entry in [`PreviewCreateOutput::prune_set`].
#[derive(Debug, Serialize, JsonSchema)]
pub struct PreviewPrunedModel {
    pub model_name: String,
    /// `"changed"` for models the diff identified directly, or
    /// `"downstream_of_changed"` for models pulled in by column-level
    /// lineage from a changed model.
    pub reason: String,
    /// Columns the diff reports as changed on this model. Only
    /// populated when `reason = "changed"`.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub changed_columns: Vec<String>,
}

/// One entry in [`PreviewCreateOutput::copy_set`].
///
/// `copy_strategy` is `"ctas"` in Phase 1 (DuckDB CTAS or generic
/// `CREATE TABLE AS SELECT *`); Phase 5 lifts this to `"shallow_clone"`
/// (Databricks Delta) and `"zero_copy_clone"` (Snowflake) per the
/// adapter's clone capability.
#[derive(Debug, Serialize, JsonSchema)]
pub struct PreviewCopiedModel {
    pub model_name: String,
    pub source_schema: String,
    pub target_schema: String,
    pub copy_strategy: String,
}

/// JSON output for `rocky preview diff`.
///
/// Combines the structural diff (column added/removed/type-changed) from
/// the existing `rocky_core::ci_diff` machinery with a sampled row-level
/// diff that extends the `rocky_core::compare`/`shadow` kernel.
///
/// **Sampling correctness ceiling.** Phase 2 sampling reads `LIMIT N`
/// rows ordered by primary key (or first column). Changes outside that
/// window appear as no-change unless the row count itself differs.
/// `sampling_window.coverage_warning = true` flags this risk on the
/// per-model level so reviewers don't infer false coverage. A
/// checksum-bisection exhaustive diff is the Phase 2.5 lift.
#[derive(Debug, Serialize, JsonSchema)]
pub struct PreviewDiffOutput {
    pub version: String,
    pub command: String,
    pub branch_name: String,
    pub base_ref: String,
    pub summary: PreviewDiffSummary,
    pub models: Vec<PreviewModelDiff>,
    /// Pre-rendered Markdown suitable for posting as a GitHub PR comment.
    pub markdown: String,
}

/// Aggregate diff counts for [`PreviewDiffOutput`].
#[derive(Debug, Serialize, JsonSchema)]
pub struct PreviewDiffSummary {
    pub models_with_changes: usize,
    pub models_unchanged: usize,
    pub total_rows_added: u64,
    pub total_rows_removed: u64,
    pub total_rows_changed: u64,
    /// `true` if **any** per-model diff carries
    /// `sampling_window.coverage_warning = true`.
    pub any_coverage_warning: bool,
}

/// Per-model diff. Combines structural (column-level) and sampled
/// (row-level) findings.
#[derive(Debug, Serialize, JsonSchema)]
pub struct PreviewModelDiff {
    pub model_name: String,
    pub structural: PreviewStructuralDiff,
    pub sampled: PreviewSampledRowDiff,
    pub sampling_window: PreviewSamplingWindow,
}

/// Column-level structural diff. Mirrors the shape produced by
/// `rocky ci-diff` at the column granularity.
#[derive(Debug, Serialize, JsonSchema)]
pub struct PreviewStructuralDiff {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub added_columns: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub removed_columns: Vec<String>,
    /// One entry per column whose type changed. Each carries `name`,
    /// `from`, `to`.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub type_changes: Vec<PreviewColumnTypeChange>,
}

/// One column-type change entry.
#[derive(Debug, Serialize, JsonSchema)]
pub struct PreviewColumnTypeChange {
    pub name: String,
    pub from: String,
    pub to: String,
}

/// Sampled row-level diff. All counts are over the sampling window.
#[derive(Debug, Serialize, JsonSchema)]
pub struct PreviewSampledRowDiff {
    pub rows_added: u64,
    pub rows_removed: u64,
    pub rows_changed: u64,
    /// Up to `--max-samples` (default 5) representative changed rows
    /// for human review. Pure noise when sampling found no change.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub samples: Vec<PreviewRowSample>,
}

/// One representative changed row in [`PreviewSampledRowDiff::samples`].
#[derive(Debug, Serialize, JsonSchema)]
pub struct PreviewRowSample {
    pub primary_key: String,
    /// Pairs of `(column_name, base_value, branch_value)` for every
    /// column that differs on this row.
    pub changes: Vec<PreviewRowSampleChange>,
}

/// One `(column, base, branch)` triple in [`PreviewRowSample::changes`].
#[derive(Debug, Serialize, JsonSchema)]
pub struct PreviewRowSampleChange {
    pub column: String,
    pub base_value: String,
    pub branch_value: String,
}

/// Sampling-window metadata surfaced verbatim in the PR comment.
///
/// `coverage_warning = true` flags that the row count outside the
/// sampling window is non-trivial; reviewers should not infer "no
/// change" from a clean sample if this flag is set.
#[derive(Debug, Serialize, JsonSchema)]
pub struct PreviewSamplingWindow {
    pub ordered_by: String,
    pub limit: usize,
    /// One of `"first_n_by_order"` (Phase 2) or `"checksum_bisection"`
    /// (Phase 2.5 lift, not yet shipped).
    pub coverage: String,
    pub coverage_warning: bool,
}

/// JSON output for `rocky preview cost`.
///
/// Diff layer over `rocky cost latest`'s machinery. Per-model deltas
/// are computed by looking up the latest base-schema `RunRecord` and
/// the branch run's `RunRecord` and subtracting field-by-field.
///
/// `models_skipped_via_copy` is the prune-set complement: copied
/// models did not run on the branch, so their delta is `None` and
/// their savings accrue to `savings_from_copy_usd`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct PreviewCostOutput {
    pub version: String,
    pub command: String,
    pub branch_name: String,
    /// `RunRecord` id for the latest base-schema run that the branch
    /// is being compared against. `None` when no base run exists.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_run_id: Option<String>,
    /// `RunRecord` id for the branch execution. Empty when the branch
    /// run was a no-op (every changed model pruned to zero downstream).
    pub branch_run_id: String,
    pub summary: PreviewCostSummary,
    pub per_model: Vec<PreviewModelCostDelta>,
    pub markdown: String,
}

/// Aggregate cost rollup for [`PreviewCostOutput`].
#[derive(Debug, Serialize, JsonSchema)]
pub struct PreviewCostSummary {
    /// Sum of every per-model `branch_cost_usd` that produced a number.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_branch_cost_usd: Option<f64>,
    /// Sum of every per-model `base_cost_usd` (limited to models in
    /// the prune set — copied models contribute 0 here, accounted for
    /// in `savings_from_copy_usd`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_base_cost_usd: Option<f64>,
    /// `total_branch_cost_usd - total_base_cost_usd`. Positive = the
    /// PR costs more to run than `main`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delta_usd: Option<f64>,
    /// Number of models that did not run on the branch because they
    /// were copied from base. Their savings show up below.
    pub models_skipped_via_copy: usize,
    /// Sum of `base_cost_usd` for every copied model — the cost the
    /// PR avoided by copying instead of re-running. `None` when no
    /// base costs are available for the copied models.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub savings_from_copy_usd: Option<f64>,
}

/// Per-model cost delta.
#[derive(Debug, Serialize, JsonSchema)]
pub struct PreviewModelCostDelta {
    pub model_name: String,
    /// `true` if this model was copied from base instead of re-run.
    /// When true, `branch_*` fields are `None` / `0` and the savings
    /// for this row roll into [`PreviewCostSummary::savings_from_copy_usd`].
    pub skipped_via_copy: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch_cost_usd: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_cost_usd: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delta_usd: Option<f64>,
    pub branch_duration_ms: u64,
    pub base_duration_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch_bytes_scanned: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_bytes_scanned: Option<u64>,
}

impl PreviewCreateOutput {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        branch_name: String,
        branch_schema: String,
        base_ref: String,
        head_ref: String,
        prune_set: Vec<PreviewPrunedModel>,
        copy_set: Vec<PreviewCopiedModel>,
        skipped_set: Vec<String>,
        run_id: String,
        run_status: String,
        duration_ms: u64,
    ) -> Self {
        PreviewCreateOutput {
            version: VERSION.to_string(),
            command: "preview-create".to_string(),
            branch_name,
            branch_schema,
            base_ref,
            head_ref,
            prune_set,
            copy_set,
            skipped_set,
            run_id,
            run_status,
            duration_ms,
        }
    }
}

impl PreviewDiffOutput {
    pub fn new(
        branch_name: String,
        base_ref: String,
        summary: PreviewDiffSummary,
        models: Vec<PreviewModelDiff>,
        markdown: String,
    ) -> Self {
        PreviewDiffOutput {
            version: VERSION.to_string(),
            command: "preview-diff".to_string(),
            branch_name,
            base_ref,
            summary,
            models,
            markdown,
        }
    }
}

impl PreviewCostOutput {
    pub fn new(
        branch_name: String,
        base_run_id: Option<String>,
        branch_run_id: String,
        summary: PreviewCostSummary,
        per_model: Vec<PreviewModelCostDelta>,
        markdown: String,
    ) -> Self {
        PreviewCostOutput {
            version: VERSION.to_string(),
            command: "preview-cost".to_string(),
            branch_name,
            base_run_id,
            branch_run_id,
            summary,
            per_model,
            markdown,
        }
    }
}
