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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
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
    /// Source schemas seen for the first time relative to the prior persisted
    /// `discover` snapshot — the catch-a-duplicate-at-onboarding signal.
    /// Populated only when the pipeline's discovery config sets
    /// `report_new_sources`; empty (and omitted from the wire format)
    /// otherwise. The first-ever discover of a pipeline establishes the
    /// baseline and reports none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub new_sources: Vec<String>,
    /// Groups of ≥2 discovered sources sharing the SAME external object id but
    /// resolving to DIFFERENT target paths — the same underlying object
    /// onboarded twice. Populated only when discovery's `on_collision` is
    /// `warn`/`error` and the adapter supplies `external_object_id`; empty (and
    /// omitted from the wire format) otherwise.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub collision_candidates: Vec<CollisionCandidateOutput>,
}

/// A cross-source collision: one external object id mapped to more than one
/// target path (the same object onboarded twice under different schemas).
#[derive(Debug, Serialize, JsonSchema)]
pub struct CollisionCandidateOutput {
    /// The shared external object id (e.g. a Fivetran ad-account id).
    pub external_object_id: String,
    /// The distinct source schemas that resolve to it (≥2), sorted.
    pub sources: Vec<String>,
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
    /// Resolved per-model check names the pipeline will emit as
    /// `CheckResult.name` at run time, keyed by unqualified table/model name.
    /// Only the NON-default checks are listed — the four defaults
    /// (row_count/column_match/freshness/anomaly) are well-known and
    /// pre-declared by consumers. A consumer (e.g. Dagster) declares an
    /// asset-check spec per name so the spec name byte-matches the emitted
    /// result. Empty (and omitted) when no non-default checks are configured.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub configured_checks: BTreeMap<String, Vec<ResolvedCheckNameOutput>>,
}

/// A resolved check name `rocky discover` projects so a consumer can
/// pre-declare a matching check spec. `name` byte-matches the
/// `CheckResult.name` the runner emits at run time. `candidate` is `true` for
/// names whose existence depends on runtime-discovered siblings
/// (`cross_source_overlap`) and so may not be emitted on every run.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ResolvedCheckNameOutput {
    pub name: String,
    /// The check-kind tag: `custom` | `assertion` | `null_rate` |
    /// `cross_source_overlap`.
    pub kind: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub candidate: bool,
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
    /// Total number of failed tables/models for the run, **including**
    /// pre-execution compile failures: a model that fails to type-check is
    /// counted here even though it never reached the execution phase. This is
    /// the authoritative failure count — consumers should key overall pass/fail
    /// on the top-level `tables_failed` / `status` / `errors`, not on
    /// `execution.tables_failed`, which counts only execution-phase (copy /
    /// runtime) failures and excludes models excluded before execution.
    pub tables_failed: usize,
    #[serde(skip_serializing_if = "is_zero")]
    pub tables_skipped: usize,
    /// Tables that the discovery adapter reported as enabled but that do not
    /// exist in the source warehouse (e.g. Fivetran has them configured but
    /// hasn't synced them, or they carry the `do_not_alter__` broken-table
    /// marker). Surfaced so orchestrators can flag the gap in their UIs
    /// instead of silently dropping the assets.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub excluded_tables: Vec<ExcludedTableOutput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resumed_from: Option<String>,
    /// True when running in shadow mode (targets rewritten).
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub shadow: bool,
    pub materializations: Vec<MaterializationOutput>,
    /// Per-model build/skip/reuse decision + reason, surfaced for
    /// transformation runs so orchestrators can explain *why* each model
    /// ran, was skipped, or was reused. Populated only when the
    /// `--skip-unchanged` gate is active or `[reuse]` is enabled; empty (and
    /// omitted) for a default run, which stays byte-identical.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub model_decisions: Vec<ModelDecisionOutput>,
    /// Models withheld this run because an upstream failed (or was itself
    /// withheld) and `[resilience] contain_failures` continued the disjoint
    /// subgraphs — the blast radius of the failures named in `errors[]`. Empty
    /// (and omitted) for a run that did not withhold anything: the default
    /// fail-fast run, and any successful run, record nothing here.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub contained: Vec<ContainedModelOutput>,
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
    /// Soft warnings raised by the per-table override resolver — one
    /// entry per `[[table_overrides]]` rule that matched zero
    /// `(connector, table)` pairs this run, or whose connector half
    /// resolved nothing. Discovery-time-only — the pipeline runs to
    /// completion regardless. Empty for runs whose pipeline declares
    /// no overrides.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub override_warnings: Vec<OverrideWarningOutput>,
}

/// Soft warning surfaced on
/// [`RunOutput::override_warnings`] when an override rule matched no
/// tables this run.
///
/// Distinct kinds let orchestrators (Dagster) branch on cause without
/// parsing free-form messages.
#[derive(Debug, Serialize, JsonSchema)]
pub struct OverrideWarningOutput {
    /// Position of the rule in `[[table_overrides]]`, 0-based — same
    /// index the validator's error messages use.
    pub rule_index: usize,
    /// Coarse kind: `"zero_match"` (rule matched no pair at all) or
    /// `"connector_match_empty"` (the connector half of the match
    /// resolved nothing).
    pub kind: String,
    /// Human-readable explanation, for logs and Dagster UI rendering.
    pub message: String,
    /// Echo of `match.connector` from the rule, for cross-reference.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connector: Option<String>,
    /// Echo of `match.table` from the rule, for cross-reference.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
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

/// One per-model budget breach surfaced on
/// [`PreviewCostOutput::projected_per_model_budget_breaches`].
///
/// Same fields as [`BudgetBreachOutput`] plus `model_name`. Kept as a
/// distinct type so the run-level `budget_breaches` shape stays
/// untouched; downstream consumers iterate the two surfaces with
/// separate code paths or merge them deliberately.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct PerModelBudgetBreachOutput {
    /// Name of the model whose resolved per-model budget was breached.
    pub model_name: String,
    /// Which limit was breached: `"max_usd"`, `"max_duration_ms"`, or
    /// `"max_bytes_scanned"`.
    pub limit_type: String,
    /// Effective limit applied — i.e. the field-inheritance result of
    /// per-model overrides composed against the project-level
    /// `[budget]`. Surfaced as the resolved value rather than the raw
    /// override so PR readers see the limit they actually crossed.
    pub limit: f64,
    pub actual: f64,
    /// Resolved on-breach action for this model: `"warn"` or `"error"`.
    /// When `"error"`, this breach would fail the run if merged.
    /// Defaults from the project-level config when the sidecar omits
    /// `on_breach`.
    pub on_breach: String,
}

fn is_zero(v: &usize) -> bool {
    *v == 0
}

/// Summary of execution parallelism and throughput.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ExecutionSummary {
    pub concurrency: usize,
    pub tables_processed: usize,
    /// Tables that failed during the **execution phase** — copy or runtime
    /// failures while materializing. This does **not** include models excluded
    /// before execution started (for example, a model that failed to compile);
    /// those are counted only in the top-level `RunOutput.tables_failed`. For
    /// overall run pass/fail, read the top-level `tables_failed` / `status`,
    /// not this `execution.tables_failed`.
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

/// Coarse-grained failure classification for an entry on
/// [`RunOutput::errors`]. Lets orchestrators branch on the kind of
/// failure (retry, page someone, surface in the UI) without parsing the
/// free-form `error` string.
///
/// Variants partition the [`rocky_databricks::connector::ConnectorError`]
/// and [`rocky_snowflake::connector::ConnectorError`] spaces; `Unknown`
/// is the fallback for non-connector failures (drift, governance,
/// adapter-internal errors) where the error reached the output layer
/// already type-erased.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum FailureKind {
    /// TCP / TLS / DNS / connection-establishment failure.
    ConnectionFailed,
    /// Credentials rejected, token expired or otherwise invalid.
    AuthFailed,
    /// Warehouse rejected the SQL — syntax error, schema mismatch,
    /// missing permission, semantic analysis failure.
    QueryRejected,
    /// Retry-worthy failure — 5xx, network glitch, statement aborted by
    /// a transient warehouse condition.
    Transient,
    /// Rate limit hit or a configured budget cap (retry budget, circuit
    /// breaker, account-level quota).
    QuotaExceeded,
    /// Requested catalog / schema / table not present.
    NotFound,
    /// A model failed to compile (parse / type-check / contract
    /// diagnostic) before any SQL was issued — e.g. a `time_interval`
    /// model whose `time_column` is absent from its SELECT output
    /// (E020). The model never reached the warehouse; the run surfaces
    /// it as a failure instead of silently skipping it.
    CompileError,
    /// Fallback when the failure could not be classified — e.g. errors
    /// raised outside the connector layer that reach this struct
    /// type-erased through `anyhow::Error`.
    #[default]
    Unknown,
}

impl From<&rocky_databricks::connector::ConnectorError> for FailureKind {
    fn from(err: &rocky_databricks::connector::ConnectorError) -> Self {
        use rocky_databricks::connector::ConnectorError as E;
        match err {
            E::Auth(_) => FailureKind::AuthFailed,
            E::Http(e) => {
                if e.is_connect() {
                    FailureKind::ConnectionFailed
                } else if e.is_timeout() {
                    FailureKind::Transient
                } else {
                    FailureKind::ConnectionFailed
                }
            }
            E::StatementFailed { .. } => FailureKind::QueryRejected,
            E::Timeout { .. } => FailureKind::Transient,
            E::Canceled { .. } => FailureKind::Unknown,
            E::UnexpectedState { .. } => FailureKind::Unknown,
            E::ApiError { status, .. } => match status {
                401 | 403 => FailureKind::AuthFailed,
                404 => FailureKind::NotFound,
                429 => FailureKind::QuotaExceeded,
                500..=599 => FailureKind::Transient,
                _ => FailureKind::QueryRejected,
            },
            // A tripped breaker is a *budget cap* — sustained transient
            // pressure exhausted the retry budget — not a one-off
            // transient blip. Match the [`FailureKind::QuotaExceeded`]
            // doc-comment ("Rate limit hit or a configured budget cap
            // (retry budget, circuit breaker, account-level quota)")
            // and the Fivetran-side classification, so the dagster
            // handler (which keys on `failure_kind == quota-exceeded`)
            // can surface a retriable `dg.Failure` with the engine's
            // cooldown hint instead of treating it as a fail-fast
            // transient error.
            E::CircuitBreakerOpen { .. } => FailureKind::QuotaExceeded,
            E::RetryBudgetExhausted { .. } => FailureKind::QuotaExceeded,
            // Arrow-disposition path failures from `execute_sql_arrow`.
            // The statement landed but the result payload was either
            // empty (no chunks where we expected some) or undecodable
            // (IPC parse / `concat_batches` mismatch). Both are
            // adapter-side defects we can't retry past, so classify
            // as `QueryRejected` (the closest "warehouse handed back
            // something we can't use" bucket).
            E::NoArrowChunks { .. } | E::Arrow(_) => FailureKind::QueryRejected,
        }
    }
}

impl From<&rocky_snowflake::connector::ConnectorError> for FailureKind {
    fn from(err: &rocky_snowflake::connector::ConnectorError) -> Self {
        use rocky_snowflake::connector::ConnectorError as E;
        match err {
            E::Auth(_) => FailureKind::AuthFailed,
            E::Http(e) => {
                if e.is_connect() {
                    FailureKind::ConnectionFailed
                } else if e.is_timeout() {
                    FailureKind::Transient
                } else {
                    FailureKind::ConnectionFailed
                }
            }
            E::StatementFailed { .. } => FailureKind::QueryRejected,
            E::Timeout { .. } => FailureKind::Transient,
            E::ApiError { status, .. } => match status {
                401 | 403 => FailureKind::AuthFailed,
                404 => FailureKind::NotFound,
                429 => FailureKind::QuotaExceeded,
                500..=599 => FailureKind::Transient,
                _ => FailureKind::QueryRejected,
            },
            // See the databricks branch above — breaker trips are
            // `QuotaExceeded` (budget cap), not `Transient`.
            E::CircuitBreakerOpen { .. } => FailureKind::QuotaExceeded,
            E::RetryBudgetExhausted { .. } => FailureKind::QuotaExceeded,
        }
    }
}

/// Extract the warehouse-reported cooldown (in whole seconds) from a
/// typed connector error, when the variant carries one. Populated only
/// for `CircuitBreakerOpen` against breakers configured with timed
/// half-open recovery (`retry.circuit_breaker_recovery_timeout_secs`);
/// every other variant returns `None`. Surfaces on
/// [`TableErrorOutput::cooldown_seconds`] so orchestrators can derive a
/// `retry_after` hint without re-parsing config — warehouse-side mirror
/// of the Fivetran `FailedSourceOutput.cooldown_seconds` path.
fn cooldown_from_databricks(err: &rocky_databricks::connector::ConnectorError) -> Option<u64> {
    use rocky_databricks::connector::ConnectorError as E;
    match err {
        E::CircuitBreakerOpen {
            cooldown_seconds, ..
        } => *cooldown_seconds,
        _ => None,
    }
}

fn cooldown_from_snowflake(err: &rocky_snowflake::connector::ConnectorError) -> Option<u64> {
    use rocky_snowflake::connector::ConnectorError as E;
    match err {
        E::CircuitBreakerOpen {
            cooldown_seconds, ..
        } => *cooldown_seconds,
        _ => None,
    }
}

/// Walk an `anyhow::Error`'s `chain()` looking for a typed
/// `ConnectorError` and classify it via [`FailureKind`]. Returns
/// [`FailureKind::Unknown`] when neither connector enum is reachable.
///
/// Production-path note: adapter calls go through
/// [`rocky_adapter_sdk::AdapterError`], a `Box<dyn Error>` wrapper
/// whose `Error::source()` impl returns the *inner*'s source — so a
/// bare `chain()` walk skips past the wrapper straight to whatever the
/// `ConnectorError` carries (e.g. `reqwest::Error`) and never sees the
/// connector variant itself. To handle that, each cause is also
/// downcast to `AdapterError`; when matched, its
/// [`AdapterError::inner`] is probed for the typed `ConnectorError`.
///
/// Many existing call sites in `run.rs` still build their `anyhow`
/// errors via `anyhow::anyhow!("…{e}")`, which stringifies the
/// `AdapterError` and drops the type. Migrating those to `.context(…)`
/// is a separate, mechanical follow-up — the classifier is correct,
/// its reach grows as those sites are converted.
fn classify_cause(cause: &(dyn std::error::Error + 'static)) -> Option<FailureKind> {
    if let Some(e) = cause.downcast_ref::<rocky_databricks::connector::ConnectorError>() {
        return Some(e.into());
    }
    if let Some(e) = cause.downcast_ref::<rocky_snowflake::connector::ConnectorError>() {
        return Some(e.into());
    }
    None
}

/// Probe a single error link for a typed warehouse connector error and
/// project it into both [`FailureKind`] and the optional cooldown hint.
/// Returns `Some((kind, cooldown))` on a match, `None` otherwise — used
/// by [`classify_anyhow_error_with_cooldown`] so a tripped warehouse
/// breaker surfaces its `recovery_timeout` on
/// [`TableErrorOutput::cooldown_seconds`] without re-walking the chain.
fn classify_cause_with_cooldown(
    cause: &(dyn std::error::Error + 'static),
) -> Option<(FailureKind, Option<u64>)> {
    if let Some(e) = cause.downcast_ref::<rocky_databricks::connector::ConnectorError>() {
        return Some((e.into(), cooldown_from_databricks(e)));
    }
    if let Some(e) = cause.downcast_ref::<rocky_snowflake::connector::ConnectorError>() {
        return Some((e.into(), cooldown_from_snowflake(e)));
    }
    None
}

pub fn classify_anyhow_error(err: &anyhow::Error) -> FailureKind {
    for cause in err.chain() {
        if let Some(kind) = classify_cause(cause) {
            return kind;
        }
        if let Some(adapter_err) = cause.downcast_ref::<rocky_adapter_sdk::AdapterError>()
            && let Some(kind) = classify_cause(adapter_err.inner())
        {
            return kind;
        }
    }
    FailureKind::Unknown
}

/// Like [`classify_anyhow_error`], but also extracts the
/// engine-supplied cooldown hint when the typed connector variant
/// carries one (currently only warehouse `CircuitBreakerOpen` against a
/// breaker configured with `circuit_breaker_recovery_timeout_secs`).
/// Returns `(FailureKind, Option<u64>)`; the cooldown is the
/// warehouse-side analogue of `FailedSourceOutput.cooldown_seconds` and
/// feeds [`TableErrorOutput::cooldown_seconds`] so dagster-rocky can
/// project it onto a retriable `dg.Failure` without re-parsing
/// `rocky.toml`.
pub fn classify_anyhow_error_with_cooldown(err: &anyhow::Error) -> (FailureKind, Option<u64>) {
    for cause in err.chain() {
        if let Some(pair) = classify_cause_with_cooldown(cause) {
            return pair;
        }
        if let Some(adapter_err) = cause.downcast_ref::<rocky_adapter_sdk::AdapterError>()
            && let Some(pair) = classify_cause_with_cooldown(adapter_err.inner())
        {
            return pair;
        }
    }
    (FailureKind::Unknown, None)
}

/// Recover the HTTP status from a single error link if it is a Databricks
/// or Snowflake `ConnectorError::ApiError`.
fn api_status_from_cause(cause: &(dyn std::error::Error + 'static)) -> Option<u16> {
    use rocky_databricks::connector::ConnectorError as Dbx;
    use rocky_snowflake::connector::ConnectorError as Sf;
    if let Some(Dbx::ApiError { status, .. }) = cause.downcast_ref::<Dbx>() {
        return Some(*status);
    }
    if let Some(Sf::ApiError { status, .. }) = cause.downcast_ref::<Sf>() {
        return Some(*status);
    }
    None
}

/// Frame a warehouse error into an actionable, user-facing message when
/// it carries a recognised HTTP auth status.
///
/// Maps `403` and `401` (Databricks / Snowflake `ApiError`) to a concise
/// remediation hint, keeping the raw status + body out of the headline.
/// Returns `None` for any other failure (including BigQuery, which has no
/// typed `ApiError` status path) so the caller falls back to the raw
/// error string.
///
/// `target` is a human label for what was being accessed (a table ref or
/// adapter name), interpolated into the framed message.
fn frame_status(status: u16, target: &str) -> Option<String> {
    match status {
        403 => Some(format!(
            "permission denied on `{target}` (HTTP 403) — verify the adapter principal \
             has the required grants"
        )),
        401 => Some(
            "authentication rejected (HTTP 401) — the token or credential may be \
             expired or invalid"
                .to_string(),
        ),
        _ => None,
    }
}

/// Walk an `anyhow` error chain (including any `rocky_adapter_sdk::
/// AdapterError` inner) for a recognised warehouse auth status and frame
/// it. See [`frame_status`].
pub fn frame_warehouse_anyhow_error(err: &anyhow::Error, target: &str) -> Option<String> {
    for cause in err.chain() {
        if let Some(status) = api_status_from_cause(cause) {
            return frame_status(status, target);
        }
        if let Some(adapter_err) = cause.downcast_ref::<rocky_adapter_sdk::AdapterError>()
            && let Some(status) = api_status_from_cause(adapter_err.inner())
        {
            return frame_status(status, target);
        }
    }
    None
}

/// Frame a `rocky_core::traits::AdapterError` (the type returned by
/// `WarehouseAdapter::ping`) for a recognised warehouse auth status.
///
/// Probes the boxed `inner` error and its `source()` chain for a
/// Databricks / Snowflake `ConnectorError::ApiError`. See
/// [`frame_status`].
pub fn frame_warehouse_adapter_error(
    err: &rocky_core::traits::AdapterError,
    target: &str,
) -> Option<String> {
    let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(err.inner());
    while let Some(c) = cause {
        if let Some(status) = api_status_from_cause(c) {
            return frame_status(status, target);
        }
        cause = c.source();
    }
    None
}

/// Error from a table that failed during parallel processing.
#[derive(Debug, Serialize, JsonSchema)]
pub struct TableErrorOutput {
    pub asset_key: Vec<String>,
    pub error: String,
    /// Coarse classification of the failure so orchestrators can branch
    /// on kind (retry, page, surface) without parsing `error`. Defaults
    /// to [`FailureKind::Unknown`] for errors that reached the output
    /// layer type-erased.
    #[serde(default)]
    pub failure_kind: FailureKind,
    /// Backoff hint in whole seconds. Populated by adapters whose
    /// failure mode carries a known cooldown — currently only the
    /// Databricks and Snowflake warehouse adapters when their shared
    /// circuit breaker trips (and the breaker was configured with
    /// `retry.circuit_breaker_recovery_timeout_secs`). Orchestrators
    /// (Dagster, etc.) use it as a `retry_after` hint when scheduling
    /// a delayed re-run. Mirrors the source-side shape on
    /// [`FailedSourceOutput::cooldown_seconds`]. Absent for failure
    /// classes without an engine-supplied hint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cooldown_seconds: Option<u64>,
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
    /// Backoff hint in whole seconds. Populated by adapters whose
    /// failure mode carries a known cooldown — currently only the
    /// Fivetran adapter when its shared circuit breaker trips.
    /// Orchestrators use it as a `retry_after` hint when scheduling a
    /// delayed re-discover. Absent for failure classes without an
    /// engine-supplied hint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cooldown_seconds: Option<u64>,
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
            cooldown_seconds: failed.cooldown_seconds,
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
    /// Tenant this materialization is attributed to, taken from the
    /// discover-time schema-pattern `{tenant}` component. Present only
    /// for replication pipelines whose schema pattern declares a
    /// `{tenant}` placeholder; transformation / `time_interval` models
    /// and non-tenant patterns leave it `None`. Carried onto the
    /// persisted `rocky_core::state::ModelExecution` by
    /// [`RunOutput::to_run_record`] so `rocky cost --by tenant` can roll
    /// per-model cost up to a tenant dimension. Omitted from JSON when
    /// `None` so non-tenant outputs stay byte-identical.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    /// Warehouse-side job identifiers for the SQL statements rocky
    /// issued to materialize this output. Populated for adapters whose
    /// REST API surfaces a job reference per statement (BigQuery
    /// today via `jobReference.jobId`).
    ///
    /// Useful for cross-checking rocky's reported figures against the
    /// warehouse's own statistics — e.g., feeding a job ID into
    /// `bq show -j <id>` and comparing `totalBytesBilled` to
    /// [`Self::bytes_scanned`]. Empty `Vec` for adapters that don't
    /// surface a job concept (DuckDB) or haven't wired it yet
    /// (Databricks, Snowflake).
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub job_ids: Vec<String>,
    /// The classified-retry attempt trail for this materialization: one
    /// [`AttemptRecord`](rocky_core::state::AttemptRecord) per try. Empty
    /// (and omitted from JSON) for a clean first-try success — the run loop
    /// only stamps a trail once a retry actually happened, so a non-retried
    /// build's output stays byte-identical to before this layer shipped.
    /// [`RunOutput::to_run_record`] copies this verbatim onto the persisted
    /// `ModelExecution.attempts` — one attempt type on both the wire and the
    /// state record.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub attempts: Vec<rocky_core::state::AttemptRecord>,
    /// State-internal skip-gate outputs, co-located with the model they
    /// describe so [`RunOutput::to_run_record`] can copy them onto the
    /// persisted `ModelExecution` without a second name-keyed map. Stamped
    /// by the `--skip-unchanged` gate after a successful build; `None` when
    /// the gate is off or the model was not skip-eligible.
    ///
    /// Never serialized and never part of the JSON schema (via
    /// `#[serde(skip)]` and `#[schemars(skip)]`) — mirrors the
    /// `name_declared` internal-field pattern on `ModelConfig`. Keeps the
    /// skip-gate state out of every `*Output` codegen surface.
    #[serde(skip)]
    #[schemars(skip)]
    pub skip_internal: Option<ModelSkipState>,
    /// State-internal recipe-identity slice (`recipe_hash` + `env_hash` +
    /// scheme), co-located with the model so [`RunOutput::to_run_record`] can
    /// stamp it onto the persisted `ModelExecution`. Computed at the per-model
    /// execution site; `None` keeps default behavior byte-identical for any
    /// path that has not (yet) been wired to compute it.
    ///
    /// Never serialized and never part of the JSON schema (via `#[serde(skip)]`
    /// and `#[schemars(skip)]`) — same pattern as [`Self::skip_internal`].
    #[serde(skip)]
    #[schemars(skip)]
    pub recipe_identity: Option<RecipeIdentityInternal>,
    /// State-internal per-output-column content hashes, co-located with the
    /// model so [`RunOutput::to_run_record`] can stamp them onto the persisted
    /// `ModelExecution.output_column_hashes`. Populated only by the
    /// content-addressed runner on a genuine unpartitioned build; `None`
    /// everywhere else (every non-content-addressed path, reuse, partitioned).
    ///
    /// Never serialized and never part of the JSON schema (via `#[serde(skip)]`
    /// and `#[schemars(skip)]`) — same pattern as [`Self::recipe_identity`].
    #[serde(skip)]
    #[schemars(skip)]
    pub output_column_hashes: Option<Vec<rocky_core::state::ColumnHash>>,
    /// State-internal consumer-side per-column baseline for this model, carried
    /// so [`RunOutput::to_run_record`] can route it onto the persisted
    /// [`rocky_core::state::ModelExecution::upstream_freshness`] as
    /// [`UpstreamSig`](rocky_core::state::UpstreamSig) entries whose
    /// `consumed_column_hashes` record what this model consumed from each
    /// upstream at build time. Populated only by the content-addressed runner
    /// on a genuine build whose consumed-column set is provably complete;
    /// `None` everywhere else. Consumer-side and local (Fork 2), never
    /// producer-now-vs-prior; captured only, nothing consults it yet.
    ///
    /// Never serialized and never part of the JSON schema (via `#[serde(skip)]`
    /// and `#[schemars(skip)]`) — same pattern as [`Self::output_column_hashes`].
    #[serde(skip)]
    #[schemars(skip)]
    pub consumed_column_baseline: Option<Vec<rocky_core::state::UpstreamSig>>,
}

/// State-internal skip-gate result for one materialized model, carried on
/// [`MaterializationOutput`] from the gate to `to_run_record`.
///
/// Best-effort optimization metadata only — not a result-equivalence proof.
#[derive(Debug, Clone)]
pub struct ModelSkipState {
    /// Cosmetic-invariant logic key for the built model (`None` when the
    /// model could not be canonicalised).
    pub skip_hash: Option<String>,
    /// Per-upstream freshness signatures observed for this build.
    pub upstream_freshness: Vec<rocky_core::state::UpstreamSig>,
}

/// State-internal recipe-identity slice for one materialized model, carried on
/// [`MaterializationOutput`] from the per-model execution site (where the typed
/// IR and the adapter are both in scope) to `to_run_record`, which stamps it —
/// together with the input side derived from [`ModelSkipState`] — onto the
/// persisted [`rocky_core::state::ModelExecution`].
///
/// Holds only the two hashes that need per-model / adapter context:
/// `recipe_hash` (the program identity, from the model's IR) and `env_hash`
/// (engine + adapter/dialect). The input side is recomputed in
/// `to_run_record` from the observed upstream signatures. Never serialized and
/// never part of the JSON schema — mirrors the `skip_internal` pattern.
#[derive(Debug, Clone)]
pub struct RecipeIdentityInternal {
    /// blake3 (hex) of the canonical `ModelIr` JSON —
    /// [`rocky_core::recipe_identity::recipe_hash`].
    pub recipe_hash: String,
    /// blake3 (hex) of the run's [`rocky_core::recipe_identity::EnvIdentity`].
    pub env_hash: String,
    /// The [`rocky_core::recipe_identity::HASH_SCHEME`] tag in force.
    pub hash_scheme: String,
}

/// Derive the persisted `(input_hash, input_proof_class)` for one materialized
/// model from its observed upstream freshness signatures.
///
/// The input side of the recipe-identity triple is populated only when this
/// run actually *observed* inputs — i.e. the `--skip-unchanged` gate ran and
/// recorded per-upstream watermark/rowcount signatures on
/// [`MaterializationOutput::skip_internal`]. Those observed signatures fold
/// into an [`rocky_core::recipe_identity::UpstreamIdentity::Watermark`] set,
/// yielding a `"heuristic"` (weak) input hash — honest about attesting
/// freshness, not byte-identity.
///
/// Returns `(None, None)` on the default run path (no gate, no observed
/// inputs), on a non-canonicalisable model (`skip_hash` absent), or when no
/// upstream signatures were captured. A bare "no upstreams" hash is
/// deliberately *not* fabricated: the declared inputs already live inside
/// `recipe_hash`, so an empty-upstream input hash would add nothing and would
/// misleadingly read as a strong claim.
/// Compute the per-model recipe-identity carrier (`recipe_hash` + `env_hash` +
/// scheme) at a model's execution site, where the typed IR and the warehouse
/// adapter are both in scope.
///
/// `recipe_hash` is the program identity (canonical `ModelIr` JSON); `env_hash`
/// folds the engine version and the `adapter_name` (the dialect identity, e.g.
/// `WarehouseAdapter::dialect().name()`), never the hostname. The input side of
/// the triple is derived later, in [`RunOutput::to_run_record`], from the
/// observed upstream signatures — see [`derive_input_identity`].
///
/// For a `time_interval` model, pass the **static** IR (`window = None`, e.g.
/// `model.to_model_ir()`) so the recipe hash is partition-invariant.
pub(crate) fn recipe_identity_internal(
    model_ir: &rocky_ir::ModelIr,
    adapter_name: &str,
) -> RecipeIdentityInternal {
    let recipe_hash = rocky_core::recipe_identity::recipe_hash(model_ir)
        .to_hex()
        .to_string();
    let env = rocky_core::recipe_identity::EnvIdentity {
        engine_version: env!("CARGO_PKG_VERSION").to_string(),
        adapter: adapter_name.to_string(),
        exec_config: std::collections::BTreeMap::new(),
    };
    let env_hash = rocky_core::recipe_identity::env_hash(&env)
        .to_hex()
        .to_string();
    RecipeIdentityInternal {
        recipe_hash,
        env_hash,
        hash_scheme: rocky_core::recipe_identity::HASH_SCHEME.to_string(),
    }
}

fn derive_input_identity(mat: &MaterializationOutput) -> (Option<String>, Option<String>) {
    let Some(skip) = mat.skip_internal.as_ref() else {
        return (None, None);
    };
    let Some(skip_hash) = skip.skip_hash.as_deref() else {
        return (None, None);
    };
    if skip.upstream_freshness.is_empty() {
        return (None, None);
    }
    let upstreams: Vec<rocky_core::recipe_identity::UpstreamIdentity> = skip
        .upstream_freshness
        .iter()
        .map(
            |sig| rocky_core::recipe_identity::UpstreamIdentity::Watermark {
                upstream_key: sig.upstream_key.clone(),
                max_ts: sig.max_ts.map(|ts| ts.to_rfc3339()),
                row_count: sig.row_count,
            },
        )
        .collect();
    let target_identity = mat.asset_key.join(".");
    let input_hash =
        rocky_core::recipe_identity::compute_input_hash(skip_hash, &target_identity, &upstreams)
            .to_hex()
            .to_string();
    let proof_class = rocky_core::recipe_identity::proof_class_for(&upstreams)
        .as_str()
        .to_string();
    (Some(input_hash), Some(proof_class))
}

/// Build the `recipe_manifest.*` warehouse-metadata key-value set for one
/// materialized model, ready to hand to
/// [`GovernanceAdapter::write_recipe_manifest`](rocky_core::traits::GovernanceAdapter::write_recipe_manifest).
///
/// This is the **engine → manifest** name mapping applied at write time. It
/// reads the recipe-identity triple stamped on
/// [`MaterializationOutput::recipe_identity`] (never recomputing it) and folds
/// in the manifest carrier fields, keying each under
/// [`RECIPE_MANIFEST_TBLPROP_PREFIX`](rocky_core::catalog::RECIPE_MANIFEST_TBLPROP_PREFIX):
///
/// | manifest field | `recipe_manifest.*` key | source |
/// |---|---|---|
/// | `manifest_version` | `manifest_version` | constant `"0.1"` |
/// | `hash_scheme` | `hash_scheme` | `recipe_identity.hash_scheme` |
/// | `program_hash` | `program_hash` | `recipe_identity.recipe_hash` |
/// | `env_hash` | `env_hash` | `recipe_identity.env_hash` |
/// | `inputs_hash` | `inputs_hash` | `derive_input_identity` (omitted when `None`) |
/// | `inputs_proof_class` | `inputs_proof_class` | `derive_input_identity` (travels with `inputs_hash`) |
/// | `producer.name` | `producer.name` | constant `"rocky"` |
/// | `producer.version` | `producer.version` | `engine_version` |
/// | `subject.model` | `subject.model` | last segment of `asset_key` |
/// | `subject.run_id` | `subject.run_id` | `run_id` |
/// | `subject.status` | `subject.status` | `"success"` (a materialized model succeeded) |
///
/// The `inputs_hash` / `inputs_proof_class` pair is written **both or
/// neither** — the manifest spec's honesty invariant. On a default
/// (non-`--skip-unchanged`, non-reuse) run neither is present, matching a
/// managed Databricks run that observes no content-hashed inputs.
///
/// Returns `None` when the materialization carries no recipe-identity (a path
/// that did not stamp it) or has no `asset_key` to name the subject — the
/// caller then skips the write for that model.
pub(crate) fn recipe_manifest_properties(
    mat: &MaterializationOutput,
    run_id: &str,
    engine_version: &str,
) -> Option<std::collections::BTreeMap<String, String>> {
    let identity = mat.recipe_identity.as_ref()?;
    let model = mat.asset_key.last()?;

    let prefix = rocky_core::catalog::RECIPE_MANIFEST_TBLPROP_PREFIX;
    let mut props = std::collections::BTreeMap::new();
    let mut put = |field: &str, value: String| {
        props.insert(format!("{prefix}{field}"), value);
    };

    put("manifest_version", "0.1".to_string());
    put("hash_scheme", identity.hash_scheme.clone());
    put("program_hash", identity.recipe_hash.clone());
    put("env_hash", identity.env_hash.clone());

    let (input_hash, proof_class) = derive_input_identity(mat);
    // Both-or-neither: the manifest spec's dependentRequired invariant.
    if let (Some(ih), Some(pc)) = (input_hash, proof_class) {
        put("inputs_hash", ih);
        put("inputs_proof_class", pc);
    }

    put("producer.name", "rocky".to_string());
    put("producer.version", engine_version.to_string());
    put("subject.model", model.clone());
    put("subject.run_id", run_id.to_string());
    put("subject.status", "success".to_string());

    Some(props)
}

/// The per-model build decision the engine made this run — what the
/// skip-gate and content-addressed reuse machinery actually decided.
///
/// Reporting-only: this never changes build behavior, it surfaces a decision
/// that today only reaches the run log. Populated on
/// [`RunOutput::model_decisions`] only when the `--skip-unchanged` gate is
/// active or `[reuse]` is enabled; a default run (both off) emits an empty
/// list and the field is omitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum ModelDecision {
    /// The model was (re-)materialized this run — its SQL ran (or its
    /// content-addressed write executed).
    Build,
    /// The `--skip-unchanged` gate proved the model's logic and upstream
    /// data unchanged since the last successful build and skipped
    /// re-materialization.
    Skip,
    /// A content-addressed model pointed-to a prior run's already-written
    /// parquet via a zero-copy commit instead of executing its SQL.
    Reused,
}

/// One per-model decision entry on [`RunOutput::model_decisions`].
///
/// Surfaces the skip/build/reuse verdict the engine reached for a single
/// transformation model, plus a short human-readable reason that mirrors the
/// gate's actual decision point (never re-derived — threaded from the
/// evaluation result). Orchestrators (Dagster) and the VS Code extension use
/// it to explain *why* a model ran, was skipped, or was reused.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct ModelDecisionOutput {
    /// The model's name (matches the model entry in the project DAG).
    pub model: String,
    /// What the engine decided for this model.
    pub decision: ModelDecision,
    /// Short human-readable justification reflecting the exact decision the
    /// gate made (e.g. "logic and upstream data unchanged since last build",
    /// "not skip-eligible", "upstream data may have changed", "reused prior
    /// run's bytes (strong proof)").
    pub reason: String,
}

/// One model withheld from a run because an upstream failed (or was itself
/// withheld) and failure-containment continued the disjoint subgraphs.
///
/// This is the blast radius of a failure — the run's `errors[]` name the root
/// cause(s). A withheld model was **not built** this run: its target was left
/// untouched (never rebuilt on a failed upstream's stale/missing output). This
/// is model-graph containment, distinct from the quality pipeline's row-level
/// `quarantine` surface.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct ContainedModelOutput {
    /// The withheld model's name (matches the model entry in the project DAG).
    pub model: String,
    /// The failed-or-withheld upstream(s) that reach this model — its direct
    /// poisoned dependencies. The run's `errors[]` carry the root-cause detail.
    pub blocked_by: Vec<String>,
    /// Operator hint: resolve the named upstream failure(s), then re-run.
    pub unblock_hint: String,
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
///
/// ## Phase 2 additions (Cluster 3 B)
///
/// `plan_id`, `plan_kind`, `created_at`, `models`, and `execution_layers`
/// are additive — all have `skip_serializing_if` so existing fixtures and
/// consumers that do not include a compile step remain byte-stable. When
/// `rocky plan` runs against a project with a `models/` directory, these
/// fields are populated and the plan is persisted to `.rocky/plans/`.
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

    // ---- D-3 stage 2: pre-execution budget diagnostics ------------------
    //
    // E027 diagnostics emitted when `rocky plan` resolves real catalog
    // stats (via `DESCRIBE DETAIL` / Iceberg snapshot summary) and finds
    // a model whose projected cost exceeds its declared `[budget]`
    // ceiling. Empty when no models have a `[budget]` block, when no
    // real stats are available (non-Databricks / non-Iceberg adapters),
    // or when the project has no `models/` directory.
    //
    // A non-empty list does NOT by itself fail `rocky plan` — severity
    // follows the per-model `on_breach` policy (`warn` or `error`).
    // Callers that want strict pre-flight enforcement should check
    // `has_budget_errors` instead.
    /// Per-model E027 budget-exceeded diagnostics produced at plan time
    /// using real catalog statistics. Severity reflects the model's
    /// `on_breach` policy (`"warn"` or `"error"`). Empty when no
    /// ceiling was exceeded or no real stats were available.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub budget_diagnostics: Vec<Diagnostic>,
    /// `true` when at least one entry in `budget_diagnostics` has
    /// error-level severity (`on_breach = "error"`). Callers can use
    /// this flag to fail a pipeline-as-code check without inspecting
    /// individual diagnostic severities.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub has_budget_errors: bool,

    // ---- Phase 2 plan-spine fields (Cluster 3 B) ------------------------
    //
    // Populated when `rocky plan` compiles a `models/` directory and
    // persists a run plan. Absent for replication-only invocations and
    // projects without a `models/` directory, so existing consumers are
    // byte-stable.
    /// Full 64-char blake3 plan identifier. Present when the plan was
    /// persisted to `.rocky/plans/<plan_id>.json`. Apply with:
    /// `rocky apply <plan_id>`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_id: Option<String>,
    /// Plan kind wire name (`"run"`). Present when `plan_id` is present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_kind: Option<String>,
    /// UTC timestamp when the plan was persisted. Present when `plan_id`
    /// is present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
    /// Qualified model names that will be executed by `rocky apply`. Empty
    /// for replication-only plans. Informational — re-derived at apply time.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub models: Vec<String>,
    /// Execution layers (topological order) as a list-of-lists of model
    /// names. Models within a layer can execute concurrently. Informational —
    /// re-derived at apply time. Empty for replication-only plans.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub execution_layers: Vec<Vec<String>>,

    // ---- Semantic breaking-change verdict (decision-support) ------------
    //
    // Populated only when `rocky plan --semantic` runs AND a baseline
    // compile is available (the `--base` git ref's models compiled
    // cleanly alongside the working tree). The verdict is REPORTING-ONLY:
    // it never gates, skips, or alters which statements are planned, and
    // a `breaking` finding does not change the exit code. The hard gate
    // lives on `rocky plan promote`.
    //
    // `None` (omitted) on a default `rocky plan`, when `--semantic` is
    // absent, or when no baseline could be materialized — so existing
    // consumers and fixtures stay byte-stable.
    /// Semantic change-impact verdict from the typed-IR breaking-change
    /// classifier, surfaced as decision-support at plan time. Present only
    /// when `--semantic` ran with a usable baseline. See
    /// [`SemanticPlanVerdict`] — note its `caveat`: the classifier diffs
    /// OUTPUT SCHEMA only and is blind to schema-stable value changes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub breaking_verdict: Option<SemanticPlanVerdict>,
}

/// Decision-support verdict from the typed-IR breaking-change classifier,
/// attached to `PlanOutput` when `rocky plan --semantic` runs against a
/// usable baseline.
///
/// # Scope and blindness
///
/// The classifier diffs the typed **output schema** of each model between
/// the baseline git ref and the working tree (column drop/add/type
/// narrowing, nullability, materialization keys, masks, target rename).
/// It is **blind to schema-stable value changes**: a `WHERE` / `JOIN`-key
/// / `CASE` rewrite that changes every output row but leaves the column
/// list and types untouched produces **no finding**. An empty `findings`
/// list therefore means "no output-schema change was detected" — it is
/// **not** a completeness or safety signal that the data is unchanged.
/// The [`Self::caveat`] field carries this statement verbatim so a
/// consumer that only reads the JSON cannot miss it.
///
/// # Reporting-only
///
/// This verdict never gates the plan. Even a `breaking`-severity finding
/// leaves the planned statements and the process exit code unchanged. The
/// hard gate (which blocks on `breaking`) lives on `rocky plan promote`.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct SemanticPlanVerdict {
    /// Verbatim statement of what the classifier does and does not see.
    /// Always populated — present even when `findings` is empty, because
    /// "no findings" is the case most easily misread as "safe". See the
    /// type-level docs for why this is load-bearing.
    pub caveat: String,
    /// The git ref the working tree was diffed against (the `--base`
    /// value, default `"main"`).
    pub base_ref: String,
    /// Classified output-schema findings (one per detected change),
    /// including `info`-severity entries. Each finding carries its own
    /// `model`, tagged `change.kind`, and `severity`
    /// (`breaking` / `warning` / `info`). Empty when no output-schema
    /// change was detected — which, per `caveat`, is not a safety signal.
    pub findings: Vec<rocky_core::breaking_change::BreakingFinding>,
}

/// Verbatim caveat text embedded in every [`SemanticPlanVerdict::caveat`].
///
/// Single source of truth so the JSON payload and the text-mode render
/// never drift. States plainly that the classifier diffs OUTPUT SCHEMA and
/// is blind to schema-stable value changes, and that an empty finding list
/// is not a completeness signal.
pub const SEMANTIC_VERDICT_CAVEAT: &str = "This verdict diffs OUTPUT SCHEMA only \
(columns, types, nullability, materialization keys, masks, target). It is BLIND to \
schema-stable value changes: a WHERE / JOIN-key / CASE rewrite that changes every output \
row but not the schema produces no finding. An empty findings list means no output-schema \
change was detected — it is NOT a signal that the data or the model is unchanged.";

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
    /// Model-level governance tags — the model's own `[tags]` block merged
    /// over any config-group `[tags]` baseline (sidecar > group). Free-form
    /// key/value strings describing the model as a whole (`domain`, `tier`,
    /// `owner`, …). `dagster-rocky` projects these onto the derived asset's
    /// Dagster tags, so a governed fan-out declared once on a config group is
    /// visible to the orchestrator end-to-end. Empty when none are declared.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub tags: std::collections::BTreeMap<String, String>,
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
    /// Per-model outcomes for the (DuckDB-backed) model-execution test —
    /// passes too, not just failures. Lets the VS Code Inspector Tests tab
    /// and the dagster integration render "good_mart: pass" without
    /// inferring it from `total - failures`. Empty when only declarative
    /// tests ran. Filtered to `--model` when that flag is set.
    #[serde(default)]
    pub model_results: Vec<ModelTestResult>,
    /// Results from declarative `[[tests]]` in model sidecars.
    /// Present only when `--declarative` is used.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub declarative: Option<DeclarativeTestSummary>,
    /// Results from fixture-driven `[[test]]` unit tests in model sidecars.
    /// Present only when at least one model declares a `[[test]]` block.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unit_tests: Option<UnitTestSummary>,
}

/// One failed test, mirroring the (name, error) tuple in
/// `rocky_engine::test_runner::TestResult::failures` but with named fields
/// because schemars/JSON Schema can't represent positional tuples cleanly.
#[derive(Debug, Serialize, JsonSchema)]
pub struct TestFailure {
    pub name: String,
    pub error: String,
}

/// One per-model outcome from the local model-execution test.
///
/// `status` is `"pass"` or `"fail"`. `error` is set only when `status =
/// "fail"`. Mirrors `rocky_engine::test_runner::ModelTestResult` with the
/// status flattened to a string so consumers (Pydantic, TypeScript) get a
/// stable, JSON-Schema-friendly shape.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ModelTestResult {
    pub model: String,
    /// `"pass"` or `"fail"`.
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
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

/// Summary of fixture-driven unit-test execution (from `[[test]]` blocks in
/// model sidecars). The per-test `results` reuse the engine's
/// [`rocky_core::unit_test::UnitTestResult`] shape (model, test, passed,
/// error, and row-level mismatches).
#[derive(Debug, Serialize, JsonSchema)]
pub struct UnitTestSummary {
    pub total: usize,
    pub passed: usize,
    pub failed: usize,
    pub results: Vec<rocky_core::unit_test::UnitTestResult>,
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
            model_results: Vec::new(),
            declarative: None,
            unit_tests: None,
        }
    }

    /// Attach per-model results from the engine test runner. Returns `self`
    /// so callers can chain it onto `new(...)`.
    pub fn with_model_results(mut self, results: Vec<ModelTestResult>) -> Self {
        self.model_results = results;
        self
    }

    /// Attach fixture-driven unit-test results. Returns `self` for chaining.
    pub fn with_unit_tests(mut self, summary: UnitTestSummary) -> Self {
        self.unit_tests = Some(summary);
        self
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
    /// Classified semantic findings from the typed-IR breaking-change
    /// classifier. Only populated when `ci-diff` is invoked with
    /// `--semantic` and both base + HEAD compiles succeed; omitted from
    /// JSON output when empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub breaking_findings: Vec<rocky_core::breaking_change::BreakingFinding>,
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
            breaking_findings: Vec::new(),
        }
    }

    /// Attach semantic breaking-change findings to this output.
    pub fn with_breaking_findings(
        mut self,
        findings: Vec<rocky_core::breaking_change::BreakingFinding>,
    ) -> Self {
        self.breaking_findings = findings;
        self
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
    /// Rolling statistics over the most recent N successful executions.
    /// Present only when `--rolling-stats` is passed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rolling_stats: Option<RollingStats>,
}

/// Rolling statistics computed over the most recent N successful executions
/// of a model. Populated by `rocky history --model <name> --rolling-stats`.
///
/// Statistics use population standard deviation (divided by N, not N-1),
/// so `std_dev` is exactly 0 when all samples are equal.
#[derive(Debug, Serialize, JsonSchema)]
pub struct RollingStats {
    /// Maximum number of executions requested for the rolling window.
    pub window: usize,
    /// Actual number of successful executions used (≤ window; may be
    /// smaller when model history is shorter than the requested window).
    pub samples: usize,
    /// Rolling statistics for the `rows_affected` dimension.
    /// Computed only over executions where `rows_affected` is not null.
    pub rows_affected: RollingDimension,
    /// Rolling statistics for the `duration_ms` dimension.
    pub duration_ms: RollingDimension,
    /// Composite health score in `[0.0, 1.0]`.
    ///
    /// Computed as `1.0 - clamp((max(|z_rows|, |z_duration|) - 2.0) / 4.0, 0.0, 1.0)`.
    /// A score of `1.0` means both z-scores are within 2σ of the mean;
    /// `0.0` means at least one z-score is 6σ or more.
    pub health_score: f64,
}

/// Per-dimension rolling statistics (mean, std dev, latest z-score).
#[derive(Debug, Serialize, JsonSchema)]
pub struct RollingDimension {
    /// Population mean over the sample window.
    pub mean: f64,
    /// Population standard deviation (÷N) over the sample window.
    pub std_dev: f64,
    /// Z-score of the most recent execution relative to the window.
    ///
    /// `None` when fewer than 2 samples are available or when `std_dev`
    /// is exactly 0 (all samples are equal — no meaningful deviation).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_z_score: Option<f64>,
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
    /// The recipe-identity triple recorded for this execution, when
    /// present. See [`RecipeIdentityView`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recipe_identity: Option<RecipeIdentityView>,
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
    /// The recipe-identity triple recorded for this execution, when
    /// present. See [`RecipeIdentityView`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recipe_identity: Option<RecipeIdentityView>,
}

// ── Recipe identity surface ─────────────────────────────────────────────────
//
// The (recipe_hash, input_hash, env_hash) triple, surfaced on the model
// records of `history` / `trace` / `catalog` and queried by
// `rocky history --recipe <hash>`. Read back from the persisted
// `ModelExecution`; the internal capture path lives in `RecipeIdentityInternal`
// above. Kept in one contiguous block.

/// The recipe-identity triple surfaced on a model record — the answer to
/// "what exact program, over what inputs, in what environment produced this?".
///
/// Read back from the persisted [`rocky_core::state::ModelExecution`]. Every
/// field is optional: a record written before the triple was captured (state
/// schema predating it) or a failed execution carries none of them, and the
/// input side is absent on the default run path (which observes no inputs).
/// The whole object is omitted from JSON when nothing was recorded — see
/// [`Self::from_execution`] — so output for pre-triple records is unchanged.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct RecipeIdentityView {
    /// The program **identity** key: blake3 (hex) of the canonical
    /// `ModelIr` JSON. Stable across environments and engine versions for
    /// the same program text. The value `rocky history --recipe <hash>`
    /// filters on.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recipe_hash: Option<String>,
    /// The **input** key: blake3 (hex) over the run's observed input
    /// identities. Present only when the run actually observed inputs (the
    /// `--skip-unchanged` gate's upstream freshness signatures, or the
    /// content-addressed reuse spine); absent on the default run path.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_hash: Option<String>,
    /// Strength of [`Self::input_hash`]: `"strong"` (every observed upstream
    /// is a content hash — offline byte-verifiable) or `"heuristic"` (at
    /// least one is a freshness signature, attesting freshness rather than
    /// byte-identity). Carried so a weak input hash is never presented as a
    /// content claim. `None` whenever [`Self::input_hash`] is `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_proof_class: Option<String>,
    /// The **environment** key: blake3 (hex) over the engine version and the
    /// adapter / dialect identity. Excludes the hostname by construction.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env_hash: Option<String>,
    /// The hash-scheme tag (`"v1"`) in force when the triple was computed,
    /// so a future canonicalisation change is an explicit new scheme rather
    /// than a silent history fork.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash_scheme: Option<String>,
}

impl RecipeIdentityView {
    /// Read the surfaced triple from a persisted [`ModelExecution`], or
    /// `None` when the execution recorded no recipe identity at all (a
    /// pre-triple or failed record). Returning `None` keeps the enclosing
    /// model record's JSON byte-identical to its pre-triple shape.
    ///
    /// [`ModelExecution`]: rocky_core::state::ModelExecution
    #[must_use]
    pub fn from_execution(exec: &rocky_core::state::ModelExecution) -> Option<Self> {
        if exec.recipe_hash.is_none()
            && exec.input_hash.is_none()
            && exec.input_proof_class.is_none()
            && exec.env_hash.is_none()
            && exec.hash_scheme.is_none()
        {
            return None;
        }
        Some(Self {
            recipe_hash: exec.recipe_hash.clone(),
            input_hash: exec.input_hash.clone(),
            input_proof_class: exec.input_proof_class.clone(),
            env_hash: exec.env_hash.clone(),
            hash_scheme: exec.hash_scheme.clone(),
        })
    }
}

/// JSON output for `rocky history --recipe <hash>` — every recorded
/// execution of one exact program (`recipe_hash`), across all runs.
///
/// The "what produced this?" query: given a `recipe_hash` (read from any
/// model record's [`RecipeIdentityView`]), list every time that exact program
/// ran — newest run first — with the run it belonged to and its per-execution
/// input / environment identity.
#[derive(Debug, Serialize, JsonSchema)]
pub struct RecipeHistoryOutput {
    pub version: String,
    pub command: String,
    /// The `recipe_hash` the history was filtered on, echoed back.
    pub recipe_hash: String,
    pub executions: Vec<RecipeExecutionRecord>,
    pub count: usize,
}

/// One execution of a given `recipe_hash`, embedded in
/// [`RecipeHistoryOutput`].
#[derive(Debug, Serialize, JsonSchema)]
pub struct RecipeExecutionRecord {
    /// The run this execution belonged to.
    pub run_id: String,
    /// Model name as recorded in the run.
    pub model_name: String,
    pub started_at: DateTime<Utc>,
    pub duration_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_affected: Option<u64>,
    pub status: String,
    pub sql_hash: String,
    /// The recipe-identity triple for this execution. Its `recipe_hash`
    /// matches the top-level filter; `input_hash` / `env_hash` can differ
    /// run-to-run for the same program (different inputs, different engine
    /// version).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recipe_identity: Option<RecipeIdentityView>,
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
    /// Path of the body file (`.rocky` or `.sql`) written to the models
    /// directory, when emission succeeded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body_path: Option<String>,
    /// Path of the `.toml` sidecar written next to the body, when emission
    /// succeeded. The sidecar carries the materialization strategy and
    /// target coordinates so Rocky's model loader picks the generated
    /// model up without manual editing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sidecar_path: Option<String>,
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

/// JSON output for `rocky ai-contract <model>`.
///
/// Reports the AI-drafted data contract for a model, grounded in the observed
/// per-column profile of its target table. The drafted contract is
/// compile-verified against the model before it's reported, so a successful
/// response is a contract that `rocky compile` accepts.
#[derive(Debug, Serialize, JsonSchema)]
pub struct AiContractOutput {
    pub version: String,
    pub command: String,
    /// The model the contract was drafted for.
    pub model: String,
    /// Number of LLM attempts taken to reach a compile-verified contract.
    pub attempts: usize,
    /// The drafted contract serialized as `.contract.toml`.
    pub contract_toml: String,
    /// Path the contract was written to, when `--save` was passed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub saved_path: Option<String>,
    /// The observed per-column profile that grounded the draft.
    pub profile: Vec<AiContractColumnProfile>,
}

/// Observed profile of one column, as reported by `rocky ai-contract`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct AiContractColumnProfile {
    pub name: String,
    /// Inferred Rocky type name.
    #[serde(rename = "type")]
    pub type_name: String,
    pub rows: u64,
    pub nulls: u64,
    pub null_rate: f64,
    pub distinct: u64,
    /// Observed low-cardinality domain. Empty above the cardinality cap. This
    /// is reported as evidence; it is not encoded into the contract file.
    pub observed_values: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<String>,
}

/// JSON output for `rocky profile <model> [--column <col>]`.
///
/// A per-column data profile (row / null / distinct counts, min / max, and the
/// low-cardinality domain) for a model's target table, computed by a single
/// aggregate query per column. DuckDB-only this release; a non-DuckDB target
/// reports `unavailable` with an empty `columns` list rather than erroring.
///
/// When the model's declared target isn't materialized (the agentic authoring
/// loop pre-`rocky run`, or a replication-pipeline POC that doesn't run the
/// transformation models), profile falls back to the model's first resolvable
/// source table. The fallback is labelled via `profiled_table` (the table
/// actually queried) and `fell_back_from` (the missing target), so consumers
/// can surface "source preview, not model output" in the UI.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ProfileOutput {
    pub version: String,
    pub command: String,
    pub model: String,
    /// Fully-qualified table actually profiled (e.g. `staging.raw_orders` or
    /// `raw__orders.orders`). When `fell_back_from` is set, this differs from
    /// the model's declared target. Omitted when `unavailable` is set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profiled_table: Option<String>,
    /// When set, profile fell back to a source because the model's declared
    /// target wasn't materialized; carries the declared-target FQN that was
    /// missing. `None` when the declared target was profiled directly.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fell_back_from: Option<String>,
    /// One entry per profiled column.
    pub columns: Vec<ProfileColumnStats>,
    /// Set when profiling could not run (e.g. a non-DuckDB target this release).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unavailable: Option<String>,
}

/// Observed data profile for one column.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ProfileColumnStats {
    pub name: String,
    /// Inferred Rocky type name.
    #[serde(rename = "type")]
    pub type_name: String,
    pub rows: u64,
    pub nulls: u64,
    pub null_rate: f64,
    pub distinct: u64,
    /// Observed low-cardinality domain (empty above the cardinality cap).
    pub observed_values: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<String>,
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
    /// Per-node metadata for every model referenced by this lineage view
    /// (the focal model plus each endpoint of `edges`). Lets consumers
    /// (e.g. the VS Code subgraph drill-in) cluster nodes by their
    /// resolved target schema or source identity instead of parsing the
    /// qualified node name. Empty when no nodes were resolved; older
    /// JSON payloads cached locally may omit the field entirely.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nodes: Vec<LineageNodeDef>,
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
    /// Every downstream column that transitively consumes
    /// `(model, column)`, deduplicated and deterministically sorted. An
    /// author-time "what does changing this column affect" signal,
    /// always populated regardless of `direction` so the default
    /// (upstream) trace still carries the blast radius. Inspection only —
    /// this never feeds a build/skip/reuse decision. Empty when the
    /// column has no consumers.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub downstream_consumers: Vec<LineageQualifiedColumn>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct LineageColumnDef {
    pub name: String,
    /// Inferred column type, rendered as a Rocky type string (e.g.
    /// `STRING`, `INT64`, `DECIMAL(10,2)`, `TIMESTAMP`). Omitted when
    /// the type could not be inferred — typically a `SELECT *` against
    /// an upstream whose schema isn't cached, or a model that did not
    /// pass typecheck.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_type: Option<String>,
}

/// Per-node metadata for the lineage graph.
///
/// One entry per distinct model referenced by `LineageOutput.edges`
/// (plus the focal model). Carries cluster keys consumers can use to
/// group nodes without having to parse the qualified node name. Either
/// optional field may be absent — `target_schema` is `None` for nodes
/// that aren't project models, and `source_id` is `None` for nodes that
/// are project models (i.e. the two fields are mutually exclusive in
/// practice).
#[derive(Debug, Serialize, JsonSchema)]
pub struct LineageNodeDef {
    /// Qualified node identifier as it appears in `edges[].source.model`
    /// and `edges[].target.model`. Stable across the rest of the payload.
    pub model: String,
    /// Resolved target schema for project models, from the model's
    /// declared target config. Omitted for external sources and any
    /// node Rocky couldn't resolve to a project model.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_schema: Option<String>,
    /// Source identifier for nodes that represent an external source
    /// (i.e. a referenced table outside the project model set). The
    /// value mirrors how the SQL referenced the source so consumers can
    /// key on it directly. Omitted for project models.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_id: Option<String>,
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

/// JSON output for `rocky catalog`.
///
/// A persisted, queryable snapshot of column-level lineage across the
/// project. Emitted to `./.rocky/catalog/catalog.json` (default) so any
/// non-Rocky consumer (BI tool, governance dashboard, PR bot) can read
/// the artifact without invoking the engine. The CLI's own stdout is a
/// one-screen summary; this struct is the structured payload backing
/// `--output json`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct CatalogOutput {
    pub version: String,
    pub command: String,
    pub generated_at: DateTime<Utc>,
    /// Pipeline name used to build the catalog. When the project has
    /// multiple pipelines, this is the first one in declaration order.
    pub project_name: String,
    /// Stable fingerprint of the `rocky.toml` bytes. Lets a downstream
    /// consumer detect whether the catalog was built against a config
    /// that has since changed.
    pub config_hash: String,
    /// Identifier of the last successful run that produced these assets,
    /// when known. Absent until the catalog is enriched with run-history
    /// metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_run_id: Option<String>,
    pub assets: Vec<CatalogAsset>,
    pub edges: Vec<CatalogEdge>,
    pub stats: CatalogStats,
}

/// A single asset (model or source) in the catalog.
#[derive(Debug, Serialize, JsonSchema)]
pub struct CatalogAsset {
    /// Fully-qualified target identifier (`catalog.schema.table`) when
    /// resolvable, otherwise the model name.
    pub fqn: String,
    /// Model or source name as it appears in lineage edges.
    pub model_name: String,
    /// Whether the asset is a project-managed model or an external source.
    pub kind: AssetKind,
    pub columns: Vec<CatalogColumn>,
    pub upstream_models: Vec<String>,
    pub downstream_models: Vec<String>,
    /// Free-form natural-language description of the asset's purpose,
    /// when supplied via the model's sidecar config.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub intent: Option<String>,
    /// Timestamp of the last successful materialization, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_materialized_at: Option<DateTime<Utc>>,
    /// Identifier of the last successful run that produced the asset,
    /// when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_run_id: Option<String>,
    /// Recipe-identity triple from the asset's most recent successful
    /// materialization, when known. See [`RecipeIdentityView`]. `None` for
    /// source-kind assets (no execution) and for models built before the
    /// triple was captured.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recipe_identity: Option<RecipeIdentityView>,
}

/// A column on a catalog asset.
#[derive(Debug, Serialize, JsonSchema)]
pub struct CatalogColumn {
    pub name: String,
    /// Declared or inferred type of the column, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_type: Option<String>,
    /// Whether the column accepts nulls, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nullable: Option<bool>,
    /// Human-readable description from the sidecar `[columns]` table, when set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// A single column-level lineage edge.
#[derive(Debug, Serialize, JsonSchema)]
pub struct CatalogEdge {
    pub source_model: String,
    pub source_column: String,
    pub target_model: String,
    pub target_column: String,
    /// Transform kind: "direct", "cast", "expression", or
    /// "aggregation: <fn>". Stringified to match the existing lineage
    /// edge shape.
    pub transform: String,
    /// Confidence the producing edge is fully understood. `Medium` is
    /// emitted for star-expanded projections (where lineage is inferred
    /// rather than parsed from an explicit projection); `High` for
    /// edges sourced from explicit columns.
    pub confidence: EdgeConfidence,
}

/// Confidence grading for a [`CatalogEdge`].
///
/// Coarse-grained on purpose: every consumer threshold-checks at "good
/// enough", and a 3-bucket enum is simpler than a numeric score. A
/// numeric `confidence_score` field can be added alongside this enum
/// later without breaking callers.
#[derive(Debug, Serialize, JsonSchema)]
pub enum EdgeConfidence {
    High,
    Medium,
    Low,
}

/// Asset kind discriminator for [`CatalogAsset`].
#[derive(Debug, Serialize, JsonSchema)]
pub enum AssetKind {
    Source,
    Model,
    View,
    MaterializedView,
}

/// Aggregate counts for the emitted catalog.
#[derive(Debug, Serialize, JsonSchema)]
pub struct CatalogStats {
    pub asset_count: usize,
    pub column_count: usize,
    pub edge_count: usize,
    /// Number of assets whose lineage was derived from `SELECT *` (and
    /// is therefore inferred rather than parsed). Surfaces partial-lineage
    /// coverage as a single number in the summary.
    pub assets_with_star: usize,
    /// Number of columns with no producing edge — typically the result
    /// of a star expansion that could not resolve an upstream column.
    pub orphan_columns: usize,
    pub duration_ms: u64,
}

/// JSON output for `rocky state show`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct StateOutput {
    pub version: String,
    pub command: String,
    pub watermarks: Vec<WatermarkEntry>,
    /// redb state-schema version this binary supports. Pair with
    /// `schema_version_on_disk` to make a compatibility decision without
    /// parsing a human error string: `on_disk > supported` means the file was
    /// written by a newer engine (forward-incompatible). See the
    /// state-schema-deploy-safety contract.
    pub schema_version_supported: u32,
    /// redb state-schema version stamped in the on-disk state file, or `null`
    /// when no state file exists yet or it predates schema versioning.
    pub schema_version_on_disk: Option<u32>,
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

/// JSON output for `rocky state retention sweep`.
///
/// Mirrors [`rocky_core::retention::SweepReport`] with the
/// version/command envelope every CLI JSON output carries. `dry_run = true`
/// reports counts that *would* result without touching the state store.
#[derive(Debug, Serialize, JsonSchema)]
pub struct RetentionSweepOutput {
    pub version: String,
    pub command: String,
    /// `true` when `--dry-run` was set; the state store was left untouched.
    pub dry_run: bool,
    /// Configured retention window in days at the time of the sweep.
    pub max_age_days: u32,
    /// Configured per-domain floor at the time of the sweep.
    pub min_runs_kept: u32,
    /// Domains that were considered in this sweep, as the canonical
    /// lowercase strings (`"history"`, `"lineage"`, `"audit"`).
    pub domains: Vec<String>,
    /// Run records (`run_history`) deleted, or that would be deleted in
    /// dry-run mode.
    pub runs_deleted: u64,
    /// Run records remaining after the sweep.
    pub runs_kept: u64,
    /// DAG snapshots (`dag_snapshots`) deleted, or that would be deleted.
    pub lineage_deleted: u64,
    /// DAG snapshots remaining.
    pub lineage_kept: u64,
    /// Quality snapshots (`quality_history`) deleted, or that would be
    /// deleted.
    pub audit_deleted: u64,
    /// Quality snapshots remaining.
    pub audit_kept: u64,
    /// JSONL trace files removed by the last-N-by-mtime sweep. Always
    /// zero for the explicit `rocky state retention sweep` command —
    /// the trace sweep is only invoked from `rocky run`'s end-of-run
    /// auto-sweep (Arc 4 span retention).
    pub traces_deleted: u64,
    /// Wall-clock duration of the sweep in milliseconds.
    pub duration_ms: u64,
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
///
/// Two shapes share this struct:
///
/// - **Single-model (`rocky compact <fqn>`)**: `model` is set; `scope`,
///   `catalog`, `tables`, and `totals` are absent. Byte-stable with
///   envelopes that predate the catalog-scope flag.
/// - **Catalog scope (`rocky compact --catalog <name>`)**: `model` is
///   absent, `scope = "catalog"`, `catalog` is set, `tables` keys per-FQN
///   statement bundles, and `totals` carries aggregate counts. The flat
///   `statements` field still carries every SQL statement across all
///   tables for consumers that just iterate it.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct CompactOutput {
    pub version: String,
    pub command: String,
    /// Set when invoked as `rocky compact <fqn>`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Set when invoked as `rocky compact --catalog <name>`. Stores the
    /// catalog identifier as resolved (lowercased to match the
    /// managed-table resolver's normalization).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog: Option<String>,
    /// `"catalog"` for the catalog-scoped path; absent for single-model
    /// invocations to keep their envelope byte-stable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    pub dry_run: bool,
    pub target_size_mb: u64,
    /// Flat list of every SQL statement across every table. Single-model
    /// invocations have one bundle here; `--catalog` invocations have
    /// the concatenation of every per-table bundle (in the same order
    /// as `tables`'s key iteration order).
    pub statements: Vec<NamedStatement>,
    /// Per-table breakdown, keyed by fully-qualified table name. Present
    /// only on `--catalog` invocations.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tables: Option<BTreeMap<String, CompactTableEntry>>,
    /// Aggregate counts across the catalog. Present only on `--catalog`
    /// invocations.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub totals: Option<CompactTotals>,
    /// Plan identifier (full 64-char blake3 hex). Populated when the plan is
    /// persisted to `.rocky/plans/` so it can be applied later via
    /// `rocky compact apply <plan_id>`. Absent when plan persistence is skipped
    /// (e.g. `--measure-dedup` path).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan_id: Option<String>,
}

/// Per-table compaction plan inside a `--catalog` envelope.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct CompactTableEntry {
    pub statements: Vec<NamedStatement>,
}

/// Aggregate counts over a `rocky compact --catalog` invocation.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct CompactTotals {
    pub table_count: usize,
    pub statement_count: usize,
}

/// Named SQL statement (purpose + sql), reused by compact and archive.
///
/// `sql` is optional in preparation for the v2 persisted plan format
/// (Cluster 3 C — "SQL as `.o` files"). Today's emission path always
/// populates `Some(string)` for both stdout JSON and the persisted plan;
/// the v2 format will write `None` and rely on the apply path to
/// regenerate SQL from the plan's typed IR.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct NamedStatement {
    pub purpose: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub sql: Option<String>,
}

/// JSON output for `rocky archive`.
///
/// Mirrors [`CompactOutput`]: single-model invocations populate `model`
/// and leave the catalog-scope fields absent; `--catalog` invocations
/// populate `catalog`, `scope = "catalog"`, `tables`, and `totals`. The
/// flat `statements` list carries every statement across every table.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ArchiveOutput {
    pub version: String,
    pub command: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Set when invoked as `rocky archive --catalog <name>`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog: Option<String>,
    /// `"catalog"` for the catalog-scoped path; absent for single-model
    /// invocations.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    pub older_than: String,
    pub older_than_days: u64,
    pub dry_run: bool,
    pub statements: Vec<NamedStatement>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tables: Option<BTreeMap<String, ArchiveTableEntry>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub totals: Option<ArchiveTotals>,
    /// Plan identifier (full 64-char blake3 hex). Populated when the plan is
    /// persisted to `.rocky/plans/` so it can be applied later via
    /// `rocky archive apply <plan_id>`. Absent when plan persistence is skipped.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan_id: Option<String>,
}

/// Per-table archive plan inside a `--catalog` envelope.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ArchiveTableEntry {
    pub statements: Vec<NamedStatement>,
}

/// Aggregate counts over a `rocky archive --catalog` invocation.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ArchiveTotals {
    pub table_count: usize,
    pub statement_count: usize,
}

/// Result of executing one SQL statement during `rocky compact apply` or
/// `rocky archive apply`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct StatementResult {
    /// Human-readable purpose label (mirrors `NamedStatement.purpose`).
    pub purpose: String,
    /// The SQL that was executed.
    pub sql: String,
    /// Whether the adapter accepted the statement without error.
    pub success: bool,
    /// Execution duration in milliseconds. Zero when execution was skipped
    /// due to a prior statement failure in the same apply run.
    pub duration_ms: u64,
    /// Adapter error message. `None` when `success == true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// JSON output for `rocky compact apply <plan_id>`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct CompactApplyOutput {
    pub version: String,
    pub command: String,
    /// The plan that was applied.
    pub plan_id: String,
    pub executed_at: DateTime<Utc>,
    /// Per-statement results, in the order they were executed.
    pub statements: Vec<StatementResult>,
    /// `true` when all statements succeeded; `false` on the first failure
    /// (remaining statements are still reported with `success: false`
    /// and `duration_ms: 0`).
    pub success: bool,
}

/// JSON output for `rocky archive apply <plan_id>`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ArchiveApplyOutput {
    pub version: String,
    pub command: String,
    /// The plan that was applied.
    pub plan_id: String,
    pub executed_at: DateTime<Utc>,
    /// Per-statement results, in the order they were executed.
    pub statements: Vec<StatementResult>,
    /// `true` when all statements succeeded; `false` on the first failure.
    pub success: bool,
}

// ---------------------------------------------------------------------------
// Plan + Apply spine (Cluster 3 B, Phase 2)
// ---------------------------------------------------------------------------

/// Operational metadata persisted by `rocky plan` for a run-kind plan.
///
/// ## Design note: operational-metadata-only (no full IR)
///
/// `ProjectIr` round-trip via serde would be a substantial refactor and
/// is deferred to a future phase. Instead we persist the flags that
/// produced this plan — `rocky apply` re-derives the `ProjectIr` by
/// re-compiling with the same flags. The trade-off: apply always re-compiles
/// (fast, CPU-only, no network) rather than deserialising a potentially
/// large IR snapshot. Phase 3 can sharpen this if re-execution from the
/// pinned record without re-compile becomes a requirement.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RunPlan {
    /// Optional filter that was passed to `rocky plan` (e.g. `"client=acme"`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
    /// Pipeline name if `--pipeline` was specified.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pipeline: Option<String>,
    /// Single compiled model to execute (`--model`). Skips replication.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Branch name if `--branch` was specified.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    /// Single partition key (`--partition`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition: Option<String>,
    /// Range lower bound (`--from`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_from: Option<String>,
    /// Range upper bound (`--to`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_to: Option<String>,
    /// Whether `--latest` was specified.
    #[serde(default)]
    pub latest: bool,
    /// Whether `--missing` was specified. Resolved against the state store
    /// at apply time (the partition set is computed when the plan is
    /// applied, not when it is planned).
    #[serde(default)]
    pub missing: bool,
    /// Lookback window (`--lookback`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lookback: Option<u32>,
    /// Partition parallelism (`--parallel`, default 1).
    #[serde(default = "default_parallel")]
    pub parallel: u32,
    /// Whether `--all` was passed (run both replication and models).
    #[serde(default)]
    pub run_all: bool,
    /// Optional governance environment (`--env`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env: Option<String>,
    /// Models directory used for transformation execution (`--models`).
    /// Defaults to `models/` when unset, resolved relative to the config.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub models_dir: Option<String>,
    /// Resume a failed run by `run_id` (`--resume`). At apply time this is
    /// passed through to the run path unchanged.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resume: Option<String>,
    /// Resume the most recent failed run (`--resume-latest`). The actual
    /// `run_id` is resolved against the state store at apply time.
    #[serde(default)]
    pub resume_latest: bool,
    /// Run in shadow mode (`--shadow`). When set, `shadow_suffix` /
    /// `shadow_schema` describe the shadow target. Mutually exclusive with
    /// `branch`; clap rejects the combination at plan time.
    #[serde(default)]
    pub shadow: bool,
    /// Suffix appended to table names in shadow mode (`--shadow-suffix`).
    /// Only meaningful when `shadow == true` or `branch.is_some()`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shadow_suffix: Option<String>,
    /// Override schema for shadow tables (`--shadow-schema`).
    /// Only meaningful when `shadow == true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shadow_schema: Option<String>,
    /// Run all pipelines as a unified DAG (`--dag`). When `true`, apply
    /// dispatches to the DAG runner instead of the standard run path.
    #[serde(default)]
    pub dag: bool,
    /// Caller-supplied idempotency key (`--idempotency-key` or
    /// `ROCKY_IDEMPOTENCY_KEY`). Different keys produce different plan_ids
    /// (the hash discriminates) — there is no key-rewriting at apply time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    /// Governance override resolved at plan time from `--governance-override`
    /// (`@file.json` is read at plan time; the parsed struct is persisted so
    /// apply does not need filesystem access to the original file).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub governance_override: Option<rocky_core::config::GovernanceOverride>,
    /// Qualified model names discovered at plan time. Used to populate
    /// `PlanOutput.models` and surfaced in `rocky apply` dry-run info.
    /// Re-derived at apply time via recompile; this list is informational.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub models: Vec<String>,
    /// Execution layers (topological order) discovered at plan time.
    /// Informational — re-derived at apply time.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub execution_layers: Vec<Vec<String>>,
}

fn default_parallel() -> u32 {
    1
}

// ---------------------------------------------------------------------------
// Supervised backfill (propose-only)
// ---------------------------------------------------------------------------

/// The composed, review-gated recovery plan emitted by `rocky backfill`.
///
/// A backfill re-runs *existing* recipes over a scoped window — it never
/// rewrites SQL to "fix" data. The command composes the set of models to
/// rebuild (the affected models plus their downstream lineage closure), the
/// order to rebuild them in, the partition window where models are
/// partitioned, and an estimated cost. The plan is persisted and **always**
/// requires a human sign-off (`rocky review <plan-id> --approve`) before
/// `rocky apply` will execute it, regardless of any configured policy —
/// backfills are where blast radius hides.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct BackfillOutput {
    /// Rocky version that composed the plan.
    pub version: String,
    /// Always `"backfill"`.
    pub command: String,
    /// What surfaced the affected models: `"manual"` (explicit `--model`) or
    /// `"last_run_failure"` (seeded from the previous run's failed models,
    /// i.e. the contained/quarantined window).
    pub trigger: String,
    /// The persisted plan identifier (64-char blake3 hex). Feed it to
    /// `rocky review <plan-id> --approve` then `rocky apply <plan-id>`.
    pub plan_id: String,
    /// Always `true` — a backfill plan is unconditionally review-gated.
    pub requires_review: bool,
    /// The models that triggered the backfill (the failure/gap seeds), before
    /// the downstream closure is added.
    pub seed_models: Vec<String>,
    /// The full set of models to rebuild — the seeds plus their downstream
    /// lineage closure — in topological (dependency-first) order.
    pub models: Vec<String>,
    /// Topological execution layers over the closure. Models in the same layer
    /// are independent; each layer runs after the previous completes.
    pub execution_layers: Vec<Vec<String>>,
    /// Partition window applied to partitioned models in the closure, when a
    /// `--from`/`--to` range was supplied. Absent when the backfill is a full
    /// rebuild of each model.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_scope: Option<BackfillPartitionScope>,
    /// Best-effort cost projection for the rebuild. Always an *estimate*.
    pub cost_estimate: BackfillCostEstimate,
    /// The exact command that clears the review gate.
    pub review_command: String,
    /// The exact command that executes the plan once reviewed.
    pub apply_command: String,
    /// Human-readable one-line summary.
    pub message: String,
}

/// The partition window a backfill scopes partitioned models to.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct BackfillPartitionScope {
    /// Range lower bound (`--from`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    /// Range upper bound (`--to`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to: Option<String>,
    /// The closure models that declare a partitioned (`time_interval`)
    /// materialization — the ones the window actually applies to. Empty when
    /// no model in the closure is partitioned (the window is then inert).
    pub models: Vec<String>,
}

/// A best-effort, label-as-estimate cost projection for a backfill.
///
/// The projection re-uses the same historical-observed cost formula as
/// `rocky cost`: each closure model's most recent recorded execution is
/// priced by the warehouse cost model. It is offline (no warehouse
/// round-trip) and therefore approximate — `is_estimate` is always `true`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct BackfillCostEstimate {
    /// Always `true` — this is a projection, not a measured cost.
    pub is_estimate: bool,
    /// How the figure was derived: `"historical_observed"` when at least one
    /// closure model had a prior execution to price, else `"unavailable"`.
    pub basis: String,
    /// Summed estimated compute cost in USD, or `None` when no closure model
    /// has priced execution history (or the warehouse is unbilled, e.g.
    /// DuckDB).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_cost_usd: Option<f64>,
    /// Per-model breakdown, in closure order.
    pub per_model: Vec<BackfillModelCost>,
}

/// Per-model entry in a [`BackfillCostEstimate`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct BackfillModelCost {
    /// The model name.
    pub model_name: String,
    /// Estimated compute cost in USD for one rebuild, or `None` when the model
    /// has no priced execution history.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost_usd: Option<f64>,
    /// Observed duration (ms) of the priced historical execution, when found.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    /// The run the historical figure was read from, for provenance.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_run_id: Option<String>,
}

/// Current version of the `rocky dag` graph-export contract.
///
/// See [`DagOutput::schema_version`] for the bump policy.
fn default_dag_schema_version() -> String {
    "1".to_string()
}

// Phase 2 plan-spine note: plan_id / plan_kind / created_at / models /
// execution_layers fields are added directly to `PlanOutput` (inline fields,
// not a wrapper) so the existing `"plan"` command wire key in
// `parse_rocky_output` works unchanged. All fields use `skip_serializing_if`
// so existing consumers and fixtures remain byte-stable when the compile path
// is absent.

/// A connector snapshot recorded in a `ReplicationPlan` for stale-source
/// detection at apply time.
///
/// Volatile fields are intentionally omitted: `last_sync_at` is a
/// wall-clock value that wiggles every sync, and the adapter-namespaced
/// `metadata` map can include fields like rate-limit counters that
/// change without altering the replication contract. Only the identity
/// of the source (`id`, `schema`, `source_type`) and the table list
/// (sorted, name + row_count) influence the plan_id digest.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ReplicationConnectorSnapshot {
    pub id: String,
    pub schema: String,
    pub source_type: String,
    /// Tables discovered for this connector. Sorted by `name` so the
    /// serialized payload is deterministic across discover runs.
    pub tables: Vec<ReplicationTableSnapshot>,
}

/// A table snapshot inside a `ReplicationConnectorSnapshot`.
///
/// `row_count` is captured when the discovery adapter surfaces it
/// (DuckDB does; Fivetran does not). Including the count in the
/// snapshot means a row-count change between plan and apply marks the
/// plan stale — that's intentional for the row-count-aware adapters,
/// since downstream materializations may depend on size class.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ReplicationTableSnapshot {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_count: Option<u64>,
}

/// Plan payload persisted by `rocky plan` for a replication-only project.
///
/// A "replication-only" project is one with no `models/` directory (or
/// where `models/` exists but compile returns zero models). For those
/// the `RunPlan` spine has nothing to persist — there are no models
/// to re-derive at apply time. This payload captures the full
/// `RockyConfig` plus the discovered source state so the plan_id is
/// genuinely content-addressed: same config + same source state ⇒
/// same plan_id.
///
/// ## Design call: plan-time discovery (not apply-time)
///
/// Discovery already runs inside `rocky plan` today (the existing
/// statement preview iterates discovered connectors). Capturing the
/// snapshot in the plan payload is essentially free I/O-wise and
/// gives us a deterministic plan_id keyed on observable inputs.
/// Apply re-runs discovery and asserts the snapshot still matches —
/// any drift fails with a clear "re-plan and re-apply" error before
/// SQL is emitted.
///
/// ## What is NOT in the snapshot
///
/// - `last_sync_at` (volatile, not part of the replication contract)
/// - Adapter-namespaced `metadata` map (rate-limit counters etc.
///   change without altering what gets replicated)
///
/// Including those would invite "I planned, waited 5 minutes, and now
/// the plan is stale" surprises with no actionable difference.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ReplicationPlan {
    /// `--filter` passed to `rocky plan` (e.g. `"client=acme"`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
    /// `--pipeline` if specified.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pipeline: Option<String>,
    /// `--env` for governance preview.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env: Option<String>,
    /// `--idempotency-key` (or `ROCKY_IDEMPOTENCY_KEY`). Different keys
    /// produce different plan_ids — the hash discriminates, mirroring
    /// `RunPlan`'s behaviour.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    /// `--resume <run_id>` if set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resume: Option<String>,
    /// `--resume-latest` toggle. Resolved against the state store at
    /// apply time, like `RunPlan.resume_latest`.
    #[serde(default)]
    pub resume_latest: bool,
    /// Governance override resolved at plan time from
    /// `--governance-override`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub governance_override: Option<rocky_core::config::GovernanceOverride>,
    /// Full `RockyConfig` snapshot, serialised after env-var
    /// substitution. Captured verbatim — apply does not try to extract
    /// "the replication-relevant subset" because that's a footgun: any
    /// hidden runtime dependency on a config field that isn't in the
    /// subset would silently break replay. Cheaper to keep the whole
    /// config and let the hash do its job.
    pub config_snapshot: serde_json::Value,
    /// Discovered connectors sorted by `id`. Tables within each
    /// connector are sorted by `name`. Apply re-discovers and asserts
    /// byte-equality against this snapshot before running SQL.
    pub source_state_snapshot: Vec<ReplicationConnectorSnapshot>,
}

/// JSON output for `rocky apply <plan-id>`.
///
/// Wraps the inner apply output (compact / archive / run) with a top-level
/// `plan_id` envelope so consumers can correlate the apply result back to the
/// plan that generated it without examining the inner payload's command field.
///
/// ## Shape decision: envelope (not discriminated enum)
///
/// A discriminated enum with `CompactApplyOutput | ArchiveApplyOutput | RunOutput`
/// would produce noisier `JsonSchema` derivations (nested `oneOf` with
/// overlapping field names). An envelope with `inner: serde_json::Value` is
/// simpler and lets consumers fall back to the per-command schema they already
/// know for the `command` field (`"compact apply"` / `"archive apply"` / `"run"`).
#[derive(Debug, Serialize, JsonSchema)]
pub struct ApplyOutput {
    pub version: String,
    /// Always `"apply"` — the top-level command selector in `parse_rocky_output`.
    pub command: String,
    /// The plan that was applied (full 64-char blake3 hex).
    pub plan_id: String,
    /// `PlanKind` wire name: `"compact"`, `"archive"`, or `"run"`.
    pub plan_kind: String,
    /// Whether all statements / materializations succeeded.
    pub success: bool,
    /// The full inner apply result, embedded verbatim. Shape depends on
    /// `plan_kind`:
    /// - `"compact"` → `CompactApplyOutput`
    /// - `"archive"` → `ArchiveApplyOutput`
    /// - `"run"`     → `RunOutput`
    pub result: serde_json::Value,
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
    /// Total dbt unit-test definitions (`manifest.unit_tests`) seen
    /// across all imported manifests.
    #[serde(default)]
    pub unit_tests_found: usize,
    /// Number of dbt unit tests written as Rocky `[[test]]` blocks in
    /// per-model sidecar TOML files.
    #[serde(default)]
    pub unit_tests_converted: usize,
    /// Number of dbt unit tests dropped — orphan target model, non-`dict`
    /// fixture format, or otherwise unsupported shape.
    #[serde(default)]
    pub unit_tests_skipped: usize,
    /// Number of dbt resources the importer does not translate that were
    /// detected and skipped (snapshots, metrics, semantic models, exposures).
    #[serde(default)]
    pub constructs_dropped: usize,
    /// Number of dbt models whose enforced `contract` (column `data_type`s /
    /// `constraints`) was dropped on import. Rocky enforces contracts via a
    /// `{model}.contract.toml` sidecar the importer does not auto-generate.
    #[serde(default)]
    pub contracts_dropped: usize,
    pub macros_detected: usize,
    pub imported_models: Vec<String>,
    pub warning_details: Vec<ImportDbtWarning>,
    /// Typed structured warnings — payload-carrying variants for dbt
    /// config Rocky can't auto-translate (dropped tags, dropped hooks,
    /// unresolvable macros, etc.). Coexists with `warning_details` —
    /// orchestrators that don't know about this field still see the
    /// flat string warnings under `warning_details`.
    #[serde(default)]
    pub structured_warnings: Vec<ImportDbtStructuredWarning>,
    pub failed_details: Vec<ImportDbtFailure>,
    /// Free-form per-model migration report payload.
    pub report: serde_json::Value,
    /// Metadata about the emitted Rocky repo. Present when `--output-dir`
    /// triggered repo emission (the default for `rocky import-dbt`).
    /// Absent when emission was skipped or failed before disk writes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub emission: Option<ImportDbtEmission>,
}

/// Files-on-disk metadata for the runnable Rocky repo emitted by
/// `rocky import-dbt --output-dir <out>`.
///
/// Consumers (Dagster, vscode) treat this block as the contract for "where
/// the importer wrote things." Absence of the block on `ImportDbtOutput`
/// means no repo was emitted (e.g. dry-run mode in a follow-up).
#[derive(Debug, Serialize, JsonSchema)]
pub struct ImportDbtEmission {
    /// Resolved output directory.
    pub out_dir: String,
    /// Path to the generated `rocky.toml`.
    pub rocky_toml_path: String,
    /// Path to the generated `MIGRATION-NOTES.md`.
    pub migration_notes_path: String,
    /// Number of dbt models successfully translated and written under `models/`.
    pub models_translated_count: usize,
    /// Number of dbt model files seen but not translated (failed entries).
    pub models_skipped_count: usize,
    /// Number of files copied from `<dbt_project>/seeds/` into `<out>/seeds/`.
    pub seeds_copied_count: usize,
    /// Rocky adapter type written into `[adapter]` (`duckdb` / `databricks` / …).
    pub adapter_type: String,
    /// dbt profile `type` value the importer mapped from. Useful when the
    /// caller passed `--target-adapter` and we want to surface the original.
    pub original_dbt_adapter_type: String,
    /// Env vars referenced by the emitted `[adapter]` block. Surfaced in
    /// `MIGRATION-NOTES.md` under "Required env vars".
    pub required_env_vars: Vec<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ImportDbtWarning {
    pub model: String,
    pub category: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggestion: Option<String>,
}

/// Lifecycle hook kind for [`ImportDbtStructuredWarning::DroppedHook`].
#[derive(Debug, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ImportDbtHookKind {
    Pre,
    Post,
}

/// Typed structured warning for dbt config that Rocky can't translate
/// automatically. Each variant carries the source payload so the
/// downstream UI (Dagster, VS Code) can route specific kinds into
/// per-kind UI affordances without parsing free-form text.
#[derive(Debug, Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ImportDbtStructuredWarning {
    /// A dbt materialization with no direct Rocky equivalent. The
    /// importer fell back to the closest match (typically
    /// `full_refresh`).
    UnsupportedMaterialization {
        model: String,
        dbt_materialization: String,
        action: String,
    },
    /// `databricks_tags` block dropped — Rocky's `[classification]`
    /// block + `rocky-databricks` governance surface covers the same
    /// use case but requires manual config.
    DroppedDatabricksTags {
        model: String,
        tags: std::collections::BTreeMap<String, String>,
    },
    /// `pre_hook` or `post_hook` dropped — Rocky supports lifecycle
    /// hooks via the `[[hook]]` block in `rocky.toml`.
    DroppedHook {
        model: String,
        hook_kind: ImportDbtHookKind,
        sql: String,
    },
    /// `on_schema_change` dropped — Rocky exposes the equivalent via
    /// per-pipeline `[drift]` policy.
    DroppedOnSchemaChange {
        model: String,
        dbt_value: String,
        rocky_equivalent: String,
    },
    /// A custom Jinja macro call survived `dbt compile` — defined
    /// out-of-tree. The user needs to hand-port the macro.
    UnresolvableMacro {
        model: String,
        macro_name: String,
        first_call_site_line: usize,
    },
    /// A microbatch model is missing the required `event_time` field —
    /// the importer fell back to `full_refresh`.
    MicrobatchMissingEventTime { model: String },
    /// A dbt microbatch model was remapped: to an idempotent `merge`
    /// (`mapped_to = "merge"`) when it has a `unique_key`, or kept append-only
    /// (`mapped_to = "append"`) when it does not.
    MicrobatchMapped { model: String, mapped_to: String },
    /// A dbt construct the importer does not translate was detected and
    /// skipped (snapshot, source freshness, grants, meta, metric, semantic
    /// model, exposure), surfaced so a migration is never silently lossy.
    DroppedConstruct {
        construct: String,
        name: String,
        detail: String,
    },
    /// A dbt model `contract` (`enforced: true`), column `data_type`s, and/or
    /// `constraints` were dropped on import. Rocky enforces contracts via a
    /// `{model}.contract.toml` sidecar the importer does not auto-generate;
    /// the user must hand-author it. Visibility only — no stub generated.
    DroppedContract {
        model: String,
        typed_columns: usize,
        constraints: usize,
        contract_path: String,
    },
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
    /// Result of the data-contract gate, when the load pipeline declares a
    /// contract. `None` for ungated loads. On a failed gate the data was not
    /// promoted to the target; the violations explain why.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contract: Option<rocky_core::contracts::ContractResult>,
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
            new_sources: vec![],
            collision_candidates: vec![],
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

    /// Attach the list of first-seen source schemas (opt-in via the
    /// discovery config's `report_new_sources`) and return self.
    #[must_use]
    pub fn with_new_sources(mut self, new_sources: Vec<String>) -> Self {
        self.new_sources = new_sources;
        self
    }

    /// Attach detected cross-source collisions (opt-in via discovery's
    /// `on_collision`) and return self.
    #[must_use]
    pub fn with_collision_candidates(mut self, candidates: Vec<CollisionCandidateOutput>) -> Self {
        self.collision_candidates = candidates;
        self
    }
}

impl ChecksConfigOutput {
    /// Project the engine `ChecksConfig` into its CLI output shape.
    ///
    /// `executed_kinds` is the set of check kinds the pipeline type actually
    /// runs (see `PipelineConfig::executed_check_kinds`) — names are projected
    /// only for kinds the runner will execute, so discover never advertises a
    /// check that won't produce a result. `sources` supplies the
    /// `(source_type, table)` pairs the projection (and the cross-source
    /// overlap grouping) needs. Returns `None` when nothing is surfaced.
    pub fn from_engine(
        cfg: &rocky_core::config::ChecksConfig,
        executed_kinds: &[rocky_core::checks::CheckKind],
        sources: &[SourceOutput],
    ) -> Option<Self> {
        use rocky_core::checks::CheckKind;

        let freshness = cfg.freshness.as_ref().map(|f| FreshnessConfigOutput {
            threshold_seconds: f.threshold_seconds,
        });

        let runs = |k: CheckKind| executed_kinds.contains(&k);
        let mut configured_checks: BTreeMap<String, Vec<ResolvedCheckNameOutput>> = BTreeMap::new();

        // (source_type, table) pairs across discovered sources.
        let pairs: Vec<(String, String)> = sources
            .iter()
            .flat_map(|s| {
                s.tables
                    .iter()
                    .map(move |t| (s.source_type.clone(), t.name.clone()))
            })
            .collect();
        let unique_tables: std::collections::BTreeSet<&str> =
            pairs.iter().map(|(_, t)| t.as_str()).collect();

        // Custom checks run against every materialized table.
        if runs(CheckKind::Custom) {
            for &table in &unique_tables {
                for custom in &cfg.custom {
                    configured_checks
                        .entry(table.to_string())
                        .or_default()
                        .push(ResolvedCheckNameOutput {
                            name: custom.name.clone(),
                            kind: "custom".into(),
                            candidate: false,
                        });
                }
            }
        }

        // Null-rate: one result per configured column, per table.
        if runs(CheckKind::NullRate)
            && let Some(nr) = cfg.null_rate.as_ref()
        {
            for &table in &unique_tables {
                for col in &nr.columns {
                    configured_checks
                        .entry(table.to_string())
                        .or_default()
                        .push(ResolvedCheckNameOutput {
                            name: rocky_core::checks::null_rate_check_name(col),
                            kind: "null_rate".into(),
                            candidate: false,
                        });
                }
            }
        }

        // Assertions attach to their specific target table.
        if runs(CheckKind::Assertions) {
            for assertion in &cfg.assertions {
                configured_checks
                    .entry(assertion.table.clone())
                    .or_default()
                    .push(ResolvedCheckNameOutput {
                        name: assertion.resolved_name(),
                        kind: "assertion".into(),
                        candidate: false,
                    });
            }
        }

        // Cross-source overlap: candidate names for ≥2 (source_type, table)
        // groups — marked `candidate` because the actual set depends on
        // runtime-discovered siblings, which may differ from what discover sees.
        if runs(CheckKind::CrossSourceOverlap) && cfg.cross_source_overlap.is_some() {
            for (source_type, table) in
                rocky_core::checks::cross_source_overlap_groups(pairs.iter().cloned())
            {
                configured_checks
                    .entry(table.clone())
                    .or_default()
                    .push(ResolvedCheckNameOutput {
                        name: rocky_core::checks::cross_source_overlap_name(&source_type, &table),
                        kind: "cross_source_overlap".into(),
                        candidate: true,
                    });
            }
        }

        // Dedup identical names per table (e.g. a custom check on a table name
        // that appears under multiple sources), preserving declaration order.
        for names in configured_checks.values_mut() {
            let mut seen = std::collections::HashSet::new();
            names.retain(|n| seen.insert(n.name.clone()));
        }

        if freshness.is_none() && configured_checks.is_empty() {
            return None;
        }
        Some(ChecksConfigOutput {
            freshness,
            configured_checks,
        })
    }
}

#[cfg(test)]
mod checks_config_projection_tests {
    use super::*;
    use rocky_core::checks::CheckKind;

    fn source(source_type: &str, table: &str) -> SourceOutput {
        SourceOutput {
            id: format!("{source_type}.{table}"),
            components: IndexMap::new(),
            source_type: source_type.to_string(),
            last_sync_at: None,
            tables: vec![TableOutput {
                name: table.to_string(),
                row_count: None,
            }],
            metadata: IndexMap::new(),
        }
    }

    fn checks_cfg() -> rocky_core::config::ChecksConfig {
        toml::from_str(
            r#"
null_rate = { columns = ["amount"], threshold = 0.1 }
cross_source_overlap = { keys = ["id"] }

[[custom]]
name = "has_rows"
sql = "SELECT COUNT(*) FROM {table}"
threshold = 1

[[assertions]]
name = "id_not_null"
table = "orders"
type = "not_null"
column = "id"
"#,
        )
        .unwrap()
    }

    #[test]
    fn replication_projects_all_kinds_with_overlap_marked_candidate() {
        // Two sources sharing (source_type, table) form an overlap sibling group.
        let sources = vec![source("duckdb", "orders"), source("duckdb", "orders")];
        let out = ChecksConfigOutput::from_engine(
            &checks_cfg(),
            rocky_core::config::ReplicationPipelineConfig::EXECUTED_CHECK_KINDS,
            &sources,
        )
        .expect("projection present");

        let names = &out.configured_checks["orders"];
        let by_name: std::collections::HashMap<&str, &ResolvedCheckNameOutput> =
            names.iter().map(|n| (n.name.as_str(), n)).collect();
        assert!(by_name.contains_key("has_rows"));
        assert!(by_name.contains_key("null_rate:amount"));
        assert!(by_name.contains_key("id_not_null"));
        let overlap = by_name
            .get("cross_source_overlap:duckdb.orders")
            .expect("overlap name projected");
        assert!(overlap.candidate, "overlap names must be marked candidate");
        // The exact-name checks are not candidates.
        assert!(!by_name["has_rows"].candidate);
    }

    #[test]
    fn gates_out_kinds_the_pipeline_type_does_not_run() {
        // A quality-style executed set runs custom + assertions but NOT
        // null_rate / cross_source_overlap — those must not be projected.
        let sources = vec![source("duckdb", "orders")];
        let executed = &[
            CheckKind::RowCount,
            CheckKind::Custom,
            CheckKind::Assertions,
        ];
        let out = ChecksConfigOutput::from_engine(&checks_cfg(), executed, &sources)
            .expect("custom + assertion still project");

        let names: Vec<&str> = out.configured_checks["orders"]
            .iter()
            .map(|n| n.name.as_str())
            .collect();
        assert!(names.contains(&"has_rows"));
        assert!(names.contains(&"id_not_null"));
        assert!(
            !names.iter().any(|n| n.starts_with("null_rate")),
            "null_rate must be gated out: {names:?}"
        );
        assert!(
            !names.iter().any(|n| n.starts_with("cross_source_overlap")),
            "cross_source_overlap must be gated out: {names:?}"
        );
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
            model_decisions: vec![],
            contained: vec![],
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
            override_warnings: vec![],
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
            // from `statistics.query.totalBytesBilled`.
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
            // Annotate the active span with this breach as an OTel
            // span event before fanning out to bus subscribers. No-op
            // when the `otel` feature is off.
            rocky_observe::events::record_span_event(&evt);
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
            let (input_hash, input_proof_class) = derive_input_identity(mat);
            models.push(rocky_core::state::ModelExecution {
                model_name,
                started_at: model_started,
                finished_at: model_finished,
                duration_ms,
                rows_affected: mat.rows_copied,
                status: "success".to_string(),
                sql_hash: mat.metadata.sql_hash.clone().unwrap_or_default(),
                // State-internal skip-gate inputs, populated on a successful
                // build by the `--skip-unchanged` gate via `skip_internal`.
                // `None` whenever the gate is off or the model was not
                // skip-eligible — its absence simply forces a rebuild next run.
                skip_hash: mat.skip_internal.as_ref().and_then(|s| s.skip_hash.clone()),
                // The consumer-side upstream baseline. A skip-eligible plain
                // model carries its freshness signatures via `skip_internal`; a
                // content-addressed model carries the per-column consumer
                // baseline via `consumed_column_baseline`. A model is at most
                // one of the two, so `or_else` picks whichever is present (both
                // land on `upstream_freshness` as `UpstreamSig` entries). The
                // per-column baseline is captured only — the skip gate never
                // reads a content-addressed model's `upstream_freshness`.
                upstream_freshness: mat
                    .skip_internal
                    .as_ref()
                    .map(|s| s.upstream_freshness.clone())
                    .or_else(|| mat.consumed_column_baseline.clone()),
                bytes_scanned: mat.bytes_scanned,
                bytes_written: mat.bytes_written,
                tenant: mat.tenant.clone(),
                // Recipe identity. `recipe_hash` / `env_hash` / scheme come
                // from the per-model carrier stamped at the execution site;
                // the input side is derived here from the observed upstream
                // freshness signatures (see `derive_input_identity`) — `None`
                // on the default path, which observes no inputs.
                recipe_hash: mat.recipe_identity.as_ref().map(|r| r.recipe_hash.clone()),
                env_hash: mat.recipe_identity.as_ref().map(|r| r.env_hash.clone()),
                hash_scheme: mat.recipe_identity.as_ref().map(|r| r.hash_scheme.clone()),
                input_hash: input_hash.clone(),
                input_proof_class,
                // Per-output-column content hashes, populated by the
                // content-addressed runner on a genuine unpartitioned build;
                // `None` on every other path (carried straight through).
                output_column_hashes: mat.output_column_hashes.clone(),
                // Classified-retry attempt trail (schema v16). Execution
                // metadata — carried alongside the identity hashes above,
                // never inside them, so a retried-then-succeeded build stays
                // byte-indistinguishable downstream from a first-try success.
                attempts: mat.attempts.clone(),
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
                // A failed execution never seeds a skip baseline.
                skip_hash: None,
                upstream_freshness: None,
                bytes_scanned: None,
                bytes_written: None,
                // Errors carry only the asset key, not the parsed schema
                // components, so the tenant dimension is unavailable here.
                tenant: None,
                // A failed execution captured no recipe identity — consistent
                // with its `None` skip_hash / empty sql_hash above.
                recipe_hash: None,
                input_hash: None,
                input_proof_class: None,
                env_hash: None,
                hash_scheme: None,
                // A failed execution recorded no output columns.
                output_column_hashes: None,
                // A run-level error entry (no per-model materialization)
                // carries no attempt trail; the failing model's own attempts,
                // when the retry layer produced them, ride on its
                // `MaterializationOutput` instead.
                attempts: Vec::new(),
            });
        }

        // Flatten every executed check's pass/fail across all tables so a later
        // reader (the `verify_after` policy gate) can confirm a named check ran
        // and passed without re-executing it.
        let check_outcomes = self
            .check_results
            .iter()
            .flat_map(|table| &table.checks)
            .map(|c| rocky_core::state::CheckOutcome {
                name: c.name.clone(),
                passed: c.passed,
            })
            .collect();

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
            check_outcomes,
        }
    }

    /// Derive the [`rocky_core::state::RunStatus`] from the output's
    /// end-state counters. Interrupt (`self.interrupted`) with no
    /// materializations → `Failure`; with at least one → `PartialFailure`.
    /// Same rule for failed-table counts. Clean completion → `Success`.
    ///
    /// "Progress" is either a replicated table (`tables_copied`) or a
    /// materialized transformation model (`materializations`) — the
    /// transformation path tracks built models in `materializations`
    /// rather than `tables_copied`, so a partial transformation run (one
    /// model built, another failed to compile) correctly reports
    /// `PartialFailure` instead of a total `Failure`.
    pub fn derive_run_status(&self) -> rocky_core::state::RunStatus {
        let has_progress = self.tables_copied > 0 || !self.materializations.is_empty();
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
            budget_diagnostics: vec![],
            has_budget_errors: false,
            plan_id: None,
            plan_kind: None,
            created_at: None,
            models: vec![],
            execution_layers: vec![],
            breaking_verdict: None,
        }
    }
}

impl StateOutput {
    pub fn new(watermarks: Vec<WatermarkEntry>, schema_version_on_disk: Option<u32>) -> Self {
        StateOutput {
            version: VERSION.to_string(),
            command: "state".to_string(),
            watermarks,
            schema_version_supported: rocky_core::state::current_schema_version(),
            schema_version_on_disk,
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

impl RetentionSweepOutput {
    pub fn new(
        report: &rocky_core::retention::SweepReport,
        policy: &rocky_core::retention::StateRetentionConfig,
        dry_run: bool,
    ) -> Self {
        RetentionSweepOutput {
            version: VERSION.to_string(),
            command: "state-retention-sweep".to_string(),
            dry_run,
            max_age_days: policy.max_age_days,
            min_runs_kept: policy.min_runs_kept,
            domains: policy.applies_to.iter().map(ToString::to_string).collect(),
            runs_deleted: report.runs_deleted,
            runs_kept: report.runs_kept,
            lineage_deleted: report.lineage_deleted,
            lineage_kept: report.lineage_kept,
            audit_deleted: report.audit_deleted,
            audit_kept: report.audit_kept,
            traces_deleted: report.traces_deleted,
            duration_ms: report.duration_ms,
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
    /// Version of the graph-export contract this payload conforms to.
    ///
    /// Distinct from `version` (the engine release, which churns every
    /// release): `schema_version` identifies the *shape* of the graph
    /// export — the node/edge/lineage fields orchestrators build an asset
    /// graph from. It is bumped only on a backward-incompatible change to
    /// that shape, so an orchestrator can pin against it across engine
    /// releases. Additive, backward-compatible field additions do not bump
    /// it (and surface through codegen-drift CI instead). Always emitted;
    /// older payloads that predate the field are treated as `"1"`.
    #[serde(default = "default_dag_schema_version")]
    pub schema_version: String,
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

/// When set, [`print_json`] emits compact (single-line) JSON instead of
/// pretty-printed. Used by `rocky run --watch` to honour its
/// newline-delimited-stream contract: each iteration's `RunOutput` lands
/// as one parseable line on stdout, with the human-readable banner
/// (`[watch] watching ...`, `[watch] detected change`, `[watch] run
/// completed in ...`) on stderr. Non-watch one-shot commands keep
/// pretty-printing because they're read by humans.
///
/// Process-global because the streaming contract spans every `run()`
/// call site reachable from the watch loop, and threading a flag through
/// every helper that calls `print_json` would touch dozens of signatures
/// for a behaviour that only one caller cares about today. If a future
/// command needs both modes interleaved, swap this for a `&Renderer`
/// passed explicitly down the call tree.
pub static COMPACT_JSON: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Prints output as JSON or formatted table.
pub fn print_json<T: Serialize>(output: &T) -> anyhow::Result<()> {
    let json = if COMPACT_JSON.load(std::sync::atomic::Ordering::Relaxed) {
        serde_json::to_string(output)?
    } else {
        serde_json::to_string_pretty(output)?
    };
    println!("{json}");
    Ok(())
}

/// Process-global flag: when set, stdout is reserved exclusively for the JSON
/// document, so human-readable progress/summary lines must go to stderr.
///
/// The unified-DAG path (`rocky run --dag`, `rocky apply`) drives each node as
/// a sub-run with `json = false`, so those sub-runs take their human-summary
/// branch and would otherwise print "transformation pipeline complete: …",
/// "Copied N tables …", "Seed complete: …" etc. to stdout *before* the outer
/// `DagRunOutput` JSON payload. With `-o json` the orchestrator contract is
/// that stdout is exactly one JSON document, so we flip this flag once at the
/// top of a JSON-mode run and have the summary printers emit to stderr via
/// [`status_line!`] instead. Human (table) mode never sets it.
static STDOUT_RESERVED_FOR_JSON: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Reserve stdout for the JSON document — see [`STDOUT_RESERVED_FOR_JSON`].
///
/// Idempotent and set-only: called once when a run resolves to JSON output.
pub fn reserve_stdout_for_json() {
    STDOUT_RESERVED_FOR_JSON.store(true, std::sync::atomic::Ordering::Relaxed);
}

/// Whether human progress/summary lines must be routed to stderr instead of
/// stdout — see [`STDOUT_RESERVED_FOR_JSON`]. Consulted by [`status_line!`].
pub fn is_stdout_reserved_for_json() -> bool {
    STDOUT_RESERVED_FOR_JSON.load(std::sync::atomic::Ordering::Relaxed)
}

/// Print a human-readable progress/summary line, honoring the
/// stdout-reserved-for-JSON contract.
///
/// Identical to `println!` in human/table mode. When stdout is reserved for a
/// JSON document (see [`reserve_stdout_for_json`]) the line is routed to stderr
/// so it can't precede or corrupt the JSON payload an orchestrator parses from
/// stdout. Use this for every run-path summary line that lives in a
/// non-`output_json` branch.
#[macro_export]
macro_rules! status_line {
    ($($arg:tt)*) => {{
        if $crate::output::is_stdout_reserved_for_json() {
            eprintln!($($arg)*);
        } else {
            println!($($arg)*);
        }
    }};
}

#[cfg(test)]
mod cost_finalize_tests {
    use super::*;
    use rocky_core::config::{BudgetBreachAction, BudgetConfig, CostSection};

    fn mat(asset_key: &[&str], duration_ms: u64) -> MaterializationOutput {
        MaterializationOutput {
            asset_key: asset_key.iter().map(|s| (*s).to_string()).collect(),
            attempts: Vec::new(),
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
            tenant: None,
            job_ids: Vec::new(),
            skip_internal: None,
            recipe_identity: None,
            output_column_hashes: None,
            consumed_column_baseline: None,
        }
    }

    fn sample_identity() -> RecipeIdentityInternal {
        RecipeIdentityInternal {
            recipe_hash: "2148e619b51421f51cfb3fac423145fe245bbe409b74f31c22d703ab07453036"
                .to_string(),
            env_hash: "bbf2c2045328f738683a6336df5fa61b5f00076aeaed7c495c04f9d6991d92bd"
                .to_string(),
            hash_scheme: "v1".to_string(),
        }
    }

    #[test]
    fn recipe_manifest_properties_none_without_identity() {
        let m = mat(&["wh", "silver", "orders"], 100);
        assert!(
            super::recipe_manifest_properties(&m, "run-1", "1.58.0").is_none(),
            "a materialization without recipe_identity yields no manifest props"
        );
    }

    #[test]
    fn recipe_manifest_properties_default_run_omits_inputs() {
        // Default run path: recipe_identity stamped, but skip_internal is None
        // (no observed inputs) — inputs_hash/inputs_proof_class must both be
        // absent, matching a managed Databricks run.
        let mut m = mat(&["wh", "silver", "orders"], 100);
        m.recipe_identity = Some(sample_identity());
        let props = super::recipe_manifest_properties(&m, "run-42", "1.58.0").unwrap();

        assert_eq!(props["recipe_manifest.manifest_version"], "0.1");
        assert_eq!(props["recipe_manifest.hash_scheme"], "v1");
        assert_eq!(
            props["recipe_manifest.program_hash"],
            "2148e619b51421f51cfb3fac423145fe245bbe409b74f31c22d703ab07453036"
        );
        assert_eq!(
            props["recipe_manifest.env_hash"],
            "bbf2c2045328f738683a6336df5fa61b5f00076aeaed7c495c04f9d6991d92bd"
        );
        assert_eq!(props["recipe_manifest.producer.name"], "rocky");
        assert_eq!(props["recipe_manifest.producer.version"], "1.58.0");
        assert_eq!(props["recipe_manifest.subject.model"], "orders");
        assert_eq!(props["recipe_manifest.subject.run_id"], "run-42");
        assert_eq!(props["recipe_manifest.subject.status"], "success");
        assert!(
            !props.contains_key("recipe_manifest.inputs_hash"),
            "no observed inputs → inputs_hash omitted"
        );
        assert!(
            !props.contains_key("recipe_manifest.inputs_proof_class"),
            "no observed inputs → inputs_proof_class omitted (both-or-neither)"
        );
    }

    #[test]
    fn recipe_manifest_properties_with_observed_inputs_writes_pair() {
        // When the run observed inputs (skip-gate populated upstream
        // signatures), inputs_hash + inputs_proof_class travel together.
        let mut m = mat(&["wh", "silver", "orders"], 100);
        m.recipe_identity = Some(sample_identity());
        m.skip_internal = Some(ModelSkipState {
            skip_hash: Some("skip-key-fixed".to_string()),
            upstream_freshness: vec![rocky_core::state::UpstreamSig {
                upstream_key: "wh.raw.events".to_string(),
                max_ts: None,
                row_count: Some(42),
                consumed_column_hashes: None,
            }],
        });
        let props = super::recipe_manifest_properties(&m, "run-7", "1.58.0").unwrap();
        assert!(props.contains_key("recipe_manifest.inputs_hash"));
        assert_eq!(
            props["recipe_manifest.inputs_proof_class"], "heuristic",
            "a watermark upstream is a heuristic (freshness, not byte-identity) claim"
        );
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
        // When the BigQuery adapter populates `bytes_scanned` from
        // `statistics.query.totalBytesBilled`, `populate_cost_summary`
        // must produce a real dollar figure rather than the `None` it
        // returned before the plumbing landed.
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
            attempts: Vec::new(),
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
            tenant: None,
            job_ids: Vec::new(),
            skip_internal: None,
            recipe_identity: None,
            output_column_hashes: None,
            consumed_column_baseline: None,
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

    /// The content-addressed consumer baseline (`consumed_column_baseline`)
    /// must land on the persisted `ModelExecution.upstream_freshness` — the
    /// 🔴 The load-bearing identity invariant for classified retry, at the
    /// state-write boundary: an execution that carries an attempt trail
    /// produces byte-identical *identity* to one that doesn't — attempt history
    /// rides on `ModelExecution.attempts` alongside the identity fields, never
    /// inside them.
    #[test]
    fn to_run_record_keeps_attempt_trail_out_of_identity() {
        use rocky_core::state::AttemptRecord;

        let started = fixed_start();
        let identity = RecipeIdentityInternal {
            recipe_hash: "recipe-abc".to_string(),
            env_hash: "env-def".to_string(),
            hash_scheme: "v1".to_string(),
        };
        let out_cols = Some(vec![rocky_core::state::ColumnHash {
            column: "amount".to_string(),
            hash: "out-hash".to_string(),
        }]);

        // Two materializations identical in every identity-bearing field; one
        // recovered after a transient retry (so it carries a trail), the other
        // a clean first try (empty trail).
        let build_mat = |attempts: Vec<AttemptRecord>| {
            let mut m = mat(&["s", "orders"], 1_000, Some("sql-xyz"), started);
            m.recipe_identity = Some(identity.clone());
            m.output_column_hashes = out_cols.clone();
            m.attempts = attempts;
            m
        };
        let clean = build_mat(Vec::new());
        let retried = build_mat(vec![
            AttemptRecord {
                attempt: 1,
                outcome: "failed".to_string(),
                failure_class: Some("transient".to_string()),
                transient_kind: Some("server_busy".to_string()),
                error: Some("lock".to_string()),
                backoff_ms: Some(0),
                duration_ms: 3,
            },
            AttemptRecord {
                attempt: 2,
                outcome: "success".to_string(),
                failure_class: None,
                transient_kind: None,
                error: None,
                backoff_ms: None,
                duration_ms: 5,
            },
        ]);

        let to_exec = |m: MaterializationOutput| {
            let mut out = RunOutput::new(String::new(), 1_000, 1);
            out.tables_copied = 1;
            out.materializations.push(m);
            out.to_run_record(
                "run-id",
                started,
                started + chrono::Duration::seconds(1),
                "cfg".to_string(),
                RunTrigger::Manual,
                RunStatus::Success,
                RunRecordAudit::test_sentinels(),
            )
            .models_executed
            .remove(0)
        };
        let clean_exec = to_exec(clean);
        let retried_exec = to_exec(retried);

        // Identity fields byte-identical across the two.
        assert_eq!(clean_exec.recipe_hash, retried_exec.recipe_hash);
        assert_eq!(clean_exec.input_hash, retried_exec.input_hash);
        assert_eq!(clean_exec.env_hash, retried_exec.env_hash);
        assert_eq!(clean_exec.hash_scheme, retried_exec.hash_scheme);
        assert_eq!(clean_exec.sql_hash, retried_exec.sql_hash);
        assert_eq!(
            clean_exec.output_column_hashes,
            retried_exec.output_column_hashes
        );
        // Only the attempt trail differs — and it is faithfully carried through.
        assert!(clean_exec.attempts.is_empty());
        assert_eq!(retried_exec.attempts.len(), 2);
        assert_eq!(
            retried_exec.attempts[0].failure_class.as_deref(),
            Some("transient")
        );
    }

    /// consumer-side per-column baseline (schema v13). A content-addressed
    /// model has no `skip_internal`, so the `or_else` route in `to_run_record`
    /// is what carries it. Proves the baseline is durable end-to-end.
    #[test]
    fn to_run_record_lands_consumer_baseline_on_upstream_freshness() {
        let mut out = RunOutput::new(String::new(), 1_000, 1);
        out.tables_copied = 1;
        let started = fixed_start();
        let mut m = mat(&["cat", "sch", "d"], 100, Some("sqlh"), started);
        // Content-addressed model: no skip_internal, only the consumer baseline.
        assert!(m.skip_internal.is_none());
        m.consumed_column_baseline = Some(vec![rocky_core::state::UpstreamSig {
            upstream_key: "cat.sch.u".to_string(),
            max_ts: None,
            row_count: None,
            consumed_column_hashes: Some(vec![rocky_core::state::ColumnHash {
                column: "id".to_string(),
                hash: "h_id".to_string(),
            }]),
        }]);
        out.materializations.push(m);

        let record = out.to_run_record(
            "run-baseline-1",
            started,
            started + chrono::Duration::seconds(1),
            "cfg".to_string(),
            RunTrigger::Manual,
            RunStatus::Success,
            RunRecordAudit::test_sentinels(),
        );

        let d = &record.models_executed[0];
        let sigs = d
            .upstream_freshness
            .as_ref()
            .expect("consumer baseline routed onto upstream_freshness");
        assert_eq!(sigs.len(), 1);
        assert_eq!(sigs[0].upstream_key, "cat.sch.u");
        assert_eq!(
            sigs[0].consumed_column_hashes,
            Some(vec![rocky_core::state::ColumnHash {
                column: "id".to_string(),
                hash: "h_id".to_string(),
            }])
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
            failure_kind: FailureKind::Unknown,
            cooldown_seconds: None,
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
    fn to_run_record_carries_tenant_onto_model_execution() {
        let mut out = RunOutput::new(String::new(), 1_000, 1);
        out.tables_copied = 1;
        let run_started = fixed_start();
        let mut m = mat(
            &["shopify", "acme", "orders"],
            1_000,
            Some("h"),
            run_started,
        );
        m.tenant = Some("acme".to_string());
        out.materializations.push(m);

        let started = fixed_start();
        let finished = started + chrono::Duration::seconds(1);
        let record = out.to_run_record(
            "run-tenant",
            started,
            finished,
            String::new(),
            RunTrigger::Manual,
            out.derive_run_status(),
            RunRecordAudit::test_sentinels(),
        );

        assert_eq!(record.models_executed.len(), 1);
        assert_eq!(
            record.models_executed[0].tenant.as_deref(),
            Some("acme"),
            "tenant on the materialization must flow onto the persisted ModelExecution"
        );
    }

    #[test]
    fn to_run_record_carries_output_column_hashes_onto_model_execution() {
        let mut out = RunOutput::new(String::new(), 1_000, 1);
        out.tables_copied = 1;
        let run_started = fixed_start();
        let mut m = mat(&["orders"], 1_000, Some("h"), run_started);
        // A content-addressed build stamps its producer-side per-column hashes
        // onto the carrier; `to_run_record` must copy them onto the persisted
        // `ModelExecution.output_column_hashes`.
        m.output_column_hashes = Some(vec![
            rocky_core::state::ColumnHash {
                column: "id".to_string(),
                hash: "hash-id".to_string(),
            },
            rocky_core::state::ColumnHash {
                column: "amount".to_string(),
                hash: "hash-amount".to_string(),
            },
        ]);
        out.materializations.push(m);

        let started = fixed_start();
        let finished = started + chrono::Duration::seconds(1);
        let record = out.to_run_record(
            "run-colhash",
            started,
            finished,
            String::new(),
            RunTrigger::Manual,
            out.derive_run_status(),
            RunRecordAudit::test_sentinels(),
        );

        assert_eq!(record.models_executed.len(), 1);
        let hashes = record.models_executed[0]
            .output_column_hashes
            .as_ref()
            .expect("output_column_hashes must flow onto the persisted ModelExecution");
        assert_eq!(hashes.len(), 2);
        assert_eq!(hashes[0].column, "id");
        assert_eq!(hashes[0].hash, "hash-id");
        assert_eq!(hashes[1].column, "amount");
        assert_eq!(hashes[1].hash, "hash-amount");
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
    fn derive_run_status_materialized_model_with_failure_is_partial() {
        // The transformation path records progress in `materializations`
        // (not `tables_copied`). A run that built one model and failed
        // another (e.g. a compile error) must be a PartialFailure, not a
        // total Failure.
        let mut out = RunOutput::new(String::new(), 0, 1);
        out.materializations
            .push(mat(&["", "marts", "good"], 10, Some("h"), fixed_start()));
        out.tables_failed = 1;
        assert!(matches!(out.derive_run_status(), RunStatus::PartialFailure));
    }

    #[test]
    fn compile_error_failure_kind_serializes_kebab_case() {
        // The codegen-driven wire contract: `FailureKind::CompileError`
        // must serialize as the kebab-case literal Dagster/VS Code branch on.
        let json = serde_json::to_string(&FailureKind::CompileError).unwrap();
        assert_eq!(json, "\"compile-error\"");
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

// ---------------------------------------------------------------------------
// Branch approval / promote — types
// ---------------------------------------------------------------------------

/// Identity of an approver, captured at sign time.
///
/// Email is sourced from `git config user.email`; name from
/// `git config user.name`. Hostname is best-effort from the `hostname` crate
/// and surfaced as an audit aid only — it is not part of the trust boundary.
/// Set `ROCKY_SCRUB_HOST` in the environment to replace the hostname with
/// `"redacted"` when recording public demos.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ApproverIdentity {
    pub email: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub host: String,
    pub source: ApproverSource,
}

/// Where the approval signature was produced.
///
/// Reserved for future CI / OIDC paths. Today only the `Local` variant is
/// emitted by the CLI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ApproverSource {
    Local,
    CiOidc,
    Pat,
}

/// Algorithm tag for an [`ApprovalSignature`].
///
/// The discriminator is on disk from day one so future "real" cryptographic
/// signing variants slot in without migrating existing artifacts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SignatureAlgorithm {
    /// blake3 over a canonical-JSON encoding of the artifact payload, with
    /// the approver's git identity bound into the hashed bytes. Detects
    /// tamper-after-write but is not asymmetric crypto — see the
    /// approval-flow docs for the security model.
    Blake3CanonicalJson,
}

/// Signature attached to an [`ApprovalArtifact`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ApprovalSignature {
    pub algorithm: SignatureAlgorithm,
    /// Hex-encoded digest. For `Blake3CanonicalJson`, this is the 32-byte
    /// blake3 hash printed as 64 lowercase hex characters.
    pub digest: String,
}

/// On-disk approval record for a single approver against a single branch.
///
/// One file per approver under `./.rocky/approvals/<branch>/<approval_id>.json`.
/// `branch_state_hash` binds the approval to the branch's content-addressed
/// state at sign time; if the branch's hash changes (config bytes change),
/// every existing artifact for that branch becomes stale and `branch promote`
/// will refuse to honour it.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ApprovalArtifact {
    /// Sortable monotonic identifier — timestamp prefix + random tail.
    pub approval_id: String,
    pub branch: String,
    pub branch_state_hash: String,
    pub approver: ApproverIdentity,
    pub signed_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    pub signature: ApprovalSignature,
}

/// JSON output for `rocky branch approve`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ApproveOutput {
    pub version: String,
    pub command: String,
    pub artifact: ApprovalArtifact,
    /// Where the artifact landed on disk.
    pub artifact_path: String,
}

/// Categorical kind of an [`AuditEvent`] emitted by `branch promote`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AuditEventKind {
    PromoteStarted,
    PromoteCompleted,
    PromoteFailed,
    /// Emitted when the approval gate was bypassed via `--skip-approval` or
    /// the `ROCKY_BRANCH_APPROVAL_SKIP` env-var override. The skip reason is
    /// recorded so a future audit can tell flag-skip apart from env-skip.
    ApprovalSkipped,
    /// Emitted when the semantic breaking-change gate blocked the promote.
    /// The findings list is carried on the parent [`AuditEvent`] so reviewers
    /// see exactly which structural changes triggered the block.
    BreakingChangesBlocked,
    /// Emitted when the semantic breaking-change gate detected one or more
    /// `Breaking`-severity findings but the operator overrode the block via
    /// `--allow-breaking`. Records the findings so the override leaves an
    /// audit trail equivalent to `ApprovalSkipped`.
    BreakingChangesAllowed,
    /// Emitted when the semantic breaking-change gate could not run — for
    /// example because the base ref failed to compile under the current
    /// Rocky version. Fail-open: the gate is skipped and the promote
    /// proceeds, but the reason is recorded so the bypass is auditable.
    BreakingChangesGateSkipped,
    /// Emitted by `rocky plan promote` when the plan has been successfully
    /// written to `.rocky/plans/<plan_id>.json`. Carries the plan_id so an
    /// audit consumer can correlate plan creation to a subsequent
    /// `rocky apply` event without scanning the filesystem.
    PromotePlanCreated,
}

/// Single audit-trail event emitted during `branch promote`.
///
/// Routed to stdout JSON only in v1; persistent audit storage is a follow-up.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct AuditEvent {
    pub kind: AuditEventKind,
    pub at: DateTime<Utc>,
    pub actor: ApproverIdentity,
    pub branch: String,
    pub branch_state_hash: String,
    /// Free-form context. Populated for `ApprovalSkipped` to record the
    /// origin of the skip, and for `BreakingChangesGateSkipped` to record
    /// why the gate could not run (e.g. "base ref did not compile"). Empty
    /// for routine state events.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Breaking-change findings carried by `BreakingChangesBlocked` and
    /// `BreakingChangesAllowed` events. Always absent for other kinds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub breaking_changes: Option<Vec<rocky_core::breaking_change::BreakingFinding>>,
}

/// One per-target promote step in [`BranchPromoteOutput::targets`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PromoteTarget {
    /// Fully-qualified production target (catalog.schema.table).
    pub target: String,
    /// Fully-qualified branch source the promote read from
    /// (catalog.branch_schema.table).
    pub source: String,
    /// SQL statement dispatched to the adapter for this target.
    pub statement: String,
    /// Whether the per-target SQL succeeded. Failures abort the run; on a
    /// failure this is `false` for the failing target and absent for any
    /// targets that never started.
    pub succeeded: bool,
    /// Adapter / SQL error text when `succeeded` is `false`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// JSON output for `rocky branch promote`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct BranchPromoteOutput {
    pub version: String,
    pub command: String,
    pub branch: String,
    pub branch_state_hash: String,
    /// Approval artifacts that satisfied the gate at promote time. Empty when
    /// the gate was disabled (`required = false`) or skipped.
    pub approvals_used: Vec<ApprovalArtifact>,
    /// Approval artifacts loaded from disk that failed verification, with
    /// the reason for rejection. Surfaced even on a successful promote so
    /// operators can spot stale artifacts to clean up.
    pub approvals_rejected: Vec<RejectedApproval>,
    /// Semantic breaking-change findings produced by the pre-promote gate.
    /// Empty when the gate ran and found no breaking changes; absent when
    /// the gate was skipped (compile failure on either side). When present
    /// and non-empty either the promote was blocked or `--allow-breaking`
    /// was set — see the audit trail for which.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub breaking_changes: Option<Vec<rocky_core::breaking_change::BreakingFinding>>,
    /// One entry per managed target the promote attempted, in dispatch order.
    pub targets: Vec<PromoteTarget>,
    /// Audit-trail events emitted during this invocation, in order. At
    /// minimum: `PromoteStarted` plus one of `PromoteCompleted` /
    /// `PromoteFailed`. `ApprovalSkipped` precedes `PromoteStarted` when
    /// the gate was bypassed.
    pub audit: Vec<AuditEvent>,
    /// True when every target's SQL succeeded.
    pub success: bool,
}

/// An approval artifact that was loaded from disk but rejected.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RejectedApproval {
    pub approval_id: String,
    /// One of: `bad_signature`, `state_hash_mismatch`, `expired`,
    /// `signer_not_allowed`, `parse_error`.
    pub reason: String,
    pub detail: String,
}

/// Per-model promote step captured in a [`PromotePlan`].
///
/// Mirrors the shape of [`PromoteTarget`] but contains only plan-time fields
/// (`target`, `source`, `statement`). Execution outcome (`succeeded`, `error`)
/// is added at apply time and lives on [`PromoteTarget`].
///
/// `statement` is persisted verbatim so `rocky apply` executes the **exact**
/// SQL generated at plan time — skipping re-discovery and ensuring the
/// `plan_id` digest is invalidated if the SQL would differ.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PromoteTargetPlan {
    /// Fully-qualified production target (catalog.schema.table).
    pub target: String,
    /// Fully-qualified branch source the promote will read from
    /// (catalog.branch_schema.table).
    pub source: String,
    /// `CREATE OR REPLACE TABLE <target> AS SELECT * FROM <source>` SQL,
    /// dialect-quoted at plan time.
    pub statement: String,
}

/// Persisted payload for a `rocky plan promote` run plan.
///
/// Written to `.rocky/plans/<plan_id>.json` by `rocky plan promote` and read
/// back by `rocky apply <plan_id>` (dispatched as `PlanKind::Promote`).
///
/// ## What is persisted vs re-derived
///
/// - **Persisted**: branch name + refs, state hash at plan time, approval
///   artifacts used/rejected, breaking-change findings, per-target SQL
///   statements, and the plan-time audit events.
/// - **Re-derived at apply time**: nothing related to approvals or
///   breaking-change gate — those gates ran at plan time and their
///   outcomes are captured here. The warehouse adapter is resolved at apply
///   time (to call `execute_statement`), but discovery is NOT re-run.
///
/// ## Branch state drift at apply time
///
/// `branch_state_hash` reflects the branch metadata + config bytes at plan
/// time. If the branch's config or metadata changes between `rocky plan
/// promote` and `rocky apply`, the persisted hash differs from the live hash.
/// By default, `rocky apply` does **not** re-check the hash — the gates
/// already ran at plan time, and that is the point of the plan/apply split.
/// Operators who need a strict re-check can re-run `rocky plan promote` to
/// produce a fresh plan.
///
/// Warehouse table contents are not covered by `branch_state_hash` in v1 —
/// if a branch's tables are mutated between plan and apply, `rocky apply`
/// will promote whatever data is in the branch schema at apply time.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PromotePlan {
    /// Branch name being promoted.
    pub branch_name: String,
    /// Git ref that `base_ref` was resolved to at plan time (e.g. `"main"`).
    pub base_ref: String,
    /// Git HEAD SHA at plan time — informational for audit purposes.
    pub head_ref: String,
    /// Content-addressed hash of the branch metadata + config bytes at plan
    /// time. Stored for audit purposes; not re-validated at apply time.
    pub branch_state_hash: String,
    /// Approval artifacts that satisfied the gate at plan time.
    pub approvals_used: Vec<ApprovalArtifact>,
    /// Approval artifacts loaded from disk that failed verification at plan
    /// time.
    pub approvals_rejected: Vec<RejectedApproval>,
    /// Semantic breaking-change findings produced by the pre-promote gate at
    /// plan time. Empty when the gate ran and found no breaking changes;
    /// absent when the gate was skipped (compile failure on either side).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub breaking_changes: Option<Vec<rocky_core::breaking_change::BreakingFinding>>,
    /// Whether `--allow-breaking` was set at plan time.
    #[serde(default)]
    pub allow_breaking: bool,
    /// Per-model SQL plan, in dispatch order. SQL is persisted verbatim so
    /// `rocky apply` executes the exact statements generated at plan time.
    pub targets: Vec<PromoteTargetPlan>,
    /// Plan-time audit events (approvals gate + breaking-change gate outcomes).
    /// Apply-time events (`PromoteStarted`, `PromoteCompleted`, `PromoteFailed`)
    /// are appended in `BranchPromoteOutput.audit` at apply time.
    pub plan_audit: Vec<AuditEvent>,
    /// When this plan was persisted.
    pub created_at: DateTime<Utc>,
}

/// JSON output for `rocky review <plan-id>`.
///
/// `rocky review` is the human sign-off gate for an AI-authored plan. It
/// compares the working-tree models against `base_ref`, classifies the
/// semantic delta, and either reports the findings (dry run) or — with
/// `--approve` — writes a review marker that unblocks `rocky apply`.
///
/// The marker is written even when breaking changes exist, on the premise
/// that the human approving has seen this report and is signing off on them
/// explicitly. `breaking_changes` therefore always lists the full classified
/// delta so the approval is informed.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ReviewOutput {
    pub version: String,
    pub command: String,
    /// The AI-authored plan being reviewed (64-char blake3 hex).
    pub plan_id: String,
    /// Git ref the working tree was compared against (default `HEAD`).
    pub base_ref: String,
    /// True when this invocation wrote the approval marker (i.e. `--approve`
    /// was set). When false, the review was a dry run and `rocky apply` stays
    /// blocked.
    pub approved: bool,
    /// True when the approval marker is now present on disk as a result of
    /// this invocation. Mirrors `approved` today; kept distinct so callers
    /// reading the JSON do not have to infer marker state from the flag.
    pub marker_written: bool,
    /// Semantic breaking-change findings between `base_ref` and the working
    /// tree. Empty when the classifier ran and found no breaking changes;
    /// absent when the gate was skipped (compile failure on either side, or
    /// the models directory was unavailable).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub breaking_changes: Option<Vec<rocky_core::breaking_change::BreakingFinding>>,
    /// Human-readable summary of the review outcome.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// JSON output for `rocky review --queue` — the pending-review work queue.
///
/// Lists every `require_review` policy decision that has not yet been
/// satisfied by a sign-off marker, ranked so the change most in need of a
/// human floats to the top. The rank blends three signals: how much sits
/// downstream of the model (blast radius), how disruptive the change class is
/// (a breaking schema change outranks an additive one), and how long the
/// escalation has waited (staleness). Each entry carries the exact
/// `rocky review <plan_id> --approve` invocation that clears it — the queue
/// is a triage view; the approval path is unchanged.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ReviewQueueOutput {
    pub version: String,
    pub command: String,
    /// Human-readable description of the ordering, e.g.
    /// `"blast_radius × classification × staleness"`.
    pub ranking: String,
    /// Count of pending escalations in the queue.
    pub total: u64,
    /// The pending escalations, highest-priority first.
    pub pending: Vec<ReviewQueueEntry>,
}

/// One pending `require_review` escalation inside [`ReviewQueueOutput`].
#[derive(Debug, Serialize, JsonSchema)]
pub struct ReviewQueueEntry {
    /// The plan whose approval clears this escalation.
    pub plan_id: String,
    /// Composite ledger key (`"{timestamp}|{plan_id}|{model}"`) — the stable
    /// identity a governor drills into via `rocky audit --for`.
    pub decision_ref: String,
    /// RFC 3339 timestamp when the decision was recorded.
    pub timestamp: String,
    /// Who authored the change (`human` / `agent`).
    pub principal: rocky_core::config::PolicyPrincipal,
    /// The capability that was evaluated (its `schema_change.*` refinement is
    /// the change class the ranking weighs).
    pub capability: rocky_core::config::PolicyCapability,
    /// The model the escalation is about.
    pub model: String,
    /// Index of the winning `[[policy.rules]]` entry, or `null` when the
    /// escalation came from the default posture.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rule_id: Option<usize>,
    /// Human-readable explanation of how `require_review` was reached.
    pub reason: String,
    /// Count of models that transitively consume this one — the blast radius
    /// the ranking weighs. `null` when the model could not be located in the
    /// compiled graph (e.g. it was removed), in which case the ranking treats
    /// the blast radius as zero and this entry says so.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blast_radius: Option<u64>,
    /// The change-class weight the ranking used (breaking > bare verb >
    /// additive / value-only).
    pub classification_weight: u32,
    /// How long the escalation has waited, in whole seconds.
    pub staleness_seconds: i64,
    /// The composite priority score. Higher sorts first. Reported so a
    /// consumer can re-rank or explain the ordering without recomputing it.
    pub score: f64,
    /// The exact command that clears this escalation.
    pub approve_command: String,
}

/// JSON output for `rocky policy check`.
///
/// Explain-mode only: reports the effect the agent policy plane *would*
/// resolve for a `(principal, capability, model)` triple, the winning rule
/// (if any), and why. It does not gate any real command in v0.
#[derive(Debug, Serialize, JsonSchema)]
pub struct PolicyCheckOutput {
    pub version: String,
    pub command: String,
    /// The principal that was checked (`human` / `agent`).
    pub principal: rocky_core::config::PolicyPrincipal,
    /// The capability that was checked.
    pub capability: rocky_core::config::PolicyCapability,
    /// The model that was checked.
    pub model: String,
    /// Resolved effect: `allow`, `require_review`, or `deny`.
    pub effect: rocky_core::config::PolicyEffect,
    /// Zero-based index of the winning rule in `[[policy.rules]]`, or
    /// `null` when the decision came from a short-circuit (`read`) or the
    /// default posture (no rule matched).
    pub matched_rule: Option<usize>,
    /// Human-readable explanation of how the effect was reached.
    pub reason: String,
    /// The compiled model attributes the matcher read.
    pub model_attributes: PolicyModelAttributes,
}

/// The compiled attributes of the checked model, echoed back so the
/// explain output is self-contained.
#[derive(Debug, Serialize, JsonSchema)]
pub struct PolicyModelAttributes {
    /// Model-level governance tags.
    pub tags: BTreeMap<String, String>,
    /// Distinct column-classification values present on the model.
    pub classifications: Vec<String>,
    /// Medallion/semantic layer (the model's `layer` tag), if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub layer: Option<String>,
    /// Whether the model sits behind a contract (best-effort: a sibling
    /// `.contract.toml` exists).
    pub contracted: bool,
    /// Direct downstream-consumer count (models that `depends_on` this one).
    /// Informational — the `max_downstreams` ceiling reads
    /// [`Self::reachable_downstreams`].
    pub downstreams: u64,
    /// Transitive downstream reachability — the full blast radius (direct +
    /// indirect), excluding the model itself. This is what a rule's
    /// `max_downstreams` ceiling is compared against. `null` when the blast
    /// radius could not be computed (the ceiling then fails closed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reachable_downstreams: Option<u64>,
}

/// JSON output for `rocky policy test` — the scenario-assertion runner.
///
/// Runs every `[[policy.tests]]` scenario through the real policy evaluator
/// and reports, per scenario, whether the resolved effect matched the
/// expectation. A non-empty `failed` count fails the command (non-zero exit),
/// so a policy edit that would silently open a hole is caught in CI.
#[derive(Debug, Serialize, JsonSchema)]
pub struct PolicyTestOutput {
    pub version: String,
    pub command: String,
    /// Total scenarios asserted.
    pub total: usize,
    /// How many scenarios resolved to their expected effect.
    pub passed: usize,
    /// How many scenarios resolved to a different effect than expected.
    pub failed: usize,
    /// Per-scenario results, in declaration order.
    pub results: Vec<PolicyTestResult>,
}

/// The outcome of one `[[policy.tests]]` scenario.
#[derive(Debug, Serialize, JsonSchema)]
pub struct PolicyTestResult {
    /// The scenario's name.
    pub name: String,
    /// `true` when the resolved effect equalled `expected`.
    pub passed: bool,
    /// The principal that was checked.
    pub principal: rocky_core::config::PolicyPrincipal,
    /// The capability that was checked.
    pub capability: rocky_core::config::PolicyCapability,
    /// The synthetic model name the scenario targeted.
    pub model: String,
    /// The effect the scenario expected.
    pub expected: rocky_core::config::PolicyEffect,
    /// The effect the evaluator actually resolved.
    pub actual: rocky_core::config::PolicyEffect,
    /// Zero-based index of the rule that decided `actual`, or `null` when the
    /// decision came from a short-circuit (`read`) or the default posture.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matched_rule: Option<usize>,
    /// The evaluator's explanation of how `actual` was reached — the decisive
    /// context on a failure.
    pub reason: String,
}

/// JSON output for `rocky policy freeze` / `rocky policy unfreeze` — the kill
/// switch.
///
/// A freeze records a decision entry per matched principal into the existing
/// policy-decision ledger; at the enforcement seam an active freeze forces
/// `deny`. `unfreeze` records a superseding entry that lifts it. No config file
/// is rewritten and no new table is created.
#[derive(Debug, Serialize, JsonSchema)]
pub struct PolicyFreezeOutput {
    pub version: String,
    /// `policy_freeze` or `policy_unfreeze`.
    pub command: String,
    /// `true` when this call lifted a freeze rather than establishing one.
    pub lifted: bool,
    /// The scope selector the freeze targets (`any`, `layer=…`, `tag=…`, …).
    pub scope: String,
    /// RFC 3339 wall clock when the freeze/unfreeze was recorded.
    pub recorded_at: String,
    /// One entry per affected principal.
    pub entries: Vec<PolicyFreezeEntry>,
}

/// One principal's freeze/unfreeze record inside a [`PolicyFreezeOutput`].
#[derive(Debug, Serialize, JsonSchema)]
pub struct PolicyFreezeEntry {
    /// The principal whose actions were frozen/unfrozen.
    pub principal: rocky_core::config::PolicyPrincipal,
    /// The recorded effect (`deny` for a freeze, `allow` for an unfreeze).
    pub effect: rocky_core::config::PolicyEffect,
    /// Composite ledger key citation (`"{timestamp}|{plan_id}|{scope}"`).
    pub decision_ref: String,
    /// The freeze decision's `plan_id`.
    pub plan_id: String,
    /// Human-readable description of the freeze/unfreeze.
    pub reason: String,
}

/// JSON output for `rocky audit` — the agent-policy decision ledger.
///
/// Lists every policy decision recorded at a mutating enforcement seam
/// (`rocky apply` / promote), oldest first. Reads are never recorded, so this
/// is exclusively the trail of *governed mutations* the plane evaluated.
#[derive(Debug, Serialize, JsonSchema)]
pub struct AuditOutput {
    pub version: String,
    pub command: String,
    /// Every recorded policy decision, oldest first.
    pub decisions: Vec<AuditDecisionEntry>,
}

/// One recorded policy decision in the [`AuditOutput`] ledger.
#[derive(Debug, Serialize, JsonSchema)]
pub struct AuditDecisionEntry {
    /// RFC 3339 timestamp when the decision was recorded.
    pub timestamp: String,
    /// The plan the decision governed.
    pub plan_id: String,
    /// Who was acting (`human` / `agent`).
    pub principal: rocky_core::config::PolicyPrincipal,
    /// The capability that was evaluated.
    pub capability: rocky_core::config::PolicyCapability,
    /// The model the decision was about.
    pub model: String,
    /// The resolved verdict (`allow` / `require_review` / `deny`).
    pub effect: rocky_core::config::PolicyEffect,
    /// Index of the winning `[[policy.rules]]` entry, or `null` for the
    /// default posture.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rule_id: Option<usize>,
    /// Human-readable explanation of how the effect was reached.
    pub reason: String,
}

// ---------------------------------------------------------------------------
// `rocky audit --for` — the custody chain drill-down
// ---------------------------------------------------------------------------

/// What `rocky audit --for <selector>` resolved its selector to.
///
/// The selector is resolved in priority order: a 64-char hex string with a
/// plan file on disk is a [`AuditSubjectKind::Plan`]; a string matching a
/// `run_id` in the run ledger is a [`AuditSubjectKind::Run`]; anything else is
/// treated as a [`AuditSubjectKind::Model`] name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AuditSubjectKind {
    /// A model / table name — the primary custody-chain entry point.
    Model,
    /// A `run_id` from the run ledger.
    Run,
    /// A `plan_id` (64-char blake3 hex) with a plan file on disk.
    Plan,
}

/// JSON output for `rocky audit --for <table|run|plan>` — the custody chain.
///
/// A one-query drill-down assembled link by link from the data Rocky already
/// records: who proposed the change and what the policy plane decided
/// ([`Self::decisions`]), what the plan changed ([`Self::plan`]), which runs
/// materialized it ([`Self::runs`]), what a post-apply verification found
/// ([`Self::verify_after`]), and what sits downstream in its blast radius
/// ([`Self::blast_radius`]).
///
/// Every link fails closed the same way the estate brief does: a link whose
/// signal is genuinely not recorded renders [`SectionAvailability::Unavailable`]
/// with a note, never a fabricated or assumed value. Notably, post-apply
/// verification outcomes are not persisted anywhere today, so
/// [`Self::verify_after`] is always `unavailable`; and the run ledger is not
/// keyed to policy decisions, so a `run` selector cannot join back to a
/// decision. The blast radius is recomputed from the current compiled graph
/// (a live query, not a stored snapshot).
#[derive(Debug, Serialize, JsonSchema)]
pub struct AuditForOutput {
    pub version: String,
    pub command: String,
    /// The selector as supplied on the command line.
    pub subject: String,
    /// What the selector resolved to.
    pub subject_kind: AuditSubjectKind,
    /// Whether the selector matched anything at all (a decision, a run, a plan
    /// file, or a model in the graph). `false` when nothing referenced it —
    /// every link is then empty and says why.
    pub resolved: bool,
    /// Who proposed the change and what the policy plane decided — the
    /// persisted decision ledger, scoped to this subject.
    pub decisions: AuditChainDecisions,
    /// What the governing plan changed (its embedded per-model change
    /// classification).
    pub plan: AuditChainPlan,
    /// Runs that materialized the subject.
    pub runs: AuditChainRuns,
    /// What a post-apply verification found — not recorded today.
    pub verify_after: AuditChainVerify,
    /// What sits downstream of the subject — the CLL blast radius.
    pub blast_radius: AuditChainBlastRadius,
}

/// Decision link of the custody chain: the policy-decision ledger scoped to
/// the subject (by model for a model selector, by plan_id for a plan
/// selector).
#[derive(Debug, Serialize, JsonSchema)]
pub struct AuditChainDecisions {
    pub availability: SectionAvailability,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    pub total: u64,
    /// The matching decisions, newest first.
    pub entries: Vec<AuditDecisionEntry>,
}

/// Plan link of the custody chain: what the governing plan changed, read from
/// the plan file's embedded change-classification.
#[derive(Debug, Serialize, JsonSchema)]
pub struct AuditChainPlan {
    pub availability: SectionAvailability,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    /// The plan that governed the subject (the newest one, for a model
    /// selector). `null` when no plan file could be located.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan_id: Option<String>,
    /// The plan's authoring principal.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub principal: Option<rocky_core::config::PolicyPrincipal>,
    /// The plan kind (`run` / `ai_authored` / …).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// Whether the base↔head change classification was available when the
    /// plan was authored. `false` means every planned model was treated as a
    /// breaking change (fail-closed), and `changes` reflects that.
    pub diff_available: bool,
    /// The per-model change classification the plan carried — the persisted
    /// stand-in for the typed diff. The full field-level `diff_project_ir`
    /// output is not persisted for run plans, only this classification is.
    pub changes: Vec<AuditPlanChange>,
}

/// One model's change classification inside [`AuditChainPlan`].
#[derive(Debug, Serialize, JsonSchema)]
pub struct AuditPlanChange {
    pub model: String,
    /// The change class the plan recorded for this model
    /// (`schema_change.additive` / `schema_change.breaking` / `value_change`
    /// / a bare verb).
    pub capability: rocky_core::config::PolicyCapability,
}

/// Run link of the custody chain: runs that materialized the subject.
#[derive(Debug, Serialize, JsonSchema)]
pub struct AuditChainRuns {
    pub availability: SectionAvailability,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    pub total: u64,
    /// Matching runs, newest first.
    pub runs: Vec<AuditRunEntry>,
}

/// One run inside [`AuditChainRuns`].
#[derive(Debug, Serialize, JsonSchema)]
pub struct AuditRunEntry {
    pub run_id: String,
    pub status: String,
    pub started_at: String,
    pub finished_at: String,
    /// Best-effort caller identity recorded on the run.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub triggering_identity: Option<String>,
}

/// Verify-after link of the custody chain.
///
/// Post-apply verification outcomes are not persisted to the state store
/// today, so this link is always [`SectionAvailability::Unavailable`] with a
/// note — never a smoothed-over "verification passed".
#[derive(Debug, Serialize, JsonSchema)]
pub struct AuditChainVerify {
    pub availability: SectionAvailability,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

/// Blast-radius link of the custody chain: the models that transitively
/// consume the subject, recomputed from the current compiled graph.
#[derive(Debug, Serialize, JsonSchema)]
pub struct AuditChainBlastRadius {
    pub availability: SectionAvailability,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    /// The model the blast radius was computed for.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Direct downstream consumers (one hop).
    pub direct: Vec<String>,
    /// All transitive downstream consumers (the full closure, sorted).
    pub transitive: Vec<String>,
    /// Size of the transitive closure.
    pub total: u64,
}

// ---------------------------------------------------------------------------
// `rocky audit --scorecard` — the decisions-by-group trust scorecard
// ---------------------------------------------------------------------------

/// The dimension a scorecard groups policy decisions by.
///
/// Each maps to a field the ledger persists on every decision: `principal`
/// groups by who acted (`agent` / `human`), `rule` by the winning
/// `[[policy.rules]]` index, and `scope` by the concrete model the decision was
/// about (the ledger records the matched model, not the rule's scope
/// predicates).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ScorecardDimension {
    /// Group by who acted (`agent` / `human`).
    Principal,
    /// Group by the winning `[[policy.rules]]` index (or the default posture).
    Rule,
    /// Group by the concrete model the decision was about.
    Scope,
}

/// JSON output for `rocky audit --scorecard` — a trust-calibration digest.
///
/// A decisions-by-group aggregation over the persisted policy-decision ledger,
/// windowed by `--window`. It is the evidence base for widening or tightening
/// autonomy, and it informs human judgment only — nothing here is wired to any
/// automatic policy change.
///
/// Only metrics the ledger actually persists are computed. The ledger records
/// one row per policy *evaluation* — `(principal, capability, model, effect,
/// rule_id)` — and nothing about what happened *after* the decision. So
/// verify-after outcomes, reverts, and escalation-resolution latency are not
/// derivable; they are declared, once, in [`Self::unavailable_metrics`] as
/// `unavailable` with the reason, never faked into a number. A ledger read
/// failure renders the whole scorecard `unavailable` rather than a
/// smoothed-over zero.
#[derive(Debug, Serialize, JsonSchema)]
pub struct AuditScorecardOutput {
    pub version: String,
    pub command: String,
    /// The grouping dimension (`--by`).
    pub by: ScorecardDimension,
    /// The window as requested (`all`, `30d`, `24h`, …).
    pub window: String,
    /// Lower bound of the window (RFC 3339), or `null` for all-time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window_start: Option<String>,
    /// Whether the scorecard could be composed at all: `unavailable` when the
    /// ledger could not be read (fail-closed), `no_data` when no decision falls
    /// in the window, `available` otherwise.
    pub availability: SectionAvailability,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    /// Total decisions in the window, across all groups.
    pub total_decisions: u64,
    /// One row per group, ranked by decision count descending.
    pub groups: Vec<ScorecardGroup>,
    /// Metrics the ledger does not persist, declared plainly rather than
    /// computed. Each is `unavailable` with the reason it cannot be derived.
    pub unavailable_metrics: Vec<ScorecardUnavailableMetric>,
}

/// One group's decision aggregate in an [`AuditScorecardOutput`].
///
/// Every rate is a ratio over [`Self::total`] and is independently
/// recomputable from [`Self::decision_refs`], which lists the exact ledger keys
/// (`timestamp|plan_id|model`) that composed the group — so the aggregate
/// summarizes citable rows, never an unverifiable number.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ScorecardGroup {
    /// The group label: a principal (`agent` / `human`), a rule (`rule 3` /
    /// `default posture`), or a model/scope name — per the scorecard's `by`.
    pub key: String,
    /// Decisions in this group within the window.
    pub total: u64,
    /// Decisions the plane allowed outright.
    pub allow: u64,
    /// Decisions the plane escalated to human review.
    pub require_review: u64,
    /// Decisions the plane denied.
    pub deny: u64,
    /// `allow / total` — the proposal acceptance rate.
    pub acceptance_rate: f64,
    /// `deny / total` — the denial rate.
    pub denial_rate: f64,
    /// `require_review / total` — the escalation rate.
    pub review_rate: f64,
    /// The ledger keys that composed this group (`timestamp|plan_id|model`),
    /// newest first — the citations backing every count above.
    pub decision_refs: Vec<String>,
}

/// A metric the scorecard cannot compute because the ledger does not persist
/// its inputs.
///
/// Declared explicitly (not silently omitted) so the honesty is
/// machine-readable: a consumer sees the metric name, that it is `unavailable`,
/// and exactly why.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ScorecardUnavailableMetric {
    /// The metric identifier (`verify_after_pass_rate`, `reverts`,
    /// `escalation_latency`).
    pub metric: String,
    /// Always [`SectionAvailability::Unavailable`].
    pub availability: SectionAvailability,
    /// Why the metric cannot be derived from the persisted ledger.
    pub note: String,
}

// ---------------------------------------------------------------------------
// `rocky brief` — the governor's estate digest
// ---------------------------------------------------------------------------

/// Whether a brief section's underlying query succeeded and had data.
///
/// The digest is composed section-by-section from independent typed queries
/// over the state store and the decision ledger. Each section fails closed:
/// a query that returns nothing renders as [`SectionAvailability::NoData`],
/// and a source that is not wired into the state store at all renders as
/// [`SectionAvailability::Unavailable`] with a note — never as a
/// smoothed-over "all clear". A brief that claims more than the ledger holds
/// poisons the whole surface, so the marker is machine-readable, not prose.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SectionAvailability {
    /// The query ran and the section carries data for the window.
    Available,
    /// The query ran cleanly but nothing fell inside the window.
    NoData,
    /// The underlying signal is not recorded in the state store, so the
    /// section cannot be composed. Accompanied by a `note` explaining why.
    Unavailable,
}

/// How the digest window was resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum BriefSinceMode {
    /// Everything recorded since the previous `--since last` digest (the
    /// stored cursor). Advances the cursor on success.
    Last,
    /// A rolling 24-hour window ending now. Does not touch the cursor.
    #[serde(rename = "24h")]
    Hours24,
    /// A rolling 7-day window ending now. Does not touch the cursor.
    #[serde(rename = "7d")]
    Days7,
}

/// JSON output for `rocky brief` — the governor's estate digest.
///
/// A typed projection of the state store and the policy-decision ledger over
/// a time window: agent activity by principal, runs, drift, freshness,
/// quality, cost, and the ranked queue of decisions still awaiting a human.
/// It is composed template-first from typed queries — there is no narration
/// layer — and every line item carries a ledger citation (`run_id`,
/// `plan_id`, or the composite `decision_ref`) so a claim can be
/// independently checked against `rocky audit` / `rocky replay`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct BriefOutput {
    pub version: String,
    pub command: String,
    /// RFC 3339 wall clock when the digest was generated (the window's upper
    /// bound).
    pub generated_at: String,
    /// How the window was resolved.
    pub since_mode: BriefSinceMode,
    /// RFC 3339 lower bound of the window. `null` only for a first-ever
    /// `--since last` with no stored cursor — the digest then spans all of
    /// recorded history.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub since_timestamp: Option<String>,
    /// Agent- and human-authored policy decisions in the window, grouped by
    /// principal and effect.
    pub agent_activity: BriefAgentActivitySection,
    /// Decisions that resolved to `require_review` and still need a human,
    /// ranked.
    pub escalations: BriefEscalationsSection,
    /// Pipeline runs in the window, with the ones needing attention listed.
    pub runs: BriefRunsSection,
    /// Schema drift observed in the window.
    pub drift: BriefDriftSection,
    /// Freshness / SLO status in the window.
    pub freshness: BriefFreshnessSection,
    /// Data-quality status in the window.
    pub quality: BriefQualitySection,
    /// Cost and budget burn across the window's runs.
    pub cost: BriefCostSection,
    /// Autonomy-budget degradations and active policy freezes — the dynamic
    /// tightening currently in force across the estate.
    pub autonomy: BriefAutonomySection,
}

/// Autonomy section — rules whose autonomy budget is currently exhausted
/// (degraded to `require_review`) and policy freezes in force.
///
/// This is a *current-state* projection over the whole decision ledger — each
/// budget uses its own configured window, independent of the digest's
/// `--since`. It fails closed: `unavailable` when the ledger or config can't be
/// read, `no_data` when nothing is degraded or frozen.
#[derive(Debug, Serialize, JsonSchema)]
pub struct BriefAutonomySection {
    pub availability: SectionAvailability,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    /// Rules whose budget is exhausted right now — each auto-degraded to
    /// `require_review` until its failures age out of the window.
    pub degraded_rules: Vec<BriefDegradedRule>,
    /// Policy freezes currently in force.
    pub active_freezes: Vec<BriefActiveFreeze>,
}

/// A budget-exhausted (degraded) rule inside [`BriefAutonomySection`].
#[derive(Debug, Serialize, JsonSchema)]
pub struct BriefDegradedRule {
    /// Index of the degraded `[[policy.rules]]` entry.
    pub rule_id: usize,
    /// Verify-after failures counted in the window.
    pub failures: u64,
    /// The rule's configured failure ceiling.
    pub limit: u64,
    /// The rule's configured window (`7d`, `24h`, …).
    pub window: String,
}

/// An active policy freeze inside [`BriefAutonomySection`].
#[derive(Debug, Serialize, JsonSchema)]
pub struct BriefActiveFreeze {
    pub principal: rocky_core::config::PolicyPrincipal,
    /// The scope selector the freeze targets.
    pub scope: String,
    /// RFC 3339 wall clock when the freeze was recorded — the citation.
    pub frozen_at: String,
    /// The freeze decision's `plan_id`.
    pub plan_id: String,
}

/// Agent-activity section — the policy-decision ledger rolled up by principal.
#[derive(Debug, Serialize, JsonSchema)]
pub struct BriefAgentActivitySection {
    pub availability: SectionAvailability,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    pub total: u64,
    pub allow: u64,
    pub require_review: u64,
    pub deny: u64,
    /// One roll-up per acting principal (`human` / `agent`).
    pub by_principal: Vec<BriefPrincipalActivity>,
    /// Every decision in the window, newest first, each fully cited.
    pub decisions: Vec<BriefDecisionEntry>,
}

/// Per-principal decision counts inside [`BriefAgentActivitySection`].
#[derive(Debug, Serialize, JsonSchema)]
pub struct BriefPrincipalActivity {
    pub principal: rocky_core::config::PolicyPrincipal,
    pub total: u64,
    pub allow: u64,
    pub require_review: u64,
    pub deny: u64,
}

/// One recorded policy decision, cited for the digest.
///
/// `decision_ref` is the composite ledger key
/// (`"{timestamp}|{plan_id}|{model}"`) — the stable identity a governor drills
/// into via `rocky audit`. `plan_id` and `rule_id` are the other two
/// citations: which plan the decision governed and which `[[policy.rules]]`
/// entry won.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct BriefDecisionEntry {
    /// RFC 3339 timestamp when the decision was recorded.
    pub timestamp: String,
    /// Composite ledger key that uniquely identifies this decision.
    pub decision_ref: String,
    /// The plan the decision governed.
    pub plan_id: String,
    pub principal: rocky_core::config::PolicyPrincipal,
    pub capability: rocky_core::config::PolicyCapability,
    /// The model the decision was about.
    pub model: String,
    pub effect: rocky_core::config::PolicyEffect,
    /// Index of the winning `[[policy.rules]]` entry, or `null` for the
    /// default posture.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rule_id: Option<usize>,
    /// Human-readable explanation of how the effect was reached.
    pub reason: String,
}

/// Escalations section — `require_review` decisions still awaiting a human.
#[derive(Debug, Serialize, JsonSchema)]
pub struct BriefEscalationsSection {
    pub availability: SectionAvailability,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    pub total: u64,
    /// How the queue is ordered. `"recency"` in v0; blast-radius ranking
    /// (CLL × classification × staleness) is a later phase.
    pub ranking: String,
    /// The pending decisions, ranked, each fully cited.
    pub pending: Vec<BriefDecisionEntry>,
}

/// Runs section — the run ledger rolled up by status.
#[derive(Debug, Serialize, JsonSchema)]
pub struct BriefRunsSection {
    pub availability: SectionAvailability,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    pub total: u64,
    pub succeeded: u64,
    pub partial_failure: u64,
    pub failed: u64,
    /// Runs that did not fully succeed, newest first — the exception view.
    /// Each cites its `run_id`.
    pub attention: Vec<BriefRunEntry>,
}

/// A single run needing attention inside [`BriefRunsSection`].
#[derive(Debug, Serialize, JsonSchema)]
pub struct BriefRunEntry {
    /// The run's identifier — the citation to drill in via `rocky replay`.
    pub run_id: String,
    pub status: String,
    pub trigger: String,
    pub started_at: String,
    pub finished_at: String,
    /// Models within the run that did not report `success`.
    pub failed_models: Vec<BriefFailedModel>,
}

/// A non-successful model execution inside a [`BriefRunEntry`].
#[derive(Debug, Serialize, JsonSchema)]
pub struct BriefFailedModel {
    pub model_name: String,
    pub status: String,
}

/// Drift section — schema drift recorded in the state store.
#[derive(Debug, Serialize, JsonSchema)]
pub struct BriefDriftSection {
    pub availability: SectionAvailability,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    /// Drift events in the window, newest first.
    pub events: Vec<BriefDriftEntry>,
}

/// A single schema-drift observation inside [`BriefDriftSection`].
#[derive(Debug, Serialize, JsonSchema)]
pub struct BriefDriftEntry {
    pub timestamp: String,
    /// The DAG graph hash the change was observed against — the citation for
    /// this drift snapshot.
    pub graph_hash: String,
    /// Human-readable description of the change.
    pub change: String,
}

/// Freshness / SLO section, derived from recorded quality snapshots.
#[derive(Debug, Serialize, JsonSchema)]
pub struct BriefFreshnessSection {
    pub availability: SectionAvailability,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    /// Per-model freshness lag, worst first. Each cites its `run_id`.
    pub models: Vec<BriefFreshnessEntry>,
}

/// A single model's freshness reading inside [`BriefFreshnessSection`].
#[derive(Debug, Serialize, JsonSchema)]
pub struct BriefFreshnessEntry {
    pub model_name: String,
    pub run_id: String,
    pub freshness_lag_seconds: u64,
    pub observed_at: String,
}

/// Quality / check section, derived from recorded quality snapshots.
#[derive(Debug, Serialize, JsonSchema)]
pub struct BriefQualitySection {
    pub availability: SectionAvailability,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    /// Per-model quality readings, newest first. Each cites its `run_id`.
    pub models: Vec<BriefQualityEntry>,
}

/// A single model's quality reading inside [`BriefQualitySection`].
#[derive(Debug, Serialize, JsonSchema)]
pub struct BriefQualityEntry {
    pub model_name: String,
    pub run_id: String,
    pub observed_at: String,
    pub row_count: u64,
    /// The highest per-column null rate observed for the model, if any
    /// columns were profiled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_null_rate: Option<f64>,
}

/// Cost section — per-run cost re-derived over the window, plus budget burn.
#[derive(Debug, Serialize, JsonSchema)]
pub struct BriefCostSection {
    pub availability: SectionAvailability,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    /// The billed-warehouse adapter cost was computed against, if resolvable
    /// from `rocky.toml`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub adapter_type: Option<String>,
    pub run_count: u64,
    /// Summed observed cost across the window. `null` when no run produced
    /// enough data to compute a cost (e.g. DuckDB, which has no billing).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_cost_usd: Option<f64>,
    pub total_duration_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_bytes_scanned: Option<u64>,
    /// Per-run cost in the window, priciest first. Each cites its `run_id`.
    pub per_run: Vec<BriefRunCost>,
    /// Budget-burn status against the configured per-run `[budget] max_usd`,
    /// when one is set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub budget: Option<BriefBudgetStatus>,
}

/// Per-run cost inside [`BriefCostSection`].
#[derive(Debug, Serialize, JsonSchema)]
pub struct BriefRunCost {
    pub run_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost_usd: Option<f64>,
    pub duration_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_scanned: Option<u64>,
}

/// Budget-burn status against the configured per-run ceiling.
#[derive(Debug, Serialize, JsonSchema)]
pub struct BriefBudgetStatus {
    /// The configured `[budget] max_usd` per-run ceiling.
    pub max_usd_per_run: f64,
    /// How many runs in the window exceeded the ceiling.
    pub runs_over_budget: u64,
    /// The priciest run in the window (its citation), when a cost was
    /// computed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worst_run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worst_run_cost_usd: Option<f64>,
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

/// JSON output for `rocky replay --at <run_id> --check`.
///
/// Read-only replayability audit: classifies every model in a recorded run
/// as replayable or not, using only the state-store ledger — the model's
/// [`rocky_core::state::ProvenanceRecord`] (which embeds the canonical
/// `ModelIr`) and the content-addressed artifact rows. Nothing is executed
/// and the working tree is never read: the recording is the source of truth.
///
/// A model is `replayable` when its provenance record exists, its embedded
/// IR parses under the current engine, and every declared input resolves
/// from the ledger. The `nondeterministic` flag on each model is orthogonal
/// to the verdict — a nondeterministic model is still replayable, but a
/// future re-execution may legitimately diverge.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ReplayCheckOutput {
    pub version: String,
    pub command: String,
    pub run_id: String,
    /// Recorded run status (`success`, `partial_failure`, ...).
    pub status: String,
    /// `true` iff every model in the run is replayable.
    pub replayable: bool,
    /// Total number of models considered (after any `--model` filter).
    pub model_count: usize,
    /// Number of those models classified replayable.
    pub replayable_count: usize,
    pub models: Vec<ReplayCheckModelOutput>,
}

/// Per-model replayability verdict inside a [`ReplayCheckOutput`].
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct ReplayCheckModelOutput {
    pub model_name: String,
    /// `replayable` or `non_replayable`.
    pub verdict: String,
    /// Human-readable reasons the model is not replayable; empty when
    /// replayable.
    pub reasons: Vec<String>,
    /// Whether a provenance record was found for this `(run, model)`.
    pub has_provenance: bool,
    /// Whether the embedded canonical `ModelIr` deserialized under the
    /// current engine. `false` both when there is no provenance and when a
    /// provenance record's IR failed to parse (an IR forward-compat break).
    pub ir_parseable: bool,
    /// Static-scan non-determinism flag (via `rocky_sql::determinism`): the
    /// model's SQL contains a volatile construct (`now()`, `current_timestamp`,
    /// `random()`, an unresolved function, ...). Orthogonal to `verdict`.
    pub nondeterministic: bool,
    /// Match-strength label carried on the provenance record (`strong` or
    /// `heuristic`); `null` when no provenance was found.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proof_class: Option<String>,
    /// Per-input resolvability, one entry per declared upstream.
    pub inputs: Vec<ReplayCheckInputOutput>,
}

/// Resolvability of one declared upstream inside a
/// [`ReplayCheckModelOutput`].
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct ReplayCheckInputOutput {
    /// Fully-qualified `catalog.schema.table` identity of the upstream.
    pub upstream_key: String,
    /// `content` (a recorded blake3 in the artifact ledger) or `watermark`
    /// (a freshness signal over a mutable source).
    pub kind: String,
    /// Whether this input resolves from the ledger for a deterministic replay.
    pub resolvable: bool,
    /// Why the input does not resolve; `null` when resolvable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// JSON output for `rocky replay --at <run_id> --execute [--verify]`.
///
/// Re-execution surface (DuckDB). Each model's recipe is reconstructed from its
/// recorded [`rocky_core::state::ProvenanceRecord`] — never the working tree —
/// re-executed on an in-memory DuckDB engine, and its output artifact's blake3
/// re-derived. With `--verify` that digest is compared against the recorded
/// output hash and a per-model verdict is emitted.
///
/// Two modes share this shape:
///
/// - `--model <m>` re-executes a single, *self-contained* recipe on a throwaway
///   engine. A recipe with recorded content upstreams is `non_replayable` in
///   this mode — resolving those inputs is the DAG mode below.
/// - no `--model` re-executes the *whole run* in topological order on one
///   shared engine, materializing each upstream's **replayed** output so a
///   downstream `SELECT` reads the replayed bytes (never the recorded
///   object-store bytes, never production). A downstream whose in-run upstream
///   could not be replayed is `non_replayable` (fail-closed cascade); an
///   upstream not produced by any model in this run, or a mutable-source
///   watermark, is likewise `non_replayable` rather than substituted.
///
/// Nothing is materialized to any warehouse schema: the entire in-memory engine
/// is an ephemeral replay namespace, discarded after the run, so no production
/// identity is ever touched.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ReplayExecuteOutput {
    pub version: String,
    pub command: String,
    pub run_id: String,
    /// Recorded run status (`success`, `partial_failure`, ...).
    pub status: String,
    /// Whether `--verify` was requested (blake3 comparison performed).
    pub verified: bool,
    /// Total number of models considered (after any `--model` filter).
    pub model_count: usize,
    /// Number of models whose re-execution reproduced the recorded output
    /// byte-for-byte. Always `0` when `--verify` was not requested.
    pub bit_exact_count: usize,
    pub models: Vec<ReplayExecuteModelOutput>,
}

/// Per-model re-execution verdict inside a [`ReplayExecuteOutput`].
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct ReplayExecuteModelOutput {
    pub model_name: String,
    /// The verdict for this model. One of:
    ///
    /// - `bit_exact` — re-execution reproduced the recorded output blake3
    ///   (reachable only through a successful re-execution whose digest
    ///   matched the recording);
    /// - `diverged` — re-execution succeeded but the blake3 differs (expected
    ///   when `nondeterministic` is set; a genuine reproducibility gap
    ///   otherwise);
    /// - `executed` — re-executed without `--verify`, so no comparison was
    ///   made;
    /// - `non_replayable` — the recording alone was insufficient to
    ///   re-execute the model (see `reasons`).
    ///
    /// A `value_equal_order_diff` verdict is reserved for a later
    /// order-insensitive refinement; v1 compares the raw blake3 only, so a
    /// row-order difference reports `diverged` (over-sensitivity fails safe).
    pub verdict: String,
    /// Static-scan non-determinism flag (via `rocky_sql::determinism`). When
    /// `true`, a `diverged` verdict is expected rather than a failure.
    pub nondeterministic: bool,
    /// The recorded output blake3 carried on the provenance record; `null`
    /// when the record held no output hash.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recorded_hash: Option<String>,
    /// The blake3 re-derived by this replay execution; `null` when the recipe
    /// was not executed (a `non_replayable` verdict).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub computed_hash: Option<String>,
    /// Rows produced by the re-execution; `null` when not executed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows: Option<u64>,
    /// Human-readable reasons for a `non_replayable` verdict, or a note on an
    /// expected `diverged`; empty on a clean `bit_exact`.
    pub reasons: Vec<String>,
}

/// JSON output for `rocky gc --derivable --dry-run`.
///
/// A read-only inventory of Rocky-managed content-addressed artifacts that
/// are provably rebuildable — *derivable* — and therefore reclamation
/// candidates. Nothing is deleted or planned for deletion: this surface is
/// inventory-only, so the whole product is the report.
///
/// The candidate universe is the content-addressed artifact ledger, grouped
/// by content hash (each distinct hash is one physical artifact; managed
/// bytes count each hash once). An artifact is `derivable` only when **all
/// five** eligibility checks hold — recipe recorded, replayable,
/// unreferenced, policy allows, past the age threshold. Every check fails
/// closed (any doubt keeps the artifact non-derivable) and is reported per
/// candidate, so each verdict is auditable rather than asserted.
///
/// Scope caveat surfaced in [`Self::notes`]: refcounts see *Rocky's* pointers
/// only. A warehouse-side reference Rocky never recorded (a BI extract, a
/// notebook `SELECT INTO`) is invisible here; the age threshold is the
/// mitigation, and this release measures written-age, not read-recency.
#[derive(Debug, Serialize, JsonSchema)]
pub struct GcReportOutput {
    pub version: String,
    pub command: String,
    /// Total physical bytes of Rocky-managed artifacts, counting each
    /// distinct content hash once.
    pub managed_bytes: u64,
    /// Physical bytes of the derivable subset (distinct content hashes whose
    /// five checks all pass).
    pub derivable_bytes: u64,
    /// [`Self::derivable_bytes`] as a percentage of [`Self::managed_bytes`];
    /// `null` when there are no managed bytes (nothing to divide by).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub derivable_pct: Option<f64>,
    /// Number of distinct content hashes considered.
    pub artifact_count: usize,
    /// How many of those are derivable.
    pub derivable_count: usize,
    /// Whether read-activity tracking backed the age/activity check. Always
    /// `false` in this release: the age check is written-age only (see
    /// [`GcCheckOutput`]), stated conservatively rather than inferred.
    pub read_tracking_available: bool,
    /// The minimum written-age (in days) an artifact must reach to pass the
    /// age/activity check.
    pub min_age_days: i64,
    /// Report-wide caveats an operator must read before trusting the numbers
    /// (Rocky-external references, estimate labeling, unwired policy plane).
    pub notes: Vec<String>,
    /// One entry per distinct content hash, newest write first.
    pub candidates: Vec<GcCandidateOutput>,
}

/// One reclamation candidate inside a [`GcReportOutput`] — a single
/// content-addressed artifact (identified by its content hash) with its five
/// printed eligibility checks.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct GcCandidateOutput {
    /// Model that produced the artifact.
    pub model_name: String,
    /// Run that produced it (joins to the provenance + execution records).
    pub run_id: String,
    /// Content hash (hex) of the artifact bytes — the reclamation unit.
    pub blake3_hash: String,
    /// Physical size of the artifact in bytes.
    pub size_bytes: u64,
    /// When the artifact was written (RFC 3339). Doubles as the conservative
    /// last-access proxy: Rocky has no read-tracking on this adapter, so
    /// write-time is the only defensible recency signal.
    pub written_at: String,
    /// How many ledger rows point at these bytes. `1` means Rocky holds a
    /// single reference (reclaimable on that axis); `> 1` means the bytes are
    /// shared (a branch or replayed run) and must never be evicted.
    pub refcount: u64,
    /// The recipe-identity key (hex) of the producing execution — the "what
    /// exact program produced this?" id. `null` when the producing execution
    /// predates recipe-identity capture; the provenance record still carries
    /// the canonical program.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recipe_id: Option<String>,
    /// Input match-strength label carried on the provenance record (`strong`
    /// or `heuristic`); `null` when no provenance was found.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_proof_class: Option<String>,
    /// Estimated cost to rebuild the artifact via replay.
    pub rebuild_cost: GcRebuildCostOutput,
    /// `true` iff every one of [`Self::checks`] passed.
    pub derivable: bool,
    /// The five eligibility checks, each with its pass/fail and a
    /// human-readable justification. Order is stable:
    /// `recipe_recorded`, `replayable`, `unreferenced`, `policy_allows`,
    /// `age_threshold`.
    pub checks: Vec<GcCheckOutput>,
}

/// One printed eligibility check inside a [`GcCandidateOutput`].
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct GcCheckOutput {
    /// Stable check id: `recipe_recorded`, `replayable`, `unreferenced`,
    /// `policy_allows`, or `age_threshold`.
    pub check: String,
    /// Whether the check passed. A candidate is derivable only when all five
    /// are `true`.
    pub passed: bool,
    /// Why the check reached that verdict — the auditable justification.
    pub detail: String,
}

/// Estimated rebuild cost for a [`GcCandidateOutput`].
///
/// Derived from the *recorded* build's metrics via the same cost model
/// `rocky cost` uses — a replay re-runs the recipe, so its original
/// execution's duration and scanned bytes are the honest predictor. Always
/// an estimate ([`Self::estimated`] is always `true`), never a measured
/// rebuild.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct GcRebuildCostOutput {
    /// Always `true`: this figure is modeled from the recorded build, not
    /// measured by re-running it.
    pub estimated: bool,
    /// Duration of the recorded build in milliseconds — the wall-clock a
    /// replay would repeat.
    pub source_duration_ms: u64,
    /// Bytes scanned by the recorded build; `null` when the adapter didn't
    /// report a figure (mirrors [`rocky_core::state::ModelExecution::bytes_scanned`]).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_bytes_scanned: Option<u64>,
    /// Estimated USD to rebuild, priced by `compute_observed_cost_usd` over
    /// the recorded metrics; `null` when the config or adapter can't price it
    /// (no `rocky.toml`, or a non-billed adapter).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_usd: Option<f64>,
}

/// One artifact scheduled for eviction inside a persisted `rocky gc` plan
/// ([`GcPlan`]).
///
/// Captures the identity of a single derivable artifact — enough to (a)
/// re-locate its exact ledger row at apply time, (b) re-verify eligibility
/// against the live ledger, and (c) write a complete restore tombstone. The
/// recipe-identity triple is captured at plan time from the producing
/// execution.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct GcPlanEviction {
    /// Model that produced the artifact.
    pub model_name: String,
    /// Run that produced it — half of the provenance key restore replays from.
    pub run_id: String,
    /// Content hash (hex) of the artifact bytes — the eviction unit and the
    /// identity a restore re-computes and compares against.
    pub blake3_hash: String,
    /// Object-store path of the artifact — the byte location a physical
    /// reclamation deletes and a restore re-materializes to.
    pub file_path: String,
    /// Physical size of the artifact in bytes.
    pub size_bytes: u64,
    /// Delta commit version the artifact was attached to.
    pub commit_version: u64,
    /// When the artifact was written (RFC 3339).
    pub written_at: String,
    /// Recipe-identity hash of the producing execution; `null` for a
    /// pre-identity build.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recipe_hash: Option<String>,
    /// Input-closure hash of the producing execution; `null` when unrecorded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_hash: Option<String>,
    /// Input match-strength (`strong` / `heuristic`); `null` when unrecorded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_proof_class: Option<String>,
    /// Environment hash of the producing execution; `null` when unrecorded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env_hash: Option<String>,
    /// Hash-scheme version of the producing execution; `null` when unrecorded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash_scheme: Option<String>,
}

/// The persisted payload of a `rocky gc --derivable` plan
/// ([`crate::plan_store::PlanKind::Gc`]).
///
/// Lists only the artifacts the derivability inventory proved are **derivable**
/// — every one passed all five fail-closed checks at plan time. Apply
/// re-verifies each against the live ledger before evicting, so this list is a
/// scoped proposal, never a blind delete order.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct GcPlan {
    /// Engine version that authored the plan.
    pub version: String,
    /// The minimum written-age (days) threshold the inventory applied.
    pub min_age_days: i64,
    /// Sum of [`GcPlanEviction::size_bytes`] across [`Self::evictions`].
    pub total_bytes: u64,
    /// The derivable artifacts proposed for eviction.
    pub evictions: Vec<GcPlanEviction>,
}

/// JSON output of `rocky gc --derivable` (plan mode — no `--dry-run`).
///
/// The plan has been written to the plan store; this reports its id and the
/// scoped proposal. Deletion is symmetric-caution gated: the operator must
/// `rocky review <plan-id> --approve` and then `rocky apply <plan-id>` — the
/// plan itself never deletes.
#[derive(Debug, Serialize, JsonSchema)]
pub struct GcPlanOutput {
    pub version: String,
    pub command: String,
    /// The persisted plan id — pass to `rocky review` then `rocky apply`.
    pub plan_id: String,
    /// Number of derivable artifacts proposed for eviction.
    pub eviction_count: usize,
    /// Total bytes proposed for reclamation.
    pub total_bytes: u64,
    /// Always `true`: a `gc` plan is unconditionally review-gated before apply.
    pub review_required: bool,
    /// Operator caveats (re-verification at apply, restore safety net, scope).
    pub notes: Vec<String>,
    /// The proposed evictions.
    pub evictions: Vec<GcPlanEviction>,
}

/// One artifact successfully evicted by `rocky apply <gc-plan>` — a tombstone
/// was recorded and the ledger row retired.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct GcEvictedOutput {
    pub model_name: String,
    pub run_id: String,
    pub blake3_hash: String,
    pub size_bytes: u64,
    /// Always `true` on this list — the durable restore tombstone was written
    /// atomically with the ledger-row retirement before anything else happened.
    pub tombstone_recorded: bool,
    /// `true` when the bytes were physically deleted through the object-store
    /// adapter; `false` when the physical delete was deferred or failed (a safe
    /// leaked orphan — the tombstone stands and restore still works).
    pub physical_reclaimed: bool,
    /// Human-readable physical-reclamation outcome (`deleted`, `deferred: …`,
    /// or `failed: …`).
    pub physical_status: String,
}

/// One artifact `rocky apply <gc-plan>` **refused** to evict because it was no
/// longer derivable at apply time (the fail-closed re-verification caught
/// ledger drift since plan time — e.g. a new reference appeared).
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct GcRefusedOutput {
    pub model_name: String,
    pub run_id: String,
    pub blake3_hash: String,
    pub size_bytes: u64,
    /// Why the eviction was refused (the failing check summary).
    pub reason: String,
    /// The eligibility checks as re-evaluated at apply time, so the refusal is
    /// auditable rather than asserted.
    pub failed_checks: Vec<GcCheckOutput>,
}

/// JSON output of `rocky apply <gc-plan>`.
///
/// Reports the eviction outcome per planned artifact. Deletion is the highest
/// -stakes operation, so the output is exhaustive: what was evicted (with its
/// tombstone), what was refused (with the live failing checks), and what was
/// an idempotent no-op (already evicted by a prior apply).
#[derive(Debug, Serialize, JsonSchema)]
pub struct GcApplyOutput {
    pub version: String,
    pub command: String,
    pub plan_id: String,
    /// Artifacts evicted — tombstone written, ledger row retired.
    pub evicted: Vec<GcEvictedOutput>,
    /// Artifacts refused because they were no longer derivable at apply time.
    pub refused: Vec<GcRefusedOutput>,
    /// Content hashes of plan entries that were already absent from the ledger
    /// (a prior apply evicted them) — idempotent no-ops, not failures.
    pub already_evicted: Vec<String>,
    /// Total bytes evicted.
    pub bytes_evicted: u64,
    /// Total bytes refused.
    pub bytes_refused: u64,
    /// Count of evicted artifacts.
    pub evicted_count: usize,
    /// Count of refused artifacts.
    pub refused_count: usize,
    /// Operator caveats (physical-reclamation reachability, restore held).
    pub notes: Vec<String>,
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
    /// The recipe-identity triple recorded for this execution, when
    /// present. See [`RecipeIdentityView`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recipe_identity: Option<RecipeIdentityView>,
}

/// JSON output for `rocky cost <run_id|latest>`.
///
/// Historical per-run cost attribution read from the embedded state
/// store's [`rocky_core::state::RunRecord`]. Re-derives per-model cost
/// via [`rocky_core::cost::compute_observed_cost_usd`] — the same
/// formula [`RunOutput::populate_cost_summary`] applies at the end of
/// a live run. The per-model and per-run totals make the "what did my
/// last run cost?" question answerable from the recorded run alone,
/// without re-materialising tables.
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
    /// Grouped cost rollup, present only when `--by <dimension>` is
    /// passed. `--by model` produces one group per model; `--by tenant`
    /// produces one group per tenant (models with no recorded tenant
    /// land in an `"<unattributed>"` bucket). `None` (and omitted from
    /// JSON) for a plain `rocky cost` invocation, so the default output
    /// shape is unchanged. `per_model` is always present regardless of
    /// grouping.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub groups: Option<Vec<CostGroup>>,
}

/// One grouped row in [`CostOutput::groups`], emitted when `rocky cost`
/// is run with `--by <dimension>`.
///
/// Each group sums the per-model figures of the executions that share the
/// grouping key (a tenant, or a model name). The cost roll-up uses the
/// same `compute_observed_cost_usd` figures as [`PerModelCostHistorical`],
/// so a `--by tenant` total equals the sum of its members' `cost_usd`.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct CostGroup {
    /// Dimension the grouping was performed on: `"tenant"` or `"model"`.
    pub dimension: String,
    /// The grouping key's value — the tenant name, the model name, or
    /// the literal `"<unattributed>"` for the `--by tenant` bucket that
    /// collects executions with no recorded tenant.
    pub key: String,
    /// Number of model executions rolled into this group.
    pub model_count: u64,
    /// Sum of every member's `cost_usd` that produced a number. `None`
    /// when no member produced a cost.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_cost_usd: Option<f64>,
    /// Wall-clock time summed across the group's member executions.
    pub total_duration_ms: u64,
    /// Sum of the group's member `bytes_scanned`. `None` when no member
    /// reported bytes scanned.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_bytes_scanned: Option<u64>,
    /// Sum of the group's member `bytes_written`. `None` when no member
    /// reported bytes written.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_bytes_written: Option<u64>,
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
    /// Tenant this execution was attributed to, read back from the
    /// persisted `rocky_core::state::ModelExecution::tenant`. Present
    /// only for replication executions whose source schema declared a
    /// `{tenant}` component; `None` (and omitted from JSON) otherwise.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
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

/// JSON output for `rocky preview rows`.
///
/// A sample of result rows for a single transformation model (or one of its
/// CTEs), executed against the pipeline's configured adapter. Classified
/// columns are masked inline before execution, so the rows match what the
/// materialized target would expose. `truncated` is `true` when the model
/// produced at least `limit_applied` rows.
#[derive(Debug, Serialize, JsonSchema)]
pub struct PreviewRowsOutput {
    pub version: String,
    pub command: String,
    /// Model whose output was sampled.
    pub model: String,
    /// CTE within the model that was isolated, if `--cte` was given.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cte: Option<String>,
    /// Output column names, in order.
    pub columns: Vec<String>,
    /// Sampled rows; each row is a list of JSON-encoded cell values aligned
    /// to `columns`.
    pub rows: Vec<Vec<serde_json::Value>>,
    /// Number of rows returned (≤ `limit_applied`).
    pub row_count: usize,
    /// The `LIMIT` applied to the preview query.
    pub limit_applied: u32,
    /// `true` when `row_count` reached `limit_applied` — more rows may exist.
    pub truncated: bool,
    /// The exact SQL executed (carries no credentials). Useful for debugging
    /// the masking projection and CTE isolation.
    pub executed_sql: String,
    /// Adapter the preview ran against (e.g. `"duckdb"`, `"databricks"`).
    pub adapter_kind: String,
    pub duration_ms: u64,
}

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
/// `copy_strategy` reports `"ctas"` for every successful copy regardless
/// of which SQL primitive the adapter emitted under the hood. Databricks
/// uses `SHALLOW CLONE` and BigQuery uses `CREATE TABLE ... COPY` (both
/// metadata-only); DuckDB and Snowflake fall through to the portable
/// CTAS default. Surfacing the per-adapter strategy in the wire output
/// is a follow-up.
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
/// the existing `rocky_core::ci_diff` machinery with a row-level diff
/// produced by either the sampled or the checksum-bisection algorithm.
/// Each per-model entry's `algorithm` field carries a `kind`
/// discriminator (`"sampled"` or `"bisection"`) plus the matching
/// payload.
///
/// **Sampled correctness ceiling.** The sampled algorithm reads
/// `LIMIT N` rows ordered by primary key (or first column). Changes
/// outside that window appear as no-change unless the row count itself
/// differs; `sampling_window.coverage_warning = true` flags that risk.
/// **Bisection** walks the chunk lattice exhaustively over a
/// single-column primary key; `bisection_stats.depth_capped = true`
/// flags the rare case where the recursion bottomed out at the depth
/// cap before reaching leaf size. `summary.any_coverage_warning` rolls
/// both signals up.
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
    /// `true` if **any** per-model diff is either a sampled diff with
    /// `sampling_window.coverage_warning = true` or a bisection diff
    /// with `bisection_stats.depth_capped = true`. Both conditions
    /// indicate the row-level findings might be incomplete and a
    /// reviewer shouldn't infer "no change" from a clean result.
    pub any_coverage_warning: bool,
}

/// Per-model diff. Combines a structural (column-level) delta with a
/// row-level diff that was produced by either the sampled or the
/// checksum-bisection algorithm. The active algorithm is encoded by the
/// `kind` discriminator on [`PreviewModelDiffAlgorithm`].
#[derive(Debug, Serialize, JsonSchema)]
pub struct PreviewModelDiff {
    pub model_name: String,
    pub structural: PreviewStructuralDiff,
    /// Row-level diff payload, tagged by which algorithm produced it.
    /// Different models in the same run can carry different variants:
    /// a model with a single-column integer primary key runs through
    /// the bisection kernel, while a model that lacks a usable PK
    /// falls back to the sampled algorithm.
    pub algorithm: PreviewModelDiffAlgorithm,
}

/// Row-level diff payload tagged by which algorithm produced it. The
/// `kind` discriminator (`"sampled"` or `"bisection"`) lets consumers
/// reading the JSON pick the right shape without having to check
/// optional fields.
#[derive(Debug, Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PreviewModelDiffAlgorithm {
    /// Sampled diff — `LIMIT N` rows ordered by primary key (or first
    /// column). Carries a coverage warning when changes outside the
    /// sampling window are not surfaced.
    Sampled {
        sampled: PreviewSampledRowDiff,
        sampling_window: PreviewSamplingWindow,
    },
    /// Checksum-bisection exhaustive diff. Walks the chunk lattice and
    /// materializes leaves for row-by-row compare. Carries
    /// [`BisectionStatsOutput`] so consumers can audit the recursion
    /// trace.
    Bisection {
        diff: PreviewBisectionRowDiff,
        bisection_stats: BisectionStatsOutput,
    },
}

/// Row-level diff produced by the checksum-bisection algorithm. All
/// counts are exhaustive over the diffed table — no sampling window.
#[derive(Debug, Serialize, JsonSchema)]
pub struct PreviewBisectionRowDiff {
    pub rows_added: u64,
    pub rows_removed: u64,
    pub rows_changed: u64,
    /// Up to `--max-samples` (default 5) representative changed rows
    /// surfaced from the leaves. Bisection samples only carry the
    /// primary key — column-level diffs are not retained on the
    /// kernel's leaf record. Empty when no rows differ.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub samples: Vec<PreviewRowSample>,
}

/// CLI-side mirror of [`rocky_core::compare::bisection::BisectionStats`].
/// Kept separate so the JSON schema lives alongside the rest of the
/// `rocky preview diff` output types — same pattern as
/// [`BudgetBreachOutput`] mirroring `rocky_core::config::BudgetBreach`.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct BisectionStatsOutput {
    /// Total non-empty chunk-checksum entries returned across both
    /// sides and all recursion levels.
    pub chunks_examined: u64,
    /// Number of leaf chunks materialized for row-by-row diff.
    pub leaves_materialized: u64,
    /// Maximum recursion depth reached.
    pub depth_max: u32,
    /// `true` if the runner hit the `max_depth` cap before any chunk
    /// fell below the leaf threshold. The dense range was still
    /// materialized exhaustively, so the diff is correct — just slower.
    pub depth_capped: bool,
    /// How the runner split the primary-key space into chunks. One of
    /// `"int_range"`, `"composite"`, `"hash_bucket"`, `"first_column"`.
    pub split_strategy: String,
    /// Rows on the base side whose primary-key column was NULL.
    /// Excluded from chunk membership; counted at the root so a
    /// null-PK divergence surfaces instead of silently dropping rows.
    pub null_pk_rows_base: u64,
    /// Rows on the branch side whose primary-key column was NULL.
    pub null_pk_rows_branch: u64,
}

impl BisectionStatsOutput {
    /// Mirror a [`rocky_core::compare::bisection::BisectionStats`] into
    /// the CLI-side output shape. The `split_strategy` enum is
    /// flattened to its serde wire tag so the JSON schema doesn't have
    /// to drag the rocky-core enum through `JsonSchema`.
    pub fn from_kernel(stats: &rocky_core::compare::bisection::BisectionStats) -> Self {
        let split_strategy = match stats.split_strategy {
            rocky_core::traits::SplitStrategy::IntRange => "int_range",
            rocky_core::traits::SplitStrategy::Composite => "composite",
            rocky_core::traits::SplitStrategy::HashBucket => "hash_bucket",
            rocky_core::traits::SplitStrategy::FirstColumn => "first_column",
        }
        .to_string();
        BisectionStatsOutput {
            chunks_examined: stats.chunks_examined,
            leaves_materialized: stats.leaves_materialized,
            depth_max: stats.depth_max,
            depth_capped: stats.depth_capped,
            split_strategy,
            null_pk_rows_base: stats.null_pk_rows_base,
            null_pk_rows_branch: stats.null_pk_rows_branch,
        }
    }
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
    /// Sampling-strategy tag. `"first_n_by_order"` is the standard
    /// PK-ordered window; `"not_yet_sampled"` is a placeholder used
    /// when the sampling layer didn't run (e.g. the structural-only
    /// fallback path).
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
    /// Budget breaches projected against the branch totals — populated
    /// only when the project declares a `[budget]` block. Lets a PR
    /// reviewer (and a CI gate) see "this PR would breach the run-level
    /// `max_usd` / `max_duration_ms` / `max_bytes_scanned` if merged"
    /// before the merge actually happens. Empty when no budget is
    /// configured or the projected totals stay within every limit.
    /// Mirrors the `RunOutput.budget_breaches` shape so the same
    /// downstream consumers (PR-comment templates, JSON listeners) can
    /// process both with one code path.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub projected_budget_breaches: Vec<BudgetBreachOutput>,
    /// Budget breaches projected against per-model branch totals —
    /// populated only when at least one model's resolved budget (its
    /// sidecar `[budget]` composed against the project-level config) is
    /// breached. Each entry carries `model_name` plus the same
    /// `limit_type` / `limit` / `actual` triple as
    /// [`Self::projected_budget_breaches`], with the resolved
    /// `on_breach` so downstream consumers can render
    /// advisory-vs-blocking per row. Empty when no per-model breach is
    /// projected. Strictly additive — kept on a separate field so
    /// existing consumers of `projected_budget_breaches` (which
    /// continues to surface only project-level breaches) are
    /// unaffected.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub projected_per_model_budget_breaches: Vec<PerModelBudgetBreachOutput>,
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
    /// Sum of every per-model `branch_duration_ms`. Used to project the
    /// `[budget]` `max_duration_ms` limit at preview time.
    pub total_branch_duration_ms: u64,
    /// Sum of every per-model `branch_bytes_scanned` that produced a
    /// number. `None` when no branch model reported a byte count
    /// (mirrors `RunOutput.cost.total_bytes_scanned` semantics — the
    /// non-BigQuery adapters today still inherit the default stub on
    /// `WarehouseAdapter::execute_statement_with_stats`). Used to
    /// project the `[budget]` `max_bytes_scanned` limit at preview time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_branch_bytes_scanned: Option<u64>,
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
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        branch_name: String,
        base_run_id: Option<String>,
        branch_run_id: String,
        summary: PreviewCostSummary,
        per_model: Vec<PreviewModelCostDelta>,
        projected_budget_breaches: Vec<BudgetBreachOutput>,
        projected_per_model_budget_breaches: Vec<PerModelBudgetBreachOutput>,
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
            projected_budget_breaches,
            projected_per_model_budget_breaches,
            markdown,
        }
    }
}

/// Structured error body returned by the `rocky serve` HTTP API.
///
/// Every non-2xx `/api/v1` response carries this envelope: the HTTP status
/// line carries the error *class* (`400`/`401`/`404`/`409`/`500`/`503`) and
/// the body carries a stable machine token plus a human message and an
/// optional actionable hint. Embedders switch on [`code`](Self::code) and
/// surface [`message`](Self::message) / [`remediation_hint`](Self::remediation_hint)
/// to operators.
///
/// Stable codes emitted today: `engine_not_ready` (no compile available
/// yet), `engine_busy` (state locked by a running job — retryable),
/// `model_not_found`, `job_not_found`, `mutation_in_progress` (a `run`/`apply`
/// job already holds the mutation permit — carries [`running_job_id`](Self::running_job_id)),
/// `bad_request`, `unauthorized`, `internal_error`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ErrorEnvelope {
    /// Stable machine token, e.g. `"model_not_found"`.
    pub code: String,
    /// Human-readable description of what went wrong.
    pub message: String,
    /// Actionable next step, or `null` when none applies.
    pub remediation_hint: Option<String>,
    /// The `job_id` of the run/apply job currently holding the mutation permit,
    /// on a `409 mutation_in_progress` (and, when known, on a
    /// `503 engine_busy`). Omitted for every other error. Additive,
    /// serde-defaulted — embedders that predate it simply never see it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub running_job_id: Option<String>,
}

/// Feature-detection payload for `GET /api/v1/meta`.
///
/// Fingerprints the running engine + bound config so an embedder can pin
/// against a build without version-sniffing. Every field is computed at
/// request time — none are baked literals — so `state_schema_version`,
/// `schemas_hash`, and `config_hash` track the live engine and the on-disk
/// config even across a long-running sidecar.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct MetaOutput {
    /// Engine release (`CARGO_PKG_VERSION`).
    pub engine_version: String,
    /// Current state-store schema version, read from the engine's
    /// `current_schema_version()` getter at request time (never a literal).
    pub state_schema_version: u32,
    /// Fingerprint of the exported JSON-schema set (the `schemas/*.schema.json`
    /// contract). Derived from the live registered schemas, so it moves on
    /// every codegen; embedders detect an output-shape change without
    /// version-sniffing.
    pub schemas_hash: String,
    /// Fingerprint of the resolved `rocky.toml` (contents + path) this
    /// sidecar bound, recomputed per request. `null` when no config resolved.
    pub config_hash: Option<String>,
    /// Feature/capability tokens the build advertises.
    pub capabilities: Vec<String>,
    /// The `/api/v1` routes this build serves.
    pub routes: Vec<String>,
}

/// The kind of long-running operation a job wraps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum JobKind {
    /// `rocky run` — materializes tables. Takes the mutation permit.
    Run,
    /// `rocky plan` — previews and persists a plan. Non-mutating; no permit.
    Plan,
    /// `rocky apply` — executes a persisted plan. Takes the mutation permit.
    Apply,
}

impl JobKind {
    /// The `rocky` subcommand this kind spawns.
    pub fn verb(self) -> &'static str {
        match self {
            JobKind::Run => "run",
            JobKind::Plan => "plan",
            JobKind::Apply => "apply",
        }
    }

    /// Whether this kind mutates warehouse state (and so takes the permit).
    /// `plan` previews only.
    pub fn mutates(self) -> bool {
        matches!(self, JobKind::Run | JobKind::Apply)
    }

    /// Parse the persisted string form; unknown values map to `None`.
    pub fn parse(s: &str) -> Option<JobKind> {
        match s {
            "run" => Some(JobKind::Run),
            "plan" => Some(JobKind::Plan),
            "apply" => Some(JobKind::Apply),
            _ => None,
        }
    }
}

/// Lifecycle state of a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum JobState {
    /// Accepted, not yet started.
    Queued,
    /// The underlying `rocky <kind>` subprocess is executing.
    Running,
    /// The subprocess exited 0.
    Succeeded,
    /// The subprocess exited non-zero, or could not be launched.
    Failed,
}

impl JobState {
    /// Parse the persisted string form; unknown values map to `None`.
    pub fn parse(s: &str) -> Option<JobState> {
        match s {
            "queued" => Some(JobState::Queued),
            "running" => Some(JobState::Running),
            "succeeded" => Some(JobState::Succeeded),
            "failed" => Some(JobState::Failed),
            _ => None,
        }
    }
}

/// Status of a `rocky serve` long-running job (`GET /api/v1/jobs/{id}`).
///
/// The presentation type over `rocky-core`'s `PersistedJob`: it carries the
/// same lifecycle facts with typed [`JobKind`]/[`JobState`] enums for embedder
/// ergonomics, and — once the job is terminal — the canonical
/// `RunOutput` / `PlanOutput` / `ApplyOutput` the underlying subprocess emitted,
/// embedded **verbatim** in [`result`](Self::result). An embedder switches on
/// [`kind`](Self::kind) to know which schema `result` conforms to (the same
/// `run` / `plan` / `apply` schemas the CLI exports).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct JobStatus {
    /// Opaque job identifier (as returned by `POST /api/v1/jobs/{kind}`).
    pub job_id: String,
    /// Which operation the job wraps.
    pub kind: JobKind,
    /// Current lifecycle state.
    pub state: JobState,
    /// When the job was submitted (RFC 3339).
    pub submitted_at: String,
    /// When execution started (RFC 3339), or `null` if not yet.
    pub started_at: Option<String>,
    /// When the job reached a terminal state (RFC 3339), or `null` if not yet.
    pub finished_at: Option<String>,
    /// The advisory, spoofable `X-Rocky-Principal` recorded for audit, or `null`.
    /// Never an authorization input under the single-shared-secret auth ceiling.
    pub principal: Option<String>,
    /// Failure detail when [`state`](Self::state) is [`JobState::Failed`], else `null`.
    pub error: Option<String>,
    /// The canonical output of the underlying `rocky <kind>` command, embedded
    /// verbatim once the job is terminal (`null` while queued/running). Its
    /// shape is the `run` / `plan` / `apply` schema selected by
    /// [`kind`](Self::kind).
    pub result: Option<serde_json::Value>,
}

#[cfg(test)]
mod failure_kind_tests {
    //! Mapping coverage for [`FailureKind`] against every variant of
    //! Databricks and Snowflake [`ConnectorError`]. The `Http(reqwest::Error)`
    //! variant is not directly constructed here (reqwest exposes no public
    //! constructor) — its mapping is exercised end-to-end inside the
    //! `classify_anyhow_error` path.
    use super::*;
    use rocky_databricks::connector::ConnectorError as DbE;
    use rocky_snowflake::connector::ConnectorError as SnE;

    fn db_api(status: u16) -> DbE {
        DbE::ApiError {
            status,
            body: String::new(),
        }
    }

    fn sn_api(status: u16) -> SnE {
        SnE::ApiError {
            status,
            body: String::new(),
        }
    }

    // ---- Databricks ConnectorError → FailureKind --------------------------

    #[test]
    fn databricks_auth_maps_to_auth_failed() {
        let e = DbE::Auth(rocky_databricks::auth::AuthError::NoAuthConfigured);
        assert_eq!(FailureKind::from(&e), FailureKind::AuthFailed);
    }

    #[test]
    fn databricks_statement_failed_maps_to_query_rejected() {
        let e = DbE::StatementFailed {
            id: "stmt-1".into(),
            message: "syntax error".into(),
        };
        assert_eq!(FailureKind::from(&e), FailureKind::QueryRejected);
    }

    #[test]
    fn databricks_timeout_maps_to_transient() {
        let e = DbE::Timeout {
            id: "stmt-1".into(),
            seconds: 30,
        };
        assert_eq!(FailureKind::from(&e), FailureKind::Transient);
    }

    #[test]
    fn databricks_canceled_maps_to_unknown() {
        let e = DbE::Canceled {
            id: "stmt-1".into(),
        };
        assert_eq!(FailureKind::from(&e), FailureKind::Unknown);
    }

    #[test]
    fn databricks_unexpected_state_maps_to_unknown() {
        let e = DbE::UnexpectedState {
            id: "stmt-1".into(),
            state: "FOO".into(),
        };
        assert_eq!(FailureKind::from(&e), FailureKind::Unknown);
    }

    #[test]
    fn databricks_api_error_401_403_maps_to_auth_failed() {
        assert_eq!(FailureKind::from(&db_api(401)), FailureKind::AuthFailed);
        assert_eq!(FailureKind::from(&db_api(403)), FailureKind::AuthFailed);
    }

    #[test]
    fn databricks_api_error_404_maps_to_not_found() {
        assert_eq!(FailureKind::from(&db_api(404)), FailureKind::NotFound);
    }

    #[test]
    fn databricks_api_error_429_maps_to_quota_exceeded() {
        assert_eq!(FailureKind::from(&db_api(429)), FailureKind::QuotaExceeded);
    }

    #[test]
    fn databricks_api_error_5xx_maps_to_transient() {
        for status in [500u16, 502, 503, 504, 599] {
            assert_eq!(
                FailureKind::from(&db_api(status)),
                FailureKind::Transient,
                "status {status}",
            );
        }
    }

    #[test]
    fn databricks_api_error_other_maps_to_query_rejected() {
        assert_eq!(FailureKind::from(&db_api(400)), FailureKind::QueryRejected);
        assert_eq!(FailureKind::from(&db_api(409)), FailureKind::QueryRejected);
    }

    #[test]
    fn databricks_circuit_breaker_maps_to_quota_exceeded() {
        // CircuitBreakerOpen is a budget-cap signal (sustained transient
        // pressure exhausted the breaker) — see the doc-comment on
        // `FailureKind::QuotaExceeded` and the dagster-side handler
        // keyed on `failure_kind == "quota-exceeded"`.
        let e = DbE::CircuitBreakerOpen {
            consecutive_failures: 5,
            cooldown_seconds: None,
        };
        assert_eq!(FailureKind::from(&e), FailureKind::QuotaExceeded);
        let e = DbE::CircuitBreakerOpen {
            consecutive_failures: 5,
            cooldown_seconds: Some(120),
        };
        assert_eq!(FailureKind::from(&e), FailureKind::QuotaExceeded);
        assert_eq!(cooldown_from_databricks(&e), Some(120));
    }

    #[test]
    fn databricks_retry_budget_exhausted_maps_to_quota_exceeded() {
        let e = DbE::RetryBudgetExhausted { limit: 10 };
        assert_eq!(FailureKind::from(&e), FailureKind::QuotaExceeded);
    }

    // ---- Snowflake ConnectorError → FailureKind ---------------------------

    #[test]
    fn snowflake_auth_maps_to_auth_failed() {
        let e = SnE::Auth(rocky_snowflake::auth::AuthError::NoAuthConfigured);
        assert_eq!(FailureKind::from(&e), FailureKind::AuthFailed);
    }

    #[test]
    fn snowflake_statement_failed_maps_to_query_rejected() {
        let e = SnE::StatementFailed {
            handle: "h-1".into(),
            message: "compile error".into(),
        };
        assert_eq!(FailureKind::from(&e), FailureKind::QueryRejected);
    }

    #[test]
    fn snowflake_timeout_maps_to_transient() {
        let e = SnE::Timeout {
            handle: "h-1".into(),
            seconds: 60,
        };
        assert_eq!(FailureKind::from(&e), FailureKind::Transient);
    }

    #[test]
    fn snowflake_api_error_401_403_maps_to_auth_failed() {
        assert_eq!(FailureKind::from(&sn_api(401)), FailureKind::AuthFailed);
        assert_eq!(FailureKind::from(&sn_api(403)), FailureKind::AuthFailed);
    }

    #[test]
    fn snowflake_api_error_404_maps_to_not_found() {
        assert_eq!(FailureKind::from(&sn_api(404)), FailureKind::NotFound);
    }

    #[test]
    fn snowflake_api_error_429_maps_to_quota_exceeded() {
        assert_eq!(FailureKind::from(&sn_api(429)), FailureKind::QuotaExceeded);
    }

    #[test]
    fn snowflake_api_error_5xx_maps_to_transient() {
        for status in [500u16, 502, 503, 504, 599] {
            assert_eq!(
                FailureKind::from(&sn_api(status)),
                FailureKind::Transient,
                "status {status}",
            );
        }
    }

    #[test]
    fn snowflake_api_error_other_maps_to_query_rejected() {
        assert_eq!(FailureKind::from(&sn_api(400)), FailureKind::QueryRejected);
        assert_eq!(FailureKind::from(&sn_api(422)), FailureKind::QueryRejected);
    }

    #[test]
    fn snowflake_circuit_breaker_maps_to_quota_exceeded() {
        // See the databricks counterpart above — breaker trips are a
        // budget-cap signal, not a one-off transient blip.
        let e = SnE::CircuitBreakerOpen {
            consecutive_failures: 5,
            cooldown_seconds: None,
        };
        assert_eq!(FailureKind::from(&e), FailureKind::QuotaExceeded);
        let e = SnE::CircuitBreakerOpen {
            consecutive_failures: 5,
            cooldown_seconds: Some(60),
        };
        assert_eq!(FailureKind::from(&e), FailureKind::QuotaExceeded);
        assert_eq!(cooldown_from_snowflake(&e), Some(60));
    }

    #[test]
    fn snowflake_retry_budget_exhausted_maps_to_quota_exceeded() {
        let e = SnE::RetryBudgetExhausted { limit: 10 };
        assert_eq!(FailureKind::from(&e), FailureKind::QuotaExceeded);
    }

    // ---- classify_anyhow_error chain walk --------------------------------

    #[test]
    fn classify_anyhow_walks_chain_for_databricks_connector_error() {
        let err = anyhow::Error::new(DbE::StatementFailed {
            id: "s".into(),
            message: "boom".into(),
        })
        .context("running materialization");
        assert_eq!(classify_anyhow_error(&err), FailureKind::QueryRejected);
    }

    #[test]
    fn classify_anyhow_walks_chain_for_snowflake_connector_error() {
        let err = anyhow::Error::new(SnE::Timeout {
            handle: "h".into(),
            seconds: 30,
        })
        .context("running materialization");
        assert_eq!(classify_anyhow_error(&err), FailureKind::Transient);
    }

    #[test]
    fn classify_anyhow_returns_unknown_when_no_connector_error_in_chain() {
        let err = anyhow::anyhow!("some non-connector failure");
        assert_eq!(classify_anyhow_error(&err), FailureKind::Unknown);
    }

    // ---- classify_anyhow_error_with_cooldown chain walk ------------------

    #[test]
    fn classify_anyhow_with_cooldown_extracts_databricks_breaker_cooldown() {
        let conn_err = DbE::CircuitBreakerOpen {
            consecutive_failures: 5,
            cooldown_seconds: Some(180),
        };
        let err = anyhow::Error::new(conn_err).context("running materialization");
        assert_eq!(
            classify_anyhow_error_with_cooldown(&err),
            (FailureKind::QuotaExceeded, Some(180)),
        );
    }

    #[test]
    fn classify_anyhow_with_cooldown_extracts_snowflake_breaker_cooldown() {
        let conn_err = SnE::CircuitBreakerOpen {
            consecutive_failures: 5,
            cooldown_seconds: Some(60),
        };
        let err = anyhow::Error::new(conn_err).context("running materialization");
        assert_eq!(
            classify_anyhow_error_with_cooldown(&err),
            (FailureKind::QuotaExceeded, Some(60)),
        );
    }

    #[test]
    fn classify_anyhow_with_cooldown_returns_none_when_breaker_has_no_recovery_timeout() {
        // Manual-reset-only breakers (no `circuit_breaker_recovery_timeout_secs`
        // configured) trip with `cooldown_seconds: None` — kind still flips
        // to `QuotaExceeded` but the hint is absent.
        let conn_err = DbE::CircuitBreakerOpen {
            consecutive_failures: 5,
            cooldown_seconds: None,
        };
        let err = anyhow::Error::new(conn_err);
        assert_eq!(
            classify_anyhow_error_with_cooldown(&err),
            (FailureKind::QuotaExceeded, None),
        );
    }

    #[test]
    fn classify_anyhow_with_cooldown_returns_none_for_non_breaker_failure() {
        let conn_err = DbE::Timeout {
            id: "s".into(),
            seconds: 60,
        };
        let err = anyhow::Error::new(conn_err);
        assert_eq!(
            classify_anyhow_error_with_cooldown(&err),
            (FailureKind::Transient, None),
        );
    }

    #[test]
    fn classify_anyhow_with_cooldown_unwraps_adapter_error_wrapping_breaker() {
        // Mirrors the production path: ConnectorError is wrapped in
        // AdapterError (boxed) before crossing into anyhow — the
        // cooldown walker must descend through the wrapper just like
        // its FailureKind-only counterpart.
        let conn_err = DbE::CircuitBreakerOpen {
            consecutive_failures: 5,
            cooldown_seconds: Some(300),
        };
        let adapter_err = rocky_adapter_sdk::AdapterError::new(conn_err);
        let err = anyhow::Error::new(adapter_err);
        assert_eq!(
            classify_anyhow_error_with_cooldown(&err),
            (FailureKind::QuotaExceeded, Some(300)),
        );
    }

    #[test]
    fn classify_anyhow_unwraps_adapter_error_wrapping_databricks_connector() {
        // Mirrors the production path: ConnectorError is wrapped in
        // AdapterError (boxed) before crossing into anyhow.
        let conn_err = DbE::ApiError {
            status: 429,
            body: String::new(),
        };
        let adapter_err = rocky_adapter_sdk::AdapterError::new(conn_err);
        let err = anyhow::Error::new(adapter_err).context("execute_statement failed");
        assert_eq!(classify_anyhow_error(&err), FailureKind::QuotaExceeded);
    }

    #[test]
    fn classify_anyhow_unwraps_adapter_error_wrapping_snowflake_connector() {
        let conn_err = SnE::Timeout {
            handle: "h".into(),
            seconds: 30,
        };
        let adapter_err = rocky_adapter_sdk::AdapterError::new(conn_err);
        let err = anyhow::Error::new(adapter_err).context("execute_statement failed");
        assert_eq!(classify_anyhow_error(&err), FailureKind::Transient);
    }

    // ---- Serialization contract ------------------------------------------

    #[test]
    fn failure_kind_serializes_to_kebab_case() {
        let cases = [
            (FailureKind::ConnectionFailed, "\"connection-failed\""),
            (FailureKind::AuthFailed, "\"auth-failed\""),
            (FailureKind::QueryRejected, "\"query-rejected\""),
            (FailureKind::Transient, "\"transient\""),
            (FailureKind::QuotaExceeded, "\"quota-exceeded\""),
            (FailureKind::NotFound, "\"not-found\""),
            (FailureKind::Unknown, "\"unknown\""),
        ];
        for (kind, expected) in cases {
            assert_eq!(
                serde_json::to_string(&kind).expect("serialize"),
                expected,
                "{kind:?}"
            );
        }
    }

    #[test]
    fn failure_kind_default_is_unknown() {
        assert_eq!(FailureKind::default(), FailureKind::Unknown);
    }

    // ---- run.rs-style conversion preserves typed variant ----------------
    //
    // These pin the conversion idiom now used in `commands/run.rs`. They
    // ensure that the way `run.rs` materializes its `anyhow::Error`s
    // (`.map_err(anyhow::Error::from)?` for bare wraps, and
    // `anyhow::Error::from(e).context(...)` for prefixed wraps) keeps the
    // typed `AdapterError` in the source chain so `classify_anyhow_error`
    // can downcast to it and map to the correct `FailureKind`. The old
    // `anyhow::anyhow!("{e}")` pattern stringified the typed error before
    // the wrap and made the classifier fall through to `Unknown`.
    //
    // Three connector variants are pinned to prove the wiring isn't
    // variant-specific.
    fn db_adapter_err(conn: DbE) -> rocky_adapter_sdk::AdapterError {
        rocky_adapter_sdk::AdapterError::new(conn)
    }

    fn sn_adapter_err(conn: SnE) -> rocky_adapter_sdk::AdapterError {
        rocky_adapter_sdk::AdapterError::new(conn)
    }

    #[test]
    fn run_path_bare_wrap_preserves_databricks_statement_failed() {
        // Mirrors `.map_err(anyhow::Error::from)?` on an
        // `AdapterResult<()>` returned from `WarehouseAdapter::execute_statement`.
        let result: rocky_adapter_sdk::AdapterResult<()> =
            Err(db_adapter_err(DbE::StatementFailed {
                id: "stmt-1".into(),
                message: "syntax error".into(),
            }));
        let err: anyhow::Error = result.map_err(anyhow::Error::from).unwrap_err();
        assert_eq!(classify_anyhow_error(&err), FailureKind::QueryRejected);
    }

    #[test]
    fn run_path_bare_wrap_preserves_snowflake_timeout() {
        let result: rocky_adapter_sdk::AdapterResult<()> = Err(sn_adapter_err(SnE::Timeout {
            handle: "h-1".into(),
            seconds: 60,
        }));
        let err: anyhow::Error = result.map_err(anyhow::Error::from).unwrap_err();
        assert_eq!(classify_anyhow_error(&err), FailureKind::Transient);
    }

    #[test]
    fn run_path_bare_wrap_preserves_databricks_quota_exceeded() {
        let result: rocky_adapter_sdk::AdapterResult<()> = Err(db_adapter_err(DbE::ApiError {
            status: 429,
            body: String::new(),
        }));
        let err: anyhow::Error = result.map_err(anyhow::Error::from).unwrap_err();
        assert_eq!(classify_anyhow_error(&err), FailureKind::QuotaExceeded);
    }

    #[test]
    fn run_path_context_wrap_preserves_databricks_statement_failed() {
        // Mirrors `anyhow::Error::from(e).context(format!("..."))` used at
        // the run.rs bootstrap / model-failed / partition record sites.
        let adapter_err = db_adapter_err(DbE::StatementFailed {
            id: "stmt-1".into(),
            message: "compile error".into(),
        });
        let err = anyhow::Error::from(adapter_err).context(format!(
            "bootstrap of '{}' for model '{}' failed",
            "tgt", "m"
        ));
        assert_eq!(classify_anyhow_error(&err), FailureKind::QueryRejected);
    }

    #[test]
    fn run_path_context_wrap_preserves_snowflake_auth_failed() {
        let adapter_err = sn_adapter_err(SnE::ApiError {
            status: 401,
            body: String::new(),
        });
        let err = anyhow::Error::from(adapter_err).context("model 'orders' failed");
        assert_eq!(classify_anyhow_error(&err), FailureKind::AuthFailed);
    }

    #[test]
    fn run_path_unconverted_anyhow_macro_still_returns_unknown() {
        // Negative control: the old lossy pattern still drops the typed
        // variant. If this ever starts returning the typed kind, the
        // classifier behaviour shifted and the rest of these pins should
        // be reviewed.
        let adapter_err = db_adapter_err(DbE::StatementFailed {
            id: "stmt-1".into(),
            message: "syntax".into(),
        });
        let err = anyhow::anyhow!("{adapter_err}");
        assert_eq!(classify_anyhow_error(&err), FailureKind::Unknown);
    }

    // ---- EF8: user-facing framing of 403 / 401 ----------------------------

    #[test]
    fn frame_403_and_401_produce_distinct_actionable_messages() {
        // anyhow path (run.rs table errors): wrap the typed ConnectorError
        // through the SDK AdapterError, as the real run path does.
        let e403: anyhow::Error = db_adapter_err(db_api(403)).into();
        let e401: anyhow::Error = db_adapter_err(db_api(401)).into();

        let m403 = frame_warehouse_anyhow_error(&e403, "cat.sch.tbl").expect("403 must frame");
        let m401 = frame_warehouse_anyhow_error(&e401, "cat.sch.tbl").expect("401 must frame");

        // The whole point of keying on status (not FailureKind, which
        // collapses both to AuthFailed): the two messages differ.
        assert_ne!(m403, m401);
        assert!(m403.contains("permission denied"));
        assert!(m403.contains("cat.sch.tbl"));
        assert!(m401.contains("authentication rejected"));
    }

    #[test]
    fn frame_non_auth_status_falls_back_to_none() {
        // A 500 is not an auth failure — keep the raw error, don't frame.
        let e: anyhow::Error = db_adapter_err(db_api(500)).into();
        assert!(frame_warehouse_anyhow_error(&e, "t").is_none());
    }

    #[test]
    fn frame_adapter_error_path_for_doctor_ping() {
        // doctor pings return rocky_core::traits::AdapterError, whose inner
        // is reached via the new inner() accessor.
        let e = rocky_core::traits::AdapterError::new(db_api(403));
        let framed = frame_warehouse_adapter_error(&e, "warehouse_a").expect("403 frames");
        assert!(framed.contains("permission denied"));

        let e_sf = rocky_core::traits::AdapterError::new(sn_api(401));
        let framed_sf = frame_warehouse_adapter_error(&e_sf, "warehouse_b").expect("401 frames");
        assert!(framed_sf.contains("authentication rejected"));
    }
}
