"""Pydantic models matching Rocky's JSON output schema (v0.3.0+).

Every Rocky CLI command that supports ``--output json`` has a corresponding
result model in this module. ``parse_rocky_output`` auto-detects the command
type from the JSON payload and returns the matching model.
"""

from __future__ import annotations

import json
from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Discover output
# ---------------------------------------------------------------------------


class TableInfo(BaseModel):
    name: str
    row_count: int | None = None


class SourceInfo(BaseModel):
    """A discovered source from Rocky's discover output."""

    id: str
    components: dict[str, str | list[str]]
    source_type: str
    last_sync_at: datetime | None = None
    tables: list[TableInfo]


class FreshnessConfig(BaseModel):
    """Freshness check configuration projected from ``rocky.toml`` ``[checks.freshness]``.

    Per-schema overrides are intentionally not exposed yet — the Rocky-side
    output omits them until the override-key semantics are nailed down.
    """

    threshold_seconds: int


class ChecksConfig(BaseModel):
    """Pipeline-level checks configuration projected into the discover output.

    A thin projection of ``rocky_core::config::ChecksConfig`` exposing only
    the fields downstream orchestrators currently consume.
    """

    freshness: FreshnessConfig | None = None


class DiscoverResult(BaseModel):
    version: str
    command: str
    sources: list[SourceInfo]
    checks: ChecksConfig | None = None
    #: Tables filtered out of ``sources`` because they were reported by
    #: the discovery adapter but don't exist in the source warehouse.
    #: Same shape as :attr:`RunResult.excluded_tables`. Empty when nothing
    #: was filtered. ``from __future__ import annotations`` defers
    #: evaluation, so the forward reference to :class:`ExcludedTable`
    #: (defined further down with the run-output classes) resolves
    #: without explicit string quotes.
    excluded_tables: list[ExcludedTable] = []


# ---------------------------------------------------------------------------
# Run output
# ---------------------------------------------------------------------------


class MaterializationMetadata(BaseModel):
    strategy: str
    watermark: datetime | None = None
    #: Fully-qualified target table identifier in ``catalog.schema.table``
    #: format. Useful for click-through links to the warehouse UI.
    #: Always set when the materialization targets a known table.
    target_table_full_name: str | None = None
    #: Short hex fingerprint (16 hex chars from a stable stdlib hash)
    #: of the SQL statements the engine sent to the warehouse. Lets
    #: orchestrators detect "what changed?" between runs without
    #: diffing full SQL bodies. Currently populated for ``time_interval``
    #: materializations; the replication path leaves it ``None`` until
    #: the engine threads SQL through.
    sql_hash: str | None = None
    #: Number of columns in the materialized table's typed schema.
    #: Populated for derived models where the compiler resolved a
    #: typed schema; ``None`` for source-replication tables.
    column_count: int | None = None
    #: Compile time in milliseconds for the model that produced this
    #: materialization. Populated only for derived models.
    compile_time_ms: int | None = None


class PartitionInfo(BaseModel):
    """Partition window information for a single ``time_interval`` materialization.

    ``key`` is the canonical Rocky partition key (e.g. ``"2026-04-07"`` for
    daily, ``"2026-04-07T13"`` for hourly). ``start`` / ``end`` are the
    half-open ``[start, end)`` window the SQL substituted for ``@start_date``
    / ``@end_date``. ``batched_with`` lists any additional partition keys
    that were merged into this batch when ``batch_size > 1`` — empty for
    the default one-partition-per-statement case.

    Mirrors :class:`rocky_cli::output::PartitionInfo` on the engine side.
    """

    key: str
    start: datetime
    end: datetime
    batched_with: list[str] = []


class PartitionSummary(BaseModel):
    """Per-model summary of ``time_interval`` partition execution.

    One entry per partitioned model touched by the run. Lets dagster-rocky
    display per-model partition stats without re-counting the per-partition
    ``MaterializationInfo.partition`` entries.

    Mirrors :class:`rocky_cli::output::PartitionSummary` on the engine side.
    """

    model: str
    partitions_planned: int
    partitions_succeeded: int
    partitions_failed: int
    #: Partitions that were already ``Computed`` in the state store and
    #: skipped by the runtime (currently always 0; reserved for the
    #: ``--missing`` change-detection optimization).
    partitions_skipped: int = 0


class MaterializationInfo(BaseModel):
    asset_key: list[str]
    rows_copied: int | None = None
    duration_ms: int
    metadata: MaterializationMetadata
    #: Partition window this materialization targeted, present only when
    #: the model's strategy is ``time_interval``. ``None`` for unpartitioned
    #: strategies (``full_refresh``, ``incremental``, ``merge``).
    partition: PartitionInfo | None = None


class CheckResult(BaseModel):
    name: str
    passed: bool
    # Row count fields
    source_count: int | None = None
    target_count: int | None = None
    # Column match fields
    missing: list[str] | None = None
    extra: list[str] | None = None
    # Freshness fields
    lag_seconds: int | None = None
    threshold_seconds: int | None = None
    # Null rate fields
    column: str | None = None
    null_rate: float | None = None
    threshold: float | None = None
    # Custom check fields
    query: str | None = None
    result_value: int | None = None


class TableCheckResult(BaseModel):
    asset_key: list[str]
    checks: list[CheckResult]


class PermissionInfo(BaseModel):
    grants_added: int
    grants_revoked: int
    catalogs_created: int
    schemas_created: int


class DriftAction(BaseModel):
    table: str
    action: str
    reason: str


class DriftInfo(BaseModel):
    tables_checked: int
    tables_drifted: int
    actions_taken: list[DriftAction]


class ContractViolation(BaseModel):
    rule: str
    column: str
    message: str


class ContractResult(BaseModel):
    passed: bool
    violations: list[ContractViolation]


class AnomalyResult(BaseModel):
    table: str
    current_count: int
    baseline_avg: float
    deviation_pct: float
    reason: str


class TableError(BaseModel):
    asset_key: list[str]
    error: str


class ExcludedTable(BaseModel):
    """A table that the discovery adapter reported but that was missing
    from the source warehouse, so the run skipped it.

    This is a generic pre-flight exclusion category, not tied to any
    specific source system. The engine emits an ExcludedTable whenever
    a discovery adapter returns a table reference that does not exist
    in the upstream warehouse at run time — the row never makes it past
    the existence check, so it can't fail at runtime and doesn't belong
    in :class:`TableError`.

    One concrete example: Fivetran may have a table enabled in its
    connector config but not yet synced to the destination warehouse,
    or may have auto-prefixed a table with its ``do_not_alter__``
    broken-table marker; in both cases the discovery adapter reports
    the table but the warehouse has nothing at that address. Other
    adapters can produce the same shape for their own reasons (sync
    paused, permissions pending, schema rename in progress) — the
    ``reason`` field is deliberately free-form so new causes can be
    added without a schema break.
    """

    asset_key: list[str]
    source_schema: str
    table_name: str
    #: Free-form reason. Currently always ``"missing_from_source"`` but
    #: kept open so future causes (disabled, sync_paused, ...) can be
    #: added without a schema break.
    reason: str


class ExecutionSummary(BaseModel):
    """Summary of execution parallelism and throughput."""

    concurrency: int
    tables_processed: int
    tables_failed: int


class MetricsSnapshot(BaseModel):
    """Execution metrics from rocky-observe."""

    tables_processed: int
    tables_failed: int
    error_rate_pct: float
    statements_executed: int
    retries_attempted: int
    retries_succeeded: int
    anomalies_detected: int
    table_duration_p50_ms: int
    table_duration_p95_ms: int
    table_duration_max_ms: int
    query_duration_p50_ms: int
    query_duration_p95_ms: int
    query_duration_max_ms: int


class RunResult(BaseModel):
    version: str
    command: str
    filter: str
    duration_ms: int
    tables_copied: int
    tables_failed: int = 0
    materializations: list[MaterializationInfo]
    check_results: list[TableCheckResult]
    errors: list[TableError] = []
    #: Tables filtered out before execution because they don't exist in
    #: the source warehouse. Empty when nothing was excluded. The CLI
    #: skips serializing this field when empty, so it remains backwards
    #: compatible with older Rocky binaries that don't emit it at all.
    excluded_tables: list[ExcludedTable] = []
    execution: ExecutionSummary | None = None
    metrics: MetricsSnapshot | None = None
    permissions: PermissionInfo
    drift: DriftInfo
    contracts: ContractResult | None = None
    anomalies: list[AnomalyResult] = []
    #: Per-model partition execution summaries, populated only when the
    #: run touched one or more ``time_interval`` models. Empty for runs
    #: that didn't execute any partitioned models.
    partition_summaries: list[PartitionSummary] = []


# ---------------------------------------------------------------------------
# Plan output
# ---------------------------------------------------------------------------


class PlannedStatement(BaseModel):
    purpose: str
    target: str
    sql: str


class PlanResult(BaseModel):
    version: str
    command: str
    filter: str
    statements: list[PlannedStatement]


# ---------------------------------------------------------------------------
# State output
# ---------------------------------------------------------------------------


class WatermarkEntry(BaseModel):
    table: str
    last_value: datetime
    updated_at: datetime


class StateResult(BaseModel):
    version: str
    command: str
    watermarks: list[WatermarkEntry]


# ---------------------------------------------------------------------------
# Compile output (v0.1.0)
# ---------------------------------------------------------------------------


class Severity(StrEnum):
    """Compiler diagnostic severity."""

    error = "Error"
    warning = "Warning"
    info = "Info"


class SourceSpan(BaseModel):
    """Location in a source file."""

    file: str
    line: int
    col: int


class Diagnostic(BaseModel):
    """A compiler diagnostic (error, warning, or info)."""

    severity: Severity
    code: str
    message: str
    model: str
    span: SourceSpan | None = None
    suggestion: str | None = None


class ModelFreshnessConfig(BaseModel):
    """Per-model freshness configuration projected from model TOML frontmatter.

    Mirrors :class:`rocky_core::models::ModelFreshnessConfig` on the Rust
    side. Declarative-only — the compiler does not enforce anything;
    downstream consumers (``dagster-rocky`` for ``FreshnessPolicy``,
    ``rocky doctor --freshness``) read this field from the compile JSON
    output.
    """

    max_lag_seconds: int


class ModelDetail(BaseModel):
    """Per-model summary projected from ``rocky_core::models::ModelConfig``.

    Intentionally excludes fields that change run-to-run (timings,
    diagnostics) — those live on run-level outputs. This is the stable,
    declarative shape of one compiled model.

    ``strategy`` is left as a generic dict because the underlying
    StrategyConfig is a tagged union; structured access lives in the
    generated ``compile_schema.ModelDetail`` for callers that want it.
    """

    name: str
    strategy: dict[str, object]
    target: dict[str, str]
    freshness: ModelFreshnessConfig | None = None


class CompileResult(BaseModel):
    """Output of ``rocky compile --json``."""

    version: str
    command: str
    models: int
    execution_layers: int
    diagnostics: list[Diagnostic]
    has_errors: bool
    models_detail: list[ModelDetail] = []


# ---------------------------------------------------------------------------
# Lineage output (v0.1.0)
# ---------------------------------------------------------------------------


class TransformKind(StrEnum):
    """Common transform kinds emitted by ``rocky lineage``.

    The engine emits ``transform`` as a free-form string — see
    ``engine/crates/rocky-cli/src/output.rs::LineageEdgeRecord``. The known
    values include the simple variants below plus parameterised ones like
    ``aggregation("sum")``, ``window(...)``, etc. ``LineageEdge.transform``
    is therefore typed as ``str`` (not as ``TransformKind``); this enum is
    provided as a convenience for code that wants to compare against the
    common cases.
    """

    direct = "direct"
    cast = "cast"
    expression = "expression"


class QualifiedColumn(BaseModel):
    """A column fully qualified by its model name."""

    model: str
    column: str


class LineageEdge(BaseModel):
    """An edge in the semantic graph connecting columns across models."""

    source: QualifiedColumn
    target: QualifiedColumn
    #: Free-form string emitted by the engine — see :class:`TransformKind`
    #: for the common values. Parameterised forms like ``aggregation("sum")``
    #: appear here verbatim.
    transform: str


class ColumnDef(BaseModel):
    """Definition of a column in a model's output schema."""

    name: str


class ModelLineageResult(BaseModel):
    """Output of ``rocky lineage <model> --json`` (full model)."""

    version: str
    command: str
    model: str
    columns: list[ColumnDef]
    upstream: list[str]
    downstream: list[str]
    edges: list[LineageEdge]


class ColumnLineageResult(BaseModel):
    """Output of ``rocky lineage <model>.<column> --json`` (single column trace)."""

    version: str
    command: str
    model: str
    column: str
    trace: list[LineageEdge]


# ---------------------------------------------------------------------------
# Test output (v0.1.0)
# ---------------------------------------------------------------------------


class TestResult(BaseModel):
    """Output of ``rocky test --json``."""

    __test__ = False  # Prevent pytest collection

    version: str
    command: str
    total: int
    passed: int
    failed: int
    failures: list[list[str]]


# ---------------------------------------------------------------------------
# CI output (v0.1.0)
# ---------------------------------------------------------------------------


class CiResult(BaseModel):
    """Output of ``rocky ci --json``."""

    version: str
    command: str
    compile_ok: bool
    tests_ok: bool
    models_compiled: int
    tests_passed: int
    tests_failed: int
    exit_code: int
    diagnostics: list[Diagnostic]
    failures: list[list[str]]


# ---------------------------------------------------------------------------
# History output (v0.3.0)
# ---------------------------------------------------------------------------


class ModelExecution(BaseModel):
    """A single model execution within a run."""

    model_name: str
    started_at: datetime
    finished_at: datetime
    duration_ms: int
    rows_affected: int | None = None
    status: str
    sql_hash: str
    bytes_scanned: int | None = None
    bytes_written: int | None = None


class RunRecord(BaseModel):
    """A complete pipeline run record."""

    run_id: str
    started_at: datetime
    finished_at: datetime
    status: str
    models_executed: list[ModelExecution]
    trigger: str
    config_hash: str


class HistoryResult(BaseModel):
    """Output of ``rocky history --json`` (all runs)."""

    version: str
    command: str
    runs: list[RunRecord]
    count: int


class ModelHistoryResult(BaseModel):
    """Output of ``rocky history --model <name> --json`` (single model)."""

    version: str
    command: str
    model: str
    executions: list[ModelExecution]
    count: int


# ---------------------------------------------------------------------------
# Metrics output (v0.3.0)
# ---------------------------------------------------------------------------


class QualityMetrics(BaseModel):
    """Quality metrics for a single model snapshot."""

    row_count: int
    null_rates: dict[str, float]
    freshness_lag_seconds: int | None = None


class QualitySnapshot(BaseModel):
    """A point-in-time quality snapshot for a model."""

    timestamp: datetime
    run_id: str
    model_name: str
    metrics: QualityMetrics


class MetricsResult(BaseModel):
    """Output of ``rocky metrics <model> --json``."""

    version: str
    command: str
    model: str
    snapshots: list[QualitySnapshot]
    count: int
    alerts: list[dict] | None = None
    column: str | None = None
    column_trend: list[dict] | None = None


# ---------------------------------------------------------------------------
# Optimize output (v0.3.0)
# ---------------------------------------------------------------------------


class MaterializationCost(BaseModel):
    """Cost estimate and strategy recommendation for a model."""

    model_name: str
    current_strategy: str
    compute_cost_per_run: float
    storage_cost_per_month: float
    downstream_references: int
    recommended_strategy: str
    estimated_monthly_savings: float
    reasoning: str


class OptimizeResult(BaseModel):
    """Output of ``rocky optimize --json``."""

    version: str
    command: str
    recommendations: list[MaterializationCost]
    total_models_analyzed: int


# ---------------------------------------------------------------------------
# AI Level 3 types
# ---------------------------------------------------------------------------


class AiResult(BaseModel):
    """Output of ``rocky ai "<intent>"``."""

    version: str
    command: str
    intent: str
    format: str
    name: str
    source: str
    attempts: int


class AiSyncProposal(BaseModel):
    """A proposed model update from ai-sync."""

    model: str
    intent: str
    current_source: str
    proposed_source: str
    diff: str
    upstream_changes: list[dict] = []


class AiSyncResult(BaseModel):
    """Output of ``rocky ai-sync``."""

    version: str
    command: str
    proposals: list[AiSyncProposal] = []


class AiExplanation(BaseModel):
    """A single model explanation."""

    model: str
    intent: str
    saved: bool


class AiExplainResult(BaseModel):
    """Output of ``rocky ai-explain``."""

    version: str
    command: str
    explanations: list[AiExplanation] = []


class AiTestAssertion(BaseModel):
    """A generated test assertion."""

    name: str
    sql: str
    description: str


class AiTestModelResult(BaseModel):
    """Test results for a single model."""

    model: str
    tests: list[AiTestAssertion] = []
    saved: bool


class AiTestResult(BaseModel):
    """Output of ``rocky ai-test``."""

    version: str
    command: str
    results: list[AiTestModelResult] = []


# ---------------------------------------------------------------------------
# Migration validation types
# ---------------------------------------------------------------------------


class ModelValidation(BaseModel):
    """Validation result for a single model."""

    model: str
    present_in_dbt: bool = True
    present_in_rocky: bool = True
    compile_ok: bool = True
    test_count: int = 0
    contracts_generated: int = 0
    warnings: list[str] = []


class ValidateMigrationResult(BaseModel):
    """Output of ``rocky validate-migration``."""

    version: str
    command: str
    validations: list[ModelValidation] = []


# ---------------------------------------------------------------------------
# Adapter conformance types
# ---------------------------------------------------------------------------


class AdapterTestResult(BaseModel):
    """A single conformance test result."""

    name: str
    category: str
    status: str
    message: str | None = None
    duration_ms: int | None = None


class ConformanceResult(BaseModel):
    """Output of ``rocky test-adapter``."""

    version: str
    command: str
    adapter: str
    sdk_version: str
    tests_run: int
    tests_passed: int
    tests_failed: int
    tests_skipped: int
    results: list[AdapterTestResult] = []


# ---------------------------------------------------------------------------
# Doctor output
# ---------------------------------------------------------------------------


class HealthStatus(StrEnum):
    healthy = "healthy"
    warning = "warning"
    critical = "critical"


class HealthCheck(BaseModel):
    """A single health check result from ``rocky doctor``."""

    name: str
    status: HealthStatus
    message: str
    duration_ms: int


class DoctorResult(BaseModel):
    """Output of ``rocky doctor``."""

    command: str
    overall: str
    checks: list[HealthCheck]
    suggestions: list[str]


# ---------------------------------------------------------------------------
# Drift output
# ---------------------------------------------------------------------------


class DriftedColumn(BaseModel):
    """A column whose type differs between source and target."""

    name: str
    source_type: str
    target_type: str


class DriftActionKind(StrEnum):
    drop_and_recreate = "DropAndRecreate"
    alter_column_types = "AlterColumnTypes"
    ignore = "Ignore"


class DriftTableResult(BaseModel):
    """Drift detection result for a single table."""

    table: str
    drifted_columns: list[DriftedColumn]
    action: DriftActionKind


class DriftDetectResult(BaseModel):
    """Output of ``rocky drift --detect``."""

    command: str
    tables_checked: int
    tables_drifted: int
    results: list[DriftTableResult]


# ---------------------------------------------------------------------------
# Phase 2 round 9 — generated types bridge
# ---------------------------------------------------------------------------
#
# As of Phase 2, every Rocky CLI command's JSON output has a typed Rust
# struct deriving JsonSchema. The schemars-generated Pydantic v2 models live
# in `dagster_rocky.types_generated`. This section re-exports them from
# `dagster_rocky.types` so consumers have ONE place to import from.
#
# Naming convention: the generated classes use the Rust field-struct names
# Generated Pydantic models from JSON schemas (via datamodel-codegen).
# The generated names use the Rust struct names (`DiscoverOutput`,
# `RunOutput`, etc.) while the hand-written classes above use
# Python-flavored names (`DiscoverResult`, `RunResult`, etc.).
# Both remain importable. The hand-written classes are the public API.

from .types_generated import (  # noqa: E402, F401
    AiExplainOutput,
    AiGenerateOutput,
    AiSyncOutput,
    AiTestOutput,
    AnomalyOutput,
    BranchDeleteOutput,
    BranchEntry,
    BranchListOutput,
    BranchOutput,
    ChecksConfigOutput,
    CiDiffOutput,
    CiOutput,
    ColumnLineageOutput,
    ColumnTrendPoint,
    CompileOutput,
    DagEdgeOutput,
    DagNodeOutput,
    DagOutput,
    DagSummaryOutput,
    DiffResult,
    DiffSummary,
    DiscoverOutput,
    DoctorOutput,
    DriftActionOutput,
    DriftOutput,
    DriftSummary,
    FreshnessConfigOutput,
    HistoryOutput,
    LineageColumnDef,
    LineageEdgeRecord,
    LineageOutput,
    LineageQualifiedColumn,
    MaterializationOutput,
    MetricsAlert,
    MetricsOutput,
    MetricsSnapshotEntry,
    ModelExecutionRecord,
    ModelHistoryOutput,
    OptimizeOutput,
    OptimizeRecommendation,
    PartitionShapeOutput,
    PermissionSummary,
    PhaseTimings,
    PlanOutput,
    ReplayModelOutput,
    ReplayOutput,
    RunHistoryRecord,
    RunOutput,
    SourceOutput,
    StateOutput,
    TableCheckOutput,
    TableErrorOutput,
    TableOutput,
    TestFailure,
    TestOutput,
)

# Python-flavored bridge aliases for the DAG output types.
DagResult = DagOutput
DagNode = DagNodeOutput
DagEdge = DagEdgeOutput

# ---------------------------------------------------------------------------
# Union type and parser
# ---------------------------------------------------------------------------

RockyOutput = (
    DiscoverResult
    | RunResult
    | PlanResult
    | StateResult
    | CompileResult
    | ModelLineageResult
    | ColumnLineageResult
    | TestResult
    | CiResult
    | CiDiffOutput
    | HistoryResult
    | ModelHistoryResult
    | MetricsResult
    | OptimizeResult
    | AiResult
    | AiSyncResult
    | AiExplainResult
    | AiTestResult
    | ValidateMigrationResult
    | ConformanceResult
    | DoctorResult
    | DriftDetectResult
    | DagResult
)


# Maps the ``command`` field of a Rocky JSON payload to the model that should
# parse it. Commands that need shape-based discrimination (lineage, history)
# are handled separately below.
_SIMPLE_DISPATCH: dict[str, type[BaseModel]] = {
    "discover": DiscoverResult,
    "run": RunResult,
    "plan": PlanResult,
    "state": StateResult,
    "compile": CompileResult,
    "test": TestResult,
    "ci": CiResult,
    "ci-diff": CiDiffOutput,
    "metrics": MetricsResult,
    "optimize": OptimizeResult,
    "ai": AiResult,
    "ai_sync": AiSyncResult,
    "ai_explain": AiExplainResult,
    "ai_test": AiTestResult,
    "validate-migration": ValidateMigrationResult,
    "test-adapter": ConformanceResult,
    "doctor": DoctorResult,
    "drift": DriftDetectResult,
    "dag": DagResult,
}


def parse_rocky_output(json_str: str) -> RockyOutput:
    """Parse a Rocky JSON output payload, auto-detecting the command type.

    Raises:
        ValueError: If the ``command`` field is missing or unrecognized.
    """
    data = json.loads(json_str)
    command = data.get("command", "")

    # Lineage and history have multiple shapes that share the same command name.
    if command == "lineage":
        if "column" in data:
            return ColumnLineageResult.model_validate(data)
        return ModelLineageResult.model_validate(data)
    if command == "history":
        if "model" in data:
            return ModelHistoryResult.model_validate(data)
        return HistoryResult.model_validate(data)

    if command in _SIMPLE_DISPATCH:
        return _SIMPLE_DISPATCH[command].model_validate(data)

    raise ValueError(f"Unknown Rocky command type: {command!r}")
