"""Pydantic models matching Rocky's JSON output schema (v0.3.0+).

Every Rocky CLI command that supports ``--output json`` has a corresponding
result model in this module. ``parse_rocky_output`` auto-detects the command
type from the JSON payload and returns the matching model.
"""

from __future__ import annotations

import json
from datetime import datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, Field

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
    #: Adapter-namespaced metadata surfaced by the discovery adapter.
    #: Keys are conventionally prefixed with the adapter kind
    #: (``fivetran.service``, ``fivetran.connector_id``,
    #: ``fivetran.custom_reports``, ``fivetran.custom_tables``,
    #: ``fivetran.schema_prefix``, ...) so entries from different adapters
    #: don't collide when the component folds them into the asset graph.
    #: Values are opaque (Rocky relays the service-specific payload as-is).
    #: Empty when the adapter hasn't opted in — absent on the wire in that
    #: case, surfaced here as ``{}``.
    metadata: dict[str, Any] = Field(default_factory=dict)


class FreshnessConfig(BaseModel):
    """Freshness check configuration projected from ``rocky.toml`` ``[checks.freshness]``.

    Per-schema overrides are intentionally not exposed yet — the Rocky-side
    output omits them until the override-key semantics are nailed down.
    """

    threshold_seconds: int


class ResolvedCheckName(BaseModel):
    """A resolved check name ``rocky discover`` projects so a consumer can
    pre-declare a matching check spec.

    ``name`` byte-matches the ``CheckResult.name`` the engine emits at run
    time, so a consumer can declare a spec with this exact name and have the
    run-time result land against it. ``candidate`` is ``True`` for names whose
    existence depends on runtime-discovered siblings (``cross_source_overlap``)
    and so may not be emitted on every run.
    """

    name: str
    #: Check-kind tag: ``custom`` | ``assertion`` | ``null_rate`` |
    #: ``cross_source_overlap``.
    kind: str
    candidate: bool = False


class ChecksConfig(BaseModel):
    """Pipeline-level checks configuration projected into the discover output.

    A thin projection of ``rocky_core::config::ChecksConfig`` exposing only
    the fields downstream orchestrators currently consume.
    """

    freshness: FreshnessConfig | None = None
    #: Resolved per-model check names the pipeline emits as ``CheckResult.name``
    #: at run time, keyed by unqualified table/model name. Only the non-default
    #: checks (custom / assertion / null_rate / cross_source_overlap) are
    #: listed. Consumed by ``dagster-rocky``'s ``surface_configured_checks`` to
    #: pre-declare matching asset-check specs. Empty when no non-default checks
    #: are configured.
    configured_checks: dict[str, list[ResolvedCheckName]] = Field(default_factory=dict)


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
    #: Sources the discovery adapter attempted to fetch metadata for and
    #: failed (transient HTTP error, timeout, rate-limit budget exhausted,
    #: auth blip). Their absence from :attr:`sources` does NOT mean they
    #: were removed upstream — consumers diffing against a prior run must
    #: treat failed sources as "unknown state, do not delete." Empty when
    #: discovery completed cleanly. See FR-014. Forward-references the
    #: codegen-generated :class:`FailedSourceOutput` re-exported in the
    #: round 9 bridge block below — ``from __future__ import annotations``
    #: defers evaluation so the forward reference resolves at runtime.
    failed_sources: list[FailedSourceOutput] = []
    #: Groups of ≥2 discovered sources sharing the SAME external object id
    #: but resolving to DIFFERENT target paths — the same underlying object
    #: onboarded twice under different schemas. Populated only when
    #: discovery's ``on_collision`` is ``warn``/``error`` and the adapter
    #: supplies an ``external_object_id``; empty otherwise. Forward-references
    #: :class:`CollisionCandidate` (defined with the run-output classes
    #: below); ``from __future__ import annotations`` defers evaluation so
    #: the reference resolves at runtime.
    collision_candidates: list[CollisionCandidate] = []
    #: Source schemas seen for the first time relative to the prior persisted
    #: ``discover`` snapshot — the catch-a-duplicate-at-onboarding signal.
    #: Populated only when the pipeline's discovery config sets
    #: ``report_new_sources``; empty otherwise. The first-ever discover of a
    #: pipeline establishes the baseline and reports none.
    new_sources: list[str] = []
    #: Count of source schemas served from the discovery cache this run.
    #: ``None`` for older binaries that don't emit it.
    schemas_cached: int | None = None


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
    #: Wall-clock timestamp at which the engine began executing this model.
    #: Present on the wire; ``None`` for older binaries that don't emit it.
    started_at: datetime | None = None
    #: Classified-retry attempt trail — one entry per try. Empty (and omitted
    #: on the wire) for a clean first-try success. Without this declared,
    #: Pydantic's ``extra="ignore"`` would drop it silently.
    attempts: list[AttemptRecord] = Field(default_factory=list)
    #: Observed dollar cost of this materialization. ``None`` for unbilled
    #: warehouses (DuckDB) or when the adapter didn't report the inputs.
    cost_usd: float | None = None
    #: Adapter-reported billing-relevant bytes figure. ``None`` when the
    #: adapter doesn't surface one.
    bytes_scanned: int | None = None
    #: Adapter-reported bytes-written figure. ``None`` on every adapter today.
    bytes_written: int | None = None
    #: Tenant this materialization is attributed to (from the discover-time
    #: ``{tenant}`` schema-pattern component). ``None`` for non-tenant patterns.
    tenant: str | None = None
    #: Warehouse-side job identifiers for the SQL statements rocky issued.
    #: Empty for adapters without a job concept (DuckDB).
    job_ids: list[str] = Field(default_factory=list)


class CheckResult(BaseModel):
    name: str
    passed: bool
    #: Configured failure severity emitted by the engine: ``"error"`` (a hard
    #: failure) or ``"warning"`` (advisory — does not fail the run). Defaults to
    #: ``"error"`` for older binaries that don't emit the field. Without this
    #: field declared, Pydantic's default ``extra="ignore"`` would silently drop
    #: the wire value, so any consumer mapping it (e.g. dagster-rocky's
    #: ``AssetCheckSeverity``) would never see anything but the default.
    severity: str | None = "error"
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
    # Assertion (unit-test) check fields — the ``TestType`` discriminant plus
    # the failing-row count. ``kind`` is snake_case (e.g. ``"not_null"``).
    kind: str | None = None
    failing_rows: int | None = None
    # Cross-source-overlap check fields.
    contributing_tables: list[str] | None = None
    overlap_count: int | None = None
    sample: list[str] | None = None


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
    #: Coarse classification of the failure so orchestrators can branch on
    #: kind (retry, page, surface) without parsing ``error``. One of
    #: ``connection-failed``, ``auth-failed``, ``query-rejected``,
    #: ``transient``, ``quota-exceeded``, ``not-found``, ``unknown``.
    #: Defaults to ``"unknown"`` for older engine binaries that omit the
    #: field.
    failure_kind: str = "unknown"
    #: Engine-supplied retry-after hint in whole seconds. Populated when a
    #: warehouse circuit breaker tripped on a half-open-recovery config —
    #: the warehouse-side mirror of ``FailedSource.cooldown_seconds``.
    #: ``None`` for failures without an engine-supplied hint, and for
    #: older engine binaries that don't yet emit the field. Read by
    #: ``_run_filters`` to project the engine's cooldown onto the
    #: retriable :class:`dg.Failure` instead of the hard-coded fallback.
    cooldown_seconds: int | None = None


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


class CollisionCandidate(BaseModel):
    """A cross-source collision: one external object id that resolves to
    more than one target path (the same underlying object onboarded twice
    under different schemas).

    Surfaced on :attr:`DiscoverResult.collision_candidates` when discovery's
    ``on_collision`` is ``warn``/``error`` and the adapter supplies an
    ``external_object_id``. Mirrors the engine-side
    ``CollisionCandidateOutput`` shape.
    """

    #: The shared external object id (e.g. a Fivetran ad-account id) that
    #: more than one discovered source resolves to.
    external_object_id: str
    #: The distinct source schemas (≥2, sorted) that resolve to the shared
    #: object id. Review these for a duplicate onboarding.
    sources: list[str]


class ExecutionSummary(BaseModel):
    """Summary of execution parallelism and throughput."""

    concurrency: int
    tables_processed: int
    tables_failed: int
    #: Whether adaptive concurrency (AIMD throttle) was enabled for this run.
    #: ``None`` for older binaries that don't emit the field.
    adaptive_concurrency: bool | None = None
    #: Concurrency the AIMD throttle settled on by end of run. ``None`` when
    #: adaptive concurrency was disabled or not reported.
    final_concurrency: int | None = None
    #: Count of rate-limit signals the throttle detected. ``None`` when not
    #: reported.
    rate_limits_detected: int | None = None


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


class ContainedModel(BaseModel):
    """A model Rocky withheld this run because an upstream failed.

    Emitted only under ``[resilience] contain_failures``: when a model (or one
    of its upstreams) fails, the engine continues the disjoint subgraphs and
    records every withheld model here — the blast radius of the failures named
    in :attr:`RunResult.errors`. A withheld model was **not built**; its target
    was left untouched. Empty (and omitted from the wire) for a default
    fail-fast run and for any successful run.

    Hand-written to match the wire field names emitted by the engine's
    ``ContainedModelOutput``. It is not re-exported from the generated barrel,
    so it lives here alongside the other hand-written run-output models. Kept in
    sync with the generated schema by the ``contained[]`` parse test in
    ``tests/test_types.py``.
    """

    #: The withheld model's name (matches the model entry in the project DAG).
    model: str
    #: The failed-or-withheld upstream(s) that reach this model — its direct
    #: poisoned dependencies. The run's :attr:`RunResult.errors` carry the
    #: root-cause detail.
    blocked_by: list[str] = Field(default_factory=list)
    #: Operator hint: resolve the named upstream failure(s), then re-run.
    unblock_hint: str = ""


class RunResult(BaseModel):
    version: str
    command: str
    #: Whole-run status (``"Success"`` / ``"PartialFailure"`` / ``"Failure"`` /
    #: ``"SkippedIdempotent"`` / ``"SkippedInFlight"``). Present on the wire;
    #: ``None`` for older binaries that keyed pass/fail off counts alone.
    status: str | None = None
    filter: str
    duration_ms: int
    tables_copied: int
    tables_failed: int = 0
    #: Tables short-circuited via the idempotency key. Defaults to 0.
    tables_skipped: int = 0
    materializations: list[MaterializationInfo]
    check_results: list[TableCheckResult]
    errors: list[TableError] = []
    #: ``True`` when the run was cancelled by SIGINT (Ctrl-C). Always emitted
    #: on the wire; defaults to ``False`` for older binaries.
    interrupted: bool = False
    #: ``True`` when the run executed in shadow mode (targets rewritten).
    shadow: bool = False
    #: Per-model build/skip/reuse decision + reason. Empty (and omitted on the
    #: wire) for a default run; populated under ``--skip-unchanged`` / ``[reuse]``.
    model_decisions: list[ModelDecisionOutput] = Field(default_factory=list)
    #: Row-quarantine outcomes — one entry per table the quality pipeline
    #: quarantined. Empty for runs that didn't use ``[checks.quarantine]``.
    quarantine: list[QuarantineOutput] = Field(default_factory=list)
    #: Aggregate cost attribution across every materialization in this run.
    #: ``None`` for DuckDB-only pipelines or when nothing produced a cost number.
    cost_summary: RunCostSummary | None = None
    #: Budget breaches detected at end of run. Empty when no ``[budget]`` block
    #: is configured or all limits were respected.
    budget_breaches: list[BudgetBreachOutput] = Field(default_factory=list)
    #: Soft warnings from the per-table override resolver — one entry per
    #: ``[[table_overrides]]`` rule that matched nothing. Empty when no
    #: overrides are declared.
    override_warnings: list[OverrideWarningOutput] = Field(default_factory=list)
    #: The ``--idempotency-key`` value this run was invoked with, echoed back.
    #: ``None`` for runs that didn't pass the flag.
    idempotency_key: str | None = None
    #: Prior/in-flight run whose idempotency key deflected this call. Populated
    #: only when ``status`` is ``skipped_idempotent`` / ``skipped_in_flight``.
    skipped_by_run_id: str | None = None
    #: Pipeline type that was executed (e.g. ``"replication"``). ``None`` for
    #: older binaries that don't emit it.
    pipeline_type: str | None = None
    #: Run id this run resumed from, when invoked with ``--resume``. ``None``
    #: for a fresh run.
    resumed_from: str | None = None
    #: Models withheld this run because an upstream failed (or was itself
    #: withheld) and ``[resilience] contain_failures`` continued the disjoint
    #: subgraphs — the blast radius of the failures in :attr:`errors`. Empty
    #: (and omitted on the wire) for a default fail-fast run and for any
    #: successful run. Without this field declared, Pydantic's default
    #: ``extra="ignore"`` would silently drop the wire value (the runtime
    #: ``RunResult`` is the hand-written dispatch target, not the generated
    #: ``RunOutput``), so a consumer mapping it — e.g. dagster-rocky surfacing
    #: the containment blast radius — would never see anything.
    contained: list[ContainedModel] = Field(default_factory=list)
    #: Tables filtered out before execution because they don't exist in
    #: the source warehouse. Empty when nothing was excluded. The CLI
    #: skips serializing this field when empty, so it remains backwards
    #: compatible with older Rocky binaries that don't emit it at all.
    excluded_tables: list[ExcludedTable] = []
    execution: ExecutionSummary | None = None
    metrics: MetricsSnapshot | None = None
    permissions: PermissionInfo
    drift: DriftInfo
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


class ClassificationAction(BaseModel):
    """Preview row for an `apply_column_tags` action Rocky would issue."""

    model: str
    column: str
    tag: str


class MaskAction(BaseModel):
    """Preview row for an `apply_masking_policy` action Rocky would issue."""

    model: str
    column: str
    tag: str
    resolved_strategy: str


class RetentionAction(BaseModel):
    """Preview row for an `apply_retention_policy` action Rocky would issue."""

    model: str
    duration_days: int
    warehouse_preview: str | None = None


class PlanResult(BaseModel):
    version: str
    command: str
    filter: str
    # Environment passed via `rocky plan --env <name>` — selects which
    # `[mask.<env>]` overrides flow into `mask_actions`.
    env: str | None = None
    statements: list[PlannedStatement]
    # Governance preview — empty / absent on projects without
    # `[classification]` / `[mask]` / `retention` sidecar config.
    classification_actions: list[ClassificationAction] = []
    mask_actions: list[MaskAction] = []
    retention_actions: list[RetentionAction] = []
    # Phase 2 plan-spine fields (Cluster 3 B). Present when rocky plan
    # compiled a models/ directory and persisted a RunPlan blueprint.
    # Absent / None for replication-only invocations — byte-stable.
    plan_id: str | None = None
    plan_kind: str | None = None
    created_at: datetime | None = None
    models: list[str] = []
    execution_layers: list[list[str]] = []
    #: Semantic change-impact verdict from the typed-IR breaking-change
    #: classifier, surfaced as decision support at plan time. Present only when
    #: ``--semantic`` ran with a usable baseline. Kept as a loose ``dict`` — the
    #: nested shape lives on the generated ``PlanOutput.breaking_verdict``.
    breaking_verdict: dict | None = None
    #: Budget diagnostics raised at plan time. Empty when no ``[budget]`` block
    #: is configured.
    budget_diagnostics: list[Diagnostic] = []
    #: ``True`` when at least one ``budget_diagnostics`` entry is error-level.
    has_budget_errors: bool = False


# ``rocky apply <plan-id>`` does NOT emit a wrapping envelope. The engine never
# constructs the old ``{plan_id, plan_kind, success, result}`` shape — each plan
# kind's apply path prints its OWN output: run-shaped kinds (run / replication /
# ai_authored / backfill) print a ``RunOutput`` with ``command:"run"``; ``gc``
# prints ``GcApplyOutput`` with ``command:"apply"``; compact / archive / promote
# print their own outputs. ``RockyClient.apply()`` dispatches by that actual
# ``command`` / shape (see ``rocky_sdk.client._parse_apply``). The dead
# ``ApplyOutput`` shadow that used to live here was removed — it never once
# validated a real engine payload.


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
    #: State-schema version this binary supports. Lets an orchestrator startup
    #: hook decide deploy compatibility structurally.
    schema_version_supported: int | None = None
    #: State-schema version currently on disk, or ``None`` when the store has
    #: no persisted version yet.
    schema_version_on_disk: int | None = None


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
    #: Names of models this model directly depends on. Empty when the model
    #: has no upstream dependencies.
    depends_on: list[str] = Field(default_factory=list)
    #: Source of the model's data contract (e.g. sidecar path), or ``None``.
    contract_source: str | None = None
    #: Heuristic cost estimate from DAG-aware cardinality propagation. Kept as
    #: a loose ``dict`` — the nested shape lives on the generated
    #: ``ModelDetail.cost_hint``. ``None`` when not computed.
    cost_hint: dict | None = None
    #: Hint that a ``full_refresh`` model could benefit from incremental
    #: materialization. Loose ``dict``; ``None`` when not applicable.
    incrementality_hint: dict | None = None
    #: Model-level governance tags — the model's own ``[tags]`` block merged
    #: over any config-group ``[tags]`` baseline (sidecar > group). Free-form
    #: ``{key: value}`` strings describing the model as a whole (``domain``,
    #: ``tier``, ``owner``, …). ``None`` when none are declared. ``dagster-rocky``
    #: projects these onto the derived asset's Dagster tags.
    tags: dict[str, str] | None = None


class CompileResult(BaseModel):
    """Output of ``rocky compile --json``."""

    version: str
    command: str
    models: int
    execution_layers: int
    diagnostics: list[Diagnostic]
    has_errors: bool
    models_detail: list[ModelDetail] = []
    #: Per-phase compile timings. Loose ``dict`` — the nested shape lives on
    #: the generated ``CompileOutput.compile_timings``.
    compile_timings: dict | None = None
    #: Expanded SQL per model after macro substitution. Populated only when
    #: ``--expand-macros`` is passed; ``{}`` otherwise.
    expanded_sql: dict[str, str] = Field(default_factory=dict)


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
    #: Per-node metadata for the lineage graph (one entry per referenced
    #: model). Loose ``dict`` entries — the nested shape lives on the generated
    #: ``LineageOutput.nodes``. Empty when the engine doesn't emit node metadata.
    nodes: list[dict] = Field(default_factory=list)


class ColumnLineageResult(BaseModel):
    """Output of ``rocky lineage <model>.<column> --json`` (single column trace)."""

    version: str
    command: str
    model: str
    column: str
    trace: list[LineageEdge]
    #: Trace direction (``"upstream"`` / ``"downstream"``). ``None`` for older
    #: binaries that don't emit it.
    direction: str | None = None
    #: Downstream consumers of the traced column. Empty when tracing upstream
    #: or when the column has no downstream consumers.
    downstream_consumers: list[QualifiedColumn] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Test / CI output (v0.1.0)
# ---------------------------------------------------------------------------
# ``TestResult`` and ``CiResult`` were hand-written shadows that diverged from
# the engine's real wire format (``failures`` is ``[{"name", "error"}]``, not
# positional ``[[name, error]]``). They are now soft-swapped to the generated
# ``TestOutput`` / ``CiOutput`` — see the alias block near the bottom of this
# module, alongside the ``HistoryResult`` / ``ModelHistoryResult`` swap.


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


# ``HistoryResult`` + ``ModelHistoryResult`` soft-swapped to the generated
# CLI-output-shaped classes below (see "generated types bridge" section).
# The hand-written classes above referenced the state-store ``RunRecord``
# shape (with ``finished_at``, ``config_hash``, ``models_executed`` as a
# list) — that shape doesn't match what ``rocky history --json`` actually
# emits. Aliases live at module scope below the re-export block.


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
    #: Human-readable status message (e.g. "no snapshots yet"). ``None`` when
    #: the command has data to report.
    message: str | None = None


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
    #: Human-readable status message (e.g. "no models to analyze"). ``None``
    #: when recommendations are present.
    message: str | None = None
    #: Advisory note on incrementality opportunities. ``None`` when not emitted.
    incrementality_note: str | None = None


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
    #: Path the generated model body was written to, when ``--save`` was used.
    #: ``None`` for a dry run.
    body_path: str | None = None
    #: Path the intent sidecar was written to, when ``--save`` was used.
    #: ``None`` for a dry run.
    sidecar_path: str | None = None


# ``AiSyncProposal`` / ``AiSyncResult`` were hand-written shadows. The proposal
# shadow required a phantom ``current_source`` the engine has never put on the
# wire (the real ``AiSyncProposal`` is ``{model, intent, diff, proposed_source}``),
# so ``ai_sync`` raised whenever the proposals list was non-empty — the
# empty-list happy path masked it. Both are now soft-swapped to the generated
# ``AiSyncOutput`` / ``AiSyncProposal`` in the bridge block below.


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
    #: Name of the dbt project that was validated. ``None`` when not reported.
    project_name: str | None = None
    #: dbt version detected in the source project. ``None`` when not reported.
    dbt_version: str | None = None
    #: Count of models successfully imported. Defaults to 0.
    models_imported: int = 0
    #: Count of models that failed to import. Defaults to 0.
    models_failed: int = 0
    #: Total tests across imported models. Defaults to 0.
    total_tests: int = 0
    #: Total contracts generated across imported models. Defaults to 0.
    total_contracts: int = 0
    #: Total warnings raised across imported models. Defaults to 0.
    total_warnings: int = 0


# ---------------------------------------------------------------------------
# Adapter conformance types
# ---------------------------------------------------------------------------


# ``ConformanceResult`` / ``AdapterTestResult`` were hand-written shadows. The
# ``ConformanceResult`` shadow required ``version`` / ``command`` that the real
# ``rocky test-adapter`` payload does NOT carry, so ``test_adapter()`` raised on
# ANY real output. Both are now soft-swapped to the generated
# ``TestAdapterOutput`` / ``TestAdapterTestResult`` in the bridge block below.
# ``test-adapter`` has no ``command`` key on the wire, so there is no
# ``parse_rocky_output`` dispatch entry for it (the payload can't be routed by
# command) — it's reachable only via the typed ``RockyClient.test_adapter()``.


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
    #: Structured ``[key, value]`` detail rows for this check. Empty when the
    #: check reports no structured detail.
    details: list[list[str]] = Field(default_factory=list)


class DoctorResult(BaseModel):
    """Output of ``rocky doctor``."""

    command: str
    overall: str
    checks: list[HealthCheck]
    suggestions: list[str]


# ---------------------------------------------------------------------------
# State health — accessor aggregating doctor + history
# ---------------------------------------------------------------------------


#: State backend identifiers, mirroring ``rocky_core::config::StateBackend``.
#: The CLI emits the lowercase variant name for each backend kind.
StateBackendKind = Literal["local", "tiered", "valkey", "s3", "gcs"]


#: Whole-run status captured by ``rocky_core::state::RunStatus``. Emitted by
#: ``rocky history --output json`` via Rust ``{:?}`` formatting, so values use
#: CamelCase on the wire (``"Success"``/``"PartialFailure"``/``"Failure"``).
#: :class:`StateHealthResult` normalises these to the snake_case variants
#: below so callers can match string-literal equality without knowing the
#: wire encoding.
LastRunStatus = Literal["success", "partial_failure", "failure"]


#: Outcome of the optional state-backend write probe. Mirrors the tri-state the
#: engine's ``probe_state_backend`` helper surfaces via the ``state_rw``
#: doctor check: probe wrote + read + deleted cleanly (``ok``), probe exceeded
#: the configured ``transfer_timeout_seconds`` (``timeout``), or some other
#: failure (auth/permissions/network/missing config).
ProbeOutcome = Literal["ok", "timeout", "error"]


class StateHealthResult(BaseModel):
    """Aggregated snapshot of Rocky's state-backend health.

    Surfaces the pair of already-shipped signals — :meth:`RockyResource.doctor`
    for the live ``state_rw`` probe and :meth:`RockyResource.history` for the
    most recent whole-run status — behind one typed API. The primary consumer
    is a Dagster sensor that wants to observe state-backend health per tick
    (see :meth:`RockyResource.state_health`).

    Always-populated fields (cheap path — one ``rocky history`` call plus a
    ``tomllib.load`` of the config):

    * :attr:`backend` — the configured :data:`StateBackendKind`. Defaults to
      ``"local"`` when the config can't be parsed or doesn't declare a
      ``[state]`` table, which matches the engine's default.
    * :attr:`last_run_status` — normalised :data:`LastRunStatus` from the most
      recent run the state store has recorded, or ``None`` when the store
      has no run history yet.
    * :attr:`last_run_at` — ``started_at`` of that same record, or ``None``.

    Probe fields (populated only when the caller passes
    ``probe_write=True``):

    * :attr:`probe_outcome` — :data:`ProbeOutcome` mapped from the ``state_rw``
      doctor check status / message (``healthy`` → ``"ok"``; ``critical`` with
      a ``"timed out"`` / ``"timeout"`` substring in the message → ``"timeout"``;
      any other ``critical`` / ``warning`` → ``"error"``).
    * :attr:`probe_duration_ms` — wall-clock time the probe took, from the
      matching :class:`HealthCheck`. ``None`` when the probe wasn't requested.
    * :attr:`probe_error` — human-readable failure message from the same
      check when :attr:`probe_outcome` is ``"timeout"`` or ``"error"``.
      ``None`` on success and when the probe wasn't requested.

    Example:

        Inspect a snapshot to log or tag per-tick state-backend health.
        ``probe_outcome`` is ``None`` on the cheap path; with
        ``probe_write=True`` it is one of ``"ok"`` / ``"timeout"`` /
        ``"error"``. Tag a run with the outcome::

            from dagster_rocky import StateHealthResult, state_health

            snapshot: StateHealthResult = state_health(rocky, probe_write=True)
            tags = {
                "rocky/state_backend": snapshot.backend,
                "rocky/last_run_status": snapshot.last_run_status or "unknown",
            }
            if snapshot.probe_outcome == "ok":
                tags["rocky/state_rw_ms"] = str(snapshot.probe_duration_ms)
            elif snapshot.probe_outcome in ("timeout", "error"):
                tags["rocky/state_rw_error"] = snapshot.probe_error or "unknown"
    """

    backend: StateBackendKind
    last_run_status: LastRunStatus | None = None
    last_run_at: datetime | None = None
    probe_outcome: ProbeOutcome | None = None
    probe_duration_ms: int | None = None
    probe_error: str | None = None


# ---------------------------------------------------------------------------
# Drift output
# ---------------------------------------------------------------------------
#
# There is no ``rocky drift`` subcommand — the binary rejects it. Drift
# detection happens INSIDE ``rocky run`` / ``rocky plan`` and is surfaced on
# ``RunResult.drift`` (a :class:`DriftInfo`). The hand-written
# ``DriftDetectResult`` / ``DriftTableResult`` / ``DriftedColumn`` /
# ``DriftActionKind`` shadows (and their ``"drift"`` dispatch entry) were
# removed — the engine never emitted a ``{command:"drift", ...}`` payload for
# them to parse. The generated ``drift_schema`` types stay (inert) for the
# ``DriftOutput`` shape referenced by other outputs.


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
    AiContractColumnProfile,
    AiContractOutput,
    AiExplainOutput,
    AiGenerateOutput,
    AiSyncOutput,
    AiSyncProposal,
    AiTestOutput,
    AnomalyOutput,
    ApprovalArtifact,
    ApprovalSignature,
    ApproveOutput,
    ApproverIdentity,
    ApproverSource,
    ArchiveApplyOutput,
    AssetKind,
    AuditEvent,
    AuditEventKind,
    BackfillOutput,
    BranchDeleteOutput,
    BranchEntry,
    BranchListOutput,
    BranchOutput,
    BranchPromoteOutput,
    CatalogAsset,
    CatalogColumn,
    CatalogEdge,
    CatalogOutput,
    CatalogStats,
    ChecksConfigOutput,
    CiDiffOutput,
    CiOutput,
    ClearSchemaCacheOutput,
    ColumnClassificationStatus,
    ColumnLineageOutput,
    ColumnTrendPoint,
    CompactApplyOutput,
    CompileOutput,
    ComplianceException,
    ComplianceOutput,
    ComplianceSummary,
    CostOutput,
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
    EdgeConfidence,
    EnvMaskingStatus,
    ErrorEnvelope,
    FailedSourceOutput,
    FreshnessConfigOutput,
    GcApplyOutput,
    HistoryOutput,
    JobStatus,
    LineageColumnChange,
    LineageColumnDef,
    LineageDiffOutput,
    LineageDiffResult,
    LineageEdgeRecord,
    LineageOutput,
    LineageQualifiedColumn,
    MaterializationOutput,
    MetaOutput,
    MetricsAlert,
    MetricsOutput,
    MetricsSnapshotEntry,
    ModelExecutionRecord,
    ModelHistoryOutput,
    ModelRetentionStatus,
    OptimizeOutput,
    OptimizeRecommendation,
    PartitionShapeOutput,
    PermissionSummary,
    PerModelCostHistorical,
    PhaseTimings,
    PlanOutput,
    PreviewColumnTypeChange,
    PreviewCopiedModel,
    PreviewCostOutput,
    PreviewCostSummary,
    PreviewCreateOutput,
    PreviewDiffOutput,
    PreviewDiffSummary,
    PreviewModelCostDelta,
    PreviewModelDiff,
    PreviewPrunedModel,
    PreviewRowSample,
    PreviewRowSampleChange,
    PreviewSampledRowDiff,
    PreviewSamplingWindow,
    PreviewStructuralDiff,
    PromotePlan,
    PromoteTarget,
    PromoteTargetPlan,
    RecipeExecutionRecord,
    RecipeHistoryOutput,
    RejectedApproval,
    ReplayModelOutput,
    ReplayOutput,
    RetentionStatusOutput,
    RunHistoryRecord,
    RunOutput,
    SignatureAlgorithm,
    SourceOutput,
    StatementResult,
    StateOutput,
    TableCheckOutput,
    TableErrorOutput,
    TableOutput,
    TestAdapterOutput,
    TestAdapterTestResult,
    TestFailure,
    TestOutput,
)

# Run-output nested types used to backfill the hand-written run models so the
# runtime dispatch target (``RunResult`` / ``MaterializationInfo``) stops
# silently dropping wire fields. Imported from the submodule because these
# nested types are not part of the curated ``types_generated`` barrel.
from .types_generated.run_schema import (  # noqa: E402, F401
    AttemptRecord,
    BudgetBreachOutput,
    ModelDecisionOutput,
    OverrideWarningOutput,
    QuarantineOutput,
    RunCostSummary,
)

# Python-flavored bridge aliases for the DAG output types.
DagResult = DagOutput
DagNode = DagNodeOutput
DagEdge = DagEdgeOutput

# Soft-swap aliases — the hand-written ``HistoryResult`` /
# ``ModelHistoryResult`` above diverged from what the Rust CLI emits (they
# mirrored state-store shapes instead). Empty arrays masked the drift until
# `rocky run` started persisting run records. The generated types are the
# source of truth; keep the Result names as exports so external consumers
# don't break. (``OptimizeResult`` is NOT swapped — it stays a hand-written
# model above, now backfilled with the ``message`` / ``incrementality_note``
# fields the wire carries.)
HistoryResult = HistoryOutput
ModelHistoryResult = ModelHistoryOutput

# ``ConformanceResult`` / ``AdapterTestResult`` soft-swap — the hand-written
# ``ConformanceResult`` required ``version`` / ``command`` the real
# ``rocky test-adapter`` payload never carries, so the client raised on ANY
# real output. The generated ``TestAdapterOutput`` / ``TestAdapterTestResult``
# are the source of truth; keep the legacy names as aliases.
ConformanceResult = TestAdapterOutput
AdapterTestResult = TestAdapterTestResult

# ``AiSyncResult`` / ``AiSyncProposal`` soft-swap — the hand-written proposal
# required a phantom ``current_source`` never on the wire, so ``ai_sync``
# raised whenever proposals were non-empty. The generated ``AiSyncOutput`` /
# ``AiSyncProposal`` are the source of truth; keep the legacy names as aliases.
AiSyncResult = AiSyncOutput

# ``TestResult`` / ``CiResult`` had the same drift: both declared
# ``failures: list[list[str]]`` (positional tuples), but the engine serializes
# ``failures`` as ``[{"name", "error"}]`` objects (``TestFailure``) — so the
# hand-written shape only ever parsed the empty (all-pass) case and raised on
# any real failure. They also lacked ``model_results`` / ``declarative`` /
# ``unit_tests``. The generated outputs are the source of truth; keep the
# Result names as aliases for import compatibility.
TestResult = TestOutput
CiResult = CiOutput
# ``TestResult``/``TestOutput`` start with "Test"; without this guard pytest
# tries to collect the alias as a test class wherever a test module imports it.
TestOutput.__test__ = False

# ``rocky ai-contract`` has no hand-written legacy class; the generated model
# is the source of truth. Expose a Python-flavored alias for symmetry with the
# other AI commands.
AiContractResult = AiContractOutput

# ---------------------------------------------------------------------------
# Fivetran state envelope (FR-C)
# ---------------------------------------------------------------------------
# Canonical shape written by ``rocky discover --emit-fivetran-state-to <PATH>``
# and the contract between Rocky and downstream consumers that want the
# Fivetran view of a destination without re-fetching it themselves. The
# generated Pydantic model exposes the same shape under the snake_case
# field set the Rust ``JsonSchema`` derive produces. The envelope's
# component types (``FivetranConnectorSummary`` / ``FivetranSchemaConfig``
# / etc.) remain importable from ``dagster_rocky.types_generated`` for
# consumers that need to type-annotate sub-fields.
from .types_generated import FivetranStateEnvelope  # noqa: E402, F401

FivetranStateEnvelopeOutput = FivetranStateEnvelope

# ---------------------------------------------------------------------------
# Union type and parser
# ---------------------------------------------------------------------------

RockyOutput = (
    DiscoverResult
    | RunResult
    | PlanResult
    | StateResult
    | ClearSchemaCacheOutput
    | CompileResult
    | ModelLineageResult
    | ColumnLineageResult
    | TestResult
    | CiResult
    | CiDiffOutput
    | LineageDiffOutput
    | HistoryResult
    | ModelHistoryResult
    | MetricsResult
    | OptimizeResult
    | CostOutput
    | BackfillOutput
    | AiResult
    | AiSyncResult
    | AiExplainResult
    | AiTestResult
    | AiContractOutput
    | ValidateMigrationResult
    | ConformanceResult
    | DoctorResult
    | DagResult
    | GcApplyOutput
    | ComplianceOutput
    | RetentionStatusOutput
    | CatalogOutput
    | ApproveOutput
    | BranchPromoteOutput
    | PromotePlan
    | CompactApplyOutput
    | ArchiveApplyOutput
)


# Maps the ``command`` field of a Rocky JSON payload to the model that should
# parse it. Commands that need shape-based discrimination (lineage, history)
# are handled separately below.
_SIMPLE_DISPATCH: dict[str, type[BaseModel]] = {
    "discover": DiscoverResult,
    "run": RunResult,
    "plan": PlanResult,
    "state": StateResult,
    "state-clear-schema-cache": ClearSchemaCacheOutput,
    "compile": CompileResult,
    "test": TestResult,
    "ci": CiResult,
    "ci-diff": CiDiffOutput,
    "lineage-diff": LineageDiffOutput,
    "metrics": MetricsResult,
    "optimize": OptimizeResult,
    "cost": CostOutput,
    "backfill": BackfillOutput,
    "ai": AiResult,
    "ai_sync": AiSyncResult,
    "ai_explain": AiExplainResult,
    "ai_test": AiTestResult,
    "ai_contract": AiContractOutput,
    "validate-migration": ValidateMigrationResult,
    "doctor": DoctorResult,
    "dag": DagResult,
    # ``rocky apply`` on a gc plan prints ``{command:"apply", ...}`` with gc
    # markers (``evicted`` / ``refused``). Run-shaped apply prints
    # ``command:"run"`` (→ ``RunResult``); compact / archive / promote apply
    # print their own commands, already routed above. ``RockyClient.apply()``
    # additionally disambiguates by shape.
    "apply": GcApplyOutput,
    "compliance": ComplianceOutput,
    "retention-status": RetentionStatusOutput,
    "catalog": CatalogOutput,
    "branch approve": ApproveOutput,
    "branch promote": BranchPromoteOutput,
    "plan promote": PromotePlan,
    "compact apply": CompactApplyOutput,
    "archive apply": ArchiveApplyOutput,
}


def parse_rocky_output(json_str: str) -> RockyOutput:
    """Parse a Rocky JSON output payload, auto-detecting the command type.

    Raises:
        ValueError: If the payload is not a JSON object, or the
            ``command`` field is missing or unrecognized.
    """
    data = json.loads(json_str)
    if not isinstance(data, dict):
        # Valid JSON that is not an object (``null``, ``[]``, ``5``) would
        # otherwise crash on ``.get`` with an uncaught AttributeError —
        # honour the documented ValueError contract instead.
        raise ValueError(f"Rocky output is not a JSON object: {data!r}")
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


# ---------------------------------------------------------------------------
# Public surface
# ---------------------------------------------------------------------------
#
# Every public model, enum, type alias, and ``parse_rocky_output`` is exported
# so ``from rocky_sdk.types import *`` (and the ``dagster_rocky.types``
# backward-compat shim that re-exports this module) pick up the full surface.
# Computed from the module namespace rather than hand-maintained so a newly
# generated model is exported automatically on the next ``just codegen-sdk``.
# The denylist drops the stdlib / typing / pydantic names pulled in at import
# time so they don't leak into ``import *``.
_NOT_EXPORTED = frozenset(
    {"annotations", "json", "datetime", "StrEnum", "Any", "Literal", "BaseModel", "Field"}
)
__all__ = sorted(
    name for name in dict(globals()) if not name.startswith("_") and name not in _NOT_EXPORTED
)
