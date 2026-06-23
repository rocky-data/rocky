"""Helpers that translate Rocky run output into Dagster events.

These functions exist for code that builds custom Dagster assets outside of
``RockyComponent`` and wants to surface Rocky materializations and check
results without re-implementing the metadata mapping.
"""

from __future__ import annotations

from typing import Any

import dagster as dg

from .contracts import sanitize_check_name
from .types import CheckResult, MaterializationInfo, OptimizeResult, RunResult


def emit_materializations(result: RunResult) -> list[dg.AssetMaterialization]:
    """Convert Rocky materializations into Dagster ``AssetMaterialization`` events."""
    return [
        dg.AssetMaterialization(
            asset_key=dg.AssetKey(mat.asset_key),
            metadata=_materialization_metadata(mat),
        )
        for mat in result.materializations
    ]


def emit_check_results(result: RunResult) -> list[dg.AssetCheckResult]:
    """Convert Rocky check results into Dagster ``AssetCheckResult`` events.

    Check names are passed through :func:`sanitize_check_name`: engine results
    carry structured names (``null_rate:<col>``, ``cross_source_overlap:…``)
    whose ``:``/``.`` separators are invalid Dagster check names and would raise
    at event construction. The matching spec must be declared with the sanitized
    name too (mirroring ``RockyComponent``'s emit path).
    """
    events: list[dg.AssetCheckResult] = []
    for table_check in result.check_results:
        key = dg.AssetKey(table_check.asset_key)
        for check in table_check.checks:
            events.append(
                dg.AssetCheckResult(
                    asset_key=key,
                    check_name=sanitize_check_name(check.name),
                    passed=check.passed,
                    severity=dagster_check_severity(check),
                    metadata=check_metadata(check),
                )
            )
    return events


def cost_metadata_from_optimize(result: OptimizeResult) -> dict[str, dict[str, Any]]:
    """Extract per-model cost metadata from an optimize result.

    Returns a dict keyed by model name. The values are suitable for attaching
    to ``AssetMaterialization`` metadata in custom assets that surface Rocky's
    cost recommendations alongside materializations.
    """
    return {
        rec.model_name: {
            "compute_cost_per_run": rec.compute_cost_per_run,
            "storage_cost_per_month": rec.storage_cost_per_month,
            "current_strategy": rec.current_strategy,
            "recommended_strategy": rec.recommended_strategy,
            "estimated_monthly_savings": rec.estimated_monthly_savings,
            "downstream_references": rec.downstream_references,
            "reasoning": rec.reasoning,
        }
        for rec in result.recommendations
    }


def _materialization_metadata(mat: MaterializationInfo) -> dict[str, dg.MetadataValue]:
    metadata: dict[str, dg.MetadataValue] = {
        "strategy": dg.MetadataValue.text(mat.metadata.strategy),
        "duration_ms": dg.MetadataValue.int(mat.duration_ms),
    }
    if mat.rows_copied is not None:
        metadata["rows_copied"] = dg.MetadataValue.int(mat.rows_copied)
    if mat.metadata.watermark is not None:
        metadata["watermark"] = dg.MetadataValue.text(mat.metadata.watermark.isoformat())
    return metadata


def check_metadata(check: CheckResult) -> dict[str, dg.MetadataValue]:
    """Build a Dagster metadata mapping for a single Rocky check result.

    Only the fields that the specific check populated are included; this keeps
    the UI free of empty/``None`` rows for checks that don't apply.
    """
    metadata: dict[str, dg.MetadataValue] = {}

    # Row count
    if check.source_count is not None:
        metadata["source_count"] = dg.MetadataValue.int(check.source_count)
    if check.target_count is not None:
        metadata["target_count"] = dg.MetadataValue.int(check.target_count)

    # Column match
    if check.missing is not None:
        metadata["missing_columns"] = dg.MetadataValue.text(
            ", ".join(check.missing) if check.missing else "(none)"
        )
    if check.extra is not None:
        metadata["extra_columns"] = dg.MetadataValue.text(
            ", ".join(check.extra) if check.extra else "(none)"
        )

    # Freshness
    if check.lag_seconds is not None:
        metadata["lag_seconds"] = dg.MetadataValue.int(check.lag_seconds)
    if check.threshold_seconds is not None:
        metadata["threshold_seconds"] = dg.MetadataValue.int(check.threshold_seconds)

    # Null rate
    if check.column is not None:
        metadata["column"] = dg.MetadataValue.text(check.column)
    if check.null_rate is not None:
        metadata["null_rate"] = dg.MetadataValue.float(check.null_rate)
    if check.threshold is not None:
        metadata["threshold"] = dg.MetadataValue.float(check.threshold)

    # Custom check
    if check.query is not None:
        metadata["query"] = dg.MetadataValue.text(check.query)
    if check.result_value is not None:
        metadata["result_value"] = dg.MetadataValue.int(check.result_value)

    # Severity — surfaced only when advisory (``"warning"``). ``"error"`` is the
    # implicit default, so showing it on every check would clutter the panel.
    # This keeps the advisory level visible even on a *passing* check, where
    # Dagster's ``AssetCheckSeverity`` is not rendered.
    if getattr(check, "severity", None) == "warning":
        metadata["severity"] = dg.MetadataValue.text("warning")

    return metadata


def dagster_check_severity(check: CheckResult) -> dg.AssetCheckSeverity:
    """Map a Rocky check's configured severity to a Dagster ``AssetCheckSeverity``.

    Rocky's :attr:`CheckResult.severity` carries the operator's configured
    intent: ``"warning"`` is advisory, ``"error"`` (the default) is a hard
    failure. A *failing* check surfaces at this severity — ``WARN`` does not
    degrade asset health, ``ERROR`` does — so an advisory check can fail without
    paging an ``ASSET_HEALTH_DEGRADED`` alert. Defaults to ``ERROR`` when the
    engine omits severity (older binaries), preserving the prior fail-hard
    behaviour.
    """
    if getattr(check, "severity", None) == "warning":
        return dg.AssetCheckSeverity.WARN
    return dg.AssetCheckSeverity.ERROR
