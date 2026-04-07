"""Helpers that translate Rocky run output into Dagster events.

These functions exist for code that builds custom Dagster assets outside of
``RockyComponent`` and wants to surface Rocky materializations and check
results without re-implementing the metadata mapping.
"""

from __future__ import annotations

from typing import Any

import dagster as dg

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
    """Convert Rocky check results into Dagster ``AssetCheckResult`` events."""
    events: list[dg.AssetCheckResult] = []
    for table_check in result.check_results:
        key = dg.AssetKey(table_check.asset_key)
        for check in table_check.checks:
            events.append(
                dg.AssetCheckResult(
                    asset_key=key,
                    check_name=check.name,
                    passed=check.passed,
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

    return metadata
