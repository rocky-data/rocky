"""Tests for Rocky check/materialization event emission."""

from __future__ import annotations

from dagster_rocky.checks import (
    check_metadata,
    cost_metadata_from_optimize,
    emit_check_results,
    emit_materializations,
)
from dagster_rocky.types import CheckResult, OptimizeResult, RunResult


def test_emit_materializations(run_json: str):
    result = RunResult.model_validate_json(run_json)
    events = emit_materializations(result)

    assert len(events) == 2
    assert events[0].asset_key.path == [
        "fivetran",
        "acme",
        "us_west",
        "shopify",
        "orders",
    ]
    assert events[0].metadata["strategy"].value == "incremental"
    assert events[0].metadata["rows_copied"].value == 150
    # The second materialization has no rows_copied set.
    assert "rows_copied" not in events[1].metadata


def test_emit_check_results(run_json: str):
    result = RunResult.model_validate_json(run_json)
    events = emit_check_results(result)

    assert len(events) == 5  # 5 checks on 1 table
    assert events[0].check_name == "row_count"
    assert events[0].passed is True
    assert events[1].check_name == "column_match"
    assert events[2].check_name == "freshness"
    assert events[3].check_name == "null_rate"
    assert events[3].metadata["column"].value == "email"
    assert events[4].check_name == "no_future_dates"


def test_check_metadata_only_includes_populated_fields():
    """``check_metadata`` should never emit ``None`` values."""
    minimal = CheckResult(name="row_count", passed=True, source_count=10, target_count=10)
    metadata = check_metadata(minimal)

    assert set(metadata.keys()) == {"source_count", "target_count"}
    assert metadata["source_count"].value == 10


def test_check_metadata_handles_empty_lists_as_none_text():
    """Empty ``missing``/``extra`` lists should still surface as ``(none)``."""
    check = CheckResult(name="column_match", passed=True, missing=[], extra=[])
    metadata = check_metadata(check)
    assert metadata["missing_columns"].value == "(none)"
    assert metadata["extra_columns"].value == "(none)"


def test_cost_metadata_from_optimize(optimize_json: str):
    """``cost_metadata_from_optimize`` returns one entry per recommendation."""
    result = OptimizeResult.model_validate_json(optimize_json)
    metadata = cost_metadata_from_optimize(result)

    assert set(metadata.keys()) == {"customer_revenue", "raw_events"}
    customer = metadata["customer_revenue"]
    assert customer["compute_cost_per_run"] == 0.0016
    assert customer["storage_cost_per_month"] == 4.60
    assert customer["recommended_strategy"] == "view"
    assert customer["estimated_monthly_savings"] == 4.20
    assert customer["downstream_references"] == 1
    assert "Cheap to compute" in customer["reasoning"]
