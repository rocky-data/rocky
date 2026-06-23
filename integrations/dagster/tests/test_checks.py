"""Tests for Rocky check/materialization event emission."""

from __future__ import annotations

import dagster as dg

from dagster_rocky.checks import (
    check_metadata,
    cost_metadata_from_optimize,
    dagster_check_severity,
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


def test_emit_check_results_maps_severity():
    """The functional helper maps engine severity onto the emitted event, just
    like ``RockyComponent``'s path — a failing advisory check is a Dagster WARN."""
    result = RunResult.model_validate(
        {
            "version": "1.0.0",
            "command": "run",
            "filter": "tenant=acme",
            "duration_ms": 1,
            "tables_copied": 0,
            "materializations": [],
            "check_results": [
                {
                    "asset_key": ["fivetran", "acme", "shopify", "orders"],
                    "checks": [
                        {"name": "null_rate", "passed": False, "severity": "warning"},
                        {"name": "row_count", "passed": False, "severity": "error"},
                    ],
                }
            ],
            "permissions": {
                "grants_added": 0,
                "grants_revoked": 0,
                "catalogs_created": 0,
                "schemas_created": 0,
            },
            "drift": {"tables_checked": 0, "tables_drifted": 0, "actions_taken": []},
        }
    )
    by_name = {e.check_name: e for e in emit_check_results(result)}
    assert by_name["null_rate"].severity == dg.AssetCheckSeverity.WARN
    assert by_name["row_count"].severity == dg.AssetCheckSeverity.ERROR


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


def test_check_metadata_surfaces_advisory_severity():
    """An advisory (``warning``) check surfaces its severity so it's visible in
    the UI regardless of pass/fail — on a *passing* check Dagster doesn't render
    the ``AssetCheckSeverity`` at all, and on a failing one the row corroborates
    the WARN level."""
    passing = CheckResult(
        name="null_rate",
        passed=True,
        column="email",
        null_rate=0.0,
        threshold=0.05,
        severity="warning",
    )
    failing = CheckResult(
        name="null_rate",
        passed=False,
        column="email",
        null_rate=0.9,
        threshold=0.05,
        severity="warning",
    )
    assert check_metadata(passing)["severity"].value == "warning"
    assert check_metadata(failing)["severity"].value == "warning"


def test_check_metadata_omits_default_error_severity():
    """The implicit ``error`` default is not surfaced — it would clutter every
    check's panel with no added signal."""
    check = CheckResult(name="row_count", passed=True, source_count=10, target_count=10)
    assert "severity" not in check_metadata(check)


def test_dagster_check_severity_maps_engine_severity():
    """``warning`` → WARN, ``error`` → ERROR, and an absent/defaulted severity
    falls back to ERROR (back-compat with older binaries). The mapping reads the
    configured *intent*, not the outcome, so it ignores ``passed`` — a passing
    advisory check is still WARN."""
    warn = CheckResult(name="null_rate", passed=False, severity="warning")
    warn_passing = CheckResult(name="null_rate", passed=True, severity="warning")
    err = CheckResult(name="row_count", passed=False, severity="error")
    defaulted = CheckResult(name="row_count", passed=False)  # severity defaults to "error"

    assert dagster_check_severity(warn) == dg.AssetCheckSeverity.WARN
    assert dagster_check_severity(warn_passing) == dg.AssetCheckSeverity.WARN
    assert dagster_check_severity(err) == dg.AssetCheckSeverity.ERROR
    assert dagster_check_severity(defaulted) == dg.AssetCheckSeverity.ERROR


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
