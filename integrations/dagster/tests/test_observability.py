"""Tests for the dagster_rocky.observability builders."""

from __future__ import annotations

import dagster as dg

from dagster_rocky.observability import (
    ANOMALY_CHECK_NAME,
    COMPLIANCE_CHECK_NAME,
    COMPLIANCE_FALLBACK_ASSET_KEY,
    RETENTION_OBSERVATION_NAME,
    anomaly_check_results,
    compliance_check_results,
    drift_observations,
    optimize_metadata_for_keys,
    retention_observations,
)
from dagster_rocky.types import (
    AnomalyResult,
    ComplianceOutput,
    DriftAction,
    DriftInfo,
    ExecutionSummary,
    MaterializationCost,
    OptimizeResult,
    PermissionInfo,
    RetentionStatusOutput,
    RunResult,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_run_result(
    *,
    drift: DriftInfo | None = None,
    anomalies: list[AnomalyResult] | None = None,
) -> RunResult:
    return RunResult(
        version="0.3.0",
        command="run",
        filter="tenant=acme",
        duration_ms=1000,
        tables_copied=2,
        tables_failed=0,
        materializations=[],
        check_results=[],
        execution=ExecutionSummary(concurrency=4, tables_processed=2, tables_failed=0),
        permissions=PermissionInfo(
            grants_added=0, grants_revoked=0, catalogs_created=0, schemas_created=0
        ),
        drift=drift or DriftInfo(tables_checked=0, tables_drifted=0, actions_taken=[]),
        anomalies=anomalies or [],
    )


def _resolver(mapping: dict[str, dg.AssetKey]) -> callable:  # type: ignore[valid-type]
    """Build a key resolver from a literal table-name → AssetKey dict."""
    return lambda table: mapping.get(table)


# ---------------------------------------------------------------------------
# drift_observations
# ---------------------------------------------------------------------------


def test_drift_observations_yields_one_per_action():
    run = _build_run_result(
        drift=DriftInfo(
            tables_checked=10,
            tables_drifted=2,
            actions_taken=[
                DriftAction(table="orders", action="ALTER ADD COLUMN", reason="new col"),
                DriftAction(table="payments", action="DROP+RECREATE", reason="type change"),
            ],
        ),
    )
    resolver = _resolver(
        {
            "orders": dg.AssetKey(["fivetran", "acme", "orders"]),
            "payments": dg.AssetKey(["fivetran", "acme", "payments"]),
        }
    )

    obs = list(drift_observations(run, key_resolver=resolver))

    assert len(obs) == 2
    assert all(isinstance(o, dg.AssetObservation) for o in obs)
    actions = [o.metadata["rocky/drift_action"].value for o in obs]
    assert "ALTER ADD COLUMN" in actions
    assert "DROP+RECREATE" in actions
    # Top-level drift counts are stamped onto every observation
    assert obs[0].metadata["rocky/drift_tables_checked"].value == 10
    assert obs[0].metadata["rocky/drift_tables_drifted"].value == 2


def test_drift_observations_skips_unresolved_tables():
    run = _build_run_result(
        drift=DriftInfo(
            tables_checked=2,
            tables_drifted=2,
            actions_taken=[
                DriftAction(table="orders", action="ALTER", reason="x"),
                DriftAction(table="unknown", action="ALTER", reason="y"),
            ],
        ),
    )
    resolver = _resolver({"orders": dg.AssetKey(["fivetran", "acme", "orders"])})

    obs = list(drift_observations(run, key_resolver=resolver))

    assert len(obs) == 1
    assert obs[0].asset_key == dg.AssetKey(["fivetran", "acme", "orders"])


def test_drift_observations_empty_when_no_actions():
    run = _build_run_result()
    obs = list(drift_observations(run, key_resolver=_resolver({})))
    assert obs == []


# ---------------------------------------------------------------------------
# anomaly_check_results
# ---------------------------------------------------------------------------


def test_anomaly_check_results_yields_warn_severity():
    run = _build_run_result(
        anomalies=[
            AnomalyResult(
                table="orders",
                current_count=900,
                baseline_avg=1500.0,
                deviation_pct=40.0,
                reason="row count below baseline by 40%",
            )
        ]
    )
    resolver = _resolver({"orders": dg.AssetKey(["fivetran", "acme", "orders"])})

    results = list(anomaly_check_results(run, key_resolver=resolver))

    assert len(results) == 1
    r = results[0]
    assert isinstance(r, dg.AssetCheckResult)
    assert r.check_name == ANOMALY_CHECK_NAME
    assert r.passed is False
    assert r.severity == dg.AssetCheckSeverity.WARN
    assert r.metadata["rocky/current_count"].value == 900
    assert r.metadata["rocky/baseline_avg"].value == 1500.0
    assert r.metadata["rocky/deviation_pct"].value == 40.0
    assert "below baseline" in r.metadata["rocky/reason"].value


def test_anomaly_check_results_skips_unresolved_tables():
    run = _build_run_result(
        anomalies=[
            AnomalyResult(
                table="ghost",
                current_count=0,
                baseline_avg=1.0,
                deviation_pct=100.0,
                reason="missing",
            )
        ]
    )
    results = list(anomaly_check_results(run, key_resolver=_resolver({})))
    assert results == []


def test_anomaly_check_results_empty_when_no_anomalies():
    run = _build_run_result()
    results = list(anomaly_check_results(run, key_resolver=_resolver({})))
    assert results == []


# ---------------------------------------------------------------------------
# optimize_metadata_for_keys
# ---------------------------------------------------------------------------


def _optimize_with(*recs: MaterializationCost) -> OptimizeResult:
    return OptimizeResult(
        version="0.3.0",
        command="optimize",
        recommendations=list(recs),
        total_models_analyzed=len(recs),
    )


def test_optimize_metadata_for_keys_maps_recommendations_to_keys():
    optimize = _optimize_with(
        MaterializationCost(
            model_name="fct_orders",
            current_strategy="full_refresh",
            compute_cost_per_run=12.0,
            storage_cost_per_month=3.0,
            downstream_references=4,
            recommended_strategy="incremental",
            estimated_monthly_savings=180.0,
            reasoning="run frequency × dataset size makes incremental cheaper",
        ),
    )
    model_to_key = {"fct_orders": dg.AssetKey(["acme", "marts", "fct_orders"])}

    result = optimize_metadata_for_keys(optimize, model_to_key=model_to_key)

    assert len(result) == 1
    metadata = result[dg.AssetKey(["acme", "marts", "fct_orders"])]
    assert metadata["rocky/current_strategy"].value == "full_refresh"
    assert metadata["rocky/recommended_strategy"].value == "incremental"
    assert metadata["rocky/estimated_monthly_savings"].value == 180.0
    assert "incremental cheaper" in metadata["rocky/optimize_reasoning"].value


def test_optimize_metadata_for_keys_skips_models_without_keys():
    optimize = _optimize_with(
        MaterializationCost(
            model_name="fct_orders",
            current_strategy="full_refresh",
            compute_cost_per_run=1.0,
            storage_cost_per_month=1.0,
            downstream_references=0,
            recommended_strategy="incremental",
            estimated_monthly_savings=10.0,
            reasoning="x",
        ),
        MaterializationCost(
            model_name="dim_users",
            current_strategy="incremental",
            compute_cost_per_run=2.0,
            storage_cost_per_month=2.0,
            downstream_references=0,
            recommended_strategy="full_refresh",
            estimated_monthly_savings=5.0,
            reasoning="y",
        ),
    )
    # Only fct_orders has a key — dim_users should be silently dropped
    result = optimize_metadata_for_keys(
        optimize,
        model_to_key={"fct_orders": dg.AssetKey(["fct_orders"])},
    )

    assert len(result) == 1
    assert dg.AssetKey(["fct_orders"]) in result
    assert dg.AssetKey(["dim_users"]) not in result


def test_optimize_metadata_for_keys_empty_when_no_recommendations():
    optimize = _optimize_with()
    result = optimize_metadata_for_keys(
        optimize, model_to_key={"fct_orders": dg.AssetKey(["fct_orders"])}
    )
    assert result == {}


# ---------------------------------------------------------------------------
# compliance_check_results
# ---------------------------------------------------------------------------


def test_compliance_check_results_parses_scenario(compliance_json: str):
    """Scenario dict parses cleanly into ComplianceOutput (parse-guard)."""
    output = ComplianceOutput.model_validate_json(compliance_json)
    assert output.command == "compliance"
    assert len(output.exceptions) == 2
    assert output.summary.total_exceptions == 2
    assert output.summary.total_masked == 2


def test_compliance_check_results_aggregates_per_asset(compliance_json: str):
    """Multiple exceptions on the same asset collapse to one WARN result.

    Dagster rejects duplicate ``(asset_key, check_name)`` pairs per
    materialization. The scenario has two ``payments.ssn`` exceptions
    (one per env); both resolve to the same payments asset key and
    must fold into a single aggregated check result.
    """
    output = ComplianceOutput.model_validate_json(compliance_json)
    payments_key = dg.AssetKey(["fivetran", "acme", "payments"])
    resolver = _resolver({"payments": payments_key})

    results = list(compliance_check_results(output, key_resolver=resolver))

    assert len(results) == 1
    r = results[0]
    assert isinstance(r, dg.AssetCheckResult)
    assert r.check_name == COMPLIANCE_CHECK_NAME
    assert r.passed is False
    assert r.severity == dg.AssetCheckSeverity.WARN
    assert r.asset_key == payments_key
    assert r.metadata["rocky/compliance_exception_count"].value == 2
    # Both (model, column, env) triples appear in the aggregated models string
    models = r.metadata["rocky/compliance_models"].value
    assert "payments.ssn (default)" in models
    assert "payments.ssn (prod)" in models
    # Summary counters are stamped on the aggregate
    assert r.metadata["rocky/compliance_total_classified"].value == 4
    assert r.metadata["rocky/compliance_total_exceptions"].value == 2
    assert r.metadata["rocky/compliance_total_masked"].value == 2


def test_compliance_check_results_falls_back_to_sentinel_when_unresolved(
    compliance_json: str,
):
    output = ComplianceOutput.model_validate_json(compliance_json)
    # Empty resolver → both exceptions aggregate into a single sentinel result
    results = list(compliance_check_results(output, key_resolver=_resolver({})))

    assert len(results) == 1
    r = results[0]
    assert r.asset_key == COMPLIANCE_FALLBACK_ASSET_KEY
    assert r.metadata["rocky/compliance_exception_count"].value == 2


def test_compliance_check_results_empty_when_no_exceptions():
    output = ComplianceOutput(
        command="compliance",
        version="1.16.0",
        summary={
            "total_classified": 0,
            "total_masked": 0,
            "total_exceptions": 0,
        },
        per_column=[],
        exceptions=[],
    )
    results = list(compliance_check_results(output, key_resolver=_resolver({})))
    assert results == []


# ---------------------------------------------------------------------------
# retention_observations
# ---------------------------------------------------------------------------


def test_retention_observations_parses_scenario(retention_status_json: str):
    """Scenario dict parses cleanly into RetentionStatusOutput (parse-guard)."""
    output = RetentionStatusOutput.model_validate_json(retention_status_json)
    assert output.command == "retention-status"
    assert len(output.models) == 3
    # All in v1 have warehouse_days=None — the --drift probe is a v2 follow-up
    assert all(m.warehouse_days is None for m in output.models)


def test_retention_observations_yields_one_per_model(retention_status_json: str):
    output = RetentionStatusOutput.model_validate_json(retention_status_json)
    orders_key = dg.AssetKey(["fivetran", "acme", "orders"])
    payments_key = dg.AssetKey(["fivetran", "acme", "payments"])
    revenue_key = dg.AssetKey(["marts", "revenue_summary"])
    resolver = _resolver(
        {
            "orders": orders_key,
            "payments": payments_key,
            "revenue_summary": revenue_key,
        }
    )

    obs = list(retention_observations(output, key_resolver=resolver))

    assert len(obs) == 3
    by_model = {o.metadata["rocky/retention_model"].value: o for o in obs}

    # Orders: configured 30 days, warehouse_days is None so the metadata
    # field is OMITTED (not None-valued) — the observation shape mirrors
    # the schema's "None means no data yet" convention.
    orders_obs = by_model["orders"]
    assert orders_obs.asset_key == orders_key
    assert orders_obs.metadata["rocky/retention_configured_days"].value == 30
    assert "rocky/retention_warehouse_days" not in orders_obs.metadata
    assert orders_obs.metadata["rocky/retention_in_sync"].value is True

    # Revenue_summary: no declaration — configured_days omitted, warehouse omitted
    revenue_obs = by_model["revenue_summary"]
    assert "rocky/retention_configured_days" not in revenue_obs.metadata
    assert "rocky/retention_warehouse_days" not in revenue_obs.metadata
    assert revenue_obs.metadata["rocky/retention_in_sync"].value is True


def test_retention_observations_skips_unresolved_models(retention_status_json: str):
    output = RetentionStatusOutput.model_validate_json(retention_status_json)
    # Only orders resolves — payments + revenue_summary silently dropped
    orders_key = dg.AssetKey(["fivetran", "acme", "orders"])
    resolver = _resolver({"orders": orders_key})

    obs = list(retention_observations(output, key_resolver=resolver))

    assert len(obs) == 1
    assert obs[0].asset_key == orders_key


def test_retention_observations_surfaces_warehouse_days_when_populated():
    """Forward-compatibility: once v2 --drift ships warehouse_days, the
    metadata field MUST appear with the observed value."""
    output = RetentionStatusOutput(
        command="retention-status",
        version="1.17.0",
        models=[
            {
                "model": "orders",
                "configured_days": 30,
                "warehouse_days": 60,
                "in_sync": False,
            },
        ],
    )
    orders_key = dg.AssetKey(["orders"])
    obs = list(retention_observations(output, key_resolver=_resolver({"orders": orders_key})))

    assert len(obs) == 1
    assert obs[0].metadata["rocky/retention_configured_days"].value == 30
    assert obs[0].metadata["rocky/retention_warehouse_days"].value == 60
    assert obs[0].metadata["rocky/retention_in_sync"].value is False


def test_retention_observation_name_is_exposed():
    """Constant is importable + stable (public API)."""
    assert RETENTION_OBSERVATION_NAME == "retention_drift"
