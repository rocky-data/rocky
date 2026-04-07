"""Tests for Pydantic models parsing Rocky JSON output."""

from __future__ import annotations

import json

import pytest

from dagster_rocky.types import (
    CiResult,
    ColumnLineageResult,
    CompileResult,
    DiscoverResult,
    DoctorResult,
    DriftActionKind,
    DriftDetectResult,
    HealthStatus,
    HistoryResult,
    MetricsResult,
    ModelLineageResult,
    OptimizeResult,
    PlanResult,
    RunResult,
    Severity,
    StateResult,
    TestResult,
    parse_rocky_output,
)


def test_parse_discover(discover_json: str):
    result = DiscoverResult.model_validate_json(discover_json)
    assert result.version == "0.1.0"
    assert result.command == "discover"
    assert len(result.sources) == 2

    acme = result.sources[0]
    assert acme.components["tenant"] == "acme"
    assert acme.components["region"] == "us_west"
    assert acme.components["source"] == "shopify"
    assert len(acme.tables) == 2
    assert acme.tables[0].name == "orders"
    assert acme.tables[0].row_count == 15000
    assert acme.last_sync_at is not None

    globex = result.sources[1]
    assert globex.components["tenant"] == "globex"
    assert len(globex.tables) == 3
    assert globex.tables[2].row_count is None


def test_parse_discover_multi_source_type(discover_multi_source_type_json: str):
    """DiscoverResult parses mixed source types and varying component
    hierarchies without hardcoded assumptions about fivetran's shape.

    Covers:
    - fivetran with {tenant, region, source} 3-level hierarchy
    - airbyte with {environment, connector} flat 2-level hierarchy
    - manual with {dataset} single-component hierarchy
    """
    result = DiscoverResult.model_validate_json(discover_multi_source_type_json)
    assert result.command == "discover"
    assert len(result.sources) == 3

    # All three source types present — proves the type field isn't hardcoded
    # anywhere in the parser.
    assert {s.source_type for s in result.sources} == {"fivetran", "airbyte", "manual"}

    fivetran = next(s for s in result.sources if s.source_type == "fivetran")
    assert fivetran.id == "src_fivetran_001"
    assert fivetran.components["tenant"] == "initech"
    assert fivetran.components["region"] == "us_east"
    assert fivetran.components["source"] == "postgres"
    assert len(fivetran.tables) == 2

    airbyte = next(s for s in result.sources if s.source_type == "airbyte")
    assert set(airbyte.components.keys()) == {"environment", "connector"}
    assert airbyte.components["environment"] == "prod"
    assert airbyte.components["connector"] == "stripe"

    manual = next(s for s in result.sources if s.source_type == "manual")
    assert set(manual.components.keys()) == {"dataset"}
    assert manual.components["dataset"] == "sales_leads"
    assert len(manual.tables) == 1


def test_parse_run_with_richer_metadata_fields():
    """T1.4: MaterializationMetadata mirrors the engine's new fields
    (target_table_full_name, sql_hash, column_count, compile_time_ms).
    All four are optional so older fixtures still parse."""
    payload = {
        "version": "0.3.0",
        "command": "run",
        "filter": "tenant=acme",
        "duration_ms": 5000,
        "tables_copied": 1,
        "materializations": [
            {
                "asset_key": ["fct_orders"],
                "rows_copied": 100,
                "duration_ms": 4500,
                "metadata": {
                    "strategy": "time_interval",
                    "target_table_full_name": "warehouse.marts.fct_orders",
                    "sql_hash": "abc123def4567890",
                    "column_count": 12,
                    "compile_time_ms": 250,
                },
            }
        ],
        "check_results": [],
        "permissions": {
            "grants_added": 0,
            "grants_revoked": 0,
            "catalogs_created": 0,
            "schemas_created": 0,
        },
        "drift": {"tables_checked": 0, "tables_drifted": 0, "actions_taken": []},
    }
    result = RunResult.model_validate_json(json.dumps(payload))
    mat = result.materializations[0]
    assert mat.metadata.target_table_full_name == "warehouse.marts.fct_orders"
    assert mat.metadata.sql_hash == "abc123def4567890"
    assert mat.metadata.column_count == 12
    assert mat.metadata.compile_time_ms == 250


def test_parse_run_with_partitions():
    """Hand-written PartitionInfo + PartitionSummary parse correctly with
    the partition fields populated. Backward compat: omitting them is fine."""
    payload = {
        "version": "0.3.0",
        "command": "run",
        "filter": "tenant=acme",
        "duration_ms": 5000,
        "tables_copied": 1,
        "tables_failed": 0,
        "materializations": [
            {
                "asset_key": ["fct_daily_orders"],
                "rows_copied": 1000,
                "duration_ms": 4500,
                "metadata": {"strategy": "time_interval"},
                "partition": {
                    "key": "2026-04-08",
                    "start": "2026-04-08T00:00:00+00:00",
                    "end": "2026-04-09T00:00:00+00:00",
                    "batched_with": [],
                },
            }
        ],
        "check_results": [],
        "permissions": {
            "grants_added": 0,
            "grants_revoked": 0,
            "catalogs_created": 0,
            "schemas_created": 0,
        },
        "drift": {"tables_checked": 0, "tables_drifted": 0, "actions_taken": []},
        "partition_summaries": [
            {
                "model": "fct_daily_orders",
                "partitions_planned": 1,
                "partitions_succeeded": 1,
                "partitions_failed": 0,
                "partitions_skipped": 0,
            }
        ],
    }
    result = RunResult.model_validate_json(json.dumps(payload))

    # Materialization includes partition info
    mat = result.materializations[0]
    assert mat.partition is not None
    assert mat.partition.key == "2026-04-08"
    assert mat.partition.batched_with == []

    # Run-level partition summary
    assert len(result.partition_summaries) == 1
    summary = result.partition_summaries[0]
    assert summary.model == "fct_daily_orders"
    assert summary.partitions_succeeded == 1
    assert summary.partitions_failed == 0
    assert summary.partitions_skipped == 0


def test_parse_run(run_json: str):
    result = RunResult.model_validate_json(run_json)
    assert result.version == "0.3.0"
    assert result.command == "run"
    assert result.filter == "tenant=acme"
    assert result.duration_ms == 18000
    assert result.tables_copied == 2
    assert result.tables_failed == 0

    # Materializations
    assert len(result.materializations) == 2
    mat = result.materializations[0]
    assert mat.asset_key == [
        "fivetran",
        "acme",
        "us_west",
        "shopify",
        "orders",
    ]
    assert mat.rows_copied == 150
    assert mat.metadata.strategy == "incremental"
    assert mat.metadata.watermark is not None

    mat2 = result.materializations[1]
    assert mat2.rows_copied is None
    assert mat2.metadata.strategy == "full_refresh"

    # Checks (5: row_count, column_match, freshness, null_rate, custom)
    assert len(result.check_results) == 1
    checks = result.check_results[0].checks
    assert len(checks) == 5
    assert checks[0].name == "row_count"
    assert checks[0].passed is True
    assert checks[0].source_count == 15000

    # Null rate check
    null_check = checks[3]
    assert null_check.name == "null_rate"
    assert null_check.column == "email"
    assert null_check.null_rate == 0.02
    assert null_check.threshold == 0.05

    # Custom check
    custom_check = checks[4]
    assert custom_check.name == "no_future_dates"
    assert custom_check.result_value == 0

    # Execution summary
    assert result.execution is not None
    assert result.execution.concurrency == 8
    assert result.execution.tables_processed == 2
    assert result.execution.tables_failed == 0

    # Metrics
    assert result.metrics is not None
    assert result.metrics.tables_processed == 2
    assert result.metrics.error_rate_pct == 0.0
    assert result.metrics.table_duration_p50_ms == 2700

    # Permissions
    assert result.permissions.grants_added == 3
    assert result.permissions.catalogs_created == 1

    # Drift
    assert result.drift.tables_drifted == 1
    assert len(result.drift.actions_taken) == 1
    assert result.drift.actions_taken[0].action == "drop_and_recreate"

    # Contracts
    assert result.contracts is not None
    assert result.contracts.passed is True

    # Anomalies
    assert len(result.anomalies) == 1
    assert result.anomalies[0].deviation_pct == 1.2


def test_parse_plan(plan_json: str):
    result = PlanResult.model_validate_json(plan_json)
    assert result.version == "0.1.0"
    assert result.filter == "tenant=acme"
    assert len(result.statements) == 3
    assert result.statements[0].purpose == "create_catalog"
    assert "CREATE CATALOG" in result.statements[0].sql
    assert result.statements[2].purpose == "incremental_copy"
    assert "INSERT INTO" in result.statements[2].sql


def test_parse_state(state_json: str):
    result = StateResult.model_validate_json(state_json)
    assert result.version == "0.1.0"
    assert len(result.watermarks) == 1
    assert "orders" in result.watermarks[0].table
    assert result.watermarks[0].last_value is not None


def test_parse_compile(compile_json: str):
    result = CompileResult.model_validate_json(compile_json)
    assert result.version == "0.1.0"
    assert result.command == "compile"
    assert result.models == 3
    assert result.execution_layers == 2
    assert result.has_errors is True
    assert len(result.diagnostics) == 2

    warning = result.diagnostics[0]
    assert warning.severity == Severity.warning
    assert warning.code == "W010"
    assert warning.model == "customer_orders"
    assert warning.span is None
    assert warning.suggestion is not None

    error = result.diagnostics[1]
    assert error.severity == Severity.error
    assert error.code == "E011"
    assert error.model == "revenue_summary"
    assert error.span is not None
    assert error.span.file == "models/revenue_summary.sql"
    assert error.span.line == 5


def test_parse_lineage(lineage_json: str):
    result = ModelLineageResult.model_validate_json(lineage_json)
    assert result.version == "0.1.0"
    assert result.command == "lineage"
    assert result.model == "customer_orders"
    assert len(result.columns) == 3
    assert result.columns[0].name == "customer_id"
    assert result.upstream == ["raw_orders", "raw_customers"]
    assert result.downstream == ["revenue_summary"]
    assert len(result.edges) == 3

    # Direct transform
    assert result.edges[0].source.model == "raw_orders"
    assert result.edges[0].target.column == "customer_id"
    assert result.edges[0].transform == "direct"

    # Aggregation transform (free-form string from engine)
    assert result.edges[1].transform == 'aggregation("count")'


def test_parse_column_lineage():
    data = {
        "version": "0.1.0",
        "command": "lineage",
        "model": "customer_orders",
        "column": "total_revenue",
        "trace": [
            {
                "source": {"model": "raw_orders", "column": "amount"},
                "target": {"model": "customer_orders", "column": "total_revenue"},
                "transform": 'aggregation("sum")',
            }
        ],
    }
    result = ColumnLineageResult.model_validate_json(json.dumps(data))
    assert result.column == "total_revenue"
    assert len(result.trace) == 1
    assert result.trace[0].source.column == "amount"


def test_parse_test_result(test_result_json: str):
    result = TestResult.model_validate_json(test_result_json)
    assert result.version == "0.1.0"
    assert result.command == "test"
    assert result.total == 3
    assert result.passed == 2
    assert result.failed == 1
    assert len(result.failures) == 1
    assert result.failures[0][0] == "revenue_summary"


def test_parse_ci_result(ci_json: str):
    result = CiResult.model_validate_json(ci_json)
    assert result.version == "0.1.0"
    assert result.command == "ci"
    assert result.compile_ok is True
    assert result.tests_ok is False
    assert result.models_compiled == 3
    assert result.tests_passed == 2
    assert result.tests_failed == 1
    assert result.exit_code == 1
    assert len(result.diagnostics) == 1
    assert result.diagnostics[0].severity == Severity.warning
    assert len(result.failures) == 1


def test_parse_history(history_json: str):
    result = HistoryResult.model_validate_json(history_json)
    assert result.version == "0.3.0"
    assert result.command == "history"
    assert result.count == 1
    assert len(result.runs) == 1

    run = result.runs[0]
    assert run.run_id == "run-001"
    assert run.status == "Success"
    assert run.trigger == "Manual"
    assert len(run.models_executed) == 1
    assert run.models_executed[0].model_name == "orders"
    assert run.models_executed[0].duration_ms == 1500
    assert run.models_executed[0].rows_affected == 15000
    assert run.models_executed[0].bytes_scanned == 52428800


def test_parse_metrics(metrics_json: str):
    result = MetricsResult.model_validate_json(metrics_json)
    assert result.version == "0.3.0"
    assert result.command == "metrics"
    assert result.model == "orders"
    assert result.count == 1
    assert len(result.snapshots) == 1

    snap = result.snapshots[0]
    assert snap.metrics.row_count == 15000
    assert snap.metrics.null_rates["email"] == 0.02
    assert snap.metrics.freshness_lag_seconds == 3600

    assert result.alerts is not None
    assert len(result.alerts) == 1
    assert result.alerts[0]["severity"] == "warning"


def test_parse_optimize(optimize_json: str):
    result = OptimizeResult.model_validate_json(optimize_json)
    assert result.version == "0.3.0"
    assert result.command == "optimize"
    assert result.total_models_analyzed == 2
    assert len(result.recommendations) == 2

    rec = result.recommendations[0]
    assert rec.model_name == "customer_revenue"
    assert rec.current_strategy == "table"
    assert rec.recommended_strategy == "view"
    assert rec.estimated_monthly_savings == 4.20


def test_parse_rocky_output_auto_detect(
    discover_json: str,
    run_json: str,
    plan_json: str,
    state_json: str,
    compile_json: str,
    lineage_json: str,
    test_result_json: str,
    ci_json: str,
    history_json: str,
    metrics_json: str,
    optimize_json: str,
    doctor_json: str,
    drift_json: str,
):
    assert isinstance(parse_rocky_output(discover_json), DiscoverResult)
    assert isinstance(parse_rocky_output(run_json), RunResult)
    assert isinstance(parse_rocky_output(plan_json), PlanResult)
    assert isinstance(parse_rocky_output(state_json), StateResult)
    assert isinstance(parse_rocky_output(compile_json), CompileResult)
    assert isinstance(parse_rocky_output(lineage_json), ModelLineageResult)
    assert isinstance(parse_rocky_output(test_result_json), TestResult)
    assert isinstance(parse_rocky_output(ci_json), CiResult)
    assert isinstance(parse_rocky_output(history_json), HistoryResult)
    assert isinstance(parse_rocky_output(metrics_json), MetricsResult)
    assert isinstance(parse_rocky_output(optimize_json), OptimizeResult)
    assert isinstance(parse_rocky_output(doctor_json), DoctorResult)
    assert isinstance(parse_rocky_output(drift_json), DriftDetectResult)


def test_parse_unknown_command():
    with pytest.raises(ValueError, match="Unknown Rocky command"):
        parse_rocky_output('{"command": "unknown", "version": "0.1.0"}')


def test_parse_missing_command_key():
    with pytest.raises(ValueError, match="Unknown Rocky command"):
        parse_rocky_output('{"version": "0.1.0"}')


def test_parse_drift_result(drift_json: str):
    result = DriftDetectResult.model_validate_json(drift_json)
    assert result.command == "drift"
    assert result.tables_checked == 3
    assert result.tables_drifted == 1
    assert len(result.results) == 1
    table_result = result.results[0]
    assert table_result.action == DriftActionKind.drop_and_recreate
    assert table_result.drifted_columns[0].name == "status"
    assert table_result.drifted_columns[0].source_type == "STRING"
    assert table_result.drifted_columns[0].target_type == "INT"


def test_run_result_without_optional_fields():
    """RunResult should parse even without execution/metrics (backward compat)."""
    minimal = {
        "version": "0.3.0",
        "command": "run",
        "filter": "tenant=acme",
        "duration_ms": 1000,
        "tables_copied": 1,
        "tables_failed": 0,
        "materializations": [],
        "check_results": [],
        "permissions": {
            "grants_added": 0,
            "grants_revoked": 0,
            "catalogs_created": 0,
            "schemas_created": 0,
        },
        "drift": {"tables_checked": 0, "tables_drifted": 0, "actions_taken": []},
    }
    result = RunResult.model_validate_json(json.dumps(minimal))
    assert result.execution is None
    assert result.metrics is None
    assert result.anomalies == []
    assert result.contracts is None


def test_parse_doctor(doctor_json: str):
    result = DoctorResult.model_validate_json(doctor_json)
    assert result.command == "doctor"
    assert result.overall == "healthy"
    assert len(result.checks) == 5
    assert result.checks[0].name == "config"
    assert result.checks[0].status == HealthStatus.healthy
    assert result.checks[4].status == HealthStatus.warning
    assert len(result.suggestions) == 1
