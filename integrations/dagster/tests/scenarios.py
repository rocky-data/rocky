"""Hand-crafted scenario data for the dagster Pydantic parsing tests.

These dicts replace the legacy ``tests/fixtures/*.json`` files. They encode
*scenario coverage* — deliberately rich variants of every Rocky output
field — that the playground POC's happy path doesn't naturally produce.
The live-binary captures live under ``tests/fixtures_generated/`` and are
exercised separately by ``test_generated_fixtures.py``.

Each scenario is a plain Python dict that mirrors the JSON shape Rocky
emits. ``conftest.py`` exposes them as JSON-string pytest fixtures via
``json.dumps``, so the existing test functions (which call
``Model.model_validate_json``) keep working unchanged.

To add a new scenario:
    1. Add a dict to this module with a clear name.
    2. Add a fixture function in ``conftest.py`` that returns
       ``json.dumps(scenarios.MY_SCENARIO)``.
    3. Use the fixture in your test like any other.

If you want to validate the scenario directly (without round-tripping
through a string), use ``Model.model_validate(scenarios.MY_SCENARIO)``.
"""

from __future__ import annotations

from typing import Any

# ---------------------------------------------------------------------------
# discover — two sources, one with multi-component fivetran schema
# ---------------------------------------------------------------------------

DISCOVER: dict[str, Any] = {
    "version": "0.1.0",
    "command": "discover",
    "checks": {
        "freshness": {"threshold_seconds": 86400},
    },
    "sources": [
        {
            "id": "src_001",
            "components": {
                "tenant": "acme",
                "region": "us_west",
                "source": "shopify",
            },
            "source_type": "fivetran",
            "last_sync_at": "2026-03-29T10:00:00Z",
            "tables": [
                {"name": "orders", "row_count": 15000},
                {"name": "payments", "row_count": 42000},
            ],
        },
        {
            "id": "src_002",
            "components": {
                "tenant": "globex",
                "region": "eu_central",
                "source": "stripe",
            },
            "source_type": "fivetran",
            "last_sync_at": "2026-03-29T12:00:00Z",
            "tables": [
                {"name": "invoices"},
                {"name": "refunds"},
                {"name": "customers"},
            ],
        },
    ],
}

# ---------------------------------------------------------------------------
# discover — multi-source-type coverage
#
# Exercises the generic shape of DiscoverResult: the engine may emit
# sources from any adapter (fivetran, airbyte, manual-declared, …) with
# any component hierarchy. The default RockyDagsterTranslator must
# behave sensibly for all of these, not just the 3-level
# {tenant, region, source} fivetran shape. See test_translator.py for
# the parametrized assertions that consume this scenario.
# ---------------------------------------------------------------------------

DISCOVER_MULTI_SOURCE_TYPE: dict[str, Any] = {
    "version": "0.1.0",
    "command": "discover",
    "sources": [
        # Fivetran with a 3-level hierarchical component layout (similar
        # shape to DISCOVER above but a different tenant, so tests that
        # iterate over both scenarios don't collide on `acme`).
        {
            "id": "src_fivetran_001",
            "components": {
                "tenant": "initech",
                "region": "us_east",
                "source": "postgres",
            },
            "source_type": "fivetran",
            "last_sync_at": "2026-04-01T10:00:00Z",
            "tables": [
                {"name": "customers", "row_count": 8500},
                {"name": "orders", "row_count": 42000},
            ],
        },
        # Airbyte with a flat 2-level layout: the adapter doesn't model
        # tenants/regions, just an environment and a connector name.
        {
            "id": "src_airbyte_001",
            "components": {
                "environment": "prod",
                "connector": "stripe",
            },
            "source_type": "airbyte",
            "last_sync_at": "2026-04-01T11:00:00Z",
            "tables": [
                {"name": "charges", "row_count": 120000},
                {"name": "customers"},
            ],
        },
        # Manual source — smallest valid case, a single component. No
        # adapter involved; the user declared this source directly in
        # rocky.toml. Exercises the degenerate-hierarchy code paths.
        {
            "id": "src_manual_001",
            "components": {
                "dataset": "sales_leads",
            },
            "source_type": "manual",
            "last_sync_at": "2026-04-01T12:00:00Z",
            "tables": [
                {"name": "leads"},
            ],
        },
    ],
}

# ---------------------------------------------------------------------------
# run — every variant: incremental + full_refresh, all 5 check kinds,
#       drift action, anomaly, contract violation, execution + metrics
# ---------------------------------------------------------------------------

RUN: dict[str, Any] = {
    "version": "0.3.0",
    "command": "run",
    "filter": "tenant=acme",
    "duration_ms": 18000,
    "tables_copied": 2,
    "tables_failed": 0,
    "materializations": [
        {
            "asset_key": ["fivetran", "acme", "us_west", "shopify", "orders"],
            "rows_copied": 150,
            "duration_ms": 2300,
            "metadata": {
                "strategy": "incremental",
                "watermark": "2026-03-29T09:55:00Z",
            },
        },
        {
            "asset_key": ["fivetran", "acme", "us_west", "shopify", "payments"],
            "rows_copied": None,
            "duration_ms": 3100,
            "metadata": {"strategy": "full_refresh"},
        },
    ],
    "check_results": [
        {
            "asset_key": ["fivetran", "acme", "us_west", "shopify", "orders"],
            "checks": [
                {
                    "name": "row_count",
                    "passed": True,
                    "source_count": 15000,
                    "target_count": 15000,
                },
                {
                    "name": "column_match",
                    "passed": True,
                    "missing": [],
                    "extra": [],
                },
                {
                    "name": "freshness",
                    "passed": True,
                    "lag_seconds": 300,
                    "threshold_seconds": 86400,
                },
                {
                    "name": "null_rate",
                    "passed": True,
                    "column": "email",
                    "null_rate": 0.02,
                    "threshold": 0.05,
                },
                {
                    "name": "no_future_dates",
                    "passed": True,
                    "query": "SELECT COUNT(*) FROM t WHERE created_at > NOW()",
                    "result_value": 0,
                    "threshold": 0,
                },
            ],
        },
    ],
    "anomalies": [
        {
            "table": "acme_warehouse.staging__us_west__shopify.payments",
            "current_count": 42000,
            "baseline_avg": 41500.0,
            "deviation_pct": 1.2,
            "reason": "within normal range",
        },
    ],
    "execution": {
        "concurrency": 8,
        "tables_processed": 2,
        "tables_failed": 0,
    },
    "metrics": {
        "tables_processed": 2,
        "tables_failed": 0,
        "error_rate_pct": 0.0,
        "statements_executed": 4,
        "retries_attempted": 0,
        "retries_succeeded": 0,
        "anomalies_detected": 0,
        "table_duration_p50_ms": 2700,
        "table_duration_p95_ms": 3100,
        "table_duration_max_ms": 3100,
        "query_duration_p50_ms": 850,
        "query_duration_p95_ms": 1200,
        "query_duration_max_ms": 1200,
    },
    "permissions": {
        "grants_added": 3,
        "grants_revoked": 0,
        "catalogs_created": 1,
        "schemas_created": 1,
    },
    "drift": {
        "tables_checked": 2,
        "tables_drifted": 1,
        "actions_taken": [
            {
                "table": "acme_warehouse.staging__us_west__shopify.payments",
                "action": "drop_and_recreate",
                "reason": "column 'status' changed STRING → INT",
            },
        ],
    },
    "contracts": {
        "passed": True,
        "violations": [],
    },
}

# ---------------------------------------------------------------------------
# plan — three statement purposes: create_catalog, create_schema, incremental
# ---------------------------------------------------------------------------

PLAN: dict[str, Any] = {
    "version": "0.1.0",
    "command": "plan",
    "filter": "tenant=acme",
    "statements": [
        {
            "purpose": "create_catalog",
            "target": "acme_warehouse",
            "sql": "CREATE CATALOG IF NOT EXISTS acme_warehouse",
        },
        {
            "purpose": "create_schema",
            "target": "acme_warehouse.staging__us_west__shopify",
            "sql": ("CREATE SCHEMA IF NOT EXISTS acme_warehouse.staging__us_west__shopify"),
        },
        {
            "purpose": "incremental_copy",
            "target": "acme_warehouse.staging__us_west__shopify.orders",
            "sql": (
                "INSERT INTO acme_warehouse.staging__us_west__shopify.orders\n"
                "SELECT *\n"
                "FROM source_catalog.src__acme__us_west__shopify.orders\n"
                "WHERE _fivetran_synced > (\n"
                "    SELECT COALESCE(MAX(_fivetran_synced), TIMESTAMP '1970-01-01')\n"
                "    FROM acme_warehouse.staging__us_west__shopify.orders\n"
                ")"
            ),
        },
    ],
}

# ---------------------------------------------------------------------------
# state — single watermark
# ---------------------------------------------------------------------------

STATE: dict[str, Any] = {
    "version": "0.1.0",
    "command": "state",
    "watermarks": [
        {
            "table": "acme_warehouse.staging__us_west__shopify.orders",
            "last_value": "2026-03-29T09:55:00Z",
            "updated_at": "2026-03-29T10:01:00Z",
        },
    ],
}

# ---------------------------------------------------------------------------
# compile — diagnostics with span info, both warning + error severities
# ---------------------------------------------------------------------------

COMPILE: dict[str, Any] = {
    "version": "0.1.0",
    "command": "compile",
    "models": 3,
    "execution_layers": 2,
    "diagnostics": [
        {
            "severity": "Warning",
            "code": "W010",
            "message": "contract column 'email' not found in model output",
            "model": "customer_orders",
            "span": None,
            "suggestion": "Add 'email' to the SELECT clause or remove from contract",
        },
        {
            "severity": "Error",
            "code": "E011",
            "message": ("type mismatch: expected INT64 but got STRING for column 'order_id'"),
            "model": "revenue_summary",
            "span": {
                "file": "models/revenue_summary.sql",
                "line": 5,
                "col": 12,
            },
            "suggestion": "Add explicit CAST(order_id AS INT64)",
        },
    ],
    "has_errors": True,
}

# ---------------------------------------------------------------------------
# lineage — direct + aggregation transforms (lowercase to match engine output)
# ---------------------------------------------------------------------------

LINEAGE: dict[str, Any] = {
    "version": "0.1.0",
    "command": "lineage",
    "model": "customer_orders",
    "columns": [
        {"name": "customer_id"},
        {"name": "total_orders"},
        {"name": "total_revenue"},
    ],
    "upstream": ["raw_orders", "raw_customers"],
    "downstream": ["revenue_summary"],
    "edges": [
        {
            "source": {"model": "raw_orders", "column": "customer_id"},
            "target": {"model": "customer_orders", "column": "customer_id"},
            "transform": "direct",
        },
        {
            "source": {"model": "raw_orders", "column": "id"},
            "target": {"model": "customer_orders", "column": "total_orders"},
            "transform": 'aggregation("count")',
        },
        {
            "source": {"model": "raw_orders", "column": "amount"},
            "target": {"model": "customer_orders", "column": "total_revenue"},
            "transform": 'aggregation("sum")',
        },
    ],
}

# ---------------------------------------------------------------------------
# test — failing test with named (model, message) failure tuple
# ---------------------------------------------------------------------------

TEST_RESULT: dict[str, Any] = {
    "version": "0.1.0",
    "command": "test",
    "total": 3,
    "passed": 2,
    "failed": 1,
    "failures": [
        ["revenue_summary", "type mismatch: expected INT64 but got STRING for column 'order_id'"],
    ],
}

# ---------------------------------------------------------------------------
# ci — compile success but test failure (mixed exit code)
# ---------------------------------------------------------------------------

CI: dict[str, Any] = {
    "version": "0.1.0",
    "command": "ci",
    "compile_ok": True,
    "tests_ok": False,
    "models_compiled": 3,
    "tests_passed": 2,
    "tests_failed": 1,
    "exit_code": 1,
    "diagnostics": [
        {
            "severity": "Warning",
            "code": "W010",
            "message": "contract column 'email' not found in model output",
            "model": "customer_orders",
            "span": None,
            "suggestion": None,
        },
    ],
    "failures": [
        ["revenue_summary", "DuckDB execution error: column 'order_id' type mismatch"],
    ],
}

# ---------------------------------------------------------------------------
# history — one completed run with bytes_scanned/written
# ---------------------------------------------------------------------------

# Shape mirrors `rocky history --json` CLI output (``RunHistoryRecord``),
# not the state-store ``RunRecord``. Fields trimmed to the projection the
# CLI actually emits (no ``finished_at`` / ``config_hash`` / per-model
# byte columns — those live in the state store, not the CLI surface).
HISTORY: dict[str, Any] = {
    "version": "0.3.0",
    "command": "history",
    "runs": [
        {
            "run_id": "run-001",
            "started_at": "2026-04-01T10:00:00Z",
            "status": "Success",
            "trigger": "Manual",
            "models_executed": 1,
            "duration_ms": 300000,
            "models": [
                {
                    "model_name": "orders",
                    "duration_ms": 1500,
                    "rows_affected": 15000,
                    "status": "success",
                },
            ],
        },
    ],
    "count": 1,
}

# ---------------------------------------------------------------------------
# metrics — snapshot with null_rates + freshness, plus a triggered alert
# ---------------------------------------------------------------------------

METRICS: dict[str, Any] = {
    "version": "0.3.0",
    "command": "metrics",
    "model": "orders",
    "snapshots": [
        {
            "timestamp": "2026-04-01T10:05:00Z",
            "run_id": "run-001",
            "model_name": "orders",
            "metrics": {
                "row_count": 15000,
                "null_rates": {
                    "email": 0.02,
                    "phone": 0.15,
                },
                "freshness_lag_seconds": 3600,
            },
        },
    ],
    "count": 1,
    "alerts": [
        {
            "type": "null_rate",
            "severity": "warning",
            "column": "phone",
            "message": "null rate 15.0% exceeds 20% threshold",
            "run_id": "run-001",
        },
    ],
}

# ---------------------------------------------------------------------------
# optimize — two recommendations: table→view (savings) and view→table (cost)
# ---------------------------------------------------------------------------

OPTIMIZE: dict[str, Any] = {
    "version": "0.3.0",
    "command": "optimize",
    "recommendations": [
        {
            "model_name": "customer_revenue",
            "current_strategy": "table",
            "compute_cost_per_run": 0.0016,
            "storage_cost_per_month": 4.60,
            "downstream_references": 1,
            "recommended_strategy": "view",
            "estimated_monthly_savings": 4.20,
            "reasoning": "Cheap to compute (800ms), queried 2x/day",
        },
        {
            "model_name": "raw_events",
            "current_strategy": "view",
            "compute_cost_per_run": 0.09,
            "storage_cost_per_month": 2.30,
            "downstream_references": 8,
            "recommended_strategy": "table",
            "estimated_monthly_savings": -12.30,
            "reasoning": "Expensive (45s), 8 downstream consumers",
        },
    ],
    "total_models_analyzed": 2,
}

# ---------------------------------------------------------------------------
# doctor — overall healthy with one warning + suggestion
# ---------------------------------------------------------------------------

DOCTOR: dict[str, Any] = {
    "command": "doctor",
    "overall": "healthy",
    "checks": [
        {
            "name": "config",
            "status": "healthy",
            "message": "Config syntax valid",
            "duration_ms": 5,
        },
        {
            "name": "state",
            "status": "healthy",
            "message": "State store healthy (12 watermarks)",
            "duration_ms": 3,
        },
        {
            "name": "adapters",
            "status": "healthy",
            "message": "2 adapter(s) configured",
            "duration_ms": 1,
        },
        {
            "name": "pipelines",
            "status": "healthy",
            "message": "1 pipeline(s) valid",
            "duration_ms": 2,
        },
        {
            "name": "state_sync",
            "status": "warning",
            "message": "State backend: local",
            "duration_ms": 0,
        },
    ],
    "suggestions": [
        "Consider using 'tiered' state backend for distributed execution",
    ],
}

# ---------------------------------------------------------------------------
# drift — one drifted column with type change, drop_and_recreate action
# ---------------------------------------------------------------------------

DRIFT: dict[str, Any] = {
    "command": "drift",
    "tables_checked": 3,
    "tables_drifted": 1,
    "results": [
        {
            "table": "acme_warehouse.staging__us_west__shopify.payments",
            "drifted_columns": [
                {
                    "name": "status",
                    "source_type": "STRING",
                    "target_type": "INT",
                },
            ],
            "action": "DropAndRecreate",
        },
    ],
}
