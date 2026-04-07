"""Dagster code location that orchestrates a Rocky pipeline.

This example shows two approaches:
1. RockyComponent — state-backed, no API calls on reload (recommended)
2. RockyResource — imperative, wraps CLI calls inside asset functions
"""

from __future__ import annotations

import dagster as dg
from dagster_rocky import RockyComponent, RockyResource


# ---------------------------------------------------------------------------
# Approach 1: RockyComponent (declarative, state-backed)
# ---------------------------------------------------------------------------
# RockyComponent discovers assets from cached state. On code location reload,
# it reads from a local state file — no API calls, no subprocess.
#
# To refresh state (e.g., in a scheduled job):
#   component = RockyComponent(
#       binary_path="rocky",
#       config_path="rocky.toml",
#       state_path=".rocky-state.redb",
#   )
#   component.write_state_to_path()


# ---------------------------------------------------------------------------
# Approach 2: RockyResource (imperative, inside asset functions)
# ---------------------------------------------------------------------------
rocky = RockyResource(
    binary_path="rocky",
    config_path="rocky.toml",
    state_path=".rocky-state.redb",
)


@dg.asset(
    group_name="rocky_orders",
    kinds={"rocky", "duckdb"},
)
def stg_orders(rocky: RockyResource) -> dg.MaterializeResult:
    """Run the staging orders model via Rocky."""
    result = rocky.run(filter="model=stg_orders")
    return dg.MaterializeResult(
        metadata={
            "tables_copied": result.tables_copied,
            "rows_processed": result.rows_processed,
        }
    )


@dg.asset(
    group_name="rocky_orders",
    kinds={"rocky", "duckdb"},
    deps=[stg_orders],
)
def fct_order_summary(rocky: RockyResource) -> dg.MaterializeResult:
    """Run the order summary fact model via Rocky."""
    result = rocky.run(filter="model=fct_order_summary")
    return dg.MaterializeResult(
        metadata={
            "tables_copied": result.tables_copied,
            "rows_processed": result.rows_processed,
        }
    )


# ---------------------------------------------------------------------------
# Definitions
# ---------------------------------------------------------------------------
defs = dg.Definitions(
    assets=[stg_orders, fct_order_summary],
    resources={"rocky": rocky},
)
