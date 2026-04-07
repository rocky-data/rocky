---
title: Observability — Drift, Anomalies, Optimize
description: Surface Rocky's drift, anomaly, and optimization signals as native Dagster events
sidebar:
  order: 12
---

`dagster-rocky` 0.4 elevates Rocky's runtime observability signals from log
warnings to first-class Dagster primitives:

- **Schema drift** → `dg.AssetObservation` events on the asset timeline
- **Row-count anomalies** → `dg.AssetCheckResult` with severity `WARN`
- **Optimization recommendations** → `AssetSpec.metadata` (load-time)

Drift and anomaly emission is **automatic** when using `RockyComponent` —
nothing to wire up. Standalone helpers are also exported for users with
hand-rolled multi_assets.

## Drift events as `AssetObservation`

When `rocky run` detects schema drift on a source replication table, the
component yields an `AssetObservation` event for the affected asset with
metadata describing the action taken:

| Metadata key | Type | Description |
|---|---|---|
| `rocky/drift_action` | text | The DDL action (e.g. `ALTER ADD COLUMN`, `DROP+RECREATE`) |
| `rocky/drift_reason` | text | Human-readable explanation |
| `rocky/drift_table` | text | Original Rocky table identifier |
| `rocky/drift_tables_checked` | int | Total tables inspected for drift this run |
| `rocky/drift_tables_drifted` | int | Total tables that drifted this run |

Why `AssetObservation` and not `AssetCheckResult`? Drift is a *change*, not a
pass/fail — observation is the right primitive. It shows up on the asset
timeline as a discrete event without affecting check status.

## Anomalies as `AssetCheckResult` (severity WARN)

When `rocky run` detects row-count anomalies, the component yields an
`AssetCheckResult` per anomaly with check name `row_count_anomaly`:

| Metadata key | Type | Description |
|---|---|---|
| `rocky/current_count` | int | Row count from this run |
| `rocky/baseline_avg` | float | Historical baseline average |
| `rocky/deviation_pct` | float | % deviation from baseline |
| `rocky/reason` | text | Human-readable anomaly description |

The check spec is **pre-declared** at load time, so the Dagster UI shows the
`row_count_anomaly` slot on every asset before any run. For runs without
anomalies, a placeholder check result is emitted automatically.

Severity is `WARN` (not `ERROR`) because Rocky's anomaly detection is a
heuristic — a row-count deviation may be legitimate business behavior.
Callers who want to treat anomalies as hard failures can post-process the
check evaluation events.

## Standalone builders

If you don't use `RockyComponent`, the same emission logic is exposed as
pure-function builders:

```python
from dagster_rocky import (
    drift_observations,
    anomaly_check_results,
    ANOMALY_CHECK_NAME,
)

@dg.multi_asset(
    specs=[...],
    check_specs=[
        dg.AssetCheckSpec(name=ANOMALY_CHECK_NAME, asset=...),
    ],
)
def my_rocky_asset(context, rocky):
    result = rocky.run(filter="tenant=acme")

    def resolver(table_name):
        # Your own table-name → AssetKey mapping
        ...

    yield dg.MaterializeResult(...)
    yield from drift_observations(result, key_resolver=resolver)
    yield from anomaly_check_results(result, key_resolver=resolver)
```

The `key_resolver` callable bridges Rocky's table-name string identifiers
(which may be `catalog.schema.table` or just `table`) to Dagster `AssetKey`s.
The default `RockyComponent` resolver handles dotted-name fallback automatically.

## Optimization recommendations

`rocky optimize` returns per-model strategy recommendations. The
`optimize_metadata_for_keys` helper builds a `{AssetKey: metadata}` dict that
you can merge into `AssetSpec.metadata` at load time:

```python
from dagster_rocky import optimize_metadata_for_keys, RockyResource

rocky = RockyResource(config_path="rocky.toml")
optimize_result = rocky.optimize()
metadata = optimize_metadata_for_keys(
    optimize_result,
    model_to_key={
        "fct_orders": dg.AssetKey(["acme", "marts", "fct_orders"]),
    },
)
# metadata = {AssetKey([...]): {"rocky/current_strategy": ..., ...}}
```

The metadata fields are namespaced under `rocky/` and include
`current_strategy`, `recommended_strategy`, `estimated_monthly_savings`, and
`optimize_reasoning`.

## Column-level lineage

`build_column_lineage` translates a Rocky `ModelLineageResult` into a
Dagster [`TableColumnLineage`](https://docs.dagster.io/api/dagster/metadata#dagster.TableColumnLineage)
ready to attach to `MaterializeResult.metadata`:

```python
from dagster_rocky import build_column_lineage

result = rocky.lineage(target="fct_orders")
lineage = build_column_lineage(result, model_to_key={
    "stg_orders": dg.AssetKey(["staging", "stg_orders"]),
})
yield dg.MaterializeResult(metadata={"dagster/column_lineage": lineage})
```

The asset detail page renders this as an interactive column-level dependency
graph showing which upstream columns feed each output column.
