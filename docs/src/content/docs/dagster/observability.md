---
title: "Observability: Drift, Anomalies, Optimize"
description: Surface Rocky's drift, anomaly, and optimization signals as native Dagster events
sidebar:
  order: 12
---

Rocky notices things while it runs. A source column changed type. A table
came back far smaller than yesterday. A model would be cheaper under a
different strategy. `dagster-rocky` surfaces each of those in Dagster
instead of leaving it in the log. It also records what a run applied.
Where each one lands:

- **Schema drift** → `dg.AssetObservation` events on the asset timeline
- **Row-count anomalies** → `dg.AssetCheckResult` with severity `WARN`
- **Optimization recommendations** → `AssetSpec.metadata` (load-time)
- **Plan artifact trail** → `.rocky/plans/<plan-id>.json` per `run_pipes` materialization

`RockyComponent` emits drift and anomaly events for you. There is nothing
to wire up. If you hand-roll your own `multi_asset`, call the standalone
helpers described below.

## Plan artifact per materialization

A [plan](/reference/glossary/#plan) is Rocky's reviewable record of what a
run will do. Which execution mode you pick decides whether that record
survives to disk.

```
  run() and run_streaming()          run_pipes()
  ─────────────────────────          ───────────────────────────────
                                     rocky plan
                                          │ writes
                                          ▼
                                     .rocky/plans/<plan-id>.json
                                          │ read back by
                                          ▼
  rocky run  (one fused process)     rocky apply <plan-id>
        │                                  │
        ▼                                  ▼
  no plan file on disk               audit record kept
```

`RockyResource.run_pipes()` is the only mode that keeps the two-step
shape. `RockyComponent` with `execution_mode="pipes"` uses it too. Both
write the typed plan to `.rocky/plans/<plan-id>.json` before the apply
phase starts. `run()` and `run_streaming()`, the default
`execution_mode="streaming"`, spawn a single fused `rocky run` subprocess
and write no plan artifact.

The plan file records exactly what Rocky tried to apply: the
materialization list, the governance plan, and the drift actions. Compact
and archive plans also carry the typed
[IR](/reference/glossary/#ir-intermediate-representation), the graph the
apply path regenerates SQL from. See the [Plan store v1 to v2 migration
guide](/concepts/plan-store-v1-to-v2/).

`run_pipes` also attaches the plan id as `extras={"plan_id": plan_id}`.
Dagster then shows it as run metadata in the run viewer, one click from a
failed materialization back to the plan that produced it.

Engine `v1.34+` content-addresses a plan for every project shape,
including replication-only projects with no `models/` directory. So
`run_pipes` always writes a `.rocky/plans/<plan-id>.json` artifact. If the
engine emits no `plan_id`, `run_pipes` raises `dg.Failure` rather than
falling back to `rocky run`.

## Drift events as `AssetObservation`

Schema [drift](/reference/glossary/#drift) is a source table whose columns
changed shape since the last run. When `rocky run` finds drift on a
replication table, the component yields an `AssetObservation` for that
asset. The metadata describes the action Rocky took:

| Metadata key | Type | Description |
|---|---|---|
| `rocky/drift_action` | text | The DDL action (e.g. `ALTER ADD COLUMN`, `DROP+RECREATE`) |
| `rocky/drift_reason` | text | Human-readable explanation |
| `rocky/drift_table` | text | Original Rocky table identifier |
| `rocky/drift_tables_checked` | int | Total tables inspected for drift this run |
| `rocky/drift_tables_drifted` | int | Total tables that drifted this run |

Why an observation and not a check result? Drift is a change, not a pass or
a fail. The event lands on the asset timeline as its own entry and leaves
every check status alone.

## Anomalies as `AssetCheckResult` (severity WARN)

`rocky run` compares each table's row count against its historical
baseline. For every anomaly it finds, the component yields one
`AssetCheckResult` named `row_count_anomaly`:

| Metadata key | Type | Description |
|---|---|---|
| `rocky/current_count` | int | Row count from this run |
| `rocky/baseline_avg` | float | Historical baseline average |
| `rocky/deviation_pct` | float | % deviation from baseline |
| `rocky/reason` | text | Human-readable anomaly description |

The check spec is declared at load time, before anything runs. The Dagster
UI therefore shows the `row_count_anomaly` slot on every asset from the
start. A run that finds no anomalies emits a placeholder check result.

Severity is `WARN`, not `ERROR`. Rocky's anomaly detection is a heuristic,
and a row-count swing is often real business behavior. To treat an anomaly
as a hard failure, post-process the check evaluation events yourself.

## Standalone builders

If you don't use `RockyComponent`, call the same emission logic as plain
functions:

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

Rocky names a table with a plain string. That string is either
`catalog.schema.table` or a bare `table`. The `key_resolver` callable maps
it to a Dagster `AssetKey`. `RockyComponent`'s own resolver handles the
dotted form for you.

## Optimization recommendations

`rocky optimize` recommends a [materialization
strategy](/reference/glossary/#materialization-strategy) per model, based
on how the model is used. `optimize_metadata_for_keys` turns those
recommendations into an `{AssetKey: metadata}` dict. Merge it into
`AssetSpec.metadata` at load time:

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

Every metadata key sits under the `rocky/` namespace. The fields include
`current_strategy`, `recommended_strategy`, `estimated_monthly_savings`, and
`optimize_reasoning`.

## Column-level lineage

Column [lineage](/reference/glossary/#lineage) traces each output column
back to the columns it was computed from. `build_column_lineage` turns a
Rocky `ModelLineageResult` into a Dagster
[`TableColumnLineage`](https://docs.dagster.io/api/dagster/metadata#dagster.TableColumnLineage),
ready to attach to `MaterializeResult.metadata`:

```python
from dagster_rocky import build_column_lineage

result = rocky.lineage(target="fct_orders")
lineage = build_column_lineage(result, model_to_key={
    "stg_orders": dg.AssetKey(["staging", "stg_orders"]),
})
yield dg.MaterializeResult(metadata={"dagster/column_lineage": lineage})
```

The asset detail page renders this as a column-level dependency graph. It
shows which upstream columns feed each output column.
