---
title: Partitions
description: Map Rocky time_interval models to Dagster partitioned assets
sidebar:
  order: 13
---

A [partition](/reference/glossary/#partition) is one time slice of a
model, such as a single day. Rocky's `time_interval` materialization
strategy says a model is partitioned by a time column at a fixed
granularity: `hour`, `day`, `month`, or `year`.

`dagster-rocky` ships `partitions.py`. It converts those strategies into
Dagster's
[`PartitionsDefinition`](https://docs.dagster.io/api/dagster/partitions#dagster.PartitionsDefinition)
variants. The translation happens twice, once when Dagster loads your
definitions and once on every run:

```
  LOAD TIME
  rocky compile ──► ModelDetail ──► partitions_def_for_model_detail()
                                              │
                                              ▼
                                    PartitionsDefinition
                                              │ passed as partitions_def=
                                              ▼
                                      your Dagster asset

  RUN TIME
  context.partition_key ──► dagster_to_rocky_partition_key()
      "2026-04-08-13:00"              │
                                      ▼
                             "2026-04-08T13" ──► rocky run --partition <key>
```

`RockyComponent` can do both steps for you, but not by default. Both
`surface_derived_models` and `dag_mode` default to `False`. Turn either
one on. The component then groups assets by partitioning shape, and
threads each Dagster partition key into a fused
`rocky run --partition <key>`.

The diagram above traces the compile-driven path. `dag_mode` takes a
different route to the same builders. It reads each node's partition
shape from `rocky dag` output, not from `rocky compile`.

Reach for the standalone helpers below when you build partitioned assets
by hand.

## Strategy mapping

| Rocky grain | Dagster `PartitionsDefinition` |
|---|---|
| `hour` | `HourlyPartitionsDefinition` |
| `day` | `DailyPartitionsDefinition` |
| `month` | `MonthlyPartitionsDefinition` |
| `year` | `TimeWindowPartitionsDefinition` (yearly cron, no first-class class) |

## API

### `partitions_def_for_time_interval(granularity, first_partition, ...)`

A pure builder. It takes a granularity and a start date, then returns the
matching `PartitionsDefinition`.

```python
from dagster_rocky import partitions_def_for_time_interval

pdef = partitions_def_for_time_interval(
    granularity="day",
    first_partition="2026-01-01",
    timezone="UTC",
)
# pdef is a DailyPartitionsDefinition starting 2026-01-01 UTC
```

### `partitions_def_for_model_detail(model)`

The higher-level builder. It takes a `ModelDetail` from
`CompileResult.models_detail`. When the strategy discriminator says
`time_interval`, it calls the pure builder above.

It returns `None` for every other strategy: `full_refresh`,
`incremental`, `merge`, `ephemeral`, `delete_insert`, and `view`. It also
returns `None` for `microbatch` today, even though that strategy is
time-based.

```python
from dagster_rocky import partitions_def_for_model_detail, RockyResource

rocky = RockyResource(config_path="rocky.toml", models_dir="models")
compile_result = rocky.compile()

for model in compile_result.models_detail:
    pdef = partitions_def_for_model_detail(model)
    if pdef is not None:
        # Build a partitioned asset for this model
        ...
```

## Format conversion

Rocky and Dagster use **different canonical key formats** for hourly and
monthly grains:

| Grain | Rocky canonical | Dagster canonical |
|---|---|---|
| `hour` | `2026-04-08T13` | `2026-04-08-13:00` |
| `day` | `2026-04-08` | `2026-04-08` |
| `month` | `2026-04` | `2026-04-01` |
| `year` | `2026` | `2026` |

Day and year are wire-compatible. Hour and month need translation. Two
helpers handle the conversion in both directions:

```python
from dagster_rocky import (
    rocky_to_dagster_partition_key,
    dagster_to_rocky_partition_key,
)

# Rocky → Dagster (e.g. building a Dagster cursor from a Rocky run output)
dagster_key = rocky_to_dagster_partition_key("hour", "2026-04-08T13")
# "2026-04-08-13:00"

# Dagster → Rocky (e.g. threading a Dagster partition key into rocky run)
rocky_key = dagster_to_rocky_partition_key("hour", "2026-04-08-13:00")
# "2026-04-08T13"
```

Round-tripping a key through both helpers is
[idempotent](/reference/glossary/#idempotent) for all grains.

## CLI argument builders

Two convenience helpers build the `rocky run` argument list for partition
execution:

```python
from dagster_rocky import partition_key_arg, partition_range_args

# Single-partition execution
args = partition_key_arg("2026-04-08")
# ["--partition", "2026-04-08"]

# Backfill range execution (BackfillPolicy.single_run())
args = partition_range_args("2026-04-01", "2026-04-08")
# ["--from", "2026-04-01", "--to", "2026-04-08"]
```

Both return `[]` when their inputs are `None`. A caller can therefore
splat them into the CLI argument list without checking first.

## End-to-end example

```python
import dagster as dg
from dagster_rocky import (
    RockyResource,
    partitions_def_for_model_detail,
    dagster_to_rocky_partition_key,
)

rocky = RockyResource(config_path="rocky.toml", models_dir="models")
compile_result = rocky.compile()

# Find the time_interval model
fct_daily_orders = next(
    m for m in compile_result.models_detail if m.name == "fct_daily_orders"
)
pdef = partitions_def_for_model_detail(fct_daily_orders)
assert pdef is not None  # it's a DailyPartitionsDefinition

@dg.asset(
    key=dg.AssetKey(["fct_daily_orders"]),
    partitions_def=pdef,
)
def fct_daily_orders_asset(
    context: dg.AssetExecutionContext,
    rocky: RockyResource,
):
    rocky_key = dagster_to_rocky_partition_key("day", context.partition_key)
    result = rocky.run(
        filter="layer=marts",
        partition=rocky_key,   # threads through to `rocky run --partition <key>`
    )
    return result.tables_copied
```
