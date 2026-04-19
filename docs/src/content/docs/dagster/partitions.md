---
title: Partitions
description: Map Rocky time_interval models to Dagster partitioned assets
sidebar:
  order: 13
---

Rocky's `time_interval` materialization strategy declares a model is
partitioned by a time column with a fixed granularity (`hour`, `day`, `month`,
or `year`). `dagster-rocky` 0.4 ships `partitions.py` — a translation layer
that converts these strategies into Dagster's
[`PartitionsDefinition`](https://docs.dagster.io/api/dagster/partitions#dagster.PartitionsDefinition)
variants.

These are **standalone helpers** today. The `RockyComponent`-side wiring
(group splitting by partitioning shape, threading partition keys through
`rocky run --partition`) is a follow-up that depends on derived-model
surfacing — currently the component emits source-replication tables only,
none of which use `time_interval`.

## Strategy mapping

| Rocky grain | Dagster `PartitionsDefinition` |
|---|---|
| `hour` | `HourlyPartitionsDefinition` |
| `day` | `DailyPartitionsDefinition` |
| `month` | `MonthlyPartitionsDefinition` |
| `year` | `TimeWindowPartitionsDefinition` (yearly cron, no first-class class) |

## API

### `partitions_def_for_time_interval(granularity, first_partition, ...)`

Pure builder. Takes a granularity + start date and returns the matching
`PartitionsDefinition`.

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

Higher-level: takes a `ModelDetail` (from `CompileResult.models_detail`) and
dispatches to the time_interval builder when the strategy discriminator
matches. Returns `None` for `full_refresh`, `incremental`, or `merge`.

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

Round-tripping a key through both helpers is idempotent for all grains.

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

Both return `[]` when their inputs are `None` so callers can unconditionally
splat them into the CLI argument list.

## End-to-end example

```python
import dagster as dg
from datetime import timedelta
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
