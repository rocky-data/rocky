---
title: Automation Conditions
description: Declarative automation for Rocky-managed assets
sidebar:
  order: 14
---

Dagster 1.12+ decides when to materialize an asset with an
[`AutomationCondition`](https://docs.dagster.io/api/dagster/declarative-automation#dagster.AutomationCondition).
The condition is a rule attached to the asset itself, not a schedule that pushes
work at it. `dagster-rocky` ships two helpers that name the Rocky-side mappings,
so you do not have to pick the condition yourself.

## `rocky_eager_automation()`

Returns `dg.AutomationCondition.eager()`. This is the 1.12+ replacement for the
deprecated `AutoMaterializePolicy.eager()`. The asset materializes whenever an
upstream dependency updates, and waits for every upstream dependency to finish
first.

Attach it to source replication assets. They then refresh as soon as an upstream
Fivetran sync completes.

```python
import dagster as dg
from dagster_rocky import (
    RockyResource,
    load_rocky_assets,
    rocky_eager_automation,
)

rocky = RockyResource(config_path="rocky.toml")
specs = load_rocky_assets(rocky)

# Attach the eager condition to every Rocky asset
eager_specs = [
    spec.replace_attributes(automation_condition=rocky_eager_automation())
    for spec in specs
]

defs = dg.Definitions(assets=eager_specs, resources={"rocky": rocky})
```

## `rocky_cron_automation(cron_schedule, timezone="UTC")`

Returns `dg.AutomationCondition.on_cron(cron_schedule, timezone)`. The asset
fires on the cron schedule, but only once its upstream dependencies have updated
since the previous tick.

A plain `ScheduleDefinition` fires regardless. This one waits for fresh upstream
data.

```python
from dagster_rocky import rocky_cron_automation

spec = dg.AssetSpec(
    key=dg.AssetKey(["fct_daily_orders"]),
    automation_condition=rocky_cron_automation("0 6 * * *", "America/Los_Angeles"),
)
```

## What the helpers add

The helpers are small on purpose. They record the canonical
`AutomationCondition` mapping in one place. They keep callers off
`AutoMaterializePolicy`, deprecated in Dagster 1.8. They also give you a stable
Rocky-side import that will not move if the mapping changes.

## Sensor + Schedule + AutomationCondition: which to use?

| Approach | Use when |
|---|---|
| `rocky_source_sensor` | You want to react to specific Fivetran sync events with custom polling logic. |
| `build_rocky_schedule` | You want fixed time-based execution regardless of upstream state. |
| `rocky_eager_automation` | You want Dagster to auto-materialize whenever upstreams update; least imperative, most declarative. |
| `rocky_cron_automation` | You want scheduled execution gated on upstream freshness. |

Pick one automation mechanism per asset. The four can coexist on the same asset
graph, but they do **not** deduplicate across mechanisms.

Only `rocky_source_sensor` sets a `run_key` on its own `RunRequest`s, which is
how Dagster collapses repeat firings of that sensor. A `build_rocky_schedule`
tick and an `AutomationCondition` firing carry no `run_key`. So a schedule and
an eager or cron condition that land in the same window each launch their own
run.
