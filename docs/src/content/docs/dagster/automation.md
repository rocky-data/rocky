---
title: Automation Conditions
description: Declarative automation for Rocky-managed assets
sidebar:
  order: 14
---

Dagster 1.12+ uses
[`AutomationCondition`](https://docs.dagster.io/api/dagster/declarative-automation#dagster.AutomationCondition)
for asset-centric declarative automation. `dagster-rocky` ships two
helpers that name the canonical Rocky-side mappings so you don't have to dig
through Dagster docs.

## `rocky_eager_automation()`

Returns `dg.AutomationCondition.eager()`, the modern 1.12+ replacement for
the deprecated `AutoMaterializePolicy.eager()`. The asset materializes
whenever any upstream dependency updates, waiting for all upstream deps to
finish first.

Use case: attach to source replication assets so they auto-refresh as soon
as an upstream Fivetran sync completes.

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

Returns `dg.AutomationCondition.on_cron(cron_schedule, timezone)`, which fires on
the cron schedule, but only after upstream dependencies have updated since
the previous tick.

Unlike a plain `ScheduleDefinition`, it waits for fresh upstream data before
firing.

```python
from dagster_rocky import rocky_cron_automation

spec = dg.AssetSpec(
    key=dg.AssetKey(["fct_daily_orders"]),
    automation_condition=rocky_cron_automation("0 6 * * *", "America/Los_Angeles"),
)
```

## Why dedicated helpers?

The helpers are tiny on purpose: they document the canonical
`AutomationCondition` mapping in one place, keep callers off the
deprecated `AutoMaterializePolicy` (deprecated in Dagster 1.8), and give
a stable Rocky-side import that won't shift if the mapping evolves.

## Sensor + Schedule + AutomationCondition: which to use?

| Approach | Use when |
|---|---|
| `rocky_source_sensor` | You want to react to specific Fivetran sync events with custom polling logic. |
| `build_rocky_schedule` | You want fixed time-based execution regardless of upstream state. |
| `rocky_eager_automation` | You want Dagster to auto-materialize whenever upstreams update; least imperative, most declarative. |
| `rocky_cron_automation` | You want scheduled execution gated on upstream freshness. |

The four can coexist on the same asset graph, but they do **not** deduplicate
across mechanisms. Only `rocky_source_sensor` sets a `run_key` on its own
`RunRequest`s, so Dagster collapses repeat firings of that sensor. A
`build_rocky_schedule` tick and an `AutomationCondition` firing carry no
`run_key`, so a schedule and an eager/cron condition landing in the same window
each launch their own run. Pick one automation mechanism per asset rather than
relying on cross-mechanism dedup.
