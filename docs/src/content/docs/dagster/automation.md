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

Returns `dg.AutomationCondition.eager()` — the modern 1.12+ replacement for
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

Returns `dg.AutomationCondition.on_cron(cron_schedule, timezone)` — fires on
the cron schedule, but only after upstream dependencies have updated since
the previous tick.

This is more intelligent than a plain `ScheduleDefinition` because it waits
for fresh upstream data before firing.

```python
from dagster_rocky import rocky_cron_automation

spec = dg.AssetSpec(
    key=dg.AssetKey(["fct_daily_orders"]),
    automation_condition=rocky_cron_automation("0 6 * * *", "America/Los_Angeles"),
)
```

## Why dedicated helpers?

The helpers are intentionally tiny — they exist to:

1. **Document the canonical mapping** in one place, so users don't need to
   know which Dagster condition variant is canonical for Rocky-managed assets.
2. **Avoid the deprecated API** entirely. Dagster's `AutoMaterializePolicy`
   was deprecated in 1.8 in favor of `AutomationCondition`; both still exist
   but the new shape is the path forward.
3. **Stable Rocky-side names** that callers can import without coupling to
   the underlying Dagster condition shape, in case the canonical mapping
   evolves between Rocky releases.

## Sensor + Schedule + AutomationCondition: which to use?

| Approach | Use when |
|---|---|
| `rocky_source_sensor` | You want to react to specific Fivetran sync events with custom polling logic. |
| `build_rocky_schedule` | You want fixed time-based execution regardless of upstream state. |
| `rocky_eager_automation` | You want Dagster to auto-materialize whenever upstreams update — least imperative, most declarative. |
| `rocky_cron_automation` | You want scheduled execution gated on upstream freshness. |

The four can coexist on the same asset graph. Dagster will deduplicate
materializations via `run_key` so simultaneous triggers don't double-execute.
