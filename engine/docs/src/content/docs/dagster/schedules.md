---
title: Schedules
description: Cron-driven scheduled materialization of Rocky assets
sidebar:
  order: 11
---

Dagster's [`ScheduleDefinition`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.ScheduleDefinition)
is fully sufficient for scheduled Rocky runs without any Rocky-specific glue.
`dagster-rocky` 0.4 ships `build_rocky_schedule()` as a thin convenience that
bakes in sensible defaults and accepts the same `target=` shape as
`rocky_source_sensor()` for symmetry.

## Quickstart

```python
import dagster as dg
from dagster_rocky import (
    RockyResource,
    build_rocky_schedule,
    load_rocky_assets,
)

rocky = RockyResource(config_path="rocky.toml")
rocky_assets = load_rocky_assets(rocky)

daily_marts = build_rocky_schedule(
    name="daily_marts",
    cron_schedule="0 6 * * *",  # 6 AM
    target=dg.AssetSelection.assets(*[s.key for s in rocky_assets]),
    timezone="America/Los_Angeles",
)

defs = dg.Definitions(
    assets=rocky_assets,
    schedules=[daily_marts],
    resources={"rocky": rocky},
)
```

## API

### `build_rocky_schedule(name, cron_schedule, target, ...)`

Returns a `ScheduleDefinition` with sensible defaults.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` | required | Schedule name. Must be unique within the code location. |
| `cron_schedule` | `str` | required | Standard cron string (e.g. `"0 6 * * *"`). |
| `target` | `CoercibleToAssetSelection \| AssetsDefinition` | required | Asset selection to materialize. |
| `timezone` | `str` | `"UTC"` | IANA timezone for cron evaluation. |
| `tags` | `dict[str, str] \| None` | `None` | Run tags. `rocky/schedule` is added automatically. |
| `description` | `str \| None` | `None` | Human-readable description. |
| `default_status` | `DefaultScheduleStatus` | `STOPPED` | Whether the schedule is enabled on deployment. |

## Tag namespacing

Every schedule built via `build_rocky_schedule` automatically adds a
`rocky/schedule=<name>` tag to runs it triggers, so you can filter the run
history view by Rocky-driven schedules.

User-supplied tags merge on top of the namespace tag, so a user explicitly
setting `rocky/schedule=<other>` wins. This is intentional — advanced users may
want different schedule-tag values for grouping.

## Pairing with sensors

Schedules and sensors complement each other:

- **Schedules** fire at fixed times regardless of upstream state — useful for
  reports that should run every morning.
- **Sensors** fire when upstream state changes — useful for pipelines that
  should kick off as soon as Fivetran completes a sync.

Both can target the same asset selection. Dagster will deduplicate
materializations via `run_key` so simultaneous triggers don't double-execute.
