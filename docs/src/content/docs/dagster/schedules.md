---
title: Schedules
description: Cron-driven scheduled materialization of Rocky assets
sidebar:
  order: 11
---

You do not need Rocky-specific glue to schedule a Rocky run. Dagster's
[`ScheduleDefinition`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.ScheduleDefinition)
already does the whole job.

`build_rocky_schedule()` is a thin convenience on top. It sets a few
defaults and takes the same `target=` shape as `rocky_source_sensor()`.

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

Every schedule from `build_rocky_schedule` tags the runs it triggers with
`rocky/schedule=<name>`. Filter the run history view on that tag to see
only Rocky-driven schedules.

Your own tags merge on top of the namespace tag. If you set
`rocky/schedule=<other>` yourself, your value wins. That is deliberate.
Some users want their own schedule-tag values for grouping.

## Pairing with sensors

Schedules and sensors complement each other:

- **Schedules** fire at fixed times regardless of upstream state, useful for
  reports that should run every morning.
- **Sensors** fire when upstream state changes, useful for pipelines that
  should kick off as soon as Fivetran completes a sync.

Both can target the same asset selection. If they do, expect two runs.

Dagster deduplicates on `run_key` per instigator. A sensor's `run_key`
only stops that same sensor from re-emitting the same `(source, sync)`
pair on a later tick. `build_rocky_schedule` sets no `run_key` at all. So
a schedule that fires at the same time as a sensor, on the same
selection, launches two runs. Dagster does not dedupe across a schedule
and a sensor.

Rocky's execution is incremental and
[idempotent](/reference/glossary/#idempotent), which keeps the redundant
run cheap. Both runs still execute.
