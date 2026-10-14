---
title: Scheduling Rocky Runs
description: Run Rocky assets on a schedule, on a condition, or when a source syncs
sidebar:
  order: 6
---

Dagster has three ways to start a Rocky run. `dagster-rocky` adds a helper for each one.

| | Schedule | Automation condition | Sensor |
|---|---|---|---|
| Helper | `build_rocky_schedule` | `rocky_eager_automation`, `rocky_cron_automation` | `rocky_source_sensor` |
| Fires | At fixed times, whatever upstream did | When the rule on the asset holds | When a source reports a newer `last_sync_at` |
| Use when | You want a fixed time | You want Dagster to decide, per asset | You want to react to Fivetran syncs |
| Waits for upstream | No | Yes | Reacts to upstream |
| Sets a `run_key` | No | No | Yes |

**Dedupe rule.** Pick one mechanism per asset. The mechanisms do **not** deduplicate across each other.

Only `rocky_source_sensor` sets a `run_key`. Dagster uses it to collapse repeat firings of that sensor. A schedule tick and an automation firing carry no `run_key`. If two land in the same window, each launches its own run. Rocky's incremental, [idempotent](/reference/glossary/#idempotent) execution keeps the second run cheap, but both runs execute.

## Schedules

Dagster's [`ScheduleDefinition`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.ScheduleDefinition) does the whole job. `build_rocky_schedule()` is a thin convenience. It takes the same `target=` shape as `rocky_source_sensor()`.

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

### `build_rocky_schedule(name, cron_schedule, target, ...)`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` | required | Schedule name. Must be unique within the code location. |
| `cron_schedule` | `str` | required | Standard cron string (e.g. `"0 6 * * *"`). |
| `target` | `CoercibleToAssetSelection \| AssetsDefinition` | required | Asset selection to materialize. |
| `timezone` | `str` | `"UTC"` | IANA timezone for cron evaluation. |
| `tags` | `dict[str, str] \| None` | `None` | Run tags. `rocky/schedule` is added automatically. |
| `description` | `str \| None` | `None` | Human-readable description. |
| `default_status` | `DefaultScheduleStatus` | `STOPPED` | Whether the schedule is enabled on deployment. |

Every run gets the tag `rocky/schedule=<name>`. Filter the run history on that tag to see Rocky schedules only. Your own tags merge on top. If you set `rocky/schedule=<other>` yourself, your value wins. That is deliberate.

## Automation conditions

Dagster 1.12+ decides when to materialize an asset with an
[`AutomationCondition`](https://docs.dagster.io/api/dagster/declarative-automation#dagster.AutomationCondition).
The rule sits on the asset. Nothing pushes work at it. Both helpers keep you off `AutoMaterializePolicy`, deprecated in Dagster 1.8.

### `rocky_eager_automation()`

Returns `dg.AutomationCondition.eager()`. The asset materializes whenever an upstream dependency updates. It waits for every upstream dependency to finish first. Attach it to source replication assets so they refresh when a Fivetran sync completes.

```python
import dagster as dg
from dagster_rocky import (
    RockyResource,
    load_rocky_assets,
    rocky_eager_automation,
)

rocky = RockyResource(config_path="rocky.toml")
specs = load_rocky_assets(rocky)

eager_specs = [
    spec.replace_attributes(automation_condition=rocky_eager_automation())
    for spec in specs
]

defs = dg.Definitions(assets=eager_specs, resources={"rocky": rocky})
```

### `rocky_cron_automation(cron_schedule, timezone="UTC")`

Returns `dg.AutomationCondition.on_cron(cron_schedule, timezone)`. The asset fires on the cron schedule, but only once its upstream dependencies have updated since the previous tick. A plain `ScheduleDefinition` fires regardless.

```python
from dagster_rocky import rocky_cron_automation

spec = dg.AssetSpec(
    key=dg.AssetKey(["fct_daily_orders"]),
    automation_condition=rocky_cron_automation("0 6 * * *", "America/Los_Angeles"),
)
```

## Sensors

`rocky_source_sensor()` builds a Dagster
[`SensorDefinition`](https://docs.dagster.io/api/dagster/sensors#dagster.SensorDefinition).
It polls `rocky discover` on a timer. It emits a `RunRequest` for any source whose upstream connector produced new data since the last tick. Your pipeline starts when Fivetran finishes a sync, not at the next scheduled run.

```python
import dagster as dg
from dagster_rocky import (
    RockyResource,
    load_rocky_assets,
    rocky_source_sensor,
)

rocky = RockyResource(config_path="rocky.toml")
rocky_assets = load_rocky_assets(rocky)

# The sensor resolves the resource key "rocky" at evaluation time.
fivetran_sync_sensor = rocky_source_sensor(
    target=dg.AssetSelection.assets(*[s.key for s in rocky_assets]),
    minimum_interval_seconds=300,  # poll every 5 minutes
)

defs = dg.Definitions(
    assets=rocky_assets,
    sensors=[fivetran_sync_sensor],
    resources={"rocky": rocky},
)
```

Defaults: `minimum_interval_seconds=300` (5-minute polling). `default_status=DefaultSensorStatus.STOPPED`, so the sensor ships disabled and you opt in from the Dagster UI.

### What one tick does

Each tick calls `rocky.discover()`, then decides per source what to do.

```
  sensor tick
      │
      ▼
  rocky.discover()
      │
      ├──► ids in failed_sources ──► log a warning
      │                              cursor NOT advanced
      │                              (re-evaluated next tick)
      ▼
  each healthy source:
  is last_sync_at newer than its cursor?
      │
      ├── no ───► nothing emitted
      │
      └── yes ──► backlog cap set and reached for this tag?
                      │
                      ├── yes ──► RunRequest suppressed
                      │           cursor still advances
                      │
                      └── no ───► RunRequest emitted
                                  cursor advances
```

The warning names every id the engine reported in `failed_sources`. The backlog cap is opt-in. See [Backlog cap](#backlog-cap).

### Transient discover failures

Source adapters can fail one at a time. A Fivetran 5xx or rate-limit window hits one connector. An Iceberg `list_tables` error hits one namespace. `rocky discover` reports these in `failed_sources` instead of dropping the connector from the output.

Without that signal, a transient error looks the same as a source removed upstream. A diff-based reconciler would then shrink the asset graph.

So the sensor skips the cursor advance for failed ids. A flapping connector comes back for evaluation until one of two things happens. It succeeds, and the cursor advances. Or it is removed upstream, and it drops out of both `sources` and `failed_sources`.

Healthy sources in the same tick still produce `RunRequest`s. A partial failure does not block the run.

### Granularity

Set it with `granularity=`.

| Value | Emits | Use when |
|---|---|---|
| `per_source` (default) | One `RunRequest` per triggered source id, scoped to that source's asset keys | You want one run per Fivetran connector |
| `per_group` | One `RunRequest` per Dagster group, bundling every triggered source in it | Many sources share a group (a tenant) and should run together |

### Cursor format

The cursor is JSON-encoded `{source_id: ISO 8601 timestamp}`. State is per source, so each connector advances on its own. A new source does not replay history for the others.

The sensor parses each timestamp into a Python `datetime` before it compares. It does not sort the strings. Mixed timezone offsets compare correctly.

### RunRequest tags

| Tag | Value |
|---|---|
| `rocky/source_id` (per_source) or `rocky/group` (per_group) | The triggering source or group |
| `rocky/sync_at` | The ISO timestamp that triggered the run |
| `rocky/sensor` | The emitting sensor's name (used by the backlog cap's self-scoping) |

They show in the run history, so you can see which sync triggered which run.

### Resource injection

`rocky_resource` accepts either form:

- **String key** (default `"rocky"`): Dagster resolves the resource from `context.resources` at evaluation time. Per-deployment overrides apply. Mock substitution through `dg.build_sensor_context(resources={...})` works without a wrapper. The resource does not need to exist before you build the sensor.
- **`RockyResource` instance**: the legacy form. The sensor captures it in a closure at build time. It stays supported indefinitely, but use the keyed form in new code.

```python
# String-key form (recommended)
sensor = rocky_source_sensor(target=...)

# Custom resource key
sensor = rocky_source_sensor(rocky_resource="my_rocky", target=...)

# Instance form (legacy, still supported)
sensor = rocky_source_sensor(rocky_resource=rocky, target=...)
```

### Backlog cap

Pass `backlog_cap=BacklogCap(...)` to suppress emits when too many in-flight Dagster runs already share a tag value. Use it when a hung downstream can turn into a runaway queue. Without it, one stuck run piles up dozens of fresh `RunRequest`s for the same tenant.

```python
from dagster_rocky import BacklogCap, rocky_source_sensor

sensor = rocky_source_sensor(
    target=...,
    backlog_cap=BacklogCap(
        tag_key="rocky/group",  # or "rocky/source_id" for per_source granularity
        max_in_flight=5,
    ),
)
```

Before each emit, the sensor counts the in-flight runs tagged `tag_key=<value>`. By default it counts the non-terminal statuses: `QUEUED`, `NOT_STARTED`, `STARTING`, and `STARTED`. Override that set with `BacklogCap.statuses`. If the count is at or above `max_in_flight`, the sensor suppresses the `RunRequest`.

The count covers **this sensor's own runs** only. Every `RunRequest` carries a stable `rocky/sensor=<name>` tag, and the count filters on it. A co-tagged run from an unrelated job never trips the cap. Pass `BacklogCap(scope_tags={...})` to narrow the count with extra exact-match tag filters.

**The cursor still advances on suppression.** The in-flight run picks up the latest data through Rocky's per-source state. A frozen cursor would make the failure worse. The next tick would re-detect the same sync and retry the same suppressed emit. It would never recover until the queue drained below the cap.

`BacklogCap` is opt-in. With no cap, behavior is unchanged.

### Lifecycle hooks

Three optional best-effort callbacks attach metrics, alerts, or audit logs without wrapping the sensor:

```python
from dagster_rocky import (
    EmitContext,
    FailedSourcesContext,
    SkipContext,
    rocky_source_sensor,
)

def record_emit(ec: EmitContext) -> None:
    # ec.run_request, ec.sources, ec.granularity, ec.sensor_context
    ...

def alert_failed(fc: FailedSourcesContext) -> None:
    # fc.failed_sources, fc.sensor_context
    ...

def gauge_idle(sc: SkipContext) -> None:
    # sc.reason, sc.cursor_size, sc.sensor_context
    ...

sensor = rocky_source_sensor(
    target=...,
    on_run_request_emitted=record_emit,
    on_failed_sources=alert_failed,
    on_skip=gauge_idle,
)
```

- **Best-effort.** Raised exceptions are caught and logged at WARN. A misbehaving hook never blocks an emit.
- **Observability, not policy.** Hooks fire after the sensor decides. Use `backlog_cap` for emit-time policy.
- **`on_run_request_emitted` fires per emit, after suppression.** It sees only the requests Dagster will be asked to launch.

### Custom translators

If your component uses a custom [`RockyDagsterTranslator`](/dagster/assets/#customize-with-a-translator), pass it to the sensor. The asset keys then match your assets:

```python
sensor = rocky_source_sensor(
    target=...,
    translator=MyTranslator(),
)
```

### Tenant partitions

When `RockyComponent` collapses tenants into partitions (`tenant:`), the sensor must emit partitioned `RunRequest`s. Pass `tenant_component` and `tenant_partitions_name` together. Passing only one raises at definition time. `sync_partitions_from_discover` (default `True`) adds new tenant values to the dynamic partition set. Set `enable_sensor: true` on the component and it wires this for you.
