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

`build_rocky_schedule()` wraps Dagster's [`ScheduleDefinition`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.ScheduleDefinition). It takes the same `target=` shape as `rocky_source_sensor()`. The snippets on this page share this setup:

```python
import dagster as dg
from dagster_rocky import (
    RockyResource,
    build_rocky_schedule,
    load_rocky_assets,
    rocky_eager_automation,
    rocky_source_sensor,
)

rocky = RockyResource(config_path="rocky.toml")
rocky_assets = load_rocky_assets(rocky)

daily_marts = build_rocky_schedule(
    name="daily_marts",
    cron_schedule="0 6 * * *",  # 6 AM
    target=dg.AssetSelection.assets(*[s.key for s in rocky_assets]),
    timezone="America/Los_Angeles",
)

defs = dg.Definitions(assets=rocky_assets, schedules=[daily_marts], resources={"rocky": rocky})
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

Every run gets the tag `rocky/schedule=<name>`. Your own tags merge on top. If you set `rocky/schedule=<other>` yourself, your value wins.

## Automation conditions

Dagster 1.12+ decides when to materialize an asset with an
[`AutomationCondition`](https://docs.dagster.io/api/dagster/declarative-automation#dagster.AutomationCondition) set on the asset. Both helpers replace `AutoMaterializePolicy`, deprecated in Dagster 1.8.

### `rocky_eager_automation()`

Returns `dg.AutomationCondition.eager()`. The asset materializes whenever an upstream dependency updates, after every upstream dependency finishes. Attach it to source replication assets so they refresh when a Fivetran sync completes.

```python
eager_specs = [
    spec.replace_attributes(automation_condition=rocky_eager_automation())
    for spec in load_rocky_assets(rocky)
]
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

`rocky_source_sensor()` builds a Dagster [`SensorDefinition`](https://docs.dagster.io/api/dagster/sensors#dagster.SensorDefinition). It polls `rocky discover` and emits a `RunRequest` for any source whose connector produced new data since the last tick.

```python
# The sensor resolves the resource key "rocky" at evaluation time.
fivetran_sync_sensor = rocky_source_sensor(
    target=dg.AssetSelection.assets(*[s.key for s in rocky_assets]),
    minimum_interval_seconds=300,  # poll every 5 minutes
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

An adapter can fail one source at a time (a Fivetran 5xx or rate limit, an Iceberg `list_tables` error). `rocky discover` reports these in `failed_sources` instead of dropping the source. Without that signal, a transient error looks like a source removed upstream, and a diff-based reconciler would shrink the asset graph.

The sensor skips the cursor advance for failed ids. A flapping source comes back for evaluation until it succeeds, or until it is removed upstream and leaves both `sources` and `failed_sources`. Healthy sources in the same tick still produce `RunRequest`s.

### Granularity

Set it with `granularity=`.

| Value | Emits | Use when |
|---|---|---|
| `per_source` (default) | One `RunRequest` per triggered source id, scoped to that source's asset keys | You want one run per Fivetran connector |
| `per_group` | One `RunRequest` per Dagster group, bundling every triggered source in it | Many sources share a group (a tenant) and should run together |

### Cursor format

The cursor is JSON-encoded `{source_id: ISO 8601 timestamp}`. State is per source, so each connector advances on its own. The sensor compares parsed `datetime` values, so mixed timezone offsets compare correctly.

### RunRequest tags

| Tag | Value |
|---|---|
| `rocky/source_id` (per_source) or `rocky/group` (per_group) | The triggering source or group |
| `rocky/sync_at` | The ISO timestamp that triggered the run |
| `rocky/sensor` | The emitting sensor's name (used by the backlog cap's self-scoping) |

### Resource injection

`rocky_resource` accepts a **string key** (default `"rocky"`), which Dagster resolves from `context.resources` at evaluation time so per-deployment overrides and `dg.build_sensor_context(resources={...})` mocks work, or a **`RockyResource` instance** (legacy: captured at build time). Use the keyed form in new code: `rocky_source_sensor(rocky_resource="my_rocky", target=...)`.

### Backlog cap

Pass `backlog_cap=BacklogCap(...)` to suppress emits when too many in-flight Dagster runs already share a tag value. Use it when a hung downstream can turn into a runaway queue. It is opt-in.

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

Before each emit, the sensor counts the in-flight runs tagged `tag_key=<value>`. By default it counts `QUEUED`, `NOT_STARTED`, `STARTING` and `STARTED`. Override that set with `BacklogCap.statuses`. At or above `max_in_flight`, the sensor suppresses the `RunRequest`. The count covers **this sensor's own runs** only (the `rocky/sensor=<name>` tag). Pass `BacklogCap(scope_tags={...})` to add exact-match tag filters.

**The cursor still advances on suppression.** The in-flight run picks up the latest data through Rocky's per-source state. A frozen cursor would retry the same suppressed emit on every tick until the queue drained.

### Lifecycle hooks

Three optional best-effort callbacks attach metrics, alerts, or audit logs without wrapping the sensor. Each takes a context object: `on_run_request_emitted` gets `EmitContext` (`run_request`, `sources`, `granularity`, `sensor_context`), `on_failed_sources` gets `FailedSourcesContext` (`failed_sources`, `sensor_context`), and `on_skip` gets `SkipContext` (`reason`, `cursor_size`, `sensor_context`).

```python
sensor = rocky_source_sensor(target=..., on_run_request_emitted=record_emit,
                             on_failed_sources=alert_failed, on_skip=gauge_idle)
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
