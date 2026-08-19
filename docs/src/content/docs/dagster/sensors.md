---
title: Sensors
description: Trigger Rocky materializations from Fivetran (or any source) sync events
sidebar:
  order: 10
---

`rocky_source_sensor()` builds a Dagster
[`SensorDefinition`](https://docs.dagster.io/api/dagster/sensors#dagster.SensorDefinition).
The sensor polls `rocky discover` on a timer. It emits a `RunRequest` for
any source whose upstream connector produced new data since the last tick.

Your pipeline then starts as soon as Fivetran finishes a sync, instead of
waiting for the next scheduled run.

## Quickstart

```python
import dagster as dg
from dagster_rocky import (
    RockyResource,
    load_rocky_assets,
    rocky_source_sensor,
)

rocky = RockyResource(config_path="rocky.toml")
rocky_assets = load_rocky_assets(rocky)

# Build a sensor that targets every Rocky asset.
# `rocky_resource` defaults to the resource key "rocky" — the sensor
# resolves it through Dagster's required-resource injection at evaluation
# time, so per-deployment overrides applied via `Definitions` reach the
# sensor without needing to rebuild it.
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

## What one tick does

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

The warning names every id the engine reported in `failed_sources`, so you
can see which connector is misbehaving. The backlog cap is opt-in. Both
branches under it are covered in [Backlog cap](#backlog-cap) below.

## Transient discover failures

Source adapters can fail one at a time. A Fivetran 5xx or rate-limit window hits a single connector. An Iceberg `list_tables` error hits one namespace. From engine `1.17.4` onward, `rocky discover` reports these in `failed_sources` instead of dropping the connector from the output.

That signal matters. Without it, a transient adapter error looks the same as a source that was removed upstream, and a diff-based reconciler would shrink the asset graph.

So the sensor skips the cursor advance for failed ids. A flapping connector keeps coming back for evaluation until one of two things happens. It succeeds, and the cursor advances normally. Or it is genuinely removed upstream, and it drops out of both `sources` and `failed_sources`.

Healthy sources in the same tick still produce `RunRequest`s. A partial failure does not block the run.

Requires engine `≥ 1.17.4`. Older engines omit the field, and the sensor reads an absent field as "no failures reported".

## Granularity

Two granularities are supported via the `granularity=` parameter:

### `per_source` (default)

One `RunRequest` per triggered source-id. Each request selects only the asset
keys belonging to that one source. This is the most predictable shape and
keeps each materialization scoped to a single Fivetran connector.

```python
sensor = rocky_source_sensor(
    rocky_resource=rocky,
    target=...,
    granularity="per_source",
)
```

### `per_group`

One `RunRequest` per Dagster group, bundling every triggered source in that
group together. Useful when many sources share a group (e.g. a tenant) and you
want them to materialize as a single Dagster run.

```python
sensor = rocky_source_sensor(
    rocky_resource=rocky,
    target=...,
    granularity="per_group",
)
```

## Cursor format

The cursor is JSON-encoded `{source_id: ISO 8601 timestamp}`. State is stored per source, so each connector advances on its own. Adding a new source does not replay history for the existing ones.

The sensor parses each timestamp into a Python `datetime` before it compares. It does not sort the strings. Mixed timezone offsets between the cursor and the current sync therefore compare correctly.

## RunRequest tags

Every `RunRequest` is tagged with:

- `rocky/source_id` (per_source) or `rocky/group` (per_group)
- `rocky/sync_at`: the ISO timestamp that triggered the run
- `rocky/sensor`: the emitting sensor's name (used by the backlog cap's self-scoping)

These show up in the Dagster run history view so you can audit which Fivetran
sync triggered which materialization.

## Resource injection

`rocky_resource` accepts either form:

- **String key** (default `"rocky"`): Dagster resolves the resource from `context.resources` at evaluation time. Per-deployment overrides apply. Mock substitution through `dg.build_sensor_context(resources={...})` works without a wrapper. The resource does not need to exist before you build the sensor.
- **`RockyResource` instance**: the legacy form. The sensor captures it in a closure at build time. It stays supported indefinitely so existing call sites keep working, but use the keyed form in new code.

```python
# String-key form (recommended) — resolves "rocky" from context.resources
sensor = rocky_source_sensor(target=...)

# Custom resource key
sensor = rocky_source_sensor(rocky_resource="my_rocky", target=...)

# Instance form (legacy, still supported)
sensor = rocky_source_sensor(rocky_resource=rocky, target=...)
```

## Backlog cap

Pass `backlog_cap=BacklogCap(...)` to suppress emits when too many in-flight Dagster runs already share a tag value. Use it when a hung downstream can turn into a runaway queue. Without back-pressure, one stuck run piles up dozens of fresh `RunRequest`s for the same tenant before anyone notices.

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

Before each emit, the sensor counts the in-flight runs tagged `tag_key=<value>`. It counts the non-terminal statuses, which are `QUEUED`, `NOT_STARTED`, `STARTING`, and `STARTED` by default. Override that set with `BacklogCap.statuses`. If the count is at or above `max_in_flight`, the sensor suppresses the `RunRequest`.

The count covers **this sensor's own runs** only. Every `RunRequest` it emits carries a stable `rocky/sensor=<name>` tag, and the in-flight count filters on that tag. A co-tagged run from an unrelated job therefore never trips the cap. Pass `BacklogCap(scope_tags={...})` to narrow the count further with extra exact-match tag filters.

**The cursor still advances on suppression.** The in-flight run picks up the latest data through Rocky's per-source state. Freezing the cursor would make the failure worse. The next tick would re-detect the same sync, retry the same suppressed emit, and never recover until the queue drained below the cap.

`BacklogCap` is opt-in. Default behavior (no cap) is unchanged.

## Lifecycle hooks

Three optional best-effort callbacks let you attach metrics, alerts, or audit logs without subclassing or wrapping the sensor:

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

Hook contract:

- **Best-effort.** Raised exceptions are caught and logged at WARN. A misbehaving hook never blocks an emit.
- **Observability, not policy.** Hooks fire after the sensor decides what to do. Use `backlog_cap` (above) for emit-time policy.
- **`on_run_request_emitted` fires per emit, after suppression.** It sees only the requests Dagster will be asked to launch.

## Defaults

- `minimum_interval_seconds=300`: 5-minute polling
- `default_status=DefaultSensorStatus.STOPPED`: sensor ships disabled, users opt in
  via the Dagster UI

## Custom translators

If your component uses a custom `RockyDagsterTranslator`, pass it to the sensor
so the asset keys it generates match the keys your assets use:

```python
from dagster_rocky import RockyDagsterTranslator

class MyTranslator(RockyDagsterTranslator):
    def get_asset_key(self, source, table):
        return dg.AssetKey(["my_prefix", source.id, table.name])

sensor = rocky_source_sensor(
    rocky_resource=rocky,
    target=...,
    translator=MyTranslator(),
)
```
