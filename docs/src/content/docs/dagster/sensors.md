---
title: Sensors
description: Trigger Rocky materializations from Fivetran (or any source) sync events
sidebar:
  order: 10
---

`dagster-rocky` ships `rocky_source_sensor()` — a factory that builds a
Dagster [`SensorDefinition`](https://docs.dagster.io/api/dagster/sensors#dagster.SensorDefinition)
that polls `rocky discover` and emits a `RunRequest` for any source whose
upstream connector has produced new data since the previous tick.

This unblocks pipelines that should kick off as soon as Fivetran finishes a
sync, instead of waiting for the next scheduled run.

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

The sensor:

1. Calls `rocky.discover()` on each tick.
2. Compares each source's `last_sync_at` timestamp to a per-source cursor.
3. Emits a `RunRequest` for any source whose latest sync is newer than the cursor.
4. Advances the cursor for the sources it processed.
5. Logs a warning naming any ids the engine reported in `failed_sources`, and **does not** advance their cursor — so the next tick re-evaluates them.

## Transient discover failures

Source adapters can partially fail — a Fivetran 5xx or rate-limit window on a single connector, an Iceberg `list_tables` error on one namespace. From engine `1.17.4` onward, `rocky discover` surfaces these as `failed_sources` rather than silently dropping the connector from the output.

The sensor relies on this signal to avoid the asset-graph-shrinkage failure mode where a transient adapter error looks indistinguishable from "removed upstream" to a diff-based reconciler. By skipping cursor advance for failed ids, the sensor guarantees that a flapping connector keeps reappearing for evaluation until it either succeeds (cursor advances normally) or is genuinely removed upstream (drops out of both `sources` and `failed_sources`).

Healthy sources in the same tick still produce `RunRequest`s — partial failure does not block the run.

Requires engine `≥ 1.17.4`. Older engines omit the field; the sensor treats absence as "no failures reported".

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

The cursor is JSON-encoded `{source_id: ISO 8601 timestamp}`. Storing per-source
state means each connector advances independently, so adding a new source
doesn't replay history for the existing ones.

ISO comparisons are done by parsing into Python `datetime` objects (not
lexicographic sort), so mixed timezone offsets in cursor and current sync work
correctly.

## RunRequest tags

Every `RunRequest` is tagged with:

- `rocky/source_id` (per_source) or `rocky/group` (per_group)
- `rocky/sync_at` — the ISO timestamp that triggered the run

These show up in the Dagster run history view so you can audit which Fivetran
sync triggered which materialization.

## Resource injection

`rocky_resource` accepts either form:

- **String key** (default `"rocky"`) — Dagster resolves the resource from `context.resources` at evaluation time. Per-deployment overrides apply, mock substitution via `dg.build_sensor_context(resources={...})` works without wrapping, and the resource doesn't need to exist before the sensor is built.
- **`RockyResource` instance** — the legacy form, closure-captured at sensor-build time. Still supported indefinitely so existing call sites don't break, but the keyed form is recommended for new code.

```python
# String-key form (recommended) — resolves "rocky" from context.resources
sensor = rocky_source_sensor(target=...)

# Custom resource key
sensor = rocky_source_sensor(rocky_resource="my_rocky", target=...)

# Instance form (legacy, still supported)
sensor = rocky_source_sensor(rocky_resource=rocky, target=...)
```

## Backlog cap

Pass `backlog_cap=BacklogCap(...)` to suppress emits when too many in-flight Dagster runs already share a tag value. Useful when a hung downstream amplifies into a runaway queue — without back-pressure, a stuck run can pile up dozens of fresh `RunRequest`s tagged for the same client/tenant before anyone notices.

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

Before each emit, the sensor counts in-flight runs matching `tag_key=<value>` in the non-terminal statuses (`QUEUED`, `NOT_STARTED`, `STARTING`, `STARTED` by default — override via `BacklogCap.statuses`). If the count is at or above `max_in_flight`, the `RunRequest` is suppressed.

**The cursor still advances on suppression** — the in-flight run picks up the latest data via Rocky's per-source state. Freezing the cursor would compound the failure: the next tick would re-detect the same sync, retry the same suppressed emit, and never recover until the queue drains below cap.

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

- `minimum_interval_seconds=300` — 5-minute polling
- `default_status=DefaultSensorStatus.STOPPED` — sensor ships disabled, users opt in
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
