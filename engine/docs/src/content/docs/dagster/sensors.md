---
title: Sensors
description: Trigger Rocky materializations from Fivetran (or any source) sync events
sidebar:
  order: 10
---

`dagster-rocky` 0.4 ships `rocky_source_sensor()` — a factory that builds a
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

# Build a sensor that targets every Rocky asset
fivetran_sync_sensor = rocky_source_sensor(
    rocky_resource=rocky,
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
4. Advances the cursor.

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
