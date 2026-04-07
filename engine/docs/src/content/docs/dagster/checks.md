---
title: Check Results
description: Convert Rocky check results into Dagster events
sidebar:
  order: 6
---

Two functions convert Rocky execution results into Dagster events that appear in the Dagster UI.

## `emit_materializations(result, translator?) -> list[AssetMaterialization]`

Converts Rocky materializations into Dagster `AssetMaterialization` events.

Each materialization includes metadata:

- `strategy` -- the materialization strategy used (e.g., `incremental`, `full_refresh`)
- `duration_ms` -- how long the materialization took
- `rows_copied` -- number of rows copied
- `watermark` -- the new high watermark value

An optional `translator` can be passed to control asset key mapping, ensuring consistency with how assets were discovered via `load_rocky_assets()`.

## `emit_check_results(result) -> list[AssetCheckResult]`

Converts Rocky check results into Dagster `AssetCheckResult` events.

Handles all Rocky check types:

- **row_count** -- validates source and target row counts match
- **column_match** -- validates columns exist in both source and target
- **freshness** -- validates data is within a staleness threshold
- **null_rate** -- validates null rates are below a threshold
- **custom** -- any user-defined check

All check metadata is propagated to the Dagster event for full observability.

## Example

```python
from dagster_rocky import RockyResource, emit_check_results, emit_materializations
import dagster as dg

@dg.asset
def replicate(context, rocky: RockyResource):
    result = rocky.run(filter="tenant=acme")

    for mat in emit_materializations(result):
        context.log_event(mat)

    for check in emit_check_results(result):
        context.log_event(check)

    return result.tables_copied
```

Both functions return lists, so you can also inspect or filter events before logging them:

```python
mats = emit_materializations(result)
context.log.info(f"Emitting {len(mats)} materializations")
for mat in mats:
    context.log_event(mat)
```
