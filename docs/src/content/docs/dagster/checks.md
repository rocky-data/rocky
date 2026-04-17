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

Handles every Rocky check type, both pipeline-level and model-level:

**Pipeline-level checks:**
- **row_count** -- validates source and target row counts match
- **column_match** -- validates columns exist in both source and target
- **freshness** -- validates data is within a staleness threshold
- **null_rate** -- validates null rates are below a threshold
- **anomaly** -- row count deviation from historical baselines
- **custom** -- any user-defined SQL check

**Model-level assertions (DQX parity):**
`not_null`, `unique`, `accepted_values`, `relationships`, `expression`, `row_count_range`, `in_range`, `regex_match`, `aggregate`, `composite`, `not_in_future`, `older_than_n_days`. Each carries a `severity` (`error` / `warning`) and an optional `filter` predicate; row-level assertions also surface `quarantined = true/false` when `[quarantine]` is configured on the pipeline.

Severity maps to Dagster's `AssetCheckSeverity` (`ERROR` / `WARN`). Warnings never fail the asset check, even when the underlying assertion returned `passed = false`. All check metadata — SQL fingerprint, failing row count, quarantine state, filter used — is propagated to the Dagster event for full observability.

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
