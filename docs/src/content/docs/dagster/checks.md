---
title: Check Results
description: Convert Rocky check results into Dagster events
sidebar:
  order: 6
---

Two functions convert Rocky execution results into Dagster events that appear in the Dagster UI.

## `emit_materializations(result) -> list[AssetMaterialization]`

Converts Rocky materializations into Dagster `AssetMaterialization` events.

Each materialization includes metadata:

- `strategy` -- the materialization strategy used (e.g., `incremental`, `full_refresh`)
- `duration_ms` -- how long the materialization took
- `rows_copied` -- number of rows copied
- `watermark` -- the new high watermark value

Asset keys come verbatim from `result.materializations[].asset_key`.

## `emit_check_results(result) -> list[AssetCheckResult]`

Converts Rocky check results into Dagster `AssetCheckResult` events.

Handles every Rocky check type, both pipeline-level and model-level:

**Pipeline-level checks:**
- **row_count** -- validates source and target row counts match
- **column_match** -- validates columns exist in both source and target
- **freshness** -- validates data is within a staleness threshold
- **null_rate** -- validates null rates are below a threshold
- **cross_source_overlap** -- detects the same business key across sibling sources feeding a shared target
- **custom** -- any user-defined SQL check

Anomalies are not part of `check_results` -- they live on `RunResult.anomalies` and are converted by the separate `anomaly_check_results()` helper (see the observability page).

**Model-level assertions (DQX parity):**
`not_null`, `unique`, `accepted_values`, `relationships`, `expression`, `row_count_range`, `in_range`, `regex_match`, `aggregate`, `composite`, `not_in_future`, `older_than_n_days`. Each carries a `severity` (`error` / `warning`).

Severity maps to Dagster's `AssetCheckSeverity` (`ERROR` / `WARN`). A failing warning-severity check still reports `passed = false`, but at severity `WARN` it does not degrade asset health or trigger `ASSET_HEALTH_DEGRADED` alerts. The metadata attached to each Dagster event is whatever the check populated: `source_count` / `target_count`, `missing_columns` / `extra_columns`, `lag_seconds` / `threshold_seconds`, `column` / `null_rate` / `threshold`, `query` / `result_value`, and a `severity` marker when the check is advisory.

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

Both functions return lists, so you can inspect or filter the events before logging them.
