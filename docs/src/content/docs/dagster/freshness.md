---
title: Freshness Policies
description: Attach Dagster freshness expectations to Rocky-managed assets
sidebar:
  order: 9
---

`dagster-rocky` maps Rocky's freshness configuration onto Dagster's
[`FreshnessPolicy`](https://docs.dagster.io/api/dagster/assets#dagster.FreshnessPolicy),
which says how old an asset's data may get before Dagster calls it stale. The
Dagster UI then shows stale-data badges, and the declarative-automation
freshness conditions fire correctly.

Freshness comes from two places. A per-model setting wins over the
pipeline-level one.

1. **Pipeline-level**: from `[checks.freshness]` in `rocky.toml`. Applies to every
   source-replication asset by default.
2. **Per-model**: from `[freshness]` in a model's TOML frontmatter. Overrides the
   pipeline-level default for the matching model.

## Pipeline-level freshness

Add a `[checks.freshness]` block to `rocky.toml`:

```toml
[checks]
enabled = true

[checks.freshness]
threshold_seconds = 86400  # 24 hours
```

`RockyComponent` reads `discover.checks.freshness` and attaches a matching
`FreshnessPolicy` to every source-replication asset. Nothing else to configure.
Wire it up in `defs.yaml`:

```yaml
type: dagster_rocky.RockyComponent
attributes:
  config_path: rocky.toml
```

The functional API works the same way:

```python
from dagster_rocky import RockyResource, load_rocky_assets

rocky = RockyResource(config_path="rocky.toml")
specs = load_rocky_assets(rocky)
# specs[*].freshness_policy is set to FreshnessPolicy.time_window(fail_window=24h)
```

## Per-model freshness

Declare freshness in a model's TOML frontmatter (or sidecar `.toml` file):

```toml
# models/fct_daily_orders.toml
name = "fct_daily_orders"
depends_on = ["stg_orders"]

[strategy]
type = "incremental"
timestamp_column = "updated_at"

[target]
catalog = "warehouse"
schema = "marts"
table = "fct_daily_orders"

[freshness]
expected_lag_seconds = 3600  # 1 hour — overrides the pipeline default (max_lag_seconds is accepted as a legacy alias)
```

`rocky compile` emits this in its JSON output, in the `models_detail` field.
`RockyComponent` reads it at load time. When a source-replication table name
matches a compiled model name, the per-model policy wins.

## Helper functions

These helpers are pure functions. Use them to attach freshness policies to
hand-rolled assets, without going through `RockyComponent`.

### `freshness_policy_from_checks(checks)`

Builds a `FreshnessPolicy` from a `ChecksConfig`, the projection of
`[checks.freshness]`. Returns `None` when freshness is not configured.

```python
from dagster_rocky import freshness_policy_from_checks, RockyResource

rocky = RockyResource(config_path="rocky.toml")
result = rocky.discover()
policy = freshness_policy_from_checks(result.checks)
# Use policy on AssetSpec.freshness_policy
```

### `freshness_policy_from_model(freshness)`

Builds a `FreshnessPolicy` from a `ModelFreshnessConfig`, the projection of a
model's `[freshness]` frontmatter. Returns `None` when not configured.

```python
from dagster_rocky import freshness_policy_from_model
from dagster_rocky.types import ModelFreshnessConfig

policy = freshness_policy_from_model(ModelFreshnessConfig(max_lag_seconds=3600))
```

### `per_model_freshness_policies(compile_result)`

Indexes `freshness_policy_from_model` by model name across a whole compile
result. A model without `[freshness]` is absent from the dict. Callers can
therefore use `.get(model_name)` and fall back to the pipeline-level default.

```python
from dagster_rocky import RockyResource, per_model_freshness_policies

rocky = RockyResource(config_path="rocky.toml", models_dir="models")
compile_result = rocky.compile()
policies = per_model_freshness_policies(compile_result)
# {"fct_daily_orders": <FreshnessPolicy>, ...}
```

## API choice: `FreshnessPolicy.time_window`

`dagster-rocky` uses the Dagster 1.12+ constructor
[`FreshnessPolicy.time_window(fail_window=...)`](https://docs.dagster.io/api/dagster/assets#dagster.FreshnessPolicy.time_window).
It does **not** use the legacy `FreshnessPolicy(maximum_lag_minutes=...)`, which
is deprecated.

This means:

- Compare in tests with `policy.fail_window.to_timedelta().total_seconds()`.
  Dagster wraps the `timedelta` in a `SerializableTimeDelta`, which does not
  compare equal to a plain `timedelta`.
- The check shows up under "Freshness" in the asset detail page, with the
  `fail_window` value.
