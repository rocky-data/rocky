---
title: RockyComponent
description: State-backed Dagster component for scalable asset discovery
sidebar:
  order: 7
---

`RockyComponent` is a state-backed Dagster component that caches `rocky discover` output. It avoids calling external APIs on every Dagster code location reload, making asset discovery fast and reliable.

## How it works

1. **`write_state_to_path()`** -- Calls `rocky discover`, serializes the result to JSON, and saves it to a state file.
2. **`build_defs_from_state()`** -- Reads the cached JSON from the state file and creates a list of `AssetSpec` objects without making any API calls.

On code location reload, Dagster reads from the cached state file rather than calling Fivetran, Databricks, or any other external API. Assets appear in the Dagster UI instantly.

## Configuration

Configure `RockyComponent` in your `defs.yaml`:

```yaml
type: dagster_rocky.RockyComponent
attributes:
  binary_path: rocky
  config_path: config/rocky.toml
  state_path: .rocky-state.redb
```

## State storage

By default, the component stores its state on the local filesystem. This is configurable via the `defs_state` mechanism in Dagster, allowing you to use alternative storage backends if needed.

## Benefits

- **Fast reloads** -- Assets are visible in the Dagster UI instantly on code location reload, with no API calls.
- **Resilience** -- If an external API is temporarily unavailable, the cached state ensures assets remain visible.
- **Scalability** -- Works well with large numbers of sources and tables without adding latency to code location startup.

## Refreshing state

To update the cached state with the latest discovery results, call `write_state_to_path()`. This is typically done as part of a scheduled job or a manual refresh operation, separate from the normal code location reload cycle.
