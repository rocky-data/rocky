---
title: DuckDB adapter
description: Local in-process execution adapter — config fields and when a persistent path is required
sidebar:
  order: 1
---

Local in-process execution adapter. Use as a warehouse, source, or both: the same adapter instance can handle discovery and execution because they share the same database.

## Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `path` | string | No | (in-memory) | Path to a persistent DuckDB file. Required when using the same DuckDB adapter for both discovery and execution, so the discovery side sees rows written by the warehouse side. |

```toml
# In-memory DuckDB
[adapter.local]
type = "duckdb"

# Persistent DuckDB file
[adapter.local]
type = "duckdb"
path = "warehouse.duckdb"
```

## Authentication

None. DuckDB runs in-process.

## See also

- [`[adapter.NAME]`](/reference/configuration/#adaptername) — fields shared by every adapter type, including the retry policy.
