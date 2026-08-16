---
title: DuckDB adapter
description: The local in-process adapter, and when it needs a file on disk instead of memory
sidebar:
  order: 1
---

DuckDB runs inside the Rocky process. Use it as a warehouse, as a source, or as both at once. One adapter instance can serve discovery and execution together, because both sides read the same database.

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
