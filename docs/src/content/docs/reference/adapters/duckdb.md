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
| `extension_directory` | string | No | (DuckDB's default, under the home directory) | Directory DuckDB keeps its extensions in. Rocky sets DuckDB's `extension_directory` to it at connect. Every extension Rocky loads uses this one directory. A relative path resolves against the working directory of the `rocky` process, not against `rocky.toml`. |
| `offline` | bool | No | `false` | Rocky never runs `INSTALL` and turns DuckDB's auto-install off. See [Offline mode](#offline-mode). |

Pick one. Each example is a complete adapter declaration, not a complete `rocky.toml`.

**In-memory.** DuckDB opens an in-memory database instead of a database file.

```toml
[adapter.local]
type = "duckdb"
```

**Persistent file.**

```toml
[adapter.local]
type = "duckdb"
path = "warehouse.duckdb"
```

## Offline mode

By default, Rocky loads DuckDB's `icu` extension at connect. `icu` provides `current_date` and the time-zone functions. If `icu` is not in the extension directory, DuckDB downloads it. On a machine with no network, that download fails, Rocky logs a warning and carries on, and a query that uses a time-zone function then fails.

Set `offline = true` to make that case explicit. DuckDB's auto-install is turned off, and Rocky never runs `INSTALL`. It runs `LOAD icu` from the extension directory only. If `icu` is not there, the connect fails with an error that names the directory and says how to fill it.

```toml
[adapter.local]
type = "duckdb"
extension_directory = "/opt/rocky/duckdb-extensions"
offline = true
```

Fill the directory once, on a machine with network access:

```sql
SET extension_directory = '/opt/rocky/duckdb-extensions';
INSTALL icu;
```

Offline mode applies to every DuckDB connection Rocky opens for the project: the adapter itself, and the local in-memory databases of `rocky test`, `rocky ci`, `rocky compile` (the seed it runs for source schemas), `rocky publish-ir --with-seed` and `rocky replay --execute`. These read the settings of the `type = "duckdb"` adapters in `rocky.toml`. With several DuckDB adapters, the local database is offline when any of them sets `offline = true`, and uses the first `extension_directory` found. For `rocky compile`, a missing extension only logs a warning, and compile goes on without the seed schemas.

Offline mode limits what Rocky does. It does not sandbox a model's own SQL: a model that runs `INSTALL` or `SET autoinstall_known_extensions = true` can still reach the network. `rocky validate` warns with `V059` when `extension_directory` or `offline` is set on an adapter whose `type` is not `duckdb`, because no other adapter reads them.

One path stays online. `rocky playground` seeds its new project database before a `rocky.toml` exists, so it uses DuckDB's defaults.

Then copy the directory to the offline machine. DuckDB stores files under `<version>/<platform>/`, so the directory must come from the same DuckDB build that Rocky runs. The connect error prints the version and platform it expects.

## Authentication

None. DuckDB runs in-process.

## See also

- [`[adapter.NAME]`](/reference/configuration/#adaptername) — fields shared by every adapter type, including the retry policy.
