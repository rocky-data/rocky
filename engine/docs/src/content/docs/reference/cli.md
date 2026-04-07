---
title: CLI Reference
description: Complete reference for all Rocky CLI commands and flags
sidebar:
  order: 1
---

Rocky provides a single binary with subcommands for every stage of the pipeline lifecycle: initialization, validation, discovery, planning, execution, and state inspection.

## Global Flags

These flags apply to all commands.

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--config <PATH>` | `-c` | `rocky.toml` | Path to the pipeline configuration file. |
| `--output <FORMAT>` | `-o` | `json` | Output format. Accepted values: `json`, `table`. |
| `--state-path <PATH>` | | `.rocky-state.redb` | Path to the embedded state store used for watermarks. |

```bash
# Example: use a custom config and table output
rocky -c pipelines/prod.toml -o table discover
```

---

## Commands

### `rocky init`

Scaffolds a new Rocky project in the target directory.

```bash
rocky init [path]
```

**Arguments:**

| Argument | Default | Description |
|----------|---------|-------------|
| `path` | `.` (current directory) | Directory where the project will be created. |

**Behavior:**

- Creates a starter `rocky.toml` with placeholder values.
- Creates a `models/` directory for SQL model files.
- Fails with an error if `rocky.toml` already exists in the target directory.

**Example:**

```bash
# Scaffold in current directory
rocky init

# Scaffold in a new directory
rocky init my-pipeline
```

---

### `rocky validate`

Checks the pipeline configuration for correctness without connecting to any external APIs.

```bash
rocky validate
```

**Checks performed:**

| Check | Description |
|-------|-------------|
| TOML syntax | The config file parses without errors as v2 (named adapters + named pipelines). |
| Adapters | Each `[adapter.NAME]` is a recognized type (`databricks`, `snowflake`, `duckdb`, `fivetran`, `manual`) with the required fields populated. |
| Pipelines | Each `[pipeline.NAME]` references existing adapters for source, target, and (optional) discovery, and its `schema_pattern` parses. |
| DAG validation | If `models/` exists, loads all models and checks for dependency cycles. |

**Output:**

Each check prints `ok` or `!!` followed by a short description. A non-zero exit code is returned if any check fails.

```
ok  Config syntax valid (v2 format)
ok  adapter.fivetran: fivetran
ok  adapter.prod: databricks (auth configured)
ok  pipeline.bronze: schema pattern parseable
ok  pipeline.bronze: replication / incremental -> {tenant}_warehouse / staging__{regions}__{source}
```

---

### `rocky discover`

Lists available connectors and their tables from the configured source.

```bash
rocky discover [--pipeline NAME]
```

**Flags:**

| Flag | Description |
|------|-------------|
| `--pipeline <NAME>` | Pipeline name. Required when more than one `[pipeline.NAME]` is defined. |

**Behavior:**

- For `fivetran` adapters, calls the Fivetran REST API to list connectors and their enabled tables. For `duckdb` adapters, queries `information_schema.{schemata,tables}`. For `manual` adapters, reads inline schema/table definitions.
- This is a **metadata-only operation** — it identifies what schemas and tables exist, it does not extract or move data.
- Parses each source schema name using the pipeline's `schema_pattern` to extract structured components (tenant, regions, source, etc.).
- Returns structured data about every discovered source and its tables.

**JSON output:**

```json
{
  "version": "0.1.0",
  "command": "discover",
  "sources": [
    {
      "id": "connector_abc123",
      "components": { "tenant": "acme", "regions": ["us_west"], "source": "shopify" },
      "source_type": "fivetran",
      "last_sync_at": "2026-03-30T10:00:00Z",
      "tables": [{ "name": "orders", "row_count": null }]
    }
  ]
}
```

**Table output:**

```
connector_id      | components                          | tables
──────────────────┼─────────────────────────────────────┼───────
connector_abc123  | acme / us_west / shopify            | 12
connector_def456  | acme / eu_central / stripe          | 8
```

---

### `rocky plan`

Generates the SQL statements Rocky would execute, without actually running them. Useful for auditing and previewing changes before a run.

```bash
rocky plan --filter <key=value> [--pipeline NAME]
```

**Flags:**

| Flag | Required | Description |
|------|----------|-------------|
| `--filter <key=value>` | Yes | Filter sources by component. Example: `--filter tenant=acme`. |
| `--pipeline <NAME>` | | Pipeline name. Required when more than one pipeline is defined. |

**Behavior:**

- Runs discovery and drift detection.
- Generates all SQL statements (catalog creation, schema creation, incremental copy, permission grants) and returns them without execution.

**JSON output:**

```json
{
  "version": "0.1.0",
  "command": "plan",
  "filter": "tenant=acme",
  "statements": [
    { "purpose": "create_catalog", "target": "acme_warehouse", "sql": "CREATE CATALOG IF NOT EXISTS acme_warehouse" },
    { "purpose": "create_schema", "target": "acme_warehouse.staging__us_west__shopify", "sql": "..." },
    { "purpose": "incremental_copy", "target": "acme_warehouse.staging__us_west__shopify.orders", "sql": "..." }
  ]
}
```

---

### `rocky run`

Executes the full pipeline end-to-end.

```bash
rocky run --filter <key=value> [flags]
```

**Flags:**

| Flag | Required | Description |
|------|----------|-------------|
| `--filter <key=value>` | Yes | Filter sources by component. Example: `--filter tenant=acme`. |
| `--pipeline <NAME>` | | Pipeline name. Required when more than one pipeline is defined. |
| `--governance-override <JSON>` | | Additional governance config as inline JSON or `@file.json`, merged with defaults. |
| `--models <PATH>` | | Models directory for transformation execution. |
| `--all` | | Execute both replication and compiled models. |
| `--resume <RUN_ID>` | | Resume a specific previous run from its last checkpoint. |
| `--resume-latest` | | Resume the most recent failed run from its last checkpoint. |
| `--shadow` | | Run in shadow mode: write to shadow targets instead of production. |
| `--shadow-suffix <SUFFIX>` | | Suffix appended to table names in shadow mode (default `_rocky_shadow`). |
| `--shadow-schema <NAME>` | | Override schema for shadow tables (mutually exclusive with `--shadow-suffix`). |

**Pipeline stages (in order):**

1. **Discover** — enumerate sources and tables from the configured source adapter.
2. **Governance setup** (sequential, per matching catalog/schema):
   - Create catalog (if `auto_create_catalogs = true`)
   - Apply catalog tags (`ALTER CATALOG SET TAGS`)
   - Bind workspaces (Unity Catalog bindings API, if `governance.isolation` configured)
   - Apply catalog-level grants (`GRANT ... ON CATALOG`)
   - Create schema (if `auto_create_schemas = true`)
   - Apply schema tags (`ALTER SCHEMA SET TAGS`)
   - Apply schema-level grants (`GRANT ... ON SCHEMA`)
3. **Parallel table processing** — for each table concurrently (up to `execution.concurrency`):
   - Drift detection (compare column types between source and target)
   - Copy data (incremental or full refresh SQL)
   - Apply table tags
   - Update watermark in state store
4. **Batched checks** — row count, column match, freshness (batched with UNION ALL for efficiency)
5. **Retry** — failed tables retried sequentially (configurable via `execution.table_retries`)

:::note
Governance (tags, workspace bindings, permissions) is NOT a separate CLI command. It runs inline during `rocky run` as catalogs and schemas are created. The governance features are Databricks Unity Catalog specific.
:::

**JSON output:**

```json
{
  "version": "0.1.0",
  "command": "run",
  "filter": "tenant=acme",
  "duration_ms": 45200,
  "tables_copied": 20,
  "materializations": [
    {
      "asset_key": ["fivetran", "acme", "us_west", "shopify", "orders"],
      "rows_copied": null,
      "duration_ms": 2300,
      "metadata": { "strategy": "incremental", "watermark": "2026-03-30T10:00:00Z" }
    }
  ],
  "check_results": [],
  "permissions": { "grants_added": 3, "grants_revoked": 0 },
  "drift": { "tables_checked": 20, "tables_drifted": 1, "actions_taken": [] }
}
```

---

### `rocky state`

Displays stored watermarks from the embedded state file.

```bash
rocky state
```

**Behavior:**

- Reads the redb state store (default: `.rocky-state.redb`).
- Lists every tracked table with its last watermark value and the timestamp it was recorded.

**JSON output:**

```json
{
  "version": "0.1.0",
  "command": "state",
  "watermarks": [
    {
      "table": "acme_warehouse.staging__us_west__shopify.orders",
      "last_value": "2026-03-30T10:00:00Z",
      "updated_at": "2026-03-30T10:01:32Z"
    }
  ]
}
```

**Table output:**

```
table                                                | last_value                | updated_at
─────────────────────────────────────────────────────┼───────────────────────────┼───────────────────────────
acme_warehouse.staging__us_west__shopify.orders       | 2026-03-30T10:00:00Z      | 2026-03-30T10:01:32Z
acme_warehouse.staging__us_west__shopify.customers    | 2026-03-30T09:55:00Z      | 2026-03-30T10:01:32Z
```
