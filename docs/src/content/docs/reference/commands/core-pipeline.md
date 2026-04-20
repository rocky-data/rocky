---
title: Core Pipeline Commands
description: Commands for the main Rocky pipeline lifecycle — init, validate, discover, plan, run, state
sidebar:
  order: 1
---

The core pipeline commands cover the full lifecycle of a Rocky pipeline: project initialization, configuration validation, source discovery, dry-run planning, execution, and state inspection.

## Global Flags

These flags apply to all commands and are documented in the [CLI Reference](/reference/cli/).

| Flag | Short | Type | Default | Description |
|------|-------|------|---------|-------------|
| `--config <PATH>` | `-c` | `PathBuf` | `rocky.toml` | Path to the pipeline configuration file. |
| `--output <FORMAT>` | `-o` | `string` | `json` | Output format: `json` or `table`. |
| `--state-path <PATH>` | | `PathBuf` | `.rocky-state.redb` | Path to the embedded state store for watermarks. |

---

## `rocky init`

Initialize a new Rocky project with starter configuration and directory structure.

```bash
rocky init [path]
```

### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `path` | `string` | `.` | Directory where the project will be created. |

### Examples

Create a project in the current directory:

```bash
rocky init
```

```
Created rocky.toml
Created models/
Rocky project initialized.
```

Create a project in a new directory:

```bash
rocky init acme-pipeline
```

```
Created acme-pipeline/rocky.toml
Created acme-pipeline/models/
Rocky project initialized in acme-pipeline/
```

### Related Commands

- [`rocky validate`](#rocky-validate) -- check the generated config
- [`rocky playground`](/reference/commands/development/#rocky-playground) -- create a sample project with DuckDB (no credentials needed)

---

## `rocky validate`

Check the pipeline configuration for correctness without connecting to any external APIs. Returns a non-zero exit code if any check fails.

```bash
rocky validate [flags]
```

### Flags

No command-specific flags. Uses [global flags](#global-flags) only.

### Checks Performed

| Check | Description |
|-------|-------------|
| TOML syntax | The config file parses without errors as v2 (named adapters + named pipelines). |
| Adapters | Each `[adapter.NAME]` is a recognized type (`databricks`, `snowflake`, `duckdb`, `fivetran`, `manual`) with the required fields populated. For Databricks, at least one of `token` or `client_id`/`client_secret` must be set. |
| Pipelines | Each `[pipeline.NAME]` references existing adapters for source, target, and (optional) discovery, and its `schema_pattern` parses. |
| DAG validation | If `models/` exists, loads all models and checks for dependency cycles. |

### Examples

Validate the default config:

```bash
rocky validate
```

```
ok  Config syntax valid (v2 format)
ok  adapter.fivetran: fivetran
ok  adapter.prod: databricks (auth configured)
ok  pipeline.bronze: schema pattern parseable
ok  pipeline.bronze: replication / incremental -> warehouse / stage__{source}

Validation complete.
```

Validate a specific config file:

```bash
rocky -c pipelines/prod.toml validate
```

```
ok  Config syntax valid (v2 format)
ok  adapter.fivetran: fivetran
!!  adapter.prod: no auth configured (token or client_id/secret)
ok  pipeline.bronze: schema pattern parseable
ok  pipeline.bronze: replication / incremental -> warehouse / stage__{source}

Validation complete.
```

### Related Commands

- [`rocky plan`](#rocky-plan) -- preview SQL without execution
- [`rocky run`](#rocky-run) -- execute the full pipeline

---

## `rocky discover`

List available connectors and their tables from the configured source. This is a metadata-only operation -- it identifies what schemas and tables exist, but does not move any data.

```bash
rocky discover [flags]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--pipeline <NAME>` | `string` | | Pipeline name (required if multiple pipelines are defined). |

### Examples

Discover all sources with JSON output:

```bash
rocky discover
```

```json
{
  "version": "1.6.0",
  "command": "discover",
  "sources": [
    {
      "id": "connector_abc123",
      "components": { "tenant": "acme", "regions": ["us_west"], "source": "shopify" },
      "source_type": "fivetran",
      "last_sync_at": "2026-03-30T10:00:00Z",
      "tables": [
        { "name": "orders", "row_count": null },
        { "name": "customers", "row_count": null }
      ]
    },
    {
      "id": "connector_def456",
      "components": { "tenant": "acme", "regions": ["eu_central"], "source": "stripe" },
      "source_type": "fivetran",
      "last_sync_at": "2026-03-29T22:15:00Z",
      "tables": [
        { "name": "charges", "row_count": null },
        { "name": "refunds", "row_count": null }
      ]
    }
  ]
}
```

Discover with table output:

```bash
rocky -o table discover
```

```
connector_id      | components                          | tables
------------------+-------------------------------------+-------
connector_abc123  | acme / us_west / shopify            | 12
connector_def456  | acme / eu_central / stripe          | 8
```

Discover a specific pipeline when multiple are defined:

```bash
rocky discover --pipeline shopify_us
```

### Related Commands

- [`rocky plan`](#rocky-plan) -- generate SQL from discovered sources
- [`rocky run`](#rocky-run) -- discover and execute in one step

---

## `rocky plan`

Generate the SQL statements Rocky would execute without actually running them. Useful for auditing, previewing changes, and CI/CD approval workflows.

```bash
rocky plan --filter <key=value> [flags]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--filter <key=value>` | `string` | **(required)** | Filter sources by component value (e.g., `client=acme`). |
| `--pipeline <NAME>` | `string` | | Pipeline name (required if multiple pipelines are defined). |

### Examples

Plan all SQL for a specific tenant:

```bash
rocky plan --filter client=acme
```

```json
{
  "version": "1.6.0",
  "command": "plan",
  "filter": "client=acme",
  "statements": [
    {
      "purpose": "create_catalog",
      "target": "acme_warehouse",
      "sql": "CREATE CATALOG IF NOT EXISTS acme_warehouse"
    },
    {
      "purpose": "create_schema",
      "target": "acme_warehouse.staging__us_west__shopify",
      "sql": "CREATE SCHEMA IF NOT EXISTS acme_warehouse.staging__us_west__shopify"
    },
    {
      "purpose": "incremental_copy",
      "target": "acme_warehouse.staging__us_west__shopify.orders",
      "sql": "SELECT *, CAST(NULL AS STRING) AS _loaded_by FROM source_catalog.src__acme__us_west__shopify.orders WHERE _fivetran_synced > (SELECT COALESCE(MAX(_fivetran_synced), TIMESTAMP '1970-01-01') FROM acme_warehouse.staging__us_west__shopify.orders)"
    }
  ]
}
```

Plan with table output and a custom config:

```bash
rocky -c pipelines/prod.toml -o table plan --filter client=acme
```

```
purpose           | target                                             | sql (truncated)
------------------+----------------------------------------------------+--------------------------
create_catalog    | acme_warehouse                                     | CREATE CATALOG IF NOT...
create_schema     | acme_warehouse.staging__us_west__shopify            | CREATE SCHEMA IF NOT...
incremental_copy  | acme_warehouse.staging__us_west__shopify.orders     | SELECT *, CAST(NULL...
incremental_copy  | acme_warehouse.staging__us_west__shopify.customers  | SELECT *, CAST(NULL...
```

Plan for a specific pipeline:

```bash
rocky plan --filter client=acme --pipeline shopify_us
```

### Related Commands

- [`rocky run`](#rocky-run) -- execute the planned SQL
- [`rocky validate`](#rocky-validate) -- check config before planning
- [`rocky discover`](#rocky-discover) -- see available sources

---

## `rocky run`

Execute the full pipeline end-to-end: discover sources, detect schema drift, create catalogs/schemas, copy data, apply governance, and run quality checks.

```bash
rocky run --filter <key=value> [flags]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--filter <key=value>` | `string` | **(required)** | Filter sources by component value (e.g., `client=acme`). |
| `--pipeline <NAME>` | `string` | | Pipeline name (required if multiple pipelines are defined). |
| `--governance-override <JSON>` | `string` | | Additional governance config as inline JSON or `@file.json`, merged with defaults. |
| `--models <PATH>` | `PathBuf` | | Models directory for transformation execution. |
| `--all` | `bool` | `false` | Execute both replication and compiled models. |
| `--resume <RUN_ID>` | `string` | | Resume a specific previous run from its last checkpoint. |
| `--resume-latest` | `bool` | `false` | Resume the most recent failed run from its last checkpoint. |
| `--shadow` | `bool` | `false` | Run in shadow mode: write to shadow targets instead of production. |
| `--shadow-suffix <SUFFIX>` | `string` | `_rocky_shadow` | Suffix appended to table names in shadow mode. |
| `--shadow-schema <NAME>` | `string` | | Override schema for shadow tables (mutually exclusive with `--shadow-suffix`). |
| `--branch <NAME>` | `string` | | Execute against a named branch previously registered with `rocky branch create`. Applies the branch's `schema_prefix` to every target (internally equivalent to `--shadow --shadow-schema <branch.schema_prefix>`). Mutually exclusive with `--shadow` / `--shadow-schema`. |

### Pipeline Stages

1. **Discover** -- enumerate sources and tables.
2. **Governance setup** (sequential, per catalog/schema) -- create catalogs, apply tags, bind workspaces, grant permissions, create schemas.
3. **Parallel table processing** (up to `execution.concurrency`) -- drift detection, incremental copy, tag application, watermark update.
4. **Batched checks** -- row count, column match, freshness.
5. **Retry** -- failed tables retried sequentially (per `execution.table_retries`).

### Examples

Run the pipeline for a specific tenant:

```bash
rocky run --filter client=acme
```

```json
{
  "version": "1.6.0",
  "command": "run",
  "filter": "client=acme",
  "duration_ms": 45200,
  "tables_copied": 20,
  "tables_failed": 0,
  "materializations": [
    {
      "asset_key": ["fivetran", "acme", "us_west", "shopify", "orders"],
      "rows_copied": null,
      "duration_ms": 2300,
      "metadata": {
        "strategy": "incremental",
        "watermark": "2026-03-30T10:00:00Z",
        "target_table_full_name": "acme_warehouse.staging__us_west__shopify.orders"
      }
    }
  ],
  "check_results": [],
  "errors": [],
  "excluded_tables": [],
  "permissions": { "grants_added": 3, "grants_revoked": 0, "catalogs_created": 0, "schemas_created": 1 },
  "drift": { "tables_checked": 20, "tables_drifted": 1, "actions_taken": [] },
  "anomalies": [],
  "partition_summaries": []
}
```

Run with a governance override file:

```bash
rocky run --filter client=acme --governance-override @overrides/acme.json
```

Run both replication and model transformations:

```bash
rocky run --filter client=acme --models models/ --all
```

Resume the most recent failed run from its last checkpoint:

```bash
rocky run --filter client=acme --resume-latest
```

Run in shadow mode (writes to `*_rocky_shadow` tables instead of production) so you can compare results before promoting:

```bash
rocky run --filter client=acme --shadow
rocky compare --filter client=acme
```

Or run against a named branch — the persistent, named analogue of `--shadow`:

```bash
rocky branch create fix-price --description "testing reprice migration"
rocky run --filter client=acme --branch fix-price
```

### Related Commands

- [`rocky plan`](#rocky-plan) -- preview SQL before execution
- [`rocky state`](#rocky-state) -- inspect watermarks after a run
- [`rocky branch`](#rocky-branch) -- manage named branches
- [`rocky history`](/reference/commands/administration/#rocky-history) -- view past runs

---

## `rocky state`

Display stored watermarks from the embedded state file. Shows every tracked table with its last watermark value and the timestamp it was recorded.

```bash
rocky state [flags]
```

### Flags

No command-specific flags. Uses [global flags](#global-flags) only.

### Examples

Show watermarks with JSON output:

```bash
rocky state
```

```json
{
  "version": "1.6.0",
  "command": "state",
  "watermarks": [
    {
      "table": "acme_warehouse.staging__us_west__shopify.orders",
      "last_value": "2026-03-30T10:00:00Z",
      "updated_at": "2026-03-30T10:01:32Z"
    },
    {
      "table": "acme_warehouse.staging__us_west__shopify.customers",
      "last_value": "2026-03-30T09:55:00Z",
      "updated_at": "2026-03-30T10:01:32Z"
    }
  ]
}
```

Show watermarks with table output using a custom state path:

```bash
rocky -o table --state-path /var/rocky/state.redb state
```

```
table                                                | last_value                | updated_at
-----------------------------------------------------+---------------------------+---------------------------
acme_warehouse.staging__us_west__shopify.orders       | 2026-03-30T10:00:00Z      | 2026-03-30T10:01:32Z
acme_warehouse.staging__us_west__shopify.customers    | 2026-03-30T09:55:00Z      | 2026-03-30T10:01:32Z
acme_warehouse.staging__eu_central__stripe.charges    | 2026-03-29T22:15:00Z      | 2026-03-30T10:01:32Z
```

### Related Commands

- [`rocky run`](#rocky-run) -- update watermarks by executing the pipeline
- [`rocky history`](/reference/commands/administration/#rocky-history) -- view run history

---

## `rocky branch`

Manage named virtual branches. A branch is the persistent, named analogue of `--shadow` mode: it records a `schema_prefix` in the state store and, when `rocky run --branch <name>` is invoked, every model target has the prefix applied. Schema-prefix branches work uniformly across every adapter today; warehouse-native clones (Delta `SHALLOW CLONE`, Snowflake zero-copy `CLONE`) are a follow-up.

```bash
rocky branch create <name> [--description <text>]
rocky branch delete <name>
rocky branch list
rocky branch show <name>
```

Branch names accept `[A-Za-z0-9_.\-]` up to 64 characters. The default schema prefix is `branch__<name>`. Deleting a branch removes the state-store entry but does **not** drop warehouse tables that were materialized under it.

### Examples

Create, list, run against, and delete a branch:

```bash
rocky branch create fix-price --description "testing reprice migration"
```

```json
{
  "version": "1.11.0",
  "command": "branch create",
  "branch": {
    "name": "fix-price",
    "schema_prefix": "branch__fix-price",
    "created_by": "hugo",
    "created_at": "2026-04-20T14:22:11+00:00",
    "description": "testing reprice migration"
  }
}
```

```bash
rocky branch list
```

```json
{
  "version": "1.11.0",
  "command": "branch list",
  "total": 2,
  "branches": [
    { "name": "fix-price", "schema_prefix": "branch__fix-price", "created_by": "hugo", "created_at": "2026-04-20T14:22:11+00:00", "description": "testing reprice migration" },
    { "name": "ingest-v2", "schema_prefix": "branch__ingest-v2", "created_by": "ci",   "created_at": "2026-04-18T09:05:00+00:00", "description": null }
  ]
}
```

```bash
rocky run --filter client=acme --branch fix-price
rocky branch delete fix-price
```

### Related Commands

- [`rocky run`](#rocky-run) -- execute a pipeline against a branch via `--run --branch`
- [`rocky compare`](/reference/cli/#rocky-compare) -- diff branch output against production
