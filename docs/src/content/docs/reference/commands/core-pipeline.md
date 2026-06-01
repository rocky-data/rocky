---
title: Core Pipeline Commands
description: Commands for the main Rocky pipeline lifecycle — init, validate, discover, plan, run, state
sidebar:
  order: 1
---

The core pipeline commands cover the full lifecycle of a Rocky pipeline: project initialization, configuration validation, source discovery, dry-run planning, execution, and state inspection.

## Global Flags

The global flags (`--config`, `--output`, `--state-path`, `--cache-ttl`) apply to every command. See [Global Flags in the CLI Reference](/reference/cli/#global-flags) for the canonical list, defaults, and the `--state-path` resolution order.

---

## `rocky init`

Initialize a new Rocky project with starter configuration and directory structure.

```bash
rocky init [path] [flags]
```

### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `path` | `string` | `.` | Directory where the project will be created. |

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--template <NAME>` | `string` | `duckdb` | Scaffold template. One of `duckdb`, `databricks-fivetran`, `snowflake`, `bigquery`, `trino`. Each template emits a runnable `rocky.toml` with the matching adapter wired up via `${VAR}` env-var placeholders (never inline secrets) plus a `models/welcome.{sql,toml}` that compiles with no source tables. |

### Examples

Create a project in the current directory (default DuckDB template):

```bash
rocky init
```

```
Created rocky.toml
Created models/
Rocky project initialized.
```

Create a Trino-targeted project in a new directory:

```bash
rocky init acme-trino --template trino
```

The emitted `rocky.toml` wires the `trino` adapter to `${TRINO_HOST}` / `${TRINO_USER}` / `${TRINO_PASSWORD}` (HTTP Basic) or `${TRINO_JWT}` (JWT bearer), with inline TOML comments documenting both auth modes.

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
| Adapters | Each `[adapter.NAME]` is a recognized type (`databricks`, `snowflake`, `duckdb`, `fivetran`, `bigquery`, `trino`, `airbyte`, `iceberg`, `manual`) with the required fields populated. For Databricks, at least one of `token` or `client_id`/`client_secret` must be set. The known-types list is driven directly off the adapter registry, so new first-party adapters propagate without a follow-up edit. |
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

> Note: the canonical, auditable form is `rocky plan` followed by `rocky apply <plan-id>`. The `rocky run` single-step alias fuses plan + apply into one invocation for local iteration and automation.

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
| `--resume <RUN_ID>` | `string` | | Resume a specific previous run from its last checkpoint; mints a new `run_id` and records the prior one as `resumed_from`. |
| `--resume-latest` | `bool` | `false` | Resume the most recent failed run from its last checkpoint; mints a new `run_id` and records the prior one as `resumed_from`. |
| `--shadow` | `bool` | `false` | Run in shadow mode: write to shadow targets instead of production. |
| `--shadow-suffix <SUFFIX>` | `string` | `_rocky_shadow` | Suffix appended to table names in shadow mode. |
| `--shadow-schema <NAME>` | `string` | | Override schema for shadow tables (mutually exclusive with `--shadow-suffix`). |
| `--branch <NAME>` | `string` | | Execute against a named branch previously registered with `rocky branch create`. Applies the branch's `schema_prefix` to every target (internally equivalent to `--shadow --shadow-schema <branch.schema_prefix>`). Mutually exclusive with `--shadow` / `--shadow-schema`. |
| `--watch` | `bool` | `false` | Wrap the run in a filesystem watcher: re-execute the pipeline on every change to `rocky.toml` or any file under `models/`, debounced to 200 ms so editor save bursts coalesce into a single re-run. Failed runs do not exit the loop; Ctrl-C exits cleanly between runs. **v0 limitations:** mutually exclusive with `--dag`, `--resume`, `--resume-latest`, `--idempotency-key`, and `--model` (rejected at parse time). |

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

Run in watch mode for the inner-loop developer workflow — every save re-materializes the pipeline against the local DuckDB warehouse:

```bash
rocky run --watch
```

`--watch` watches the parent directory of `rocky.toml` (filtered to `rocky.toml` itself) plus the resolved `models/` directory recursively. The directory watch is FSEvents-safe on macOS — atomic-rename saves (vim's `:w`, VSCode's default) trigger correctly where a file-level watch can miss the new inode. Banner / "detected change" lines go to `stderr` so `stdout` stays parseable; with `--output json`, each iteration emits one `RunOutput` JSON object on `stdout` (newline-delimited).

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

Manage named virtual branches. A branch is the persistent, named analogue of `--shadow` mode: it records a `schema_prefix` in the state store and, when `rocky plan --branch <name>` + `rocky apply <plan-id>` is invoked (or the single-step `rocky run --branch <name>` alias), every model target has the prefix applied. Schema-prefix branches work uniformly across every adapter today; warehouse-native clones (Delta `SHALLOW CLONE`, Snowflake zero-copy `CLONE`) are a follow-up.

```bash
rocky branch create <name> [--description <text>]
rocky branch delete <name>
rocky branch list
rocky branch show <name>
rocky branch compare <name> [--filter <key=value>]
rocky branch approve <name> [--message <text>] [--out <path>]
rocky branch promote <name> [--allow-breaking] [--base-ref <ref>]
                            [--models <path>] [--skip-approval]
                            [--filter <key=value>]
rocky branch promote <name> --plan <plan-id>   # canonical: plan + apply
```

Branch names accept `[A-Za-z0-9_.\-]` up to 64 characters. The default schema prefix is `branch__<name>`. Deleting a branch removes the state-store entry but does **not** drop warehouse tables that were materialized under it.

### `branch approve` flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--message <text>` | `string` | (none) | Optional free-form note persisted in the approval artifact. |
| `--out <path>` | `PathBuf` | `./.rocky/approvals/<branch>/<approval_id>.json` | Override the artifact destination path. |

Writes a content-addressed approval artifact that binds the approver's git identity to the exact bytes of the branch's models and project config. Editing, adding, or renaming any model after approval voids that approval, so `rocky branch promote` refuses to run unless the on-disk approvals still match the current state and satisfy the [`[branch.approval]`](/reference/configuration/#branchapproval) policy.

> **Upgrade note (engine v1.43):** approvals created before v1.43 bound to the project config only, not the model bytes. They no longer satisfy the gate after upgrading. Run `rocky branch approve <name>` once to re-sign each branch against its current model contents.

### `branch promote` flags

> Note: as of engine v1.33, the canonical form is `rocky plan promote <name>` followed by `rocky apply <plan-id>` (or `rocky branch promote <name> --plan <plan-id>`). The bare `rocky branch promote <name>` form continues to work and is now an alias; it emits a one-line `[deprecated]` notice to stderr that can be silenced with `ROCKY_SUPPRESS_DEPRECATION=1`.

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--allow-breaking` | flag | off | Bypass the semantic breaking-change gate. Always emits a `breaking_changes_allowed` audit event so the override leaves a paper trail. |
| `--base-ref <ref>` | `string` | `main` | Git ref to diff against for the breaking-change gate. |
| `--models <path>` | `PathBuf` | `models` | Models directory used by the breaking-change gate. |
| `--skip-approval` | flag | off | Bypass the approval gate. Always emits an `approval_skipped` audit event so the bypass leaves a paper trail. |
| `--pipeline <name>` | `string` | (none) | Which pipeline to promote, in a multi-pipeline project. Optional when the project defines a single pipeline. |
| `--filter <key=value>` | `string` | (none) | Filter the promote targets. Replication pipelines filter sources by schema-pattern component (e.g. `--filter client=acme`); transformation pipelines filter models by `table`, `model`, `catalog`, or `schema`. |

Enumerates the pipeline's production targets and promotes each one. A replication pipeline discovers the source connector's tables through the schema-pattern templates; a transformation pipeline walks the configured `models` glob and promotes one target per model, skipping ephemeral models. It then runs the optional `[branch.approval]` gate, runs the semantic breaking-change gate against `--base-ref`, and dispatches `CREATE OR REPLACE TABLE prod.<x> AS SELECT * FROM branch__<name>.<x>` per target. Quality and snapshot pipelines are not supported and return a clear error.

The breaking-change gate vetoes the promote (exit nonzero) when any finding has `severity == "breaking"` unless `--allow-breaking` is passed. Every gate decision — block, allow-via-override, or fail-open when the gate couldn't run — is recorded in the audit trail. See [`rocky ci-diff --semantic`](/reference/commands/modeling/#rocky-ci-diff) to surface the same findings informationally on every PR.

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

Diff a branch's materialized tables against production (row counts + schemas):

```bash
rocky branch compare fix-price
```

Internally this is `rocky compare` pointed at the branch's `schema_prefix` via `ShadowConfig.schema_override` — the same mechanism `rocky run --branch` uses for writes, so compare always hits exactly the tables the branch produced. Accepts the shared [`--filter`](/reference/filters/) flag.

### Related Commands

- [`rocky run`](#rocky-run) -- execute a pipeline against a branch via `--run --branch`
- [`rocky compare`](/reference/cli/#rocky-compare) -- diff an ad-hoc shadow against production (the generic form `rocky branch compare` specialises)
