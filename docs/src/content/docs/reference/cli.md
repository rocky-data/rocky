---
title: CLI Reference
description: Complete reference for all Rocky CLI commands and flags
sidebar:
  order: 1
---

Rocky ships one binary. Every subcommand below is a step in the pipeline lifecycle, grouped here by the job it does:

- **Core Pipeline**: `init`, `validate`, `discover`, `plan`, `apply`, `state`, `branch` (single-step alias: `run`)
- **Modeling**: `compile`, `lineage`, `lineage-diff`, `test`, `ci`, `ci-diff`, `preview`, `emit-sql`, `catalog`, `publish-ir`, `imports`
- **Data**: `seed`, `snapshot`, `docs`, `load`, `profile`
- **AI**: `ai`, `ai-sync`, `ai-explain`, `ai-test`, `ai-contract`
- **Development**: `playground`, `shell`, `watch`, `fmt`, `list`, `serve`, `lsp`, `mcp`, `import-dbt`, `init-adapter`, `adapter`, `hooks`, `validate-migration`, `test-adapter`, `completions`, `bench`
- **Administration**: `history`, `replay`, `trace`, `metrics`, `optimize`, `estimate`, `compact`, `profile-storage`, `archive`, `compliance`, `retention-status`, `export-schemas`, `export-openapi`
- **Governance & Reclamation**: `policy`, `audit`, `review`, `brief`, `backfill`, `gc` — see [Governance & Reclamation Commands](/reference/commands/governance-reclamation/)
- **Diagnostics**: `doctor`, `compare`

The pages under **Reference → Commands** group these same commands by category.

## Command index

One line each, for finding the right command. Commands with a section on this page link to it.

| Command | What it does |
|---|---|
| [`init`](#rocky-init) | Scaffold a new Rocky project. |
| [`validate`](#rocky-validate) | Check `rocky.toml` for correctness, with no network calls. |
| [`discover`](#rocky-discover) | List the connectors and tables the source exposes. |
| [`plan`](#rocky-plan) | Build a reviewable record of what a run would do, without running it. |
| `apply` | Execute a plan that was already built and reviewed. |
| [`run`](#rocky-run) | Plan and apply in one step. |
| `tick` | Evaluate schedule demand once and run what is due. Experimental. |
| [`state`](#rocky-state) | Show stored watermarks, and flush the schema cache. |
| `branch` | Create, list, promote, and drop isolated output branches. |
| `compile` | Resolve dependencies, type-check, and validate contracts. |
| `lineage` | Trace a column back through every transformation that feeds it. |
| `lineage-diff` | Report the downstream blast radius of a change, for PR review. |
| `test` | Run declarative tests and fixture-driven unit tests. |
| `ci` | Compile plus test, for a CI runner with no warehouse credentials. |
| `ci-diff` | Compare a branch against a base and report what changed. |
| `preview` | Build only the changed subtree of a PR into a branch. |
| [`emit-sql`](#rocky-emit-sql) | Print the warehouse SQL a model compiles to. |
| [`catalog`](#rocky-catalog) | Write a project-wide column-level lineage snapshot to disk. |
| `dag` | Show the whole DAG: every pipeline stage and its dependencies. |
| `publish-ir` | Publish this project's compiled schema for other teams to check against. |
| `imports` | Maintain the vendored producer snapshots your project checks against. |
| [`seed`](#rocky-seed) | Load static reference CSVs into the warehouse. |
| [`snapshot`](#rocky-snapshot) | Run an SCD Type 2 snapshot pipeline. |
| [`docs`](#rocky-docs) | Generate an HTML catalog of the project. |
| [`load`](#rocky-load) | Bulk-load CSV, Parquet, or JSONL files from a directory into the warehouse. |
| [`profile`](#rocky-profile) | Report per-column row, null, and distinct counts for a model's data. |
| `ai` | Generate a model from a plain-English description. |
| `ai-sync` | Reconcile model intent against the current schema. |
| `ai-explain` | Explain what a model does in plain English. |
| `ai-test` | Draft test assertions from a model's intent. |
| `ai-contract` | Draft a data contract from a model's observed data. |
| `playground` | Create a sample DuckDB project that needs no credentials. |
| [`shell`](#rocky-shell) | Open an interactive SQL shell against the target. |
| [`watch`](#rocky-watch) | Recompile when a file in the models directory changes. |
| [`fmt`](#rocky-fmt) | Format `.rocky` files: normalize indentation, trim whitespace. |
| [`list`](#rocky-list) | List pipelines, adapters, models, sources, and dependency relationships. |
| `serve` | Start the HTTP API server exposing the compiler's semantic graph. |
| `lsp` | Start the Language Server Protocol server for IDE integration. |
| `mcp` | Serve Rocky's tools to an AI agent over MCP. |
| `import-dbt` | Convert a dbt project into Rocky models. |
| `validate-migration` | Compare a migrated project against its dbt original. |
| `init-adapter` | Scaffold a new warehouse adapter crate. |
| `adapter` | Discover and inspect process adapters installed on `$PATH`. |
| `test-adapter` | Run conformance tests against an adapter. |
| `hooks` | List and test the configured lifecycle hooks. |
| `completions` | Print a shell completion script. |
| `bench` | Run the built-in performance benchmarks. |
| `history` | Show past runs. |
| `replay` | Inspect, audit, and re-execute a past run against its record. |
| `trace` | Show what a run did over time: per-model offsets, duration bars, concurrency lanes. |
| `cost` | Roll up per-model cost for a recorded run. |
| `metrics` | Show quality metrics for a model. |
| `optimize` | Analyze materialization costs and recommend strategy changes. |
| [`estimate`](#rocky-estimate) | Estimate each transformation model's cost with warehouse `EXPLAIN`, without running it. |
| `compact` | Generate `OPTIMIZE` / `VACUUM` SQL for storage compaction. |
| `profile-storage` | Profile storage and recommend column encodings. |
| `archive` | Generate an archive plan, then apply it with `archive apply <plan-id>`. |
| [`compliance`](#rocky-compliance) | Report whether every classified column is masked as policy requires. |
| [`retention-status`](#rocky-retention-status) | Report each model's declared retention policy. |
| `export-schemas` | Write a JSON Schema file for every `--output json` payload. |
| `export-openapi` | Write an OpenAPI 3.1 document for the `rocky serve` API. |
| [`doctor`](#rocky-doctor) | Diagnose a broken setup. |
| [`compare`](#rocky-compare) | Compare shadow tables against production tables. |
| `restore` | Write a review-gated plan to rebuild an artifact that `gc` evicted. |
| `policy`, `audit`, `review`, `brief`, `backfill`, `gc` | See [Governance & Reclamation](/reference/commands/governance-reclamation/). |

## Global Flags

These flags apply to all commands.

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--config <PATH>` | `-c` | `rocky.toml` | Path to the pipeline configuration file. |
| `--output <FORMAT>` | `-o` | terminal-aware | Output format. Accepted values: `json`, `table` (plus `md`, which only `rocky brief` renders distinctly — every other command treats it as `table`). When unset, Rocky picks `table` if stdout is an interactive terminal and `json` otherwise, so piped consumers (Dagster, the LSP, CI) still receive JSON. |
| `--state-path <PATH>` | | resolved (see below) | Path to the embedded state store. When omitted, Rocky resolves to `<models>/.rocky-state.redb` (canonical) or a legacy CWD `.rocky-state.redb` (deprecated, warns on stderr). Passing the flag explicitly is always a hard override. See [`rocky state`](/reference/commands/administration/#rocky-state). |
| `--cache-ttl <SECONDS>` | | `[cache.schemas] ttl_seconds` or `86400` | Override the `DESCRIBE TABLE` schema-cache TTL for this invocation. Precedence: `--cache-ttl` > `rocky.toml` > `86400` (24 h). `--cache-ttl 0` treats every entry as instantly stale. To disable the cache entirely, set `[cache.schemas] enabled = false` in `rocky.toml`. Applies to the CLI read path only (`rocky compile`, `rocky plan`, `rocky apply`, `rocky run`, …); `rocky lsp` / `rocky serve` keep the config-derived TTL. |

```bash
# Example: use a custom config and table output
rocky -c pipelines/prod.toml -o table discover

# Force a fresh typecheck against warehouse metadata
rocky --cache-ttl 0 compile
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
| Adapters | Each `[adapter.NAME]` is a recognized type (`databricks`, `snowflake`, `duckdb`, `bigquery`, `trino`, `fivetran`, `airbyte`, `iceberg`, `manual`) with the required fields populated. |
| Pipelines | Each `[pipeline.NAME]` references existing adapters for source, target, and (optional) discovery, and its `schema_pattern` parses. |
| DAG validation | If `models/` exists, loads all models and checks for dependency cycles. |

**Output:**

Each check prints `ok` or `!!` followed by a short description. A non-zero exit code is returned if any check fails.

```
ok  Config syntax valid (v2 format)
ok  adapter.fivetran: fivetran
ok  adapter.prod: databricks (auth configured)
ok  pipeline.bronze: schema pattern parseable
ok  pipeline.bronze: replication / incremental -> warehouse / stage__{source}
```

---

### `rocky discover`

Lists available connectors and their tables from the configured source.

```bash
rocky discover [--pipeline NAME] [--with-schemas]
               [--emit-fivetran-state-to PATH] [--no-cache]
```

**Flags:**

| Flag | Description |
|------|-------------|
| `--pipeline <NAME>` | Pipeline name. Required when more than one `[pipeline.NAME]` is defined. |
| `--with-schemas` | Warm the schema cache for every discovered source. For each `(catalog, schema)` pair reachable via the source adapter, issues one `batch_describe_schema` round-trip and persists the per-table columns to `state.redb::schema_cache`. Subsequent `rocky compile` / `rocky lsp` invocations pick up the entries instead of typechecking leaf models as `Unknown`. Errors on individual sources are logged and skipped. Setting this flag with `[cache.schemas] enabled = false` errors with a clear message rather than silently no-op-ing. `DiscoverOutput.schemas_cached` records the count. |
| `--emit-fivetran-state-to <PATH>` | Write a canonical Fivetran state envelope for every Fivetran adapter in the config. See [Emitting the Fivetran state envelope](/reference/commands/core-pipeline/#emitting-the-fivetran-state-envelope). |
| `--no-cache` | Takes effect only together with `--emit-fivetran-state-to`. On its own the flag changes nothing. It makes Rocky fetch the Fivetran state envelope straight from the API and skip the read caches configured under `[adapter.<name>.cache]`. A successful fetch still writes back to that cache. It does not touch the schema cache or the state store, and an open circuit breaker still short-circuits the fetch. |

**Behavior:**

- For `fivetran` adapters, calls the Fivetran REST API to list connectors and their enabled tables. For `duckdb` adapters, queries `information_schema.{schemata,tables}`. For `manual` adapters, reads inline schema/table definitions.
- This is a **metadata-only operation**: it identifies what schemas and tables exist, it does not extract or move data.
- Parses each source schema name using the pipeline's `schema_pattern` to extract structured components (tenant, regions, source, etc.).
- Returns structured data about every discovered source and its tables.

**JSON output:**

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
rocky plan [--filter <key=value>] [--pipeline NAME] [flags]
```

**Flags:**

The two flags below are the ones most plans use. `rocky plan` accepts many more: model selection, partition selection, shadow and branch routing, `--dag`, `--semantic`, and others. [`rocky plan` in Core Pipeline Commands](/reference/commands/core-pipeline/#rocky-plan) holds the complete table.

| Flag | Required | Description |
|------|----------|-------------|
| `--filter <key=value>` | No | Filter sources by component. Example: `--filter tenant=acme`. Without it, the plan covers every discovered source. |
| `--pipeline <NAME>` | Only with several pipelines | Pipeline name. Required when `rocky.toml` defines more than one pipeline. |

**Behavior:**

- Runs discovery and drift detection.
- Generates all SQL statements (catalog creation, schema creation, incremental copy, permission grants) and returns them without execution.
- Writes the plan to `.rocky/plans/<plan-id>.json` and prints the `plan_id`. Pass that id to `rocky apply` to execute it.

**JSON output:**

```json
{
  "version": "1.6.0",
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

> Note: the canonical, auditable form is `rocky plan` followed by `rocky apply <plan-id>`. The `rocky run` single-step alias fuses plan + apply into one invocation for local iteration and automation.

Executes the full pipeline end-to-end.

```bash
rocky run [--filter <key=value>] [flags]
```

**Flags:**

| Flag | Required | Description |
|------|----------|-------------|
| `--filter <key=value>` | No | Filter sources by component. Example: `--filter tenant=acme`. Without it, the run covers every discovered source. |
| `--pipeline <NAME>` | Only with several pipelines | Pipeline name. Required when `rocky.toml` defines more than one pipeline. |
| `--governance-override <JSON>` | | Additional governance config as inline JSON or `@file.json`, merged with defaults. |
| `--models <PATH>` | | Models directory for transformation execution. |
| `--all` | | Execute both replication and compiled models. |
| `--resume <RUN_ID>` | | Resume a specific previous replication run from its last checkpoint; mints a new `run_id` and records the prior one as `resumed_from`. Rejected with `--dag`, which does not replay the resume into its sub-runs (rejected at parse time). |
| `--resume-latest` | | Resume the most recent failed replication run from its last checkpoint; mints a new `run_id` and records the prior one as `resumed_from`. Rejected with `--dag`, which does not replay the resume into its sub-runs (rejected at parse time). |
| `--shadow` | | Run in shadow mode: write to shadow targets instead of production. |
| `--shadow-suffix <SUFFIX>` | | Suffix appended to table names in shadow mode (default `_rocky_shadow`). |
| `--shadow-schema <NAME>` | | Override schema for shadow tables (mutually exclusive with `--shadow-suffix`). |
| `--branch <NAME>` | | Execute against a named branch created with `rocky branch create`. Mutually exclusive with `--shadow` / `--shadow-schema`. See [`rocky branch`](/reference/commands/core-pipeline/#rocky-branch). |
| `--idempotency-key <KEY>` | | Caller-supplied opaque key used to dedup this run against prior runs with the same key. Three outcomes: a prior run succeeded (or reached a terminal state under `dedup_on = "any"`) → exit 0 with `status = "skipped_idempotent"` and the prior `skipped_by_run_id`; another caller currently holds the claim within `in_flight_ttl_hours` → exit 0 with `status = "skipped_in_flight"`; otherwise proceed normally. Rejected when combined with `--resume` / `--resume-latest` (resume is an explicit override). Stamps are stored verbatim; do not put secrets in the key. See [`[state.idempotency]`](/reference/configuration/) for tuning. |

**Pipeline stages (in order):**

1. **Discover.** Enumerate sources and tables from the configured source adapter.
2. **Governance setup** (sequential, per matching catalog/schema):
   - Create catalog (if `auto_create_catalogs = true`)
   - Apply catalog tags (`ALTER CATALOG SET TAGS`)
   - Bind workspaces (Unity Catalog bindings API, if `governance.isolation` configured)
   - Apply catalog-level grants (`GRANT ... ON CATALOG`)
   - Create schema (if `auto_create_schemas = true`)
   - Apply schema tags (`ALTER SCHEMA SET TAGS`)
   - Apply schema-level grants (`GRANT ... ON SCHEMA`)
3. **Parallel table processing.** For each table concurrently (up to `execution.concurrency`):
   - Drift detection (compare column types between source and target)
   - Copy data (incremental or full refresh SQL)
   - Apply table tags
   - Update watermark in state store
4. **Batched checks.** Row count, column match, freshness (batched with UNION ALL for efficiency)
5. **Retry.** Failed tables retried sequentially (configurable via `execution.table_retries`)

:::note
Governance (tags, workspace bindings, permissions) is NOT a separate CLI command. It runs inline during `rocky apply` (or the `rocky run` alias) as catalogs and schemas are created. The governance features are Databricks Unity Catalog specific.
:::

**JSON output:**

```json
{
  "version": "1.6.0",
  "command": "run",
  "filter": "tenant=acme",
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
        "target_table_full_name": "acme_warehouse.staging__us_west__shopify.orders",
        "sql_hash": null
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

---

### `rocky doctor`

Runs aggregate health checks on your Rocky project: config validation, state store health, adapter connectivity, pipeline consistency, state backend configuration, live state read/write, and auth.

```bash
rocky doctor
```

**Checks performed:**

| Check | Name | Description |
|-------|------|-------------|
| Config | `config` | Parses `rocky.toml`, validates adapters and pipelines |
| State | `state` | Verifies the local state store is readable and not corrupted |
| Adapters | `adapters` | Tests connectivity to configured adapters |
| Pipelines | `pipelines` | Validates schema patterns, templates, and governance config |
| State Sync | `state_sync` | Inspects the configured remote state backend (type only) |
| State RW | `state_rw` | Round-trips a marker object against the configured backend (put → get → delete). Surfaces IAM and reachability problems at cold start instead of end-of-run upload. No-op for `local`; tiered probes both legs. |
| State Concurrency | `state_concurrency` | Reports lost-update exposure of a remote `[state]` backend. Every remote configuration warns today, with distinct messages for the three situations: `concurrency_control = "off"` (not enabled), `"cas"` on a backend that performs no compare-and-swap write (enabled but silently downgraded to an unconditional upload), and `"cas"` where it does take effect (runs and the `rocky policy` freeze/unfreeze ledger write protected, but `rocky gc` / `rocky apply` still write state unconditionally — see issue #1228). Silent for `local`. |
| Auth | `auth`, `auth/<adapter>` | Pings each warehouse and discovery adapter to verify credentials and connectivity |

**JSON output:**

```json
{
  "command": "doctor",
  "overall": "warning",
  "checks": [
    { "name": "config", "status": "healthy", "message": "rocky.toml valid", "duration_ms": 4 },
    { "name": "state", "status": "healthy", "message": "state store readable", "duration_ms": 2 },
    { "name": "adapters", "status": "warning", "message": "adapter.fivetran: API key not set", "duration_ms": 120 }
  ],
  "suggestions": [
    "Set FIVETRAN_API_KEY to enable the Fivetran discovery adapter."
  ]
}
```

Run a specific check:

```bash
rocky doctor --check auth
rocky doctor --check state_rw   # live round-trip probe against the remote state backend
```

**Verbose mode (v1.20.0+):**

```bash
rocky doctor --verbose
```

Prints extra per-check context (config path, state file size, adapter type + credential signal, pipeline kind, state backend) under each check in human-readable output. The JSON output is unchanged unless `--verbose` is passed; each `checks[]` entry then carries a `details` array of `[key, value]` string pairs (omitted entirely when empty). Credential signal values: `token`, `oauth_client`, `oauth_token`, `key_pair`, `password`, `service_account`, `adc`, `env`, `none`.

---

### `rocky list`

Inspect project contents: pipelines, adapters, models, sources, and dependency relationships.

```bash
rocky list pipelines         # List all pipeline definitions
rocky list adapters          # List all adapter configurations
rocky list models            # List all transformation models
rocky list sources           # List replication source configurations
rocky list deps <model>      # Show what a model depends on
rocky list consumers <model> # Show what depends on a model
```

All subcommands support `--output json` (via the parent `-o json` flag) for machine-readable output.

**Example (table format):**

```
$ rocky -o table list pipelines
NAME                      TYPE             TARGET               SOURCE               DEPENDS ON
playground                replication      default              default              -
```

**Example (JSON format):**

```json
{
  "version": "1.6.0",
  "command": "list_pipelines",
  "pipelines": [
    {
      "name": "playground",
      "pipeline_type": "replication",
      "target_adapter": "default",
      "source_adapter": "default",
      "depends_on": [],
      "concurrency": "16"
    }
  ]
}
```

---

### `rocky seed`

Load static reference data from CSV files into the target warehouse.

```bash
rocky seed                           # Load all seeds from seeds/
rocky seed --seeds data/seeds/       # Custom seeds directory
rocky seed --filter dim_date         # Load a specific seed by name
```

Seeds are `.csv` files in the `seeds/` directory. Rocky infers column types (STRING, BIGINT, DOUBLE, BOOLEAN, TIMESTAMP) from the data and creates/replaces the target tables. Optional `.toml` sidecars can override inferred types.

**Sidecar example** (`seeds/dim_date.toml`):
```toml
# SQL run on the warehouse around the load (root-level keys, before any table)
pre_hook  = ["CREATE SCHEMA IF NOT EXISTS warehouse.reference"]
post_hook = ["ANALYZE warehouse.reference.dim_date"]

[target]
catalog = "warehouse"
schema = "reference"
table = "dim_date"

# Override inferred column types (column name -> SQL type string)
[column_types]
date_key = "DATE"
```

**Seed hooks.** `pre_hook` and `post_hook` are lists of SQL statements the seed runs against the target warehouse, in order. Each `pre_hook` statement runs **before** the seed writes anything; each `post_hook` runs **after** the table loads successfully. A failing `pre_hook` aborts the seed before any data is written, so a guard like `pre_hook = ["SELECT 1 / COUNT(*) FROM warehouse.reference.dim_date"]` (which errors on an empty source) stops the load rather than replacing the table with bad data. These are seed-scoped relatives of the pipeline lifecycle [hooks](/concepts/hooks/), which fire shell commands and webhooks on run events rather than SQL around a single seed.

**JSON output:**
```json
{
  "version": "1.6.0",
  "command": "seed",
  "seeds_dir": "seeds",
  "tables_loaded": 1,
  "tables_failed": 0,
  "tables": [
    {
      "name": "dim_date",
      "target": "warehouse.reference.dim_date",
      "rows": 365,
      "columns": 4,
      "duration_ms": 42
    }
  ],
  "duration_ms": 55
}
```

---

### `rocky compare`

Compare shadow tables against production tables. Used after `rocky plan --shadow` + `rocky apply <plan-id>` (or the single-step `rocky run --shadow` alias) to validate results before promoting shadow data to production.

```bash
rocky compare [--filter <key=value>] [flags]
```

**Flags:**

| Flag | Required | Description |
|------|----------|-------------|
| `--filter <key=value>` | No | Filter tables by component. Without it, the comparison covers every discovered table. |
| `--pipeline <NAME>` | Only with several pipelines | Pipeline name. Required when `rocky.toml` defines more than one pipeline. |
| `--shadow-suffix <SUFFIX>` | | Shadow table suffix (default `_rocky_shadow`). |
| `--shadow-schema <NAME>` | | Override schema for shadow tables. |

**JSON output:**

```json
{
  "version": "1.6.0",
  "command": "compare",
  "filter": "tenant=acme",
  "tables_compared": 1,
  "tables_passed": 1,
  "tables_warned": 0,
  "tables_failed": 0,
  "results": [
    {
      "production_table": "warehouse.staging.orders",
      "shadow_table": "warehouse.staging.orders_rocky_shadow",
      "row_count_match": true,
      "production_count": 15000,
      "shadow_count": 15000,
      "row_count_diff_pct": 0.0,
      "schema_match": true,
      "schema_diffs": [],
      "verdict": "pass"
    }
  ],
  "overall_verdict": "pass"
}
```

---

### `rocky state`

Inspect or manage the embedded state store. `rocky state` is a subcommand group; bare `rocky state` continues to display watermarks for backwards compatibility.

```bash
rocky state                                # show watermarks (default)
rocky state show                           # same as bare `rocky state`
rocky state clear-schema-cache [--dry-run] # flush the DESCRIBE cache
```

**Subcommands:**

| Subcommand | Description |
|------------|-------------|
| `show` (default) | Display stored watermarks. |
| `clear-schema-cache` | Remove every entry from the `SCHEMA_CACHE` redb table. `--dry-run` reports what would be removed without touching the store. A missing state store is a no-op (CI-safe on ephemeral runners). Emits `ClearSchemaCacheOutput`. See [`rocky state clear-schema-cache`](/reference/commands/administration/#rocky-state-clear-schema-cache). |

**State-path resolution (v1.16.0):**

When `--state-path` is not passed, Rocky resolves the state file via `rocky_core::state::resolve_state_path`:

1. `<models>/.rocky-state.redb`: canonical location for new projects; matches the LSP convention so inlay hints observe the same file `rocky apply` writes.
2. Legacy `.rocky-state.redb` in CWD: still works; emits a one-time deprecation warning on stderr.
3. Both present: CWD wins (to preserve existing watermarks / branches / partitions); a louder warning asks you to reconcile. Merge is lossy.
4. Neither present: fresh project lands on `<models>/.rocky-state.redb` when a `models/` directory exists, otherwise CWD.

Explicit `--state-path <PATH>` always overrides the resolver.

**`rocky state` behavior (show):**

- Reads the redb state store at the resolved path.
- Lists every tracked table with its last watermark value and the timestamp it was recorded.

**JSON output:**

```json
{
  "version": "1.6.0",
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

---

### `rocky snapshot`

Execute an SCD Type 2 snapshot pipeline. Generates and runs MERGE SQL that tracks historical changes to a source table, maintaining `valid_from`, `valid_to`, `is_current`, and `snapshot_id` columns in the target history table.

```bash
rocky snapshot                          # Run the snapshot pipeline
rocky snapshot --dry-run                # Preview generated SQL without executing
rocky snapshot --pipeline customers_scd # Select a specific pipeline
```

**Flags:**

| Flag | Description |
|------|-------------|
| `--pipeline <NAME>` | Pipeline name. Required when more than one pipeline is defined. |
| `--dry-run` | Show generated SQL without executing. |

**Pipeline config** (`rocky.toml`):

```toml
[pipeline.customers_history]
type = "snapshot"
unique_key = ["customer_id"]
updated_at = "updated_at"
invalidate_hard_deletes = true

[pipeline.customers_history.source]
adapter = "prod"
catalog = "main"
schema = "raw"
table = "customers"

[pipeline.customers_history.target]
adapter = "prod"
catalog = "warehouse"
schema = "history"
table = "customers_history"
```

**Strategies:**

- **Timestamp.** Detects changes by comparing the `updated_at` column between source and target. Efficient when the source maintains a reliable last-modified timestamp.
- **Check.** Detects changes by comparing specified columns between source and target. Used when there is no reliable timestamp.

**Generated SQL steps:**

1. **Initial load.** `CREATE TABLE IF NOT EXISTS` with SCD2 columns added
2. **Close changed rows.** MERGE that sets `valid_to` and `is_current = FALSE`
3. **Insert new versions.** INSERT for rows that were just closed
4. **Invalidate hard deletes** (optional). UPDATE rows missing from source

**JSON output:**

```json
{
  "version": "1.6.0",
  "command": "snapshot",
  "pipeline": "customers_history",
  "source": "main.raw.customers",
  "target": "warehouse.history.customers_history",
  "dry_run": false,
  "steps_total": 4,
  "steps_ok": 4,
  "steps": [
    { "step": "initial_load", "sql": "...", "status": "ok", "duration_ms": 12 },
    { "step": "merge_1", "sql": "...", "status": "ok", "duration_ms": 45 }
  ],
  "duration_ms": 120
}
```

---

### `rocky docs`

Generate project documentation as a single-page HTML catalog. Discovers models from the models directory and renders them with metadata, dependencies, and tests.

```bash
rocky docs                                        # Generate to docs/catalog.html
rocky docs --models models/ --output-path site/api.html  # Custom paths
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--models <PATH>` | `models` | Models directory to scan. |
| `--output-path <PATH>` | `docs/catalog.html` | Output HTML file path. |

**Behavior:**

- Loads all `.sql` and `.rocky` model files with their TOML sidecars.
- Extracts: name, description (from `intent`), target table, strategy, dependencies, tests.
- Renders a self-contained HTML page with dark theme, search, and model cards.
- No external dependencies; the HTML is fully self-contained.

**JSON output:**

```json
{
  "version": "1.6.0",
  "command": "docs",
  "output_path": "docs/catalog.html",
  "models_count": 12,
  "pipelines_count": 2,
  "duration_ms": 15
}
```

---

### `rocky emit-sql`

Render the SQL each transformation model would produce, without a warehouse connection and without running anything. Rocky reduces your models to plain SQL wherever it can. The behavior notes below say which models emit a statement you can run as-is, which need an existing target, and which emit nothing. See [No lock-in](/guides/no-lock-in/) for the full workflow.

```bash
rocky emit-sql                                   # Print SQL for every model to stdout
rocky emit-sql --out-dir build/sql/              # Write one <model>.sql file per model
rocky emit-sql --model stg_orders --out-dir sql/ # Emit a single model
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--models <PATH>` | `models` | Models directory to compile. |
| `--model <NAME>` | (all) | Restrict output to a single model by name. |
| `--out-dir <PATH>` | (stdout) | Write one `<model>.sql` file per model into this directory, in dependency order. When omitted, the concatenated SQL is printed to stdout, also in dependency order. |

**Behavior:**

- Compiles the project offline and generates SQL through the same path `rocky run` uses, including declared surrogate-key columns, so the emitted statements match what a run executes.
- The dialect is the project's configured target adapter type, resolved from `rocky.toml` without credentials. With no resolvable config it defaults to DuckDB. All models render in this one resolved dialect, so for a project whose models target more than one adapter, the emitted SQL matches `rocky run` only for the models whose target uses that dialect.
- **Full-refresh models.** Emit a complete `CREATE OR REPLACE TABLE … AS …` that runs as-is against a fresh warehouse and matches what a run executes in the resolved dialect.
- **Incremental and merge models.** Emit their steady-state statement (a bare `INSERT` / `MERGE` against an existing target). `rocky run` bootstraps the target table on first build and threads the incremental watermark from state, neither of which a static emit can reproduce, so each such file carries a leading `-- NOTE:` comment to that effect.
- Models that produce no standalone SQL are reported on stderr rather than silently dropped. This covers ephemeral models (inlined as CTEs) and strategies that cannot render offline, such as Snowflake dynamic tables, which need a live compute-warehouse name.

This command prints SQL or writes files; it has no JSON output mode.

---

### `rocky catalog`

Emit a project-wide column-level lineage snapshot as a persisted catalog artifact, so any non-Rocky consumer can read column-level lineage without invoking the engine.

```bash
rocky catalog                              # Write all artifacts to ./.rocky/catalog/
rocky catalog --out build/catalog/         # Custom output directory
rocky catalog --format json               # Emit only catalog.json
rocky catalog --catalog acme_warehouse     # Scope to a single warehouse catalog
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--models <PATH>` | `models` | Models directory to compile. |
| `--out <PATH>` | `./.rocky/catalog/` | Output directory for the catalog artifacts. |
| `--format <FORMAT>` | `both` | Which artefact family to emit. `json` writes only `catalog.json`; `parquet` writes only `edges.parquet` and `assets.parquet`; `both` writes all three. |
| `--catalog <NAME>` | (all) | Scope the snapshot to a single warehouse catalog. Only assets whose fully-qualified name sits in the named catalog are emitted, and edges referencing dropped assets are pruned. Mirrors `compact --catalog` and `archive --catalog`. |

**Artifacts:**

- `catalog.json` is the single-file front door for the snapshot.
- `edges.parquet` holds one row per column-lineage edge.
- `assets.parquet` holds one row per asset column.

**Behavior:**

- JSON output is [`CatalogOutput`](/reference/json-output/). Under `--output json` the same `CatalogOutput` is mirrored to stdout, independent of `--format`, so a consumer can pipe it without re-reading the written files.

---

### `rocky shell`

Interactive SQL shell against the configured warehouse. Supports multi-line queries, `.tables` and `.schema` meta-commands, and command history.

```bash
rocky shell                    # Use default adapter
rocky shell --pipeline prod    # Use a specific pipeline's adapter
```

**Flags:**

| Flag | Description |
|------|-------------|
| `--pipeline <NAME>` | Pipeline name to select the warehouse adapter. |

**Meta-commands:**

| Command | Description |
|---------|-------------|
| `.tables` | List tables in the current catalog/schema. |
| `.schema <table>` | Describe columns for a table. |
| `.quit` / `.exit` | Exit the shell. |

Multi-line queries are supported; end a statement with `;` to execute.

---

### `rocky watch`

Watch the models directory for file changes and auto-recompile. Useful during development to get instant feedback on model changes.

```bash
rocky watch                              # Watch models/ directory
rocky watch --models src/models/         # Custom directory
rocky watch --contracts contracts/       # Include contracts
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--models <PATH>` | `models` | Models directory to watch. |
| `--contracts <PATH>` | | Contracts directory (optional). |

**Behavior:**

- Uses filesystem notifications (platform-native) to detect changes.
- Debounces rapid changes (waits for writes to settle before recompiling).
- Runs `compile` on each change and reports diagnostics to the terminal.

---

### `rocky fmt`

Format `.rocky` DSL files. Normalizes indentation, trims trailing whitespace, and enforces consistent style.

```bash
rocky fmt                    # Format all .rocky files in current directory
rocky fmt models/            # Format a specific directory
rocky fmt --check            # Check mode: exit non-zero if any file needs formatting
```

**Flags:**

| Flag | Description |
|------|-------------|
| `--check` | Check mode for CI; exits non-zero if any file would be reformatted. |

**Arguments:**

| Argument | Default | Description |
|----------|---------|-------------|
| `paths` | `.` | Files or directories to format. |

---

### `rocky compliance`

Governance rollup over classification sidecars plus the project `[mask]` policy. Answers: "are all classified columns masked wherever policy says they should be?" Static resolver, no warehouse calls.

```bash
rocky compliance [--env NAME] [--exceptions-only] [--fail-on exception]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--env <NAME>` | (expand all) | Scope the report to a single environment (e.g. `prod`). When unset, the report expands across the defaults plus every `[mask.<env>]` override block. |
| `--exceptions-only` | `false` | Filter `per_column` to rows that produced at least one exception. The `exceptions` list is unaffected. |
| `--fail-on <CONDITION>` | | Gate condition. The only supported value is `exception`; it exits `1` when any exception is emitted. Useful as a CI gate to block merges that leave classified columns unmasked. |
| `--models <PATH>` | `models` | Models directory to scan for `[classification]` sidecars. |

**Behavior:**

- Walks every model's `[classification]` sidecar block and, for each `(model, column, env)` triple, resolves the masking strategy from `[mask]` / `[mask.<env>]`.
- `MaskStrategy::None` counts as masked; an explicit-identity policy is a conscious decision, not an enforcement gap.
- Tags listed under `[classifications] allow_unmasked` suppress exception emission but still report `enforced = false` in the per-column breakdown.
- JSON output is [`ComplianceOutput`](/reference/json-output/) (`summary` / `per_column` / `exceptions`).

---

### `rocky retention-status`

Report each model's declared data-retention policy (`retention = "<N>[dy]"` in the model sidecar).

```bash
rocky retention-status [--model NAME] [--drift]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--model <NAME>` | (all) | Scope the report to a single model. |
| `--drift` | `false` | Keep only the models that declare a policy, and probe the warehouse for the value it currently applies. |
| `--models <PATH>` | `models` (via `rocky.toml`) | Models directory. |

**Behavior:**

- Compiles the project, then emits one `ModelRetentionStatus` per model with `configured_days`, `warehouse_days`, and `in_sync`.
- Models without a `retention` sidecar value report `configured_days = null` and `in_sync = true`.
- `--drift` probes the warehouse through the governance adapter. Databricks reads the Delta table properties; Snowflake reads `DATA_RETENTION_TIME_IN_DAYS`. BigQuery and DuckDB have no probe, so they report `warehouse_days = null`.
- A probe failure prints a warning on stderr for that model and leaves `warehouse_days` null. It does not fail the command.
- JSON output is [`RetentionStatusOutput`](/reference/json-output/).

**An unknown `--model` fails.** `rocky retention-status --model <NAME>` exits `1` when no model carries that name. Stderr reads `model '<NAME>' not found (no transformation model with that name)`, and stdout stays empty even under `--output json`.

**An empty `--drift` result says why it is empty.** `--drift` keeps only the models that declare a policy, so it can legitimately return nothing. That case exits `0` and the JSON payload gains a `message` field, absent whenever `models` is non-empty.

| Situation | `message` |
|---|---|
| `--drift --model <NAME>`, and that model declares no policy | `model '<NAME>' declares no retention policy` |
| `--drift` with no model selected, and none declares a policy | `no models declare a retention policy` |

:::caution[This is a behavior change]
Earlier engine versions exited `0` for an unknown `--model` and returned an empty `models` array. A CI job that treated the empty array as a pass now fails on a bad selector.
:::

---

### `rocky load`

Bulk-load data files from a directory into the warehouse. Rocky reads CSV, Parquet, and JSONL, and infers the format from the file extension unless you pin it.

```bash
rocky load                              # Load from the pipeline's configured directory
rocky load --source-dir data/dropbox/   # Load from a specific directory
rocky load --format parquet --truncate  # Empty each target before its file loads
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--source-dir <PATH>` | (from pipeline config) | Directory holding the data files. Overrides the pipeline's configured location. |
| `--format <FORMAT>` | auto-detect | `csv`, `parquet`, or `jsonl`. Detected from the file extension when unset. |
| `--target <NAME>` | derived from file name | Target table name. Pins every file in the directory to this one table. |
| `--pipeline <NAME>` | | Pipeline name. Required when more than one pipeline is defined. |
| `--truncate` | `false` | Empty the target table before each file loads. Read the warning below first. |

:::caution[`--truncate` empties the target once per file, not once per command]
Rocky loads the files one at a time, and `--truncate` deletes every row of the target before each one. When several files share a single target table, **only the last file's rows survive**. Each earlier file's rows are deleted by the next file's truncate. Rocky loads the directory in sorted filename order, so the last name wins.

Files share one target when you pass `--target <NAME>`, or when the pipeline config sets `target.table`. With neither, Rocky derives the table name from each file's own name. Each file then lands in its own table, and the truncates do not erase each other.

To combine several files into one table, leave `--truncate` off. Empty that table yourself first if you need a clean replacement.
:::

A `load` pipeline re-ingests every file it finds on each run rather than tracking what it already read. That is why a `load` pipeline cannot join the [`[pipeline.NAME.schedule]`](/reference/configuration/#pipelinenameschedule) graph: scheduling one would duplicate data. `rocky validate` rejects that config with `V044`.

---

### `rocky profile`

Report what is actually in a model's data, column by column: row count, null count, and distinct count. Use it before you write a contract or a test, so the assertion matches the data. DuckDB only.

```bash
rocky profile fct_orders                  # Profile every column
rocky profile fct_orders --column amount  # Profile one column
```

**Arguments and flags:**

| Argument / flag | Default | Description |
|------|---------|-------------|
| `model` | required | Model to profile. Rocky profiles its target table, or a source table when the target does not exist yet. |
| `--column <NAME>` | (every column) | Profile only this column. |
| `--models <PATH>` | `models` | Models directory. Rocky compiles it to obtain the model's inferred schema. |

**Which table Rocky profiles.** Rocky profiles the model's target table when that table is materialized. When it is not, Rocky profiles the first source table it can resolve instead, so you still get observed numbers before the first `rocky run`. On that fallback path Rocky skips any column the source does not have. The JSON output names the table it read under `profiled_table` and the missing target under `fell_back_from`. The text output prints neither field, so read the JSON when you need to know which table the numbers came from.

**Minimum and maximum.** `--output json` carries a `min` and a `max` for every column. The text output prints the row, null, and distinct counts only.

---

### `rocky ai-contract`

Draft a data contract from a model's observed data, rather than writing the column list by hand. Rocky profiles the target table, sends the shape to Anthropic, and prints the drafted contract. DuckDB only.

```bash
rocky ai-contract fct_orders           # Print the draft to stdout
rocky ai-contract fct_orders --save    # Write <model>.contract.toml
```

**Arguments and flags:**

| Argument / flag | Default | Description |
|------|---------|-------------|
| `model` | required | Model whose target table to profile and draft a contract for. |
| `--save` | `false` | Write the draft to `<model>.contract.toml` in the models directory instead of printing it. |
| `--with-data` | `false` | Include observed cell **values** (min/max plus low-cardinality domain samples) in the prompt. Off by default: without it, only the schema and aggregate statistics — row, null, and distinct counts — leave the machine. Turn it on when sending sample values is acceptable for that table. |
| `--models <PATH>` | `models` | Models directory, and the destination when `--save` is passed. |

Rocky reads the API key from the `ANTHROPIC_API_KEY` environment variable, never from `rocky.toml`. See [`[ai]`](/reference/configuration/#ai) for the token budget.

---

### `rocky publish-ir`

Publish this project's compiled schema so another team can check their models against it. Rocky compiles the project and writes its typed `ProjectIr` as JSON. The consumer vendors that file and points an [`[imports.<name>]`](/reference/configuration/#importsname) block at it; their `rocky compile` then fails (`E030`) when you drop a column they still read.

```bash
rocky publish-ir --with-seed --out project-ir.json
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--models <PATH>` | `models` | Models directory. |
| `--contracts <PATH>` | | Contracts directory. |
| `--out <PATH>` | `project-ir.json` | Where to write the snapshot JSON. |
| `--with-seed` | `false` | Run `data/seed.sql` against an in-memory DuckDB before compiling, so leaf models resolve to concrete column types in the snapshot. |

Pass `--with-seed` for a self-contained DuckDB producer. Without concrete types, the snapshot gives the consumer's contract nothing to check against.

---

### `rocky imports`

Maintain the vendored producer snapshots your project checks against. Nothing advances a baseline on its own: advancing it is your explicit statement that you reviewed the producer's change and accept it.

```bash
rocky imports update           # Advance every baseline to its current snapshot
rocky imports update --check   # CI guard: report what is behind, write nothing
```

**Subcommand `update` flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--check` | `false` | Read-only. Report what is out of date and exit non-zero without writing anything. |

`update` reports a stale `pin` but never rewrites `rocky.toml`. See [Cross-team contracts](/concepts/cross-team-contracts/) and [`[imports.<name>]`](/reference/configuration/#importsname).

---

### `rocky estimate`

Estimate what your transformation models would cost before you run them. Rocky loads the models directory, generates each model's SQL, and asks the warehouse to `EXPLAIN` it. Nothing materializes.

`rocky estimate` prices the transformation models only. It does not estimate a replication pipeline's tables, and it does not price the rest of a run.

```bash
rocky estimate                    # Estimate every model
rocky estimate --model fct_orders # Estimate one model
rocky estimate --verbose          # Show the full EXPLAIN plan and pricing rates
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--models <PATH>` | `models` | Models directory. |
| `--model <NAME>` | (all) | Estimate a single model. |
| `--pipeline <NAME>` | | Pipeline name. Required when more than one pipeline is defined. |
| `--verbose` | `false` | Print extra context per model: the full `EXPLAIN` plan, the pricing rates used, and any models skipped before `EXPLAIN`. |

**Where the prices come from.** Rocky carries one built-in rate table per adapter type: Databricks, Snowflake, BigQuery, and DuckDB. It picks the table matching the pipeline's target adapter. An unrecognized adapter type falls back to the Databricks rates, and `--verbose` labels that as a fallback. `rocky estimate` does not read the [`[cost]`](/reference/configuration/#cost) block, so editing those keys does not move these numbers. For a recommendation rather than an estimate, use `rocky optimize`.

**An unknown `--model` fails.** `rocky estimate --model <NAME>` exits `1` when no model carries that name. Stderr reads `model '<NAME>' not found (no transformation model with that name)`, and stdout stays empty even under `--output json`.

**An empty result says why it is empty.** A run that produces no estimate still exits `0`. Its JSON payload gains a `message` field, absent whenever `estimates` is non-empty.

| Situation | `message` |
|---|---|
| The project has no models to estimate | `no models found to estimate` |
| Models were selected, but SQL generation or `EXPLAIN` failed for every one | `no model produced an estimate` |

:::caution[This is a behavior change]
Earlier engine versions exited `0` for an unknown `--model` and returned an empty `estimates` array. They also emitted that bare array with no `message`, so text output said `No models found.` while JSON output said nothing. A CI job that treated the empty array as a pass now fails on a bad selector.
:::

---

### `rocky bench`

Run Rocky's built-in performance benchmarks, and compare a run against a saved baseline. Useful when a change might have slowed compilation down.

```bash
rocky bench                              # Run compile, dag, and sql_gen
rocky bench startup                      # Run the startup group
rocky bench compile --models 500         # Compile benchmark at 500 models
rocky bench --save baseline.json         # Record a baseline
rocky bench --compare baseline.json      # Compare against it
```

**Arguments and flags:**

| Argument / flag | Default | Description |
|------|---------|-------------|
| `group` | `all` | Benchmark group: `compile`, `dag`, `sql_gen`, `startup`, or `all`. `all` runs `compile`, `dag`, and `sql_gen`. It leaves `startup` out, so name that group to run it. |
| `--models <N>` | | Number of models to generate for the compile benchmarks. |
| `--format <FORMAT>` | `table` | `json` for machine-readable output. |
| `--save <PATH>` | | Write the results to a JSON baseline file. |
| `--compare <PATH>` | | Compare the results against a saved baseline file. |

---

### `rocky completions`

Print a shell completion script. Write it wherever your shell reads completions from.

```bash
rocky completions zsh  > ~/.zsh/completions/_rocky
rocky completions bash > /etc/bash_completion.d/rocky
rocky completions fish > ~/.config/fish/completions/rocky.fish
```

**Arguments:**

| Argument | Description |
|------|-------------|
| `shell` | Target shell: `bash`, `elvish`, `fish`, `powershell`, or `zsh`. |

---

### `rocky export-schemas`

Write a JSON Schema file for every `--output json` payload the CLI emits. The Python SDK and the VS Code extension generate their bindings from these files, so one Rust definition drives all three languages.

```bash
rocky export-schemas schemas/
```

**Arguments:**

| Argument | Default | Description |
|------|---------|-------------|
| `output_dir` | `schemas` | Directory to write the `.schema.json` files into. |

---

### `rocky export-openapi`

Write an OpenAPI 3.1 document describing the `rocky serve` HTTP API. Rocky assembles `components/schemas` from the same registry `export-schemas` uses, and builds `paths` from the `/api/v1` route table. It validates the result against the OpenAPI 3.1 meta-schema before writing it.

```bash
rocky export-openapi docs/public/openapi.json
```

**Arguments:**

| Argument | Default | Description |
|------|---------|-------------|
| `output_path` | `docs/public/openapi.json` | Where to write the OpenAPI document (`.json`). |

See [Embedding Rocky](/guides/embedding/) for the API itself.
