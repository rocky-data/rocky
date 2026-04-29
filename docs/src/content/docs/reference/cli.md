---
title: CLI Reference
description: Complete reference for all Rocky CLI commands and flags
sidebar:
  order: 1
---

Rocky provides a single binary with subcommands for the full pipeline lifecycle. Commands are organized into categories:

- **Core Pipeline**: `init`, `validate`, `discover`, `plan`, `run`, `state`, `branch`
- **Modeling**: `compile`, `lineage`, `test`, `ci`, `ci-diff`, `preview`
- **Data**: `seed`, `snapshot`, `docs`
- **AI**: `ai`, `ai-sync`, `ai-explain`, `ai-test`
- **Development**: `playground`, `shell`, `watch`, `fmt`, `list`, `serve`, `lsp`, `import-dbt`, `init-adapter`, `hooks`, `validate-migration`, `test-adapter`
- **Administration**: `history`, `replay`, `trace`, `metrics`, `optimize`, `compact`, `profile-storage`, `archive`, `compliance`, `retention-status`
- **Diagnostics**: `doctor`, `compare`

See the dedicated command reference pages for detailed documentation of each category.

## Global Flags

These flags apply to all commands.

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--config <PATH>` | `-c` | `rocky.toml` | Path to the pipeline configuration file. |
| `--output <FORMAT>` | `-o` | `json` | Output format. Accepted values: `json`, `table`. |
| `--state-path <PATH>` | | resolved (see below) | Path to the embedded state store. When omitted, Rocky resolves to `<models>/.rocky-state.redb` (canonical) or a legacy CWD `.rocky-state.redb` (deprecated, warns on stderr). Passing the flag explicitly is always a hard override. See [`rocky state`](/reference/commands/administration/#rocky-state). |
| `--cache-ttl <SECONDS>` | | `[cache.schemas] ttl_seconds` or `86400` | Override the `DESCRIBE TABLE` schema-cache TTL for this invocation. Precedence: `--cache-ttl` > `rocky.toml` > `86400` (24 h). `--cache-ttl 0` treats every entry as instantly stale. To disable the cache entirely, set `[cache.schemas] enabled = false` in `rocky.toml`. Applies to the CLI read path only (`rocky compile`, `rocky run`, …); `rocky lsp` / `rocky serve` keep the config-derived TTL. |

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
ok  pipeline.bronze: replication / incremental -> warehouse / stage__{source}
```

---

### `rocky discover`

Lists available connectors and their tables from the configured source.

```bash
rocky discover [--pipeline NAME] [--with-schemas]
```

**Flags:**

| Flag | Description |
|------|-------------|
| `--pipeline <NAME>` | Pipeline name. Required when more than one `[pipeline.NAME]` is defined. |
| `--with-schemas` | Warm the schema cache for every discovered source. For each `(catalog, schema)` pair reachable via the source adapter, issues one `batch_describe_schema` round-trip and persists the per-table columns to `state.redb::schema_cache`. Subsequent `rocky compile` / `rocky lsp` invocations pick up the entries instead of typechecking leaf models as `Unknown`. Errors on individual sources are logged and skipped. Setting this flag with `[cache.schemas] enabled = false` errors with a clear message rather than silently no-op-ing. `DiscoverOutput.schemas_cached` records the count. |

**Behavior:**

- For `fivetran` adapters, calls the Fivetran REST API to list connectors and their enabled tables. For `duckdb` adapters, queries `information_schema.{schemata,tables}`. For `manual` adapters, reads inline schema/table definitions.
- This is a **metadata-only operation** — it identifies what schemas and tables exist, it does not extract or move data.
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
| `--branch <NAME>` | | Execute against a named branch created with `rocky branch create`. Mutually exclusive with `--shadow` / `--shadow-schema`. See [`rocky branch`](/reference/commands/core-pipeline/#rocky-branch). |
| `--idempotency-key <KEY>` | | Caller-supplied opaque key used to dedup this run against prior runs with the same key. Three outcomes: a prior run succeeded (or reached a terminal state under `dedup_on = "any"`) → exit 0 with `status = "skipped_idempotent"` and the prior `skipped_by_run_id`; another caller currently holds the claim within `in_flight_ttl_hours` → exit 0 with `status = "skipped_in_flight"`; otherwise proceed normally. Rejected when combined with `--resume` / `--resume-latest` (resume is an explicit override). Stamps are stored verbatim — do not put secrets in the key. See [`[state.idempotency]`](/reference/configuration/) for tuning. |

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

Load static reference data from CSV files into the target warehouse. Rocky's equivalent of dbt's `dbt seed`.

```bash
rocky seed                           # Load all seeds from seeds/
rocky seed --seeds data/seeds/       # Custom seeds directory
rocky seed --filter name=dim_date    # Load specific seed
```

Seeds are `.csv` files in the `seeds/` directory. Rocky infers column types (STRING, BIGINT, DOUBLE, BOOLEAN, TIMESTAMP) from the data and creates/replaces the target tables. Optional `.toml` sidecars can override inferred types.

**Sidecar example** (`seeds/dim_date.toml`):
```toml
[target]
catalog = "warehouse"
schema = "reference"
table = "dim_date"

[[columns]]
name = "date_key"
data_type = "DATE"
```

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

Compare shadow tables against production tables. Used after `rocky run --shadow` to validate results before promoting shadow data to production.

```bash
rocky compare --filter <key=value> [flags]
```

**Flags:**

| Flag | Required | Description |
|------|----------|-------------|
| `--filter <key=value>` | Yes | Filter tables by component. |
| `--pipeline <NAME>` | | Pipeline name. |
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

1. `<models>/.rocky-state.redb` — canonical location for new projects; matches the LSP convention so inlay hints observe the same file `rocky run` writes.
2. Legacy `.rocky-state.redb` in CWD — still works; emits a one-time deprecation warning on stderr.
3. Both present — CWD wins (to preserve existing watermarks / branches / partitions); a louder warning asks you to reconcile. Merge is lossy.
4. Neither present — fresh project lands on `<models>/.rocky-state.redb` when a `models/` directory exists, otherwise CWD.

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

- **Timestamp** — detects changes by comparing the `updated_at` column between source and target. Efficient when the source maintains a reliable last-modified timestamp.
- **Check** — detects changes by comparing specified columns between source and target. Used when there is no reliable timestamp.

**Generated SQL steps:**

1. **Initial load** — `CREATE TABLE IF NOT EXISTS` with SCD2 columns added
2. **Close changed rows** — MERGE that sets `valid_to` and `is_current = FALSE`
3. **Insert new versions** — INSERT for rows that were just closed
4. **Invalidate hard deletes** (optional) — UPDATE rows missing from source

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
rocky docs --models models/ --output site/api.html  # Custom paths
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--models <PATH>` | `models` | Models directory to scan. |
| `--output <PATH>` | `docs/catalog.html` | Output HTML file path. |

**Behavior:**

- Loads all `.sql` and `.rocky` model files with their TOML sidecars.
- Extracts: name, description (from `intent`), target table, strategy, dependencies, tests.
- Renders a self-contained HTML page with dark theme, search, and model cards.
- No external dependencies — the HTML is fully self-contained.

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

Multi-line queries are supported — end a statement with `;` to execute.

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
| `--check` | Check mode for CI — exits non-zero if any file would be reformatted. |

**Arguments:**

| Argument | Default | Description |
|----------|---------|-------------|
| `paths` | `.` | Files or directories to format. |

---

### `rocky compliance`

Governance rollup over classification sidecars plus the project `[mask]` policy. Answers: "are all classified columns masked wherever policy says they should be?" Static resolver — no warehouse calls.

```bash
rocky compliance [--env NAME] [--exceptions-only] [--fail-on exception]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--env <NAME>` | (expand all) | Scope the report to a single environment (e.g. `prod`). When unset, the report expands across the defaults plus every `[mask.<env>]` override block. |
| `--exceptions-only` | `false` | Filter `per_column` to rows that produced at least one exception. The `exceptions` list is unaffected. |
| `--fail-on <CONDITION>` | | Gate condition. The only supported value is `exception` — exits `1` when any exception is emitted. Useful as a CI gate to block merges that leave classified columns unmasked. |
| `--models <PATH>` | `models` | Models directory to scan for `[classification]` sidecars. |

**Behavior:**

- Walks every model's `[classification]` sidecar block and, for each `(model, column, env)` triple, resolves the masking strategy from `[mask]` / `[mask.<env>]`.
- `MaskStrategy::None` counts as masked — an explicit-identity policy is a conscious decision, not an enforcement gap.
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
| `--drift` | `false` | Stretch goal reserved for v2 — today this filters output to models with a declared policy and leaves `warehouse_days` null. The warehouse probe (`SHOW TBLPROPERTIES` / `SHOW PARAMETERS`) is deferred. |
| `--models <PATH>` | `models` (via `rocky.toml`) | Models directory. |

**Behavior:**

- Compiles the project, then emits one `ModelRetentionStatus` per model with `configured_days`, `warehouse_days` (always `None` in v1), and `in_sync`.
- Models without a `retention` sidecar value report `configured_days = null` and `in_sync = true`.
- JSON output is [`RetentionStatusOutput`](/reference/json-output/).
