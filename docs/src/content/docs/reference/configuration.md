---
title: Configuration
description: Complete rocky.toml reference
sidebar:
  order: 2
---

Rocky reads one `rocky.toml` file for everything. You name your adapters (`[adapter.NAME]`) and your pipelines (`[pipeline.NAME]`), so one file can hold several sources, several warehouses, and several pipelines side by side.

<span id="at-a-glance"></span>
## Config at a glance

:::caution[A key that does not exist is usually an error, not a no-op]
A typo (`tooken` for `token`, `retires` for `retries`) fails the config load in most blocks, and the message names the key. In the lenient blocks a clean `rocky validate` is not evidence that Rocky read your key. Check every key against this page.
:::

| Block | What happens to an unknown key |
|---|---|
| `[pipeline.NAME.source]`, `[pipeline.NAME.target]`, `[pipeline.NAME.checks.quarantine]` | Refused. The error names the key. |
| Directly under `[pipeline.NAME]` | Accepted and ignored, on every pipeline type. `strategyy = "x"` loads and `rocky validate` reports the config valid. |
| A check toggle such as `[pipeline.NAME.checks.row_count]`, and an `[[pipeline.NAME.checks.assertions]]` entry | Ignored. `severty = "warning"` is dropped. |
| `[hook.*]` | An unknown event key is ignored silently. |
| `[adapter.NAME.extra]` | Every key passes through untouched. |

Top-level blocks, in page order: `[adapter.NAME]`, `[pipeline.NAME]`, `[state]`, `[connector.NAME]`, `[run]`, `[reuse]`, `[resilience]`, `[retry]`, `[budget]`, `[product_budget.NAME]`, `[cost]`, `[cache]`, `[freshness]`, `[schedule]`, `[contracts]`, `[imports.NAME]`, `[lint]`, `[portability]`, `[mask]`, `[classifications]`, `[role.NAME]`, `[policy]`, `[residency]`, `[branch]`, `[gc]`, `[selectors]`, `[ai]`, `[fulfill]` and `[hook.*]`. `[schedule]` holds project defaults for [`[pipeline.NAME.schedule]`](#pipelinenameschedule). `[schema_evolution]` was [removed](#schema_evolution-removed): a config that declares it fails to load.

A minimal full config. It runs a DuckDB replication with no credentials:

```toml
[adapter.local]
type = "duckdb"
path = "warehouse.duckdb"

[pipeline.replication]
type = "replication"
strategy = "full_refresh"

[pipeline.replication.source]
adapter = "local"

[pipeline.replication.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.replication.target]
adapter = "local"
catalog_template = "warehouse"
schema_template = "analytics"

[state]
backend = "local"
```

A config can declare more adapters and pipelines. A pipeline picks its adapters with `adapter = "..."` on `source` and `target`.

## Config Inference and Defaults

Write less by leaning on the defaults. Rocky fills in most fields, so a small project needs only a handful of lines.

| What | Default | When to omit |
|---|---|---|
| `pipeline.type` | `"replication"` | Always (unless using a different type) |
| Unnamed `[adapter]` with a `type` key | Auto-wraps as `adapter.default`. Then `source.adapter` and `target.adapter` can be left out. | Single-adapter projects |
| Pipeline `adapter` refs | `"default"` | When only one adapter is defined |
| `[state]\nbackend = "local"` | `"local"` | Local development (always the default) |
| `auto_create_catalogs` / `auto_create_schemas` | `false` | When you don't need auto-creation |
| Model sidecar `name` | Filename stem | When file is `fct_orders.toml` and name is `fct_orders` |
| Model sidecar `target.table` | Same as `name` | When table name matches model name |
| `models/_defaults.toml` | N/A | Provides directory-level `[target]` defaults for `catalog` and `schema` |

### Directory-level defaults

Create a `models/_defaults.toml` so you do not repeat `[target]` in every model:

```toml
# models/_defaults.toml
[target]
catalog = "analytics"
schema = "warehouse"
```

Each model inherits these values and overrides only what differs.

## Environment Variables

Keep secrets out of the file. Write `${VAR_NAME}` anywhere in the config and Rocky substitutes the environment variable's value at parse time, before it reads the TOML.

```toml
[adapter.prod]
type = "databricks"
host = "${DATABRICKS_HOST}"
token = "${DATABRICKS_TOKEN}"
```

If a referenced variable is not set, Rocky returns a parse error listing the missing variable.

The same substitution runs over every TOML config Rocky loads, not just `rocky.toml`:

- **Per-model sidecars** (`models/<name>.toml`): useful for orchestrator-injected `[target]` overrides.
- **`models/_defaults.toml`**: directory-level defaults applied to every sibling sidecar.
- **Inline `---toml` frontmatter** in `.sql` / `.rocky` files: only the frontmatter block is substituted. The SQL body below the closing `---` is left untouched, so `${VAR}` in SQL stays literal.

```toml
# models/customer_facts.toml — sidecar example
[target]
catalog = "${ROCKY_TARGET_CATALOG:-warehouse}"
schema  = "${ROCKY_TARGET_SCHEMA:-marts}"
table   = "${ROCKY_TABLE_OVERRIDE:-customer_facts}"
```

A worked example covering all three layers lives in `examples/playground/pocs/00-foundations/07-config-layering/`.

### Where a resolved value is shown

Rocky prints the placeholder, not the value. A value of 8 bytes or more is rewritten to `${NAME}` wherever it appears, even inside a longer string. With `BUCKET=analytics_prod`, the hook command `upload.sh s3://analytics_prod/out` prints as `upload.sh s3://${BUCKET}/out`. A value shorter than 8 bytes prints as itself, and so does the literal in `${VAR:-default}`. A value that a TOML basic string unescapes (`A`, `\t`) is matched in its unescaped form too.

| Surface | What prints `${NAME}` |
|---|---|
| `rocky serve` responses | Every value of 8 bytes or more. |
| `rocky policy show`, `rocky policy check`, `rocky brief` | Policy `scope`, `verify_after`, `autonomy_budget.window`; a model's `tags`, `classifications` and `layer`; a degraded rule's `window`. |
| `rocky compile`, `rocky dag` | `models_detail` (`strategy`, `freshness`, `tags`, `depends_on`, `name`), diagnostics text; a node's `strategy`, `freshness` and `partition_shape`. |
| `rocky list models` / `adapters` / `sources`, `rocky catalog` | `depends_on`; an adapter's `host`; a source's `catalog`, `schema_prefix` and `components`; an asset's `intent` and column descriptions. |
| `rocky validate`, `rocky doctor` | A pipeline's `strategy`, `catalog_template` and `schema_template`; every message, detail and suggestion. |
| `rocky hooks list`, `rocky hooks test` | A hook's `command`, and the abort reason. |
| `GET /api/v1/schedule`, `rocky audit`, policy-decision ledger | `timezone`, `cron`, `after`, config error; a decision's `model` and `reason`; a verification's `checks` and `reason`; a `verify_after` check name and outcome. |
| `rocky plan` (replication) plan files | `config_snapshot` holds `${NAME}`. A keyed digest of each config section sits beside it, so `rocky apply` refuses when an environment value changed. The key is `.rocky/plan-digest.key`; without it, apply refuses. |
| Config errors and model-file errors | Every quoted value of 8 bytes or more, including TOML parse errors in a sidecar, `_defaults.toml` or frontmatter. |
| Adapter connection fields | DuckDB `path`, `extension_directory`; Databricks `host`, `http_path`, `client_id`; Snowflake `account`, `warehouse`, `username`, `private_key_path`, `role`, `database`; BigQuery `project_id`, `location`; Fivetran `destination_id`. Only the adapter connect path reads the value. |

These outputs show a resolved value:

- **Target coordinates, on purpose.** A target catalog, schema and table print resolved wherever they appear as a target: `rocky compile` and `rocky dag` `target`, `rocky list models` `target`, `rocky catalog` `fqn`, `rocky lineage` `target_schema`, `rocky ci-diff` `resolved_target`, `rocky history --audit` `target_catalog`. `dagster-rocky` matches the asset key from `rocky compile`'s `target` against `rocky run`'s `asset_key`, so a placeholder on one side would drop materializations.
- SQL that Rocky generates (`rocky plan`, `rocky run`, `rocky preview`), `rocky run` and `rocky discover` results (including each `asset_key`), the `config_identity` in a governed plan file, `rocky docs` HTML, and log lines.

Never put a secret in a target catalog, schema or table name, or in any other field these outputs carry.

### Default Values

Use `${VAR_NAME:-default}` to provide a fallback when a variable is unset or empty:

```toml
backend = "${ROCKY_STATE_BACKEND:-local}"
```

---

## `[adapter.NAME]`

Declare a connection once, then reference it by name from any number of pipelines. Each `[adapter.NAME]` block is one connection. You choose the name; `type` selects which adapter implementation handles it.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string | Yes | Adapter type. One of `"databricks"`, `"snowflake"`, `"duckdb"`, `"bigquery"`, `"trino"`, `"postgres"`, `"redshift"`, `"clickhouse"`, `"sqlserver"`, `"spark"`, `"fivetran"`, `"airbyte"`, `"iceberg"`, `"manual"`. An unrecognized value is a hard error. |
| `kind` | `"data"` \| `"discovery"` | See description | The role of this block. `"discovery"` is **required** for the discovery-only types: `fivetran`, `airbyte`, `iceberg` and `manual`. Leave it out for `databricks`, `snowflake`, `postgres`, `redshift`, `clickhouse`, `sqlserver` and `spark`, which move data only. For `duckdb` and `bigquery`, which can do both, leaving it out registers both roles. Rocky does not check `kind` for `trino`. |
| `retry` | table | No | Retry policy (see [`[adapter.NAME.retry]`](#adapternameretry)). |
| `extra` | table | No | Escape hatch for adapter-specific keys Rocky's typed config doesn't model (see below). |

The remaining fields depend on the adapter type.

Rocky validates the top-level adapter fields strictly. An unrecognized key — a typo like `tooken` — is a hard error, not a silent no-op. Put keys that a custom or process adapter needs but Rocky does not model in a nested `[adapter.NAME.extra]` table, which passes through untouched:

```toml
[adapter.my_warehouse]
type = "trino"

[adapter.my_warehouse.extra]
default_schema = "analytics"
x_custom_header = "service-account"
```

### Per-adapter setup

Connection fields, authentication and examples live on one page per adapter: [DuckDB](/reference/adapters/duckdb/), [Databricks](/reference/adapters/databricks/), [Snowflake](/reference/adapters/snowflake/), [BigQuery](/reference/adapters/bigquery/), [PostgreSQL](/reference/adapters/postgres/), [SQL Server](/reference/adapters/sqlserver/) (Azure SQL and Fabric Warehouse are Beta), [Redshift](/reference/adapters/redshift/) (Beta), [ClickHouse](/reference/adapters/clickhouse/) (Beta), [Spark](/reference/adapters/spark/) (Beta), [Fivetran](/reference/adapters/fivetran/) and [Airbyte](/reference/adapters/airbyte/) (metadata-only source discovery).

Two types have no page yet. They read these top-level fields:

| `type` | Role | Fields |
|--------|------|--------|
| `"trino"` | Warehouse | `host` (coordinator URL, required). `token` for JWT bearer auth, else `username` + `password` for Basic auth. `username` also names the Trino user. `database` sets the default catalog. `timeout_secs` defaults to `300`. |
| `"iceberg"` | Discovery | `host` (REST catalog URL, required). `token` (optional bearer token). |


### `type = "manual"`

A discovery adapter that lists source schemas and tables inline instead of calling an API. Use it for a source with no discovery adapter of its own, for tests, and for small stable sources. It is discovery-only, so `kind = "discovery"` is required.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `schemas` | array of tables | Yes | The source schemas this adapter returns. At least one. |
| `schemas[].name` | string | Yes | Schema name, matched against the pipeline's `schema_pattern.prefix` like any discovered schema. |
| `schemas[].tables` | array of strings | Yes | Tables in the schema. At least one. |

```toml
[adapter.local_discovery]
type = "manual"
kind = "discovery"

[[adapter.local_discovery.schemas]]
name = "raw__orders"
tables = ["orders", "order_items"]

[pipeline.poc.source.discovery]
adapter = "local_discovery"
```

The adapter makes no network call and does not check that the tables exist. `rocky plan` plans every listed table, so a missing table fails at `rocky run`. `rocky discover` with `source.catalog` set drops missing tables, as for every discovery adapter. Rocky uses a listed schema only when the pipeline's `schema_pattern` parses it: the name starts with `prefix` (case-sensitive) and the rest splits into the declared `components`.

`rocky validate` reports `V057`, and every command that loads the config refuses it, for:

- a manual adapter with no `schemas`, or a schema with no `tables`;
- a schema or table name that is not a plain identifier (`[a-zA-Z0-9_]+`);
- a schema listed twice, or a table listed twice in one schema (compared ignoring case);
- `schemas` on an adapter whose type is not `manual`.

`V058` warns when the `schema_pattern` of a pipeline that uses the adapter parses none of the listed schemas.

### `[adapter.NAME.retry]`

Decide how hard this adapter tries again after a transient error: an HTTP 429 or 503, a rate limit, a timeout. Rocky backs off exponentially between attempts, with jitter by default.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_retries` | integer | `3` | Maximum retry attempts. Set to 0 to disable. |
| `initial_backoff_ms` | integer | `1000` | Initial backoff in milliseconds. |
| `max_backoff_ms` | integer | `30000` | Maximum backoff cap in milliseconds. |
| `backoff_multiplier` | float | `2.0` | Multiplier applied after each retry. |
| `jitter` | bool | `true` | Add random jitter to prevent thundering herd. |
| `circuit_breaker_threshold` | integer | `5` | Trip after this many consecutive failures. Set to 0 to disable. |
| `circuit_breaker_recovery_timeout_secs` | integer | `null` | When set, the breaker auto-recovers after this many seconds: it enters half-open, admits a single trial request, and either closes on success or re-opens on failure. When unset, a tripped breaker stays tripped for the rest of the run (manual-reset behaviour). |
| `max_retries_per_run` | integer | `null` | Per-adapter cross-statement retry budget for a single run. Use the top-level [`[retry]`](#retry) block when you want one shared budget across every adapter instead. |

When the breaker trips, Rocky emits a `circuit_breaker_tripped` pipeline event; on auto-recovery it emits `circuit_breaker_recovered`. Hook subscribers can observe both without polling the adapter. See [Hooks](/concepts/hooks/).

---

## `[pipeline.NAME]`

Declare a unit of work: what it reads, where it writes, and how. Each `[pipeline.NAME]` block is one pipeline. You choose the name, and every command takes `--pipeline NAME` to select one when the config declares more than one.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `type` | string | No | `"replication"` | Pipeline type. One of `"replication"`, `"transformation"`, `"quality"`, `"snapshot"`, `"load"`. The remaining fields depend on the type; the fields below apply to `"replication"` (the default). |
| `strategy` | string | No | `"incremental"` | Replication strategy: `"incremental"`, `"full_refresh"`, `"merge"`, `"view"`, `"materialized_view"` or `"dynamic_table"`. `"merge"` needs `merge_keys` (or `merge_keys_fallback`), or the config load fails. `"dynamic_table"` is recognized but unusable here (no `target_lag` on the pipeline block): declare it on a transformation model sidecar. Any other value is **not** an error: `rocky validate` warns (`V035`) and the run falls back to `full_refresh`. |
| `timestamp_column` | string | No | `"_fivetran_synced"` | Watermark column for incremental strategy. |
| `metadata_columns` | list | No | `[]` | Extra columns to add to copied data (see below). |

```toml
[pipeline.bronze]
type = "replication"
strategy = "incremental"
timestamp_column = "_fivetran_synced"
metadata_columns = [
    { name = "_loaded_by", type = "STRING", value = "NULL" },
    { name = "_loaded_at", type = "TIMESTAMP", value = "CURRENT_TIMESTAMP()" },
]
```

Rocky inserts `value` into the copy statement as a SQL expression (`"NULL"`, `"CURRENT_TIMESTAMP()"`) and checks it at config load:

- A `value` must be **one** expression, and any function it calls must be on Rocky's scalar-function allowlist. The refusal names the list.
- A **placeholder in a `value` must be quoted**: `value = "'{tenant}'"` parses, `value = "{tenant}"` is refused. A placeholder in a **name** template stays unquoted (`schema_template = "stage__{source}"`).
- The check parses under the **target** adapter's dialect (DuckDB, Snowflake, BigQuery, Databricks). A Trino target parses under a generic dialect that accepts more, so Trino can still reject a `value` that passes. The allowlist applies either way.

A `"transformation"` pipeline takes these fields instead:

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `models` | string | No | `"models/**"` | Glob for the model files, relative to the config file. |
| `allow_empty_models` | bool | No | `false` | Lets `rocky run` and `rocky run --dag` go on when the models directory does not exist. By default a missing directory fails the run, and `rocky validate` reports it as error `V025`. Set `true` only for a pipeline that has no models on purpose, for example one that only loads seeds. The key covers a missing directory only. A directory that exists but holds no models (or a glob that matches nothing in it) fails a single-pipeline `rocky run` with a compile error, whatever this key says; a `--dag` run plans that pipeline with no nodes. |

```toml
[pipeline.seeds_only]
type = "transformation"
allow_empty_models = true   # no models/ directory on purpose
```

### `[pipeline.NAME.source]`

Point the pipeline at the system it reads from.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `adapter` | string | Yes | Name of the adapter that reads the source rows. Must match a `[adapter.NAME]` key, and that adapter must move data. A discovery-only adapter such as Fivetran goes in [`source.discovery`](#pipelinenamesourcediscovery) instead. |
| `catalog` | string | No | Source catalog name (used by warehouse-resident sources like Databricks). |
| `region` | string | No | The region where the source data lives, for example `"eu-west-1"` or `"eu"`. Lowercase letters, digits and hyphens. Required when [`[residency] enforce = true`](#residency). A `source.discovery` block has no `region` of its own. |

A discovery-only adapter such as Fivetran (`kind = "discovery"`) only lists what exists. Set it in `source.discovery`; the warehouse adapter reads the rows.

### `[pipeline.NAME.source.discovery]`

Use a different adapter to list schemas and tables than the one that reads the rows. Reach for this when one system knows what exists (DuckDB, say) while the data itself lives elsewhere. Omit the block and Rocky discovers through the source `adapter`.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `adapter` | string | Yes | — | Adapter name to use for discovery. |
| `report_new_sources` | bool | No | `false` | Diff the discovered source inventory against the prior persisted snapshot and report first-seen schemas in the discover output's `new_sources`. The first discover of a pipeline records the baseline and reports nothing. Off by default — the diff and its state write only happen when opted in. |
| `on_collision` | `"off"` \| `"warn"` \| `"error"` | No | `"off"` | Cross-source collision detection. When the same external object (e.g. an ad account) is onboarded under two schemas, its data lands in two target tables and silently doubles any downstream `UNION ALL`. `warn` reports the pairs in the discover output's `collision_candidates` and emits a `source_collision_detected` event; `error` additionally fails the discover so a colliding onboard can't silently create a catalog. `off` (default) skips detection entirely. Only adapters that resolve external object ids (e.g. Fivetran) participate; others contribute nothing. |
| `sync` | table | No | absent | `[pipeline.NAME.source.discovery.sync]`. Wait for a running source sync before `rocky run` reads the source, and optionally start one. Fivetran and Airbyte only. See the keys below. |

See [Cross-source duplicate detection](#cross-source-duplicate-detection) for the counterpart that runs during replication rather than at discovery.

#### `[pipeline.NAME.source.discovery.sync]`

Waits for a running source sync to finish before the run starts. With `wait` alone Rocky sends `GET` requests only. With `trigger` and `allow_sync_trigger = true` on the adapter, it first starts one sync per connector. Works on `fivetran` and `airbyte` discovery adapters; `rocky validate` refuses `wait = true` on any other known type. See [Fivetran](/reference/adapters/fivetran/#wait-for-a-running-sync) and [Airbyte](/reference/adapters/airbyte/#wait-for-a-running-sync).

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `wait` | bool | No | `false` | Wait for a running sync to end. The other keys do nothing without it. |
| `wait_timeout_secs` | integer | No | `3600` | Longest wait for the whole phase. A sync still running at the limit counts as a failure. `rocky validate` requires at most half of `[state.idempotency] in_flight_ttl_hours`. |
| `poll_interval_secs` | integer | No | `30` | Seconds between polls. At least 10. |
| `on_sync_failure` | `"fail"` \| `"warn"` | No | `"fail"` | `fail` stops the run before any warehouse write. `warn` logs the failure and runs on the data already landed. |
| `trigger` | bool | No | `false` | Start a sync on each selected connector, then wait for it. Needs `wait = true` and `allow_sync_trigger = true` on the discovery adapter. `rocky run --no-sync-trigger` turns it off for one run. See [Start a sync](/reference/adapters/fivetran/#start-a-sync) for Fivetran and [Start a sync](/reference/adapters/airbyte/#start-a-sync) for Airbyte. |

### `[pipeline.NAME.source.schema_pattern]`

Teach Rocky to read structure out of your source schema names. It can then route each source to the right target, and you can select sources with `--filter`. A schema like `src__acme__us_west__shopify` becomes a set of named components.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `prefix` | string | Yes | Prefix that identifies managed schemas (e.g., `"src__"`). |
| `separator` | string | Yes | Separator between components (e.g., `"__"`). Letters, digits, `_`, `.` and `-` only. |
| `components` | list of strings | Yes | Ordered list of component names. A trailing `"..."` marks a component as multi-valued. |

```toml
[pipeline.bronze.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["client", "regions...", "connector"]
```

Given `src__acme__us_west__us_east__shopify`, this pattern extracts:

| Component | Value |
|-----------|-------|
| `client` | `"acme"` |
| `regions` | `["us_west", "us_east"]` |
| `connector` | `"shopify"` |

### `[pipeline.NAME.target]`

Say where the pipeline writes. The two templates build the catalog and schema name per source, filling `{component}` placeholders from the components `schema_pattern` parsed above.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `adapter` | string | Yes | Name of the warehouse adapter. Must match a `[adapter.NAME]` key. |
| `catalog_template` | string | Yes | Template for the target catalog name. Uses `{component}` placeholders (or `{component:SEP}` to pin the join separator for variadic components). |
| `schema_template` | string | Yes | Template for the target schema name. Uses `{component}` placeholders (or `{component:SEP}` to pin the join separator for variadic components). |
| `region` | string | No | The region where the target warehouse stores the data. Lowercase letters, digits and hyphens. Required when [`[residency] enforce = true`](#residency). |

A template without a placeholder (`warehouse`) is static. `schema_pattern` takes a source schema apart (`src__acme__shopify` gives `client = acme`, `source = shopify`), and the templates put a destination name back together from the pieces (`stage__{source}` gives `stage__shopify`).

For per-tenant catalogs, put a placeholder in `catalog_template` too: `catalog_template = "{tenant}_warehouse"` with `components = ["tenant", "regions...", "source"]`. See [Schema Patterns](/concepts/schema-patterns/) for the full pattern reference.

One name clash to know about. This `schema_template` routes replication targets from parsed source schemas. The config-group `schema_template` is a different feature: it fills its placeholders from a model's `[args]` to route a fan-out of transformation models. See [Config groups](/reference/model-format/#config-groups).

### `[pipeline.NAME.target.governance]`

Have Rocky create and label the catalogs and schemas it writes to. Support depends on the target adapter; the two `auto_create_*` flags work on every adapter that emits `CREATE SCHEMA` SQL.

| Target | `tags` | `grants` / `schema_grants` | `isolation` |
|---|---|---|---|
| Databricks | Yes (Unity Catalog) | Yes | Yes |
| Snowflake | Yes | Yes | No |
| BigQuery | Schema and table labels only. A catalog (project) label logs a warning. | No: logs a warning. Use Google Cloud IAM. | No |
| Other adapters | No | No | No |

Where a row says No, the call does nothing and the run continues. Catalog tags, grants and isolation run only with `auto_create_catalogs = true`. Schema tags and `schema_grants` run only with `auto_create_schemas = true`. Rocky adds grants and never revokes one. See [Permissions](/reference/permissions/).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `auto_create_catalogs` | bool | `false` | Create target catalogs if they do not exist. |
| `auto_create_schemas` | bool | `false` | Create target schemas if they do not exist. Honored on **both** replication and transformation targets. |
| `tags` | table | `{}` | Tags applied to managed catalogs, schemas, and tables. |
| `grants` | list | `[]` | Catalog-level grants. Each entry has `principal` (string) and `permissions` (list of strings). |
| `schema_grants` | list | `[]` | Schema-level grants. Same format as `grants`. |
| `isolation` | table | | Workspace isolation settings (below). |

### `[pipeline.NAME.target.governance.isolation]`

Restrict which Databricks workspaces can reach the managed catalogs. Rocky binds each catalog to the listed workspaces and can set its isolation mode to `ISOLATED`. Reconcile behaviour: [Permissions](/reference/permissions/#workspace-isolation).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `false` | Set catalog isolation mode to `ISOLATED`. |
| `workspace_ids` | list of tables | `[]` | Workspace bindings. Each has `id` (integer, required: the Databricks workspace ID) and `binding_type` (`"READ_WRITE"` default, or `"READ_ONLY"`; the API values are `BINDING_TYPE_READ_WRITE` and `BINDING_TYPE_READ_ONLY`). |

### `[pipeline.NAME.checks]`

Assert that the data is right while the run is happening, not in a separate test step afterwards. Two surfaces share this section:

- **Pipeline-level switches** — row count, column match, freshness, null rate, custom SQL, anomaly detection.
- **Model-level `[[assertions]]` blocks** — `not_null`, `unique`, `in_range`, `regex_match`, and the rest.

See [Data quality checks](/concepts/data-quality-checks/) for what each one means.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `false` | Master switch for the pipeline-level checks below. |
| `fail_on_error` | bool | `true` | When `false`, downgrades every `error`-severity assertion to a non-fatal result. |
| `row_count` | bool | `false` | Compare row counts between source and target. |
| `column_match` | bool | `false` | Verify source and target have the same column sets. |
| `freshness` | table | | `{ warn_after = "12h", error_after = "24h" }`. Set at least one. Every table is checked against the same levels. There is no per-schema override. An age above `error_after` fails the check at error severity and gates the run. An age above `warn_after` fails it at warning severity. See [Freshness durations](#freshness-durations). |
| `null_rate` | table | | `{ columns = [...], threshold = 0.0–1.0, sample_percent = 10 }`. |
| `custom` | list | `[]` | Custom SQL checks. Each entry has `name`, `sql`, and optional `threshold`. |
| `anomaly_threshold_pct` | float | `50.0` | Row count deviation percentage that triggers an anomaly. Set to 0 (or a negative value) to disable detection. Must be a finite number: `nan` and `inf` are refused when the config loads. |
| `quarantine` | table | | `{ enabled = true, mode = "split" \| "tag" \| "drop" }`. Off unless `enabled = true`. See below. |
| `assertions` | list | `[]` | Repeated `[[assertions]]` blocks (DQX parity). See below. |
| `cross_source_overlap` | table | | Flags the same business key appearing across sibling sources that feed one consolidation target. See [Cross-source duplicate detection](#cross-source-duplicate-detection). |

#### `[[pipeline.NAME.checks.assertions]]`

Declarative assertions. Each block declares a `type` and its parameters. Every assertion shares these base fields:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `table` | string | (required) | Unqualified target table the assertion runs against. Implied in a model sidecar; required in a pipeline-level block. |
| `name` | string | | Result identifier; synthesized from `{kind}:{column}` when unset. Set it to tell apart several assertions on one table, kind and column. Refused at config load: the reserved names `quarantine:compile`, `quarantine:execute`, `row_count`, `column_match`, `row_count_anomaly`, and `freshness` when the pipeline declares `[checks.freshness]`. Names are compared after every non-alphanumeric character maps to `_` (as Dagster keys a check), so `quarantine.execute` is refused too. The same rule covers a `[[checks.custom]]` name and any two check names that sanitize alike. `rocky run` and `rocky plan` re-check once discovery has resolved the real table set, which catches a `cross_source_overlap` collision. |
| `type` | string | (required) | One of: `not_null`, `unique`, `unique_expr`, `accepted_values`, `relationships`, `expression`, `row_count_range`, `in_range`, `regex_match`, `aggregate`, `composite`, `not_in_future`, `older_than_n_days`. |
| `column` | string | | Required for the row-level column kinds (`not_null`, `unique`, `accepted_values`, `relationships`, `in_range`, `regex_match`, `not_in_future`, `older_than_n_days`). |
| `severity` | string | `"error"` | `error` fails the pipeline (subject to `fail_on_error`); `warning` reports but never fails. |
| `filter` | string | | SQL boolean predicate that scopes the assertion to a subset of rows. |

| Type | Additional fields |
|---|---|
| `accepted_values` | `values: [String]` |
| `relationships` | `to_table: String`, `to_column: String` |
| `expression` | `expression: String` (SQL boolean predicate) |
| `row_count_range` | `min: u64?`, `max: u64?` |
| `in_range` | `min: String?`, `max: String?` (numeric literals) |
| `regex_match` | `pattern: String` (dialect-specific regex; no single quotes, backticks, or semicolons) |
| `aggregate` | `op: sum\|count\|avg\|min\|max`, `cmp: lt\|lte\|gt\|gte\|eq\|ne`, `value: String` |
| `composite` | `kind: "unique"`, `columns: [String]` (≥2) |
| `unique_expr` | `key_expr: String`: one derived key expression over the row (for example `md5(tenant \|\| '-' \|\| id)`), allowlisted functions only, no volatile function or `COLLATE`. Use it when the identity is computed. See [Per-assertion `filter`](/concepts/data-quality-checks/#per-assertion-filter). |
| `older_than_n_days` | `days: u32` |

```toml
[[pipeline.silver.checks.assertions]]
table = "orders"
type = "in_range"
column = "amount_cents"
min = "0"
filter = "region = 'US' AND status != 'cancelled'"
```

#### `[pipeline.NAME.checks.quarantine]`

Keep failing rows out of the clean table. Rocky takes the rows that fail an `error`-severity row-level assertion and puts them in their own table, or marks them in place. Quarantine runs in `quality` pipelines only. Other pipeline types accept the block and ignore it, with no warning.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `false` | Turn quarantine on. Without it the block does nothing. |
| `mode` | `"split"` \| `"tag"` \| `"drop"` | `"split"` | What Rocky does with the failing rows. See the table below. |
| `suffix_valid` | string | `"__valid"` | Suffix for the table of passing rows. `split` and `drop` refuse a suffix that makes this name match the source table, so an empty suffix is refused. |
| `suffix_quarantine` | string | `"__quarantine"` | Suffix for the table of failing rows. `split` refuses a suffix that makes this name match the source or valid table. `drop` writes no quarantine table and does not check it. Names are compared exactly, so suffixes that differ only in case are not refused. On a warehouse that ignores case, such as DuckDB or Databricks, the two name one table and the valid write replaces the quarantine rows. |

| Mode | Behavior |
|---|---|
| `split` | Writes `<table>__valid` (passing rows) and `<table>__quarantine` (failing rows, each with an `_error_<name>` column per assertion). The original `<table>` stays. Point downstream models at `<table>__valid`. Refused on a dialect with no `SELECT * EXCEPT` form, such as Trino. |
| `tag` | Rewrites `<table>` in place and adds an `_error_<name>` column per assertion, set on the failing rows. Every row stays. This rewrites the source, so take care on a raw replication target. |
| `drop` | Writes only `<table>__valid`. Rocky discards the failing rows. |

Only these row-level kinds are quarantined: `not_null`, `accepted_values`, `expression`, `in_range`, `regex_match`, `not_in_future` and `older_than_n_days`. All three modes accept them, and a clock function in an `expression` or `filter`. A quarantine that is refused (`quarantine:compile`) or fails at the warehouse (`quarantine:execute`) fails the run, whatever `fail_on_error` says. Set-based, table-level and referential assertions (`unique`, `unique_expr`, `composite`, `relationships`, `row_count_range`, `aggregate`) run as ordinary checks.

#### Cross-source duplicate detection

The assertions above also run on **replication** pipelines. For one business key that arrives through two **sibling** sources later `UNION`-ed, use `[pipeline.NAME.checks.cross_source_overlap]`: a per-table `unique` passes on each table, and only an overlap check across the siblings sees the duplicate. It is the **detective** counterpart to discover-time `on_collision`. See [Data Quality Checks](/concepts/data-quality-checks/#cross-source-overlap).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `keys` | list of strings | `[]` | Business-key columns whose shared value across sibling tables signals a duplicate. Mutually exclusive with `key_expr`. |
| `key_expr` | string | | Derived business-key expression for sources without a natural key. Mutually exclusive with `keys`. Same gate as `unique_expr`'s `key_expr`. |
| `severity` | string | `"error"` | `error` fails the pipeline (subject to `fail_on_error`); `warning` reports but never fails. |
| `max_overlap_rows` | integer | `0` | Overlap-key count above which the check fails. `0` means any overlap fails. |
| `sample` | integer | `20` | Maximum overlapping keys attached to the result for triage. |

Exactly one of `keys` or `key_expr` is required. A sibling table whose key cannot be evaluated is skipped with a logged reason.

### `[pipeline.NAME.execution]`

Tune how many tables this pipeline processes at once, and when it gives up. The defaults suit a remote warehouse; pin `concurrency` to a number when you need a predictable load.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `concurrency` | string \| integer | `"adaptive"` | Max tables processed in parallel. `"adaptive"` (default) uses an AIMD throttle starting at 32 and adjusting to rate-limit signals — best for remote warehouses. A fixed integer (e.g. `8`) pins concurrency; `1` is serial. |
| `fail_fast` | bool | `false` | Abort all remaining tables on first error. |
| `error_rate_abort_pct` | integer | `50` | Abort if error rate exceeds this percentage (0–100). Set to 0 to disable. |
| `table_retries` | integer | `1` | Times to retry failed tables after the initial parallel phase. Set to 0 to disable. |

---

### `[[pipeline.NAME.sources]]`

Declare the external tables a transformation pipeline reads, and how fresh each must be. A source is a table your models read but do not build, for example a landing table that Fivetran loads. [`rocky freshness`](/reference/commands/core-pipeline/#rocky-freshness) checks each declared freshness block against the warehouse. Transformation pipelines only.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `catalog` | string | `""` | Catalog of the table. Leave it out for two-part names (DuckDB). |
| `schema` | string | (required) | Schema of the table. |
| `table` | string | (required) | Table name. |
| `freshness.loaded_at_field` | string | (required in `freshness`) | Column that holds each row's load time. Use a `DATE` or `TIMESTAMP` column. |
| `freshness.warn_after` | string | (unset) | Age above which the source reports `warn`. Format: a positive integer and one unit, `s`, `h` or `d` (`"3600s"`, `"12h"`, `"7d"`). |
| `freshness.error_after` | string | (unset) | Age above which the source reports `error`. Must not be shorter than `warn_after`. |
| `freshness.filter` | string | (unset) | SQL predicate that limits the rows Rocky takes the maximum over. Rocky adds it as `WHERE (<filter>)`. A `;` is refused. |

Set at least one of `warn_after` and `error_after`.

```toml
[[pipeline.silver.sources]]
schema = "raw"
table  = "orders"

[pipeline.silver.sources.freshness]
loaded_at_field = "_loaded_at"
warn_after      = "12h"
error_after     = "24h"
filter          = "status <> 'test'"
```

`rocky compile` checks each block. It raises `E050` for a block it cannot evaluate: no threshold, a duration that does not parse, `error_after` shorter than `warn_after`, a `loaded_at_field` that is not a plain column name, or a `filter` with a `;`. It raises `W050` when the compiler's source schema shows the `loaded_at_field` as missing or not a date or time type. That schema comes from a seed or the schema cache and can be out of date, so this case only warns.

### `[pipeline.NAME.schedule]`

Declare when this pipeline is due, so [`rocky tick`](/guides/running-without-an-orchestrator/#native-scheduling-with-rocky-tick-experimental) can run it without an external orchestrator. Every field is optional. With no block the pipeline runs only when you invoke it. **Experimental.**

A `load` pipeline cannot join the schedule graph (it re-ingests every discovered file, so scheduling one duplicates data): `rocky validate` rejects a scheduled load pipeline (`V044`) and an `after` that names one (`V045`). Run one `rocky tick` timer per project: schedule state is per-machine, so two hosts ticking one project both fire the same occurrence.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `cron` | string | — | Standard 5-field cron expression (`minute hour day-of-month month day-of-week`). Due at each occurrence. The first evaluation anchors at "now" and waits for the next occurrence. |
| `timezone` | string | project `[schedule].timezone`, else `"UTC"` | IANA timezone (e.g. `"Europe/Lisbon"`) for `cron`. DST is handled: a skipped hour fires once at the next valid instant, a repeated hour fires on its first occurrence only. |
| `after` | array of strings | `[]` | Upstream pipeline names. Due once **every** listed pipeline has a successful run that completed after this pipeline's own latest success *started*. A partial-success (exit 2) upstream run does not count. A cycle is a validation error. |
| `freshness` | bool | `false` | Due once the pipeline's own run-staleness exceeds its budget: the minimum over its member models, else the project [`[freshness]`](#freshness) block (the smaller of `warn_after` and `error_after`). This is run-staleness, **not** a warehouse probe; the reconciler issues no queries. `true` with no resolvable budget is a validation error. |
| `catchup` | string | `"latest"` | Policy when more than one occurrence elapsed since the last fire. `"latest"` fires one demand at the most recent missed occurrence; `"skip"` advances the anchor and runs nothing. `"all"` is **rejected**: runs are watermark-driven, so replaying every occurrence is pure cost. |
| `retry` | table | `{ max = 0 }` | In-tick retry. `retry.max` is the number of additional immediate re-submissions after the first attempt fails. No sleep between attempts. Partial (exit 2) runs are never retried. |
| `timeout_minutes` | integer | `0` | Scheduler-level timeout for a launched run. `0` means none. On elapse the child is terminated gracefully, then forcibly. |
| `enabled` | bool | `true` | `false` suppresses demand but keeps the config (shown as `disabled` in the tick's `skipped[]`). |

Any one source being due makes the pipeline due. A `[schedule]` block with none of `cron`/`after`/`freshness` and `enabled = true` is an inert-config warning.

```toml
[pipeline.staging]
type = "transformation"
models = "models/**"
[pipeline.staging.schedule]
after = ["raw"]            # runs in the same tick, after raw, once raw has a newer success
freshness = true          # ...and also whenever staging's output goes stale
retry = { max = 2 }
```

Project defaults live in a top-level `[schedule]` block:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `timezone` | string | `"UTC"` | Default timezone for every pipeline's `cron`. |
| `poll_interval_seconds` | integer | `15` | Cadence of `rocky serve --scheduler`. `--poll-interval-seconds` overrides it. The one-shot `rocky tick` does not read it. |

---

## `[state]`

Choose where Rocky keeps what it remembers between runs: watermarks, run history, plans, and checkpoint progress. Local is the default and needs no setup. Pick an object store or Valkey when runs happen on machines that do not survive — a CI runner, a Kubernetes pod.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `backend` | string | `"local"` | Storage backend: `"local"`, `"s3"`, `"gcs"`, `"valkey"`, or `"tiered"`. |
| `s3_bucket` | string | | S3 bucket name. Required when `backend` is `"s3"` or `"tiered"`. |
| `s3_prefix` | string | `"rocky/state/"` | S3 key prefix for state files. |
| `gcs_bucket` | string | | GCS bucket name. Required when `backend` is `"gcs"`. |
| `gcs_prefix` | string | `"rocky/state/"` | GCS object prefix for state files. |
| `valkey_url` | string | | Valkey/Redis connection URL. Required when `backend` is `"valkey"` or `"tiered"`. |
| `valkey_prefix` | string | `"rocky:state:"` | Valkey key prefix for state entries. |
| `transfer_timeout_seconds` | int | `300` | Wall-clock budget for each transfer (upload *or* download). Retries share this budget rather than extending it; raise for large state or slow networks. |
| `on_upload_failure` | string | `"skip"` | `"skip"` warns and continues (recovery needs a surviving local ledger or a published recovery intent). `"fail"` propagates the error. Governed runs require durability. See [State management](/concepts/state-management/#retry-and-failure-policy). |
| `namespacing` | string | `"none"` | `"none"` keeps one global state file. `"pipeline"` gives each pipeline its own ([State namespacing](#state-namespacing)). |
| `concurrency_control` | string | per backend | `"cas"` or `"off"`. Unset means `"cas"` on `s3`, `gcs` and `tiered`, `"off"` on `local` and `valkey`. `"cas"` makes each upload conditional on the remote object still carrying the generation this writer downloaded, so a run that lost a race fails closed. The ledger-seam commands (`policy`, `gc`, `restore`, `apply`) retry onto the winner. `"off"` is last writer wins. See [Concurrent writers](#concurrent-writers). |
| `on_schema_mismatch` | string | per backend | What to do when the binary opens a store written by a **newer** binary. Unset means `"fail"` on `local` and `"recreate"` on `s3`, `gcs`, `valkey` and `tiered`. `"recreate"` moves the newer file to `<file>.v<N>.bak`, warns once, starts from fresh local state, runs one full refresh, never writes the downgraded state to the shared tier, and refuses if that `.bak` exists. `"fail"` aborts and leaves the file untouched. Only the run path honours it; inspection and branch commands always hard-fail. |
| `freeze_marker_writes` | bool | `false` | Write durable freeze and unfreeze marker objects beside the remote state file (`<prefix>/freeze/`, `<prefix>/unfreeze/`) when `rocky policy freeze` / `unfreeze` run. Rocky always reads and enforces markers where a durable object tier exists, so you can upgrade a fleet to marker readers first. Needs `backend = "s3"`, `"gcs"` or `"tiered"`; on `"local"` or `"valkey"` it is a hard error. |

`"tiered"` downloads from Valkey first and falls back to S3, and uploads to both. Set the keys each backend requires:

```toml
[state]
backend = "tiered"
valkey_url = "${VALKEY_URL}"
s3_bucket = "${ROCKY_STATE_BUCKET}"
```

### Concurrent writers

`concurrency_control = "cas"` (compare-and-swap) makes each upload conditional on the remote object still carrying the generation this run downloaded. A run that loses the race exits non-zero with a conflict and the winner's state stands. Rocky does not merge the runs or roll back warehouse writes: re-run the loser. `"off"` uploads unconditionally, so the last writer wins silently. `local` and `valkey` offer no conditional write, so there an explicit `"cas"` downgrades to `off` with a warning. Set `"off"` only for a deployment with one writer per prefix.

Mechanism, the conditional-write probe, the `cas-required` marker, the 1.77.0 default change, and what `cas` covers: [State management](/concepts/state-management/#concurrent-writers). The backend matrix is in the [deployment contract](/advanced/deployment-contract/#when-two-processes-overlap).

### `[state.retry]`

Retries for a state upload or download that failed for a transient reason (a network hiccup, a 5xx, an endpoint that hung past the per-request HTTP timeout). It has the same fields and defaults as [`[adapter.NAME.retry]`](#adapternameretry). Retries share the outer `transfer_timeout_seconds` budget, so the wall-clock ceiling does not move. `max_retries_per_run` defaults to `null` (unbounded: the per-transfer `max_retries` is the only cap).

Every transfer ends with a structured `outcome` on its `state.upload` or `state.download` event: `ok`, `absent`, `carried_forward`, `timeout`, `error_then_fresh`, `skipped_after_failure`, `transient_exhausted`, `circuit_open` or `budget_exhausted`. Alert on those values, not on log text. Meanings: [State management](/concepts/state-management/#retry-and-failure-policy).

### `[state.idempotency]`

Tune how `rocky plan --idempotency-key <KEY>` deduplicates a retried run (the `rocky run --idempotency-key` alias reads the same block). Every field is optional and takes the default shown. The block does nothing on a run that passes no key, and Rocky rejects an unknown field rather than ignoring it.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `retention_days` | integer | `30` | Lifetime of a terminal idempotency stamp before garbage collection. `rocky run` sweeps the local `state.redb` copy after each run. It does not sweep expired entries in an `s3` or `gcs` bucket; use a bucket lifecycle rule for those. |
| `dedup_on` | string | `"success"` | Which terminal statuses count as "already processed". `"success"` only stamps successful runs (failures stay claimable for retries); `"any"` stamps every terminal status. |
| `in_flight_ttl_hours` | integer | `24` | Hours after which an `InFlight` claim is treated as a crashed-pod corpse and adopted by a fresh caller. Read when a run takes the claim and stamped on it: lowering the value later does not shorten a claim already made. On `valkey` and `tiered` it sets the key's expiry on the server (`EX`), and the server decides when the claim is gone. |

Stamps live in the `IDEMPOTENCY_KEYS` redb table and replicate on tiered backends so sibling pods see the same entry. See [`rocky plan --idempotency-key`](/reference/cli/#rocky-run) for the three outcomes (`fresh_run`, `skipped_idempotent`, `skipped_in_flight`). Bucket lifecycle rules for `s3` and `gcs`: [State management](/concepts/state-management/#idempotency-stamps-in-a-bucket).

### State namespacing

`namespacing = "none"` keeps one global `<models>/.rocky-state.redb`. `"pipeline"` gives each pipeline its own `<models>/.rocky-state/<pipeline>.redb`, so concurrent runs do not wait on one writer lock. `--state-namespace <key>` (a `^[a-zA-Z0-9_]+$` identifier) fans out by client instead and overrides the config. An explicit `--state-path` disables namespacing. Namespaced files start fresh. See [State management](/concepts/state-management/#per-namespace-state-files).


---

## `[run]`

Skip rebuilding a transformation model when neither its logic nor its upstream data appears to have moved since the last successful build. Off unless you turn it on.

:::caution[Best-effort, not a result-equivalence guarantee]
Skipping rests on two heuristics: a cosmetic-invariant IR hash for the logic, and `MAX(ts)` or rowcount movement for the upstream data. Any missing, unreadable or ambiguous input rebuilds. It stays off until you set `skip_unchanged = true` or pass `--skip-unchanged`.
:::

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `skip_unchanged` | bool | `false` | Master switch. `--skip-unchanged` turns the gate on for one invocation; `--force-rebuild` overrides both. |
| `skip_rowcount_fallback` | bool | `false` | Allow a rowcount-only (`COUNT(*)`) stability signal when an upstream has no timestamp to probe. Only `incremental` and `microbatch` models with a timestamp column have one. Weaker than a watermark: it can miss a same-size in-place `UPDATE`. |
| `lag_tolerance_seconds` | integer | `0` | Treat an upstream `MAX(ts)` that moved by at most this many seconds as unchanged. `0`: any movement rebuilds. A `MAX(ts)` that went backwards always rebuilds. |
| `strict_scheduling` | bool | `false` | Refuse a run instead of warning when Rocky cannot safely resolve the ordering edges it derives from the physical `schema.table` names a model reads: contradicting bare-read pairs, failed reference extraction, colliding targets, a `catalog.schema.table` read of a model with no catalog in its `[target]`, a by-name edge that `rocky run --dag` dropped because an exact read contradicted it, and, with `[resilience] contain_failures = true`, a bare-name read dropped because it would close a cycle (checked on every run, `--shadow` and `--branch` included, before any write). `false` reports warnings and exits `0`, so a consumer that ignores them can read a stale target. It lives on `[run]` because `rocky run --dag` derives ordering across pipelines. |
| `tests_on_run` | `"off"`, `"report"`, `"enforce"` | `"enforce"` | Run each built model's sidecar `[[tests]]` during `rocky run`. `off` runs none. `report` records results under `test_results` and never fails a model. `enforce` fails a model whose error-severity test fails or cannot run, so its downstream models do not build (its table stays). A warning-severity test never fails a model. `rocky run --tests <mode>` overrides it. `rocky plan promote` refuses a branch whose newest test evidence has a failing error-severity test, or none. |

### What is and is not skip-eligible

Rocky skips a model only when **both** hold. Anything else rebuilds.

- **(B) Eligible.** A plain materialization strategy (**not** `content_addressed` or `time_interval`; `full_refresh` **is** eligible). `[skip] eligible` is not `false`, and Rocky can prove the SQL deterministic. It always rebuilds SQL containing `CURRENT_TIMESTAMP` / `NOW()`, `RANDOM()`, `UUID()`, `CURRENT_USER`, `CURRENT_CATALOG`, `ANY_VALUE`, `ARRAY_AGG`, `COLLECT_LIST`, `COLLECT_SET`, `MODE`, an unordered `LIMIT` / `TOP` / `FETCH`, or any function not on the pure-function allowlist.
- **(G) Upstreams provably unchanged.** Lineage must be provably complete, so only a single plain `SELECT` over bare tables qualifies. A CTE, a subquery in `FROM`, an `IN (SELECT …)` / `EXISTS` / scalar sub-select, a `PIVOT` / `UNNEST` / nested-join table factor, or a set operation (`UNION` / `INTERSECT` / `EXCEPT`) is **never skipped**.

`--force-rebuild` plus `full_refresh` always builds. A model's `[skip]` block (`eligible`, `deterministic`) overrides the automatic decision: see [`[skip]` in Model format](/reference/model-format/#skip).

---

## `[reuse]`

Let a content-addressed model reuse work it already did, with a record an auditor can check. Both knobs apply **only** to the content-addressed (S3/UniForm) write path. On DuckDB and plain warehouse targets they do nothing.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `false` | Byte-level **point-to reuse**. A successful run records, per model, an input-match index entry and an offline-verifiable provenance record. A later run whose recomputed `input_hash` hits the index for a prior **strong** run may point to that run's already-written Parquet: a zero-copy commit that skips the SQL. `false` writes no input-match spine and makes no reuse decision. |
| `column_level` | bool | `true` | **Column-level skip.** An unpartitioned content-addressed model whose logic, environment and every provably-consumed upstream column are unchanged since its last successful build is skipped: no SQL, no new commit, the prior output stays authoritative. Independent of `enabled`. `column_level = false` restores always-build. |

Both decisions fail closed: any unproven input (a non-deterministic model, a changed recipe or environment, an unenumerable consumed-column set, a missing column hash) forces a build. `--no-reuse` forces every model to build for one run. The provenance record is what an auditor reads in [Verify a run](/guides/verify-a-run/).

---

## `[ai]`

Cap what an AI generation can spend and choose where requests go. It applies to every `rocky ai*` command: `rocky ai`, `rocky ai-explain`, `rocky ai-sync` and `rocky ai-test`. Rocky rejects an unknown field here.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_tokens` | integer | `4096` | Per-request `max_tokens` sent to the Anthropic Messages API, and the cumulative output-token budget across the compile-verify retry loop. When the running total of `output_tokens` exceeds it, Rocky stops with `TokenBudgetExceeded`. |
| `base_url` | string | `https://api.anthropic.com` | Base URL of the Anthropic-compatible API. Requests go to `{base_url}/v1/messages`. A trailing slash is ignored. It must use `https`; plain `http` works only for loopback hosts (`localhost`, `127.0.0.1`, `::1`). If unset, Rocky reads `ROCKY_AI_BASE_URL`. |
| `region` | string | None | Region of the AI endpoint, for example `"us"`. Required when [`[residency] enforce = true`](#residency). |

Rocky never reads the API key from `rocky.toml`. It reads `ANTHROPIC_API_KEY` and sends it to `base_url`, so point `base_url` only at a server you trust.

---

## `[cache]`

Stop paying a warehouse round-trip on every typecheck. Today the block holds one cache, the schema cache. It stores `DESCRIBE TABLE` results. `rocky compile` and `rocky lsp` then typecheck leaf models against real column types without asking the warehouse each time.

### `[cache.schemas]`

Control how long a cached table shape stays trusted, and whether other machines see it.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `true` | Enable schema cache reads + writes. Set to `false` for strict CI where every typecheck should resolve against the current warehouse. |
| `ttl_seconds` | integer | `86400` | TTL for cache entries in seconds (default 24h). Lower for high-DDL-churn teams. |
| `replicate` | bool | `false` | Replicate the schema cache via `[state]` sync. Default is off; a fresh clone should warm its cache from its own `rocky apply`, not inherit another machine's stale types. |
| `trusted_max_age_seconds` | integer | unset | Cache entries younger than this are trusted as current. A reference to a column a trusted entry lacks is the `E041` error. Older entries, and every entry when the key is unset, give the `W041` warning. |
| `strict_sources` | bool | `false` | Trust every known source schema, including seeds and old cache entries. Every `W041` becomes `E041`. `rocky compile --strict-sources` does the same for one invocation. |

`trusted_max_age_seconds` and `strict_sources` control the missing-source-column check. See [Missing columns in external sources](/concepts/compiler/#missing-columns-in-external-sources-e041--w041).

---

## `[cost]`

Tell Rocky what your warehouse costs. `rocky optimize` and run cost estimates use these prices and fall back to Rocky's defaults for an unset storage or DBU price. `rocky cost --month <YYYY-MM> --by product` uses only the prices you set: a figure with no price shows no money. A price of `0` is valid and means free. Every price must be a finite number, 0 or more.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `storage_cost_per_gb_month` | float | unset | USD per GB-month. When unset, `rocky optimize` estimates with `0.023`. |
| `compute_cost_per_dbu` | float | unset | USD per DBU-hour (Databricks, Snowflake). When unset, run cost estimates (`rocky cost <run>`, `rocky optimize`, run summaries) use `0.40`. |
| `compute_cost_per_machine_hour` | float | unset | USD per machine-hour, for engines that bill by machine time. Prices model time in the monthly product view. |
| `cross_region_transfer_cost_per_gb` | float | unset | USD to move 1 GB (2^30 bytes) across regions. The monthly product view prices the egress that adapters report. No adapter reports egress yet, so it prices nothing today. |
| `ai_cost_per_million_tokens` | float | unset | One blended USD price per 1,000,000 tokens (input and output cost the same). The monthly product view prices the AI calls recorded in this state store with it (developer spend, this store only). |
| `warehouse_size` | string | `"Medium"` | Warehouse size for cost estimation (`"Small"`, `"Medium"`, `"Large"`). |
| `min_history_runs` | integer | `5` | Runs Rocky needs in history before it makes a cost recommendation. |

In the monthly view, a month with usage and no price has no money total.

---

## `[budget]`

Put a ceiling on what one run may cost, how long it may take, and how much it may scan. When a run crosses a limit, Rocky emits a `budget_breach` pipeline event and fires the `HookEvent::BudgetBreach` hook. With `on_breach = "error"` the run also exits non-zero. Rocky rejects an unknown field here.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_usd` | float | | Maximum run cost in USD, from per-materialization `cost_usd` on `RunOutput.cost_summary`. `None` when no adapter produced cost data. |
| `max_duration_ms` | integer | | Maximum run wall time in milliseconds. |
| `max_bytes_scanned` | integer | | Maximum total bytes scanned across every materialization in the run, from per-model `bytes_scanned`. Catches scan-volume regressions even when cost stays inside `max_usd`. Only BigQuery reports it today (`totalBytesBilled`); Databricks, Snowflake and DuckDB give `None` and skip the dimension. |
| `on_breach` | string | `"warn"` | `"warn"` fires the event and keeps the run successful. `"error"` also fails the run. |

The limits are independent: any one breach trips the event, checked once per run against observed totals. Subscribe to `on_budget_breach` under `[hook.*]` to route it. Each [`BudgetBreachOutput`](/reference/json-output/) carries a `limit_type` tag: `"max_usd"`, `"max_duration_ms"` or `"max_bytes_scanned"`.

---

## `[product_budget.NAME]`

Put a monthly ceiling on what one data product is charged. `NAME` is the product name, the same as `products/NAME.toml`. `[budget]` checks one run; this checks one product's month to date, across runs.

```toml
[product_budget.orders]
max_compute_hours = 40       # model time
max_storage_gb_month = 200   # stored data, each storage basis on its own
max_usd = 120.0              # needs prices in [cost]
on_breach = "warn"           # the only value
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_compute_hours` | float | | Most model time the product may be charged this month, in hours. |
| `max_storage_gb_month` | float | | Most stored data the product may be charged this month, in GB-months (1 GB = 2^30 bytes). Each storage basis (`data_files`, `allocated_blocks`) is checked on its own, never added together. |
| `max_usd` | float | | Most USD the product may be charged this month, priced from your [`[cost]`](#cost) prices only. With no price, this limit is not evaluated. |
| `on_breach` | string | `"warn"` | Only `"warn"`. A breach is reported and fires the hook. The run's exit code does not change. |

Each limit must be a finite number, 0 or more. A limit left unset is not checked. Rocky rejects an unknown field here.

- **What is tested.** The figure the product is **charged** in `rocky cost --month <YYYY-MM> --by product`: `kept` plus `allocated_upstream`. A table this product builds that another product reads is charged to the reader. `built` is not tested. The month is cut in the project's `[schedule].timezone`.
- **When.** At the end of every production `rocky run`, after the run record is written, for **every** product that has a budget. Shadow and branch runs check nothing. An interrupted run and a backfill do not check either, but their builds count: if one moves a product over, its event names that run and the next run fires no hook.
- **Verdict.** `in_budget`, `breached` or `not_evaluated`. `breached` when any limit is over (more than the limit, not equal). Otherwise `not_evaluated` when any limit cannot be checked (`max_usd` has no price, or the month has no model time or no stored size), or when run history does not cover the whole month. Rocky never reports `in_budget` by default.
- **Breach events.** Rocky records an event each time a product goes from not over to over, by re-checking after each production run of the month in finish order. Each names the run (`run_id`), the limit and the figure, and is never withdrawn. Stored data accrues between runs, so `rocky cost --month` can show `breached` with no event yet. The next production run records it and fires the hook.
- **The hook.** `on_budget_breach` fires once per exceeded limit, only at the run that made the change. A `max_storage_gb_month` over on both bases fires twice. Its metadata has `scope = "product_month"`, `product` and `month` beside `limit_type`, `limit` and `actual`. See [Hooks](/concepts/hooks/).
- **Large months.** The end-of-run check splits at most 8 earlier runs. Past that with no verdict, a breach counts as the product's first. `rocky cost --month` and `rocky product status` list breach events only below a work cap (750 runs of 21 executions), and say so in `breach_events_skipped` above it.
- **Where to read it.** `product_budgets` in `rocky run --output json` (replication, transformation and model-only runs; a quality or snapshot run fires the hook but has none). `budget` and `breach_events` per product in `rocky cost --month <YYYY-MM> --by product`. `cost_month` in `rocky product status`.

`rocky validate` warns with **V065** when `NAME` matches no file in `products/`, and with **V066** when `[product_budget]` is set and `[state.retention] max_age_days` is below 62.

---

## `[portability]`

Find SQL that will not run on the warehouse you plan to move to, before you move. Set `target_dialect` and every `rocky compile` — and every in-editor check the LSP drives — runs the **P001** portability lint against that target. The `rocky compile --target-dialect <DIALECT>` flag overrides this block. Rocky rejects an unknown field here rather than ignoring it.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `target_dialect` | string | | `"databricks"`, `"snowflake"`, `"bigquery"`, or `"duckdb"`. When unset, no lint runs (the flag-only opt-in behavior). |
| `allow` | list of string | `[]` | Project-wide allow-list of construct labels (case-insensitive). Useful when a project standardizes on a non-portable extension like `QUALIFY`. Prefer per-model `-- rocky-allow: …` pragmas for targeted exemptions. |

```toml
[portability]
target_dialect = "bigquery"
allow = ["QUALIFY"]
```

The effective target dialect is the `--target-dialect` flag if set, else `target_dialect`, else none (no lint).

See [Linters](/concepts/linters/) for the full list of covered constructs and the per-model pragma syntax.

---

## `[lint]`

Tune the style rules that `rocky lint` runs. Rocky rejects an unknown field here, and an unknown rule code is an error.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `disable` | list of string | `[]` | Rule codes that do not run, such as `["S003"]`. |
| `severity` | table | `{}` | Severity per rule code: `"error"`, `"warning"` or `"info"`. A rule keeps its default severity if it has no entry. |

```toml
[lint]
disable = ["S004"]

[lint.severity]
S001 = "error"
```

`rocky lint` exits with code `1` when a finding has `error` severity. See [`rocky lint`](/reference/commands/modeling/#rocky-lint) for the rules.

---

## `[retry]`

Share one retry budget across every adapter in a run, instead of giving each its own. Rocky builds a single counter and passes it to all of them; once it runs out, no adapter retries again. This stops one failing endpoint from consuming retries the other adapters would have used. Omit the block and each `[adapter.NAME.retry]` keeps its own independent budget.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_retries_per_run` | integer | | Total retries allowed across every adapter for this run. Omit the block to keep per-adapter budgets (each `[adapter.NAME.retry]` remains in isolation). |

---

## `[contracts]`

Project-level contract settings.

| Key | Type | Default | Description |
|---|---|---|---|
| `strict` | bool | `false` | Refuse a contract column whose declared type Rocky cannot check. Every `I003` note becomes the `E059` error, which names the model and the column, says why the type is unknown, and says how to fix it. `rocky compile --strict-contracts` and `rocky ci --strict-contracts` do the same for one invocation. `rocky run` and `rocky test` read this key. |

## `[mask]`

Hide sensitive column values without editing a model. Tag a column with a `[classification]` in its sidecar, then map the tag to a strategy here. Every column carrying the tag gets the same treatment. Walkthrough: [Governance](/guides/governance/#4-column-classification-and-masking-pillar-2-of-5).

| Strategy | Behavior |
|---|---|
| `"hash"` | SHA-256 hex digest of the column value. Deterministic, one-way. |
| `"redact"` | Replace the value with the literal string `'***'`. |
| `"partial"` | Keep the first and last two characters; replace the middle with `***`. Values shorter than 5 characters become `'***'`. |
| `"none"` | Explicit identity: no masking. Counts as masked for `rocky compliance`. Useful as a per-env override. |

```toml
[mask]
pii = "hash"
confidential = "redact"
```

An unknown strategy (a typo like `"mask"`) fails at config load.

:::note[Adapter support]
Masking works today on **Databricks** Unity Catalog only (column tags plus `CREATE MASK` / `SET MASKING POLICY`, one statement per column). Rocky applies masks after a successful DAG run, best-effort: a failure logs a warning and does not abort the pipeline.
:::

### `[mask.<env>]`

Mask a column differently per environment. Rocky starts from the `[mask]` defaults and layers the block for the active environment on top.

```toml
[mask.prod]
pii = "none"            # unmask pii in prod (e.g. service principal reads)
confidential = "partial"
```

Precedence: the `[mask.<env>]` entry (when `rocky plan --env <env>` supplies the env), then `[mask]`. An unmatched tag raises `W004` unless it is listed in [`[classifications] allow_unmasked`](#classifications).

---

## `[classifications]`

Silence the `W004` warning for a classification tag you never intend to mask. This is a project-wide setting, distinct from the per-model `[classification]` sidecar block that applies the tags (see [Model format](/reference/model-format/#classification)).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `allow_unmasked` | list of strings | `[]` | Classification tags allowed to appear in a model sidecar without a matching `[mask]` strategy. Suppresses the W004 compiler warning. |

Use it for tags that exist only for discovery or lineage tracking. [`rocky compliance`](/reference/cli/#rocky-compliance) still lists those columns and still suppresses their exceptions, so the report never pretends they are enforced.

---

## `[role.<name>]`

Describe your access model as roles that inherit from each other. Rocky flattens the inheritance graph before it reconciles against the warehouse's role or group system, and rejects a cycle or an unknown parent at config load.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `inherits` | list of strings | `[]` | Immediate parent role names. Permissions union transitively across every ancestor. |
| `permissions` | list of strings | `[]` | Permissions this role grants, in canonical uppercase (`"SELECT"`, `"USE CATALOG"`, `"USE SCHEMA"`, `"MODIFY"`, `"MANAGE"`). An empty list makes a pure grouping role. |

```toml
[role.reader]
permissions = ["SELECT", "USE CATALOG", "USE SCHEMA"]

[role.analyst]
inherits = ["reader"]
permissions = ["MODIFY"]
```

Rocky forwards the resolved set to `GovernanceAdapter::reconcile_role_graph` after a successful DAG.

:::caution[Databricks only, best-effort]
Rocky creates a SCIM group `rocky_role_<name>` per role and grants each resolved permission on each managed catalog. A failed create or `GRANT` logs a warning and the run continues. Without workspace auth Rocky only checks the group names and logs the graph. Other adapters do nothing. See [Governance](/guides/governance/#6-role-graph-reconciliation-pillar-4-of-5).
:::

---

## `[selectors]`

Saved selectors. Each entry maps a name to a `--select` expression. Use one with `--select selector:<name>` (or `--exclude selector:<name>`) on any command that takes `--select`. See [Saved selectors](/reference/node-selection/#saved-selectors).

```toml
[selectors]
nightly = "tag:nightly+ config.materialized:incremental"
finance = "path:marts/finance,tag:certified"
```

## `[freshness]`

Set a project-wide staleness budget so you do not repeat a threshold in every model sidecar. Freshness is how far behind the newest row in a table is.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `warn_after` | duration | (unset) | Age above which a model counts as stale at warning severity. |
| `error_after` | duration | (unset) | Age above which a model counts as stale at error severity. A model past this level makes `rocky freshness` exit 1. Must not be shorter than `warn_after`. |
| `time_column` | string | (unset) | Timestamp column to measure age from. [`rocky freshness`](/reference/commands/core-pipeline/#rocky-freshness) reads `MAX(time_column)` from the model's target table. |

Set at least one of `warn_after` and `error_after`: without a level the project has no freshness default and a model inherits nothing, so `time_column` alone is inert. A model with no `[freshness]` block of its own inherits this one **whole**, never field by field. Precedence, first match wins:

```
model sidecar [freshness]   ->  models/_defaults.toml [freshness]  ->  project [freshness]
```

`W005` fires on a model that has a temporal output column (`DATE`, `TIMESTAMP`, `TIMESTAMP_NTZ`) and no `freshness` declaration in scope. A level here puts a declaration in scope for every model, which silences it project-wide.

The scheduler uses this block as the staleness budget for a pipeline with [`[pipeline.NAME.schedule]`](#pipelinenameschedule) `freshness = true`. For a transformation pipeline, the smallest level its member models declare wins, and this value applies when none declares one. Every other pipeline type loads no member models, so this value is its only budget. The block is separate from the [`[pipeline.NAME.checks]`](#pipelinenamechecks) freshness check, which has its own `warn_after` / `error_after`.

### Freshness durations

Every freshness level (pipeline check, model sidecar, this block, product spec, per-source `freshness`) is a positive integer plus one unit: `s`, `h` or `d` (`"3600s"`, `"12h"`, `"7d"`). Rocky rejects minutes, uppercase units, zero and leading zeros.

```toml
[freshness]
warn_after  = "1h"
error_after = "6h"
time_column = "updated_at"
```

### Deprecated freshness spellings

These older keys still parse as deprecated aliases. `rocky validate` warns with `W058`, naming the file, key and replacement. A block that mixes an old key (`severity` included) with `warn_after` or `error_after` is rejected.

| Old spelling | Where | Replacement |
|---|---|---|
| `threshold_seconds = N` with `severity` (default `"error"`) | pipeline check | `error_after = "<N>s"`, or `warn_after` when `severity = "warning"` |
| `max_lag_seconds` or `expected_lag_seconds = N` with `severity` (default `"warning"`) | model sidecar and project block | `warn_after = "<N>s"`, or `error_after` when `severity = "error"` |
| `max_lag = "24h"` with `severity` | product spec | same rule as the model sidecar |

---

## `[schema_evolution]` (removed)

A config that declares this block fails to load, and the error says to delete it. Nothing read the block, so removing it changes no behaviour. A source-side column removal alone never schedules `ALTER TABLE ... DROP COLUMN`, and there is no opt-in. A full refresh or a drift-driven table recreation can still discard a target-only column. See [Schema drift](/concepts/schema-drift/) and issue #1616.

---

## `[resilience]`

The run loop's retry policy: whether a model that fails for a transient reason gets another attempt. It re-runs a whole **model**, unlike [`[adapter.NAME.retry]`](#adapternameretry) (one statement inside a connector) and [`[retry]`](#retry) (a connector retry cap across the run). Unlike the skip and reuse gates it is **on by default**, with a small budget: at most two retries per model and eight per run. Rocky retries only a failure it can prove transient. Permanent and unknown failures are never retried.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `true` | Master switch. `false` attempts every model once, with no classification and no backoff. |
| `transient_max_retries` | integer | `2` | Re-runs for a model that failed transiently (three attempts in total). `0` disables retry but keeps the classifier for observability. |
| `initial_backoff_ms` | integer | `500` | Wait before the first retry. |
| `max_backoff_ms` | integer | `30000` | Cap on exponential backoff growth. |
| `backoff_multiplier` | float | `2.0` | Multiplier applied after each retry. |
| `jitter` | bool | `true` | Add ±25% jitter so concurrent runs do not retry in lockstep. |
| `circuit_breaker_threshold` | integer | `3` | Stop retrying for the rest of the run after this many consecutive transient model failures. Each model still gets its one attempt. `0` disables the breaker. |
| `max_retries_per_run` | integer \| null | `8` | Ceiling on total retries in one run. `null` removes it; `0` forbids all retries. |
| `contain_failures` | bool | `true` | `true` withholds a model that fails while it runs, and its downstream closure; unrelated subtrees finish and Rocky reports `PartialFailure` with the blast radius. `false` restores fail-fast: no later layer starts, though running models finish. Every error-severity compile failure withholds its DAG descendants by default; a project that does not compile builds nothing. See [Failure containment](/advanced/failure-modes/#failure-containment-across-the-model-graph). |
| `auto_apply_additive_drift` | bool | `false` | `false` evolves the target for a new nullable upstream column with no policy gate. `true` routes the change through the [`[policy]`](#policy) plane first: Rocky applies only a provably additive change with an `allow` verdict on `schema_change.additive` (from a matching rule, or `default_agent_effect` when none matches). A freeze, an exhausted autonomy budget, an unreadable or untrusted ledger, or a decision row that fails to persist still refuses. With no `[policy]` block, nothing is granted. |

For single-attempt behaviour (for example in CI) set `enabled = false`. For fail-fast set `contain_failures = false`.

---

## `[imports.<name>]`

Check your models against another team's published schema at compile time. A producer publishes a snapshot of its compiled [IR](/reference/glossary/#ir-intermediate-representation) with `rocky publish-ir`. You vendor that file and point an `[imports.<name>]` block at it. `rocky compile` then fails when the producer drops a column you still read.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `path` | string | Yes | Directory holding the vendored snapshot files, relative to `rocky.toml`. |
| `snapshot` | string | Yes | Filename of the producer's current snapshot, relative to `path`. |
| `baseline` | string | No | Filename of the reviewed "before" snapshot. Rocky diffs it against `snapshot` to find columns the producer changed. Without it, the column-level diagnostics have nothing to compare. |
| `pin` | string | No | Recipe-hash pin. A concrete hash fails compilation (`E033`) when the vendored snapshot differs at all. `"*"` or unset trusts whatever is vendored. |

```toml
[imports.orders]
path     = "vendor/orders"
snapshot = "current.json"
baseline = "baseline.json"
pin      = "*"
```

`pin` is a whole-project tripwire on any drift. `baseline` is the column-level "before" image that lets Rocky report which reads broke. Nothing advances the baseline on its own: run `rocky imports update` after you review a producer change, and `rocky imports update --check` in CI to fail when a baseline is behind. Diagnostics (`E030`–`E034`, `W030`, `W031`): [Cross-team contracts](/concepts/cross-team-contracts/).

---

## `[branch]`

Require sign-off before a branch is promoted. Without this block the gate is off, and `rocky branch promote` runs unguarded.

### `[branch.approval]`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `required` | bool | `false` | `true` makes `rocky branch promote` refuse unless enough valid approvals exist for the branch. |
| `min_approvers` | integer | `1` | Valid approvals needed when `required = true`. |
| `allowed_signers` | list of strings | `[]` | When non-empty, only approvals from these signer emails count. Empty accepts any signer. |
| `max_age_seconds` | integer | `86400` | Approvals older than this are rejected, even when they still match the branch state. Default is 24 hours. |

An approval counts as valid only when all of these hold:

- Its signature verifies.
- Its recorded branch-state hash matches the branch as it stands now.
- It was signed inside `max_age_seconds`.
- Its signer is on the list, when `allowed_signers` is set.

---

## `[gc]`

Control what `rocky gc` may delete. Eviction is recorded, not destructive: an approved `rocky apply <gc-plan>` writes a durable tombstone and retires the artifact's ledger row, and the bytes stay where they are.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `physical_delete` | bool | `false` | Reserved for a future protocol-aware `VACUUM`. `true` is a **hard error** at apply time today. |

`rocky restore` can rebuild an evicted artifact only when the recipe is non-partitioned, content-addressed and reads no recorded upstreams, and it can still refuse (a missing tombstone, a re-derivation whose hash differs, a lost race on ledger reinstatement). What is retained is the eviction record and its provenance, not a promise that the bytes come back. See [governance and reclamation commands](/reference/commands/governance-reclamation/).

---

## `[policy]`

State who may change what, and let Rocky enforce it. Each rule maps a `(principal, capability, scope)` triple to one effect. Rocky evaluates the rules at the mutating [seams](/reference/glossary/#seam) (`rocky apply`, branch promote, the MCP write tools, `rocky run` run by an agent, and drift auto-apply when [`auto_apply_additive_drift`](#resilience) is on) and records every decision in the audit ledger. A run started by the native scheduler is a plain `rocky run` and is governed the same way. Evaluation rules, the `run` capability, the table names the gate sees, and an example: [Writing and testing policies](/guides/testing-policies/).

:::caution[An absent block does not gate agents]
With no `[policy]` block, the gate returns `NotConfigured` and allows the action whoever the principal is. `rocky policy check` *predicts* against the safe default posture (agents on mutating actions require review, humans never gated), so with no block its prediction is stricter than enforcement. Drift auto-apply with `auto_apply_additive_drift = true` governs as `require_review` with no block, so it refuses every drift change.
:::

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `version` | integer | Yes | Schema version. Must be `1`. |
| `default_agent_effect` | string | No | Effect for an `agent` on a mutating capability when no rule matches. Defaults to `"require_review"`. |
| `rules` | list of tables | No | The rules, written as `[[policy.rules]]`. |
| `tests` | list of tables | No | Scenario assertions run by `rocky policy test`, written as `[[policy.tests]]`. Never read by enforcement. |

- **Effects.** `"allow"`; `"require_review"` (permitted only after a human signs off); `"deny"` (no `allow` overturns it).
- **Principals.** `"human"` or `"agent"` (a non-human caller). You supply the principal explicitly, for example `rocky policy check --principal agent`.
- **Capabilities.** `read` (always allowed), `propose`, `apply`, `promote`, `backfill`, `gc`, `restore`, `retry`, `run`, `quarantine`, plus the refinements `schema_change.additive`, `schema_change.breaking` and `value_change`. A rule naming `apply` or `promote` also matches those three refinements. Every other capability, and each refinement, matches only itself.

### `[[policy.rules]]`

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `principal` | string | Yes | `"human"` or `"agent"`. |
| `capability` | string | Yes | One of the capabilities above. |
| `effect` | string | Yes | `"allow"`, `"require_review"`, or `"deny"`. |
| `scope` | table | No | Which models the rule covers. See below. |
| `verify_after` | list of strings | No | Named checks that must pass after a mutation this rule governed lands. A failing **or absent** check halts the apply and raises an alert. Empty means no post-apply gate. A `run` rule cannot set it (`V046`). |
| `autonomy_budget` | table | No | `{ failures = <n>, window = "<N>d" \| "<N>h" }`. When verify-after failures inside the rolling window reach `failures`, the rule degrades to `require_review`. It only tightens, and recovers as old failures age out. |
| `conditions` | table | No | Parsed and ignored. Reserved so a config written against a later version still loads. |

### `[policy.rules.scope]`

A model matches only when it satisfies **every** key you set. A scope with neither `any = true` nor a real predicate is rejected at config load.

| Field | Type | Description |
|-------|------|-------------|
| `any` | bool | Match every model. Mutually exclusive with every other key. |
| `models` | list of strings | Glob patterns (`*`, `?`) over the model name. |
| `tags` | table | Required `key = value` tags. The model must carry all of them. |
| `classifications` | list of strings | At least one column carries any listed classification. |
| `exclude_classifications` | list of strings | No column carries any listed classification. |
| `contracted` | bool | The model's contracted status equals this value. |
| `layer` | string | The model's `layer` tag equals this value. |
| `max_downstreams` | integer | Blast-radius ceiling. Not a match predicate: an `allow` degrades to `require_review` when the transitive downstream count exceeds it **or cannot be computed**. `deny` and `require_review` rules are unaffected. |

Pin a policy with `[[policy.tests]]` scenarios and run `rocky policy test` in CI ([Writing and testing policies](/guides/testing-policies/)). Where the gates sit: [Operating Rocky with agents](/concepts/operating-rocky-with-agents/).

---

## `[residency]`

Say which region each pipeline and the AI endpoint is in, and stop a plan or an AI request that moves data between regions. A region is a label of lowercase letters, digits and hyphens that starts with a letter or digit (`"eu-west-1"`, `"eu"`). Rocky compares labels as exact text and never corrects one: `"EU"` or `" eu"` is a `V060` error, whether or not `enforce` is on. How the checks work and which AI features carry which data: [Data residency](/guides/governance/#data-residency).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enforce` | bool | `false` | Require a `region` label on every source, target and the `[ai]` block. |
| `allow` | array of `{ from, to }` | `[]` | Cross-region moves you mean to make. Each entry lets data move from `from` to `to`, in that direction only. |

```toml
[residency]
enforce = true
allow = [{ from = "eu-west-1", to = "us-east-1" }]

[ai]
region = "us-east-1"

[pipeline.bronze.source]
region = "eu-west-1"

[pipeline.bronze.target]
region = "eu-west-1"
```

| Code | Raised when |
|---|---|
| `V060` | `enforce = true` and a required `region` is missing or empty (replication and snapshot: `source` and `target`; transformation, quality and load: `target`; any: `[ai]`), or a label is malformed. |
| `V061` | A replication or snapshot pipeline's `source.region` and `target.region` differ with no matching `allow` entry. Checked whether or not `enforce` is on. |
| `V062` | An AI request carries data from a region that differs from `[ai] region` with no matching `allow` entry. `rocky validate` also warns for each pipeline label that differs from `[ai] region`. |

`rocky validate` reports `V060` and `V061` offline, and every command that loads the config refuses them.

---

## `[fulfill]`

Configure the agent driver for the experimental `rocky fulfill` loop. The deterministic commands never read this block. Rocky refuses an unknown field here. See [`rocky fulfill` configuration](/reference/commands/fulfill/#configuration) for the fields and the sandbox caveats.

---

## `[connector.NAME]`

Declare a child connector: a program that `rocky load --connector` runs once and whose Parquet output it loads into one table. Rocky refuses an unknown key here. The [child connectors guide](/guides/child-connectors/) explains the manifest, the limits and the guarantees.

```toml
[connector.stripe_charges]
manifest   = "connectors/stripe/connector.toml"
output_uri = "file://./.rocky/landing/stripe"
secrets    = ["STRIPE_API_KEY"]
target     = { catalog = "raw", schema = "stripe", table = "charges" }
retries    = 1

[connector.stripe_charges.limits]
wall_time_secs = 3600

[connector.stripe_charges.options]
account = "acct_main"
page_size = 100
```

| Key | Default | Meaning |
|---|---|---|
| `manifest` | none | Path to `connector.toml`, relative to the directory of `rocky.toml`. |
| `output_uri` | none | `file://` only. `s3://` and `gs://` are refused as "not supported yet". |
| `secrets` | `[]` | Names of environment variables to hand to the program. |
| `target` | none | `catalog`, `schema` and `table`. `adapter` defaults to `default`. A BigQuery adapter is refused. |
| `retries` | `1` | Extra attempts after a failed one. At most `10`. |
| `contract` | none | A data contract, checked on the staging table. |
| `limits` | see guide | `wall_time_secs` (3600), `cpu_time_secs`, `memory_bytes`, `output_bytes` (5 GiB). The smaller of this and the manifest wins. |
| `keep_scratch` | `false` | Keep the scratch directory of a successful run. By default Rocky removes it after the state commit. |
| `options` | `{}` | Settings for the program, as any TOML table. Rocky writes it as JSON to `options.json` in the scratch directory. At most 64 KiB as JSON. Not for secrets: the file is on disk in clear. See [Options](/guides/child-connectors/#options). |

---

## Full Example

A Fivetran → Databricks replication pipeline with governance. The [minimal config](#config-at-a-glance) at the top of this page is the credential-free DuckDB version.

```toml
[adapter.fivetran]
type = "fivetran"
kind = "discovery"
destination_id = "${FIVETRAN_DESTINATION_ID}"
api_key = "${FIVETRAN_API_KEY}"
api_secret = "${FIVETRAN_API_SECRET}"

[adapter.prod]
type = "databricks"
host = "${DATABRICKS_HOST}"
http_path = "${DATABRICKS_HTTP_PATH}"
token = "${DATABRICKS_TOKEN}"

[pipeline.bronze]
type = "replication"
strategy = "incremental"
timestamp_column = "_fivetran_synced"

[pipeline.bronze.source]
adapter = "prod"

[pipeline.bronze.source.discovery]
adapter = "fivetran"

[pipeline.bronze.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["source"]

[pipeline.bronze.target]
adapter = "prod"
catalog_template = "warehouse"
schema_template = "stage__{source}"

[pipeline.bronze.target.governance]
auto_create_catalogs = true
auto_create_schemas = true

[pipeline.bronze.target.governance.tags]
managed_by = "rocky"

[[pipeline.bronze.target.governance.grants]]
principal = "group:data_engineers"
permissions = ["USE CATALOG", "MANAGE"]

[pipeline.bronze.checks]
enabled = true
row_count = true
freshness = { error_after = "24h" }

[pipeline.bronze.execution]
concurrency = 8

[state]
backend = "${ROCKY_STATE_BACKEND:-local}"
```

---

## Hooks

Run a shell command or post to an HTTP endpoint when something happens in a pipeline. A shell hook receives the event's JSON context on stdin. A webhook posts a templated body to a URL.

```toml
[[hook.on_pipeline_start]]            # shell hook: JSON context on stdin
command = "bash scripts/notify.sh"
timeout_ms = 5000
on_failure = "warn"                   # abort | warn | ignore

[hook.webhooks.on_pipeline_complete]  # webhook: HTTP POST
url = "https://hooks.slack.com/services/T.../B.../xxx"
preset = "slack"
secret = "${WEBHOOK_SECRET}"
```

### Hook Events

Name the table after the event's config key, always in the `on_<event>` form. Rocky treats a key without the `on_` prefix as unknown and ignores it silently, so check the spelling against this list.

| Config key | Trigger |
|-------|---------|
| `on_pipeline_start` | Pipeline execution begins |
| `on_discover_complete` | Source discovery finishes |
| `on_compile_complete` | Compilation finishes |
| `on_pipeline_complete` | Pipeline execution succeeds |
| `on_pipeline_error` | Pipeline execution fails |
| `on_before_materialize` | Before a table is materialized |
| `on_after_materialize` | After a table is materialized |
| `on_materialize_error` | Table materialization fails |
| `on_before_model_run` | Before a compiled model runs |
| `on_after_model_run` | After a compiled model runs |
| `on_model_error` | Compiled model execution fails |
| `on_before_checks` | Before a table's quality checks run |
| `on_check_result` | A quality check completes |
| `on_after_checks` | After a table's quality checks run |
| `on_drift_detected` | Schema drift detected |
| `on_anomaly_detected` | Row count anomaly detected |
| `on_state_synced` | State store sync completes |
| `on_budget_breach` | A run-level budget limit is breached |

### Hook Config Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `command` | string | required | Shell command to execute |
| `timeout_ms` | number | 30000 | Max execution time in milliseconds |
| `on_failure` | string | `"warn"` | Behavior on failure: `abort`, `warn`, or `ignore` |
| `env` | object | {} | Extra environment variables |

### Webhook Config Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | string | required | Webhook endpoint URL |
| `preset` | string | — | Built-in preset: `slack`, `pagerduty`, `datadog`, `teams` |
| `method` | string | `"POST"` | HTTP method |
| `headers` | object | {} | Additional HTTP headers |
| `body_template` | string | — | Mustache-style template (`{{event}}`, `{{model}}`, `{{error}}`) |
| `secret` | string | — | HMAC-SHA256 signing key |
| `timeout_ms` | number | 10000 | Request timeout |
| `async` | boolean | false | Fire-and-forget (don't wait for response) |
| `on_failure` | string | `"warn"` | Behavior on failure |
| `retry_count` | number | 0 | Number of retries |
| `retry_delay_ms` | number | 1000 | Delay between retries |
