---
title: Configuration
description: Complete rocky.toml reference
sidebar:
  order: 2
---

Rocky reads one `rocky.toml` file for everything. You name your adapters (`[adapter.NAME]`) and your pipelines (`[pipeline.NAME]`), so one file can hold several sources, several warehouses, and several pipelines side by side.

:::caution[A key that does not exist is usually an error, not a no-op]
Rocky rejects an unrecognized key in most blocks. A typo (`tooken` for `token`, `retires` for `retries`) fails the config load with a message naming the key. This is deliberate: a silently ignored setting looks like it is working. Two blocks are exceptions. `[hook.*]` ignores an unknown event key silently, and `[adapter.NAME.extra]` passes every key through untouched. Check a key against this page before you add it.
:::

## Config Inference and Defaults

Write less by leaning on the defaults. Rocky fills in most fields, so a small project needs only a handful of lines.

| What | Default | When to omit |
|---|---|---|
| `pipeline.type` | `"replication"` | Always (unless using a different type) |
| Unnamed `[adapter]` with a `type` key | Auto-wraps as `adapter.default` | Single-adapter projects |
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

## At a glance

```toml
# Define one or more adapter instances by name
[adapter.local]
type = "duckdb"
path = "warehouse.duckdb"

# Define one or more pipelines and reference adapters by name
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

The same config can declare additional adapters (`[adapter.prod_databricks]`, `[adapter.prod_fivetran]`) and additional pipelines, and pipelines select which adapters to use via the `adapter = "..."` field on `source`/`target`.

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

### Default Values

Use `${VAR_NAME:-default}` to provide a fallback when a variable is unset or empty:

```toml
[state]
backend = "${ROCKY_STATE_BACKEND:-local}"
s3_bucket = "${ROCKY_STATE_BUCKET:-}"
```

---

## `[adapter.NAME]`

Declare a connection once, then reference it by name from any number of pipelines. Each `[adapter.NAME]` block is one connection. You choose the name; `type` selects which adapter implementation handles it.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string | Yes | Adapter type. One of `"databricks"`, `"snowflake"`, `"duckdb"`, `"bigquery"`, `"trino"`, `"fivetran"`, `"airbyte"`, `"iceberg"`, `"manual"`. An unrecognized value is a hard error. |
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

The connection fields, authentication, and examples for each adapter type live on their own page:

- [DuckDB](/reference/adapters/duckdb/) — local in-process execution
- [Databricks](/reference/adapters/databricks/) — SQL warehouse + Unity Catalog governance
- [Snowflake](/reference/adapters/snowflake/) — PAT, OAuth, key-pair, and password auth
- [BigQuery](/reference/adapters/bigquery/) — project/location plus environment-supplied credentials
- [Fivetran](/reference/adapters/fivetran/) — metadata-only source discovery

`type = "trino"`, `type = "airbyte"`, and `type = "iceberg"` are accepted by the config parser but have no dedicated page yet; configure adapter-specific keys through [`[adapter.NAME.extra]`](#adaptername).


### `type = "manual"`

Define source schemas and tables inline in `rocky.toml` instead of discovering them from an API. Use it for tests and for small sources whose shape does not change.

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

```toml
[adapter.prod.retry]
max_retries = 5
initial_backoff_ms = 500
max_backoff_ms = 60000
circuit_breaker_recovery_timeout_secs = 30
```

When the breaker trips, Rocky emits a `circuit_breaker_tripped` pipeline event; on auto-recovery it emits `circuit_breaker_recovered`. Hook subscribers can observe both without polling the adapter. See [Hooks](/concepts/hooks/).

---

## `[pipeline.NAME]`

Declare a unit of work: what it reads, where it writes, and how. Each `[pipeline.NAME]` block is one pipeline. You choose the name, and every command takes `--pipeline NAME` to select one when the config declares more than one.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `type` | string | No | `"replication"` | Pipeline type. One of `"replication"`, `"transformation"`, `"quality"`, `"snapshot"`, `"load"`. The remaining fields depend on the type; the fields below apply to `"replication"` (the default). |
| `strategy` | string | No | `"incremental"` | Replication strategy: `"incremental"` or `"full_refresh"`. |
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

Rocky inserts the `value` field as a SQL expression, so write `"NULL"` for a null and a function call like `"CURRENT_TIMESTAMP()"` for a computed value.

### `[pipeline.NAME.source]`

Point the pipeline at the system it reads from.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `adapter` | string | Yes | Name of the adapter that owns the source data. Must match a `[adapter.NAME]` key. |
| `catalog` | string | No | Source catalog name (used by warehouse-resident sources like Databricks). |

```toml
[pipeline.bronze.source]
adapter = "fivetran"
```

### `[pipeline.NAME.source.discovery]`

Use a different adapter to list schemas and tables than the one that reads the rows. Reach for this when one system knows what exists (DuckDB, say) while the data itself lives elsewhere. Omit the block and Rocky discovers through the source `adapter`.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `adapter` | string | Yes | — | Adapter name to use for discovery. |
| `report_new_sources` | bool | No | `false` | Diff the discovered source inventory against the prior persisted snapshot and report first-seen schemas in the discover output's `new_sources`. The first discover of a pipeline records the baseline and reports nothing. Off by default — the diff and its state write only happen when opted in. |
| `on_collision` | `"off"` \| `"warn"` \| `"error"` | No | `"off"` | Cross-source collision detection. When the same external object (e.g. an ad account) is onboarded under two schemas, its data lands in two target tables and silently doubles any downstream `UNION ALL`. `warn` reports the pairs in the discover output's `collision_candidates` and emits a `source_collision_detected` event; `error` additionally fails the discover so a colliding onboard can't silently create a catalog. `off` (default) skips detection entirely. Only adapters that resolve external object ids (e.g. Fivetran) participate; others contribute nothing. |

```toml
[pipeline.bronze.source.discovery]
adapter = "fivetran"
report_new_sources = true   # surface freshly-onboarded sources in `new_sources`
on_collision = "warn"       # surface same-object-twice onboards in `collision_candidates`
```

See [Cross-source duplicate detection](#cross-source-duplicate-detection) for the counterpart that runs during replication rather than at discovery.

### `[pipeline.NAME.source.schema_pattern]`

Teach Rocky to read structure out of your source schema names. It can then route each source to the right target, and you can select sources with `--filter`. A schema like `src__acme__us_west__shopify` becomes a set of named components.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `prefix` | string | Yes | Prefix that identifies managed schemas (e.g., `"src__"`). |
| `separator` | string | Yes | Separator between components (e.g., `"__"`). |
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

```toml
[pipeline.bronze.target]
adapter = "prod"
catalog_template = "warehouse"
schema_template = "stage__{source}"
```

Given `source=shopify`:

| Template | Result |
|----------|--------|
| `warehouse` | `warehouse` (static, no substitution) |
| `stage__{source}` | `stage__shopify` |

The two blocks work as one path. `schema_pattern` takes a source schema apart, and the target templates put a destination name back together from the pieces:

```
  source schema                  parsed components        target name
  ─────────────                  ─────────────────        ───────────
                    schema_pattern                template
  src__acme__shopify ──────────► client  = acme  ─────────► warehouse
                       parse     source  = shopify  fill    stage__shopify
                                                    │
   prefix "src__"                                   └── catalog_template
   separator "__"                                       = "warehouse"
   components ["client","source"]                       schema_template
                                                        = "stage__{source}"
```

For per-tenant catalogs, put a placeholder in `catalog_template` too: `catalog_template = "{tenant}_warehouse"` with `components = ["tenant", "regions...", "source"]`. See [Schema Patterns](/concepts/schema-patterns/) for the full pattern reference.

One name clash to know about. This `schema_template` routes replication targets from parsed source schemas. The config-group `schema_template` is a different feature: it fills its placeholders from a model's `[args]` to route a fan-out of transformation models. See [Config groups](/reference/model-format/#config-groups).

### `[pipeline.NAME.target.governance]`

Have Rocky create and label the catalogs and schemas it writes to, instead of provisioning them by hand. The block covers four things: catalog and schema creation, tags, grants, and workspace isolation.

Tagging, grants, and isolation run against Databricks Unity Catalog APIs, so they apply only when the target adapter is Databricks. The two `auto_create_*` flags work on every adapter that emits `CREATE SCHEMA` SQL.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `auto_create_catalogs` | bool | `false` | Create target catalogs if they do not exist. |
| `auto_create_schemas` | bool | `false` | Create target schemas if they do not exist. Honored on **both** replication and transformation pipeline targets (transformation parity landed in engine v1.29.0; prior versions silently no-op'd on transformation pipelines, surfacing as a "Schema with name X does not exist" execute-time error). |
| `tags` | table | `{}` | Tags applied to managed catalogs, schemas, and tables. |
| `grants` | list | `[]` | Catalog-level grants. Each entry has `principal` (string) and `permissions` (list of strings). |
| `schema_grants` | list | `[]` | Schema-level grants. Same format as `grants`. |
| `isolation` | table | | Workspace isolation settings (see below). |

```toml
[pipeline.bronze.target.governance]
auto_create_catalogs = true
auto_create_schemas = true

[pipeline.bronze.target.governance.tags]
managed_by = "rocky"
environment = "production"

[[pipeline.bronze.target.governance.grants]]
principal = "group:data_engineers"
permissions = ["USE CATALOG", "MANAGE"]

[[pipeline.bronze.target.governance.schema_grants]]
principal = "group:data_engineers"
permissions = ["USE SCHEMA", "SELECT", "MODIFY"]
```

### `[pipeline.NAME.target.governance.isolation]`

Restrict which Databricks workspaces can reach the catalogs Rocky manages. Rocky binds each managed catalog to the workspaces you list, and can set the catalog's isolation mode so no other workspace sees it.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `false` | Set catalog isolation mode to `ISOLATED`. |
| `workspace_ids` | list of tables | `[]` | Workspace bindings; see below. |

Each entry in `workspace_ids` is a table with two fields:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | integer | required | Databricks workspace ID. |
| `binding_type` | string | `"READ_WRITE"` | Access level: `"READ_WRITE"` or `"READ_ONLY"`. |

```toml
[pipeline.bronze.target.governance.isolation]
enabled = true

[[pipeline.bronze.target.governance.isolation.workspace_ids]]
id = 7474656540609532
binding_type = "READ_WRITE"

[[pipeline.bronze.target.governance.isolation.workspace_ids]]
id = 7474647537929812
binding_type = "READ_ONLY"
```

`binding_type` maps to the Databricks API values `BINDING_TYPE_READ_WRITE` and `BINDING_TYPE_READ_ONLY`.

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
| `freshness` | table | | `{ threshold_seconds = N, overrides = { ... } }`. |
| `null_rate` | table | | `{ columns = [...], threshold = 0.0–1.0, sample_percent = 10 }`. |
| `custom` | list | `[]` | Custom SQL checks. Each entry has `name`, `sql`, and optional `threshold`. |
| `anomaly_threshold_pct` | float | `50.0` | Row count deviation percentage that triggers an anomaly. Set to 0 to disable. |
| `quarantine` | table | | `{ mode = "split" \| "tag" \| "drop" }`. See below. |
| `assertions` | list | `[]` | Repeated `[[assertions]]` blocks (DQX parity). See below. |
| `cross_source_overlap` | table | | Flags the same business key appearing across sibling sources that feed one consolidation target. See [Cross-source duplicate detection](#cross-source-duplicate-detection). |

```toml
[pipeline.bronze.checks]
enabled = true
row_count = true
column_match = true
freshness = { threshold_seconds = 86400 }
anomaly_threshold_pct = 50.0
```

#### `[[pipeline.NAME.checks.assertions]]`

Declarative model-level assertions. Each block declares a `type` and type-specific parameters. All assertions share the same base fields:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `table` | string | (required) | Unqualified target table the assertion runs against. When assertions live in a model's sidecar TOML the table is implied; in pipeline-level `[[checks.assertions]]` blocks (shown below) it must be set explicitly. |
| `name` | string | | Optional identifier used as the result's `name`; synthesized from `{kind}:{column}` when unset. Set it to disambiguate multiple assertions on the same table/kind/column. |
| `type` | string | (required) | One of: `not_null`, `unique`, `unique_expr`, `accepted_values`, `relationships`, `expression`, `row_count_range`, `in_range`, `regex_match`, `aggregate`, `composite`, `not_in_future`, `older_than_n_days`. |
| `column` | string | | Required for row-level column kinds (`not_null`, `unique`, `accepted_values`, `relationships`, `in_range`, `regex_match`, `not_in_future`, `older_than_n_days`). |
| `severity` | string | `"error"` | `error` fails the pipeline (subject to `fail_on_error`); `warning` reports but never fails. |
| `filter` | string | | SQL boolean predicate that scopes the assertion to a subset of rows. |

Type-specific parameters:

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
| `unique_expr` | `key_expr: String` (derived SQL key, e.g. `md5(tenant \|\| '-' \|\| id)`; passed through verbatim) |
| `older_than_n_days` | `days: u32` |

```toml
[[pipeline.silver.checks.assertions]]
table = "orders"
type = "not_null"
column = "order_id"

[[pipeline.silver.checks.assertions]]
table = "orders"
type = "accepted_values"
column = "status"
values = ["pending", "shipped", "delivered"]
severity = "warning"

[[pipeline.silver.checks.assertions]]
table = "orders"
type = "in_range"
column = "amount_cents"
min = "0"
filter = "region = 'US' AND status != 'cancelled'"

[[pipeline.silver.checks.assertions]]
table = "orders"
type = "aggregate"
op = "sum"
cmp = "gt"
value = "0"
column = "amount_cents"

[[pipeline.silver.checks.assertions]]
table = "order_lines"
type = "composite"
kind = "unique"
columns = ["order_id", "line_item_id"]

[[pipeline.silver.checks.assertions]]
table = "orders"
type = "unique_expr"
key_expr = "md5(tenant_id || '-' || order_id)"
```

Use `unique_expr` when the meaningful identity is a *computed* value rather than any stored column, for example a surrogate key built to be stable across a multi-tenant union, which neither `unique` (single column) nor `composite` (column tuple) can express.

#### `[pipeline.NAME.checks.quarantine]`

Keep the bad rows out of the clean table instead of only counting them. Pick a mode and Rocky routes rows that fail a row-level assertion into their own table, or marks them in place.

| Mode | Behavior |
|---|---|
| `split` | Materializes `<target>` (valid rows) and `<target>__quarantine` (failing rows). Downstream models see only the clean table. |
| `tag` | Adds `__dqx_valid` boolean column; failing rows stay with `__dqx_valid = FALSE`. |
| `drop` | Drops failing rows from `<target>`. |

Set-based and table-level assertions (`unique`, `unique_expr`, `composite`, `row_count_range`, `aggregate`) run as post-hoc checks regardless of mode.

#### Cross-source duplicate detection

The assertions above (especially `unique` / `unique_expr` / `composite`) also run on **replication** pipelines, not just transformation/quality ones, so a target table that's silently doubled by the same source arriving twice is caught at load time, not three models downstream.

For the cross-*table* case (the same business key arriving through two **sibling** sources that later get `UNION`-ed into one consolidation target), use `[pipeline.NAME.checks.cross_source_overlap]`. A per-table `unique` check passes on each table individually; only an overlap check spanning the siblings sees the duplication.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `keys` | list of strings | `[]` | Business-key columns whose shared value across sibling tables signals a duplicate. Mutually exclusive with `key_expr`. |
| `key_expr` | string | | Derived business-key expression (e.g. `md5(a \|\| '-' \|\| b)`) for sources without a single natural key. Mutually exclusive with `keys`. Passed through verbatim. |
| `severity` | string | `"error"` | `error` fails the pipeline (subject to `fail_on_error`); `warning` reports but never fails. |
| `max_overlap_rows` | integer | `0` | Overlap-key count above which the check fails. `0` means any overlap fails. |
| `sample` | integer | `20` | Maximum overlapping keys attached to the result for triage. |

Exactly one of `keys` or `key_expr` is required. Sibling tables whose key can't be evaluated (missing column / keyless) are skipped with a logged reason rather than erroring.

```toml
[pipeline.bronze.checks.cross_source_overlap]
keys = ["order_id"]
severity = "warning"
max_overlap_rows = 0
```

This is the **detective** counterpart to discover-time `on_collision` (the **preventive** catch). See [Data Quality Checks](/concepts/data-quality-checks/#cross-source-overlap) for the full semantics.

### `[pipeline.NAME.execution]`

Tune how many tables this pipeline processes at once, and when it gives up. The defaults suit a remote warehouse; pin `concurrency` to a number when you need a predictable load.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `concurrency` | string \| integer | `"adaptive"` | Max tables processed in parallel. `"adaptive"` (default) uses an AIMD throttle starting at 32 and adjusting to rate-limit signals — best for remote warehouses. A fixed integer (e.g. `8`) pins concurrency; `1` is serial. |
| `fail_fast` | bool | `false` | Abort all remaining tables on first error. |
| `error_rate_abort_pct` | integer | `50` | Abort if error rate exceeds this percentage (0–100). Set to 0 to disable. |
| `table_retries` | integer | `1` | Times to retry failed tables after the initial parallel phase. Set to 0 to disable. |

```toml
[pipeline.bronze.execution]
concurrency = 8
fail_fast = false
error_rate_abort_pct = 50
table_retries = 1
```

---

### `[pipeline.NAME.schedule]`

Declare when this pipeline is due, so [`rocky tick`](/guides/running-without-an-orchestrator/#native-scheduling-with-rocky-tick-experimental) can run it without an external orchestrator. Every field is optional. Omit the block and the pipeline has no standing demand: it runs only when you invoke it. **Experimental** while the reconciler soaks.

It works on `replication`, `transformation`, `quality`, and `snapshot` pipelines. A `load` pipeline cannot join the schedule graph yet, for two reasons. A load re-ingests every discovered file on each run rather than incrementally, so scheduling one would duplicate data. It also records no run the scheduler can observe. `rocky validate` rejects a scheduled load pipeline (`V044`) and an `after` that names one (`V045`).

Run one `rocky tick` timer per project. Schedule state is per-machine. A remote `[state]` backend never replicates the scheduler's cursor and claim tables, and remote state has no cross-host locking. Two hosts ticking the same project therefore both fire the same occurrence.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `cron` | string | — | Standard 5-field cron expression (`minute hour day-of-month month day-of-week`). Due at each occurrence. First-ever evaluation anchors at "now" and waits for the next occurrence rather than firing immediately. |
| `timezone` | string | project `[schedule].timezone`, else `"UTC"` | IANA timezone name (e.g. `"Europe/Lisbon"`) the `cron` occurrences are evaluated in. DST transitions are handled: a skipped wall-clock hour fires at the next valid instant once, a repeated hour fires on the first occurrence only. |
| `after` | array of strings | `[]` | Upstream pipeline names. Due once **every** listed pipeline has a successful run that completed after this pipeline's own latest success *started*. A partial-success (exit 2) upstream run does **not** count. Cycles are a validation error. |
| `freshness` | bool | `false` | When `true`, the pipeline is due once its own run-staleness exceeds its freshness budget. That budget is the minimum of its member models' `max_lag_seconds`, falling back to the project [`[freshness]`](#freshness) `expected_lag_seconds`. This is run-staleness, **not** a warehouse data-freshness probe; the reconciler issues no queries. `freshness = true` with no resolvable budget is a validation error. |
| `catchup` | string | `"latest"` | Policy when more than one cron occurrence elapsed since the last fire. `"latest"` fires one demand at the most recent missed occurrence; `"skip"` advances the anchor and runs nothing. `"all"` is **rejected** at validation — Rocky runs are watermark-driven, not windowed, so replaying every missed occurrence is pure cost with no extra data. |
| `retry` | table | `{ max = 0 }` | In-tick retry on failure. `retry.max` is the number of *additional* immediate re-submissions after the first attempt fails. The reconciler never sleeps between attempts; minutes-scale spacing between ticks is the always-on cross-tick backoff, not this knob. Partial (exit 2) runs are never retried. |
| `timeout_minutes` | integer | `0` | Scheduler-level timeout for a launched run. `0` means none — the run's own limits apply. On elapse the child is terminated gracefully, then forcibly. |
| `enabled` | bool | `true` | When `false`, demand is suppressed but the config is kept (it shows as `disabled` in the tick's `skipped[]`). |

A pipeline may combine sources — any one being due makes the pipeline due. A `[schedule]` block with none of `cron`/`after`/`freshness` set and `enabled = true` is an inert-config warning.

```toml
[pipeline.raw]
type = "replication"
[pipeline.raw.schedule]
cron = "0 3 * * *"
timezone = "Europe/Lisbon"

[pipeline.staging]
type = "transformation"
models = "models/**"
[pipeline.staging.schedule]
after = ["raw"]            # runs in the same tick, after raw, once raw has a newer success
freshness = true          # ...and also whenever staging's output goes stale
retry = { max = 2 }
```

Project-level defaults live in a top-level `[schedule]` block:

```toml
[schedule]
timezone = "UTC"          # default timezone for every pipeline's cron
poll_interval_seconds = 15 # resident-loop cadence; not consumed by the one-shot `rocky tick`
```

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
| `on_upload_failure` | string | `"skip"` | What to do when upload exhausts retries + circuit-breaker. `"skip"` logs a warning and continues (state goes stale, next run re-derives); `"fail"` propagates the error. |
| `namespacing` | string | `"none"` | State-file namespacing policy. `"none"` (default) keeps one global state file — byte-identical to a project that omits this key. `"pipeline"` gives each pipeline its own state file (see [State namespacing](#state-namespacing) below). |
| `concurrency_control` | string | `"off"` | `"off"` (default) uploads unconditionally — last writer wins. `"cas"` makes the end-of-run upload conditional on the remote object still carrying the generation this run downloaded, so a run that lost a cross-pod race fails closed instead of erasing the winner. See [Concurrent writers](#concurrent-writers) below. |
| `on_schema_mismatch` | string | `"recreate"` | What to do when the binary opens a state store written by a **newer** binary, which happens mid-way through a rolling upgrade. `"recreate"` logs one warning, starts from fresh local state, does one full-refresh run, and never writes the downgraded state back to the shared tier. `"fail"` aborts the open instead. Only the run path honours this; inspection and branch commands always hard-fail on a forward-incompatible store. |
| `freeze_marker_writes` | bool | `false` | Write durable freeze and unfreeze marker objects beside the remote state file (under `<prefix>/freeze/` and `<prefix>/unfreeze/`) when `rocky policy freeze` / `unfreeze` run. Reading and enforcing markers is always on wherever a durable object tier exists. You can therefore upgrade a whole fleet to marker readers before any marker is written. Requires `backend = "s3"`, `"gcs"`, or `"tiered"`; setting it on `"local"` or `"valkey"` is a hard error rather than a silent no-op. |

**Local (default):**

```toml
[state]
backend = "local"
```

**S3 (durable, for ephemeral environments):**

```toml
[state]
backend = "s3"
s3_bucket = "${ROCKY_STATE_BUCKET}"
s3_prefix = "rocky/state/"
```

**Valkey (low-latency, shared):**

```toml
[state]
backend = "valkey"
valkey_url = "${VALKEY_URL}"
```

**Tiered (Valkey + S3 fallback):**

```toml
[state]
backend = "tiered"
valkey_url = "${VALKEY_URL}"
s3_bucket = "${ROCKY_STATE_BUCKET}"
```

Tiered downloads from Valkey first (fast), falls back to S3 (durable). Uploads to both.

### Concurrent writers

Stop a second run from erasing a first when both share one `[state]` prefix. By default Rocky overwrites the remote state unconditionally. Two concurrent runs both download the ledger, both change it, and the second upload wipes the first. The first run's watermarks, run records, and policy rows are lost, and Rocky raises no error.

Set `concurrency_control = "cas"` to close that:

```toml
[state]
backend = "s3"
s3_bucket = "${ROCKY_STATE_BUCKET}"
concurrency_control = "cas"
```

The end-of-run upload then commits only if the remote object still carries the generation this run downloaded. A run that lost the race exits non-zero with a compare-and-swap conflict rather than overwriting the winner.

What it does not do is reconcile the two runs. You re-run the loser yourself. Rocky does not roll back the warehouse writes it already made, so a non-merge strategy can duplicate rows on that re-run. Prefer merge-style strategies for pipelines you expect to contend.

It needs a backend with a durable object tier: `s3`, `gcs`, or `tiered`. On `local` and `valkey` it downgrades to `off` with a warning, because neither offers a conditional write. Turning it on also stops the mid-run periodic uploader on every backend. A crashed run then leaves the remote ledger at its last committed generation instead of a partial mid-run snapshot.

**What `cas` does not yet cover.** It protects the end-of-run state upload and the `rocky policy freeze` / `unfreeze` ledger write, which retries onto the winner when it loses a race. The remaining single-record ledger seams — the state writes made by `gc apply` and `apply` — still upload unconditionally on every backend, so a concurrent `gc apply` can overwrite a run's committed state without raising a conflict and still exit zero. This is tracked as [issue #1228](https://github.com/rocky-data/rocky/issues/1228) and applies equally to `s3`, `gcs`, and `tiered`. Until it is closed, keep the orchestrator-level rule of one writer per `[state]` prefix for the seam commands.

**On `tiered`,** `cas` additionally makes the Valkey tier coherent with the durable object. The compare-and-swap runs against S3 first; only after it commits is the Valkey copy written, stored together with the generation it was committed at. A read may use the cached copy only after confirming that generation is still the durable object's — otherwise it reads S3. So a Valkey write that fails, a process that dies between the two, or a cache entry left over from an earlier run can no longer shadow durable state. Cached copies are held under a separate key from the `off` path's, so a fleet can move pods from `off` to `cas` one at a time.

With `concurrency_control = "off"` the tiered backend keeps its historical behaviour, including the stale-cache window: an `off` write carries no generation, so there is nothing for a read to validate a cached copy against.

### `[state.retry]`

Decide how hard Rocky retries a state upload or download that failed for a transient reason: a network hiccup, a transient 5xx, an endpoint that hung past the per-request HTTP timeout. The block has the same shape as [`[adapter.NAME.retry]`](#adapternameretry), so both layers read the same way. Retries share the outer `transfer_timeout_seconds` budget rather than extending it, so the total wall-clock ceiling does not move.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_retries` | int | `3` | Maximum retry attempts per transfer. Set to `0` to disable retries. |
| `initial_backoff_ms` | int | `1000` | Initial backoff before the first retry. |
| `max_backoff_ms` | int | `30000` | Cap on exponential backoff growth. |
| `backoff_multiplier` | float | `2.0` | Multiplier applied between retries (e.g. 2.0 = doubling). |
| `jitter` | bool | `true` | Add random jitter to prevent concurrent runs from retrying in lockstep. |
| `circuit_breaker_threshold` | int | `5` | Trip the breaker after this many consecutive failures. `0` disables. |
| `circuit_breaker_recovery_timeout_secs` | int \| null | `null` | Seconds in `Open` before a half-open trial is allowed. `null` = manual reset only. |
| `max_retries_per_run` | int \| null | `null` | Cross-transfer retry budget for a single run. `null` = unbounded (per-transfer `max_retries` is the only cap). |

```toml
[state]
backend = "s3"
s3_bucket = "${ROCKY_STATE_BUCKET}"
on_upload_failure = "fail"    # strict: treat state durability as required

[state.retry]
max_retries = 5
circuit_breaker_threshold = 3
circuit_breaker_recovery_timeout_secs = 30
```

Every transfer ends with a structured `outcome` field on its `state.upload` or `state.download` event: `ok`, `absent`, `timeout`, `error_then_fresh`, `skipped_after_failure`, `transient_exhausted`, `circuit_open`, or `budget_exhausted`. Build alerts on those values, not on the free-form log message.

### `[state.idempotency]`

Tune how `rocky plan --idempotency-key <KEY>` deduplicates a retried run (the `rocky run --idempotency-key` alias reads the same block). Every field is optional and takes the default shown. The block does nothing on a run that passes no key, and Rocky rejects an unknown field rather than ignoring it.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `retention_days` | integer | `30` | Lifetime of a terminal idempotency stamp before garbage collection. GC runs during the state upload sweep; no separate cron. |
| `dedup_on` | string | `"success"` | Which terminal statuses count as "already processed". `"success"` only stamps successful runs (failures stay claimable for retries); `"any"` stamps every terminal status. |
| `in_flight_ttl_hours` | integer | `24` | Hours after which an `InFlight` claim is treated as a crashed-pod corpse and adopted by a fresh caller. Informational on Valkey/tiered backends, which set the TTL server-side via `SET NX EX`. |

```toml
[state.idempotency]
retention_days = 30
dedup_on = "success"
in_flight_ttl_hours = 24
```

Stamps live in the `IDEMPOTENCY_KEYS` redb table and replicate on tiered backends so sibling pods see the same entry. See [`rocky plan --idempotency-key`](/reference/cli/#rocky-run) for the three possible outcomes (`fresh_run`, `skipped_idempotent`, `skipped_in_flight`).

#### Bucket-native lifecycle for object-store backends

Rocky's built-in sweep deletes idempotency stamps from `state.redb` after `retention_days`. On `s3` / `gcs` / `tiered` backends, the sweep is correct but pays a per-key delete during state upload. For projects that emit thousands of stamps per day, configuring a bucket-native lifecycle rule is faster, cheaper, and keeps GC running even when no Rocky process is active.

Both rules below match the default `state.s3_prefix` / `state.gcs_prefix` of `rocky/state/`. Adjust the prefix if you've overridden it. The retention window should match `[state.idempotency] retention_days`, or be larger, so Rocky's own sweep doesn't try to delete an object the bucket has already removed.

**S3, `s3api put-bucket-lifecycle-configuration` payload:**

```json
{
  "Rules": [
    {
      "ID": "rocky-idempotency-stamps-30d",
      "Status": "Enabled",
      "Filter": { "Prefix": "rocky/state/" },
      "Expiration": { "Days": 30 }
    }
  ]
}
```

```bash
aws s3api put-bucket-lifecycle-configuration \
  --bucket "$ROCKY_STATE_BUCKET" \
  --lifecycle-configuration file://rocky-lifecycle.json
```

The same rule works for any object Rocky writes under `rocky/state/`, including state-store snapshots. If you want to retain snapshots longer than stamps, namespace them under separate prefixes via `state.s3_prefix` and configure two rules.

**GCS, `gcloud storage buckets update` lifecycle JSON:**

```json
{
  "lifecycle": {
    "rule": [
      {
        "action": { "type": "Delete" },
        "condition": {
          "age": 30,
          "matchesPrefix": ["rocky/state/"]
        }
      }
    ]
  }
}
```

```bash
gcloud storage buckets update "gs://$ROCKY_STATE_BUCKET" \
  --lifecycle-file=rocky-lifecycle.json
```

**Terraform equivalents:**

```hcl
# S3
resource "aws_s3_bucket_lifecycle_configuration" "rocky_state" {
  bucket = aws_s3_bucket.rocky_state.id

  rule {
    id     = "rocky-idempotency-stamps-30d"
    status = "Enabled"

    filter { prefix = "rocky/state/" }
    expiration { days = 30 }
  }
}

# GCS
resource "google_storage_bucket" "rocky_state" {
  name     = "rocky-state"
  location = "US"

  lifecycle_rule {
    action    { type = "Delete" }
    condition {
      age            = 30
      matches_prefix = ["rocky/state/"]
    }
  }
}
```

**Operational notes:**

- **Bucket lifecycle does not replace `[state.idempotency] retention_days`.** The local redb mirror on each pod still has its own copy of the stamp; Rocky's sweep is what evicts that. Bucket lifecycle handles the durable copy.
- **In-flight claims (`InFlight`) are TTL-bounded by `in_flight_ttl_hours`, not by the lifecycle rule.** Don't set the lifecycle window shorter than `in_flight_ttl_hours` (default 24) or you risk reaping a live claim.
- **Tiered backends already serve hits from Valkey first.** A bucket lifecycle that's slightly behind `retention_days` is harmless; Valkey's own TTL evicts the hot copy long before the cold S3/GCS copy expires.

### State namespacing

Let independent runs proceed at the same time instead of queueing behind one lock. redb permits **one writer per state file**. Run one `rocky run` process per pipeline or client against the single global file (`<models>/.rocky-state.redb`) and those runs serialize on one advisory lock. Namespacing gives each pipeline, or each client, its own state file with its own lock, its own redb handle, and its own remote object key.

Namespacing is **opt-in and default-off**.

| Mode | Behavior |
|---|---|
| `"none"` (default) | One global `<models>/.rocky-state.redb` for the whole project. Identical to today's behavior. |
| `"pipeline"` | One state file per pipeline, under `<models>/.rocky-state/<pipeline>.redb`. |

```toml
[state]
backend = "local"
namespacing = "pipeline"   # each pipeline gets its own state file
```

To fan out by **client/tenant** rather than by pipeline name, use the per-invocation [`--state-namespace <key>`](/reference/commands/core-pipeline/#global-flags) flag instead. Its precedence:

1. An explicit `--state-path <path>` is a hard override that **disables** namespacing for that invocation.
2. Otherwise `--state-namespace <key>` wins over the `[state] namespacing` config.
3. Neither set ⇒ the single global state file (default).

`<key>` must be a SQL identifier (`^[a-zA-Z0-9_]+$`): it becomes a path segment. Namespaced files start fresh; the legacy global file is never moved or auto-seeded, so carry watermarks forward manually if needed (copy the global file to `<models>/.rocky-state/<key>.redb`, or point `--state-path` at it for the first run).

---

## `[run]`

Skip rebuilding a transformation model when neither its logic nor its upstream data appears to have moved since the last successful build. The gate is off unless you turn it on.

:::caution[Best-effort, not a result-equivalence guarantee]
Skipping is a best-effort optimization. It is **not** a promise that a fresh rebuild would produce identical bytes. It rests on two heuristics: a cosmetic-invariant IR hash for the logic, and `MAX(ts)` or rowcount movement for the upstream data. Every field defaults to the safe no-skip choice, and **any** missing, unreadable, or ambiguous input rebuilds. The feature stays off until you set `skip_unchanged = true` or pass `--skip-unchanged`.
:::

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `skip_unchanged` | bool | `false` | Master switch for the gate. `false` ⇒ every selected model always builds, exactly as before. The `--skip-unchanged` CLI flag turns the gate on for a single invocation regardless of this value; `--force-rebuild` overrides both. |
| `skip_rowcount_fallback` | bool | `false` | Allow a rowcount-only (`COUNT(*)`) data-stability signal when an upstream has no tracked timestamp column. Default off: without this, a model whose upstreams are not watermarkable is never skip-eligible. Rowcount equality is weaker than a watermark — it can miss a same-size in-place `UPDATE` (or a matched insert+delete) that mutates values without changing the row count. |
| `lag_tolerance_seconds` | integer | `0` | Treat an upstream `MAX(ts)` that moved by fewer than this many seconds as unchanged — the late-arriving-but-irrelevant micro-update analog of a freshness SLA threshold. Default `0`: any movement at all forces a rebuild. |
| `strict_scheduling` | bool | `false` | Turn physical-read scheduling warnings into a run refusal. Rocky derives ordering edges from the physical `schema.table` names a model reads. It reports what it could not safely resolve: contradicting bare-read pairs, models whose reference extraction failed, colliding targets. `false` reports these as warnings and the run still exits `0`, so a consumer that ignores them can read a stale target and see success. `true` refuses the run instead. It lives on `[run]` rather than `[pipeline.NAME.execution]` because `rocky run --dag` derives ordering across pipelines, where a per-pipeline switch would have no single answer. |

```toml
[run]
skip_unchanged = true
skip_rowcount_fallback = false   # default; only flip on if you accept the weaker signal
lag_tolerance_seconds = 0        # default; any MAX(ts) movement rebuilds
strict_scheduling = false        # default; flip on for fail-closed ordering
```

### What is and is not skip-eligible

Rocky skips a model only when **both** of these hold. Anything else rebuilds.

- **(B) Eligible.** The model uses a plain materialization strategy (**not** `content_addressed` or `time_interval`). Its `[skip] eligible` is not `false`, and Rocky can prove its SQL deterministic. It always rebuilds SQL containing `CURRENT_TIMESTAMP` / `NOW()`, `RANDOM()`, `UUID()`, `CURRENT_USER`, `CURRENT_CATALOG`, `ANY_VALUE`, `ARRAY_AGG`, an unordered `LIMIT`, or any function it cannot resolve. `full_refresh` **is** eligible: a deterministic full refresh whose logic and inputs have not moved is safe to skip.
- **(G) Upstreams provably unchanged.** Rocky must be able to prove every upstream stable, and that needs the model's lineage to be provably complete. Only a single plain `SELECT` over bare tables qualifies. A model that uses a CTE, a subquery in `FROM`, or an `IN (SELECT …)` / `EXISTS` / scalar sub-select is **never skipped**. Nor is one that uses a `PIVOT` / `UNNEST` / nested-join table factor, or a set operation (`UNION` / `INTERSECT` / `EXCEPT`). Rocky cannot prove it saw every upstream of those shapes.

`--force-rebuild` plus `full_refresh` always builds.

### Per-model `[skip]` overrides

Override the automatic decision for one model with a `[skip]` block in its sidecar. See [Model format](/reference/model-format/) for the sidecar structure.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `eligible` | bool | (auto) | `false` ⇒ this model always builds, even when the gate is on and everything else looks unchanged (use for known-volatile models the static scan might miss). `true` ⇒ the model is eligible, subject to the other gate clauses. Unset ⇒ fall back to the automatic eligibility rules. |
| `deterministic` | bool | (auto) | Owner assertion about the SQL. `true` is the only way a model the static non-determinism scan flagged (timestamps, randomness, unresolved UDFs, …) becomes skip-eligible — an explicit, auditable, per-model opt-in. `false` forces the model to be treated as non-deterministic (never auto-skipped). Unset ⇒ trust the static scan. |

```toml
# models/fct_orders.toml
name = "fct_orders"

[skip]
eligible = true        # opt this model in/out of the gate explicitly
deterministic = true   # owner asserts the SQL is pure → re-eligible
```

---

## `[reuse]`

Let a content-addressed model reuse work it already did, with a record an auditor can check. The two knobs are independent of each other. Both apply **only** to the content-addressed (S3/UniForm) write path. On DuckDB and plain warehouse targets they do nothing, and `rocky run` behaves exactly as if the block were absent.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `false` | Byte-level **point-to reuse**. When `true`, a successful run records, per model, an input-match index entry and an offline-verifiable provenance record; on a later run, an eligible model whose recomputed `input_hash` hits the index for a prior **strong** run may point to that run's already-written Parquet — a zero-copy commit that skips the SQL. `false` (the default) writes no input-match spine: no per-model hashing cost, no extra state write, no reuse decision. Live-verified on the content-addressed path; active when you turn it on. |
| `column_level` | bool | `true` | **Column-level skip.** An unpartitioned content-addressed model whose logic, environment, and every provably-consumed upstream column are unchanged since its last successful build is skipped — its SQL does not run and no new commit is written; the prior output stays authoritative. On by default since engine 1.61.0, after live S3/UniForm verification of the skip-on-unchanged / build-on-changed decision. Independent of `enabled`. |

Both decisions fail closed. Any unproven input forces a build: a non-deterministic model, a changed recipe or environment, a consumed-column set Rocky cannot enumerate, a missing or moved column hash. Doubt always resolves to running the SQL.

The kill switch for the default-on knob:

```toml
[reuse]
column_level = false   # restore the always-build behavior
```

The per-invocation `--no-reuse` flag forces every model to build (the escape hatch parallel to `--force-rebuild` for `--skip-unchanged`). The provenance record this path writes is what an auditor reads in [Verify a run](/guides/verify-a-run/).

---

## `[ai]`

Cap what an AI generation can spend. The block applies to `rocky ai`, `rocky ai-explain`, `rocky ai-sync`, and `rocky ai-test`. Rocky rejects an unknown field here rather than ignoring it.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_tokens` | integer | `4096` | Per-request `max_tokens` sent to the Anthropic Messages API **and** the cumulative output-token budget enforced across the compile-verify retry loop. When the running total of `output_tokens` returned by the LLM across attempts exceeds this value, Rocky fail-stops with a `TokenBudgetExceeded` error instead of issuing another retry. The default preserves Rocky's pre-1.x hard-coded behaviour. Increase only when generations legitimately need more headroom (large model surfaces, verbose tests). |

```toml
[ai]
max_tokens = 8192
```

Every `rocky ai*` command reads this block. Rocky never reads the API key from `rocky.toml`. It comes from the `ANTHROPIC_API_KEY` environment variable instead, so the key never lands on disk in a project file.

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

```toml
[cache.schemas]
ttl_seconds = 3600   # 1h TTL for teams with high-DDL churn
replicate = true     # opt in to share cache via the remote state backend
```

A Valkey-backed runtime cache exists in the codebase but `rocky.toml` does not reach it yet. A future `[cache.valkey]` key is reserved for it.

---

## `[cost]`

Tell `rocky optimize` what your warehouse costs, so its materialization recommendations reflect your prices rather than generic ones.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `storage_cost_per_gb_month` | float | `0.023` | Storage cost per GB-month. |
| `compute_cost_per_dbu` | float | `0.40` | Compute cost per DBU. |
| `warehouse_size` | string | `"Medium"` | Warehouse size for cost estimation (e.g., `"Small"`, `"Medium"`, `"Large"`). |
| `min_history_runs` | integer | `5` | Runs Rocky needs in history before it makes a cost recommendation. |

```toml
[cost]
storage_cost_per_gb_month = 0.023
compute_cost_per_dbu = 0.40
warehouse_size = "Medium"
```

---

## `[budget]`

Put a ceiling on what one run may cost, how long it may take, and how much it may scan. When a run crosses a limit, Rocky emits a `budget_breach` pipeline event and fires the `HookEvent::BudgetBreach` hook. Set `on_breach = "error"` and the run also exits non-zero. Rocky rejects an unknown field here rather than ignoring it.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_usd` | float | | Maximum allowed run cost in USD. Cost is computed from per-materialization `cost_usd` values on `RunOutput.cost_summary`. `None` on runs where no adapter produced cost data (e.g. a BigQuery job with no bytes billed). |
| `max_duration_ms` | integer | | Maximum allowed run wall time in milliseconds. |
| `max_bytes_scanned` | integer | | Maximum allowed total bytes scanned across every materialization in the run. Useful for CI gates on scan volume even when the dollar cost stays inside `max_usd` (e.g. a BigQuery query that stops pruning partitions). Aggregated from the per-model `bytes_scanned` figures the adapter reports. Today that's BigQuery's `totalBytesBilled`; Databricks / Snowflake / DuckDB still inherit `None` and skip the dimension rather than treating "no data" as zero. |
| `on_breach` | string | `"warn"` | Either `"warn"` (fire the event, keep the run successful) or `"error"` (also fail the run). |

```toml
[budget]
max_usd = 25.0
max_duration_ms = 900000          # 15 minutes
max_bytes_scanned = 1099511627776 # 1 TiB
on_breach = "error"
```

The three limits are independent, and any one of them breaching trips the event. Rocky evaluates them once per run against the observed totals; per-model budgets are a follow-up. Subscribe to `on_budget_breach` under `[hook.*]` to route a breach into a notification system.

Each [`BudgetBreachOutput`](/reference/json-output/) carries a `limit_type` tag: `"max_usd"`, `"max_duration_ms"`, or `"max_bytes_scanned"`. A consumer can branch on the dimension that broke without matching strings in the human message.

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

Precedence for the effective target dialect:

1. `rocky compile --target-dialect <DIALECT>` flag (wins if set).
2. `[portability] target_dialect`.
3. Unset: no lint.

See [Linters](/concepts/linters/) for the full list of covered constructs and the per-model pragma syntax.

---

## `[retry]`

Share one retry budget across every adapter in a run, instead of giving each its own. Rocky builds a single counter and passes it to all of them; once it runs out, no adapter retries again. This stops one failing endpoint from consuming retries the other adapters would have used. Omit the block and each `[adapter.NAME.retry]` keeps its own independent budget.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_retries_per_run` | integer | | Total retries allowed across every adapter for this run. Omit the block to keep per-adapter budgets (each `[adapter.NAME.retry]` remains in isolation). |

```toml
[retry]
max_retries_per_run = 50
```

---

## `[mask]`

Hide sensitive column values without editing a model. Tag a column with a `[classification]` in its sidecar, then map that tag to a strategy here. Every column carrying the tag gets the same treatment, across every model. See [Governance](/guides/governance/) for the narrative.

| Strategy | Behavior |
|---|---|
| `"hash"` | SHA-256 hex digest of the column value. Deterministic, one-way. |
| `"redact"` | Replace the value with the literal string `'***'`. |
| `"partial"` | Keep the first and last two characters; replace the middle with `***`. Values shorter than 5 chars are fully replaced with `'***'`. |
| `"none"` | Explicit identity; no masking applied. Useful as a per-env override to unmask a column that defaults to masked at the workspace level. |

```toml
# models/customers.toml tags email + ssn with these classifications
[mask]
pii = "hash"
confidential = "redact"
```

An unknown strategy — a typo like `"mask"` — fails at config load. Rocky never accepts a spelling it cannot emit SQL for.

:::note[Adapter support]
Masking works today against **Databricks** Unity Catalog, using column tags plus `CREATE MASK` / `SET MASKING POLICY`. Rocky emits one statement per column, because Unity Catalog rejects multi-column masking DDL. Snowflake, BigQuery, and DuckDB do not support it until there is demand. Rocky applies masks after a successful DAG run, best-effort: a failure logs a warning and does not abort the pipeline, the same as grants.
:::

### `[mask.<env>]`

Mask a column differently per environment. Rocky starts from the `[mask]` defaults and layers the block matching the active environment on top.

```toml
[mask]
pii = "hash"
confidential = "redact"

[mask.prod]
pii = "none"            # unmask pii in prod (e.g. service principal reads)
confidential = "partial"

[mask.staging]
pii = "partial"         # staging gets a softer mask than the dev default
```

Resolution precedence:

1. `[mask.<env>]` entry for the active env (when supplied to `rocky plan --env <env>`).
2. `[mask]` workspace default.
3. Unmatched tag: W004 warning unless the tag is listed in [`[classifications] allow_unmasked`](#classifications).

---

## `[classifications]`

Silence the `W004` warning for a classification tag you never intend to mask. This is a project-wide setting, distinct from the per-model `[classification]` sidecar block that applies the tags (see [Model format](/reference/model-format/#classification)).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `allow_unmasked` | list of strings | `[]` | Classification tags allowed to appear in a model sidecar without a matching `[mask]` strategy. Suppresses the W004 compiler warning. |

```toml
[classifications]
allow_unmasked = ["internal", "lineage_only"]
```

Use it for tags that exist only for discovery or lineage tracking. [`rocky compliance`](/reference/cli/#rocky-compliance) still lists those columns and still suppresses their exceptions, so the report never pretends they are enforced.

---

## `[role.<name>]`

Describe your access model as roles that inherit from each other, rather than repeating the same permission list everywhere. Each `[role.<name>]` block declares one role. Rocky flattens the inheritance graph before it reconciles against the warehouse's own role or group system. It rejects a cycle or an unknown parent at config load.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `inherits` | list of strings | `[]` | Immediate parent role names. Rocky unions permissions transitively across every ancestor. Cycles and unknown parents are rejected at config-load time. |
| `permissions` | list of strings | `[]` | Permissions this role grants. Canonical uppercase spellings (e.g. `"SELECT"`, `"USE CATALOG"`, `"USE SCHEMA"`, `"MODIFY"`, `"MANAGE"`). Empty lists are legal; pure grouping roles exist only to aggregate children. |

```toml
[role.reader]
permissions = ["SELECT", "USE CATALOG", "USE SCHEMA"]

[role.analyst]
inherits = ["reader"]
permissions = ["MODIFY"]

[role.admin]
inherits = ["analyst"]
permissions = ["MANAGE"]
```

Rocky flattens the graph into `admin → {SELECT, USE CATALOG, USE SCHEMA, MODIFY, MANAGE}` and forwards the resolved set to `GovernanceAdapter::reconcile_role_graph` after a successful DAG.

:::caution[v1 is log-only]
The v1 Databricks implementation validates each `rocky_role_<name>` principal against the identifier grammar and emits a `debug!` trace. SCIM group creation and per-catalog GRANT emission are deferred as a follow-up. The resolver still catches cycles and unknown parents at config-load regardless of adapter capability, so invalid graphs fail fast even before reconcile runs.
:::

---

## `[freshness]`

Set a project-wide staleness budget, so you do not repeat the same threshold in every model sidecar. Freshness is how far behind the newest row in a table is. Rocky acts on one key here: `expected_lag_seconds`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `expected_lag_seconds` | integer | (unset) | Maximum lag before a model counts as stale. This is the field that makes the block active: without it, Rocky treats the project as having no freshness default. |
| `time_column` | string | (unset) | Timestamp column to measure lag from. Rocky accepts the key. Nothing reads it today. See the note below. |
| `severity` | string | (unset) | `"error"` or `"warning"`. Rocky accepts the key. Nothing reads it today. See the note below. |

```toml
[freshness]
expected_lag_seconds = 3600   # every model should be under an hour behind
```

:::caution[time_column and severity are inert]
Rocky parses both keys, so a config that sets them still loads. Nothing reads either value today. This block also does not merge field by field into a per-model `[freshness]` block. The engine carries a helper that would copy `time_column` and `severity` into a model. Nothing calls it. Only `expected_lag_seconds` changes what Rocky does.
:::

The compiler raises `W005` on a model that has at least one temporal output column (`DATE`, `TIMESTAMP`, `TIMESTAMP_NTZ`) and no `freshness` declaration in scope. A model with no temporal column never raises it. Setting `expected_lag_seconds` here puts a declaration in scope for every model at once, which silences the warning project-wide; a per-model `[freshness]` block silences it for that model.

The scheduler uses it as the staleness budget for a pipeline that sets [`[pipeline.NAME.schedule]`](#pipelinenameschedule) `freshness = true`. For a transformation pipeline, a member model's own `max_lag_seconds` wins: the budget is the smallest one the members declare. When no member declares one, this project value applies. Every other pipeline type loads no member models, so this value is its only budget.

This block is separate from the [`[pipeline.NAME.checks]`](#pipelinenamechecks) freshness check, which runs as part of the data-quality pipeline and has its own `threshold_seconds`.

---

## `[schema_evolution]`

This block holds the grace period for a column that disappears from the source. It takes one key. Rocky does not act on that key yet, so read the note below before you rely on it.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `grace_period_days` | integer | `7` | Days a target table would keep a dropped column before Rocky removes it. Rocky accepts the key. Nothing reads the value today. |

```toml
[schema_evolution]
grace_period_days = 30
```

:::caution[The grace period does not run yet]
Rocky accepts the key, so a config that sets it still loads. No run starts a grace period, warns while one is open, or drops a column when one expires.

Drift detection reports two kinds of change: a column the source has and the target lacks, and a column whose type differs between the two. It never reports a column that disappeared from the source, so `grace_period_days` never takes effect. The engine does carry the remaining parts: a grace-period detector, an `ALTER TABLE ... DROP COLUMN` generator, and a state-store record. Only tests call them.
:::

See [Schema drift](/concepts/schema-drift/) for the changes Rocky does detect and act on.

---

## `[resilience]`

Decide whether a model that fails for a transient reason gets another attempt. This is the run loop's retry policy, and it re-runs a whole **model**. It is a different layer from [`[adapter.NAME.retry]`](#adapternameretry), which retries one statement inside a connector. It also differs from [`[retry]`](#retry), which caps connector retries across the run.

Unlike the skip and reuse gates, this block is **on by default**. What keeps that safe is the small budget: at most two retries per model, and at most eight across the whole run.

Rocky classifies each failure before deciding. Only a failure it can prove is transient is retried. Permanent and unknown failures are never retried, whatever these settings say.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `true` | Master switch. `false` attempts every model exactly once, with no classification and no backoff. |
| `transient_max_retries` | integer | `2` | Re-runs allowed for a model that failed transiently, so three attempts in total. `0` disables retry but keeps the classifier running for observability. |
| `initial_backoff_ms` | integer | `500` | Wait before the first retry. |
| `max_backoff_ms` | integer | `30000` | Cap on exponential backoff growth. |
| `backoff_multiplier` | float | `2.0` | Multiplier applied to the backoff after each retry. |
| `jitter` | bool | `true` | Add ±25% jitter so concurrent runs do not retry in lockstep. |
| `circuit_breaker_threshold` | integer | `3` | Stop retrying for the rest of the run after this many consecutive transient model failures. Each model still gets its one attempt. `0` disables the breaker. |
| `max_retries_per_run` | integer \| null | `8` | Ceiling on total retries across every model in one run. `null` removes the ceiling; `0` forbids all retries. |
| `contain_failures` | bool | `false` | `false` stops the run at the first failing model. `true` withholds the failed model and everything downstream of it, and lets unrelated subtrees finish. It reports `PartialFailure` with a manifest naming what failed and its blast radius. |
| `auto_apply_additive_drift` | bool | `false` | `false` evolves the target for a new nullable upstream column with no policy gate, as Rocky has always done. `true` routes the mutation through the [`[policy]`](#policy) plane first. Rocky applies only a provably additive change with an `allow` verdict on the `schema_change.additive` capability, and leaves anything else for review. Changing behaviour needs both this switch and a matching policy rule. |

```toml
[resilience]
transient_max_retries = 2    # default
contain_failures = true      # let unrelated models finish when one fails
```

To restore single-attempt behaviour, for example in CI where a fast failure is what you want:

```toml
[resilience]
enabled = false
```

---

## `[imports.<name>]`

Check your models against another team's published schema at compile time. A producer project publishes a snapshot of its compiled [IR](/reference/glossary/#ir-intermediate-representation) with `rocky publish-ir`. You vendor that file into your repository and point an `[imports.<name>]` block at it. Your `rocky compile` then fails when the producer drops a column you still read.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `path` | string | Yes | Directory holding the vendored snapshot files, relative to `rocky.toml`. |
| `snapshot` | string | Yes | Filename of the producer's current snapshot, relative to `path`. |
| `baseline` | string | No | Filename of the reviewed "before" snapshot, relative to `path`. Rocky diffs `baseline` against `snapshot` to find columns the producer changed. Without it, the column-level diagnostics have nothing to compare. |
| `pin` | string | No | Recipe-hash pin. A concrete hash fails compilation (`E033`) when the vendored snapshot differs at all. `"*"`, or leaving it unset, trusts whatever is vendored. |

```toml
[imports.orders]
path     = "vendor/orders"
snapshot = "current.json"
baseline = "baseline.json"
pin      = "*"
```

`pin` and `baseline` answer different questions, so use both. `pin` is a whole-project tripwire that fires on any drift. `baseline` is the column-level "before" image that lets Rocky report exactly which of your reads broke.

Nothing advances the baseline on its own. Run `rocky imports update` once you have reviewed a producer change and decided to take it. Run `rocky imports update --check` in CI to fail when a baseline is behind.

See [Cross-team contracts](/concepts/cross-team-contracts/) for the full diagnostic table (`E030`–`E034`, `W030`, `W031`).

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

```toml
[branch.approval]
required = true
min_approvers = 2
allowed_signers = ["lead@example.com", "governance@example.com"]
max_age_seconds = 43200   # 12 hours
```

---

## `[gc]`

Control what `rocky gc` is allowed to delete. Eviction is recorded, not destructive: an approved `rocky apply <gc-plan>` writes a durable tombstone and retires the artifact's ledger row, and the bytes stay where they are.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `physical_delete` | bool | `false` | Reserved for a future protocol-aware `VACUUM`. `false` tombstones and retires the ledger row only. `true` is a **hard error** at apply time today, so the flag fails loudly rather than deleting bytes or silently doing nothing. |

```toml
[gc]
physical_delete = false   # the only supported value today
```

`rocky restore` can rebuild an evicted artifact only when the recorded recipe is non-partitioned, content-addressed, and reads no recorded upstreams. Even then it can refuse — a missing tombstone, a re-derivation whose hash differs, or a lost race on ledger reinstatement all stop it. A recipe with any recorded upstream cannot be restored yet. What is durably retained is the eviction record and the provenance it points at, not a promise that the exact bytes come back. See the [governance and reclamation commands](/reference/commands/governance-reclamation/).

---

## `[policy]`

State who may change what, and let Rocky enforce it. Each rule maps a `(principal, capability, scope)` triple to one of three effects. Rocky evaluates the rules at the mutating [seams](/reference/glossary/#seam) — `rocky apply`, branch promote, and the MCP write tools — and records every decision in the audit ledger.

:::caution[An absent block does not gate agents]
With no `[policy]` block, the gate returns `NotConfigured` and allows the action whoever the principal is. One surface differs. `rocky policy check` *predicts* against the safe default posture: agents on mutating actions require review, and humans are never gated. With no block, its prediction is stricter than what enforcement actually does.
:::

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `version` | integer | Yes | Schema version. Must be `1`. |
| `default_agent_effect` | string | No | Effect for an `agent` on a mutating capability when no rule matches. Defaults to `"require_review"`. |
| `rules` | list of tables | No | The rules themselves, written as `[[policy.rules]]`. |
| `tests` | list of tables | No | Scenario assertions run by `rocky policy test`, written as `[[policy.tests]]`. Never read by enforcement. |

**Effects.** `"allow"` permits the action. `"require_review"` permits it only after a human signs off. `"deny"` refuses it, and no `allow` overturns a `deny`.

**Principals.** `"human"` is a person. `"agent"` is a non-human caller. In this version you supply the principal explicitly, for example `rocky policy check --principal agent`.

**Capabilities.** `read` (always allowed), `propose`, `apply`, `promote`, `backfill`, `gc`, `restore`, `retry`, `quarantine`, plus three refinements of apply and promote: `schema_change.additive`, `schema_change.breaking`, and `value_change`. A rule naming `apply` or `promote` also matches those three refinements. Every other capability matches only itself, and a rule naming a refinement matches only that refinement.

### `[[policy.rules]]`

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `principal` | string | Yes | `"human"` or `"agent"`. |
| `capability` | string | Yes | One of the capabilities above. |
| `effect` | string | Yes | `"allow"`, `"require_review"`, or `"deny"`. |
| `scope` | table | No | Which models the rule covers. See below. |
| `verify_after` | list of strings | No | Named checks that must pass after a mutation this rule governed lands. A failing **or absent** check halts the apply and raises an alert. Empty means no post-apply gate. |
| `autonomy_budget` | table | No | `{ failures = <n>, window = "<N>d" \| "<N>h" }`. When verify-after failures inside the rolling window reach `failures`, the rule degrades to `require_review`. It only ever tightens, never widens, and it recovers on its own as old failures age out. |
| `conditions` | table | No | Parsed and ignored. Reserved so a config written against a later version still loads. |

### `[policy.rules.scope]`

A model matches the scope only when it satisfies **every** key you set.

| Field | Type | Description |
|-------|------|-------------|
| `any` | bool | Match every model. Mutually exclusive with every other key. |
| `models` | list of strings | Glob patterns (`*`, `?`) over the model name. Matches when the name matches at least one. |
| `tags` | table | Required `key = value` tags. Matches when the model carries all of them. |
| `classifications` | list of strings | Matches when the model has at least one column with any listed classification. |
| `exclude_classifications` | list of strings | Matches when the model has **no** column with any listed classification. |
| `contracted` | bool | Matches when the model's contracted status equals this value. |
| `layer` | string | Matches when the model's `layer` tag equals this value. |
| `max_downstreams` | integer | Blast-radius ceiling. Not a match predicate: the rule still matches, but an `allow` degrades to `require_review` when the transitive downstream count exceeds the ceiling **or cannot be computed**. `deny` and `require_review` rules are unaffected. |

A scope with neither `any = true` nor any real predicate is rejected at config load, so an empty scope can never silently match everything.

```toml
[policy]
version = 1
default_agent_effect = "require_review"

# Agents may add columns to staging models on their own.
[[policy.rules]]
principal = "agent"
capability = "schema_change.additive"
effect = "allow"
scope = { models = ["stg_*"], max_downstreams = 20 }

# Nobody drops a column from a model that carries PII without review.
[[policy.rules]]
principal = "agent"
capability = "schema_change.breaking"
effect = "deny"
scope = { classifications = ["pii"] }
```

Pin the behaviour you just wrote with `[[policy.tests]]` scenarios, and run `rocky policy test` in CI. A later policy edit then cannot open a hole unnoticed. See [Testing policies](/guides/testing-policies/) for the scenario fields, and [Operating Rocky with agents](/concepts/operating-rocky-with-agents/) for where the gates sit.

---

## Full Example

A complete Fivetran → Databricks pipeline with governance:

```toml
# ──────────────────────────────────────────────────
# Adapters: connections to source and warehouse
# ──────────────────────────────────────────────────
[adapter.fivetran]
type = "fivetran"
destination_id = "${FIVETRAN_DESTINATION_ID}"
api_key = "${FIVETRAN_API_KEY}"
api_secret = "${FIVETRAN_API_SECRET}"

[adapter.prod]
type = "databricks"
host = "${DATABRICKS_HOST}"
http_path = "${DATABRICKS_HTTP_PATH}"
token = "${DATABRICKS_TOKEN}"

# ──────────────────────────────────────────────────
# Pipeline: bronze layer replication
# ──────────────────────────────────────────────────
[pipeline.bronze]
type = "replication"
strategy = "incremental"
timestamp_column = "_fivetran_synced"
metadata_columns = [
    { name = "_loaded_by", type = "STRING", value = "NULL" },
]

[pipeline.bronze.source]
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
environment = "production"

[[pipeline.bronze.target.governance.grants]]
principal = "group:data_engineers"
permissions = ["USE CATALOG", "MANAGE"]

[[pipeline.bronze.target.governance.schema_grants]]
principal = "group:data_engineers"
permissions = ["USE SCHEMA", "SELECT", "MODIFY"]

[pipeline.bronze.target.governance.isolation]
enabled = true

[[pipeline.bronze.target.governance.isolation.workspace_ids]]
id = 123456789
binding_type = "READ_WRITE"

[pipeline.bronze.checks]
enabled = true
row_count = true
column_match = true
freshness = { threshold_seconds = 86400 }

[pipeline.bronze.execution]
concurrency = 8
fail_fast = false
table_retries = 1

# ──────────────────────────────────────────────────
# State: persistence backend for watermarks
# ──────────────────────────────────────────────────
[state]
backend = "${ROCKY_STATE_BACKEND:-local}"
# s3_bucket = "${ROCKY_STATE_BUCKET}"
# valkey_url = "${VALKEY_URL}"
```

A credential-free DuckDB pipeline (good for examples and tests). Using config inference, many defaults can be omitted:

```toml
[adapter.local]
type = "duckdb"
path = "warehouse.duckdb"

[pipeline.demo]
strategy = "full_refresh"

[pipeline.demo.source]
adapter = "local"

[pipeline.demo.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.demo.target]
adapter = "local"
catalog_template = "warehouse"
schema_template = "analytics"

[pipeline.demo.checks]
row_count = true

[pipeline.demo.execution]
concurrency = 4
```

Note: `pipeline.type = "replication"` (default), `auto_create_catalogs = false` (default), `auto_create_schemas = false` (default), and `[state]\nbackend = "local"` (default) are all omitted.

With a single adapter, this can be even more minimal using the unnamed adapter shorthand:

```toml
[adapter]
type = "duckdb"
path = "warehouse.duckdb"

[pipeline.demo]
strategy = "full_refresh"

[pipeline.demo.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.demo.target]
catalog_template = "warehouse"
schema_template = "analytics"
```

---

## Hooks

Run a shell command or post to an HTTP endpoint when something happens in a pipeline. A shell hook receives the event's JSON context on stdin. A webhook posts a templated body to a URL.

```toml
# Shell hooks — run a command, pipe JSON context to stdin.
# Event keys are always the `on_<event>` form; a key without the
# `on_` prefix is treated as unknown and silently ignored.
[[hook.on_pipeline_start]]
command = "bash scripts/notify.sh"
timeout_ms = 5000
on_failure = "warn"    # abort | warn | ignore

[[hook.on_materialize_error]]
command = "bash scripts/pagerduty.sh"
on_failure = "ignore"

# Webhooks — HTTP POST with template body
[hook.webhooks.on_pipeline_complete]
url = "https://hooks.slack.com/services/T.../B.../xxx"
preset = "slack"
secret = "${WEBHOOK_SECRET}"

[hook.webhooks.on_materialize_error]
url = "https://events.pagerduty.com/v2/enqueue"
preset = "pagerduty"
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
