---
title: Model Format
description: SQL and TOML model file specification
sidebar:
  order: 4
---

Rocky models define SQL transformations and their materialization behavior. Each model is a SQL query paired with configuration that tells Rocky how to materialize it, what it depends on, and where to write the output.

Rocky supports two model formats: **sidecar** (recommended) and **inline** (legacy).

## Sidecar Format (Recommended)

The sidecar format keeps SQL and configuration in separate files with matching names:

```
models/
├── fct_orders.sql          <- pure SQL
├── fct_orders.toml         <- configuration
├── stg_customers.sql
├── stg_customers.toml
├── dim_products.sql
└── dim_products.toml
```

This separation keeps SQL files clean and editable by any SQL tool without needing to understand Rocky-specific syntax.

### SQL File

The `.sql` file contains a plain SQL query. No templating, no Jinja, no special markers.

```sql
-- models/fct_orders.sql
SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.total_amount,
    c.customer_name,
    c.segment
FROM analytics.staging.orders AS o
JOIN analytics.staging.customers AS c
    ON o.customer_id = c.customer_id
WHERE o.order_date >= '2024-01-01'
```

### TOML Config File

The `.toml` file specifies the model name, dependencies, materialization strategy, and target table.

**Fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Model identifier. Must be unique across all models. |
| `depends_on` | list of strings | No | Names of upstream models that must run before this one. Defaults to `[]`. |
| `group` | string | No | Name of a [config group](#config-groups) (`models/groups/<name>.toml`) this model opts into for shared routing and materialization. |
| `retention` | string | No | Data retention policy for this model. Grammar `^\d+[dy]$` — e.g. `"90d"` or `"1y"`. See [Retention](#retention). |

**`[args]`** -- Placeholder values for a config group's `schema_template` (only meaningful when the model declares a `group`):

| Key pattern | Value type | Description |
|---|---|---|
| `<placeholder>` | string | Fills a `{placeholder}` in the group's `schema_template` (e.g. `region = "emea"` resolves `mart_{region}` to `mart_emea`). Ignored when the model declares no `group`. See [Config groups](#config-groups). |

**`[strategy]`** -- Materialization configuration:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | string | `"full_refresh"` | Materialization type. One of `"full_refresh"`, `"incremental"`, `"merge"`, `"time_interval"`, `"ephemeral"`, `"delete_insert"`, `"microbatch"`, `"content_addressed"`. |
| `timestamp_column` | string | | Column used as the incremental watermark. Required when `type = "incremental"` or `type = "microbatch"`. |
| `unique_key` | list of strings | | Key columns for merge matching. Required when `type = "merge"`. |
| `update_columns` | list of strings | | Columns to update on merge match. Defaults to all non-key columns if omitted. |
| `partition_by` | list of strings | | Column(s) identifying the partition to delete. Required when `type = "delete_insert"`. |
| `time_column` | string | | Partition column for time-interval processing. Required when `type = "time_interval"`. |
| `granularity` | string | `"hour"` (microbatch) | Partition granularity: `"hour"`, `"day"`, `"month"`, or `"year"`. Required when `type = "time_interval"`; optional default for `"microbatch"`. |
| `lookback` | integer | `0` | Number of past partitions to reprocess. Optional for `"time_interval"`. |
| `batch_size` | integer | `1` | Max partitions per batch. Optional for `"time_interval"`. |
| `first_partition` | string | | Earliest partition key (e.g., `"2024-01-01"`). Optional for `"time_interval"`. |
| `storage_prefix` | string | | Object-store key prefix that holds `_delta_log/` + Parquet files for the target table (e.g. `"s3://bucket/path/table"`). Required when `type = "content_addressed"`. |
| `partition_columns` | list of strings | `[]` | Logical partition columns for content-addressed tables. Empty for unpartitioned tables. Optional for `"content_addressed"`. |

:::note[Lakehouse formats]
Warehouse-managed table shapes (**Delta tables**, **Iceberg tables**, **materialized views**, **streaming tables**, **plain views**) are modeled as a separate `format` axis (a top-level `format = "delta_table"` / `"iceberg_table"` key plus an optional `[format_options]` block for partitioning, clustering, table properties, and a comment). `[strategy]` controls how Rocky writes data into the table; `format` controls the physical table shape. The two are orthogonal. The engine-side DDL generator (`rocky-core::lakehouse::generate_lakehouse_ddl`) handles each format; end-to-end TOML wiring varies by adapter, so consult the per-adapter guides before committing to one.

The chosen `format` and `format_options` are now applied on the **first** materialization of incremental-family models (`incremental`, `delete_insert`, `microbatch`, `time_interval`), not just on full-create strategies — so the table that bootstraps an incremental model is created as the requested Delta or Iceberg shape from the start, rather than as a plain table that only later gains the format.
:::

**`[target]`** -- Output table:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `catalog` | string | Yes | Target catalog name. |
| `schema` | string | Yes | Target schema name. |
| `table` | string | Yes | Target table name. |

**`[[sources]]`** -- Input tables (optional, for documentation and lineage):

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `catalog` | string | Yes | Source catalog name. |
| `schema` | string | Yes | Source schema name. |
| `table` | string | Yes | Source table name. |

### `[skip]`

Per-model overrides for the opt-in `--skip-unchanged` model-skip gate. The gate is conservative by default: a model is auto-skip-eligible only when a static scan finds its SQL deterministic and it uses a plain materialization strategy. This block lets an owner override that decision per model. Omit it entirely to follow the automatic rules.

| Field | Type | Default | Description |
|---|---|---|---|
| `eligible` | bool \| null | `null` | Explicit eligibility override. `false` ⇒ this model **always builds**, even when the gate is on and everything looks unchanged (use for a known-volatile model the static scan might miss). `true` ⇒ the model is eligible, subject to the other gate clauses. `null` ⇒ fall back to the automatic rules. |
| `deterministic` | bool \| null | `null` | Owner assertion about the SQL's purity. `true` is the only way a model the static non-determinism scan flagged (timestamps, randomness, unresolved UDFs, order-unstable aggregates) becomes skip-eligible — an explicit, auditable opt-in. `false` forces the model to be treated as non-deterministic (never auto-skipped). `null` ⇒ trust the static scan. |

```toml
name = "fct_orders"

[skip]
eligible = false        # opt this model out — always rebuild
```

```toml
name = "dim_dates"

[skip]
deterministic = true    # owner asserts the SQL is pure → re-eligible despite the scan
```

**Fail-safe rules.** The gate exists to avoid silent production staleness, so it builds on any doubt. Beyond `[skip]`, a model is **never** auto-skip-eligible (it always rebuilds) when:

- its SQL is **non-deterministic**: it calls a volatile builtin (`CURRENT_TIMESTAMP`, `NOW`, `RANDOM`, `UUID`, `CURRENT_USER`, `CURRENT_CATALOG`, …), an order/tie-break-unstable aggregate (`ANY_VALUE`, `ARRAY_AGG`, `COLLECT_LIST`, `COLLECT_SET`, `MODE`), an unordered `LIMIT`/`TOP`/`FETCH`, or any function not on Rocky's pure-function allowlist;
- its **lineage isn't provably complete**: anything beyond a single plain `SELECT` over bare tables (CTEs, sub-queries in `FROM`, `PIVOT`/`UNNEST`/nested joins, `IN (SELECT …)`/`EXISTS`/scalar sub-selects, or set operations) forces a rebuild;
- it uses a `content_addressed` or `time_interval` strategy (a `full_refresh` model **is** eligible).

`deterministic = true` overrides only the first bullet. Even an eligible model is skipped only when its logic and every upstream's data are both unchanged. See [Skip Unchanged Models and Defer to Prod](/guides/skip-and-defer/) for the full workflow and the `[run]` tuning knobs.

### Environment variables

Sidecar `.toml` files (and `models/_defaults.toml`) go through the same `${VAR}` / `${VAR:-default}` substitution as `rocky.toml`, so an orchestrator can inject per-model `[target]` values via subprocess env without templating the sidecar. See [Environment Variables](/reference/configuration/#environment-variables) for the syntax and a sidecar example, and [`examples/playground/pocs/00-foundations/07-config-layering/`](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/00-foundations/07-config-layering) for a runnable three-layer example.

### `@var()` run variables

A model body can carry per-run placeholders of the form `@var(name)` or `@var(name, default)`. They are bound at run time by `rocky run --var name=value` (repeatable) and substituted into the SQL before it reaches the warehouse:

```sql
-- models/orders.sql
SELECT *
FROM raw.orders
WHERE region = '@var(region)'
  AND status = '@var(status, shipped)'
```

```bash
rocky run --var region=emea --var status=delivered
```

Here `@var(region)` has no default and must be supplied; `@var(status, shipped)` falls back to `shipped` when `--var status=...` is omitted.

The substitution is **textual** — Rocky replaces the marker with the supplied string verbatim, so you own the surrounding quoting and casting (the example quotes the marker because the value is a string literal). Only the variable *name* is validated, as a SQL identifier.

This is deliberately distinct from config-time `${ENV}` substitution: `${ENV}` resolves config values while Rocky parses `rocky.toml` and the sidecars, before any model is seen; `@var()` resolves a run's logical inputs at compile/render time and stays visible in the model source. A `@var(name)` with no `--var` binding and no inline default is a **compile error** that names the missing variable, so a forgotten value fails fast. `rocky import-dbt` maps dbt's `{{ var('name') }}` / `{{ var('name', default) }}` onto these markers.

### Config groups

When many models share the same routing and materialization, define a **config group** once and have each model opt in by name. A group lives in `models/groups/<name>.toml` (the file stem is the group name) and supplies a `schema_template` and a `strategy`:

```toml
# models/groups/daily_marts.toml
schema_template = "mart_{region}"

[strategy]
type = "merge"
unique_key = ["id"]
update_columns = ["amount", "status"]
```

A model joins the group with `group = "<name>"` and fills the template's placeholders from its own `[args]`:

```toml
# models/fct_orders.toml
group = "daily_marts"

[target]
catalog = "warehouse"   # schema comes from the group template

[args]
region = "emea"         # fills {region} -> schema "mart_emea"
```

Precedence is **per-model sidecar > group > `_defaults.toml`**: a model can still pin its own `schema` or `strategy` to override the group, and the group in turn overrides directory defaults. A `group` that names no definition, or a `schema_template` placeholder the model doesn't supply, fails the load with a clear error rather than routing a model to the wrong place.

A model that pins its own `schema` overrides the group's template entirely — so it must **not** also supply `[args]`, since the args could only fill a template that's now bypassed. That combination is a misplacement (the args silently do nothing, usually masking a routing mistake) and fails the load. Pin a schema *or* supply args, not both.

#### Enforced groups

Set `enforce = true` on a group to make its fields binding rather than defaults. A member model that locally pins a field the group controls — its target `schema` or its `strategy` — then fails the load instead of silently routing or materializing itself differently from the rest of the group:

```toml
# models/groups/regulated.toml
enforce = true
schema_template = "mart_{region}"

[strategy]
type = "merge"
unique_key = ["id"]
```

Enforcement is strictly opt-in: without `enforce`, groups stay overridable defaults. A model under an enforced group still supplies its own `[args]` (and any field the group doesn't set, like `target.catalog`); it just can't override what the group owns. Use this when a set of models must share routing and materialization as a governance guarantee.

The model loader does not recurse into subdirectories, so `models/groups/` is never mistaken for model files.

#### Group tags

A group can also declare a `[tags]` block. Every member model inherits the group's tags as a shared baseline, so a governance attribute applied once on the group lands on the whole fan-out:

```toml
# models/groups/finance.toml
schema_template = "mart_{region}"

[tags]
domain = "finance"
tier = "gold"
```

A member model's own `[tags]` override the group per key (sidecar > group) without dropping the rest of the group's tags — so one model can set `tier = "silver"` and still inherit `domain = "finance"`. See [`[tags]`](#tags) for how resolved tags surface on `models_detail[].tags` and project onto Dagster assets.

A group carries `schema_template`, `strategy`, `tags`, `governance`, and `enforce`. An unrecognized key in a group file is rejected at load so typos surface immediately.

### `[classification]`

Per-column classification tags. Keys are column names, values are free-form classification strings. Rocky resolves each value against `[mask]` / `[mask.<env>]` in `rocky.toml` to pick the masking strategy, then applies both the column tag and the mask via the governance adapter after a successful DAG.

| Key pattern | Value type | Description |
|---|---|---|
| `<column_name>` | string | Free-form classification tag (e.g. `"pii"`, `"confidential"`, `"internal"`). Matched case-insensitively against `[mask]` keys in `rocky.toml`. Tags without a matching strategy emit the W004 compiler warning unless listed in [`[classifications] allow_unmasked`](/reference/configuration/#classifications). |

```toml
# models/customers.toml
name = "customers"

[classification]
email = "pii"
phone = "pii"
ssn = "confidential"
```

Tags are free-form strings (no enum), so teams can coin new classifications without touching the engine. See [Governance](/guides/governance/) for the end-to-end story (classify → mask → audit → compliance rollup) and [`[mask]`](/reference/configuration/#mask) for the resolver semantics.

:::note[Adapter support]
Classification tags + masking policies are applied today against **Databricks** Unity Catalog (column tags + `CREATE MASK` / `SET MASKING POLICY`, one statement per column). Snowflake, BigQuery, and DuckDB default-unsupported until demand. Best-effort: failures emit `warn!` and don't abort the run.
:::

### `[tags]`

Model-level governance tags. Unlike `[classification]` (keyed by column, drives masking), these describe the model **as a whole** — `domain`, `tier`, `owner`, anything your governance model needs:

```toml
# models/fct_orders.toml
name = "fct_orders"

[tags]
domain = "finance"
tier = "gold"
owner = "data-eng"
```

| Key pattern | Value type | Description |
|---|---|---|
| `<tag_name>` | string | Free-form governance attribute. Merged over any [config-group `[tags]`](#group-tags) baseline (sidecar > group). |

Resolved tags are emitted on `rocky compile --output json` as `models_detail[].tags`. The `dagster-rocky` integration projects them onto the derived asset's Dagster tags, so the same attribute drives both Rocky's view of the model and the orchestrator's. Tags are inherited from a model's config group when it belongs to one — see [Group tags](#group-tags).

`[tags]` never touches the warehouse. For tags that should land on the warehouse securable itself, use [`[governance.tags]`](#governancetags).

### `[governance.tags]`

Where `[tags]` is orchestrator-facing metadata, the `[governance.tags]` block writes Unity Catalog tags onto the model's **own target securable** after it materializes. The DDL is view-aware: `ALTER VIEW ... SET TAGS (...)` for view-format models, `ALTER TABLE ... SET TAGS (...)` otherwise.

```toml
# models/fct_orders.toml
name = "fct_orders"

[governance.tags]
domain = "finance"
tier = "gold"
```

| Key pattern | Value type | Description |
|---|---|---|
| `<tag_name>` | string | Unity Catalog tag applied to this model's target table or view. Keys and values are used verbatim — no prefix. |

This is the per-model counterpart to the pipeline-level [tagging strategy](/guides/governance/#9-tagging-strategy) (`[pipeline.*.target.governance.tags]`), which tags catalogs and schemas during replication. Application is best-effort: a failure warns but never aborts the run, matching the classification and retention governance posture. An empty block is skipped (Unity Catalog rejects `SET TAGS ()`). Distinct from `[tags]`, which is projected onto Dagster asset metadata and never written to the warehouse.

### `[[surrogate_key]]`

Declares a computed surrogate-key column. Rocky injects a deterministic hash of the listed input columns into the materialized SELECT, so you don't hand-write the hash expression in your SQL.

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | Yes | Output column name for the injected key. Must be a valid SQL identifier (`^[a-zA-Z0-9_]+$`). |
| `columns` | list of strings | Yes | Input columns to hash. At least one, each a valid SQL identifier. |

```toml
# models/dim_customers.toml
name = "dim_customers"

[[surrogate_key]]
name = "customer_sk"
columns = ["tenant_id", "customer_id"]

[target]
catalog = "warehouse"
schema = "marts"
table = "dim_customers"
```

At `rocky run` (and on the emit-SQL path), Rocky appends `CAST(md5(...) AS <string_type>) AS <name>` to the model's projection, computed over the input columns. The hash expression is dialect-correct: it uses the warehouse's variable-length string type (`STRING` on Databricks and BigQuery, `VARCHAR` on Snowflake, DuckDB, and Trino) and BigQuery's `to_hex(...)` / `concat(...)` form where the default `||` concatenation doesn't apply. On a given warehouse the hash value matches what `dbt_utils.generate_surrogate_key` produces over the same columns, so keys join across Rocky and dbt models either way. NULL inputs coalesce to a fixed sentinel before hashing, matching dbt-utils.

A `[[surrogate_key]]` block uses `deny_unknown_fields`: a typo such as `colums = [...]` fails the load rather than silently hashing nothing. An empty `columns` list or a `name` / column that isn't a valid identifier is rejected at load with a clear diagnostic. Declare multiple blocks to inject more than one key column.

### `[[tests]]`

Inline declarative data-quality assertions. Each `[[tests]]` block is one assertion that runs against the model's target table. Tests are declarative TOML, not SQL macros. Rocky generates the assertion SQL for the active dialect.

| Field | Type | Required | Description |
|---|---|---|---|
| `type` | string | Yes | Assertion kind. Common types: `not_null`, `unique`, `accepted_values`, `relationships`, `expression`, `row_count_range`. (More are available, including `in_range`, `regex_match`, `aggregate`, and composite-key uniqueness.) |
| `column` | string | Sometimes | Column under test. Required for `not_null`, `unique`, `accepted_values`, `relationships`. Ignored for `expression` and `row_count_range`. |
| `severity` | string | No | `"error"` (default) fails the run; `"warning"` records the failure and continues. |
| `filter` | string | No | SQL boolean predicate that scopes the assertion to a subset of rows. Only rows where the filter is `TRUE` are checked; rows where it's `FALSE` or `NULL` pass unconditionally. |

Type-specific fields: `accepted_values` takes `values` (a list of allowed string literals), `relationships` takes `to_table` and `to_column` (referential integrity against another table), `expression` takes an `expression` (a SQL boolean that must hold for every row), and `row_count_range` takes `min` and/or `max` (inclusive bounds on the total row count).

```toml
# models/fct_orders.toml
name = "fct_orders"

[[tests]]
type = "not_null"
column = "order_id"

[[tests]]
type = "unique"
column = "order_id"

[[tests]]
type = "accepted_values"
column = "status"
values = ["pending", "shipped", "delivered"]
severity = "warning"

[[tests]]
type = "expression"
expression = "amount >= 0"
filter = "status != 'cancelled'"

[[tests]]
type = "row_count_range"
min = 1
```

`filter` and `expression` are user-supplied SQL passed through verbatim, so treat them with the same trust as any SQL you run against the warehouse.

### `[[use_test]]`

References a reusable test defined once in `models/test_definitions.toml` and applies it to this model by name. Use this when several models share the same assertion and you don't want to repeat it as inline `[[tests]]`.

A named definition lives in `models/test_definitions.toml`, keyed by name, carrying the test `type` and its parameters plus an optional default `column`:

```toml
# models/test_definitions.toml
[positive_amount]
type = "expression"
expression = "amount > 0"

[known_status]
type = "accepted_values"
values = ["pending", "shipped", "delivered"]
column = "status"
```

A model applies one with a `[[use_test]]` reference:

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | Yes | Name of the definition in `test_definitions.toml`. An unknown name fails the load. |
| `column` | string | No | Column to bind the test to. Overrides the definition's own `column` at this use site. |
| `severity` | string | No | Failure severity here. Defaults to `error`. |
| `filter` | string | No | Row-scoping SQL predicate, same contract as an inline test's `filter`. |

```toml
# models/fct_orders.toml
name = "fct_orders"

[[use_test]]
name = "positive_amount"
severity = "warning"

[[use_test]]
name = "known_status"
column = "order_status"   # override the definition's default column
```

Resolved references are appended to the model's `[[tests]]` at load. A `[[use_test]]` block uses `deny_unknown_fields`, so a mistyped key (`colum =`, `filer =`) is rejected at load rather than silently applying the test with the wrong binding.

### `[[test]]`

A fixture-driven unit test. Where `[[tests]]` asserts properties of materialized output, a `[[test]]` checks the model's SQL logic against hand-written inputs: it seeds mock upstream tables, runs the model SQL, and compares the result to an expected set of rows. The block name is singular (`[[test]]`), unlike the plural `[[tests]]` used for declarative assertions.

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | Yes | Test name. Unique within the model. |
| `description` | string | No | Free-form note describing what the test covers. |

Each test declares one or more input fixtures and one expected output:

- **`[[test.given]]`** — a mocked upstream model or source. `ref` is the name to mock (matches a `depends_on` or `from` reference); `rows` is an inline list of TOML tables seeded as that table's contents.
- **`[test.expect]`** — the expected output. `rows` is the list of expected output rows. Set `ordered = true` to require the output in exactly this order; the default is a multiset comparison where row order doesn't matter.

```toml
# models/high_value_orders.toml
name = "high_value_orders"

[[test]]
name = "flags_orders_over_100"
description = "Orders over $100 should be flagged as high value"

[[test.given]]
ref = "orders"
rows = [
    { id = 1, amount = 150.0, status = "completed" },
    { id = 2, amount = 50.0, status = "completed" },
    { id = 3, amount = 200.0, status = "cancelled" },
]

[test.expect]
rows = [
    { id = 1, amount = 150.0, is_high_value = true },
    { id = 3, amount = 200.0, is_high_value = true },
]
```

A test may declare several `[[test.given]]` blocks to mock more than one upstream, and a model may declare several `[[test]]` blocks.

### `[columns.<name>]`

Per-column documentation. Each `[columns.<name>]` table attaches a description to one output column:

| Field | Type | Description |
|---|---|---|
| `description` | string | Natural-language description of the column. |

```toml
# models/fct_orders.toml
name = "fct_orders"

[columns.order_id]
description = "Unique order identifier"

[columns.amount]
description = "Order total in USD"
```

Descriptions surface in `rocky catalog --output json` as each asset's `CatalogColumn.description`. A description is attached only when its `<name>` matches a column the model actually projects; a description for a column the SELECT doesn't produce is silently dropped, so keep the key in sync with your output columns. The `rocky docs` HTML catalog does not emit per-column detail (it has no warehouse connection to introspect the column list), so column descriptions reach consumers through `rocky catalog`, not the generated HTML.

The singular `[columns.<name>]` table documents columns, and is distinct from the plural `[[columns]]` array used to declare a contract's column schema. The two look similar but do different jobs.

### Retention

Top-level `retention` key on the sidecar declares a data-retention policy for the model. Parsed at load time into a typed `RetentionPolicy { duration_days: u32 }`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `retention` | string \| null | `null` (disabled) | Grammar `^\d+[dy]$`. `"d"` = days verbatim, `"y"` = years flattened at 365 days per year (no leap-year semantics). Zero (`"0d"`, `"0y"`) is rejected — use `null` to disable. |

```toml
# models/fct_orders.toml
name = "fct_orders"
retention = "90d"

[strategy]
type = "incremental"
timestamp_column = "_fivetran_synced"

[target]
catalog = "analytics"
schema = "warehouse"
table = "fct_orders"
```

Applied by `GovernanceAdapter::apply_retention_policy` after a successful DAG run:

| Adapter | SQL emitted |
|---|---|
| **Databricks (Delta)** | `ALTER TABLE ... SET TBLPROPERTIES ('delta.logRetentionDuration' = '{N} days', 'delta.deletedFileRetentionDuration' = '{N} days')` — both keys written together. |
| **Snowflake** | `ALTER TABLE ... SET DATA_RETENTION_TIME_IN_DAYS = {N}`. |
| **BigQuery / DuckDB** | Default-unsupported — those warehouses lack a first-class retention knob at the config level. |

Garbage inputs (`"abc"`, `"90"`, `"-3d"`, `"1.5d"`, leading signs, exponents) are rejected at sidecar parse time with a `ModelError::InvalidRetention` diagnostic naming the offending value. Inspect resolved policies + warehouse state with [`rocky retention-status`](/reference/cli/#rocky-retention-status).

---

## Inline Format (Legacy)

The inline format embeds TOML configuration directly in the SQL file using a `---toml` / `---` fenced block at the top of the file:

```sql
---toml
name = "stg_orders"
depends_on = []

[target]
catalog = "analytics"
schema = "staging"
table = "orders"
---

SELECT
    order_id,
    customer_id,
    order_date,
    total_amount
FROM raw_catalog.src__acme__us_west__shopify.orders
```

The inline format uses the same fields as the sidecar TOML file. The SQL query follows the closing `---` marker.

The frontmatter block supports the same `${VAR}` / `${VAR:-default}` substitution as sidecar `.toml` files (see [Environment Variables](/reference/configuration/#environment-variables)); the SQL body below the closing `---` is **not** substituted, so any `${VAR}` token in the query stays literal.

This format is supported for backward compatibility; prefer the sidecar format.

---

## Strategy Examples

### Full Refresh

Drops and recreates the target table on every run. Use this for small dimension tables or when you need a clean rebuild.

**SQL** (`models/dim_products.sql`):

```sql
SELECT
    product_id,
    product_name,
    category,
    price,
    is_active
FROM raw_catalog.src__acme__us_west__shopify.products
WHERE _fivetran_deleted = false
```

**Config** (`models/dim_products.toml`):

```toml
name = "dim_products"
depends_on = []

[strategy]
type = "full_refresh"

[target]
catalog = "analytics"
schema = "warehouse"
table = "dim_products"

[[sources]]
catalog = "raw_catalog"
schema = "src__acme__us_west__shopify"
table = "products"
```

Generated SQL:

```sql
CREATE OR REPLACE TABLE analytics.warehouse.dim_products AS
SELECT
    product_id,
    product_name,
    category,
    price,
    is_active
FROM raw_catalog.src__acme__us_west__shopify.products
WHERE _fivetran_deleted = false
```

---

### Incremental

Appends only new rows based on a watermark column. Use this for large fact tables where full refresh is too slow.

**SQL** (`models/fct_orders.sql`):

```sql
SELECT
    order_id,
    customer_id,
    order_date,
    total_amount,
    _fivetran_synced
FROM raw_catalog.src__acme__us_west__shopify.orders
```

**Config** (`models/fct_orders.toml`):

```toml
name = "fct_orders"
depends_on = ["dim_products"]

[strategy]
type = "incremental"
timestamp_column = "_fivetran_synced"

[target]
catalog = "analytics"
schema = "warehouse"
table = "fct_orders"

[[sources]]
catalog = "raw_catalog"
schema = "src__acme__us_west__shopify"
table = "orders"
```

Generated SQL (on incremental runs):

```sql
INSERT INTO analytics.warehouse.fct_orders
SELECT
    order_id,
    customer_id,
    order_date,
    total_amount,
    _fivetran_synced
FROM raw_catalog.src__acme__us_west__shopify.orders
WHERE _fivetran_synced > TIMESTAMP '2026-04-17 09:30:00'
```

The watermark literal is the previous run's `MAX(_fivetran_synced)`, read from Rocky's state store — not a subquery against the target. On the first run (when the target table does not exist), Rocky performs a full refresh automatically.

---

### Merge

Upserts rows based on a unique key. Matching rows are updated; non-matching rows are inserted. Use this for slowly changing dimensions or tables with late-arriving updates.

**SQL** (`models/dim_customers.sql`):

```sql
SELECT
    customer_id,
    customer_name,
    email,
    segment,
    lifetime_value,
    updated_at
FROM raw_catalog.src__acme__us_west__shopify.customers
WHERE _fivetran_deleted = false
```

**Config** (`models/dim_customers.toml`):

```toml
name = "dim_customers"
depends_on = []

[strategy]
type = "merge"
unique_key = ["customer_id"]
update_columns = ["customer_name", "email", "segment", "lifetime_value", "updated_at"]

[target]
catalog = "analytics"
schema = "warehouse"
table = "dim_customers"

[[sources]]
catalog = "raw_catalog"
schema = "src__acme__us_west__shopify"
table = "customers"
```

Generated SQL:

```sql
MERGE INTO analytics.warehouse.dim_customers AS target
USING (
    SELECT
        customer_id,
        customer_name,
        email,
        segment,
        lifetime_value,
        updated_at
    FROM raw_catalog.src__acme__us_west__shopify.customers
    WHERE _fivetran_deleted = false
) AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN UPDATE SET
    target.customer_name = source.customer_name,
    target.email = source.email,
    target.segment = source.segment,
    target.lifetime_value = source.lifetime_value,
    target.updated_at = source.updated_at
WHEN NOT MATCHED THEN INSERT *
```

When `update_columns` is omitted, Rocky updates all non-key columns.

---

### Ephemeral

An ephemeral model is never materialized; Rocky inlines it as a CTE in every downstream consumer. Useful for lightweight intermediate transformations you don't want to persist.

**Config** (`models/stg_recent_orders.toml`):

```toml
name = "stg_recent_orders"
depends_on = []

[strategy]
type = "ephemeral"

[target]
catalog = "analytics"
schema = "staging"
table = "stg_recent_orders"
```

No DDL runs for ephemeral models. The SQL body is injected as a `WITH stg_recent_orders AS (…)` CTE wherever the model is referenced.

---

### Delete + Insert

Deletes matching rows by partition key, then inserts fresh data. A lower-overhead alternative to `merge` when the partition key identifies the rows being rewritten.

**Config** (`models/fct_daily_activity.toml`):

```toml
name = "fct_daily_activity"
depends_on = []

[strategy]
type = "delete_insert"
partition_by = ["activity_date"]

[target]
catalog = "analytics"
schema = "warehouse"
table = "fct_daily_activity"
```

---

### Microbatch

An alias for `time_interval` with `hour`-granularity defaults. dbt-compatible naming for partition-based incremental processing.

**Config** (`models/fct_hourly_events.toml`):

```toml
name = "fct_hourly_events"
depends_on = []

[strategy]
type = "microbatch"
timestamp_column = "event_at"   # TIMESTAMP column on the model output
# granularity = "hour"           # optional — defaults to hour

[target]
catalog = "analytics"
schema = "warehouse"
table = "fct_hourly_events"
```

---

### Content-Addressed

Writes the model's SELECT result to a Delta UniForm table as content-addressed Parquet (blake3-hashed file names) plus a Delta log commit. Designed for cross-engine reads from DuckDB, Trino, Spark, and any Iceberg-compatible reader: Rocky owns the writer, and the consumers read directly from the object store. See [Content-Addressed Materialization](/concepts/content-addressed/) for the why and when.

**Config** (`models/fct_events.toml`):

```toml
name = "fct_events"
depends_on = []

[strategy]
type = "content_addressed"
storage_prefix = "s3://${ROCKY_BUCKET}/marts/fct_events"
partition_columns = ["event_date"]

[target]
catalog = "analytics"
schema = "marts"
table = "fct_events"
```

The runtime executes the model SQL, converts the result to Arrow, hashes the Parquet bytes, uploads to `storage_prefix`, and emits a Delta log commit. `partition_columns` may be omitted for unpartitioned tables. Backed by the `rocky-iceberg` writer (shipped in engine v1.30.0 across Phases 1–5: discover, write, sync, partitioned, rowTracking, schema evolution).

---

### Time Interval

Partition-keyed materialization for time-series data. The model SQL uses `@start_date` and `@end_date` placeholders that the runtime substitutes per partition.

**SQL** (`models/fct_daily_events.sql`):

```sql
SELECT
    event_date,
    event_type,
    COUNT(*) AS event_count
FROM raw_catalog.events.page_views
WHERE event_date >= @start_date
  AND event_date < @end_date
GROUP BY event_date, event_type
```

**Config** (`models/fct_daily_events.toml`):

```toml
name = "fct_daily_events"
depends_on = []

[strategy]
type = "time_interval"
time_column = "event_date"
granularity = "day"
lookback = 3
first_partition = "2024-01-01"

[target]
catalog = "analytics"
schema = "warehouse"
table = "fct_daily_events"
```

**CLI flags** for time-interval models. Every flag below is accepted on both `rocky plan` and the `rocky run` single-step alias, which fuses plan + apply into one invocation for local iteration and automation. The canonical, auditable form is `rocky plan` followed by `rocky apply <plan-id>`.

```bash
# Process a specific partition
rocky plan --partition 2026-04-01 && rocky apply <plan-id>

# Process a date range
rocky plan --from 2026-03-01 --to 2026-04-01 && rocky apply <plan-id>

# Process the latest partition
rocky plan --latest && rocky apply <plan-id>

# Discover and process missing partitions
rocky plan --missing && rocky apply <plan-id>

# Set lookback window
rocky plan --lookback 7 && rocky apply <plan-id>

# Parallelize partition processing
rocky plan --parallel 4 && rocky apply <plan-id>
```

Per-partition state is tracked in the state store. The `--missing` flag consults stored partition records to discover gaps.

---

## DAG Resolution

Rocky automatically resolves the execution order of models based on their `depends_on` declarations. Models are executed in topological order, meaning every upstream dependency runs before its downstream dependents.

During `rocky validate`, the DAG is checked for cycles. If a cycle is detected (e.g., model A depends on B, B depends on A), validation fails with an error listing the cycle.

```
!!  dag_validation — cycle detected: fct_orders -> dim_customers -> fct_orders
```

Models with no dependencies run first. Models at the same depth in the DAG may run concurrently in future versions.
