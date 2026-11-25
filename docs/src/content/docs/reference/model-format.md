---
title: Model Format
description: SQL and TOML model file specification
sidebar:
  order: 4
---

A Rocky model is one SQL query plus the configuration that says how to materialize it, what it depends on, and where the output goes. One model produces one table or view.

Rocky reads two model formats: **sidecar** (recommended) and **inline** (legacy).

## Model format at a glance

| Block | What it does |
|---|---|
| [Top-level keys](#toml-config-file) | `name`, `depends_on`, `group`, `access`, `retention`, `adapter`, `intent`. |
| [`[strategy]`](#toml-config-file) | How Rocky writes the table. Examples are under [Strategy Examples](#strategy-examples). |
| [`[target]`](#toml-config-file), [`[[sources]]`](#toml-config-file) | Where the output lands; input tables for lineage. |
| [`[skip]`](#skip) | Override the `--skip-unchanged` gate for this model. |
| [`[args]`](#toml-config-file), [config groups](#config-groups) | Shared routing and materialization for a fan-out of models. |
| [`[classification]`](#classification), [`[tags]`](#tags), [`[governance.tags]`](#governancetags) | Column sensitivity, model attributes, warehouse tags. |
| [`[[surrogate_key]]`](#surrogate_key) | Inject a stable hash key column. |
| [`[[tests]]`](#tests), [`[[use_test]]`](#use_test), [`[[test]]`](#test) | Output assertions, shared assertions, unit tests with mock inputs. |
| [`[columns.NAME]`](#columnsname) | Column descriptions. |
| [`retention`](#retention) | How long to keep the data. |
| `[freshness]`, `[budget]`, `[redshift]`, `[clickhouse]` | Described in the notes under the field table. |

A minimal model is two files:

```toml
# models/fct_orders.toml
name = "fct_orders"
depends_on = ["stg_orders"]

[strategy]
type = "merge"
unique_key = ["order_id"]

[target]
catalog = "analytics"
schema = "warehouse"
table = "fct_orders"
```

The `.sql` file next to it holds a plain `SELECT`.

## Sidecar Format (Recommended)

Keep the SQL and the configuration in two files that share a name. The `.toml` file is the [sidecar](/reference/glossary/#sidecar): it carries everything that is not SQL.

```
models/
├── fct_orders.sql          <- pure SQL
├── fct_orders.toml         <- configuration
├── stg_customers.sql
├── stg_customers.toml
├── dim_products.sql
└── dim_products.toml
```

The `.sql` file stays readable by any SQL tool: open it in a query editor or run it by hand.

### SQL File

The `.sql` file holds a plain SQL query. It has no templating and no Jinja.

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

The `.toml` file names the model, lists what it depends on, picks a materialization strategy, and says where the output lands.

**Fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Model identifier. Must be unique across all models. |
| `drop_existing_kind` | `"table"` or `"view"` | No | Standing permission to drop a target of this existing kind when switching between `full_refresh` and `view`. DuckDB only today. |
| `depends_on` | list of strings | No | Names of upstream models that must run before this one. Defaults to `[]`. |
| `group` | string | No | Name of a [config group](#config-groups) (`models/groups/<name>.toml`) this model opts into for shared routing and materialization. |
| `access` | string | No | `private`, `protected` (default) or `public`. Who may reference the model. See [Model governance](/concepts/model-governance/). |
| `access_group` | string | No | Ownership group for access checks. Falls back to `group`. Inherits no config. See [Model governance](/concepts/model-governance/#ownership-groups-and-owners). |
| `retention` | string | No | Data retention policy for this model. Grammar `^\d+[dy]$` — e.g. `"90d"` or `"1y"`. See [Retention](#retention). |
| `adapter` | string | No | An `[adapter.NAME]` key from `rocky.toml`. Runs this model on that adapter instead of the pipeline's. |
| `intent` | string | No | A plain-language description of what the model does. `rocky ai-sync` reads it. |

More sidecar blocks: `[freshness]` (`warn_after`, `error_after`, `time_column`; see [`[freshness]`](/reference/configuration/#freshness)), `[budget]` (a per-model override of the project [`[budget]`](/reference/configuration/#budget), checked at compile time as `E027`), and the adapter-specific [`[redshift]`](/reference/adapters/redshift/#table-distribution-and-sort-keys) and [`[clickhouse]`](/reference/adapters/clickhouse/#table-engine-partitions-and-sort-key) blocks.

:::caution[Unknown top-level keys are ignored]
Rocky does not refuse an unknown top-level key in a sidecar. A typo such as `depends_onn = ["x"]` loads without an error, and the setting has no effect. Check each key against this page.
:::

`drop_existing_kind` applies only when a `full_refresh` model finds a view, or a `view` model finds a table. Rocky checks the existing kind before using the permission. On DuckDB, the DROP and CREATE run in one transaction. Rocky refuses a `full_refresh` or `view` model carrying this key on every other adapter. The key is a standing permission on the model, not a one-time approval. Rocky keeps no ownership record for the old object; confirm the target belongs to this model before setting the key.

**`[args]`** -- Placeholder values for a config group's `schema_template` (only meaningful when the model declares a `group`):

| Key pattern | Value type | Description |
|---|---|---|
| `<placeholder>` | string | Fills a `{placeholder}` in the group's `schema_template` (e.g. `region = "emea"` resolves `mart_{region}` to `mart_emea`). Ignored when the model declares no `group`. See [Config groups](#config-groups). |

**`[strategy]`** -- Materialization configuration:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | string | `"full_refresh"` | Materialization type. One of `"full_refresh"`, `"incremental"`, `"merge"`, `"time_interval"`, `"view"`, `"materialized_view"`, `"dynamic_table"`, `"delete_insert"`, `"microbatch"`, `"content_addressed"`, `"ephemeral"`, `"snapshot"`. `"incremental"` needs a watermark column, or compile fails with `E037` (see [Incremental](#incremental)). `"ephemeral"` is inlined into its readers; `E038` marks a use that cannot work (see [Ephemeral](#ephemeral)). See [Snapshot](#snapshot) for `"snapshot"`. |
| `timestamp_column` | string | | The watermark column. Required for `incremental` (alias `watermark`) and for `microbatch`, where it names the output partition column. |
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

The chosen `format` and `format_options` apply on the **first** materialization of incremental-family models (`delete_insert`, `microbatch`, `time_interval`), not only on full-create strategies. The table that bootstraps such a model is created in the requested Delta or Iceberg shape from the start. It is not a plain table that gains the format later.
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

Overrule the `--skip-unchanged` gate for this one model. By default the gate is cautious. A model qualifies to be skipped only when a static scan finds its SQL [deterministic](/reference/glossary/#deterministic): same inputs, same output, every time. It must also use a plain materialization strategy. This block lets the model's owner say otherwise. Omit it and the automatic rules apply.

| Field | Type | Default | Description |
|---|---|---|---|
| `eligible` | bool \| null | `null` | Explicit eligibility override. `false` ⇒ this model **always builds**, even when the gate is on and everything looks unchanged (use for a known-volatile model the static scan might miss). `true` ⇒ the model is eligible, subject to the other gate clauses. `null` ⇒ fall back to the automatic rules. |
| `deterministic` | bool \| null | `null` | Owner assertion about the SQL's purity. `true` is the only way a model the static non-determinism scan flagged (timestamps, randomness, unresolved UDFs, order-unstable aggregates) becomes skip-eligible — an explicit, auditable opt-in. `false` forces the model to be treated as non-deterministic (never auto-skipped). `null` ⇒ trust the static scan. |

Example: `eligible = false` always rebuilds the model. `deterministic = true` re-qualifies a model the scan flagged.

**Fail-safe rules.** The gate builds on any doubt. A model is never auto-skip-eligible when its SQL is non-deterministic, its lineage is not provably complete, or it uses `content_addressed` or `time_interval` (a `full_refresh` model **is** eligible). `deterministic = true` overrides only the non-deterministic case. See [What is and is not skip-eligible](/reference/configuration/#what-is-and-is-not-skip-eligible) for the full list, and [Skip Unchanged Models and Defer to Prod](/guides/skip-and-defer/) for the workflow.

### Environment variables

Sidecar files and `models/_defaults.toml` get the same `${VAR}` and `${VAR:-default}` substitution as `rocky.toml`. An orchestrator can therefore set a model's `[target]` through the subprocess environment, with no templating in the sidecar. See [Environment variables](/reference/configuration/#environment-variables) for the syntax and a sidecar example, and [`examples/playground/pocs/00-foundations/07-config-layering/`](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/00-foundations/07-config-layering) for a runnable three-layer example.

### `@var()` run variables

Parameterize a model per run. Write `@var(name)` or `@var(name, default)` in the model body and bind it with a repeatable `rocky run --var name=value`. Rocky substitutes the value into the SQL before it reaches the warehouse:

```sql
-- models/orders.sql
SELECT * FROM raw.orders
WHERE region = '@var(region)' AND status = '@var(status, shipped)'
```

```bash
rocky run --var region=emea --var status=delivered
```

`@var(region)` has no default, so you must supply it. A `@var(name)` with no binding and no default is a **compile error** naming the variable. The substitution is **textual**: you own the quoting and casting around the marker. Rocky validates only the variable *name*, as a SQL identifier. `rocky import-dbt` maps dbt's `{{ var('name') }}` and `{{ var('name', default) }}` onto these markers.

`${ENV}` resolves while Rocky parses `rocky.toml` and the sidecars, before any model is read, and sets config values (target catalog, credentials). `@var(name)` resolves at compile time, stays visible in the model source, and sets a run's logical inputs (a region, a status).

### Config groups

Write shared settings once when a fan-out of models routes and materializes the same way. A **config group** lives in `models/groups/<name>.toml` (the file stem is the group name) and supplies a `schema_template` and a `strategy`. A model joins with `group = "<name>"` and fills the template's placeholders from its own `[args]`:

```toml
# models/groups/daily_marts.toml
schema_template = "mart_{region}"

[strategy]
type = "merge"
unique_key = ["id"]
update_columns = ["amount", "status"]
```

```toml
# models/fct_orders.toml
group = "daily_marts"

[target]
catalog = "warehouse"   # schema comes from the group template

[args]
region = "emea"         # fills {region} -> schema "mart_emea"
```

Three layers can set the same field. The nearest one to the model wins: the model's sidecar, then `models/groups/<name>.toml`, then `models/_defaults.toml`.

A `group` naming no definition fails the load, and so does a `schema_template` placeholder the model does not supply. A model that pins its own `schema` bypasses the template, so it must **not** also supply `[args]`: pin a schema *or* supply args, never both.

A group file may carry `schema_template`, `strategy`, `tags`, `governance`, `enforce`, and `[owner]` (`name`, `email`; see [Model governance](/concepts/model-governance/#ownership-groups-and-owners)). Rocky rejects an unrecognized key at load. The model loader does not recurse, so it never reads `models/groups/` as model files.

#### Enforced groups

Set `enforce = true` in the group file (opt-in; otherwise a group is overridable defaults). A member that pins a field the group owns, its target `schema` or its `strategy`, then fails the load with `GroupOverride`. It may still supply its own `[args]` and any field the group leaves unset, such as `target.catalog`. Use it when models must share routing and materialization as a governance guarantee.

#### Group tags

A group can declare a `[tags]` block that every member inherits as a baseline. A model's own `[tags]` override it key by key: a model can set `tier = "silver"` and still inherit `domain = "finance"`. See [`[tags]`](#tags).

### `[classification]`

Label a column as sensitive so the [masking policy](/reference/glossary/#masking-policy) can act on it. Keys are column names. Values are free-form classification strings (`"pii"`, `"confidential"`, `"internal"`), matched case-insensitively against `[mask]` and `[mask.<env>]` in `rocky.toml` to pick a strategy. Rocky applies the column tag and the mask through the governance adapter after a successful DAG run. A tag without a matching strategy emits `W004` unless listed in [`[classifications] allow_unmasked`](/reference/configuration/#classifications).

```toml
# models/customers.toml
name = "customers"

[classification]
email = "pii"
ssn = "confidential"
```

Masking works today on **Databricks** Unity Catalog only (column tags plus `CREATE MASK` / `SET MASKING POLICY`, one statement per column), best-effort. End-to-end story: [Governance](/guides/governance/). Resolver semantics: [`[mask]`](/reference/configuration/#mask).

### `[tags]`

Describe the model **as a whole**: its `domain`, `tier`, `owner`, or anything else your governance model needs. `[classification]`, by contrast, is keyed by column and drives masking.

| Key pattern | Value type | Description |
|---|---|---|
| `<tag_name>` | string | Free-form attribute. Merged over any [config-group `[tags]`](#group-tags) baseline (sidecar wins). |

`rocky compile --output json` reports the resolved tags as `models_detail[].tags`, and `dagster-rocky` projects them onto the derived asset's Dagster tags. `[tags]` never touches the warehouse: use [`[governance.tags]`](#governancetags) for that.

### `[governance.tags]`

Put a tag on the warehouse object itself. After the model materializes, Rocky writes these as Unity Catalog tags on its **own target table or view**: `ALTER VIEW … SET TAGS (…)` for a view-format model, `ALTER TABLE … SET TAGS (…)` otherwise.

```toml
# models/fct_orders.toml
[governance.tags]
domain = "finance"
tier = "gold"
```

| Key pattern | Value type | Description |
|---|---|---|
| `<tag_name>` | string | Unity Catalog tag on this model's target table or view. Keys and values are used verbatim, with no prefix. |

This is the per-model counterpart to the pipeline-level [tagging strategy](/guides/governance/#9-tagging-strategy). It is best-effort (a failure warns and never aborts the run), and an empty block is skipped because Unity Catalog rejects `SET TAGS ()`.

### `[[surrogate_key]]`

Add a stable key column without writing the hash expression. Rocky injects a deterministic hash over the columns you list into the model's SELECT.

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | Yes | Output column name. A valid SQL identifier (`^[a-zA-Z0-9_]+$`). |
| `columns` | list of strings | Yes | Input columns to hash. At least one, each a valid SQL identifier. |

```toml
# models/dim_customers.toml
[[surrogate_key]]
name = "customer_sk"
columns = ["tenant_id", "customer_id"]
```

At `rocky run` (and on the emit-SQL path), Rocky appends `CAST(md5(...) AS <string_type>) AS <name>` to the projection, in the dialect's form (`STRING` on Databricks and BigQuery, `VARCHAR` on Snowflake, DuckDB and Trino; BigQuery uses `to_hex(...)` / `concat(...)`). The hash matches `dbt_utils.generate_surrogate_key` over the same columns on the same warehouse, so keys join across Rocky and dbt models. NULL inputs coalesce to a fixed sentinel first. The block refuses an unknown key (`colums = [...]` fails the load), an empty `columns` list, and a name or column that is not a valid identifier. Declare several blocks for several key columns.

### `[[tests]]`

Assert a property of the model's output. Each `[[tests]]` block is one assertion that runs against the target table. Rocky generates the SQL for the target dialect.

| Field | Type | Required | Description |
|---|---|---|---|
| `type` | string | Yes | Assertion kind. Common types: `not_null`, `unique`, `accepted_values`, `relationships`, `expression`, `row_count_range`. Others: `in_range`, `regex_match`, `aggregate`, composite-key uniqueness. |
| `column` | string | Sometimes | Column under test. Required for `not_null`, `unique`, `accepted_values`, `relationships`. Ignored for `expression` and `row_count_range`. |
| `severity` | string | No | `"error"` (default) fails the run; `"warning"` records the failure and continues. |
| `filter` | string | No | SQL boolean predicate that scopes the assertion. Rows where it is `FALSE` or `NULL` pass unconditionally. |

Type-specific fields: `accepted_values` takes `values`, `relationships` takes `to_table` and `to_column`, `expression` takes an `expression` (a SQL boolean that must hold for every row), `row_count_range` takes `min` and/or `max` (inclusive).

```toml
[[tests]]
type = "not_null"
column = "order_id"

[[tests]]
type = "expression"
expression = "amount >= 0"
filter = "status != 'cancelled'"
```

`expression` and `filter` share one gate. Rocky parses them under the target dialect and refuses, before anything runs, a subquery, a qualified function name (`schema.fn(...)`) or a function outside its allowlist of pure scalar functions. Comparisons, `CASE`, `CAST`, `coalesce`, `length`, `lower` and `date_trunc` pass. The refusal names the function. See [Per-assertion `filter`](/concepts/data-quality-checks/#per-assertion-filter).

### `[[use_test]]`

Apply a test you defined once, by name. Reach for this when several models share the same assertion and repeating it as inline `[[tests]]` would mean maintaining it in several places.

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

Rocky appends resolved references to the model's `[[tests]]` at load. It rejects a mistyped key (`colum =`, `filer =`) at load.

### `[[test]]`

Check the model's SQL logic against inputs you write by hand, with no warehouse involved. Rocky seeds mock upstream tables, runs the model's SQL over them, and compares the result to the rows you expect. Where `[[tests]]` asserts a property of real materialized output, `[[test]]` tests the logic itself. Note the singular block name: `[[test]]` here, `[[tests]]` for declarative assertions.

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

Document what an output column means. Each `[columns.<name>]` table describes one column:

| Field | Type | Description |
|---|---|---|
| `description` | string | Natural-language description of the column. |

Write one table per column, for example `[columns.order_id]` with `description = "Unique order identifier"`.

`rocky catalog --output json` reports each description as the asset's `CatalogColumn.description`. Rocky attaches a description only when `<name>` matches a column the model actually projects. It drops a description for a column the SELECT does not produce, silently, so keep these keys in step with your output columns. `rocky docs` shows each description next to its column on the model page. It takes the column list from the offline compile, so it needs no warehouse connection.

`[columns.<name>]` documents columns. It differs from the `[[columns]]` array that declares a contract's column schema.

### Retention

Declare how long this model's data should be kept with the top-level `retention` key. Rocky parses it at load into a typed `RetentionPolicy { duration_days: u32 }`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `retention` | string \| null | `null` (disabled) | Grammar `^\d+[dy]$`. `"d"` is days, `"y"` is years flattened at 365 days. Zero (`"0d"`, `"0y"`) is rejected: use `null` to disable. |

Example: `retention = "90d"`. Rocky applies it through `GovernanceAdapter::apply_retention_policy` after a successful DAG run: Delta table properties on Databricks, `DATA_RETENTION_TIME_IN_DAYS` on Snowflake, nothing on other adapters (see [Governance](/guides/governance/#7-data-retention-pillar-5-of-5)). A malformed value (`"abc"`, `"90"`, `"-3d"`, `"1.5d"`, a sign, an exponent) fails the sidecar parse with `ModelError::InvalidRetention`. Inspect resolved policies with [`rocky retention-status`](/reference/cli/#rocky-retention-status).

---

## Inline Format (Legacy)

The inline format puts the TOML configuration inside the SQL file, in a `---toml` / `---` fenced block at the top:

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

The fields are identical to the sidecar file's. The SQL query follows the closing `---` marker.

The frontmatter block takes the same `${VAR}` and `${VAR:-default}` substitution as a sidecar (see [Environment variables](/reference/configuration/#environment-variables)). The SQL body below the closing `---` gets **no** substitution, so a `${VAR}` token in the query stays literal.

This format exists for backward compatibility. Prefer the sidecar.

---

## Strategy Examples

### Full Refresh

Drops and recreates the target table on every run. Use this for small dimension tables or when you need a clean rebuild.

**Config** (`models/dim_products.toml`):

```toml
name = "dim_products"

[strategy]
type = "full_refresh"

[target]
catalog = "analytics"
schema = "warehouse"
table = "dim_products"
```

Rocky runs `CREATE OR REPLACE TABLE analytics.warehouse.dim_products AS <model SELECT>`.

---

### Incremental

Loads only the rows newer than what the target already holds. Use it when the source has a column whose values only grow, such as `updated_at`. The column is the model's watermark (the value of the newest row already loaded).

**SQL** (`models/fct_orders.sql`):

```sql
SELECT order_id, customer_id, amount, status, updated_at
FROM raw.orders
WHERE @incremental_filter
```

**Config** (`models/fct_orders.toml`):

```toml
[strategy]
type = "incremental"
timestamp_column = "updated_at"   # the watermark; `watermark` is an alias
unique_key = ["order_id"]         # optional: MERGE on this key instead of appending
lookback = "2 days"               # optional: re-read this far below the watermark
on_schema_change = "fail"         # or "append_new_columns"
```

| Key | Required | Meaning |
|---|---|---|
| `timestamp_column` | yes | An output column. Rocky reads `MAX` of it from the target each run. Alias: `watermark`. |
| `unique_key` | no | Upsert on these columns with `MERGE`. Without it, Rocky appends. |
| `lookback` | no | `"<n> seconds"`, `"minutes"`, `"hours"` or `"days"`. Re-reads late rows. Pair it with `unique_key`, or the re-read rows are appended again (`W046`). |
| `on_schema_change` | no | `fail` (default) stops the run when the model's columns differ from the target's. `append_new_columns` adds new columns with `ALTER TABLE ... ADD COLUMN`. A removed column fails the run in both modes. |
| `filter_column` | no | The input column `@incremental_filter` compares, when it is not the watermark itself: `"o.updated_at"` in a join, or `"_synced_at"` when the model renames it. |

#### How Rocky resolves `@incremental_filter`

The placeholder marks where the filter goes. Rocky replaces it on every run:

```
 run                          @incremental_filter becomes
 ───────────────────────────  ──────────────────────────────────────────────
 first run (no target yet)    TRUE                    → CREATE TABLE AS
 rocky run --full-refresh     TRUE                    → CREATE OR REPLACE
 every later run              (updated_at > (SELECT MAX(updated_at)
                                 FROM <target>)
                               OR NOT EXISTS
                                 (SELECT 1 FROM <target>))
                                                      → INSERT or MERGE
```

Rocky reads the watermark from the target, not from its state store. A manual edit of the target therefore moves the watermark too. The `NOT EXISTS` arm loads every row when the target exists but is empty. A `lookback` subtracts its interval from `MAX`. Rows whose watermark is `NULL` load only on the first run and on a full refresh, because `NULL` never compares greater.

The run output reports the watermark. `metadata.watermark` holds the new `MAX` when it is a timestamp, and `notes` names the value the run started from.

#### A model without the placeholder

Rocky can filter the model's output instead of its input. It then runs `SELECT * FROM (<model>) AS _rocky_incremental WHERE <filter on the output column>`. This gives the same rows only when the watermark column is copied unchanged from one input table. `rocky compile` checks that with column lineage. It refuses a column read from a CTE or a subquery, and a model with a top-level `LIMIT`. `filter_column` has no effect on this path. When it cannot prove it, the compile fails with `E046` and asks for the placeholder. A model in the `.rocky` DSL always takes this path, because the DSL has no placeholder.

#### What `rocky compile` refuses

| Code | Cause |
|---|---|
| `E037` | `type = "incremental"` with no `timestamp_column`. Rocky could only append every row again on each run. |
| `E046` | No placeholder and the watermark is not a provable passthrough. Also: the watermark is missing from the model's output, `timestamp_column` or `filter_column` is not a plain column name, or `@incremental_filter` appears in a model of another strategy. |
| `W046` | `lookback` without `unique_key`. |
| `W056` | No `lookback`. The filter is a strict `>`, so a late row whose timestamp equals the target's `MAX` is never loaded. `unique_key` alone does not fix this: it merges only the rows the filter reads. Set `lookback` with `unique_key`. |

`rocky run` records a refused model as a failed table and leaves its existing table alone. Rocky also withholds every model that depends on it. With `contain_failures = true` under `[resilience]` (the default), the same holds for a model that fails while it runs. Set it to `false` to restore fail-fast for those. See [`[resilience]`](/reference/configuration/#resilience).

Other strategies fit other needs:

| You need | Use |
|---|---|
| Update existing rows by key from the whole result | [`merge`](#merge) with `unique_key` |
| Replace whole partitions | [`delete_insert`](#delete--insert) with `partition_by` |
| Process one time window per run, with late data | [`time_interval`](#time-interval), with `@start_date` and `@end_date` in the SQL |
| Rebuild the table from the model's SQL | [`full_refresh`](#full-refresh) |

On a replication pipeline, `incremental` copies source tables and filters each copy on a watermark in the state store. See [Incremental processing](/concepts/incremental/).

---

### Merge

[Upserts](/reference/glossary/#upsert) on a unique key: Rocky updates a row whose key already exists and inserts one whose key does not. Use it for a [slowly changing dimension](/reference/glossary/#scd-slowly-changing-dimension), or for any table that gets late-arriving updates.

**Config** (`models/dim_customers.toml`):

```toml
name = "dim_customers"

[strategy]
type = "merge"
unique_key = ["customer_id"]
update_columns = ["customer_name", "email", "segment", "lifetime_value", "updated_at"]

[target]
catalog = "analytics"
schema = "warehouse"
table = "dim_customers"
```

Rocky runs `MERGE INTO <target> AS target USING (<model SELECT>) AS source ON target.customer_id = source.customer_id`. It sets each `update_columns` column on `WHEN MATCHED` and inserts the row on `WHEN NOT MATCHED`. When `update_columns` is omitted, Rocky updates all non-key columns.

---

### Ephemeral

An ephemeral model is never materialized. Rocky creates no table or view for it. Instead, each model that reads it gets the ephemeral model's SQL as a CTE (a named subquery in a `WITH` clause). This matches dbt's `materialized='ephemeral'`.

**Config** (`models/eph_paid_orders.toml`):

```toml
[strategy]
type = "ephemeral"
```

**SQL** (`models/eph_paid_orders.sql`):

```sql
SELECT order_id, customer_id, amount FROM raw.orders WHERE status = 'paid'
```

A consumer reads it by its bare name, `FROM eph_paid_orders`. Rocky runs the consumer as:

```sql
WITH __rocky_ephemeral__eph_paid_orders AS (
  SELECT order_id, customer_id, amount FROM raw.orders WHERE status = 'paid'
)
SELECT customer_id, SUM(amount) AS total
FROM __rocky_ephemeral__eph_paid_orders AS eph_paid_orders
GROUP BY customer_id
```

How the inlining works:

- The CTE is named `__rocky_ephemeral__<model>`. Rocky adds `_2`, `_3` if the name is taken.
- A reference with no alias keeps the model name as its alias, so `eph_paid_orders.amount` still works.
- The CTE goes in front of any `WITH` clause the consumer has.
- An ephemeral model that reads another works: one CTE each, in dependency order, once per consumer.
- Only a bare model name is a reference. A CTE of the same name in the consumer wins.
- The rewrite works on parsed SQL, so the consumer's executed SQL loses its comments and spacing.

What each command does with an ephemeral model:

| Command | Behavior |
|---|---|
| `rocky compile` | Type-checks the model and its consumers as written. Column types and lineage flow through the ephemeral model as through any other model. `--expand-macros` shows each consumer's SQL with the CTE inlined. |
| `rocky run` | Skips the model. It never appears in `materializations`. `rocky run --dag` marks its node as skipped, and its consumers still run. |
| `rocky run --model <ephemeral>` | Fails with `E038`. There is nothing to build. Run a model that reads it instead. |
| `rocky plan`, `rocky emit-sql` | List the model as skipped. Its SQL appears inside each consumer's statement. |
| Shadow and branch runs | Skip the model. The reads inside its inlined SQL are routed to shadow targets like any other read. |

`rocky compile` reports `E038` for a use that cannot work:

- The model declares `[[tests]]`. There is no table to test. Move the tests to a model that reads it.
- Another model reads the model's nominal `[target]` by a qualified name, such as `main.eph_paid_orders`. No table has that name. Read the model by its bare name.
- A consumer cannot be rewritten: its SQL is not one `SELECT` the parser accepts, or a `WITH RECURSIVE` CTE has the same name as a table the inlined SQL reads.

A contract on an ephemeral model is checked at compile time against the inferred columns, as for any model.

---

### Delete + Insert

Deletes the rows in a [partition](/reference/glossary/#partition) — a slice of the table identified by a column value — then inserts fresh ones. It costs less than `merge` when the partition key already identifies exactly the rows you are rewriting.

**Config** (`models/fct_daily_activity.toml`):

```toml
name = "fct_daily_activity"

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

An alias for `time_interval` that defaults to `hour` granularity. The name matches dbt's for partition-based incremental processing.

The SQL must use both `@start_date` and `@end_date` to bound each partition (`WHERE event_at >= @start_date AND event_at < @end_date`, as in [Time Interval](#time-interval)). A missing placeholder is compile error `E024`. A placeholder counts as missing when it appears only in a comment, a string or the `SELECT` list, when it does not bound a column in an `AND`-joined comparison (`ts >= @start_date OR 1 = 1`, `NOT (ts >= @start_date)`), or when the filter misses emitted rows (an unfiltered `UNION ALL` branch, a filter in a CTE the output never reads, a filter in a scalar subquery in the `SELECT` list). See [Time interval](/concepts/time-interval/#sql-placeholders). Each run replaces its selected partitions instead of appending.

**Config** (`models/fct_hourly_events.toml`):

```toml
name = "fct_hourly_events"

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

[strategy]
type = "content_addressed"
storage_prefix = "s3://${ROCKY_BUCKET}/marts/fct_events"
partition_columns = ["event_date"]

[target]
catalog = "analytics"
schema = "marts"
table = "fct_events"
```

The runtime executes the model SQL, converts the result to Arrow, hashes the Parquet bytes, uploads to `storage_prefix`, and emits a Delta log commit. `partition_columns` may be omitted for unpartitioned tables. Backed by the `rocky-iceberg` writer.

---

### Snapshot

A snapshot keeps the history of its model's rows. This is a slowly changing dimension of type 2 (SCD2): each change to a row adds a new version and closes the old one. Rocky follows [dbt snapshots](https://docs.getdbt.com/docs/build/snapshots), but a snapshot here is an ordinary model. It runs in the model DAG under `rocky run`, and downstream models can read it.

**Config** (`models/customers_history.toml`):

```toml
[strategy]
type = "snapshot"
unique_key = "customer_id"     # one column, or a list for a composite key
strategy = "timestamp"         # or "check"
updated_at = "updated_at"      # timestamp strategy: the change column
# check_cols = ["name", "email"] # check strategy: a list, or "all"
hard_deletes = "invalidate"    # "ignore" (default), "invalidate", "new_record"

[target]
catalog = "analytics"
schema = "snapshots"
table = "customers_history"
```

The model SQL is a plain SELECT, for example `SELECT customer_id, name, email, updated_at FROM raw.customers`.

**How a change is detected:**

- `timestamp`: a row changed when its `updated_at` is later than the current version's. The new version's `valid_from` is that `updated_at`.
- `check`: a row changed when a column in `check_cols` differs from the current version. NULL counts as a value. `"all"` compares every column except the key. The new version's `valid_from` is the run time, or the `updated_at` column when you also set one.

**Columns Rocky adds to each row:**

| Column | Meaning |
|---|---|
| `valid_from` | When this version became current. |
| `valid_to` | When this version stopped being current. NULL on the current version. |
| `is_current` | `TRUE` on the current version of each key. |
| `snapshot_id` | A hash of the key and `valid_from`, unique per version. |
| `is_deleted` | Only with `hard_deletes = "new_record"`: `TRUE` on a deletion marker. |

These are the names the `snapshot` pipeline uses. Rename any of them with `snapshot_meta_column_names`. The dbt key names are accepted too, so an imported dbt config reads unchanged:

```toml
[strategy]
type = "snapshot"
unique_key = ["id", "region"]
strategy = "check"
check_cols = "all"
snapshot_meta_column_names = { valid_from = "dbt_valid_from", valid_to = "dbt_valid_to", scd_id = "dbt_scd_id", updated_at = "dbt_updated_at", is_current = false }
valid_to_current = "CAST('9999-12-31' AS TIMESTAMP)"
```

- `updated_at` in `snapshot_meta_column_names` adds a copy of the version's change time (dbt's `dbt_updated_at`). It is off by default.
- `is_current = false` writes no flag column. A version is then current when its `valid_to` is NULL or equals `valid_to_current`. Use this to continue a snapshot table that dbt built.
- `valid_to_current` (dbt's `dbt_valid_to_current`) is a SQL expression written to `valid_to` on current versions, in place of NULL.
- `invalidate_hard_deletes = true` is accepted as dbt's older spelling of `hard_deletes = "invalidate"`.

**What a run does:**

```text
first run   CREATE TABLE ... AS: every row becomes its first current version
later runs  1. MERGE   close the current version of each changed key
            2. INSERT  a new current version for each key with no current version
            3. hard deletes (when not "ignore"):
               invalidate  UPDATE: close the current version of each key that left the result
               new_record  INSERT a deletion marker, then UPDATE: close the version it replaces
```

The statements run in sequence, without a transaction, because not every warehouse has multi-statement transactions. Each step is safe to repeat:

- A run that stops after step 1 leaves some keys with no current version. The next run's step 2 opens them.
- Step 2 inserts only where no current version exists, and a deletion marker is not inserted twice.
- A run over an unchanged source matches nothing and writes nothing.
- All statements in one run use one timestamp for "now", so a closed version and its successor meet exactly.

**Limits:**

- `unique_key`, `updated_at` and `check_cols` must name output columns. To key on an expression, compute it in the model SQL.
- A row with a NULL key is not snapshotted: it never matches its earlier version, so it would be inserted on every run.
- `is_deleted` is a metadata column only under `hard_deletes = "new_record"`. Under the other modes a model column with that name is ordinary data. If you leave `new_record`, keys whose current version is a deletion marker still reopen when they return.
- The target keeps the columns it was created with. A column added later is not captured, and `rocky run` reports it in the model's `notes`. Drop the target to rebuild the history with it.
- The key must be unique in the result. Databricks and Snowflake refuse the MERGE when it is not.
- `rocky plan` and `rocky emit-sql` show steady-state statements built from the compile-time column list. `rocky run` reads the column list from the target.
- A `branch` promotion and a shadow run refuse a snapshot model.
- Each statement reads the model SELECT again, so a source that changes mid-run can look different to two statements. The next run reconciles it.
- Databricks and Snowflake have not run these statements live yet. On Databricks the run timestamp is a session-time-zone `TIMESTAMP`, and a SELECT with a subquery may be refused inside the hard-delete `UPDATE`.

`rocky compile` reports an invalid config as `E049` and a risky one as `W049`. See the [diagnostic codes](/concepts/compiler/).

---

### Time Interval

Rebuild one time slice at a time instead of the whole table. Write `@start_date` and `@end_date` placeholders in the model SQL, and Rocky substitutes the bounds of each [partition](/reference/glossary/#partition) as it processes it.

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

**CLI flags.** Every flag is accepted on `rocky plan` and on the single-step `rocky run` alias. The auditable form is `rocky plan <flag>` then `rocky apply <plan-id>`.

| Flag | Effect |
|---|---|
| `--partition 2026-04-01` | Process one partition. |
| `--from 2026-03-01 --to 2026-04-01` | Process a date range. |
| `--latest` | Process the latest partition. |
| `--missing` | Discover and process missing partitions from the stored partition records. |
| `--lookback 7` | Set the lookback window. |
| `--parallel 4` | Parallelize partition processing. |

---

## DAG Resolution

You never write an execution order. Rocky derives it from the `depends_on` declarations and the table references in each model's SQL, and runs the models in [topological order](/reference/glossary/#topological-order), so every upstream finishes before anything that reads it starts.

```
   stg_orders ─────┐
                   ▼
   stg_customers ─► fct_orders ─────► mart_revenue
                                          ▲
   dim_products ─────────────────────────┘

   depth 0: stg_orders, stg_customers, dim_products   (no dependencies)
   depth 1: fct_orders
   depth 2: mart_revenue
```

Models with no dependencies run first. Models at the same depth run concurrently, up to the limit `rocky run --parallel <N>` sets (default 4). A warehouse that cannot run statements concurrently, such as DuckDB, runs them one at a time. So does a depth that holds a `content_addressed` or `time_interval` model.

`rocky validate` checks the DAG for cycles. A cycle — model A depends on B, B depends on A — fails validation with an error naming the loop:

```
!!  dag_validation — cycle detected: fct_orders -> dim_customers -> fct_orders
```
