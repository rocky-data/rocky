---
title: Model Format
description: SQL and TOML model file specification
sidebar:
  order: 4
---

A Rocky model is one SQL query plus the configuration that says how to materialize it, what it depends on, and where the output goes. One model produces one table or view.

Rocky reads two model formats: **sidecar** (recommended) and **inline** (legacy).

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

The split matters because it keeps the `.sql` file readable by anything that reads SQL. You can open it in a query editor, run it by hand, or hand it to a colleague who has never heard of Rocky.

### SQL File

The `.sql` file holds a plain SQL query. No templating, no Jinja, no special markers.

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

Overrule the `--skip-unchanged` gate for this one model. By default the gate is cautious. A model qualifies to be skipped only when a static scan finds its SQL [deterministic](/reference/glossary/#deterministic): same inputs, same output, every time. It must also use a plain materialization strategy. This block lets the model's owner say otherwise. Omit it and the automatic rules apply.

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

Sidecar files and `models/_defaults.toml` get the same `${VAR}` and `${VAR:-default}` substitution as `rocky.toml`. An orchestrator can therefore set a model's `[target]` through the subprocess environment, with no templating in the sidecar. See [Environment variables](/reference/configuration/#environment-variables) for the syntax and a sidecar example, and [`examples/playground/pocs/00-foundations/07-config-layering/`](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/00-foundations/07-config-layering) for a runnable three-layer example.

### `@var()` run variables

Parameterize a model per run. Write `@var(name)` or `@var(name, default)` in the model body. Bind it with `rocky run --var name=value`, which you can repeat. Rocky substitutes the value into the SQL before it reaches the warehouse:

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

`@var(region)` has no default, so you must supply it. `@var(status, shipped)` falls back to `shipped` when you omit `--var status=…`.

The substitution is **textual**. Rocky replaces the marker with your string verbatim, so you own the quoting and the casting around it. The example quotes the marker because the value is a string literal. Rocky validates only the variable *name*, as a SQL identifier.

`@var()` and `${ENV}` solve different problems and run at different times:

```
   ${ENV}                             @var(name)
   ──────                             ──────────
   resolves while Rocky parses        resolves at compile/render time,
   rocky.toml and the sidecars,       after the model is read
   before any model is read
                                      stays visible in the model source
   sets config values                 sets a run's logical inputs
   (target catalog, credentials)      (a region, a status)
```

A `@var(name)` with no `--var` binding and no inline default is a **compile error** naming the missing variable. A forgotten value fails before anything runs. `rocky import-dbt` maps dbt's `{{ var('name') }}` and `{{ var('name', default) }}` onto these markers.

### Config groups

Write the shared settings once when a fan-out of models routes and materializes the same way. A **config group** lives in `models/groups/<name>.toml`, where the file stem is the group name, and supplies a `schema_template` and a `strategy`:

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

Three layers can set the same field. The nearest one to the model wins:

```
   models/fct_orders.toml    ◄── highest: the model's own sidecar
            ▲
   models/groups/<name>.toml     the group it opted into
            ▲
   models/_defaults.toml     ◄── lowest: directory defaults
```

So a model can pin its own `schema` or `strategy` and override the group, and the group in turn overrides the directory defaults. A `group` naming no definition fails the load with a clear error. So does a `schema_template` placeholder the model does not supply. Rocky refuses rather than routing the model somewhere wrong.

One combination is rejected. A model that pins its own `schema` bypasses the group's template completely, so it must **not** also supply `[args]`. Those args could only fill a template nothing now reads. Rocky fails the load rather than let the args sit there doing nothing and masking a routing mistake. Pin a schema *or* supply args, never both.

#### Enforced groups

Make the group's fields binding instead of overridable. Set `enforce = true`. A member model that pins a field the group owns, its target `schema` or its `strategy`, then fails the load. It cannot quietly route or materialize itself differently from the rest of the group:

```toml
# models/groups/regulated.toml
enforce = true
schema_template = "mart_{region}"

[strategy]
type = "merge"
unique_key = ["id"]
```

Enforcement is opt-in. Without `enforce`, a group stays a set of overridable defaults. A model under an enforced group still supplies its own `[args]`, and any field the group leaves unset such as `target.catalog`. It simply cannot override what the group owns. Use enforcement when a set of models must share routing and materialization as a governance guarantee.

The model loader does not recurse into subdirectories, so it never mistakes `models/groups/` for model files.

#### Group tags

Apply a governance attribute once and have it land on the whole fan-out. A group can declare a `[tags]` block, and every member model inherits it as a baseline:

```toml
# models/groups/finance.toml
schema_template = "mart_{region}"

[tags]
domain = "finance"
tier = "gold"
```

A model's own `[tags]` override the group key by key, without dropping the rest. One model can set `tier = "silver"` and still inherit `domain = "finance"`. See [`[tags]`](#tags) for how the resolved tags surface on `models_detail[].tags` and project onto Dagster assets.

A group file may carry `schema_template`, `strategy`, `tags`, `governance`, and `enforce`. Rocky rejects an unrecognized key at load, so a typo surfaces immediately.

### `[classification]`

Label a column as sensitive so the [masking policy](/reference/glossary/#masking-policy) can act on it. Keys are column names; values are free-form classification strings. Rocky resolves each value against `[mask]` and `[mask.<env>]` in `rocky.toml` to pick a strategy. It then applies both the column tag and the mask through the governance adapter, after a successful DAG run.

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

Describe the model **as a whole**: its `domain`, its `tier`, its `owner`, or anything else your governance model needs. This differs from `[classification]`, which is keyed by column and drives masking.

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

`rocky compile --output json` reports the resolved tags as `models_detail[].tags`. The `dagster-rocky` integration projects them onto the derived asset's Dagster tags, so one attribute drives both Rocky's view of the model and the orchestrator's. A model in a config group inherits that group's tags too — see [Group tags](#group-tags).

`[tags]` never touches the warehouse. To put a tag on the warehouse object itself, use [`[governance.tags]`](#governancetags).

### `[governance.tags]`

Put a tag on the warehouse object itself. After the model materializes, Rocky writes these as Unity Catalog tags on its **own target table or view**. The [DDL](/reference/glossary/#ddl-data-definition-language) matches the shape: `ALTER VIEW … SET TAGS (…)` for a view-format model, `ALTER TABLE … SET TAGS (…)` otherwise.

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

Add a stable key column without writing the hash expression yourself. Rocky injects a deterministic hash over the columns you list into the model's SELECT.

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

Assert a property of the model's output. Each `[[tests]]` block is one assertion, and it runs against the target table. You write TOML, not a SQL macro: Rocky generates the assertion SQL for whichever dialect the run targets.

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

Resolved references are appended to the model's `[[tests]]` at load. A `[[use_test]]` block uses `deny_unknown_fields`, so a mistyped key (`colum =`, `filer =`) is rejected at load rather than silently applying the test with the wrong binding.

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

```toml
# models/fct_orders.toml
name = "fct_orders"

[columns.order_id]
description = "Unique order identifier"

[columns.amount]
description = "Order total in USD"
```

`rocky catalog --output json` reports each description as the asset's `CatalogColumn.description`. Rocky attaches a description only when `<name>` matches a column the model actually projects. It drops a description for a column the SELECT does not produce, silently, so keep these keys in step with your output columns. The `rocky docs` HTML catalog carries no per-column detail, because it has no warehouse connection with which to read the column list. Descriptions reach consumers through `rocky catalog`, not the generated HTML.

The singular `[columns.<name>]` table documents columns, and is distinct from the plural `[[columns]]` array used to declare a contract's column schema. The two look similar but do different jobs.

### Retention

Declare how long this model's data should be kept. The top-level `retention` key on the sidecar carries the policy, and Rocky parses it at load time into a typed `RetentionPolicy { duration_days: u32 }`.

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

Rocky rejects a malformed value when it parses the sidecar: `"abc"`, `"90"`, `"-3d"`, `"1.5d"`, a leading sign, an exponent. The `ModelError::InvalidRetention` diagnostic names what it saw. Inspect the resolved policies with [`rocky retention-status`](/reference/cli/#rocky-retention-status).

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

Appends only the rows that arrived since last time. Rocky stores a [watermark](/reference/glossary/#watermark) — the timestamp of the newest row it has already loaded — and reads past it on the next run. Use this for a large fact table where a full refresh is too slow.

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

[Upserts](/reference/glossary/#upsert) on a unique key: Rocky updates a row whose key already exists and inserts one whose key does not. Use it for a [slowly changing dimension](/reference/glossary/#scd-slowly-changing-dimension), or for any table that gets late-arriving updates.

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

An [ephemeral](/reference/glossary/#ephemeral) model never becomes a table. Rocky inlines it as a [CTE](/reference/glossary/#cte-common-table-expression) — a named subquery in a `WITH` clause — inside every model that reads it. Use it for a small intermediate step you do not want to keep.

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

Deletes the rows in a [partition](/reference/glossary/#partition) — a slice of the table identified by a column value — then inserts fresh ones. It costs less than `merge` when the partition key already identifies exactly the rows you are rewriting.

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

An alias for `time_interval` that defaults to `hour` granularity. The name matches dbt's for partition-based incremental processing.

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

You never write an execution order. Rocky derives it from the `depends_on` declarations and runs the models in [topological order](/reference/glossary/#topological-order), so every upstream finishes before anything that reads it starts.

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
