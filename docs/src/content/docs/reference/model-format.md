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
| `drop_existing_kind` | `"table"` or `"view"` | No | Standing permission to drop a target of this existing kind when switching between `full_refresh` and `view`. DuckDB only today. |
| `depends_on` | list of strings | No | Names of upstream models that must run before this one. Defaults to `[]`. |
| `group` | string | No | Name of a [config group](#config-groups) (`models/groups/<name>.toml`) this model opts into for shared routing and materialization. |
| `access` | string | No | `private`, `protected` (default) or `public`. Who may reference the model. See [Model governance](/concepts/model-governance/). |
| `access_group` | string | No | Ownership group for access checks. Falls back to `group`. Inherits no config. See [Model governance](/concepts/model-governance/#ownership-groups-and-owners). |
| `retention` | string | No | Data retention policy for this model. Grammar `^\d+[dy]$` — e.g. `"90d"` or `"1y"`. See [Retention](#retention). |

`drop_existing_kind` applies only when a `full_refresh` model finds a view, or a `view` model finds a table. Rocky checks the existing kind before using the permission. On DuckDB, the DROP and CREATE run in one transaction. Rocky refuses a `full_refresh` or `view` model carrying this key on every other adapter. The key is a standing permission on the model, not a one-time approval. Rocky keeps no ownership record for the old object; confirm the target belongs to this model before setting the key.

**`[args]`** -- Placeholder values for a config group's `schema_template` (only meaningful when the model declares a `group`):

| Key pattern | Value type | Description |
|---|---|---|
| `<placeholder>` | string | Fills a `{placeholder}` in the group's `schema_template` (e.g. `region = "emea"` resolves `mart_{region}` to `mart_emea`). Ignored when the model declares no `group`. See [Config groups](#config-groups). |

**`[strategy]`** -- Materialization configuration:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | string | `"full_refresh"` | Materialization type. One of `"full_refresh"`, `"merge"`, `"time_interval"`, `"view"`, `"materialized_view"`, `"dynamic_table"`, `"delete_insert"`, `"microbatch"`, `"content_addressed"`, `"ephemeral"` (see [Ephemeral](#ephemeral)). `"incremental"` is refused on a transformation model (`E037`, see [Incremental](#incremental)). |
| `type` | string | `"full_refresh"` | Materialization type. One of `"full_refresh"`, `"incremental"`, `"merge"`, `"time_interval"`, `"view"`, `"materialized_view"`, `"dynamic_table"`, `"delete_insert"`, `"microbatch"`, `"content_addressed"`. `"incremental"` needs a watermark column (`E037` without one, see [Incremental](#incremental)). `"ephemeral"` is refused outright (`E038`, see [Ephemeral](#ephemeral)). |
| `type` | string | `"full_refresh"` | Materialization type. One of `"full_refresh"`, `"merge"`, `"time_interval"`, `"view"`, `"materialized_view"`, `"dynamic_table"`, `"delete_insert"`, `"microbatch"`, `"content_addressed"`, `"snapshot"` (see [Snapshot](#snapshot)). Two are refused: `"incremental"` on a transformation model (`E037`, see [Incremental](#incremental)), and `"ephemeral"` outright (`E038`, see [Ephemeral](#ephemeral)). |
| `timestamp_column` | string | | Replication watermark column. Required for transformation `microbatch`; it names the output partition column. |
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

A group file may carry `schema_template`, `strategy`, `tags`, `governance`, `enforce`, and `[owner]` (`name`, `email`; see [Model governance](/concepts/model-governance/#ownership-groups-and-owners)). Rocky rejects an unrecognized key at load, so a typo surfaces immediately.

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

`expression` is bounded. Rocky parses it under the target dialect and refuses anything that is not one boolean expression over the model's own columns: a subquery, a qualified function name (`schema.fn(...)`), or a function outside its allowlist of pure scalar functions is refused when the test SQL is generated, before anything runs. Comparisons, `CASE`, `CAST`, and functions such as `coalesce`, `length`, `lower` and `date_trunc` pass; anything that can read a file, a secret, session state or a remote endpoint does not. The refusal names the function.

`filter` is bounded by the same gate. A subquery, a qualified function name or an off-allowlist function is refused when the test SQL is generated. The refusal names the field and the table. See [Per-assertion `filter`](/concepts/data-quality-checks/#per-assertion-filter) for the full rule, including the extra rule for a key expression.

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
type = "merge"
unique_key = ["order_id"]

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

`rocky run` records a refused model as a failed table and leaves its existing table alone. By default, Rocky also withholds every model that depends on it. Set `contain_failures = true` under `[resilience]` to widen that hold. See [`[resilience]`](/reference/configuration/#resilience).

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

- The CTE is named `__rocky_ephemeral__<model>`. If that name is already used in the statement, Rocky adds `_2`, `_3`, and so on.
- A reference with no alias keeps the model name as its alias. So `eph_paid_orders.amount` still works.
- The CTE goes in front of any `WITH` clause the consumer already has.
- An ephemeral model that reads another ephemeral model works. Each one becomes one CTE, in dependency order, once per consumer.
- Only a bare model name is a reference. A CTE of the same name in the consumer wins, and the model is not inlined.
- The rewrite works on the parsed SQL, not on the text. The consumer's executed SQL loses its comments and original spacing.

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

The model SQL must use both `@start_date` and `@end_date` to bound each partition. Missing either is a compile error (`E024`). A placeholder that is only in a comment, a string or the `SELECT` list counts as missing. So does one that does not bound a column in an `AND`-joined comparison, such as `ts >= @start_date OR 1 = 1` or `NOT (ts >= @start_date)`. So does a filter that misses some emitted rows: an unfiltered `UNION ALL` branch, a filter in a CTE the output never reads, or a filter inside a scalar subquery in the `SELECT` list. See [Time interval](/concepts/time-interval/#sql-placeholders). Each run replaces its selected partitions instead of appending the full result again.

**SQL** (`models/fct_hourly_events.sql`):

```sql
SELECT event_at, event_type
FROM raw_catalog.events.page_views
WHERE event_at >= @start_date
  AND event_at < @end_date
```

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

The statements run one after the other, without a transaction. Not every warehouse has multi-statement transactions. Instead, each step is safe to repeat:

- A run that stops after step 1 leaves some keys with no current version. The next run's step 2 opens them.
- Step 2 only inserts where no current version exists.
- A deletion marker is not inserted twice.
- A run over an unchanged source matches nothing, so it writes nothing.

All statements in one run use one timestamp for "now". A version closed at step 1 and its successor from step 2 meet exactly.

**Limits:**

- `unique_key`, `updated_at` and `check_cols` must name output columns. To key on an expression, compute it in the model SQL and name it.
- A row whose key is NULL is not snapshotted. A NULL key never matches its earlier version, so it would be inserted again on every run.
- `is_deleted` is a metadata column only under `hard_deletes = "new_record"`. Under the other modes a model column with that name is ordinary data. If you leave `new_record`, keys whose current version is a deletion marker still reopen when they return.
- The target keeps the columns it was created with. A column added to the model later is not captured, and `rocky run` reports it in the model's `notes`. Drop the target to rebuild the history with the new column.
- The key must be unique in the model's result. Databricks and Snowflake refuse the MERGE when it is not.
- `rocky plan` and `rocky emit-sql` show the steady-state statements, built from the compile-time column list. `rocky run` reads the column list from the target.
- A `branch` promotion and a shadow run refuse a snapshot model, like the other strategies that build on rows the target already holds.
- Each statement reads the model SELECT again, so a source that changes during a run can be seen differently by two statements. The next run reconciles it.
- Databricks and Snowflake have not run these statements live yet. On Databricks, the run timestamp is a session-time-zone `TIMESTAMP`, and a model whose SELECT contains a subquery may be refused inside the hard-delete `UPDATE`.

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
