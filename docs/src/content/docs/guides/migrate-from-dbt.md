---title: Migrating from dbt
description: How rocky import-dbt converts a dbt Core project into a Rocky project, what it translates, what it refuses, and what you fix by hand.
sidebar:
  order: 7
---

`rocky import-dbt` converts a dbt Core project into a Rocky project. Point it at a dbt project directory. It writes a body file and a `.toml` sidecar per model, a `rocky.toml`, and a `MIGRATION-NOTES.md` that lists everything it could not translate. Flags, the `materialized` map and the JSON output: [`rocky import-dbt`](/reference/commands/development/#rocky-import-dbt).

```
   my-dbt-project/            rocky import-dbt        imported/
   ┌─────────────────┐        ┌────────────────┐      ┌──────────────────┐
   │ dbt_project.yml │        │ manifest fast  │      │ rocky.toml       │
   │ models/*.sql    │───────►│ path when one  │─────►│ models/*.sql     │
   │ models/*.yml    │        │ exists, else   │      │ models/*.toml    │
   │ profiles.yml    │        │ the regex      │      │ seeds/           │
   │ target/         │        │ extractor      │      │ MIGRATION-       │
   │   manifest.json │        └────────────────┘      │   NOTES.md       │
   └─────────────────┘                                └──────────────────┘
```

Rocky has no Jinja runtime. The importer reports or refuses each construct it cannot translate instead of guessing.

| Step | Command | Section |
|---|---|---|
| Import | `rocky import-dbt` | [1](#1-import-the-dbt-project) |
| Review and fix Jinja | read the sidecars | [2](#2-review-the-imported-models), [3](#3-handle-unsupported-jinja) |
| Configure the warehouse | `rocky.toml` | [4](#4-configure-rockytoml) |
| Compile and test | `rocky compile`, `rocky test` | [5](#5-compile-the-imported-models), [6](#6-run-tests-locally) |
| Validate and compare | `rocky validate-migration`, `rocky plan` | [7](#7-validate-the-migration), [8](#8-verify-the-output-before-cutover) |
| Tests and contracts | `[[tests]]`, `.contract.toml` | [9](#9-convert-dbt-tests-to-rocky-tests-and-contracts) |
| Cut over in stages | leaf models first | [11](#11-move-a-large-project-in-stages) |

## Prerequisites

- Rocky installed. See [Installation](/getting-started/installation/).
- A dbt project with models in a `models/` directory. You do not need dbt installed.
- Warehouse credentials. See [Connect to a warehouse](/guides/connect-a-warehouse/).

## Walkthrough: end-to-end against a tiny dbt project

This runs the path against the POC at [`examples/playground/pocs/06-developer-experience/03-import-dbt-validate/`](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/06-developer-experience/03-import-dbt-validate). The dbt project has two models (`stg_orders`, a view over `{{ source('raw', 'orders') }}`, and `fct_revenue`, a table over `{{ ref('stg_orders') }}`), one source and a `schema.yml` of generic tests. It has no `profiles.yml` and no `target/manifest.json`, so it uses the regex importer and needs no credentials.

```bash
rocky import-dbt --dbt-project dbt_project --output-dir imported --no-manifest --overwrite
```

```
imported/
├── MIGRATION-NOTES.md    # counts, skipped tests and macros, env vars, known limitations
├── rocky.toml            # DuckDB stub: no profiles.yml was found
└── models/
    ├── _defaults.toml    # directory-level [target] defaults
    ├── stg_orders.sql    # Jinja resolved to bare references
    ├── stg_orders.toml
    ├── fct_revenue.sql
    └── fct_revenue.toml
```

`stg_orders.toml`: `view` maps to Rocky's `view` strategy, the model `description` becomes `intent`, and the `schema.yml` generic tests become `[[tests]]` blocks:

```toml
name = "stg_orders"
intent = "Staged orders, one row per order, with a normalised status."

[strategy]
type = "view"

[target]
catalog = "warehouse"
schema = "main"
table = "stg_orders"

[[sources]]
catalog = "warehouse"
schema = "raw"
table = "orders"

[[tests]]
type = "unique"
column = "order_id"

[[tests]]
type = "accepted_values"
values = ["pending", "completed", "shipped"]
column = "status"
```

Read `MIGRATION-NOTES.md` first. Then compile and validate:

```bash
cd imported
rocky compile --models models
rocky -c rocky.toml validate
```

A clean `rocky compile` plus a clean `rocky validate` is the success criterion. The POC's `run.sh` then runs `rocky validate-migration`. To run `rocky plan` and `rocky apply <plan-id>`, load the source data first: the importer translates `{{ source('raw', 'orders') }}` to `FROM raw.orders` and neither creates nor populates that table.

### What translates cleanly today, and what doesn't

Each `{{ config(...) }}` key maps onto a sidecar field:

| dbt `{{ config(...) }}` | Rocky sidecar |
|---|---|
| `materialized='table' \| 'incremental' \| 'view'` | The `[strategy]` block. |
| `materialized='ephemeral'` | `type = "ephemeral"`. Rocky inlines the model into each consumer as a CTE. A consumer imported from the manifest's compiled SQL already carries dbt's `__dbt__cte__<model>` CTE and runs as-is. |
| `unique_key=...` | The `merge` strategy, with `unique_key` as an array. |
| `alias='name'` | `[target].table`, so the data lands in the aliased table. |
| `materialized='microbatch'` | `merge` by default, or `time_interval`. Choose with `--microbatch-as <merge\|time_interval>`. `MIGRATION-NOTES.md` records the choice. |
| `merge_update_columns=[...]` | The `merge` strategy's `update_columns`. |

The rest of the project translates like this:

| dbt | Rocky |
|---|---|
| `{{ ref('model') }}` | Bare table reference + sidecar `depends_on`. |
| `{{ source('s', 't') }}` | Fully qualified reference + sidecar `[[sources]]`. |
| `{{ var('name') }}`, `{{ var('name', default) }}` | An `@var(name)` or `@var(name, default)` marker. Resolve it with `rocky run --var name=value` (see [Handle unsupported Jinja](#3-handle-unsupported-jinja)). |
| Tags (node and folder level) | Sidecar `[tags]` block (`<tag> = "true"`). |
| `{{ this }}` | The model's own `catalog.schema.table`. |
| Model governance | [Rocky model governance](/concepts/model-governance/). See [Access, groups and versions](#access-groups-and-versions). |
| Generic tests (`unique`, `not_null`, `accepted_values`, `relationships`) | `[[tests]]` blocks, including `severity:` (a `warn` becomes a Rocky warning) and `where:` (a row filter). See [Generic test mapping](#generic-test-mapping). |
| `dbt_utils.unique_combination_of_columns` | A `composite` uniqueness `[[tests]]` block over the same columns. |
| `dbt_project.yml` | The project name and seeds path. |
| `<dbt_project>/seeds/` | Copied verbatim into `<out>/seeds/`. |
| `profiles.yml` adapter type | An `[adapter]` block (DuckDB, Databricks, Snowflake or BigQuery), or a DuckDB stub when the type is absent or unrecognised. The parser resolves YAML anchors and aliases and `{{ env_var('VAR', 'default') }}` in `type`. |

The importer does not translate the items below. It refuses some models and reports other expressions in `MIGRATION-NOTES.md`, marked in the emitted SQL.

| Construct | What happens |
|---|---|
| Tests with no native equivalent (other `dbt_utils.*` or `dbt_expectations.*`, project-defined generics, model-level tests) | An `UnsupportedTest` warning per occurrence, no stub. Rewrite as a Rocky `expression` test or a quality-pipeline check. |
| Singular tests in `tests/` | Rewrite them yourself. |
| Macros and `dbt_packages/` | No macro body expands. A custom macro that emits SQL (`{{ generate_schema_name() }}`, a dynamic `UNION ALL`) is a failed model, with the macro name in the reason. |
| Python models (`.py`) | Rewrite them yourself. |
| Incremental models on the raw path | The standard `{% if is_incremental() %} WHERE <col> > (SELECT MAX(<col>) FROM {{ this }}) {% endif %}` block becomes `@incremental_filter`. Another `is_incremental()` use stays as a commented TODO with no watermark, and `rocky compile` refuses the model (`E037`). The importer refuses an incremental model with no `is_incremental()` block, a non-incremental model that calls it, and versioned models. Import the `dbt compile --full-refresh` artifact pair instead. |
| Jinja control flow (`{% if %}`, `{% for %}`, `{% macro %}`, `{% call %}`) on the no-manifest path | Refused. Import with the manifest, or rewrite. `{{ var() }}` still converts outside a config expression. |
| Unmapped `materialized` (`dynamic_table`, `seed`) | `full_refresh`, listed in `MIGRATION-NOTES.md`. `materialized_view` maps to Rocky's `materialized_view` strategy. |
| Profile types not mapped (Postgres, Redshift, others) | A DuckDB stub. Replace the `[adapter]` block ([PostgreSQL](/reference/adapters/postgres/), [Redshift](/reference/adapters/redshift/), [SQL Server](/reference/adapters/sqlserver/)) or pass `--target-adapter <kind>`. |
| Snapshots | [`type = "snapshot"` models](/reference/model-format/#snapshot), from `{% snapshot %}` blocks or YAML snapshots (dbt 1.9+). See below. |
| MetricFlow metrics and semantic models | Detected and counted. Each raises a `DroppedConstruct` warning and increments `constructs_dropped`. |
| Exposures | Each becomes a [downstream consumer](/concepts/downstream-consumers/) at `consumers/<name>.toml`, carrying `type`, `owner`, `url` and `description`. A consumer keeps a dependency only when it names an imported model. A lossy exposure raises `DroppedConstruct`. `consumers_imported` counts the files. |
| Model contracts (`contract: {enforced: true}`) | `<model>.contract.toml`. Each declared column becomes a `[[columns]]` entry and is listed in `[rules] required`. See [Column-level contracts](#column-level-contracts). |

**Snapshot import.** The importer reports a failure for a custom strategy macro, a missing or expression `unique_key`, or Jinja control flow in the body. Without a manifest, a project-level `snapshots:` block in `dbt_project.yml` also fails the import, and the target schema is the configured one: check it before the first run, because `generate_schema_name` may have built the table elsewhere.

**Contract types.** `data_type` maps to a Rocky type. The importer reads the dbt adapter from the manifest because names differ by warehouse: `float` is `Float64` on BigQuery, PostgreSQL and Redshift and `Float32` on DuckDB, Databricks and Spark; a bare `timestamp` is `Timestamp` on those six. On Snowflake, the floating-point names, a bare `timestamp` and the integer names stay unchecked, as do `float` and `timestamp` when the manifest names no adapter. A `not_null` or `primary_key` constraint is not checked, because `nullable = false` would refuse a valid model (`E012`): the file marks each such column with a comment, so add a `not_null` test. A column type Rocky has no name for stays required and unchecked. The constraints `unique`, `check`, `foreign_key` and `custom` have no contract check. A model with one raises a warning and increments `contracts_dropped`.

:::caution[Run `dbt compile --full-refresh` first]
Run `dbt compile --full-refresh` before import, with any required `--vars`. Keep `manifest.json` and `run_results.json` in the same target directory. Every dbt incremental model needs matching invocation IDs, `args.full_refresh = true`, `compiled_code`, and a successful result for its `unique_id`. dbt 2 writes no per-model results for `compile`, so Rocky accepts the `compiled_code` of the same invocation as the evidence. `dbt run --full-refresh` also works, but it rebuilds the tables.

Compile without `--select`, or include each model you import. Remove any effective `full_refresh=false` config, because it overrides the command flag. A custom macro can hide the `is_incremental()` call, and Rocky's first run then fails or omits old rows.
:::

### Snapshots: use the model strategy

Rocky has two snapshot (SCD Type 2) forms. A dbt snapshot maps to the model form (`[strategy] type = "snapshot"` in a model sidecar, run by `rocky run`). The importer emits it, and you should use it when you port one by hand. The other form, `[pipeline.<name>] type = "snapshot"`, runs with `rocky snapshot` and reads one source table. The [Snapshot](/reference/model-format/#snapshot) reference compares the two.

The model keeps dbt's column names (`dbt_valid_from`, `dbt_valid_to`, `dbt_scd_id`, `dbt_updated_at`, `dbt_is_deleted`) and writes no `is_current` column, so `rocky run` can continue a table dbt built. Two differences: new versions get Rocky's `dbt_scd_id` hash, and `dbt_is_deleted` is a BOOLEAN. To continue a dbt-built table, set `is_current = false`. Without it, Rocky looks for an `is_current` column dbt never wrote, and the run fails. Add `dbt_is_deleted` when you use `hard_deletes = "new_record"`.

## 1. Import the dbt Project

```bash
rocky import-dbt --dbt-project ./my-dbt-project --output-dir ./rocky-models
```

It scans `my-dbt-project/models/` for `.sql` files and writes `<model>.sql` and `<model>.toml` per model into `./rocky-models/`. Add the global `-o json` flag when a script reads the result. Warnings carry a `category` and `suggestion`; failures carry a `reason`.

### Manifest Fast Path

If `target/manifest.json` exists, Rocky uses it without being asked. The import is more accurate, because dbt already resolved the Jinja. Two flags override that:

- `--manifest path/to/manifest.json`: use this manifest.
- `--no-manifest`: ignore any manifest and use the regex-based import.

## 2. Review the Imported Models

Read each generated pair of files. Check these points:

- `{{ config(materialized='incremental', unique_key='id') }}` with no `incremental_strategy` becomes a `merge` strategy keyed on `unique_key`. The `materialized` map and the `full_refresh` fallbacks (no `unique_key`, `append`, an unknown strategy) are in the [`rocky import-dbt` reference](/reference/commands/development/#how-materialized-maps).
- The `[[sources]]` block comes from the `sources.yml` entry. Without a match, the importer warns and emits no `[[sources]]` block.

## 3. Handle Unsupported Jinja

The manifest path uses SQL compiled by dbt. The raw path converts a limited set of Jinja expressions and refuses statement tags.

:::tip[`{{ var() }}` and per-model routing]
`{{ var('name') }}` converts to an `@var(name)` marker. Supply the value with `rocky run --var name=value`. A marker with neither a `--var` binding nor an inline default (`@var(name, fallback)`) fails to compile.

Per-model `catalog`, `schema` and `table` routing from an orchestrator (`{{ target.* }}` in dbt) is different. Rocky's config has three layers: the pipeline `rocky.toml`, `models/_defaults.toml` and the per-model sidecar. Every layer accepts `${VAR}` and `${VAR:-default}`, resolved at config-load time. See [`examples/playground/pocs/00-foundations/07-config-layering/`](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/00-foundations/07-config-layering).
:::

For each refused model, read the error and rewrite the SQL. Many macros do something Rocky expresses in config: incremental logic, schema naming, environment branching. Move shared macro logic into CTEs or separate models. Write a dynamic `UNION ALL` out as a CTE.

### `generate_surrogate_key`

The importer does not convert `{{ dbt_utils.generate_surrogate_key([...]) }}`. It raises an `UnsupportedMacro` warning and leaves a `/* TODO: unsupported macro */` marker. Rewrite it as a [surrogate key](/reference/glossary/#surrogate-key) in the sidecar. `rocky run` injects the hash column at materialization time:

```toml
# models/fct_orders.toml
[[surrogate_key]]
name = "order_key"
columns = ["order_id", "customer_id"]
```

Drop the `{{ ... }}` expression from the SQL. The hash matches dbt-utils on the same warehouse: each input cast to text, NULL coalesced to the `_dbt_utils_surrogate_key_null_` sentinel, values joined with `-`, then MD5. It is dialect-correct on DuckDB, Databricks, Snowflake and BigQuery.

## 4. Configure rocky.toml

Rocky uses **named adapters** and **named pipelines**. Adapter fields for every warehouse: [Connect to a warehouse](/guides/connect-a-warehouse/).

A replication pipeline needs a **data** adapter (reads and writes tables) and, for a source such as Fivetran, a **discovery** adapter that lists the source schemas. If you ran dbt on Databricks fed by Fivetran, set `[adapter.prod]` and `[adapter.fivetran]` (`kind = "discovery"`) as in [Connect to a warehouse](/guides/connect-a-warehouse/) and the [Fivetran adapter](/reference/adapters/fivetran/), then:

```toml
[pipeline.bronze]
type = "replication"
strategy = "incremental"
timestamp_column = "_fivetran_synced"

[pipeline.bronze.source]
adapter = "prod"
catalog = "raw_catalog"

[pipeline.bronze.source.discovery]
adapter = "fivetran"

[pipeline.bronze.source.schema_pattern]
prefix = ""
separator = "__"
components = ["source"]

[pipeline.bronze.target]
adapter = "prod"
catalog_template = "warehouse"
schema_template = "staging"

[pipeline.bronze.execution]
concurrency = 8

[state]
backend = "local"
```

### Mapping dbt config to Rocky

| dbt (`profiles.yml` / `dbt_project.yml`) | Rocky (`rocky.toml`) |
|---|---|
| `host`, `http_path`, `token` | `[adapter.prod] host`, `http_path`, `token` |
| `catalog` | `[pipeline.<name>.target] catalog_template` |
| `schema` | `[pipeline.<name>.target] schema_template` |
| `threads` | `[pipeline.<name>.execution] concurrency` |

### Folder-level config (`+materialized`, `+schema`)

dbt's `dbt_project.yml` sets per-directory defaults such as `marts: +materialized: table` and `+schema: marts`. Rocky's equivalent is a **config group** in `models/groups/<name>.toml`. Each member model opts in with `group = "<name>"`.

| dbt folder-level | Rocky config group |
|---|---|
| `+materialized: table` (or `incremental`, etc.) | `[strategy]` block |
| `+schema: marts` | `schema_template = "marts"` (a literal is a template with no placeholders) |

```toml
# models/groups/daily_marts.toml
schema_template = "mart_{region}"

[strategy]
type = "merge"
unique_key = ["id"]

[tags]
domain = "finance"
```

```toml
# models/fct_orders.toml
group = "daily_marts"

[args]
region = "emea"
```

A dbt folder default applies to every model in the directory. A group applies only to models that name it. Precedence: per-model sidecar over group over `models/_defaults.toml`. A group with `enforce = true` makes a member that pins a field the group controls (its target schema or strategy) fail to load.

### Access, groups and versions

dbt's [model governance](https://docs.getdbt.com/docs/mesh/govern/about-model-governance) maps onto [Rocky model governance](/concepts/model-governance/). The importer carries each part over:

| dbt | Rocky |
|---|---|
| `access: private \| protected \| public` | `access = "..."` in the model sidecar. |
| `group: finance` on a model | `access_group = "finance"` in the model sidecar. |
| `groups:` entry with `owner: {name, email}` | `models/groups/finance.toml` with an `[owner]` table. |
| `versions:` with `v: 1`, `v: 2` | Models `orders_v1` and `orders_v2`, plus the version declaration `models/orders.toml`. |
| `latest_version: 2` | `latest_version = 2` in the declaration. `FROM orders` reads the latest version through a view. |
| `deprecation_date` on a version | `deprecation_date = "YYYY-MM-DD"` on the version. A reader gets `W048`. |
| `{{ ref('orders', v=1) }}` | `orders_v1`, a pinned version. |
| `{{ ref('orders') }}` to a versioned model | `orders`, the latest version. |

- As in dbt, only `public` models may be read from another project. Rocky checks this in `rocky publish-ir` and in the consumer's `[[sources]]`.
- Rocky versions are whole numbers. Rename a dbt version such as `1.5` first.
- When dbt's latest version already materializes to the unversioned name (an `alias`), the importer sets `latest_alias = false`, so the alias view does not collide with that table.
- With a manifest, dbt has resolved every `access` and `group`, including `+access` and `+group` folder defaults. Without one, the importer reads them only from model YAML or `config:`. Set folder defaults on the models after import.

## 5. Compile the Imported Models

```bash
rocky compile --models ./rocky-models
```

The compiler resolves `depends_on` into a DAG, type-checks columns across models, and reports type mismatches, contract violations and missing dependencies.

A bare table reference that matches no model is **not** a compile error. Rocky treats it as an external reference: it appears in lineage and creates no DAG dependency. See [Read dbt Package Tables](/guides/using-dbt-packages/). The diagnostics you will see:

- **Missing `depends_on`**: a dependency implicit in dbt, such as a `{{ ref() }}` inside a macro. Add it to the model's TOML.
- **Type mismatches** (`E011`): a column used in a context its inferred type does not fit.
- **Contract violations** (`E010`-`E013`): a missing required column, wrong type, nullability violation or removed protected column. See [Section 9](#9-convert-dbt-tests-to-rocky-tests-and-contracts).

## 6. Run Tests Locally

```bash
rocky test --models ./rocky-models
```

`rocky test` runs each model's SQL on DuckDB in dependency order. It catches syntax and runtime errors and needs no warehouse.

## 7. Validate the Migration

```bash
rocky validate-migration --dbt-project ~/my-dbt-project
```

It compiles both projects and compares schemas and column types. It can also compare sample data.

## 8. Verify the Output Before Cutover

Run both projects and compare their output before you move production traffic.

`rocky plan --filter tenant=acme` prints the SQL Rocky will generate per model. Compare it with `dbt compile`. `rocky emit-sql --models ./rocky-models` gives the same comparison with no warehouse: it renders compiled SQL for every transformation model in the shape `dbt compile` writes to `target/`. See [No lock-in](/guides/no-lock-in/).

### Run on a test catalog

Add a second pipeline. Reuse the `prod` and `fivetran` adapters and change only these keys:

```toml
[pipeline.bronze_test]
type = "replication"
strategy = "full_refresh"
# source, source.discovery and source.schema_pattern: same as [pipeline.bronze]

[pipeline.bronze_test.target]
adapter = "prod"
catalog_template = "test_warehouse"
schema_template = "staging"
```

```bash
plan_id=$(rocky plan --pipeline bronze_test --filter tenant=acme --output json | jq -r .plan_id)
rocky apply "$plan_id"
```

Then compare row counts, column types and values between the dbt tables and the Rocky tables.

### Select models the way you did in dbt

Rocky accepts dbt's node selection syntax on `--select` and `--exclude`. See [Node selection](/reference/node-selection/) for the grammar.

| dbt | Rocky |
|---|---|
| `dbt ls --select <sel>` | `rocky list --select <sel>` |
| `dbt compile --select <sel>` | `rocky compile --select <sel>`, or `rocky emit-sql --select <sel>` for the SQL |
| `dbt run --select <sel>` | `rocky run --select <sel>` |
| `dbt test --select <sel>` | `rocky test --select <sel>` |
| `dbt docs generate --select <sel>` | `rocky docs --select <sel>` |
| `--select +model`, `model+`, `2+model`, `@model` | The same graph operators |
| `--select tag:nightly` | The same. Rocky matches a `[tags]` key or value. |
| `--select path:models/staging` | The same |
| `--select config.materialized:incremental` | The same. `table` matches Rocky's `full_refresh`. |
| `--select source:raw+` | The same. Rocky matches the external tables a model reads. |
| `--select state:modified+ --state path/to/artifacts` | `--select state:modified+ --state-ref main`. Rocky compares against a git ref, not a manifest. |
| `--exclude <sel>` | The same |
| `--defer --state path/to/artifacts` | `--defer`, optionally `--defer-to <schema>` |

## 9. Convert dbt Tests to Rocky Tests and Contracts

`rocky import-dbt` translates two kinds of dbt test:

- The four column-level generic tests plus common `dbt_utils` and `dbt_expectations` tests with a native equivalent. Each becomes a `[[tests]]` block.
- Unit tests from `manifest.unit_tests` (dbt 1.8+). Each becomes a `[[test]]` block. This is manifest-only: the regex path never sees a unit test.

Everything else needs a manual step: column-level contracts, project-defined generics and singular tests.

### Generic test mapping

The importer reads `tests:` and its synonym `data_tests:` (dbt 1.7+) in `schema.yml` and emits `[[tests]]` blocks into `models/<model>.toml`. It resolves `relationships.to: ref('m')` to a fully-qualified Rocky table; a cross-project ref falls back to the importer defaults. `rocky test` runs these tests against the materialised tables.

| dbt test | Rocky `[[tests]]` |
|---|---|
| `not_null` | `type = "not_null"` + `column` |
| `unique` | `type = "unique"` + `column` |
| `accepted_values` | `type = "accepted_values"` + `values = [...]` + `column` |
| `relationships` | `type = "relationships"` + `to_table` + `to_column` + `column` |
| `dbt_utils.unique_combination_of_columns` (model-level) | `type = "composite"` + `kind = "unique"` + `columns = [...]` |
| `dbt_utils.accepted_range`, `dbt_expectations.expect_column_values_to_be_between` | `type = "in_range"` + `min` / `max` (at least one) + `column` (numeric bounds only) |
| `dbt_expectations.expect_column_values_to_match_regex` | `type = "regex_match"` + `pattern` + `column` |
| `dbt_expectations.expect_column_values_to_be_in_set` | `type = "accepted_values"` + `values = [...]` + `column` |
| `dbt_utils.expression_is_true` | `type = "expression"` + `expression` |

Any other test becomes an `UnsupportedTest` warning naming the model, column and test.

#### Consolidating repeated tests into named definitions

The importer writes one inline `[[tests]]` block per column, so a `not_null` on twelve models lands as twelve blocks. After the import you can define a test once in `models/test_definitions.toml` and apply it with `[[use_test]]`:

```toml
# models/test_definitions.toml
[positive_amount]
type = "expression"
expression = "amount > 0"

[known_status]
type = "accepted_values"
values = ["pending", "shipped", "delivered"]
```

```toml
# models/fct_orders.toml
[[use_test]]
name = "positive_amount"
column = "total_amount"

[[use_test]]
name = "known_status"
column = "status"
```

Entries are written as `[name]`, not `[[...]]`. A `[[use_test]]` may override `column`, `severity` or the row `filter`. An unknown name is a hard error at load. These resolve into the same `[[tests]]`, so `rocky test --declarative` runs them against the configured warehouse.

### dbt unit tests (manifest path)

With `rocky import-dbt --manifest target/manifest.json`, the importer writes each `manifest.unit_tests` entry as a `[[test]]` block in the model's sidecar. It strips the `ref(...)` and `source(...)` wrappers on `given.input` to bare references:

```toml
# models/fct_orders.toml
[[test]]
name = "stamps_status_when_completed"

[[test.given]]
ref = "stg_orders"

[[test.given.rows]]
order_id = 1
status = "completed"

[test.expect]
ordered = false

[[test.expect.rows]]
order_id = 1
status = "completed"
```

The `--output json` payload and `MIGRATION-NOTES.md` report `unit_tests_found`, `unit_tests_converted` and `unit_tests_skipped`. Two warnings explain a skip:

- `OrphanUnitTest`: the test targets a model the importer did not pick up.
- `UnsupportedUnitTestFormat`: `expect.format = "csv"` or `"sql"`, a fixture reference, or another shape Rocky's `UnitTestDef` does not model.

Rocky does not import CSV and SQL fixtures or `overrides:` blocks. `rocky test` seeds a fresh in-memory DuckDB with each `given` fixture, materializes the model, and compares the output to `expect` as a multiset, or as an ordered list when `expect.ordered` is set.

### Column-level contracts

A test checks rows at run time. A `.contract.toml` checks column types and nullability at compile time. The importer generates one only from an enforced dbt contract. For any other model, write one:

```toml
# contracts/stg_orders.contract.toml
[[columns]]
name = "order_id"
type = "Int64"
nullable = false

[[columns]]
name = "total_amount"
type = "Decimal"
nullable = false

[rules]
required = ["order_id", "total_amount"]
protected = ["order_id"]
```

```bash
rocky compile --models ./rocky-models --contracts ./contracts
```

The compile fails when a model's output breaks its contract.

## 10. Add Intent Descriptions

An intent is a plain-English description of a model, stored in its sidecar. `ai-sync` and `ai-test` read it. Generate intent for every model:

```bash
export ANTHROPIC_API_KEY="sk-ant-..."
rocky ai-explain --all --save --models ./rocky-models
```

## 11. Move a Large Project in Stages

1. **Import and compile.** Run `rocky import-dbt`, fix the compile errors, add contracts on the models that matter, and run `rocky ci` next to dbt in CI.
2. **Reach test parity.** Run `rocky test` locally and compare output against dbt on a test catalog. Make `rocky compile` a required PR check.
3. **Cut over, leaf first.** Switch one layer at a time, starting with the models nothing depends on. Watch parity for a week or two before the next layer.
4. **Finish.** Move the rest, drop the dbt CI steps, and set up the [Dagster integration](/dagster/introduction/) if you want an orchestrator.


While you migrate, keep the dbt `models/` and Rocky `rocky-models/` directories separate. Run `dbt compile`, `rocky compile --models ./rocky-models --contracts ./contracts` and `rocky test --models ./rocky-models` in the same CI job.

You do not have to convert everything. A vendor package such as `fivetran/stripe` builds tables in your warehouse. Rocky reads a schema-qualified table such as `dbt_fivetran.stg_facebook_ads__ad_history` as an external source. See [Read dbt Package Tables](/guides/using-dbt-packages/).

## Troubleshooting

### "model not found" after import

The importer names each model after its SQL file stem. If dbt renames models with `{{ config(alias='...') }}`, a `depends_on` may not match. Read each TOML `name` and update the `depends_on` references.

### An incremental model imported as `full_refresh`

An append-style dbt model has no watermark to apply, and a Rocky `incremental` model without one is refused with `E037`. The importer maps it to `full_refresh`, which cannot duplicate rows: an `incremental` model with no `unique_key`, `incremental_strategy = 'append'`, `'merge'` with no `unique_key`, an unrecognised strategy, or a `microbatch` model with no `unique_key`. Each appears as a warning. To keep incremental behaviour, give the model a `unique_key` and leave `incremental_strategy` unset or `'merge'`. Otherwise rewrite it as a [`time_interval`](/concepts/time-interval/) model with `@start_date` and `@end_date`.

The importer refuses the model when its SQL uses `is_incremental()` in any form other than the standard watermark filter, because a kept delta filter would replace the table with only recent rows. Remove the filter by hand, then use `merge` with a `unique_key` or a `time_interval` model. The raw and no-manifest path refuses every incremental model it resolves, plus Jinja control flow, versioned models and unresolvable config expressions. Run `dbt compile --full-refresh` first, as the caution above explains.

### Environment-specific logic

dbt branches with `{{ target.name }}`. Rocky has no environment-specific SQL. Use one `rocky.toml` per environment:

```bash
rocky --config pipeline.prod.toml compile --models ./rocky-models
rocky --config pipeline.dev.toml compile --models ./rocky-models
```
