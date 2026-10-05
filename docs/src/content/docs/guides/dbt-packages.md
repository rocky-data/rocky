---
title: Use dbt Packages
description: Vendor a dbt Hub package, such as a Fivetran connector package, as Rocky models with rocky package, then extend or override it.
sidebar:
  order: 2
---

`rocky package` brings a dbt Hub package into your Rocky project as plain Rocky models. dbt runs once, when you add or update the package, to compile it. After that, Rocky owns the models. It type-checks them, tracks their lineage, selects them, runs them and tests them. You do not need dbt to compile, run or test the project.

This page walks through the [`fivetran/stripe`](https://hub.getdbt.com/fivetran/stripe/latest/) package on DuckDB. The same steps work for other Fivetran packages, such as `fivetran/hubspot` and `fivetran/salesforce`.

If you prefer to keep a package in dbt and only read its tables, see [Using Rocky with dbt Packages](/guides/using-dbt-packages/).

## How it works

```
rocky package add fivetran/stripe [--build-empty]
        │
        ▼  throwaway dbt project in a temp directory
   dbt deps ─▶ [dbt run --empty] ─▶ dbt compile --full-refresh
        │       only with                    │
        │       --build-empty                ▼
        │                          target/manifest.json
        ▼
models/packages/stripe/*.sql + *.toml   (plain SQL, no Jinja)
rocky-packages.lock                     (versions + file hashes)
        │
        ▼
rocky compile ─▶ rocky run ─▶ rocky test --declarative
```

- **`dbt deps`** downloads the package and its dependencies from the dbt Hub.
- **`dbt run --empty`** runs only when you pass `--build-empty`. It builds every package model with zero rows in a scratch schema, `rocky_package_build` (and `rocky_package_build_<custom schema>`).
- **`dbt compile --full-refresh`** writes the final SQL. Rocky imports the models of the package you asked for, plus any model the package reads from another package.

### Why some packages need `--build-empty`

Many package macros read the columns of an upstream relation while they compile. Fivetran staging models and `dbt_utils.star` do this. When the upstream does not exist yet, the macro sees no columns. Fivetran's macro then turns every column into `cast(null as ...)`. That SQL runs and loads nothing but NULLs.

Rocky never vendors that SQL. It looks for a model that selects only NULL columns from a table, or the `dbt_utils.star` placeholder `*`. Without `--build-empty`, such a model refuses the whole package with `E055`, and nothing is written. The message offers two ways out:

| Option | What happens |
|---|---|
| `--build-empty` | dbt runs `dbt run --empty` first. This writes empty `rocky_package_build*` schemas to the warehouse with your adapter's credentials, and runs the package's `on-run-start` / `on-run-end` hooks. Rocky never builds there; you can drop the schemas. |
| `--compiled <dir>` | Compile the package elsewhere, for example against a scratch warehouse, and import the result. Nothing runs against your warehouse. |

`fivetran/stripe` needs one of them. Rocky records the mode in `rocky-packages.lock` as `compile-only`, `build-empty` or `compiled`, and `rocky package update` reuses it.

## Before you start

You need:

- dbt-core 1.8 or later and the dbt adapter for your warehouse, on `PATH`. For DuckDB: `uv tool install dbt-core --with dbt-duckdb`, or `pip install dbt-core dbt-duckdb`.
- The package's source tables in the warehouse that `rocky.toml` points at. dbt reads their columns at compile time.
- A transformation pipeline whose `models` setting includes `models/packages/`, for example `models = "models/**"`.

`rocky package` generates a dbt profile from the `[adapter]` block. It supports `duckdb`, `snowflake`, `databricks`, `bigquery` and `postgres`. Secrets go to dbt through environment variables, never through a file. For another adapter, compile the package yourself and pass `--compiled <dbt project dir>`.

## Walkthrough: Fivetran Stripe on DuckDB

### 1. Point Rocky at the warehouse

**rocky.toml**

```toml
[adapter]
type = "duckdb"
path = "dev.duckdb"

[pipeline.analytics]
type = "transformation"
models = "models/**"

[pipeline.analytics.target.governance]
auto_create_schemas = true
```

Fivetran writes the Stripe connector tables to the `stripe` schema: `stripe.charge`, `stripe.customer`, `stripe.balance_transaction`, and so on.

### 2. Add the package

```bash
rocky package add "fivetran/stripe@>=1.0.0,<2.0.0" --build-empty
```

Without `--build-empty`, this package is refused with `E055`. See [Why some packages need `--build-empty`](#why-some-packages-need---build-empty).

The argument is `<namespace>/<name>`, then optionally `@` and a version requirement. A requirement is one version (`1.10.1`) or a comma-separated range. Without one, dbt takes the latest version.

Rocky writes 65 models to `models/packages/stripe/` and creates `rocky-packages.lock`:

```text
fivetran/stripe 1.10.1 (stripe) → models/packages/stripe/
  65 models (65 added, 0 removed), 23 sources, 15 tests mapped, 0 tests dropped
  files: 130 written, 0 unchanged, 0 deleted, 0 kept (edited), 0 .incoming
```

If your connector writes to another schema, pass the package's own variable:

```bash
rocky package add fivetran/stripe --build-empty --vars stripe_schema=raw_stripe
```

`--vars` takes `key=value` and can repeat. Rocky reads the value as YAML, so `false`, `5` and `[a, b]` keep their types. The lockfile records the vars, and `rocky package update` uses them again.

### 3. Compile and run

```bash
rocky compile
rocky run --dag
rocky test --declarative
```

`rocky compile` type-checks the package models like any other model. `rocky run --dag` builds them in dependency order. `rocky test --declarative` runs the package's `not_null` tests, which Rocky mapped to `[[tests]]` in each sidecar.

## What Rocky writes

Each package model is a `.sql` file and a `.toml` sidecar, like a model you write by hand.

**models/packages/stripe/stripe__customer_overview.sql** (start)

```sql
-- Vendored by `rocky package` from a dbt package. Edit freely: `rocky package update`
-- keeps edited files and writes the new upstream version beside them as `.incoming`.
with balance_transaction_joined as (
    select *
    from stripe__balance_transactions
), ...
```

- **Model names stay as the package names them.** References between package models are bare names, so Rocky sees them as DAG edges.
- **Every package model builds into one schema**, `main` on DuckDB. Rocky resolves a bare model name through the connection's current schema, so a model in another schema would be out of reach. Change it with `--target-schema`. dbt's per-folder schemas (`stg_stripe`, `stripe`) are not kept.
- **Source tables stay fully qualified**, as dbt resolved them: `"dev"."stripe"."charge"`. Each model's sidecar lists them as `[[sources]]`.
- **Tests:** dbt's `not_null`, `unique`, `accepted_values` and `relationships` tests become `[[tests]]`, with their `severity` and `where`. Rocky reports any other test as dropped (`W055`).
- **Materializations:** `table` becomes `full_refresh`, `view` stays a view, and `ephemeral` stays ephemeral. An `incremental` model goes through the same conversion as [`rocky import-dbt`](/guides/migrate-from-dbt/). Rocky reports one that does not stay incremental (`W055`).

**rocky-packages.lock** records, for each package: the Hub name, the version requirement, the resolved version, the dbt version, the adapter, the target schema, the vars and their hash, the build mode, the compile time, the source tables, and a hash of every file Rocky wrote. Commit it with the models.

## Extend a package

To build on a package, write a model that reads a package model by its bare name:

**models/marts/customer_revenue.sql**

```sql
SELECT
    customer_id,
    total_sales,
    total_refunds,
    total_sales + total_refunds AS net_sales
FROM stripe__customer_overview
WHERE customer_id <> 'No Customer ID'
```

**models/marts/customer_revenue.toml**

```toml
[target]
catalog = "dev"
schema = "main"
```

`stripe__customer_overview` is a model in the project, so the read is a DAG edge. `rocky run --dag` builds `customer_revenue` after it, and `rocky compile` checks its columns. A misspelled column fails with `E039`. This is the pattern to use for most changes.

## Override a package model

To change a package model, edit its vendored file. `rocky package update` never overwrites a file you edited.

For example, a type mismatch in your connector data can break one package model. Fix the SQL in `models/packages/stripe/<model>.sql`. Then `rocky run` uses your version.

## Update a package

```bash
rocky package update            # every package
rocky package update stripe     # one package
```

`update` recompiles the package within its version requirement, with the vars and the build mode in the lockfile. `--vars` adds or replaces vars. `--build-empty` or `--build-empty=false` changes the mode. A package added with `--compiled` needs `--compiled <dir>` again, or `--build-empty`. Then Rocky compares three versions of each file: what it wrote last time (the hash in the lockfile), the file on disk, and the new upstream file.

| Your file | Upstream | Result |
|---|---|---|
| Not edited | Changed or new | Written in place |
| Not edited | Removed | Deleted |
| Edited | Not changed | Kept |
| Edited | Changed | Kept. The new version is written to `<file>.incoming` (`W055`) |
| Edited | Removed | Kept (`W055`) |
| Deleted | Any | Written again |

Merge an `.incoming` file by hand, then delete it. Rocky does not load `.incoming` files as models. The JSON output lists the models the update added and removed.

## List and remove

```bash
rocky package list
rocky package remove stripe
```

`list` shows each package with its version, its models, and any files you edited, deleted, or have an `.incoming` copy for. `remove` deletes the package's files and its lock entry. It refuses (`E055`) when you edited a vendored file. Pass `--force` to delete those too.

## Model names

Package models keep their dbt names. Rocky does not prefix or rename them, because your models read them by bare name. When a package model has the name of a project model or of another package's model, `add` and `update` refuse with `E055` and write nothing. Rename or remove the existing model, then try again.

Rocky compares the name each model resolves to: the `name =` in its sidecar or frontmatter, else its file name. Names that differ only by case collide too, because warehouses fold unquoted names and some file systems ignore case. A package with two such models of its own is refused as well.

## Without dbt on the machine

In CI, or with no network, compile the package where dbt is available and import the result:

```bash
rocky package add fivetran/stripe --compiled path/to/dbt-project
```

`--compiled` reads `<dir>/target/manifest.json` and `<dir>/package-lock.yml`. Compile that project with `dbt run --empty --full-refresh` and then `dbt compile --full-refresh`. Use a warehouse with the same source table names. Rocky applies the same NULL-column check, so a project compiled without `dbt run --empty` is refused.

To use a private dbt Hub mirror, set `DBT_PACKAGE_HUB_URL`. Rocky passes the environment through to dbt.

## Limits

- **The compiled SQL is fixed at add time.** dbt resolved its macros against your warehouse and vars on that day. Run `rocky package update` when the connector adds columns, or when you change vars.
- **One schema.** All package models build into `--target-schema`.
- **Introspection is detected by its output.** Rocky catches all-NULL columns and the `dbt_utils.star` placeholder. A macro that degrades another way without built upstreams is not caught. Use `--build-empty` for packages that introspect. With `--build-empty`, a model whose upstream failed in `dbt run --empty` is refused on its own (`W055`) and the rest are vendored.
- **dbt-only features are dropped.** Hooks, `on_schema_change` and other config that the [migration importer](/guides/migrate-from-dbt/) does not translate are not translated here either. Tests on sources are not mapped.
- **Two packages that share a model-carrying dependency** collide on that dependency's model names. Rocky refuses the second package.
- **`--build-empty` writes to the warehouse.** It uses the adapter's real credentials, creates the `rocky_package_build*` schemas with empty relations, and runs the package's hooks. Rocky does not drop these schemas. Where that is not allowed, use `--compiled`.
- **Vars are stored in plain text** in `rocky-packages.lock`. Do not pass secrets as `--vars`.

## Reference

See [`rocky package`](/reference/commands/development/#rocky-package) for every flag and the JSON output.
