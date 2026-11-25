---
title: Vendor dbt Packages
description: Vendor a dbt Hub package, such as a Fivetran connector package, as Rocky models with rocky package, then extend or override it.
sidebar:
  order: 8
---

`rocky package` brings a dbt Hub package into your Rocky project as plain Rocky models. dbt runs once, when you add or update the package, to compile it. After that, Rocky owns the models: it type-checks, selects, runs and tests them. You do not need dbt for any of that.

This page walks through [`fivetran/stripe`](https://hub.getdbt.com/fivetran/stripe/latest/) on DuckDB. The same steps work for other Fivetran packages. To keep a package in dbt and only read its tables, see [Read dbt Package Tables](/guides/using-dbt-packages/).

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
- **`dbt run --empty`** runs only with `--build-empty`. It builds every package model with zero rows in a scratch schema, `rocky_package_build` (and `rocky_package_build_<custom schema>`).
- **`dbt compile --full-refresh`** writes the final SQL. Rocky imports the models of the package you asked for, plus any model it reads from another package.

### Why some packages need `--build-empty`

Many package macros (Fivetran staging models, `dbt_utils.star`) read the columns of an upstream relation while they compile. When the upstream does not exist yet, the macro sees no columns, and Fivetran's macro turns every column into `cast(null as ...)`: SQL that loads only NULLs. Rocky never vendors it. A model that selects only NULL columns from a table, or the `dbt_utils.star` placeholder `*`, refuses the whole package with `E055` and nothing is written, unless you choose:

| Option | What happens |
|---|---|
| `--build-empty` | dbt runs `dbt run --empty` first. This writes empty `rocky_package_build*` schemas to the warehouse with your adapter's credentials, and runs the package's `on-run-start` / `on-run-end` hooks. Rocky never builds there; you can drop the schemas. |
| `--compiled <dir>` | Compile the package elsewhere, for example against a scratch warehouse, and import the result. Nothing runs against your warehouse. |

`fivetran/stripe` needs one. The lockfile records the mode (`compile-only`, `build-empty` or `compiled`), and `rocky package update` reuses it.

## Before you start

You need:

- dbt-core 1.8 or later and the dbt adapter for your warehouse, on `PATH`. For DuckDB: `uv tool install dbt-core --with dbt-duckdb`, or `pip install dbt-core dbt-duckdb`.
- The package's source tables in the warehouse that `rocky.toml` points at. dbt reads their columns at compile time.
- A transformation pipeline whose `models` setting includes `models/packages/`, for example `models = "models/**"`.

`rocky package` generates a dbt profile from the `[adapter]` block for `duckdb`, `snowflake`, `databricks`, `bigquery` and `postgres`. Secrets go to dbt through environment variables, never a file. For another adapter, pass `--compiled <dbt project dir>`.

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

Fivetran writes the Stripe tables to the `stripe` schema.

### 2. Add the package

```bash
rocky package add "fivetran/stripe@>=1.0.0,<2.0.0" --build-empty
```

Without `--build-empty`, this package is refused with `E055` (see [above](#why-some-packages-need---build-empty)). The argument is `<namespace>/<name>`, then optionally `@` and a version (`1.10.1`) or comma-separated range. Without one, dbt takes the latest.

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

`--vars` takes `key=value` and can repeat. Rocky reads the value as YAML, so `false`, `5` and `[a, b]` keep their types. The lockfile records the vars, and `update` reuses them.

### 3. Compile and run

```bash
rocky compile
rocky run --dag
rocky test --declarative
```

`rocky test --declarative` runs the package's tests, which Rocky mapped to `[[tests]]` in each sidecar.

## What Rocky writes

Each package model is a `.sql` file and a `.toml` sidecar, like a model you write by hand.

Each vendored file starts with a comment: edit freely, `rocky package update` keeps edited files and writes the new upstream beside them as `.incoming`.

- **Model names stay as the package names them.** References between package models are bare names, so Rocky sees them as DAG edges.
- **Every package model builds into one schema**, `main` on DuckDB. Rocky resolves a bare model name through the connection's current schema, so a model in another schema would be out of reach. Change it with `--target-schema`. dbt's per-folder schemas (`stg_stripe`, `stripe`) are not kept.
- **Source tables stay fully qualified**, as dbt resolved them: `"dev"."stripe"."charge"`. Each model's sidecar lists them as `[[sources]]`.
- **Tests:** dbt's `not_null`, `unique`, `accepted_values` and `relationships` tests become `[[tests]]`, with their `severity` and `where`. An `accepted_values` test with numeric values becomes an `expression` test (`col IS NULL OR col IN (1, 2)`), so the column is not compared with strings. A `where` filter must be one boolean expression that Rocky's filter rules accept; otherwise the test is dropped. Rocky reports any dropped test (`W055`).
- **Materializations:** `table` becomes `full_refresh`, `view` stays a view, and `ephemeral` stays ephemeral. An `incremental` model goes through the same conversion as [`rocky import-dbt`](/guides/migrate-from-dbt/). Rocky reports one that does not stay incremental (`W055`).

**rocky-packages.lock** records, per package: the Hub name, version requirement and resolved version, dbt version, adapter, target schema, vars and their hash, build mode, compile time, source tables, and a hash of every file Rocky wrote. Commit it with the models.

## Extend a package

Write a model that reads a package model by its bare name:

```sql
-- models/marts/customer_revenue.sql (sidecar [target]: catalog "dev", schema "main")
SELECT customer_id, total_sales, total_refunds, total_sales + total_refunds AS net_sales
FROM stripe__customer_overview
WHERE customer_id <> 'No Customer ID'
```

`stripe__customer_overview` is a project model, so the read is a DAG edge. `rocky run --dag` builds `customer_revenue` after it, and `rocky compile` checks its columns (`E039` on a misspelled column). Use this pattern for most changes.

## Override a package model

Edit the vendored file, for example `models/packages/stripe/<model>.sql`. `rocky package update` never overwrites a file you edited.

## Update a package

```bash
rocky package update            # every package
rocky package update stripe     # one package
```

`update` recompiles the package within its version requirement, with the lockfile's vars and build mode. `--vars` adds or replaces vars. `--build-empty` or `--build-empty=false` changes the mode. A package added with `--compiled` needs `--compiled <dir>` again, or `--build-empty`. Rocky then compares three versions of each file: what it wrote last time (the lockfile hash), the file on disk, and the new upstream file.

| Your file | Upstream | Result |
|---|---|---|
| Not edited | Changed or new | Written in place |
| Not edited | Removed | Deleted |
| Edited | Not changed | Kept |
| Edited | Changed | Kept. The new version is written to `<file>.incoming` (`W055`) |
| Edited | Removed | Kept (`W055`) |
| Deleted | Any | Written again |

If a model Rocky vendored before no longer imports, `update` refuses (`E055`) and writes nothing, instead of deleting that model's files. `rocky package update` with no name updates packages in name order and stops at the first failure; the message names the packages it already updated.

Merge an `.incoming` file by hand, then delete it. Rocky does not load it as a model. The JSON output lists the models the update added and removed.

## List and remove

```bash
rocky package list
rocky package remove stripe
```

`list` shows each package with its version, its models, and any files you edited, deleted, or have an `.incoming` copy for. `remove` deletes the package's files and its lock entry. It refuses (`E055`) when you edited a vendored file. Pass `--force` to delete those too.

## Model names

Package models keep their dbt names, because your models read them by bare name. `add` and `update` refuse with `E055`, and write nothing, when a package model has the name of a project model or another package's model, or writes the same `[target]` `catalog.schema.table` as another model (ignoring case), including two models of the package itself. Rocky compares the resolved name (`name =` in the sidecar or frontmatter, else the file name). Names that differ only by case collide, because warehouses fold unquoted names and some file systems ignore case.

## What Rocky refuses

Before it writes anything, `add` and `update` check that the vendored models make a project `rocky compile` accepts. Each problem below refuses the package with `E055`:

- A package model reads a model that was not vendored, for example one that failed to import.
- A package model reads a dbt seed. Rocky does not vendor seeds.
- A package model's SQL does not parse the way `rocky compile` parses it.

A model that fails to import and that nothing reads is skipped with `W055`.

## How Rocky runs dbt

The package's macros are third-party code, and dbt runs them. Rocky limits what they can reach:

- **Environment.** dbt starts with an empty environment plus an allowlist: `PATH`, `HOME`, user and locale variables, temp directories, TLS and proxy settings, `GOOGLE_APPLICATION_CREDENTIALS`, `CLOUDSDK_CONFIG` and `DBT_PACKAGE_HUB_URL`. The credentials the generated profile needs are passed as `DBT_ENV_SECRET_ROCKY_*` variables, which dbt scrubs from its logs.
- **Vars.** dbt renders vars through Jinja. Rocky refuses `{{`, `{%` and `{#` in a var, from `--vars` or from the lockfile. A var whose name looks like a credential (`api_key`, `stripe_token`, `db_password`) is refused unless you pass `--allow-secret-var`, because vars are stored in clear text in `rocky-packages.lock`.
- **Timeout.** Each dbt step stops after 30 minutes. Change it with `--dbt-timeout <seconds>`.
- **Files.** Rocky writes each vendored file to a temp file and renames it. It refuses to write or delete through a symlink under `models/packages/`.
- **Paths.** A relative `--dbt`, Snowflake `private_key_path` or BigQuery `keyfile` is resolved against the directory you ran `rocky` in.

## Without dbt on the machine

In CI, or with no network, compile the package where dbt is available and import the result:

```bash
rocky package add fivetran/stripe --compiled path/to/dbt-project
```

`--compiled` reads `<dir>/target/manifest.json` and `<dir>/package-lock.yml`. Compile that project with `dbt run --empty --full-refresh` and then `dbt compile --full-refresh`. Use a warehouse with the same source table names. Rocky applies the same NULL-column check, so a project compiled without `dbt run --empty` is refused.

For a private dbt Hub mirror, set `DBT_PACKAGE_HUB_URL`.

## Limits

- **The compiled SQL is fixed at add time.** dbt resolved its macros against your warehouse and vars that day. Run `rocky package update` when the connector adds columns or you change vars.
- **One schema.** All package models build into `--target-schema`.
- **Introspection is detected by its output.** Rocky catches all-NULL columns and the `dbt_utils.star` placeholder. A macro that degrades another way without built upstreams is not caught. Use `--build-empty` for packages that introspect. With it, a model whose upstream failed in `dbt run --empty` is refused on its own (`W055`) and the rest are vendored.
- **dbt-only features are dropped.** Hooks, `on_schema_change` and other config that the [migration importer](/guides/migrate-from-dbt/) does not translate are not translated here either. Tests on sources are not mapped.
- **Two packages that share a model-carrying dependency** collide on its model names. Rocky refuses the second package.
- **`--build-empty` writes to the warehouse** and Rocky does not drop the `rocky_package_build*` schemas. Where that is not allowed, use `--compiled`.
- **Vars are stored in plain text** in `rocky-packages.lock`. Do not pass secrets as `--vars`.

## Reference

See [`rocky package`](/reference/commands/development/#rocky-package) for every flag and the JSON output.
