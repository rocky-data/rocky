# dbt Migration

A small dbt project and the same two models written for Rocky. Use it to try
`rocky import-dbt` and `rocky validate-migration` on input you can read in
full.

## Files

```
dbt-migration/
  dbt-project/                       # the input
    dbt_project.yml
    models/staging/stg_customers.sql # {{ config() }}, {{ source() }}
    models/marts/fct_orders.sql      # {{ config() }}, {{ ref() }}

  rocky-project/                     # a hand-written Rocky equivalent
    rocky.toml
    models/stg_customers.sql   + stg_customers.toml   # plain SQL
    models/fct_orders.rocky    + fct_orders.toml      # Rocky DSL
```

`rocky-project/` shows both model forms. `stg_customers` stays plain SQL with
the Jinja removed. `fct_orders` is rewritten in the Rocky DSL. Rocky runs SQL
models and DSL models in the same project, so rewriting is optional.

## Import the dbt project

Run these from the repository root:

```bash
cd engine/examples/dbt-migration
rocky import-dbt --dbt-project dbt-project/ --output-dir imported/
```

The importer prints a report and writes a runnable Rocky project:

```
dbt Migration Report
====================

Project: ecommerce
Method:  regex

Models:  2 total
  1 imported successfully  (view: 1, full_refresh: 1)
  1 with warnings

Next Steps:
  1. rocky compile
  2. rocky ai-explain --all --save
Output:  2 models translated, 0 seeds copied → imported/
         rocky.toml         → imported/rocky.toml
         MIGRATION-NOTES.md → imported/MIGRATION-NOTES.md

Warnings:
  stg_customers: source('raw', 'customers') not found in sources.yml
    -> add a sources.yml definition for this source
```

Rocky offers the intent step only when an imported model has no `intent`.
This project's dbt models carry no YAML descriptions, so both arrive without
one and the step appears.

`imported/` then holds `rocky.toml`, `MIGRATION-NOTES.md`, and a `models/`
directory with one `.sql` and one `.toml` per model, plus `_defaults.toml`.

Read `MIGRATION-NOTES.md` before you trust the output. It records what the
importer could not translate.

### Import flags worth knowing

| Flag | Effect |
|---|---|
| `--output-dir <dir>` | Destination. Defaults to `rocky-out`. |
| `--overwrite` | Write into a non-empty destination. Refused otherwise. |
| `--manifest <path>` | Use a specific `manifest.json`. Auto-detected from `target/` when omitted. |
| `--no-manifest` | Force the regex importer even when a manifest exists. |
| `--target-adapter <name>` | Override the adapter. Read from `profiles.yml` otherwise. |
| `--skip-unit-tests` | Skip dbt unit-test translation. |
| `--microbatch-as <mode>` | `merge` (default) or `time_interval` for dbt microbatch models. |

This example ships no `target/manifest.json`, so the importer falls back to
the regex path and reports `Method: regex`. Run `dbt compile` in your own
project first to get the manifest path, which resolves `ref()` and `source()`
exactly.

### Two kinds of Jinja the importer refuses

The importer fails a model rather than translate it when it cannot render the
Jinja faithfully. It writes no `.sql` and no `.toml` for that model, and the
report lists it under `Failed:`.

Case one is `is_incremental()`. When a model still holds a
`{% if is_incremental() %}` block that dbt has not compiled away, every raw
import path refuses it. For a model named `stg_events` the line reads:

```
  stg_events: contains an unresolved reference to dbt's `is_incremental()` macro; the raw SQL importer cannot preserve dbt's false-on-bootstrap, true-on-existing-target semantics without either referencing a missing target during bootstrap or deleting bounded incremental logic. Compile dbt in an incremental context, verify the compiled SQL retains its intended predicate and is valid for Rocky's initial target state, and import that manifest; otherwise, rewrite the model with a Rocky-supported strategy
```

The message names both ways out. Compile dbt in an incremental context and
import that manifest. Or rewrite the model with a Rocky strategy: set
`type = "incremental"` and a `timestamp_column` in the sidecar.

```toml
[strategy]
type = "incremental"
timestamp_column = "updated_at"
```

Case two is `{% for %}` and `{% set %}`. The regex importer strips the
delimiters and leaves the body behind once, which is wrong, so it refuses
these instead. The manifest path resolves them, so this refusal is specific to
a no-manifest import:

```
  stg_wide: contains unsupported Jinja control flow ({% for %} or {% set %}) that the no-manifest importer cannot faithfully render — re-run after `dbt compile` (the manifest path resolves Jinja) or rewrite the model without loops/assignments
```

A `{% if %}` block that is not `is_incremental()` is a warning, not a failure.
The importer keeps the model, heads it with
`-- TODO: dbt-jinja-not-translated — see MIGRATION-NOTES.md`, and wraps the
block in `/* TODO: unsupported Jinja block */`. The conditional body then runs
unconditionally, so review those models.

`rocky import-dbt` still exits `0` when a model fails either way, so read the
report rather than the exit code. Neither model in `dbt-project/` hits either
case, so you will not see these here.

## Compare the two projects

```bash
rocky validate-migration --dbt-project dbt-project/ --rocky-project rocky-project/
```

The report lists models it could not match and metadata it found missing.
`--rocky-project` is optional; drop it to inspect the dbt side alone. Add
`--sample-size <n>` to sample rows when a warehouse is reachable.

## Compile the Rocky project

```bash
cd rocky-project
rocky compile
```

```
  ✓ stg_customers (7 columns)
  ✓ fct_orders (7 columns)
  Compiled: 2 models, 0 errors, 0 warnings
```

`rocky plan` also works here and writes a plan file without executing SQL.

`rocky run` does not work here, for three reasons.

The sidecars target `warehouse.staging` and `warehouse.analytics`. A DuckDB
session has no catalog called `warehouse`, so `rocky run --models models/`
reports `Catalog with name warehouse does not exist`.

The models read `source.raw.customers`. This example ships no such table.

`fct_orders.rocky` opens `from stg_orders`, and `rocky-project/` ships no
`stg_orders` model. That is a missing model, not a missing table. Rocky reads
an unknown name as a relation it will find in the warehouse, so the compile
above still passes. The gap shows only at run time. Add a `stg_orders` model,
or point that line at a table you have.

## What changes when you move a model

| Concern | dbt | Rocky |
|---|---|---|
| Materialization, schema, tags | `{{ config(...) }}` in the SQL body | `.toml` sidecar beside the body |
| Model reference | `{{ ref('stg_orders') }}` | bare name: `FROM stg_orders` or `from stg_orders` |
| Source reference | `{{ source('raw', 'customers') }}` | qualified name: `source.raw.customers` |
| Project + connection | `dbt_project.yml` plus `profiles.yml` | one `rocky.toml` |
| Templating | Jinja | none; the body is SQL or Rocky DSL |
| Incremental logic | `{% if is_incremental() %}` in the SQL body | `type = "incremental"` plus `timestamp_column` in the sidecar |

The last row is the one that can block an import. `rocky import-dbt` refuses a
model whose SQL still holds an unresolved `is_incremental()`, as described
above.

## NULL handling in the Rocky DSL

`fct_orders.rocky` writes `where status != "cancelled"`. The DSL compiles `!=`
to `IS DISTINCT FROM`:

```sql
WHERE status IS DISTINCT FROM 'cancelled'
```

`NULL IS DISTINCT FROM 'cancelled'` is true, so rows with a NULL `status`
survive the filter. SQL's `!=` evaluates to NULL there and drops them.

This rewrite applies to the DSL only. A `.sql` model keeps SQL's own
three-valued logic, so `WHERE status != 'cancelled'` still drops NULL rows.
Check `rocky emit-sql` when you want to see exactly what a model will run.

## Where `--config` goes

`--config` is a top-level flag, not a per-command flag. It comes before the
subcommand, never after:

```bash
rocky --config rocky.toml compile   # works
rocky compile --config rocky.toml   # error: unexpected argument '--config' found
```

The `rocky compile` above omits it, because `--config` already defaults to
`rocky.toml` in the working directory.

`rocky import-dbt` and `rocky validate-migration` never read a `rocky.toml`.
Each takes its paths from its own flags, so neither needs `--config`.
