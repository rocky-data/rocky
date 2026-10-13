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

### How the importer handles `is_incremental()` and other Jinja

The importer fails a model rather than translate it wrongly. A failed model
gets no `.sql` and no `.toml`, and the report lists it under `Failed:`.

**The standard watermark filter converts.** A dbt incremental model whose only
`is_incremental()` use is this shape imports as a Rocky `incremental` model:

```sql
{% if is_incremental() %}
  where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
```

The block becomes `WHERE @incremental_filter` (or `AND`), and the sidecar gets
`timestamp_column`. The placeholder is `TRUE` on the first run, so the first
run loads every row.

**Other `is_incremental()` uses need a manifest or a rewrite.** On the
no-manifest path, an incremental model with an unrecognized
`is_incremental()` block imports with the block commented out and no
watermark. `rocky compile` then refuses it with `E037` until you set one. An
incremental model with no `is_incremental()` at all is refused on this path. An
`is_incremental()` check in a model that is not incremental is refused:

```
  stg_events: contains an unresolved reference to dbt's `is_incremental()` macro; the raw SQL importer cannot preserve dbt's false-on-bootstrap, true-on-existing-target semantics without either referencing a missing target during bootstrap or deleting bounded incremental logic. Run `dbt compile --full-refresh` and import its compiled SQL from manifest.json with the matching run_results.json, or rewrite the model with a Rocky-supported strategy
```

A manifest import of an incremental model needs proof that dbt compiled it for
a full refresh: a matching `run_results.json` from the same
`dbt compile --full-refresh` invocation. Without it the model is refused. So
run `dbt compile --full-refresh` without `--select`, then import
`manifest.json` and `run_results.json` together.

To rewrite by hand, use `incremental` with `timestamp_column` and
`@incremental_filter`, or `merge` with a `unique_key`, or `time_interval` with
`@start_date` and `@end_date` in the SQL. A transformation `incremental` model
with no watermark is refused with `E037`, because it would append every row
again on each run.

```toml
[strategy]
type = "merge"
unique_key = ["event_id"]
```

**Jinja control flow needs a manifest.** The no-manifest importer cannot
evaluate `{% for %}`, `{% set %}`, or `{% if %}`. Dropping the tags would run
the body once or unconditionally, so it refuses the model:

```
  stg_wide: raw import cannot evaluate Jinja control flow; run `dbt compile --full-refresh` and import with the manifest
```

The manifest path imports the compiled SQL, so it resolves these.

`rocky import-dbt` still exits `0` when a model fails, so read the report
rather than the exit code. Neither model in `dbt-project/` hits these cases,
so you will not see them here.

## Compare the two projects

```bash
rocky validate-migration --dbt-project dbt-project/ --rocky-project rocky-project/
```

The report lists models it could not match and metadata it found missing.
`--rocky-project` is optional; drop it to inspect the dbt side alone. The
command connects to no warehouse. It accepts `--sample-size <n>` but ignores
it, so the flag samples no rows and does not change the report.

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
| Incremental logic | `{% if is_incremental() %}` in the SQL body | `type = "incremental"` with `timestamp_column` and `@incremental_filter` in the SQL; or `merge` / `time_interval` in the sidecar. `incremental` with no watermark is refused (`E037`) |

The last row is the one that can block an import. See
[how the importer handles `is_incremental()`](#how-the-importer-handles-is_incremental-and-other-jinja).

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
