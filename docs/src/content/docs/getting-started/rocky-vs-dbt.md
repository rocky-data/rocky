---
title: Rocky vs dbt, Visually
description: A plain-language, diagram-first explanation of how Rocky differs from dbt Core and dbt Fusion.
sidebar:
  order: 4.4
---

This page explains the difference between Rocky and dbt in pictures. If you want the full feature-by-feature tables, see the [Feature Comparison](/getting-started/comparison/); if you're ready to try a migration, see [Migrate from dbt](/guides/migrate-from-dbt/).

:::note[A note on "dbt"]
"dbt" is three things today: **dbt Core 1.x** (the Python line, still the dominant deployment), **dbt Fusion** (the Rust runtime open-sourced as dbt Core v2.0, plus a SQL-comprehension layer), and the commercial **dbt platform**. The diagrams below contrast Rocky with dbt Core 1.x first, because that's what most teams run. Fusion changes part of the picture, and gets its own section.
:::

## The same job

Both tools take a set of SQL transformations, organize them into a dependency graph, and run them against your warehouse in the right order:

```
   raw_orders ──┐
                ├──▶ stg_orders ──┐
   raw_users ───┘                 ├──▶ orders_enriched ──▶ revenue_report
                     stg_users ───┘
```

If you know dbt, the day-to-day loop in Rocky feels familiar: write a model, declare what it depends on, run the pipeline. The difference is what each tool understands about the SQL, and what that lets it catch, and when.

## dbt Core: the SQL is rendered text

dbt Core parses your project before running anything: it resolves every `ref()`, builds the DAG, and fails fast on a reference to a model that doesn't exist. What it does *not* do is read the SQL inside each model. A model is a Jinja template that renders to a string, and whether that string is correct is discovered when the warehouse executes it:

```
 your SQL + Jinja
        │
        ▼
 ┌──────────────┐    parse the project: resolve refs,
 │   parse &    │    build the DAG. broken refs fail
 │    render    │    here — but the SQL inside each
 └──────┬───────┘    model stays opaque, rendered text
        ▼
 ┌──────────────┐    the warehouse finds the SQL-level
 │  run against │──▶ problems: ✗ misspelled column,
 │   warehouse  │    ✗ type mismatch, ✗ silent
 └──────┬───────┘    double-count — discovered mid-run
        ▼
 ┌──────────────┐    data tests run right after each
 │     test     │    model builds and can skip its
 │ (built data) │    downstream — but the bad table
 └──────────────┘    itself is already materialized
```

Two fairness notes. Since dbt 1.5, a model can declare an **enforced contract** (its output column names and types), checked when that model is built. And `dbt build` interleaves tests with models in DAG order, so an error-severity test failure stops downstream models from building. Both help; neither reads the SQL. A misspelled column, a type mismatch inside a join, or a `SELECT *` that pulls a column nobody designed for still surfaces in the warehouse, against data.

## Rocky: the SQL is a typed program

Rocky reads the SQL itself. No Jinja: models are plain SQL, and the compiler parses each one, infers the columns and types it produces, and tracks how every column flows into the next model across the whole DAG. Statically detectable mistakes become compile errors before any model is materialized:

```
 your SQL (plain SQL, first-class)
        │
        ▼
 ┌──────────────┐    the compiler reads the SQL and
 │   compile    │    tracks columns + types across
 │  the whole   │──▶ the DAG: ✗ misspelled column?
 │   pipeline   │    ✗ type mismatch? ✗ contract
 └──────┬───────┘    violated? caught here — no
        │            model has been materialized
        │ compile errors block the run
        ▼
 ┌──────────────┐    during the run: replication drift
 │  run against │    is checked and acted on, quality
 │   warehouse  │    checks report into typed output,
 └──────────────┘    cost is recorded per model
```

The compiler is honest about its limits: a type it can't infer is tracked as `Unknown` rather than guessed, and what it can't prove statically still surfaces at runtime like anywhere else. See [The Compiler](/concepts/compiler/) for exactly what each pass checks.

The mental model in one picture:

```
       dbt Core                      Rocky
 ┌───────────────────┐      ┌───────────────────┐
 │  rendered text    │      │   typed program   │
 │                   │      │                   │
 │  the graph is     │      │  the graph AND    │
 │  understood; the  │      │  the SQL inside   │
 │  SQL inside is    │      │  it are checked   │
 │  trusted          │      │  before running   │
 └───────────────────┘      └───────────────────┘
   SQL-level errors           statically detectable
   surface at RUNTIME,        errors surface at
   in your data               COMPILE TIME, in CI
```

## Example: a source column changes type

An upstream loader changes a column's type under you: yesterday `order_id` was `INT`, today it arrives as something else. Nothing in your repo changed, so a code diff can't see it:

```
 dbt Core:
   source changes ──▶ pipeline runs ──▶ ✗ fails mid-run, or quietly
                                          builds on the changed type

 Rocky (replication pipelines):
   source changes ──▶ drift check ──▶ classified, then acted on:

     widening (INT → BIGINT)       ──▶ ALTER TABLE in place where
                                       the warehouse supports it,
                                       existing rows preserved

     narrowing (BIGINT → INT)      ──▶ target dropped and re-copied
                                       from the source — automatic,
                                       and logged in the run output

     with the governance opt-in    ──▶ non-additive changes refused
                                       and surfaced for review;
                                       target left untouched
```

On replication runs where the target table already exists, Rocky compares the source's actual columns against the target's before copying, and classifies every mismatch. Changes classified as safe widenings — like `INT` to `BIGINT`; each dialect ships its own allowlist — are applied in place with an `ALTER TABLE`, preserving existing rows, where the warehouse supports that `ALTER` (support varies by adapter today; the [Schema Drift](/concepts/schema-drift/) page has the per-warehouse rules and the current gaps). Incompatible changes (a narrowed type, a changed column family) drop the target and re-copy it from the current source — automatic and recorded in the run output, but still a rebuild: the new target holds only what the source holds today, so it is not lossless for targets that accumulate history the source has pruned. Teams that don't want automatic schema mutations opt into the drift-governance policy gate, which refuses any non-additive change and surfaces it for review instead of applying it. The honest summary: the drift check runs as part of a table's copy, and when it finds a mismatch the outcome is one of an in-place evolution, a logged rebuild, a policy refusal, or a loud error. It is not airtight, though: tables pruned by the opt-in `prune_unchanged` optimization skip the check along with the copy, and if a table's schema can't be read that run, the check degrades instead of failing (an unreadable target is rebuilt; an unreadable source goes unchecked until the next run). None of the dbt engines detect source-schema drift at all today (see the [drift table](/getting-started/comparison/#schema-drift-detection)).

## What about dbt Fusion?

Fusion is dbt's Rust engine, and its SQL-comprehension layer genuinely closes part of the compile-time gap: in its opt-in `strict` mode it type-checks SQL and computes column-level lineage. Rocky does not claim those as differentiators against Fusion.

The durable difference is what happens *around* the compiler. Rocky treats the run itself as something to check, record, and optionally gate, not just execute:

```
             ┌────────────────────────────────────────┐
   your ───▶ │ COMPILE  types · contracts · lineage · │──▶ ✗ errors
   code      │          dialect-portability lint      │      fail CI
             └───────────────────┬────────────────────┘
                                 ▼
             ┌────────────────────────────────────────┐
             │ RUN      drift checked · quality       │──▶ gates when
             │          checks reported · [budget]    │    configured
             │          overspend warns, or fails the │    to error
             │          run with on_breach = "error"  │
             └───────────────────┬────────────────────┘
                                 ▼
             ┌────────────────────────────────────────┐
             │ RECORD   per-model cost on every run · │
             │          content-addressed run record ·│
             │          replay                        │
             └────────────────────────────────────────┘
```

Concretely, on top of the compiler Rocky ships: per-model cost on every run plus `[budget]` blocks that can fail the run on overspend (no dbt distribution fails a run on overspend), schema-drift detection, declarative governance as Apache-2.0 code (RBAC diffing, masking bound to classification tags, with `rocky compliance --fail-on exception` as the CI gate), a dialect-portability lint, named branches as a first-class primitive, and a content-addressed record of every run. Quality checks run inline during replication and land in the typed run output with a severity, so CI and orchestrators gate on them rather than re-deriving state from logs. And there is no Jinja anywhere: your models stay plain SQL. The [Feature Comparison](/getting-started/comparison/) has the precise per-engine breakdown, including where Fusion and SQLMesh are ahead.

## One binary, typed outputs

dbt Core is a Python package; you install it into an environment and parse its logs and artifacts. Rocky is a single Rust binary, and every command that supports `--output json` emits a typed, schema-backed payload, which is what the surrounding tooling is built on:

```
 ┌────────────┐  ┌───────────┐  ┌───────────┐
 │ Python SDK │  │  Dagster  │  │  VS Code  │
 │(rocky-sdk) │  │integration│  │ extension │
 └─────┬──────┘  └─────┬─────┘  └─────┬─────┘
       └───────────────┼──────────────┘
                       ▼
               ┌───────────────┐
               │   rocky CLI   │   one binary, typed JSON out,
               └───────┬───────┘   compiles 10k models in ~1 s
                       ▼
      Databricks · Snowflake · BigQuery · DuckDB
```

Startup is milliseconds and a 10,000-model project compiles in about a second; see [Benchmarks](/getting-started/benchmarks/) for methodology.

## The short version

Same job: run SQL pipelines in dependency order. Different unit of understanding:

- **dbt Core** understands your project's *graph* — refs, DAG order, and (since 1.5) per-model contract declarations — but the SQL inside each model is rendered text, so SQL-level problems surface in the warehouse, and tests catch them after the model is built.
- **Rocky** understands the *SQL itself* — it compiles the pipeline as a typed program, blocks statically detectable errors before any model is materialized, detects source-schema drift on replication runs, and records checks and per-model cost on every run, with budgets and compliance gates you can configure to fail CI.

Where to go next:

- [Feature Comparison](/getting-started/comparison/) — the full tables, including dbt Fusion and SQLMesh
- [Migrate from dbt](/guides/migrate-from-dbt/) — `rocky import-dbt` and the migration path
- [The Compiler](/concepts/compiler/) — what actually happens at compile time
- [Quickstart](/getting-started/quickstart/) — a running pipeline in 5 minutes, no credentials needed
