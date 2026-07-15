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

If you know dbt, the day-to-day loop in Rocky feels familiar: write a model, declare what it depends on, run the pipeline. The difference is what happens **before and during** the run.

## dbt Core: render, run, test afterwards

dbt Core models are SQL strings with Jinja templating in them. dbt's job is to render the templates and submit the results in dependency order. Whether the rendered SQL is *correct* is the warehouse's problem, discovered at runtime:

```
 your SQL + Jinja
        │
        ▼
 ┌──────────────┐    "did the template render? ship it."
 │    render    │
 │   templates  │
 └──────┬───────┘
        ▼
 ┌──────────────┐    the warehouse finds the problems:
 │  run against │──▶ ✗ type mismatch, missing column,
 │   warehouse  │      broken ref — discovered mid-run
 └──────┬───────┘
        ▼
 ┌──────────────┐
 │   dbt test   │──▶ the data is already built; tests
 │  (after the  │    inspect the damage after the fact
 │     fact)    │
 └──────────────┘
```

## Rocky: compile first, then run

Rocky parses your SQL (plain SQL, no Jinja) and builds a typed model of the entire pipeline: every model's columns, their types, and how each column flows into the next model. That whole program is checked before anything touches the warehouse:

```
 your SQL (plain SQL, first-class)
        │
        ▼
 ┌──────────────┐    the compiler knows every model's
 │   compile    │    schema and types:
 │  the whole   │──▶ ✗ column renamed upstream?
 │   pipeline   │    ✗ type mismatch across models?
 └──────┬───────┘    ✗ contract violated?
        │            caught here — nothing has run yet
        │ only if everything checks out
        ▼
 ┌──────────────┐
 │  run against │──▶ drift, quality checks, and cost
 │   warehouse  │    budgets enforced as gates during
 └──────────────┘    the run
```

The mental model in one picture:

```
       dbt Core                      Rocky
 ┌───────────────────┐      ┌───────────────────┐
 │   SQL templates   │      │   typed program   │
 │                   │      │                   │
 │  "trust me, run   │      │ "prove it works,  │
 │   it and we'll    │      │   then run it"    │
 │   test later"     │      │                   │
 └───────────────────┘      └───────────────────┘
   errors surface at          errors surface at
   RUNTIME, in your           COMPILE TIME, as
   data                       diagnostics in CI
```

## Example: a source column changes

A source team renames `user_id` to `customer_id`, or an upstream loader changes a column's type. Nothing in your repo changed, so a code diff can't see it:

```
 dbt Core:
   source changes ──▶ pipeline runs ──▶ ✗ breaks mid-run, or worse:
                                          silently builds wrong data

 Rocky:
   source changes ──▶ drift detected ──▶ ✋ handled at the gate:
                                           safe widenings evolve in
                                           place; unsafe changes get
                                           a deliberate rebuild, not
                                           a silent one
```

Rocky checks the warehouse's *actual* schema against what your models expect on every run. Safe type widenings (like `INT` to `BIGINT`) are evolved in place; unsafe changes trigger a controlled rebuild instead of a quiet divergence. See [Schema Drift](/concepts/schema-drift/) for the full graduated-evolution policy. None of the dbt engines detect this today (see the [drift table](/getting-started/comparison/#schema-drift-detection)).

## What about dbt Fusion?

Fusion is dbt's Rust engine, and its SQL-comprehension layer genuinely closes part of the compile-time gap: in its opt-in `strict` mode it type-checks SQL and computes column-level lineage. Rocky does not claim those as differentiators against Fusion.

The durable difference is what happens *around* the compiler. Rocky treats the run itself as something to enforce and record, not just execute:

```
             ┌────────────────────────────────────────┐
   your ───▶ │ COMPILE  types · contracts · lineage · │──▶ ✗ fails CI
   code      │          dialect-portability lint      │
             └───────────────────┬────────────────────┘
                                 ▼
             ┌────────────────────────────────────────┐
             │ RUN      drift gate · quality checks · │──▶ ✗ blocks the
             │          [budget] fails on overspend   │      build
             └───────────────────┬────────────────────┘
                                 ▼
             ┌────────────────────────────────────────┐
             │ RECORD   per-model cost on every run · │
             │          content-addressed run record ·│
             │          replay                        │
             └────────────────────────────────────────┘
```

Concretely, on top of the compiler Rocky ships: cost budgets that fail the build (no dbt distribution fails a run on overspend), schema-drift detection, declarative governance as Apache-2.0 code (RBAC diffing, masking bound to classification tags), a dialect-portability lint, named branches as a first-class primitive, and a content-addressed record of every run. And there is no Jinja anywhere: your models stay plain SQL. The [Feature Comparison](/getting-started/comparison/) has the precise per-engine breakdown, including where Fusion and SQLMesh are ahead.

## One binary, typed outputs

dbt Core is a Python package; you install it into an environment and parse its logs and artifacts. Rocky is a single Rust binary whose every command emits versioned, typed JSON, which is what the surrounding tooling is built on:

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

Same job: run SQL pipelines in dependency order. Different philosophy:

- **dbt Core** renders templates and lets problems surface in the warehouse, then tests the built data.
- **Rocky** compiles the pipeline as a typed program, refuses to run what it can't verify, and enforces drift, quality, cost, and governance gates on the runs it does allow.

Where to go next:

- [Feature Comparison](/getting-started/comparison/) — the full tables, including dbt Fusion and SQLMesh
- [Migrate from dbt](/guides/migrate-from-dbt/) — `rocky import-dbt` and the migration path
- [The Compiler](/concepts/compiler/) — what actually happens at compile time
- [Quickstart](/getting-started/quickstart/) — a running pipeline in 5 minutes, no credentials needed
