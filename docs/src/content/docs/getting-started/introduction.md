---
title: Introduction
description: What Rocky is, who it is for, and how far each part has shipped.
sidebar:
  order: 1
---

**Rocky is the typed graph between your code and your warehouse.** You write SQL and TOML. Rocky compiles them and reports supported static problems across the project graph. A later run sends generated SQL to the warehouse.

Your warehouse keeps storage and compute: Databricks, Snowflake, BigQuery, DuckDB, or another [supported adapter](/guides/adapter-sdk/). Rocky owns the graph between the two.

```
   your SQL + rocky.toml
            │
            ▼
   ┌──────────────────┐   compile errors, E### ──► you fix the project
   │      Rocky       │
   │  compile · plan  │
   └────────┬─────────┘
            │ dialect SQL
            ▼
   ┌──────────────────┐
   │    warehouse     │  storage + compute stay here
   └──────────────────┘
```

Rocky is not a warehouse, not a table format, and not a query engine.

## Start here

| Step | Page |
|---|---|
| Install | [Installation](/getting-started/installation/) |
| Run a first project, no credentials | [Playground](/guides/playground/) |
| Copy real tables into a warehouse | [Quickstart](/getting-started/quickstart/) |
| See the project in a browser | [Browser UI](/guides/browser-ui/): `rocky serve --ui` |
| Get types and errors in the editor | [VS Code extension](/guides/ide-setup/) |
| Move over a dbt Core project | [Migrate from dbt](/guides/migrate-from-dbt/) |

## Why Rocky exists

The expensive failures on a data platform are not slow queries. They are trust failures. Nothing breaks loudly, and the damage surfaces days later.

- A source column type changes upstream. A revenue dashboard quietly diverges.
- An engineer renames a column on `stg_orders`. 47 downstream models break in production.
- A Snowflake-only function lands in a Databricks project. It fails only in prod.
- Warehouse spend doubles. Nobody can say which model caused it.

Rocky can report or gate selected failures of this kind. It checks only what it can resolve from your SQL, schemas, and configuration.

## The trust primitives

Each item names the CLI surface or code it ships as.

| Primitive | Surface |
|---|---|
| **Typed SQL.** Types are inferred where source and expression information exists. Problems are `E###` errors, `W###` warnings, and `P###` lints, each with a suggested fix. | `rocky compile` |
| **Column-level lineage.** Rocky traces represented columns through supported transformations. | `rocky lineage-diff main` |
| **Branches and run records.** A branch is an isolated schema. Each run records per-model SQL hashes, row counts, and bytes. | `rocky branch create`, `rocky replay <run_id>` |
| **Per-model cost.** Cost is recorded on every run. `[budget]` blocks can fail a run. An allocation is not a warehouse bill. | `rocky cost` |
| **AI-assisted drafting.** The result is a draft. Review the files, run checks, then plan and approve. | `rocky ai` |
| **Dialect lint.** `P001` catches a Snowflake-only construct in a Databricks project, and the reverse. | `rocky compile` |
| **Declarative governance.** Roles, tags, and mask strategies as code. | `rocky compliance --fail-on exception` |

A clean compile does not prove that every warehouse query is valid. It does not prove the result values are correct. Add [tests and contracts](/concepts/data-quality-checks/) for that.

## Where Rocky is today

| Area | Status |
|---|---|
| Compiler, branches, replay, lineage, contracts, cost on Databricks | GA (live-tested) |
| DuckDB | Supported. Powers the playground and `rocky test`. |
| Every other warehouse adapter | Beta. The core run loop works. Conformance coverage is still growing. |
| Wider AI workflow, Iceberg-native writes, semantic layer | [Roadmap](/getting-started/roadmap/) |

Adapter list and capabilities: [Capability matrix](/concepts/capability-matrix/).

## Who Rocky is for

Rocky is built first for the lead data platform engineer who runs production-critical, multi-tenant pipelines on Databricks. A silent failure costs real money there, and Rocky has the most production mileage there.

The next ring out is Snowflake and BigQuery teams evaluating SQLMesh. See the [comparison](/getting-started/comparison/).

Rocky is **not** a fit for these groups:

- Greenfield analytics shops with no scale pain.
- Single-analyst setups.
- Teams on a warehouse-native pipeline product, such as Databricks LakeFlow or Snowflake Dynamic Tables.

## What Rocky does not do

| Stage | Rocky |
|---|---|
| Extract from SaaS sources | No. Use Fivetran, Airbyte, Stitch, or warehouse-native CDC. |
| Extract from files | Yes. `rocky load` reads CSV, Parquet, and JSONL. |
| Load (bronze replication) | Yes. Config-driven replication pipelines. |
| Transform | Yes. Compiled SQL models. No Jinja, no manifest. |
| Quality | Yes. Inline checks during `rocky apply`, plus [declarative tests](/concepts/data-quality-checks/). |
| Orchestration | Partial. Native Dagster integration, and `rocky serve` for small standalone teams. |

## Next

- [Quickstart](/getting-started/quickstart/)
- [Architecture](/concepts/architecture/)
- [Roadmap](/getting-started/roadmap/)
- [Benchmarks](/getting-started/benchmarks/)
- Questions: [GitHub Discussions](https://github.com/rocky-data/rocky/discussions), [hello@rocky-data.dev](mailto:hello@rocky-data.dev). Security: [SECURITY.md](https://github.com/rocky-data/rocky/blob/main/SECURITY.md). License: Apache 2.0.
