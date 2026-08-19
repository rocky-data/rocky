---
title: Introduction
description: What Rocky is, who it is for, and how far each part has shipped.
sidebar:
  order: 1
---

**Rocky is the typed graph between your code and your warehouse.** You write SQL
and TOML. Rocky compiles them, checks every column type across the whole
dependency graph, and only then sends SQL to the warehouse.

Rocky owns the graph. Your warehouse keeps storage and compute: Databricks,
Snowflake, BigQuery, or DuckDB. Everything between the two is Rocky's job:
branches, run records, column-level lineage, compile-time contracts, a
dialect-portability lint, and per-model cost.

```
   your SQL + rocky.toml
            │
            ▼
   ┌──────────────────┐   type errors, E### ──► fails the PR
   │      Rocky       │───────────────────────────────────┐
   │  compile · plan  │                                   │
   └────────┬─────────┘                                   ▼
            │ dialect SQL                            you fix it
            ▼
   ┌──────────────────┐
   │    warehouse     │  storage + compute stay here
   └──────────────────┘
```

## Why Rocky exists

The expensive failures on a data platform are not slow queries. They are trust
failures. Nothing breaks loudly, and the damage surfaces days later, inside a
number somebody already acted on.

- A source column type changes upstream. A revenue dashboard quietly diverges
  for three days.
- An engineer renames a column on `stg_orders`. 47 downstream models break in
  production.
- A `SELECT *` pulls in a new column nobody designed for. A downstream join
  silently double-counts.
- A Snowflake-only function lands in a Databricks-targeted project. It fails
  only in prod.
- Warehouse spend doubles in a month. Nobody can say which model caused it.
- An auditor asks who changed `fct_revenue.amount`, when, and why. The honest
  answer is `git blame` and screenshots.

**Rocky turns each of these into a compile error or a CI gate.** A column-type
change is `E011` at compile time. A rename's blast radius is a
`rocky lineage-diff` comment on the pull request. An unbudgeted cost spike is a
`[budget]` block that fails the run. Classified data with no mask strategy fails
`rocky compliance`.

None of these are visible to the warehouse. It sees one statement at a time and
has no idea what the previous shape was. Rocky is the layer that does know.

## Who Rocky is for

Rocky is built first for the **lead data platform engineer running
production-critical, multi-tenant pipelines on Databricks**. On those pipelines
a silent failure costs real money. Dagster is already the orchestrator. That is
the wedge, and that is where Rocky has the most production mileage.

The next ring out is **Snowflake and BigQuery teams evaluating SQLMesh**. They
want correctness moved into the compiler rather than the planner. They prefer
SQL over Python-first ergonomics. Those adapters work today, but they are Beta.
See the [Roadmap](/getting-started/roadmap/).

Rocky is **not** a fit for three groups:

- Greenfield analytics shops with no scale pain.
- Single-analyst setups.
- Teams on a warehouse-native pipeline product, such as Databricks LakeFlow or
  Snowflake Dynamic Tables. They will not give up its features for portability
  and compile-time safety.

## What Rocky is, and what it is not

Rocky owns the graph: dependencies, compile-time types, drift handling,
incremental logic, lineage, cost, contracts, and governance.

**Rocky is not a warehouse, not a table format, and not a query engine.**

## Scope on the ELT spectrum

| Stage | Rocky | Notes |
|---|---|---|
| Extract (SaaS sources) | — | Use Fivetran, Airbyte, Stitch, or warehouse-native CDC |
| Extract (files) | ✅ | `rocky load`: CSV, Parquet, JSONL from a directory into the warehouse |
| Load (bronze replication) | ✅ | Config-driven replication pipelines. Discovery via Fivetran metadata, DuckDB `information_schema`, or manual declaration |
| Transform | ✅ | Compiled SQL models; no Jinja, no manifest, no parse step |
| Quality | ✅ | Inline assertions during `rocky apply`; no separate test step |
| Orchestration | Partial | Native Dagster integration; `rocky serve` for small standalone teams |

Quality is more than the inline gate at run time. A model can also declare
fixture-driven unit tests, which `rocky test` runs locally on DuckDB. It can
declare data tests such as not-null and uniqueness, which
`rocky test --declarative` runs against warehouse rows. See
[Testing and Contracts](/concepts/testing/).

## The seven trust primitives

Each item below names the CLI surface or diagnostic code it ships as, so you can
check it rather than take it on faith.

1. **SQL as a typed, compiled language.** Rocky infers column types across the
   whole dependency graph. It reports problems as 35+ diagnostic codes: `E###`
   errors, `W###` warnings, and `P###` portability lints, each with a suggested
   fix. This is a compiler with a language server, not a text-macro engine.
2. **Column-level lineage at compile time.** Rocky traces every column through
   every transformation before anything runs. `rocky lineage-diff main` lists
   the per-column downstream blast radius for a pull request. That CI gate needs
   a compiler; there is no way to do it from templated strings.
3. **Branches and a content-addressed run record.** A named branch is an
   isolated schema: `rocky branch create`, then `rocky run --branch`. Each run
   records the per-model SQL hash, row count, and bytes, and keys the written
   artifacts by the hash of their content. `rocky replay <run_id>` inspects that
   record. For a deterministic model, `rocky replay --execute --verify` re-runs
   the recipe locally on DuckDB and reproduces the output bit for bit. Add
   `--warehouse` to re-run it in an isolated replay schema on the live warehouse
   instead.
4. **Per-model cost attribution.** Cost is a column on every run record, not a
   dashboard you check afterwards. `[budget]` blocks fail the run on overspend,
   `budget_breach` fires the hook, and `rocky preview cost` projects spend at
   pull-request time.
5. **AI gated through the compiler.** Every AI suggestion has to type-check
   before it lands. `rocky ai` generates a model, compiles it, auto-fixes what it
   can, and ships. The `Attempts: 2` retry line is the signature. The wider AI
   surface, such as a mass refactor after a column-type change, is on the
   [Roadmap](/getting-started/roadmap/).
6. **Dialect-divergence lint.** `P001` catches a Snowflake-only construct in a
   Databricks project, and the reverse. It is useful the day you start a
   migration and essential the day you finish one.
7. **Declarative governance.** Roles as code with GRANT/REVOKE diffing, Unity
   Catalog tags, workspace isolation, and mask strategies bound to
   classification tags. `rocky compliance --fail-on exception` gates CI on
   unmasked sensitive columns.

## Where Rocky is today

The trust primitives (compiler, branches, replay, lineage, contracts, cost) are
production-grade on Databricks. Snowflake, BigQuery, and Trino are Beta: the
core run loop works, and conformance coverage is still growing. The wider AI
workflow, Iceberg-native writes, and a semantic layer are on the roadmap.

See the [Roadmap](/getting-started/roadmap/) for the full breakdown.

## What Rocky is like to use

- **Fast.** A single binary that starts in under 100 ms. It compiles 10k models
  in about 1 s with about 150 MB of peak memory. See
  [benchmarks](/getting-started/benchmarks/).
- **Type-safe.** Column-level type inference catches schema errors at compile
  time, before a row is written.
- **SQL-first.** No Jinja. Business logic stays in SQL. An optional Rocky DSL
  exists for the cases plain SQL handles badly; it never replaces SQL.
- **Config-first bronze.** Source replication is driven by `rocky.toml`, with
  zero SQL files for a 1:1 copy.
- **Embedded state.** Watermarks live in a local `redb` database, with optional
  S3 or Valkey sync. There is no manifest file. A watermark is the timestamp of
  the newest row Rocky has already loaded; see the
  [glossary](/reference/glossary/).

## Importing an existing dbt project

`rocky import-dbt` reads a dbt Core project and writes a runnable Rocky repo to
disk. You move over one piece at a time rather than all at once.
`rocky validate-migration` compares the dbt project against the Rocky project
side by side, so you can confirm what changed before you cut over. See the
[migration guide](/guides/migrate-from-dbt/).

Rocky models can also read tables that other tools produce, including dbt
packages, so a hybrid setup works while you migrate.

## How Rocky compares to SQLMesh

SQLMesh is the tool Rocky most resembles. It also analyzes SQL statically, using
SQLGlot rather than templating. Its virtual environments, plan/apply, and
column-level lineage are mature primitives that Rocky shares rather than beats.

Rocky differs in two ways. It keeps SQL as the default surface, where SQLMesh
leans Python-first. And it enforces more. Declarative open-source governance and
`[budget]` blocks that fail the build are not in SQLMesh OSS. Rocky also adds
source-schema-drift detection and a dialect-portability lint at pull-request
time. SQLMesh instead transpiles between dialects with SQLGlot.

SQLMesh is more mature in years, funding, and adoption. It ships native Python
models and an open-source CI/CD bot.

Full side-by-side table: [Feature comparison](/getting-started/comparison/).

## Design principles

1. **Adapter-based.** Source adapters (Fivetran, Airbyte, DuckDB, Iceberg,
   manual) handle discovery. Warehouse adapters (Databricks, Snowflake,
   BigQuery, Trino, DuckDB) handle execution. The core engine stays
   warehouse-agnostic.
2. **Inline quality checks.** Data checks run during replication, not as a
   separate step afterwards.
3. **Structured output.** Every command emits versioned JSON for an
   orchestrator to read.

## Supported adapters

| Role | Adapter | Notes |
|---|---|---|
| Source | Fivetran | REST API discovery; metadata only |
| Source | Airbyte | REST API discovery; metadata only |
| Source | DuckDB | `information_schema` discovery |
| Source | Iceberg | Catalog/manifest discovery for content-addressed reads |
| Source | Manual | Tables declared in `rocky.toml` |
| Warehouse | Databricks | SQL Statement API, Unity Catalog, adaptive concurrency |
| Warehouse | Snowflake | REST API; OAuth / JWT / password (Beta) |
| Warehouse | BigQuery | REST API; service account / ADC (Beta) |
| Warehouse | Trino | `/v1/statement` REST polling; HTTP Basic / JWT (Beta) |
| Warehouse | DuckDB | In-process; powers the playground and `rocky test` |

Source adapters are metadata-only. They identify what exists. The data itself
already lives in the warehouse, or arrives from files through `rocky load`. One
DuckDB instance can act as both source and warehouse, which is how the
credential-free playground runs end to end.

A new adapter plugs in through the [Adapter SDK](/concepts/adapters/) without
changes to the core engine.

## Monorepo layout

| Path | Artifact | Language |
|---|---|---|
| `engine/` | `rocky` CLI | Rust (Cargo workspace) |
| `sdk/python/` | `rocky-sdk` wheel | Python |
| `integrations/dagster/` | `dagster-rocky` wheel | Python |
| `editors/vscode/` | Rocky VSIX | TypeScript |
| `examples/playground/` | POC catalog | TOML / SQL |

Crate-level breakdown: [Architecture](/concepts/architecture/).

## Community

- **Discussions:** [github.com/rocky-data/rocky/discussions](https://github.com/rocky-data/rocky/discussions)
- **Issues:** [github.com/rocky-data/rocky/issues](https://github.com/rocky-data/rocky/issues)
- **Email:** [hello@rocky-data.dev](mailto:hello@rocky-data.dev)
- **Security:** [security@rocky-data.dev](mailto:security@rocky-data.dev) ([SECURITY.md](https://github.com/rocky-data/rocky/blob/main/SECURITY.md))

## License

[Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0).
