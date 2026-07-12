---
title: Introduction
description: What Rocky is, who it's for, and where it is today.
sidebar:
  order: 1
---

**Rocky is the typed graph between your code and whichever warehouse, table format, or query engine you've chosen.** A typed compiler that owns the graph between your code and your data: branches, content-addressed run records, column-level lineage, compile-time contracts, dialect-portability lint, and per-model cost attribution. Storage and compute stay with your warehouse (Databricks, Snowflake, BigQuery, or DuckDB); Rocky owns everything else.

## Why Rocky exists

The expensive failures in modern data platforms aren't slow queries. They're trust failures:

- A source column type changes upstream and a revenue dashboard quietly diverges for three days.
- An engineer renames a column on `stg_orders` and 47 downstream models break in production.
- A `SELECT *` pulls a new column nobody designed for, and a downstream join silently double-counts.
- A Snowflake-only function lands in a Databricks-targeted project and only fails in prod.
- Warehouse spend doubles in a month and nobody can attribute which model caused it.
- An auditor asks who changed `fct_revenue.amount`, when, and why, and the honest answer is `git blame` and screenshots.

**Rocky's answer is to make each of these failures a compile error or a CI gate**, caught before it ships. A column-type change is `E011` at compile; a rename's blast radius is a `rocky lineage-diff` comment on the PR; an unbudgeted cost spike is a `[budget]` block that fails the run; classified PII with no mask strategy fails `rocky compliance`. These are failures the warehouse can't see and the templating layer above it can't catch at compile time. For how Rocky stacks up against dbt Core, dbt Fusion, and SQLMesh, see the [comparison](/getting-started/comparison/).

## Who Rocky is for

Rocky is built first for the **lead data platform engineer running production-critical, multi-tenant pipelines on Databricks**, where dbt Core has hit a ceiling, silent failures cost real money, and Dagster is already the orchestrator. That's the wedge, and that's where Rocky is most battle-tested.

The next ring out is **Snowflake and BigQuery teams evaluating SQLMesh** who want correctness moved to the compiler rather than the planner, and prefer SQL by default over Python-first ergonomics. The adapters work today but are Beta; see the [Roadmap](/getting-started/roadmap/).

Rocky is **not** a fit for: greenfield analytics shops with no scale pain, single-analyst dbt setups, or teams using a warehouse-native pipeline product (Databricks LakeFlow, Snowflake Dynamic Tables) and unwilling to give up its features for portability and compile-time safety.

## What Rocky is

Rocky owns the graph: dependencies, compile-time types, drift handling, incremental logic, lineage, cost, contracts, and governance. Storage and compute stay with your warehouse.

**Rocky is not a warehouse, not a table format, not a query engine.**

## Scope on the ELT spectrum

| Stage | Rocky | Notes |
|---|---|---|
| Extract (SaaS sources) | — | Use Fivetran, Airbyte, Stitch, or warehouse-native CDC |
| Extract (files) | ✅ | `rocky load`: CSV, Parquet, JSONL from a directory into the warehouse |
| Load (bronze replication) | ✅ | Config-driven replication pipelines. Discovery via Fivetran metadata, DuckDB `information_schema`, or manual declaration |
| Transform | ✅ | Compiled SQL models; no Jinja, no manifest, no parse step |
| Quality | ✅ | Inline assertions during `rocky apply`; no separate test step |
| Orchestration | Partial | First-class Dagster integration; `rocky serve` for small standalone teams |

Quality is more than the inline runtime gate. Models can also declare fixture-driven unit tests (`rocky test`, run locally on DuckDB) and declarative data tests like not-null and uniqueness checks against warehouse rows (`rocky test --declarative`). See [Testing and Contracts](/concepts/testing/).

## The trust dimensions

1. **SQL as a typed, compiled language.** Column-level type inference across the full DAG. 35+ diagnostic codes (`E###` errors, `W###` warnings, `P###` portability lints) with actionable suggestions. Not text macros, but a real compiler with a real LSP.
2. **Compile-time column-level lineage.** Every column traced through every transformation, before execution. `rocky lineage-diff main` lists per-column downstream blast radius for PR review. That CI gate is impossible without a compiled engine.
3. **Branches + a content-addressed run record.** Named branches as isolated schemas. `rocky branch create` / `rocky run --branch` / `rocky replay <run_id>`. Each run records per-model SQL hashes, row counts, and bytes, and content-addresses the written artifacts; `rocky replay` inspects that record against the ledger, and `rocky replay --execute --verify` re-runs a single self-contained model on a local DuckDB engine and checks the re-derived hash for bit-exactness. (Multi-model and warehouse-side re-execution from pinned inputs is on the roadmap.)
4. **Per-model cost attribution.** Cost is a column on every run record, not an afterthought dashboard. `[budget]` blocks fail the run on overspend; `budget_breach` fires the hook; `rocky preview cost` projects spend at PR time.
5. **AI gated through the compiler.** Every AI suggestion type-checks before it lands. `rocky ai` generates, compiles, auto-fixes, and ships; the `Attempts: 2` retry loop is the signature feature. (The broader AI surface, like mass refactor or auto-migration on a column-type change, is on the [Roadmap](/getting-started/roadmap/).)
6. **Dialect-divergence lint.** `P001` catches Snowflake-only constructs in a Databricks project, and the reverse. Useful the day you start a migration, essential the day you finish one.
7. **Declarative governance.** RBAC as code with GRANT/REVOKE diffing, Unity Catalog tags, workspace isolation, and mask strategies bound to classification tags. `rocky compliance --fail-on exception` gates CI on unmasked PII.

## Where Rocky is today

The trust primitives (compiler, branches, replay, lineage, contracts, cost) are production-grade on Databricks. Snowflake, BigQuery, and Trino are Beta: the core run loop works, and conformance coverage is still growing. The wider AI workflow, Iceberg-native writes, and a semantic layer are on the roadmap.

See the [Roadmap](/getting-started/roadmap/) for the full breakdown.

## Practical differentiators

- **Fast.** Single binary, starts in under 100ms. Compiles 10k models in ~1 s with ~150 MB peak memory. See [benchmarks](/getting-started/benchmarks/).
- **Type-safe.** Column-level type inference catches schema errors at compile time, before a row is written.
- **Pure SQL.** No Jinja; business logic stays in SQL. An optional Rocky DSL exists for the cases SQL doesn't handle well.
- **Config-first bronze.** Source replication is driven by `rocky.toml`, with zero SQL files for 1:1 copies.
- **Embedded state.** Watermarks live in a local `redb` database, with optional S3 / Valkey sync. No manifest file.

## How Rocky compares

**Coming from dbt?** dbt is the incumbent, not the competitor. Rocky's path is import-compatibility plus an order of magnitude on the failure modes that bite production teams: schema drift, lineage at PR time, cost attribution, and contracts as compile errors. Start with `rocky import-dbt` (see the [migration guide](/guides/migrate-from-dbt/)).

| | dbt Core | Rocky |
|---|---|---|
| Templating | Jinja | None (pure SQL) |
| Staging models | One `.sql` per source table | Config-driven bronze (zero SQL) |
| Dependencies | `{{ ref('model') }}` | `depends_on = ["model"]` |
| Tests | `schema.yml` + `dbt test` | Inline checks + assertions in `rocky run` |
| State | `manifest.json` + `target/` | Embedded `redb` database |
| Branches | — | `rocky branch create`, `rocky run --branch <name>` |
| Column-level lineage | Table-level (`dbt docs`); column-level needs Fusion or paid Catalog | Compile-time output, queryable per column |
| Schema drift | Silent | Detected at run, rebuilt safely |
| Cost attribution | — | Per-model, every run |
| Replay | — | Content-addressed run record; `rocky replay --execute --verify` re-runs single models locally |

**Evaluating SQLMesh?** SQLMesh is the tool Rocky most resembles: it also analyzes SQL statically (via SQLGlot, no Jinja), and its virtual environments, plan/apply, and column-level lineage are mature primitives Rocky shares rather than beats. Rocky keeps SQL as the default (SQLMesh leans Python-first) and differentiates on the enforcement plane: declarative OSS governance and `[budget]` blocks that fail the build (neither in SQLMesh OSS), plus source-schema-drift detection and a dialect-portability lint at PR time (where SQLMesh instead transpiles dialects via SQLGlot). SQLMesh is more mature in years, funding, and adoption, and ships native Python models and an OSS CI/CD bot.

Full side-by-side comparison: [features/comparison](/getting-started/comparison/).

## Design principles

1. **Adapter-based.** Source adapters (Fivetran, Airbyte, DuckDB, Iceberg, manual) handle discovery. Warehouse adapters (Databricks, Snowflake, BigQuery, Trino, DuckDB) handle execution. The core engine is warehouse-agnostic.
2. **Inline quality checks.** Data checks run during replication, not as a separate step.
3. **Structured output.** Every command emits versioned JSON for orchestrator consumption.

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

Source adapters are metadata-only: they identify what exists. The data itself already lives in the warehouse, or is loaded from files via `rocky load`. A single DuckDB instance can serve as both source and warehouse, which is how the credential-free playground works end-to-end.

New adapters plug in via the [Adapter SDK](/concepts/adapters/) without modifying the core engine.

## Monorepo layout

| Path | Artifact | Language |
|---|---|---|
| `engine/` | `rocky` CLI | Rust (24-crate workspace) |
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
