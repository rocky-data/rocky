---
title: Introduction
description: What Rocky is, who it's for, and where it is today.
sidebar:
  order: 1
---

**Rocky is the typed graph between your code and whichever warehouse, table format, or query engine you've chosen.** A typed compiler that owns the graph between your code and your data: branches, deterministic replay, column-level lineage, compile-time contracts, dialect-portability lint, and per-model cost attribution. Storage and compute stay with your warehouse (Databricks, Snowflake, BigQuery, or DuckDB); Rocky owns everything else.

## Why Rocky exists

The expensive failures in modern data platforms aren't slow queries. They're trust failures:

- A source column type changes upstream and a revenue dashboard quietly diverges for three days.
- An engineer renames a column on `stg_orders` and 47 downstream models break in production.
- A `SELECT *` pulls a new column nobody designed for, and a downstream join silently double-counts.
- A Snowflake-only function lands in a Databricks-targeted project and only fails in prod.
- Warehouse spend doubles in a month and nobody can attribute which model caused it.
- An auditor asks who changed `fct_revenue.amount`, when, and why, and the honest answer is `git blame` and screenshots.

dbt Core is a templating engine by design, so it can't catch any of these at compile time. dbt Fusion (dbt Labs' Rust rewrite of dbt Core, in public beta since 2025-05-28) catches some compile-time issues, but it still templates with Jinja. It doesn't ship named branches, content-addressed replay, per-model cost attribution, dialect-portability lint, or declarative governance and masking outside dbt platform's paid tiers. SQLMesh moved correctness to the planner. **Rocky owns the trust dimensions all of them leave open.** Each failure above maps to a diagnostic code, a CI gate, or a content-addressed replay artifact.

## Who Rocky is for

Rocky is built first for the **lead data platform engineer running production-critical, multi-tenant pipelines on Databricks**, where dbt Core has hit a ceiling, silent failures cost real money, and Dagster is already the orchestrator. That's the wedge, and that's where Rocky is most battle-tested.

The next ring out is **Snowflake and BigQuery teams evaluating SQLMesh** who want correctness moved to the compiler rather than the planner, and prefer SQL by default over Python-first ergonomics. The adapters work today but are Beta; see the [Roadmap](/getting-started/roadmap/).

Rocky is **not** a fit for: greenfield analytics shops with no scale pain, single-analyst dbt setups, or teams using a warehouse-native pipeline product (Databricks LakeFlow, Snowflake Dynamic Tables) and unwilling to give up its features for portability and compile-time safety.

## What Rocky is

A typed compiler that drives your warehouse. Storage and compute stay where they are. Rocky owns the graph: dependencies, compile-time types, drift handling, incremental logic, lineage, cost, contracts, and governance.

**Rocky is not a warehouse, not a table format, not a query engine.** It's the typed graph between your code and whichever of those you've chosen.

## Scope on the ELT spectrum

| Stage | Rocky | Notes |
|---|---|---|
| Extract (SaaS sources) | — | Use Fivetran, Airbyte, Stitch, or warehouse-native CDC |
| Extract (files) | ✅ | `rocky load`: CSV, Parquet, JSONL from a directory into the warehouse |
| Load (bronze replication) | ✅ | Config-driven replication pipelines. Discovery via Fivetran metadata, DuckDB `information_schema`, or manual declaration |
| Transform | ✅ | Compiled SQL models; no Jinja, no manifest, no parse step |
| Quality | ✅ | Inline assertions during `rocky apply`; no separate test step |
| Orchestration | Partial | First-class Dagster integration; `rocky serve` for small standalone teams |

## The seven trust dimensions

1. **SQL as a typed, compiled language.** Column-level type inference across the full DAG. 35+ diagnostic codes (`E001`–`E026`, `W001`–`W011`, `P001`–`P002`) with actionable suggestions. Not text macros, but a real compiler with a real LSP.
2. **Compile-time column-level lineage.** Every column traced through every transformation, before execution. `rocky lineage-diff main` lists per-column downstream blast radius for PR review. That CI gate is impossible without a compiled engine.
3. **Branches + deterministic replay.** Named branches as isolated schemas. `rocky branch create` / `rocky run --branch` / `rocky replay <run_id>`. Replay is content-addressed: inputs and code map to outputs as a pure function, recorded.
4. **Per-model cost attribution.** Cost is a column on every run record, not an afterthought dashboard. `[budget]` blocks fail the run on overspend; `budget_breach` fires the hook; `rocky preview cost` projects spend at PR time.
5. **AI gated through the compiler.** Every AI suggestion type-checks before it lands. `rocky ai` generates, compiles, auto-fixes, and ships; the `Attempts: 2` retry loop is the signature feature. (The broader AI surface, like mass refactor or auto-migration on a column-type change, is on the [Roadmap](/getting-started/roadmap/).)
6. **Dialect-divergence lint.** `P001` catches Snowflake-only constructs in a Databricks project, and the reverse. Useful the day you start a migration, essential the day you finish one.
7. **Declarative governance.** RBAC as code with GRANT/REVOKE diffing, Unity Catalog tags, workspace isolation, and mask strategies bound to classification tags. `rocky compliance --fail-on exception` gates CI on unmasked PII.

## Where Rocky is today

The trust primitives (compiler, branches, replay, lineage, contracts, cost) are production-grade on Databricks. Snowflake, BigQuery, and Trino are Beta: the core run loop works, and conformance coverage is still growing. The wider AI workflow, Iceberg-native writes, and a semantic layer are on the roadmap.

See the [Roadmap](/getting-started/roadmap/) for the full breakdown of what's shipped, what's Beta, and what's coming.

## Practical differentiators

- **Fast.** Single binary, starts in under 100ms. Compiles 10k models in ~1 s with ~150 MB peak memory. See [benchmarks](/features/benchmarks/).
- **Type-safe.** Column-level type inference catches schema errors at compile time, before a row is written.
- **Pure SQL.** No Jinja; business logic stays in SQL. An optional Rocky DSL exists for the cases SQL doesn't handle well.
- **Config-first bronze.** Source replication is driven by `rocky.toml`, with zero SQL files for 1:1 copies.
- **Embedded state.** Watermarks live in a local `redb` database, with optional S3 / Valkey sync. No manifest file.

## How Rocky compares

**Coming from dbt?** dbt is the incumbent, not the competitor. Rocky's path is import-compatibility plus an order of magnitude on the failure modes that bite production teams: schema drift, lineage at PR time, cost attribution, and contracts as compile errors. Start with `rocky import-dbt` (see the [migration guide](/guides/migrate-from-dbt/)).

| | dbt | Rocky |
|---|---|---|
| Templating | Jinja | None (pure SQL) |
| Staging models | One `.sql` per source table | Config-driven bronze (zero SQL) |
| Dependencies | `{{ ref('model') }}` | `depends_on = ["model"]` |
| Tests | `schema.yml` + `dbt test` | Inline checks + assertions in `rocky run` |
| State | `manifest.json` + `target/` | Embedded `redb` database |
| Branches | — | `rocky branch create`, `rocky run --branch <name>` |
| Column-level lineage | Post-hoc (`dbt docs`) | Compile-time output, queryable per column |
| Schema drift | Silent | `E013` at compile, blocks the PR |
| Cost attribution | — | Per-model, every run |
| Replay | — | Content-addressed, deterministic |

**Evaluating SQLMesh?** SQLMesh moved correctness to the planner. Rocky moved it to the compiler. SQLMesh's checks are runtime-ish; Rocky's are compile-time. SQLMesh is Python-first; Rocky keeps SQL as the default and reaches for a DSL only when SQL doesn't fit. SQLMesh has virtual environments, a genuinely good idea that Rocky's branches express differently. SQLMesh is more mature in years and funding; Rocky is further along on Rust-native compile-time enforcement, the LSP and IDE experience, column-level lineage at compile time, and cost attribution as a first-class citizen.

Full side-by-side comparison: [features/comparison](/features/comparison/).

## Design principles

1. **Adapter-based.** Source adapters (Fivetran, Airbyte, DuckDB, Iceberg, manual) handle discovery. Warehouse adapters (Databricks, Snowflake, BigQuery, Trino, DuckDB) handle execution. The core engine is warehouse-agnostic.
2. **Config over code.** Bronze layer replication needs no SQL, just TOML.
3. **Pure SQL models.** Silver-layer transformations use standard SQL plus a `.toml` sidecar.
4. **Inline quality checks.** Data checks run during replication, not as a separate step.
5. **Structured output.** Every command emits versioned JSON for orchestrator consumption.

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
| `engine/` | `rocky` CLI | Rust (23-crate workspace) |
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
