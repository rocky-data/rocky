---
title: Introduction
description: What Rocky is and how it compares to dbt
sidebar:
  order: 1
---

Rocky is the **trust system for your data**. Branches, replay, provable reproducibility, complete column-level lineage, compile-time safety, per-model cost attribution — things your current stack can't offer because it doesn't own the DAG.

Keep Databricks or Snowflake. Bring Rocky for the DAG.

## What Rocky is

A Rust-based control plane for warehouse-side data work. Storage and compute stay with your warehouse (Databricks, Snowflake, BigQuery, or DuckDB). Rocky owns the graph — dependencies, compile-time types, drift handling, incremental logic, lineage, cost.

**Rocky is not a warehouse.** It's what sits on top of one.

## Scope on the ELT spectrum

| Stage | Rocky | Notes |
|---|---|---|
| Extract (SaaS sources) | — | Use Fivetran, Airbyte, Stitch, or warehouse-native CDC |
| Extract (files) | ✅ | `rocky load` — CSV, Parquet, JSONL from a directory into the warehouse |
| Load (bronze replication) | ✅ | Config-driven replication pipelines. Discovery via Fivetran metadata, DuckDB `information_schema`, or manual declaration |
| Transform | ✅ | Compiled SQL models — no Jinja, no manifest, no parse step |
| Quality | ✅ | Inline assertions during `rocky run`; no separate test step |
| Orchestration | Partial | First-class Dagster integration; `rocky serve` for small standalone teams |

## The seven trust dimensions

1. **Branches + replay + column-level lineage** — `rocky branch create`, `rocky run --branch`, `rocky replay <run_id>`. Branch and replay workflow on top of your warehouse.
2. **Cost attribution + budgets** — per-model cost on every run. `[budget]` block in `rocky.toml`; `budget_breach` hook event on exceed.
3. **Resume + circuit breakers** — three-state `CircuitBreaker`, checkpointed run state, deploy safety.
4. **Observability** — `rocky trace` Gantt output, OpenTelemetry OTLP export (feature-gated).
5. **Schema-grounded AI** — every AI feature gated through the compiler; generated SQL type-checks before it lands.
6. **Polyglot correctness** — dialect-divergence lint across Databricks / Snowflake / BigQuery / DuckDB (12 constructs today).
7. **SQL as first-class with types** — type inference over raw `.sql`, `SELECT *` blast-radius lint, DAG-aware refactoring.

## Practical differentiators

- **Fast.** Single binary, starts in under 100ms. Compiles 500 models in seconds.
- **Type-safe.** Column-level type inference catches schema errors at compile time.
- **Pure SQL.** No Jinja; business logic stays in SQL.
- **Config-first bronze.** Source replication is driven by `rocky.toml` — zero SQL files for 1:1 copies.
- **Embedded state.** Watermarks live in a local `redb` database, with optional S3 / Valkey sync.

## How Rocky compares to dbt

| | dbt | Rocky |
|---|---|---|
| Templating | Jinja | None — pure SQL |
| Staging models | One `.sql` per source table | Config-driven bronze (zero SQL) |
| Dependencies | `{{ ref('model') }}` | `depends_on = ["model"]` |
| Tests | `schema.yml` + `dbt test` | Inline checks + assertions in `rocky run` |
| State | `manifest.json` + `target/` | Embedded `redb` database |
| Branches | — | `rocky branch create`, `rocky run --branch` |
| Column-level lineage | Post-hoc (`dbt docs`) | Compile-time output |
| Cost attribution | — | Per-model, every run |

Full side-by-side comparison: [features/comparison](/features/comparison/).

## Design principles

1. **Adapter-based.** Source adapters (Fivetran, DuckDB, manual) handle discovery. Warehouse adapters (Databricks, Snowflake, BigQuery, DuckDB) handle execution. The core engine is warehouse-agnostic.
2. **Config over code.** Bronze layer replication needs no SQL — just TOML.
3. **Pure SQL models.** Silver-layer transformations use standard SQL plus a `.toml` sidecar.
4. **Inline quality checks.** Data checks run during replication, not as a separate step.
5. **Structured output.** Every command emits versioned JSON for orchestrator consumption.

## Supported adapters

| Role | Adapter | Notes |
|---|---|---|
| Source | Fivetran | REST API discovery; metadata only |
| Source | DuckDB | `information_schema` discovery |
| Source | Manual | Tables declared in `rocky.toml` |
| Warehouse | Databricks | SQL Statement API, Unity Catalog, adaptive concurrency |
| Warehouse | Snowflake | REST API; OAuth / JWT / password (Beta) |
| Warehouse | BigQuery | REST API; service account / ADC (Beta) |
| Warehouse | DuckDB | In-process; powers the playground and `rocky test` |

Source adapters are metadata-only — they identify what exists. Actual data already lives in the warehouse, or is loaded via `rocky load` for files. A single DuckDB instance can serve as both source and warehouse, which is how the credential-free playground works end-to-end.

New adapters plug in via the [Adapter SDK](/concepts/adapters/) without modifying the core engine.

## Monorepo layout

| Path | Artifact | Language |
|---|---|---|
| `engine/` | `rocky` CLI | Rust (20-crate workspace) |
| `integrations/dagster/` | `dagster-rocky` wheel | Python |
| `editors/vscode/` | Rocky VSIX | TypeScript |
| `examples/playground/` | POC catalog | TOML / SQL |

Crate-level breakdown: [Architecture](/concepts/architecture/).

## Community

- **Discussions** — [github.com/rocky-data/rocky/discussions](https://github.com/rocky-data/rocky/discussions)
- **Issues** — [github.com/rocky-data/rocky/issues](https://github.com/rocky-data/rocky/issues)
- **Email** — [hello@rocky-data.dev](mailto:hello@rocky-data.dev)
- **Security** — [security@rocky-data.dev](mailto:security@rocky-data.dev) ([SECURITY.md](https://github.com/rocky-data/rocky/blob/main/SECURITY.md))

## License

[Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0).
