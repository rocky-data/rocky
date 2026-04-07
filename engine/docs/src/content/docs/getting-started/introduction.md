---
title: Introduction
description: What is Rocky and how it compares to dbt
sidebar:
  order: 1
---

Rocky is a **compiled SQL transformation engine** written in Rust. It replaces dbt's core responsibilities — DAG resolution, incremental logic, SQL generation, and schema management — with a type-safe, config-driven approach.

No Jinja. No manifest. No parse step.

Rocky follows the **ELT pattern** — it operates on data already in your warehouse, landed by ingestion tools like Fivetran or Airbyte. Rocky handles the **T** (transformation and replication within the warehouse), not the **E** or **L**.

## Why Rocky?

Traditional tools like dbt work well at small scale, but introduce significant overhead as pipelines grow:

- **Slow startup**: dbt takes 30-60 seconds just to start, then 2-3 minutes to parse 500 models
- **Memory-hungry**: A mid-size dbt project consumes 500MB-2GB of RAM
- **Jinja complexity**: Business logic buried in template macros is hard to test and debug
- **Repetitive staging**: Writing `SELECT * FROM {{ source(...) }}` for every source table

Rocky eliminates these problems by compiling to a single binary that starts in under 100ms, parses instantly, and uses pure SQL instead of Jinja templates.

## Key Design Principles

1. **Adapter-based architecture**: Source adapters (Fivetran, DuckDB, manual) handle discovery; warehouse adapters (Databricks, Snowflake, DuckDB) handle execution. The core engine is warehouse-agnostic.
2. **Config over code**: The bronze layer (source replication) is driven entirely by `rocky.toml` — no SQL files needed for 1:1 copies
3. **Pure SQL**: Transformation models use standard SQL with TOML configuration — no templating language
4. **Inline quality checks**: Data checks run during replication, not as a separate step
5. **Structured output**: All CLI output is versioned JSON, designed for orchestrator integration
6. **Embedded state**: Watermarks stored in a local `redb` database, with optional remote sync to S3 or Valkey

## How Rocky Compares to dbt

| Concept | dbt | Rocky |
|---------|-----|-------|
| Project config | `dbt_project.yml` + `profiles.yml` | `rocky.toml` |
| Connection config | `profiles.yml` | `[adapter.NAME]` blocks in `rocky.toml` |
| Sources | `schema.yml` with `source()` macro | Auto-discovered (Fivetran, DuckDB, manual) |
| Staging models | One `.sql` file per source table | Config-driven bronze layer (zero SQL) |
| Transformation models | SQL + Jinja `{{ ref() }}` | SQL + TOML config |
| Materialization config | `{{ config(materialized='...') }}` | `[strategy]` in TOML |
| Dependencies | `{{ ref('model') }}` | `depends_on = ["model"]` |
| Macros | Jinja macros | Not needed — pure SQL |
| Seeds | CSV files loaded as tables | Not supported (out of scope) |
| Snapshots | SCD Type 2 via snapshots | `merge` strategy with unique key |
| Tests | `schema.yml` tests | Inline checks in `rocky.toml` |
| Compile | `dbt compile` | `rocky plan` |
| Run | `dbt run` | `rocky run` |
| Test separately | `dbt test` | Built into `rocky run` |
| State | `manifest.json` + `target/` | Embedded `redb` database |

## Supported Adapters

Rocky's adapter model separates *where data is discovered* from *where data is processed*:

| Role | Adapter | Description |
|------|---------|-------------|
| Source | Fivetran | Calls Fivetran REST API to discover connectors and enabled tables |
| Source | DuckDB | Lists schemas and tables from a local DuckDB database via `information_schema` |
| Source | Manual | Reads schema/table definitions from `rocky.toml` config |
| Warehouse | Databricks | Executes SQL via SQL Statement API, manages Unity Catalog governance |
| Warehouse | Snowflake | Executes SQL via Snowflake REST API; OAuth, key-pair JWT, and password auth |
| Warehouse | DuckDB | Local in-process execution — runs `rocky validate/discover/plan/run` end-to-end with no credentials |

Source adapters are metadata-only — they identify what schemas and tables exist, they do not extract or move data. The actual data must already be in the warehouse, landed by an ingestion tool.

A single DuckDB adapter instance can act as both source and warehouse, which is how the playground and credential-free examples run end-to-end.

Rocky is designed for extensibility — new source and warehouse adapters can be added through the [Adapter SDK](/rocky/concepts/adapters/) without modifying the core engine.

## Architecture

Rocky is a Cargo workspace with focused crates:

- **rocky-core** — Warehouse-agnostic transformation engine: IR, schema patterns, SQL generation, checks, state
- **rocky-sql** — SQL parsing and validation (built on `sqlparser-rs`)
- **rocky-lang** + **rocky-compiler** — Rocky DSL parser, lexer, semantic graph, type checker
- **rocky-adapter-sdk** — Traits and conformance tests for building custom warehouse adapters
- **rocky-databricks** — Databricks warehouse adapter: SQL Statement API, Unity Catalog governance, workspace bindings
- **rocky-snowflake** — Snowflake warehouse adapter: REST API, OAuth + key-pair JWT auth
- **rocky-duckdb** — DuckDB warehouse + discovery adapter, also used by `rocky test` and the bundled E2E suite
- **rocky-fivetran** — Fivetran source adapter: REST API discovery (metadata only), schema config, sync detection
- **rocky-engine** — Local query engine (DataFusion + Arrow) used for type inference and `rocky test`
- **rocky-server** — HTTP API and LSP server (`rocky serve` / `rocky lsp`)
- **rocky-cache** — Three-tier caching (memory, Valkey, API)
- **rocky-ai** — AI intent layer (`explain`, `sync`, `test`, `generate`)
- **rocky-observe** — Metrics, event bus, and structured JSON logging
- **rocky-cli** — CLI commands and output formatting
- **rocky** — The binary crate

## License

Rocky is licensed under [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0).
