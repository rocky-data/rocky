---
title: Introduction
description: What Rocky is and how it compares to dbt
sidebar:
  order: 1
---

Rocky is a **compiled SQL transformation engine** written in Rust. It replaces dbt's core responsibilities — DAG resolution, incremental logic, SQL generation, schema management — with a type-safe, config-driven approach.

No Jinja. No manifest. No parse step.

Rocky follows the **ELT pattern**: it operates on data already in the warehouse, landed by ingestion tools like Fivetran or Airbyte. Rocky owns the **T** (warehouse-side transformation and replication), not the E or L.

## Why Rocky

- **Fast.** Single binary, starts in under 100ms. Compiles 500 models in seconds, not minutes.
- **Type-safe.** Column-level type inference catches schema errors at compile time.
- **Pure SQL.** No Jinja templating; business logic stays in SQL.
- **Config-first bronze.** Source replication is driven by `rocky.toml` — zero SQL files for 1:1 copies.
- **Embedded state.** Watermarks live in a local `redb` database, with optional S3 / Valkey sync.

## Design principles

1. **Adapter-based.** Source adapters (Fivetran, DuckDB, manual) handle discovery. Warehouse adapters (Databricks, Snowflake, BigQuery, DuckDB) handle execution. The core engine is warehouse-agnostic.
2. **Config over code.** Bronze layer replication needs no SQL — just TOML.
3. **Pure SQL models.** Silver-layer transformations use standard SQL plus a `.toml` sidecar.
4. **Inline quality checks.** Data checks run during replication, not as a separate step.
5. **Structured output.** Every command emits versioned JSON for orchestrator consumption.

## How Rocky compares to dbt

| | dbt | Rocky |
|---|---|---|
| Templating | Jinja | None — pure SQL |
| Staging models | One `.sql` per source table | Config-driven bronze (zero SQL) |
| Dependencies | `{{ ref('model') }}` | `depends_on = ["model"]` |
| Tests | `schema.yml` + `dbt test` | Inline checks + assertions built into `rocky run` |
| State | `manifest.json` + `target/` | Embedded `redb` database |

Full side-by-side comparison: [features/comparison](/features/comparison/).

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

Source adapters are metadata-only — they identify what exists. Actual data must already be in the warehouse. A single DuckDB instance can serve as both source and warehouse, which is how the credential-free playground works end-to-end.

New adapters plug in via the [Adapter SDK](/concepts/adapters/) without modifying the core engine.

## Monorepo layout

| Path | Artifact | Language |
|---|---|---|
| `engine/` | `rocky` CLI | Rust (20-crate workspace) |
| `integrations/dagster/` | `dagster-rocky` wheel | Python |
| `editors/vscode/` | Rocky VSIX | TypeScript |
| `examples/playground/` | POC catalog (47 POCs) | TOML / SQL |

Crate-level breakdown: [Architecture](/concepts/architecture/).

## Community

- **Discussions** — [github.com/rocky-data/rocky/discussions](https://github.com/rocky-data/rocky/discussions)
- **Issues** — [github.com/rocky-data/rocky/issues](https://github.com/rocky-data/rocky/issues)
- **Email** — [hello@rocky-data.dev](mailto:hello@rocky-data.dev)
- **Security** — [security@rocky-data.dev](mailto:security@rocky-data.dev) ([SECURITY.md](https://github.com/rocky-data/rocky/blob/main/SECURITY.md))

## License

[Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0).
