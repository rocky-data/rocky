---
title: Architecture
description: How Rocky's crates fit together
sidebar:
  order: 1
---

Rocky is a Cargo workspace composed of several crates, each with a focused responsibility. This page describes what each crate does and how they connect.

## Adapter model

Rocky separates concerns through two adapter types:

**Source adapters** handle *discovery* — finding what schemas and tables exist and are available for processing. They do NOT extract data. The data must already be in the warehouse, landed by an ingestion tool (Fivetran, Airbyte, etc.) or loaded manually.

- `rocky-fivetran` — Calls the Fivetran REST API to list connectors and their enabled tables in the destination
- Manual source (built into `rocky-core`) — Reads schema/table definitions from `[source.schemas]` in `rocky.toml`

**Warehouse adapters** handle *execution* — running SQL, managing catalog lifecycle, and applying governance (tags, permissions, workspace isolation).

- `rocky-databricks` — Executes via the Databricks SQL Statement API and manages Unity Catalog

**rocky-core** sits between them. It defines the Intermediate Representation (IR) and all warehouse-agnostic logic (DAG resolution, schema pattern parsing, SQL generation templates, checks, contracts, state management). rocky-core has no knowledge of Databricks, Fivetran, or any specific system.

This architecture means adding a new warehouse (e.g., Snowflake) or a new source (e.g., Airbyte) requires implementing an adapter crate without modifying the core engine.

## Crate overview

```
rocky/
├── crates/
│   ├── rocky-core/          # Generic SQL transformation engine
│   ├── rocky-sql/           # SQL parsing + typed AST
│   ├── rocky-databricks/    # Databricks warehouse adapter
│   ├── rocky-fivetran/      # Fivetran source adapter
│   ├── rocky-cache/         # Three-tier caching
│   ├── rocky-duckdb/        # DuckDB local execution adapter
│   ├── rocky-observe/       # Observability
│   └── rocky-cli/           # CLI framework
└── rocky/                   # Binary crate
```

### rocky-core

The warehouse-agnostic transformation engine. This crate has no knowledge of specific warehouses or sources — it works entirely through adapter abstractions, producing IR that warehouse adapters consume.

Key modules:

- **ir.rs** — Intermediate Representation types: `Plan`, `ReplicationPlan`, `TransformationPlan`, `MaterializationStrategy`
- **schema.rs** — Configurable schema pattern parsing (e.g., `src__acme__us_west__shopify` into structured components)
- **drift.rs** — Schema drift detection (compares column types between source and target)
- **sql_gen.rs** — IR to dialect-specific SQL generation
- **state.rs** — Embedded state store backed by `redb` for watermarks and run history
- **state_sync.rs** — Remote state persistence: download/upload state from S3, Valkey, or tiered (Valkey + S3)
- **catalog.rs** — Catalog and schema lifecycle management (CREATE IF NOT EXISTS, tagging)
- **checks.rs** — Inline data quality checks (row counts, column matching, freshness, null rate, custom)
- **contracts.rs** — Data contracts (required columns, protected columns, allowed type changes)
- **dag.rs** — DAG resolution for model dependencies (topological sort)
- **models.rs** — SQL model loading (sidecar `.sql` + `.toml` files)
- **source.rs** — Source adapter traits and manual source configuration
- **config.rs** — TOML configuration parsing with environment variable substitution (`${VAR}` and `${VAR:-default}`)

### rocky-sql

SQL parsing and validation built on `sqlparser-rs`.

- **parser.rs** — Wraps sqlparser-rs with typed extensions for Rocky's needs
- **dialect.rs** — Databricks SQL dialect support
- **validation.rs** — SQL identifier validation using strict regex patterns. All identifiers must pass through this module before being interpolated into SQL. This prevents SQL injection by rejecting anything that doesn't match `^[a-zA-Z0-9_]+$`.

### rocky-databricks

The Databricks warehouse adapter. Implements the warehouse traits defined in rocky-core.

- **connector.rs** — SQL Statement Execution REST API client (`POST /api/2.0/sql/statements`, polling for results)
- **catalog.rs** — Unity Catalog CRUD operations, tagging, and catalog isolation
- **permissions.rs** — GRANT/REVOKE execution, SHOW GRANTS parsing
- **workspace.rs** — Workspace binding management for catalog isolation
- **auth.rs** — Authentication with auto-detection: tries PAT (`DATABRICKS_TOKEN`) first, falls back to OAuth M2M (`DATABRICKS_CLIENT_ID` + `DATABRICKS_CLIENT_SECRET`)
- **batch.rs** — Batched information_schema queries using UNION ALL (batches of 200)

### rocky-fivetran

The Fivetran source adapter. Discovers what schemas and tables exist in the Fivetran destination. This is a metadata-only operation — the actual data is already in the warehouse, landed by Fivetran's sync process.

- **client.rs** — Async REST client using reqwest with Basic Auth
- **connector.rs** — Connector discovery and filtering
- **schema.rs** — Schema configuration parsing (nested JSON structures from Fivetran's API)
- **pagination.rs** — Cursor-based pagination for large result sets
- **sync.rs** — Sync detection via timestamp comparison (determines if new data is available)

### rocky-cache

Three-tier caching system that reduces API calls and speeds up repeated operations.

- **memory.rs** — In-process LRU cache with configurable TTL
- **valkey.rs** — Valkey/Redis distributed cache with distributed locks
- **tiered.rs** — Fallback chain: memory -> Valkey -> API. A cache miss at one tier populates all tiers above it.

### rocky-duckdb

DuckDB local execution adapter. Minimal implementation providing a local warehouse backend for development and testing without requiring a Databricks connection.

### rocky-observe

Observability infrastructure.

- **metrics.rs** — In-process metrics collection: counters (tables processed/failed, statements executed, retries, anomalies) and duration histograms (p50/p95/max for tables and queries). Thread-safe via atomics, serialized to JSON in run output.
- **tracing.rs** — Structured JSON logging via the `tracing` crate
- **events.rs** — Event broadcasting over Valkey Pub/Sub for real-time monitoring

### rocky-cli

CLI framework built on `clap`.

- **commands.rs** — Command implementations: `discover`, `plan`, `run`, `state`, `init`, `validate`
- **output.rs** — JSON and table formatters for CLI output

### rocky (binary)

The `rocky` binary crate. Contains only `main.rs`, which wires all the library crates together and dispatches CLI commands.

## Intermediate Representation (IR)

Rocky compiles configuration and SQL into an intermediate representation before generating executable SQL. This separation means the core engine never deals with raw strings — everything is typed and validated.

### Plan

The top-level `Plan` enum represents what Rocky will execute:

```rust
enum Plan {
    Replication(ReplicationPlan),
    Transformation(TransformationPlan),
}
```

- **ReplicationPlan** — A config-driven copy from source to target (the bronze layer). Contains source/target table references, the incremental strategy, metadata columns, and quality checks.
- **TransformationPlan** — A user-written SQL model (the silver layer). Contains the parsed SQL, target table, materialization strategy, and dependency references.

### MaterializationStrategy

Controls how data is written to the target table:

```rust
enum MaterializationStrategy {
    FullRefresh,
    Incremental {
        timestamp_column: String,
        watermark: Option<DateTime>,
    },
    Merge {
        unique_key: Vec<String>,
        update_columns: Option<Vec<String>>,
    },
}
```

- **FullRefresh** — `CREATE OR REPLACE TABLE ... AS SELECT ...`. Rebuilds the entire table on every run.
- **Incremental** — `INSERT INTO ... SELECT ... WHERE ts > watermark`. Only processes new rows. The watermark is read from the embedded state store.
- **Merge** — `MERGE INTO ... USING (...) ON key WHEN MATCHED THEN UPDATE WHEN NOT MATCHED THEN INSERT`. Upserts based on a unique key.

The IR is generated by rocky-core and consumed by `sql_gen.rs`, which produces dialect-specific SQL strings ready for execution.

### Adapter SDK

The `rocky-adapter-sdk` crate provides stable, versioned traits for building custom warehouse adapters:

- **`WarehouseAdapter`** — execute SQL, describe tables, manage catalog objects
- **`SqlDialect`** — format SQL for a specific warehouse (table refs, DDL, DML, type mapping)
- **`DiscoveryAdapter`** — discover connectors and tables from a source
- **`GovernanceAdapter`** — manage tags, grants, and workspace bindings
- **`AdapterManifest`** — declares capabilities per adapter (which traits it implements)

Adapters can be built in Rust (direct trait implementation) or in any language via the **process adapter protocol** (JSON-RPC over stdio).

Scaffold a new adapter:
```bash
rocky init-adapter bigquery
```

Run conformance tests:
```bash
rocky test-adapter --adapter duckdb
```
