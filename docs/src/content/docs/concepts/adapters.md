---
title: Adapter SDK
description: Building custom warehouse and source adapters
sidebar:
  order: 13
---

## Overview

Rocky's adapter system separates the transformation engine from warehouse-specific logic. Each adapter implements a set of traits from the `rocky-adapter-sdk` crate, declaring its capabilities through an `AdapterManifest`.

## Architecture

```
rocky-core (engine)
    │
    ├── WarehouseAdapter trait ──► rocky-databricks
    ├── SqlDialect trait         ► rocky-snowflake
    ├── DiscoveryAdapter trait    ► rocky-duckdb
    └── GovernanceAdapter trait   ► your-custom-adapter
```

The core engine calls trait methods without knowing which adapter is behind them. This means Rocky can support any SQL warehouse — Databricks, Snowflake, BigQuery, Redshift, DuckDB — through the same interface.

## Adapter Traits

### WarehouseAdapter

The primary trait for executing SQL and managing tables:

```rust
#[async_trait]
pub trait WarehouseAdapter {
    async fn execute_statement(&self, sql: &str) -> Result<()>;
    async fn execute_query(&self, sql: &str) -> Result<QueryResult>;
    async fn describe_table(&self, table: &TableRef) -> Result<Vec<ColumnInfo>>;
    async fn table_exists(&self, table: &TableRef) -> Result<bool>;
    async fn close(&self) -> Result<()>;
}
```

### SqlDialect

Generates warehouse-specific SQL syntax:

```rust
pub trait SqlDialect {
    fn name(&self) -> &str;
    fn format_table_ref(&self, table: &TableRef) -> String;
    fn create_table_as(&self, table: &TableRef, query: &str) -> String;
    fn insert_into(&self, table: &TableRef, query: &str) -> String;
    fn merge_into(&self, target: &TableRef, source: &str, keys: &[String], updates: &[String]) -> String;
    fn row_hash_expr(&self, columns: &[String]) -> String;
    fn watermark_where(&self, column: &str, value: &str) -> String;
    // ... and more
}
```

### Optional Traits

| Trait | Capability | Methods |
|-------|-----------|---------|
| `DiscoveryAdapter` | Discover connectors/tables | `discover()` |
| `GovernanceAdapter` | Tags, grants, bindings | `set_tags()`, `get_grants()`, `apply_grants()`, `revoke_grants()` |
| `BatchCheckAdapter` | Batched quality checks | `batch_row_counts()`, `batch_freshness()` |
| `TypeMapper` | Type normalization | `normalize_type()`, `types_compatible()` |

## AdapterManifest

Each adapter declares what it supports:

```rust
AdapterManifest {
    name: "bigquery",
    version: "0.1.0",
    sdk_version: "0.1.0",
    dialect: "bigquery",
    capabilities: AdapterCapabilities {
        warehouse: true,
        discovery: false,
        governance: true,
        batch_checks: true,
        create_catalog: false,  // BigQuery uses projects
        create_schema: true,    // BigQuery datasets
        merge: true,
        tablesample: true,
    },
    auth_methods: vec!["service_account", "oauth"],
    config_schema: None,
}
```

## Building a Rust Adapter

Scaffold a new adapter:

```bash
rocky init-adapter bigquery
```

This creates `crates/rocky-bigquery/` with:
- `Cargo.toml` depending on `rocky-adapter-sdk`
- `src/lib.rs` with trait implementation stubs
- Conformance test template

Implement the required traits, then run conformance tests:

```bash
rocky test-adapter --adapter bigquery
```

## Process Adapter Protocol

Adapters can be built in **any language** using the process adapter protocol — JSON-RPC 2.0 over stdio.

Rocky spawns the adapter as a child process and communicates via stdin/stdout:

```
Rocky ──stdin──► Adapter Process
Rocky ◄─stdout── Adapter Process
```

### Protocol Flow

1. Rocky sends `initialize` with config → adapter responds with `AdapterManifest`
2. Rocky sends method calls (`execute_statement`, `describe_table`, etc.)
3. Adapter responds with results or errors
4. Rocky sends `shutdown` when done

### Example Request

```json
{"jsonrpc": "2.0", "id": 1, "method": "execute_query", "params": {"sql": "SELECT 1"}}
```

### Example Response

```json
{"jsonrpc": "2.0", "id": 1, "result": {"columns": ["1"], "rows": [["1"]]}}
```

This enables adapters in Python, Go, Java, or any language that can read/write JSON.

## Conformance Tests

The SDK includes 26 test specifications (19 core + 7 optional):

| Category | Core Tests | Optional Tests |
|----------|-----------|----------------|
| Connection | 2 | — |
| DDL | 3 | — |
| DML | 4 | — |
| Query | 3 | — |
| Types | 4 | — |
| Dialect | 3 | — |
| Governance | — | 3 |
| Discovery | — | 2 |
| Batch Checks | — | 2 |

Run them with:

```bash
rocky test-adapter --adapter duckdb
rocky test-adapter --command ./my-adapter-binary
```

Conformance results report pass/fail/skip per test with the adapter's declared capabilities used to determine which optional tests apply.
