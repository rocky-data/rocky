---
title: Adapter SDK
description: Building custom warehouse and source adapters
sidebar:
  order: 13
---

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

The core engine calls trait methods without knowing which adapter is behind them. This means Rocky can support any SQL warehouse (Databricks, Snowflake, BigQuery, Redshift, DuckDB) through the same interface.

## Adapter traits

### WarehouseAdapter

The primary trait for executing SQL and managing tables:

```rust
#[async_trait]
pub trait WarehouseAdapter: Send + Sync {
    fn dialect(&self) -> &dyn SqlDialect;
    async fn execute_statement(&self, sql: &str) -> AdapterResult<()>;
    async fn execute_query(&self, sql: &str) -> AdapterResult<QueryResult>;
    async fn describe_table(&self, table: &TableRef) -> AdapterResult<Vec<ColumnInfo>>;
    async fn table_exists(&self, table: &TableRef) -> AdapterResult<bool>;
    async fn close(&self) -> AdapterResult<()>;
    // ... plus defaulted methods (ping, explain, execute_statement_with_stats)
}
```

### SqlDialect

Generates warehouse-specific SQL syntax:

```rust
pub trait SqlDialect: Send + Sync {
    fn name(&self) -> &str;
    fn format_table_ref(&self, catalog: &str, schema: &str, table: &str) -> AdapterResult<String>;
    fn create_table_as(&self, target: &str, select_sql: &str) -> String;
    fn insert_into(&self, target: &str, select_sql: &str) -> String;
    fn merge_into(&self, target: &str, source_sql: &str, keys: &[String], update_cols: Option<&[String]>) -> AdapterResult<String>;
    fn row_hash_expr(&self, columns: &[String]) -> String;
    fn watermark_where(&self, timestamp_col: &str, last_watermark: Option<&DateTime<Utc>>) -> AdapterResult<String>;
    // ... and more
}
```

### Optional traits

| Trait | Capability | Methods |
|-------|-----------|---------|
| `DiscoveryAdapter` | Discover connectors/tables | `discover() -> DiscoveryResult` |
| `GovernanceAdapter` | Tags, grants, bindings | `set_tags()`, `get_grants()`, `apply_grants()`, `revoke_grants()` |
| `BatchCheckAdapter` | Batched quality checks | `batch_row_counts()`, `batch_freshness()` |
| `TypeMapper` | Type normalization | `normalize_type()`, `types_compatible()` |

`DiscoveryAdapter::discover` returns `DiscoveryResult { connectors, failed }` so adapters that fan out per-source metadata fetches (per-connector REST calls, per-namespace `list_tables`) can surface partial failures instead of silently dropping them, protecting downstream diff-based reconcilers from misreading a transient fetch failure as "removed upstream". Adapters that complete in a single shot return `DiscoveryResult::ok(connectors)`. Each `FailedSource` carries an `error_class` (`transient` / `timeout` / `rate_limit` / `auth` / `unknown`) so consumers can branch on operating-mode without parsing free-form messages.

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
        file_load: false,
    },
    auth_methods: vec!["service_account", "oauth"],
    config_schema: serde_json::json!({}),
}
```

## Building a Rust adapter

Scaffold a new adapter:

```bash
rocky init-adapter bigquery
```

This creates `crates/rocky-bigquery/` with:
- `Cargo.toml` depending on `rocky-core` and `rocky-sql`
- `src/lib.rs` declaring the `adapter`, `dialect`, and `types` modules
- `src/{dialect,adapter,types}.rs` trait implementation stubs
- `tests/integration.rs` — an `#[ignore]`d live-connection test stub

Implement the required traits, then run the conformance suite. Note that
`test-adapter --adapter <name>` only resolves the builtins (`databricks`,
`snowflake`, `duckdb`) or a `rocky-<name>` process-adapter binary on your
`PATH`; use a builtin to see the suite, or expose your adapter as a process
adapter (or `--command`) to test it:

```bash
rocky test-adapter --adapter duckdb
```

## Process adapter protocol

Adapters can be built in **any language** using the process adapter protocol: JSON-RPC 2.0 over stdio.

Rocky spawns the adapter as a child process and communicates via stdin/stdout:

```
Rocky ──stdin──► Adapter Process
Rocky ◄─stdout── Adapter Process
```

### Discovering installed adapters

Rocky follows the `cargo`-subcommand convention: any executable on your `PATH` named `rocky-<name>` registers as the process adapter `<name>` (the bundled `rocky-lsp` is filtered out). Use [`rocky adapter list`](/reference/commands/development/#rocky-adapter) to enumerate the adapters Rocky can see, and `rocky adapter info <name>` to inspect one adapter's manifest.

### Protocol flow

1. Rocky sends `initialize` with config → adapter responds with `AdapterManifest`
2. Rocky sends method calls (`execute_statement`, `describe_table`, etc.)
3. Adapter responds with results or errors
4. Rocky sends `shutdown` when done

### Example request

```json
{"jsonrpc": "2.0", "id": 1, "method": "execute_query", "params": {"sql": "SELECT 1"}}
```

### Example response

```json
{"jsonrpc": "2.0", "id": 1, "result": {"columns": ["1"], "rows": [["1"]]}}
```

## Conformance tests

The SDK includes 26 test specifications: 18 always run, and 8 are
capability-gated (skipped when the adapter's manifest declares the
required capability as `false`).

| Category | Tests |
|----------|-------|
| Connection | 1 |
| DDL | 4 |
| DML | 2 |
| Query | 4 |
| Types | 7 |
| Dialect | 3 |
| Governance | 2 |
| Batch Checks | 2 |
| Discovery | 1 |

Run them with:

```bash
rocky test-adapter --adapter duckdb
rocky test-adapter --command ./my-adapter-binary
```

Conformance results report pass/fail/skip per test with the adapter's declared capabilities used to determine which optional tests apply.
