---
title: Adapter SDK
description: Build a warehouse or source adapter in Rust, or in any language over stdio.
sidebar:
  order: 13
---

An adapter is the plug between Rocky's engine and one outside system. A warehouse
adapter runs SQL. A source adapter lists what tables exist. See the
[glossary](/reference/glossary/) for the short definition.

The in-tree adapters implement traits from `rocky-core`. The `rocky-adapter-sdk`
crate mirrors those traits for adapters built outside this repository. Each
adapter also ships an `AdapterManifest` that declares which optional features it
supports.

## How the engine reaches a warehouse

The engine calls trait methods. It never names a warehouse.

```
                    rocky-core (engine)
                             │
         calls trait methods, never a product name
                             ▼
     WarehouseAdapter   SqlDialect   DiscoveryAdapter
                     GovernanceAdapter
                             │
            each adapter implements these traits
                             │
  ┌──────────┬───────────────┼───────────────┬───────────────┐
  ▼          ▼               ▼               ▼               ▼
rocky-     rocky-          rocky-          rocky-          your-custom-
databricks snowflake       bigquery        duckdb          adapter
```

This is why a new warehouse needs a new crate and no change to the engine. Rocky
runs against any SQL warehouse through the same interface.

## Adapter traits

### WarehouseAdapter

The main trait. It executes SQL and manages tables.

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

Writes the SQL syntax one warehouse accepts.

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

Implement these only when the system behind your adapter supports them.

| Trait | Capability | Methods |
|-------|-----------|---------|
| `DiscoveryAdapter` | Discover connectors/tables | `discover() -> DiscoveryResult` |
| `GovernanceAdapter` | Tags, grants, bindings | `set_tags()`, `get_grants()`, `apply_grants()`, `revoke_grants()` |
| `BatchCheckAdapter` | Batched quality checks | `batch_row_counts()`, `batch_freshness()` |
| `TypeMapper` | Type normalization | `normalize_type()`, `types_compatible()` |

### Why discovery reports partial failures

`DiscoveryAdapter::discover` returns `DiscoveryResult { connectors, failed }`.

Some adapters fetch metadata one source at a time: a REST call per connector, a
`list_tables` per namespace. Any one of those calls can fail on its own. The
`failed` list carries those failures instead of dropping them.

That matters downstream. A reconciler that diffs discovery output against the
warehouse would otherwise read a dropped source as "removed upstream" and act on
it. Each `FailedSource` carries an `error_class` — `transient`, `timeout`,
`rate_limit`, `auth`, or `unknown` — so a consumer branches on the class instead
of parsing message text.

An adapter that finishes in one shot returns `DiscoveryResult::ok(connectors)`.

## AdapterManifest

Each adapter declares what it supports. Rocky reads the manifest to decide which
conformance tests apply.

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

Implement the required traits, then run the conformance suite.

`test-adapter --adapter <name>` resolves only two things. It resolves the
builtins (`databricks`, `snowflake`, `duckdb`), or a `rocky-<name>` process-adapter
binary on your `PATH`. To see the suite run, pass a builtin. To test your own
adapter, expose it as a process adapter or point at it with `--command`:

```bash
rocky test-adapter --adapter duckdb
```

## Process adapter protocol

Write an adapter in **any language** using the process adapter protocol: JSON-RPC
2.0 over stdio. Rocky spawns the adapter as a child process and talks to it over
that process's stdin and stdout.

### Discovering installed adapters

Rocky follows the `cargo`-subcommand convention. Any executable on your `PATH`
named `rocky-<name>` registers as the process adapter `<name>`. The bundled
`rocky-lsp` is filtered out. Use [`rocky adapter list`](/reference/commands/development/#rocky-adapter)
to see the adapters Rocky can find, and `rocky adapter info <name>` to read one
adapter's manifest.

### Protocol flow

```
  rocky                               rocky-<name>
    │                                          │
    │──── initialize { config } ─ stdin ──────►│
    │◄─── AdapterManifest ─────── stdout ──────│
    │                                          │
    │──── execute_query, describe_table, … ───►│
    │◄─── result or error ─────────────────────│
    │            (repeats per call)            │
    │                                          │
    │──── shutdown ───────────────────────────►│
    ▼                                          ▼
```

### Example request

```json
{"jsonrpc": "2.0", "id": 1, "method": "execute_query", "params": {"sql": "SELECT 1"}}
```

### Example response

```json
{"jsonrpc": "2.0", "id": 1, "result": {"columns": ["1"], "rows": [["1"]]}}
```

## Conformance tests

The SDK ships 26 test specifications. 18 always run. The other 8 are
capability-gated: Rocky skips one when the adapter's manifest declares the
required capability as `false`.

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

The report gives pass, fail, or skip per test. The adapter's declared
capabilities decide which optional tests apply.
