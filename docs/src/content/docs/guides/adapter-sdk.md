---
title: Building a Custom Adapter
description: Build a warehouse or source adapter in Rust, or in any language over stdio. The traits, a worked skeleton, the process protocol, auth, conformance tests, and how to ship it.
sidebar:
  order: 20
---

An [adapter](/reference/glossary/#adapter) is the plugin that connects Rocky to one system. A warehouse adapter runs SQL. A source adapter lists what tables exist. The engine calls trait methods and never names a warehouse, so a new warehouse needs a new crate and no engine change.

The `rocky-adapter-sdk` crate mirrors the `rocky-core` traits for adapters built outside this repository. Each adapter ships an `AdapterManifest` that declares its optional features.

This guide takes you to a compiling skeleton with passing tests: [`examples/playground/pocs/07-adapters/06-rust-native-adapter-skeleton/`](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/07-adapters/06-rust-native-adapter-skeleton). It is shaped after ClickHouse but fits any SQL warehouse not shipped in-tree (Trino and ClickHouse already ship: see [`rocky-trino`](/concepts/architecture/#rocky-trino) and the [ClickHouse adapter](/reference/adapters/clickhouse/)).

Your crate supplies two parts. The planner reads `AdapterCapabilities` to decide what it may emit, then hands a target table ref and `SELECT` text to your `SqlDialect`, which composes every SQL string Rocky sends. Your `WarehouseAdapter` (`execute_statement`, `execute_query`) sends that SQL to the warehouse by driver call or HTTP request.

## When to reach for the SDK

Use the SDK for a warehouse that is not in-tree (DuckDB, Databricks, Snowflake, BigQuery, PostgreSQL, Redshift, Trino, ClickHouse, SQL Server, Spark), a forked variant of an in-tree adapter, or a tool that owns its warehouse client. For an in-tree warehouse use `[adapter]` in `rocky.toml`. For another language use the [process adapter protocol](#process-adapter-protocol).

## The trait surface

The public traits live in `engine/crates/rocky-adapter-sdk/src/traits.rs`. Every warehouse adapter must implement `WarehouseAdapter` and `SqlDialect`. You opt into the rest by capability.

| Trait | Required? | What it does | Key methods (full surface in `rocky-adapter-sdk/src/traits.rs`) |
|---|---|---|---|
| `WarehouseAdapter` | yes | Execute SQL against the warehouse | `dialect`, `execute_statement`, `execute_query`, `describe_table`, `table_exists`, `close` |
| `SqlDialect` | yes | Generate warehouse-specific SQL | `name`, `format_table_ref`, `create_table_as`, `insert_into`, `merge_into`, `describe_table_sql`, `drop_table_sql`, `create_catalog_sql`, `create_schema_sql`, `row_hash_expr`, `tablesample_clause`, `select_clause`, `watermark_where`, `insert_overwrite_partition` |
| `DiscoveryAdapter` | no | Enumerate connectors / tables in a source system | `discover` |
| `GovernanceAdapter` | no | Tags, grants, catalog/schema lifecycle | `set_tags`, `get_grants`, `apply_grants`, `revoke_grants` |
| `BatchCheckAdapter` | no | Batched data-quality queries | `batch_row_counts`, `batch_freshness`, and the `supports_row_counts` / `supports_freshness` capability predicates (both default to `true`) |
| `LoaderAdapter` | no | File ingestion (CSV, Parquet, JSONL) | `load`, `supported_formats` |
| `TypeMapper` | no | Cross-warehouse type normalization | `normalize_type`, `types_compatible` |

A flag in `AdapterCapabilities` gates each opt-in trait: set it and implement the trait, and the planner uses it. `DiscoveryAdapter::discover` returns `DiscoveryResult { connectors, failed }`. List a source that failed in `failed` instead of dropping it, or a reconciler reads it as "removed upstream". Each `FailedSource` carries an `error_class`: `transient`, `timeout`, `rate_limit`, `auth`, or `unknown`. An adapter that finishes in one shot returns `DiscoveryResult::ok(connectors)`.

### When each method is called

| Method | Called for |
|---|---|
| `execute_statement` | Every generated DDL and DML statement: `CREATE TABLE`, `INSERT INTO`, `MERGE INTO`, `ALTER TABLE`, `DROP TABLE`, partition replace. |
| `execute_query` | `EXPLAIN`, `DESCRIBE`, row-count assertions, the `SELECT 1` check at `rocky compile`. |
| `describe_table` | Drift detection, contract validation, the column-list step of an incremental insert. |
| `table_exists` | The full-refresh-versus-create branch of a materialization. |
| `dialect()` methods | Every SQL string Rocky emits. Identifier validation lives here. |

## Scaffold a crate

```bash
rocky init-adapter bigquery
```

This creates `crates/rocky-bigquery/` with a `Cargo.toml`, `src/{lib,dialect,adapter,types}.rs` stubs and an `#[ignore]`d live test in `tests/integration.rs`. Add the crate to the workspace first.

**The scaffolded crate does not compile until you answer one question.** `SqlDialect::literal_escape` says how your warehouse reads a single-quoted string.

| Answer | Meaning | Warehouses |
|---|---|---|
| `LiteralEscape::Standard` | The lexer ignores backslashes. A quote is doubled (`''`). | Trino, DuckDB |
| `LiteralEscape::Backslash` | The lexer reads backslashes. A quote is `\'`, a backslash is `\\`, a line break is `\n`. | Snowflake, Databricks, BigQuery |

The scaffold emits a `compile_error!` instead of guessing, because a wrong answer silently corrupts values or lets a quote end the literal early. Encode a value holding a quote and a backslash, `SELECT` it back, and check it is unchanged. A process adapter is never asked.

## Worked example: a ClickHouse-shaped skeleton

The POC at [`examples/playground/pocs/07-adapters/06-rust-native-adapter-skeleton/`](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/07-adapters/06-rust-native-adapter-skeleton) is a compiling, tested starter. Run it:

```bash
git clone https://github.com/rocky-data/rocky.git
cd rocky/examples/playground/pocs/07-adapters/06-rust-native-adapter-skeleton
./run.sh
```

The script runs `cargo check`, the unit tests, and a demo binary that prints the SQL the adapter would send. The crate (`Cargo.toml` with a path dependency on `rocky-adapter-sdk`, `src/lib.rs`, `examples/demo.rs`) is standalone, not in the workspace.

### Adapter struct + manifest

`SkeletonAdapter` holds a `backend: Arc<dyn Backend>` and a `SkeletonDialect`. `SkeletonAdapter::manifest()` returns an `AdapterManifest` with `name`, `version`, `sdk_version`, `dialect`, `auth_methods` (`basic`, `token`), `config_schema` and these capabilities:

```rust
AdapterCapabilities {
    warehouse: true, discovery: false, governance: false, batch_checks: false,
    create_catalog: false,  // ClickHouse has no catalogs
    create_schema: true,    // ClickHouse calls these "databases"
    merge: false,           // No MERGE: use incremental instead
    tablesample: true, file_load: false,
}
```

With `merge: false`, the planner refuses a `strategy = "merge"` config at validate time. With `create_catalog: false`, `auto_create_catalogs = true` gets a "warehouse doesn't support catalogs" error.

### Backend abstraction

The skeleton hides the warehouse client behind a `Backend` trait (`execute`, `query`, `describe`, `exists`), so a test can swap in `MockBackend`, a `HashMap` plus a statement log. The production implementation wraps `clickhouse::Client`, or `reqwest::Client` for a warehouse with no typed driver.

### Dialect implementation

The skeleton's `format_table_ref` drops the arguments the warehouse lacks and validates every identifier.

```rust
fn format_table_ref(&self, _catalog: &str, schema: &str, table: &str) -> AdapterResult<String> {
    validate_ident(schema)?;   // ClickHouse has no catalogs: `_catalog` is dropped
    validate_ident(table)?;
    Ok(format!("`{schema}`.`{table}`"))
}
```

Four methods deserve extra thought:

- **`merge_into`**: return `AdapterError::not_supported("merge_into")` when the warehouse has no `MERGE`.
- **`insert_overwrite_partition`**: returns `Vec<String>`, because some warehouses need a transaction (Snowflake's `BEGIN; DELETE; INSERT; COMMIT`). The runtime runs the statements in order and rolls back on a partial failure.
- **`row_hash_expr`**: change detection. ClickHouse uses `sipHash128(tuple(...))`. For cross-warehouse hashes see `rocky-bigquery` and `rocky-snowflake`.
- **`watermark_where`**: Rocky passes the watermark as a `DateTime<Utc>` literal. Format it as a native timestamp in `col > <literal>`. `None` (first run, or after `delete_watermark`) means the `1970-01-01` sentinel. Validate `timestamp_col` before splicing it in.

## Auth and connection management

The SDK ships an optional `AuthProvider` trait that composes the `Authorization` header and any other header your warehouse needs. `StaticAuthProvider` covers fixed credentials. Two in-tree patterns are worth copying:

- **`engine/crates/rocky-databricks/src/auth.rs`**: `${DATABRICKS_TOKEN}` first, then the OAuth M2M `client_credentials` flow with `${DATABRICKS_CLIENT_ID}` and `${DATABRICKS_CLIENT_SECRET}`.
- **`engine/crates/rocky-snowflake/src/auth.rs`**: a Programmatic Access Token, then a pre-supplied OAuth bearer, then an RS256 key-pair JWT, then a password. Each reads its own `${SNOWFLAKE_*}` variable.

Two rules for every adapter:

1. **Read credentials at config-parse time.** Rocky substitutes `${VAR}` while parsing `rocky.toml`. Take the resolved string from `SkeletonConfig`. Do not re-read the environment in the constructor, or tests collide.
2. **Pool HTTP clients in the adapter struct.** Build `reqwest::Client` once in `SkeletonAdapter::new` and clone the handle.

For retry and rate limiting, see `rocky-adapter-sdk/src/throttle.rs` (AIMD) and the `is_transient` / `is_rate_limit` loop in `rocky-databricks/src/connector.rs`.

## Testing your adapter

Neither layer needs a live warehouse. Unit tests with a mock backend assert on the SQL the dialect generated (`backend.statement_log()` after `adapter.execute_statement(...)`). For a REST-backed adapter, use `wiremock` as `rocky-fivetran/src/client.rs` does: one `MockServer` per test, expected handlers, assertions on verbs and paths.

### The conformance harness

`rocky-adapter-sdk::conformance::run_conformance(&manifest, Some(adapter.dialect()))` returns a `ConformanceResult` saying which tests apply, from your declared capabilities, and which were skipped. The SDK ships 26 test specifications across Connection, DDL, DML, Query, Types, Dialect, Governance, Batch Checks and Discovery. 18 always run, and 8 are capability-gated (skipped when the manifest declares the capability `false`).



`rocky test-adapter --adapter <name>` resolves only the builtins (`databricks`, `snowflake`, `duckdb`) or a `rocky-<name>` process-adapter binary on your `PATH`. To test your own adapter, expose it as a process adapter or pass `--command`:

```bash
rocky test-adapter --adapter duckdb
rocky test-adapter --command ./my-adapter-binary
```

The report gives pass, fail or skip per test. With a live dialect, the harness makes three real trait calls: `SqlDialect::format_table_ref`, `watermark_where` and `row_hash_expr`. With `None` (what `rocky test-adapter --adapter <name>` passes) it skips the dialect checks and validates the test plan without a warehouse. Every other check is a plan entry, not a warehouse call: treat the result as a checklist for your own unit tests.

## Process adapter protocol

Write an adapter in any language with JSON-RPC 2.0 over stdio (`rocky-adapter-sdk/src/process.rs`). Rocky spawns it as a child process. Like a `cargo` subcommand, any executable on your `PATH` named `rocky-<name>` registers as the process adapter `<name>` (the bundled `rocky-lsp` is filtered out). [`rocky adapter list`](/reference/commands/development/#rocky-adapter) shows the adapters Rocky can find. `rocky adapter info <name>` reads one manifest.

```
  rocky                               rocky-<name>
    │──── initialize { config } ─ stdin ──────►│
    │◄─── AdapterManifest ─────── stdout ──────│
    │──── execute_query, describe_table, … ───►│
    │◄─── result or error ─────────────────────│   (repeats per call)
    │──── shutdown ───────────────────────────►│
```

```json
{"jsonrpc": "2.0", "id": 1, "method": "execute_query", "params": {"sql": "SELECT 1"}}
{"jsonrpc": "2.0", "id": 1, "result": {"columns": ["1"], "rows": [["1"]]}}
```

## Distributing your adapter

The adapter registry is static at compile time, with no dynamic plugin system. To ship an adapter, fork `rocky-data/rocky`, put your crate in `engine/crates/rocky-<name>/`, add it to the `engine/Cargo.toml` workspace `members` and the CLI's adapter dispatch, and open a PR. If you cannot upstream, keep a private fork, or use the [process adapter protocol](#process-adapter-protocol), which works in any language. [`pocs/07-adapters/04-custom-process-adapter/`](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/07-adapters/04-custom-process-adapter) is a working Python adapter against SQLite.

## Gotchas worth knowing about

**The SDK mirrors most of the in-tree trait surface.** `rocky-adapter-sdk/src/traits.rs` is the public contract. The SDK has default-impl methods for `execute_statement_with_stats` (and `ExecutionStats`), `ping`, `explain` (and `ExplainResult`), `is_experimental`, `warehouse_name` and `list_tables`. These still differ from `rocky-core/src/traits.rs`: `fetch_arrow_batch`, `clone_table_for_branch`, the `merge_into` signature, and the duplicated types `TableRef`, `ColumnInfo`, `Grant` and `MetadataColumn`. Target the SDK surface and treat those as unstable.

**Identifier validation is not optional.** Anything you splice into SQL must pass `[A-Za-z0-9_]+`, or your warehouse's equivalent (see the skeleton's `validate_ident`). A string-literal injection got [a CVE-class fix](https://github.com/rocky-data/rocky/pull/293).

**The `catalog` field in `TableRef` is always present.** A warehouse with no catalogs (ClickHouse, Postgres, MySQL) gets an empty string. Your dialect's `format_table_ref` must drop it.

**`AdapterError` is type-erased on purpose.** Use `AdapterError::msg(...)` for an ad-hoc error, `AdapterError::new(my_err)` to wrap an `std::error::Error`, and `AdapterError::not_supported("method_name")` for a missing capability. Do not use `thiserror` inside the trait impl.

See the in-tree adapters in `engine/crates/rocky-{databricks,snowflake,bigquery,duckdb,fivetran}/`. File an [issue](https://github.com/rocky-data/rocky/issues) if a trait method is missing something.
