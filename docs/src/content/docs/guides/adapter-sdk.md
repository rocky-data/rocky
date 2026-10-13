---
title: Building a Custom Adapter
description: Build a warehouse or source adapter in Rust, or in any language over stdio. The traits, a worked skeleton, the process protocol, auth, conformance tests, and how to ship it.
sidebar:
  order: 20
---

An [adapter](/reference/glossary/#adapter) is the plugin that connects Rocky to one system. A warehouse adapter runs SQL. A source adapter lists what tables exist. The engine calls trait methods and never names a warehouse, so a new warehouse needs a new crate and no engine change.

The `rocky-adapter-sdk` crate mirrors the traits in `rocky-core` for adapters built outside this repository. Each adapter ships an `AdapterManifest` that declares which optional features it supports.

This guide takes a Rust developer from "I want a ClickHouse adapter" to a compiling skeleton with passing tests. The runnable skeleton lives at [`examples/playground/pocs/07-adapters/06-rust-native-adapter-skeleton/`](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/07-adapters/06-rust-native-adapter-skeleton). It is shaped after ClickHouse, but the same shape fits StarRocks, MotherDuck, or any SQL warehouse Rocky does not ship in-tree. (Trino and ClickHouse already ship in-tree: see the [`rocky-trino` crate](/concepts/architecture/#rocky-trino) and the [ClickHouse adapter](/reference/adapters/clickhouse/), which is Beta. The skeleton below stays a teaching example.)

Your crate sits at the bottom of this chain. It supplies the two boxes marked "yours":

```
   rocky plan / apply
          │
          ▼
   ┌──────────────────────────────┐
   │ Rocky planner                │  reads AdapterCapabilities
   │                              │  to decide what it may emit
   └──────────────┬───────────────┘
                  │ target table ref + SELECT text
                  ▼
   ┌──────────────────────────────┐
   │ SqlDialect            yours  │  composes every SQL string
   │ create_table_as, merge_into… │  Rocky sends
   └──────────────┬───────────────┘
                  │ SQL text
                  ▼
   ┌──────────────────────────────┐
   │ WarehouseAdapter      yours  │  execute_statement
   │                              │  execute_query
   └──────────────┬───────────────┘
                  │ driver call or HTTP request
                  ▼
            your warehouse
```

## When to reach for the SDK

Use the adapter SDK when one of these is true:

- The warehouse you need is not in the in-tree adapter list (DuckDB, Databricks, Snowflake, BigQuery, PostgreSQL, Redshift, Trino, ClickHouse, SQL Server, Spark).
- You need a forked variant of an existing adapter, such as Databricks Serverless on top of `rocky-databricks`.
- You embed Rocky in a tool that owns its own warehouse client, and you would rather wrap that client than spawn `rocky` as a subprocess.

If your warehouse already ships in-tree, use it directly through `[adapter]` in `rocky.toml`. If you want an adapter in another language (Python, Go, Node), use the [process adapter protocol](#process-adapter-protocol) instead: JSON-RPC over stdio. The POC at `pocs/07-adapters/04-custom-process-adapter/` walks that pattern.

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

A flag in `AdapterCapabilities` gates each opt-in trait. Set the flag, implement the trait, and Rocky's planner starts using the new behavior.

`DiscoveryAdapter::discover` returns `DiscoveryResult { connectors, failed }`. An adapter that fetches metadata one source at a time lists a failed source in `failed` instead of dropping it. Otherwise a reconciler would read the dropped source as "removed upstream" and act on it. Each `FailedSource` carries an `error_class`: `transient`, `timeout`, `rate_limit`, `auth`, or `unknown`. An adapter that finishes in one shot returns `DiscoveryResult::ok(connectors)`.

### When each method is called

- `execute_statement`: every DDL and DML statement Rocky generates: `CREATE TABLE`, `INSERT INTO`, `MERGE INTO`, `ALTER TABLE`, `DROP TABLE`, partition replace.
- `execute_query`: `EXPLAIN`, `DESCRIBE`, row-count assertions, and the `SELECT 1` connectivity check at `rocky compile` time.
- `describe_table`: schema drift detection during `rocky run`, contract validation, and the column-list step before Rocky generates an incremental insert.
- `table_exists`: the full-refresh-versus-create branch at the start of a materialization.
- `dialect()` methods: every SQL string Rocky emits. Identifier validation lives here.

## Scaffold a crate

```bash
rocky init-adapter bigquery
```

This creates `crates/rocky-bigquery/` with a `Cargo.toml`, `src/{lib,dialect,adapter,types}.rs` stubs, and an `#[ignore]`d live-connection test in `tests/integration.rs`. Add the crate to the workspace first. Cargo refuses a crate that inherits workspace settings before it is a member.

**The scaffolded crate does not compile until you answer one question.** `SqlDialect::literal_escape` says how your warehouse reads a single-quoted string.

| Answer | Meaning | Warehouses |
|---|---|---|
| `LiteralEscape::Standard` | The lexer ignores backslashes. A quote is doubled (`''`). | Trino, DuckDB |
| `LiteralEscape::Backslash` | The lexer reads backslashes. A quote is `\'`, a backslash is `\\`, a line break is `\n`. | Snowflake, Databricks, BigQuery |

The scaffold emits a `compile_error!` instead of guessing. A wrong answer is silent: it corrupts values on some warehouses and lets a quote end the literal early on others. Read your warehouse's documentation. Then prove it: encode a value holding a quote and a backslash, `SELECT` it back, and check it is unchanged. The out-of-process trait does not carry this question yet, so a process adapter is never asked.

## Worked example: a ClickHouse-shaped skeleton

The POC at [`examples/playground/pocs/07-adapters/06-rust-native-adapter-skeleton/`](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/07-adapters/06-rust-native-adapter-skeleton) is a compiling, tested starter. Run it:

```bash
git clone https://github.com/rocky-data/rocky.git
cd rocky/examples/playground/pocs/07-adapters/06-rust-native-adapter-skeleton
./run.sh
```

The script runs `cargo check`, the unit tests, and a demo binary. The demo prints the SQL the adapter would have sent to a real warehouse.

### Crate layout

```
adapter/
├── Cargo.toml            # Path-dep on rocky-adapter-sdk; standalone (not in workspace)
├── src/lib.rs            # SkeletonAdapter, SkeletonDialect, MockBackend, tests
└── examples/demo.rs      # End-to-end driver
```

### Adapter struct + manifest

```rust
pub struct SkeletonAdapter {
    backend: Arc<dyn Backend>,
    dialect: SkeletonDialect,
}

impl SkeletonAdapter {
    pub fn manifest() -> AdapterManifest {
        AdapterManifest {
            name: "skeleton".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            sdk_version: SDK_VERSION.into(),
            dialect: "skeleton".into(),
            capabilities: AdapterCapabilities {
                warehouse: true,
                discovery: false,
                governance: false,
                batch_checks: false,
                create_catalog: false,  // ClickHouse has no catalogs
                create_schema: true,    // ClickHouse calls these "databases"
                merge: false,           // No MERGE — use incremental instead
                tablesample: true,
                file_load: false,
            },
            auth_methods: vec!["basic".into(), "token".into()],
            config_schema: serde_json::json!({ /* ... */ }),
        }
    }
}
```

Capability flags change what Rocky does. With `merge: false`, the planner refuses a `strategy = "merge"` config against this adapter at validate time instead of failing mid-run. With `create_catalog: false`, an `auto_create_catalogs = true` config gets a clear "warehouse doesn't support catalogs" error instead of broken SQL.

### Backend abstraction

The skeleton hides the real warehouse client behind a small `Backend` trait, so a test can swap in an in-memory mock:

```rust
#[async_trait]
pub trait Backend: Send + Sync {
    async fn execute(&self, sql: &str) -> AdapterResult<()>;
    async fn query(&self, sql: &str) -> AdapterResult<QueryResult>;
    async fn describe(&self, table: &TableRef) -> AdapterResult<Vec<ColumnInfo>>;
    async fn exists(&self, table: &TableRef) -> AdapterResult<bool>;
}
```

The production implementation wraps `clickhouse::Client`, or `reqwest::Client` for a warehouse with no typed driver. The test implementation is `MockBackend`: a `HashMap` plus a statement log, so a test can assert on the SQL the dialect produced.

### Dialect implementation

`SqlDialect` is where warehouses differ most. The skeleton's `format_table_ref` shows the two patterns you almost always need. Drop the arguments your warehouse does not have, and validate every identifier you splice into SQL.

```rust
fn format_table_ref(
    &self,
    _catalog: &str,    // ClickHouse has no catalogs — drop on the floor
    schema: &str,
    table: &str,
) -> AdapterResult<String> {
    validate_ident(schema)?;
    validate_ident(table)?;
    Ok(format!("`{schema}`.`{table}`"))
}
```

Four methods deserve extra thought:

- **`merge_into`**: return `AdapterError::not_supported("merge_into")` when your warehouse has no `MERGE`. Rocky's planner reads the capability flag and generates no merge plans, but the defensive implementation still helps if something bypasses the planner.
- **`insert_overwrite_partition`**: returns `Vec<String>`, because some warehouses need a multi-statement transaction, such as Snowflake's `BEGIN; DELETE; INSERT; COMMIT`. The runtime executes the statements in order and rolls back on a partial failure.
- **`row_hash_expr`**: Rocky uses this for change detection. ClickHouse uses `sipHash128(tuple(...))`. For hashes that compare across warehouses, see how `rocky-bigquery` and `rocky-snowflake` agree on a stable encoding.
- **`watermark_where`**: the caller supplies the watermark, not a subquery. Rocky reads the previous run's max source timestamp from its own state store and passes it in as a literal `DateTime<Utc>`; your dialect only formats that value as a warehouse-native timestamp literal in `col > <literal>`. `None` (first run, or after `delete_watermark`) means format the `1970-01-01` sentinel so the whole source is scanned. Validate `timestamp_col` before you splice it in.

## Auth and connection management

The SDK ships an optional `AuthProvider` trait. It composes the `Authorization` header together with any other header your warehouse demands, such as a user-identity header alongside a bearer token. `StaticAuthProvider` covers the fixed-credential case. An adapter with other needs can wire its own. Two in-tree patterns are worth copying:

- **`engine/crates/rocky-databricks/src/auth.rs`**: token first, OAuth M2M second. It reads `${DATABRICKS_TOKEN}`. If that is absent, it falls through to the `client_credentials` flow with `${DATABRICKS_CLIENT_ID}` and `${DATABRICKS_CLIENT_SECRET}`. The detection logic is about twenty lines.
- **`engine/crates/rocky-snowflake/src/auth.rs`**: four methods in priority order. A Programmatic Access Token wins, then a pre-supplied OAuth bearer, then an RS256 key-pair JWT, then a password. Each method reads its own `${SNOWFLAKE_*}` variable, so no config file carries a secret.

Two rules apply to every adapter, whatever the auth method:

1. **Read credentials at config-parse time, not at adapter-construct time.** Rocky substitutes `${VAR}` references while it parses `rocky.toml`. Take the resolved string out of `SkeletonConfig`. Do not re-read environment variables in the adapter constructor, or your tests will collide over shared state.
2. **Pool HTTP clients in the adapter struct.** `reqwest::Client` is `Arc`-counted internally and cheap to clone. Construct it once in `SkeletonAdapter::new` and clone the handle into every request. Do not construct a client per call.

For retry and rate-limiting, read `rocky-adapter-sdk/src/throttle.rs`, the AIMD adaptive-concurrency helper. `rocky-databricks/src/connector.rs` shows an `is_transient` / `is_rate_limit` retry loop.

## Testing your adapter

There are two test layers. Neither needs a live warehouse.

### Unit tests with a mock backend

The skeleton's tests assert on the SQL the dialect generated:

```rust
#[tokio::test]
async fn execute_statement_round_trips_to_backend() {
    let backend = Arc::new(MockBackend::new());
    let adapter = SkeletonAdapter::new(backend.clone());

    adapter
        .execute_statement("CREATE TABLE foo (id Int64) ENGINE=Memory")
        .await
        .unwrap();

    let log = backend.statement_log().await;
    assert!(log[0].contains("CREATE TABLE foo"));
}
```

This style covers everything except real network behavior.

### Wiremock for HTTP-backed adapters

An adapter that talks to a REST API is tested with `wiremock` in-tree. See how `rocky-fivetran/src/client.rs` is tested. You stand up a `MockServer` per test, register the expected `Match::path("/v1/connectors")` handlers, and assert that your adapter sends the right verbs to the right paths. CI runs it without a real Fivetran account.

### The conformance harness

`rocky-adapter-sdk::conformance::run_conformance(&manifest, Some(adapter.dialect()))` returns a `ConformanceResult`. The result says which tests apply, based on your declared capabilities, and which were skipped.

The SDK ships 26 test specifications. 18 always run. The other 8 are capability-gated: Rocky skips one when the manifest declares the capability `false`.

| Category | Tests | Category | Tests |
|---|---|---|---|
| Connection | 1 | Dialect | 3 |
| DDL | 4 | Governance | 2 |
| DML | 2 | Batch Checks | 2 |
| Query | 4 | Discovery | 1 |
| Types | 7 | | |

`rocky test-adapter --adapter <name>` resolves only the builtins (`databricks`, `snowflake`, `duckdb`) or a `rocky-<name>` process-adapter binary on your `PATH`. To test your own adapter, expose it as a process adapter or pass `--command`:

```bash
rocky test-adapter --adapter duckdb
rocky test-adapter --command ./my-adapter-binary
```

The report gives pass, fail, or skip per test. Pass a live dialect and the harness makes three real trait calls: `SqlDialect::format_table_ref`, `SqlDialect::watermark_where`, and `SqlDialect::row_hash_expr`. Those calls are the first step toward live execution. Pass `None` when you have no live adapter, and the harness reports the dialect-category checks as skipped rather than running them against a stub. `rocky test-adapter --adapter <name>` does exactly that: it validates the test plan without a warehouse.

Every other check is still a plan entry, not a warehouse call. Treat the result as a checklist of behaviors your own unit tests should cover. Broader trait execution lands in later SDK releases.

## Process adapter protocol

Write an adapter in any language with JSON-RPC 2.0 over stdio. Rocky spawns the adapter as a child process and talks over its stdin and stdout. The code is in `rocky-adapter-sdk/src/process.rs`.

Rocky follows the `cargo`-subcommand convention. Any executable on your `PATH` named `rocky-<name>` registers as the process adapter `<name>`. The bundled `rocky-lsp` is filtered out. [`rocky adapter list`](/reference/commands/development/#rocky-adapter) shows the adapters Rocky can find. `rocky adapter info <name>` reads one manifest.

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

Today you fork and merge. The adapter registry is registered statically at compile time, and there is no dynamic plugin system. To ship a new adapter:

1. Fork `rocky-data/rocky`.
2. Drop your crate into `engine/crates/rocky-<name>/`.
3. Add it to `engine/Cargo.toml` workspace `members` and the CLI's adapter dispatch.
4. Open a PR upstream. The SDK pins the trait shape, so the diff stays small: usually a few hundred lines of crate plus one wiring line in the CLI.

Two looser paths, if you cannot upstream yet:

- **Vendor the crate.** Keep your fork private and ship Rocky internally with your adapter linked in. The in-tree adapters follow the same model. They are simply upstreamed.
- **Process adapter, in any language.** To leave Rust entirely, use the [process adapter protocol](#process-adapter-protocol). It works today. See [`pocs/07-adapters/04-custom-process-adapter/`](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/07-adapters/04-custom-process-adapter) for a working Python adapter against SQLite.

A dynamic registration path, with declarative config and crates.io discovery, is on the roadmap but unscheduled. Until it lands, the SDK keeps the trait surface stable enough that your fork stays forward-compatible.

## Gotchas worth knowing about

**The SDK trait surface now mirrors most of the in-tree one.** `rocky-adapter-sdk/src/traits.rs` is the public contract. `rocky-core/src/traits.rs` is what the in-tree adapters use. The SDK gained default-impl methods for `execute_statement_with_stats` (and `ExecutionStats`), `ping`, `explain` (and `ExplainResult`), `is_experimental`, `warehouse_name`, and `list_tables`. An out-of-tree adapter now gets the shape the in-tree adapters use. A few methods still differ while cross-crate types are unified: `fetch_arrow_batch`, `clone_table_for_branch`, and the `merge_into` signature, plus the duplicated types `TableRef`, `ColumnInfo`, `Grant`, and `MetadataColumn`. Target the SDK surface, and treat those as not yet stable.

**Identifier validation is not optional.** Anything you splice into SQL must pass `[A-Za-z0-9_]+`, or your warehouse's equivalent. The skeleton's `validate_ident` shows the pattern. String literals that carried SQL injection were the subject of [a real CVE-class fix](https://github.com/rocky-data/rocky/pull/293). Do not reopen that hole.

**The `catalog` field in `TableRef` is always present.** A warehouse with no catalogs (ClickHouse, Postgres, MySQL) gets an empty string. Your dialect's `format_table_ref` must drop it.

**`AdapterError` is type-erased on purpose.** Use `AdapterError::msg(...)` for an ad-hoc error, `AdapterError::new(my_err)` to wrap an `std::error::Error`, and `AdapterError::not_supported("method_name")` for a capability your warehouse lacks. Do not reach for `thiserror` inside the trait impl. The SDK boxes everything.

## Next steps

- Read the [skeleton POC source](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/07-adapters/06-rust-native-adapter-skeleton). `adapter/src/lib.rs` is written to be read top to bottom.
- Read the in-tree adapters in `engine/crates/rocky-{databricks,snowflake,bigquery,duckdb,fivetran}/` for production patterns.
- File an issue on [github.com/rocky-data/rocky](https://github.com/rocky-data/rocky/issues) if a trait method is missing something you need. The SDK is young, and feedback shapes the roadmap.
