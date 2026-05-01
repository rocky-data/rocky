---
title: Building a Custom Adapter
description: Build a Rust-native warehouse adapter against the Rocky adapter SDK — trait surface, worked ClickHouse-shaped skeleton, auth, testing, distribution.
sidebar:
  order: 9
---

Rocky talks to warehouses through a small set of traits in the `rocky-adapter-sdk` crate. Implementing those traits gives you a working warehouse adapter — the same way `rocky-databricks`, `rocky-snowflake`, `rocky-bigquery`, and `rocky-duckdb` are wired today.

This guide walks a Rust developer from "I want a ClickHouse adapter" to a compiling skeleton with passing tests in roughly fifteen minutes. The runnable skeleton lives at [`examples/playground/pocs/07-adapters/06-rust-native-adapter-skeleton/`](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/07-adapters/06-rust-native-adapter-skeleton) and is shaped after ClickHouse, but the same shape works for Trino, Redshift, StarRocks, MotherDuck, or any SQL warehouse Rocky doesn't ship in-tree.

## When to reach for the SDK

The adapter SDK is the right tool when:

- The warehouse you need is not in [the in-tree adapter list](/concepts/adapters/) (Databricks, Snowflake, BigQuery, DuckDB).
- You need a forked variant of an existing adapter (e.g. Databricks Serverless on top of `rocky-databricks`).
- You want to embed Rocky in a tool that owns its own warehouse client and would rather wrap it than spawn `rocky` as a subprocess.

If your warehouse already ships in-tree, use it directly via `[adapter]` in `rocky.toml`. If you want adapters in a non-Rust language (Python, Go, Node), see the [process adapter protocol](/concepts/adapters/#process-adapter-protocol) — JSON-RPC over stdio. The POC at `pocs/07-adapters/04-custom-process-adapter/` walks that pattern.

## The trait surface

Public traits live in `engine/crates/rocky-adapter-sdk/src/traits.rs`. The two you must implement for any warehouse adapter are `WarehouseAdapter` and `SqlDialect`. The rest are opt-in by capability.

| Trait | Required? | What it does | Key methods (full surface in `rocky-adapter-sdk/src/traits.rs`) |
|---|---|---|---|
| `WarehouseAdapter` | yes | Execute SQL against the warehouse | `dialect`, `execute_statement`, `execute_query`, `describe_table`, `table_exists`, `close` |
| `SqlDialect` | yes | Generate warehouse-specific SQL | `name`, `format_table_ref`, `create_table_as`, `insert_into`, `merge_into`, `describe_table_sql`, `drop_table_sql`, `create_catalog_sql`, `create_schema_sql`, `row_hash_expr`, `tablesample_clause`, `select_clause`, `watermark_where`, `insert_overwrite_partition` |
| `DiscoveryAdapter` | no | Enumerate connectors / tables in a source system | `discover` |
| `GovernanceAdapter` | no | Tags, grants, catalog/schema lifecycle | `set_tags`, `get_grants`, `apply_grants`, `revoke_grants` |
| `BatchCheckAdapter` | no | Batched data-quality queries | `batch_row_counts`, `batch_freshness` |
| `LoaderAdapter` | no | File ingestion (CSV, Parquet, JSONL) | `load`, `supported_formats` |
| `TypeMapper` | no | Cross-warehouse type normalization | `normalize_type`, `types_compatible` |

Each opt-in trait is gated by a flag in `AdapterCapabilities`. Set the flag, implement the trait, and Rocky's planner picks up the new behavior automatically.

### When each method is called

- `execute_statement` — every DDL / DML Rocky generates: `CREATE TABLE`, `INSERT INTO`, `MERGE INTO`, `ALTER TABLE`, `DROP TABLE`, partition replace.
- `execute_query` — `EXPLAIN`, `DESCRIBE`, row-count assertions, the `rocky compile`-time `SELECT 1` connectivity check.
- `describe_table` — drift detection (`rocky drift`), contract validation, the column-list step before generating an incremental insert.
- `table_exists` — full-refresh-vs-create branching at the start of a materialization.
- `dialect()` methods — every SQL string Rocky emits is composed by a dialect call. Identifier validation lives here.

## Worked example: a ClickHouse-shaped skeleton

The POC at [`examples/playground/pocs/07-adapters/06-rust-native-adapter-skeleton/`](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/07-adapters/06-rust-native-adapter-skeleton) is a compiling, tested starter. To run it:

```bash
git clone https://github.com/rocky-data/rocky.git
cd rocky/examples/playground/pocs/07-adapters/06-rust-native-adapter-skeleton
./run.sh
```

This runs `cargo check`, the unit tests, and a demo binary that prints the SQL the adapter would have sent to a real warehouse.

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

Capability flags are not cosmetic — they gate behavior. `merge: false` makes Rocky's planner refuse `strategy = "merge"` configs against this adapter at validate time rather than failing mid-run. `create_catalog: false` makes `auto_create_catalogs = true` surface a clear "warehouse doesn't support catalogs" error instead of emitting broken SQL.

### Backend abstraction

The skeleton hides the actual warehouse client behind a small `Backend` trait so tests can substitute an in-memory mock:

```rust
#[async_trait]
pub trait Backend: Send + Sync {
    async fn execute(&self, sql: &str) -> AdapterResult<()>;
    async fn query(&self, sql: &str) -> AdapterResult<QueryResult>;
    async fn describe(&self, table: &TableRef) -> AdapterResult<Vec<ColumnInfo>>;
    async fn exists(&self, table: &TableRef) -> AdapterResult<bool>;
}
```

The production impl wraps `clickhouse::Client` (or `reqwest::Client` for warehouses without a typed driver). The test impl is `MockBackend` — a `HashMap` plus a statement log so tests can assert on the SQL the dialect produced.

### Dialect implementation

`SqlDialect` is where most adapter divergence lives. The skeleton's `format_table_ref` shows the two patterns you almost always need: drop arguments your warehouse doesn't have, and validate every identifier you splice into SQL.

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

Methods worth thinking carefully about:

- **`merge_into`** — return `AdapterError::not_supported("merge_into")` if your warehouse has no `MERGE`. Rocky's planner sees the capability flag and won't generate merge plans, but a defensive impl still helps if someone bypasses the planner.
- **`insert_overwrite_partition`** — returns `Vec<String>` because some warehouses need a multi-statement transaction (Snowflake's `BEGIN; DELETE; INSERT; COMMIT`). The runtime executes them in order and rolls back on partial failure.
- **`row_hash_expr`** — used for change detection. ClickHouse uses `sipHash128(tuple(...))`; if you want cross-warehouse comparable hashes, look at how `rocky-bigquery` and `rocky-snowflake` agree on a stable encoding.
- **`watermark_where`** — the standard incremental filter (`col > (SELECT max(col) FROM target)`). Validate `timestamp_col` before splicing.

## Auth and connection management

The SDK does not prescribe an auth trait — each adapter wires its own. The two patterns worth copying are in-tree:

- **`engine/crates/rocky-databricks/src/auth.rs`** — PAT-first, OAuth M2M fallback. Reads `${DATABRICKS_TOKEN}`; if absent, falls through to the `client_credentials` flow with `${DATABRICKS_CLIENT_ID}` / `${DATABRICKS_CLIENT_SECRET}`. The auto-detection logic is roughly twenty lines.
- **`engine/crates/rocky-snowflake/src/auth.rs`** — multi-method priority: pre-supplied OAuth bearer wins, then RS256 key-pair JWT, then password. Each method reads from a distinct `${SNOWFLAKE_*}` variable so config files never carry secrets.

Two rules that apply to every adapter regardless of method:

1. **Read credentials at config-parse time, not at adapter-construct time.** Rocky substitutes `${VAR}` references when parsing `rocky.toml`. Pull the resolved string out of `SkeletonConfig`; do not re-read env vars from the adapter constructor or tests will collide on shared state.
2. **Pool HTTP clients in the adapter struct.** `reqwest::Client` is internally `Arc`-counted and cheap to clone — construct it once in `SkeletonAdapter::new` and clone the handle into every request. Don't construct a new client per call.

For retry and rate-limiting, look at `rocky-adapter-sdk/src/throttle.rs` (the AIMD adaptive concurrency helper) and `rocky-databricks/src/connector.rs` for an `is_transient` / `is_rate_limit` retry loop.

## Testing your adapter

Two test layers, neither of which needs a live warehouse.

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

For adapters that talk to a REST API, the in-tree pattern is `wiremock`-based — see how `rocky-fivetran/src/client.rs` is tested. You stand up a `MockServer` per test, register expected `Match::path("/v1/connectors")` handlers, and assert your adapter sends the right verbs against the right paths. CI runs without a real Fivetran account.

### The conformance harness

`rocky-adapter-sdk::conformance::run_conformance(&manifest)` returns a `ConformanceResult` describing which tests apply (based on declared capabilities) and which were skipped. Today this is a test plan, not a live runner — it prints the matrix but does not yet execute trait calls against your adapter. Treat it as a checklist of behaviors your unit tests should cover. Live execution will land in a future SDK release.

## Distributing your adapter

The honest answer for now: **fork and merge**. The adapter registry is statically registered at compile time — there is no dynamic plugin system today. To ship a new adapter:

1. Fork `rocky-data/rocky`.
2. Drop your crate into `engine/crates/rocky-<name>/`.
3. Add it to `engine/Cargo.toml` workspace `members` and the CLI's adapter dispatch.
4. Open a PR upstream. The SDK pins the trait shape so the diff stays small — usually a few hundred lines of crate plus one wiring line in the CLI.

Two looser paths if upstreaming isn't an option yet:

- **Vendor the crate.** Keep your fork private, ship Rocky internally with your adapter linked in. The in-tree adapters use this same model — they're just upstreamed.
- **Process adapter (any language).** If you want to escape Rust entirely, the JSON-RPC stdio protocol in `rocky-adapter-sdk/src/process.rs` works today — see [`pocs/07-adapters/04-custom-process-adapter/`](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/07-adapters/04-custom-process-adapter) for a working Python adapter against SQLite.

A dynamic registration path (declarative config + crates.io discovery) is on the roadmap but unscheduled. Until it lands, the SDK's job is to keep the trait surface stable enough that your fork is forward-compatible.

## Gotchas worth knowing about

These are real and surface during implementation — flagging them up front so you don't lose half a day debugging.

- **Two trait surfaces exist today.** `rocky-adapter-sdk/src/traits.rs` is the public, marketed contract. `rocky-core/src/traits.rs` is a richer superset that the in-tree adapters currently use (it adds methods like `execute_statement_with_stats`, `ExplainResult`, retention APIs). For new out-of-tree adapters, target the SDK — it's the contract that will stay stable. The in-tree richer surface is migrating toward the SDK over time.
- **Identifier validation is not optional.** Anything you splice into SQL must pass `[A-Za-z0-9_]+` (or your warehouse's equivalent). The skeleton's `validate_ident` shows the pattern. SQL-injection-bearing string literals were the subject of [a real CVE-class fix](https://github.com/rocky-data/rocky/pull/293) — don't reinvent that hole.
- **The `catalog` field in `TableRef` is always present.** Warehouses without catalogs (ClickHouse, Postgres, MySQL) get an empty string. Your dialect's `format_table_ref` is responsible for dropping it.
- **`AdapterError` is intentionally type-erased.** Use `AdapterError::msg(...)` for ad-hoc errors, `AdapterError::new(my_err)` to wrap an `std::error::Error`, and `AdapterError::not_supported("method_name")` for capabilities your warehouse doesn't have. Don't reach for `thiserror` inside the trait impl — the SDK boxes everything.

## Next steps

- Browse the [skeleton POC source](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/07-adapters/06-rust-native-adapter-skeleton) — `adapter/src/lib.rs` is meant to be read top-to-bottom.
- Read the [adapter concepts page](/concepts/adapters/) for the architecture overview.
- Look at the in-tree adapters in `engine/crates/rocky-{databricks,snowflake,bigquery,duckdb,fivetran}/` for production patterns.
- File an issue on [github.com/rocky-data/rocky](https://github.com/rocky-data/rocky/issues) if a trait method is missing what you need — the SDK is still young and feedback shapes the roadmap.
