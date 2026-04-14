---
name: rocky-new-adapter
description: Adding a new warehouse or source adapter crate to the Rocky engine. Use when introducing a new target (e.g. BigQuery, Redshift, ClickHouse) or source (e.g. Airbyte, Singer). Covers crate layout, trait implementation, dialect specifics, auth, conformance tests, and factory registration.
---

# Adding a new Rocky adapter

Rocky adapters are Cargo crates that implement traits from `rocky-adapter-sdk`. Existing examples:

| Kind | Crate | What it talks to |
|---|---|---|
| Target (warehouse) | `rocky-databricks` | Databricks SQL Statement Execution API + Unity Catalog |
| Target (warehouse) | `rocky-snowflake` | Snowflake REST API |
| Target (local) | `rocky-duckdb` | Embedded DuckDB |
| Source | `rocky-fivetran` | Fivetran REST API |

## When to use this skill

- Adding a new **target adapter** (warehouse): e.g. BigQuery, Redshift, ClickHouse, Iceberg
- Adding a new **source adapter**: e.g. Airbyte, Singer, Stitch, database CDC
- Forking an existing adapter to support a cousin backend (e.g. Databricks → Databricks Serverless)

## Crate scaffold

Create `engine/crates/rocky-<name>/` with a `Cargo.toml` registered in the workspace (`engine/Cargo.toml` members list). Use `rocky-databricks` or `rocky-snowflake` as a template for a warehouse adapter, `rocky-fivetran` for a source adapter.

### Warehouse adapter files (mirror `rocky-databricks/src/`)

| File | Responsibility |
|---|---|
| `lib.rs` | `pub mod` exports + crate-level docs |
| `adapter.rs` | `impl Adapter for <Name>Adapter` — the main trait from `rocky-adapter-sdk::traits` |
| `dialect.rs` | `impl SqlDialect` — dialect-specific SQL (quoting, type widening rules, `insert_overwrite_partition`, MV/dynamic-table DDL) |
| `connector.rs` | REST client or native connection — use `reqwest` for HTTP-based adapters |
| `auth.rs` | Credential handling (PAT, OAuth M2M, key-pair JWT, etc.) with env var auto-detection |
| `catalog.rs` | Catalog/schema CRUD (CREATE/ALTER/DESCRIBE) |
| `permissions.rs` | GRANT/REVOKE + SHOW GRANTS parsing |
| `batch.rs` | Bulk operations (e.g. `information_schema` UNION ALL batching) |
| `throttle.rs` | Rate limiting / adaptive concurrency if the backend needs it |
| `types.rs` | Native → internal type mapping |

### Source adapter files (mirror `rocky-fivetran/src/`)

| File | Responsibility |
|---|---|
| `lib.rs` | Exports |
| `adapter.rs` | `impl SourceAdapter` |
| `client.rs` | Async REST client |
| `connector.rs` | Source discovery + filtering |
| `schema.rs` | Schema config parsing (often nested JSON) |
| `sync.rs` | Sync detection / staleness checks |

## Trait implementation

Traits live in `engine/crates/rocky-adapter-sdk/src/traits.rs`. The key ones:

- `Adapter` — warehouse adapter core: `execute_sql`, `describe_table`, `list_tables`, `create_catalog`, …
- `SqlDialect` — SQL generation specifics. Pay special attention to:
  - `is_safe_type_widening()` — allowlist for ALTER TABLE type changes; conservative by default
  - `insert_overwrite_partition()` — returns `Vec<String>` so multi-statement transactions work (Snowflake needs BEGIN/DELETE/INSERT/COMMIT)
  - Materialized views + dynamic tables DDL
- `SourceAdapter` — source discovery and sync staleness detection
- `Authenticator` — if there's a credential flow worth unifying

Use `#[async_trait]` for all async trait impls.

## Conformance tests

`rocky-adapter-sdk/src/conformance.rs` hosts a reusable test suite. Wire your adapter into it so `rocky test-adapter <name>` (the CLI command) validates the baseline behavior: CREATE/DROP, incremental copy, schema drift, permission reconcile.

Test pattern: add an integration test file in your crate that instantiates the adapter and runs the conformance suite. For credential-free local testing, gate behind an env var — see how `rocky-databricks` skips network tests when `DATABRICKS_HOST` is unset.

## Factory registration

The CLI + config parser needs to know your adapter type string. Search for where existing adapter types are registered (typically in `rocky-core/src/config.rs` or the CLI's adapter dispatch) and add yours. The config block in `rocky.toml` will then accept:

```toml
[adapter]
type = "<name>"        # ← the string you registered
host = "${MY_HOST}"
# … adapter-specific fields
```

## Auth patterns to copy

- **PAT-first, OAuth M2M fallback** — see `rocky-databricks/src/auth.rs`. Tries PAT from env, falls back to `client_credentials` flow if PAT is absent.
- **Multi-method priority** — see `rocky-snowflake/src/auth.rs`. OAuth (pre-supplied) > RS256 key-pair JWT > password.
- **Basic auth** — see `rocky-fivetran/src/client.rs`. Simplest case — base64 of `key:secret`.
- **SQL identifier validation** — always route catalog/schema/table names through `rocky-sql/validation.rs` before interpolation.

## Dialect gotchas

The `SqlDialect` trait is where adapters actually diverge. Things to get right:

- **Quoting**: backticks vs double-quotes vs brackets. `rocky-databricks` uses backticks for principals; most warehouses use the standard double-quote identifier.
- **Safe type widening allowlist**: `is_safe_type_widening(from, to)` controls whether Rocky will `ALTER TABLE` or fall back to full refresh. Be conservative — data loss from an unsafe cast is worse than an extra full refresh.
- **Partition overwrite atomicity**: Databricks has `INSERT INTO ... REPLACE WHERE` as a single atomic op. Snowflake and DuckDB need `BEGIN; DELETE; INSERT; COMMIT;` — hence the `Vec<String>` return type.
- **MV / dynamic table semantics**: Databricks materialized views and Snowflake dynamic tables have different refresh models. Read `engine/CLAUDE.md` for the canonical DDL strings.

## Adapter SDK process helper

`rocky-adapter-sdk/src/process.rs` provides helpers for out-of-process adapters (spawning an external binary, parsing JSON, handling exit codes). Useful if you're wrapping a dbt-like adapter or a CLI tool instead of speaking a REST API directly.

## Shipping checklist

- [ ] Crate added to `engine/Cargo.toml` workspace members
- [ ] `impl Adapter` (or `SourceAdapter`) complete
- [ ] `impl SqlDialect` with all required methods
- [ ] Auth module with env-var auto-detection
- [ ] Conformance test wired up (`rocky test-adapter <name>` passes)
- [ ] Factory registration so `rocky.toml` `type = "<name>"` resolves
- [ ] Env vars documented in `engine/README.md` and/or `docs/src/content/docs/adapters/<name>.md`
- [ ] `cargo test -p rocky-<name>` green
- [ ] `cargo clippy -p rocky-<name> -- -D warnings` clean

## Commit style

```
feat(engine/rocky-bigquery): scaffold BigQuery target adapter
feat(engine/rocky-bigquery): implement SqlDialect + conformance tests
feat(engine/rocky-bigquery): OAuth service-account auth
docs(engine): add BigQuery adapter reference
```
