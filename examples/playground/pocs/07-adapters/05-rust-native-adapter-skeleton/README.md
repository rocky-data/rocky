# 05-rust-native-adapter-skeleton — Rust-native warehouse adapter starter

> **Category:** 07-adapters
> **Credentials:** none (Rust toolchain only — `cargo` 1.85+)
> **Runtime:** ~10s on a warm cache
> **Rocky features:** `rocky-adapter-sdk` traits (`WarehouseAdapter`, `SqlDialect`), in-process testing

## Feature

A copy-and-edit Rust crate that implements Rocky's adapter SDK trait surface against an in-memory mock backend. The crate is shaped like a ClickHouse adapter — backtick quoting, two-part names, no `MERGE`, partition replace via `ALTER TABLE ... DELETE` + `INSERT` — but the file structure is the same one you'd use for Trino, Redshift, StarRocks, or any other SQL warehouse Rocky doesn't ship in-tree.

## Why it's distinctive

- **Targets the published SDK crate**, not `rocky-core`'s internal traits. What you read here is the surface a community contributor sees.
- **Zero warehouse dependency.** The adapter talks to a `Backend` trait; tests substitute a `MockBackend` so CI doesn't need a live cluster. Swap in `clickhouse::Client` and the rest of the file doesn't move.
- **Compiles green out of the box.** `./run.sh` runs `cargo check`, the unit tests, and a demo that prints the SQL the adapter would have sent to a real warehouse.

For an out-of-process adapter shape (any language, JSON-RPC over stdio), see the sibling POC `04-custom-process-adapter/`. That one is for Python / Go / Node consumers; this one is for Rust.

## Status — what works, what doesn't

- **Works:** SDK trait impls, dialect SQL generation, identifier validation, in-process testing pattern, `AdapterManifest` shape, capability declaration.
- **Not wired yet:** out-of-tree adapter registration. To make a real ClickHouse adapter ship today, you fork `rocky-data/rocky` and add the crate to `engine/Cargo.toml` plus the CLI's adapter dispatch — the SDK pins the trait shape so the fork stays small and upstreamable. The forward-looking `[adapter.skeleton]` block in `rocky.toml` shows what dynamic registration will look like once it lands.

See `docs/src/content/docs/guides/adapter-sdk.md` for the full walkthrough.

## Layout

```
.
├── README.md
├── rocky.toml                # Sketch of the config block (with DuckDB fallback for `rocky validate`)
├── run.sh                    # Build + test + run demo
└── adapter/
    ├── Cargo.toml            # Standalone crate, path-dep on rocky-adapter-sdk
    ├── src/lib.rs            # SkeletonAdapter, SkeletonDialect, MockBackend (~320 lines)
    └── examples/demo.rs      # End-to-end driver — prints generated SQL
```

The `adapter/` crate is **not** a member of the rocky workspace. That is intentional — it models exactly what an out-of-tree consumer's repo looks like.

## Run

```bash
./run.sh
```

## Expected output

```
=== cargo check (skeleton compiles against published SDK) ===
=== cargo test (8 unit tests) ===
test result: ok. 8 passed; 0 failed; ...

=== cargo run --example demo (end-to-end against mock backend) ===
manifest:
  name           = skeleton
  dialect        = skeleton
  sdk_version    = 1.18.0
  warehouse      = true
  merge          = false
  create_schema  = true

describe_table(default.events):
  - id Int64 (nullable=false)
  - ts DateTime64(3) (nullable=false)

statements executed:
  [0] CREATE TABLE `raw`.`events_copy` ENGINE = MergeTree() ORDER BY tuple() AS ...
  [1] ALTER TABLE `raw`.`events_copy` DELETE WHERE `day` = '2026-01-01'
  [2] INSERT INTO `raw`.`events_copy` SELECT `id`, `ts` FROM `default`.`events` ...
```

## What to change to make this real

1. **Backend.** Replace `MockBackend` with `clickhouse::Client` (or `reqwest::Client` for warehouses without a typed driver).
2. **Dialect.** Edit `SkeletonDialect` to match your warehouse's quoting, type widening, partition semantics. The most-edited methods are `format_table_ref`, `merge_into`, `insert_overwrite_partition`, and `row_hash_expr`.
3. **Auth.** Swap the example `AdapterAuth` enum for whatever your warehouse supports. Look at `engine/crates/rocky-databricks/src/auth.rs` (PAT then OAuth M2M) and `rocky-snowflake/src/auth.rs` (OAuth → key-pair JWT → password) for in-tree patterns.
4. **Registry.** For now, fork the repo and add your crate to `engine/Cargo.toml` workspace members. Track the dynamic-registration roadmap in the SDK guide.
