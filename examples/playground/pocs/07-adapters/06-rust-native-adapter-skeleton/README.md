# 06-rust-native-adapter-skeleton — Rust-native warehouse adapter starter

> **Credentials:** none (Rust toolchain only, `cargo` 1.88+)
> **Runtime:** ~10s on a warm cache

## What it shows

A copy-and-edit crate implementing the `rocky-adapter-sdk` traits against an in-memory `MockBackend`, so no live warehouse is needed. It is ClickHouse-shaped (backtick quoting, two-part names, no `MERGE`, partition replace via `ALTER TABLE ... DELETE` + `INSERT`). `adapter/` is deliberately not a member of the rocky workspace, like an out-of-tree repo.

For an adapter in any language over stdio, see [`04-custom-process-adapter`](../04-custom-process-adapter/).

## Status — what works, what doesn't

- **Works:** SDK trait impls, dialect SQL generation, identifier validation, in-process testing, `AdapterManifest`, capability declaration.
- **Not wired yet:** out-of-tree registration of a Rust-native adapter. To ship one today, fork `rocky-data/rocky` and add the crate to `engine/Cargo.toml` and the CLI's adapter dispatch. The commented `[adapter.skeleton]` block in `rocky.toml` sketches future dynamic registration.

Full walkthrough: `docs/src/content/docs/guides/adapter-sdk.md`.

## Run

```bash
./run.sh
```

## Expected output

```
=== cargo check (skeleton compiles against published SDK) ===
=== cargo test ===
test result: ok. 11 passed; 0 failed; ...

=== cargo run --example demo (end-to-end against mock backend) ===
manifest:
  name           = skeleton
  dialect        = skeleton
  sdk_version    = 1.80.0
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

`sdk_version` is the `rocky-adapter-sdk` crate version, so it matches the engine checkout you build against.

## What to change to make this real

1. **Backend:** replace `MockBackend` with `clickhouse::Client` (or `reqwest::Client`).
2. **Dialect:** edit `SkeletonDialect` for quoting, type widening and partitions, mostly `format_table_ref`, `merge_into`, `insert_overwrite_partition`, `row_hash_expr`.
3. **Auth:** replace the example `AdapterAuth` enum. In-tree patterns: `engine/crates/rocky-databricks/src/auth.rs` (PAT then OAuth M2M), `rocky-snowflake/src/auth.rs` (OAuth, key-pair JWT, password).
4. **Registry:** fork and add your crate to `engine/Cargo.toml` workspace members; the SDK guide tracks dynamic registration.
