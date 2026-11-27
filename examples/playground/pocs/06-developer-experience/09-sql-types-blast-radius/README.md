# 09-sql-types-blast-radius — typed raw SQL + `SELECT *` lints

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB, ephemeral)
> **Runtime:** < 2s
> **Rocky features:** seed-typed `rocky compile` (`data/seed.sql`, `--with-seed`), contract type checks (`I003`), `SELECT *` lints (`I001` info / `P002` blast-radius)

## What it shows

1. **Seed-typed `rocky compile`** — Rocky loads `data/seed.sql` into an in-memory DuckDB and takes raw source schemas from `information_schema`. A plain `rocky compile` does this whenever the project has `data/seed.sql`; `--with-seed` makes the seed required, so a missing or broken seed fails the compile. Raw `.sql` leaf models go from `Unknown` columns to concrete types, which lets a data contract's type check run and cascades into downstream inference.
2. **Two tiers of `SELECT *` lint** — `I001` (Info) fires on any `SELECT *` and carries a source span for editors. `P002` (Warning) is semantic-graph aware and fires only when a `SELECT *` model has a downstream consumer that pins specific columns.

## Run

```bash
./run.sh
```

## What to expect

1. **Compile with no seed file** (a scratch copy of `models/` and `contracts/`): every column of `raw__orders.orders` is `Unknown`, so `orders_typed` reports `I003` for `order_id` and `amount`.
2. **Compile the project** (plain and `--with-seed` give the same result): the columns resolve to `Int32` and `Decimal`, the contract type check runs and passes, and the `I003` diagnostics are gone.
3. **`SELECT *` lint**: `orders_star.sql` fires `I001`. `P002` stays quiet because this leaf has no downstream consumer.

## Related

- Engine source: `engine/crates/rocky-cli/src/commands/compile.rs` (`--with-seed`), `engine/crates/rocky-compiler/src/typecheck.rs` (`I001`), `engine/crates/rocky-compiler/src/blast_radius.rs` (`P002`)
- Companion POC: [`08-portability-lint/`](../08-portability-lint/) (P001 portability lint).
- Against a real warehouse, the schema cache (cached `DESCRIBE TABLE` results) types leaf models without a local seed; see [`05-orchestration/06-valkey-distributed-cache`](../../05-orchestration/06-valkey-distributed-cache/).
