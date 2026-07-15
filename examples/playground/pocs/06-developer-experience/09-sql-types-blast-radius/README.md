# 09-sql-types-blast-radius — Trust arc 7: SQL as first-class with types

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB, ephemeral)
> **Runtime:** < 2s
> **Rocky features:** `rocky compile --with-seed`, `SELECT *` lints (`I001` info / `P002` blast-radius)

## What it shows

Raw `.sql` models become first-class in Rocky's semantic graph.

1. **`rocky compile --with-seed`** — load `data/seed.sql` into an
   in-memory DuckDB, introspect `information_schema`, and use the
   result as the source of truth for raw source schemas. Leaf `.sql`
   models that read from `raw__*` tables go from `Unknown` columns to
   concrete types, which cascades into incrementality hints, cost
   estimates, and downstream type inference.
2. **`SELECT *` lint** — the `orders_star` leaf trips the always-on
   `I001` (Info) `SELECT *` lint, which carries a source span so VS
   Code can flag it. The stronger `P002` blast-radius lint (Warning) is
   a separate, semantic-graph-aware check: it fires only when a
   `SELECT *` model has a downstream consumer that pins specific
   columns, so an upstream schema change would silently propagate. This
   leaf has no such consumer, so `P002` correctly stays quiet.

## Why it's distinctive

- **Type inference over raw `.sql`** — dbt keeps `.sql` as opaque
  strings; Rocky parses it with `sqlparser-rs`, types it against real
  schemas, and makes the result first-class. The DSL is one surface
  over the same semantic graph; `.sql` is not a second-class citizen.
- **Two tiers of `SELECT *` diagnostic.** `I001` (Info) always fires on
  any `SELECT *` as a style nudge. `P002` (Warning) is semantic-graph
  aware: it fires only when the star has a downstream consumer that pins
  columns, where the blast radius of an upstream schema change is real.
  A leaf `SELECT *` like `orders_star` trips only `I001`.

## Layout

```
.
├── README.md                this file
├── rocky.toml               DuckDB pipeline
├── run.sh                   compile with + without --with-seed; diff the result
├── data/seed.sql            raw__orders.orders with typed columns
└── models/
    ├── _defaults.toml       catalog=poc, schema=demo
    ├── orders_typed.sql     leaf .sql — column types cascade from the seed
    └── orders_star.sql      SELECT * — flagged by the blast-radius lint
```

## Run

```bash
./run.sh
```

## What happened

1. **Compile without `--with-seed`** — `orders_typed.incrementality_hint`
   has `confidence = medium`, signals reference only the column name.
2. **Compile with `--with-seed`** — Rocky spun up an in-memory DuckDB,
   ran `data/seed.sql`, introspected `information_schema`, and fed
   those columns into the semantic graph. `orders_typed` now reports
   `confidence = high`, signals include the actual integer type.
3. **`SELECT *` lint** — `orders_star.sql` fires `I001` (Info,
   "SELECT * used…") with a source span pointing at the model file
   (editor-integration ready). The `P002` blast-radius lint stays quiet
   because this leaf has no downstream consumer.

## Related

- Engine source: `engine/crates/rocky-cli/src/commands/compile.rs`
  (`--with-seed`), `engine/crates/rocky-compiler/src/typecheck.rs` (`I001`),
  `engine/crates/rocky-compiler/src/blast_radius.rs` (`P002`)
- Companion POC: [`08-portability-lint/`](../08-portability-lint/)
  covers the P001 portability lint (sibling compile-time gate).
- Future: Arc 7 wave 2 ships cached `DESCRIBE TABLE` so real warehouses
  get the same type grounding without a local seed.
