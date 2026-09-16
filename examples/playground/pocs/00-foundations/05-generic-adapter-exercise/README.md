# 05-generic-adapter-exercise — Full generic adapter surface against DuckDB

> **Category:** 00-foundations
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** validate, list, doctor, seed, discover, plan, compile, test

## What it shows

An exercise of every generic adapter capability using a local DuckDB file
as the warehouse. This POC proves the end-to-end flow without any external
credentials or cloud services.

The pipeline:

```
seeds/                  rocky seed         rocky discover       rocky plan/compile/test
  customers.csv  ──┐                     ┌─ raw__customers ─┐
  orders.csv     ──┼──→ raw__* schemas ──┤  raw__orders     ├──→ stg_orders (model)
  products.csv   ──┘                     └─ raw__products   ┘
```

Commands exercised:

| Command | What it proves |
|---------|----------------|
| `rocky validate` | Config parsing, adapter type resolution |
| `rocky list pipelines` | Pipeline introspection |
| `rocky list adapters` | Adapter registry |
| `rocky list models` | Model discovery with `_defaults.toml` |
| `rocky doctor --check auth` | Adapter health check (DuckDB file access) |
| `rocky seed` | CSV seed loading with TOML sidecar target routing |
| `rocky discover` | Schema pattern matching against DuckDB schemas |
| `rocky plan` | Dry-run SQL generation with `--filter` |
| `rocky compile` | Model type-checking |
| `rocky test` | Local model execution in an in-memory DuckDB (1 model passes) |

## Why it's distinctive

- **Only POC that exercises `rocky seed` with CSV files and TOML sidecars.**
  Seeds are routed into source schemas (`raw__customers`, `raw__orders`,
  `raw__products`) so that `rocky discover` finds them naturally.
- Covers the widest surface of generic commands in a single script.
- Validates the adapter-agnostic contract: everything here works identically
  against Databricks or Snowflake (with credentials).

## Layout

```
.
├── README.md
├── rocky.toml              pipeline: flat schema pattern, checks enabled
├── run.sh                  exercises every generic command
├── seeds/
│   ├── customers.csv       7 rows (customer_id, name, email, tier, created_at)
│   ├── customers.toml      routes to raw__customers schema
│   ├── orders.csv          10 rows (order_id, customer_id, product, amount, status, ordered_at)
│   ├── orders.toml         routes to raw__orders schema
│   ├── products.csv        5 rows (product_id, name, category, price, in_stock)
│   └── products.toml       routes to raw__products schema
├── data/
│   └── seed.sql            raw__orders table — auto-loaded by `rocky test`
└── models/
    ├── _defaults.toml      target: poc.analytics
    ├── stg_orders.sql      SELECT from replicated orders
    └── stg_orders.toml     [[tests]]: unique(order_id), not_null(customer_id), accepted_values(status)
```

`run.sh` calls `rocky test` without `--declarative`. That compiles the models
and runs them in an in-memory DuckDB loaded from `data/seed.sql`. It does
**not** run the `[[tests]]` in `stg_orders.toml`. Those run only under
`rocky test --declarative`, against the configured warehouse adapter.

## Prerequisites

- `rocky` CLI on PATH (build with `cargo build` from the engine directory)

## Run

```bash
bash run.sh
```

## Expected output

Rocky emits **JSON by default** when stdout is not a TTY (as under `bash run.sh`);
only the explicit `-o table` steps render as tables. The `plan`, `compile`, and
`test` JSON is redirected into `expected/`, so those sections print little or
nothing to the terminal.

```
=== validate ===
{ "command": "validate", "valid": true, ... }        # JSON (auto-selected)

=== list pipelines ===
NAME                      TYPE             TARGET               SOURCE               DEPENDS ON
exercise                  replication      default              default              -

=== list adapters ===
NAME                      TYPE             HOST
default                   duckdb           -

=== list models ===
NAME                           TARGET                                   STRATEGY         CONTRACT     DEPENDS ON
stg_orders                     poc.analytics.stg_orders                 full_refresh     -            -

=== doctor --check auth ===
{ "command": "doctor", "overall": "healthy", ... }   # JSON

=== seed ===
{ "command": "seed", "tables_loaded": 3, "tables_failed": 0, ... }   # JSON

=== discover ===
raw__customers | source="customers" | 1 tables
raw__orders | source="orders" | 1 tables
raw__products | source="products" | 1 tables

=== plan (orders only) ===
-- full_refresh_copy (staging__orders.orders)
CREATE OR REPLACE TABLE staging__orders.orders AS
SELECT *
FROM raw__orders.orders;

Run plan persisted — 1 model(s) across 1 layer(s)
Plan ID:   <64-hex plan id>
Apply with: rocky apply <64-hex plan id>

=== compile ===
                                                     # JSON in expected/compile.json

=== test ===
                                                     # expected/test.json: 1 passed, 0 failed

POC complete: generic adapter exercise passed.
  - 3 seeds loaded (customers, orders, products)
  - 3 source schemas discovered
  - plan generated for orders pipeline
  - stg_orders compiled and tested (unique, not_null, accepted_values)
  - Artifacts saved to expected/
```

The `stg_orders compiled and tested` line of the summary names the three
`[[tests]]`. As explained above, plain `rocky test` does not run them.

## Related

- [00-playground-default](../00-playground-default/) -- baseline scaffold
- [02-inline-checks](../../01-quality/02-inline-checks/) -- pipeline-level `[checks]` in depth
- [04-local-test-with-duckdb](../../01-quality/04-local-test-with-duckdb/) -- `rocky test` focus
