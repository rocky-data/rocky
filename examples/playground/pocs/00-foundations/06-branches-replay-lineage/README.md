# 06-branches-replay-lineage — Trust arc 1: branches, replay, column lineage

> **Category:** 00-foundations
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s
> **Rocky features:** `rocky branch`, `rocky run --branch`, `rocky replay`, `rocky lineage --downstream`

## What it shows

The four primitives that make up Rocky's **trust system** storage arc — all
on a single 3-model DuckDB pipeline:

1. **Named branches** — `rocky branch create fix-revenue` registers a
   virtual branch in the state store; runs written against the branch
   land in a prefixed schema, leaving `main` untouched.
2. **Branch-scoped runs** — `rocky run --branch fix-revenue` is the
   persistent analogue of `--shadow`. Warehouse-native zero-copy clones
   (Delta `SHALLOW CLONE`, Snowflake `CLONE`) slot into the same API.
3. **Replay** — `rocky replay latest` reads `RunRecord`s back from the
   state store: every model, SQL hash, row count, bytes, timings. The
   reproducibility artefact for *"what exactly ran?"*. Today the
   command is **inspection-only** (Arc 1 wave 1) — the run-record
   *write* path arrives in Arc 1 wave 2 alongside the content-addressed
   storage primitive.
4. **Column-level blast radius** — `rocky lineage raw_orders --column
   amount --downstream` traces every consumer of `amount`, so you can
   see before you ship a schema change exactly which downstream columns
   will need to move.

## Why it's distinctive

- **The DAG is the primitive.** Warehouses don't own it; Rocky does.
  That's why branches, replay, and column lineage are all one command
  away here and wouldn't be in any warehouse.
- **Replay captures SQL hashes and row counts** at the time of the run
  — the same `run_id` answers both *"what ran at 03:15 UTC?"* and
  *"are you sure it was deterministic?"* (content-addressed re-run
  comes next; inspection already works).
- **Column lineage is semantic-graph aware** (compiler-driven, not
  regex). An expression like `group customer_id { total: sum(amount) }`
  is correctly traced as `amount → total` through the group-by.

## Layout

```
.
├── README.md         this file
├── rocky.toml        DuckDB pipeline, full-refresh strategy
├── run.sh            end-to-end demo (compile → run → branch → replay → lineage)
├── data/seed.sql     200-row synthetic orders table
└── models/
    ├── _defaults.toml       catalog=poc, schema=demo
    ├── raw_orders.sql       leaf: reads from seeds.orders
    ├── stg_orders.rocky     filter cancelled orders
    └── fct_revenue.rocky    group by customer_id → total
```

## Prerequisites

- `rocky` ≥ 1.11.0 on PATH
- `duckdb` CLI for seeding (`brew install duckdb`)

## Run

```bash
./run.sh
```

## What happened

1. **Compile** — semantic graph for 3 models; column schemas propagated
   from `raw_orders` through to `fct_revenue`.
2. **Run on main** — writes to `poc.demo.*`, records a `RunRecord`.
3. **Create branch `fix-revenue`** — state-store entry, no warehouse
   side effects.
4. **Run on the branch** — writes to a prefixed schema, separate
   `RunRecord`.
5. **Replay latest** — dumps the recorded artefact for the branch run.
6. **Lineage downstream from `raw_orders.amount`** — shows every
   downstream column that would break if `amount` changes type.

## Related

- Engine source: `engine/crates/rocky-core/src/branch.rs`,
  `engine/crates/rocky-cli/src/commands/{branch.rs,replay.rs,lineage.rs}`
- Companion example: [`engine/examples/multi-layer/`](../../../../../engine/examples/multi-layer/)
- Sibling POC: [`06-developer-experience/01-lineage-column-level/`](../../06-developer-experience/01-lineage-column-level/)
