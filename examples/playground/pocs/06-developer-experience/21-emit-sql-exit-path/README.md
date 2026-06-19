# 21-emit-sql-exit-path — the tested no-lock-in exit path

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `rocky emit-sql --out-dir`, `rocky dag` execution order, surrogate-key injection on the emit path

## What it shows

Adopting Rocky is not a one-way door. `rocky emit-sql` compiles the whole
model DAG offline (no warehouse, no engine to run it) and writes one
dialect-correct `<model>.sql` file per model — the exact SQL `rocky run`
would execute, surrogate keys included.

This POC then **proves** those files run Rocky-free. It seeds a fresh DuckDB
and replays the emitted SQL in dependency order through the plain `duckdb`
CLI. There is no `rocky run` anywhere in `run.sh`: the final tables are built
by vanilla DuckDB from the emitted SQL.

```
rocky emit-sql --out-dir build/sql      # Rocky → plain .sql files (offline)
duckdb poc.duckdb < data/seed.sql       # fresh DuckDB, seeded source
for m in $(rocky dag …); do             # replay in dependency order
    duckdb poc.duckdb < build/sql/$m.sql #   ← plain DuckDB, no rocky run
done
```

The feature proof is a table built by plain DuckDB from emitted SQL — and a
surrogate key that survives the trip.

## Why it's distinctive

- **The proof is execution, not inspection.** Most "export your SQL" stories
  stop at "here are the files." This one runs them: the assertion is
  `SELECT count(*) FROM poc.main.revenue_by_region` against a database Rocky
  never touched.
- **Rocky's value-add survives the exit.** `stg_orders` declares a
  `[[surrogate_key]]`. On the emit path Rocky injects
  `CAST(md5(…) AS VARCHAR) AS order_key` into the SELECT, so the 32-char hash
  key is rebuilt by plain DuckDB's `md5()` — not by `rocky run`. The exit path
  carries the modeling, not just the raw SELECT.
- **The DSL reduces too.** `customer_orders` is a Rocky DSL group-by, yet it
  emits ordinary `GROUP BY … HAVING …` SQL that DuckDB runs unaided.
- **Dependency order is audited, not guessed.** Replay order comes from
  `rocky dag --output json` (`execution_layers`), not a fragile alphabetical
  glob — `customer_orders.sql` would otherwise sort before its upstream
  `stg_orders.sql`.

## How it resolves with zero gymnastics

The emitted SQL mixes fully-qualified write targets
(`CREATE OR REPLACE TABLE poc.main.stg_orders`) with bare upstream references
(`FROM stg_orders`). Both resolve in vanilla DuckDB because the catalog name
(`poc`) matches the DuckDB file stem (`poc.duckdb`): opening `poc.duckdb`
makes `poc` the default catalog and `main` the default schema, so
`poc.main.stg_orders` and bare `FROM stg_orders` point at the same table. No
`ATTACH`, no `SET search_path`.

## Layout

```
.
├── README.md                  this file
├── rocky.toml                 transformation pipeline, catalog=poc, schema=main
├── run.sh                     emit-sql → seed → replay → assert (no rocky run)
├── data/
│   └── seed.sql               seeds raw__sales.orders (acme / globex, emea / us_west)
└── models/
    ├── stg_orders.sql         FROM raw__sales.orders
    ├── stg_orders.toml        [[surrogate_key]] order_key = md5(tenant, order_id)
    ├── customer_orders.rocky  Rocky DSL group-by over stg_orders
    ├── customer_orders.toml   depends_on = ["stg_orders"]
    ├── revenue_by_region.sql  GROUP BY tenant, region over stg_orders
    └── revenue_by_region.toml depends_on = ["stg_orders"]
```

## Prerequisites

- `rocky` CLI on PATH
- `duckdb` CLI (`brew install duckdb`)
- `jq` (to read `rocky dag` JSON)

## Run

```bash
./run.sh
# or, by hand:
rocky emit-sql --models models --out-dir build/sql   # Rocky → plain .sql files
duckdb poc.duckdb < data/seed.sql                     # seed the source
duckdb poc.duckdb < build/sql/stg_orders.sql          # replay in dep order…
duckdb poc.duckdb < build/sql/customer_orders.sql
duckdb poc.duckdb < build/sql/revenue_by_region.sql
duckdb poc.duckdb "SELECT * FROM poc.main.revenue_by_region"  # built Rocky-free
```

## Expected output

```text
emit-sql: wrote 3 model(s) to build/sql in dependency order
OK: build/sql/stg_orders.sql builds order_key with md5()
poc.main.revenue_by_region rows: 4
poc.main.stg_orders.order_key length: 32
POC complete: 3 models reduced to plain SQL via emit-sql; a fresh DuckDB built
revenue_by_region (and a 32-char md5 order_key) with no rocky run.
```

## What happened

1. `rocky emit-sql --out-dir build/sql` compiles the DAG offline and writes
   `stg_orders.sql`, `customer_orders.sql`, `revenue_by_region.sql` — each a
   `CREATE OR REPLACE TABLE poc.main.<model> AS …` statement.
2. `rocky dag --output json` yields the replay order (`stg_orders` →
   `customer_orders`, `revenue_by_region`).
3. `duckdb poc.duckdb < data/seed.sql` seeds `raw__sales.orders` into the same
   file the emitted SQL writes into.
4. Each emitted file is replayed through the plain `duckdb` CLI. No `rocky run`.
5. `run.sh` asserts `poc.main.revenue_by_region` has rows and that
   `order_key` is a 32-char md5 — both produced by DuckDB alone.
6. `rocky test` runs the same models in-memory for parity, confirming the SQL
   you just proved is the SQL Rocky would have executed.

## Related

- [`00-foundations/00-playground-default`](../../00-foundations/00-playground-default) — the same DAG shape, materialized the normal way with `rocky run`.
- [`08-portability-lint`](../08-portability-lint) — the other half of "your SQL travels": compile-time rejection of dialect-divergent constructs before they ever reach a warehouse.
