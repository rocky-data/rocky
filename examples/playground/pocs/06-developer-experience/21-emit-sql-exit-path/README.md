# 21-emit-sql-exit-path — the tested no-lock-in exit path

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `rocky emit-sql --out-dir`, `rocky dag` execution order, surrogate-key injection on the emit path

## What it shows

`rocky emit-sql` compiles the model DAG offline (no warehouse) and writes one dialect-correct `<model>.sql` per model: the SQL `rocky run` would execute, surrogate keys included. The POC then runs those files in dependency order through the plain `duckdb` CLI, with no `rocky run`.

- `stg_orders` declares a `[[surrogate_key]]` (`order_key`, from `tenant` and `order_id`). The emitted SELECT contains `CAST(md5(…) AS VARCHAR) AS order_key`, so plain DuckDB rebuilds the 32-char key.
- `customer_orders` is a Rocky DSL group-by that emits ordinary `GROUP BY … HAVING …` SQL.
- Replay order comes from `rocky dag --output json` (`execution_layers`); an alphabetical glob would run `customer_orders.sql` before its upstream `stg_orders.sql`.

**Condition for replay without setup:** emitted SQL mixes qualified write targets (`CREATE OR REPLACE TABLE poc.main.stg_orders`) with bare upstream references (`FROM stg_orders`). Both resolve because the catalog name (`poc`) matches the DuckDB file stem (`poc.duckdb`), making `poc` the default catalog and `main` the default schema. No `ATTACH` or `SET search_path` is needed.

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

`run.sh` asserts that `revenue_by_region` has rows and `order_key` is 32 chars, then runs `rocky test` on the same models in memory.

## Related

- [`00-foundations/00-playground-default`](../../00-foundations/00-playground-default) — the same DAG shape, materialized with `rocky run`.
- [`08-portability-lint`](../08-portability-lint) — compile-time rejection of dialect-divergent constructs.
