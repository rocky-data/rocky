# 13-surrogate-keys — Inject a dbt_utils-compatible surrogate key from a sidecar

> **Category:** 00-foundations
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `[[surrogate_key]]` sidecar block, deterministic hash injection, dbt_utils parity

## What it shows

A transformation model whose `.toml` sidecar declares a `[[surrogate_key]]` block.
You don't hand-write the hash expression in your SQL: Rocky appends a deterministic
md5 of the listed input columns to the model's projection at materialization time.

```toml
# models/order_keys.toml
[[surrogate_key]]
name = "order_key"
columns = ["order_id", "customer_id"]
```

The SQL stays a plain `SELECT` over the seeded source. After `rocky run`, the
materialized `poc.main.order_keys` table carries an extra `order_key` column
holding a 32-character md5 hash, computed over `(order_id, customer_id)`.

## Why it's distinctive

- **The hash lives in config, not SQL.** The surrogate key is declared once in the
  sidecar; the `.sql` file never mentions `md5(...)`. Rename or re-key without
  editing the query.
- **Byte-identical to `dbt_utils.generate_surrogate_key`.** On a given warehouse the
  value Rocky computes is exactly the value `dbt_utils.generate_surrogate_key` produces
  over the same columns, so a key Rocky computes joins against the same key in an
  upstream dbt model. This makes incremental dbt-to-Rocky migrations safe: a fact
  table re-keyed by Rocky still joins against dimensions keyed by the old dbt project.
- **The POC proves it twice.** `run.sh` first asserts the column matches `^[0-9a-f]{32}$`
  in the real materialized table, then asserts every row equals the hand-written
  dbt-utils form — 0 mismatches.

## How the hash is built (dbt_utils parity)

Rocky appends `CAST(md5(...) AS <string_type>) AS order_key` to the projection.
The expression is dialect-correct. On DuckDB (this POC) it resolves to:

```sql
md5(cast(
    coalesce(cast(order_id    as VARCHAR), '_dbt_utils_surrogate_key_null_') || '-' ||
    coalesce(cast(customer_id as VARCHAR), '_dbt_utils_surrogate_key_null_')
as VARCHAR))
```

Matching the dbt-utils contract exactly:

- each input is cast to the warehouse's variable-length string type
  (`VARCHAR` on DuckDB, Snowflake, and Trino; `STRING` on Databricks and BigQuery),
- inputs are concatenated in **declared column order** with a `'-'` separator,
- a NULL input coalesces to the fixed sentinel `_dbt_utils_surrogate_key_null_`
  **before** hashing — so a row with a NULL `customer_id` still gets a stable,
  collision-resistant key. (The seed includes one such row to exercise this.)

BigQuery uses `to_hex(...)` / `concat(...)` where the `||` form doesn't apply, but the
resulting value is the same one dbt-utils produces there.

## Layout

```
.
├── README.md
├── rocky.toml              # transformation pipeline over poc.duckdb
├── data/
│   └── seed.sql            # seeds raw__orders.orders (one row has a NULL customer_id)
├── models/
│   ├── order_keys.sql      # plain SELECT — no hash expression
│   └── order_keys.toml     # [[surrogate_key]] name="order_key" columns=["order_id","customer_id"]
└── run.sh                  # seed → run → assert hash present + dbt_utils parity
```

## Prerequisites

- `rocky` CLI on PATH
- `duckdb` CLI (`brew install duckdb`)

## Run

```bash
./run.sh
# or, by hand:
duckdb poc.duckdb < data/seed.sql                                   # seed the source
rocky run                                                           # materialize order_keys with the injected key
duckdb -noheader -list poc.duckdb "SELECT order_key FROM main.order_keys LIMIT 5"
```

## Expected output

```text
materialized in poc.main:
order_keys
=== query the injected surrogate key ===
9a9311b626de39f67f9f2e67f40353fc
6498e323aaf8bf05a7bb8ce6a6a876ac
...
PASS: order_key holds a 32-char md5 surrogate key
PASS: every order_key matches the dbt_utils.generate_surrogate_key form (0 mismatches)
```

## What happened

1. `duckdb … < data/seed.sql` seeds `raw__orders.orders`.
2. `rocky run` materializes `order_keys` into `poc.main`, appending
   `CAST(md5(...) AS VARCHAR) AS order_key` to the model's SELECT.
3. The first assert confirms the column exists and every sampled value is a
   32-char md5 hex string.
4. The second assert confirms each `order_key` equals the dbt-utils hash form
   computed independently in SQL — the byte-identical-parity proof.

## Related

- [`reference/model-format`](https://github.com/rocky-data/rocky/blob/main/docs/src/content/docs/reference/model-format.md) — the `[[surrogate_key]]` sidecar field reference.
- [`00-playground-default`](../00-playground-default) — the baseline transformation pipeline this POC's structure mirrors.
