# 13-surrogate-keys — Inject a dbt_utils-compatible surrogate key from a sidecar

> **Category:** 00-foundations
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `[[surrogate_key]]` sidecar block, deterministic hash injection, dbt_utils parity

## What it shows

A `[[surrogate_key]]` block in the model sidecar makes Rocky append a deterministic md5 column at materialization time, so the `.sql` stays a plain `SELECT`:

```toml
# models/order_keys.toml
[[surrogate_key]]
name = "order_key"
columns = ["order_id", "customer_id"]
```

After `rocky run`, `poc.main.order_keys` carries a 32-character `order_key`.

The value is byte-identical to `dbt_utils.generate_surrogate_key` over the same columns on a given warehouse, so a Rocky-built fact table still joins against dimensions keyed by an existing dbt project.

## How the hash is built (dbt_utils parity)

Rocky appends `CAST(md5(...) AS <string_type>) AS order_key`. On DuckDB the expression is:

```sql
md5(cast(
    coalesce(cast(order_id    as VARCHAR), '_dbt_utils_surrogate_key_null_') || '-' ||
    coalesce(cast(customer_id as VARCHAR), '_dbt_utils_surrogate_key_null_')
as VARCHAR))
```

- each input is cast to the warehouse's variable-length string type (`VARCHAR` on DuckDB, Snowflake and Trino; `STRING` on Databricks and BigQuery),
- inputs are concatenated in **declared column order** with a `'-'` separator,
- a NULL input becomes the sentinel `_dbt_utils_surrogate_key_null_` **before** hashing (one seed row has a NULL `customer_id`).

BigQuery uses `to_hex(...)` / `concat(...)` where the `||` form doesn't apply; the value matches dbt-utils there.

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

The first assert checks that at least one of five sampled values is a 32-char md5 hex string. The second compares `order_key` to the dbt-utils form computed independently in SQL.

## Related

- [`reference/model-format`](https://github.com/rocky-data/rocky/blob/main/docs/src/content/docs/reference/model-format.md): `[[surrogate_key]]` field reference
