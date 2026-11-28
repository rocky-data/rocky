# 02-merge-upsert — Model-level MERGE strategy, executed end-to-end

> **Category:** 02-performance
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `[strategy] type = "merge"`, `unique_key`, `update_columns`, transformation pipeline, `auto_create_schemas`

## What it shows

`dim_customers` declares a model-level MERGE, so you write no `MERGE INTO` SQL. It reads from `raw_customers`, a full-refresh staging copy of `seeds.customers` (50 rows). Between two runs the seed changes (one tier update, one rename, one insert); the second run upserts them against DuckDB and leaves other rows untouched. Only the listed `update_columns` are overwritten.

```toml
[strategy]
type = "merge"
unique_key = ["customer_id"]
update_columns = ["name", "email", "tier"]
```

## Prerequisites

- `rocky` ≥ 1.29.0 on PATH (PRs [#448](https://github.com/rocky-data/rocky/pull/448), `auto_create_schemas` for transformation pipelines, and [#449](https://github.com/rocky-data/rocky/pull/449), DuckDB MERGE unqualified-LHS)
- `duckdb` CLI (`brew install duckdb`)
- `python3`

## Run

```bash
./run.sh
```

## Expected output

```text
=== first rocky run — initial materialization ===
  - poc.staging.raw_customers (full_refresh)
  - poc.demo.dim_customers (merge)

  dim_customers row count after run #1:  50

=== apply delta (UPDATE customer 5 tier, rename customer 7, INSERT customer 51) ===

=== second rocky run — MERGE upsert ===
  - poc.staging.raw_customers (full_refresh)
  - poc.demo.dim_customers (merge)

=== verify MERGE semantics ===
-- row count after MERGE (expect 51 = 50 original + 1 inserted) --
51

customer_id |     name              |     email                       | tier
----------- | --------------------- | ------------------------------- | ------
          5 | Customer 5            | customer5@example.com           | gold     <- tier updated
          7 | Customer 7 (renamed)  | customer7+renamed@example.com   | bronze   <- name+email updated
         51 | Customer 51           | customer51@example.com          | bronze   <- newly inserted
```

## What happened

On the first run the target does not exist, so Rocky bootstraps an empty `poc.demo.dim_customers` from the model schema and MERGE-inserts all 50 rows. Both runs use `rocky run --pipeline transform`.

Related: [`01-incremental-watermark`](../01-incremental-watermark) (append only, no MERGE).
