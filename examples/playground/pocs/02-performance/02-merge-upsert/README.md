# 02-merge-upsert — Model-level MERGE strategy, executed end-to-end

> **Category:** 02-performance
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `[strategy] type = "merge"`, `unique_key`, `update_columns`, transformation pipeline, `auto_create_schemas`

## What it shows

Two models in one transformation pipeline. `raw_customers` materializes a
staging copy of `seeds.customers`; `dim_customers` reads from it with
`[strategy] type = "merge"`, declaring `unique_key = ["customer_id"]` and
`update_columns = ["name", "email", "tier"]`. The first `rocky run`
materializes the dim table; between runs we mutate the seed (update one
row's tier, rename another, insert a new customer); the second `rocky run`
issues a MERGE that upserts the changes — existing rows updated in place,
new rows inserted, untouched rows untouched.

```toml
[strategy]
type = "merge"
unique_key = ["customer_id"]
update_columns = ["name", "email", "tier"]
```

## Why it's distinctive

- **Model-level MERGE** — no hand-written `MERGE INTO` SQL. Rocky lowers
  `[strategy] type = "merge"` to dialect-specific MERGE on the target
  warehouse.
- **Update column list is explicit** so unrelated columns aren't
  overwritten when the source schema grows.
- **End-to-end against DuckDB.** Every other Rocky strategy POC also
  materializes against DuckDB; this one closes the loop by exercising
  MERGE semantics on a real warehouse, not just sql-gen string assertions.

## Layout

```
.
├── README.md
├── rocky.toml                        transformation pipeline + auto_create_schemas
├── run.sh                            seed -> run #1 -> mutate -> run #2 -> verify
├── data/
│   ├── seed.sql                      50 customers in seeds.customers
│   └── delta.sql                     1 update + 1 rename + 1 insert (between runs)
└── models/
    ├── raw_customers.{sql,toml}      seeds.customers -> poc.staging.raw_customers (full_refresh)
    └── dim_customers.{sql,toml}      poc.staging.raw_customers -> poc.demo.dim_customers (merge)
```

## Prerequisites

- `rocky` on PATH
- `duckdb` CLI (`brew install duckdb`) — seeds the source table, applies
  the delta, and verifies post-MERGE row counts
- Engine ≥ 1.29 (or this branch). The two enabling fixes shipped in PRs
  [#448](https://github.com/rocky-data/rocky/pull/448)
  (`auto_create_schemas` for transformation pipelines) and
  [#449](https://github.com/rocky-data/rocky/pull/449) (DuckDB MERGE
  unqualified-LHS).

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

1. `data/seed.sql` populates `seeds.customers` with 50 rows.
2. First `rocky run --pipeline transform`:
   - `auto_create_schemas` materializes `poc.staging` and `poc.demo`.
   - `raw_customers` runs first (full_refresh CTAS).
   - `dim_customers` runs second — Rocky sees the target doesn't exist, so
     it bootstraps an empty `poc.demo.dim_customers` from the model schema
     and then issues the MERGE (which inserts all 50 rows on the first
     pass).
3. `data/delta.sql` mutates `seeds.customers`: update tier on id=5, rename
   id=7, insert id=51.
4. Second `rocky run --pipeline transform`:
   - `raw_customers` rebuilds (full_refresh).
   - `dim_customers` issues a MERGE. WHEN MATCHED THEN UPDATE updates the
     two changed rows; WHEN NOT MATCHED THEN INSERT inserts customer 51.
     Untouched rows stay byte-identical.
5. The final query proves 51 total rows + the expected per-row deltas.

## Related

- DuckDB MERGE dialect:
  [`engine/crates/rocky-duckdb/src/dialect.rs`](../../../../engine/crates/rocky-duckdb/src/dialect.rs)
  (`merge_into`)
- Transformation MERGE codegen:
  [`engine/crates/rocky-core/src/sql_gen.rs`](../../../../engine/crates/rocky-core/src/sql_gen.rs)
  (`generate_transformation_sql` MERGE arm)
- Sibling: [`01-incremental-watermark`](../01-incremental-watermark) —
  watermark-based incremental writes (no MERGE, append only)
