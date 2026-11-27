# 02-null-safe-operators — `!=` lowering to `IS DISTINCT FROM`

> **Category:** 00-foundations
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** NULL-safe `!=` lowering, side-by-side DSL vs SQL comparison

## What it shows

Rocky's `!=` compiles to `IS DISTINCT FROM`, which treats NULL as a value. SQL's `!=` returns `UNKNOWN` for NULL comparisons, so `WHERE col != value` silently drops NULL rows. Two models (`filter_dsl.rocky`, `filter_sql.sql`) filter the same seed both ways.

## Run

```bash
./run.sh
```

## Expected output

`run.sh` prints the SQL Rocky actually lowered `filter_dsl` to (from `rocky compile --expand-macros`) and fails if the lowering regresses. It then builds both DuckDB tables from Rocky's compiled SQL and reports the row counts:

```
Rocky lowered the DSL filter (filter_dsl) to:
    SELECT * FROM raw_orders WHERE status IS DISTINCT FROM 'cancelled'
+-------------------------+------+
| query                   | rows |
+-------------------------+------+
| filter_dsl (NULL-safe)  | 87   |
| filter_sql (NULL-blind) | 77   |
+-------------------------+------+
```

## Related

- DSL lowering: `engine/crates/rocky-lang/src/lower.rs` (search for `IS DISTINCT FROM`)
- `docs/src/content/docs/concepts/rocky-dsl.md` ("NULL-safe operators")
