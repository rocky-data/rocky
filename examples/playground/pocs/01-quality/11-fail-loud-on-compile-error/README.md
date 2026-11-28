# Fail loud on a compile error

## Feature

`rocky run` over a project with a model that fails to compile exits non-zero, reports status `PartialFailure` and records the model as a `compile-error`. The broken model is excluded from execution; the clean model in the same run still materializes.

The broken model, `daily_revenue`, is a `time_interval` model whose `time_column = "order_date"` is missing from its `SELECT` output (**E020**, caught before any SQL reaches the warehouse). `stg_orders` is the clean one. In its final step `run.sh` applies `daily_revenue.fixed.sql` and shows a green re-run.

## Run

```bash
cd examples/playground/pocs/01-quality/11-fail-loud-on-compile-error
./run.sh
```

No credentials required (DuckDB only). `run.sh` needs `rocky`, the `duckdb` CLI and `jq` on `PATH`.

## Expected output

```
==== 3. assert: the run exited NON-ZERO ====
OK: run exited 2 (non-zero) instead of silently succeeding

==== 4. assert: status is PartialFailure with a compile-error on daily_revenue ====
status: PartialFailure
errors recorded by the run:
  - daily_revenue: failure_kind=compile-error :: [E020] time_column 'order_date' is not in the output schema of model 'daily_revenue'
OK: daily_revenue failed loud with failure_kind=compile-error (E020)

==== 5. assert: the broken model's table is ABSENT (never built) ====
OK: poc.main.daily_revenue does not exist

==== 6. assert: the clean model's data DID land ====
poc.main.stg_orders rows: 30
OK: the clean model materialized 30 rows despite the sibling failure

==== 7. apply the one-line fix and re-run — the same pipeline goes green ====
fixed run status: Success (tables_failed: 0)
OK: with the compile error fixed, the same pipeline runs green and builds daily_revenue

PASS: rocky run failed loud on the compile error ...
```

The script exits 0 only because the run **correctly failed**.
