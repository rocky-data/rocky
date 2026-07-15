# Fail loud on a compile error

## Feature

`rocky run` over a project that contains a model with a real compile error
fails the run loudly: it exits non-zero, reports `partial_failure`, and records
the broken model as a `compile-error` instead of quietly reporting a green run
over a half-built warehouse.

## Why it's distinctive

A pipeline is only trustworthy if a broken model can't masquerade as a healthy
one. Rocky type-checks every model before issuing SQL, so a model that can't
compile is excluded from execution and surfaced as a failure rather than
skipped in silence. The run's exit code and JSON status both reflect that one
model didn't build, which is what lets an orchestrator (or a human) stop and
look instead of shipping downstream on stale or missing data.

Just as important: the failure is contained. The clean model in the same run
still materializes. Rocky fails the broken model loudly without throwing away
the good data that compiled fine, so a partial outage stays partial.

The broken model here is a `time_interval` model whose declared
`time_column = "order_date"` is absent from its `SELECT` output. Rocky catches
that as **E020** at compile time, before any SQL reaches the warehouse.

## Layout

```
rocky.toml                  Transformation pipeline over models/**
models/
  stg_orders.sql/.toml      Clean full_refresh model — always materializes
  daily_revenue.sql/.toml   Broken time_interval model — E020 (time_column
                            order_date missing from the SELECT output)
daily_revenue.fixed.sql     The corrected SELECT (outputs order_date); run.sh
                            applies it in the final step to show a green re-run
data/seed.sql               One raw__sales.orders table feeding both models
run.sh                      Runs the broken pipeline, asserts the loud failure
                            + that good data landed, then fixes and re-runs
```

## Run

```bash
cd examples/playground/pocs/01-quality/11-fail-loud-on-compile-error
./run.sh
```

No credentials required — DuckDB only.

## Expected output

The broken run exits non-zero and the script asserts every claim:

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

The script exits 0 only because the run **correctly failed**: a green exit here
proves Rocky reported the failure, kept the broken table out of the warehouse,
and still landed the data that compiled.
