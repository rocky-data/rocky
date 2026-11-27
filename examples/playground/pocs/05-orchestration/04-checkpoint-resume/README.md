# 04-checkpoint-resume — `rocky run --resume-latest` after a partial failure

> **Category:** 05-orchestration
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `--resume-latest`, per-table run progress in the state store, pipeline-scoped checkpoints

## What it shows

`rocky run` records per-table progress. After a partial failure,
`rocky run --resume-latest` skips the tables that succeeded and retries the rest.

A checkpoint belongs to its scope: the pipeline, its `--filter`, and where it
writes. `--resume-latest` from another scope is refused, and so is one when the
latest run in scope succeeded. (An explicit `--resume <run-id>` checks the scope
but not whether that run succeeded.)

## How it works

```
run 1  (no filter)         orders ok, customers ok, products FAILS   exit 2
                           checkpoint: orders + customers recorded as Success
   |
   +-- resume --filter source=orders   refused: different scope, no checkpoint visible
   |
run 2  --resume-latest     skips orders + customers, copies products   exit 0
                           output carries resumed_from = <run 1 id>
   |
   +-- resume again        refused: the latest run succeeded
```

The failure is induced by a view Rocky did not create occupying the `products`
target (matching column, so drift detection has nothing to alter). DuckDB refuses
to replace it (`Existing object products is of type View, trying to replace with
type Table`). The script drops the view before resuming.

## Run

```bash
./run.sh
```

## Expected output

- Run 1 exits 2: `status = PartialFailure`, `tables_copied = 2`,
  `tables_failed = 1`; the error names the view.
- The `--filter source=orders` resume is refused with `no progress found`.
- Run 2 exits 0: `tables_skipped = 2`, `tables_copied = 1`, `resumed_from`
  naming run 1.
- A third `--resume-latest` is refused:
  `nothing to resume: run <run 2 id> already succeeded`.
