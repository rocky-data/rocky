# 04-checkpoint-resume — `rocky run --resume-latest` after a partial failure

> **Category:** 05-orchestration
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `--resume-latest`, per-table run progress in the state store, pipeline-scoped checkpoints

## What it shows

`rocky run` records per-table progress in the state store as it processes a
replication pipeline. When a run fails partway, `rocky run --resume-latest`
reads that run's checkpoint, skips the tables that already succeeded, and
retries only the ones that did not.

A checkpoint belongs to the scope that wrote it: the pipeline, its `--filter`,
and where it writes. A `--resume-latest` from another scope is refused, and
`--resume-latest` refuses when the latest run in scope succeeded. Both
refusals are demonstrated here. (An explicit `--resume <run-id>` checks the
scope but not whether that run succeeded.)

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

The failure is induced by occupying the `products` target with a view that
Rocky did not create, with a column that matches the source so drift detection
has nothing to alter. `full_refresh` then replaces the target table, and DuckDB
refuses to replace a view with a table (`Existing object products is of type
View, trying to replace with type Table`), so that one table fails while the
other two copy. The script asserts that error, then drops the view before
resuming.

## Layout

```
.
├── README.md
├── rocky.toml
├── run.sh           Demonstrates the --resume-latest contract
└── data/seed.sql
```

## Run

```bash
./run.sh
```

## Expected output

- `Run 1` exits 2 with `status = PartialFailure`, `tables_copied = 2`,
  `tables_failed = 1`, and the recorded error names the view.
- A resume with `--filter source=orders` is refused with `no progress found`:
  the filter makes it a different scope, and the checkpoint is not visible
  from there.
- `Run 2 (--resume-latest)` exits 0 with `tables_skipped = 2`,
  `tables_copied = 1`, and a `resumed_from` naming run 1.
- A third `--resume-latest` is refused: `nothing to resume: run <run 2 id>
  already succeeded`.
