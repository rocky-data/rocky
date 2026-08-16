# Snapshot

A snapshot pipeline that tracks row history with the SCD Type 2 pattern
(slowly changing dimensions: keep the old row, mark it closed, insert the new
version). The whole example is one `rocky.toml`. There are no model files.

## What the config declares

```toml
[pipeline.customers_history]
type = "snapshot"
unique_key = ["customer_id"]
updated_at = "updated_at"
invalidate_hard_deletes = true
```

- `unique_key` identifies a row across versions.
- `updated_at` is the column Rocky compares to detect a change.
- `invalidate_hard_deletes` closes rows that vanished from the source.

The source is `main.raw.customers`. The target is
`main.history.customers_history`.

## The four statements Rocky generates

```
  main.raw.customers                    main.history.customers_history
         │                                          │
         │  initial_load ─── CREATE TABLE IF NOT EXISTS, source columns
         │                   plus valid_from, valid_to, is_current, snapshot_id
         │                                          │
         ├─ merge_1 ──► updated_at differs?  ──► close the current row
         │              (IS DISTINCT FROM)        valid_to = now, is_current = FALSE
         │              new key?             ──► INSERT first version
         │                                          │
         ├─ merge_2 ──► key closed but has no current row
         │                                    ──► INSERT the new version
         │                                          │
         └─ merge_3 ──► key gone from source  ──► close the current row
                        (invalidate_hard_deletes)
```

## Try it

Run these from the repository root:

```bash
cd engine/examples/snapshot
rocky snapshot --dry-run
```

`--dry-run` prints the four statements and executes none of them. It still
builds the target adapter, because the adapter chooses the SQL dialect. It
reads no rows from `main.raw.customers`.

It does write one directory. Rocky creates `.rocky/` here and logs the run to
`.rocky/traces/{timestamp}-{pid}.jsonl`, one file per process. It also writes
`.rocky/.gitignore`, which holds a comment and a single `*`, so git ignores
the whole directory. Delete `.rocky/` when you are done.

```bash
rocky --output json snapshot --dry-run
```

Without `--dry-run`, `rocky snapshot` executes those statements against the
configured adapter. It reads `main.raw.customers` and writes
`main.history.customers_history`. This example ships neither table, so create
`main.raw.customers` first.

## The four history columns

`initial_load` adds them to the target:

| Column | Meaning |
|---|---|
| `valid_from` | when this version became current |
| `valid_to` | when it stopped being current; NULL while current |
| `is_current` | TRUE for the live version of a key |
| `snapshot_id` | identifier of the snapshot run that wrote the row |

## Where `--config` goes

`--config` is a top-level flag, not a per-command flag. It comes before the
subcommand, never after:

```bash
rocky --config rocky.toml snapshot --dry-run   # works
rocky snapshot --dry-run --config rocky.toml   # error: unexpected argument '--config' found
```

`rocky snapshot` reads only the config, so a path from anywhere works. From
the repository root:

```bash
rocky --config engine/examples/snapshot/rocky.toml snapshot --dry-run
```

Pass `--pipeline <name>` when a config declares more than one pipeline. This
one declares a single pipeline, so the flag is optional here.
