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

The source is `warehouse.raw.customers`. The target is
`warehouse.history.customers_history`. DuckDB names a file database's
catalog after the file, so `path = "warehouse.duckdb"` gives the catalog
`warehouse`.

`auto_create_schemas = true` under `[pipeline.customers_history.target.governance]`
makes Rocky create the `history` schema before the first load.

## The statements Rocky generates

```
  warehouse.raw.customers               warehouse.history.customers_history
         │                                          │
         │  create_schema ── CREATE SCHEMA IF NOT EXISTS history
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

Run these from the repository root. The seed needs the
[DuckDB CLI](https://duckdb.org/docs/installation/).

```bash
cd engine/examples/snapshot
duckdb warehouse.duckdb < seed.sql
rocky snapshot --dry-run
rocky snapshot
```

`--dry-run` prints the statements and executes none of them. It still
builds the target adapter, because the adapter chooses the SQL dialect. It
reads no rows from `warehouse.raw.customers`.

`rocky snapshot` creates `history.customers_history` and writes the first
version of each customer. Change a row and run it again:

```bash
duckdb warehouse.duckdb "UPDATE raw.customers SET name = 'Ada L.', updated_at = TIMESTAMP '2026-02-01' WHERE customer_id = 1"
rocky snapshot
duckdb warehouse.duckdb "SELECT customer_id, name, is_current FROM history.customers_history ORDER BY customer_id, valid_from"
```

Customer 1 now has two rows. The old row has `is_current = false` and a
`valid_to`. The new row is current.

Rocky also writes one directory. It creates `.rocky/` here and logs the run to
`.rocky/traces/{timestamp}-{pid}.jsonl`, one file per process. It also writes
`.rocky/.gitignore`, which holds a comment and a single `*`, so git ignores
the whole directory. Delete `.rocky/` and `warehouse.duckdb` when you are done.

For machine-readable output:

```bash
rocky --output json snapshot --dry-run
```

On DuckDB, `merge_1` inserts new keys with `INSERT BY NAME` from a source
subquery that adds the history columns. DuckDB's MERGE rejects the
`INSERT (*) VALUES (source.*, ...)` form that Databricks accepts.

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
