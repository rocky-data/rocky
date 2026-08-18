# Seed demo

This example shows `rocky seed`. The command reads CSV files from a directory
and loads each one into a table. Use it for small reference data that lives in
git: country codes, category lookups, date spines, test fixtures.

`rocky seed` writes to your warehouse. For every file it loads, it runs
`DROP TABLE IF EXISTS` and then recreates the table. Anything already in that
table is gone.

## The files

```
seed-demo/
  rocky.toml              # DuckDB adapter, one replication pipeline
  seeds/
    customers.csv         # 8 rows: id, name, email, country, created_at
    products.csv          # 7 rows: id, name, price, category
```

## What one seed file goes through

```
  seeds/customers.csv
         │
         ▼
  ┌──────────────┐  reads the header row and samples up to 1000 data rows
  │ infer types  │
  └──────┬───────┘
         │  id BIGINT, name STRING, …
         ▼
  ┌──────────────┐  create the schema if the dialect supports it
  │ create       │  drop the table if it exists
  │ the table    │  create <catalog>.<schema>.customers (id BIGINT, …)
  └──────┬───────┘
         │
         ▼
  ┌──────────────┐  INSERT INTO … VALUES (…), (…), … in batches
  │ load rows    │
  └──────┬───────┘
         │
         ▼
  8 rows in <catalog>.<schema>.customers
```

Rocky picks up every `.csv` file directly inside the seeds directory. It does
not descend into subdirectories.

Rocky also finds `.tsv` files, but only a single-column one loads. Type
inference splits every seed file on commas. A tab-separated header of two or
more columns therefore becomes one column name, and the `CREATE TABLE` fails.

## Run it

```bash
cd engine/examples/seed-demo
rocky seed --seeds seeds/
```

Load one file by name:

```bash
rocky seed --seeds seeds/ --filter customers
```

Get machine-readable results:

```bash
rocky seed --seeds seeds/ --output json
```

The flag is `--seeds`, and it defaults to `seeds`. Add `--pipeline <name>` when
the config declares more than one pipeline.

## Where the table lands

Rocky resolves each table's three parts in this order.

| Part | First choice | Fallback |
|------|--------------|----------|
| Catalog | sidecar `[target] catalog` | the replication pipeline's `catalog_template`, otherwise `main` |
| Schema | sidecar `[target] schema` | `seeds` |
| Table | sidecar `[target] table` | the seed's name |

The seed's name is the sidecar `name` when the sidecar sets one, and the file
name without its extension otherwise.

Rocky uses `main` when the pipeline is not a replication pipeline, and when the
`catalog_template` holds a `{placeholder}` it cannot resolve here.

`catalog` and `table` are optional inside a `[target]` block. `schema` is not.
A `[target]` block without `schema` stops the whole command at discovery,
before Rocky loads any seed:

```
Error: failed to discover seeds in seeds/

Caused by:
    0: failed to parse sidecar TOML seeds/customers.toml: TOML parse error at line 1, column 1
         |
       1 | [target]
         | ^^^^^^^^
       missing field `schema`
    ...
```

## The catalog must exist

The command succeeds because the config points DuckDB at a file:

```
path = "warehouse.duckdb"   ->   the file's catalog is `warehouse`
catalog_template            ->   `warehouse`   (they match)
```

A file-backed DuckDB names its catalog after the database file. An adapter
with no `path` opens an in-memory database whose only catalog is `memory` —
with the same config, every seed then fails with
`Catalog "warehouse" does not exist`. A real warehouse uses its own catalog
names. Match `catalog_template` to whichever you use.

To redirect a single seed, put a `[target]` block in its sidecar. One sidecar
covers one seed, so write one next to each CSV you want to move:

```toml
# seeds/customers.toml
[target]
catalog = "warehouse"
schema = "reference"
```

The first run creates `warehouse.duckdb` next to `rocky.toml`. Delete it to
start over; git ignores it.

## Type inference

Rocky reads the header row, then samples up to 1000 data rows. It widens a
column's type as it goes and picks the first rule that fits each value.

| Example value | Type |
|---------------|------|
| `true`, `FALSE` | `BOOLEAN` |
| `1`, `-42` | `BIGINT` |
| `29.99` | `DOUBLE` |
| `2024-01-15`, `2024-01-15 09:30:00` | `TIMESTAMP` |
| `alice@example.com` | `STRING` |

Empty cells never narrow a type. A column whose sampled cells are all empty
becomes `STRING`.

Pin a type in the sidecar when the heuristic guesses wrong:

```toml
# seeds/customers.toml
[column_types]
id = "INTEGER"
```

A pinned column skips inference entirely.

## Other sidecar keys

- `name` renames the seed. `--filter` matches this name, and so does the table
  name when `[target] table` is absent.
- `pre_hook` is a list of SQL statements run before any write, including before
  the `DROP TABLE`. A failing statement aborts that seed with nothing written.
  Use it to refuse a reload when the target already holds rows.
- `post_hook` is a list of SQL statements run after a successful load.
