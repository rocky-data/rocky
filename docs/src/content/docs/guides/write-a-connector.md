---
title: Write a Connector
description: Write a child connector from start to finish. Scaffold it with rocky connector init, read your source, test it with rocky connector test, and load it with rocky load --connector.
sidebar:
  order: 15
---

A child connector is a program that reads one source and writes Parquet files. Rocky starts it, checks the files, and loads them into a table. This page takes you from an empty directory to a connector that loads rows. The [Child connectors](/guides/child-connectors/) guide holds the full protocol. You do not need it to follow the steps here.

You need `rocky`, `python3` (3.8 or newer) and the `duckdb` CLI on `PATH`. The scaffold runs on Linux and macOS.

```
  rocky connector init ──► edit fetch() ──► rocky connector test ──► rocky load
   (scaffold)               (your source)    (3 runs, no load)       (real load)
```

## Step 1: Scaffold the connector

Run `rocky connector init` in the directory that holds `rocky.toml`. The name uses letters, digits and underscores.

```bash
rocky connector init orders
```

Rocky writes `connectors/orders/` and prints the next steps. The directory holds:

- `connector.toml`: the manifest, which says how Rocky starts the program.
- `main.py`: your program. This is the file you edit.
- `rocky_connector.py`: a helper. Do not edit it.
- `connector-test.toml`: the runs that `rocky connector test` makes.

The scaffold loads as written. Its example source is a fixed list of 5 rows.

## Step 2: Add the connector to rocky.toml

Add the block that the command printed. Change the target to your own catalog and schema. This example uses a DuckDB file.

```toml
[adapter]
type = "duckdb"
path = "poc.duckdb"

[connector.orders]
manifest   = "connectors/orders/connector.toml"
output_uri = "file://./.rocky/landing/orders"
target     = { catalog = "poc", schema = "main", table = "orders" }
```

Set `license` in `connectors/orders/connector.toml` to the license of your code, such as `license = "Apache-2.0"`. Until you do, `rocky validate` warns V068.

Run `rocky validate`. It reads the manifest offline and reports V067 as `ok`.

## Step 3: Read your source in fetch()

The program does one job on each run. It reads the saved cursor, fetches the rows that are newer, and writes them. Open `main.py` and replace the body of `fetch()`.

```python
def fetch(options, cursor):
    """Return the source rows newer than `cursor`, as a list of dicts."""
```

- `options` holds the table `[connector.orders.options]` from `rocky.toml`. Put settings there, such as a table name.
- `cursor` is the value the last run saved, or `None` on the first run. Return only rows with an `id` above it.
- Read a credential from the environment. Declare its name in `secrets` in `rocky.toml`. Never put a credential in `options`: Rocky writes the options to disk in clear.

`main()` writes the rows with `run.write(rows, columns=...)`. List one DuckDB type per column. Then it saves the new cursor with `run.finish({"cursor": cursor})`. When `fetch()` returns no row, the program writes no file, and Rocky still saves the state.

Keep the cursor logic. Without it, every run loads every row again. If you change the shape of the state, raise `STATE_SCHEMA_VERSION` in `main.py` and `state_schema_version` in `connector.toml` together.

## Step 4: Test with recorded input

`rocky connector test` runs the connector once for each `[[run]]` in `connector-test.toml`. It loads nothing, saves no state and passes no secrets.

```bash
rocky connector test orders
```

```
run 1: ok, 3 rows in 1 file(s)
run 2: ok, 2 rows in 1 file(s)
run 3: ok, 0 rows in 0 file(s)
Connector `orders` passed all 3 run(s) in connectors/orders/connector-test.toml
```

Each run gets the state that the run before it returned. The command checks four things:

- Rocky accepts the result file and every Parquet file.
- The run writes the number of rows that `rows` says.
- The state is `advanced` or `unchanged`, as the run declares.
- Every Parquet file has the same column names and types.

Edit `connector-test.toml` so that it replays your source and not the example. Give each run an option that points `fetch()` at a recorded file. Make that option fail the run when the file is missing. Rocky does not block the network, so a connector that ignores its replay option can pass against the live source.

The second run must read only the new rows, and the third must read none. This is how you prove that the cursor works. See [`rocky connector test`](/reference/commands/development/#rocky-connector-test) for every key.

## Step 5: Load for real

Export the variables you declared under `secrets`. Then run the connector.

```bash
rocky load --connector orders
```

Rocky starts the program with a cleared environment, checks the files, and appends the rows to the target table. It then saves the cursor. Run the command a second time. The second run loads only the rows that are new, and the output shows `commit_seq_after` one higher.

Delivery is at least once. A crash between the load and the state save loads a batch twice. Deduplicate downstream on a key.

## Read a finished connector

The Postgres connector is the reference connector. The Rocky project maintains it and tests it in CI. Its manifest sets `support_tier = "reference"`. Read it to see a cursor, a replay mode and a secret in real code.

- The program, manifest, spec and recorded input are in [`07-adapters/08-postgres-child-connector`](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/07-adapters/08-postgres-child-connector).
- It keeps a timestamp cursor and holds back the rows at the source maximum, so rows that tie are never split across two runs.
- Its `replay_fixture` option reads a SQL file instead of Postgres. `rocky connector test pg_events` uses it with no Docker.
- It was written before `rocky connector init`, so it does not use the `rocky_connector.py` helper. It follows the same protocol.

Two checks keep it working. The pull request CI runs `rocky connector test pg_events` when connector code or this connector changes. The weekly CI runs `run.sh` against a Postgres container. A failure of either turns the job red. The [Child connectors](/guides/child-connectors/#reference-connector) guide lists the limits of its design.

## Related

- [Child connectors](/guides/child-connectors/): the protocol, the checks and the guarantees
- [`rocky connector init`](/reference/commands/development/#rocky-connector-init) and [`rocky connector test`](/reference/commands/development/#rocky-connector-test)
- [Connector shims](/guides/child-connectors/#connector-shims): run dlt, Airbyte or Meltano as a child connector
