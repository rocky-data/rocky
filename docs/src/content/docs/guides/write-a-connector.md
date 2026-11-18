---
title: Write a Connector
description: Build a child connector from start to finish. Scaffold it with rocky connector init, write the read, test it against recorded input with rocky connector test, then load it with rocky load --connector.
sidebar:
  order: 14
---

This page builds one child connector from start to finish. A child connector is a program you write. Rocky starts it, and the program writes Parquet files that Rocky loads into one table. The example reads orders from an HTTP API and loads only the orders it has not loaded before.

```
 rocky connector init ──► write fetch() ──► rocky connector test ──► rocky load --connector
   (the files)            (your source)       (recorded input,          (the live source,
                                               no warehouse)             into the table)
```

For how a run works and what Rocky guarantees, see [Child Connectors](/guides/child-connectors/). This page is the procedure.

## Before you start

You need three things on the machine that runs Rocky. The scaffold runs on Linux and macOS, because Rocky starts `main.py` through its `#!` line.

- `python3`, version 3.8 or newer. The scaffold uses the standard library only.
- The `duckdb` CLI on `PATH`. The helper uses it to write Parquet.
- A Rocky project: a directory with a `rocky.toml` that has an `[adapter]` block.

The examples use a DuckDB warehouse:

```toml
[adapter]
type = "duckdb"
path = "shop.duckdb"
```

## Step 1: Scaffold the connector

Run `rocky connector init` in the project directory. The name must be a plain identifier: letters, digits and `_`.

```bash
rocky connector init orders
```

Rocky writes the directory `connectors/orders/` and prints the next steps:

| File | What it is |
|---|---|
| `connector.toml` | The manifest: the program to run and its state version. |
| `main.py` | Your program. You replace its `fetch()` function. |
| `rocky_connector.py` | The helper. It reads the request, writes Parquet and writes the result. Do not edit it. |
| `connector-test.toml` | The runs for `rocky connector test`. |
| `.gitignore` | Ignores `__pycache__/`. |

The scaffold works as written. Its `fetch()` returns example rows, so you can run every step below before you change it. Use `--dir <path>` to put the connector in another directory.

## Step 2: Add the connector to rocky.toml

Add the block that `rocky connector init` printed to `rocky.toml`. Set the target catalog and schema. For a DuckDB warehouse, the catalog is the database name: the file name of `path` without `.duckdb`.

```toml
[connector.orders]
manifest   = "connectors/orders/connector.toml"
output_uri = "file://./.rocky/landing/orders"
target     = { catalog = "shop", schema = "main", table = "orders" }
secrets    = ["SHOP_API_TOKEN"]

[connector.orders.options]
base_url = "https://shop.example.com/api"
```

- `manifest` is relative to the directory that holds `rocky.toml`.
- `secrets` lists the environment variables Rocky hands to the program. The program gets no other variables from your shell, except `PATH` and `LANG`.
- `[connector.orders.options]` holds settings the program reads. Never put a credential here: Rocky writes the options to a file in clear.

## Step 3: Write fetch()

Replace `main.py` with a program that reads your source. The program below calls the API with the token, and asks only for orders above the cursor. The cursor is the largest order id the last load saved.

```python
#!/usr/bin/env python3
"""orders: load new orders from the shop API into Rocky.

State (schema version 1): {"cursor": <the largest order id loaded> | null}
"""

import json
import os
import urllib.parse
import urllib.request

import rocky_connector

STATE_SCHEMA_VERSION = 1
HERE = os.path.dirname(os.path.abspath(__file__))


def fetch(options, cursor):
    """Return the orders with an id above `cursor`, oldest first."""
    replay = options.get("replay_file")
    if replay is not None:
        # Recorded input for `rocky connector test`. A missing file fails
        # the run: it never falls back to the live API.
        path = os.path.join(HERE, replay)
        if not os.path.isfile(path):
            rocky_connector.fail("replay_file %s does not exist" % replay)
        with open(path, encoding="utf-8") as handle:
            orders = json.load(handle)
    else:
        query = urllib.parse.urlencode({"since_id": cursor or 0})
        request = urllib.request.Request(
            "%s/orders?%s" % (options["base_url"], query),
            headers={"Authorization": "Bearer " + os.environ["SHOP_API_TOKEN"]},
        )
        with urllib.request.urlopen(request, timeout=60) as response:
            orders = json.load(response)
    rows = [o for o in orders if cursor is None or o["id"] > cursor]
    return sorted(rows, key=lambda o: o["id"])


def main():
    run = rocky_connector.start(STATE_SCHEMA_VERSION)
    cursor = run.state.get("cursor") if run.state else None
    rows = fetch(run.options, cursor)
    if rows:
        run.write(
            rows,
            columns={
                "id": "BIGINT",
                "customer": "VARCHAR",
                "amount_cents": "BIGINT",
                "created_at": "TIMESTAMP",
            },
        )
        cursor = rows[-1]["id"]
    run.finish({"cursor": cursor})


if __name__ == "__main__":
    main()
```

The program has three parts:

1. `rocky_connector.start()` reads the request. `run.state` is `None` on the first load. After that, it is the value the last successful load passed to `run.finish()`.
2. `run.write(rows, columns=...)` writes one Parquet file. `columns` gives each column a DuckDB type. A row with a key that is not in `columns` stops the run, so a new field in the API never disappears without a warning. A row that leaves out a column gets NULL there.
3. `run.finish(state)` writes the result. Rocky saves the state only after the load into the table succeeds. Pass the state on unchanged when there are no new rows.

The `replay_file` option is how the next step tests the program without the API. Give your connector an option like it. Make the option fail the run when its input is missing, as above.

:::caution[Keep the state small and in step with the code]
Rocky saves the state as JSON, up to 1 MiB. If you change its shape, raise `state_schema_version` in `connector.toml` and `STATE_SCHEMA_VERSION` in `main.py` together. A load with the old state then stops with an error instead of reading the state wrongly.
:::

## Step 4: Finish the manifest

Open `connectors/orders/connector.toml` and set two keys.

```toml
egress_hosts         = ["shop.example.com"]
license              = "MIT"
```

- `egress_hosts` lists the hosts the program calls. Rocky prints the list in its output and logs. It does not enforce it.
- `license` is the license of the code in this directory, as an SPDX identifier. Rocky reports it in the load output. While it is not set, `rocky validate` warns (V068) and the load output reports `unknown`.

## Step 5: Test it against recorded input

`rocky connector test` runs the program once per `[[run]]` in `connector-test.toml`. It checks the rows and the state of each run, and loads nothing. Record what the API returns, and save it beside `main.py`:

- `connectors/orders/replay/orders-1.json`: orders 1 to 3.
- `connectors/orders/replay/orders-2.json`: orders 1 to 5, as the API returns them later.

Each file is a JSON list of orders:

```json
[
  {"id": 1, "customer": "c1", "amount_cents": 250, "created_at": "2026-11-02T10:00:00"}
]
```

Replace `connector-test.toml` with three runs that use the recordings:

```toml
[[run]]
options = { replay_file = "replay/orders-1.json" }
rows    = 3   # orders 1 to 3
state   = "advanced"

[[run]]
options = { replay_file = "replay/orders-2.json" }
rows    = 2   # only orders 4 and 5: the cursor came back as state
state   = "advanced"

[[run]]
options = { replay_file = "replay/orders-2.json" }
rows    = 0   # nothing new
state   = "unchanged"
```

Each run gets the state the run before it returned. Run 2 writes 2 rows only if the cursor from run 1 reached it. Run 3 checks that a load with nothing new keeps the cursor.

```bash
rocky connector test orders
```

```
run 1: ok, 3 rows in 1 file(s)
run 2: ok, 2 rows in 1 file(s)
run 3: ok, 0 rows in 0 file(s)
Connector `orders` passed all 3 run(s) in /path/to/project/connectors/orders/connector-test.toml
```

The command also checks that every Parquet file has the same column names and types. It exits non-zero when a check fails, so you can run it in CI. It passes the program no secrets, but it does not block the network. A program that ignores its replay option and reaches a public source can still pass.

## Step 6: Load the live source

Set the secret in the environment, then load.

```bash
export SHOP_API_TOKEN=...
rocky load --connector orders
```

```
Connector `orders` loaded 1 file(s), 3 rows, 690 bytes (1 attempt(s), commit 0 -> 1, 377 ms)
```

Run it again later. The program reads the saved cursor and asks only for the newer orders:

```
Connector `orders` loaded 1 file(s), 2 rows, 669 bytes (1 attempt(s), commit 1 -> 2, 282 ms)
```

A load with no new orders loads no file and still saves the state. Each line shows the commit number moving forward: Rocky saved the state of that load. To run the connector on a schedule, run the same command from your scheduler.

## When a step fails

The program's stderr appears in Rocky's log, one line per line. These are the common failures:

| Message | Cause | Fix |
|---|---|---|
| ``the connector declares the secret `SHOP_API_TOKEN`, but that environment variable is not set`` | A name in `secrets` is not set in Rocky's environment. | Set it before `rocky load`. |
| `row 0 has columns that are not in columns: ['note']` | The source returned a field that `columns` does not list. | Add the column and its type to `columns`, or drop the field in `fetch()`. |
| `the saved state has schema version 1, but this connector reads 2` | `STATE_SCHEMA_VERSION` changed and the saved state is older. | Go back to the old version. The helper refuses a state of another version before `fetch()` runs, and Rocky has no command that resets a connector's state. |
| `the run wrote 3 rows, but the spec expects 2` | A `[[run]]` in `connector-test.toml` saw other rows than it declares. Often the cursor did not reach the run. | Check the state the program passes to `run.finish()`. |
| `the connector exited with code 1` | The program failed. | Read the program's own error line just above it in the log. |

## Keep the connector tested

Run `rocky connector test` in CI on each change to the connector. The step needs `python3` and the `duckdb` CLI, and no warehouse and no secrets:

```bash
rocky connector test orders --output json
```

The [reference Postgres connector](/guides/child-connectors/#reference-connector) is a complete example with a replay mode. Its `connector-test.toml` replays three loads with no Postgres server.

## Related

- [Child Connectors](/guides/child-connectors/) -- how a run works, the protocol, limits and guarantees
- [`rocky connector init`](/reference/commands/development/#rocky-connector-init) -- the scaffold
- [`rocky connector test`](/reference/commands/development/#rocky-connector-test) -- the spec format and the checks
