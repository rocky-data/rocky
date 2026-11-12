# 10-dlt-connector-shim — dlt source as a Rocky child connector

> **Category:** 07-adapters
> **Credentials:** none (installs dlt 1.32.0 into a POC-local `.venv` on the first run; needs a package registry once)
> **Runtime:** ~15s after the first install
> **Rocky features:** `rocky load --connector`, the child connector protocol, connector state, `[connector.<name>.options]`

## Feature

A thin shim, `connector/main.py`, that runs one user [dlt](https://dlthub.com) source under `rocky load --connector`. dlt writes Parquet with its `filesystem` destination. The shim lists the files of one stream, and keeps dlt's incremental state in Rocky state.

## Why it's distinctive

Rocky ships no extractors and no catalog. You bring the dlt source. The shim only adapts dlt's output and state to the connector protocol.

dlt keeps its cursors (`dlt.sources.incremental`) in its own pipeline state. The shim does not let dlt keep them anywhere else. It restores them from Rocky state before the run, and hands them back to Rocky after it. Rocky's commit fence then governs them. A crash before the state commit replays the batch. It never skips one.

```
  rocky load ──▶ main.py ──▶ dlt pipeline ──▶ Parquet (filesystem destination)
      ▲  state ───────────▶ managed_state()        │
      │                                            ▼
      └── check files, append, save state ◀── {"dlt_version", "sources"}
```

## What this shim does and does not do

| Fact | What it means for you |
|---|---|
| One stream per connector. | `options.stream` names one resource of the source. The source here has `orders` and `customers`. The shim loads `orders` and runs only that resource. A name the source lacks fails the run and lists the streams. |
| The tool's state lives in Rocky state. | The state is dlt's `state["sources"]`. dlt's own pipeline directory is in the scratch directory and is never read again. dlt does not restore state from the destination (`RESTORE_FROM_DESTINATION=false`). |
| Delivery is at least once. | A crash between the promote and the state commit loads a batch twice. Deduplicate downstream on a key. |
| No catalog. | Rocky does not list or discover dlt sources. You name the module and the function. |
| The state must stay under 1 MiB. | The shim stops at 900 KiB. A resource with a cursor and no `primary_key` keeps a hash per row in `unique_hashes`. That grows. Give the resource a `primary_key`, or a cursor that moves forward only. |
| It pins dlt. | `requirements.txt` pins `dlt[parquet]==1.32.0`. The state carries `dlt_version`. dlt can change the state shape, so test an upgrade before you trust old state. |
| Egress: `[]`. | The shim calls no host. dlt telemetry is switched off. Your dlt source may call hosts: list them in the manifest. |

## Options

Set them in `[connector.shop_orders.options]` in `rocky.toml`.

| Option | Meaning |
|---|---|
| `source_dir` | Directory of your dlt source module, relative to `connector/main.py`. |
| `module` | Module name inside `source_dir`. |
| `source` | Function that returns a dlt source. |
| `stream` | The one resource to load. |
| `source_args` | Optional table of keyword arguments for the function. |
| `pipeline_name` | Optional. Default `rocky_dlt`. |

## Credentials in a real deployment

dlt reads its credentials from the environment, for example `SOURCES__MY_SOURCE__API_KEY`. Declare each name under `secrets` in `rocky.toml`. Rocky hands exactly those variables to the shim, and dlt reads them. Never put a credential under `options`: Rocky writes `options.json` to disk in clear.

## Layout

```
.
├── README.md
├── rocky.toml             DuckDB target; [connector.shop_orders] and a failing second connector
├── run.sh                 validate → install dlt → 3 loads with asserts → state check → failure check
├── requirements.txt       dlt[parquet]==1.32.0
├── shop_src/shop.py       the user's offline dlt source: orders (incremental on id), customers
└── connector/
    ├── connector.toml     manifest: command = ["./launch.sh"], egress_hosts = []
    ├── launch.sh          starts main.py with the .venv Python
    └── main.py            the shim (Python 3.8+ syntax; needs dlt in the .venv, and the duckdb CLI)
```

## Run

```bash
./run.sh
```

When dlt cannot be installed, `run.sh` runs `rocky validate` and exits 0 with a SKIP message. Set `REQUIRE_DLT=1` to make that a failure.

## Expected output

```text
=== load 1: rocky load --connector shop_orders (expect 5 rows) ===
    status=loaded total_rows=5 commit_seq 0 -> 1
=== append orders with id 6..8 to the source ===
=== load 2: rocky load --connector shop_orders (expect 3 rows) ===
    status=loaded total_rows=3 commit_seq 1 -> 2
=== load 3: rocky load --connector shop_orders (expect 0 rows) ===
    status=loaded total_rows=0 commit_seq 2 -> 3
=== target table poc.main.orders ===
    rows=8 distinct ids=8
    the other stream (customers) was not loaded
=== dlt state lives in Rocky state (the result of the last run) ===
    dlt_version=1.32.0 last_value=8
=== a stream the source does not have fails clearly ===
    failed as expected: the source has no stream 'refunds'; ...

POC complete: 3 loads (5, 3, 0 rows), 8 rows total, dlt cursor carried in Rocky state.
```

## Related

- [Child connectors guide](../../../../../docs/src/content/docs/guides/child-connectors.md): the protocol, the checks and the guarantees
- [`08-postgres-child-connector`](../08-postgres-child-connector/): the reference connector this shim copies
- [`11-airbyte-connector-shim`](../11-airbyte-connector-shim/) and [`12-meltano-connector-shim`](../12-meltano-connector-shim/)
