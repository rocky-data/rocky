# 10-dlt-connector-shim — dlt source as a Rocky child connector

> **Credentials:** none (installs dlt 1.32.0 into a POC-local `.venv` on the first run; needs a package registry once)
> **Runtime:** ~15s after the first install

## What it shows

A shim, `connector/main.py`, runs your [dlt](https://dlthub.com) source under `rocky load --connector`. dlt writes Parquet with its `filesystem` destination. Rocky ships no extractors; the shim only adapts dlt's output and state.

dlt's incremental cursors are restored from Rocky state before the run and handed back after it, so Rocky's commit fence governs them. A crash before the state commit replays the batch; it never skips one.

## What this shim does and does not do

| Fact | What it means for you |
|---|---|
| One stream per connector. | `options.stream` names one resource; only it runs. The POC loads `orders`, not `customers`. A name the source lacks fails the run and lists the streams. |
| One table per connector. | The shim follows dlt's table naming. A resource that writes two tables (a nested list) fails. |
| The tool's state lives in Rocky state. | It is dlt's `state["sources"]`. dlt's pipeline directory is in scratch and never read again, and dlt does not restore state from the destination (`RESTORE_FROM_DESTINATION=false`). |
| Delivery is at least once. | A crash between the promote and the state commit loads a batch twice. Deduplicate downstream on a key. |
| No catalog. | Rocky does not discover dlt sources. You name the module and function. |
| The state must stay under 1 MiB. | The shim stops at 900 KiB. A resource with a cursor and no `primary_key` keeps a growing hash per row in `unique_hashes`. Give it a `primary_key`, or a cursor that only moves forward. |
| It pins dlt. | `requirements.txt` pins `dlt[parquet]==1.32.0`. State carries `dlt_version`; dlt can change the state shape, so test upgrades before trusting old state. |
| Egress: `[]`. | The shim calls no host and dlt telemetry is off. If your source calls hosts, list them in the manifest. |

## Options

Set in `[connector.shop_orders.options]` in `rocky.toml`.

| Option | Meaning |
|---|---|
| `source_dir` | Directory of your dlt source module, relative to `connector/main.py`. |
| `module` | Module name inside `source_dir`. |
| `source` | Function that returns a dlt source. |
| `stream` | The one resource to load. |
| `source_args` | Optional table of keyword arguments. |
| `pipeline_name` | Optional. Default `rocky_dlt`. |

## Credentials in a real deployment

dlt reads credentials from the environment, e.g. `SOURCES__MY_SOURCE__API_KEY`. Declare each name under `secrets` in `rocky.toml`; Rocky hands exactly those to the shim. Never put a credential under `options`: Rocky writes `options.json` to disk in clear.

## Run

Needs `rocky`, the `duckdb` CLI, and `uv` or `python3 -m venv`.

```bash
./run.sh
```

When dlt cannot be installed, `run.sh` runs `rocky validate` and exits 0 with a SKIP message. `REQUIRE_DLT=1` makes that a failure.

## Expected output

First a contract-gated load fails and the state does not move. Then three loads of 5, 3 and 0 rows leave 8 rows in `poc.main.orders`, and the saved dlt cursor is 8. A missing stream and a nested resource fail clearly; renamed tables load.

```text
POC complete: 3 loads (5, 3, 0 rows), 8 rows total, dlt cursor carried in Rocky state.
```

## Related

- [Child connectors guide](../../../../../docs/src/content/docs/guides/child-connectors.md)
- [`08-postgres-child-connector`](../08-postgres-child-connector/): the reference connector this shim copies
