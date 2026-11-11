# 11-meltano-connector-shim — Singer tap run by Meltano as a Rocky child connector

> **Category:** 07-adapters
> **Credentials:** none (installs Meltano 4.4.1 and the POC tap into a local `.venv` on the first run; needs a package registry once)
> **Runtime:** ~20s after the first install
> **Rocky features:** `rocky load --connector`, the child connector protocol, connector state, `[connector.<name>.options]`

## Feature

A thin shim, `connector/main.py`, that runs one Singer tap of a [Meltano](https://meltano.com) project with `meltano invoke` under `rocky load --connector`. It parses the Singer messages, writes one stream to Parquet, and keeps the last STATE value in Rocky state.

## Why it's distinctive

Rocky ships no extractors and no catalog. The tap is yours. The shim only adapts Singer and Meltano to the connector protocol.

The shim uses `meltano invoke` and never `meltano run`. `meltano run` keeps Singer state in Meltano's own state backend, where Rocky's commit fence cannot see it. The only Singer state is the one in Rocky state. A crash before the state commit replays the batch. It never skips one.

```
  rocky load ──▶ main.py ──▶ meltano invoke tap --discover
      ▲                  └─▶ meltano invoke tap --catalog c.json [--state s.json]
      │  state ───▶ --state file            │ Singer messages on stdout
      │                                      ▼
      └── check file, append, save state ◀── RECORD → Parquet, last STATE → state
```

## What this shim does and does not do

| Fact | What it means for you |
|---|---|
| One stream per connector. | `options.stream` names one stream. The tap has `orders` and `customers`. The shim marks only `orders` as selected in the catalog. A name the tap lacks fails the run and lists the streams. |
| The tool's state lives in Rocky state. | The state is the last Singer STATE `value`. A `bookmarks` map is cut down to the loaded stream. |
| The tap supports bookmarks, so the state round trip is real. | `orders` is incremental on `id`. Load 2 passes the bookmark with `--state` and the tap emits only the new rows. |
| The tap is a POC fixture. | `tap-smoke-test` is not on PyPI (Meltano installs it from GitHub), so the POC ships `tap-orders`, a 60-line Singer SDK tap over two local JSONL files. It is the user's tap in this demo, not part of Rocky. |
| Delivery is at least once. | A crash between the promote and the state commit loads a batch twice. Deduplicate downstream on a key. |
| No catalog beyond the tap's own. | The shim asks the tap for its catalog with `--discover` and edits only the selection. |
| Only `RECORD` and `STATE` messages are used. | `SCHEMA` is ignored: DuckDB infers the types. A `BATCH` message fails the run. A tap that emits no `STATE` message keeps the old state. |
| Secrets stay out of the project. | Meltano writes the tap's resolved config to its system directory. The shim points `MELTANO_SYS_DIR_ROOT` at a private temp dir (mode 0700) per run and removes it. The shim always passes `--state` (`{}` on the first run), so a stale Meltano state file is never read. |
| Egress: `[]`. | Meltano usage tracking is switched off (`MELTANO_DISABLE_TRACKING=true`). The POC tap reads local files. A real tap calls its own hosts: list them in the manifest. |

## Options

Set them in `[connector.orders_tap.options]` in `rocky.toml`.

| Option | Meaning |
|---|---|
| `project_dir` | Required. The Meltano project directory, relative to `connector/main.py`. |
| `tap` | Required. The plugin name in `meltano.yml`. |
| `stream` | Required. The one stream to load. |
| `meltano_bin` | Optional. The `meltano` executable, relative to `connector/main.py`. Default `../.venv/bin/meltano`. |

## Credentials in a real deployment

Meltano reads a plugin setting from the variable `<PLUGIN>_<SETTING>`, for example `TAP_ORDERS_API_TOKEN`. Declare each name under `secrets` in `rocky.toml`. Rocky hands exactly those variables to the shim, which leaves them in the environment of `meltano invoke`. Never put a credential under `options` or in `meltano.yml`: Rocky writes `options.json` to disk in clear.

## Layout

```
.
├── README.md
├── rocky.toml             DuckDB target; [connector.orders_tap] and a failing second connector
├── run.sh                 validate → install Meltano + tap → 3 loads with asserts → state check → failure check
├── requirements.txt       meltano==4.4.1
├── meltano_project/
│   ├── meltano.yml        defines the tap-orders plugin (no Meltano Hub needed)
│   └── tap-orders/        the offline Singer SDK tap (singer-sdk==0.54.9)
└── connector/
    ├── connector.toml     manifest: command = ["./main.py"], egress_hosts = []
    └── main.py            the shim (Python 3.8+, standard library only; needs the duckdb CLI)
```

## Run

```bash
./run.sh
```

When Meltano or the tap cannot be installed, `run.sh` runs `rocky validate` and exits 0 with a SKIP message. Set `REQUIRE_MELTANO=1` to make that a failure.

## Expected output

```text
=== load 1: rocky load --connector orders_tap (expect 4 rows) ===
    status=loaded total_rows=4 commit_seq 0 -> 1
=== append orders with id 5..7 to the tap's data ===
=== load 2: rocky load --connector orders_tap (expect 3 rows) ===
    status=loaded total_rows=3 commit_seq 1 -> 2
=== load 3: rocky load --connector orders_tap (expect 0 rows) ===
    status=loaded total_rows=0 commit_seq 2 -> 3
=== target table poc.main.orders ===
    rows=7 distinct ids=7
    the other stream (customers) was not loaded
=== the Singer state lives in Rocky state ===
    load 1: bookmark id=4
    load 3: bookmark id=7
=== a stream the tap does not have fails clearly ===

POC complete: 3 loads (4, 3, 0 rows), 7 rows total, Singer bookmark carried in Rocky state.
```

## Related

- [Child connectors guide](../../../../../docs/src/content/docs/guides/child-connectors.md): the protocol, the checks and the guarantees
- [`08-postgres-child-connector`](../08-postgres-child-connector/): the reference connector this shim copies
- [`09-dlt-connector-shim`](../09-dlt-connector-shim/) and [`10-airbyte-connector-shim`](../10-airbyte-connector-shim/)
