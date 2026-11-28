# 12-meltano-connector-shim — Singer tap run by Meltano as a Rocky child connector

> **Credentials:** none (installs Meltano 4.4.1 and the POC tap into a local `.venv` on the first run; needs a package registry once)
> **Runtime:** ~20s after the first install

## What it shows

A shim, `connector/main.py`, runs one Singer tap of a [Meltano](https://meltano.com) project under `rocky load --connector`, writes one stream to Parquet, and keeps the last STATE value in Rocky state.

It uses `meltano invoke`, never `meltano run`, which would keep state in Meltano's backend where Rocky's commit fence cannot see it. A crash before the state commit replays the batch; it never skips one.

## What this shim does and does not do

| Fact | What it means for you |
|---|---|
| One stream per connector. | `options.stream` names it. The tap has `orders` and `customers`; only `orders` is selected. A name the tap lacks fails the run and lists the streams. |
| The tool's state lives in Rocky state. | It is the last Singer STATE `value`; a `bookmarks` map is cut down to the loaded stream. |
| The tap supports bookmarks, so the state round trip is real. | `orders` is incremental on `id`; load 2 passes the bookmark with `--state` and gets only new rows. |
| The tap is a POC fixture. | `tap-smoke-test` is not on PyPI (Meltano installs it from GitHub), so the POC ships `tap-orders`, a small Singer SDK tap over local JSONL files. It stands in for your tap; it is not part of Rocky. |
| Delivery is at least once. | A crash between the promote and the state commit loads a batch twice. Deduplicate downstream on a key. |
| No catalog beyond the tap's own. | The shim runs `--discover` and edits only the selection. |
| Only `RECORD` and `STATE` messages are used. | `SCHEMA` is ignored (DuckDB infers types). `BATCH` fails the run. A tap that emits no `STATE` keeps the old state. |
| Secrets stay out of the project. | Meltano writes the resolved tap config to its system directory, so the shim points `MELTANO_SYS_DIR_ROOT` at a private per-run temp dir (mode 0700) and removes it. It always passes `--state` (`{}` first), so a stale Meltano state file is never read. |
| Egress: `[]`. | Meltano tracking is off (`MELTANO_DISABLE_TRACKING=true`) and the POC tap reads local files. List a real tap's hosts in the manifest. |

## Options

Set in `[connector.orders_tap.options]` in `rocky.toml`.

| Option | Meaning |
|---|---|
| `project_dir` | Required. Meltano project directory, relative to `connector/main.py`. |
| `tap` | Required. Plugin name in `meltano.yml`. |
| `stream` | Required. The one stream to load. |
| `meltano_bin` | Optional. Relative to `connector/main.py`. Default `../.venv/bin/meltano`. |

## Credentials in a real deployment

Meltano reads a plugin setting from `<PLUGIN>_<SETTING>`, e.g. `TAP_ORDERS_API_TOKEN`. Declare each name under `secrets` in `rocky.toml`; Rocky hands exactly those to the shim, which leaves them in the environment of `meltano invoke`. Never put a credential under `options` or in `meltano.yml`: Rocky writes `options.json` to disk in clear.

## Run

Needs `rocky`, `python3` 3.8+, the `duckdb` CLI, and `uv` or `python3 -m venv` with Python 3.10+ to install Meltano.

```bash
./run.sh
```

When Meltano or the tap cannot be installed, `run.sh` runs `rocky validate` and exits 0 with a SKIP message. `REQUIRE_MELTANO=1` makes that a failure.

## Expected output

First a contract-gated load fails and the state does not move. Then three loads of 4, 3 and 0 rows leave 7 rows in `poc.main.orders`; the saved bookmark goes from `id=4` to `id=7`. A missing stream and a stream-name mismatch fail the run.

```text
POC complete: 3 loads (4, 3, 0 rows), 7 rows total, Singer bookmark carried in Rocky state.
```

## Related

- [Child connectors guide](../../../../../docs/src/content/docs/guides/child-connectors.md)
- [`08-postgres-child-connector`](../08-postgres-child-connector/): the reference connector this shim copies
