# 10-airbyte-connector-shim — Airbyte source image as a Rocky child connector

> **Category:** 07-adapters
> **Credentials:** none (needs a Docker daemon and one pull of `airbyte/source-faker:6.2.21`)
> **Runtime:** ~30s with the image present
> **Rocky features:** `rocky load --connector`, the child connector protocol, connector state, `[connector.<name>.options]`

## Feature

A thin shim, `connector/main.py`, that runs one open-source Airbyte source image under `rocky load --connector`. It builds a configured catalog for one stream, runs `read`, converts the records to Parquet, and keeps the last Airbyte STATE message in Rocky state.

## Why it's distinctive

Rocky ships no extractors and no catalog. The image is Airbyte's. The shim only adapts the Airbyte protocol to the connector protocol.

The Airbyte state lives in Rocky state, not in an Airbyte database. Rocky's commit fence governs it. A crash before the state commit replays the batch. It never skips one.

```
  rocky load ──▶ main.py ──▶ docker run <image> spec / discover / read
      ▲  state ───▶ --state file            │ JSONL on stdout
      │                                      ▼
      └── check file, append, save state ◀── RECORD → Parquet, last STATE → state
```

## What this shim does and does not do

| Fact | What it means for you |
|---|---|
| One stream per connector. | `options.stream` names one stream. The faker source has `users`, `products` and `purchases`. The shim puts only `users` in the catalog. A name the source lacks fails the run and lists the streams. |
| Sync mode: incremental when the stream supports it, else full refresh. | `users` supports incremental, so the shim uses `incremental` + `append`. `sync_mode = "full_refresh"` forces a full read. Full refresh with an append target loads duplicates on every run. |
| The last STATE message is the Rocky state. | The shim handles `STREAM`, `GLOBAL` and `LEGACY` state. It writes the state back to `--state` in the shape the source expects. The POC source emits `STREAM` state. The `GLOBAL` and `LEGACY` paths are not exercised by `run.sh`. |
| A TRACE message of type ERROR, or a non-zero exit, fails the run. | The state does not move. The run is replayed. |
| Delivery is at least once. | A crash between the promote and the state commit loads a batch twice. Deduplicate downstream on a key. |
| No catalog. | Rocky does not list sources. You name the image. |
| `source-faker` uses its state as a done marker. | With `always_updated = false`, the state makes faker emit nothing. So load 2 reads 0 rows. It does not show new rows arriving. A source with a real cursor would. The POC proves the round trip with a control: a connector with no saved state loads all 20 rows. |
| The shim never pulls an image. | `run.sh` pulls it once. The shim runs `docker run --pull never`. Pin a tag or a digest. The shim refuses an unpinned image. |
| Egress: `[]`. | The shim calls no host. The faker container runs with `--network none`. A real source calls its own hosts: list them in the manifest. |
| The docker CLI uses the default socket. | Rocky clears the environment. To reach another daemon, add `DOCKER_HOST` to `secrets`. |

## Options

Set them in `[connector.faker_users.options]` in `rocky.toml`.

| Option | Meaning |
|---|---|
| `image` | Required. The Docker image, with a pinned tag or digest. |
| `stream` | Required. The one stream to load. |
| `config` | Table: the source's connection config, as its spec defines it. No secrets. |
| `config_env` | Table: config key → name of an environment variable. The name must be in `secrets`. |
| `sync_mode` | `auto` (default) or `full_refresh`. |
| `docker_network` | Passed to `docker run --network`. `none` makes the container offline. |

## Credentials in a real deployment

Declare the variable under `secrets` in `rocky.toml`, then map it into the source config:

```toml
secrets = ["SOURCE_API_TOKEN"]

[connector.my_source.options.config_env]
api_token = "SOURCE_API_TOKEN"
```

The shim writes the config to a `0600` file in a `0700` directory and deletes it when the run ends. It mounts the directory read-only into the container and runs the container as the same user. Never put a credential under `options.config`: Rocky writes `options.json` to disk in clear.

## Layout

```
.
├── README.md
├── rocky.toml             DuckDB target; faker_users, a control, and two failing connectors
├── run.sh                 validate → pull image → 3 loads + control → state check → failure checks
└── connector/
    ├── connector.toml     manifest: command = ["./main.py"], egress_hosts = []
    └── main.py            the shim (Python 3.8+, standard library only; needs the docker and duckdb CLIs)
```

## Run

```bash
./run.sh
```

When Docker is absent, the daemon is down, or the image cannot be pulled, `run.sh` runs `rocky validate` and exits 0 with a SKIP message. Set `REQUIRE_DOCKER=1` to make that a failure.

## Expected output

```text
=== load 1: rocky load --connector faker_users (expect 20 rows) ===
    status=loaded total_rows=20 commit_seq 0 -> 1
=== load 2: rocky load --connector faker_users (expect 0 rows) ===
    status=loaded total_rows=0 commit_seq 1 -> 2
=== load 3: rocky load --connector faker_users (expect 0 rows) ===
    status=loaded total_rows=0 commit_seq 2 -> 3
=== load 1: rocky load --connector faker_users_fresh (expect 20 rows) ===
    status=loaded total_rows=20 commit_seq 0 -> 1
=== the Airbyte state lives in Rocky state ===
    load 1: state_type=STREAM stream=users loop_offset=20
    load 3: state_type=STREAM stream=users loop_offset=20
=== a stream the source does not have fails clearly ===
=== a source that reports an error TRACE message fails the run ===

POC complete: 3 loads (20, 0, 0 rows), Airbyte STREAM state carried in Rocky state.
```

## Related

- [Child connectors guide](../../../../../docs/src/content/docs/guides/child-connectors.md): the protocol, the checks and the guarantees
- [`08-postgres-child-connector`](../08-postgres-child-connector/): the reference connector this shim copies
- [`09-dlt-connector-shim`](../09-dlt-connector-shim/) and [`11-meltano-connector-shim`](../11-meltano-connector-shim/)
