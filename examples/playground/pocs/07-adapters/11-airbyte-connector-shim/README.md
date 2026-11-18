# 11-airbyte-connector-shim — Airbyte source image as a Rocky child connector

> **Category:** 07-adapters
> **Credentials:** none. `run.sh` needs a Docker daemon and one pull of `airbyte/source-faker:6.2.21`. `run-cdk.sh` needs no Docker: it needs `airbyte-cdk` from PyPI, on Python 3.11 to 3.14
> **Runtime:** ~30s with the image present; `run-cdk.sh` ~15s with `airbyte-cdk` installed
> **Rocky features:** `rocky load --connector`, the child connector protocol, connector state, `[connector.<name>.options]`

## Feature

A thin shim, `connector/main.py`, that runs one Airbyte source image under `rocky load --connector`. It builds a configured catalog for one stream, runs `read`, converts the records to Parquet, and keeps the last Airbyte STATE message in Rocky state.

## Why it's distinctive

Rocky ships no extractors and no catalog. The image is Airbyte's. The shim only adapts the Airbyte protocol to the connector protocol.

The Airbyte protocol is MIT. Each source image has its own license, often Elastic License 2.0 (ELv2), which is not open source. `airbyte/source-faker`, used here, is ELv2. Check an image's `metadata.yaml` before you use it. The shim's own code is Apache-2.0.

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
| `image` | Image mode. The Docker image, with a pinned tag or digest. Set exactly one of `image` and `manifest`. |
| `manifest` | Manifest mode. A declarative source manifest (YAML), relative to `connector/main.py`. See [No Docker](#no-docker-a-declarative-manifest-through-airbyte-cdk). |
| `manifest_sha256` | Manifest mode, required. The SHA-256 of the manifest file. |
| `cdk_bin` | Manifest mode, required. The `source-declarative-manifest` program of a pinned `airbyte-cdk` install, relative to `connector/main.py`. |
| `stream` | Required. The one stream to load. |
| `config` | Table: the source's connection config, as its spec defines it. No secrets. |
| `config_env` | Table: config key → name of an environment variable. The name must be in `secrets`. |
| `sync_mode` | `auto` (default) or `full_refresh`. |
| `docker_network` | Image mode. Passed to `docker run --network`. `none` makes the container offline. |

## No Docker: a declarative manifest through airbyte-cdk

Most Airbyte sources are declarative: a YAML manifest, not code. Airbyte's `airbyte-cdk` (MIT, on PyPI) runs a manifest with its `source-declarative-manifest` program, with no Docker. Set `manifest` instead of `image`, and the shim runs the same `discover` and `read` through it. Everything else stays the same: one stream, the configured catalog, the TRACE and lost-batch checks, and the state in Rocky state.

```toml
[connector.my_source.options]
manifest        = "../manifests/source-example/manifest.yaml"
manifest_sha256 = "<shasum -a 256 of that file>"
cdk_bin         = "../cdk/.venv/bin/source-declarative-manifest"
stream          = "items"
```

| Fact | What it means for you |
|---|---|
| The shim never downloads a manifest. | Fetch the manifest yourself, read it, and pin its SHA-256 in `manifest_sha256`. A file with another hash fails the run before `airbyte-cdk` starts. To take a new version, review it and update the hash. |
| The hash is required because the manifest decides where the data goes. | The manifest names the hosts the source calls, and the source sends its config there, secrets from `config_env` included. A changed manifest could send a token to another host. |
| No Airbyte manifest ships here. | Most Airbyte source manifests are Elastic License 2.0 (ELv2), which is not open source. Check the source's `metadata.yaml` before you use it. This POC uses only its own manifest, `cdk/orders-manifest.yaml` (Apache-2.0). |
| No custom code runs. | The shim never passes `--components-path`, and `airbyte-cdk` 7.48.0 or newer refuses any custom class (`class_name`) in a manifest it did not ship. The shim refuses an older `airbyte-cdk`. It also refuses to run when `AIRBYTE_ENABLE_UNSAFE_CODE` or any `PYTHON*` variable is set, or when `/airbyte/integration_code/source_declarative_manifest/manifest.yaml` exists (`airbyte-cdk` would run that file instead, with custom code trusted). A manifest that needs custom components does not work in this mode: use its image. |
| The row at the cursor can load again on every load. | `airbyte-cdk` starts the next read at the saved cursor, not after it. When the API's lower bound is inclusive, as in this POC's stub, the row at the cursor comes back. The target only appends, so that row is a duplicate. Deduplicate downstream on a key. |
| A lookback window loads its rows again. | Some manifests set `lookback_window`: each read starts that far before the cursor. For example, a manifest with `lookback_window: P31D` reads the last 31 days on every load. Every row in that window lands again as a duplicate. |
| `airbyte-cdk` needs Python 3.11 to 3.14. | Install it in its own venv, pinned. The shim itself still runs on Python 3.8 or newer. `cdk/requirements.lock` pins the whole dependency tree by hash, for Python 3.12 on Linux x86_64 only; elsewhere, install from `cdk/requirements.txt`. |
| A rotated OAuth refresh token is lost. | Some sources get a new refresh token on each run and update their config. The shim does not save it, in either mode. A source with single-use refresh tokens fails on the next run. |
| `spec` is skipped. | In this mode `spec` returns a generic spec, not the source's. |
| Egress: `connector.toml` says `[]`. | The manifest's hosts are the egress. List them in your own manifest for the connector. This POC calls only its local stub, `127.0.0.1`. |

Run it:

```bash
./run-cdk.sh
```

`run-cdk.sh` installs `airbyte-cdk` into `cdk/.venv` when it can, starts the stub API in `cdk/stub.py`, and loads three times (4, 4 and 1 rows). The stub's log shows that load 2 asked only for orders from the cursor on. Then it checks four failures: a wrong hash, a manifest with a custom component, `AIRBYTE_ENABLE_UNSAFE_CODE`, and a `PYTHON*` variable. With no `airbyte-cdk` and no way to install it, the script runs `rocky validate` and exits 0 with a SKIP message. Set `REQUIRE_AIRBYTE_CDK=1` to make that a failure. The weekly CI runs it with `REQUIRE_AIRBYTE_CDK=1`. PR CI does not run it, because it needs `airbyte-cdk` from PyPI.

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
├── rocky-cdk.toml         the no-Docker connectors: orders_cdk and three failing ones
├── run-cdk.sh             validate → airbyte-cdk → stub API → 3 loads → table check → failure checks
├── cdk/
│   ├── orders-manifest.yaml         this POC's own declarative manifest (Apache-2.0)
│   ├── probe-custom-component.yaml  the same, with a custom component: must fail
│   ├── stub.py                      the local orders API
│   ├── requirements.txt             airbyte-cdk==7.48.0
│   └── requirements.lock            the whole tree, pinned by hash (Linux, Python 3.12)
└── connector/
    ├── connector.toml     manifest: command = ["./main.py"], egress_hosts = []
    └── main.py            the shim (Python 3.8+, standard library only; needs the duckdb CLI, and docker or airbyte-cdk)
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
- [`10-dlt-connector-shim`](../10-dlt-connector-shim/) and [`12-meltano-connector-shim`](../12-meltano-connector-shim/)
