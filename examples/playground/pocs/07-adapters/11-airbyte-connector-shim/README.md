# 11-airbyte-connector-shim — Airbyte source image as a Rocky child connector

> **Credentials:** none. `run.sh` needs a Docker daemon and one pull of `airbyte/source-faker:6.2.21`. `run-cdk.sh` needs no Docker: it needs `airbyte-cdk` from PyPI, on Python 3.11 to 3.14
> **Runtime:** ~30s with the image present; `run-cdk.sh` ~15s with `airbyte-cdk` installed

## What it shows

A shim, `connector/main.py`, runs one Airbyte source under `rocky load --connector`: it builds a configured catalog for one stream, runs `read`, converts records to Parquet, and keeps the last Airbyte STATE in Rocky state. Rocky's commit fence governs it, so a crash before the state commit replays the batch and never skips one.

**Licensing:** the Airbyte protocol is MIT, but each source image has its own license, often Elastic License 2.0 (ELv2), which is not open source. `airbyte/source-faker`, used here, is ELv2. Check an image's `metadata.yaml` before use. The shim's code is Apache-2.0.

## What this shim does and does not do

| Fact | What it means for you |
|---|---|
| One stream per connector. | `options.stream` names it (faker has `users`, `products`, `purchases`; the POC uses `users`). A name the source lacks fails the run and lists the streams. |
| Sync mode: incremental when the stream supports it, else full refresh. | `sync_mode = "full_refresh"` forces a full read. Full refresh with an append target loads duplicates on every run. |
| The last STATE message is the Rocky state. | `STREAM`, `GLOBAL` and `LEGACY` state are handled and written back to `--state` as the source expects. The POC source emits `STREAM`; `run.sh` does not exercise `GLOBAL` or `LEGACY`. |
| A TRACE message of type ERROR, or a non-zero exit, fails the run. | The state does not move. The run is replayed. |
| Delivery is at least once. | A crash between the promote and the state commit loads a batch twice. Deduplicate downstream on a key. |
| No catalog. | Rocky does not list sources. You name the image. |
| `source-faker` uses its state as a done marker. | With `always_updated = false`, load 2 reads 0 rows, so it does not show new rows arriving (a source with a real cursor would). A control connector with no saved state loads all 20 rows. |
| The shim never pulls an image. | `run.sh` pulls it once; the shim runs `docker run --pull never`. It refuses an unpinned image: pin a tag or digest. |
| Egress: `[]`. | The shim calls no host and faker runs with `--network none`. List a real source's hosts in the manifest. |
| The docker CLI uses the default socket. | Rocky clears the environment. For another daemon, add `DOCKER_HOST` to `secrets`. |

## Options

Set in `[connector.faker_users.options]` in `rocky.toml`.

| Option | Meaning |
|---|---|
| `image` | Image mode: Docker image with a pinned tag or digest. Set exactly one of `image` and `manifest`. |
| `manifest` | Manifest mode: declarative source manifest (YAML), relative to `connector/main.py`. |
| `manifest_sha256` | Manifest mode, required: SHA-256 of the manifest file. |
| `cdk_bin` | Manifest mode, required: `source-declarative-manifest` of a pinned `airbyte-cdk` install, relative to `connector/main.py`. |
| `stream` | Required: the one stream to load. |
| `config` | The source's connection config, as its spec defines it. No secrets. |
| `config_env` | Config key → environment variable name. The name must be in `secrets`. |
| `sync_mode` | `auto` (default) or `full_refresh`. |
| `docker_network` | Image mode: passed to `docker run --network`; `none` is offline. |

## No Docker: a declarative manifest through airbyte-cdk

Most Airbyte sources are declarative YAML manifests. Set `manifest` instead of `image` and the shim runs the same `discover` and `read` through `airbyte-cdk`'s (MIT) `source-declarative-manifest`, with the same stream, checks and state handling.

```toml
[connector.my_source.options]
manifest        = "../manifests/source-example/manifest.yaml"
manifest_sha256 = "<shasum -a 256 of that file>"
cdk_bin         = "../cdk/.venv/bin/source-declarative-manifest"
stream          = "items"
```

| Fact | What it means for you |
|---|---|
| The shim never downloads a manifest. | Fetch it, read it, pin its SHA-256. Another hash fails the run before `airbyte-cdk` starts. Review each new version and update the hash. |
| The hash is required because the manifest decides where the data goes. | The source sends its config, `config_env` secrets included, to the hosts the manifest names. A changed manifest could send a token elsewhere. |
| No Airbyte manifest ships here. | Most are ELv2; check the source's `metadata.yaml`. The POC uses only its own `cdk/orders-manifest.yaml` (Apache-2.0). |
| No custom code runs. | The shim never passes `--components-path`, and `airbyte-cdk` 7.48.0 or newer refuses any custom class (`class_name`) in a manifest it did not ship; the shim refuses older versions. It also refuses to run when `AIRBYTE_ENABLE_UNSAFE_CODE` or any `PYTHON*` variable is set, or when `/airbyte/integration_code/source_declarative_manifest/manifest.yaml` exists (`airbyte-cdk` would run that file instead, with custom code trusted). A manifest needing custom components must use its image. |
| The row at the cursor can load again on every load. | `airbyte-cdk` starts the next read at the saved cursor, not after it. With an inclusive API lower bound, as in the POC stub, that row is a duplicate (the target only appends). Deduplicate downstream on a key. |
| A lookback window loads its rows again. | E.g. `lookback_window: P31D` rereads the last 31 days on every load, and every such row lands again as a duplicate. |
| `airbyte-cdk` needs Python 3.11 to 3.14. | Install it pinned in its own venv; the shim runs on Python 3.8+. `cdk/requirements.lock` hash-pins the tree for Python 3.12 on Linux x86_64 only; elsewhere use `cdk/requirements.txt`. |
| A rotated OAuth refresh token is lost. | Some sources update their config with a new token each run. The shim does not save it, in either mode, so single-use refresh tokens fail on the next run. |
| `spec` is skipped. | In this mode `spec` returns a generic spec, not the source's. |
| Egress: `connector.toml` says `[]`. | The manifest's hosts are the egress; list them in your connector's manifest. The POC calls only its local stub, `127.0.0.1`. |

```bash
./run-cdk.sh
```

It installs `airbyte-cdk` into `cdk/.venv` if it can, starts the stub API (`cdk/stub.py`), and loads three times (4, 4 and 1 rows); the stub log shows load 2 asked only from the cursor on. It then checks four failures: wrong hash, custom component, `AIRBYTE_ENABLE_UNSAFE_CODE`, a `PYTHON*` variable. With no `airbyte-cdk` and no way to install it, it runs `rocky validate` and exits 0 with a SKIP message; `REQUIRE_AIRBYTE_CDK=1` makes that a failure. Weekly CI runs it that way; PR CI does not, because it needs PyPI.

## Credentials in a real deployment

Declare the variable under `secrets` in `rocky.toml`, then map it into the source config:

```toml
secrets = ["SOURCE_API_TOKEN"]

[connector.my_source.options.config_env]
api_token = "SOURCE_API_TOKEN"
```

The config goes to a `0600` file in a `0700` directory, deleted when the run ends and mounted read-only into the container, which runs as the same user. Never put a credential under `options.config`: Rocky writes `options.json` to disk in clear.

## Run

Needs `rocky`, `python3` 3.8+, the `duckdb` CLI and Docker.

```bash
./run.sh
```

When Docker is absent, the daemon is down, or the image cannot be pulled, `run.sh` runs `rocky validate` and exits 0 with a SKIP message. `REQUIRE_DOCKER=1` makes that a failure.

## Expected output

First a contract-gated load fails and the state does not move. Then `faker_users` loads 20, 0 and 0 rows, and the control `faker_users_fresh` loads 20. The saved state is `STREAM` with `loop_offset` 20. A missing stream and an error TRACE message fail the run.

```text
POC complete: 3 loads (20, 0, 0 rows), Airbyte STREAM state carried in Rocky state.
```

## Related

- [Child connectors guide](../../../../../docs/src/content/docs/guides/child-connectors.md)
- [`08-postgres-child-connector`](../08-postgres-child-connector/): the reference connector this shim copies
