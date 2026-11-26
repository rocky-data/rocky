---
title: Run the Container Image
description: "Run Rocky from the container image that ships with every engine release: docker run, the volume, the token, the health probe, the drain, upgrades, and a Compose example."
sidebar:
  order: 11
---

Every engine release publishes one container image, `ghcr.io/rocky-data/rocky`. It holds the release's own `rocky` binary and nothing else: the release workflow copies the Linux binaries out of the release archives, and nothing is compiled in the image build. `docker run ... --version` prints the version you pulled.

## What the image is

| Property | Value |
|---|---|
| Name | `ghcr.io/rocky-data/rocky` |
| Tags | `<version>` (for example `1.74.0`), `<major>.<minor>` (for example `1.74`), `latest` |
| Platforms | `linux/amd64`, `linux/arm64` |
| Contents | `/usr/local/bin/rocky` |
| Base | `gcr.io/distroless/cc-debian12:nonroot`, pinned by digest. No shell, no package manager. |
| User | `65532` (`nonroot`) |
| Working directory | `/data`, the one path the server writes to |
| Port | `8080` |
| Default command | `serve --host 0.0.0.0` |
| Stop signal | `SIGTERM`, then the server drains |
| Size | about 40 MB |

A pre-release (such as `1.74.0-rc.1`) gets only its own tag and never moves `latest` or `<major>.<minor>`. A tag is never rebuilt in place: a base-image bump ships as a new patch release. Pin the `<version>` tag to reproduce a run, or the digest (`ghcr.io/rocky-data/rocky@sha256:...`, printed by `docker buildx imagetools inspect`) for an exact pull.

## Run the server

From a project directory, this publishes the port on loopback, mounts the project as `/data`, sets a token and turns the browser UI on.

```bash
docker run --rm \
  -p 127.0.0.1:8080:8080 \
  -v "$PWD:/data" \
  -e ROCKY_SERVE_TOKEN="$(openssl rand -hex 32)" \
  -e ROCKY_SERVE_TOKEN_SCOPE=read-only \
  ghcr.io/rocky-data/rocky:latest \
  serve --host 0.0.0.0 --ui
```

Open the address the server prints (`Rocky UI: http://localhost:8080/login?t=...`). The server checks the token, sets a session cookie and redirects to `/ui/`. A proxy may log the `/login` query: configure it not to. See [sign-in](/guides/browser-ui/#sign-in-and-the-session-cookie).

```
 browser ── http://localhost:8080/ui/ ──► host :8080 ──► container :8080 (rocky serve)
                                                              │
                                                              ▼
                                                        /data (your mount)
                                                        ├── rocky.toml
                                                        └── models/
                                                            ├── *.sql, *.toml, *.rocky
                                                            └── .rocky-state.redb  (the state store)
```

Three rules:

- **Keep `--host 0.0.0.0`** when you pass your own `serve` arguments. Rocky binds loopback by default, which a published port cannot reach. The default command sets it.
- **A token is required.** A non-loopback bind needs `ROCKY_SERVE_TOKEN` or `--token`, so the default command fails fast without one. `--ui` also needs `--token-scope read-only` (or `ROCKY_SERVE_TOKEN_SCOPE=read-only`), so a leaked browser token cannot reach a mutating route.
- **Publish on loopback** (`-p 127.0.0.1:8080:8080`) unless you mean to serve the network. To reach the UI by another name, add `--allowed-host <name>`: the server answers `421` to any other `Host`, except on the health route.

The server reads `rocky.toml` from `/data` and models from `/data/models`. `--config`, `--models` and `--state-path` resolve relative to `/data`.

## What to mount

Mount the project as `/data`, the only path the server writes to. The state store lives under `models/` (`models/.rocky-state.redb`, or `models/.rocky-state/<key>.redb` with `[state] namespacing`), so one mount carries the project and its state.

On Linux a bind mount keeps the host's owner and the server runs as uid `65532`. Give it a writable directory:

```bash
# Either run as your own user ...
docker run --user "$(id -u):$(id -g)" -v "$PWD:/data" ...

# ... or let uid 65532 own the state store's directory.
sudo chown -R 65532:65532 models
```

Docker Desktop on macOS and Windows maps ownership for you, and a named volume (`-v rocky-data:/data`) is created owned by `65532`.

## Run a pipeline once

The image is the CLI, so any command works. The state store is written under `/data/models`, so the next run reuses the watermarks:

```bash
docker run --rm -v "$PWD:/data" ghcr.io/rocky-data/rocky:latest run --output json
```

## Probe the health route

`GET /api/v1/health` answers `200` without a token. Probe it from outside the container:

```bash
curl -fsS http://127.0.0.1:8080/api/v1/health
```

The image has no `HEALTHCHECK`, and you cannot add one (nor a Compose `healthcheck`): a health check runs inside the container, which has no shell or HTTP client. A Kubernetes `httpGet` probe runs from the kubelet, so it works. The health route answers any `Host`, so `--ui` needs no `--allowed-host` for a probe.

## Stop and drain

`docker stop` sends `SIGTERM`. The server stops accepting connections, drains in-flight requests and, with `--scheduler`, a running scheduled child. Docker waits 10 seconds, then sends `SIGKILL`, which a scheduled run can outlast. Set the grace on the container:

```bash
docker run --stop-timeout 125 ...
```

`--drain-timeout-seconds` (default 60) caps the wait for the child. A child still running then gets its own `SIGTERM` and a **further fixed 60 seconds** before `SIGKILL`.

```
  drain the child   --drain-timeout-seconds, default 60
  then SIGTERM it   a further 60s, fixed, not configurable
  worst case        120s with the defaults
```

**Set the grace above `--drain-timeout-seconds` + 60** (in Kubernetes, `terminationGracePeriodSeconds`). A shorter grace kills the child mid-write and records its occurrence as a failure.

## Run the scheduler

`serve --scheduler` runs every pipeline's `[schedule]` in-process, like `rocky tick` on a cron. Two rules:

- **One instance per project directory.** Two `--scheduler` containers on one mount both run what is due. Run one, and replace it in place (Kubernetes: `replicas: 1`, `strategy: Recreate`).
- **`--ui` with `--scheduler` needs `ROCKY_WEBHOOK_SECRET`**, so the scheduler's webhook route cannot be reached with the read-only browser token.

## Verify what you pull

Each image carries a software bill of materials (SBOM) and a provenance attestation, attached by BuildKit:

```bash
docker buildx imagetools inspect ghcr.io/rocky-data/rocky:<version>
docker buildx imagetools inspect ghcr.io/rocky-data/rocky:<version> --format '{{ json .SBOM }}'
docker buildx imagetools inspect ghcr.io/rocky-data/rocky:<version> --format '{{ json .Provenance }}'
```

The first command prints the index digest and one digest per platform. The image is not signed yet: pin the digest you inspected and compare `docker run --rm ghcr.io/rocky-data/rocky@sha256:... --version` with the release.

## Upgrade and roll back

Pull the next version tag and restart on the same mount. On first read the engine migrates the state store forward if the state schema changed (the [changelog](https://github.com/rocky-data/rocky/blob/main/engine/CHANGELOG.md) names each change). Stop the old container before you start the new one: the [deployment contract](/advanced/deployment-contract/) says what overlapping processes do.

Rolling back is running the previous tag. If the upgrade crossed a state-schema change, the older engine finds a store written by a newer one. `rocky serve` and every inspection command refuse it. `rocky run` follows `[state] on_schema_mismatch`: unset on the default `local` backend it refuses too, and `recreate` moves the newer file to `<file>.v<N>.bak` (refusing if that exists), starts fresh and does one full-refresh run. To roll back with history, restore a copy taken before the upgrade: copy `models/.rocky-state.redb` (or `models/.rocky-state/`) before every upgrade.

## A Compose example

[`deploy/rocky/`](https://github.com/rocky-data/rocky/tree/main/deploy/rocky) holds a `compose.yaml` that runs the image with the UI and the scheduler, an `.env.example` for the two secrets and a README with the three start steps. It encodes this page's rules: pinned tag, loopback port, volume, drain grace, one replica. It is community-supported: an example, not a supported deployment.

## Build the image yourself

`engine/Dockerfile` expects a build context with one binary per architecture, `<context>/amd64/rocky` and `<context>/arm64/rocky`. To build from a release archive:

```bash
mkdir -p ctx/arm64
gh release download engine-v<version> --repo rocky-data/rocky \
  --pattern rocky-aarch64-unknown-linux-gnu.tar.gz --dir ctx
tar xzf ctx/rocky-aarch64-unknown-linux-gnu.tar.gz -C ctx/arm64
docker buildx build --platform linux/arm64 -f engine/Dockerfile -t rocky:local --load ctx
docker run --rm rocky:local --version
```

Use `amd64` and `rocky-x86_64-unknown-linux-gnu.tar.gz` on an x86_64 machine.

## Related pages

- [Running Rocky Without an Orchestrator](/guides/running-without-an-orchestrator/): drive `rocky run` from a timer.
- [Development commands](/reference/commands/development/): every `rocky serve` flag.
- [Embedding](/guides/embedding/): the HTTP API the UI is built on.
