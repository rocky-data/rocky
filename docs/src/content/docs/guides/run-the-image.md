---
title: Run the Container Image
description: "Run Rocky from the container image that ships with every engine release: docker run, the volume, the token, the health probe, the drain, upgrades, and a minikube example."
sidebar:
  order: 5.6
---

Every engine release publishes one container image, `ghcr.io/rocky-data/rocky`. The image holds the release's own `rocky` binary and nothing else. This guide shows you how to run it with `docker run`, what to mount, how to probe it, how to stop it, and how to upgrade it. A minikube example closes the page. Kubernetes is not a supported target; the example is a starting point, not a contract.

## What the image is

The image is the release, bit for bit. The release workflow copies the Linux binaries out of the release archives into the image; nothing is compiled in the image build. The `docker run --version` output is the version you pulled.

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

A pre-release version (one with a `-`, such as `1.74.0-rc.1`) gets only its own tag. It never moves `latest` or `<major>.<minor>`. A tag is never rebuilt in place: a base-image bump ships as a new patch release.

Pin the `<version>` tag for anything that must be reproducible. Pin the digest (`ghcr.io/rocky-data/rocky@sha256:...`) when the pull must be exact; `docker buildx imagetools inspect` prints it.

## Run the server

Run the server from a project directory. The command below publishes the port on loopback only, mounts the project as `/data`, hands the server a token, and turns the browser UI on.

```bash
docker run --rm \
  -p 127.0.0.1:8080:8080 \
  -v "$PWD:/data" \
  -e ROCKY_SERVE_TOKEN="$(openssl rand -hex 32)" \
  -e ROCKY_SERVE_TOKEN_SCOPE=read-only \
  ghcr.io/rocky-data/rocky:latest \
  serve --host 0.0.0.0 --ui
```

The server prints one line with the address to open, `Rocky UI: http://localhost:8080/ui/#token=...`. Open it. The token travels in the URL fragment, which never reaches the server, and the page clears it after reading it once.

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

Three rules follow from how the image is built:

- **Keep `--host 0.0.0.0`** when you pass your own `serve` arguments. Rocky binds loopback by default, and a loopback bind inside the container is unreachable through a published port. The default command sets it for you.
- **A token is required.** A non-loopback bind needs one, so the default command fails fast without `ROCKY_SERVE_TOKEN` or `--token`. `--ui` also needs `--token-scope read-only` (or `ROCKY_SERVE_TOKEN_SCOPE=read-only`), so a leaked browser token cannot reach a mutating route.
- **Publish the port on loopback** (`-p 127.0.0.1:8080:8080`) unless you mean to serve the network. To reach the UI by another name or address, add `--allowed-host <name>`; the server answers `421` to any `Host` it was not told about.

The server reads `rocky.toml` from `/data` and the models from `/data/models`. Every path the CLI takes, `--config`, `--models`, `--state-path`, works the same way inside the container, relative to `/data`.

## What to mount

Mount the project directory as `/data`. It is the only path the server writes to: the state store lives under `models/` (`models/.rocky-state.redb`, or `models/.rocky-state/<key>.redb` with `[state] namespacing`), so one mount carries the project and its state.

On Linux, a bind mount keeps the host's owner, and the server runs as uid `65532`. Give it a directory it can write, one of two ways:

```bash
# Either run as your own user ...
docker run --user "$(id -u):$(id -g)" -v "$PWD:/data" ...

# ... or let uid 65532 own the state store's directory.
sudo chown -R 65532:65532 models
```

Docker Desktop on macOS and Windows maps ownership for you; nothing is needed there. A named volume (`-v rocky-data:/data`) is created owned by `65532` from the image, so it needs nothing either.

## Run a pipeline once

The image is the CLI, so any command runs the same way. This runs the project's pipelines once and prints the JSON result:

```bash
docker run --rm -v "$PWD:/data" ghcr.io/rocky-data/rocky:latest run --output json
```

The state store is written under `/data/models`, so the next run picks up the watermarks from the first.

## Probe the health route

`GET /api/v1/health` answers `200` without a token. Probe it from outside the container:

```bash
curl -fsS http://127.0.0.1:8080/api/v1/health
```

The image carries no `HEALTHCHECK` instruction, and you cannot add one: a Docker health check runs inside the container, and the image has no shell or HTTP client. The same limit applies to a Compose `healthcheck`. A Kubernetes `httpGet` probe comes from the kubelet, outside the container, so it works; the example below uses it.

## Stop and drain

`docker stop` sends `SIGTERM`. The server stops accepting connections, drains in-flight requests, and, with `--scheduler`, drains a running scheduled child before it exits.

Docker waits 10 seconds by default, then sends `SIGKILL`. A scheduled run can need longer. Set the grace on the container:

```bash
docker run --stop-timeout 90 ...
```

In Kubernetes the same knob is `terminationGracePeriodSeconds`. `--drain-timeout-seconds` (default 60) caps how long the server waits for the child; keep the container's grace above it.

## Run the scheduler

`serve --scheduler` runs every pipeline's `[schedule]` in-process, like `rocky tick` on a cron. Two rules:

- **One instance per project directory.** Two containers with `--scheduler` on one mount would both run what is due. Run one replica, and replace it in place rather than side by side; the example below sets `replicas: 1` and `strategy: Recreate` for that reason.
- **`--ui` with `--scheduler` needs `ROCKY_WEBHOOK_SECRET`**, so the scheduler's webhook route cannot be reached with the read-only browser token.

## Verify what you pull

Each pushed image carries a software bill of materials (SBOM) and a provenance attestation, attached by BuildKit at publish time. Read them with:

```bash
docker buildx imagetools inspect ghcr.io/rocky-data/rocky:<version>
docker buildx imagetools inspect ghcr.io/rocky-data/rocky:<version> --format '{{ json .SBOM }}'
docker buildx imagetools inspect ghcr.io/rocky-data/rocky:<version> --format '{{ json .Provenance }}'
```

The first command prints the index digest and one digest per platform. The image is not signed yet. Until it is, pin the digest you inspected and compare the binary's version against the release: `docker run --rm ghcr.io/rocky-data/rocky@sha256:... --version`.

## Upgrade and roll back

Pull the next version tag and restart the container on the same mount. On the first read, the engine migrates the state store forward when the state schema changed; the [changelog](https://github.com/rocky-data/rocky/blob/main/engine/CHANGELOG.md) names every such change. Stop the old container before you start the new one: the [deployment contract](/advanced/deployment-contract/) says what two overlapping processes and a fleet mid-upgrade do.

Rolling back is running the previous tag. When the upgrade crossed a state-schema change, the older engine finds a store written by a newer one. `rocky serve` and every inspection command refuse it. `rocky run` follows `[state] on_schema_mismatch`: the default, `recreate`, starts from a fresh local state and does one full-refresh run; `fail` refuses like the rest. To roll back with the history intact, restore the store from a copy taken before the upgrade. Copy `models/.rocky-state.redb` (or the `models/.rocky-state/` directory) before every upgrade; it is one file per store.

## A Compose example

The repository ships [`deploy/rocky/`](https://github.com/rocky-data/rocky/tree/main/deploy/rocky): one `compose.yaml` that runs the image as a long-lived process with the UI and the scheduler, an `.env.example` for the two secrets, and a README that walks the three steps to start it. It encodes the rules on this page, one line each: the pinned tag, the loopback port, the volume, the drain grace, one replica. It is community-supported: an example to start from, not a supported deployment.

## A minikube example

This is an example, not a supported deployment. It follows the rules above: one replica, replaced in place, on a persistent volume, with the health probe from the kubelet and a drain grace above the server's own.

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: rocky-serve
stringData:
  token: replace-me-with-openssl-rand-hex-32
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: rocky-data
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 1Gi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: rocky
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app: rocky
  template:
    metadata:
      labels:
        app: rocky
    spec:
      terminationGracePeriodSeconds: 90
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        fsGroup: 65532
      containers:
        - name: rocky
          image: ghcr.io/rocky-data/rocky:1.74.0
          args: ["serve", "--host", "0.0.0.0", "--ui", "--token-scope", "read-only"]
          env:
            - name: ROCKY_SERVE_TOKEN
              valueFrom:
                secretKeyRef:
                  name: rocky-serve
                  key: token
          ports:
            - containerPort: 8080
          readinessProbe:
            httpGet:
              path: /api/v1/health
              port: 8080
          livenessProbe:
            httpGet:
              path: /api/v1/health
              port: 8080
            periodSeconds: 30
          volumeMounts:
            - name: data
              mountPath: /data
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: rocky-data
```

Copy a project into the volume, then reach the UI through a port-forward, which presents the `localhost` host the server allows:

```bash
kubectl apply -f rocky.yaml
kubectl cp ./my-project "$(kubectl get pod -l app=rocky -o name | cut -d/ -f2):/data"
kubectl port-forward deploy/rocky 8080:8080
# open http://localhost:8080/ui/#token=<the secret's token>
```

`fsGroup: 65532` makes the volume writable by the server's user. The pod has one container and one replica because the scheduler, and the state store, allow one writer per project.

## Build the image yourself

The Dockerfile is `engine/Dockerfile` in the repository. It expects a build context that holds one binary per architecture, `<context>/amd64/rocky` and `<context>/arm64/rocky`, and copies the one for the platform being built. To build the image for your machine from a release archive:

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
