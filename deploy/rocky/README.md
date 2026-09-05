# Rocky on Docker Compose

One long-lived `rocky serve` on one machine, with the browser UI and the resident scheduler, from the image every engine release publishes.

> **Community-supported.** This is an example to start from, not a supported deployment. The rules it encodes are the engine's, and the [image guide](https://rocky-data.dev/guides/run-the-image/) explains each one. The file itself carries no compatibility promise. Local state only: the state store lives on the mounted volume, and this example does not configure remote state.

## Start it

Three steps from this directory.

1. Copy `.env.example` to `.env` and fill the two secrets:

   ```bash
   cp .env.example .env
   openssl rand -hex 32   # ROCKY_SERVE_TOKEN
   openssl rand -hex 32   # ROCKY_WEBHOOK_SECRET
   ```

2. Put a project in `./project`: a `rocky.toml` and a `models/` directory. To try the example with the sample DuckDB project instead, let the image scaffold one:

   ```bash
   docker run --rm -v "$PWD:/data" ghcr.io/rocky-data/rocky:1.74.0 playground project
   ```

3. Start it and read the address the server prints:

   ```bash
   docker compose up -d
   docker compose logs rocky | grep 'Rocky UI'
   # Rocky UI: http://localhost:8080/ui/#token=...
   ```

Open that address. The token travels in the URL fragment, which never reaches the server; the page reads it once and clears it.

## What is in it

| Line in `compose.yaml` | Why |
|---|---|
| `image: ${ROCKY_IMAGE:-ghcr.io/rocky-data/rocky:1.74.0}` | A version tag, pinned. `latest` moves on every release; a version tag never does. `ROCKY_IMAGE` in `.env` overrides it. |
| `command: serve --host 0.0.0.0 --ui --scheduler` | Inside a container the server must bind every interface to be reachable through the published port. `--ui` serves the browser UI. `--scheduler` runs every pipeline's `[schedule]` in-process. |
| `ROCKY_SERVE_TOKEN`, `ROCKY_SERVE_TOKEN_SCOPE=read-only` | A non-loopback bind needs a token, and `--ui` needs the read-only scope, so a leaked browser token cannot reach a mutating route. |
| `ROCKY_WEBHOOK_SECRET` | `--ui --scheduler` refuses to start without it. The scheduler's webhook route authenticates with this secret, not with the bearer token. |
| `ports: 127.0.0.1:8080:8080` | Loopback only. The server terminates no TLS and knows no users; put your own gateway in front before publishing wider, and add `--allowed-host <name>` to the command for any name the UI is reached by. |
| `volumes: ./project:/data` | The project and, under `models/`, the state store. One mount carries both. |
| `stop_grace_period: 90s` | `docker compose stop` sends `SIGTERM`; the server drains requests and a running scheduled child (up to `--drain-timeout-seconds`, default 60). The grace stays above that. |
| `deploy.replicas: 1` | One scheduler per project. Two instances on one volume would both run what is due. Never `--scale rocky=2`; run one service per project. |

There is no `healthcheck`. A Compose health check runs inside the container, and the image has no shell or HTTP client. Probe from the host instead:

```bash
curl -fsS http://127.0.0.1:8080/api/v1/health
```

## On Linux: make the volume writable

The server runs as uid `65532` and writes the state store under `./project/models/`. A bind mount keeps the host's owner, so on Linux do one of two things: give the directory to that user (`sudo chown -R 65532:65532 project/models`), or run the container as yourself by adding to the service:

```yaml
    user: "1000:1000"   # your uid:gid, from `id -u` and `id -g`
```

Docker Desktop on macOS and Windows maps ownership for you; nothing is needed there.

## Run a pipeline once

The image is the CLI. Any command runs against the same project:

```bash
docker compose run --rm rocky run --output json
```

The run and the server share the state store on the volume, so the server's next read shows the run.

## Logs, stop, upgrade

```bash
docker compose logs -f rocky       # JSON lines on stderr, the UI address on stdout
docker compose stop                # SIGTERM, then the drain, inside the 90 s grace
docker compose down                # also removes the container; the volume is your directory, untouched
```

To upgrade, copy `./project/models/.rocky-state.redb` somewhere safe, change the tag in `compose.yaml` (or `ROCKY_IMAGE` in `.env`), then:

```bash
docker compose pull
docker compose up -d
```

On its first read the new engine migrates the state store forward when the state schema changed; the engine changelog names every such change. Rolling back across such a change: `rocky serve` refuses a store written by a newer engine, and `rocky run` starts from a fresh local state by default (`[state] on_schema_mismatch`), so restore the copy first to keep the history.

## What it does not do

- Remote state. `[state] backend = "s3"` and the other sync backends are not configured here; the state store is the file on the volume.
- More than one replica, a rolling update, or a second machine. One scheduler per project is the rule the engine enforces with a per-volume lock, and this example keeps to it.
- TLS, users, or a public port. Those belong to the gateway you put in front.
