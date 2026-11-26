# Rocky observability quickstart

A single-node Grafana + Alloy + Tempo + Loki + Prometheus stack that receives Rocky's OpenTelemetry and shows your runs: traces, run metrics and logs. Logs are not linked to traces: Rocky's JSON log lines carry no trace ID (see [Log collection](#log-collection)).

Rocky exports OpenTelemetry when you set `OTEL_EXPORTER_OTLP_ENDPOINT`. The exporter is a default Cargo feature (`otel`), so release binaries and a plain `cargo build` include it. It emits traces (a span tree per run), run metrics (tables processed/failed, error rate, table and query duration histograms, retries) and structured logs on stderr.

> This is a **dev / quickstart** stack, not a production deployment. Storage is local and ephemeral, nothing is authenticated, retention is short, and every service is single-node. Use it to try Rocky's telemetry, then point Rocky at your real collector.

## What's in it

| Component | Role | Host port |
|---|---|---|
| Alloy | OTLP collector — the endpoint Rocky exports to | 4317 (gRPC), 4318 (HTTP), 12345 (UI) |
| Tempo | Trace store + TraceQL search | 3200 |
| Prometheus | Metric store (remote-write receiver) | 9090 |
| Loki | Log store (OTLP + a file-tail example) | 3100 |
| Grafana | Dashboards, datasources, alerts (pre-provisioned) | 3000 |

Data flow:

```
rocky --(OTLP/gRPC :4317)--> alloy --> tempo        (traces)
                                   \-> prometheus   (metrics, remote-write)
                                   \-> loki         (logs)
grafana <-- tempo / prometheus / loki               (provisioned datasources)
```

## Prerequisites

- Docker with Compose v2 (`docker compose version`).
- The DuckDB CLI (`duckdb`), which step 2 uses to seed a local file.
- A `rocky` binary with the OpenTelemetry exporter. Release binaries have it. Only a build with `--no-default-features` leaves it out.

## Quickstart (3 commands)

Run these from `deploy/observability/`.

**1. Bring the stack up.**

```bash
docker compose up -d
```

Give it a few seconds, then check everything is healthy:

```bash
docker compose ps
```

**2. Run a Rocky pipeline with OTel pointed at the stack.** This seeds and runs the replication playground POC, which produces a trace, run metrics and the table-duration histogram. `2>` writes Rocky's stderr logs into `./logs` for the log pipeline. `--output json` makes the logs JSON lines, the shape Alloy parses. At a terminal, Rocky otherwise prints tables and colored text logs.

```bash
cd ../../examples/playground/pocs/00-foundations/01-replication-basics
duckdb playground.duckdb < data/seed.sql
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4317 OTEL_SERVICE_NAME=rocky \
  rocky --output json run --filter source=orders \
  2> "$(git rev-parse --show-toplevel)/deploy/observability/logs/rocky.jsonl"
```

**3. Open Grafana and look at the run.**

```bash
open http://localhost:3000        # macOS; use xdg-open on Linux
```

Grafana is provisioned with anonymous admin access (no login). Open the **Rocky Runs** dashboard in the **Rocky** folder. It shows the run's metrics and, in the bottom trace panel, the run's trace. Click it for the full span tree. The span-to-logs link in Loki has nothing to match on, because log lines carry no trace ID.

Any `rocky run`, `plan` or `apply` with those two env vars lands here. A DuckDB run leaves the "Query duration" panel empty: the warehouse adapter (Databricks today) records statement timing, not the local DuckDB path.

### Verify from the command line (optional)

Confirm a trace landed in Tempo without opening a browser:

```bash
curl -s 'http://localhost:3200/api/search?q=%7B%20resource.service.name%20%3D%20%22rocky%22%20%7D' | jq '.traces[] | {traceID, rootServiceName, rootTraceName}'
```

Confirm metrics landed in Prometheus:

```bash
curl -s 'http://localhost:9090/api/v1/label/__name__/values' | jq -r '.data[] | select(startswith("rocky"))'
```

## Teardown

```bash
docker compose down -v
```

`down -v` removes the containers **and** the named volumes. Log files you tee'd into `./logs/` are gitignored. Delete them by hand for a clean tree.

## Log collection

Rocky writes structured logs to stderr: one JSON object per line with `--output json` (or any run whose stdout is not a terminal), colored text at a terminal. Alloy has two log paths:

- **OTLP logs** go to Loki's native OTLP endpoint. Rocky does not export OTLP logs today, so this route is for forward-compatibility.
- **File-tail** (the one Rocky uses): redirect stderr to `./logs/` as the quickstart does. Alloy tails `./logs/*.jsonl`, promotes `level` to a label, and reads `trace_id` / `span_id` into structured metadata. Rocky's JSON lines do not contain those fields at the default log level, even with OpenTelemetry on: a line has span names and span fields, not the OpenTelemetry trace ID. So Grafana cannot jump from a Tempo span to its Loki logs.

See `alloy/config.alloy` for both pipelines; the file-tail block is documented
inline.

## Metrics reference

The dashboard reads the metrics Rocky emits (names as they appear in
Prometheus after OTLP conversion):

| Prometheus series | Rocky metric | Shape |
|---|---|---|
| `rocky_tables_processed` | `rocky.tables_processed` | per-run gauge |
| `rocky_tables_failed` | `rocky.tables_failed` | per-run gauge |
| `rocky_error_rate_pct` | `rocky.error_rate_pct` | per-run gauge |
| `rocky_retries_attempted` | `rocky.retries_attempted` | per-run gauge |
| `rocky_retries_succeeded` | `rocky.retries_succeeded` | per-run gauge |
| `rocky_statements_executed` | `rocky.statements_executed` | per-run gauge |
| `rocky_table_duration_ms_bucket` / `_sum` / `_count` | `rocky.table_duration_ms` | histogram |
| `rocky_query_duration_ms_bucket` / `_sum` / `_count` | `rocky.query_duration_ms` | histogram (warehouse adapter) |

The counters are exported as **last-value gauges**: each run pushes its own totals, so a series is a per-run snapshot, not a monotonic counter. Do not wrap them in `increase()` or `rate()`.

## Configuration

Everything is provisioned from files in this directory, so edits are picked up
on the next `docker compose up`:

```
compose.yaml                          # the stack
alloy/config.alloy                    # OTLP routing + the file-tail example
tempo/tempo.yaml                      # trace store
loki/loki-config.yaml                 # log store
prometheus/prometheus.yml             # metric store
grafana/provisioning/datasources.yaml # Tempo + Loki + Prometheus (fixed UIDs)
grafana/provisioning/dashboards.yaml  # dashboard provider
grafana/dashboards/rocky-runs.json    # the Rocky Runs dashboard
grafana/dashboards/rocky-scheduler.json # the Rocky Scheduler dashboard
grafana/alerting/rocky-alerts.yaml    # provisioned alert rules
```

All images are pinned to explicit version tags in `compose.yaml`.

### Alerts

Six provisioned rules live in `grafana/alerting/rocky-alerts.yaml`, in two
groups.

The `rocky-runs` group reads run metrics:

- **Rocky run reported failed tables** — fires when any run in the last 15
  minutes reported `rocky.tables_failed > 0`.
- **Rocky error rate above 5 percent** — fires when `rocky.error_rate_pct`
  stays above 5 for 15 minutes.

The `rocky-scheduler` group reads the `rocky.scheduler.*` metrics. Only a
running `rocky serve --scheduler` (experimental) emits them. With no scheduler
running, the series are absent and the rules stay quiet.

- **Rocky scheduler is ticking but not reconciling** — ticks end in
  `config_error` or `fault` for 5 minutes.
- **Rocky scheduler cannot read its webhook spool** — ticks end in
  `spool_unreadable` for 5 minutes.
- **Rocky scheduled pipeline is failing repeatedly** — a scheduled pipeline
  failed 3 or more times in a row.
- **Rocky scheduler is falling behind** — p95 execution lag stays above 120
  seconds for 5 minutes.

The file provisions the rules only; wire a Grafana contact point to deliver
notifications.
