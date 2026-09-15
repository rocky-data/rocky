# Rocky observability quickstart

A single-node Grafana + Alloy + Tempo + Loki + Prometheus stack that receives
Rocky's OpenTelemetry and shows your runs: traces, run metrics, and logs.
Logs are not linked to traces: Rocky's JSON log lines carry no trace ID
(see [Log collection](#log-collection)).

Rocky exports OpenTelemetry when you set `OTEL_EXPORTER_OTLP_ENDPOINT`. The
exporter is a default Cargo feature (`otel`), so release binaries and a plain
`cargo build` include it.
It emits distributed traces (a span tree per run), run metrics (tables
processed/failed, error rate, table and query duration histograms, retries),
and structured logs on stderr. This stack is somewhere to send all of
that so you can look at it.

> This is a **dev / quickstart** stack, not a hardened production deployment.
> Storage is local and ephemeral, there is no authentication in front of any
> component, retention is short, and every service runs single-node with no
> replication. Use it to try Rocky's telemetry on your laptop or in CI, then
> point Rocky at your real collector for anything that matters.

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
- The DuckDB CLI (`duckdb`) — step 2 seeds a local DuckDB file with it.
- A `rocky` binary with the OpenTelemetry exporter. Release binaries have it,
  because `otel` is a default feature. Only a build with
  `--no-default-features` leaves it out.

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

**2. Run a Rocky pipeline with OTel pointed at the stack.** This seeds and
runs the replication playground POC, which produces a trace, run metrics, and
the table-duration histogram. `2>` also writes Rocky's stderr logs into
`./logs` so the log pipeline picks them up.

The log format follows the output format. At a terminal, Rocky prints tables
and writes colored text logs. `--output json` makes the logs JSON lines, which
is the shape Alloy parses.

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

Grafana is provisioned with anonymous admin access (no login). Open the
**Rocky Runs** dashboard in the **Rocky** folder. You'll see the run's metrics
and, in the trace panel at the bottom, the run's trace; click it to open the
full span tree. The link from a span to its logs in Loki has nothing to match
on, because the log lines carry no trace ID.

Any `rocky run` (or `plan`, `apply`, …) with those two env vars set will land
here — the replication POC is just a convenient one that exercises every
panel. A DuckDB run leaves the "Query duration" panel empty because SQL
statement timing is recorded by the warehouse adapter (Databricks today), not
by the local DuckDB path.

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

`down -v` removes the containers **and** the named volumes, so nothing is left
behind. Runtime log files you tee'd into `./logs/` are gitignored; delete them
by hand if you want a truly clean tree.

## Log collection

Rocky writes structured logs to stderr. With JSON output (`--output json`, or
any run whose stdout is not a terminal) each log is one JSON object per line.
At a terminal the logs are colored text. The Alloy config ships two log paths:

- **OTLP logs** — wired to Loki's native OTLP endpoint for any source that
  exports OTLP logs. Rocky does not export OTLP logs today (it uses stderr
  JSON), so this route is there for completeness and forward-compatibility.
- **File-tail (the one Rocky uses today)** — redirect Rocky's stderr to a file
  under `./logs/` (as the quickstart does) and Alloy tails `./logs/*.jsonl`,
  parses each line, promotes `level` to a label, and reads `trace_id` /
  `span_id` into structured metadata. Rocky's JSON log lines do not contain
  those fields, even with OpenTelemetry on. A line carries span names and
  span fields, not the OpenTelemetry trace ID. So Grafana cannot jump from a span in Tempo to its log lines in Loki.

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

The counters are exported as **last-value gauges** — each run pushes the totals
for that run, so the series is a per-run snapshot over time rather than a
monotonic counter. Read "runs over time" panels accordingly; do not wrap these
in `increase()` or `rate()`, which assume a cumulative counter.

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
