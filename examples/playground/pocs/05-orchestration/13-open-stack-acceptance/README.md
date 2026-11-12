# 13-open-stack-acceptance — The open-stack acceptance journey

> **Category:** 05-orchestration
> **Credentials:** none (Docker daemon required; the POC brings its own Postgres)
> **Runtime:** about 4 minutes, most of it one wait for a deadline to pass (`rocky validate` only, in under 1 s, when Docker is absent)
> **Rocky features:** `rocky load --connector`, `deliver_by`, `[[product.source.volume]]`, `[product.output.volume]`, `[pipeline.*.schedule]`, `rocky tick`, `[cost]`, `rocky cost --month --by product`, `[product_budget.*]`, `on_budget_breach`, `rocky serve` HTTP API

## Feature

One project that runs every open data platform feature end to end, from a Postgres source to the browser UI's HTTP API, and checks each step's JSON output.

## Why it's distinctive

Each feature has its own tests. This journey checks that they work together in one project, the way an operator uses them. `run.sh` asserts the facts in the JSON, not only exit codes. The [operator runbook](../../../../../docs/src/content/docs/guides/open-stack-acceptance.md) has the same steps for a person to run by hand and record pass or fail.

```
 Postgres ──rocky load──▶ poc.main.events ──rocky run──▶ poc.main.events_by_type
 public.events  --connector      │                              │
                     source range (1 to 20 rows)     output range (1 to 3 rows)
                                                     deadline every minute
                                                     budget 0.72 ms a month
                                                                │
                                         rocky serve ──▶ GET /api/v1/...
```

## Layout

```
.
├── README.md                      this file
├── rocky.toml                     DuckDB target, [connector.pg_events], a scheduled transformation
│                                  pipeline, [cost] prices, a product budget and its hook
├── products/events_by_type.toml   the product: deliver_by, source and output volume ranges
├── models/events_by_type.sql      event count and amount per event type
├── models/events_by_type.toml     full refresh into poc.main
├── docker-compose.yml             postgres:16-alpine on port 55433, seeded by the S-2 POC's seed.sql
├── check.py                       the assertions, one subcommand per step (Python 3.8+, stdlib only)
└── run.sh                         the scripted journey
```

The connector is the S-2 reference connector, used as shipped: `[connector.pg_events]` points at [`07-adapters/08-postgres-child-connector/connector/connector.toml`](../../07-adapters/08-postgres-child-connector/).

## Run

```bash
./run.sh                    # skips with exit 0 when Docker is absent
REQUIRE_DOCKER=1 ./run.sh   # fails instead (the weekly CI does this)
```

Needs `rocky`, `docker compose`, `python3` 3.8+, the `duckdb` CLI and `curl`. Set `ROCKY_BIN` to test a specific binary, and `ROCKY_ACCEPTANCE_PORT` to move the server off port 8093.

| Step | Command | What `check.py` asserts |
|---|---|---|
| 1 | `rocky load --connector pg_events` | 19 rows, commit_seq 1 |
| 2 | `rocky run --pipeline events` | source 19 `in_range`, output 3 `in_range`, budget `breached` with a breach event, the hook fired once |
| 3 | `rocky product status` | `on_time: true`, estimate `unknown`, `at_risk: null` |
| 4 | 4 more runs | no new breach event, still one hook call |
| 5 | insert 5 rows, `rocky load` | 5 rows, commit_seq 2 |
| 6 | wait about 2 minutes | the open deadline passes |
| 7 | `rocky run` | source 24 `above_max` with a warning line; output still builds |
| 8 | `rocky product status` | `on_time: false`, `late_by_seconds > 0`, estimate `known`, `at_risk` a boolean |
| 9 | `rocky load` | 0 rows, commit_seq 3 |
| 10 | `rocky tick` | the 23:59 cron is not due; nothing runs |
| 11 | `rocky cost --month <M> --by product` | compute priced at the `[cost]` rate, budget `breached`, one breach event at run 1, the totals add up |
| 12 | `rocky serve`, `GET /api/v1/cost/month`, `/products/events_by_type`, `/schedule` | the same facts as the CLI; `cron_check: too_late` |
| 13 | password grep | 0 hits |

## Expected output

```text
=== 2. run 1: rocky run --pipeline events ===
    source range: poc.main.events rows=19 (in_range)
    output range: events_by_type rows=3 (in_range)
    budget: breached, breach event at this run
    on_budget_breach fired once: product=events_by_type month=2026-11 limit_type=max_compute_hours
...
=== 8. rocky product status events_by_type (late delivery, known estimate) ===
    delivery: on_time=False late_by_seconds=4 at_risk=False estimate=known (n=6)
    volume: output in_range, source above_max
    cost_month: 2026-11 budget breached
...
=== 12. rocky serve: the Delivery and cost panel's HTTP API ===
    GET /api/v1/cost/month?month=2026-11 matches rocky cost --month --by product (compute charged, kept and built; budget verdict; breach events)
    GET /api/v1/products/events_by_type matches rocky product status (delivery, volume, source_volumes); budget breached
    GET /api/v1/schedule: pipeline events delivers events_by_type, on_time=False, latest_start state=known cron_check=too_late
...
ACCEPTANCE PASS: connector, source and output volume, delivery on time and late,
                 monthly cost by product, budget breach and hook, HTTP API.
```

A run started after 23:50 UTC first waits for midnight, so it cannot cross into a new cost month or reach the 23:59 cron.

## Related

- [`07-adapters/08-postgres-child-connector`](../../07-adapters/08-postgres-child-connector/): the connector on its own
- [Open-stack acceptance runbook](../../../../../docs/src/content/docs/guides/open-stack-acceptance.md)
