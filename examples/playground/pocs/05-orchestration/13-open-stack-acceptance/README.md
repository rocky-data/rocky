# 13-open-stack-acceptance — The open-stack acceptance journey

> **Category:** 05-orchestration
> **Credentials:** none (Docker daemon required; the POC brings its own Postgres)
> **Runtime:** about 4 minutes, most of it one wait for a deadline to pass (`rocky validate` only, in under 1 s, when Docker is absent)
> **Rocky features:** `rocky load --connector`, `deliver_by`, `[[product.source.volume]]`, `[product.output.volume]`, `[pipeline.*.schedule]`, `rocky tick`, `[cost]`, `rocky cost --month --by product`, `[product_budget.*]`, `on_budget_breach`, `rocky serve` HTTP API

## Feature

One project that runs the open data platform features end to end, from a Postgres
source to the browser UI's HTTP API, and asserts facts in each step's JSON, not
only exit codes.

```
 Postgres ──rocky load──▶ poc.main.events ──rocky run──▶ poc.main.events_by_type
 public.events  --connector      │                              │
                     source range (1 to 20 rows)     output range (1 to 3 rows)
                                                     deadline every minute
                                                     budget 0.72 ms a month
                                                                │
                                         rocky serve ──▶ GET /api/v1/...
```

The product is defined in `products/events_by_type.toml`. The connector is the
S-2 reference connector, used as shipped: `[connector.pg_events]` points at
[`07-adapters/08-postgres-child-connector/connector/connector.toml`](../../07-adapters/08-postgres-child-connector/).

## Run

```bash
./run.sh                    # skips with exit 0 when Docker is absent
REQUIRE_DOCKER=1 ./run.sh   # fails instead (the weekly CI does this)
```

Needs `rocky`, `docker compose`, `python3` 3.8+, the `duckdb` CLI and `curl`.
Set `ROCKY_BIN` to test a specific binary, and `ROCKY_ACCEPTANCE_PORT` to move
the server off port 8093. The compose file publishes Postgres on port 55433.

A run started after 23:50 UTC first waits for midnight, so it cannot cross into a
new cost month or reach the 23:59 cron.

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

The last lines:

```text
ACCEPTANCE PASS: connector, source and output volume, delivery on time and late,
                 monthly cost by product, budget breach and hook, HTTP API.
```

## Related

- [`07-adapters/08-postgres-child-connector`](../../07-adapters/08-postgres-child-connector/): the connector on its own
- [Open-stack acceptance runbook](../../../../../docs/src/content/docs/guides/open-stack-acceptance.md): the same steps by hand
