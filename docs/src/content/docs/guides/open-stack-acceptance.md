---
title: Open-Stack Acceptance Runbook
description: Run the open-stack acceptance journey by hand and check each result. Covers the Postgres child connector, a product deadline, volume ranges, monthly cost by product, a budget breach, and the browser UI HTTP API.
sidebar:
  order: 15
---

This runbook runs one project by hand. At each step you check a fact in the JSON output. The same journey runs as a script.

The project is the POC [`05-orchestration/13-open-stack-acceptance`](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/05-orchestration/13-open-stack-acceptance). It needs no warehouse credentials. It starts its own Postgres in Docker and writes to a local DuckDB file.

## What the journey tests

```
 Postgres ──rocky load──▶ poc.main.events ──rocky run──▶ poc.main.events_by_type
 public.events  --connector      │                              │
                                 │ source range                 │ output range
                                 │ (1 to 20 rows)               │ (1 to 3 rows)
                                 ▼                              ▼
                        counted at run start          counted after the build
                                                                │
              deadline every minute ◀── delivery: on time, late, at risk
              budget 0.72 ms a month ◀── cost by product, breach event, hook
                                                                │
                                         rocky serve ──▶ GET /api/v1/...
```

The deadline is every minute (`deliver_by = "* * * * *"`), so you see an on-time and a late delivery in minutes. The budget limit is 0.72 ms of model time, so the first run breaches it.

## Before you start

Put `rocky`, `docker` (with `docker compose` and a running daemon), `python3` 3.8+, the `duckdb` CLI and `curl` on `PATH`. The first run downloads the `postgres:16-alpine` image and the DuckDB `postgres` extension.

Open a terminal in the POC directory. Set the Postgres settings. They match `docker-compose.yml`.

```bash
cd examples/playground/pocs/05-orchestration/13-open-stack-acceptance
export PGHOST=127.0.0.1 PGPORT=55433 PGUSER=rocky_reader \
       PGPASSWORD=rocky-acceptance-reader-pw PGDATABASE=rocky_src
alias r='rocky -c rocky.toml --state-path .rocky/state.redb --output json'
rm -rf .rocky expected poc.duckdb && mkdir expected
```

## Run the scripted journey

Run the script first. It does every step below and asserts every fact.

```bash
REQUIRE_DOCKER=1 ./run.sh
```

It takes about 4 minutes and ends with `ACCEPTANCE PASS`. It prints `FAIL:` at the first failed step. Without Docker it prints `SKIP`; `REQUIRE_DOCKER=1` turns that into a failure. Then reset and run the steps by hand:

```bash
rm -rf .rocky expected poc.duckdb && mkdir expected
```

## Steps

Do the steps in order. Each step names the facts that make it pass.

### 1. Check the config

```bash
rocky -c rocky.toml validate
```

Pass: `"valid": true`, and a `V067` message with severity `ok` for `connector.pg_events`.

### 2. Start Postgres

```bash
docker compose up -d --wait
```

Pass: the container `rocky-open-stack-acceptance` is `Healthy`. Postgres holds 20 rows in `public.events`.

### 3. Load 1 with the child connector

```bash
r load --connector pg_events > expected/load-1.json
```

Pass: `connector.status` is `loaded`, `total_rows` is `19`, and `connector.commit_seq_after` is `1`. The 20th row waits: the connector holds back the rows at the source maximum.

### 4. Run 1: build the product

```bash
r run --pipeline events > expected/run-1.json
```

Pass, all of:

- `status` is `Success`.
- `source_volumes[0]`: `table` is `poc.main.events`, `row_count` is `19`, `verdict` is `in_range`.
- The `materializations` entry for `events_by_type` has `volume.row_count` `3` and `volume.verdict` `in_range`.
- `product_budgets[0].budget.verdict` is `breached`, and `product_budgets[0].breach_event` is present. Write down its `run_id`.
- `expected/budget-breach-hook.jsonl` has one line. Its `event` is `budget_breach`, and its `metadata.product` is `events_by_type`.

### 5. Product status after one delivery

```bash
r product status events_by_type > expected/status-1.json
```

Pass: `delivery.on_time` is `true`, `delivery.late_by_seconds` is `0`, `delivery.estimate.state` is `unknown`, and `delivery.at_risk` is `null`. The estimate needs 5 runs before it gives a verdict.

### 6. Runs 2 to 5

Run the pipeline four more times, quickly.

```bash
for n in 2 3 4 5; do r run --pipeline events > expected/run-$n.json; done
```

Pass: each run has `status` `Success` and a `breached` budget with no `breach_event`. The breach was already recorded at run 1. `expected/budget-breach-hook.jsonl` still has one line.

### 7. Load 2: new rows at the source

Insert 5 rows in Postgres, then load.

```bash
docker compose exec -T postgres psql -U "$PGUSER" -d "$PGDATABASE" -c \
  "INSERT INTO public.events (id, event_type, amount_cents, created_at)
   SELECT g, 'purchase', g * 100, timestamptz '2026-10-01 00:00:00+00' + g * interval '1 minute'
   FROM generate_series(21, 25) AS g;"
r load --connector pg_events > expected/load-2.json
```

Pass: `total_rows` is `5`, `connector.commit_seq_after` is `2`. The table now holds 24 rows.

### 8. Wait for a deadline to pass

Wait about two minutes, until the clock is past the second minute boundary after run 5. Run 5 covered the next deadline. The deadline after it must pass with no delivery.

### 9. Run 6: a late delivery

```bash
r run --pipeline events > expected/run-6.json
```

Pass:

- `source_volumes[0].row_count` is `24` and its `verdict` is `above_max`. The range is a warning, so the output still builds.
- `volume_warnings` has a line that names `poc.main.events`.
- The output range is still `in_range` with 3 rows.

### 10. Product status after the late delivery

```bash
r product status events_by_type > expected/status-2.json
```

Pass:

- `delivery.on_time` is `false`, and `delivery.late_by_seconds` is more than `0`.
- `delivery.estimate.state` is `known`, and `delivery.at_risk` is `true` or `false`, not `null`.
- `source_volumes[0].verdict` is `above_max`, and `volume.verdict` is `in_range`.
- `cost_month.cost.budget.verdict` is `breached`.

### 11. Load 3: nothing new

```bash
r load --connector pg_events > expected/load-3.json
```

Pass: `total_rows` is `0` and `connector.commit_seq_after` is `3`. The state commits even when no row moves.

### 12. Let the scheduler see the cron

```bash
r tick > expected/tick.json
```

Pass: `evaluated` has the pipeline `events` with `due` `false`, and `executed` is empty. The cron fires at 23:59 UTC, so do not run this step between 23:50 and 00:00 UTC.

### 13. Cost for the month, by product

Use the month in `cost_month.month` from step 10, for example `2026-11`.

```bash
r cost --month 2026-11 --by product > expected/cost-month.json
```

Pass, for the product `events_by_type`:

- `charged` has a `compute_ms` amount above `0`.
- `money_status` is `priced`. The `compute` entry in `money_parts` is `compute_ms` × $0.000001: `[cost] compute_cost_per_machine_hour` is `3.6`.
- `egress` and `ai_tokens` in `money_parts` are `not_measured`.
- `budget.verdict` is `breached`. `breach_events` has one event, and its `run_id` is the one from step 4.
- For each unit in `measured_total`, the products' `charged` plus `unallocated` equals the total.

### 14. The browser UI's HTTP API

Start the server in a second terminal, with the same state store.

```bash
rocky -c rocky.toml --state-path .rocky/state.redb serve --port 8093
```

The product page's **Delivery and cost** panel reads these routes.

```bash
curl -s "http://127.0.0.1:8093/api/v1/cost/month?month=2026-11" > expected/api-cost-month.json
curl -s http://127.0.0.1:8093/api/v1/products/events_by_type > expected/api-product.json
curl -s http://127.0.0.1:8093/api/v1/schedule > expected/api-schedule.json
```

Pass:

- `api-cost-month.json` has the same product row as step 13: the same `compute_ms` in `charged`, `kept` and `built`, the same budget verdict, the same breach events. Storage byte-hours grow with time, so they can differ.
- `api-product.json` has the same `delivery` facts, `volume` and `source_volumes` as step 10. `next_due_at` and `at_risk` move with the clock.
- In `api-schedule.json`, the pipeline `events` has `delivery.product` `events_by_type` and `delivery.on_time` `false`. Its `latest_start` has `cron_check` `too_late`, `cron_too_late` `true` and a `warning`. A cron at 23:59 cannot meet a deadline every minute.

### 15. No secret in the output

```bash
grep -r -a -F -l -- "$PGPASSWORD" expected .rocky && echo FAIL || echo "0 hits"
```

Pass: `0 hits`.

### 16. Clean up

```bash
docker compose down -v
```

## Result

The journey passes when every step passes. For a failed step, keep its JSON file and `.log` from `expected/`.

## Related

- [Child connectors](/guides/child-connectors/): the protocol and the reference Postgres connector
- [Product commands](/reference/commands/products/): deadlines, volume ranges and `rocky product status`
- [`rocky cost --month`](/reference/commands/administration/#one-month-split-by-product): the monthly split by product
- [`[product_budget.NAME]`](/reference/configuration/#product_budgetname): monthly budgets
- [Browser UI](/guides/browser-ui/#delivery-and-cost): the Delivery and cost panel
