# 22-edit-one-model-loop — edit one model, rebuild only it and its downstream

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 15s
> **Rocky features:** `[run] skip_unchanged`, `rocky run --watch --models`, `rocky run --model --defer --defer-to`, `--defer-to-state`

## What it shows

Edit one model, and Rocky rebuilds only that model and its downstream, skipping the rest. It combines the skip gate ([`02-performance/14-skip-unchanged`](../../02-performance/14-skip-unchanged)), watch mode ([`13-run-watch-inner-loop`](../13-run-watch-inner-loop)) and defer ([`20-defer-against-prod`](../20-defer-against-prod)), all in `rocky run`.

```
raw__shop.orders ──► stg_orders ──┬──► daily_revenue
                                  └──► customer_orders ◄── stg_customers ◄── raw__shop.customers
                                                                │
                                                                └──► region_summary
```

## Prerequisites

- `rocky` on PATH, or `ROCKY_BIN=/path/to/rocky`
- `duckdb` CLI for seeding (`brew install duckdb`)
- `python3`

## Run

```bash
./run.sh
```

`run.sh` edits two model files during the run and restores both on exit. It checks the exact set of models built and skipped (steps 1 to 4 against `model_decisions` in the JSON output, steps 5 and 6 against `materializations`) and exits 1 when a claim does not hold.

## Expected output

```text
=== 1. first run: nothing is built yet, so all 5 models build ===
    built|skipped = customer_orders,daily_revenue,region_summary,stg_customers,stg_orders|
=== 2. run again with no edit: all 5 models skip ===
    built|skipped = |customer_orders,daily_revenue,region_summary,stg_customers,stg_orders
=== 3. edit ONE model (stg_orders: double the amount), run again ===
    built|skipped = customer_orders,daily_revenue,stg_orders|region_summary,stg_customers
=== 4. watch mode: save a model file, Rocky re-runs by itself ===
    after the save: built|skipped = daily_revenue|customer_orders,region_summary,stg_customers,stg_orders
=== 5. --defer --defer-to: build one model, read its upstream from another schema ===
    with --defer --defer-to prod: built = daily_revenue
=== 6. --defer-to-state: find the upstream from a saved production state ===
    with --defer --defer-to-state: built = daily_revenue
```

## What happened

1. The first run builds all 5 models and records a logic hash for each.
2. With no change, all 5 skip.
3. Editing the SQL of `stg_orders` changes its hash, so it builds, as do its readers `daily_revenue` and `customer_orders`. `stg_customers` and `region_summary` skip.
4. `rocky run --watch --models models` re-runs on save. `--watch` cannot be combined with `--model`; the skip gate does the narrowing.
5. With the local `stg_orders` gone, `--model daily_revenue` fails. `--model daily_revenue --defer --defer-to prod` builds one model, reading `stg_orders` from the `prod` schema. The local `stg_orders` stays absent.
6. The script builds a copy of the project into the `prod` schema and keeps its state file. `--model daily_revenue --force-rebuild` fails. Adding `--defer --defer-to-state <file>` builds only `daily_revenue`; the state file says where to read `stg_orders`.

## Related

- Guide: `docs/src/content/docs/guides/edit-one-model-loop.md`
- Reference: [Skip Unchanged Models and Defer to Prod](../../../../../docs/src/content/docs/guides/skip-and-defer.md)
