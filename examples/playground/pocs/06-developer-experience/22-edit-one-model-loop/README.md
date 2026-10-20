# 22-edit-one-model-loop — edit one model, rebuild only it and its downstream

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 15s
> **Rocky features:** `[run] skip_unchanged`, `rocky run --watch --models`, `rocky run --model --defer --defer-to`, `--defer-to-state`

## What it shows

The fast inner loop. You edit one model. Rocky rebuilds that model and the models downstream of it. It skips every model that is not affected. `run.sh` checks steps 1 to 4 against the `model_decisions` array in the JSON output. It checks steps 5 and 6 against the `materializations` array. It exits 1 when a claim does not hold.

```
raw__shop.orders ──► stg_orders ──┬──► daily_revenue
                                  └──► customer_orders ◄── stg_customers ◄── raw__shop.customers
                                                                │
                                                                └──► region_summary
```

## Why it's distinctive

- It combines three flags that other POCs show one at a time: the skip gate ([`02-performance/14-skip-unchanged`](../../02-performance/14-skip-unchanged)), watch mode ([`13-run-watch-inner-loop`](../13-run-watch-inner-loop)) and defer ([`20-defer-against-prod`](../20-defer-against-prod)).
- It checks the exact set of models built and skipped, not only a count.
- No new command. Everything is in `rocky run`.

## Layout

```
.
├── README.md
├── rocky.toml        DuckDB transformation pipeline, [run] skip_unchanged = true
├── expected/         run output (gitignored JSON)
├── run.sh            seed, edit, rebuild, watch, defer; fails loudly
├── data/seed.sql     50 customers, 500 orders (byte-stable)
└── models/           5 models: 2 staging, 3 downstream
```

## Prerequisites

- `rocky` on PATH, or `ROCKY_BIN=/path/to/rocky`
- `duckdb` CLI for seeding (`brew install duckdb`)
- `python3`

## Run

```bash
./run.sh
```

`run.sh` edits two model files during the run. It restores both on exit.

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
=== 5. --defer: build one model, read its upstream from a base copy ===
    with --defer --defer-to prod: built = daily_revenue
=== 6. --defer-to-state: find the upstream from a saved production state ===
    with --defer --defer-to-state: built = daily_revenue
```

## What happened

1. The first run builds all 5 models and records a logic hash for each.
2. The second run changes nothing. All 5 models skip.
3. The edit changes the SQL of `stg_orders`. Its hash differs, so it builds. `daily_revenue` and `customer_orders` read it, so they build too. `stg_customers` and `region_summary` do not depend on it, so they skip.
4. `rocky run --watch --models models` re-runs when you save a file. `--watch` cannot be combined with `--model`. The skip gate does the narrowing.
5. `--model daily_revenue` fails, because the local `stg_orders` is gone. `--model daily_revenue --defer --defer-to prod` builds one model. It reads `stg_orders` from the `prod` schema. The local `stg_orders` stays absent.
6. The script builds a copy of the project into the `prod` schema and keeps its state file. The local `stg_orders` is still absent. `--model daily_revenue` fails. With `--defer --defer-to-state <file>` it builds only `daily_revenue`, and the state file says where to read `stg_orders`.

## Related

- Guide: `docs/src/content/docs/guides/edit-one-model-loop.md`
- Reference: [Skip Unchanged Models and Defer to Prod](../../../../../docs/src/content/docs/guides/skip-and-defer.md)
