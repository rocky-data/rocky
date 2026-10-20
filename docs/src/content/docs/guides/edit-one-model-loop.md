---
title: Edit One Model, Rebuild Only It
description: The fast local loop. Edit one model, and Rocky rebuilds that model and its downstream models, and skips the rest. Uses --skip-unchanged, --watch and --defer. No new command.
sidebar:
  order: 22
---

This guide shows the local development loop for a transformation pipeline. You edit one model. Rocky rebuilds only that model and the models that read it. You can follow it in about a minute.

There is no special command. The loop uses three flags of `rocky run` that already exist. The [runnable POC](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/06-developer-experience/22-edit-one-model-loop) proves every step below from the JSON output.

## The pieces

| Flag | What it does in the loop |
|---|---|
| `--skip-unchanged` (or `[run] skip_unchanged = true`) | Skips a model when its logic and its inputs are unchanged since its last successful build. |
| `--watch` | Runs the pipeline again each time you save a file. |
| `--defer` | Builds only the model you pick. Reads its upstream models from production. |

A skipped model is not rebuilt. A model that reads a rebuilt model is always rebuilt. The [safety contract](/guides/skip-and-defer/#the-skip-unchanged-safety-contract) lists the main cases of models that never skip. The list is not complete: a model that calls a user-defined function, or that sets `[skip] eligible = false`, also always rebuilds.

## The example project

The POC has five models in `models/`. `stg_orders` and `stg_customers` read raw tables. The other three read the staging models.

```
raw orders ──► stg_orders ──┬──► daily_revenue
                            └──► customer_orders ◄── stg_customers ◄── raw customers
                                                          │
                                                          └──► region_summary
```

Each model is a plain `SELECT` over bare table names, with no CTE and no sub-query. That shape lets Rocky prove that the model's inputs are unchanged. A model that uses a CTE or a non-deterministic function always rebuilds.

## Step 1: turn on the skip gate

Add this to `rocky.toml`:

```toml
[run]
skip_unchanged = true
skip_rowcount_fallback = true
```

Rocky must prove that a raw table did not change. It probes `MAX(<timestamp>)` only for an `incremental` or `microbatch` model that sets a timestamp column. A model with any other strategy, `full_refresh` included, has no timestamp to probe. All five models in this example are `full_refresh`. Without the second line, the two staging models never skip, and so nothing downstream of them skips.

With the second line, Rocky compares the row count of the raw table instead. A row count is a weaker signal than a timestamp. It can miss an `UPDATE` that keeps the same number of rows. If your models are `incremental` with a `timestamp_column`, you do not need it.

## Step 2: build once, then edit one model

```bash
rocky run                     # first run: all 5 models build
rocky run                     # second run: all 5 models skip
```

Now edit `models/stg_orders.sql` and run again:

```bash
rocky run -o json
```

The `model_decisions` array in the JSON output names each decision:

```
build  stg_orders       model logic changed since last build
build  customer_orders  upstream data may have changed since last build
build  daily_revenue    upstream data may have changed since last build
skip   stg_customers    logic and upstream data unchanged since last build
skip   region_summary   logic and upstream data unchanged since last build
```

You changed one model. Three models built. Two skipped.

## Step 3: let Rocky run on save

```bash
rocky run --watch --models models
```

Rocky runs once when it starts. Then it runs again about 200 ms after each save. Edit `models/daily_revenue.sql` and save. The next run builds only `daily_revenue`, because no other model reads it.

Two limits apply:

- Pass `--models models` (the models directory). Without it, `--watch` watches `rocky.toml` only, and a model edit does not start a run. Checked with Rocky 1.86.0.
- `--watch` cannot be combined with `--model`, `--select`, `--exclude`, `--dag`, `--resume`, `--resume-latest`, `--idempotency-key`, `--contracts`, `--keep-shadow`, `--refuse-hooks` or `--assume-fresh-state`. The skip gate does the narrowing instead.

With `-o json`, each run prints one JSON object to stdout. Banner lines go to stderr.

## Step 4: read upstream models from production

When you work in a fresh checkout, you may have no local copy of the upstream tables. Build one model and read its upstream from another schema:

```bash
rocky run --model daily_revenue --defer --defer-to prod
```

Rocky builds `daily_revenue`. It reads `stg_orders` from the `prod` schema. It does not build `stg_orders`. Without `--defer`, the same command fails, because the local `stg_orders` does not exist.

If production writes to different schemas than your development config, point Rocky at a copy of production's state file instead. The state file records where each model was written:

```bash
rocky run --model daily_revenue --defer --defer-to-state ./prod-state.redb
```

The POC builds a copy of the project into the `prod` schema, keeps its state file, and checks that the command fails without `--defer-to-state` and works with it. `--defer-to` and `--defer-to-state` cannot be used together. `--defer` does nothing without `--model`. Read the [defer section](/guides/skip-and-defer/#--defer-develop-against-production-upstreams) for the SQL constructs that `--defer` cannot rewrite.

## How much time it saves

The time you save is the build time of the models Rocky skips. A small project saves nothing, because the fixed cost of starting Rocky is about 140 ms.

We measured on a laptop, with a release build of `rocky` and a local DuckDB file. Each figure is the median of 10 runs. The full-run baseline uses `--force-rebuild`, which turns the skip gate off, so every model builds. The command was:

```bash
rocky --state-path state.redb -c rocky.toml -o json run [--force-rebuild]
```

| Orders in the raw table | Full run (`--force-rebuild`, 5 of 5 build) | No edit (0 of 5 build) | Edit `stg_orders` (3 of 5 build) | Edit `daily_revenue` (1 of 5 build) |
|---|---|---|---|---|
| 500 (the POC seed) | 148 ms | 140 ms | 150 ms | 144 ms |
| 20,000,000 (one-off scratch run) | 351 ms | 142 ms | 350 ms | 159 ms |

The 500-row figures use the POC seed. The 20-million-row figures come from one scratch copy of the POC, with `generate_series(1, 500)` in `data/seed.sql` changed to `generate_series(1, 20000000)`. The POC does not ship that variant. Read the table with care:

- At 500 rows, no run saves time. All the time is start-up.
- At 20 million rows, an edit to a leaf model is 2.2 times faster than a full run. An edit to `stg_orders` saves nothing, because the expensive models read `stg_orders` and must rebuild.
- The skip gate saves most when you edit a model near the end of the graph.

Your numbers will differ. They depend on the warehouse, the model SQL and the size of the data.

## Related

- [Skip Unchanged Models and Defer to Prod](/guides/skip-and-defer/) is the full reference for both flags, including the safety contract.
- [`rocky run`](/reference/commands/core-pipeline/#rocky-run) lists every flag.
- [Incremental Processing](/concepts/incremental/) covers the materialization strategies.
