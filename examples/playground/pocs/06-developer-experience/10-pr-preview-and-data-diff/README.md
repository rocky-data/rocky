# 10-pr-preview-and-data-diff — `rocky preview` PR-bundle

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 30s
> **Rocky features:** `rocky preview create`, `rocky run --branch`, `rocky preview diff`, `rocky preview cost`

## What it shows

The three `rocky preview` subcommands and their JSON output schemas,
driven end-to-end on a 5-model DuckDB transformation pipeline:

1. **`rocky preview create --base <ref>`** — `git diff <ref>...HEAD`
   identifies the model files that changed *between two committed refs*;
   those changed models plus their transitive downstream form the
   **prune set**; every model *not* in the prune set is copied from the
   base schema via CTAS into a per-PR branch schema (`branch__<name>`).
   `preview create` runs no model itself. You run the branch with
   `rocky run --branch <name>`, which builds the whole pipeline, or one
   model with `--model`.
2. **`rocky preview diff`** — a row-level diff between the branch run
   and the base run, for every model the branch run executed. By
   default it compares the row counts the two runs recorded and reads
   no rows. A model whose run recorded no row count reports `null`,
   never `0`. The structural arrays (column added/removed/type changed)
   stay empty today. `--algorithm bisection` switches on exhaustive
   checksum-bisection for models declaring a single-column integer /
   numeric `unique_key` on a `Merge` strategy. The POC runs both
   invocations to demonstrate the two entry points.
3. **`rocky preview cost`** — per-model bytes / duration / USD delta
   versus the newest run that is not the branch's own. Copied models contribute cost
   savings; only re-run models contribute to the delta. When `[budget]`
   is configured, the output surfaces projected budget breaches so a
   reviewer (and the CI gate) sees *"this PR would breach `max_usd` if
   merged"* before merge.

The 5-model DAG is `raw_orders` + `raw_customers` → `stg_orders` +
`dim_customers` → `fct_revenue`. On a **real PR** — where the model
change is a committed diff between `main` and the PR HEAD — a change to
`fct_revenue` prunes exactly itself, and a change to `raw_orders` prunes
`raw_orders → stg_orders → fct_revenue` while `raw_customers +
dim_customers` are copied from base.

### What the local `./run.sh` actually produces

**Read this before comparing output to the narrative above.** The prune
set is computed from a **committed** `git diff <base>...HEAD`, but
`run.sh` applies its synthetic `fct_revenue` edit to the *working tree*
without committing it (POCs don't create commits). So in a local run:

- `git diff <base>...HEAD` sees **no** changed model files → the prune
  set is **empty**.
- With an empty prune set, `preview create` copies **all 5** models from
  the base schema via CTAS (`copy_strategy: "ctas"`) and re-runs none;
  `run_status` is `"planned"` with an empty `run_id`.
- `run.sh` then runs `rocky run --branch pr_preview_poc_10` itself. That
  run builds all 5 models, with the working-tree edit, into the branch
  schema and records a branch run in the state store.
- `preview diff` and `preview cost` pair that branch run against the
  base run from step 4. `run.sh` fails unless both found it: the diff
  must cover at least one model, and the cost output must name a
  `branch_run_id`.

The local run therefore exercises the **CLI surface, branch
registration, CTAS copy-from-base, the branch run, and the paired diff
and cost**. Only a non-empty prune set needs a committed diff. The
composite GitHub Action runs the same `rocky run --branch` step between
`preview create` and `preview diff`, skipping it when the prune set is
empty ([#2162](https://github.com/rocky-data/rocky/issues/2162)).

## Why it's distinctive

The pattern is "copy what didn't change, re-run what did, diff the result".
Rocky builds it from its own parts:

- **Branches are the copy target.** Every copy lands in a schema-prefix
  branch (`branch__<name>`). Each adapter picks its cheapest copy through
  `WarehouseAdapter::clone_table_for_branch`: Databricks `SHALLOW CLONE`,
  Snowflake `CREATE TABLE … CLONE` and BigQuery `CREATE TABLE … COPY` are
  metadata-only; DuckDB and the other adapters use a portable CTAS. The
  output reports `copy_strategy: "ctas"` whichever primitive ran.
- **Pruning is model-level today.** The prune set is the changed models plus
  everything downstream of them. Column-level pruning is not implemented
  (`changed_columns` is always empty). For column-level blast radius on a PR,
  see [`11-lineage-diff`](../11-lineage-diff/).
- **Cost delta is a state-store query, not a fresh measurement.**
  `rocky cost latest` already rolls up per-run cost from adapter telemetry;
  `preview cost` is the diff layer over it.
- **Single PR comment.** The composite GitHub Action stitches all three
  outputs into one comment: row counts, columns, and cost delta.

## Layout

```
.
├── README.md                 this file
├── rocky.toml                DuckDB transformation pipeline
├── run.sh                    end-to-end demo (compile → run on main → preview)
├── data/
│   └── seed.sql              200 orders + 25 customers (synthetic)
├── models/
│   ├── _defaults.toml        catalog=poc, schema=demo
│   ├── raw_orders.sql        leaf: reads poc.demo.seed_orders
│   ├── raw_customers.sql     leaf: reads poc.demo.seed_customers
│   ├── stg_orders.sql        filter cancelled orders
│   ├── dim_customers.sql     project customer attributes
│   ├── fct_revenue.sql       join + group → total per customer
│   └── fct_revenue.sql.changed   synthetic-PR variant (added WHERE)
└── expected/                    generated by run.sh, gitignored
    ├── compile.json                 from `rocky compile`
    ├── run_main.json                from `rocky run`
    ├── preview_create.json          from `rocky preview create`
    ├── run_branch.json              from `rocky run --branch`
    ├── preview_diff.json            from `rocky preview diff` (sampled)
    ├── preview_diff_bisection.json  from `rocky preview diff --algorithm bisection`
    └── preview_cost.json            from `rocky preview cost`
```

The `expected/` directory is regenerated on every run and is gitignored,
so a fresh checkout ships none of these files until `./run.sh` runs.

## Note on model surfaces

The 5-model DAG is all SQL. Raw SQL stays first-class in Rocky; the
preview workflow's prune set, copy set, and data diff are surface-agnostic
(they care about a model's compiled output, not which DSL produced it).
A `.rocky` DSL variant of this POC will be added once cross-model
references in transformation pipelines round-trip cleanly through `rocky
run` for both DSL and SQL surfaces.

## Status

`run.sh` treats `preview create / diff / cost` like any other CLI command:
`set -e` fails the script on any error. The `expected/preview_*.json`
outputs are gitignored. Their timestamps, branch schema and run ids change
from run to run.

## Prerequisites

- `rocky` on PATH (or `ROCKY_BIN`; `run.sh` prefers `engine/target/release/rocky`, then `engine/target/debug/rocky`)
- `duckdb` CLI for seeding (`brew install duckdb`)
- `jq`, to check that `preview diff` and `preview cost` paired with the branch run
- `git`, and the POC inside a git checkout (`rocky preview create` runs
  `git diff` against the base ref)

## How to run

```bash
cd examples/playground/pocs/06-developer-experience/10-pr-preview-and-data-diff
./run.sh
```

`run.sh`:

1. Cleans `.rocky-state.redb` (root + `models/`) and `poc.duckdb`.
2. Runs `rocky compile` against the 5-model DAG.
3. Seeds the raw tables into DuckDB.
4. Runs the pipeline on `main` state.
5. Captures the current git HEAD as the `--base` ref. Run the POC inside
   a git checkout. Outside one, `run.sh` falls back to the sentinel ref
   `poc-base`, and the `rocky preview create` step below then fails: it
   cannot `git diff` against that ref, so `run.sh` exits 1.
6. Swaps `models/fct_revenue.sql` for `fct_revenue.sql.changed` in the
   working tree, adding a `WHERE s.amount > 25` filter. (This edit is
   *uncommitted* — see "What the local `./run.sh` actually produces": the
   prune set is computed from committed refs, so this working-tree swap
   does not appear in the prune set.)
7. `rocky preview create --base <ref> --name pr_preview_poc_10` —
   registers the branch and, with an empty prune set, copies all 5
   models from the base schema via DuckDB CTAS
   (`copy_strategy: "ctas"`).
8. `rocky run --branch pr_preview_poc_10` — runs the pipeline, with the
   edit, into the branch schema and records the branch run.
9. `rocky preview diff --name pr_preview_poc_10` — row-count diff
   between the branch run and the base run. Re-invoked with
   `--algorithm bisection`, which skips the POC's `full_refresh` models.
10. `rocky preview cost --name pr_preview_poc_10` — per-model bytes /
    duration / USD delta vs. the base run.
11. Checks that diff and cost both paired with the branch run, then
    reverts the synthetic change (`trap`-protected, idempotent).

## Related

- Sibling POC: [`00-foundations/06-branches-replay-lineage/`](../../00-foundations/06-branches-replay-lineage/),
  covering the primitives (branches, replay, column lineage, state store)
  that `preview` composes.
- Sibling POC: [`06-developer-experience/04-shadow-mode-compare/`](../04-shadow-mode-compare/),
  the precursor `rocky compare` kernel that `preview diff` extends.
- Engine source: `engine/crates/rocky-cli/src/commands/preview.rs`,
  `engine/crates/rocky-cli/src/output.rs`
  (`PreviewCreateOutput`, `PreviewDiffOutput`, `PreviewCostOutput`).
