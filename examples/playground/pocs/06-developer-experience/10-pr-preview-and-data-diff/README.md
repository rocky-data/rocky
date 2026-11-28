# 10-pr-preview-and-data-diff — `rocky preview` PR-bundle

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 30s
> **Rocky features:** `rocky preview create`, `rocky run --branch`, `rocky preview diff`, `rocky preview cost`

## What it shows

The three `rocky preview` subcommands and their JSON output, on a 5-model SQL DuckDB pipeline (`raw_orders` + `raw_customers` → `stg_orders` + `dim_customers` → `fct_revenue`):

1. **`rocky preview create --base <ref>`** — `git diff <ref>...HEAD` finds model files changed *between two committed refs*. They and their transitive downstream form the **prune set**; every other model is copied from the base schema into a per-PR branch schema (`branch__<name>`). It runs no model. Run the branch with `rocky run --branch <name>` (whole pipeline, or one model with `--model`).
2. **`rocky preview diff`** — row-level diff between the branch run and the base run, for every model the branch run executed. By default it compares the row counts the runs recorded and reads no rows; a model with no recorded count reports `null`, never `0`. The structural arrays (column added/removed/type changed) stay empty today. `--algorithm bisection` does exhaustive checksum-bisection for `Merge` models declaring a single-column integer / numeric `unique_key`.
3. **`rocky preview cost`** — per-model bytes / duration / USD delta versus the newest run that is not the branch's own. Copied models contribute savings; only re-run models contribute to the delta. With `[budget]` configured, it surfaces projected breaches (for example `max_usd`) before merge.

On a **real PR**, where the change is committed between `main` and PR HEAD, changing `fct_revenue` prunes only itself. Changing `raw_orders` prunes `raw_orders → stg_orders → fct_revenue`, and `raw_customers` and `dim_customers` are copied.

### What the local `./run.sh` actually produces

**Read this before comparing output to the above.** `run.sh` applies its synthetic `fct_revenue` edit to the *working tree* without committing, and the prune set comes from committed refs. So locally:

- The **prune set is empty**.
- `preview create` copies **all 5** models via CTAS (`copy_strategy: "ctas"`) and re-runs none; `run_status` is `"planned"` with an empty `run_id`.
- `run.sh` then runs `rocky run --branch pr_preview_poc_10`, which builds all 5 models, with the edit, into the branch schema and records a branch run.
- `preview diff` and `preview cost` pair that run against the base run on `main`. `run.sh` fails unless the diff covers at least one model and the cost output names a `branch_run_id`.

Only a non-empty prune set needs a committed diff. The composite GitHub Action runs the same `rocky run --branch` step between `preview create` and `preview diff`, skipping it when the prune set is empty ([#2162](https://github.com/rocky-data/rocky/issues/2162)).

## Behaviour and limits

- **Copy primitive per adapter** (`WarehouseAdapter::clone_table_for_branch`): Databricks `SHALLOW CLONE`, Snowflake `CREATE TABLE … CLONE` and BigQuery `CREATE TABLE … COPY` are metadata-only; DuckDB and other adapters use a portable CTAS. The output reports `copy_strategy: "ctas"` whichever ran.
- **Pruning is model-level.** Column-level pruning is not implemented (`changed_columns` is always empty). For column-level blast radius, see [`11-lineage-diff`](../11-lineage-diff/).
- **Cost delta is a state-store query, not a fresh measurement**, layered over the per-run cost that `rocky cost latest` rolls up.
- The composite GitHub Action posts one PR comment with row counts, columns and cost delta.
- There is no `.rocky` DSL variant yet; it waits for cross-model references in transformation pipelines to round-trip through `rocky run` for both DSL and SQL.

## Prerequisites

- `rocky` on PATH (or `ROCKY_BIN`; `run.sh` prefers `engine/target/release/rocky`, then `engine/target/debug/rocky`)
- `duckdb` CLI for seeding (`brew install duckdb`)
- `jq`
- `git`, and the POC inside a git checkout. Outside one, `run.sh` falls back to the sentinel ref `poc-base`, `rocky preview create` cannot `git diff` against it, and `run.sh` exits 1.

## How to run

```bash
cd examples/playground/pocs/06-developer-experience/10-pr-preview-and-data-diff
./run.sh
```

The script cleans state and `poc.duckdb`, compiles, seeds, runs on `main`, takes git HEAD as `--base`, swaps in `models/fct_revenue.sql.changed` (adds `WHERE s.amount > 25`), then runs `preview create`, `run --branch`, `preview diff` (default, then `--algorithm bisection`, which skips the POC's `full_refresh` models) and `preview cost`. The edit is reverted on exit.

Any `preview` command error fails the script (`set -e`). JSON goes to `expected/` (gitignored; absent until you run). Timestamps, branch schema and run ids change from run to run.

## Related

- [`00-foundations/06-branches-replay-lineage/`](../../00-foundations/06-branches-replay-lineage/): the primitives `preview` composes.
- [`06-developer-experience/04-shadow-mode-compare/`](../04-shadow-mode-compare/): the `rocky compare` kernel that `preview diff` extends.
- Engine source: `engine/crates/rocky-cli/src/commands/preview.rs`, `engine/crates/rocky-cli/src/output.rs` (`PreviewCreateOutput`, `PreviewDiffOutput`, `PreviewCostOutput`).
