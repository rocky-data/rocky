# 10-pr-preview-and-data-diff — `rocky preview` PR-bundle

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 30s
> **Rocky features:** `rocky preview create`, `rocky preview diff`, `rocky preview cost`

## What it shows

The three `rocky preview` subcommands and their JSON output schemas,
driven end-to-end on a 5-model DuckDB transformation pipeline:

1. **`rocky preview create --base <ref>`** — `git diff <ref>...HEAD`
   identifies the model files that changed *between two committed refs*;
   those changed models plus their transitive downstream form the
   **prune set**; every model *not* in the prune set is copied from the
   base schema via CTAS into a per-PR branch schema (`branch__<name>`);
   the prune set re-executes against the branch.
2. **`rocky preview diff`** — structural (column added/removed/type
   changed) plus row-level diff between branch and base for every
   model in the prune set. A `--sample-size N` sampled row diff is the
   default; `--algorithm bisection` switches on exhaustive
   checksum-bisection for models declaring a single-column integer /
   numeric `unique_key` on a `Merge` strategy. The POC runs both
   invocations to demonstrate the two entry points.
3. **`rocky preview cost`** — per-model bytes / duration / USD delta
   versus the latest base-schema run. Copied models contribute cost
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
- Because no model was re-run on the branch, there is no branch run in
  the state store, so `preview diff` reports `models: []` (*"No paired
  runs in the state store"*) and `preview cost` reports an empty
  `branch_run_id` (*"No branch run yet"*).

The local run therefore exercises the **CLI surface, branch
registration, CTAS copy-from-base, and all three output schemas** — but
the non-empty prune set, the row-level data diff, and the cost delta
only light up when the change is a committed diff, which is how the
composite GitHub Action drives `preview` in CI.

## Why it's distinctive

Built on Rocky primitives that Fivetran's Smart Run technique
(`COPY` + git-diff + re-run-changed) reaches for but cannot match
without rewriting dbt's compiler:

- **Column-level pruning > model-level.** Compiler IR knows column-level
  dependencies. A column added to an unused tail of a wide table prunes
  to zero downstream; Smart Run has to re-run the whole subtree.
- **Branches are the substrate.** Schema-prefix branches everywhere;
  warehouse-native clones slot into the same API via the
  `WarehouseAdapter::clone_table_for_branch` trait method: Databricks
  `SHALLOW CLONE` and BigQuery `CREATE TABLE … COPY` (both metadata-only)
  are live as of `engine-v1.19.1`. DuckDB and Snowflake use the portable
  CTAS default; Snowflake's native `CLONE` lands when a Snowflake
  consumer drives the integration test.
- **Compile-time change detection > git-diff alone.** `rocky ci-diff`
  already does git-diff between refs and produces a structural diff;
  the next step (*"this textual change is type-equivalent and produces
  no output diff"*) is something only a compiled engine can do.
- **Cost delta as a state-store query, not a fresh measurement.**
  `rocky cost latest` already rolls per-run cost from adapter telemetry;
  `preview cost` is the diff layer over that machinery.
- **Single PR comment.** The composite GitHub Action stitches all three
  outputs into one comment: row counts, columns, and cost delta in one place.

Compare:

| Property | Fivetran Smart Run | `rocky preview` |
|---|---|---|
| Pruning granularity | Model-level | Column-level (compiler IR) |
| Copy substrate | `COPY` | Per-adapter dispatch: Databricks `SHALLOW CLONE`, BigQuery `CREATE TABLE … COPY` (metadata-only); DuckDB / Snowflake CTAS |
| Cost delta | Not surfaced | First-class output |
| Data diff | Not surfaced | First-class output |
| PR comment | Not described | First-class output |

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

Production path as of **engine-v1.18.0**. Earlier revisions of `run.sh`
wrapped each preview call in a stub-tolerating helper while the engine
handlers were still scaffolding; that scaffolding is gone, and the script
now treats `preview create / diff / cost` like any other production CLI
command (`set -e` enforces failure).

The live `expected/preview_*.json` outputs are captured on each run for
inspection; they are gitignored and vary in their non-deterministic
fields (timestamps, branch schema, run ids) run to run.

## Prerequisites

- `rocky` ≥ 1.18.0 on PATH
- `duckdb` CLI for seeding (`brew install duckdb`)

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
5. Captures the current git HEAD as the `--base` ref (or a sentinel
   string when not in a git checkout).
6. Swaps `models/fct_revenue.sql` for `fct_revenue.sql.changed` in the
   working tree, adding a `WHERE s.amount > 25` filter. (This edit is
   *uncommitted* — see "What the local `./run.sh` actually produces": the
   prune set is computed from committed refs, so this working-tree swap
   does not appear in the prune set.)
7. `rocky preview create --base <ref> --name pr_preview_poc_10` —
   registers the branch and, with an empty prune set, copies all 5
   models from the base schema via DuckDB CTAS
   (`copy_strategy: "ctas"`).
8. `rocky preview diff --name pr_preview_poc_10` — structural + sampled
   row diff between branch and base. Re-invoked with `--algorithm
   bisection`. Both report no paired branch run in a local run.
9. `rocky preview cost --name pr_preview_poc_10` — per-model bytes /
   duration / USD delta vs. the latest base run.
10. Reverts the synthetic change (`trap`-protected, idempotent).

## Related

- Sibling POC: [`00-foundations/06-branches-replay-lineage/`](../../00-foundations/06-branches-replay-lineage/),
  covering the four trust-arc primitives (branches, replay, column lineage,
  state store) that `preview` composes.
- Sibling POC: [`06-developer-experience/04-shadow-mode-compare/`](../04-shadow-mode-compare/),
  the precursor `rocky compare` kernel that `preview diff` extends.
- Engine source: `engine/crates/rocky-cli/src/commands/preview.rs`,
  `engine/crates/rocky-cli/src/output.rs`
  (`PreviewCreateOutput`, `PreviewDiffOutput`, `PreviewCostOutput`).
