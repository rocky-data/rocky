# 10-pr-preview-and-data-diff — `rocky preview` PR-bundle

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 30s
> **Rocky features:** `rocky preview create`, `rocky preview diff`, `rocky preview cost`

## What it shows

The end-to-end `rocky preview` workflow that turns a PR into a single
review surface — *"this PR will change these N rows / these M
columns, costs $X more or less to run, here's the diff"* — on a 5-model
DuckDB transformation pipeline:

1. **`rocky preview create --base <ref>`** — git-diff identifies changed
   model files between `<ref>` and HEAD; the compiler IR computes a
   **column-level prune set**; every model *not* in the prune set is
   copied from the base schema via CTAS into a per-PR branch
   schema; the prune set re-executes against the branch.
2. **`rocky preview diff`** — structural (column added/removed/type
   changed) plus row-level diff between branch and base for every
   model in the prune set. Each per-model entry carries an
   `algorithm` tagged enum picking which row-level technique ran:
   `kind: "sampled"` (default — `LIMIT N` rows ordered by primary
   key, with `sampling_window.coverage_warning`) or `kind:
   "bisection"` (`--algorithm bisection` — exhaustive
   checksum-bisection over a single-column integer / numeric
   `unique_key`). The POC runs both invocations to demonstrate the
   discriminator shape.
3. **`rocky preview cost`** — per-model bytes / duration / USD delta
   versus the latest base-schema run. Copied models contribute to
   `savings_from_copy_usd`; only re-run models contribute to
   `delta_usd`. When `[budget]` is configured, surfaces
   `projected_budget_breaches` so a reviewer (and the CI gate) sees
   *"this PR would breach `max_usd` if merged"* before merge.

The 5-model DAG (`raw_orders` + `raw_customers` → `stg_orders` +
`dim_customers` → `fct_revenue`) gives the prune-set computation
something non-trivial to chew on: a change to `fct_revenue` prunes
exactly itself; a change to `raw_orders.amount` prunes `raw_orders →
stg_orders → fct_revenue` and copies `raw_customers + dim_customers`
from base.

## Why it's distinctive

Built on Rocky primitives that Fivetran's Smart Run technique
(`COPY` + git-diff + re-run-changed) reaches for but cannot match
without rewriting dbt's compiler:

- **Column-level pruning > model-level.** Compiler IR knows column-level
  dependencies. A column added to an unused tail of a wide table prunes
  to zero downstream — Smart Run has to re-run the whole subtree.
- **Branches are the substrate.** Schema-prefix branches everywhere;
  warehouse-native clones slot into the same API via the
  `WarehouseAdapter::clone_table_for_branch` trait method — Databricks
  `SHALLOW CLONE` and BigQuery `CREATE TABLE … COPY` (both metadata-only)
  are live as of `engine-v1.19.1`. DuckDB and Snowflake use the portable
  CTAS default; Snowflake's native `CLONE` lands when a Snowflake
  consumer drives the integration test.
- **Compile-time change detection > git-diff alone.** `rocky ci-diff`
  already does git-diff between refs and produces a structural diff;
  the next step — *"this textual change is type-equivalent and produces
  no output diff"* — is something only a compiled engine can do.
- **Cost delta as a state-store query, not a fresh measurement.**
  `rocky cost latest` already rolls per-run cost from adapter telemetry;
  `preview cost` is the diff layer over that machinery.
- **Single artefact for the reviewer.** The composite GitHub Action
  stitches all three outputs into one PR comment that answers *"should
  I merge this?"* with row counts, columns, and dollars — not log lines.

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
└── expected/
    ├── compile.json                   from `rocky compile`
    ├── run_main.json                  from `rocky run`
    ├── preview_create.example.json    shape contract for `rocky preview create`
    ├── preview_diff.example.json      shape contract for `rocky preview diff`
    └── preview_cost.example.json      shape contract for `rocky preview cost`
```

## Note on model surfaces

The 5-model DAG is all SQL. Raw SQL stays first-class in Rocky — the
preview workflow's prune set, copy set, and data diff are surface-agnostic
(they care about a model's compiled output, not which DSL produced it).
A `.rocky` DSL variant of this POC will be added once cross-model
references in transformation pipelines round-trip cleanly through `rocky
run` for both DSL and SQL surfaces.

## Status

Production path as of **engine-v1.18.0**. Earlier revisions of `run.sh`
wrapped each preview call in a stub-tolerating helper while the engine
handlers were still scaffolding; that scaffolding is gone — the script
now treats `preview create / diff / cost` like any other production CLI
command (`set -e` enforces failure).

The `expected/preview_*.example.json` files remain as the shape contract
that the live `expected/preview_*.json` outputs should match modulo
non-deterministic fields (timestamps, branch suffixes, hashes).

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
6. Swaps `models/fct_revenue.sql` for `fct_revenue.sql.changed` — adds a
   `WHERE s.amount > 25` filter that produces a real row-level diff.
7. `rocky preview create --base <ref> --name pr-preview-poc-10` —
   materializes the per-PR branch schema and copies unchanged upstream
   from the base via DuckDB CTAS.
8. `rocky preview diff --name pr-preview-poc-10` — structural + sampled
   row diff between branch and base. Re-invoked with `--algorithm
   bisection` to demonstrate the tagged-enum output shape.
9. `rocky preview cost --name pr-preview-poc-10` — per-model bytes /
   duration / USD delta vs. the latest base run.
10. Reverts the synthetic change (`trap`-protected, idempotent).

## Related

- Sibling POC: [`00-foundations/06-branches-replay-lineage/`](../../00-foundations/06-branches-replay-lineage/)
  — the four trust-arc primitives (branches, replay, column lineage,
  state store) that `preview` composes.
- Sibling POC: [`06-developer-experience/04-shadow-mode-compare/`](../04-shadow-mode-compare/)
  — the precursor `rocky compare` kernel that `preview diff` extends.
- Engine source: `engine/crates/rocky-cli/src/commands/preview.rs`,
  `engine/crates/rocky-cli/src/output.rs`
  (`PreviewCreateOutput`, `PreviewDiffOutput`, `PreviewCostOutput`).
