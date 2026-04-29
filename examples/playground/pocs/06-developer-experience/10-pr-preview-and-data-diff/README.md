# 10-pr-preview-and-data-diff — `rocky preview` PR-bundle

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 30s
> **Rocky features:** `rocky preview create`, `rocky preview diff`, `rocky preview cost`

## What it shows

The end-to-end `rocky preview` workflow that turns a PR into a single
trust-system surface — *"this PR will change these N rows / these M
columns, costs $X more or less to run, here's the diff"* — on a 5-model
DuckDB transformation pipeline:

1. **`rocky preview create --base <ref>`** — git-diff identifies changed
   model files between `<ref>` and HEAD; the compiler IR computes a
   **column-level prune set**; every model *not* in the prune set is
   copied from the base schema via CTAS (Phase 1) into a per-PR branch
   schema; the prune set re-executes against the branch.
2. **`rocky preview diff`** — structural (column added/removed/type
   changed) plus sampled row-level diff between branch and base for
   every model in the prune set, with a `sampling_window` that
   surfaces the false-negative ceiling verbatim.
3. **`rocky preview cost`** — per-model bytes / duration / USD delta
   versus the latest base-schema run. Copied models contribute to
   `savings_from_copy_usd`; only re-run models contribute to
   `delta_usd`.

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
- **Branches are the substrate.** Today schema-prefix branches; tomorrow
  Delta `SHALLOW CLONE` / Snowflake zero-copy `CLONE` slot into the same
  API at Phase 5 (strict dominance over `COPY` once that lands).
- **Compile-time change detection > git-diff alone.** `rocky ci-diff`
  already does git-diff between refs and produces a structural diff;
  the next step — *"this textual change is type-equivalent and produces
  no output diff"* — is something only a compiled engine can do.
- **Cost delta as a state-store query, not a fresh measurement.**
  `rocky cost latest` already rolls per-run cost from adapter telemetry;
  Phase 3 is the diff layer over that machinery.
- **Single artefact for the reviewer.** Phase 4 stitches all three
  outputs into one PR comment that answers *"should I merge this?"*
  with row counts, columns, and dollars — not log lines.

Compare:

| Property | Fivetran Smart Run | `rocky preview` |
|---|---|---|
| Pruning granularity | Model-level | Column-level (compiler IR) |
| Copy substrate (Phase 1) | `COPY` | CTAS / `COPY` |
| Copy substrate (Phase 5) | — | `SHALLOW CLONE` / zero-copy `CLONE` |
| Cost delta | Not surfaced | First-class output |
| Data diff | Not surfaced | First-class output |
| PR comment | Not described | First-class output (Phase 4) |

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
    ├── preview_create.example.json    target shape (Phase 1 not yet wired)
    ├── preview_diff.example.json      target shape (Phase 2 not yet wired)
    └── preview_cost.example.json      target shape (Phase 3 not yet wired)
```

## Note on model surfaces

The 5-model DAG is all SQL. Raw SQL stays first-class in Rocky — the
preview workflow's prune set, copy set, and data diff are surface-agnostic
(they care about a model's compiled output, not which DSL produced it).
A `.rocky` DSL variant of this POC will be added once cross-model
references in transformation pipelines round-trip cleanly through `rocky
run` for both DSL and SQL surfaces.

## Status

`rocky preview {create,diff,cost}` are scaffolded today; their
implementations land in Phases 1–3 of the
[`rocky-pr-preview-and-data-diff`](../../../../../) plan. This POC ships
the integration-test baseline:

- `run.sh` exits **0** today by capturing each preview-stub error and
  surfacing it without sinking the run.
- The `expected/preview_*.example.json` files are the **aspirational
  target shapes** the Phase 1+ implementation must match — values are
  illustrative, the keys / types / reason strings are the contract.
- When Phase 1 lands, the stub `.err` sidecars disappear and the JSON
  files in `expected/` become real golden fixtures.

## Prerequisites

- `rocky` ≥ 1.18.0 on PATH
- `duckdb` CLI for seeding (`brew install duckdb`)

## How to run

```bash
cd examples/playground/pocs/06-developer-experience/10-pr-preview-and-data-diff
./run.sh
```

`run.sh`:

1. Cleans `.rocky-state.redb` and `poc.duckdb`.
2. Runs `rocky compile` against the 5-model DAG.
3. Seeds the raw tables into DuckDB.
4. Runs the pipeline on `main` state.
5. Captures the current git HEAD as the `--base` ref (or a sentinel
   string when not in a git checkout).
6. Swaps `models/fct_revenue.rocky` for `fct_revenue.rocky.changed` —
   adds a `where amount > 25` filter that should produce a real
   row-level diff.
7. Runs `rocky preview create --base <ref> --name pr-preview-poc-10`.
   Today: bails with a Phase 1 stub message; `run.sh` keeps going.
8. Runs `rocky preview diff` against the branch. Same.
9. Runs `rocky preview cost` against the branch. Same.
10. Reverts the synthetic change (`trap`-protected, idempotent).

## Related

- Sibling POC: [`00-foundations/06-branches-replay-lineage/`](../../00-foundations/06-branches-replay-lineage/)
  — the four trust-arc primitives (branches, replay, column lineage,
  state store) that `preview` composes.
- Sibling POC: [`06-developer-experience/04-shadow-mode-compare/`](../04-shadow-mode-compare/)
  — the precursor `rocky compare` kernel that Phase 2's `preview diff`
  extends.
- Engine source: `engine/crates/rocky-cli/src/commands/preview.rs`
  (Phase 0 stubs), `engine/crates/rocky-cli/src/output.rs`
  (`PreviewCreateOutput`, `PreviewDiffOutput`, `PreviewCostOutput`).
