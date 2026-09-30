---
title: How Preview Works
description: How rocky preview runs only the models a PR changed, copies the rest, and diffs the result against the base ref.
sidebar:
  order: 15
---

`rocky preview` is the workflow you reach for when reviewing a PR that touches transformation models. It runs only the models the PR changed. Everything else is copied from the base ref into a per-PR branch.

It answers a reviewer's question before merge: *what does this PR change in the warehouse, and what does it cost?* It does that on a small fraction of a full run's bytes. It produces three artifacts you can attach to the PR: a comparison of the `rows_affected` the two runs recorded, a row-content diff when you ask for `--algorithm bisection`, and a cost delta against base. The column-level delta is an open follow-up, so the structural arrays come back empty.

The sampling described below is the design, not what the default path runs. The default path samples no rows. It compares the `rows_affected` the two runs recorded, and it reports `limit: 0` and `coverage: "not_yet_sampled"`. `rocky preview diff` has no `--sample-size` flag, because nothing read it.

## The prune-and-copy substrate

The workflow has four steps. `rocky preview create` performs the first three, and you run the fourth.

```
  ┌──────────────┐  git diff --name-only  ┌──────────────────┐
  │ --base ref   │───────────────────────►│ changed model    │
  │ (e.g. main)  │        vs HEAD         │ files            │
  └──────────────┘                        └────────┬─────────┘
                                                   │ scan model sidecars
                                                   │ for depends_on
                                                   ▼ model-level DAG
  ┌────────────────────────────────────────────────────────────┐
  │  every model in the working DAG lands in one of two sets   │
  ├──────────────────────────────┬─────────────────────────────┤
  │ PRUNE SET                    │ COPY SET                    │
  │ the changed models, plus     │ everything else. Logically  │
  │ every model downstream of a  │ identical to its --base     │
  │ changed model via depends_on │ counterpart                 │
  └──────────────┬───────────────┴──────────────┬──────────────┘
                 │ you run: rocky run           │ preview create runs
                 │ --branch <name>              │ clone_table_for_branch
                 ▼                              ▼
        ┌───────────────────────────────────────────────┐
        │  the PR branch's schema (its schema_prefix)   │
        └───────────────────────┬───────────────────────┘
                                ▼
            structural diff  ·  data diff  ·  cost delta
```

1. **Identify the change set.** Rocky shells out to `git diff --name-only <base_ref> HEAD` against the models directory, the same plumbing [`rocky ci-diff`](/reference/commands/modeling/#rocky-ci-diff) uses. The output is the set of model files that changed between `--base` and `HEAD`.

2. **Compute the prune set from model dependencies.** Rocky scans the working-tree model sidecars for `depends_on`. It includes every changed model and every model transitively downstream of one. The prune set does not use column lineage.

3. **Compute the copy set.** Every model in the working DAG that is not in the prune set is a copy candidate. It is logically identical to its counterpart on `--base`, so re-running it would produce the same bytes. Rocky issues `CREATE TABLE <branch_schema>.<model> AS SELECT * FROM <base_schema>.<model>` against the configured adapter, with the per-adapter overrides described below.

4. **Run the prune set yourself.** `preview create` does not run these models: it reports `run_status: "planned"` with an empty `run_id`. You run them with [`rocky run --branch <name>`](/reference/commands/core-pipeline/#rocky-run). `preview create` has already registered the branch, as [`rocky branch create`](/reference/commands/core-pipeline/#rocky-branch) does, so the run writes into its `schema_prefix` and records the branch name. That name is how `preview diff` and `preview cost` find the run.

`rocky run` has no selector for a set of models. It builds the whole pipeline, or one model with `--model`. `preview diff` and `preview cost` read only the newest branch run. So a prune set of one model can run alone. A prune set of several models needs one whole-pipeline run, which also rebuilds the copy set. Separate `--model` runs leave only the last model in the comparison.

The final output ([`PreviewCreateOutput`](#output-shapes)) records `prune_set`, `copy_set`, and `skipped_set`, so the decision is auditable from the JSON alone.

### How each warehouse copies a table

The copy step dispatches per adapter through the `WarehouseAdapter::clone_table_for_branch` trait method:

- **Databricks** — `CREATE OR REPLACE TABLE … SHALLOW CLONE …`. Metadata-only; the branch table references the source's underlying files until either side mutates.
- **BigQuery** — `CREATE OR REPLACE TABLE … COPY …`. Metadata-only; same single-project scope as the source dataset.
- **DuckDB** — `CREATE OR REPLACE TABLE … AS SELECT *` (CTAS). Bytes-copying but trivially portable; matches the trait's default impl, so the same code path works on any future adapter that doesn't override.
- **Snowflake** — `CREATE TABLE … CLONE …`. The adapter uses Snowflake's native zero-copy clone.

On Databricks, BigQuery, and Snowflake, `clone_table_for_branch` uses a metadata-only copy. DuckDB uses CTAS to copy the table data.

## How diff and cost pair the runs

`preview diff` and `preview cost` pair a branch run with a base run. They find both in run history in the state store. Each run record holds two branch fields, and only one of them finds the branch run.

| Run | `rocky_branch` | `git_branch` |
|---|---|---|
| `rocky run --branch pr_preview_fix_price`, from git branch `fix-price` | `pr_preview_fix_price` | `fix-price` |
| `rocky run`, from git branch `main` | none | `main` |

`rocky_branch` is the literal `--branch` name. `git_branch` is the git branch checked out when the run started.

- **Branch run.** The newest run whose `rocky_branch` equals `--name`. `git_branch` cannot find it: it holds `fix-price`, not the preview name. Only this one run is compared.
- **Base run for `preview diff`.** The newest run with no `rocky_branch` whose `git_branch` equals `--base`, or whose `git_commit` matches it. Rocky refuses a commit prefix that matches more than one commit. It never falls back to another run. With none found, the diff stays empty and `base_note` says why.
- **Base run for `preview cost`.** `preview cost` has no `--base` flag. It takes the newest run whose `rocky_branch` is not `--name`. It also skips a run with no `rocky_branch` whose `git_branch` is `--name`, the shape of a branch run recorded before Rocky added `rocky_branch`. Any other run can be the base: a run on another git branch, or a run made with another `--branch` name.

A run recorded before Rocky added `rocky_branch` has none, so it never matches as a branch run. Run `rocky run --branch <name>` again to record one. `rocky history --output json` shows `rocky_branch` on every run that has one.

## Comparison to Fivetran's Smart Run

The closest published commercial analogue is Fivetran's [Smart Run for dbt Core](https://www.fivetran.com/blog/how-we-execute-dbt-runs-faster-and-cheaper). Both rest on the same insight. Re-running unchanged upstream is wasted work, so copy it and run only the changed subtree.

| Property | Fivetran Smart Run (per article) | Rocky `preview` |
|---|---|---|
| Change detection | "Manifest-independent" — mechanism not specified in the article | Git diff identifies changed model files; sidecar `depends_on` links identify downstream models |
| Pruning granularity | Model-level (per the article's red / I-node / R-node example) | Model-level; a changed model pulls in all downstream models linked by `depends_on` |
| Copy substrate | `COPY` ("the COPY command is free" per article) | Per-adapter dispatch: Databricks `SHALLOW CLONE`, BigQuery `CREATE TABLE … COPY`, Snowflake `CREATE TABLE … CLONE`, DuckDB CTAS |
| Cost delta | Not surfaced in the article | First-class output ([`PreviewCostOutput`](#output-shapes)) |
| Data diff | Not surfaced in the article | First-class output ([`PreviewDiffOutput`](#output-shapes)) |
| PR comment | Not described in the article | Pre-rendered Markdown in every output |

The article does not document Smart Run's internal mechanism beyond a conceptual diagram and the "manifest-independent" claim. The rows above hedge accordingly.

## Two diff algorithms

`rocky preview diff` produces a row-level diff per model the branch run executed. It uses one of two algorithms, and a `kind` discriminator on each per-model entry says which one ran.

### `--algorithm sampled` (default)

**What runs today.** The default reads no rows. It subtracts the `rows_affected` the two runs recorded. When a run recorded no count for a model, that model's row delta is `null`, and `summary.models_unknown` counts it. The `sampling_window` block reports `limit: 0` and `coverage: "not_yet_sampled"`.

**The design.** The intended algorithm reads a window of rows from both sides:

```
ORDER BY <primary_key>     -- or first column if no PK declared
LIMIT <sample_size>        -- design default 1000; no flag sets it today
```

This is fast, deterministic, and bounded. It has one known blind spot: a row that changed outside the sampling window reads as no change. The diff layer flags that risk explicitly. Each per-model `Sampled` variant carries a `sampling_window` block. With the design in place, it reads:

```jsonc
{
  "kind": "sampled",
  "sampled": { /* per-row totals */ },
  "sampling_window": {
    "ordered_by": "order_id",
    "limit": 1000,
    "coverage": "first_n_by_order",
    "coverage_warning": true
  }
}
```

`coverage_warning: true` means a meaningful number of rows sit outside the sampling window. A clean sample does not mean "no change".

### `--algorithm bisection`

Bisection checks every row, by splitting the primary-key range and comparing checksums. It needs a single-column integer or numeric primary key. [Datafold's data-diff](https://github.com/datafold/data-diff) uses the same technique.

1. Split the primary-key range into `K` chunks (default `K=32`).
2. On both the branch and base sides, compute a per-chunk checksum: a `BIT_XOR` aggregate over a per-row hash (DuckDB `hash`, BigQuery `FARM_FINGERPRINT`, Databricks Spark `xxhash64`).
3. Compare the two sides chunk-by-chunk. Matching chunks (equal row count + equal checksum) are pruned from the search.
4. Recurse into mismatched chunks until each one falls below a leaf threshold (default `MIN_CHUNK_ROWS=1000`). At the leaf, materialize both sides and walk them in lockstep, classifying each row as added / removed / changed.
5. Bound recursion at `MAX_DEPTH=8` (covers `K^8 ≈ 10^12` rows). On hit, surface `bisection_stats.depth_capped: true`.

Two properties set this apart from sampling:

- **Bounded scan cost.** A no-op diff bottoms out at `K=32` chunk checksums per side. A single-row change recurses to that row in `O(K · log_K(N))` chunks examined. For a 1B-row table at `K=32`, that is about 128 chunk reads.
- **Exhaustive coverage.** Every row hashes into exactly one chunk. If any row differs, the chunk it lives in must mismatch, and the recursion must find it. There is no `coverage_warning` hedge.

Each per-model `Bisection` variant carries a `bisection_stats` block:

```jsonc
{
  "kind": "bisection",
  "diff": { "rows_added": 0, "rows_removed": 0, "rows_changed": 1, "samples": [...] },
  "bisection_stats": {
    "chunks_examined": 64,
    "leaves_materialized": 1,
    "depth_max": 2,
    "depth_capped": false,
    "split_strategy": "int_range",
    "null_pk_rows_base": 0,
    "null_pk_rows_branch": 0
  }
}
```

The `samples` field carries up to 5 changed rows (`DEFAULT_MAX_SAMPLES`) surfaced from the leaves.

### Which algorithm runs?

Bisection needs a single-column integer or numeric `unique_key` declared on the model's `Merge` strategy. A model without a usable primary key skips bisection and falls back to the sampled placeholder, logging the reason through `tracing::warn`. That covers composite keys, non-numeric keys, and any non-`Merge` strategy.

Planned work extends bisection two ways. Composite primary keys would use per-level `NTILE` quantile boundaries on the base side. UUID and hash-bucket primary keys would use single-level hash bucketing, with the cost bound stated up front.

### Coverage-warning roll-up

`summary.any_coverage_warning` rolls both incompleteness signals up to the run level. It fires when *any* per-model diff is `Sampled` with `sampling_window.coverage_warning: true`, *or* `Bisection` with `bisection_stats.depth_capped: true`. A reviewer sees either signal in the Markdown PR comment without scanning every model.

## Output shapes

The wire contracts for all three subcommands live in the repo as JSON Schemas exported by `rocky export-schemas`:

- `schemas/preview_create.schema.json` — [`PreviewCreateOutput`](#the-prune-and-copy-substrate)
- `schemas/preview_diff.schema.json` — `PreviewDiffOutput`, including the per-model `sampling_window` block above
- `schemas/preview_cost.schema.json` — `PreviewCostOutput`, including `summary.delta_usd` and `summary.savings_from_copy_usd`

The [codegen pipeline](/reference/json-output/) generates the Pydantic (Dagster) and TypeScript (VS Code) bindings from these schemas. For the command-line usage and the Markdown the PR comment renders, see [`rocky preview`](/reference/commands/modeling/#rocky-preview) in the CLI reference.

## Related concepts

- [The Rocky Compiler](/concepts/compiler/) — type checks models; preview builds its prune set from sidecar dependencies.
- [Shadow Mode](/concepts/shadow-mode/) — the comparison kernel `preview diff` extends with sampled row-level diffing.
- [State Management](/concepts/state-management/) — the `RunRecord` store `preview cost` reads to compute base-vs-branch deltas.
