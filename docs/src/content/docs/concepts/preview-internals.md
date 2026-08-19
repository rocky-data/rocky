---
title: How Preview Works
description: How rocky preview runs only the models a PR changed, copies the rest, and diffs the result against the base ref.
sidebar:
  order: 15
---

`rocky preview` is the workflow you reach for when reviewing a PR that touches transformation models. It runs only the models the PR changed. Everything else is copied from the base ref into a per-PR branch.

It answers a reviewer's question before merge: *what does this PR change in the warehouse, and what does it cost?* It does that on a small fraction of a full run's bytes. It produces three artifacts you can attach to the PR: a structural diff, a sampled row-level data diff, and a cost delta against base.

## The prune-and-copy substrate

`rocky preview create` chains four existing Rocky primitives into one workflow.

```
  ┌──────────────┐  git diff --name-only  ┌──────────────────┐
  │ --base ref   │───────────────────────►│ changed model    │
  │ (e.g. main)  │        vs HEAD         │ files            │
  └──────────────┘                        └────────┬─────────┘
                                                   │ load into the
                                                   │ compiler: a
                                                   ▼ column-level DAG
  ┌────────────────────────────────────────────────────────────┐
  │  every model in the working DAG lands in one of two sets   │
  ├──────────────────────────────┬─────────────────────────────┤
  │ PRUNE SET                    │ COPY SET                    │
  │ the changed models, plus     │ everything else. Logically  │
  │ every model downstream of a  │ identical to its --base     │
  │ changed COLUMN               │ counterpart                 │
  └──────────────┬───────────────┴──────────────┬──────────────┘
                 │ rocky plan --branch <name>   │ clone_table_for_
                 │ + rocky apply <plan-id>      │ branch, per adapter
                 ▼                              ▼
        ┌───────────────────────────────────────────────┐
        │  the PR branch's schema (its schema_prefix)   │
        └───────────────────────┬───────────────────────┘
                                ▼
            structural diff  ·  data diff  ·  cost delta
```

1. **Identify the change set.** Rocky shells out to `git diff --name-only <base_ref> HEAD` against the models directory, the same plumbing [`rocky ci-diff`](/reference/commands/modeling/#rocky-ci-diff) uses. The output is the set of model files that changed between `--base` and `HEAD`.

2. **Compute the prune set from the compiler IR.** Loading the working-tree models into the [compiler](/concepts/compiler/) gives a column-level dependency graph. The prune set is every changed model **plus** every model that transitively depends on a changed column. A model downstream of an *unchanged* column on a changed model is not pulled in. That makes column-level pruning strictly tighter than git-diff alone.

3. **Compute the copy set.** Every model in the working DAG that is not in the prune set is a copy candidate. It is logically identical to its counterpart on `--base`, so re-running it would produce the same bytes. Rocky issues `CREATE TABLE <branch_schema>.<model> AS SELECT * FROM <base_schema>.<model>` against the configured adapter, with the per-adapter overrides described below.

4. **Run the prune set.** Rocky calls the existing branch run path with a model selector limited to the prune set. That path is [`rocky plan --branch <name>`](/reference/commands/core-pipeline/#rocky-run) followed by `rocky apply <plan-id>`. The single-step `rocky run --branch <name>` alias does the same in one invocation. [`rocky branch create`](/reference/commands/core-pipeline/#rocky-branch) registers the branch, and the run writes into the branch's `schema_prefix`.

The final output ([`PreviewCreateOutput`](#output-shapes)) records `prune_set`, `copy_set`, and `skipped_set`, so the decision is auditable from the JSON alone.

### How each warehouse copies a table

The copy step dispatches per adapter through the `WarehouseAdapter::clone_table_for_branch` trait method:

- **Databricks** — `CREATE OR REPLACE TABLE … SHALLOW CLONE …`. Metadata-only; the branch table references the source's underlying files until either side mutates.
- **BigQuery** — `CREATE OR REPLACE TABLE … COPY …`. Metadata-only; same single-project scope as the source dataset.
- **DuckDB** — `CREATE OR REPLACE TABLE … AS SELECT *` (CTAS). Bytes-copying but trivially portable; matches the trait's default impl, so the same code path works on any future adapter that doesn't override.
- **Snowflake** — falls through to the CTAS default. Native zero-copy `CLONE TABLE` is a planned override. It switches in once a Snowflake consumer drives the integration test against a workspace.

On Databricks and BigQuery, `clone_table_for_branch` turns the copy step from a bytes-bearing CTAS into a metadata operation. That makes preview cheap enough to run on tables you could not afford to CTAS today.

## Comparison to Fivetran's Smart Run

The closest published commercial analogue is Fivetran's [Smart Run for dbt Core](https://www.fivetran.com/blog/how-we-execute-dbt-runs-faster-and-cheaper). Both rest on the same insight. Re-running unchanged upstream is wasted work, so copy it and run only the changed subtree.

| Property | Fivetran Smart Run (per article) | Rocky `preview` |
|---|---|---|
| Change detection | "Manifest-independent" — mechanism not specified in the article | git-diff plus compiler-IR type-equivalence (the compiler can tell that two textually different models produce identical column types and lineage) |
| Pruning granularity | Model-level (per the article's red / I-node / R-node example) | Column-level — derived from the compiler IR; a column added to an unused tail of a wide table prunes to zero downstream |
| Copy substrate | `COPY` ("the COPY command is free" per article) | Per-adapter dispatch: Databricks `SHALLOW CLONE`, BigQuery `CREATE TABLE … COPY` (both metadata-only), DuckDB CTAS, Snowflake CTAS pending native `CLONE` override |
| Cost delta | Not surfaced in the article | First-class output ([`PreviewCostOutput`](#output-shapes)) |
| Data diff | Not surfaced in the article | First-class output ([`PreviewDiffOutput`](#output-shapes)) |
| PR comment | Not described in the article | Pre-rendered Markdown in every output |

The article does not document Smart Run's internal mechanism beyond a conceptual diagram and the "manifest-independent" claim. The rows above hedge accordingly. Rocky's column-level pruning follows from owning the compiler that builds the graph.

## Two diff algorithms

`rocky preview diff` produces a row-level diff per model in the prune set. It uses one of two algorithms, and a `kind` discriminator on each per-model entry says which one ran.

### `--algorithm sampled` (default)

```
ORDER BY <primary_key>     -- or first column if no PK declared
LIMIT <sample_size>        -- default 1000, override with --sample-size
```

This is fast, deterministic, and bounded. It has one known blind spot: a row that changed outside the sampling window reads as no change. The diff layer flags that risk explicitly. Each per-model `Sampled` variant carries a `sampling_window` block:

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

- [The Rocky Compiler](/concepts/compiler/) — the IR `preview` queries to build the prune set.
- [Shadow Mode](/concepts/shadow-mode/) — the comparison kernel `preview diff` extends with sampled row-level diffing.
- [State Management](/concepts/state-management/) — the `RunRecord` store `preview cost` reads to compute base-vs-branch deltas.
