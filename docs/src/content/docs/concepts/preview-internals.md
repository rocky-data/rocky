---
title: How Preview Works
description: The prune-and-copy substrate behind rocky preview, the comparison to Fivetran's Smart Run, and the sampling correctness ceiling
sidebar:
  order: 15
---

`rocky preview` is the workflow you reach for when reviewing a PR that touches transformation models. It runs only the models the PR's diff actually changed, against a per-PR branch pre-populated from the base ref, and produces three artifacts you can attach to the PR: a structural diff, a sampled row-level data diff, and a cost delta vs. base. The point is to make a reviewer's question — *"what does this PR change in the warehouse, and what does it cost?"* — answerable before merge, on a small fraction of a full run's bytes.

## The prune-and-copy substrate

`rocky preview create` orchestrates four existing Rocky primitives into a single workflow:

1. **Identify the change set.** Rocky shells out to `git diff --name-only <base_ref> HEAD` against the models directory — the same plumbing [`rocky ci-diff`](/reference/commands/modeling/#rocky-ci-diff) uses. Output: the set of model files that changed between `--base` and `HEAD`.

2. **Compute the prune set from the compiler IR.** Loading the working-tree models into the [compiler](/concepts/compiler/) gives a column-level dependency graph. The prune set is every changed model **plus** every model that transitively depends on a changed column. Models downstream of an *unchanged* column on a changed model are not pulled in — column-level pruning is strictly tighter than git-diff alone.

3. **Compute the copy set.** Every working-DAG model not in the prune set is a copy candidate: it's logically identical to its counterpart on `--base`, so re-running it would produce the same bytes. For each copy-set model, Rocky issues `CREATE TABLE <branch_schema>.<model> AS SELECT * FROM <base_schema>.<model>` against the configured adapter — the portable copy substrate, with per-adapter overrides described below.

4. **Run the prune set.** Rocky calls the existing branch run path ([`rocky run --branch <name>`](/reference/commands/core-pipeline/#rocky-run)) with a model selector limited to the prune set. The branch is registered via [`rocky branch create`](/reference/commands/core-pipeline/#rocky-branch); the run writes into the branch's `schema_prefix`.

The final output ([`PreviewCreateOutput`](#output-shapes)) records `prune_set`, `copy_set`, and `skipped_set` so the decision is auditable from the JSON alone.

### Copy substrate

The copy substrate dispatches per-adapter via the `WarehouseAdapter::clone_table_for_branch` trait method:

- **Databricks** — `CREATE OR REPLACE TABLE … SHALLOW CLONE …`. Metadata-only; the branch table references the source's underlying files until either side mutates.
- **BigQuery** — `CREATE OR REPLACE TABLE … COPY …`. Metadata-only; same single-project scope as the source dataset.
- **DuckDB** — `CREATE OR REPLACE TABLE … AS SELECT *` (CTAS). Bytes-copying but trivially portable; matches the trait's default impl, so the same code path works on any future adapter that doesn't override.
- **Snowflake** — falls through to the CTAS default. Native zero-copy `CLONE TABLE` is a planned override; it'll switch in once a Snowflake consumer drives the integration test against a workspace.

For the warehouses with native overrides (Databricks, BigQuery), `clone_table_for_branch` lifts the copy step from bytes-bearing CTAS to a metadata operation, which makes preview cheap enough to run on tables that would be uneconomic to CTAS today.

## Comparison to Fivetran's Smart Run

The closest published commercial analogue is Fivetran's [Smart Run for dbt Core](https://www.fivetran.com/blog/how-we-execute-dbt-runs-faster-and-cheaper). Both approaches rest on the same insight: re-running unchanged upstream is wasted work; copy it instead and run only the changed subtree.

| Property | Fivetran Smart Run (per article) | Rocky `preview` |
|---|---|---|
| Change detection | "Manifest-independent" — mechanism not specified in the article | git-diff plus compiler-IR type-equivalence (the compiler can tell that two textually different models produce identical column types and lineage) |
| Pruning granularity | Model-level (per the article's red / I-node / R-node example) | Column-level — derived from the compiler IR; a column added to an unused tail of a wide table prunes to zero downstream |
| Copy substrate | `COPY` ("the COPY command is free" per article) | Per-adapter dispatch: Databricks `SHALLOW CLONE`, BigQuery `CREATE TABLE … COPY` (both metadata-only), DuckDB CTAS, Snowflake CTAS pending native `CLONE` override |
| Cost delta | Not surfaced in the article | First-class output ([`PreviewCostOutput`](#output-shapes)) |
| Data diff | Not surfaced in the article | First-class output ([`PreviewDiffOutput`](#output-shapes)) |
| PR comment | Not described in the article | Pre-rendered Markdown in every output |

The article does not document Smart Run's internal mechanism beyond the conceptual diagram and the "manifest-independent" claim, so the rows above hedge accordingly. The structural advantages — column-level pruning, compile-time type-equivalence detection, warehouse-native clones — are reachable because Rocky has its own compiler. They are unreachable from inside dbt without rewriting dbt's compiler.

## Two diff algorithms

`rocky preview diff` produces a row-level diff per model in the prune set using one of two algorithms; the active algorithm is encoded by a `kind` discriminator on each per-model entry.

### `--algorithm sampled` (default)

```
ORDER BY <primary_key>     -- or first column if no PK declared
LIMIT <sample_size>        -- default 1000, override with --sample-size
```

Fast, deterministic, bounded — but with a known false-negative mode: a row that changed outside the sampling window appears as no-change. The diff layer flags this risk explicitly. Each per-model `Sampled` variant carries a `sampling_window` block:

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

`coverage_warning: true` means the row count outside the sampling window is non-trivial — a clean sample does not imply "no change".

### `--algorithm bisection`

Exhaustive checksum-bisection over a single-column integer / numeric primary key. The technique is what [datafold's data-diff](https://github.com/datafold/data-diff) uses:

1. Split the primary-key range into `K` chunks (default `K=32`).
2. On both the branch and base sides, compute a per-chunk checksum: a `BIT_XOR` aggregate over a per-row hash (DuckDB `hash`, BigQuery `FARM_FINGERPRINT`, Databricks Spark `xxhash64`).
3. Compare the two sides chunk-by-chunk. Matching chunks (equal row count + equal checksum) are pruned from the search.
4. Recurse into mismatched chunks until each chunk falls below a leaf threshold (default `MIN_CHUNK_ROWS=1000`); at the leaf, materialize both sides and walk them in lockstep, classifying each row as added / removed / changed.
5. Bound recursion at `MAX_DEPTH=8` (covers `K^8 ≈ 10^12` rows). On hit, surface `bisection_stats.depth_capped: true`.

Two properties make this a step-change over sampling:

- **Bounded scan cost.** A no-op diff bottoms out at `K=32` chunk checksums per side. A single-row change recurses to the row in `O(K · log_K(N))` chunks examined — for a 1B-row table at `K=32`, that's ~128 chunk reads.
- **Exhaustive coverage.** Every row hashes into exactly one chunk; if any row differs, the chunk it lives in is guaranteed to mismatch and the recursion is guaranteed to find it. No `coverage_warning` hedge.

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

The `samples` field carries up to `--max-samples` (default 5) representative changed rows surfaced from the leaves.

### Which algorithm runs?

Bisection requires a single-column integer / numeric `unique_key` declared on the model's `Merge` strategy. Models without a usable PK (composite key, non-numeric PK, non-Merge strategy) skip bisection with a `tracing::warn` reason and fall back to the sampled placeholder. Future work extends bisection to composite primary keys (per-level `NTILE` quantile boundaries on the base side) and UUID / hash-bucket primary keys (single-level hash bucketing with explicit cost-bound disclosure).

### Coverage-warning roll-up

The aggregate `summary.any_coverage_warning` rolls both signals up to the run level: it fires when *any* per-model diff is `Sampled` with `sampling_window.coverage_warning: true` *or* `Bisection` with `bisection_stats.depth_capped: true`. A reviewer can spot either incompleteness signal in the Markdown PR comment without scanning every model.

## Output shapes

The wire contracts for all three subcommands live in the repo as JSON Schemas exported by `rocky export-schemas`:

- `schemas/preview_create.schema.json` — [`PreviewCreateOutput`](#the-prune-and-copy-substrate)
- `schemas/preview_diff.schema.json` — `PreviewDiffOutput`, including the per-model `sampling_window` block above
- `schemas/preview_cost.schema.json` — `PreviewCostOutput`, including `summary.delta_usd` and `summary.savings_from_copy_usd`

The schemas back the autogenerated Pydantic (Dagster) and TypeScript (VS Code) bindings via the [codegen pipeline](/reference/json-output/). For human-readable command-line usage and the Markdown the PR comment renders, see [`rocky preview`](/reference/commands/modeling/#rocky-preview) in the CLI reference.

## Related concepts

- [The Rocky Compiler](/concepts/compiler/) — the IR `preview` queries to build the prune set.
- [Shadow Mode](/concepts/shadow-mode/) — the comparison kernel `preview diff` extends with sampled row-level diffing.
- [State Management](/concepts/state-management/) — the `RunRecord` store `preview cost` reads to compute base-vs-branch deltas.
