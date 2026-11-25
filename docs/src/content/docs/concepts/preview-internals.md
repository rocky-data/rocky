---
title: How Preview Works
description: How rocky preview runs only the models a PR changed, copies the rest, and diffs the result against the base ref.
sidebar:
  order: 15
---

`rocky preview` reviews a PR that touches transformation models. It runs only the changed models and copies everything else from the base ref into a per-PR branch. It answers: *what does this PR change in the warehouse, and what does it cost?* Task steps: [Preview a PR](/guides/preview-a-pr/).

It produces a comparison of the `rows_affected` the two runs recorded, a row-content diff with `--algorithm bisection`, and a cost delta against base. The column-level delta is an open follow-up, so the structural arrays come back empty.

## The prune-and-copy substrate

The workflow has four steps. `rocky preview create` performs the first three, and you run the fourth.

```
  --base ref ──git diff──▶ changed models ──depends_on──▶ PRUNE SET: changed + all downstream
                                                          COPY SET:  everything else
  PRUNE SET ── you run: rocky run --branch <name> ──┐
  COPY SET  ── preview create: clone_table_for_branch ─┴▶ PR branch schema (its schema_prefix)
                                                           ──▶ structural diff · data diff · cost delta
```

1. **Identify the change set.** `git diff --name-only <base_ref> HEAD` against the models directory, as [`rocky ci-diff`](/reference/commands/modeling/#rocky-ci-diff) does.
2. **Compute the prune set.** Every changed model and every model transitively downstream of one, from the sidecar `depends_on`. It does not use column lineage.
3. **Compute the copy set.** Every other model in the working DAG. It is logically identical to its `--base` counterpart. Rocky issues `CREATE TABLE <branch_schema>.<model> AS SELECT * FROM <base_schema>.<model>`, with the per-adapter overrides below.
4. **Run the prune set yourself.** `preview create` reports `run_status: "planned"` with an empty `run_id`. Run [`rocky run --branch <name>`](/reference/commands/core-pipeline/#rocky-run). `preview create` has already registered the branch, as [`rocky branch create`](/reference/commands/core-pipeline/#rocky-branch) does, so the run writes into its `schema_prefix` and records the branch name that `preview diff` and `preview cost` use to find it.

`rocky run` has no selector for a set of models: it builds the whole pipeline, or one model with `--model`. `preview diff` and `preview cost` read only the newest branch run. A one-model prune set can run alone. A larger one needs one whole-pipeline run, which also rebuilds the copy set. Separate `--model` runs leave only the last model in the comparison.

`PreviewCreateOutput` records `prune_set`, `copy_set` and `skipped_set`, so the decision is auditable from the JSON alone.

### How each warehouse copies a table

The copy step dispatches per adapter through `WarehouseAdapter::clone_table_for_branch`:

| Adapter | Statement | Cost |
|---|---|---|
| Databricks | `CREATE OR REPLACE TABLE … SHALLOW CLONE …` | Metadata only. The branch table references the source's files until either side mutates. |
| BigQuery | `CREATE OR REPLACE TABLE … COPY …` | Metadata only. Same single-project scope as the source dataset. |
| Snowflake | `CREATE TABLE … CLONE …` | Native zero-copy clone |
| DuckDB (and the trait default) | `CREATE OR REPLACE TABLE … AS SELECT *` (CTAS) | Copies bytes |

### Where the branch schema is created

Databricks, Snowflake, BigQuery, and Spark write each copy into the model's own catalog. For those warehouses, `rocky preview create` creates the branch schema once per distinct model catalog, for the copied and the changed models, before any copy. Your role needs `CREATE SCHEMA` in every one of those catalogs. If one catalog refuses, only its models report `copy_strategy: "failed"`, and the rest still copy. If the refusing catalog holds only changed models, the JSON output shows nothing. Rocky logs a warning, and `rocky run --branch` fails for those models.

Other warehouses (DuckDB, Postgres, Trino, ClickHouse, SQL Server) ignore the catalog, so Rocky creates the schema once in the connection's default catalog.

## How diff and cost pair the runs

`preview diff` and `preview cost` pair a branch run with a base run from run history in the state store. Each run record holds two branch fields. Only `rocky_branch` finds the branch run.

| Run | `rocky_branch` | `git_branch` |
|---|---|---|
| `rocky run --branch pr_preview_fix_price`, from git branch `fix-price` | `pr_preview_fix_price` | `fix-price` |
| `rocky run`, from git branch `main` | none | `main` |

`rocky_branch` is the literal `--branch` name. `git_branch` is the git branch checked out when the run started.

| Run to find | Rule |
|---|---|
| Branch run | The newest run whose `rocky_branch` equals `--name`. Only this run is compared. |
| Base run for `preview diff` | The newest run with no `rocky_branch` whose `git_branch` equals `--base`, or whose `git_commit` matches it. A commit prefix that matches more than one commit is refused. There is no fallback to another run. With none found, the diff stays empty and `base_note` says why. |
| Base run for `preview cost` | No `--base` flag. The newest run whose `rocky_branch` is not `--name`. It also skips a run with no `rocky_branch` whose `git_branch` is `--name` (a branch run recorded before Rocky added `rocky_branch`). Any other run can be the base: another git branch, or another `--branch` name. |

A run recorded before Rocky added `rocky_branch` never matches as a branch run. Run `rocky run --branch <name>` again. `rocky history --output json` shows `rocky_branch` on every run that has one.

## Two diff algorithms

`rocky preview diff` produces a row-level diff per model the branch run executed. It uses one of two algorithms, and a `kind` discriminator on each per-model entry says which one ran.

### `--algorithm sampled` (default)

The default reads no rows. It subtracts the `rows_affected` the two runs recorded. When a run recorded no count for a model, the delta is `null` and `summary.models_unknown` counts it. Each per-model entry carries `sampling_window` with `limit: 0` and `coverage: "not_yet_sampled"`, and `coverage_warning: true` means rows may sit outside the window. A clean sample does not mean "no change". `rocky preview diff` has no `--sample-size` flag.

### `--algorithm bisection`

Bisection checks every row, by splitting the primary-key range and comparing checksums. It needs a single-column integer or numeric primary key. [Datafold's data-diff](https://github.com/datafold/data-diff) uses the same technique.

1. Split the primary-key range into `K` chunks (default `K=32`).
2. On both the branch and base sides, compute a per-chunk checksum: the row count plus a `BIT_XOR` aggregate over a per-row hash (DuckDB `hash`, BigQuery `FARM_FINGERPRINT`, Databricks Spark `xxhash64`, Snowflake `HASH`). The row hash covers the primary key and every value column.
3. Compare the two sides chunk-by-chunk. Matching chunks (equal row count + equal checksum) are pruned from the search.
4. Recurse into mismatched chunks until each one falls below a leaf threshold (default `MIN_CHUNK_ROWS=1000`). At the leaf, materialize both sides and walk them in lockstep, classifying each row as added / removed / changed.
5. Bound recursion at `MAX_DEPTH=8` (covers `K^8 ≈ 10^12` rows). On hit, surface `bisection_stats.depth_capped: true`.

Compared with sampling:

- **Bounded scan cost.** A no-op diff bottoms out at `K=32` chunk checksums per side. A single-row change examines `O(K · log_K(N))` chunks: about 128 chunk reads for a 1B-row table.
- **Whole-table coverage.** Every row with a non-NULL key hashes into one chunk, so a changed, added or removed row changes its chunk's count or checksum. There is no `coverage_warning` hedge.

Rows with a NULL key belong to no chunk. Bisection compares them as a separate group: the row count, then the sorted list of row hashes. When the groups differ, it diffs the NULL-key rows by value. It reports each surplus row as added or removed, with the sample key `NULL`. A NULL-key row cannot be "changed", because it has no key to match on.

Bisection is a checksum, not a proof. It can miss a change in three cases:

- **Hash collision.** The row hashes are 64-bit and non-cryptographic. A change can, rarely, produce the same chunk checksum.
- **Duplicate keys.** Two rows with the same key and the same values have the same hash, and `BIT_XOR` cancels them in pairs. Bisection runs only on Merge models with a single-column `unique_key`, where keys are expected to be unique.
- **Linear multi-column hashes.** If a warehouse combines per-column hashes linearly, `BIT_XOR` can cancel a change across rows. DuckDB does this, so Rocky hashes the DuckDB row as one serialized string. That string form does not tell `1` from `'1'`. Snowflake's multi-column `HASH` has not been tested for this property. Treat a Snowflake "no change" result with care until it is.

A table with more than 100,000 NULL-key rows on one side stops the bisection with an error, because those rows are compared in memory.

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

### Coverage-warning roll-up

`summary.any_coverage_warning` fires when any per-model diff is `Sampled` with `sampling_window.coverage_warning: true`, or `Bisection` with `bisection_stats.depth_capped: true`. The Markdown PR comment shows it without scanning every model.

## Output shapes

Schemas exported by `rocky export-schemas`: `schemas/preview_create.schema.json` (`PreviewCreateOutput`), `schemas/preview_diff.schema.json` (`PreviewDiffOutput`, with the per-model `sampling_window` block), `schemas/preview_cost.schema.json` (`PreviewCostOutput`, with `summary.delta_usd` and `summary.savings_from_copy_usd`). The [codegen pipeline](/reference/json-output/) generates the Pydantic and TypeScript bindings from them. Usage and the PR comment are in [`rocky preview`](/reference/commands/modeling/#rocky-preview).

## Related concepts

- [Shadow Mode](/concepts/shadow-mode/) — the comparison kernel `preview diff` builds on.
- [State Management](/concepts/state-management/) — the `RunRecord` store `preview cost` reads to compute base-vs-branch deltas.
