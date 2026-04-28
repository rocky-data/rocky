---
title: Preview a PR Before Merging
description: Use rocky preview to run only the models a PR changed, diff them against base, and surface the cost delta — locally today, in CI as a follow-up
sidebar:
  order: 8
---

`rocky preview` runs the models a PR's diff actually changes against a per-PR branch, leaves everything else untouched (copied from the base ref), and produces a structural + sampled row-level diff and a cost delta vs. base. This guide walks you through running it locally on a feature branch.

For the design — how Rocky picks the prune set, why CTAS today and clones tomorrow, how the sampling window works — see the [How Preview Works](/concepts/preview-internals/) concept page. For the full output schemas, see the [`rocky preview` CLI reference](/reference/commands/modeling/#rocky-preview).

## Prerequisites

You'll need:

- Rocky installed and on `$PATH` (the [Getting Started guide](/getting-started/introduction/) has install instructions).
- A repo with a `rocky.toml` and a `models/` directory.
- A git working tree on a feature branch with at least one model change vs. the base ref.
- The base schema's tables already materialized — `preview create` copies them into the per-PR branch schema, so they need to exist. Running `rocky run` once on `main` is enough.

The walkthrough below uses `--base main`, but any git ref works.

## Step 1: Create the preview branch

```bash
rocky preview create --base main
```

What this does:

1. Runs `git diff --name-only main HEAD` against the models directory to find changed model files.
2. Loads the working tree into the compiler and computes the **prune set** — every changed model plus everything that depends on a changed column.
3. Computes the **copy set** — every working-DAG model not in the prune set.
4. Registers a branch in the state store (mirrors `rocky branch create`).
5. Issues `CREATE TABLE <branch_schema>.<model> AS SELECT * FROM <base_schema>.<model>` for each copy-set model.
6. Calls `rocky run --branch <name>` with a model selector limited to the prune set.

The output is a `PreviewCreateOutput` JSON document:

```json
{
  "version": "1.18.0",
  "command": "preview-create",
  "branch_name": "preview-fix-price",
  "branch_schema": "branch__preview-fix-price",
  "base_ref": "main",
  "head_ref": "HEAD",
  "prune_set": [
    { "model_name": "fct_revenue", "reason": "changed", "changed_columns": ["amount_cents"] },
    { "model_name": "rev_by_region", "reason": "downstream_of_changed" }
  ],
  "copy_set": [
    { "model_name": "stg_orders",    "source_schema": "main", "target_schema": "branch__preview-fix-price", "copy_strategy": "ctas" },
    { "model_name": "stg_customers", "source_schema": "main", "target_schema": "branch__preview-fix-price", "copy_strategy": "ctas" }
  ],
  "skipped_set": [],
  "run_id": "run-20260428-141033-002",
  "run_status": "succeeded",
  "duration_ms": 4321
}
```

The `run_id` is the handle the next two commands use to look up cost telemetry.

## Step 2: Diff the branch against base

```bash
rocky preview diff --name preview-fix-price --output markdown
```

This combines two layers into one report:

- **Structural diff** — column-level added/removed/type-changed, the same shape `rocky ci-diff` produces.
- **Sampled row-level diff** — for each prune-set model, sample N rows (default 1000, override with `--sample-size`) ordered by the model's primary key, hash each row, and report added / removed / changed counts plus a small set of representative changed-row samples.

`--output markdown` writes a PR-comment-ready snippet to stdout. Use `--output json` for the full `PreviewDiffOutput` shape (the JSON output also includes the rendered Markdown in a top-level `markdown` field, so you can pipe it through `jq -r .markdown` for the same effect).

## Step 3: Compare cost vs. base

```bash
rocky preview cost --name preview-fix-price --output markdown
```

This is a diff layer over [`rocky cost latest`](/reference/commands/administration/#rocky-cost). For each model in the prune set, Rocky looks up the latest base-schema `RunRecord` from the state store and the branch run's `RunRecord`, then subtracts the per-model duration, bytes scanned, and USD cost.

The summary fields tell you:

- `delta_usd` — total branch cost minus base cost. Positive means the PR will cost more to run on `main` after merge.
- `savings_from_copy_usd` — what the preview itself saved by copying instead of re-running. This is the empirical evidence that the prune-and-copy substrate is doing work.
- `models_skipped_via_copy` — count of models that didn't run on the branch because they were copy-set.

## What the prune set means

The prune set is the set of models that re-execute against the branch. Two reasons can put a model in the prune set:

- `reason: "changed"` — the model file itself changed in the diff (`changed_columns` lists which columns).
- `reason: "downstream_of_changed"` — the model didn't change but transitively depends on a column that did.

Models in neither bucket are either in the **copy set** (logically identical to base — they get CTAS'd over) or the **skipped set** (the column-level pruner determined they're unaffected and not depended on). The skipped set is the empty-cost residue: nothing copies them, nothing runs them.

If the prune set is empty, your PR doesn't change any model output (e.g. a whitespace-only edit). The branch run is a no-op and `preview cost` reports a zero delta.

## What `coverage_warning: true` means

`preview diff` samples rows in a fixed window (`ORDER BY <pk> LIMIT N`), so it can miss changes that fall outside that window. When the row count outside the window is non-trivial, the per-model diff sets:

```json
"sampling_window": {
  "ordered_by": "<column>",
  "limit": <N>,
  "coverage": "first_n_by_order",
  "coverage_warning": true
}
```

The aggregate `summary.any_coverage_warning` lifts the flag to the run level so it's visible in the Markdown PR comment.

When you see this flag, your options are:

- Re-run with a larger `--sample-size` if a bounded sample of N more rows is enough confidence for this PR.
- Inspect the changed columns directly via `rocky compile --model <name>` and reason about the change manually.
- Wait for the Phase 2.5 checksum-bisection exhaustive diff (planned) — it covers the whole table at bounded scan cost.

A clean sample with `coverage_warning: true` is **not** evidence the PR is no-op for that model.

## Troubleshooting

**`base ref not found`.** `rocky preview create --base <ref>` requires the ref to exist locally. Run `git fetch origin <ref>` first if you're working against a remote-only ref like `origin/main`.

**`preview cost` reports `null` deltas.** Cost requires a prior `RunRecord` for each compared model on the base schema. If the base schema has never been run end-to-end, `base_run_id` is `null` and per-model `delta_usd` falls back to `null`. Run `rocky run` once on `main` to populate the state store, then re-run `preview cost`.

**`preview cost` reports `null` for the branch.** The cost rollup uses the same adapter telemetry as [`rocky cost`](/reference/commands/administration/#rocky-cost). DuckDB and unconfigured adapters report `null` USD by design; duration and bytes still surface. Configure `[cost]` in `rocky.toml` to get dollar amounts on Databricks / Snowflake.

**Copy step is slow.** Phase 1 uses CTAS, which physically copies bytes for every copy-set table. On large tables this is the dominant cost of `preview create`. Warehouse-native clones (`SHALLOW CLONE` on Databricks, zero-copy `CLONE` on Snowflake) lift this to a metadata operation; they're a planned follow-up but not yet shipped.

**The diff finds no changes but the model definitely changed.** Check `summary.any_coverage_warning` in the JSON output. If it's `true`, the sampling window missed the changed rows — see the section above.

## Posting to a PR

For now, `rocky preview` is a local CLI workflow. You run the three commands, copy the Markdown into your PR description by hand (or pipe it via your shell), and the reviewer sees the same artifacts you do.

A composite GitHub Action that runs all three and upserts a single PR comment is on the roadmap as a follow-up. When it lands, it will live at `.github/actions/rocky-preview/` and be drop-in for any repo. Until then, the [CI/CD Integration guide](/guides/ci-cd/) covers the existing patterns for posting Rocky output to PR comments — `rocky preview` plugs into the same `gh pr comment --body-file` pattern.
