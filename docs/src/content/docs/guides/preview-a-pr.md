---
title: Preview a PR Before Merging
description: Use rocky preview to run only the models a PR changed, diff them against base, and see the cost difference. Locally today, in CI as a follow-up.
sidebar:
  order: 8
---

`rocky preview` runs only the models a PR's diff actually changes. It runs them against a per-PR branch schema and copies every other model from the base ref, rather than rebuilding it. It then reports a structural and row-level diff, plus the cost difference against base. This guide walks you through running it locally on a feature branch.

For the design, see the [How Preview Works](/concepts/preview-internals/) concept page: how Rocky picks the prune set, why CTAS today and clones tomorrow, and how the sampling window works. For the full output schemas, see the [`rocky preview` CLI reference](/reference/commands/modeling/#rocky-preview).

Preview surfaces the data and cost shape of a PR. It does not detect breaking schema changes. For that, pair it with [`rocky ci-diff --semantic`](/reference/commands/modeling/#rocky-ci-diff) on the same PR. A hard semantic gate then fires when the branch is promoted through `rocky plan promote` and `rocky apply` (or the legacy `rocky branch promote` alias). The [CI/CD integration guide](/guides/ci-cd/#semantic-breaking-change-findings-and-the-promote-gate) documents the full flow: PR-time detection, then the promote-time gate, then an audited override.

## Prerequisites

You need:

- Rocky installed and on `$PATH` (the [Getting Started guide](/getting-started/introduction/) has install instructions).
- A repo with a `rocky.toml` and a `models/` directory.
- A git working tree on a feature branch, with at least one model change against the base ref.
- The base schema's tables already materialized. `preview create` copies them into the per-PR branch schema, so they have to exist. Running `rocky plan` and `rocky apply` once on `main` is enough.

The walkthrough below uses `--base main`, but any git ref works.

## Step 1: Create the preview branch

```bash
rocky preview create --base main
```

This does five things:

1. Runs `git diff --name-only main HEAD` against the models directory to find changed model files.
2. Scans the models directory into a DAG and computes the **prune set**: every changed model, plus everything transitively downstream of it via each model's `depends_on`.
3. Computes the **copy set**: every working-DAG model not in the prune set.
4. Registers a branch in the state store, mirroring `rocky branch create`.
5. Issues `CREATE TABLE <branch_schema>.<model> AS SELECT * FROM <base_schema>.<model>` for each copy-set model.

`preview create` does **not** run the prune-set models itself. It emits `run_status: "planned"` with an empty `run_id`. Run `rocky run --branch <name>`, with a selector limited to the prune set, before `preview diff` or `preview cost`. That gives them a branch run to compare against.

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
    { "model_name": "fct_revenue", "reason": "changed", "changed_columns": [] },
    { "model_name": "rev_by_region", "reason": "downstream_of_changed", "changed_columns": [] }
  ],
  "copy_set": [
    { "model_name": "stg_orders",    "source_schema": "main", "target_schema": "branch__preview-fix-price", "copy_strategy": "ctas" },
    { "model_name": "stg_customers", "source_schema": "main", "target_schema": "branch__preview-fix-price", "copy_strategy": "ctas" }
  ],
  "skipped_set": [],
  "run_id": "",
  "run_status": "planned",
  "duration_ms": 4321
}
```

`run_id` comes back empty and `run_status` is `"planned"`, because `preview create` only registers the branch and copies the base tables. `preview diff` and `preview cost` do not key off this `run_id` anyway: they pair the latest run tagged to the branch name against base, over run history. So run `rocky run --branch preview-fix-price` before either one.

## Step 2: Diff the branch against base

```bash
rocky preview diff --name preview-fix-price --output json | jq -r .markdown
```

This combines two layers into one report.

**Structural diff.** One `structural` block per model, with `added_columns`, `removed_columns`, and `type_changes`. These arrays are placeholders today: the `RunRecord` does not persist column lists, so they are always empty on the wire. Typed schema-level detection lives in `rocky ci-diff`.

**Row-level diff.** A per-model row delta, produced by one of two algorithms. A discriminator on the JSON output tells you which one ran.

- `kind: "sampled"` (default): a row-count and bytes delta computed off the two `RunRecord`s. It does not read row content yet, so it always reports `coverage: "not_yet_sampled"` and `coverage_warning: true`, and `--sample-size` is currently ignored. A change that does not shift row counts will not surface here.
- `kind: "bisection"`: exhaustive checksum-bisection over a single-column integer or numeric `unique_key`. It walks the chunk lattice, recurses into mismatched chunks, and surfaces every row-level diff. See the [How Preview Works](/concepts/preview-internals/) page for the algorithm. It runs only on Merge-strategy models with a single integer PK; other models stay on sampled, and the skip reason is logged via `tracing::warn`.

The full `PreviewDiffOutput` shape (`--output json`) carries the rendered PR-comment-ready snippet in a top-level `markdown` field. Pipe it through `jq -r .markdown` to print just that snippet to stdout. There is no `--output markdown` mode: the valid values are `json`, `table`, and `md`, and `md` only logs a one-line status. The Markdown always lives in the JSON `markdown` field.

### Choosing the algorithm

```bash
# Default — sampled (fast, may miss out-of-window changes)
rocky preview diff --name preview-fix-price

# Exhaustive — checksum-bisection (covers the whole table)
rocky preview diff --name preview-fix-price --algorithm bisection
```

`--algorithm` is currently hidden from `rocky preview diff --help`, but it is accepted and stable.

Per-model output uses a tagged `algorithm` discriminator:

```jsonc
"models": [
  {
    "model_name": "fct_revenue",
    "structural": { /* ... */ },
    "algorithm": {
      "kind": "bisection",
      "diff": { "rows_added": 0, "rows_removed": 0, "rows_changed": 1, "samples": [...] },
      "bisection_stats": {
        "chunks_examined": 64, "leaves_materialized": 1,
        "depth_max": 2, "depth_capped": false,
        "split_strategy": "int_range",
        "null_pk_rows_base": 0, "null_pk_rows_branch": 0
      }
    }
  }
]
```

A direct JSON consumer should read `model.algorithm.kind` first, then unpack the matching variant. The Dagster typed-resource layer absorbs this automatically.

## Step 3: Compare cost vs. base

```bash
rocky preview cost --name preview-fix-price --output json | jq -r .markdown
```

This is a diff layer over [`rocky cost latest`](/reference/commands/administration/#rocky-cost). For each model in the prune set, Rocky looks up two `RunRecord`s from the state store: the latest one on the base schema, and the branch run's. It then subtracts the per-model duration, bytes scanned, and USD cost.

The summary fields tell you:

- `delta_usd`: total branch cost minus base cost. A positive value means the PR will cost more to run on `main` after merge.
- `total_branch_duration_ms` and `total_branch_bytes_scanned`: run-level totals, used for the budget projection below.
- `savings_from_copy_usd`: what the preview itself saved by copying instead of re-running.
- `models_skipped_via_copy`: how many models did not run on the branch because they were copy-set.

### Pre-merge budget projection

When the project declares a `[budget]` block in `rocky.toml`, `preview cost` projects breaches against the branch totals before merge. A reviewer, and the CI gate, then sees `this PR would breach max_usd / max_duration_ms / max_bytes_scanned if merged` *before* the merge happens. The output field:

```jsonc
"projected_budget_breaches": [
  { "limit_type": "max_usd",          "limit": 2.5,   "actual": 5.0 },
  { "limit_type": "max_duration_ms",  "limit": 60000, "actual": 90000 }
]
```

It is empty when no budget is configured, or when the projected totals stay within every limit. It mirrors the `RunOutput.budget_breaches` shape, so the same downstream consumers (PR-comment templates, JSON listeners) can process both with one code path.

The Markdown rendering shows a "Budget projection" section only when breaches exist. Its wording follows `[budget].on_breach`: either advisory ("would breach") or "would fail the run".

## What the prune set means

The prune set is the set of models that re-execute against the branch. Each model in the working DAG lands in exactly one of two sets:

```
   models changed in the PR diff
              │
              ▼
   the working DAG splits into two sets
        │                    │
        ▼                    ▼
   PRUNE SET            COPY SET
   changed models +     every other model
   everything           in the DAG
   downstream of them
        │                    │
        │ you run            │ preview create
        │ rocky run          │ copies each from
        │ --branch <name>    │ the base schema
        ▼                    ▼
   rebuilt on the       copied into the
   branch schema        branch schema
```

The JSON carries a third field, `skipped_set`. It is reserved, and always empty on the wire today: nothing runs those models, and nothing copies them.

Two reasons put a model in the prune set:

- `reason: "changed"`: the model file itself changed in the diff. (`changed_columns` is a placeholder that is always empty on the wire today.)
- `reason: "downstream_of_changed"`: the model did not change, but it sits transitively downstream of a changed model via `depends_on`.

If the prune set is empty, your PR changes no model output, a whitespace-only edit for example. The branch run is then a no-op, and `preview cost` reports a zero delta.

## What `coverage_warning: true` means

The default `--algorithm sampled` does not read row content yet. It computes a row-count and bytes delta off the two `RunRecord`s. So every model comes back flagged, with `coverage: "not_yet_sampled"` and `coverage_warning: true`:

```jsonc
"algorithm": {
  "kind": "sampled",
  "sampled": { /* ... */ },
  "sampling_window": {
    "ordered_by": "",
    "limit": 0,
    "coverage": "not_yet_sampled",
    "coverage_warning": true
  }
}
```

The aggregate `summary.any_coverage_warning` fires on either of two conditions: a sampled diff with `coverage_warning: true`, or a bisection diff with `bisection_stats.depth_capped: true`. The second means the recursion hit the depth cap on a pathologically skewed PK distribution before it reached leaf size. Either way, the per-model findings might be incomplete.

When you see the warning on a sampled diff, you have two options:

- **Re-run with `--algorithm bisection`.** It covers the whole table exhaustively. It works for any model with a single-column integer or numeric `unique_key`.
- **Inspect the changed columns directly** with `rocky compile --model <name>`, and reason about the change yourself.

A clean sample with `coverage_warning: true` is **not** evidence the PR is a no-op for that model.

## Troubleshooting

**`base ref not found`.** `rocky preview create --base <ref>` needs the ref to exist locally. Run `git fetch origin <ref>` first if you are working against a remote-only ref such as `origin/main`.

**`preview cost` reports `null` deltas.** Cost needs a prior `RunRecord` for each compared model on the base schema. If the base schema has never been run end to end, `base_run_id` is `null` and each per-model `delta_usd` falls back to `null`. Run `rocky plan` and `rocky apply` once on `main` to populate the state store, then re-run `preview cost`.

**`preview cost` reports `null` for the branch.** The cost rollup uses the same adapter telemetry as [`rocky cost`](/reference/commands/administration/#rocky-cost). DuckDB and unconfigured adapters report `null` USD by design; duration and bytes still surface. Configure `[cost]` in `rocky.toml` to get dollar amounts on Databricks or Snowflake.

**Copy step is slow.** The copy substrate dispatches per adapter, through `WarehouseAdapter::clone_table_for_branch`. Databricks (`SHALLOW CLONE`), BigQuery (`CREATE TABLE … COPY`), and Snowflake (zero-copy `CREATE TABLE … CLONE`) all ship metadata-only overrides, so the per-PR branch table is effectively zero-cost at create time. Only DuckDB falls through to the portable CTAS default, which physically copies bytes. On large tables that is the dominant cost of `preview create`.

**The diff finds no changes but the model definitely changed.** Check `summary.any_coverage_warning` in the JSON output. If it is `true`, the sampling window missed the changed rows. See the section above.

## Posting to a PR

`rocky preview` ships a composite GitHub Action. It runs all three commands on every push to a pull request, and upserts a single Markdown comment carrying the prune/copy/skip plan, the structural diff, and the cost delta. The action lives at `.github/actions/rocky-preview/` in the [rocky-data repo](https://github.com/rocky-data/rocky/tree/main/.github/actions/rocky-preview). It is drop-in for any repo with a `rocky.toml` and a `models/` directory.

### Setting up the GitHub Action

Add the workflow below to `.github/workflows/preview.yml` in your own repo:

```yaml
name: rocky-preview

on:
  pull_request:
    types: [opened, synchronize, reopened]

permissions:
  contents: read
  pull-requests: write

jobs:
  preview:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6
        with:
          fetch-depth: 0  # required for git diff against the base branch

      - uses: rocky-data/rocky/.github/actions/rocky-preview@main
        with:
          base_ref: ${{ github.event.pull_request.base.ref }}
          branch_name: ${{ github.event.pull_request.head.ref }}
          github_token: ${{ github.token }}
          # working_directory: my-pipeline   # if rocky.toml lives in a subdir
          # models_dir: models               # default
          # rocky_version: latest            # or 1.17.4 / engine-v1.17.4
```

The first PR after you wire this in installs Rocky and posts a comment with the plan, the diff, and the cost delta. Later pushes update that same comment in place, through the `<!-- rocky-preview -->` marker, so there is no PR-comment spam.

### Action inputs

| Input | Default | Description |
|---|---|---|
| `base_ref` | (required) | Git ref to compare against. Typically `${{ github.event.pull_request.base.ref }}`. |
| `branch_name` | PR head ref, slugged | Preview branch name passed to `rocky preview --name`. Pre-slug if you pass it explicitly: only `[A-Za-z0-9_-]` are preserved. |
| `models_dir` | `models` | Directory containing model files. Passed to `rocky preview create --models`. |
| `working_directory` | `.` | Directory containing `rocky.toml`. The action `cd`s here before each subcommand. |
| `rocky_version` | `latest` | Engine version. `latest` resolves the highest `engine-v*` tag; otherwise pass `1.17.4` or `engine-v1.17.4`. |
| `comment_marker` | `<!-- rocky-preview -->` | Magic-string marker used for comment upsert. Override only if you run multiple preview workflows on the same PR. |
| `fail_on_preview_error` | `false` | When `true`, fail the PR check if any `rocky preview` subcommand errors. The default keeps preview advisory: failures still post a section in the comment. |
| `github_token` | (required) | Token used to read the PR and upsert the comment. Pass `${{ github.token }}` from the workflow (or a PAT for cross-repo permissions). Required because composite actions cannot reference `${{ github.token }}` in input defaults. |

### Action outputs

| Output | Description |
|---|---|
| `comment_url` | HTML URL of the upserted PR comment. |
| `prune_set_size` | Number of models in the prune set (changed + downstream-of-changed). |
| `delta_usd` | Total branch-vs-base USD cost delta. Empty when no paired runs exist yet (e.g. first preview against an unpopulated base). |

### Failure modes

By default the action never blocks a PR:

- A `rocky preview <subcommand>` failure surfaces as an `:x:` section in the comment, with the captured stderr.
- A missing or unfetched base ref produces a hint to add `fetch-depth: 0` to `actions/checkout`.
- A PR that touches no model files renders a tight one-liner (`This PR does not change any pipeline models.`) instead of empty diff and cost tables.

Set `fail_on_preview_error: true` to turn any of those into a hard PR-check failure.
