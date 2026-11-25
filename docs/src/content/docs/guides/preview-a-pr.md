---
title: Preview a PR Before Merging
description: Use rocky preview to run only the models a PR changed, diff them against base, and see the cost difference. Locally, or on every PR with the GitHub Action.
sidebar:
  order: 6
---

`rocky preview` runs only the models a PR's diff changes, against a per-PR branch schema. It copies every other model from the base ref instead of rebuilding it. It then reports a diff and the cost difference against base. This guide runs it locally on a feature branch. The mechanisms (prune set, run pairing, algorithms, coverage) are in [How Preview Works](/concepts/preview-internals/). The output schemas are in the [`rocky preview` CLI reference](/reference/commands/modeling/#rocky-preview).

Preview shows the data and cost shape of a PR. It does not detect breaking schema changes. Pair it with [`rocky ci-diff --semantic`](/reference/commands/modeling/#rocky-ci-diff). A hard semantic gate then fires when the branch is promoted through `rocky plan promote` and `rocky apply` (or the legacy `rocky branch promote` alias). See the [CI/CD guide](/guides/ci-cd/#semantic-breaking-change-findings-and-the-promote-gate).

## Prerequisites

- Rocky installed and on `$PATH` ([Getting Started](/getting-started/introduction/)).
- A repo with a `rocky.toml` and a `models/` directory.
- A git working tree on a feature branch, with at least one model change against the base ref.
- The base schema's tables already materialized, because `preview create` copies them. One `rocky run` on `main` is enough.

The walkthrough uses `--base main`. Any git ref works.

## Step 1: Create the preview branch

```bash
rocky preview create --base main
```

`preview create` finds the changed models, computes the **prune set** (changed models plus everything downstream via `depends_on`) and the **copy set** (every other model), registers a branch like `rocky branch create`, and copies each copy-set table from the base schema.

It does **not** run the prune set. It emits `run_status: "planned"` with an empty `run_id`. Run the prune set yourself before `preview diff` or `preview cost`:

```bash
rocky run --branch pr_preview_fix_price
```

`rocky run --branch` stores that literal name on the run as `rocky_branch`. The checked-out git branch can have another name. `rocky run` has no selector for a set of models: without `--model` it builds the whole pipeline. A one-model prune set can run alone with `--model`. For a larger set, run the whole pipeline once, because `preview diff` and `preview cost` read only the newest branch run. See [How Preview Works](/concepts/preview-internals/#the-prune-and-copy-substrate).

The output is a `PreviewCreateOutput` JSON document:

```json
{
  "version": "1.18.0",
  "command": "preview-create",
  "branch_name": "pr_preview_fix_price",
  "branch_schema": "branch__pr_preview_fix_price",
  "base_ref": "main",
  "head_ref": "HEAD",
  "prune_set": [
    { "model_name": "fct_revenue", "reason": "changed" },
    { "model_name": "rev_by_region", "reason": "downstream_of_changed" }
  ],
  "copy_set": [
    { "model_name": "stg_orders",    "source_schema": "main", "target_schema": "branch__pr_preview_fix_price", "copy_strategy": "ctas" },
    { "model_name": "stg_customers", "source_schema": "main", "target_schema": "branch__pr_preview_fix_price", "copy_strategy": "ctas" }
  ],
  "skipped_set": [],
  "run_id": "",
  "run_status": "planned",
  "duration_ms": 4321
}
```

## Step 2: Diff the branch against base

```bash
rocky preview diff --name pr_preview_fix_price --output json | jq -r .markdown
```

Rocky pairs the newest branch run with a base run from the state store (pairing rules: [How Preview Works](/concepts/preview-internals/#how-diff-and-cost-pair-the-runs)). `--base` takes a branch name, a full commit sha, or a sha prefix of at least seven characters. With no base run, the diff stays empty and `base_note` in the JSON says why. Run the pipeline once on the base branch against this state store to record one.

| Layer | Content |
|---|---|
| `structural` | Per model: `added_columns`, `removed_columns`, `type_changes`. Always empty today, because the `RunRecord` stores no column lists. Use `rocky ci-diff` for schema changes. |
| Row-level | `--algorithm sampled` (default): a delta of the `rows_affected` the two runs recorded. It reads no rows, so every model reports `coverage: "not_yet_sampled"` and `coverage_warning: true`. `--algorithm bisection`: a checksum search over the whole table for a `Merge` model with a single integer or numeric `unique_key`. |

When a run recorded no row count for a model, `rows_added` is `null`, never `0`. The Markdown shows `?`, and `summary.models_unknown` counts the model. An ordinary transformation run records no row count, so this is the usual case.

`--output json` carries the PR-comment Markdown in the top-level `markdown` field: pipe it through `jq -r .markdown`. The valid `--output` values are `json`, `table` and `md`. `md` only logs a one-line status.

```bash
rocky preview diff --name pr_preview_fix_price                          # sampled
rocky preview diff --name pr_preview_fix_price --algorithm bisection    # whole table
```

`--algorithm` is hidden from `--help` but accepted and stable. A direct JSON consumer reads `model.algorithm.kind` first (`sampled` or `bisection`), then unpacks that variant. The `bisection_stats` shape is in [How Preview Works](/concepts/preview-internals/#--algorithm-bisection).

## Step 3: Compare cost vs. base

```bash
rocky preview cost --name pr_preview_fix_price --output json | jq -r .markdown
```

This is a diff layer over [`rocky cost latest`](/reference/commands/administration/#rocky-cost). `preview cost` has no `--base` flag. It pairs the models of the branch run and a base run by name, then subtracts per-model duration, bytes scanned and USD cost. The base run is the newest run other than the branch's own.

| Summary field | Meaning |
|---|---|
| `delta_usd` | Branch cost minus base cost. Positive means the branch cost more. Read it as the cost change on `main` only after you check that `base_run_id` is an ordinary run of `main`. |
| `total_branch_duration_ms`, `total_branch_bytes_scanned` | Run-level totals, used for the budget projection |
| `savings_from_copy_usd` | What copying instead of re-running saved |
| `models_skipped_via_copy` | Models that did not run on the branch because they were copy-set |

### Pre-merge budget projection

With a `[budget]` block in `rocky.toml`, `preview cost` projects breaches against the branch totals before merge: `this PR would breach max_usd / max_duration_ms / max_bytes_scanned if merged`.

```jsonc
"projected_budget_breaches": [
  { "limit_type": "max_usd",          "limit": 2.5,   "actual": 5.0 },
  { "limit_type": "max_duration_ms",  "limit": 60000, "actual": 90000 }
]
```

The field is empty with no budget or no breach. It mirrors `RunOutput.budget_breaches`. The Markdown shows a "Budget projection" section only when breaches exist, worded by `[budget].on_breach` (advisory "would breach", or "would fail the run").

## Reading the output

- `prune_set` entries carry `reason: "changed"` or `reason: "downstream_of_changed"`. `skipped_set` is reserved and always empty. An empty prune set means the PR changes no model output (a whitespace-only edit, for example): `preview diff` and `preview cost` then report no branch run.
- `coverage_warning: true` means the findings can be incomplete. A clean sampled diff is **not** evidence that the PR is a no-op for that model. Re-run with `--algorithm bisection`, or inspect with `rocky compile --model <name>`. `summary.any_coverage_warning` also fires on a bisection `depth_capped: true`. See [How Preview Works](/concepts/preview-internals/#coverage-warning-roll-up).

## Troubleshooting

**`base ref not found`.** `rocky preview create --base <ref>` needs the ref to exist locally. Run `git fetch origin <ref>` first if you are working against a remote-only ref such as `origin/main`.

**`preview diff` returns no models.** Rocky found no pair of runs. Read `base_note` or the `markdown` field to see which is missing. With no run on the branch name, run `rocky run --branch <name>`. With no base run, run the pipeline once on the base branch against this state store.

**`preview cost` reports no deltas.** Cost needs a base run: any run in the state store other than the branch's own. With none, `base_run_id` is missing from the output and `per_model` is empty. The Markdown then says "No branch run yet", even when `branch_run_id` is set. Run `rocky run` once on `main` to record a base run, then re-run `preview cost`.

**`preview cost` reports `null` for the branch.** Check that Rocky found both the branch and base runs. An unknown adapter can also leave USD cost unset. DuckDB reports `0.0` USD when both runs exist. Configure `[cost]` in `rocky.toml` to price Databricks or Snowflake runs.

**Copy step is slow.** Databricks, BigQuery and Snowflake copy metadata only. DuckDB copies bytes with CTAS, which dominates `preview create` on large tables. See [How each warehouse copies a table](/concepts/preview-internals/#how-each-warehouse-copies-a-table).

**The diff finds no changes but the model changed.** Check `summary.any_coverage_warning` and `summary.models_unknown`. A `true` warning means the default comparison read no rows, so a change that keeps the row count does not show. A `models_unknown` above zero means a run recorded no row count for a model.

## Posting to a PR

`rocky preview` ships a composite GitHub Action at `.github/actions/rocky-preview/` in the [rocky-data repo](https://github.com/rocky-data/rocky/tree/main/.github/actions/rocky-preview). On every push to a pull request it runs the three commands and upserts one Markdown comment with the plan, the diff and the cost delta.

:::caution[The action runs the pull request's config]
The action runs `preview create`, then `rocky run --branch <name> --models <dir>`, then `preview diff` and `preview cost`. The branch run executes the pull request's `rocky.toml` and SQL. By default the action passes `rocky run --refuse-hooks`, so a config that defines `[hook]` commands or `[hook.webhooks]` refuses the branch run instead of firing them. Set `allow_hooks: true` only for a trusted config. Do not give the preview job secrets that a fork's pull request can reach: the config also controls the SQL and the `${VAR}` values it reads. `--refuse-hooks` needs engine 1.77.0 or later. With an older `rocky_version`, the action does not start the branch run, and the comment says why.
:::

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
          github_token: ${{ github.token }}
          # working_directory: my-pipeline   # if rocky.toml lives in a subdir
          # models_dir: models               # default
          # rocky_version: latest            # or 1.74.0 / engine-v1.74.0
```

The first PR after you wire this in installs Rocky and posts a comment with the plan. Later pushes update that same comment in place, through the `<!-- rocky-preview -->` marker, so there is no PR-comment spam.

### Action inputs

| Input | Default | Description |
|---|---|---|
| `base_ref` | (required) | Git ref to compare against. Typically `${{ github.event.pull_request.base.ref }}`. |
| `branch_name` | `pr_<PR number>_<head ref slug>` | Preview branch name passed to `rocky preview create --name`. The action maps characters outside `[A-Za-z0-9_]` to underscores and truncates the name to 64 characters. The default PR number prevents different PRs with equivalent slugs from sharing a schema. If you override this input, choose a unique name. |
| `models_dir` | `models` | Directory containing model files. Passed to `rocky preview create --models` and `rocky run --models`. |
| `working_directory` | `.` | Directory containing `rocky.toml`. The action `cd`s here before each subcommand. |
| `rocky_version` | `latest` | Engine version. `latest` resolves the highest `engine-v*` tag; otherwise pass `1.74.0` or `engine-v1.74.0`. |
| `comment_marker` | `<!-- rocky-preview -->` | Magic-string marker used for comment upsert. Override only if you run multiple preview workflows on the same PR. |
| `allow_hooks` | `false` | When `true`, the branch run fires the `[hook]` commands and `[hook.webhooks]` that the pull request's `rocky.toml` defines. The default passes `rocky run --refuse-hooks`, so such a config refuses the branch run. |
| `fail_on_preview_error` | `false` | When `true`, fail the PR check if any `rocky preview` subcommand errors. The default keeps preview advisory: failures still post a section in the comment. |
| `github_token` | empty | Token used to upsert the PR comment. Pass `${{ github.token }}` from a `pull_request` workflow. Without it, the action renders the comment body but skips the upsert. |

### Action outputs

| Output | Description |
|---|---|
| `comment_url` | HTML URL of the upserted PR comment. |
| `body_path` | Path to the rendered Markdown body, for a trusted caller to stage. |
| `prune_set_size` | Number of models in the prune set (changed + downstream-of-changed). |
| `delta_usd` | Total branch-vs-base USD cost delta. Empty when no paired runs exist yet (e.g. first preview against an unpopulated base). |

### Failure modes

By default the action never blocks a PR:

- A `rocky preview <subcommand>` failure surfaces as an `:x:` section in the comment, with the captured stderr.
- A missing or unfetched base ref produces a hint to add `fetch-depth: 0` to `actions/checkout`.
- A PR that touches no model files renders a tight one-liner (`This PR does not change any pipeline models.`) instead of empty diff and cost tables.

Set `fail_on_preview_error: true` to turn any of those into a hard PR-check failure.
