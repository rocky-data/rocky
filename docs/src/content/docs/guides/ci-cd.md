---
title: CI/CD Integration
description: Run Rocky in CI to compile and test every PR, diff a branch against main, and gate a promote on breaking changes.
sidebar:
  order: 5
---

`rocky ci` compiles and tests your project in one step. It runs on your CI machine, on DuckDB, with no warehouse credentials, so it is a cheap required check on a pull request.

## 1. The rocky ci Command

```bash
rocky ci --contracts contracts
```

With no `--models`, the command covers the models of every transformation pipeline. It runs two phases in order:

1. **Compile**: type-check every model, resolve the DAG, validate contracts. When the project has `data/seed.sql`, the types come from the tables it makes.
2. **Test**: execute each model's SQL against DuckDB in dependency order, on the seed's tables

```
Rocky CI Pipeline

  Compile: PASS (12 models)
  Test:    PASS (12 passed, 0 failed)

  Exit code: 0
```

Exit codes:
- **0** -- every check passed
- **1** -- a compilation or test failure

Compile catches type mismatches, missing dependencies and contract violations. Test catches faults that show only when the SQL runs: syntax errors, division by zero, invalid casts.

### Structural diff against a base ref

[`rocky ci-diff`](/reference/commands/modeling/#rocky-ci-diff) answers a different question for a reviewer: what did this branch change? It compares model files between a base git ref and `HEAD`, compiles both sides, and reports the added, modified, and removed columns per model. It writes JSON for your pipeline and Markdown for a PR comment:

```bash
rocky ci-diff                    # defaults to main
rocky ci-diff release/2026-04 --models src/models
```

`ci-diff` reads `HEAD` and the merge base from git, so uncommitted edits never reach the report. Pass `--working-tree` to preview uncommitted work. The JSON output says which ran in `mode`.

In GitHub Actions, post that Markdown block straight to the PR:

```yaml
- name: Post diff to PR
  run: |
    rocky ci-diff --output json | jq -r .markdown | \
      gh pr comment "$PR_NUMBER" --body-file -
  env:
    PR_NUMBER: ${{ github.event.pull_request.number }}
    GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```

### Semantic breaking-change findings and the promote gate

Three commands run the same breaking-change classifier over the typed IR (the compiler's typed graph of your models, see the [glossary](/reference/glossary/#ir-intermediate-representation)). Two of them only report. One of them blocks:

```
  reporting only                 the gate
  ─────────────────────────────  ────────────────────────────────────
  rocky plan --semantic          rocky plan promote <branch>
    (your working tree)            │
  rocky ci-diff --semantic         ├─ a `breaking` finding
    (a branch, at PR time)         │    └─► no plan_id: the promote
    │                              │        stops here
    ├─► findings in the JSON       │
    │   the exit code never        └─ no `breaking` finding
    │   changes                         └─► plan_id ─► rocky apply
    │                                                  replays the
    └─► a reviewer reads them                          recorded verdict
```

`rocky ci-diff --semantic` runs the classifier on top of the structural diff and puts the findings under `breaking_findings` in the JSON output:

```bash
rocky ci-diff --semantic --output json | jq '.breaking_findings'
```

Each finding carries a tagged `change.kind` (such as `column_dropped`, `column_type_changed`, `target_renamed`) and a `severity` (`breaking`, `warning`, or `info`). `ci-diff --semantic` is **informational**: a `breaking` finding does not change its exit code.

`rocky plan --semantic` gives the author the same verdict at plan time. It diffs your *working tree*, uncommitted edits included, against `--base` (default `main`), and attaches the verdict under `breaking_verdict` in the JSON output:

```bash
rocky plan --semantic --base main --output json | jq '.breaking_verdict'
```

This reports only. When no baseline exists (no `models/` directory, or the `--base` ref's models do not compile), Rocky omits `breaking_verdict`. The hard gate is `rocky plan promote`, below.

:::caution[The classifier diffs OUTPUT SCHEMA; it is blind to value changes]
The classifier compares the typed **output schema** of each model: columns, types, nullability, materialization keys, masks, target. A `WHERE`, `JOIN`-key, or `CASE` rewrite that changes every output row but leaves the columns and types alone produces **no finding**. An empty `breaking_verdict.findings` means "no output-schema change was detected", **not** "the data is unchanged". The verdict repeats this in its `caveat` field. To see whether values moved, use [`rocky preview`](/guides/preview-a-pr/), which diffs rows on real data.
:::

The hard gate lives on `rocky plan promote` and `rocky apply`. When you promote a branch to production, Rocky runs the same classifier against `--base` (default `main`). Any finding with `severity == "breaking"` blocks the promote **at plan time**.

The gate fires once. A blocked promote produces no `plan_id`, so `rocky apply` has nothing to run. Rocky records the gate result in the plan and does **not** re-evaluate it at apply time. To ship a breaking change on purpose, pass `--allow-breaking` at plan time. The override emits a `breaking_changes_allowed` audit event.

:::note[Multi-pipeline projects]
In a project whose `rocky.toml` declares more than one pipeline, pass `--pipeline <name>` to `rocky plan promote`. The plan records the resolved pipeline, so `rocky apply <plan-id>` reads it from the plan and needs no `--pipeline` flag of its own:

```bash
plan_id=$(rocky plan promote fix_price --pipeline shopify_us --output json | jq -r .plan_id)
rocky apply "$plan_id"
```
:::

```bash
# PR-time: detect (informational)
rocky ci-diff --semantic

# Promote-time: gate (blocks on `breaking` findings)
plan_id=$(rocky plan promote fix_price --base main --output json | jq -r .plan_id)
rocky apply "$plan_id"

# Promote-time override (audited)
plan_id=$(rocky plan promote fix_price --base main --allow-breaking --output json | jq -r .plan_id)
rocky apply "$plan_id"
```

The bare `rocky branch promote <name>` form still works as an alias for the two-step flow above. See [`rocky branch promote`](/reference/commands/core-pipeline/#rocky-branch) for the flag list, and the [`branch_promote` schema](https://github.com/rocky-data/rocky/blob/main/schemas/branch_promote.schema.json) for the audit-event reference.

## 2. GitHub Actions

### Basic setup

```yaml
name: Rocky CI
on:
  pull_request:
    paths:
      - "models/**"
      - "contracts/**"
      - "rocky.toml"
      - "tests/**"

jobs:
  rocky:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install Rocky
        run: |
          curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash
          echo "$HOME/.local/bin" >> $GITHUB_PATH

      - name: Compile and Test
        run: rocky ci --models models --contracts contracts
```

### JSON output and artifacts

Add `-o json > ci-report.json` to any command, then upload the file with `actions/upload-artifact@v4` (`if: always()`) to keep the detail after the job ends. To comment on the PR, use the `rocky ci-diff` step above.

## 3. GitLab CI

### Basic setup

```yaml
rocky-ci:
  image: python:3.13-slim
  before_script:
    - apt-get update && apt-get install -y --no-install-recommends curl ca-certificates
    - curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash
    - export PATH="$HOME/.local/bin:$PATH"
  script:
    - rocky ci --models models --contracts contracts
  rules:
    - changes:
        - models/**
        - contracts/**
        - rocky.toml
```

To report a compile failure without waiting for tests, split `rocky compile` and `rocky test` into two jobs (stages `compile` and `test`, with `needs: [rocky-compile]` on the second).

## 4. Using rocky compile for PR Checks

`rocky compile` skips tests, so it is faster than `rocky ci`:

```bash
rocky compile --models models --contracts contracts
```

The compiler catches:
- **Type mismatches**: a column used as `Int64` in one model and `String` in another
- **Missing dependencies**: a `depends_on` that names a model which does not exist
- **Contract violations**: a missing required column, a wrong type, or a removed protected column
- **DAG cycles**: model A depends on B, and B depends on A
- **Unresolved references**: SQL that names a table or column Rocky cannot find

To check one model while you work on it:

```bash
rocky compile --models models --model revenue_summary
```

## 5. AI-Powered Test Coverage

`rocky ai-test` writes test assertions from your models. It needs `ANTHROPIC_API_KEY` set.

### Generate tests locally

```bash
export ANTHROPIC_API_KEY="sk-ant-..."

# Add intent descriptions to all models (one-time setup)
rocky ai-explain --all --save --models models

# Generate test assertions from intent
rocky ai-test --all --save --models models
```

This writes one standalone `.sql` assertion per file into `tests/`, a sibling of `models/`. `rocky ci` and `rocky test` do **not** pick them up: they execute your models and any `[[test]]` sidecar blocks, never loose `tests/*.sql` files. Commit them and run them in your own CI step against DuckDB. For gating that Rocky runs itself, declare `[[tests]]` in the model sidecars and run `rocky test --declarative`.

### Generate tests for a single model

```bash
rocky ai-test revenue_summary --save --models models
```

### CI workflow with AI test generation

Generate the tests on a schedule, and open a PR with the result:

```yaml
name: Update AI Tests
on:
  schedule:
    - cron: "0 6 * * 1"  # Every Monday at 6am

jobs:
  update-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install Rocky
        run: |
          curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash
          echo "$HOME/.local/bin" >> $GITHUB_PATH

      - name: Generate Tests
        env:
          ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
        run: |
          rocky ai-explain --all --save --models models
          rocky ai-test --all --save --models models

      - name: Create PR
        uses: peter-evans/create-pull-request@v6
        with:
          title: "test: update AI-generated test assertions"
          body: "Auto-generated test updates from `rocky ai-test`"
          branch: update-ai-tests
```

## 6. Integration with Dagster CI

If Dagster orchestrates Rocky, add a second job that runs after `rocky ci` (`needs: [rocky]`): install `dagster dagster-rocky` with `uv add` and run `uv run dg check defs`. `rocky ci` validates the models on their own. `dg check defs` confirms that the orchestration layer can load them into assets.

## 7. JSON Output

Every CI command emits structured JSON. The [JSON output reference](/reference/json-output/) has the full shapes. `rocky ci` returns:

```json
{
  "version": "1.80.0",
  "command": "ci",
  "compile_ok": true,
  "tests_ok": true,
  "models_compiled": 12,
  "tests_passed": 12,
  "tests_failed": 0,
  "exit_code": 0,
  "diagnostics": [],
  "failures": []
}
```

Parse the payload with `jq`:

```bash
# Check if any tests failed
rocky ci -o json | jq -e '.tests_failed == 0'

# Extract error messages
rocky compile -o json | jq '.diagnostics[] | select(.severity == "Error") | .message'
```
