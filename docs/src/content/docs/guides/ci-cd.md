---
title: CI/CD Integration
description: Run Rocky in CI to compile and test every PR, diff a branch against main, and gate a promote on breaking changes.
sidebar:
  order: 5
---

`rocky ci` compiles and tests your project in one step. It runs entirely on your CI machine, on DuckDB, so it needs no warehouse credentials and no external service. That makes it a cheap required check on a pull request.

## 1. The rocky ci Command

```bash
rocky ci --models models --contracts contracts
```

The command runs two phases in order:

1. **Compile**: type-check every model, resolve the DAG, validate contracts
2. **Test**: execute each model's SQL against DuckDB in dependency order

```
Rocky CI Pipeline

  Compile: PASS (12 models)
  Test:    PASS (12 passed, 0 failed)

  Exit code: 0
```

Exit codes:
- **0** -- every check passed
- **1** -- a compilation or test failure

The two phases catch different faults. Compile catches type mismatches, missing dependencies, and contract violations. Test catches what only shows up when the SQL runs: syntax errors, division by zero, invalid casts.

### Structural diff against a base ref

[`rocky ci-diff`](/reference/commands/modeling/#rocky-ci-diff) answers a different question for a reviewer: what did this branch change? It compares model files between a base git ref and `HEAD`, compiles both sides, and reports the added, modified, and removed columns per model. It writes JSON for your pipeline and Markdown for a PR comment:

```bash
rocky ci-diff                    # defaults to main
rocky ci-diff release/2026-04 --models src/models
```

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

Each finding carries a tagged `change.kind` (such as `column_dropped`, `column_type_changed`, `target_renamed`) and a `severity` (`breaking`, `warning`, or `info`). `ci-diff --semantic` is **informational**. A `breaking` finding does not change its exit code. Run it on every PR so reviewers see breaking changes before anyone promotes.

`rocky plan --semantic` gives the author the same verdict at plan time. It diffs your *working tree*, uncommitted edits included, against `--base` (default `main`), and attaches the verdict under `breaking_verdict` in the JSON output:

```bash
rocky plan --semantic --base main --output json | jq '.breaking_verdict'
```

This reports only. The verdict never gates the plan. When no baseline exists, Rocky omits `breaking_verdict` rather than inventing one. That happens when there is no `models/` directory, or when the `--base` ref's models do not compile. The hard gate is `rocky plan promote`, below.

:::caution[The classifier diffs OUTPUT SCHEMA; it is blind to value changes]
The breaking-change classifier compares the typed **output schema** of each model: columns, types, nullability, materialization keys, masks, target. It is **blind to schema-stable value changes**. A `WHERE`, `JOIN`-key, or `CASE` rewrite that changes every output row, but leaves the column list and types alone, produces **no finding**. An empty `breaking_verdict.findings` therefore means "no output-schema change was detected". It is **not** a signal that the data is unchanged. The verdict repeats this statement verbatim in its `caveat` field, so a JSON-only consumer cannot miss it. To see whether values moved, pair it with [`rocky preview`](/guides/preview-a-pr/), which diffs rows on real data.
:::

The hard gate lives on `rocky plan promote` and `rocky apply`. When you promote a branch to production, Rocky runs the same classifier against `--base` (default `main`). Any finding with `severity == "breaking"` blocks the promote **at plan time**.

The gate fires once. A blocked promote produces no `plan_id`, so `rocky apply` has nothing to run. Rocky records the gate result in the persisted plan and does **not** re-evaluate it at apply time; `rocky apply` replays the recorded verdict. To ship a breaking change on purpose, once downstream consumers have migrated, pass `--allow-breaking` at plan time. The override emits a `breaking_changes_allowed` audit event, so the bypass leaves a paper trail.

```bash
# PR-time: detect (informational)
rocky ci-diff --semantic

# Promote-time: gate (blocks on `breaking` findings)
plan_id=$(rocky plan promote fix-price --base main --output json | jq -r .plan_id)
rocky apply "$plan_id"

# Promote-time override (audited)
plan_id=$(rocky plan promote fix-price --base main --allow-breaking --output json | jq -r .plan_id)
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

### With JSON output and artifact upload

Write JSON and upload it as an artifact when you want the detail after the job ends:

```yaml
name: Rocky CI
on:
  pull_request:
    paths:
      - "models/**"
      - "contracts/**"
      - "rocky.toml"

jobs:
  rocky:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install Rocky
        run: |
          curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash
          echo "$HOME/.local/bin" >> $GITHUB_PATH

      - name: Compile
        run: rocky compile --models models --contracts contracts -o json > compile-report.json

      - name: Test
        run: rocky test --models models --contracts contracts -o json > test-report.json

      - name: CI Check
        run: rocky ci --models models --contracts contracts -o json > ci-report.json

      - name: Upload Reports
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: rocky-reports
          path: |
            compile-report.json
            test-report.json
            ci-report.json
```

### PR comment with results

Parse the JSON and post a summary comment on the PR:

```yaml
      - name: CI Check
        id: ci
        run: |
          rocky ci --models models --contracts contracts -o json > ci-report.json
          echo "models=$(jq '.models_compiled' ci-report.json)" >> $GITHUB_OUTPUT
          echo "passed=$(jq '.tests_passed' ci-report.json)" >> $GITHUB_OUTPUT
          echo "failed=$(jq '.tests_failed' ci-report.json)" >> $GITHUB_OUTPUT

      - name: Comment PR
        if: always()
        uses: actions/github-script@v7
        with:
          script: |
            const models = '${{ steps.ci.outputs.models }}';
            const passed = '${{ steps.ci.outputs.passed }}';
            const failed = '${{ steps.ci.outputs.failed }}';
            const status = failed === '0' ? 'PASS' : 'FAIL';
            const body = `### Rocky CI: ${status}\n| Models | Tests Passed | Tests Failed |\n|---|---|---|\n| ${models} | ${passed} | ${failed} |`;
            github.rest.issues.createComment({
              issue_number: context.issue.number,
              owner: context.repo.owner,
              repo: context.repo.repo,
              body: body
            });
```

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

### Separate compile and test stages

Split compile and test into two stages. A compile failure then reports without waiting for the tests:

```yaml
stages:
  - compile
  - test

rocky-compile:
  stage: compile
  image: python:3.13-slim
  before_script:
    - apt-get update && apt-get install -y --no-install-recommends curl ca-certificates
    - curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash
    - export PATH="$HOME/.local/bin:$PATH"
  script:
    - rocky compile --models models --contracts contracts
  rules:
    - changes:
        - models/**
        - contracts/**

rocky-test:
  stage: test
  image: python:3.13-slim
  needs: [rocky-compile]
  before_script:
    - apt-get update && apt-get install -y --no-install-recommends curl ca-certificates
    - curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash
    - export PATH="$HOME/.local/bin:$PATH"
  script:
    - rocky test --models models --contracts contracts
  rules:
    - changes:
        - models/**
        - contracts/**
```

## 4. Using rocky compile for PR Checks

`rocky compile` skips test execution, so it finishes faster than `rocky ci`. Use it as a cheap required check on PRs:

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

This writes one `.sql` file per assertion into the `tests/` directory, a sibling of `models/`. Each file is a standalone SQL assertion. `rocky ci` and `rocky test` do **not** pick them up: those commands execute your models and any `[[test]]` sidecar blocks, never loose `tests/*.sql` files. So commit them and run them in a CI step of your own, executing each assertion against DuckDB. For gating that Rocky runs itself, declare `[[tests]]` in the model sidecars and run `rocky test --declarative`.

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

If Dagster orchestrates Rocky, run both checks in CI:

```yaml
name: Data Pipeline CI
on:
  pull_request:
    paths:
      - "models/**"
      - "contracts/**"
      - "rocky.toml"
      - "dagster/**"

jobs:
  rocky:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install Rocky
        run: |
          curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash
          echo "$HOME/.local/bin" >> $GITHUB_PATH

      - name: Rocky CI
        run: rocky ci --models models --contracts contracts

  dagster:
    runs-on: ubuntu-latest
    needs: [rocky]
    steps:
      - uses: actions/checkout@v4

      - name: Install Python dependencies
        run: uv add dagster dagster-rocky

      - name: Validate Dagster definitions
        run: uv run dg check defs
```

The two checks cover different layers. `rocky ci` validates the models on their own. Dagster's `definitions validate` confirms that the orchestration layer can load those models and wire them into assets.

## 7. JSON Output Schema

Every CI command emits structured JSON.

### rocky ci

```json
{
  "version": "1.6.0",
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

### rocky compile

```json
{
  "version": "1.6.0",
  "command": "compile",
  "models": 12,
  "execution_layers": 4,
  "has_errors": true,
  "diagnostics": [
    {
      "severity": "Error",
      "code": "E001",
      "model": "fct_revenue",
      "message": "unknown column 'nonexistent'",
      "span": { "file": "models/fct_revenue.sql", "line": 5, "col": 9 },
      "suggestion": "did you mean 'revenue'?"
    }
  ],
  "compile_timings": { "project_load_ms": 5, "semantic_graph_ms": 1, "typecheck_ms": 12, "typecheck_join_keys_ms": 3, "contracts_ms": 2, "total_ms": 23 }
}
```

### rocky test

```json
{
  "version": "1.6.0",
  "command": "test",
  "total": 12,
  "passed": 11,
  "failed": 1,
  "failures": [
    { "name": "fct_revenue", "error": "division by zero at line 8" }
  ]
}
```

Parse the payload with `jq` to build your own CI report:

```bash
# Check if any tests failed
rocky ci -o json | jq -e '.tests_failed == 0'

# Extract error messages
rocky compile -o json | jq '.diagnostics[] | select(.severity == "Error") | .message'
```
