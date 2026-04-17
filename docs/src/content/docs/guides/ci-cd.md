---
title: CI/CD Integration
description: Set up Rocky in your CI pipeline for compile-time safety, automated testing, and AI-powered coverage
sidebar:
  order: 5
---

Rocky's `ci` command combines compilation and testing into a single step that runs entirely locally using DuckDB. No warehouse credentials, no external services. This makes it ideal for PR checks, branch protection rules, and automated pipelines.

## 1. The rocky ci Command

```bash
rocky ci --models models --contracts contracts
```

This runs two phases in sequence:

1. **Compile**: Type-check all models, resolve the DAG, validate contracts
2. **Test**: Execute each model's SQL against DuckDB in dependency order

```
Rocky CI Pipeline

  Compile: PASS (12 models)
  Test:    PASS (12 passed, 0 failed)

  Exit code: 0
```

Exit codes:
- **0** -- all checks passed
- **1** -- compilation or test failures detected

The command detects both compile-time issues (type mismatches, missing dependencies, contract violations) and runtime issues (SQL syntax errors, division by zero, invalid casts).

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

For richer CI reporting, output JSON and upload it as an artifact:

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

Parse the JSON output to post a summary comment on the PR:

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

Split compilation and testing into separate stages for faster feedback:

```yaml
stages:
  - compile
  - test

rocky-compile:
  stage: compile
  image: python:3.13-slim
  before_script:
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
    - curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash
    - export PATH="$HOME/.local/bin:$PATH"
  script:
    - rocky test --models models --contracts contracts
  artifacts:
    reports:
      junit: test-report.xml
  rules:
    - changes:
        - models/**
        - contracts/**
```

## 4. Using rocky compile for PR Checks

`rocky compile` is faster than `rocky ci` because it skips test execution. Use it as a lightweight required check on PRs:

```bash
rocky compile --models models --contracts contracts
```

This catches at compile time:
- **Type mismatches**: A column used as `Int64` in one model but `String` in another
- **Missing dependencies**: `depends_on` references a model that does not exist
- **Contract violations**: Required columns missing, wrong types, or protected columns removed
- **DAG cycles**: Model A depends on B, B depends on A
- **Unresolved references**: SQL references a table or column that cannot be found

For a single model check during development:

```bash
rocky compile --models models --model revenue_summary
```

## 5. AI-Powered Test Coverage

Use `rocky ai-test` to automatically generate test assertions from your models. This requires `ANTHROPIC_API_KEY` to be set.

### Generate tests locally

```bash
export ANTHROPIC_API_KEY="sk-ant-..."

# Add intent descriptions to all models (one-time setup)
rocky ai-explain --all --save --models models

# Generate test assertions from intent
rocky ai-test --all --save --models models
```

This creates test files in the `tests/` directory. Commit them to your repository -- they run as part of `rocky ci` and `rocky test` on every PR.

### Generate tests for a single model

```bash
rocky ai-test --model revenue_summary --save --models models
```

### CI workflow with AI test generation

Run AI test generation as a scheduled job to keep coverage up to date:

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

If you use Dagster to orchestrate Rocky, add both checks to your CI pipeline:

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

Rocky's `ci` command validates the models independently. Dagster's `definitions validate` ensures the orchestration layer can load and wire the models into assets.

## 7. JSON Output Schema

All CI-related commands produce structured JSON for programmatic consumption.

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
  "diagnostics": [
    {
      "severity": "error",
      "model": "fct_revenue",
      "message": "unknown column 'nonexistent'",
      "location": { "file": "models/fct_revenue.sql", "line": 5, "column": 9 }
    }
  ],
  "has_errors": true
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
    ["fct_revenue", "division by zero at line 8"]
  ]
}
```

Parse with `jq` for custom CI reporting:

```bash
# Check if any tests failed
rocky ci -o json | jq -e '.tests_failed == 0'

# Extract error messages
rocky compile -o json | jq '.diagnostics[] | select(.severity == "error") | .message'
```
