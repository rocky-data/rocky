<p align="center">
  <img src="editors/vscode/icons/rocky-icon-512.png" alt="Rocky" width="128" />
</p>

<h1 align="center">Rocky</h1>

[![Engine CI](https://github.com/rocky-data/rocky/actions/workflows/engine-ci.yml/badge.svg)](https://github.com/rocky-data/rocky/actions/workflows/engine-ci.yml)
[![Dagster CI](https://github.com/rocky-data/rocky/actions/workflows/dagster-ci.yml/badge.svg)](https://github.com/rocky-data/rocky/actions/workflows/dagster-ci.yml)
[![VS Code CI](https://github.com/rocky-data/rocky/actions/workflows/vscode-ci.yml/badge.svg)](https://github.com/rocky-data/rocky/actions/workflows/vscode-ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

The **trust system for your data**. A Rust-based control plane for warehouse pipelines: branches, replay, column-level lineage, compile-time safety, per-model cost attribution. Keep Databricks or Snowflake. Bring Rocky for the DAG.

<p align="center">
  <img src="docs/public/demo-quickstart.gif" alt="Rocky quickstart — create a project, compile, and run 3 models in under 15s" width="900" />
</p>

## Try it in 60 seconds

```bash
# macOS / Linux
curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash

# Windows (PowerShell)
irm https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.ps1 | iex
```

```bash
rocky playground my-first-project
cd my-first-project
rocky compile && rocky test && rocky run
```

No credentials needed — the playground runs end-to-end on local DuckDB.

## See it in action

Each demo below is a self-contained POC in [`examples/playground/pocs/`](examples/playground/) — `cd` in, run `./run.sh`, reproduce locally.

### Detects schema drift the moment it happens

A source column type changes upstream. On the next run, Rocky diffs source vs. target, drops the target, and recreates it. No silent data corruption, no dbt-style quiet divergence.

<p align="center">
  <img src="docs/public/demo-drift-recover.gif" alt="rocky run detects source type change and recreates the target" width="900" />
</p>

[POC — `02-performance/06-schema-drift-recover`](examples/playground/pocs/02-performance/06-schema-drift-recover/)

### Enforces data contracts at compile time

Missing required columns, protected columns being removed, or unsafe type changes surface as diagnostic codes (`E010`, `E013`) before a single row is written.

<p align="center">
  <img src="docs/public/demo-data-contracts.gif" alt="rocky compile flags E010 and E013 contract violations on broken_metrics" width="900" />
</p>

[POC — `01-quality/01-data-contracts-strict`](examples/playground/pocs/01-quality/01-data-contracts-strict/)

### Named branches for risk-free experiments

Create a branch, run against it in an isolated schema, inspect, then drop or promote. Column-level lineage shows the downstream blast radius before you ship.

<p align="center">
  <img src="docs/public/demo-branches-replay.gif" alt="rocky branch create, run on branch, and trace column lineage downstream" width="900" />
</p>

[POC — `00-foundations/06-branches-replay-lineage`](examples/playground/pocs/00-foundations/06-branches-replay-lineage/)

### Column-level lineage, not table-level

Trace a single column from a downstream fact back through its aggregations, all the way to the seed. Blast-radius analysis without reading every model.

<p align="center">
  <img src="docs/public/demo-column-lineage.gif" alt="rocky lineage --column traces fct_revenue.total back to seeds.orders.amount" width="900" />
</p>

[POC — `06-developer-experience/01-lineage-column-level`](examples/playground/pocs/06-developer-experience/01-lineage-column-level/)

### AI model generation with a compile-validate loop

Describe what you want in plain English. Rocky generates a Rocky DSL model, compiles it, and retries on parse failure — the `Attempts: 2` line shows the loop catching a first-pass error invisibly.

<p align="center">
  <img src="docs/public/demo-ai-model-generation.gif" alt="rocky ai generates a .rocky model from natural language intent, Attempts: 2" width="900" />
</p>

[POC — `03-ai/01-model-generation`](examples/playground/pocs/03-ai/01-model-generation/)

### PR-time blast-radius with `rocky lineage-diff`

Compare two git refs and get a per-changed-column readout of downstream consumers — pre-rendered Markdown drops straight into a GitHub PR comment. CODEOWNERS-style review tooling can't reach this granularity without a compiled engine.

<p align="center">
  <img src="docs/public/demo-lineage-diff.gif" alt="rocky lineage-diff main lists added and removed columns across two models with downstream consumers per change" width="900" />
</p>

[POC — `06-developer-experience/11-lineage-diff`](examples/playground/pocs/06-developer-experience/11-lineage-diff/)

### Classify columns, mask by environment, gate CI

Tag PII columns in the model sidecar; bind tags to mask strategies in `[mask]` / `[mask.<env>]`. `rocky compliance --env prod --fail-on exception` exits 1 the moment a classified column has no resolved strategy — a one-line CI gate against accidentally-unmasked data.

<p align="center">
  <img src="docs/public/demo-classification-masking.gif" alt="rocky compliance rolls up classification tags to mask strategies; --fail-on exception exits 1, gating CI on unmasked PII" width="900" />
</p>

[POC — `04-governance/05-classification-masking-compliance`](examples/playground/pocs/04-governance/05-classification-masking-compliance/)

### Incremental loads with persistent watermark state

`strategy = "incremental"` plus a `timestamp_column` is all it takes. Rocky writes the high-water mark to the embedded state store; subsequent runs only `INSERT … WHERE timestamp > watermark`. Append 25 rows after a 500-row load — run 2 still finishes in 0.2s.

<p align="center">
  <img src="docs/public/demo-incremental-watermark.gif" alt="rocky run with incremental strategy: run 1 copies 500 rows; appended 25 rows; run 2 only copies the delta in 0.2s" width="900" />
</p>

[POC — `02-performance/01-incremental-watermark`](examples/playground/pocs/02-performance/01-incremental-watermark/)

## Subprojects

| Path | Artifact | Language | Description |
|---|---|---|---|
| [`engine/`](engine/) | `rocky` CLI binary | Rust | Core SQL transformation engine — 20-crate Cargo workspace |
| [`integrations/dagster/`](integrations/dagster/) | `dagster-rocky` PyPI wheel | Python | Dagster resource and component wrapping the Rocky CLI |
| [`editors/vscode/`](editors/vscode/) | Rocky VSIX | TypeScript | VS Code extension — LSP client + commands for AI features |
| [`examples/playground/`](examples/playground/) | (config only) | TOML / SQL | Self-contained DuckDB sample pipeline used for smoke tests and benchmarks |

Each subproject has its own README with detailed usage. The [`engine/README.md`](engine/README.md) is the canonical product reference for the Rocky CLI.

## Building from source

```bash
git clone https://github.com/rocky-data/rocky.git
cd rocky
just build       # builds engine + dagster wheel + vscode extension
just test        # runs all test suites
just lint        # cargo clippy/fmt + ruff + eslint
```

`just` is optional — you can also build each subproject directly. See [`CONTRIBUTING.md`](CONTRIBUTING.md) for per-subproject build commands.

## Releases

Each artifact is released independently using a tag-namespaced scheme:

- `engine-v*` → Rocky CLI binary (cross-compiled, on GitHub Releases)
- `dagster-v*` → `dagster-rocky` wheel
- `vscode-v*` → Rocky VSIX

See [`CONTRIBUTING.md`](CONTRIBUTING.md#releases) for the full release flow.

## Documentation

Full documentation: **[rocky-data.dev](https://rocky-data.dev)** — concepts, guides, CLI reference, Dagster integration, adapter SDK.

Building a warehouse adapter Rocky doesn't ship in-tree (ClickHouse, Trino, Redshift, ...)? See the [Adapter SDK guide](https://rocky-data.dev/guides/adapter-sdk/) and the [Rust-native skeleton POC](examples/playground/pocs/07-adapters/05-rust-native-adapter-skeleton/).

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md). Before opening a PR, please read the cross-project change guidance — schema and DSL changes must update consumers atomically.

## Sponsoring

Rocky is free and open source. If it saves your team time, consider [sponsoring the project](https://github.com/sponsors/hugocorreia90) so development can continue.

## License

[Apache 2.0](LICENSE)
