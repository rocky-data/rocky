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

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md). Before opening a PR, please read the cross-project change guidance — schema and DSL changes must update consumers atomically.

## Sponsoring

Rocky is free and open source. If it saves your team time, consider [sponsoring the project](https://github.com/sponsors/hugocorreia90) so development can continue.

## License

[Apache 2.0](LICENSE)
