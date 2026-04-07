# Rocky

[![Engine CI](https://github.com/rocky-data/rocky/actions/workflows/engine-ci.yml/badge.svg)](https://github.com/rocky-data/rocky/actions/workflows/engine-ci.yml)
[![Dagster CI](https://github.com/rocky-data/rocky/actions/workflows/dagster-ci.yml/badge.svg)](https://github.com/rocky-data/rocky/actions/workflows/dagster-ci.yml)
[![VS Code CI](https://github.com/rocky-data/rocky/actions/workflows/vscode-ci.yml/badge.svg)](https://github.com/rocky-data/rocky/actions/workflows/vscode-ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

A SQL transformation engine built in Rust. Type-safe compilation, column-level lineage, AI-powered intent, and a language server — for data pipelines that don't break.

This repository is a monorepo containing the Rocky engine and its first-party integrations.

## Subprojects

| Path | Artifact | Language | Description |
|---|---|---|---|
| [`engine/`](engine/) | `rocky` CLI binary | Rust | Core SQL transformation engine — 20-crate Cargo workspace |
| [`integrations/dagster/`](integrations/dagster/) | `dagster-rocky` PyPI wheel | Python | Dagster resource and component wrapping the Rocky CLI |
| [`editors/vscode/`](editors/vscode/) | Rocky VSIX | TypeScript | VS Code extension — LSP client + commands for AI features |
| [`examples/playground/`](examples/playground/) | (config only) | TOML / SQL | Self-contained DuckDB sample pipeline used for smoke tests and benchmarks |

Each subproject has its own README with detailed usage. The [`engine/README.md`](engine/README.md) is the canonical product reference for the Rocky CLI.

## Quick install (Rocky CLI only)

**macOS / Linux:**

```bash
curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash
```

**Windows (PowerShell):**

```powershell
irm https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.ps1 | iex
```

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

Full documentation: **[rocky-data.github.io/rocky](https://rocky-data.github.io/rocky/)**

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md). Before opening a PR, please read the cross-project change guidance — schema and DSL changes must update consumers atomically.

## Sponsoring

Rocky is free and open source. If it saves your team time, consider [sponsoring the project](https://github.com/sponsors/hugocorreia90) so development can continue.

## License

[Apache 2.0](LICENSE)
