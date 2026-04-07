---
title: Contributing
description: How to set up a development environment and contribute to Rocky
sidebar:
  order: 1
---

Rocky lives in a monorepo at `rocky-data/rocky`. The four subprojects share one repository, one issue tracker, and one PR flow, but each has its own build system. See the repository [`CONTRIBUTING.md`](https://github.com/rocky-data/rocky/blob/main/CONTRIBUTING.md) for the full contribution guide.

## Development Setup

### Rocky CLI engine (Rust)

```bash
git clone https://github.com/rocky-data/rocky.git
cd rocky/engine

# Build
cargo build

# Run tests
cargo test

# Lint
cargo clippy -- -D warnings
cargo fmt -- --check
```

### dagster-rocky (Python)

```bash
git clone https://github.com/rocky-data/rocky.git
cd rocky/integrations/dagster

# Install with dev dependencies
uv sync --dev

# Run tests
uv run pytest -v

# Lint
uv run ruff check
uv run ruff format --check
```

## Repository layout

```
rocky/                              # the rocky-data/rocky monorepo
├── engine/                         # Rust CLI + 20-crate Cargo workspace
│   ├── Cargo.toml
│   ├── crates/
│   │   ├── rocky-core/             # Generic transformation engine
│   │   ├── rocky-sql/              # SQL parsing + validation
│   │   ├── rocky-databricks/       # Databricks adapter
│   │   ├── rocky-fivetran/         # Fivetran adapter
│   │   ├── rocky-cache/            # Three-tier caching
│   │   ├── rocky-duckdb/           # DuckDB local execution
│   │   ├── rocky-observe/          # Observability
│   │   └── rocky-cli/              # CLI framework
│   ├── rocky/                      # Binary crate (the `rocky` CLI)
│   │   └── src/main.rs
│   └── docs/                       # This documentation site
├── integrations/dagster/           # Python — Dagster integration
├── editors/vscode/                 # TypeScript — VS Code extension
└── examples/playground/            # Sample DuckDB pipeline (no credentials)
```

## Coding Standards

- **Rust edition**: 2024
- **Error handling**: `thiserror` for library errors, `anyhow` for binary/CLI errors
- **Logging**: `tracing` crate (not `println!`)
- **SQL safety**: All identifiers validated via `rocky-sql/validation.rs` before interpolation
- **Tests**: In the same file (`#[cfg(test)] mod tests`)
- **Public types**: Must derive `Debug`, `Clone`, `Serialize`, `Deserialize` where applicable

## Git Conventions

- Use conventional commits: `feat:`, `fix:`, `refactor:`, `test:`, `docs:`, `chore:`
- Scope by crate when relevant: `feat(rocky-databricks): add OAuth M2M auth`

## Testing

```bash
# All tests
cargo test

# Single crate
cargo test -p rocky-core

# With output
cargo test -- --nocapture

# dagster-rocky
uv run pytest -v
```
