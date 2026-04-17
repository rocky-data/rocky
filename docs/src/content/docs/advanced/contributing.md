---
title: Contributing
description: How to set up a development environment and contribute to Rocky
sidebar:
  order: 1
---

## Monorepo Structure

Rocky is a monorepo with four subprojects:

```
rocky-data/
├── engine/                     # Rust CLI + engine (20-crate Cargo workspace)
│   ├── Cargo.toml
│   ├── crates/
│   │   ├── rocky-core/         # Generic transformation engine
│   │   ├── rocky-sql/          # SQL parsing + validation
│   │   ├── rocky-lang/         # Rocky DSL parser (.rocky files)
│   │   ├── rocky-compiler/     # Type checking + semantic analysis
│   │   ├── rocky-adapter-sdk/  # Adapter SDK + conformance tests
│   │   ├── rocky-databricks/   # Databricks adapter
│   │   ├── rocky-snowflake/    # Snowflake adapter
│   │   ├── rocky-fivetran/     # Fivetran source adapter
│   │   ├── rocky-duckdb/       # DuckDB local execution
│   │   ├── rocky-engine/       # Local query engine (DataFusion + Arrow)
│   │   ├── rocky-server/       # HTTP API + LSP server
│   │   ├── rocky-cache/        # Three-tier caching
│   │   ├── rocky-ai/           # AI intent layer
│   │   ├── rocky-observe/      # Observability
│   │   └── rocky-cli/          # CLI framework + Dagster Pipes
│   └── rocky/                  # Binary crate
├── integrations/dagster/       # dagster-rocky Python package
├── editors/vscode/             # VS Code extension (LSP client)
├── examples/playground/        # POC catalog (28 POCs) + benchmarks
├── docs/                       # This documentation site (Astro + Starlight)
├── justfile                    # Cross-project build orchestration
└── CLAUDE.md                   # Monorepo conventions
```

## Development Setup

### Rocky Engine (Rust)

```bash
git clone https://github.com/rocky-data/rocky.git
cd rocky/engine

# Build
cargo build
cargo build --release

# Run tests
cargo test

# Lint
cargo clippy -- -D warnings
cargo fmt -- --check
```

### dagster-rocky (Python)

```bash
cd rocky-data/integrations/dagster

# Install with dev dependencies
uv sync --dev

# Run tests
uv run pytest -v

# Lint
uv run ruff check
uv run ruff format --check
```

### VS Code Extension (TypeScript)

```bash
cd rocky-data/editors/vscode

# Install dependencies
npm install

# Compile
npm run compile

# Run tests
npm test
```

### Build Orchestration

The top-level `justfile` orchestrates common tasks across all subprojects:

```bash
just build       # cargo build --release + uv build --wheel + npm compile
just test        # cargo test + pytest + vitest
just lint        # cargo clippy/fmt + ruff + eslint
just codegen     # Export JSON schemas + regenerate Pydantic/TS bindings
just --list      # All recipes
```

## Coding Standards

### Rust
- **Edition**: 2024 (MSRV 1.85)
- **Error handling**: `thiserror` for library errors, `anyhow` for binary/CLI errors
- **Logging**: `tracing` crate (not `println!`)
- **SQL safety**: All identifiers validated via `rocky-sql/validation.rs` before interpolation
- **Tests**: In the same file (`#[cfg(test)] mod tests`)
- **Public types**: Must derive `Debug`, `Clone`, `Serialize`, `Deserialize` where applicable

### Python
- Target Python 3.10+
- Type annotations required (use modern syntax: `list[str]`, `X | None`)
- Use `ruff` for linting and formatting

### TypeScript
- Strict mode enabled
- ESLint + Prettier formatting

## Git Conventions

- Use conventional commits: `feat:`, `fix:`, `refactor:`, `test:`, `docs:`, `chore:`
- Scope by subproject or crate: `feat(engine/rocky-databricks): add OAuth M2M auth`, `fix(dagster): handle partial-success exit codes`, `chore(vscode): bump vscode-languageclient`
- Never include `Co-Authored-By` trailers

## Cross-Project Changes

When modifying the CLI's JSON output schema:

1. Edit the relevant `*Output` struct in `engine/crates/rocky-cli/src/output.rs`
2. Run `just codegen` from the monorepo root to regenerate bindings
3. Commit the schema and regenerated bindings together with the Rust change

The `codegen-drift` CI workflow fails any PR where the committed bindings drift from what `just codegen` produces locally.

When modifying Rocky DSL syntax (`.rocky` files):

1. `engine/crates/rocky-lang/` (parser + lexer)
2. `engine/crates/rocky-compiler/` (type checking)
3. `editors/vscode/syntaxes/rocky.tmLanguage.json` (TextMate grammar)
4. `editors/vscode/snippets/rocky.json` (snippets)

## Testing

```bash
# Engine — all tests
cargo test

# Engine — single crate
cargo test -p rocky-core

# Engine — E2E integration tests (DuckDB, no credentials)
cargo test -p rocky-core --test e2e

# Engine — with output
cargo test -- --nocapture

# Dagster integration
cd integrations/dagster && uv run pytest -v

# VS Code extension
cd editors/vscode && npm test
```

## CI

`.github/workflows/` contains path-filtered workflows:

| Workflow | Trigger | What it does |
|---|---|---|
| `engine-ci.yml` | `engine/**` changes | Tests, clippy, fmt |
| `engine-weekly.yml` | Monday schedule + manual | Coverage (tarpaulin) + security audit |
| `engine-release.yml` | `engine-v*` tag | Full 5-target matrix (macOS, Linux, Windows) |
| `engine-bench.yml` | PRs labeled `perf` | Benchmark with 120% alert threshold |
| `dagster-ci.yml` | `integrations/dagster/**` changes | pytest + ruff |
| `dagster-release.yml` | `dagster-v*` tag | PyPI publish via OIDC |
| `vscode-ci.yml` | `editors/vscode/**` changes | npm test + eslint |
| `vscode-release.yml` | `vscode-v*` tag | VS Code Marketplace publish |
| `codegen-drift.yml` | Any subproject | Validates committed bindings match `just codegen` output |
| `integration-ci.yml` | 2+ subprojects touched | Cross-project integration tests |

## Releases

Tag-namespaced — each artifact ships independently. All three are **CI-driven**: land a release PR (version bump + CHANGELOG entry), tag the merged commit, push the tag.

| Artifact | Tag | Workflow |
|---|---|---|
| Rocky CLI binary | `engine-v*` | `engine-release.yml` — 5-target matrix (macOS ARM64/Intel, Linux x86_64/ARM64, Windows) |
| dagster-rocky wheel | `dagster-v*` | `dagster-release.yml` — PyPI publish via OIDC |
| Rocky VSIX | `vscode-v*` | `vscode-release.yml` — VS Code Marketplace publish |

```bash
git tag engine-v1.7.0
git push origin engine-v1.7.0   # CI builds + publishes
```

The `scripts/release.sh` helper remains as a **local-build fallback** for hotfix scenarios; `just release-engine <version>`, `just release-dagster <version> [--publish]`, and `just release-vscode <version> [--publish]` wrap it.
