---
title: Contributing
description: How to set up a development environment and contribute to Rocky
sidebar:
  order: 1
---

## Where to start

The right entry point depends on what you want to change.

| You want to change | Start here |
|---|---|
| How a warehouse executes SQL | `engine/crates/rocky-<name>/` — the adapter crate. Implement `WarehouseAdapter` / `SqlDialect` from `rocky-adapter-sdk`. |
| How SQL is generated from the IR | `engine/crates/rocky-core/src/sql_gen.rs` |
| What the IR looks like (model shape) | `engine/crates/rocky-ir/src/ir.rs` |
| The type checker or semantic graph | `engine/crates/rocky-compiler/src/` |
| A diagnostic code (error/warning) | `engine/crates/rocky-compiler/src/diagnostic.rs` |
| Incremental / watermark logic | `engine/crates/rocky-core/src/state.rs` + `sql_gen.rs` |
| The `rocky run` execution loop | `engine/crates/rocky-cli/src/commands/run.rs` |
| DAG / dependency resolution | `engine/crates/rocky-ir/src/dag.rs` |
| Data contracts enforcement | `engine/crates/rocky-core/src/contracts.rs` |
| Schema drift detection | `engine/crates/rocky-core/src/drift.rs` |
| Masking strategies | `engine/crates/rocky-core/src/masking.rs` |
| Permission / role graph | `engine/crates/rocky-core/src/role_graph.rs` |
| SCD-2 snapshot logic | `engine/crates/rocky-core/src/snapshots.rs` |
| Quality checks | `engine/crates/rocky-core/src/checks.rs` |
| Hooks / webhooks | `engine/crates/rocky-core/src/hooks/` |
| Column lineage extraction | `engine/crates/rocky-sql/src/lineage.rs` |
| Skip-unchanged gate / hashing | `engine/crates/rocky-sql/src/determinism.rs` + `rocky-ir/src/ir.rs::skip_hash()` |
| CLI JSON output shape | `engine/crates/rocky-cli/src/output.rs` — then run `just codegen` |
| A new CLI subcommand | See the `rocky-new-cli-command` skill in `.claude/skills/` |
| The Rocky DSL (`.rocky` files) | `engine/crates/rocky-lang/src/` — then update VS Code grammar too |
| The LSP server | `engine/crates/rocky-server/src/lsp.rs` |
| The Dagster integration | `integrations/dagster/src/dagster_rocky/` |
| The Python SDK | `sdk/python/src/rocky_sdk/client.py` |
| The VS Code extension | `editors/vscode/src/` |

### Key invariants before you commit

- **Never** commit a Rust `*Output` change without running `just codegen` — `codegen-drift.yml` CI will fail.
- **Never** `format!()` untrusted input into SQL — use `rocky-sql/validation.rs` to validate identifiers first.
- **Never** hand-edit files under `*/types_generated/` or `*/types/generated/` — those are codegen outputs.
- **Never** skip hooks (`--no-verify`). Fix the underlying issue instead.

## Monorepo Structure

Rocky is a monorepo with five subprojects:

```
rocky-data/
├── engine/                     # Rust CLI + engine (Cargo workspace)
├── sdk/python/                 # rocky-sdk Python client
├── integrations/dagster/       # dagster-rocky Python package
├── editors/vscode/             # VS Code extension (LSP client)
├── examples/playground/        # POC catalog + benchmarks
├── docs/                       # Documentation site (Astro + Starlight)
├── justfile                    # Cross-project build orchestration
└── CLAUDE.md                   # Monorepo conventions
```

For the full crate-level breakdown of `engine/`, see [Architecture](/concepts/architecture/).

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

### rocky-sdk (Python)

```bash
cd rocky-data/sdk/python

# Install with dev dependencies
uv sync --dev

# Run tests
uv run pytest

# Lint
uv run ruff check
uv run ruff format --check
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
- **Edition**: 2024 (MSRV 1.88)
- **Error handling**: `thiserror` for library errors, `anyhow` for binary/CLI errors
- **Logging**: `tracing` crate (not `println!`)
- **SQL safety**: All identifiers validated via `rocky-sql/validation.rs` before interpolation
- **Tests**: In the same file (`#[cfg(test)] mod tests`)
- **Public types**: Must derive `Debug`, `Clone`, `Serialize`, `Deserialize` where applicable

### Python
- Target Python 3.11+
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
| `engine-bench.yml` | PRs labeled `perf` | criterion benchmarks; raw bencher output uploaded as an artifact (no baseline comparison) |
| `sdk-ci.yml` | `sdk/python/**` changes | pytest + ruff |
| `sdk-release.yml` | `sdk-v*` tag | PyPI publish via OIDC |
| `dagster-ci.yml` | `integrations/dagster/**` changes | pytest + ruff |
| `dagster-release.yml` | `dagster-v*` tag | PyPI publish via OIDC |
| `vscode-ci.yml` | `editors/vscode/**` changes | npm test + eslint |
| `vscode-release.yml` | `vscode-v*` tag | VS Code Marketplace publish |
| `codegen-drift.yml` | Any subproject | Validates committed bindings match `just codegen` output |

## Releases

Tag-namespaced: each artifact ships independently. All four are **CI-driven**: land a release PR (version bump + CHANGELOG entry), tag the merged commit, push the tag.

| Artifact | Tag | Workflow |
|---|---|---|
| Rocky CLI binary | `engine-v*` | `engine-release.yml` — 5-target matrix (macOS ARM64/Intel, Linux x86_64/ARM64, Windows) |
| rocky-sdk wheel | `sdk-v*` | `sdk-release.yml` — PyPI publish via OIDC |
| dagster-rocky wheel | `dagster-v*` | `dagster-release.yml` — PyPI publish via OIDC |
| Rocky VSIX | `vscode-v*` | `vscode-release.yml` — VS Code Marketplace publish |

Release **rocky-sdk before any dagster-rocky release that raises its `rocky-sdk>=…` floor** — the published `dagster-rocky` wheel resolves the SDK from PyPI, not the monorepo path source.

```bash
git tag engine-v<version>
git push origin engine-v<version>   # CI builds + publishes
```

The `scripts/release.sh` helper remains as a **local-build fallback** for hotfix scenarios; `just release-engine <version>`, `just release-sdk <version> [--publish]`, `just release-dagster <version> [--publish]`, and `just release-vscode <version> [--publish]` wrap it.
