# Development Guide

## Prerequisites

- **Python 3.11+**
- **[uv](https://docs.astral.sh/uv/)** — package manager
- **[Rocky CLI](https://github.com/rocky-data/rocky)** — installed and on `$PATH` (for integration testing)
- **Git** — for hooks and version control

## Getting Started

```bash
git clone https://github.com/rocky-data/rocky.git
cd rocky/integrations/dagster

uv sync --dev

# Optional, from the monorepo root: install the shared git hooks
just install-hooks
```

## Project Structure

```
integrations/dagster/            # path inside the rocky-data/rocky monorepo
├── src/dagster_rocky/
│   ├── __init__.py          # Public API exports
│   ├── py.typed             # PEP 561 type marker
│   ├── types.py             # Shim: re-exports rocky_sdk.types (models + parse_rocky_output)
│   ├── types_generated.py   # Shim: re-exports rocky_sdk.types_generated
│   ├── resource.py          # RockyResource — ConfigurableResource wrapping Rocky CLI
│   ├── translator.py        # RockyDagsterTranslator — customizable Rocky→Dagster mapping
│   ├── component.py         # RockyComponent — state-backed Dagster component
│   ├── assets.py            # load_rocky_assets() — functional asset discovery
│   ├── checks.py            # emit_materializations(), emit_check_results()
│   ├── automation.py        # Dagster automation rules from Rocky config
│   ├── branch_deploy.py     # Branch deployment support
│   ├── column_lineage.py    # Column-level lineage mapping
│   ├── contracts.py         # Data contract enforcement
│   ├── dag_assets.py        # DAG-mode asset builder (rocky dag)
│   ├── derived_models.py    # Silver-layer derived model assets
│   ├── freshness.py         # FreshnessPolicy projection
│   ├── health.py            # Health check integration
│   ├── observability.py     # Observability event helpers
│   ├── partitions.py        # Time-partitioned asset support
│   ├── scaffold.py          # Project scaffolding utilities
│   ├── schedules.py         # Schedule definitions
│   └── sensor.py            # rocky_source_sensor (source sync events)
├── tests/
│   ├── conftest.py              # Pytest fixtures (loads from scenarios.py)
│   ├── scenarios.py             # Hand-crafted test data as Python dicts
│   ├── fixtures/                # Static fixtures (Pipes wire captures)
│   ├── fixtures_generated/      # Live-binary fixtures from `just regen-fixtures`
│   └── test_*.py                # One or more test modules per source module
├── scripts/
│   └── dev_setup.py         # Development environment setup
├── .git-hooks/              # Pre-commit, commit-msg, pre-push hooks
├── pyproject.toml           # Project config, deps, ruff, pytest
└── uv.lock                  # Locked dependency versions
```

## Architecture

dagster-rocky has four layers, each usable independently:

### 1. Types (`types.py` + `types_generated/`)

The Pydantic models and `parse_rocky_output()` live in `rocky-sdk` (`sdk/python/src/rocky_sdk/`). `types.py` and `types_generated.py` here are backward-compatibility shims, so `from dagster_rocky import RunResult` keeps working. Edit models in the SDK, not here. See [`sdk/python/AGENTS.md`](../../sdk/python/AGENTS.md).

### 2. Resource (`resource.py`)

`RockyResource` is a Dagster `ConfigurableResource`. It builds a `rocky_sdk.RockyClient` from its config and delegates each command method to it. It translates the SDK's `RockyError` hierarchy into `dagster.Failure`. The Dagster-specific parts stay here: per-call resolvers, the strict-doctor startup gate, `context.log` streaming, and Dagster Pipes.

The configuration fields and every method are listed in the [RockyResource docs](https://rocky-data.dev/dagster/resource/).

### 3. Translator (`translator.py`)

`RockyDagsterTranslator` maps Rocky sources, tables, models, and DAG nodes to Dagster asset keys, groups, tags, and metadata. Subclass it to change naming without touching execution code. The methods and defaults are in the [Translator docs](https://rocky-data.dev/dagster/translator/).

### 4. Component (`component.py`)

`RockyComponent` is a state-backed Dagster component. A state refresh runs `rocky discover`, plus `rocky compile` and `rocky optimize` when `models_dir` exists, and caches the results in a JSON state file. Loading the definitions reads that file and builds `multi_asset` definitions from it. At execution time, it calls `rocky run` for the selected subset of tables and yields `MaterializeResult` and `AssetCheckResult` events.

The body of the component is intentionally small. The work is split across module-level helpers so each piece can be tested in isolation:

| Helper | Responsibility |
|--------|----------------|
| `_load_state` | Parse the cached JSON, supporting both legacy and current formats |
| `_build_group_contexts` | Walk the discover output and accumulate one `_GroupBuild` per Dagster group |
| `_build_asset_spec` | Build a single `AssetSpec` for one Rocky table |
| `_build_check_specs` | Pre-declare the default, contract, compliance, and configured checks |
| `_make_rocky_asset` | Wrap a group as a subset-aware `multi_asset` |
| `_select_filters` | Decide between the group-level filter and per-source filters |
| `_run_filters` | Invoke `rocky run` and emit per-run log lines |
| `_log_run_diagnostics` | Surface errors / drift / anomalies / contract violations to the asset log |
| `_emit_results` | Translate run results into `MaterializeResult` + `AssetCheckResult` events, dropping anything outside the requested subset or outside the declared check_specs |
| `_emit_placeholder_checks` | Yield placeholders for declared checks that Rocky did not produce |

Key design decisions:
- **State-backed**: Discover is cached so Dagster doesn't call Rocky on every code server restart
- **Subset-aware**: Users can materialize individual tables; only the needed sources are executed and results outside the subset are dropped
- **Declared-check filter**: Rocky may emit additional check kinds (e.g. `null_rate`); the component only surfaces checks present in `check_specs` so Dagster doesn't reject the run
- **Compile diagnostics**: Logged during execution so errors appear in asset logs
- **Backward compatible**: Reads both legacy (raw DiscoverResult) and current (discover + compile) state formats

### Helpers (`checks.py`, `assets.py`)

- `emit_materializations()` / `emit_check_results()` — convert Rocky run results to Dagster events (useful when building custom assets outside `RockyComponent`)
- `check_metadata()` — build a Dagster metadata mapping for a single `CheckResult`
- `load_rocky_assets()` — simple functional API that calls `rocky discover` and returns `AssetSpec` list
- `cost_metadata_from_optimize()` — extracts per-model cost recommendations from optimize results (compute cost, storage cost, recommended strategy, savings, downstream references, reasoning)

## Common Commands

```bash
# Run all tests
uv run pytest -v

# Run a specific test file
uv run pytest tests/test_types.py -v

# Run a specific test
uv run pytest tests/test_types.py::test_parse_discover -v

# Lint
uv run ruff check src/ tests/

# Auto-fix lint issues
uv run ruff check --fix src/ tests/

# Format
uv run ruff format src/ tests/

# Check formatting (CI mode)
uv run ruff format --check src/ tests/

# Build distribution
uv build
```

## Testing

Tests read hand-written scenarios from `tests/scenarios.py` and live-binary captures from `tests/fixtures_generated/`. No Rocky binary or credentials are needed to run tests. Everything is mocked or fixture-based. `just regen-fixtures` recaptures the generated fixtures; it does need the binary.

**Adding a new Rocky command:**

1. Add the typed `*Output` struct in `engine/crates/rocky-cli/src/output.rs` deriving `JsonSchema`.
2. Register it in `engine/crates/rocky-cli/src/commands/export_schemas.rs::schemas()`.
3. From the monorepo root, run `just codegen-sdk`. This regenerates `sdk/python/src/rocky_sdk/types_generated/`.
4. In `rocky_sdk/types.py`, re-export the new type if needed and add a `parse_rocky_output()` dispatch entry.
5. Add a typed method to `RockyClient` in `rocky_sdk/client.py`.
6. Add a delegating method to `RockyResource` in `resource.py`.
7. Run `just regen-fixtures` to capture a fresh fixture into `tests/fixtures_generated/`.
8. Add a scenario in `scenarios.py` and tests here.
9. If `resource.py` now imports a name the released SDK lacks, raise the `rocky-sdk>=` floor in `pyproject.toml`.

## Git Hooks

The monorepo root `.git-hooks/` holds the hooks that run. Install them from the root with `just install-hooks`. For this package, `pre-commit` runs `ruff format --check`, and `pre-push` runs `ruff check`. Each check runs only when the change touches its subproject.

The `integrations/dagster/.git-hooks/` directory predates the monorepo. Git does not run it.

## Code Style

Enforced by [Ruff](https://docs.astral.sh/ruff/) with these rules:

- **Line length**: 100 characters
- **Target**: Python 3.11
- **Rules**: `E` (pycodestyle), `F` (pyflakes), `I` (isort), `N` (pep8-naming), `UP` (pyupgrade), `B` (flake8-bugbear), `SIM` (flake8-simplify)

Additional conventions:
- Use `from __future__ import annotations` in all modules
- Type hints on all public functions
- Pydantic `BaseModel` for all data structures (no dataclasses or TypedDicts for Rocky types)

## CI

GitHub Actions runs on push to `main` and on pull requests:

1. `uv sync --dev` — install dependencies
2. `uv run python -m pytest -v` — run tests
3. `uv run python -m ruff check src/ tests/` — lint
4. `uv run python -m ruff format --check src/ tests/` — format check

All three must pass for merge.

## Git Conventions

- **Conventional commits**: `feat:`, `fix:`, `docs:`, `style:`, `refactor:`, `perf:`, `test:`, `build:`, `ci:`, `chore:`, `revert:`
- Optional scope: `feat(component): add compile diagnostics`
- Breaking changes: `feat!: rename RockyResource.execute to run`

## Versioning

`dagster-rocky` versions on its own, with `dagster-v*` tags. Its version does not track the engine version. The published wheel resolves `rocky-sdk` from PyPI through the `rocky-sdk>=` floor in `pyproject.toml`. Release `rocky-sdk` before any `dagster-rocky` release that raises that floor.

## Related Projects

- **[Rocky](https://github.com/rocky-data/rocky)** — the Rust SQL transformation engine (the CLI this library wraps)
- **[Rocky VS Code extension](https://github.com/rocky-data/rocky/tree/main/editors/vscode)** — VS Code extension with LSP, syntax highlighting, and AI features
