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

One module per feature in `src/dagster_rocky/` (`resource.py`, `component.py`, `translator.py`, `checks.py`, `sensor.py`, and others). `types.py` and `types_generated.py` are shims over `rocky_sdk`. Tests sit in `tests/`, one `test_*.py` per module. `tests/scenarios.py` holds hand-written data and `tests/fixtures_generated/` holds live-binary captures. The full tree is in [`AGENTS.md`](AGENTS.md#project-structure).

## Architecture

dagster-rocky has four layers, each usable independently:

### 1. Types (`types.py` + `types_generated/`)

The Pydantic models and `parse_rocky_output()` live in `rocky-sdk` (`sdk/python/src/rocky_sdk/`). `types.py` and `types_generated.py` here are backward-compatibility shims, so `from dagster_rocky import RunResult` keeps working. Edit models in the SDK, not here. See [`sdk/python/AGENTS.md`](../../sdk/python/AGENTS.md).

### 2. Resource (`resource.py`)

`RockyResource` is a Dagster `ConfigurableResource`. It builds a `rocky_sdk.RockyClient` from its config and delegates each command method to it. It translates the SDK's `RockyError` hierarchy into `dagster.Failure`. The Dagster-specific parts stay here: per-call resolvers, the strict-doctor startup gate, `context.log` streaming, and Dagster Pipes.

The configuration fields and every method are listed in the [RockyResource docs](https://rocky-data.dev/dagster/resource/).

### 3. Translator (`translator.py`)

`RockyDagsterTranslator` maps Rocky sources, tables, models, and DAG nodes to Dagster asset keys, groups, tags, and metadata. Subclass it to change naming without touching execution code. The methods and defaults are in the [Translator docs](https://rocky-data.dev/dagster/assets/#customize-with-a-translator).

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

`emit_materializations()`, `emit_check_results()`, `check_metadata()`, `load_rocky_assets()` and `cost_metadata_from_optimize()` serve custom assets outside `RockyComponent`. See the [README](README.md#public-api).

## Common Commands

```bash
uv run pytest -v                                   # all tests
uv run pytest tests/test_types.py::test_parse_discover -v   # one test
uv run ruff check src/ tests/                      # lint (--fix to auto-fix)
uv run ruff format src/ tests/                     # format (--check for CI mode)
uv build                                           # build distribution
```

## Testing

Tests read hand-written scenarios from `tests/scenarios.py` and live-binary captures from `tests/fixtures_generated/`. No Rocky binary or credentials are needed to run tests. Everything is mocked or fixture-based. `just regen-fixtures` recaptures the generated fixtures; it does need the binary.

**Adding a new Rocky command:** follow the `rocky-new-cli-command` skill (`.agents/skills/rocky-new-cli-command/SKILL.md`). It is the single checklist; Steps 5 and 6 cover the SDK and this package.

## Git Hooks

The monorepo root `.git-hooks/` holds the hooks that run. Install them from the root with `just install-hooks`. For this package, `pre-commit` runs `ruff format --check`, and `pre-push` runs `ruff check`. Each check runs only when the change touches its subproject.

The `integrations/dagster/.git-hooks/` directory predates the monorepo. Git does not run it.

## Code Style and CI

[Ruff](https://docs.astral.sh/ruff/) enforces style: line length 100, target Python 3.11, rules `E`, `F`, `I`, `N`, `UP`, `B`, `SIM`. Use `from __future__ import annotations` in every module, type hints on public functions, and Pydantic `BaseModel` for Rocky data types.

CI runs on push to `main` and on pull requests: `uv sync --dev`, `uv run python -m pytest -v`, `uv run python -m ruff check src/ tests/`, and `uv run python -m ruff format --check src/ tests/`. All must pass. Commits follow the conventional-commit rules in the [monorepo `AGENTS.md`](../../AGENTS.md#git-conventions).

## Versioning

`dagster-rocky` versions on its own, with `dagster-v*` tags. Its version does not track the engine version. The published wheel resolves `rocky-sdk` from PyPI through the `rocky-sdk>=` floor in `pyproject.toml`. Release `rocky-sdk` before any `dagster-rocky` release that raises that floor.
