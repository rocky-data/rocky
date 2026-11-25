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

The four layers (types, resource, translator, component) and the three `rocky run` modes are described in [`AGENTS.md`](AGENTS.md#architecture). The type models live in `rocky-sdk`: edit them there. The configuration fields and every method are in the [RockyResource docs](https://rocky-data.dev/dagster/resource/). The translator methods and defaults are in the [Translator docs](https://rocky-data.dev/dagster/assets/#customize-with-a-translator).

### Component helpers

`RockyComponent` is state-backed: a state refresh runs `rocky discover`, plus `rocky compile` and `rocky optimize` when `models_dir` exists, and caches the results in a JSON state file. The body of the component is small. The work is split across module-level helpers so each piece can be tested in isolation:

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

The design decisions are in [`AGENTS.md`](AGENTS.md#key-design-decisions). Compile diagnostics are logged during execution, so errors appear in asset logs.

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

No Rocky binary or credentials are needed: tests read hand-written scenarios and live-binary captures (see [`AGENTS.md`](AGENTS.md#testing)). `just regen-fixtures` recaptures the generated fixtures and does need the binary. To add a Rocky command, follow the `rocky-new-cli-command` skill (`.agents/skills/rocky-new-cli-command/SKILL.md`): Steps 5 and 6 cover the SDK and this package.

## Git Hooks

The monorepo root `.git-hooks/` holds the hooks that run. Install them from the root with `just install-hooks`. For this package, `pre-commit` runs `ruff format --check`, and `pre-push` runs `ruff check`. Each check runs only when the change touches its subproject.

The `integrations/dagster/.git-hooks/` directory predates the monorepo. Git does not run it.

## Code Style and CI

Style rules are in [`AGENTS.md`](AGENTS.md#coding-standards). CI runs on push to `main` and on pull requests: `uv sync --dev`, `uv run python -m pytest -v`, `uv run python -m ruff check src/ tests/`, and `uv run python -m ruff format --check src/ tests/`. All must pass. Commits follow [Git conventions](../../AGENTS.md#git-conventions).

## Versioning

`dagster-rocky` versions on its own, with `dagster-v*` tags. Its version does not track the engine version. The published wheel resolves `rocky-sdk` from PyPI through the `rocky-sdk>=` floor in `pyproject.toml`. Release `rocky-sdk` before any `dagster-rocky` release that raises that floor.
