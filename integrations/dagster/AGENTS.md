# dagster-rocky

Dagster integration for the Rocky SQL engine: a thin adapter over [`rocky-sdk`](../../sdk/python/AGENTS.md)'s `RockyClient` that maps Rocky's discovery and execution output into Dagster assets, checks and materializations. It depends on `rocky-sdk` (in-repo: a `[tool.uv.sources]` path dependency; published: a `rocky-sdk>=…` floor).

## Architecture

Four layers, each independently usable:

1. **Types** (`types.py`): a backward-compat shim. The Pydantic models and `parse_rocky_output()` live in `rocky_sdk.types`; `dagster_rocky.types` and `dagster_rocky.types_generated` re-export them so existing imports keep working. For a new command, follow the `rocky-new-cli-command` skill.
2. **Resource** (`resource.py`): `RockyResource`, a `ConfigurableResource` that builds a `rocky_sdk.RockyClient` (`_get_client()`) and delegates each command to it inside a `_translating()` boundary that converts the SDK's `RockyError` hierarchy into `dagster.Failure`. The client uses `mirror_stderr=True` and the `dagster_rocky` logger, so Dagster's compute-log capture keeps engine stderr. Dagster-only concerns stay here: per-call resolvers, `timeout_seconds`, the strict-doctor startup gate, the governance pre-flight (raises `dg.Failure` directly), live `context.log` streaming and Pipes. `rocky run` has three modes, all sharing the `_build_run_args` argv builder so flags stay in sync:
   - **`run(filter, ...)`**: buffered `subprocess.run`, no Dagster context, returns `RunResult`. For scripts, tests, notebooks.
   - **`run_streaming(context, filter, ...)`**: `subprocess.Popen` with a daemon thread forwarding stderr to `context.log.info`. Returns `RunResult`. Keeps the last 20 stderr lines in `dg.Failure.stderr_tail` on hard failure. **`RockyComponent` uses this by default** via `_run_filters`.
   - **`run_pipes(context, filter, ...)`**: Dagster Pipes via `dg.PipesSubprocessClient`. The engine detects `DAGSTER_PIPES_CONTEXT` / `DAGSTER_PIPES_MESSAGES` and emits `report_asset_materialization` / `report_asset_check` / `log` messages. Returns a `PipesClientCompletedInvocation`; chain `.get_results()`. Optional `pipes_client` kwarg (env, cwd, message_reader).
3. **Translator** (`translator.py`) — `RockyDagsterTranslator` maps Rocky metadata → Dagster asset keys, groups, tags, metadata. Subclass to customize.
4. **Component** (`component.py`) — `RockyComponent` state-backed Dagster component. Caches `rocky discover` + `rocky compile` output. Subset-aware. Helpers: `_load_state`, `_build_group_contexts`, `_select_filters`, `_run_filters`, `_emit_results`, `_emit_placeholder_checks`.

## Project Structure

```
src/dagster_rocky/
├── __init__.py          # Public API
├── types.py             # Shim — re-exports rocky_sdk.types
├── types_generated.py   # Shim — re-exports rocky_sdk.types_generated
│
├── resource.py          # RockyResource (ConfigurableResource)
├── translator.py        # RockyDagsterTranslator (asset key/group/tag mapping)
├── component.py         # RockyComponent (state-backed, subset-aware)
├── assets.py            # load_rocky_assets() functional helper
├── dag_assets.py        # DAG-mode asset generation from `rocky dag`
├── checks.py            # emit_materializations, emit_check_results, check_metadata, cost_metadata_from_optimize
│
├── automation.py        # Dagster automation rules from Rocky config
├── branch_deploy.py     # Branch deployment support
├── column_lineage.py    # Column-level lineage mapping
├── contracts.py         # Data contract enforcement
├── derived_models.py    # Silver-layer derived model assets
├── freshness.py         # FreshnessPolicy projection from discover.checks
├── health.py            # Health check integration (wraps `rocky doctor`)
├── observability.py     # Observability event helpers
├── partitions.py        # Time-partitioned asset support
├── scaffold.py          # Project scaffolding utilities
├── schedules.py         # Schedule definitions from Rocky config
└── sensor.py            # Sensors: freshness, drift detection
tests/
├── conftest.py                  # Pytest fixtures — json.dumps over scenarios.py dicts
├── scenarios.py                 # Hand-crafted test dicts (source of truth)
├── fixtures_generated/          # Live-binary fixtures from `just regen-fixtures`
├── fixtures/pipes_wire/          # Pipes wire captures (*.jsonl) for test_pipes_mode.py
├── test_generated_fixtures.py   # Validates live-binary fixtures parse correctly
├── test_resource.py             # Subprocess + HTTP plumbing
├── test_types.py                # Pydantic model parsing
└── test_<area>.py               # One file per feature area; run `ls tests/` for the full list
```

## Coding Standards

### Python
- Python 3.11+, `from __future__ import annotations` in all modules
- Pydantic `BaseModel` for data structures (not dataclasses/TypedDicts)
- Type hints on public functions
- Line length: 100 characters
- Ruff rules: E, F, I, N, UP, B, SIM

### Dependencies

The version floors are in `pyproject.toml`. Read them there; do not copy them here.

- `rocky-sdk` — the typed client this package wraps (release it first when the floor rises)
- `dagster`, `pydantic`, `pygments` (error display). Dev: `pytest`, `ruff`

## Common Commands

```bash
# Setup
uv sync --dev
git config core.hooksPath .git-hooks

# Test
uv run pytest -v                              # All tests
uv run pytest tests/test_types.py -v          # Single file
uv run pytest tests/test_types.py::test_parse_discover -v  # Single test

# Lint & Format
uv run ruff check src/ tests/                 # Lint
uv run ruff check --fix src/ tests/           # Auto-fix
uv run ruff format src/ tests/                # Format
uv run ruff format --check src/ tests/        # Own CI gate (pytest green ≠ format green)

# Build
uv build                                      # Build wheel + sdist
```

## Testing

All tests run without the Rocky binary or credentials. They use hand-crafted dicts in `tests/scenarios.py`, exposed as `*_json` fixtures by `conftest.py`. `tests/fixtures/pipes_wire/` holds recorded Pipes wire captures, not command output. Fixture lifecycle (`tests/fixtures_generated/`, `just regen-fixtures`): see the `rocky-test-fixtures` skill.

**Translator coverage.** `RockyDagsterTranslator` is generic. `tests/test_translator.py` runs a parametrized `SCENARIOS` matrix: `fivetran_deep` (`{tenant, region, source}`), `fivetran_list_valued_region`, `airbyte_flat` (`{environment, connector}`), `manual_single_component` (`{dataset}`). Two edge tests cover the all-lists `get_group_name` fallback (to `source_type`) and the dict-order dependency of the "first string component" default. For a new source shape, add a `SCENARIOS` entry, not a bespoke test. `DISCOVER_MULTI_SOURCE_TYPE` in `scenarios.py` and `test_parse_discover_multi_source_type` in `test_types.py` cover multi-source parsing.

**New Rocky CLI command.** Follow the `rocky-new-cli-command` skill. Parsing and the typed method live in `rocky-sdk` (its "Step 5"). The Dagster part is its "Step 6": a delegating `RockyResource` method, a `tests/scenarios.py` dict with a `*_json` fixture in `conftest.py`, tests, and optionally a capture in `scripts/regen_fixtures.sh`.

## Git Conventions

Follow "Git conventions" in the root `../../AGENTS.md`. Scope: `feat(dagster)`. Hooks in `.git-hooks/`: pre-commit (ruff), commit-msg (conventional commit validation), pre-push (pytest).

## Key Design Decisions

- **State-backed component**: `rocky discover` is cached, so a code server restart does not call Rocky. Compile is best-effort: a `dg.Failure` from `rocky compile` does not block writing discovery state.
- **Partial success**: Rocky can exit non-zero with valid JSON. The resource parses stdout when `allow_partial=True` and stdout starts with `{`.
- **Subset-aware**: multi-asset `can_subset=True`. Some sources selected: a per-source `id=<source_id>` filter. All selected: the group's natural filter.
- **Selection / declared-check filtering**: Rocky runs at source granularity. One `rocky run` may emit results for tables outside the subset, and check kinds (e.g. `null_rate`) the component never declared. Both are filtered out of the asset-check stream. Dagster raises `DagsterInvariantViolationError` on a result outside the declared `check_specs`, which fails the step. **Both paths filter on the same `(asset_key, check_name)` set**: `_emit_results` (streaming) and `_run_filters_pipes` (pipes; it once filtered by asset key only, #1673). `_undeclared_check_observation` logs a warning and carries the verdict as an `AssetObservation`.
- **Group checks**: `cross_source_overlap` runs once per `(source_type, table)` sibling group. The engine reports it on the sibling it copied first. This is not knowable at spec-build time. Its specs carry `GROUP_CHECK_METADATA_KEY`. `_emit_placeholder_checks` yields **nothing** for the other siblings, only one INFO line naming the carrier. A passing placeholder would be a green verdict for a check that never ran. A failing one would warn on every run. Dagster records the unevaluated check as `PLANNED`; `AssetCheckExecutionRecord.resolve_status` maps it to `SKIPPED` on a finished, non-failed run (#1669).
- **HTTP fallback**: compile/lineage/metrics support `server_url` for live Rocky server mode.
- **Backward compatible state**: reads the legacy raw DiscoverResult and the current `{discover, compile}` format.
