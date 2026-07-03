# dagster-rocky

Dagster integration for the Rocky SQL transformation engine. A thin Dagster
adapter over [`rocky-sdk`](../../sdk/python/CLAUDE.md)'s `RockyClient` that maps
Rocky's discovery/execution output into Dagster assets, checks, and
materializations. `dagster-rocky` depends on `rocky-sdk` (in-repo: a
`[tool.uv.sources]` path dependency; published: a `rocky-sdk>=…` floor).

## Architecture

Four layers, each independently usable:

1. **Types** (`types.py`) — a backward-compat shim. The Pydantic models and
   `parse_rocky_output()` now live in `rocky_sdk.types`; `dagster_rocky.types`
   re-exports the full surface so existing `from dagster_rocky import RunResult`
   / `parse_rocky_output` imports keep working. `dagster_rocky.types_generated`
   is likewise a shim over `rocky_sdk.types_generated`. To add support for a new
   command, edit the Rust output struct in `engine/crates/rocky-cli/src/output.rs`,
   run `just codegen-sdk` from the monorepo root, then add a dispatch entry in
   `rocky_sdk`'s `parse_rocky_output()`.
2. **Resource** (`resource.py`) — `RockyResource`, a `ConfigurableResource` that
   builds a `rocky_sdk.RockyClient` from its config (`_get_client()`) and
   delegates each command method to it inside a `_translating()` boundary that
   converts the SDK's `RockyError` hierarchy into `dagster.Failure` (rebuilding
   the same descriptions + metadata operators are used to). The client is
   constructed with `mirror_stderr=True` + the `dagster_rocky` logger so the
   engine's stderr is preserved by Dagster's compute-log capture and lifecycle
   lines keep their provenance. The Dagster-specific concerns stay here: per-call
   resolvers, the strict-doctor startup gate, the governance pre-flight (raises
   `dg.Failure` directly), live `context.log` streaming, and Dagster Pipes. Three
   execution modes for `rocky run`, all sharing the `_build_run_args` argv builder
   (which delegates to the client) so flags stay in sync:
   - **`run(filter, ...)`** — buffered `subprocess.run`. No Dagster context. Returns `RunResult`. For scripts / tests / notebooks.
   - **`run_streaming(context, filter, ...)`** — `subprocess.Popen` with a daemon reader thread that forwards stderr to `context.log.info` line-by-line. Returns `RunResult`. Captures the last 20 stderr lines into `dg.Failure.stderr_tail` on hard failure. Doesn't depend on engine Pipes support — works against any rocky binary. **`RockyComponent` uses this by default** via `_run_filters`.
   - **`run_pipes(context, filter, ...)`** — full Dagster Pipes integration via `dg.PipesSubprocessClient`. The engine detects `DAGSTER_PIPES_CONTEXT`/`DAGSTER_PIPES_MESSAGES` and emits `report_asset_materialization` / `report_asset_check` / `log` messages — Dagster surfaces them as `MaterializationEvent` / `AssetCheckEvaluation` in the run viewer. Returns a `PipesClientCompletedInvocation`; users chain `.get_results()`. Canonical Dagster Pipes pattern. Optional `pipes_client` kwarg for custom env / cwd / message_reader.
3. **Translator** (`translator.py`) — `RockyDagsterTranslator` maps Rocky metadata → Dagster asset keys, groups, tags, metadata. Subclass to customize.
4. **Component** (`component.py`) — `RockyComponent` state-backed Dagster component. Caches `rocky discover` + `rocky compile` output. Subset-aware multi-asset execution. Internally split into small helpers (`_load_state`, `_build_group_contexts`, `_select_filters`, `_run_filters`, `_emit_results`, `_emit_placeholder_checks`).

Helper modules:
- `assets.py` — `load_rocky_assets()` functional API for custom asset definitions
- `checks.py` — `emit_materializations()`, `emit_check_results()`, `check_metadata()`, `cost_metadata_from_optimize()` event helpers
- `automation.py` — Dagster automation rules derived from Rocky config
- `branch_deploy.py` — Branch deployment support for dev/staging pipelines
- `column_lineage.py` — Column-level lineage mapping to Dagster metadata
- `contracts.py` — Data contract enforcement via Dagster asset checks
- `derived_models.py` — Silver-layer derived model asset generation
- `freshness.py` — FreshnessPolicy projection from discover checks
- `health.py` — Health check integration (wraps `rocky doctor`)
- `observability.py` — Observability event helpers
- `partitions.py` — Time-partitioned asset support
- `scaffold.py` — Project scaffolding utilities
- `schedules.py` — Schedule definitions from Rocky config
- `sensor.py` — Sensor definitions (freshness, drift detection)

## Project Structure

```
src/dagster_rocky/
├── __init__.py          # Public API exports
├── types.py             # Backward-compat shim — re-exports rocky_sdk.types (see Architecture §1)
├── types_generated.py   # Backward-compat shim — re-exports rocky_sdk.types_generated (single file)
│
├── resource.py          # RockyResource (ConfigurableResource)
├── translator.py        # RockyDagsterTranslator (asset key/group/tag mapping)
├── component.py         # RockyComponent (state-backed, subset-aware)
├── assets.py            # load_rocky_assets() functional helper
├── dag_assets.py        # DAG-mode asset generation from `rocky dag`
├── checks.py            # Event emission helpers
│
├── automation.py        # Dagster automation rules from Rocky config
├── branch_deploy.py     # Branch deployment support
├── column_lineage.py    # Column-level lineage mapping
├── contracts.py         # Data contract enforcement
├── derived_models.py    # Silver-layer derived model assets
├── freshness.py         # FreshnessPolicy projection from discover.checks
├── health.py            # Health check integration
├── observability.py     # Observability event helpers
├── partitions.py        # Time-partitioned asset support
├── scaffold.py          # Project scaffolding utilities
├── schedules.py         # Schedule definitions from Rocky config
└── sensor.py            # Sensor definitions (e.g. freshness, drift)
tests/
├── conftest.py                  # Pytest fixtures — json.dumps over scenarios.py dicts
├── scenarios.py                 # Hand-crafted test data as Python dicts (source of truth for tests)
├── fixtures_generated/          # Live-binary fixtures from `just regen-fixtures` (parsed by test_generated_fixtures.py)
├── test_automation.py
├── test_branch_approval.py
├── test_branch_deploy.py
├── test_checks.py
├── test_column_lineage.py
├── test_component.py
├── test_contracts.py
├── test_dag_assets.py
├── test_derived_models.py
├── test_emit_results_dedup.py
├── test_execution.py
├── test_freshness.py
├── test_generated_fixtures.py   # Validates live-binary fixtures parse correctly
├── test_health.py
├── test_observability.py
├── test_partitions.py
├── test_pipes_mode.py
├── test_resolvers.py
├── test_resource.py             # Subprocess + HTTP plumbing tests
├── test_scaffold.py
├── test_schedules.py
├── test_sensor.py
├── test_state_health.py
├── test_strict_doctor.py
├── test_translator.py
└── test_types.py                # Pydantic model parsing
```

## Coding Standards

### Python
- Python 3.11+, use `from __future__ import annotations` in all modules
- Pydantic `BaseModel` for all data structures (not dataclasses/TypedDicts)
- Type hints on all public functions
- Line length: 100 characters
- Ruff rules: E, F, I, N, UP, B, SIM

### Dependencies
- `dagster>=1.13.0` — Orchestration framework
- `pydantic>=2.0` — Data validation
- `pygments>=2.20.0` — Syntax highlighting for error display
- Dev: `pytest>=8.0`, `ruff>=0.4`

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
uv run ruff format --check src/ tests/        # Check only (CI)

# Build
uv build                                      # Build wheel + sdist
```

## Testing

All tests run without the Rocky binary or credentials — they use hand-crafted Python dicts defined in `tests/scenarios.py` (exposed as `*_json` pytest fixtures via `conftest.py`, which `json.dumps` each scenario). The legacy `tests/fixtures/*.json` directory was removed when scenarios.py became the test source of truth — see `conftest.py` for the migration notes. Live-binary captures under `tests/fixtures_generated/` (refreshed via `just regen-fixtures`) are parsed by `test_generated_fixtures.py` for drift detection but are not loaded by other tests.

**Translator scenario coverage.** The default `RockyDagsterTranslator` is deliberately generic — it has no awareness of any specific source type or component hierarchy. `tests/test_translator.py` exercises it against a parametrized matrix of source shapes to prove the genericity and pin down edge-case behavior:

- `fivetran_deep` — 3-level `{tenant, region, source}` hierarchy (a common multi-tenant layout)
- `fivetran_list_valued_region` — same shape but with a list-valued `regions` component
- `airbyte_flat` — 2-level flat `{environment, connector}` hierarchy
- `manual_single_component` — single `{dataset}` component (smallest valid case)

Plus two edge-case tests: the all-lists fallback path for `get_group_name` (falls back to `source_type`) and a pinning test for the dict-iteration-order dependency of the "first string component" default. When adding support for a new source type or hierarchy shape that materially differs from the above, add a new entry to `SCENARIOS` in `test_translator.py` rather than writing a bespoke test — parametrization keeps the matrix honest and visible. The multi-source-type parsing surface is additionally covered by `DISCOVER_MULTI_SOURCE_TYPE` in `scenarios.py` / `test_parse_discover_multi_source_type` in `test_types.py`.

**Adding support for a new Rocky CLI command (codegen-driven):**
1. Add the typed `*Output` struct in `engine/crates/rocky-cli/src/output.rs` (or `commands/<name>.rs`) deriving `JsonSchema`.
2. Register it in `engine/crates/rocky-cli/src/commands/export_schemas.rs::schemas()`.
3. From the monorepo root, run `just codegen-sdk`. This regenerates `types_generated/<command>_schema.py` and refreshes the `__init__.py` barrel.
4. Re-export the new type from `types.py` (in the re-export section near the bottom).
5. Add a route in `parse_rocky_output()` to dispatch the new command.
6. Add a scenario entry in `tests/scenarios.py` (a Python dict with the expected output shape) and a matching `*_json` fixture in `conftest.py`.
7. Add parsing tests in `test_types.py`.
8. Add a method to `RockyResource` in `resource.py`.
9. Optionally extend `scripts/regen_fixtures.sh` with a `capture` call so the playground POC contributes a live-binary sample under `tests/fixtures_generated/`.

## Git Conventions

- **Never** include `Co-Authored-By` trailers in commit messages
- Conventional commits: `feat:`, `fix:`, `refactor:`, `test:`, `docs:`, `chore:`
- Git hooks in `.git-hooks/`: pre-commit (ruff), commit-msg (conventional commit validation), pre-push (pytest)

## Key Design Decisions

- **State-backed component**: `rocky discover` cached to avoid calling Rocky on every Dagster code server restart. Compile is best-effort: a `dg.Failure` from `rocky compile` does not block discovery state from being written.
- **Partial success**: Rocky can exit non-zero but return valid JSON — resource falls back to stdout parsing when `allow_partial=True` and stdout starts with `{`.
- **Subset-aware**: Multi-asset `can_subset=True`. The component picks a per-source `id=<source_id>` filter when only some sources are selected, and the group's natural filter when all are.
- **Selection / declared-check filtering**: Rocky always runs at source granularity, so a single `rocky run` may emit results for tables outside the requested subset, and may emit check kinds (e.g. `null_rate`) the component never declared. `_emit_results` drops both, otherwise Dagster raises `DagsterInvariantViolationError`.
- **HTTP fallback**: compile/lineage/metrics support `server_url` for live Rocky server mode.
- **Backward compatible state**: Reads both legacy raw DiscoverResult and current `{discover, compile}` format.
