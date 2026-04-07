---
name: rocky-test-fixtures
description: Dagster integration test fixture lifecycle. Use when regenerating JSON fixtures via `just regen-fixtures` / `scripts/regen_fixtures.sh`, deciding between the parallel corpus and `--in-place` promotion, or debugging why a fixture differs across runs. Covers determinism (zeroed timings, sentinel timestamps), the `--in-place` flag, and the two POCs that back the corpus.
---

# Dagster test fixture regeneration

The dagster integration tests parse JSON fixtures captured from the live `rocky` binary. There are two parallel fixture directories with different roles — conflating them causes confusion, so this skill is worth reading before touching either.

## The two fixture directories

| Path | Role | Edit policy |
|---|---|---|
| `integrations/dagster/tests/fixtures/` | **Source of truth.** Hand-curated + refined corpus that `conftest.py` loads. | Edit carefully. Promote from `fixtures_generated/` only via `--in-place`. |
| `integrations/dagster/tests/fixtures_generated/` | **Parallel corpus.** Captured from the live binary against the playground POCs. Used for drift detection and future migration. | Regenerated on every `just regen-fixtures` run. Don't hand-edit. |

Today, tests consume `fixtures/`. The generated corpus is validated by `test_generated_fixtures.py` (which just checks it still parses with the current Pydantic models), but no test asserts byte equality between the two. That's deliberate — it lets the captured corpus drift ahead of the committed one while you're iterating, and you promote the delta intentionally with `--in-place` when you're ready.

## When to use this skill

- Changing any `*Output` struct that affects a fixture's shape (pairs with the `rocky-codegen` skill — codegen updates the Pydantic models; fixtures update the JSON they parse)
- Adding a brand new CLI command that needs a fixture for dagster tests
- Investigating why `test_types.py` fails against a new binary
- Debugging non-determinism in a fixture (timestamps, durations, row counts)

## Regeneration workflow

### Safe default — parallel corpus

```bash
just regen-fixtures
# or
./scripts/regen_fixtures.sh
```

Writes to `integrations/dagster/tests/fixtures_generated/`. The committed `fixtures/` are untouched. This is the command you want 95% of the time — it's reversible (just `rm -rf fixtures_generated/`), lets you eyeball the diff, and doesn't break existing tests.

### Promoting — destructive

```bash
./scripts/regen_fixtures.sh --in-place
```

**Overwrites** `integrations/dagster/tests/fixtures/`. Only do this when:
- You've reviewed the diff in `fixtures_generated/` and confirmed the shape change is intentional
- You're committing the fixture update in the same PR as the matching schema change (otherwise existing tests break)
- You're prepared to update `test_types.py` if field names changed

After `--in-place`, always run `uv run pytest -v` from `integrations/dagster/` to catch any parsing regressions before committing.

## Prerequisites

The script hard-fails if either is missing:

1. **Release binary at `engine/target/release/rocky`** — build with:
   ```bash
   cd engine && cargo build --release --bin rocky
   ```
   or run `just codegen` (which also builds in release mode and shares the artifact).

2. **`duckdb` CLI on `$PATH`** — used to seed the playground POC's DuckDB file:
   ```bash
   brew install duckdb        # macOS
   ```

## Two POCs back the corpus

`regen_fixtures.sh` runs the binary against **two** playground POCs, captured into different subdirectories of the output:

| POC | Purpose | Fixtures produced |
|---|---|---|
| `examples/playground/pocs/00-foundations/00-playground-default/` | Baseline `full_refresh` pipeline. Produces most fixtures. | `discover.json`, `plan.json`, `run.json`, `state.json`, `compile.json`, `test.json`, `ci.json`, `lineage.json`, `doctor.json`, `history.json`, `metrics.json`, `optimize.json` |
| `examples/playground/pocs/02-performance/03-partition-checksum/` | `time_interval` (partition-keyed) pipeline. Exercises partition-specific shapes. | `partition/compile.json`, `partition/run_single.json`, `partition/run_backfill.json`, `partition/run_late.json` |

If you need a fixture shape that neither POC produces naturally (e.g. `drift.json` from a non-zero schema diff), the script falls back to the **hand-written** fixtures in the committed `fixtures/` directory. Don't try to coerce the POCs into generating edge cases — hand-write the fixture instead and put it directly in `fixtures/`.

## Determinism: why captured JSON is stable across runs

Without post-processing, every regen would produce a slightly different diff because Rocky stamps:
- **Duration fields** with real wall-clock elapsed time (`_ms`, `_seconds`, `_secs` suffixes)
- **Timestamp fields** with `now()` (`updated_at`, `started_at`, `finished_at`, `timestamp`, `captured_at`)

The `capture` helper inside `regen_fixtures.sh` runs a Python normalizer over each captured JSON file that:
- Zeroes any numeric field whose key ends in `_ms` / `_seconds` / `_secs`
- Replaces any string field whose key is in the wall-clock set with `"2000-01-01T00:00:00Z"`

**Important invariants the normalizer preserves**:
- `last_value` (watermarks) — NOT touched. It's a logical value derived from seeded data and is genuinely deterministic across runs.
- Anything inside a `data` payload — NOT touched. Only top-level wall-clock fields get the sentinel.

If you add a new timing or timestamp field to an `*Output` struct, you may need to extend `TIMING_SUFFIXES` or `WALL_CLOCK_FIELDS` in the normalizer (defined twice — once for the main capture, once for partition capture). Otherwise your new fixture will drift on every regen.

## Exit-code tolerance

The capture helper tolerates non-zero exit codes as long as stdout parses as JSON. This is intentional:
- `rocky doctor` exits 1 when any check is `warning`, 2 when `critical` — and both still emit valid JSON.
- `rocky run` can exit non-zero on partial success and still emit a valid `RunOutput`.

The dagster integration's `allow_partial=True` path mirrors this tolerance in production — see `integrations/dagster/src/dagster_rocky/resource.py`.

## Adding a new fixture

For a new CLI command whose output the dagster tests need to parse:

1. **Rust schema exists** (from the `rocky-codegen` skill).
2. **Pydantic binding exists** (from `just codegen-dagster`).
3. **Run the binary against the playground POC**:
   ```bash
   cd examples/playground/pocs/00-foundations/00-playground-default
   ../../../../../engine/target/release/rocky <command> [flags] > /tmp/<name>.json
   ```
4. **Validate** — check the shape matches the Pydantic model:
   ```python
   from dagster_rocky.types import parse_rocky_output
   parse_rocky_output(open("/tmp/<name>.json").read())
   ```
5. **Add a `capture` call** in `scripts/regen_fixtures.sh`:
   ```bash
   capture <name> <command> [flags]
   ```
6. **Add the fixture to `conftest.py`** so tests can load it by name.
7. **Regenerate**:
   ```bash
   just regen-fixtures --in-place    # or write fixture by hand to fixtures/
   ```

If the playground POC can't produce the command naturally (e.g. an adapter-specific command that needs Databricks), skip steps 5-6 and hand-write `fixtures/<name>.json` directly.

## Debugging fixture drift

Symptoms and causes:

| Symptom | Likely cause |
|---|---|
| `test_generated_fixtures.py` fails with `ValidationError` | Pydantic model changed but fixture is stale — run `just regen-fixtures` |
| `fixtures_generated/<name>.json` differs by a single `_ms` field | Normalizer missed a new duration field — extend `TIMING_SUFFIXES` |
| Fixture differs wildly between runs | Playground POC has non-deterministic seed data (rare) or a new timestamp field — extend `WALL_CLOCK_FIELDS` |
| `capture` prints `==> <name>` but the file is empty | Command exited non-zero AND wrote to stderr, not stdout — check the command manually |
| `rocky binary not found at engine/target/release/rocky` | Missing release build — run `cd engine && cargo build --release --bin rocky` or `just codegen` |
| Partition fixtures missing but main ones present | `02-performance/03-partition-checksum` POC missing or broken — check `ls examples/playground/pocs/02-performance/03-partition-checksum/` |

## Reference files

- `scripts/regen_fixtures.sh` — the capture pipeline, normalizer, and POC list
- `integrations/dagster/tests/conftest.py` — fixture loading dispatch
- `integrations/dagster/tests/test_generated_fixtures.py` — the test that guards the parallel corpus
- `integrations/dagster/CLAUDE.md` — "Adding support for a new Rocky CLI command" 9-step checklist (includes the fixture step)
- `justfile` — the `regen-fixtures` recipe (wraps the script)

## Related skills

- **`rocky-codegen`** — the Rust → Pydantic/TS cascade that fixtures are validated against
- **`rocky-new-cli-command`** — the end-to-end checklist that includes fixture creation
- **`rocky-poc`** — authoring the playground POCs that the fixture corpus depends on
