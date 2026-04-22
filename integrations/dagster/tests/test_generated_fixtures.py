"""Parser-compatibility guard for live-binary fixtures.

The fixtures under ``tests/fixtures_generated/`` are captured directly from
the ``rocky`` binary by ``scripts/regen_fixtures.sh`` running against the
playground POCs. They are NOT scenario fixtures (those live under
``tests/fixtures/`` and are hand-crafted to exercise rich variants of every
field). Instead, they encode "what the live engine actually emits today" so
we can detect when an engine change ships JSON the dagster Pydantic parser
no longer accepts.

This test walks every ``*.json`` file under ``fixtures_generated/`` (incl.
the ``partition/`` sub-directory) and asserts that ``parse_rocky_output``
returns a non-None model of the expected type for the file's ``command``
field. If the engine ever ships a backwards-incompatible JSON change, this
suite catches it before the dagster integration breaks at runtime.

Refresh the corpus with::

    just regen-fixtures            # or scripts/regen_fixtures.sh

The companion CI workflow (``.github/workflows/codegen-drift.yml``) runs the
same regen and fails on any uncommitted diff.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from dagster_rocky.types import (
    CiResult,
    ColumnLineageResult,
    CompileResult,
    CostOutput,
    DagResult,
    DiscoverResult,
    DoctorResult,
    HistoryResult,
    MetricsResult,
    ModelHistoryResult,
    ModelLineageResult,
    OptimizeResult,
    PlanResult,
    RunResult,
    StateResult,
    TestResult,
    parse_rocky_output,
)

GENERATED_DIR = Path(__file__).parent / "fixtures_generated"

# Map the JSON ``command`` field to the Pydantic class ``parse_rocky_output``
# is expected to return for that command. ``lineage`` and ``history`` have
# two shapes each (model-level vs column-level for lineage; all-runs vs
# per-model for history); the helper :func:`_expected_type` below resolves
# them via the same discriminator the parser uses (``"column"`` / ``"model"``
# field presence).
EXPECTED_TYPES: dict[str, type] = {
    "discover": DiscoverResult,
    "run": RunResult,
    "plan": PlanResult,
    "state": StateResult,
    "compile": CompileResult,
    "test": TestResult,
    "ci": CiResult,
    "history": HistoryResult,
    "metrics": MetricsResult,
    "optimize": OptimizeResult,
    "doctor": DoctorResult,
    "dag": DagResult,
    "cost": CostOutput,
}


def _expected_type(payload: dict) -> type | None:
    """Pick the expected Pydantic class for a Rocky JSON payload.

    Mirrors the dispatch logic in :func:`dagster_rocky.types.parse_rocky_output`.
    """
    command = payload.get("command", "")
    if command == "lineage":
        return ColumnLineageResult if "column" in payload else ModelLineageResult
    if command == "history":
        return ModelHistoryResult if "model" in payload else HistoryResult
    return EXPECTED_TYPES.get(command)


def _generated_fixtures() -> list[Path]:
    return sorted(GENERATED_DIR.rglob("*.json"))


def test_corpus_is_non_empty():
    """Sanity check: regen has been run at least once."""
    files = _generated_fixtures()
    assert files, (
        f"No fixtures under {GENERATED_DIR}. "
        f"Run `just regen-fixtures` (or scripts/regen_fixtures.sh) to capture them."
    )


@pytest.mark.parametrize(
    "fixture_path",
    _generated_fixtures(),
    ids=lambda p: str(p.relative_to(GENERATED_DIR)),
)
def test_generated_fixture_parses(fixture_path: Path):
    """Every live-binary fixture must parse via parse_rocky_output().

    The hand-written fixtures under ``tests/fixtures/`` already cover field-
    level shape variants in ``test_types.py``; this test guards against the
    *engine* shipping JSON the parser doesn't recognize.
    """
    import json

    payload = fixture_path.read_text(encoding="utf-8")
    data = json.loads(payload)

    parsed = parse_rocky_output(payload)
    assert parsed is not None

    expected_type = _expected_type(data)
    assert expected_type is not None, (
        f"{fixture_path.name}: command {data.get('command')!r} has no expected "
        f"type mapping. Add it to EXPECTED_TYPES if a new command was added to "
        f"the playground."
    )
    assert isinstance(parsed, expected_type), (
        f"{fixture_path.name}: parse_rocky_output returned {type(parsed).__name__}, "
        f"expected {expected_type.__name__}"
    )
