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

import json
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
    GcApplyOutput,
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
    # ``rocky apply`` prints the plan-kind's own output: a run / replication
    # plan prints ``command:"run"`` (→ RunResult, keyed above), a ``gc`` plan
    # prints ``command:"apply"`` with gc markers (→ GcApplyOutput).
    "apply": GcApplyOutput,
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


# ---------------------------------------------------------------------------
# Golden field-value assertions
# ---------------------------------------------------------------------------
#
# The parametrized ``test_generated_fixture_parses`` above proves every
# captured fixture still parses into the expected Pydantic *type* — it guards
# JSON *shape*. But because the corpus is regenerated *from* the binary, the
# shape check can only ever match the binary against itself: it pins nothing
# about whether the engine emits the *correct* drift / incremental / anomaly
# *content* for a given scenario.
#
# The tests below close that gap on a handful of high-signal, live-captured
# fixtures by asserting the deterministic content values the scenario is
# supposed to produce. Each loads its fixture by an explicit path (a wrong
# path raises ``FileNotFoundError`` and fails loudly, rather than silently
# skipping the assertions). They assert only seeded / logical content — never
# wall-clock or timing fields, which the regen normalizer zeroes or replaces
# with a sentinel (see ``scripts/_normalize_fixture.py``), so timing asserts
# would be brittle by construction.
#
# Coverage note: ``governance`` / ``compliance`` / ``retention-status`` /
# failure-mode outputs have generated Pydantic models under ``types_generated/``
# but no live capture under ``fixtures_generated/`` (the playground POCs don't
# exercise them), so they remain shape-only — covered by the hand-crafted
# ``scenarios.py`` dicts in ``test_types.py``, not by a golden value here.


def _load(rel_path: str):
    """Load a generated fixture by its path relative to ``fixtures_generated/``."""
    payload = (GENERATED_DIR / rel_path).read_text(encoding="utf-8")
    parsed = parse_rocky_output(payload)
    assert parsed is not None, f"{rel_path}: parse_rocky_output returned None"
    return parsed


def test_drift_after_drift_golden():
    """The drift scenario's *drifted* run must report the one detected change.

    Pins that the engine still detects the seeded ``amount`` column widening
    (``VARCHAR`` → ``DECIMAL(10,2)``) and emits a drop-and-recreate action —
    a regression in drift detection would flip ``tables_drifted`` to 0 or lose
    the column-change reason.
    """
    parsed = _load("drift/run_after_drift.json")

    assert isinstance(parsed, RunResult)
    assert parsed.drift.tables_checked == 1
    assert parsed.drift.tables_drifted == 1

    assert len(parsed.drift.actions_taken) == 1
    action = parsed.drift.actions_taken[0]
    assert action.action == "drop_and_recreate"
    assert action.table == "poc.staging__orders.orders"
    # ASCII substrings only — the live reason contains a non-ASCII "→" arrow
    # (→) whose exact spacing is brittle to pin literally.
    assert "amount" in action.reason
    assert "VARCHAR" in action.reason
    assert "DECIMAL(10,2)" in action.reason


def test_drift_clean_golden():
    """The drift scenario's *clean* run is the negative control: no drift.

    Without this companion to ``run_after_drift``, a bug that reported drift
    unconditionally would still pass the drifted-run assertions above.
    """
    parsed = _load("drift/run_clean.json")

    assert isinstance(parsed, RunResult)
    assert parsed.drift.tables_checked == 1
    assert parsed.drift.tables_drifted == 0
    assert parsed.drift.actions_taken == []


def test_incremental_strategy_progression_golden():
    """The two-run incremental scenario must switch full_refresh → incremental.

    The watermark ``last_value`` is a temporal value the regen normalizer
    replaces with the sentinel timestamp, so it can't anchor a golden assert
    here (it reads identically across both runs and rows_copied is null for
    DuckDB). The materialization ``strategy`` is the *deterministic* signal
    that the second run actually took the incremental path: run 1 does a full
    load, run 2 loads only the delta. Break incremental detection and run 2
    reverts to ``full_refresh`` — caught here.
    """
    run1 = _load("incremental/run1_initial.json")
    run2 = _load("incremental/run2_delta.json")

    assert isinstance(run1, RunResult)
    assert isinstance(run2, RunResult)
    assert run1.materializations[0].metadata.strategy == "full_refresh"
    assert run2.materializations[0].metadata.strategy == "incremental"


def test_anomaly_baseline_golden():
    """A baseline run sees the full table and flags no anomaly."""
    parsed = _load("anomaly/run_baseline_1.json")

    assert isinstance(parsed, RunResult)
    assert parsed.anomalies == []
    assert parsed.metrics is not None
    assert parsed.metrics.anomalies_detected == 0

    # The row_count check sees the full seeded table on both sides.
    check = parsed.check_results[0].checks[0]
    assert check.name == "row_count"
    assert check.passed is True
    assert check.source_count == 500
    assert check.target_count == 500


def test_anomaly_incident_golden():
    """The incident run must detect the seeded row-count collapse.

    The events table drops from a ~376-row historical baseline to 5 rows; the
    engine must surface exactly one anomaly with the collapsed current count.
    A regression in baseline comparison would drop the anomaly entirely.
    """
    parsed = _load("anomaly/run_incident.json")

    assert isinstance(parsed, RunResult)
    assert parsed.metrics is not None
    assert parsed.metrics.anomalies_detected == 1

    assert len(parsed.anomalies) == 1
    anomaly = parsed.anomalies[0]
    assert anomaly.table == "poc.staging__events.events"
    assert anomaly.current_count == 5
    assert "row count dropped" in anomaly.reason

    # The row_count check itself still passes (source == target == 5); the
    # anomaly is a *historical* signal, distinct from the source/target match.
    check = parsed.check_results[0].checks[0]
    assert check.name == "row_count"
    assert check.source_count == 5
    assert check.target_count == 5
