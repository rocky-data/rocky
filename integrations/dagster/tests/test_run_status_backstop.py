"""Two engine verdicts the run path ignored (#1814, the Dagster finding).

``RockyClient.run`` passes ``allow_partial=True``, so a non-zero ``rocky run``
comes back as a parsed result rather than raising. Two shapes then finished
as a green Dagster step:

* ``verify_after_failed`` — the engine auto-applied schema drift it could not
  confirm and exited non-zero. The only trace is a ``<verify_after>`` entry in
  ``errors``, which maps to no asset.
* ``SkippedInFlight`` — the engine ran nothing because another run holds the
  idempotency claim. With ``satisfy_empty_outputs`` on, every selected key got
  a zero-row "materialization" for work that never happened.
"""

from __future__ import annotations

import dagster as dg
import pytest

from dagster_rocky.component import _emit_results
from dagster_rocky.types import (
    DriftInfo,
    ExecutionSummary,
    PermissionInfo,
    RunResult,
)

ORDERS = dg.AssetKey(["fivetran", "acme", "orders"])
MAPPING = {("fivetran", "acme", "orders"): ORDERS}


def _run_result(**overrides) -> RunResult:
    fields = {
        "version": "0.3.0",
        "command": "run",
        "filter": "tenant=acme",
        "duration_ms": 1000,
        "tables_copied": 0,
        "tables_failed": 0,
        "materializations": [],
        "check_results": [],
        "execution": ExecutionSummary(concurrency=4, tables_processed=0, tables_failed=0),
        "permissions": PermissionInfo(
            grants_added=0, grants_revoked=0, catalogs_created=0, schemas_created=0
        ),
        "drift": DriftInfo(tables_checked=0, tables_drifted=0, actions_taken=[]),
        "anomalies": [],
        "errors": [],
    }
    fields.update(overrides)
    return RunResult(**fields)


def _emit(result: RunResult, *, satisfy_empty_outputs: bool = False) -> list:
    return list(
        _emit_results(
            results=[result],
            check_specs=[],
            selected_keys={ORDERS},
            rocky_key_to_dagster_key=MAPPING,
            satisfy_empty_outputs=satisfy_empty_outputs,
        )
    )


def test_an_unverified_auto_applied_migration_fails_the_step():
    result = _run_result(status="Failure", tables_failed=1, verify_after_failed=True)
    with pytest.raises(dg.Failure) as excinfo:
        _emit(result)
    assert "verify_after_failed" in (excinfo.value.description or "")


@pytest.mark.parametrize("status", ["SkippedInFlight", "skipped_in_flight"])
def test_a_run_skipped_on_an_in_flight_claim_fails_before_any_event(status):
    result = _run_result(status=status, skipped_by_run_id="run-holder")
    events = []
    with pytest.raises(dg.Failure) as excinfo:
        for event in _emit_results(
            results=[result],
            check_specs=[],
            selected_keys={ORDERS},
            rocky_key_to_dagster_key=MAPPING,
            satisfy_empty_outputs=True,
        ):
            events.append(event)
    assert events == [], "no zero-row materialization for work that never ran"
    description = excinfo.value.description or ""
    assert "run-holder" in description
    assert excinfo.value.allow_retries


def test_a_run_skipped_because_a_prior_run_succeeded_stays_green():
    """The control: ``SkippedIdempotent`` means the key is already satisfied."""
    result = _run_result(status="SkippedIdempotent", skipped_by_run_id="run-done")
    _emit(result, satisfy_empty_outputs=True)


def test_a_verified_run_stays_green():
    _emit(_run_result(status="Success"))
