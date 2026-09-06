"""A check-gated Rocky run must not finish as a green Dagster step (#1728).

Two filters run in ``_emit_results``, in this order:

    1. selection    `asset_key not in selected_keys`  -> the whole result dropped
    2. declaration  `spec_key not in declared_checks` -> observation (#1709)

#1709 fixed the second. The first still dropped everything — no check, no
observation, no log — and it is the one a ``cross_source_overlap`` verdict
lands behind, because the engine attaches that group check to one sibling in
materialization order, so the carrier is not knowable when specs are built.

Underneath sat the run-level twin: nothing in this package read
``check_gate_failed``. ``RockyClient.run`` passes ``allow_partial=True``, so a
check-gated exit 2 comes back parsed rather than raising. The engine said "I
failed this run on a check" and the step finished successful.
"""

from __future__ import annotations

import logging

import dagster as dg
import pytest

from dagster_rocky.component import _emit_results
from dagster_rocky.types import (
    CheckResult,
    DriftInfo,
    ExecutionSummary,
    PermissionInfo,
    RunResult,
    TableCheckResult,
)

CARRIER = dg.AssetKey(["fivetran", "acme", "us_west", "shopify", "orders"])
SIBLING = dg.AssetKey(["fivetran", "acme", "us_east", "shopify", "orders"])
CARRIER_ROCKY = ("fivetran", "acme", "us_west", "shopify", "orders")
SIBLING_ROCKY = ("fivetran", "acme", "us_east", "shopify", "orders")
MAPPING = {CARRIER_ROCKY: CARRIER, SIBLING_ROCKY: SIBLING}
OVERLAP = "cross_source_overlap:fivetran.orders"


def _run_result(
    *,
    check_results: list[TableCheckResult] | None = None,
    check_gate_failed: bool = False,
) -> RunResult:
    return RunResult(
        version="0.3.0",
        command="run",
        filter="tenant=acme",
        duration_ms=1000,
        tables_copied=0,
        tables_failed=0,
        check_gate_failed=check_gate_failed,
        materializations=[],
        check_results=check_results or [],
        execution=ExecutionSummary(concurrency=4, tables_processed=0, tables_failed=0),
        permissions=PermissionInfo(
            grants_added=0, grants_revoked=0, catalogs_created=0, schemas_created=0
        ),
        drift=DriftInfo(tables_checked=0, tables_drifted=0, actions_taken=[]),
        anomalies=[],
        errors=[],
    )


def _failing_overlap_on_carrier() -> TableCheckResult:
    return TableCheckResult(
        asset_key=list(CARRIER_ROCKY),
        checks=[CheckResult(name=OVERLAP, passed=False)],
    )


def test_a_gated_run_whose_failing_check_is_outside_the_selection_fails_the_step(caplog):
    """The shape #1669 was filed for, still live after #1709.

    The selection spans the group but omits the carrier sibling — an ordinary
    partial selection. Before #1728 this produced NO event of any kind and a
    successful step.
    """
    result = _run_result(
        check_results=[_failing_overlap_on_carrier()],
        check_gate_failed=True,
    )

    with caplog.at_level(logging.WARNING), pytest.raises(dg.Failure) as excinfo:
        list(
            _emit_results(
                results=[result],
                check_specs=[],
                # The carrier is NOT selected.
                selected_keys={SIBLING},
                rocky_key_to_dagster_key=MAPPING,
            )
        )

    description = excinfo.value.description or ""
    assert "check_gate_failed" in description
    assert "Refusing to report green" in description
    # The refusal names what it could not report, not just that something went
    # wrong — the operator has to know which check to go look at.
    assert OVERLAP in description
    assert OVERLAP in caplog.text, caplog.text


def test_a_gated_run_with_a_visible_failing_check_does_not_double_report(caplog):
    """The control. A failing check the step CAN report is already visible in
    the UI and on the asset's health, so the backstop stays quiet — otherwise
    every ordinary check failure would become a step-level exception too."""
    result = _run_result(
        check_results=[_failing_overlap_on_carrier()],
        check_gate_failed=True,
    )

    events = list(
        _emit_results(
            results=[result],
            check_specs=[
                dg.AssetCheckSpec(name="cross_source_overlap_fivetran_orders", asset=CARRIER)
            ],
            selected_keys={CARRIER, SIBLING},
            rocky_key_to_dagster_key=MAPPING,
        )
    )

    failed = [e for e in events if isinstance(e, dg.AssetCheckResult) and not e.passed]
    assert len(failed) == 1, f"the check is reported as a check: {events}"


def test_an_ungated_run_with_an_unselected_check_still_does_not_fail_the_step(caplog):
    """The other control, and the reason this is not just 'raise on any drop'.

    A check outside the selection with no gate is not this step's problem — it
    is warned about and nothing more. Only the engine's own gate verdict turns
    it into a failure.
    """
    result = _run_result(
        check_results=[_failing_overlap_on_carrier()],
        check_gate_failed=False,
    )

    with caplog.at_level(logging.WARNING):
        events = list(
            _emit_results(
                results=[result],
                check_specs=[],
                selected_keys={SIBLING},
                rocky_key_to_dagster_key=MAPPING,
            )
        )

    assert not [e for e in events if isinstance(e, dg.AssetCheckResult)]
    assert "FAILED check" in caplog.text, caplog.text


def test_an_inherited_gate_with_clean_checks_still_fails_the_step():
    """A resume can inherit a standing gate from the run it resumed, so its own
    ``check_results`` are clean (#1720). Nothing in this step explains the gate
    — which is exactly when reporting green would be wrong."""
    result = _run_result(check_results=[], check_gate_failed=True)

    with pytest.raises(dg.Failure) as excinfo:
        list(
            _emit_results(
                results=[result],
                check_specs=[],
                selected_keys={CARRIER},
                rocky_key_to_dagster_key=MAPPING,
            )
        )

    description = excinfo.value.description or ""
    assert "inherited a standing gate" in description
