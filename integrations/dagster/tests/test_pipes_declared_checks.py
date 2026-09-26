"""Every declared check gets a verdict over Dagster Pipes (#2160).

A Pipes step runs with a typed event stream. Dagster then fails the whole
step on any declared check that yields nothing
(``DagsterInvariantViolationError: ... did not yield or return expected
outputs``). Two causes reached it:

* the component declared ``row_count`` / ``column_match`` on every asset,
  even when the project does not enable them;
* a declared check on a table Rocky did not report gets no result at all.

The fake ``run_pipes`` below opens the typed event stream the way a real
Pipes session does (``open_pipes_session`` calls
``set_requires_typed_event_stream``). Without it the step would never fail,
and these tests would pass without the fix.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import dagster as dg

from dagster_rocky.component import RockyComponent, _default_check_is_emitted
from dagster_rocky.resource import RockyResource
from dagster_rocky.types import ChecksConfig

ORDERS_KEY = dg.AssetKey(["fivetran", "acme", "us_west", "shopify", "orders"])
PAYMENTS_KEY = dg.AssetKey(["fivetran", "acme", "us_west", "shopify", "payments"])
ORDERS = ORDERS_KEY.to_user_string()
PAYMENTS = PAYMENTS_KEY.to_user_string()


def _build_defs(discover: dict[str, Any], tmp_path: Path) -> dg.Definitions:
    state_file = tmp_path / "state.json"
    state_file.write_text(json.dumps({"discover": discover}))
    component = RockyComponent(config_path="rocky.toml", execution_mode="pipes")
    return component.build_defs_from_state(context=None, state_path=state_file)


def _materialize_pipes(defs: dg.Definitions, reported: list[Any], selection):
    """Run the component's assets with a fake ``run_pipes`` that behaves like
    a real Pipes session: typed event stream on, and ``get_results`` returning
    only what the engine reported unless implicit materializations are asked
    for (Dagster's default)."""

    def fake_run_pipes(context, **_kwargs):
        context.set_requires_typed_event_stream(error_message="pipes typed stream")
        invocation = MagicMock()

        def get_results(*, implicit_materializations: bool = True):
            results = list(reported)
            if implicit_materializations:
                seen = {r.asset_key for r in reported if isinstance(r, dg.MaterializeResult)}
                results += [
                    dg.MaterializeResult(asset_key=k)
                    for k in context.selected_asset_keys
                    if k not in seen
                ]
            return results

        invocation.get_results = get_results
        return invocation

    with patch.object(RockyResource, "run_pipes", side_effect=fake_run_pipes, autospec=False):
        return dg.materialize(
            list(defs.assets or []),
            resources={"rocky": RockyResource(config_path="rocky.toml")},
            selection=selection,
            raise_on_error=False,
        )


def _failure_messages(exec_result) -> list[str]:
    return [
        e.event_specific_data.error.message
        for e in exec_result.all_events
        if e.event_type_value == "STEP_FAILURE"
    ]


def _check_evaluations(exec_result) -> dict[tuple[str, str], Any]:
    return {
        (
            e.event_specific_data.asset_key.to_user_string(),
            e.event_specific_data.check_name,
        ): e.event_specific_data
        for e in exec_result.all_events
        if e.event_type_value == "ASSET_CHECK_EVALUATION"
    }


def _discover(checks: dict[str, Any] | None) -> dict[str, Any]:
    discover: dict[str, Any] = {
        "version": "0.1.0",
        "command": "discover",
        "sources": [
            {
                "id": "src_001",
                "components": {"tenant": "acme", "region": "us_west", "source": "shopify"},
                "source_type": "fivetran",
                "tables": [{"name": "orders"}, {"name": "payments"}],
            }
        ],
    }
    if checks is not None:
        discover["checks"] = checks
    return discover


def test_pipes_step_succeeds_when_rocky_omits_a_declared_check(tmp_path: Path):
    """The live #2160 failure: an old-shape projection (no toggles), so every
    default check is declared, and Rocky reports only ``row_count``. The
    step used to fail on the missing ``column_match``. Now each missing
    check gets a failing WARN result that says why."""
    defs = _build_defs(_discover(None), tmp_path)

    exec_result = _materialize_pipes(
        defs,
        [
            dg.MaterializeResult(asset_key=ORDERS_KEY, metadata={"strategy": "full_refresh"}),
            dg.AssetCheckResult(asset_key=ORDERS_KEY, check_name="row_count", passed=True),
        ],
        [ORDERS_KEY],
    )

    assert exec_result.success, _failure_messages(exec_result)
    evaluations = _check_evaluations(exec_result)
    assert evaluations[(ORDERS, "row_count")].passed is True
    for name in ("column_match", "row_count_anomaly"):
        placeholder = evaluations[(ORDERS, name)]
        assert placeholder.passed is False
        assert placeholder.severity == dg.AssetCheckSeverity.WARN
        assert "not produced by rocky" in placeholder.metadata["status"].value


def test_pipes_never_passes_a_check_on_a_table_rocky_did_not_report(tmp_path: Path):
    """``payments`` is selected but Rocky reported nothing for it. Dagster's
    implicit materialization still lands, but it is not evidence of a copy,
    so no declared check on ``payments`` passes."""
    defs = _build_defs(_discover(None), tmp_path)

    exec_result = _materialize_pipes(
        defs,
        [dg.MaterializeResult(asset_key=ORDERS_KEY, metadata={"strategy": "full_refresh"})],
        [ORDERS_KEY, PAYMENTS_KEY],
    )

    assert exec_result.success, _failure_messages(exec_result)
    materialized = {
        e.event_specific_data.materialization.asset_key
        for e in exec_result.all_events
        if e.event_type_value == "ASSET_MATERIALIZATION"
    }
    assert materialized == {ORDERS_KEY, PAYMENTS_KEY}, "implicit materialization kept"
    payments_checks = {
        name: ev
        for (asset, name), ev in _check_evaluations(exec_result).items()
        if asset == PAYMENTS
    }
    assert set(payments_checks) == {"row_count", "column_match", "row_count_anomaly"}
    for ev in payments_checks.values():
        assert ev.passed is False
        assert "rocky reported no materialization" in ev.metadata["status"].value


def test_declared_default_checks_follow_the_projected_toggles(tmp_path: Path):
    """A project that turns ``column_match`` off gets no ``column_match``
    spec, so a Pipes run that never produces one does not report a
    placeholder for it either."""
    defs = _build_defs(_discover({"row_count": True, "column_match": False}), tmp_path)

    specs = {
        (spec.asset_key, spec.name)
        for asset in defs.assets or []
        for spec in getattr(asset, "check_specs", ())
    }
    assert (ORDERS_KEY, "row_count") in specs
    assert (ORDERS_KEY, "column_match") not in specs
    assert (ORDERS_KEY, "row_count_anomaly") in specs

    exec_result = _materialize_pipes(
        defs,
        [
            dg.MaterializeResult(asset_key=ORDERS_KEY, metadata={"strategy": "full_refresh"}),
            dg.AssetCheckResult(asset_key=ORDERS_KEY, check_name="row_count", passed=True),
            dg.AssetCheckResult(
                asset_key=ORDERS_KEY,
                check_name="row_count_anomaly",
                passed=True,
                severity=dg.AssetCheckSeverity.WARN,
            ),
        ],
        [ORDERS_KEY],
    )
    assert exec_result.success, _failure_messages(exec_result)
    assert (ORDERS, "column_match") not in _check_evaluations(exec_result)


def test_default_check_is_emitted_reads_none_as_an_older_engine():
    assert _default_check_is_emitted(None, "row_count") is True
    assert _default_check_is_emitted(ChecksConfig(), "column_match") is True
    off = ChecksConfig(row_count=False, column_match=False)
    assert _default_check_is_emitted(off, "row_count") is False
    assert _default_check_is_emitted(off, "column_match") is False
    on = ChecksConfig(row_count=True, column_match=True)
    assert _default_check_is_emitted(on, "row_count") is True


def test_pipes_group_check_gets_a_non_green_result_instead_of_nothing():
    """Streaming yields nothing for a group check a sibling carried (Dagster
    records a skip). Over Pipes nothing fails the step, so the member gets a
    failing WARN result naming the carrier."""
    from dagster_rocky.component import GROUP_CHECK_METADATA_KEY, _emit_placeholder_checks

    spec = dg.AssetCheckSpec(
        name="overlap", asset=PAYMENTS_KEY, metadata={GROUP_CHECK_METADATA_KEY: True}
    )
    kwargs: dict[str, Any] = {
        "check_specs": [spec],
        "selected_keys": {ORDERS_KEY, PAYMENTS_KEY},
        "yielded_checks": {(ORDERS_KEY, "overlap")},
        "materialized_keys": {ORDERS_KEY, PAYMENTS_KEY},
    }
    assert list(_emit_placeholder_checks(**kwargs)) == []
    (result,) = list(_emit_placeholder_checks(**kwargs, pipes=True))
    assert result.passed is False
    assert result.severity == dg.AssetCheckSeverity.WARN
    assert ORDERS in result.metadata["status"].value
