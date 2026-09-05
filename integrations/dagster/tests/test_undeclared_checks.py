"""A check result with no declared spec, on both execution paths (#1673),
and the cross-source-overlap group check (#1669).

Two defects met at the same seam in ``component.py``:

* The streaming path dropped an undeclared ``(asset_key, check_name)``
  silently; the Pipes path yielded it and Dagster raised
  ``DagsterInvariantViolationError``, failing the step. A ``rocky.toml``
  edit without a component-state refresh is enough to reach it.
* ``cross_source_overlap`` is a GROUP check: the engine evaluates it once per
  sibling group and reports it on one member. The component excluded those
  names from its declared specs, and the emit side then dropped the one real
  result — so a FAILED overlap check reached nobody.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import dagster as dg
import pytest

from dagster_rocky.component import RockyComponent
from dagster_rocky.resource import RockyResource
from dagster_rocky.types import RunResult

ORDERS_KEY = dg.AssetKey(["fivetran", "acme", "us_west", "shopify", "orders"])
SIBLING_KEY = dg.AssetKey(["fivetran", "acme", "us_east", "shopify", "orders"])
OVERLAP_ENGINE_NAME = "cross_source_overlap:fivetran.orders"
OVERLAP_CHECK = "cross_source_overlap_fivetran_orders"


# ---------------------------------------------------------------------------
# Fixtures / builders
# ---------------------------------------------------------------------------


def _build_defs(
    discover: dict[str, Any],
    tmp_path: Path,
    *,
    execution_mode: str = "streaming",
    surface_configured_checks: bool = False,
) -> dg.Definitions:
    state_file = tmp_path / "state.json"
    state_file.write_text(json.dumps({"discover": discover}))
    component = RockyComponent(
        config_path="rocky.toml",
        execution_mode=execution_mode,
        surface_configured_checks=surface_configured_checks,
    )
    return component.build_defs_from_state(context=None, state_path=state_file)


def _discover_without_freshness(discover_json: str) -> dict[str, Any]:
    """The #1664 stale-state shape: the project configures no
    ``[checks.freshness]``, so the component declares no ``freshness`` spec —
    but the binary that runs still emits a ``freshness`` result."""
    d = json.loads(discover_json)
    d.pop("checks", None)
    return d


def _discover_with_siblings() -> dict[str, Any]:
    """Two sources in one group whose tables share the name ``orders`` — the
    sibling shape ``cross_source_overlap`` groups on ``(source_type, table)``."""
    return {
        "version": "0.1.0",
        "command": "discover",
        "checks": {
            "configured_checks": {
                "orders": [
                    {
                        "name": OVERLAP_ENGINE_NAME,
                        "kind": "cross_source_overlap",
                        "candidate": True,
                    }
                ]
            }
        },
        "sources": [
            {
                "id": "src_west",
                "components": {"tenant": "acme", "region": "us_west", "source": "shopify"},
                "source_type": "fivetran",
                "tables": [{"name": "orders"}],
            },
            {
                "id": "src_east",
                "components": {"tenant": "acme", "region": "us_east", "source": "shopify"},
                "source_type": "fivetran",
                "tables": [{"name": "orders"}],
            },
        ],
    }


def _run_result(
    *,
    materialized: list[dg.AssetKey],
    checks: dict[dg.AssetKey, list[dict[str, Any]]],
) -> RunResult:
    return RunResult.model_validate(
        {
            "version": "0.3.0",
            "command": "run",
            "filter": "tenant=acme",
            "duration_ms": 10,
            "tables_copied": len(materialized),
            "tables_failed": 0,
            "permissions": {
                "grants_added": 0,
                "grants_revoked": 0,
                "catalogs_created": 0,
                "schemas_created": 0,
            },
            "drift": {"tables_checked": 0, "tables_drifted": 0, "actions_taken": []},
            "materializations": [
                {
                    "asset_key": list(key.path),
                    "rows_copied": 1,
                    "duration_ms": 1,
                    "metadata": {"strategy": "full_refresh"},
                }
                for key in materialized
            ],
            "check_results": [
                {"asset_key": list(key.path), "checks": entries} for key, entries in checks.items()
            ],
        }
    )


def _materialize_streaming(defs: dg.Definitions, run_result: RunResult, selection):
    with (
        patch.object(RockyResource, "run", return_value=run_result),
        patch.object(RockyResource, "run_streaming", return_value=run_result),
    ):
        return dg.materialize(
            list(defs.assets or []),
            resources={"rocky": RockyResource(config_path="rocky.toml")},
            selection=selection,
            raise_on_error=False,
        )


def _materialize_pipes(defs: dg.Definitions, results: list[Any], selection):
    invocation = MagicMock()
    invocation.get_results = MagicMock(return_value=list(results))
    with patch.object(RockyResource, "run_pipes", return_value=invocation):
        return dg.materialize(
            list(defs.assets or []),
            resources={"rocky": RockyResource(config_path="rocky.toml")},
            selection=selection,
            raise_on_error=False,
        )


def _check_evaluations(exec_result) -> dict[tuple[str, str], Any]:
    return {
        (
            e.event_specific_data.asset_key.to_user_string(),
            e.event_specific_data.check_name,
        ): e.event_specific_data
        for e in exec_result.all_events
        if e.event_type_value == "ASSET_CHECK_EVALUATION"
    }


def _observations(exec_result) -> list[Any]:
    return [
        e.event_specific_data.asset_observation
        for e in exec_result.all_events
        if e.event_type_value == "ASSET_OBSERVATION"
    ]


# ---------------------------------------------------------------------------
# #1673 — an undeclared check result on either path
# ---------------------------------------------------------------------------


def test_pipes_undeclared_check_does_not_fail_the_step(discover_json: str, tmp_path: Path):
    """Pipes: a ``freshness`` result the component never declared used to
    reach Dagster and raise ``DagsterInvariantViolationError`` — a hard step
    failure. It is now filtered out and carried as an observation."""
    defs = _build_defs(_discover_without_freshness(discover_json), tmp_path, execution_mode="pipes")

    exec_result = _materialize_pipes(
        defs,
        [
            dg.MaterializeResult(asset_key=ORDERS_KEY),
            dg.AssetCheckResult(
                asset_key=ORDERS_KEY,
                check_name="freshness",
                passed=False,
                severity=dg.AssetCheckSeverity.ERROR,
                metadata={"lag_seconds": 900},
            ),
        ],
        [ORDERS_KEY],
    )

    assert exec_result.success, [
        e.event_specific_data.error.message
        for e in exec_result.all_events
        if e.event_type_value == "STEP_FAILURE"
    ]
    assert ("fivetran/acme/us_west/shopify/orders", "freshness") not in _check_evaluations(
        exec_result
    )
    observations = _observations(exec_result)
    assert len(observations) == 1
    assert observations[0].metadata["rocky/undeclared_check"].value is True
    assert observations[0].metadata["rocky/check_name"].value == "freshness"
    assert observations[0].metadata["rocky/check_passed"].value is False
    # The engine's own metadata rides along, so the verdict is readable.
    assert observations[0].metadata["lag_seconds"].value == 900


def test_streaming_undeclared_check_is_observed_not_dropped(discover_json: str, tmp_path: Path):
    """Streaming: the same input. It never failed the step here — it vanished.
    Same helper, same outcome as the Pipes path above."""
    defs = _build_defs(_discover_without_freshness(discover_json), tmp_path)
    run_result = _run_result(
        materialized=[ORDERS_KEY],
        checks={
            ORDERS_KEY: [
                {"name": "row_count", "passed": True, "source_count": 1, "target_count": 1},
                {
                    "name": "freshness",
                    "passed": False,
                    "lag_seconds": 900,
                    "threshold_seconds": 60,
                },
            ]
        },
    )

    exec_result = _materialize_streaming(defs, run_result, [ORDERS_KEY])

    assert exec_result.success
    evaluations = _check_evaluations(exec_result)
    assert ("fivetran/acme/us_west/shopify/orders", "row_count") in evaluations
    assert ("fivetran/acme/us_west/shopify/orders", "freshness") not in evaluations
    undeclared = [o for o in _observations(exec_result) if "rocky/undeclared_check" in o.metadata]
    assert len(undeclared) == 1
    assert undeclared[0].metadata["rocky/check_name"].value == "freshness"
    assert undeclared[0].metadata["rocky/check_passed"].value is False


def test_undeclared_check_warning_names_asset_check_and_cause():
    """The warning both paths log names the asset, the check, the verdict and
    the stale-state cause an operator has to act on."""
    from dagster_rocky.component import _undeclared_check_observation

    log = MagicMock(spec=logging.Logger)
    observation = _undeclared_check_observation(
        asset_key=ORDERS_KEY,
        check_name="freshness",
        passed=False,
        metadata={},
        selected_keys={ORDERS_KEY},
        log=log,
    )

    assert observation is not None
    log.warning.assert_called_once()
    message = log.warning.call_args.args[0]
    assert "freshness" in message
    assert "fivetran/acme/us_west/shopify/orders" in message
    assert "FAILED" in message
    assert "refresh the component state" in message
    assert "surface_configured_checks" in message


def test_undeclared_check_on_a_foreign_asset_warns_without_an_observation():
    """A result whose asset is not part of this step gets the warning but no
    event — an observation there would land on a timeline this step doesn't own."""
    from dagster_rocky.component import _undeclared_check_observation

    log = MagicMock(spec=logging.Logger)
    observation = _undeclared_check_observation(
        asset_key=dg.AssetKey(["somewhere", "else"]),
        check_name="row_count",
        passed=True,
        metadata={},
        selected_keys={ORDERS_KEY},
        log=log,
    )

    assert observation is None
    log.warning.assert_called_once()


# ---------------------------------------------------------------------------
# #1669 — the cross-source-overlap group check
# ---------------------------------------------------------------------------


def test_failed_overlap_check_is_a_named_failed_check_on_the_engines_asset(tmp_path: Path):
    """A FAILED ``cross_source_overlap`` result reaches Dagster as a named,
    failed asset check on the sibling the engine reported it on — with the
    overlap count and the contributing tables attached."""
    defs = _build_defs(_discover_with_siblings(), tmp_path, surface_configured_checks=True)
    run_result = _run_result(
        materialized=[ORDERS_KEY, SIBLING_KEY],
        checks={
            ORDERS_KEY: [
                {
                    "name": OVERLAP_ENGINE_NAME,
                    "passed": False,
                    "severity": "error",
                    "overlap_count": 42,
                    "contributing_tables": [
                        "wh.staging__us_west__shopify.orders",
                        "wh.staging__us_east__shopify.orders",
                    ],
                    "sample": ["1001", "1002"],
                }
            ]
        },
    )

    exec_result = _materialize_streaming(defs, run_result, [ORDERS_KEY, SIBLING_KEY])

    assert exec_result.success
    evaluation = _check_evaluations(exec_result)[
        ("fivetran/acme/us_west/shopify/orders", OVERLAP_CHECK)
    ]
    assert evaluation.passed is False
    assert evaluation.severity == dg.AssetCheckSeverity.ERROR
    assert evaluation.metadata["overlap_count"].value == 42
    assert "us_east__shopify.orders" in evaluation.metadata["contributing_tables"].value


def _overlap_run_result() -> RunResult:
    return _run_result(
        materialized=[ORDERS_KEY, SIBLING_KEY],
        checks={
            ORDERS_KEY: [
                {
                    "name": OVERLAP_ENGINE_NAME,
                    "passed": False,
                    "severity": "error",
                    "overlap_count": 42,
                    "contributing_tables": [],
                    "sample": [],
                }
            ]
        },
    )


def test_unevaluated_sibling_yields_no_result_and_the_step_succeeds(tmp_path: Path):
    """The sibling the engine did not evaluate gets NO result for the group
    check — not a passing placeholder (a green verdict for a check that never
    ran), and not a failing one either (two permanent warnings on every run of
    a three-sibling group reads as a broken pipeline).

    Yielding nothing is Dagster's own "planned, did not run": the declared
    check keeps its ``ASSET_CHECK_EVALUATION_PLANNED`` record at status
    ``PLANNED``, which ``AssetCheckExecutionRecord.resolve_status`` maps to
    ``SKIPPED`` once the run finishes without failing."""
    defs = _build_defs(_discover_with_siblings(), tmp_path, surface_configured_checks=True)

    exec_result = _materialize_streaming(defs, _overlap_run_result(), [ORDERS_KEY, SIBLING_KEY])

    assert exec_result.success
    evaluations = _check_evaluations(exec_result)
    # The engine's own sibling still carries the real verdict.
    assert ("fivetran/acme/us_west/shopify/orders", OVERLAP_CHECK) in evaluations
    # The other sibling reports nothing at all for that check.
    assert ("fivetran/acme/us_east/shopify/orders", OVERLAP_CHECK) not in evaluations
    # Its own per-asset checks are unaffected.
    assert ("fivetran/acme/us_east/shopify/orders", "row_count") in evaluations


def test_unevaluated_sibling_logs_where_the_group_was_evaluated(tmp_path: Path):
    """One INFO line names the sibling with no verdict and the asset the group
    was evaluated on, so the skip is explained rather than merely silent."""
    from dagster_rocky.component import _emit_placeholder_checks

    log = MagicMock(spec=logging.Logger)
    spec = dg.AssetCheckSpec(
        name=OVERLAP_CHECK,
        asset=SIBLING_KEY,
        metadata={"rocky/group_check": True},
    )

    events = list(
        _emit_placeholder_checks(
            check_specs=[spec],
            selected_keys={ORDERS_KEY, SIBLING_KEY},
            yielded_checks={(ORDERS_KEY, OVERLAP_CHECK)},
            materialized_keys={ORDERS_KEY, SIBLING_KEY},
            log=log,
        )
    )

    assert events == []
    log.info.assert_called_once()
    message = log.info.call_args.args[0]
    assert OVERLAP_CHECK in message
    assert "fivetran/acme/us_east/shopify/orders" in message
    assert "this run evaluated it on fivetran/acme/us_west/shopify/orders" in message
    assert "skipped for this run" in message


def test_unevaluated_group_check_with_no_carrier_says_so(tmp_path: Path):
    """A subset that copies fewer than two siblings runs the group check on
    nobody. The INFO line must say that, not name a carrier."""
    from dagster_rocky.component import _emit_placeholder_checks

    log = MagicMock(spec=logging.Logger)
    spec = dg.AssetCheckSpec(
        name=OVERLAP_CHECK,
        asset=SIBLING_KEY,
        metadata={"rocky/group_check": True},
    )

    events = list(
        _emit_placeholder_checks(
            check_specs=[spec],
            selected_keys={SIBLING_KEY},
            yielded_checks=set(),
            materialized_keys={SIBLING_KEY},
            log=log,
        )
    )

    assert events == []
    assert "it was not evaluated in this run" in log.info.call_args.args[0]


def test_not_evaluated_overlap_result_carries_the_engines_reason(tmp_path: Path):
    """A keyless table / misconfigured key makes the engine report the check as
    not evaluated (``passed=false`` plus a reason). The reason must reach the
    UI — it is the difference between "no overlap" and "never measured"."""
    defs = _build_defs(_discover_with_siblings(), tmp_path, surface_configured_checks=True)
    run_result = _run_result(
        materialized=[ORDERS_KEY, SIBLING_KEY],
        checks={
            ORDERS_KEY: [
                {
                    "name": OVERLAP_ENGINE_NAME,
                    "passed": False,
                    "severity": "error",
                    "overlap_count": 0,
                    "contributing_tables": ["wh.a.orders", "wh.b.orders"],
                    "sample": [],
                    "not_evaluated": "key column `id` is missing from wh.b.orders",
                }
            ]
        },
    )

    exec_result = _materialize_streaming(defs, run_result, [ORDERS_KEY, SIBLING_KEY])

    evaluation = _check_evaluations(exec_result)[
        ("fivetran/acme/us_west/shopify/orders", OVERLAP_CHECK)
    ]
    assert evaluation.passed is False
    assert "key column `id` is missing" in evaluation.metadata["not_evaluated"].value


def test_group_check_spec_is_not_declared_on_a_foreign_source_type(tmp_path: Path):
    """``configured_checks`` is keyed by TABLE name alone, so the candidate
    name ``cross_source_overlap:fivetran.orders`` is projected under
    ``orders`` — including for an ``orders`` table under a different source
    type, which is not in that sibling group and can never get the verdict."""
    discover = _discover_with_siblings()
    discover["sources"].append(
        {
            "id": "src_pg",
            "components": {"tenant": "acme", "region": "us_west", "source": "postgres"},
            "source_type": "postgres",
            "tables": [{"name": "orders"}],
        }
    )
    defs = _build_defs(discover, tmp_path, surface_configured_checks=True)

    specs_by_key: dict[dg.AssetKey, set[str]] = {}
    for asset_def in defs.assets or []:
        if isinstance(asset_def, dg.AssetsDefinition):
            for cs in asset_def.check_specs:
                specs_by_key.setdefault(cs.asset_key, set()).add(cs.name)

    assert OVERLAP_CHECK in specs_by_key[ORDERS_KEY]
    assert OVERLAP_CHECK in specs_by_key[SIBLING_KEY]
    postgres_key = dg.AssetKey(["postgres", "acme", "us_west", "postgres", "orders"])
    assert OVERLAP_CHECK not in specs_by_key[postgres_key]


@pytest.mark.parametrize(
    ("name", "source_types", "expected"),
    [
        ("cross_source_overlap:fivetran.orders", {"fivetran"}, True),
        ("cross_source_overlap:fivetran.orders", {"postgres"}, False),
        # A collapsed key folds several native keys onto one asset.
        ("cross_source_overlap:fivetran.orders", {"postgres", "fivetran"}, True),
        # Unknown shape → declared everywhere (fail open, never silently dropped).
        ("some_future_group_check", {"postgres"}, True),
        ("cross_source_overlap:fivetran.orders", set(), True),
    ],
)
def test_group_check_targeting_fails_open(name, source_types, expected):
    from dagster_rocky.component import _group_check_targets_source_types

    assert _group_check_targets_source_types(name, source_types) is expected
