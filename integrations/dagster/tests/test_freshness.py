"""Tests for freshness policy mapping from Rocky checks config."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import dagster as dg
import pytest

from dagster_rocky import (
    ChecksConfig,
    DiscoverResult,
    FreshnessConfig,
    RockyResource,
    SourceInfo,
    TableInfo,
    freshness_policy_from_checks,
    load_rocky_assets,
)
from dagster_rocky.component import RockyComponent, _build_group_contexts
from dagster_rocky.freshness import freshness_is_configured
from dagster_rocky.translator import RockyDagsterTranslator
from dagster_rocky.types import RunResult


def _discover_with_freshness(threshold_seconds: int) -> DiscoverResult:
    return DiscoverResult(
        version="0.3.0",
        command="discover",
        sources=[
            SourceInfo(
                id="src_001",
                components={"tenant": "acme", "region": "us_west", "source": "shopify"},
                source_type="fivetran",
                tables=[TableInfo(name="orders"), TableInfo(name="payments")],
            )
        ],
        checks=ChecksConfig(freshness=FreshnessConfig(threshold_seconds=threshold_seconds)),
    )


def _discover_without_freshness() -> DiscoverResult:
    return DiscoverResult(
        version="0.3.0",
        command="discover",
        sources=[
            SourceInfo(
                id="src_001",
                components={"tenant": "acme", "region": "us_west", "source": "shopify"},
                source_type="fivetran",
                tables=[TableInfo(name="orders")],
            )
        ],
        checks=None,
    )


# ---------------------------------------------------------------------------
# freshness_policy_from_checks helper
# ---------------------------------------------------------------------------


def test_freshness_policy_from_checks_returns_none_when_checks_missing():
    assert freshness_policy_from_checks(None) is None


def test_freshness_policy_from_checks_returns_none_when_freshness_missing():
    assert freshness_policy_from_checks(ChecksConfig(freshness=None)) is None


def test_freshness_policy_from_checks_builds_time_window_policy():
    policy = freshness_policy_from_checks(
        ChecksConfig(freshness=FreshnessConfig(threshold_seconds=3600))
    )
    assert policy is not None
    # Dagster 1.12+ wraps the timedelta in SerializableTimeDelta which does
    # not compare-equal to a plain timedelta — convert via .to_timedelta().
    assert policy.fail_window.to_timedelta().total_seconds() == 3600
    assert policy.warn_window is None


# ---------------------------------------------------------------------------
# load_rocky_assets wiring
# ---------------------------------------------------------------------------


def test_load_rocky_assets_attaches_freshness_when_configured():
    # RockyResource is a frozen Pydantic model, so we patch the class-level
    # method rather than the instance attribute.
    rocky = RockyResource(binary_path="rocky", config_path="rocky.toml")
    with patch.object(RockyResource, "discover", return_value=_discover_with_freshness(7200)):
        specs = load_rocky_assets(rocky)

    assert len(specs) == 2
    for spec in specs:
        assert spec.freshness_policy is not None
        assert spec.freshness_policy.fail_window.to_timedelta().total_seconds() == 7200


def test_load_rocky_assets_leaves_freshness_unset_when_not_configured():
    rocky = RockyResource(binary_path="rocky", config_path="rocky.toml")
    with patch.object(RockyResource, "discover", return_value=_discover_without_freshness()):
        specs = load_rocky_assets(rocky)

    assert len(specs) == 1
    assert specs[0].freshness_policy is None


# ---------------------------------------------------------------------------
# RockyComponent group-context wiring
# ---------------------------------------------------------------------------


def test_build_group_contexts_propagates_freshness_to_specs():
    discover = _discover_with_freshness(86400)
    groups = _build_group_contexts(discover, RockyDagsterTranslator())

    assert len(groups) == 1
    group = groups[0]
    assert len(group.specs) == 2
    for spec in group.specs:
        assert isinstance(spec, dg.AssetSpec)
        assert spec.freshness_policy is not None
        assert spec.freshness_policy.fail_window.to_timedelta().total_seconds() == 86400


def test_build_group_contexts_leaves_freshness_unset_when_missing():
    groups = _build_group_contexts(_discover_without_freshness(), RockyDagsterTranslator())
    assert len(groups) == 1
    for spec in groups[0].specs:
        assert spec.freshness_policy is None


# ---------------------------------------------------------------------------
# Fixture round-trip — verify the on-disk fixture deserializes correctly
# ---------------------------------------------------------------------------


def test_discover_fixture_includes_freshness(discover_json):
    result = DiscoverResult.model_validate_json(discover_json)
    assert result.checks is not None
    assert result.checks.freshness is not None
    assert result.checks.freshness.threshold_seconds == 86400


# ---------------------------------------------------------------------------
# Per-model freshness (T1.2) — freshness_policy_from_model + per_model_freshness_policies
# ---------------------------------------------------------------------------


def test_freshness_policy_from_model_returns_none_when_freshness_missing():
    from dagster_rocky import freshness_policy_from_model

    assert freshness_policy_from_model(None) is None


def test_freshness_policy_from_model_builds_time_window_policy():
    from dagster_rocky import freshness_policy_from_model
    from dagster_rocky.types import ModelFreshnessConfig

    policy = freshness_policy_from_model(ModelFreshnessConfig(max_lag_seconds=3600))
    assert policy is not None
    assert policy.fail_window.to_timedelta().total_seconds() == 3600


def test_per_model_freshness_policies_indexes_by_model_name():
    from dagster_rocky import per_model_freshness_policies
    from dagster_rocky.types import (
        CompileResult,
        ModelDetail,
        ModelFreshnessConfig,
    )

    result = CompileResult(
        version="0.3.0",
        command="compile",
        models=2,
        execution_layers=1,
        diagnostics=[],
        has_errors=False,
        models_detail=[
            ModelDetail(
                name="orders",
                strategy={"type": "incremental", "timestamp_column": "updated_at"},
                target={"catalog": "warehouse", "schema": "marts", "table": "orders"},
                freshness=ModelFreshnessConfig(max_lag_seconds=7200),
            ),
            ModelDetail(
                name="dim_users",
                strategy={"type": "full_refresh"},
                target={"catalog": "warehouse", "schema": "marts", "table": "dim_users"},
                freshness=None,
            ),
        ],
    )

    policies = per_model_freshness_policies(result)

    assert "orders" in policies
    assert policies["orders"].fail_window.to_timedelta().total_seconds() == 7200
    # Models without freshness frontmatter are absent so callers can use
    # .get(name) to fall back to the pipeline-level default.
    assert "dim_users" not in policies


def test_per_model_freshness_policies_handles_none_compile_result():
    """When `rocky compile` has never run for the project, the helper
    returns an empty dict — no error."""
    from dagster_rocky import per_model_freshness_policies

    assert per_model_freshness_policies(None) == {}


def test_per_model_freshness_overrides_pipeline_default():
    """End-to-end: when a model name matches a source-replication table,
    the per-model freshness wins over the pipeline-level default."""
    from dagster_rocky.component import _build_group_contexts
    from dagster_rocky.translator import RockyDagsterTranslator
    from dagster_rocky.types import ModelFreshnessConfig

    discover = _discover_with_freshness(86400)  # default 24h
    # Override 'orders' specifically with a 1h policy
    model_policies = {
        "orders": dg.FreshnessPolicy.time_window(
            fail_window=__import__("datetime").timedelta(seconds=3600)
        ),
    }

    groups = _build_group_contexts(discover, RockyDagsterTranslator(), model_policies)
    assert len(groups) == 1
    by_name = {spec.key.path[-1]: spec for spec in groups[0].specs}

    # 'orders' uses the per-model 1h policy
    assert by_name["orders"].freshness_policy.fail_window.to_timedelta().total_seconds() == 3600
    # 'payments' falls back to the pipeline-level 24h default
    assert by_name["payments"].freshness_policy.fail_window.to_timedelta().total_seconds() == 86400
    # Silence unused-import warning so the helper is exercised
    _ = ModelFreshnessConfig


# ---------------------------------------------------------------------------
# freshness_is_configured — the one predicate behind BOTH the FreshnessPolicy
# and the pre-declared `freshness` AssetCheckSpec (#1645)
# ---------------------------------------------------------------------------

#: (id, checks projection, freshness is configured?)
_FRESHNESS_CASES = [
    ("no_checks_projection", None, False),
    ("checks_without_freshness", ChecksConfig(freshness=None), False),
    (
        "checks_with_freshness",
        ChecksConfig(freshness=FreshnessConfig(threshold_seconds=3600)),
        True,
    ),
]


@pytest.mark.parametrize(
    ("checks", "expected"),
    [(c, e) for _, c, e in _FRESHNESS_CASES],
    ids=[i for i, _, _ in _FRESHNESS_CASES],
)
def test_freshness_is_configured(checks: ChecksConfig | None, expected: bool):
    """``checks=None`` means no ``[checks]`` block at all (or a binary that
    predates the projection). Both read as "no freshness"."""
    assert freshness_is_configured(checks) is expected


@pytest.mark.parametrize(
    "checks",
    [c for _, c, _ in _FRESHNESS_CASES],
    ids=[i for i, _, _ in _FRESHNESS_CASES],
)
def test_freshness_is_configured_agrees_with_the_policy(checks: ChecksConfig | None):
    """The check-spec gate and the FreshnessPolicy gate must never disagree.

    The component declares the ``freshness`` :class:`dg.AssetCheckSpec` when
    :func:`freshness_is_configured` is ``True`` and attaches a
    :class:`dg.FreshnessPolicy` when :func:`freshness_policy_from_checks`
    returns one. If those two ever diverge, an asset gets a stale-data badge
    with no check behind it, or the reverse. This pins the equivalence.
    """
    assert freshness_is_configured(checks) is (freshness_policy_from_checks(checks) is not None)


# ---------------------------------------------------------------------------
# The `freshness` AssetCheckSpec is declared only when the pipeline configures
# freshness (#1645). The engine gates its `freshness` CheckResult on
# `[checks.freshness]` (rocky-cli `commands/run.rs`), so an unconditional spec
# left `_emit_placeholder_checks` reporting `passed=True` for a check that
# never ran, on every materialized table.
# ---------------------------------------------------------------------------

_ORDERS_KEY = dg.AssetKey(["fivetran", "acme", "shopify", "orders"])
_OTHER_DEFAULT_CHECKS = {"row_count", "column_match", "row_count_anomaly"}


def _state_file(tmp_path: Path, checks: dict | None) -> Path:
    """Write a component state file for one source with one table."""
    discover: dict = {
        "version": "0.3.0",
        "command": "discover",
        "sources": [
            {
                "id": "src_001",
                "components": {"tenant": "acme", "source": "shopify"},
                "source_type": "fivetran",
                "tables": [{"name": "orders"}],
            }
        ],
    }
    if checks is not None:
        discover["checks"] = checks
    state_file = tmp_path / "state.json"
    state_file.write_text(json.dumps({"discover": discover}))
    return state_file


def _declared_check_names(tmp_path: Path, checks: dict | None) -> set[str]:
    component = RockyComponent(config_path="rocky.toml")
    defs = component.build_defs_from_state(context=None, state_path=_state_file(tmp_path, checks))
    return {
        cs.name
        for a in (defs.assets or [])
        if isinstance(a, dg.AssetsDefinition)
        for cs in a.check_specs
    }


def test_freshness_check_spec_declared_when_pipeline_configures_freshness(tmp_path: Path):
    names = _declared_check_names(tmp_path, {"freshness": {"threshold_seconds": 86400}})
    assert names == _OTHER_DEFAULT_CHECKS | {"freshness"}


def test_freshness_check_spec_absent_when_there_is_no_checks_block(tmp_path: Path):
    """`rocky discover` omits `checks` entirely when the pipeline declares
    neither freshness nor any non-default check."""
    names = _declared_check_names(tmp_path, None)
    assert names == _OTHER_DEFAULT_CHECKS


def test_freshness_check_spec_absent_when_checks_block_has_no_freshness(tmp_path: Path):
    """`[checks]` exists (a non-default check is configured) but there is no
    `[checks.freshness]`, so `discover.checks.freshness` is null."""
    names = _declared_check_names(tmp_path, {"freshness": None, "configured_checks": {}})
    assert names == _OTHER_DEFAULT_CHECKS


def test_materialized_table_reports_no_freshness_verdict_without_a_freshness_config(
    tmp_path: Path,
):
    """The bug, end to end: rocky copies the table and emits no `freshness`
    result, because the pipeline declares no `[checks.freshness]`.

    Before the fix the placeholder pass reported `freshness` as
    `passed=True` ("not produced by rocky") — a green badge for a check that
    never ran. Now no `freshness` evaluation is recorded at all, while the
    other declared defaults still are.
    """
    component = RockyComponent(config_path="rocky.toml")
    defs = component.build_defs_from_state(context=None, state_path=_state_file(tmp_path, None))
    asset_defs = [a for a in (defs.assets or []) if isinstance(a, dg.AssetsDefinition)]

    run_result = RunResult.model_validate(
        {
            "version": "0.3.0",
            "command": "run",
            "filter": "tenant=acme",
            "duration_ms": 10,
            "tables_copied": 1,
            "tables_failed": 0,
            "materializations": [
                {
                    "asset_key": ["fivetran", "acme", "shopify", "orders"],
                    "rows_copied": 100,
                    "duration_ms": 5,
                    "metadata": {"strategy": "full_refresh"},
                }
            ],
            # The engine emits NO freshness result: `[checks.freshness]` is absent.
            "check_results": [],
            "errors": [],
            "excluded_tables": [],
            "contained": [],
            "permissions": {
                "grants_added": 0,
                "grants_revoked": 0,
                "catalogs_created": 0,
                "schemas_created": 0,
            },
            "drift": {"tables_checked": 0, "tables_drifted": 0, "actions_taken": []},
        }
    )

    with (
        patch.object(RockyResource, "run", return_value=run_result),
        patch.object(RockyResource, "run_streaming", return_value=run_result),
    ):
        result = dg.materialize(
            asset_defs,
            resources={"rocky": RockyResource(config_path="rocky.toml")},
            selection=[_ORDERS_KEY],
            raise_on_error=False,
        )

    assert result.success
    evaluated = {
        e.event_specific_data.check_name
        for e in result.all_events
        if e.event_type_value == "ASSET_CHECK_EVALUATION"
    }
    assert "freshness" not in evaluated
    # The op really ran and the other declared defaults still get a verdict.
    assert evaluated == _OTHER_DEFAULT_CHECKS
