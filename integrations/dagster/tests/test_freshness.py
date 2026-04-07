"""Tests for freshness policy mapping from Rocky checks config."""

from __future__ import annotations

from unittest.mock import patch

import dagster as dg

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
from dagster_rocky.component import _build_group_contexts
from dagster_rocky.translator import RockyDagsterTranslator


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
