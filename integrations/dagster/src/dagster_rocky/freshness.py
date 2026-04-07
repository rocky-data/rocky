"""Mapping helpers from Rocky check config to Dagster freshness primitives.

Rocky's ``[checks.freshness]`` configuration declares a single
``threshold_seconds`` value: a source replication asset is considered stale
if it has not been refreshed within that window. Dagster 1.12+ expresses the
same idea via :class:`dagster.FreshnessPolicy.time_window`, with a
``fail_window`` timedelta after which the asset is reported as failing its
freshness policy.

This module is the single place that knows how to translate between the two
worlds, so the wiring at every call site (``load_rocky_assets``,
``RockyComponent._build_asset_spec``) is a one-line lookup.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

import dagster as dg

from .types import ChecksConfig

if TYPE_CHECKING:
    from .types import CompileResult, ModelFreshnessConfig


def freshness_policy_from_checks(checks: ChecksConfig | None) -> dg.FreshnessPolicy | None:
    """Build a Dagster ``FreshnessPolicy`` from Rocky's projected checks config.

    Returns ``None`` when the pipeline has no ``[checks.freshness]`` block,
    in which case the caller should leave ``AssetSpec.freshness_policy``
    unset and rely on Dagster's default behavior (no freshness expectation).

    The returned policy uses :meth:`dagster.FreshnessPolicy.time_window` —
    the canonical 1.12+ API for "stale if not materialized within X
    seconds". The legacy ``FreshnessPolicy(maximum_lag_minutes=...)`` ctor
    is intentionally avoided.
    """
    if checks is None or checks.freshness is None:
        return None

    return dg.FreshnessPolicy.time_window(
        fail_window=timedelta(seconds=checks.freshness.threshold_seconds),
    )


def freshness_policy_from_model(
    freshness: ModelFreshnessConfig | None,
) -> dg.FreshnessPolicy | None:
    """Build a Dagster ``FreshnessPolicy`` from a per-model freshness config.

    Mirrors :func:`freshness_policy_from_checks` but for the per-model
    case: ``ModelConfig.freshness`` projects to a single
    ``max_lag_seconds`` value, which becomes the ``fail_window`` of a
    :meth:`dagster.FreshnessPolicy.time_window`.

    Returns ``None`` when the model has no freshness frontmatter.
    """
    if freshness is None:
        return None

    return dg.FreshnessPolicy.time_window(
        fail_window=timedelta(seconds=freshness.max_lag_seconds),
    )


def per_model_freshness_policies(
    compile_result: CompileResult | None,
) -> dict[str, dg.FreshnessPolicy]:
    """Index per-model freshness policies by model name from a compile result.

    Returns a ``{model_name: FreshnessPolicy}`` dict containing only the
    models that declared a ``[freshness]`` frontmatter block. Models
    without frontmatter are absent from the result so callers can use
    ``policies.get(name)`` to fall back to ``None`` (or to the
    pipeline-level default from :func:`freshness_policy_from_checks`).

    ``compile_result`` may be ``None`` (e.g. when ``rocky compile`` has
    never been run for the project) — returns an empty dict in that case.
    """
    if compile_result is None:
        return {}

    out: dict[str, dg.FreshnessPolicy] = {}
    for model in compile_result.models_detail:
        policy = freshness_policy_from_model(model.freshness)
        if policy is not None:
            out[model.name] = policy
    return out
