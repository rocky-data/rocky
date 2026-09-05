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

#: Name of the built-in freshness check. The engine emits a ``CheckResult``
#: under this exact name, and only when the pipeline declares
#: ``[checks.freshness]`` — see :func:`freshness_is_configured`.
FRESHNESS_CHECK_NAME: str = "freshness"


def freshness_is_configured(checks: ChecksConfig | None) -> bool:
    """Return ``True`` when the given ``rocky discover`` projection reports a
    ``[checks.freshness]`` config.

    This reads a PROJECTION, not the engine. It answers "the discover output in
    front of me says freshness is configured", which is the best available
    stand-in for "the engine will emit a ``freshness`` ``CheckResult``" — but it
    is not the same statement. It is wrong in two known cases, both of which
    make it say ``False`` while the engine emits a result:

    * **Stale state.** ``RockyComponent`` caches ``rocky discover``. Adding
      ``[checks.freshness]`` to ``rocky.toml`` without refreshing the state
      leaves this ``False``.
    * **An old binary.** A ``rocky`` that predates the ``checks`` projection
      emits no ``checks`` field at all, which parses as ``None``.

    In both cases the emitted result meets no declared spec: the streaming path
    drops it (``_emit_results``) and the Pipes path fails the step. Refresh the
    state after changing ``[checks]``.

    Two consumers depend on it:

    * :func:`freshness_policy_from_checks` returns the pipeline-level
      :class:`dagster.FreshnessPolicy` only when it is ``True``;
    * ``RockyComponent`` pre-declares the ``freshness``
      :class:`dagster.AssetCheckSpec` only when it is ``True``.

    Both must agree, so they read the same predicate. ``tests/test_freshness.py``
    pins the equivalence.

    A per-model ``[freshness]`` frontmatter is a separate thing: it can put a
    :class:`dagster.FreshnessPolicy` on one asset through
    :func:`freshness_policy_from_model` while this predicate is ``False``. That
    policy is Dagster's own staleness evaluation. The engine never emits a
    ``freshness`` ``CheckResult`` for it — per-model ``max_lag_seconds`` is read
    by ``rocky tick`` and ``rocky validate``, not by the check runner — so it
    does not change the answer here.

    ``checks`` is ``None`` when the pipeline declares no ``[checks]`` block at
    all — the engine's ``ChecksConfigOutput::from_engine`` returns ``None`` when
    there is neither a freshness config nor a configured check — and also in the
    old-binary case above. The two are indistinguishable here.
    """
    return checks is not None and checks.freshness is not None


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
