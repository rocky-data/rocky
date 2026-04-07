"""Observability builders for surfacing Rocky run signals as Dagster events.

Rocky's ``RunResult`` contains four observability signals that the
``RockyComponent`` only logs as warnings today: drift detection actions,
row-count anomalies, contract violations, and optimization recommendations.
This module provides pure-function builders that translate each of those
signals into the corresponding Dagster primitive:

* :func:`drift_observations` — yields one :class:`dg.AssetObservation` per
  drift action (ALTER COLUMN, DROP+RECREATE, …). Drift is a *change*, not
  a pass/fail, so observation is the right primitive — it shows up on the
  asset timeline as a discrete event without affecting check status.

* :func:`anomaly_check_results` — yields one :class:`dg.AssetCheckResult`
  with severity ``WARN`` per row-count anomaly. The check name is
  :data:`ANOMALY_CHECK_NAME` so callers can pre-declare a matching
  :class:`dg.AssetCheckSpec`.

* :func:`optimize_metadata_for_keys` — returns a dict of
  ``{asset_key: metadata}`` mapping each Rocky model to its strategy
  recommendation, suitable for merging into ``AssetSpec.metadata`` at
  load time.

These builders are deliberately decoupled from :class:`RockyComponent` so:

1. They are unit-testable in isolation without spinning up the component.
2. Users with hand-rolled multi-assets (not using ``RockyComponent``) can
   still adopt the same observability patterns.
3. The integration into ``component._emit_results`` is a one-line yield
   per kind, kept small enough to land independently from the helpers.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import dagster as dg

from .checks import cost_metadata_from_optimize

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from .types import OptimizeResult, RunResult

    #: Type alias for the key resolver callback used by drift/anomaly builders.
    #: Takes a Rocky table identifier (possibly ``catalog.schema.table``) and
    #: returns the corresponding Dagster ``AssetKey``, or ``None`` if no mapping
    #: exists or the table is not in the current selection.
    KeyResolver = Callable[[str], dg.AssetKey | None]


#: Canonical Dagster check name used to surface row-count anomalies. Pre-declare
#: a matching :class:`dg.AssetCheckSpec` with this name on every Rocky asset
#: that should expose anomaly detection in the UI before any run.
ANOMALY_CHECK_NAME: str = "row_count_anomaly"


# ---------------------------------------------------------------------------
# Drift → AssetObservation
# ---------------------------------------------------------------------------


def drift_observations(
    run_result: RunResult,
    *,
    key_resolver: KeyResolver,
) -> Iterator[dg.AssetObservation]:
    """Yield one ``AssetObservation`` per drift action in a ``RunResult``.

    Args:
        run_result: The run result to inspect. ``run_result.drift.actions_taken``
            is iterated; each action becomes one observation.
        key_resolver: Callable taking a Rocky table identifier (a string,
            possibly ``catalog.schema.table``) and returning the Dagster
            ``AssetKey`` it maps to, or ``None`` if no mapping exists. The
            caller passes a function backed by their group's
            ``rocky_key_to_dagster_key`` map.

    Yields:
        ``dg.AssetObservation`` events with ``rocky/drift_*`` metadata
        keys, one per drift action. Tables that don't resolve are
        silently skipped (the resolver decides whether to log).
    """
    for action in run_result.drift.actions_taken:
        asset_key = key_resolver(action.table)
        if asset_key is None:
            continue
        yield dg.AssetObservation(
            asset_key=asset_key,
            description=f"Schema drift: {action.action}",
            metadata={
                "rocky/drift_action": dg.MetadataValue.text(action.action),
                "rocky/drift_reason": dg.MetadataValue.text(action.reason),
                "rocky/drift_table": dg.MetadataValue.text(action.table),
                "rocky/drift_tables_checked": dg.MetadataValue.int(run_result.drift.tables_checked),
                "rocky/drift_tables_drifted": dg.MetadataValue.int(run_result.drift.tables_drifted),
            },
        )


# ---------------------------------------------------------------------------
# Anomalies → AssetCheckResult (WARN)
# ---------------------------------------------------------------------------


def anomaly_check_results(
    run_result: RunResult,
    *,
    key_resolver: KeyResolver,
) -> Iterator[dg.AssetCheckResult]:
    """Yield one ``AssetCheckResult`` per row-count anomaly in a ``RunResult``.

    Each anomaly becomes a check result with::

        check_name = ANOMALY_CHECK_NAME ("row_count_anomaly")
        passed     = False
        severity   = dg.AssetCheckSeverity.WARN

    The check is severity WARN (not ERROR) because Rocky's anomaly
    detection is a heuristic — a row-count deviation may be legitimate
    business behavior, not a data-quality failure. Callers who want to
    treat anomalies as hard failures can post-process or override.

    Args:
        run_result: The run result to inspect.
        key_resolver: Same shape as :func:`drift_observations`.

    Yields:
        ``dg.AssetCheckResult`` events with ``rocky/current_count``,
        ``rocky/baseline_avg``, ``rocky/deviation_pct`` and
        ``rocky/reason`` metadata.
    """
    for anomaly in run_result.anomalies:
        asset_key = key_resolver(anomaly.table)
        if asset_key is None:
            continue
        yield dg.AssetCheckResult(
            asset_key=asset_key,
            check_name=ANOMALY_CHECK_NAME,
            passed=False,
            severity=dg.AssetCheckSeverity.WARN,
            metadata={
                "rocky/current_count": dg.MetadataValue.int(anomaly.current_count),
                "rocky/baseline_avg": dg.MetadataValue.float(anomaly.baseline_avg),
                "rocky/deviation_pct": dg.MetadataValue.float(anomaly.deviation_pct),
                "rocky/reason": dg.MetadataValue.text(anomaly.reason),
            },
        )


# ---------------------------------------------------------------------------
# Optimize → AssetSpec.metadata
# ---------------------------------------------------------------------------


def optimize_metadata_for_keys(
    optimize_result: OptimizeResult,
    *,
    model_to_key: dict[str, dg.AssetKey],
) -> dict[dg.AssetKey, dict[str, dg.MetadataValue]]:
    """Build per-asset-key optimize metadata from a ``rocky optimize`` result.

    Returns a mapping of ``AssetKey`` → metadata dict ready to merge into
    ``AssetSpec.metadata`` at load time so the Dagster UI shows current
    strategy, recommended strategy, estimated savings, and reasoning
    without requiring a run.

    Args:
        optimize_result: The result returned by ``RockyResource.optimize()``.
        model_to_key: A mapping of Rocky model name → Dagster ``AssetKey``.
            The caller is responsible for building this from their
            translator. Models in ``optimize_result`` that are not present
            in the map are silently ignored.

    Returns:
        ``{asset_key: {field_name: MetadataValue}}``. The metadata field
        names are namespaced under ``rocky/`` for the UI.
    """
    cost_data = cost_metadata_from_optimize(optimize_result)
    out: dict[dg.AssetKey, dict[str, dg.MetadataValue]] = {}
    for model_name, fields in cost_data.items():
        asset_key = model_to_key.get(model_name)
        if asset_key is None:
            continue
        metadata: dict[str, dg.MetadataValue] = {}
        if (current := fields.get("current_strategy")) is not None:
            metadata["rocky/current_strategy"] = dg.MetadataValue.text(str(current))
        if (recommended := fields.get("recommended_strategy")) is not None:
            metadata["rocky/recommended_strategy"] = dg.MetadataValue.text(str(recommended))
        if (savings := fields.get("estimated_monthly_savings")) is not None:
            metadata["rocky/estimated_monthly_savings"] = dg.MetadataValue.float(float(savings))
        if (reasoning := fields.get("reasoning")) is not None:
            metadata["rocky/optimize_reasoning"] = dg.MetadataValue.text(str(reasoning))
        if metadata:
            out[asset_key] = metadata
    return out
