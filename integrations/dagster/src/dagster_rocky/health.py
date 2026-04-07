"""Standalone health-check wrapper around ``rocky doctor``.

The Dagster+ code-location lifecycle supports custom health probes that
run on startup. This module provides a tiny wrapper that calls
:meth:`RockyResource.doctor` with try/except so callers (Dagster+ health
endpoints, custom asset checks, ops, sensors) can decide what to do on
failure without catching the underlying ``dg.Failure`` themselves.

The wrapper lives outside :class:`RockyResource` because attaching it as
a method would require touching the resource module on every iteration,
and ``RockyResource`` is currently being modified by parallel work.
Once the integration stabilizes, this can be promoted to a method on
the resource itself.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import dagster as dg

if TYPE_CHECKING:
    from .resource import RockyResource
    from .types import DoctorResult


@dataclass(frozen=True)
class HealthcheckResult:
    """Outcome of :func:`rocky_healthcheck`.

    Attributes:
        healthy: ``True`` when ``rocky doctor`` ran AND all checks
            passed (i.e. no check is in critical state).
        doctor_result: The parsed :class:`DoctorResult` when the binary
            ran successfully (whether or not all checks passed).
            ``None`` when the binary itself failed to invoke.
        error: Human-readable error message when the binary failed.
            ``None`` on success.
    """

    healthy: bool
    doctor_result: DoctorResult | None
    error: str | None


def rocky_healthcheck(rocky: RockyResource) -> HealthcheckResult:
    """Lightweight health probe suitable for Dagster+ code-location startup.

    Calls :meth:`RockyResource.doctor` and translates the outcome into
    a :class:`HealthcheckResult`. Three cases:

    * ``healthy=True, doctor_result=<result>, error=None`` — ``rocky doctor``
      ran and all checks are non-critical.
    * ``healthy=False, doctor_result=<result>, error=None`` — ``rocky doctor``
      ran but at least one check is critical. Inspect ``doctor_result.checks``.
    * ``healthy=False, doctor_result=None, error=<message>`` — the binary
      itself failed (missing binary, timeout, non-zero exit without
      parseable JSON). The :class:`dg.Failure` is caught and its
      description is returned in ``error``.

    Args:
        rocky: The :class:`RockyResource` to probe.

    Returns:
        A :class:`HealthcheckResult` describing the outcome.
    """
    try:
        result = rocky.doctor()
    except dg.Failure as exc:
        return HealthcheckResult(
            healthy=False,
            doctor_result=None,
            error=str(exc.description or exc),
        )

    any_critical = any(_is_critical(check) for check in result.checks)
    return HealthcheckResult(
        healthy=not any_critical,
        doctor_result=result,
        error=None,
    )


def _is_critical(check: object) -> bool:
    """Return ``True`` when a doctor check is in critical state.

    Tolerant of both the hand-written :class:`HealthCheck` (with
    :class:`HealthStatus` enum) and the generated equivalent (with a
    plain string status). ``warning`` is treated as non-blocking.
    """
    status = getattr(check, "status", None)
    status_str = status.value if hasattr(status, "value") else str(status)
    return status_str.lower() == "critical"
