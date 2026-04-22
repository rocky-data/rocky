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

import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, cast, get_args

import dagster as dg

from .types import (
    StateBackendKind,
    StateHealthResult,
)

if TYPE_CHECKING:
    from datetime import datetime

    from .resource import RockyResource
    from .types import DoctorResult, ProbeOutcome


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


# ---------------------------------------------------------------------------
# State health accessor
# ---------------------------------------------------------------------------

_VALID_BACKENDS: frozenset[str] = frozenset(get_args(StateBackendKind))

#: Mapping from ``RunHistoryRecord.status`` wire values to the normalised
#: :data:`~.types.LastRunStatus` the accessor exposes. The engine formats
#: ``RunStatus`` via ``{:?}`` on the CLI side, so CamelCase variant names
#: land on the wire. Unknown values map to ``"failure"`` — the safer
#: default for anything we don't explicitly recognise as success.
_RUN_STATUS_NORMALISE: dict[str, str] = {
    "success": "success",
    "partialfailure": "partial_failure",
    "partial_failure": "partial_failure",
    "failure": "failure",
}


def state_health(rocky: RockyResource, *, probe_write: bool = False) -> StateHealthResult:
    """Return a live snapshot of Rocky's state-backend health.

    Aggregates the two already-shipped surfaces that expose state-backend
    observability:

    1. A best-effort parse of the configured backend from ``rocky.toml``
       (``[state] backend``). Cheap and network-free; defaults to
       ``"local"`` when the config can't be read or doesn't declare the
       field.
    2. The most recent :class:`~.types.RunHistoryRecord` from
       :meth:`RockyResource.history`. Populates :attr:`~.types.StateHealthResult.last_run_status`
       and :attr:`~.types.StateHealthResult.last_run_at`. A fresh state
       store with no runs yields ``None`` for both, as does a binary
       failure — the accessor is tolerant of ``dagster.Failure`` so a
       caller running this every sensor tick doesn't crash the tick
       when rocky itself can't be invoked.
    3. Optional ``state_rw`` probe: when ``probe_write=True``, runs
       ``rocky doctor`` and extracts the ``state_rw`` check into
       :attr:`~.types.StateHealthResult.probe_outcome` / duration /
       error fields. A probe failure populates the ``probe_*`` fields
       rather than raising. Full doctor is invoked because
       :meth:`RockyResource.doctor` does not currently accept a
       ``--check`` filter — selectively running just the ``state_rw``
       check is a follow-up optimisation that would cut the probe cost
       to the same sub-second figure the engine's
       :func:`rocky_core::state_sync::probe_state_backend` helper runs
       in, without the surrounding config/adapter/pipelines checks.

    The design is a thin facade over existing CLI surfaces — the FR's
    stretch-goal "recent outcomes" rollup (persisted ring buffer of
    state-upload / state-download outcomes) stays deferred because that
    data is tracing-only today and would require new engine-side
    persistence.

    Args:
        rocky: The :class:`~.resource.RockyResource` whose configured
            state backend we're probing.
        probe_write: When ``True``, run ``rocky doctor`` to exercise
            the engine's ``state_rw`` put/get/delete round-trip against
            the configured backend. Default ``False`` — cheap-path only.

    Returns:
        A :class:`~.types.StateHealthResult` describing the current
        state-backend health.
    """
    backend = _read_state_backend(rocky.config_path)
    last_run_status, last_run_at = _last_run_from_history(rocky)

    probe_outcome: ProbeOutcome | None = None
    probe_duration_ms: int | None = None
    probe_error: str | None = None
    if probe_write:
        probe_outcome, probe_duration_ms, probe_error = _run_state_rw_probe(rocky)

    return StateHealthResult(
        backend=backend,
        last_run_status=last_run_status,
        last_run_at=last_run_at,
        probe_outcome=probe_outcome,
        probe_duration_ms=probe_duration_ms,
        probe_error=probe_error,
    )


def _read_state_backend(config_path: str) -> StateBackendKind:
    """Parse ``[state] backend`` from ``rocky.toml`` with local fallback.

    Uses :mod:`tomllib` directly rather than the full engine config
    loader so the accessor stays cheap (no env-var substitution, no
    pipeline parsing) and side-effect-free. Unknown backend strings
    fall back to ``"local"`` — matches what the engine itself assumes
    when the config omits the field.
    """
    try:
        with Path(config_path).open("rb") as fp:
            raw = tomllib.load(fp)
    except (FileNotFoundError, PermissionError, tomllib.TOMLDecodeError, IsADirectoryError):
        return "local"

    state_section = raw.get("state")
    if not isinstance(state_section, dict):
        return "local"
    backend = state_section.get("backend")
    if not isinstance(backend, str):
        return "local"
    normalised = backend.strip().lower()
    if normalised in _VALID_BACKENDS:
        return cast("StateBackendKind", normalised)
    return "local"


def _last_run_from_history(rocky: RockyResource) -> tuple[str | None, datetime | None]:
    """Fetch the most recent run from ``rocky history``, tolerant of failures.

    Returns a two-tuple of (normalised last-run status, ``started_at``
    datetime). Either element is ``None`` when the state store has no
    runs or when the binary itself fails — the accessor is meant to be
    sensor-tick-safe, so ``dagster.Failure`` from a missing binary /
    unreadable state store / parse error is swallowed and reported as
    "unknown" rather than propagated.
    """
    try:
        result = rocky.history()
    except dg.Failure:
        return None, None

    runs = getattr(result, "runs", None)
    if not runs:
        return None, None

    # The CLI returns runs newest-first; take the first entry.
    first = runs[0]
    wire_status = str(getattr(first, "status", "") or "")
    normalised = _RUN_STATUS_NORMALISE.get(wire_status.strip().lower(), "failure")
    started_at = getattr(first, "started_at", None)
    # Guard against the rare case where a (legacy) shape lacks ``runs``
    # or ``started_at`` — treat as "unknown" rather than synthesising.
    if started_at is None:
        return normalised, None
    return normalised, started_at


def _run_state_rw_probe(rocky: RockyResource) -> tuple[ProbeOutcome, int | None, str | None]:
    """Run ``rocky doctor`` and translate the ``state_rw`` check into probe fields.

    Returns a three-tuple of (outcome, duration_ms, error_message). The
    mapping is deliberately tight to the engine's
    :func:`rocky_core::state_sync::probe_state_backend` helper:

    * ``status == healthy`` → ``("ok", duration_ms, None)``.
    * ``status == critical`` with ``"timed out"`` / ``"timeout"`` in
      the message → ``("timeout", duration_ms, message)``. Distinguishes
      the operator-actionable timeout case from generic errors.
    * ``status == critical`` otherwise → ``("error", duration_ms, message)``.
    * ``status == warning`` → ``("error", duration_ms, message)`` —
      the probe is binary on the engine side today, but we degrade
      gracefully if that ever changes.

    When the check is missing (older binary that doesn't emit it yet)
    or the binary itself fails, the probe is reported as an ``error``
    with an explanatory message — a strict caller can then surface it
    as a warning observation without having to distinguish "probe
    wasn't run" from "probe failed."
    """
    try:
        report = rocky.doctor()
    except dg.Failure as exc:
        return "error", None, str(exc.description or exc)

    state_rw = next((c for c in report.checks if _check_name(c) == "state_rw"), None)
    if state_rw is None:
        return "error", None, "rocky doctor did not emit a state_rw check"

    status_str = _check_status(state_rw).lower()
    duration_ms = int(getattr(state_rw, "duration_ms", 0) or 0)
    message = str(getattr(state_rw, "message", "") or "")

    if status_str == "healthy":
        return "ok", duration_ms, None

    # Everything else (critical / warning / unknown) is a probe failure.
    # Surface "timeout" separately when the message carries the engine's
    # canonical timeout substring, otherwise bucket as "error".
    lowered = message.lower()
    if "timed out" in lowered or "timeout" in lowered:
        return "timeout", duration_ms, message
    return "error", duration_ms, message or f"state_rw check returned status={status_str}"


def _check_name(check: object) -> str:
    """Return a check's ``name`` field as a plain string."""
    return str(getattr(check, "name", "") or "")


def _check_status(check: object) -> str:
    """Return a check's ``status`` field as a plain string.

    Tolerates both :class:`~.types.HealthStatus` (hand-written enum) and
    the generated equivalent (plain string), same as :func:`_is_critical`.
    """
    status = getattr(check, "status", None)
    return status.value if hasattr(status, "value") else str(status or "")
