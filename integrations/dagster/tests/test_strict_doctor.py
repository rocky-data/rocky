"""Tests for the strict ``rocky doctor`` startup gate on RockyResource.

Exercises :meth:`RockyResource.setup_for_execution`, covering:

* Default ``strict_doctor=False`` — no doctor call, no startup cost.
* ``strict_doctor=True`` + empty ``strict_doctor_checks`` — fail on any
  critical check ("all").
* ``strict_doctor=True`` + populated list — fail only on matching
  critical checks; non-matching criticals surface as warnings.
* Warning severity never fails (even when the check name is listed).
* Binary failures (missing rocky, timeout, malformed JSON) raise in
  strict mode but are inert in tolerant mode.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import dagster as dg
import pytest

from dagster_rocky.resource import RockyResource
from dagster_rocky.types import DoctorResult, HealthCheck, HealthStatus


def _doctor_result(*pairs: tuple[str, HealthStatus]) -> DoctorResult:
    """Build a DoctorResult from ``(check_name, status)`` pairs."""
    return DoctorResult(
        command="doctor",
        overall="healthy" if all(s == HealthStatus.healthy for _, s in pairs) else "warning",
        checks=[
            HealthCheck(
                name=name,
                status=status,
                message=f"{name} is {status.value}",
                duration_ms=5,
            )
            for name, status in pairs
        ],
        suggestions=[],
    )


def _init_context() -> dg.InitResourceContext:
    """Build a minimal InitResourceContext substitute with a captured log."""
    log = SimpleNamespace(
        info=lambda msg: log._events.append(("info", msg)),  # type: ignore[attr-defined]
        warning=lambda msg: log._events.append(("warning", msg)),  # type: ignore[attr-defined]
    )
    log._events = []  # type: ignore[attr-defined]
    # Only the ``.log`` attribute is touched by setup_for_execution.
    return SimpleNamespace(log=log)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Disabled-mode path (default) — the zero-cost branch
# ---------------------------------------------------------------------------


def test_disabled_mode_skips_doctor_entirely():
    """``strict_doctor=False`` (default) must not call doctor at all."""
    rocky = RockyResource()
    context = _init_context()

    with patch.object(RockyResource, "doctor") as doctor_mock:
        rocky.setup_for_execution(context)  # type: ignore[arg-type]

    doctor_mock.assert_not_called()


def test_disabled_mode_survives_broken_binary():
    """When doctor is disabled, a missing/broken binary does not fail startup."""
    rocky = RockyResource()
    context = _init_context()

    # Even if doctor would raise, disabled mode never reaches it.
    with patch.object(
        RockyResource,
        "doctor",
        side_effect=dg.Failure(description="rocky not found"),
    ):
        rocky.setup_for_execution(context)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Strict + empty list ("all critical") path
# ---------------------------------------------------------------------------


def test_strict_any_critical_fails_on_any_critical():
    """Empty ``strict_doctor_checks`` means fail on any critical check."""
    rocky = RockyResource(strict_doctor=True)
    context = _init_context()
    report = _doctor_result(
        ("config_valid", HealthStatus.healthy),
        ("state_rw", HealthStatus.critical),
    )

    with (
        patch.object(RockyResource, "doctor", return_value=report),
        pytest.raises(dg.Failure) as excinfo,
    ):
        rocky.setup_for_execution(context)  # type: ignore[arg-type]

    # The failing check name should appear in the description.
    assert "state_rw" in str(excinfo.value.description)


def test_strict_any_critical_passes_when_all_checks_are_healthy():
    """Empty list + all checks healthy → no failure."""
    rocky = RockyResource(strict_doctor=True)
    context = _init_context()
    report = _doctor_result(
        ("config_valid", HealthStatus.healthy),
        ("state_rw", HealthStatus.healthy),
    )

    with patch.object(RockyResource, "doctor", return_value=report):
        rocky.setup_for_execution(context)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Strict + allowlist path
# ---------------------------------------------------------------------------


def test_strict_allowlist_fails_only_on_listed_critical():
    """Critical check outside the allowlist logs a warning but does not fail."""
    rocky = RockyResource(strict_doctor=True, strict_doctor_checks=["state_rw"])
    context = _init_context()
    report = _doctor_result(
        ("state_rw", HealthStatus.healthy),
        ("adapter_connect", HealthStatus.critical),  # NOT in allowlist
    )

    with patch.object(RockyResource, "doctor", return_value=report):
        rocky.setup_for_execution(context)  # type: ignore[arg-type]

    # The non-strict critical was surfaced as a warning.
    events = context.log._events  # type: ignore[attr-defined]
    warning_msgs = [m for lvl, m in events if lvl == "warning"]
    assert any("adapter_connect" in m for m in warning_msgs), events


def test_strict_allowlist_fails_on_matching_critical():
    rocky = RockyResource(strict_doctor=True, strict_doctor_checks=["state_rw"])
    context = _init_context()
    report = _doctor_result(
        ("config_valid", HealthStatus.healthy),
        ("state_rw", HealthStatus.critical),
    )

    with (
        patch.object(RockyResource, "doctor", return_value=report),
        pytest.raises(dg.Failure) as excinfo,
    ):
        rocky.setup_for_execution(context)  # type: ignore[arg-type]

    assert "state_rw" in str(excinfo.value.description)


def test_strict_allowlist_with_mixed_critical_reports_both():
    """Failure description lists strict failures; metadata lists non-strict warns."""
    rocky = RockyResource(strict_doctor=True, strict_doctor_checks=["state_rw"])
    context = _init_context()
    report = _doctor_result(
        ("state_rw", HealthStatus.critical),
        ("adapter_connect", HealthStatus.critical),
    )

    with (
        patch.object(RockyResource, "doctor", return_value=report),
        pytest.raises(dg.Failure) as excinfo,
    ):
        rocky.setup_for_execution(context)  # type: ignore[arg-type]

    metadata = excinfo.value.metadata
    assert "strict_failures" in metadata
    assert "non_strict_warnings" in metadata
    # Only state_rw should be in the strict-failures metadata.
    strict_failures_text = metadata["strict_failures"].text  # type: ignore[union-attr]
    non_strict_text = metadata["non_strict_warnings"].text  # type: ignore[union-attr]
    assert "state_rw" in strict_failures_text
    assert "adapter_connect" in non_strict_text


# ---------------------------------------------------------------------------
# Severity-gating — warnings must never raise
# ---------------------------------------------------------------------------


def test_warning_severity_never_fails_even_when_listed():
    """A check in the allowlist that fires with ``warning`` must not raise."""
    rocky = RockyResource(
        strict_doctor=True,
        strict_doctor_checks=["state_rw"],
    )
    context = _init_context()
    report = _doctor_result(("state_rw", HealthStatus.warning))

    with patch.object(RockyResource, "doctor", return_value=report):
        rocky.setup_for_execution(context)  # type: ignore[arg-type]

    # And it should still be logged as a warning so operators can see it.
    events = context.log._events  # type: ignore[attr-defined]
    assert any(lvl == "warning" and "state_rw" in msg for lvl, msg in events)


def test_warning_severity_never_fails_with_empty_allowlist():
    """Empty allowlist (fail-on-any-critical) must not fail on warnings."""
    rocky = RockyResource(strict_doctor=True)
    context = _init_context()
    report = _doctor_result(
        ("config_valid", HealthStatus.healthy),
        ("freshness", HealthStatus.warning),
    )

    with patch.object(RockyResource, "doctor", return_value=report):
        rocky.setup_for_execution(context)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Binary-failure paths
# ---------------------------------------------------------------------------


def test_strict_mode_wraps_binary_failure_as_startup_failure():
    """A missing rocky binary under strict mode surfaces as a startup Failure."""
    rocky = RockyResource(strict_doctor=True)
    context = _init_context()

    with (
        patch.object(
            RockyResource,
            "doctor",
            side_effect=dg.Failure(description="Rocky binary not found"),
        ),
        pytest.raises(dg.Failure) as excinfo,
    ):
        rocky.setup_for_execution(context)  # type: ignore[arg-type]

    description = str(excinfo.value.description)
    assert "rocky doctor could not execute" in description
    assert "not found" in description.lower()


def test_strict_mode_with_no_critical_checks_passes_silently():
    """Doctor with only healthy checks passes in strict mode."""
    rocky = RockyResource(strict_doctor=True, strict_doctor_checks=["state_rw"])
    context = _init_context()
    report = _doctor_result(
        ("config_valid", HealthStatus.healthy),
        ("state_rw", HealthStatus.healthy),
    )

    with patch.object(RockyResource, "doctor", return_value=report):
        rocky.setup_for_execution(context)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Component pass-through
# ---------------------------------------------------------------------------


def test_component_forwards_strict_doctor_to_resource():
    """RockyComponent._get_rocky_resource must propagate both strict fields."""
    from dagster_rocky.component import RockyComponent

    component = RockyComponent(
        strict_doctor=True,
        strict_doctor_checks=["state_rw", "auth"],
    )
    rocky = component._get_rocky_resource()
    assert rocky.strict_doctor is True
    assert rocky.strict_doctor_checks == ["state_rw", "auth"]


def test_component_default_leaves_resource_tolerant():
    """Default component config means strict_doctor=False on the resource."""
    from dagster_rocky.component import RockyComponent

    component = RockyComponent()
    rocky = component._get_rocky_resource()
    assert rocky.strict_doctor is False
    assert rocky.strict_doctor_checks == []
