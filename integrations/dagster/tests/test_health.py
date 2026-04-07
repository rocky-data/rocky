"""Tests for ``rocky_healthcheck``."""

from __future__ import annotations

from unittest.mock import patch

import dagster as dg

from dagster_rocky.health import HealthcheckResult, rocky_healthcheck
from dagster_rocky.resource import RockyResource
from dagster_rocky.types import DoctorResult, HealthCheck, HealthStatus


def _doctor_result(*statuses: HealthStatus) -> DoctorResult:
    return DoctorResult(
        command="doctor",
        overall="healthy" if all(s == HealthStatus.healthy for s in statuses) else "warning",
        checks=[
            HealthCheck(
                name=f"check_{i}",
                status=status,
                message="ok",
                duration_ms=10,
            )
            for i, status in enumerate(statuses)
        ],
        suggestions=[],
    )


def test_rocky_healthcheck_healthy_when_all_checks_pass():
    rocky = RockyResource()
    result = _doctor_result(HealthStatus.healthy, HealthStatus.healthy)

    with patch.object(RockyResource, "doctor", return_value=result):
        outcome = rocky_healthcheck(rocky)

    assert isinstance(outcome, HealthcheckResult)
    assert outcome.healthy is True
    assert outcome.doctor_result is result
    assert outcome.error is None


def test_rocky_healthcheck_warning_status_is_treated_as_healthy():
    """Warnings are non-blocking — only critical fails the check."""
    rocky = RockyResource()
    result = _doctor_result(HealthStatus.healthy, HealthStatus.warning)

    with patch.object(RockyResource, "doctor", return_value=result):
        outcome = rocky_healthcheck(rocky)

    assert outcome.healthy is True
    assert outcome.doctor_result is result
    assert outcome.error is None


def test_rocky_healthcheck_critical_status_marks_unhealthy():
    rocky = RockyResource()
    result = _doctor_result(HealthStatus.healthy, HealthStatus.critical)

    with patch.object(RockyResource, "doctor", return_value=result):
        outcome = rocky_healthcheck(rocky)

    assert outcome.healthy is False
    assert outcome.doctor_result is result
    assert outcome.error is None


def test_rocky_healthcheck_binary_failure_returns_error_message():
    rocky = RockyResource()
    failure = dg.Failure(description="Rocky binary not found")

    with patch.object(RockyResource, "doctor", side_effect=failure):
        outcome = rocky_healthcheck(rocky)

    assert outcome.healthy is False
    assert outcome.doctor_result is None
    assert outcome.error is not None
    assert "binary" in outcome.error.lower()


def test_rocky_healthcheck_empty_checks_list_is_healthy():
    """A doctor result with zero checks is degenerate but should not fail."""
    rocky = RockyResource()
    result = _doctor_result()  # no statuses

    with patch.object(RockyResource, "doctor", return_value=result):
        outcome = rocky_healthcheck(rocky)

    assert outcome.healthy is True
    assert outcome.doctor_result is result
