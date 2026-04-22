"""Tests for :func:`dagster_rocky.health.state_health` and
:meth:`RockyResource.state_health`.

The accessor aggregates two already-shipped surfaces:

* ``rocky history --output json`` (for ``last_run_status`` / ``last_run_at``)
* ``rocky doctor`` (for the ``state_rw`` probe, when ``probe_write=True``)

plus a ``tomllib`` read of the configured ``rocky.toml`` for the backend
type. All three sources are mocked here via ``patch.object`` so the tests
stay binary-free and rely on hand-written fixtures rather than the
playground POC corpus.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import dagster as dg
import pytest

from dagster_rocky.health import state_health
from dagster_rocky.resource import RockyResource
from dagster_rocky.types import (
    DoctorResult,
    HealthCheck,
    HealthStatus,
    HistoryResult,
    StateHealthResult,
)
from dagster_rocky.types_generated import RunHistoryRecord

# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _doctor(
    *,
    state_rw: HealthCheck | None,
    extras: list[HealthCheck] | None = None,
) -> DoctorResult:
    """Build a :class:`DoctorResult` containing the given state_rw check.

    ``extras`` lets a test include other checks (config, state, auth) so
    the parser exercises the name-filtering logic rather than picking
    the first entry blindly.
    """
    checks: list[HealthCheck] = list(extras or [])
    if state_rw is not None:
        checks.append(state_rw)
    # Overall is irrelevant to ``state_health`` — keep it healthy to
    # avoid confusing assertions when the probe itself is unhealthy.
    return DoctorResult(
        command="doctor",
        overall="healthy",
        checks=checks,
        suggestions=[],
    )


def _history_with_runs(*runs: RunHistoryRecord) -> HistoryResult:
    """Wrap one or more :class:`RunHistoryRecord`s in a :class:`HistoryResult`."""
    return HistoryResult(
        version="1.13.0",
        command="history",
        count=len(runs),
        runs=list(runs),
    )


def _run_history_record(
    *,
    run_id: str = "run-001",
    status: str = "Success",
    started_at: datetime | None = None,
) -> RunHistoryRecord:
    """Build a single :class:`RunHistoryRecord` with defaults suitable for tests."""
    return RunHistoryRecord(
        run_id=run_id,
        started_at=started_at or datetime(2026, 4, 22, 12, 0, 0, tzinfo=UTC),
        status=status,
        trigger="Manual",
        models_executed=1,
        duration_ms=42,
        models=[],
    )


@pytest.fixture
def rocky_with_local_backend(tmp_path: Path) -> RockyResource:
    """:class:`RockyResource` pointed at a minimal ``rocky.toml``.

    Writing a real config file is the simplest way to exercise the
    ``tomllib`` branch in ``_read_state_backend`` without having to
    patch :mod:`tomllib` itself. Each test can override the file to
    test other backends.
    """
    config = tmp_path / "rocky.toml"
    config.write_text('[state]\nbackend = "local"\n')
    return RockyResource(config_path=str(config))


# ---------------------------------------------------------------------------
# Backend parsing
# ---------------------------------------------------------------------------


def test_state_health_reads_local_backend_from_config(
    rocky_with_local_backend: RockyResource,
) -> None:
    with (
        patch.object(RockyResource, "history", return_value=_history_with_runs()),
    ):
        result = state_health(rocky_with_local_backend)

    assert isinstance(result, StateHealthResult)
    assert result.backend == "local"


@pytest.mark.parametrize("backend", ["tiered", "valkey", "s3", "gcs"])
def test_state_health_reads_non_local_backend_from_config(tmp_path: Path, backend: str) -> None:
    config = tmp_path / "rocky.toml"
    config.write_text(f'[state]\nbackend = "{backend}"\n')
    rocky = RockyResource(config_path=str(config))

    with patch.object(RockyResource, "history", return_value=_history_with_runs()):
        result = state_health(rocky)

    assert result.backend == backend


def test_state_health_falls_back_to_local_when_config_missing(tmp_path: Path) -> None:
    """A non-existent config path should not crash — defaults to ``local``."""
    rocky = RockyResource(config_path=str(tmp_path / "does-not-exist.toml"))

    with patch.object(RockyResource, "history", return_value=_history_with_runs()):
        result = state_health(rocky)

    assert result.backend == "local"


def test_state_health_falls_back_to_local_on_malformed_toml(tmp_path: Path) -> None:
    """Garbage in ``rocky.toml`` should not crash the sensor tick."""
    config = tmp_path / "rocky.toml"
    config.write_text("this is not = valid [toml\n")
    rocky = RockyResource(config_path=str(config))

    with patch.object(RockyResource, "history", return_value=_history_with_runs()):
        result = state_health(rocky)

    assert result.backend == "local"


def test_state_health_falls_back_to_local_when_backend_field_missing(tmp_path: Path) -> None:
    """A config with no ``[state]`` table should default to ``local``."""
    config = tmp_path / "rocky.toml"
    config.write_text('[adapter]\nkind = "duckdb"\n')
    rocky = RockyResource(config_path=str(config))

    with patch.object(RockyResource, "history", return_value=_history_with_runs()):
        result = state_health(rocky)

    assert result.backend == "local"


def test_state_health_falls_back_to_local_on_unknown_backend_string(tmp_path: Path) -> None:
    """Future backend names we don't know about yet map to ``local``.

    Keeps the ``Literal`` invariant on :class:`StateHealthResult` intact
    even if a consumer points Rocky at a bleeding-edge config.
    """
    config = tmp_path / "rocky.toml"
    config.write_text('[state]\nbackend = "cassandra"\n')
    rocky = RockyResource(config_path=str(config))

    with patch.object(RockyResource, "history", return_value=_history_with_runs()):
        result = state_health(rocky)

    assert result.backend == "local"


# ---------------------------------------------------------------------------
# History parsing
# ---------------------------------------------------------------------------


def test_state_health_empty_history_yields_none_last_run(
    rocky_with_local_backend: RockyResource,
) -> None:
    """Fresh state store — no runs — surfaces ``None`` for both last-run fields."""
    with patch.object(RockyResource, "history", return_value=_history_with_runs()):
        result = state_health(rocky_with_local_backend)

    assert result.last_run_status is None
    assert result.last_run_at is None


def test_state_health_normalises_success_status(
    rocky_with_local_backend: RockyResource,
) -> None:
    """Wire value ``"Success"`` (CamelCase from Rust ``{:?}``) normalises to ``"success"``."""
    started = datetime(2026, 4, 22, 9, 30, 0, tzinfo=UTC)
    record = _run_history_record(status="Success", started_at=started)
    with patch.object(RockyResource, "history", return_value=_history_with_runs(record)):
        result = state_health(rocky_with_local_backend)

    assert result.last_run_status == "success"
    assert result.last_run_at == started


def test_state_health_normalises_partial_failure_status(
    rocky_with_local_backend: RockyResource,
) -> None:
    """Wire value ``"PartialFailure"`` normalises to ``"partial_failure"``."""
    record = _run_history_record(status="PartialFailure")
    with patch.object(RockyResource, "history", return_value=_history_with_runs(record)):
        result = state_health(rocky_with_local_backend)

    assert result.last_run_status == "partial_failure"


def test_state_health_normalises_failure_status(
    rocky_with_local_backend: RockyResource,
) -> None:
    record = _run_history_record(status="Failure")
    with patch.object(RockyResource, "history", return_value=_history_with_runs(record)):
        result = state_health(rocky_with_local_backend)

    assert result.last_run_status == "failure"


def test_state_health_unknown_status_maps_to_failure(
    rocky_with_local_backend: RockyResource,
) -> None:
    """A wire value we don't recognise falls back to ``"failure"``.

    Safer than guessing "success" — the caller gating on a status
    they can't identify should treat it as a potential regression.
    """
    record = _run_history_record(status="Cancelled")
    with patch.object(RockyResource, "history", return_value=_history_with_runs(record)):
        result = state_health(rocky_with_local_backend)

    assert result.last_run_status == "failure"


def test_state_health_picks_most_recent_run_as_first(
    rocky_with_local_backend: RockyResource,
) -> None:
    """``rocky history`` returns runs newest-first; the accessor trusts that."""
    newest = _run_history_record(
        run_id="newest",
        status="Success",
        started_at=datetime(2026, 4, 22, 12, 0, 0, tzinfo=UTC),
    )
    older = _run_history_record(
        run_id="older",
        status="Failure",
        started_at=datetime(2026, 4, 22, 10, 0, 0, tzinfo=UTC),
    )
    with patch.object(RockyResource, "history", return_value=_history_with_runs(newest, older)):
        result = state_health(rocky_with_local_backend)

    assert result.last_run_status == "success"
    assert result.last_run_at == newest.started_at


def test_state_health_tolerates_history_binary_failure(
    rocky_with_local_backend: RockyResource,
) -> None:
    """A ``dg.Failure`` from the binary is swallowed to keep sensor ticks alive.

    Matches :func:`rocky_healthcheck`'s pattern — when the accessor
    runs every minute, a missing binary / unreadable state store
    shouldn't crash every tick.
    """
    with patch.object(
        RockyResource,
        "history",
        side_effect=dg.Failure(description="Rocky binary not found"),
    ):
        result = state_health(rocky_with_local_backend)

    assert result.last_run_status is None
    assert result.last_run_at is None
    # Backend still reported (it doesn't depend on the binary).
    assert result.backend == "local"


# ---------------------------------------------------------------------------
# Probe path (probe_write=True)
# ---------------------------------------------------------------------------


def test_state_health_probe_skipped_by_default(
    rocky_with_local_backend: RockyResource,
) -> None:
    """``probe_write=False`` (the default) must never invoke ``rocky doctor``."""
    with (
        patch.object(RockyResource, "history", return_value=_history_with_runs()),
        patch.object(RockyResource, "doctor") as doctor_mock,
    ):
        result = state_health(rocky_with_local_backend)

    doctor_mock.assert_not_called()
    assert result.probe_outcome is None
    assert result.probe_duration_ms is None
    assert result.probe_error is None


def test_state_health_probe_healthy_check_maps_to_ok(
    rocky_with_local_backend: RockyResource,
) -> None:
    state_rw = HealthCheck(
        name="state_rw",
        status=HealthStatus.healthy,
        message="State backend RW probe succeeded (valkey)",
        duration_ms=87,
    )
    # Include a sibling check so the filter exercises name-matching.
    extras = [
        HealthCheck(
            name="config",
            status=HealthStatus.healthy,
            message="Config syntax valid",
            duration_ms=3,
        ),
    ]
    with (
        patch.object(RockyResource, "history", return_value=_history_with_runs()),
        patch.object(
            RockyResource,
            "doctor",
            return_value=_doctor(state_rw=state_rw, extras=extras),
        ),
    ):
        result = state_health(rocky_with_local_backend, probe_write=True)

    assert result.probe_outcome == "ok"
    assert result.probe_duration_ms == 87
    assert result.probe_error is None


def test_state_health_probe_critical_timeout_message_maps_to_timeout(
    rocky_with_local_backend: RockyResource,
) -> None:
    """The engine prefixes timeout failures with the ``state transfer timed out`` string."""
    message = "State backend RW probe failed: state transfer timed out after 30s"
    state_rw = HealthCheck(
        name="state_rw",
        status=HealthStatus.critical,
        message=message,
        duration_ms=30_000,
    )
    with (
        patch.object(RockyResource, "history", return_value=_history_with_runs()),
        patch.object(
            RockyResource,
            "doctor",
            return_value=_doctor(state_rw=state_rw),
        ),
    ):
        result = state_health(rocky_with_local_backend, probe_write=True)

    assert result.probe_outcome == "timeout"
    assert result.probe_duration_ms == 30_000
    assert result.probe_error == message


def test_state_health_probe_critical_generic_maps_to_error(
    rocky_with_local_backend: RockyResource,
) -> None:
    message = "State backend RW probe failed: S3 upload failed: AccessDenied"
    state_rw = HealthCheck(
        name="state_rw",
        status=HealthStatus.critical,
        message=message,
        duration_ms=512,
    )
    with (
        patch.object(RockyResource, "history", return_value=_history_with_runs()),
        patch.object(
            RockyResource,
            "doctor",
            return_value=_doctor(state_rw=state_rw),
        ),
    ):
        result = state_health(rocky_with_local_backend, probe_write=True)

    assert result.probe_outcome == "error"
    assert result.probe_duration_ms == 512
    assert result.probe_error == message


def test_state_health_probe_missing_check_maps_to_error(
    rocky_with_local_backend: RockyResource,
) -> None:
    """An older binary that doesn't emit ``state_rw`` should surface as error."""
    extras = [
        HealthCheck(
            name="config",
            status=HealthStatus.healthy,
            message="Config syntax valid",
            duration_ms=3,
        ),
    ]
    with (
        patch.object(RockyResource, "history", return_value=_history_with_runs()),
        patch.object(
            RockyResource,
            "doctor",
            return_value=_doctor(state_rw=None, extras=extras),
        ),
    ):
        result = state_health(rocky_with_local_backend, probe_write=True)

    assert result.probe_outcome == "error"
    assert result.probe_duration_ms is None
    assert result.probe_error is not None
    assert "state_rw" in result.probe_error


def test_state_health_probe_binary_failure_maps_to_error(
    rocky_with_local_backend: RockyResource,
) -> None:
    """Doctor itself failing should populate ``probe_error`` rather than raise."""
    with (
        patch.object(RockyResource, "history", return_value=_history_with_runs()),
        patch.object(
            RockyResource,
            "doctor",
            side_effect=dg.Failure(description="Rocky binary not found"),
        ),
    ):
        result = state_health(rocky_with_local_backend, probe_write=True)

    assert result.probe_outcome == "error"
    assert result.probe_duration_ms is None
    assert result.probe_error is not None
    assert "binary" in result.probe_error.lower()


def test_state_health_probe_warning_maps_to_error(
    rocky_with_local_backend: RockyResource,
) -> None:
    """Defensive — the engine's probe is binary today, but ``warning`` degrades to error."""
    state_rw = HealthCheck(
        name="state_rw",
        status=HealthStatus.warning,
        message="Probe skipped: Valkey not reachable yet",
        duration_ms=0,
    )
    with (
        patch.object(RockyResource, "history", return_value=_history_with_runs()),
        patch.object(
            RockyResource,
            "doctor",
            return_value=_doctor(state_rw=state_rw),
        ),
    ):
        result = state_health(rocky_with_local_backend, probe_write=True)

    assert result.probe_outcome == "error"
    assert result.probe_error == state_rw.message


# ---------------------------------------------------------------------------
# Resource facade
# ---------------------------------------------------------------------------


def test_rocky_resource_state_health_delegates_to_health_module(
    rocky_with_local_backend: RockyResource,
) -> None:
    """The method on :class:`RockyResource` should be a thin facade.

    The exact shape of delegation isn't part of the public API, but the
    method must accept ``probe_write`` and return a
    :class:`StateHealthResult`.
    """
    state_rw = HealthCheck(
        name="state_rw",
        status=HealthStatus.healthy,
        message="Local backend — no remote probe needed",
        duration_ms=0,
    )
    with (
        patch.object(RockyResource, "history", return_value=_history_with_runs()),
        patch.object(
            RockyResource,
            "doctor",
            return_value=_doctor(state_rw=state_rw),
        ),
    ):
        cheap = rocky_with_local_backend.state_health()
        probed = rocky_with_local_backend.state_health(probe_write=True)

    assert isinstance(cheap, StateHealthResult)
    assert cheap.probe_outcome is None
    assert isinstance(probed, StateHealthResult)
    assert probed.probe_outcome == "ok"
