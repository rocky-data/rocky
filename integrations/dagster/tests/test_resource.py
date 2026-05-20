"""Tests for ``RockyResource`` subprocess and HTTP plumbing.

These tests patch the ``subprocess.run`` and ``urllib.request.urlopen``
calls so they exercise resource behavior end-to-end without invoking the
real Rocky binary or hitting a server.
"""

from __future__ import annotations

import json
import logging
import signal
import subprocess
import sys
import threading
import time
import urllib.error
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import dagster as dg
import pytest

from dagster_rocky.resource import (
    DEFAULT_HTTP_TIMEOUT_SECONDS,
    MIN_ROCKY_VERSION,
    RockyResource,
    _redact_argv,
    _truncate_stderr_for_metadata,
    _validate_governance_override,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _completed(stdout: str = "{}", stderr: str = "", returncode: int = 0):
    return subprocess.CompletedProcess(
        args=["rocky"],
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
    )


def _patched_run(**kwargs: Any):
    return patch("dagster_rocky.resource.subprocess.run", **kwargs)


# ---------------------------------------------------------------------------
# _build_cmd
# ---------------------------------------------------------------------------


def test_build_cmd_includes_global_flags():
    """``_build_cmd`` always inserts the global config / state / output flags."""
    rocky = RockyResource(
        binary_path="rocky",
        config_path="config/rocky.toml",
        state_path=".rocky-state.redb",
    )
    cmd = rocky._build_cmd(["plan", "--filter", "tenant=acme"])
    # binary_path is resolved through shutil.which; whatever value comes
    # back, the rest of the args are deterministic.
    assert cmd[1:] == [
        "--config",
        "config/rocky.toml",
        "--state-path",
        ".rocky-state.redb",
        "--output",
        "json",
        "plan",
        "--filter",
        "tenant=acme",
    ]


# ---------------------------------------------------------------------------
# _run_rocky — happy path, partial success, failures
#
# Both the buffered (``_run_rocky``) and streaming (``_run_rocky_streaming``)
# entry points delegate to ``_run_rocky_with_log_sink`` which spawns rocky
# via ``subprocess.Popen`` with sole-reader threads and a watchdog timeout.
# Tests therefore mock ``subprocess.Popen`` rather than ``subprocess.run``.
# Each stderr line is forwarded to the sink — the module logger for the
# buffered path, ``context.log.info`` for the streaming path — so the
# buffered tests rely on ``caplog`` at INFO to assert routing.
# ---------------------------------------------------------------------------


def _popen_mock(
    *,
    stdout: str = "",
    stderr_lines: list[str] | None = None,
    returncode: int = 0,
):
    """Build a Popen mock the shared Popen + reader-threads helper can drive.

    ``stdout`` is exposed as a single-element iterator so the stdout
    accumulator concatenates it verbatim. ``stderr`` is exposed as a
    line-by-line iterator (with trailing newlines added) so the
    stderr forwarder sees one line at a time and drains to EOF.
    ``proc.wait()`` is a no-op mock — the watchdog Event is dismissed
    by the helper's ``finally`` clause once the iterators have drained.
    """
    proc = MagicMock()
    proc.pid = 12345
    proc.stdout = iter([stdout]) if stdout else iter([])
    proc.stderr = iter(line + "\n" for line in (stderr_lines or []))
    proc.returncode = returncode
    proc.kill = MagicMock()
    proc.wait = MagicMock()
    return proc


def test_run_rocky_returns_stdout_on_success():
    rocky = RockyResource()
    proc = _popen_mock(stdout='{"hello": "world"}')
    with (
        patch.object(RockyResource, "_verify_engine_version"),
        patch("dagster_rocky.resource.subprocess.Popen", return_value=proc),
    ):
        out = rocky._run_rocky(["discover"])
    assert out == '{"hello": "world"}'


def test_run_rocky_partial_success_returns_stdout():
    """A non-zero exit with valid JSON should return the JSON when allowed."""
    rocky = RockyResource()
    payload = '{"command": "run", "tables_failed": 1}'
    proc = _popen_mock(stdout=payload, returncode=1)
    with (
        patch.object(RockyResource, "_verify_engine_version"),
        patch("dagster_rocky.resource.subprocess.Popen", return_value=proc),
    ):
        out = rocky._run_rocky(["run", "--filter", "x=y"], allow_partial=True)
    assert out == payload


def test_run_rocky_partial_success_disallowed_raises():
    """Without ``allow_partial`` a non-zero exit always becomes a Failure."""
    rocky = RockyResource()
    proc = _popen_mock(stdout="{}", stderr_lines=["boom"], returncode=1)
    with (
        patch.object(RockyResource, "_verify_engine_version"),
        patch("dagster_rocky.resource.subprocess.Popen", return_value=proc),
        pytest.raises(dg.Failure, match="exit 1"),
    ):
        rocky._run_rocky(["discover"])


def test_run_rocky_partial_success_only_when_json():
    """Partial success only kicks in when stdout actually starts with JSON."""
    rocky = RockyResource()
    proc = _popen_mock(stdout="not json", stderr_lines=["boom"], returncode=2)
    with (
        patch.object(RockyResource, "_verify_engine_version"),
        patch("dagster_rocky.resource.subprocess.Popen", return_value=proc),
        pytest.raises(dg.Failure),
    ):
        rocky._run_rocky(["run", "--filter", "x=y"], allow_partial=True)


def test_run_rocky_missing_binary_raises_failure():
    rocky = RockyResource(binary_path="/nope/rocky")
    with (
        patch.object(RockyResource, "_verify_engine_version"),
        patch("dagster_rocky.resource.subprocess.Popen", side_effect=FileNotFoundError),
        pytest.raises(dg.Failure, match="not found"),
    ):
        rocky._run_rocky(["discover"])


def test_run_rocky_failure_metadata_carries_stderr_tail():
    """A non-partial failure exposes the stderr tail in ``dg.Failure.metadata``."""
    rocky = RockyResource()
    proc = _popen_mock(
        stdout="not json",
        stderr_lines=[f"INFO step {i}" for i in range(5)] + ["ERROR fatal: connection refused"],
        returncode=2,
    )
    with (
        patch.object(RockyResource, "_verify_engine_version"),
        patch("dagster_rocky.resource.subprocess.Popen", return_value=proc),
        pytest.raises(dg.Failure) as excinfo,
    ):
        rocky._run_rocky(["discover"])
    assert "stderr_tail" in excinfo.value.metadata
    tail = excinfo.value.metadata["stderr_tail"].text
    assert "ERROR fatal: connection refused" in tail


def test_run_rocky_streams_stderr_to_module_logger(caplog: pytest.LogCaptureFixture):
    """Each non-empty stderr line reaches the ``dagster_rocky.resource`` logger.

    This is the FR-020 contract — the buffered path no longer holds
    progress lines until exit; they're surfaced to the module logger
    line-by-line so ``dg dev`` cold starts and code-server reloads
    show the same live progress as the streaming path.
    """
    rocky = RockyResource()
    proc = _popen_mock(
        stdout='{"ok": true}',
        stderr_lines=[
            "INFO discovering sources",
            "INFO 53 connectors enumerated",
            "INFO discover complete",
        ],
    )
    with (
        caplog.at_level(logging.INFO, logger="dagster_rocky.resource"),
        patch.object(RockyResource, "_verify_engine_version"),
        patch("dagster_rocky.resource.subprocess.Popen", return_value=proc),
    ):
        rocky._run_rocky(["discover"])
    streamed = [
        rec.getMessage()
        for rec in caplog.records
        if rec.name == "dagster_rocky.resource" and rec.getMessage().startswith("rocky:")
    ]
    assert "rocky: INFO discovering sources" in streamed
    assert "rocky: INFO 53 connectors enumerated" in streamed
    assert "rocky: INFO discover complete" in streamed


def test_run_rocky_skips_empty_stderr_lines(caplog: pytest.LogCaptureFixture):
    """Blank stderr lines are not propagated to the module logger."""
    rocky = RockyResource()
    proc = _popen_mock(
        stdout='{"ok": true}',
        stderr_lines=["INFO start", "", "INFO done"],
    )
    with (
        caplog.at_level(logging.INFO, logger="dagster_rocky.resource"),
        patch.object(RockyResource, "_verify_engine_version"),
        patch("dagster_rocky.resource.subprocess.Popen", return_value=proc),
    ):
        rocky._run_rocky(["discover"])
    streamed = [rec.getMessage() for rec in caplog.records if rec.getMessage().startswith("rocky:")]
    assert streamed == ["rocky: INFO start", "rocky: INFO done"]


def test_run_rocky_redacts_argv_in_startup_log(caplog: pytest.LogCaptureFixture):
    """Credential-bearing flag values are redacted in the startup log line.

    The startup line is shared with the streaming path and lives in the
    helper, so the same redaction guarantee applies whether the caller
    has a Dagster context or not.
    """
    rocky = RockyResource()
    proc = _popen_mock(stdout='{"ok": true}')
    with (
        caplog.at_level(logging.INFO, logger="dagster_rocky.resource"),
        patch.object(RockyResource, "_verify_engine_version"),
        patch("dagster_rocky.resource.subprocess.Popen", return_value=proc),
    ):
        rocky._run_rocky(["run", "--idempotency-key", "secret-token-123"])
    startup_lines = [
        rec.getMessage()
        for rec in caplog.records
        if rec.getMessage().startswith("rocky subprocess started")
    ]
    assert startup_lines, "startup line should be emitted on the module logger"
    joined = "\n".join(startup_lines)
    assert "secret-token-123" not in joined
    assert "***" in joined
    assert "--idempotency-key" in joined


def test_run_rocky_timeout_kills_proc_and_raises():
    """When the subprocess hangs past ``timeout_seconds``, the watchdog
    thread kills the process group via ``os.killpg(SIGKILL)`` and
    ``_run_rocky`` raises ``dg.Failure`` with the configured timeout
    in the message — same path as ``_run_rocky_streaming``.
    """
    rocky = RockyResource(timeout_seconds=1)

    killed = threading.Event()

    def fake_wait(timeout: float | None = None) -> int:
        killed.wait()
        return proc.returncode

    proc = _popen_mock(stdout="", stderr_lines=["INFO hung at step 3"])
    proc.wait = MagicMock(side_effect=fake_wait)

    killpg_mock = MagicMock(
        side_effect=lambda pgid, sig: (
            object.__setattr__(proc, "returncode", -signal.SIGKILL),
            killed.set(),
        )[0],
    )

    with (
        patch.object(RockyResource, "_verify_engine_version"),
        patch("dagster_rocky.resource.subprocess.Popen", return_value=proc),
        patch("dagster_rocky.resource.os.killpg", killpg_mock),
        patch("dagster_rocky.resource.os.getpgid", return_value=proc.pid),
        pytest.raises(dg.Failure) as excinfo,
    ):
        rocky._run_rocky(["discover"])

    killpg_mock.assert_called_once()
    desc = excinfo.value.description or ""
    assert "1s" in desc
    assert "timed out" in desc.lower()
    assert "watchdog-killed" in desc.lower()


# ---------------------------------------------------------------------------
# CLI command builders
# ---------------------------------------------------------------------------


def test_run_passes_governance_override_as_json():
    rocky = RockyResource()
    captured: list[list[str]] = []
    plan_id = "a" * 64

    def fake_run(*, args, **_):
        captured.append(args)
        if args[0] == "plan":
            # Phase 5b: plan emits a real plan_id even for
            # replication-only projects. ``--governance-override``
            # appears on both ``plan`` and ``apply``, but the test
            # only needs to see it land on the plan argv.
            return json.dumps(
                {
                    "version": "0.1.0",
                    "command": "plan",
                    "filter": "tenant=acme",
                    "statements": [],
                    "plan_id": plan_id,
                    "plan_kind": "replication",
                    "created_at": "2026-05-18T00:00:00Z",
                }
            )
        return (
            '{"version":"0.3.0","command":"run","filter":"tenant=acme",'
            '"duration_ms":0,"tables_copied":0,"materializations":[],'
            '"check_results":[],"permissions":{"grants_added":0,"grants_revoked":0,'
            '"catalogs_created":0,"schemas_created":0},"drift":{"tables_checked":0,'
            '"tables_drifted":0,"actions_taken":[]}}'
        )

    with patch.object(RockyResource, "_run_rocky", autospec=True) as run_mock:
        run_mock.side_effect = lambda self, args, allow_partial=False: fake_run(args=args)
        rocky.run(filter="tenant=acme", governance_override={"workspace_ids": [1, 2]})

    # `rocky plan` is invoked first; the governance override flag must
    # land on its argv (the persisted plan stamps the override).
    assert "--governance-override" in captured[0]
    idx = captured[0].index("--governance-override")
    assert '"workspace_ids"' in captured[0][idx + 1]


def _capture_discover_args(rocky: RockyResource, discover_json: str, **kwargs: Any) -> list[str]:
    """Invoke ``rocky.discover(**kwargs)`` against a captured-argv fake.

    Returns the argv list passed to ``_run_rocky`` so callers can assert
    flag presence / value / coercion without spinning up the full
    Popen + parse pipeline.
    """
    captured: list[list[str]] = []

    def fake_run(self: RockyResource, args: list[str], allow_partial: bool = False) -> str:
        captured.append(args)
        return discover_json

    with patch.object(RockyResource, "_run_rocky", autospec=True) as run_mock:
        run_mock.side_effect = fake_run
        rocky.discover(**kwargs)
    assert captured, "_run_rocky was not called"
    return captured[0]


def test_discover_omits_emit_fivetran_state_to_by_default(discover_json: str):
    """No kwarg → no ``--emit-fivetran-state-to`` flag on the argv."""
    args = _capture_discover_args(RockyResource(), discover_json)
    assert args == ["discover"]


def test_discover_passes_emit_fivetran_state_to_str(discover_json: str):
    """``str`` path lands verbatim after ``--emit-fivetran-state-to``."""
    args = _capture_discover_args(
        RockyResource(), discover_json, emit_fivetran_state_to="/tmp/envelope.json"
    )
    assert args == ["discover", "--emit-fivetran-state-to", "/tmp/envelope.json"]


def test_discover_coerces_pathlib_emit_fivetran_state_to(discover_json: str):
    """``Path`` input is stringified — the CLI takes a path-string, not a Path."""
    args = _capture_discover_args(
        RockyResource(), discover_json, emit_fivetran_state_to=Path("/tmp/envelope.json")
    )
    assert args == ["discover", "--emit-fivetran-state-to", "/tmp/envelope.json"]


def test_discover_emit_fivetran_state_to_composes_with_pipeline(discover_json: str):
    """``--pipeline`` and ``--emit-fivetran-state-to`` coexist; order matches build."""
    args = _capture_discover_args(
        RockyResource(),
        discover_json,
        pipeline="raw_replication",
        emit_fivetran_state_to="/tmp/envelope.json",
    )
    assert args == [
        "discover",
        "--pipeline",
        "raw_replication",
        "--emit-fivetran-state-to",
        "/tmp/envelope.json",
    ]


# ---------------------------------------------------------------------------
# FR-009 — governance_override.workspace_ids safety validator
# ---------------------------------------------------------------------------


class TestValidateGovernanceOverride:
    """Parametrised coverage of the FR-009 payload -> validator outcome table.

    See :func:`dagster_rocky.resource._validate_governance_override` and
    the engine-side ``GovernanceOverride::validate_workspace_ids`` for
    the authoritative semantics.
    """

    def test_none_is_noop(self):
        # `governance_override=None` (the default): no reconciler runs,
        # no validation needed, no error.
        _validate_governance_override(None)

    def test_key_absent_is_noop(self):
        # `{}` omits `workspace_ids` entirely — engine interprets as
        # "skip binding reconciliation"; validator must stay out of
        # the way so callers can pass governance-only grants.
        _validate_governance_override({})
        _validate_governance_override({"grants": [{"principal": "p", "permissions": ["SELECT"]}]})

    def test_non_empty_list_is_noop(self):
        # Happy path: the caller supplied a non-empty workspace-binding
        # set. Payload shape isn't structurally validated here (the
        # engine deserializes the JSON) — we only guard the empty-list
        # footgun.
        _validate_governance_override(
            {"workspace_ids": [{"id": 123, "binding_type": "READ_WRITE"}]}
        )

    def test_empty_list_without_flag_raises(self):
        # The core footgun. `{"workspace_ids": []}` with no opt-in flag
        # tells the reconciler to revoke every binding — refuse.
        with pytest.raises(dg.Failure) as exc_info:
            _validate_governance_override({"workspace_ids": []})
        msg = exc_info.value.description or ""
        assert "revoke every workspace binding" in msg
        assert "allow_empty_workspace_ids" in msg

    def test_empty_list_with_flag_is_noop(self):
        # Explicit consent path — decommissioning flow.
        _validate_governance_override({"workspace_ids": [], "allow_empty_workspace_ids": True})

    def test_empty_list_with_false_flag_raises(self):
        # Typo-safety: the flag must be truthy, not merely present.
        with pytest.raises(dg.Failure):
            _validate_governance_override({"workspace_ids": [], "allow_empty_workspace_ids": False})

    def test_non_list_workspace_ids_raises(self):
        # Type error — rejected before we reach the engine, which
        # would also fail (but less clearly, after a subprocess hop).
        with pytest.raises(dg.Failure, match="must be a list"):
            _validate_governance_override({"workspace_ids": "not-a-list"})
        with pytest.raises(dg.Failure, match="must be a list"):
            _validate_governance_override({"workspace_ids": 42})

    def test_non_dict_override_raises(self):
        # Caller confusion: passed the list directly instead of
        # wrapping it in a dict. Reject before it ever hits the CLI.
        with pytest.raises(dg.Failure, match="must be a dict"):
            _validate_governance_override([])  # type: ignore[arg-type]


def test_run_rejects_empty_workspace_ids_before_subprocess():
    """End-to-end: `rocky.run(...)` raises before `_run_rocky` is called."""
    rocky = RockyResource()
    with (
        patch.object(RockyResource, "_run_rocky", autospec=True) as run_mock,
        pytest.raises(dg.Failure, match="revoke every workspace binding"),
    ):
        rocky.run(filter="tenant=acme", governance_override={"workspace_ids": []})
    # The validator must fire before any subprocess work.
    run_mock.assert_not_called()


def test_run_with_run_models_appends_models_and_all():
    rocky = RockyResource(models_dir="m")
    captured: list[list[str]] = []
    plan_id = "a" * 64

    def fake_run(self, args, allow_partial=False):
        captured.append(args)
        if args[0] == "plan":
            return json.dumps(
                {
                    "version": "0.1.0",
                    "command": "plan",
                    "filter": "tenant=acme",
                    "statements": [],
                    "plan_id": plan_id,
                    "plan_kind": "replication",
                    "created_at": "2026-05-18T00:00:00Z",
                }
            )
        return (
            '{"version":"0.3.0","command":"run","filter":"tenant=acme",'
            '"duration_ms":0,"tables_copied":0,"materializations":[],'
            '"check_results":[],"permissions":{"grants_added":0,"grants_revoked":0,'
            '"catalogs_created":0,"schemas_created":0},"drift":{"tables_checked":0,'
            '"tables_drifted":0,"actions_taken":[]}}'
        )

    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        rocky.run(filter="tenant=acme", run_models=True)

    # `--models <dir> --all` lands on the plan argv (the persisted plan
    # stamps the model-execution intent).
    assert captured[0][-3:] == ["--models", "m", "--all"]


# ---------------------------------------------------------------------------
# Partition flag plumbing (Phase 3)
# ---------------------------------------------------------------------------


def _empty_run_json() -> str:
    return (
        '{"version":"0.3.0","command":"run","filter":"tenant=acme",'
        '"duration_ms":0,"tables_copied":0,"materializations":[],'
        '"check_results":[],"permissions":{"grants_added":0,"grants_revoked":0,'
        '"catalogs_created":0,"schemas_created":0},"drift":{"tables_checked":0,'
        '"tables_drifted":0,"actions_taken":[]}}'
    )


def _plan_then_run_json(plan_id: str = "a" * 64) -> str:
    """Return the Phase-5b plan JSON used by plan-args captors."""
    return json.dumps(
        {
            "version": "0.1.0",
            "command": "plan",
            "filter": "tenant=acme",
            "statements": [],
            "plan_id": plan_id,
            "plan_kind": "replication",
            "created_at": "2026-05-18T00:00:00Z",
        }
    )


def _capture_run_args() -> tuple[list[list[str]], Any]:
    """Set up a captor + side_effect for the plan/apply two-step flow.

    Phase 5b: ``rocky.run()`` invokes ``rocky plan`` then
    ``rocky apply <plan_id>``. Returns the plan JSON on the first call
    and the empty run JSON on the second. Most flag-plumbing tests
    assert against ``captured[0]`` — the plan argv — since the engine
    flag-surface parity guarantees plan and run share the same flags.
    """
    captured: list[list[str]] = []

    def fake_run(self, args, allow_partial=False):
        captured.append(args)
        if args[0] == "plan":
            return _plan_then_run_json()
        return _empty_run_json()

    return captured, fake_run


def test_run_with_partition_appends_partition_flag():
    rocky = RockyResource()
    captured, fake_run = _capture_run_args()
    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        rocky.run(filter="tenant=acme", partition="2026-04-08")
    assert "--partition" in captured[0]
    idx = captured[0].index("--partition")
    assert captured[0][idx + 1] == "2026-04-08"


def test_run_with_partition_range_appends_from_and_to():
    rocky = RockyResource()
    captured, fake_run = _capture_run_args()
    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        rocky.run(
            filter="tenant=acme",
            partition_from="2026-04-01",
            partition_to="2026-04-08",
        )
    assert "--from" in captured[0]
    from_idx = captured[0].index("--from")
    assert captured[0][from_idx + 1] == "2026-04-01"
    assert "--to" in captured[0]
    to_idx = captured[0].index("--to")
    assert captured[0][to_idx + 1] == "2026-04-08"


def test_run_with_partition_from_only_omits_both():
    """Both --from and --to must be set; otherwise neither is emitted."""
    rocky = RockyResource()
    captured, fake_run = _capture_run_args()
    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        rocky.run(filter="tenant=acme", partition_from="2026-04-01")
    assert "--from" not in captured[0]
    assert "--to" not in captured[0]


def test_run_with_latest_appends_latest_flag():
    rocky = RockyResource()
    captured, fake_run = _capture_run_args()
    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        rocky.run(filter="tenant=acme", latest=True)
    assert "--latest" in captured[0]


def test_run_with_missing_appends_missing_flag():
    rocky = RockyResource()
    captured, fake_run = _capture_run_args()
    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        rocky.run(filter="tenant=acme", missing=True)
    assert "--missing" in captured[0]


def test_run_with_lookback_appends_lookback_flag():
    rocky = RockyResource()
    captured, fake_run = _capture_run_args()
    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        rocky.run(filter="tenant=acme", lookback=3)
    assert "--lookback" in captured[0]
    idx = captured[0].index("--lookback")
    assert captured[0][idx + 1] == "3"


def test_run_with_parallel_appends_parallel_flag():
    rocky = RockyResource()
    captured, fake_run = _capture_run_args()
    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        rocky.run(filter="tenant=acme", parallel=4)
    assert "--parallel" in captured[0]
    idx = captured[0].index("--parallel")
    assert captured[0][idx + 1] == "4"


# ---------------------------------------------------------------------------
# run_streaming — T2 Pipes-style live stderr forwarding
# ---------------------------------------------------------------------------


def _streaming_popen_mock(
    *,
    stdout: str,
    stderr_lines: list[str],
    returncode: int = 0,
):
    """Build a Popen mock that mimics a real subprocess.Popen for streaming.

    Both ``stdout`` and ``stderr`` are exposed as iterators so the
    dedicated reader threads (the sole readers of each pipe, per the
    post-2026-04-19 watchdog rewrite of ``_run_rocky_streaming``) see
    one line at a time and drain to EOF.

    The stdout accumulator concatenates whatever it reads verbatim; to
    keep existing tests reconstructing the exact JSON payload, stdout
    is exposed as a single-element iterator yielding the full string.

    ``proc.wait()`` returns immediately (returncode already set), which
    matches a normally-exiting subprocess. The watchdog's ``fired.wait``
    is dismissed in the main thread's ``finally`` block before
    ``proc.wait()``'s effect matters.
    """
    proc = MagicMock()
    proc.pid = 12345
    proc.stdout = iter([stdout]) if stdout else iter([])
    proc.stderr = iter(line + "\n" for line in stderr_lines)
    proc.returncode = returncode
    proc.kill = MagicMock()
    proc.wait = MagicMock()
    return proc


def _captured_log_context() -> Any:
    """Build a fake context whose .log.info captures lines into a list."""
    captured: list[str] = []
    context = MagicMock()
    context.log = MagicMock()
    context.log.info = MagicMock(side_effect=lambda msg: captured.append(msg))
    context.captured = captured  # type: ignore[attr-defined]
    return context


def _run_json() -> str:
    return (
        '{"version":"0.3.0","command":"run","filter":"tenant=acme",'
        '"duration_ms":12,"tables_copied":1,"materializations":[],'
        '"check_results":[],"permissions":{"grants_added":0,"grants_revoked":0,'
        '"catalogs_created":0,"schemas_created":0},"drift":{"tables_checked":0,'
        '"tables_drifted":0,"actions_taken":[]}}'
    )


def _plan_json(plan_id: str = "a" * 64) -> str:
    """Minimal valid ``rocky plan`` JSON with a persisted ``plan_id``.

    Used by the Phase 5 streaming/pipes tests so the plan/apply
    orchestration dispatches the apply path (not the replication-only
    fallback). The body shape matches the engine's ``PlanOutput`` —
    only ``plan_id`` is load-bearing for ``_extract_plan_id``.
    """
    return json.dumps(
        {
            "version": "0.1.0",
            "command": "plan",
            "filter": "tenant=acme",
            "statements": [],
            "plan_id": plan_id,
            "plan_kind": "run",
            "created_at": "2026-05-17T00:00:00Z",
            "models": [],
            "execution_layers": [],
        }
    )


def _apply_envelope_json(*, plan_id: str = "a" * 64, inner: dict | None = None) -> str:
    """``rocky apply`` envelope JSON wrapping a run result (Phase 5)."""
    return json.dumps(
        {
            "version": "0.3.0",
            "command": "apply",
            "plan_id": plan_id,
            "plan_kind": "run",
            "success": True,
            "result": inner or json.loads(_run_json()),
        }
    )


def _two_phase_popen_side_effect(
    plan_proc: Any,
    apply_proc: Any,
) -> Callable[..., Any]:
    """Return a ``subprocess.Popen`` side-effect that yields procs in order.

    Phase 5 ``run_streaming`` spawns two subprocesses: ``rocky plan``
    (buffered, single-digit seconds) followed by ``rocky apply``
    (streamed). Tests use this to script those two return values without
    monkeypatching the higher-level methods.
    """
    procs = iter([plan_proc, apply_proc])

    def _side_effect(*_args: Any, **_kwargs: Any) -> Any:
        return next(procs)

    return _side_effect


def test_run_streaming_forwards_stderr_to_context_log():
    """Each non-empty stderr line from the apply phase is forwarded to
    context.log.info with a 'rocky:' prefix as the subprocess runs.

    Phase 5 narrative note: plan-phase stderr is forwarded to the module
    logger only (not to ``context.log``) — so this test scripts the apply
    subprocess as the source of every line that should land in the run
    viewer. See ``_apply_plan`` + ``run_streaming``'s docstring.
    """
    rocky = RockyResource()
    context = _captured_log_context()
    plan_proc = _streaming_popen_mock(stdout=_plan_json(), stderr_lines=[])
    apply_proc = _streaming_popen_mock(
        stdout=_apply_envelope_json(),
        stderr_lines=[
            "INFO discovering sources",
            "INFO copying table acme.orders",
            "INFO copying table acme.payments",
            "INFO run complete",
        ],
    )

    with (
        patch.object(RockyResource, "_verify_engine_version"),
        patch(
            "dagster_rocky.resource.subprocess.Popen",
            side_effect=_two_phase_popen_side_effect(plan_proc, apply_proc),
        ),
    ):
        result = rocky.run_streaming(context, filter="tenant=acme")

    assert result.command == "run"
    assert result.tables_copied == 1
    # All four stderr lines were forwarded with the rocky: prefix
    forwarded = context.captured
    assert "rocky: INFO discovering sources" in forwarded
    assert "rocky: INFO copying table acme.orders" in forwarded
    assert "rocky: INFO copying table acme.payments" in forwarded
    assert "rocky: INFO run complete" in forwarded


def test_run_streaming_skips_empty_stderr_lines():
    """Blank stderr lines are dropped so the run viewer doesn't get spam."""
    rocky = RockyResource()
    context = _captured_log_context()
    plan_proc = _streaming_popen_mock(stdout=_plan_json(), stderr_lines=[])
    apply_proc = _streaming_popen_mock(
        stdout=_apply_envelope_json(),
        stderr_lines=["INFO start", "", "  ", "INFO done"],
    )

    with (
        patch.object(RockyResource, "_verify_engine_version"),
        patch(
            "dagster_rocky.resource.subprocess.Popen",
            side_effect=_two_phase_popen_side_effect(plan_proc, apply_proc),
        ),
    ):
        rocky.run_streaming(context, filter="tenant=acme")

    forwarded = context.captured
    # Only the two non-empty lines forwarded; blank/whitespace dropped
    assert "rocky: INFO start" in forwarded
    assert "rocky: INFO done" in forwarded
    # The whitespace-only line is non-empty after rstrip("\n") since it
    # has spaces, so it WILL be forwarded — verify the empty-string line
    # was dropped
    assert "rocky: " not in forwarded


def test_run_streaming_returns_parsed_run_result():
    """The captured stdout is parsed into a RunResult after the
    subprocess exits.

    Phase 5: the apply step's stdout is the ``ApplyOutput`` envelope;
    ``_parse_run_or_apply`` unwraps the inner ``result`` field which is
    the ``RunResult`` payload the public surface promises.
    """
    rocky = RockyResource()
    context = _captured_log_context()
    inner = {
        "version": "0.3.0",
        "command": "run",
        "filter": "tenant=acme",
        "duration_ms": 12345,
        "tables_copied": 7,
        "materializations": [],
        "check_results": [],
        "permissions": {
            "grants_added": 0,
            "grants_revoked": 0,
            "catalogs_created": 0,
            "schemas_created": 0,
        },
        "drift": {"tables_checked": 0, "tables_drifted": 0, "actions_taken": []},
    }
    plan_proc = _streaming_popen_mock(stdout=_plan_json(), stderr_lines=[])
    apply_proc = _streaming_popen_mock(
        stdout=_apply_envelope_json(inner=inner),
        stderr_lines=[],
    )

    with (
        patch.object(RockyResource, "_verify_engine_version"),
        patch(
            "dagster_rocky.resource.subprocess.Popen",
            side_effect=_two_phase_popen_side_effect(plan_proc, apply_proc),
        ),
    ):
        result = rocky.run_streaming(context, filter="tenant=acme")

    assert result.duration_ms == 12345
    assert result.tables_copied == 7


def test_run_streaming_partial_success_returns_result_on_nonzero_exit():
    """Same partial-success semantics as run(): non-zero exit + valid
    JSON stdout still returns the parsed result.

    Phase 5: partial-success applies to the apply step (where rows are
    actually written). The plan step always succeeds in this scenario.
    """
    rocky = RockyResource()
    context = _captured_log_context()
    plan_proc = _streaming_popen_mock(stdout=_plan_json(), stderr_lines=[])
    apply_proc = _streaming_popen_mock(
        stdout=_apply_envelope_json(),
        stderr_lines=["WARN one table failed"],
        returncode=1,
    )

    with (
        patch.object(RockyResource, "_verify_engine_version"),
        patch(
            "dagster_rocky.resource.subprocess.Popen",
            side_effect=_two_phase_popen_side_effect(plan_proc, apply_proc),
        ),
    ):
        result = rocky.run_streaming(context, filter="tenant=acme")

    assert result.command == "run"


def test_run_streaming_failure_raises_with_stderr_tail():
    """Non-zero exit with non-JSON stdout raises dg.Failure including
    the captured stderr tail in metadata."""
    rocky = RockyResource()
    context = _captured_log_context()
    proc = _streaming_popen_mock(
        stdout="not valid json",
        stderr_lines=[f"INFO line {i}" for i in range(5)] + ["ERROR fatal: connection refused"],
        returncode=2,
    )

    with (
        patch.object(RockyResource, "_verify_engine_version"),
        patch("dagster_rocky.resource.subprocess.Popen", return_value=proc),
        pytest.raises(dg.Failure) as excinfo,
    ):
        rocky.run_streaming(context, filter="tenant=acme")

    # The stderr tail metadata captured the lines for debugging
    assert "stderr_tail" in excinfo.value.metadata
    tail = excinfo.value.metadata["stderr_tail"].text
    assert "ERROR fatal: connection refused" in tail
    assert "INFO line 0" in tail


def test_run_streaming_missing_binary_raises_failure():
    rocky = RockyResource(binary_path="/nonexistent/rocky")
    context = _captured_log_context()

    with (
        _patched_run(side_effect=FileNotFoundError),
        pytest.raises(dg.Failure) as excinfo,
    ):
        rocky.run_streaming(context, filter="tenant=acme")

    # The version check catches FileNotFoundError first
    assert "not found" in (excinfo.value.description or "").lower()


def test_run_streaming_timeout_kills_proc_and_raises():
    """When the subprocess hangs past ``timeout_seconds``, the watchdog
    thread kills the process group via ``os.killpg(SIGKILL)`` and
    ``run_streaming`` raises ``dg.Failure`` with the configured timeout
    in the message.

    This is the mock-based smoke test; the real regression guard is the
    pair of fake-binary tests below
    (``test_run_streaming_hard_kills_hung_binary_with_stderr_chatter`` and
    ``test_run_streaming_timeout_fires_natively_without_daemon_reader``)
    which exercise the actual two-readers race that caused the
    2026-04-18 / 2026-04-19 production hangs.
    """
    # Short timeout keeps the test fast while still exercising the
    # real ``threading.Event.wait`` path (no monkeypatching of the
    # watchdog itself).
    rocky = RockyResource(timeout_seconds=1)
    context = _captured_log_context()

    # proc.wait() blocks until the watchdog signals the parent — we
    # simulate a live hang by making wait() wait on a sentinel Event
    # that never fires naturally. ``_kill_process_group`` is stubbed
    # out in the patch block below (mock proc.pid isn't a real pid),
    # and our side_effect sets returncode to -SIGKILL and unblocks.
    killed = threading.Event()

    def fake_wait(timeout: float | None = None) -> int:
        # mimic proc.wait() blocking until the watchdog's side-effect
        # unblocks us by setting the sentinel
        killed.wait()
        return proc.returncode

    proc = _streaming_popen_mock(
        stdout="",
        stderr_lines=["INFO hung at step 3"],
    )
    proc.wait = MagicMock(side_effect=fake_wait)

    killpg_mock = MagicMock(
        side_effect=lambda pgid, sig: (
            object.__setattr__(proc, "returncode", -signal.SIGKILL),
            killed.set(),
        )[0],
    )

    with (
        patch.object(RockyResource, "_verify_engine_version"),
        patch("dagster_rocky.resource.subprocess.Popen", return_value=proc),
        patch("dagster_rocky.resource.os.killpg", killpg_mock),
        patch("dagster_rocky.resource.os.getpgid", return_value=proc.pid),
        pytest.raises(dg.Failure) as excinfo,
    ):
        rocky.run_streaming(context, filter="tenant=acme")

    killpg_mock.assert_called_once()
    # Watchdog kills via SIGKILL and surfaces the configured duration.
    desc = excinfo.value.description or ""
    assert "1s" in desc
    assert "timed out" in desc.lower()
    assert "watchdog-killed" in desc.lower()


# ---------------------------------------------------------------------------
# Real-binary regression tests for the 2026-04-18 / 2026-04-19 hang
#
# These tests replace the fake ``rocky`` binary with a POSIX shell script
# that deliberately hangs while spamming stderr (the exact production
# pattern that triggered the two-readers race in dagster-rocky 1.7.0).
# The previous ``_run_rocky_streaming`` called ``proc.communicate(timeout)``
# concurrently with a daemon stderr reader thread — two CPython readers on
# the same pipe FD — which violates the documented subprocess contract and
# caused the timeout to intermittently fail to fire.
#
# Skipped on Windows: the fix path (``os.killpg`` + ``start_new_session``)
# is POSIX-only, and the fake binary is a shell script.
# ---------------------------------------------------------------------------


def _write_hang_fake(tmp_path: Path) -> Path:
    """Write the hanging-with-stderr-chatter fake rocky binary to ``tmp_path``.

    Returns the absolute path. Chmod 0o755 so it's directly executable.
    Adds a ``--version`` shortcut so the version check doesn't hang.
    """
    fake = tmp_path / "rocky"
    # The version shortcut runs when the script is called with the first
    # arg ``--version`` (the version-check codepath). Otherwise the
    # script hangs, which is what we want for the timeout test.
    fake.write_text(
        "#!/bin/sh\n"
        'if [ "$1" = "--version" ]; then\n'
        "  echo 'rocky 99.0.0'\n"
        "  exit 0\n"
        "fi\n"
        "echo '{\"started\": true}' >&2\n"
        "while true; do\n"
        "    echo 'still uploading state...' >&2\n"
        "    sleep 0.05\n"
        "done\n"
    )
    fake.chmod(0o755)
    return fake


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only fake binary")
def test_run_streaming_hard_kills_hung_binary_with_stderr_chatter(tmp_path: Path):
    """Positive regression test for the two-readers race fix.

    Spawns a **real** shell script that hangs forever while spamming
    stderr. Confirms the watchdog kills the process group via SIGKILL
    and ``_run_rocky_streaming`` raises ``dg.Failure`` within the
    configured timeout + a small grace window (not hours, as happened
    in prod on 2026-04-18 and 2026-04-19).

    Wall-clock assertion uses ``time.monotonic()`` because
    ``pytest-timeout`` is not declared in the dagster-rocky dev deps.
    """
    fake = _write_hang_fake(tmp_path)
    rocky = RockyResource(
        binary_path=str(fake),
        # Pass a config_path/state_path/models_dir that point at the
        # tmp dir so we don't need real config files.
        config_path=str(tmp_path / "rocky.toml"),
        state_path=str(tmp_path / ".rocky-state.redb"),
        models_dir=str(tmp_path),
        timeout_seconds=2,
    )
    context = _captured_log_context()

    t0 = time.monotonic()
    with pytest.raises(dg.Failure, match="timed out"):
        rocky._run_rocky_streaming(
            ["run", "--filter", "client=test"],
            context,
            allow_partial=True,
        )
    elapsed = time.monotonic() - t0

    # Budget: 2s timeout + 3s grace for watchdog + reader joins.
    # The 2026-04-19 incident sat at 11.5 hours; if this test takes >
    # 5s the fix is not actually bounded.
    assert elapsed < 5.0, (
        f"run_streaming took {elapsed:.2f}s; expected < 5s. Watchdog may not be firing."
    )


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only fake binary")
def test_run_streaming_timeout_fires_natively_without_daemon_reader(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    """Negative-control regression test — documents the race mechanism.

    Same hanging fake binary as the positive test, but the stderr
    forwarder is monkeypatched to a no-op. With only the stdout
    accumulator reading pipes, there's no two-readers race.

    The watchdog still fires (it's the enforcer regardless of pipe
    traffic), so the observable behaviour is identical: ``dg.Failure``
    raised within the timeout + grace window. The *value* of this test
    is documentation — it demonstrates that the mechanism by which the
    fix works is independent of pipe-FD semantics: an external SIGKILL
    via ``os.killpg`` bypasses the race entirely.

    Without this control, a passing positive test only shows the
    watchdog works; it doesn't validate that the race was the root
    cause of the prior hangs.
    """

    def _noop_forwarder(stderr, log_line, sink):
        # Drain stderr quickly to avoid filling the pipe buffer (which
        # would block the subprocess after a few KB). We don't call
        # ``log_line`` — this is the "no daemon reader" simulation.
        if stderr is None:
            return
        try:
            for _ in stderr:
                pass
        except (OSError, ValueError):
            # Drain-only helper: pipe close / decode errors are the
            # same termination signals the real forwarder swallows.
            # Exit quietly; the watchdog still owns timeout enforcement.
            return

    monkeypatch.setattr(
        "dagster_rocky.resource._forward_stderr_to_sink",
        _noop_forwarder,
    )

    fake = _write_hang_fake(tmp_path)
    rocky = RockyResource(
        binary_path=str(fake),
        config_path=str(tmp_path / "rocky.toml"),
        state_path=str(tmp_path / ".rocky-state.redb"),
        models_dir=str(tmp_path),
        timeout_seconds=2,
    )
    context = _captured_log_context()

    t0 = time.monotonic()
    with pytest.raises(dg.Failure, match="timed out"):
        rocky._run_rocky_streaming(
            ["run", "--filter", "client=test"],
            context,
            allow_partial=True,
        )
    elapsed = time.monotonic() - t0

    # Same budget: if the mechanism depended on pipe semantics, a
    # different rail would need a different budget. It doesn't.
    assert elapsed < 5.0, (
        f"run_streaming took {elapsed:.2f}s without daemon reader; "
        "expected < 5s. The watchdog is the enforcer — race or no race."
    )


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only fake binary")
def test_run_rocky_buffered_path_hard_kills_hung_binary(tmp_path: Path):
    """Regression test for the buffered ``_run_rocky`` after the FR-020 refactor.

    Before this FR ``_run_rocky`` used ``subprocess.run(timeout=…)``, which
    raised ``TimeoutExpired`` synchronously. After the refactor it shares
    the same Popen + sole-reader-threads + watchdog model as the streaming
    path, so the timeout enforcement now runs through ``os.killpg(SIGKILL)``.
    Confirms a hung rocky binary still raises ``dg.Failure`` within the
    configured timeout + a small grace window for the buffered path too,
    not just the streaming path.
    """
    fake = _write_hang_fake(tmp_path)
    rocky = RockyResource(
        binary_path=str(fake),
        config_path=str(tmp_path / "rocky.toml"),
        state_path=str(tmp_path / ".rocky-state.redb"),
        models_dir=str(tmp_path),
        timeout_seconds=2,
    )

    t0 = time.monotonic()
    with pytest.raises(dg.Failure, match="timed out"):
        rocky._run_rocky(["discover"])
    elapsed = time.monotonic() - t0

    # Same budget as the streaming path's positive test: 2s timeout + 3s
    # grace for watchdog + reader joins.
    assert elapsed < 5.0, (
        f"_run_rocky took {elapsed:.2f}s; expected < 5s. Watchdog may not be firing."
    )


def test_run_streaming_threads_partition_flags():
    """run_streaming accepts the same partition kwargs as run() and
    threads them through to both the plan and apply subprocess commands.

    Phase 5: both ``rocky plan`` and ``rocky apply`` (which the engine
    expands back into the persisted flag surface) need the same flags.
    Asserting on the plan-phase argv keeps the test robust to either
    end of the orchestration.
    """
    rocky = RockyResource()
    context = _captured_log_context()
    plan_proc = _streaming_popen_mock(stdout=_plan_json(), stderr_lines=[])
    apply_proc = _streaming_popen_mock(stdout=_apply_envelope_json(), stderr_lines=[])

    captured_cmd: list[list[str]] = []
    procs = iter([plan_proc, apply_proc])

    def fake_popen(cmd, **kwargs):
        captured_cmd.append(cmd)
        return next(procs)

    with (
        patch.object(RockyResource, "_verify_engine_version"),
        patch("dagster_rocky.resource.subprocess.Popen", side_effect=fake_popen),
    ):
        rocky.run_streaming(
            context,
            filter="tenant=acme",
            partition="2026-04-08",
            lookback=2,
            parallel=4,
        )

    # Plan subprocess argv carries every flag (flag-surface parity is
    # enforced by engine PR #535).
    plan_cmd = captured_cmd[0]
    assert "plan" in plan_cmd
    assert "--partition" in plan_cmd
    assert "2026-04-08" in plan_cmd
    assert "--lookback" in plan_cmd
    assert "2" in plan_cmd
    assert "--parallel" in plan_cmd
    assert "4" in plan_cmd
    # Apply subprocess argv is keyed by the plan_id only — the engine
    # rehydrates flags from the persisted plan file.
    apply_cmd = captured_cmd[1]
    assert "apply" in apply_cmd


def test_run_streaming_default_omits_partition_flags():
    """Plain run_streaming() emits no partition flags on the plan argv."""
    rocky = RockyResource()
    context = _captured_log_context()
    plan_proc = _streaming_popen_mock(stdout=_plan_json(), stderr_lines=[])
    apply_proc = _streaming_popen_mock(stdout=_apply_envelope_json(), stderr_lines=[])

    captured_cmd: list[list[str]] = []
    procs = iter([plan_proc, apply_proc])

    def fake_popen(cmd, **kwargs):
        captured_cmd.append(cmd)
        return next(procs)

    with (
        patch.object(RockyResource, "_verify_engine_version"),
        patch("dagster_rocky.resource.subprocess.Popen", side_effect=fake_popen),
    ):
        rocky.run_streaming(context, filter="tenant=acme")

    plan_cmd = captured_cmd[0]
    for flag in ("--partition", "--from", "--to", "--latest", "--missing", "--lookback"):
        assert flag not in plan_cmd


# ---------------------------------------------------------------------------
# run_pipes — full Dagster Pipes integration (T2)
# ---------------------------------------------------------------------------


def _patch_pipes_plan_step(plan_id: str = "a" * 64) -> Any:
    """Patch ``_run_rocky`` so the Phase 5 plan step returns a fixed plan_id.

    ``run_pipes`` runs the plan step via the buffered ``_run_rocky`` and
    then hands the ``["apply", <plan_id>]`` argv to
    ``PipesSubprocessClient.run``. Tests use this helper to skip the
    plan-side subprocess entirely so they can assert on the Pipes
    client call arguments without staging two Popen mocks.
    """
    return patch.object(RockyResource, "_run_rocky", return_value=_plan_json(plan_id))


def test_run_pipes_calls_pipes_client_with_built_command():
    """T2 (Phase 5): run_pipes() runs ``rocky plan`` then forwards the
    ``rocky apply <plan_id>`` argv to a PipesSubprocessClient and returns
    the client's invocation result. The Pipes-side command is keyed off
    the plan id; the plan-side argv carries the user-supplied flags."""
    rocky = RockyResource(config_path="rocky.toml")
    context = MagicMock(spec=dg.AssetExecutionContext)
    fake_client = MagicMock(spec=dg.PipesSubprocessClient)
    fake_invocation = MagicMock(spec=dg.PipesClientCompletedInvocation)
    fake_client.run = MagicMock(return_value=fake_invocation)

    plan_id = "b" * 64
    with _patch_pipes_plan_step(plan_id=plan_id):
        result = rocky.run_pipes(
            context,
            filter="tenant=acme",
            partition="2026-04-08",
            pipes_client=fake_client,
        )

    assert result is fake_invocation
    fake_client.run.assert_called_once()
    call_kwargs = fake_client.run.call_args.kwargs
    assert call_kwargs["context"] is context

    cmd = call_kwargs["command"]
    # Global flags are present
    assert "--config" in cmd
    assert "--output" in cmd
    assert "json" in cmd
    # Apply subcommand + plan id (no per-flag re-emission — the engine
    # rehydrates from the persisted plan file).
    assert "apply" in cmd
    assert plan_id in cmd
    # Phase 5 audit-artifact: plan id is also surfaced as Pipes extras
    # so Dagster shows it as run metadata.
    assert call_kwargs.get("extras") == {"plan_id": plan_id}


def test_run_pipes_threads_all_partition_flags():
    """run_pipes() threads partition kwargs through to the plan-phase
    subprocess argv (Phase 5 — apply-phase argv is just ``apply <plan_id>``).
    """
    rocky = RockyResource()
    context = MagicMock(spec=dg.AssetExecutionContext)
    fake_client = MagicMock(spec=dg.PipesSubprocessClient)
    fake_client.run = MagicMock(return_value=MagicMock())

    captured_plan_args: list[list[str]] = []

    def fake_run_rocky(self, args, *, allow_partial=False):
        captured_plan_args.append(args)
        return _plan_json()

    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run_rocky):
        rocky.run_pipes(
            context,
            filter="tenant=acme",
            partition_from="2026-04-01",
            partition_to="2026-04-08",
            lookback=2,
            parallel=4,
            pipes_client=fake_client,
        )

    plan_args = captured_plan_args[0]
    assert "plan" in plan_args
    assert "--from" in plan_args
    assert "2026-04-01" in plan_args
    assert "--to" in plan_args
    assert "2026-04-08" in plan_args
    assert "--lookback" in plan_args
    assert "2" in plan_args
    assert "--parallel" in plan_args
    assert "4" in plan_args


def test_run_pipes_constructs_default_client_when_none_passed():
    """When no pipes_client is supplied, run_pipes constructs a fresh
    PipesSubprocessClient with Dagster defaults."""
    rocky = RockyResource()
    context = MagicMock(spec=dg.AssetExecutionContext)

    fake_invocation = MagicMock()

    with (
        _patch_pipes_plan_step(),
        patch("dagster_rocky.resource.dg.PipesSubprocessClient") as mock_client_cls,
    ):
        mock_instance = mock_client_cls.return_value
        mock_instance.run = MagicMock(return_value=fake_invocation)

        result = rocky.run_pipes(context, filter="tenant=acme")

        # Default client constructed once with no kwargs
        mock_client_cls.assert_called_once_with()
        # The run() call passed our context + the built command
        mock_instance.run.assert_called_once()
        assert result is fake_invocation


def test_run_pipes_returns_pipes_client_completed_invocation():
    """The return value is whatever PipesSubprocessClient.run() returns —
    callers chain .get_results() to extract materialization events."""
    rocky = RockyResource()
    context = MagicMock(spec=dg.AssetExecutionContext)
    fake_client = MagicMock(spec=dg.PipesSubprocessClient)
    sentinel = MagicMock(name="sentinel_invocation")
    fake_client.run = MagicMock(return_value=sentinel)

    with _patch_pipes_plan_step():
        result = rocky.run_pipes(context, filter="tenant=acme", pipes_client=fake_client)
    assert result is sentinel


def test_run_default_omits_all_partition_flags():
    """Plain rocky.run() with no partition kwargs emits no partition flags."""
    rocky = RockyResource()
    captured, fake_run = _capture_run_args()
    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        rocky.run(filter="tenant=acme")
    args = captured[0]
    partition_flags = (
        "--partition",
        "--from",
        "--to",
        "--latest",
        "--missing",
        "--lookback",
        "--parallel",
    )
    for flag in partition_flags:
        assert flag not in args, f"unexpected {flag} in {args}"


# ---------------------------------------------------------------------------
# Shadow suffix plumbing
# ---------------------------------------------------------------------------


def test_run_with_shadow_suffix_appends_shadow_flags():
    """shadow_suffix enables shadow mode and passes the suffix to the CLI."""
    rocky = RockyResource()
    captured, fake_run = _capture_run_args()
    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        rocky.run(filter="tenant=acme", shadow_suffix="_dagster_pr_42")
    args = captured[0]
    assert "--shadow" in args
    shadow_idx = args.index("--shadow-suffix")
    assert args[shadow_idx + 1] == "_dagster_pr_42"


def test_run_without_shadow_suffix_omits_shadow_flags():
    """When shadow_suffix is None, no --shadow flags appear."""
    rocky = RockyResource()
    captured, fake_run = _capture_run_args()
    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        rocky.run(filter="tenant=acme")
    args = captured[0]
    assert "--shadow" not in args
    assert "--shadow-suffix" not in args


def test_run_streaming_with_shadow_suffix():
    """run_streaming threads shadow_suffix through to the plan argv."""
    rocky = RockyResource()
    context = _captured_log_context()
    plan_proc = _streaming_popen_mock(stdout=_plan_json(), stderr_lines=[])
    apply_proc = _streaming_popen_mock(stdout=_apply_envelope_json(), stderr_lines=[])

    captured_cmd: list[list[str]] = []
    procs = iter([plan_proc, apply_proc])

    def fake_popen(cmd, **kwargs):
        captured_cmd.append(cmd)
        return next(procs)

    with (
        patch.object(RockyResource, "_verify_engine_version"),
        patch("dagster_rocky.resource.subprocess.Popen", side_effect=fake_popen),
    ):
        rocky.run_streaming(
            context,
            filter="tenant=acme",
            shadow_suffix="_dagster_pr_99",
        )

    plan_cmd = captured_cmd[0]
    assert "--shadow" in plan_cmd
    shadow_idx = plan_cmd.index("--shadow-suffix")
    assert plan_cmd[shadow_idx + 1] == "_dagster_pr_99"


def test_run_pipes_with_shadow_suffix():
    """run_pipes threads shadow_suffix through to the plan-phase argv
    (Phase 5: the apply-phase argv is just ``apply <plan_id>``)."""
    rocky = RockyResource()
    context = MagicMock(spec=dg.AssetExecutionContext)
    fake_client = MagicMock(spec=dg.PipesSubprocessClient)
    fake_client.run = MagicMock(return_value=MagicMock())

    captured_plan_args: list[list[str]] = []

    def fake_run_rocky(self, args, *, allow_partial=False):
        captured_plan_args.append(args)
        return _plan_json()

    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run_rocky):
        rocky.run_pipes(
            context,
            filter="tenant=acme",
            shadow_suffix="_dagster_pr_7",
            pipes_client=fake_client,
        )

    plan_args = captured_plan_args[0]
    assert "--shadow" in plan_args
    shadow_idx = plan_args.index("--shadow-suffix")
    assert plan_args[shadow_idx + 1] == "_dagster_pr_7"


def test_compile_uses_http_when_server_url_is_set():
    rocky = RockyResource(server_url="http://localhost:8080")
    payload = (
        '{"version":"0.1.0","command":"compile","models":0,"execution_layers":0,'
        '"diagnostics":[],"has_errors":false}'
    )
    with patch.object(RockyResource, "_http_get", return_value=payload) as http_mock:
        result = rocky.compile()
    http_mock.assert_called_once_with("/api/v1/compile")
    assert result.command == "compile"


def test_lineage_uses_http_when_server_url_is_set():
    rocky = RockyResource(server_url="http://localhost:8080")
    model_payload = (
        '{"version":"0.1.0","command":"lineage","model":"orders",'
        '"columns":[],"upstream":[],"downstream":[],"edges":[]}'
    )
    column_payload = (
        '{"version":"0.1.0","command":"lineage","model":"orders","column":"total","trace":[]}'
    )

    with patch.object(RockyResource, "_http_get") as http_mock:
        http_mock.return_value = model_payload
        result = rocky.lineage("orders")
        assert result.model == "orders"
        http_mock.assert_called_with("/api/v1/models/orders/lineage")

        http_mock.return_value = column_payload
        col = rocky.lineage("orders", column="total")
        assert col.column == "total"
        http_mock.assert_called_with("/api/v1/models/orders/lineage/total")


def test_metrics_uses_http_when_server_url_is_set():
    rocky = RockyResource(server_url="http://localhost:8080")
    payload = '{"version":"0.3.0","command":"metrics","model":"orders","snapshots":[],"count":0}'
    with patch.object(RockyResource, "_http_get", return_value=payload) as http_mock:
        result = rocky.metrics("orders")
    http_mock.assert_called_once_with("/api/v1/models/orders/metrics")
    assert result.model == "orders"


# ---------------------------------------------------------------------------
# Phase 5 plan/apply orchestration
# ---------------------------------------------------------------------------


def test_run_dispatches_plan_then_apply_when_plan_id_persisted():
    """Phase 5 happy path — ``rocky plan`` persists a plan_id, then the
    integration dispatches ``rocky apply <plan_id>`` and returns the
    inner RunResult unwrapped from the apply envelope."""
    rocky = RockyResource()
    captured: list[list[str]] = []
    plan_id = "f" * 64

    def fake_run(self, args, allow_partial=False):
        captured.append(list(args))
        if args[0] == "plan":
            return _plan_json(plan_id=plan_id)
        # apply <plan_id> — return the envelope.
        return _apply_envelope_json(plan_id=plan_id)

    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        result = rocky.run(filter="tenant=acme")

    # First call: rocky plan with the user-supplied flags.
    assert captured[0][:3] == ["plan", "--filter", "tenant=acme"]
    # Second call: rocky apply <plan_id>.
    assert captured[1] == ["apply", plan_id]
    # Inner RunResult parsed out of the envelope.
    assert result.command == "run"
    assert result.tables_copied == 1


def test_run_routes_replication_only_project_through_apply():
    """Phase 5b — replication-only projects now content-address a plan
    just like model-driven projects do, so ``run`` always routes
    through ``rocky apply`` and never falls back to ``rocky run``. The
    plan output carries ``plan_kind == "replication"`` and a real
    64-char plan_id.
    """
    rocky = RockyResource()
    captured: list[list[str]] = []
    plan_id = "f" * 64

    def fake_run(self, args, allow_partial=False):
        captured.append(list(args))
        if args[0] == "plan":
            # Phase 5b: engine emits a real plan_id for replication-only.
            return json.dumps(
                {
                    "version": "0.1.0",
                    "command": "plan",
                    "filter": "tenant=acme",
                    "statements": [],
                    "plan_id": plan_id,
                    "plan_kind": "replication",
                    "created_at": "2026-05-18T00:00:00Z",
                    "models": [],
                    "execution_layers": [],
                }
            )
        # Apply path returns the standard RunResult shape.
        return _run_json()

    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        result = rocky.run(filter="tenant=acme")

    # First call: rocky plan.
    assert captured[0][0] == "plan"
    # Second call: rocky apply <plan_id> — NOT the legacy run fallback.
    assert captured[1] == ["apply", plan_id]
    assert result.command == "run"
    assert result.tables_copied == 1


def test_run_raises_when_engine_returns_null_plan_id():
    """Phase 5b — the engine should always emit a plan_id. When it
    doesn't (engine version skew, malformed payload), ``_apply_plan``
    raises :class:`dg.Failure` with an upgrade hint instead of
    silently falling back to ``rocky run``."""
    rocky = RockyResource()

    def fake_run(self, args, allow_partial=False):
        if args[0] == "plan":
            # Simulated stale-engine payload — plan_id absent.
            return json.dumps(
                {
                    "version": "0.1.0",
                    "command": "plan",
                    "filter": "tenant=acme",
                    "statements": [],
                    "plan_id": None,
                }
            )
        # If apply were called we'd reach here — but we shouldn't.
        return _run_json()

    with (
        patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run),
        pytest.raises(dg.Failure) as exc,
    ):
        rocky.run(filter="tenant=acme")

    assert "plan_id" in str(exc.value)
    assert "engine-v1.34" in str(exc.value) or "plan_id" in str(exc.value)


def test_run_pipes_attaches_extras_with_plan_id():
    """Phase 5 audit-artifact wiring — when ``rocky plan`` returns a
    plan_id, ``run_pipes`` surfaces it as Pipes ``extras`` so Dagster
    shows the plan id as run metadata. The persisted plan lives at
    ``.rocky/plans/<plan_id>.json`` for offline inspection."""
    rocky = RockyResource()
    context = MagicMock(spec=dg.AssetExecutionContext)
    fake_client = MagicMock(spec=dg.PipesSubprocessClient)
    fake_client.run = MagicMock(return_value=MagicMock())

    plan_id = "c" * 64
    with patch.object(RockyResource, "_run_rocky", return_value=_plan_json(plan_id=plan_id)):
        rocky.run_pipes(context, filter="tenant=acme", pipes_client=fake_client)

    fake_client.run.assert_called_once()
    extras = fake_client.run.call_args.kwargs.get("extras")
    assert extras == {"plan_id": plan_id}


def test_run_pipes_raises_when_engine_returns_null_plan_id():
    """Phase 5b — ``run_pipes`` no longer falls back to ``rocky run``
    when the engine returns a null ``plan_id``. It raises
    :class:`dg.Failure` so version skew surfaces loudly instead of a
    silent behaviour change.
    """
    rocky = RockyResource()
    context = MagicMock(spec=dg.AssetExecutionContext)
    fake_client = MagicMock(spec=dg.PipesSubprocessClient)
    fake_client.run = MagicMock(return_value=MagicMock())

    plan_without_id = json.dumps(
        {
            "version": "0.1.0",
            "command": "plan",
            "filter": "tenant=acme",
            "statements": [],
            "plan_id": None,
        }
    )
    with (
        patch.object(RockyResource, "_run_rocky", return_value=plan_without_id),
        pytest.raises(dg.Failure) as exc,
    ):
        rocky.run_pipes(context, filter="tenant=acme", pipes_client=fake_client)

    # Pipes client should NOT have been invoked — the failure is raised
    # before reaching the subprocess.
    fake_client.run.assert_not_called()
    assert "plan_id" in str(exc.value)


def test_extract_plan_id_returns_none_for_malformed_payload():
    """``_extract_plan_id`` is the gate keeping malformed plan output
    from sending the apply step into a broken state. Treat any
    non-string ``plan_id`` (missing, null, wrong type, malformed JSON)
    as 'no plan persisted' so :meth:`_apply_plan` raises a clear
    upgrade-hint Failure instead of silently routing into a broken
    state."""
    assert RockyResource._extract_plan_id("not json") is None
    assert RockyResource._extract_plan_id("{}") is None
    assert RockyResource._extract_plan_id(json.dumps({"plan_id": None})) is None
    assert RockyResource._extract_plan_id(json.dumps({"plan_id": 42})) is None
    assert RockyResource._extract_plan_id(json.dumps({"plan_id": "abc"})) == "abc"


def test_build_plan_args_mirrors_build_run_args_flag_surface():
    """Cluster 3 B Phase 5 leans on engine PR #535 (flag-surface parity
    between ``rocky run`` and ``rocky plan``). This test pins the parity
    on the Python side — every flag ``_build_run_args`` emits must also
    appear in ``_build_plan_args``'s output for the same kwargs (only the
    leading verb differs).
    """
    rocky = RockyResource(models_dir="m")
    kwargs: dict[str, Any] = {
        "governance_override": {"workspace_ids": [1]},
        "pipeline": "main",
        "run_models": True,
        "partition": "2026-04-08",
        "partition_from": None,
        "partition_to": None,
        "latest": False,
        "missing": False,
        "lookback": 3,
        "parallel": 2,
        "shadow_suffix": "_pr_42",
        "idempotency_key": "key-1",
    }
    run_args = rocky._build_run_args("tenant=acme", **kwargs)
    plan_args = rocky._build_plan_args("tenant=acme", **kwargs)
    # Only the verb token differs.
    assert run_args[0] == "run"
    assert plan_args[0] == "plan"
    assert run_args[1:] == plan_args[1:]


# ---------------------------------------------------------------------------
# cost — historical per-run cost attribution
# ---------------------------------------------------------------------------


def _cost_json(run_id: str = "run-abc123") -> str:
    """Return a minimal valid CostOutput JSON payload for `run_id`."""
    return json.dumps(
        {
            "version": "1.0.0",
            "command": "cost",
            "run_id": run_id,
            "trigger": "Manual",
            "status": "Success",
            "started_at": "2026-04-22T10:00:00Z",
            "finished_at": "2026-04-22T10:00:05Z",
            "duration_ms": 5000,
            "total_duration_ms": 5000,
            "total_bytes_scanned": 1024,
            "total_bytes_written": 512,
            "total_cost_usd": 0.0001,
            "adapter_type": "duckdb",
            "per_model": [
                {
                    "model_name": "orders",
                    "status": "Success",
                    "duration_ms": 5000,
                    "rows_affected": 100,
                    "bytes_scanned": 1024,
                    "bytes_written": 512,
                    "cost_usd": 0.0001,
                },
            ],
        }
    )


def test_cost_defaults_to_latest():
    """``cost()`` with no arg should shell out with ``cost latest``."""
    rocky = RockyResource()
    captured: list[list[str]] = []

    def fake_run(self, args, allow_partial=False):
        captured.append(args)
        return _cost_json()

    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        result = rocky.cost()

    assert captured[0] == ["cost", "latest"]
    assert result.command == "cost"
    assert result.run_id == "run-abc123"
    assert result.total_cost_usd == 0.0001
    assert len(result.per_model) == 1
    assert result.per_model[0].model_name == "orders"


def test_cost_passes_explicit_run_id():
    """A specific run_id is forwarded positionally to the CLI."""
    rocky = RockyResource()
    captured: list[list[str]] = []

    def fake_run(self, args, allow_partial=False):
        captured.append(args)
        return _cost_json(run_id="run-xyz789")

    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        result = rocky.cost("run-xyz789")

    assert captured[0] == ["cost", "run-xyz789"]
    assert result.run_id == "run-xyz789"


# ---------------------------------------------------------------------------
# _http_get
# ---------------------------------------------------------------------------


def test_http_get_returns_decoded_body():
    rocky = RockyResource(server_url="http://localhost:8080")
    response = MagicMock()
    response.read.return_value = b"hello"
    response.__enter__.return_value = response
    response.__exit__.return_value = False

    with patch("dagster_rocky.resource.urllib.request.urlopen", return_value=response) as mock:
        body = rocky._http_get("/path")

    assert body == "hello"
    mock.assert_called_once()
    assert mock.call_args.kwargs["timeout"] == DEFAULT_HTTP_TIMEOUT_SECONDS


def test_http_get_raises_failure_on_url_error():
    rocky = RockyResource(server_url="http://localhost:8080")
    with (
        patch(
            "dagster_rocky.resource.urllib.request.urlopen",
            side_effect=urllib.error.URLError("nope"),
        ),
        pytest.raises(dg.Failure, match="Rocky server request failed"),
    ):
        rocky._http_get("/path")


def test_http_get_without_server_url_raises():
    rocky = RockyResource()
    with pytest.raises(dg.Failure, match="server_url is not configured"):
        rocky._http_get("/path")


# ---------------------------------------------------------------------------
# _verify_engine_version — MIN_ROCKY_VERSION check
# ---------------------------------------------------------------------------


def _version_completed(version_stdout: str, returncode: int = 0):
    """Build a CompletedProcess for a ``rocky --version`` call."""
    return subprocess.CompletedProcess(
        args=["rocky", "--version"],
        returncode=returncode,
        stdout=version_stdout,
        stderr="",
    )


def test_verify_version_passes_when_binary_is_new_enough():
    """When rocky --version reports a version >= MIN_ROCKY_VERSION, the check passes silently."""
    rocky = RockyResource()
    with _patched_run(return_value=_version_completed(f"rocky {MIN_ROCKY_VERSION}")):
        rocky._verify_engine_version()
    # Subsequent calls are no-ops (cached)
    rocky._verify_engine_version()


def test_verify_version_passes_with_newer_version():
    """A version newer than the minimum passes."""
    rocky = RockyResource()
    with _patched_run(return_value=_version_completed("rocky 99.0.0")):
        rocky._verify_engine_version()


def test_verify_version_passes_without_rocky_prefix():
    """rocky --version output without the 'rocky ' prefix is also parsed."""
    rocky = RockyResource()
    with _patched_run(return_value=_version_completed(MIN_ROCKY_VERSION)):
        rocky._verify_engine_version()


def test_verify_version_raises_when_binary_is_too_old():
    """When rocky --version reports a version below MIN_ROCKY_VERSION,
    a clear dg.Failure is raised with the detected and required versions."""
    rocky = RockyResource()
    with (
        _patched_run(return_value=_version_completed("rocky 0.1.0")),
        pytest.raises(dg.Failure) as excinfo,
    ):
        rocky._verify_engine_version()

    desc = excinfo.value.description or ""
    assert "0.1.0" in desc
    assert MIN_ROCKY_VERSION in desc
    assert "below the minimum" in desc
    assert excinfo.value.metadata is not None
    assert excinfo.value.metadata["detected_version"].text == "0.1.0"
    assert excinfo.value.metadata["min_version"].text == MIN_ROCKY_VERSION


def test_verify_version_raises_when_binary_not_found():
    """FileNotFoundError from the binary results in a clear Failure."""
    rocky = RockyResource(binary_path="/nonexistent/rocky")
    with (
        _patched_run(side_effect=FileNotFoundError),
        pytest.raises(dg.Failure) as excinfo,
    ):
        rocky._verify_engine_version()

    desc = excinfo.value.description or ""
    assert "not found" in desc.lower()
    assert "/nonexistent/rocky" in desc


def test_verify_version_skips_on_timeout():
    """If rocky --version hangs, the check is skipped (best-effort)."""
    rocky = RockyResource()
    with _patched_run(side_effect=subprocess.TimeoutExpired(cmd="rocky", timeout=10)):
        rocky._verify_engine_version()
    # Should have cached the skip — subsequent calls are no-ops
    rocky._verify_engine_version()


def test_verify_version_skips_on_empty_output():
    """If rocky --version returns empty stdout, the check is skipped."""
    rocky = RockyResource()
    with _patched_run(return_value=_version_completed("")):
        rocky._verify_engine_version()


def test_verify_version_skips_on_unparseable_output():
    """Non-semver output (e.g. a dev build hash) skips the check."""
    rocky = RockyResource()
    with _patched_run(return_value=_version_completed("rocky dev-abc123")):
        rocky._verify_engine_version()


@pytest.mark.parametrize(
    "version_stdout",
    [
        f"rocky {MIN_ROCKY_VERSION}-dev",
        f"rocky {MIN_ROCKY_VERSION}-pre",
        f"rocky {MIN_ROCKY_VERSION}-rc.1",
        f"rocky {MIN_ROCKY_VERSION}-alpha.2",
        f"rocky {MIN_ROCKY_VERSION}+sha.abc123",
        f"{MIN_ROCKY_VERSION}-dev",  # also handle the no-prefix case
    ],
)
def test_verify_version_strips_pre_release_suffix(version_stdout: str):
    """Pre-release / build suffixes (``-dev``, ``-pre``, ``-rc.N``,
    ``+sha.<hash>``) must be stripped before comparison so a dev build
    that matches the minimum version on the core triple is treated as
    eligible. Without this, ``int("4-dev")`` fails the parse and the
    check silently skips — which is exactly how a too-old dev build
    used to slip past the gate."""
    rocky = RockyResource()
    with _patched_run(return_value=_version_completed(version_stdout)):
        rocky._verify_engine_version()  # must not raise + must not silently skip


def test_verify_version_dev_suffix_below_minimum_fails():
    """A dev build whose core version is below the minimum still fails."""
    rocky = RockyResource()
    with (
        _patched_run(return_value=_version_completed("rocky 0.99.99-dev")),
        pytest.raises(dg.Failure, match="below the minimum"),
    ):
        rocky._verify_engine_version()


def test_verify_version_caches_result():
    """After a successful check, subsequent calls don't invoke subprocess."""
    rocky = RockyResource()
    with _patched_run(return_value=_version_completed(f"rocky {MIN_ROCKY_VERSION}")) as run_mock:
        rocky._verify_engine_version()
        rocky._verify_engine_version()
        rocky._verify_engine_version()
    # Only called once
    assert run_mock.call_count == 1


def test_verify_version_semver_comparison_logic():
    """Test that tuple comparison works correctly for semver:
    (1, 0, 0) >= (1, 0, 0), (0, 99, 99) < (1, 0, 0), etc."""
    rocky = RockyResource()

    # Equal to minimum — should pass
    with _patched_run(return_value=_version_completed(f"rocky {MIN_ROCKY_VERSION}")):
        rocky._verify_engine_version()

    # Reset for next check
    object.__setattr__(rocky, "_version_checked", False)

    # Major version ahead — should pass
    with _patched_run(return_value=_version_completed("rocky 2.0.0")):
        rocky._verify_engine_version()

    # Reset for next check
    object.__setattr__(rocky, "_version_checked", False)

    # Minor version ahead, same major — should pass
    with _patched_run(return_value=_version_completed("rocky 1.1.0")):
        rocky._verify_engine_version()

    # Reset for next check
    object.__setattr__(rocky, "_version_checked", False)

    # Below minimum — should fail
    with (
        _patched_run(return_value=_version_completed("rocky 0.99.99")),
        pytest.raises(dg.Failure, match="below the minimum"),
    ):
        rocky._verify_engine_version()


def test_verify_version_called_by_run_rocky():
    """_run_rocky calls _verify_engine_version before executing the command.

    Version check still goes through ``subprocess.run`` (one-shot, no
    streaming needed). The actual command goes through the shared
    Popen + log-sink helper.
    """
    rocky = RockyResource()
    proc = _popen_mock(stdout='{"ok": true}')

    with (
        _patched_run(return_value=_version_completed(f"rocky {MIN_ROCKY_VERSION}")) as run_mock,
        patch("dagster_rocky.resource.subprocess.Popen", return_value=proc) as popen_mock,
    ):
        rocky._run_rocky(["discover"])

    # subprocess.run was called exactly once — for the version check.
    version_call = run_mock.call_args_list[0]
    assert "--version" in version_call.args[0]
    # The actual command went through Popen.
    actual_call = popen_mock.call_args_list[0]
    assert "discover" in actual_call.args[0]


def test_verify_version_called_by_run_rocky_streaming():
    """_run_rocky_streaming also calls _verify_engine_version."""
    rocky = RockyResource()
    context = _captured_log_context()
    proc = _streaming_popen_mock(stdout=_run_json(), stderr_lines=[])

    with (
        _patched_run(return_value=_version_completed(f"rocky {MIN_ROCKY_VERSION}")),
        patch("dagster_rocky.resource.subprocess.Popen", return_value=proc),
    ):
        rocky._run_rocky_streaming(["run", "--filter", "x=y"], context)

    # If we got here without a Failure, the version check passed and
    # the streaming path ran successfully.


# ---------------------------------------------------------------------------
# doctor — --check filter argv plumbing
# ---------------------------------------------------------------------------


def _doctor_json() -> str:
    """Minimal well-formed DoctorResult JSON payload."""
    return json.dumps(
        {
            "command": "doctor",
            "overall": "healthy",
            "checks": [],
            "suggestions": [],
        }
    )


def test_doctor_without_check_kwarg_omits_check_flag():
    """Default ``doctor()`` call must not emit ``--check`` — backwards compat."""
    rocky = RockyResource()
    captured: list[list[str]] = []

    def fake_run(self, args, allow_partial=False):
        captured.append(args)
        return _doctor_json()

    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        rocky.doctor()

    assert captured[0] == ["doctor"]
    assert "--check" not in captured[0]


def test_doctor_with_check_kwarg_appends_check_flag():
    """``doctor(check="state_rw")`` must forward ``--check state_rw`` to the CLI."""
    rocky = RockyResource()
    captured: list[list[str]] = []

    def fake_run(self, args, allow_partial=False):
        captured.append(args)
        return _doctor_json()

    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        rocky.doctor(check="state_rw")

    assert captured[0] == ["doctor", "--check", "state_rw"]


def test_doctor_with_check_kwarg_forwards_arbitrary_id():
    """The Python side must not pre-validate the check id — the engine owns that set."""
    rocky = RockyResource()
    captured: list[list[str]] = []

    def fake_run(self, args, allow_partial=False):
        captured.append(args)
        return _doctor_json()

    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        rocky.doctor(check="totally-made-up-id")

    assert captured[0] == ["doctor", "--check", "totally-made-up-id"]


# ---------------------------------------------------------------------------
# compliance() / retention_status() — governance Waves B + C-2 accessors
# ---------------------------------------------------------------------------


def test_compliance_builds_argv_without_env(compliance_json: str):
    """Default ``compliance()`` call emits ``compliance --output json``."""
    rocky = RockyResource()
    captured: list[list[str]] = []

    def fake_run(self, args, allow_partial=False):
        captured.append(args)
        return compliance_json

    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        result = rocky.compliance()

    assert captured[0] == ["compliance", "--output", "json"]
    assert result.command == "compliance"
    assert len(result.exceptions) == 2


def test_compliance_forwards_env_flag(compliance_json: str):
    rocky = RockyResource()
    captured: list[list[str]] = []

    def fake_run(self, args, allow_partial=False):
        captured.append(args)
        return compliance_json

    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        rocky.compliance(env="prod")

    assert captured[0] == ["compliance", "--output", "json", "--env", "prod"]


def test_retention_status_builds_argv_without_env(retention_status_json: str):
    """Default ``retention_status()`` call emits ``retention-status --output json``."""
    rocky = RockyResource()
    captured: list[list[str]] = []

    def fake_run(self, args, allow_partial=False):
        captured.append(args)
        return retention_status_json

    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        result = rocky.retention_status()

    assert captured[0] == ["retention-status", "--output", "json"]
    assert result.command == "retention-status"
    assert len(result.models) == 3


def test_retention_status_forwards_env_flag(retention_status_json: str):
    rocky = RockyResource()
    captured: list[list[str]] = []

    def fake_run(self, args, allow_partial=False):
        captured.append(args)
        return retention_status_json

    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        rocky.retention_status(env="prod")

    assert captured[0] == ["retention-status", "--output", "json", "--env", "prod"]


# ---------------------------------------------------------------------------
# Argv redaction + stderr truncation (security hardening)
# ---------------------------------------------------------------------------


class TestRedactArgv:
    """``_redact_argv`` masks the value of credential-bearing flags only.

    The subprocess still receives the unredacted argv — only the *log
    line* changes. Tests pin both that the value is masked and that
    nothing else gets touched.
    """

    def test_idempotency_key_value_is_masked(self):
        argv = [
            "rocky",
            "--config",
            "rocky.toml",
            "run",
            "--filter",
            "tenant=acme",
            "--idempotency-key",
            "abcd-secret-token",
        ]
        redacted = _redact_argv(argv)

        # The flag itself stays intact so the log line still says what
        # was passed; only the *next* token is replaced.
        assert "--idempotency-key" in redacted
        assert "***" in redacted
        # The actual key MUST NOT survive into the redacted form.
        assert "abcd-secret-token" not in redacted
        # The flag's index is preserved and the redacted token sits
        # immediately after it, matching the original positional shape.
        idx = redacted.index("--idempotency-key")
        assert redacted[idx + 1] == "***"

    def test_governance_override_json_payload_is_masked(self):
        # The governance-override value carries a JSON blob containing
        # workspace IDs and grant principals — sensitive payload.
        argv = [
            "rocky",
            "run",
            "--governance-override",
            '{"workspace_ids":[42],"grants":[{"principal":"svc-acct"}]}',
        ]
        redacted = _redact_argv(argv)
        assert "--governance-override" in redacted
        assert "***" in redacted
        # No fragment of the JSON payload may leak.
        assert "workspace_ids" not in " ".join(redacted)
        assert "svc-acct" not in " ".join(redacted)

    def test_non_sensitive_flags_pass_through(self):
        # Non-sensitive flags (filter, partition, etc.) keep their values
        # so debugging ``--filter tenant=acme`` stays informative.
        argv = ["rocky", "run", "--filter", "tenant=acme", "--parallel", "4"]
        redacted = _redact_argv(argv)
        assert redacted == argv  # no changes

    def test_multiple_sensitive_flags_each_redacted(self):
        argv = [
            "rocky",
            "run",
            "--governance-override",
            '{"workspace_ids":[1]}',
            "--filter",
            "tenant=acme",
            "--idempotency-key",
            "key-2026-04-29",
        ]
        redacted = _redact_argv(argv)
        # Both sensitive values masked; non-sensitive filter survives.
        assert redacted.count("***") == 2
        assert "tenant=acme" in redacted
        assert "key-2026-04-29" not in redacted

    def test_redaction_does_not_mutate_input(self):
        # The redacted form is a *copy* — the subprocess argv must stay
        # untouched so the real values still reach rocky.
        argv = ["rocky", "--idempotency-key", "real-key"]
        original = list(argv)
        _redact_argv(argv)
        assert argv == original


class TestTruncateStderrForMetadata:
    """``_truncate_stderr_for_metadata`` caps ``dg.Failure.metadata`` size."""

    def test_short_input_unchanged(self):
        stderr = "short error message"
        assert _truncate_stderr_for_metadata(stderr) == stderr

    def test_input_at_cap_unchanged(self):
        # Exactly 8192 bytes — boundary case, must NOT add the marker.
        stderr = "x" * 8192
        out = _truncate_stderr_for_metadata(stderr)
        assert out == stderr
        assert "truncated" not in out

    def test_oversize_input_truncated_with_marker(self):
        # 20KB of stderr — well above the 8KB cap.
        stderr = "x" * 20_000
        out = _truncate_stderr_for_metadata(stderr)

        # The output is bounded — the prefix is exactly ``cap`` bytes
        # plus a single ~40-byte marker advertising the original size.
        # 8KB + a generous marker headroom keeps the cap operator-friendly.
        assert len(out) <= 8300
        assert "truncated" in out
        assert "20000 bytes" in out

    def test_marker_advertises_correct_original_size(self):
        # The marker must report the *input* length so operators know
        # how much was clipped, not just that clipping happened.
        stderr = "y" * 50_000
        out = _truncate_stderr_for_metadata(stderr)
        assert "50000 bytes" in out


def test_run_rocky_failure_truncates_oversize_stderr_in_metadata():
    """End-to-end: a Rocky failure with multi-KB stderr surfaces a
    truncated string in ``dg.Failure.metadata``, not the raw blob.

    Pins both the SEC fix (no unbounded blob in the Dagster UI) and the
    operator-debugging path (the marker tells them how much was clipped).
    The buffered path now keeps only the last 20 stderr lines (same tail
    semantics as the streaming path), so we feed in 50 long lines and
    verify the surfaced ``stderr_tail`` is bounded by the metadata cap.
    """
    rocky = RockyResource()
    long_line = "ERR " * 200  # 800 bytes/line × 50 lines = 40KB, well above the 8KB cap
    huge_stderr_lines = [long_line for _ in range(50)]
    proc = _popen_mock(stdout="", stderr_lines=huge_stderr_lines, returncode=2)
    with (
        patch.object(RockyResource, "_verify_engine_version"),
        patch("dagster_rocky.resource.subprocess.Popen", return_value=proc),
        pytest.raises(dg.Failure) as excinfo,
    ):
        rocky._run_rocky(["discover"])

    metadata = excinfo.value.metadata
    assert "stderr_tail" in metadata
    text = metadata["stderr_tail"].text
    assert len(text) <= 8300
    assert "truncated" in text
