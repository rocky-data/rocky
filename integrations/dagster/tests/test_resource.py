"""Tests for ``RockyResource`` subprocess and HTTP plumbing.

These tests patch the ``subprocess.run`` and ``urllib.request.urlopen``
calls so they exercise resource behavior end-to-end without invoking the
real Rocky binary or hitting a server.
"""

from __future__ import annotations

import json
import signal
import subprocess
import sys
import threading
import time
import urllib.error
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import dagster as dg
import pytest

from dagster_rocky.resource import (
    DEFAULT_HTTP_TIMEOUT_SECONDS,
    DEFAULT_TIMEOUT_SECONDS,
    MIN_ROCKY_VERSION,
    RockyResource,
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
# ---------------------------------------------------------------------------


def test_run_rocky_returns_stdout_on_success():
    rocky = RockyResource()
    with _patched_run(return_value=_completed(stdout='{"hello": "world"}')) as run_mock:
        out = rocky._run_rocky(["discover"])
    assert out == '{"hello": "world"}'
    # Default timeout was passed through.
    assert run_mock.call_args.kwargs["timeout"] == DEFAULT_TIMEOUT_SECONDS


def test_run_rocky_uses_configured_timeout():
    rocky = RockyResource(timeout_seconds=42)
    with _patched_run(return_value=_completed()) as run_mock:
        rocky._run_rocky(["discover"])
    assert run_mock.call_args.kwargs["timeout"] == 42


def test_run_rocky_partial_success_returns_stdout():
    """A non-zero exit with valid JSON should return the JSON when allowed."""
    rocky = RockyResource()
    payload = '{"command": "run", "tables_failed": 1}'
    with _patched_run(return_value=_completed(stdout=payload, returncode=1)):
        out = rocky._run_rocky(["run", "--filter", "x=y"], allow_partial=True)
    assert out == payload


def test_run_rocky_partial_success_disallowed_raises():
    """Without ``allow_partial`` a non-zero exit always becomes a Failure."""
    rocky = RockyResource()
    with (
        _patched_run(return_value=_completed(stdout="{}", stderr="boom", returncode=1)),
        pytest.raises(dg.Failure, match="exit 1"),
    ):
        rocky._run_rocky(["discover"])


def test_run_rocky_partial_success_only_when_json():
    """Partial success only kicks in when stdout actually starts with JSON."""
    rocky = RockyResource()
    with (
        _patched_run(return_value=_completed(stdout="not json", stderr="boom", returncode=2)),
        pytest.raises(dg.Failure),
    ):
        rocky._run_rocky(["run", "--filter", "x=y"], allow_partial=True)


def test_run_rocky_missing_binary_raises_failure():
    rocky = RockyResource(binary_path="/nope/rocky")
    with (
        _patched_run(side_effect=FileNotFoundError),
        pytest.raises(dg.Failure, match="not found"),
    ):
        rocky._run_rocky(["discover"])


def test_run_rocky_timeout_raises_failure():
    rocky = RockyResource(timeout_seconds=5)
    with (
        _patched_run(side_effect=subprocess.TimeoutExpired(cmd="rocky", timeout=5)),
        pytest.raises(dg.Failure, match="timed out after 5s"),
    ):
        rocky._run_rocky(["discover"])


# ---------------------------------------------------------------------------
# CLI command builders
# ---------------------------------------------------------------------------


def test_run_passes_governance_override_as_json():
    rocky = RockyResource()
    captured: list[list[str]] = []

    def fake_run(*, args, **_):
        captured.append(args)
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

    assert "--governance-override" in captured[0]
    idx = captured[0].index("--governance-override")
    assert '"workspace_ids"' in captured[0][idx + 1]


def test_run_with_run_models_appends_models_and_all():
    rocky = RockyResource(models_dir="m")
    captured: list[list[str]] = []

    def fake_run(self, args, allow_partial=False):
        captured.append(args)
        return (
            '{"version":"0.3.0","command":"run","filter":"tenant=acme",'
            '"duration_ms":0,"tables_copied":0,"materializations":[],'
            '"check_results":[],"permissions":{"grants_added":0,"grants_revoked":0,'
            '"catalogs_created":0,"schemas_created":0},"drift":{"tables_checked":0,'
            '"tables_drifted":0,"actions_taken":[]}}'
        )

    with patch.object(RockyResource, "_run_rocky", autospec=True, side_effect=fake_run):
        rocky.run(filter="tenant=acme", run_models=True)

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


def _capture_run_args() -> tuple[list[list[str]], Any]:
    """Set up a captor + side_effect that records args and returns empty JSON."""
    captured: list[list[str]] = []

    def fake_run(self, args, allow_partial=False):
        captured.append(args)
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


def test_run_streaming_forwards_stderr_to_context_log():
    """Each non-empty stderr line is forwarded to context.log.info with a
    'rocky:' prefix as the subprocess runs."""
    rocky = RockyResource()
    context = _captured_log_context()
    proc = _streaming_popen_mock(
        stdout=_run_json(),
        stderr_lines=[
            "INFO discovering sources",
            "INFO copying table acme.orders",
            "INFO copying table acme.payments",
            "INFO run complete",
        ],
    )

    with (
        patch.object(RockyResource, "_verify_engine_version"),
        patch("dagster_rocky.resource.subprocess.Popen", return_value=proc),
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
    proc = _streaming_popen_mock(
        stdout=_run_json(),
        stderr_lines=["INFO start", "", "  ", "INFO done"],
    )

    with (
        patch.object(RockyResource, "_verify_engine_version"),
        patch("dagster_rocky.resource.subprocess.Popen", return_value=proc),
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
    subprocess exits."""
    rocky = RockyResource()
    context = _captured_log_context()
    payload = json.dumps(
        {
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
    )
    proc = _streaming_popen_mock(stdout=payload, stderr_lines=[])

    with (
        patch.object(RockyResource, "_verify_engine_version"),
        patch("dagster_rocky.resource.subprocess.Popen", return_value=proc),
    ):
        result = rocky.run_streaming(context, filter="tenant=acme")

    assert result.duration_ms == 12345
    assert result.tables_copied == 7


def test_run_streaming_partial_success_returns_result_on_nonzero_exit():
    """Same partial-success semantics as run(): non-zero exit + valid
    JSON stdout still returns the parsed result."""
    rocky = RockyResource()
    context = _captured_log_context()
    proc = _streaming_popen_mock(
        stdout=_run_json(),
        stderr_lines=["WARN one table failed"],
        returncode=1,
    )

    with (
        patch.object(RockyResource, "_verify_engine_version"),
        patch("dagster_rocky.resource.subprocess.Popen", return_value=proc),
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


_HANG_WITH_STDERR_CHATTER_SCRIPT = """#!/bin/sh
# Mimics the 2026-04-19 production behaviour: stream stderr at ~20Hz
# while never exiting. Under the two-readers race on proc.stderr, the
# old communicate(timeout=) path intermittently failed to fire, leaving
# the subprocess hanging for hours.
echo '{"started": true}' >&2
while true; do
    echo 'still uploading state...' >&2
    sleep 0.05
done
"""


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

    def _noop_forwarder(stderr, context, sink):
        # Drain stderr quickly to avoid filling the pipe buffer (which
        # would block the subprocess after a few KB). We don't log
        # anything to context.log — this is the "no daemon reader"
        # simulation.
        if stderr is None:
            return
        try:
            for _ in stderr:
                pass
        except (OSError, ValueError):
            pass

    monkeypatch.setattr(
        "dagster_rocky.resource._forward_stderr_to_context",
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


def test_run_streaming_threads_partition_flags():
    """run_streaming accepts the same partition kwargs as run() and
    threads them through to the subprocess command."""
    rocky = RockyResource()
    context = _captured_log_context()
    proc = _streaming_popen_mock(stdout=_run_json(), stderr_lines=[])

    captured_cmd: list[list[str]] = []

    def fake_popen(cmd, **kwargs):
        captured_cmd.append(cmd)
        return proc

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

    cmd = captured_cmd[0]
    assert "--partition" in cmd
    assert "2026-04-08" in cmd
    assert "--lookback" in cmd
    assert "2" in cmd
    assert "--parallel" in cmd
    assert "4" in cmd


def test_run_streaming_default_omits_partition_flags():
    """Plain run_streaming() emits no partition flags."""
    rocky = RockyResource()
    context = _captured_log_context()
    proc = _streaming_popen_mock(stdout=_run_json(), stderr_lines=[])

    captured_cmd: list[list[str]] = []

    def fake_popen(cmd, **kwargs):
        captured_cmd.append(cmd)
        return proc

    with (
        patch.object(RockyResource, "_verify_engine_version"),
        patch("dagster_rocky.resource.subprocess.Popen", side_effect=fake_popen),
    ):
        rocky.run_streaming(context, filter="tenant=acme")

    cmd = captured_cmd[0]
    for flag in ("--partition", "--from", "--to", "--latest", "--missing", "--lookback"):
        assert flag not in cmd


# ---------------------------------------------------------------------------
# run_pipes — full Dagster Pipes integration (T2)
# ---------------------------------------------------------------------------


def test_run_pipes_calls_pipes_client_with_built_command():
    """T2: run_pipes() forwards the rocky CLI command to a
    PipesSubprocessClient and returns the client's invocation result.
    The command must include the global flags + the partition argv
    that _build_run_args produced."""
    rocky = RockyResource(config_path="rocky.toml")
    context = MagicMock(spec=dg.AssetExecutionContext)
    fake_client = MagicMock(spec=dg.PipesSubprocessClient)
    fake_invocation = MagicMock(spec=dg.PipesClientCompletedInvocation)
    fake_client.run = MagicMock(return_value=fake_invocation)

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
    # Subcommand + partition flag is present
    assert "run" in cmd
    assert "--filter" in cmd
    assert "tenant=acme" in cmd
    assert "--partition" in cmd
    assert "2026-04-08" in cmd


def test_run_pipes_threads_all_partition_flags():
    """run_pipes() accepts the same partition kwargs as run() / run_streaming()
    and threads them through to the subprocess command."""
    rocky = RockyResource()
    context = MagicMock(spec=dg.AssetExecutionContext)
    fake_client = MagicMock(spec=dg.PipesSubprocessClient)
    fake_client.run = MagicMock(return_value=MagicMock())

    rocky.run_pipes(
        context,
        filter="tenant=acme",
        partition_from="2026-04-01",
        partition_to="2026-04-08",
        lookback=2,
        parallel=4,
        pipes_client=fake_client,
    )

    cmd = fake_client.run.call_args.kwargs["command"]
    assert "--from" in cmd
    assert "2026-04-01" in cmd
    assert "--to" in cmd
    assert "2026-04-08" in cmd
    assert "--lookback" in cmd
    assert "2" in cmd
    assert "--parallel" in cmd
    assert "4" in cmd


def test_run_pipes_constructs_default_client_when_none_passed():
    """When no pipes_client is supplied, run_pipes constructs a fresh
    PipesSubprocessClient with Dagster defaults."""
    rocky = RockyResource()
    context = MagicMock(spec=dg.AssetExecutionContext)

    fake_invocation = MagicMock()

    with patch(
        "dagster_rocky.resource.dg.PipesSubprocessClient",
    ) as mock_client_cls:
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
    """run_streaming threads shadow_suffix through to the CLI command."""
    rocky = RockyResource()
    context = _captured_log_context()
    proc = _streaming_popen_mock(stdout=_run_json(), stderr_lines=[])

    captured_cmd: list[list[str]] = []

    def fake_popen(cmd, **kwargs):
        captured_cmd.append(cmd)
        return proc

    with (
        patch.object(RockyResource, "_verify_engine_version"),
        patch("dagster_rocky.resource.subprocess.Popen", side_effect=fake_popen),
    ):
        rocky.run_streaming(
            context,
            filter="tenant=acme",
            shadow_suffix="_dagster_pr_99",
        )

    cmd = captured_cmd[0]
    assert "--shadow" in cmd
    shadow_idx = cmd.index("--shadow-suffix")
    assert cmd[shadow_idx + 1] == "_dagster_pr_99"


def test_run_pipes_with_shadow_suffix():
    """run_pipes threads shadow_suffix through to the CLI command."""
    rocky = RockyResource()
    context = MagicMock(spec=dg.AssetExecutionContext)
    fake_client = MagicMock(spec=dg.PipesSubprocessClient)
    fake_client.run = MagicMock(return_value=MagicMock())

    rocky.run_pipes(
        context,
        filter="tenant=acme",
        shadow_suffix="_dagster_pr_7",
        pipes_client=fake_client,
    )

    cmd = fake_client.run.call_args.kwargs["command"]
    assert "--shadow" in cmd
    shadow_idx = cmd.index("--shadow-suffix")
    assert cmd[shadow_idx + 1] == "_dagster_pr_7"


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
    """_run_rocky calls _verify_engine_version before executing the command."""
    rocky = RockyResource()

    # Two subprocess.run calls: first for --version, second for the actual command
    with _patched_run(return_value=_completed(stdout='{"ok":true}')) as run_mock:
        rocky._run_rocky(["discover"])

    # First call was the version check
    version_call = run_mock.call_args_list[0]
    assert "--version" in version_call.args[0]
    # Second call was the actual command
    actual_call = run_mock.call_args_list[1]
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
