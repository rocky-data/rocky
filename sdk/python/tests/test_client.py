"""Unit tests for RockyClient — argv construction, version gate, subprocess
plumbing, governance pre-flight, and HTTP fallback. All offline: the binary is
mocked at ``rocky_sdk.client.subprocess``.
"""

from __future__ import annotations

import io
import signal
from unittest.mock import MagicMock, patch

import pytest

from rocky_sdk import RockyClient
from rocky_sdk.exceptions import (
    RockyBinaryNotFoundError,
    RockyCommandError,
    RockyGovernanceError,
    RockyPartialFailure,
    RockyServerError,
    RockyTimeoutError,
    RockyVersionError,
)

DISCOVER_JSON = '{"version": "1.0.0", "command": "discover", "sources": []}'


def _fake_popen(*, stdout: str = "", stderr: str = "", returncode: int = 0) -> MagicMock:
    """A Popen double whose stdout/stderr are line-iterable streams."""
    proc = MagicMock()
    proc.pid = 4321
    proc.stdout = io.StringIO(stdout)
    proc.stderr = io.StringIO(stderr)
    proc.returncode = returncode
    proc.wait.return_value = returncode
    return proc


def _client(**kwargs) -> RockyClient:
    client = RockyClient(binary_path="rocky", **kwargs)
    # Skip the lazy ``rocky --version`` round-trip for argv/subprocess tests.
    client._version_checked = True
    return client


# --------------------------------------------------------------------------- #
# argv construction
# --------------------------------------------------------------------------- #


def test_build_cmd_uses_state_path_by_default():
    client = _client(config_path="r.toml", state_path="s.redb")
    with patch("rocky_sdk.client.shutil.which", return_value="/bin/rocky"):
        cmd = client._build_cmd(["discover"])
    assert cmd == [
        "/bin/rocky",
        "--config",
        "r.toml",
        "--state-path",
        "s.redb",
        "--output",
        "json",
        "discover",
    ]


def test_build_cmd_uses_state_namespace_when_set():
    client = _client(state_namespace="tenant_a")
    with patch("rocky_sdk.client.shutil.which", return_value="/bin/rocky"):
        cmd = client._build_cmd(["state"])
    assert "--state-namespace" in cmd
    assert "tenant_a" in cmd
    assert "--state-path" not in cmd


def test_build_run_args_threads_every_flag():
    client = _client(models_dir="models")
    args = client._build_run_args(
        "tenant=acme",
        governance_override={"workspace_ids": ["w1"]},
        run_models=True,
        partition_from="2026-01-01",
        partition_to="2026-01-31",
        lookback=3,
        parallel=4,
        shadow_suffix="_pr42",
        idempotency_key="key-1",
        defer=True,
        defer_to="prod",
    )
    assert args[:3] == ["run", "--filter", "tenant=acme"]
    assert "--governance-override" in args
    assert "--models" in args and "--all" in args
    assert "--from" in args and "--to" in args
    assert args[args.index("--shadow") : args.index("--shadow") + 3] == [
        "--shadow",
        "--shadow-suffix",
        "_pr42",
    ]
    assert "--lookback" in args and "3" in args
    assert "--defer" in args and "--defer-to" in args


def test_build_plan_args_mirrors_run_with_plan_verb():
    client = _client()
    run = client._build_run_args("c=1", governance_override=None, latest=True)
    plan = client._build_plan_args("c=1", governance_override=None, latest=True)
    assert run[0] == "run"
    assert plan[0] == "plan"
    assert run[1:] == plan[1:]


# --------------------------------------------------------------------------- #
# version gate
# --------------------------------------------------------------------------- #


def test_verify_version_raises_on_old_binary():
    client = RockyClient(binary_path="rocky")
    completed = MagicMock(stdout="rocky 1.2.0\n")
    with (
        patch("rocky_sdk.client.shutil.which", return_value="/bin/rocky"),
        patch("rocky_sdk.client.subprocess.run", return_value=completed),
        pytest.raises(RockyVersionError) as exc,
    ):
        client._verify_engine_version()
    assert exc.value.detected_version == "1.2.0"
    assert exc.value.min_version == "1.34.0"


def test_verify_version_accepts_dev_suffix_at_or_above_floor():
    client = RockyClient(binary_path="rocky")
    completed = MagicMock(stdout="rocky 1.40.0-dev\n")
    with (
        patch("rocky_sdk.client.shutil.which", return_value="/bin/rocky"),
        patch("rocky_sdk.client.subprocess.run", return_value=completed),
    ):
        client._verify_engine_version()  # no raise
    assert client._version_checked is True


def test_verify_version_missing_binary():
    client = RockyClient(binary_path="rocky")
    with (
        patch("rocky_sdk.client.shutil.which", return_value="rocky"),
        patch("rocky_sdk.client.subprocess.run", side_effect=FileNotFoundError),
        pytest.raises(RockyBinaryNotFoundError),
    ):
        client._verify_engine_version()


# --------------------------------------------------------------------------- #
# subprocess plumbing (run_cli)
# --------------------------------------------------------------------------- #


def test_run_cli_returns_stdout_on_success():
    client = _client()
    proc = _fake_popen(stdout=DISCOVER_JSON, returncode=0)
    with patch("rocky_sdk.client.subprocess.Popen", return_value=proc):
        out = client.run_cli(["discover"])
    assert out == DISCOVER_JSON


def test_run_cli_partial_returned_when_allowed():
    client = _client()
    proc = _fake_popen(stdout='{"command": "run"}', returncode=2)
    with patch("rocky_sdk.client.subprocess.Popen", return_value=proc):
        out = client.run_cli(["run"], allow_partial=True)
    assert out == '{"command": "run"}'


def test_run_cli_partial_raises_when_not_allowed():
    client = _client()
    proc = _fake_popen(stdout='{"command": "run"}', stderr="boom\n", returncode=2)
    with (
        patch("rocky_sdk.client.subprocess.Popen", return_value=proc),
        pytest.raises(RockyPartialFailure) as exc,
    ):
        client.run_cli(["run"], allow_partial=False)
    assert exc.value.returncode == 2
    assert exc.value.stdout == '{"command": "run"}'
    assert "boom" in exc.value.stderr_tail


def test_run_cli_hard_failure_carries_stderr_tail():
    client = _client()
    proc = _fake_popen(stdout="not json", stderr="line1\nfatal: nope\n", returncode=1)
    with (
        patch("rocky_sdk.client.subprocess.Popen", return_value=proc),
        pytest.raises(RockyCommandError) as exc,
    ):
        client.run_cli(["discover"])
    assert exc.value.returncode == 1
    assert "fatal: nope" in exc.value.stderr_tail


def test_run_cli_timeout_when_watchdog_kills():
    client = _client()
    # POSIX SIGKILL marker is the canonical timeout signal.
    proc = _fake_popen(stdout="", stderr="slow\n", returncode=-signal.SIGKILL)
    with (
        patch("rocky_sdk.client.os.name", "posix"),
        patch("rocky_sdk.client.subprocess.Popen", return_value=proc),
        pytest.raises(RockyTimeoutError) as exc,
    ):
        client.run_cli(["run"], allow_partial=True)
    assert exc.value.timeout_seconds == client.timeout_seconds


def test_run_cli_binary_not_found():
    client = _client()
    with (
        patch("rocky_sdk.client.subprocess.Popen", side_effect=FileNotFoundError),
        pytest.raises(RockyBinaryNotFoundError),
    ):
        client.run_cli(["discover"])


def test_run_cli_log_callback_receives_stderr_lines():
    client = _client()
    proc = _fake_popen(stdout=DISCOVER_JSON, stderr="progress 1\nprogress 2\n", returncode=0)
    seen: list[str] = []
    with patch("rocky_sdk.client.subprocess.Popen", return_value=proc):
        client.run_cli(["discover"], log_callback=seen.append)
    assert seen == ["progress 1", "progress 2"]


# --------------------------------------------------------------------------- #
# typed method end-to-end (mocked binary)
# --------------------------------------------------------------------------- #


def test_discover_parses_into_model():
    client = _client()
    proc = _fake_popen(stdout=DISCOVER_JSON, returncode=0)
    with patch("rocky_sdk.client.subprocess.Popen", return_value=proc):
        result = client.discover()
    assert result.command == "discover"
    assert result.sources == []


# --------------------------------------------------------------------------- #
# governance pre-flight
# --------------------------------------------------------------------------- #


def test_governance_empty_workspace_ids_rejected():
    with pytest.raises(RockyGovernanceError):
        RockyClient.validate_governance_override({"workspace_ids": []})


def test_governance_empty_allowed_with_consent():
    RockyClient.validate_governance_override(
        {"workspace_ids": [], "allow_empty_workspace_ids": True}
    )


def test_governance_non_list_rejected():
    with pytest.raises(RockyGovernanceError):
        RockyClient.validate_governance_override({"workspace_ids": "w1"})


def test_governance_none_and_absent_are_noops():
    RockyClient.validate_governance_override(None)
    RockyClient.validate_governance_override({"grants": []})


def test_run_validates_governance_before_spawn():
    client = _client()
    # Popen must never be reached — governance fails first.
    with (
        patch("rocky_sdk.client.subprocess.Popen") as popen,
        pytest.raises(RockyGovernanceError),
    ):
        client.run("tenant=acme", governance_override={"workspace_ids": []})
    popen.assert_not_called()


# --------------------------------------------------------------------------- #
# HTTP fallback
# --------------------------------------------------------------------------- #


def test_http_fallback_used_for_compile_when_server_url_set():
    client = _client(server_url="http://localhost:8080")
    body = (
        '{"version": "1", "command": "compile", "models": 0, '
        '"execution_layers": 0, "diagnostics": [], "has_errors": false}'
    )
    resp = MagicMock()
    resp.read.return_value = body.encode()
    resp.__enter__.return_value = resp
    resp.__exit__.return_value = False
    with patch("rocky_sdk.client.urllib.request.urlopen", return_value=resp):
        result = client.compile()
    assert result.command == "compile"


def test_http_error_raises_server_error():
    import urllib.error

    client = _client(server_url="http://localhost:8080")
    with (
        patch(
            "rocky_sdk.client.urllib.request.urlopen",
            side_effect=urllib.error.URLError("refused"),
        ),
        pytest.raises(RockyServerError),
    ):
        client.compile()
