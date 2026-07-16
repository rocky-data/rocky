"""Unit tests for RockyClient — argv construction, version gate, subprocess
plumbing, governance pre-flight, and HTTP fallback. All offline: the binary is
mocked at ``rocky_sdk.client.subprocess``.
"""

from __future__ import annotations

import io
import json
import signal
import threading
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from rocky_sdk import RockyClient
from rocky_sdk.exceptions import (
    RockyBinaryNotFoundError,
    RockyCommandError,
    RockyGovernanceError,
    RockyOutputParseError,
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


@contextmanager
def _hung_popen(*, stderr: str = "slow\n"):
    """A Popen double that blocks in ``wait()`` until the watchdog kills it.

    Patches ``kill_process_group`` (so no real signal is sent) to release the
    blocked ``wait()`` with the POSIX SIGKILL exit status — the shape a genuine
    watchdog kill produces.
    """
    proc = MagicMock()
    proc.pid = 4321
    proc.stdout = io.StringIO("")
    proc.stderr = io.StringIO(stderr)
    killed = threading.Event()

    def _wait() -> int:
        killed.wait(timeout=30)  # released by the (patched) watchdog kill
        proc.returncode = -signal.SIGKILL
        return proc.returncode

    proc.wait.side_effect = _wait
    with (
        patch("rocky_sdk.client.subprocess.Popen", return_value=proc),
        patch("rocky_sdk.client.kill_process_group", side_effect=lambda _p: killed.set()),
    ):
        yield proc


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


def test_compliance_threads_models_dir():
    client = _client(models_dir="custom-models")
    output = json.dumps(
        {
            "version": "1.64.0",
            "command": "compliance",
            "summary": {"total_classified": 0, "total_exceptions": 0, "total_masked": 0},
            "per_column": [],
            "exceptions": [],
        }
    )
    with patch.object(client, "run_cli", return_value=output) as run_cli:
        client.compliance(env="prod")
    run_cli.assert_called_once_with(
        ["compliance", "--output", "json", "--models", "custom-models", "--env", "prod"]
    )


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


def test_verify_version_accepts_two_component_version_at_floor():
    """A short "X.Y" version compares as "X.Y.0", not below it."""
    client = RockyClient(binary_path="rocky")
    completed = MagicMock(stdout="rocky 1.34\n")
    with (
        patch("rocky_sdk.client.shutil.which", return_value="/bin/rocky"),
        patch("rocky_sdk.client.subprocess.run", return_value=completed),
    ):
        client._verify_engine_version()  # no raise
    assert client._version_checked is True


def test_verify_version_enforces_despite_trailing_build_metadata():
    """ "rocky 1.2.0 (abc123)" must still be gated, not silently skipped."""
    client = RockyClient(binary_path="rocky")
    completed = MagicMock(stdout="rocky 1.2.0 (abc123)\n")
    with (
        patch("rocky_sdk.client.shutil.which", return_value="/bin/rocky"),
        patch("rocky_sdk.client.subprocess.run", return_value=completed),
        pytest.raises(RockyVersionError) as exc,
    ):
        client._verify_engine_version()
    assert exc.value.min_version == "1.34.0"


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


def test_run_cli_stderr_tail_is_bounded_to_last_20_lines():
    # A long verbose run must not retain the whole stderr stream — only the
    # last 20 lines are ever surfaced, so the reader's sink is capped there.
    client = _client()
    noise = "".join(f"line {i}\n" for i in range(1000))
    proc = _fake_popen(stdout="not json", stderr=noise, returncode=1)
    with (
        patch("rocky_sdk.client.subprocess.Popen", return_value=proc),
        pytest.raises(RockyCommandError) as exc,
    ):
        client.run_cli(["discover"])
    tail_lines = exc.value.stderr_tail.splitlines()
    assert len(tail_lines) == 20
    assert tail_lines[0] == "line 980"
    assert tail_lines[-1] == "line 999"


def test_run_cli_timeout_when_watchdog_kills():
    client = _client(timeout_seconds=0.05)
    with (
        patch("rocky_sdk.client.os.name", "posix"),
        _hung_popen(),
        pytest.raises(RockyTimeoutError) as exc,
    ):
        client.run_cli(["run"], allow_partial=True)
    assert exc.value.timeout_seconds == client.timeout_seconds


def test_run_cli_external_sigkill_is_not_a_timeout():
    """A SIGKILL the watchdog did not deliver (e.g. the OOM killer) is a
    command failure — reporting it as a timeout hides the real cause."""
    client = _client(timeout_seconds=3600)
    proc = _fake_popen(stdout="", stderr="Killed\n", returncode=-signal.SIGKILL)
    with (
        patch("rocky_sdk.client.os.name", "posix"),
        patch("rocky_sdk.client.subprocess.Popen", return_value=proc),
        pytest.raises(RockyCommandError) as exc,
    ):
        client.run_cli(["run"])
    assert exc.value.returncode == -signal.SIGKILL
    assert "Killed" in exc.value.stderr_tail


def test_run_cli_per_call_timeout_overrides_static():
    """A per-call ``timeout_seconds`` supersedes the construction-time budget."""
    client = _client(timeout_seconds=3600)
    with (
        patch("rocky_sdk.client.os.name", "posix"),
        _hung_popen(),
        pytest.raises(RockyTimeoutError) as exc,
    ):
        client.run_cli(["run"], allow_partial=True, timeout_seconds=0.05)
    # The error carries the effective (per-call) budget, not the static 3600.
    assert exc.value.timeout_seconds == 0.05


def test_run_cli_none_timeout_uses_static_budget():
    """``timeout_seconds=None`` leaves the construction-time budget in force."""
    client = _client(timeout_seconds=0.05)
    with (
        patch("rocky_sdk.client.os.name", "posix"),
        _hung_popen(stderr=""),
        pytest.raises(RockyTimeoutError) as exc,
    ):
        client.run_cli(["run"], allow_partial=True, timeout_seconds=None)
    assert exc.value.timeout_seconds == 0.05


@pytest.mark.parametrize("bad", [0, -1, -900])
def test_run_cli_rejects_nonpositive_timeout(bad):
    """A resolver bug returning a non-positive budget fails loudly, not silently."""
    client = _client()
    with (
        patch("rocky_sdk.client.subprocess.Popen") as popen,
        pytest.raises(ValueError, match="timeout_seconds must be a positive"),
    ):
        client.run_cli(["run"], timeout_seconds=bad)
    # Rejected before any subprocess is spawned.
    popen.assert_not_called()


def test_run_forwards_per_call_timeout_to_watchdog():
    """``run()`` threads its ``timeout_seconds`` down to the watchdog boundary."""
    client = _client(timeout_seconds=3600)
    with (
        patch("rocky_sdk.client.os.name", "posix"),
        _hung_popen(stderr=""),
        pytest.raises(RockyTimeoutError) as exc,
    ):
        client.run("client=cocacola", timeout_seconds=0.05)
    assert exc.value.timeout_seconds == 0.05


def test_run_cli_binary_not_found():
    client = _client()
    with (
        patch("rocky_sdk.client.subprocess.Popen", side_effect=FileNotFoundError),
        pytest.raises(RockyBinaryNotFoundError),
    ):
        client.run_cli(["discover"])


@pytest.mark.parametrize("exc", [PermissionError, NotADirectoryError])
def test_run_cli_non_executable_binary_maps_to_typed_error(exc):
    # RockyBinaryNotFoundError's contract is "could not be found *or executed*":
    # a non-executable file (PermissionError) or a bad path component
    # (NotADirectoryError) must surface as the typed error, not a raw OSError.
    client = _client()
    with (
        patch("rocky_sdk.client.subprocess.Popen", side_effect=exc),
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


def test_http_metrics_uses_default_endpoint():
    client = _client(server_url="http://localhost:8080")
    body = '{"version":"1","command":"metrics","model":"orders","snapshots":[],"count":0}'
    with patch.object(client, "_http_get", return_value=body) as http_get:
        result = client.metrics("orders")
    http_get.assert_called_once_with("/api/v1/models/orders/metrics")
    assert result.model == "orders"


@pytest.mark.parametrize("kwargs", [{"trend": True}, {"column": "email"}, {"alerts": True}])
def test_http_metrics_rejects_cli_only_options(kwargs):
    client = _client(server_url="http://localhost:8080")
    with (
        patch.object(client, "_http_get") as http_get,
        pytest.raises(ValueError, match="not supported when server_url is set"),
    ):
        client.metrics("orders", **kwargs)
    http_get.assert_not_called()


def test_http_compile_uses_default_endpoint():
    client = _client(server_url="http://localhost:8080")
    body = (
        '{"version":"1","command":"compile","models":0,'
        '"execution_layers":0,"diagnostics":[],"has_errors":false}'
    )
    with patch.object(client, "_http_get", return_value=body) as http_get:
        result = client.compile()
    http_get.assert_called_once_with("/api/v1/compile")
    assert result.command == "compile"


def test_http_compile_rejects_model_filter():
    client = _client(server_url="http://localhost:8080")
    with (
        patch.object(client, "_http_get") as http_get,
        pytest.raises(ValueError, match="not supported when server_url is set"),
    ):
        client.compile("orders")
    http_get.assert_not_called()


# --------------------------------------------------------------------------- #
# apply() — dispatch by real wire shape (there is NO wrapping envelope)
# --------------------------------------------------------------------------- #

# A run-shaped ``rocky apply`` output — what a run / replication / ai_authored /
# backfill plan prints. Note ``command == "run"``, NOT ``"apply"``, and NO
# ``{plan_id, plan_kind, success, result}`` envelope: the old ``ApplyOutput``
# shadow required exactly that envelope and so never validated a real payload.
_RUN_APPLY_JSON = json.dumps(
    {
        "version": "1.63.0",
        "command": "run",
        "status": "Success",
        "filter": "source=orders",
        "duration_ms": 42,
        "tables_copied": 1,
        "tables_failed": 0,
        "tables_skipped": 0,
        "materializations": [
            {
                "asset_key": ["db", "s", "orders"],
                "rows_copied": 10,
                "duration_ms": 5,
                "metadata": {"strategy": "full_refresh"},
                "started_at": "2026-01-01T00:00:00Z",
                "attempts": [{"attempt": 1, "outcome": "success", "duration_ms": 5}],
                "cost_usd": 0.0,
                "job_ids": [],
            }
        ],
        "check_results": [],
        "errors": [],
        "interrupted": False,
        "shadow": False,
        "permissions": {
            "grants_added": 0,
            "grants_revoked": 0,
            "catalogs_created": 0,
            "schemas_created": 0,
        },
        "drift": {"tables_checked": 1, "tables_drifted": 0, "actions_taken": []},
        "execution": {
            "concurrency": 1,
            "tables_processed": 1,
            "tables_failed": 0,
            "adaptive_concurrency": False,
        },
    }
)

# A ``gc`` plan's apply output — ``command == "apply"`` WITH gc markers.
_GC_APPLY_JSON = json.dumps(
    {
        "version": "1.63.0",
        "command": "apply",
        "plan_id": "a" * 64,
        "evicted": [
            {
                "model_name": "stale_model",
                "run_id": "run-1",
                "blake3_hash": "deadbeef",
                "size_bytes": 2048,
                "physical_reclaimed": True,
                "physical_status": "reclaimed",
                "tombstone_recorded": True,
            }
        ],
        "refused": [],
        "already_evicted": [],
        "bytes_evicted": 2048,
        "bytes_refused": 0,
        "evicted_count": 1,
        "refused_count": 0,
        "notes": ["physical reclamation is object-store-only"],
    }
)

# A ``restore`` plan's apply output — also ``command == "apply"``, but with
# restore-specific markers. The shared ``refused`` field must not route it to gc.
_RESTORE_APPLY_JSON = json.dumps(
    {
        "version": "1.64.0",
        "command": "apply",
        "plan_id": "b" * 64,
        "restored": [
            {
                "model_name": "orders",
                "run_id": "run-1",
                "blake3_hash": "deadbeef",
                "size_bytes": 2048,
                "file_path": "s3://warehouse/orders.parquet",
                "status": "restored",
                "bytes_written": True,
                "hash_verified": True,
            }
        ],
        "refused": [],
        "already_restored": [],
        "restored_count": 1,
        "refused_count": 0,
        "bytes_restored": 2048,
        "notes": ["rebuilt bytes verified before publication"],
    }
)


def _run_apply(client: RockyClient, stdout: str):
    proc = _fake_popen(stdout=stdout, returncode=0)
    with patch("rocky_sdk.client.subprocess.Popen", return_value=proc):
        return client.apply("a" * 64)


def test_apply_run_shaped_plan_returns_run_result():
    """A run / replication plan apply prints a bare ``RunOutput`` (command="run").
    ``apply()`` must parse it as a populated ``RunResult`` — the backfilled
    ``status`` / ``tables_skipped`` / per-materialization ``attempts`` survive."""
    from rocky_sdk.types import RunResult

    result = _run_apply(_client(), _RUN_APPLY_JSON)
    assert isinstance(result, RunResult)
    assert result.command == "run"
    assert result.status == "Success"
    assert result.tables_copied == 1
    assert result.materializations[0].attempts[0].attempt == 1
    assert result.materializations[0].started_at is not None


def test_apply_gc_plan_returns_gc_apply_output():
    """A ``gc`` plan apply prints ``command:"apply"`` with gc markers →
    ``GcApplyOutput`` (populated evicted list, not the empty happy path)."""
    from rocky_sdk.types import GcApplyOutput

    result = _run_apply(_client(), _GC_APPLY_JSON)
    assert isinstance(result, GcApplyOutput)
    assert result.evicted_count == 1
    assert result.evicted[0].model_name == "stale_model"
    assert result.bytes_evicted == 2048


def test_apply_restore_plan_returns_restore_apply_output():
    from rocky_sdk.types import RestoreApplyOutput

    result = _run_apply(_client(), _RESTORE_APPLY_JSON)
    assert isinstance(result, RestoreApplyOutput)
    assert result.restored_count == 1
    assert result.restored[0].model_name == "orders"
    assert result.bytes_restored == 2048


def test_apply_unknown_shape_raises_named_parse_error():
    """An unrecognized apply shape raises a clear parse error naming the
    received command instead of silently mis-parsing."""
    result_json = json.dumps({"version": "1", "command": "totally-new-apply-kind"})
    proc = _fake_popen(stdout=result_json, returncode=0)
    with (
        patch("rocky_sdk.client.subprocess.Popen", return_value=proc),
        pytest.raises(RockyOutputParseError) as excinfo,
    ):
        _client().apply("a" * 64)
    assert "totally-new-apply-kind" in excinfo.value.parse_error


# --------------------------------------------------------------------------- #
# test_adapter() — the ConformanceResult shadow raised on ANY real output
# --------------------------------------------------------------------------- #


def test_test_adapter_parses_real_conformance_payload():
    """``rocky test-adapter`` output carries NO ``version`` / ``command`` keys,
    so the old hand-written ``ConformanceResult`` (which required them) raised on
    every real payload. The generated ``TestAdapterOutput`` alias parses it —
    tested with a POPULATED results list, not the empty happy path."""
    payload = json.dumps(
        {
            "adapter": "duckdb",
            "sdk_version": "0.6.0",
            "tests_run": 2,
            "tests_passed": 1,
            "tests_failed": 1,
            "tests_skipped": 0,
            "results": [
                {
                    "name": "connection",
                    "category": "connection",
                    "status": "passed",
                    "duration_ms": 3,
                },
                {
                    "name": "types",
                    "category": "types",
                    "status": "failed",
                    "message": "unsupported type",
                    "duration_ms": 5,
                },
            ],
        }
    )
    proc = _fake_popen(stdout=payload, returncode=0)
    with patch("rocky_sdk.client.subprocess.Popen", return_value=proc):
        result = _client().test_adapter(adapter="duckdb")
    assert result.adapter == "duckdb"
    assert result.tests_failed == 1
    assert result.results[1].status == "failed"
    assert result.results[1].message == "unsupported type"


# --------------------------------------------------------------------------- #
# ai_sync() — the AiSyncProposal shadow required a phantom ``current_source``
# --------------------------------------------------------------------------- #


def test_ai_sync_parses_populated_proposals_without_phantom_field():
    """A real ``AiSyncProposal`` is ``{model, intent, diff, proposed_source}`` —
    no ``current_source``. The old shadow required ``current_source``, so
    ``ai_sync`` raised whenever the proposals list was non-empty (the empty-list
    happy path masked it). Tested with a POPULATED proposal."""
    payload = json.dumps(
        {
            "version": "1.63.0",
            "command": "ai_sync",
            "proposals": [
                {
                    "model": "revenue",
                    "intent": "add tax column",
                    "diff": "@@ +tax",
                    "proposed_source": "SELECT *, tax FROM raw",
                }
            ],
        }
    )
    proc = _fake_popen(stdout=payload, returncode=0)
    with patch("rocky_sdk.client.subprocess.Popen", return_value=proc):
        result = _client().ai_sync()
    assert len(result.proposals) == 1
    assert result.proposals[0].model == "revenue"
    assert result.proposals[0].proposed_source == "SELECT *, tax FROM raw"
