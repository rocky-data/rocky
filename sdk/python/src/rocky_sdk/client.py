"""RockyClient — typed Python client wrapping the Rocky CLI binary.

The client exposes one method per Rocky CLI command. Each method builds the
right argv, invokes the binary via subprocess (or hits the ``rocky serve`` HTTP
API when ``server_url`` is set), parses the JSON output, and returns a Pydantic
model from :mod:`rocky_sdk.types`. Failures surface as
:class:`rocky_sdk.exceptions.RockyError` subclasses.

``run`` accepts a ``log_callback`` so callers (notebooks, orchestrators, the
``dagster-rocky`` adapter) can route the engine's live stderr progress wherever
they want. The subprocess concurrency model — single reader per pipe plus an
external watchdog — lives in :mod:`rocky_sdk._subprocess`.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import signal
import subprocess
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, TypeVar

from pydantic import BaseModel, ValidationError

from rocky_sdk._subprocess import (
    accumulate_stdout,
    forward_stderr_to_sink,
    kill_process_group,
    redact_argv,
)
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
from rocky_sdk.types import (
    AiContractOutput,
    AiExplainResult,
    AiResult,
    AiSyncResult,
    AiTestResult,
    ApplyOutput,
    ApproveOutput,
    BranchPromoteOutput,
    CatalogOutput,
    CiResult,
    ColumnLineageResult,
    CompileResult,
    ComplianceOutput,
    ConformanceResult,
    CostOutput,
    DagResult,
    DiscoverResult,
    DoctorResult,
    HistoryResult,
    MetricsResult,
    ModelHistoryResult,
    ModelLineageResult,
    OptimizeResult,
    PlanResult,
    PromotePlan,
    RetentionStatusOutput,
    RunResult,
    StateResult,
    TestResult,
    ValidateMigrationResult,
)

if TYPE_CHECKING:
    from collections.abc import Iterable  # noqa: F401

# Default subprocess timeout for any single Rocky CLI invocation. One hour is
# generous enough for full pipeline runs but still bounds runaway processes.
DEFAULT_TIMEOUT_SECONDS = 3600

# HTTP read timeout for the optional ``rocky serve`` fallback used by
# compile/lineage/metrics. Kept short — these endpoints are read-only.
DEFAULT_HTTP_TIMEOUT_SECONDS = 30

# Minimum Rocky binary version this SDK release is compatible with. Checked
# lazily on the first CLI invocation; an older binary raises
# :class:`RockyVersionError`. The floor is shared with the dagster-rocky adapter
# (whose Pipes path content-addresses every plan, a 1.34+ guarantee).
MIN_ROCKY_VERSION = "1.34.0"

# Bytes of an inner payload surfaced on an error so the cause is visible without
# dumping potentially-MB blobs.
_JSON_ERROR_PREVIEW_BYTES = 500

_TModel = TypeVar("_TModel", bound=BaseModel)


def _parse_rocky_json(output: str, model_cls: type[_TModel], *, command: str) -> _TModel:
    """Parse Rocky CLI JSON into ``model_cls`` or raise :class:`RockyOutputParseError`.

    Pydantic raises ``ValidationError`` for both malformed JSON and schema drift;
    this wrapper distinguishes the two (``kind="json"`` vs ``"validation"``) and
    attaches the raw stdout so the caller can see what the binary actually wrote.
    """
    try:
        return model_cls.model_validate_json(output)
    except ValidationError as exc:
        raise RockyOutputParseError(
            command, stdout=output or "", parse_error=str(exc), kind="validation"
        ) from exc
    except json.JSONDecodeError as exc:
        # Defense in depth — pydantic wraps JSON errors in ValidationError in
        # modern versions, but this catches direct ``json.loads`` usage too.
        raise RockyOutputParseError(
            command, stdout=output or "", parse_error=str(exc), kind="json"
        ) from exc


def _parse_run_or_apply(output: str, *, command: str) -> RunResult:
    """Parse ``rocky run`` / ``rocky apply`` JSON into a :class:`RunResult`.

    ``rocky run`` stdout is the bare ``RunResult``. ``rocky apply`` stdout is the
    ``{plan_id, plan_kind, success, result}`` envelope; the inner ``result`` is
    the ``RunResult`` payload, unwrapped here. A ``success: false`` envelope
    raises :class:`RockyOutputParseError` (``kind="envelope"``) so a failed apply
    never reads as a successful run.
    """
    try:
        payload = json.loads(output)
    except json.JSONDecodeError as exc:
        raise RockyOutputParseError(
            command, stdout=output or "", parse_error=str(exc), kind="json"
        ) from exc

    if isinstance(payload, dict) and payload.get("command") == "apply":
        inner = payload.get("result")
        if not isinstance(inner, dict):
            raise RockyOutputParseError(
                command,
                stdout=output or "",
                parse_error="apply envelope missing inner result (expected dict under `result`)",
                kind="envelope",
                plan_id=str(payload.get("plan_id", "")),
                plan_kind=str(payload.get("plan_kind", "")),
            )
        if payload.get("success") is False:
            inner_preview = json.dumps(inner)[:_JSON_ERROR_PREVIEW_BYTES]
            raise RockyOutputParseError(
                command,
                stdout=output or "",
                parse_error="rocky reported success=false on the apply envelope",
                kind="envelope",
                plan_id=str(payload.get("plan_id", "")),
                plan_kind=str(payload.get("plan_kind", "")),
                inner_result_preview=inner_preview,
            )
        return _parse_rocky_json(json.dumps(inner), RunResult, command=command)

    return _parse_rocky_json(output, RunResult, command=command)


def _validate_governance_override(override: dict | None) -> None:
    """Guard against the silent full-revoke footgun before spawning the binary.

    Mirrors the engine-side check so callers see the error in-process rather than
    paying a subprocess round-trip. Semantics match the engine exactly:

    * ``None`` / omitted → no-op.
    * ``workspace_ids`` absent → no-op (binding reconciliation skipped).
    * ``workspace_ids`` not a list → error.
    * ``workspace_ids = []`` without ``allow_empty_workspace_ids`` → error
      (would revoke every existing workspace binding on the target catalog).
    * ``workspace_ids = []`` with ``allow_empty_workspace_ids = True`` → no-op.
    * non-empty list → no-op.
    """
    if override is None:
        return
    if not isinstance(override, dict):
        raise RockyGovernanceError(
            f"governance_override must be a dict (or None), got {type(override).__name__}."
        )
    if "workspace_ids" not in override:
        return
    ws_ids = override["workspace_ids"]
    if not isinstance(ws_ids, list):
        raise RockyGovernanceError(
            f"governance_override.workspace_ids must be a list, got {type(ws_ids).__name__}."
        )
    if not ws_ids and not override.get("allow_empty_workspace_ids"):
        raise RockyGovernanceError(
            "governance_override.workspace_ids is empty. Rocky's binding reconciler "
            "would revoke every workspace binding on the target catalog. Pass "
            "allow_empty_workspace_ids=True if that's intentional, or omit the "
            "workspace_ids key to skip binding reconciliation."
        )


class RockyClient:
    """Typed client over the ``rocky`` CLI binary.

    Args:
        config_path: Path to ``rocky.toml``.
        binary_path: Path to the ``rocky`` binary (default ``"rocky"`` on PATH).
        state_path: Path to the state store file. Mutually exclusive with
            ``state_namespace``.
        state_namespace: Optional per-namespace state key (engine
            ``--state-namespace``). When set, ``--state-path`` is omitted.
        models_dir: Directory containing model files (compile/lineage/test/ci).
        contracts_dir: Optional directory containing contract files.
        server_url: Optional ``rocky serve`` URL. When set, ``compile``,
            ``lineage`` and ``metrics`` use the HTTP API instead of a subprocess.
        timeout_seconds: Wall-clock timeout for any one CLI invocation.
        logger: Logger for subprocess lifecycle + default stderr forwarding.
            Defaults to the ``rocky_sdk`` logger.
        mirror_stderr: When ``True``, also write each rocky stderr line to this
            process's ``sys.stderr`` (so an outer capture that only sees the real
            fds — e.g. Dagster's compute-log capture — preserves rocky tracing).
            Default ``False`` for clean library output.
    """

    def __init__(
        self,
        *,
        config_path: str = "rocky.toml",
        binary_path: str = "rocky",
        state_path: str = ".rocky-state.redb",
        state_namespace: str | None = None,
        models_dir: str = "models",
        contracts_dir: str | None = None,
        server_url: str | None = None,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
        logger: logging.Logger | None = None,
        mirror_stderr: bool = False,
    ) -> None:
        self.config_path = config_path
        self.binary_path = binary_path
        self.state_path = state_path
        self.state_namespace = state_namespace
        self.models_dir = models_dir
        self.contracts_dir = contracts_dir
        self.server_url = server_url
        self.timeout_seconds = timeout_seconds
        self._logger = logger or logging.getLogger("rocky_sdk")
        self._mirror_stderr = mirror_stderr
        self._resolved_binary: str | None = None
        self._version_checked = False

    # ------------------------------------------------------------------ #
    # Binary resolution + version gate                                   #
    # ------------------------------------------------------------------ #

    def _resolve_binary(self) -> str:
        """Resolve and memoize the path to the rocky binary.

        ``shutil.which`` walks ``$PATH`` once; the result is cached. When
        ``which`` returns ``None`` (bare name missing from ``$PATH``, or an
        absolute path that does not exist) the configured ``binary_path`` is
        returned unchanged so the downstream subprocess error still fires.
        """
        if self._resolved_binary is not None:
            return self._resolved_binary
        binary = shutil.which(self.binary_path) or self.binary_path
        self._resolved_binary = binary
        return binary

    def _verify_engine_version(self) -> None:
        """Check the binary meets :data:`MIN_ROCKY_VERSION` (lazy, cached).

        Raises:
            RockyBinaryNotFoundError: The binary is missing.
            RockyVersionError: The binary is older than the minimum.
        """
        if self._version_checked:
            return

        binary = self._resolve_binary()
        try:
            result = subprocess.run(
                [binary, "--version"],
                capture_output=True,
                text=True,
                check=False,
                timeout=10,
            )
        except FileNotFoundError:
            raise RockyBinaryNotFoundError(self.binary_path) from None
        except (subprocess.TimeoutExpired, OSError):
            # Best-effort — don't block the actual command on a slow/odd env.
            self._version_checked = True
            return

        # ``rocky --version`` outputs "rocky X.Y.Z" or just "X.Y.Z".
        version_str = result.stdout.strip().removeprefix("rocky ").strip()
        if not version_str:
            self._version_checked = True
            return

        # Strip pre-release / build suffix so dev builds (``1.17.4-dev``,
        # ``1.17.4+sha.abc``) gate against the matching release.
        core_version = version_str.split("-", 1)[0].split("+", 1)[0]
        try:
            detected = tuple(int(p) for p in core_version.split(".")[:3])
            required = tuple(int(p) for p in MIN_ROCKY_VERSION.split(".")[:3])
        except ValueError:
            self._version_checked = True
            return

        if detected < required:
            raise RockyVersionError(version_str, MIN_ROCKY_VERSION, binary)

        self._version_checked = True

    # ------------------------------------------------------------------ #
    # argv construction                                                  #
    # ------------------------------------------------------------------ #

    def _build_cmd(self, args: list[str]) -> list[str]:
        """Prefix ``args`` with the binary + global flags (--config/--state/--output)."""
        binary = self._resolve_binary()
        cmd = [binary, "--config", self.config_path]
        # ``--state-namespace`` and ``--state-path`` are mutually exclusive: the
        # engine disables namespacing when an explicit ``--state-path`` is
        # present, so send exactly one.
        if self.state_namespace is not None:
            cmd += ["--state-namespace", self.state_namespace]
        else:
            cmd += ["--state-path", self.state_path]
        cmd += ["--output", "json", *args]
        return cmd

    def _build_run_args(
        self,
        filter: str,
        *,
        governance_override: dict | None,
        pipeline: str | None = None,
        run_models: bool = False,
        partition: str | None = None,
        partition_from: str | None = None,
        partition_to: str | None = None,
        latest: bool = False,
        missing: bool = False,
        lookback: int | None = None,
        parallel: int | None = None,
        shadow_suffix: str | None = None,
        idempotency_key: str | None = None,
        defer: bool = False,
        defer_to: str | None = None,
    ) -> list[str]:
        """Build the argv for a fused ``rocky run`` invocation.

        Single source of truth for the run-path flag plumbing — adding a flag is
        a one-place change. Defensive: ``partition_from`` without ``partition_to``
        emits neither (the engine requires both for range mode).
        """
        return self._build_run_or_plan_args(
            "run",
            filter,
            governance_override=governance_override,
            pipeline=pipeline,
            run_models=run_models,
            partition=partition,
            partition_from=partition_from,
            partition_to=partition_to,
            latest=latest,
            missing=missing,
            lookback=lookback,
            parallel=parallel,
            shadow_suffix=shadow_suffix,
            idempotency_key=idempotency_key,
            defer=defer,
            defer_to=defer_to,
        )

    def _build_plan_args(
        self,
        filter: str,
        *,
        governance_override: dict | None,
        pipeline: str | None = None,
        run_models: bool = False,
        partition: str | None = None,
        partition_from: str | None = None,
        partition_to: str | None = None,
        latest: bool = False,
        missing: bool = False,
        lookback: int | None = None,
        parallel: int | None = None,
        shadow_suffix: str | None = None,
        idempotency_key: str | None = None,
        defer: bool = False,
        defer_to: str | None = None,
    ) -> list[str]:
        """Build the argv for ``rocky plan`` — identical flag plumbing to ``run``.

        Used by the two-step plan + apply path (e.g. dagster Pipes) that needs the
        persisted ``plan_id``. ``--watch`` is intentionally omitted (no run-path
        counterpart; orchestrators own the re-run cadence).
        """
        return self._build_run_or_plan_args(
            "plan",
            filter,
            governance_override=governance_override,
            pipeline=pipeline,
            run_models=run_models,
            partition=partition,
            partition_from=partition_from,
            partition_to=partition_to,
            latest=latest,
            missing=missing,
            lookback=lookback,
            parallel=parallel,
            shadow_suffix=shadow_suffix,
            idempotency_key=idempotency_key,
            defer=defer,
            defer_to=defer_to,
        )

    def _build_run_or_plan_args(
        self,
        verb: str,
        filter: str,
        *,
        governance_override: dict | None,
        pipeline: str | None,
        run_models: bool,
        partition: str | None,
        partition_from: str | None,
        partition_to: str | None,
        latest: bool,
        missing: bool,
        lookback: int | None,
        parallel: int | None,
        shadow_suffix: str | None,
        idempotency_key: str | None,
        defer: bool,
        defer_to: str | None,
    ) -> list[str]:
        """Shared flag plumbing for ``run`` and ``plan`` (the engine backfilled
        every ``run`` flag onto ``plan`` so the only difference is the verb)."""
        args = [verb, "--filter", filter]
        if pipeline is not None:
            args.extend(["--pipeline", pipeline])
        if governance_override:
            args.extend(["--governance-override", json.dumps(governance_override)])
        if run_models:
            args.extend(["--models", self.models_dir, "--all"])
        if shadow_suffix is not None:
            args.extend(["--shadow", "--shadow-suffix", shadow_suffix])
        if partition is not None:
            args.extend(["--partition", partition])
        if partition_from is not None and partition_to is not None:
            args.extend(["--from", partition_from, "--to", partition_to])
        if latest:
            args.append("--latest")
        if missing:
            args.append("--missing")
        if lookback is not None:
            args.extend(["--lookback", str(lookback)])
        if parallel is not None:
            args.extend(["--parallel", str(parallel)])
        if idempotency_key is not None:
            args.extend(["--idempotency-key", idempotency_key])
        if defer:
            args.append("--defer")
        if defer_to is not None:
            args.extend(["--defer-to", defer_to])
        return args

    # ------------------------------------------------------------------ #
    # Subprocess + HTTP plumbing                                         #
    # ------------------------------------------------------------------ #

    def run_cli(
        self,
        args: list[str],
        *,
        allow_partial: bool = False,
        log_callback: Callable[[str], None] | None = None,
    ) -> str:
        """Execute the Rocky CLI and return stdout.

        Spawns the binary via ``subprocess.Popen`` with separated stdout/stderr,
        starts dedicated reader threads on each pipe, and relies on an external
        watchdog thread (not ``communicate(timeout=)``) to enforce the wall-clock
        timeout. Each non-empty stderr line is appended to a failure-tail buffer
        and handed to ``log_callback`` for live routing.

        Args:
            args: CLI arguments after the global flags. ``--config``,
                ``--state-path`` / ``--state-namespace`` and ``--output json``
                are inserted automatically by :meth:`_build_cmd`.
            allow_partial: Return stdout on a non-zero exit when stdout starts
                with valid JSON (Rocky's partial-success semantics).
            log_callback: Per-stderr-line sink. Defaults to the client's logger
                (``"rocky: <line>"`` at INFO).

        Raises:
            RockyBinaryNotFoundError: The binary is missing.
            RockyTimeoutError: The subprocess exceeded ``timeout_seconds``.
            RockyPartialFailure: Non-zero exit with parseable JSON and
                ``allow_partial=False``.
            RockyCommandError: Any other non-zero exit.
        """
        self._verify_engine_version()
        cmd = self._build_cmd(args)
        sink = log_callback or (lambda line: self._logger.info("rocky: %s", line))

        # Inherit the parent environment and force-suppress engine deprecation
        # notices (operator-overridable via setdefault).
        rocky_env = os.environ.copy()
        rocky_env.setdefault("ROCKY_SUPPRESS_DEPRECATION", "1")
        popen_kwargs: dict[str, object] = {
            "stdout": subprocess.PIPE,
            "stderr": subprocess.PIPE,
            "text": True,
            "bufsize": 1,  # line-buffered so readers see lines as they're written
            "env": rocky_env,
        }
        # POSIX-only process-group isolation so the watchdog's os.killpg reaches
        # any children rocky spawns. Windows has no equivalent; proc.kill()
        # handles the single-process case.
        if os.name != "nt":
            popen_kwargs["start_new_session"] = True

        t0 = time.monotonic()
        try:
            proc = subprocess.Popen(cmd, **popen_kwargs)  # noqa: S603 - typed argv
        except FileNotFoundError:
            raise RockyBinaryNotFoundError(self.binary_path) from None

        self._logger.info(
            "rocky subprocess started: pid=%s timeout_s=%s cmd=%s",
            proc.pid,
            self.timeout_seconds,
            " ".join(redact_argv(cmd)),
        )

        # Sole-reader threads must start before proc.wait() so they drain the
        # pipe buffers concurrently with rocky writing (else a large write
        # blocks rocky and we deadlock).
        stderr_lines: list[str] = []
        stdout_chunks: list[str] = []
        stderr_reader = threading.Thread(
            target=forward_stderr_to_sink,
            args=(proc.stderr, sink, stderr_lines),
            kwargs={"mirror_to_stderr": self._mirror_stderr},
            daemon=True,
            name="rocky-stderr-forwarder",
        )
        stdout_reader = threading.Thread(
            target=accumulate_stdout,
            args=(proc.stdout, stdout_chunks),
            daemon=True,
            name="rocky-stdout-accumulator",
        )
        stderr_reader.start()
        stdout_reader.start()

        # Watchdog: hard-kill the process group if not dismissed within timeout.
        fired = threading.Event()

        def _watchdog() -> None:
            if not fired.wait(self.timeout_seconds):
                kill_process_group(proc)
                fired.set()

        watchdog = threading.Thread(target=_watchdog, daemon=True, name="rocky-watchdog")
        watchdog.start()

        try:
            # No timeout on wait(): the watchdog enforces it by killing the
            # process externally. communicate(timeout=) raced with the stderr
            # reader on the same pipe FD and failed to fire.
            proc.wait()
        finally:
            # Dismiss the watchdog first, then drain the readers (the pipes are
            # closed on the subprocess side, so the readers hit EOF and exit).
            fired.set()
            watchdog.join(timeout=1.0)
            stderr_reader.join(timeout=2.0)
            stdout_reader.join(timeout=2.0)

        duration_ms = int((time.monotonic() - t0) * 1000)
        stdout = "".join(stdout_chunks)
        stderr_tail = "\n".join(stderr_lines[-20:])

        # On POSIX a SIGKILL'd process has returncode == -SIGKILL — the canonical
        # timeout marker. On Windows proc.kill() leaves returncode == 1.
        killed_by_watchdog = os.name != "nt" and proc.returncode == -signal.SIGKILL

        if killed_by_watchdog:
            outcome = "timeout-killed"
        elif proc.returncode == 0:
            outcome = "success"
        elif allow_partial and stdout.lstrip().startswith("{"):
            outcome = "partial-success"
        else:
            outcome = "failure"

        self._logger.info(
            "rocky subprocess ended: pid=%s returncode=%s duration_ms=%d outcome=%s",
            proc.pid,
            proc.returncode,
            duration_ms,
            outcome,
        )

        if killed_by_watchdog:
            raise RockyTimeoutError(
                self.timeout_seconds,
                stderr_tail=stderr_tail,
                duration_ms=duration_ms,
                pid=proc.pid,
            )

        if proc.returncode == 0:
            return stdout

        if stdout.lstrip().startswith("{"):
            if allow_partial:
                return stdout
            raise RockyPartialFailure(
                proc.returncode,
                stdout=stdout,
                stderr_tail=stderr_tail,
                duration_ms=duration_ms,
            )

        raise RockyCommandError(proc.returncode, stderr_tail=stderr_tail, duration_ms=duration_ms)

    def _http_get(self, path: str) -> str:
        """GET a JSON document from the ``rocky serve`` HTTP API."""
        if self.server_url is None:
            raise RockyServerError("", error="HTTP fallback called but server_url is not set")
        url = f"{self.server_url.rstrip('/')}{path}"
        try:
            with urllib.request.urlopen(  # noqa: S310 - URL from validated config
                url, timeout=DEFAULT_HTTP_TIMEOUT_SECONDS
            ) as resp:
                return resp.read().decode("utf-8")
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            raise RockyServerError(url, error=str(exc)) from None

    # ------------------------------------------------------------------ #
    # Governance pre-flight (exposed for callers that build their own argv)
    # ------------------------------------------------------------------ #

    @staticmethod
    def validate_governance_override(override: dict | None) -> None:
        """Raise :class:`RockyGovernanceError` if ``override`` would full-revoke.

        See :func:`_validate_governance_override` for the exact semantics.
        """
        _validate_governance_override(override)

    # ------------------------------------------------------------------ #
    # Discovery & execution                                              #
    # ------------------------------------------------------------------ #

    def discover(
        self,
        *,
        pipeline: str | None = None,
        emit_fivetran_state_to: str | Path | None = None,
    ) -> DiscoverResult:
        """Run ``rocky discover`` and return the parsed result.

        Args:
            pipeline: Pipeline name (required when multiple pipelines are defined).
            emit_fivetran_state_to: Optional path to write the canonical Fivetran
                state envelope to as a side effect (atomic + idempotent). The
                envelope is delivered only to the file, not in the return value.
        """
        args = ["discover"]
        if pipeline is not None:
            args.extend(["--pipeline", pipeline])
        if emit_fivetran_state_to is not None:
            args.extend(["--emit-fivetran-state-to", str(emit_fivetran_state_to)])
        return _parse_rocky_json(self.run_cli(args), DiscoverResult, command="discover")

    def plan(
        self,
        filter: str | None = None,
        *,
        pipeline: str | None = None,
        env: str | None = None,
    ) -> PlanResult:
        """Run ``rocky plan`` and return the parsed plan.

        Every project shape content-addresses a plan and persists it to
        ``.rocky/plans/<plan_id>.json``. Pass the returned ``plan_id`` to
        :meth:`apply` to execute it.
        """
        args = ["plan"]
        if filter is not None:
            args.extend(["--filter", filter])
        if pipeline is not None:
            args.extend(["--pipeline", pipeline])
        if env is not None:
            args.extend(["--env", env])
        return _parse_rocky_json(self.run_cli(args), PlanResult, command="plan")

    def apply(self, plan_id: str) -> ApplyOutput:
        """Run ``rocky apply <plan-id>`` and return the parsed envelope.

        Reads ``.rocky/plans/<plan_id>.json`` and dispatches by kind (run /
        replication / compact / archive / promote).
        """
        return _parse_rocky_json(
            self.run_cli(["apply", plan_id], allow_partial=True),
            ApplyOutput,
            command="apply",
        )

    def run(
        self,
        filter: str,
        governance_override: dict | None = None,
        *,
        pipeline: str | None = None,
        run_models: bool = False,
        partition: str | None = None,
        partition_from: str | None = None,
        partition_to: str | None = None,
        latest: bool = False,
        missing: bool = False,
        lookback: int | None = None,
        parallel: int | None = None,
        shadow_suffix: str | None = None,
        idempotency_key: str | None = None,
        defer: bool = False,
        defer_to: str | None = None,
        log_callback: Callable[[str], None] | None = None,
    ) -> RunResult:
        """Run ``rocky run --filter <key=value>`` and return the parsed result.

        Spawns a single fused plan+apply ``rocky run``. Partial success (some
        tables fail, others succeed) returns the parsed result so callers can act
        on the tables that did materialize.

        Args:
            filter: Component filter (e.g. ``"client=acme"``).
            governance_override: Optional per-run governance config (workspace_ids,
                grants), merged additively with ``rocky.toml`` defaults. Validated
                in-process before the spawn.
            run_models: Also execute compiled models (``--models`` + ``--all``).
            partition / partition_from / partition_to / latest / missing /
                lookback / parallel: Partition selection (see the CLI docs).
            shadow_suffix: Enable shadow mode with this table-name suffix.
            idempotency_key: Opaque dedup token. ⚠️ stored verbatim — no secrets.
            defer / defer_to: Resolve unbuilt ``ref()`` upstreams against an
                existing schema instead of rebuilding.
            log_callback: Per-stderr-line sink for live progress (e.g. ``print``).
        """
        _validate_governance_override(governance_override)
        run_args = self._build_run_args(
            filter,
            governance_override=governance_override,
            pipeline=pipeline,
            run_models=run_models,
            partition=partition,
            partition_from=partition_from,
            partition_to=partition_to,
            latest=latest,
            missing=missing,
            lookback=lookback,
            parallel=parallel,
            shadow_suffix=shadow_suffix,
            idempotency_key=idempotency_key,
            defer=defer,
            defer_to=defer_to,
        )
        stdout = self.run_cli(run_args, allow_partial=True, log_callback=log_callback)
        return _parse_run_or_apply(stdout, command="run")

    def run_model(
        self,
        model_name: str,
        *,
        filter: str | None = None,
        partition: str | None = None,
        partition_from: str | None = None,
        partition_to: str | None = None,
        latest: bool = False,
        missing: bool = False,
        lookback: int | None = None,
        parallel: int | None = None,
    ) -> RunResult:
        """Run ``rocky run --model <name>`` for a single compiled model.

        ``--model`` skips the replication phase and executes only the named model.
        """
        args = ["run", "--model", model_name, "--models", self.models_dir]
        if filter is not None:
            args.extend(["--filter", filter])
        if partition is not None:
            args.extend(["--partition", partition])
        if partition_from is not None and partition_to is not None:
            args.extend(["--from", partition_from, "--to", partition_to])
        if latest:
            args.append("--latest")
        if missing:
            args.append("--missing")
        if lookback is not None:
            args.extend(["--lookback", str(lookback)])
        if parallel is not None:
            args.extend(["--parallel", str(parallel)])
        return _parse_rocky_json(self.run_cli(args, allow_partial=True), RunResult, command="run")

    def resume_run(
        self,
        run_id: str | None = None,
        *,
        filter: str = "",
        governance_override: dict | None = None,
    ) -> RunResult:
        """Resume a failed run from where it left off (latest when ``run_id`` is None)."""
        _validate_governance_override(governance_override)
        args: list[str] = ["run"]
        if run_id is not None:
            args.extend(["--resume", run_id])
        else:
            args.append("--resume-latest")
        if filter:
            args.extend(["--filter", filter])
        if governance_override:
            args.extend(["--governance-override", json.dumps(governance_override)])
        return _parse_rocky_json(self.run_cli(args, allow_partial=True), RunResult, command="run")

    def state(self) -> StateResult:
        """Run ``rocky state`` and return the parsed watermarks/checkpoints."""
        return _parse_rocky_json(self.run_cli(["state"]), StateResult, command="state")

    # ------------------------------------------------------------------ #
    # Branch approval / promote                                          #
    # ------------------------------------------------------------------ #

    def branch_approve(
        self, name: str, *, message: str | None = None, out: str | None = None
    ) -> ApproveOutput:
        """Run ``rocky branch approve <name>`` and return the signed artifact."""
        args = ["branch", "approve", name]
        if message is not None:
            args.extend(["--message", message])
        if out is not None:
            args.extend(["--out", out])
        return _parse_rocky_json(self.run_cli(args), ApproveOutput, command="branch approve")

    def branch_promote(
        self, name: str, *, filter: str | None = None, skip_approval: bool = False
    ) -> BranchPromoteOutput:
        """Run ``rocky branch promote <name>`` and return the parsed result."""
        args = ["branch", "promote", name]
        if filter is not None:
            args.extend(["--filter", filter])
        if skip_approval:
            args.append("--skip-approval")
        return _parse_rocky_json(self.run_cli(args), BranchPromoteOutput, command="branch promote")

    def plan_promote(
        self,
        name: str,
        *,
        base: str = "main",
        allow_breaking: bool = False,
        filter: str | None = None,
    ) -> PromotePlan:
        """Run ``rocky plan promote <name>`` — gates at plan time, persists a plan."""
        args = ["plan", "promote", name, "--base", base]
        if allow_breaking:
            args.append("--allow-breaking")
        if filter is not None:
            args.extend(["--filter", filter])
        return _parse_rocky_json(self.run_cli(args), PromotePlan, command="plan promote")

    # ------------------------------------------------------------------ #
    # Compiler / analysis (HTTP when server_url is set, CLI otherwise)   #
    # ------------------------------------------------------------------ #

    def compile(self, model_filter: str | None = None) -> CompileResult:
        """Run ``rocky compile`` (or the HTTP API when ``server_url`` is set)."""
        if self.server_url is not None:
            return _parse_rocky_json(
                self._http_get("/api/v1/compile"), CompileResult, command="compile"
            )
        args = ["compile", "--models", self.models_dir]
        if self.contracts_dir is not None:
            args.extend(["--contracts", self.contracts_dir])
        if model_filter is not None:
            args.extend(["--model", model_filter])
        return _parse_rocky_json(
            self.run_cli(args, allow_partial=True), CompileResult, command="compile"
        )

    def lineage(
        self, target: str, column: str | None = None
    ) -> ModelLineageResult | ColumnLineageResult:
        """Run ``rocky lineage`` (or the HTTP API when ``server_url`` is set)."""
        if self.server_url is not None:
            if column is not None:
                output = self._http_get(f"/api/v1/models/{target}/lineage/{column}")
                return _parse_rocky_json(output, ColumnLineageResult, command="lineage")
            output = self._http_get(f"/api/v1/models/{target}/lineage")
            return _parse_rocky_json(output, ModelLineageResult, command="lineage")

        args = ["lineage", "--models", self.models_dir, target]
        if column is not None:
            args.extend(["--column", column])
        output = self.run_cli(args)
        if column is not None:
            return _parse_rocky_json(output, ColumnLineageResult, command="lineage")
        return _parse_rocky_json(output, ModelLineageResult, command="lineage")

    def catalog(self, *, out: str | None = None) -> CatalogOutput:
        """Run ``rocky catalog`` and return the project-wide lineage snapshot."""
        args = ["catalog", "--models", self.models_dir]
        if out is not None:
            args.extend(["--out", out])
        return _parse_rocky_json(self.run_cli(args), CatalogOutput, command="catalog")

    def dag(self, *, column_lineage: bool = False) -> DagResult:
        """Run ``rocky dag`` and return the full unified DAG."""
        args = ["dag", "--models", self.models_dir]
        if self.contracts_dir is not None:
            args.extend(["--contracts", self.contracts_dir])
        if column_lineage:
            args.append("--column-lineage")
        return _parse_rocky_json(self.run_cli(args), DagResult, command="dag")

    # ------------------------------------------------------------------ #
    # Local testing                                                      #
    # ------------------------------------------------------------------ #

    def test(self, model_filter: str | None = None) -> TestResult:
        """Run ``rocky test`` (local DuckDB execution, no warehouse creds)."""
        args = ["test", "--models", self.models_dir]
        if self.contracts_dir is not None:
            args.extend(["--contracts", self.contracts_dir])
        if model_filter is not None:
            args.extend(["--model", model_filter])
        return _parse_rocky_json(self.run_cli(args, allow_partial=True), TestResult, command="test")

    def ci(self) -> CiResult:
        """Run ``rocky ci`` (compile + test) and return the parsed result."""
        args = ["ci", "--models", self.models_dir]
        if self.contracts_dir is not None:
            args.extend(["--contracts", self.contracts_dir])
        return _parse_rocky_json(self.run_cli(args, allow_partial=True), CiResult, command="ci")

    # ------------------------------------------------------------------ #
    # Observability (HTTP when server_url is set, CLI otherwise)         #
    # ------------------------------------------------------------------ #

    def history(
        self, model: str | None = None, since: str | None = None
    ) -> HistoryResult | ModelHistoryResult:
        """Run ``rocky history`` and return the parsed run history."""
        args: list[str] = ["history"]
        if model is not None:
            args.extend(["--model", model])
        if since is not None:
            args.extend(["--since", since])
        output = self.run_cli(args)
        if model is not None:
            return _parse_rocky_json(output, ModelHistoryResult, command="history")
        return _parse_rocky_json(output, HistoryResult, command="history")

    def metrics(
        self,
        model: str,
        *,
        trend: bool = False,
        column: str | None = None,
        alerts: bool = False,
    ) -> MetricsResult:
        """Run ``rocky metrics`` (or the HTTP API when ``server_url`` is set)."""
        if self.server_url is not None:
            return _parse_rocky_json(
                self._http_get(f"/api/v1/models/{model}/metrics"),
                MetricsResult,
                command="metrics",
            )
        args = ["metrics", model]
        if trend:
            args.append("--trend")
        if column is not None:
            args.extend(["--column", column])
        if alerts:
            args.append("--alerts")
        return _parse_rocky_json(self.run_cli(args), MetricsResult, command="metrics")

    def optimize(self, model: str | None = None) -> OptimizeResult:
        """Run ``rocky optimize`` and return materialization recommendations."""
        args: list[str] = ["optimize"]
        if model is not None:
            args.extend(["--model", model])
        return _parse_rocky_json(self.run_cli(args), OptimizeResult, command="optimize")

    def cost(self, run_id: str = "latest") -> CostOutput:
        """Run ``rocky cost <run_id>`` and return historical cost attribution."""
        return _parse_rocky_json(self.run_cli(["cost", run_id]), CostOutput, command="cost")

    # ------------------------------------------------------------------ #
    # AI Level 3 commands                                                #
    # ------------------------------------------------------------------ #

    def ai(self, intent: str, format: str = "rocky") -> AiResult:
        """Generate a model from a natural-language intent."""
        return _parse_rocky_json(
            self.run_cli(["ai", intent, "--format", format]), AiResult, command="ai"
        )

    def ai_sync(
        self, *, apply: bool = False, model: str | None = None, with_intent: bool = False
    ) -> AiSyncResult:
        """Detect schema changes and propose intent-guided updates."""
        args = ["ai-sync", "--models", self.models_dir]
        if apply:
            args.append("--apply")
        if model is not None:
            args.extend(["--model", model])
        if with_intent:
            args.append("--with-intent")
        return _parse_rocky_json(self.run_cli(args), AiSyncResult, command="ai sync")

    def ai_explain(
        self, model: str | None = None, *, all: bool = False, save: bool = False
    ) -> AiExplainResult:
        """Generate intent descriptions from existing model code."""
        args: list[str] = ["ai-explain", "--models", self.models_dir]
        if model is not None:
            args.append(model)
        if all:
            args.append("--all")
        if save:
            args.append("--save")
        return _parse_rocky_json(self.run_cli(args), AiExplainResult, command="ai explain")

    def ai_test(
        self, model: str | None = None, *, all: bool = False, save: bool = False
    ) -> AiTestResult:
        """Generate test assertions from intent."""
        args: list[str] = ["ai-test", "--models", self.models_dir]
        if model is not None:
            args.append(model)
        if all:
            args.append("--all")
        if save:
            args.append("--save")
        return _parse_rocky_json(self.run_cli(args), AiTestResult, command="ai test")

    def ai_contract(self, model: str, *, save: bool = False) -> AiContractOutput:
        """AI-draft a data contract from a model's observed data (DuckDB only)."""
        args: list[str] = ["ai-contract", model, "--models", self.models_dir]
        if save:
            args.append("--save")
        return _parse_rocky_json(self.run_cli(args), AiContractOutput, command="ai contract")

    # ------------------------------------------------------------------ #
    # Hooks                                                              #
    # ------------------------------------------------------------------ #

    def hooks_list(self) -> str:
        """List configured hooks (returns raw stdout)."""
        return self.run_cli(["hooks", "list"])

    def hooks_test(self, event: str) -> str:
        """Fire a test hook event (returns raw stdout)."""
        return self.run_cli(["hooks", "test", event])

    # ------------------------------------------------------------------ #
    # Migration / adapter / doctor / governance                         #
    # ------------------------------------------------------------------ #

    def validate_migration(
        self,
        dbt_project: str,
        rocky_project: str | None = None,
        *,
        sample_size: int | None = None,
    ) -> ValidateMigrationResult:
        """Compare a dbt project against a Rocky import."""
        args = ["validate-migration", "--dbt-project", dbt_project]
        if rocky_project is not None:
            args.extend(["--rocky-project", rocky_project])
        if sample_size is not None:
            args.extend(["--sample-size", str(sample_size)])
        return _parse_rocky_json(self.run_cli(args), ValidateMigrationResult, command="validate")

    def test_adapter(
        self, adapter: str | None = None, command: str | None = None
    ) -> ConformanceResult:
        """Run adapter conformance tests."""
        args: list[str] = ["test-adapter"]
        if adapter is not None:
            args.extend(["--adapter", adapter])
        if command is not None:
            args.extend(["--command", command])
        return _parse_rocky_json(self.run_cli(args), ConformanceResult, command="conformance")

    def doctor(self, *, check: str | None = None) -> DoctorResult:
        """Run ``rocky doctor`` and return the parsed health-check results."""
        args = ["doctor"]
        if check is not None:
            args.extend(["--check", check])
        return _parse_rocky_json(self.run_cli(args), DoctorResult, command="doctor")

    def compliance(self, *, env: str | None = None) -> ComplianceOutput:
        """Run ``rocky compliance`` and return the governance rollup."""
        args = ["compliance", "--output", "json"]
        if env is not None:
            args.extend(["--env", env])
        return _parse_rocky_json(self.run_cli(args), ComplianceOutput, command="compliance")

    def retention_status(self, *, env: str | None = None) -> RetentionStatusOutput:
        """Run ``rocky retention-status`` and return per-model retention status."""
        args = ["retention-status", "--output", "json"]
        if env is not None:
            args.extend(["--env", env])
        return _parse_rocky_json(
            self.run_cli(args), RetentionStatusOutput, command="retention-status"
        )
