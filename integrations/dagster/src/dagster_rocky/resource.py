"""RockyResource — Dagster resource wrapping the Rocky CLI binary.

The resource exposes one Python method per Rocky CLI command. Each method
builds the right argv, invokes the binary via subprocess (or hits the
``rocky serve`` HTTP API when ``server_url`` is set), parses the JSON
output, and returns a Pydantic model from :mod:`.types`.

Two execution modes for ``rocky run``:

* :meth:`RockyResource.run` — buffered: ``subprocess.run`` collects the
  full stdout/stderr in memory and returns the parsed result. Suitable
  for short runs and non-Dagster callers.
* :meth:`RockyResource.run_streaming` — Pipes-style: ``subprocess.Popen``
  + a stderr reader thread that forwards each line to ``context.log``
  in real time. Suitable for long Dagster runs where users want live
  progress visibility in the run viewer.
"""

from __future__ import annotations

import contextlib
import json
import shutil
import subprocess
import threading
import urllib.error
import urllib.request
from typing import TYPE_CHECKING

import dagster as dg

if TYPE_CHECKING:
    from collections.abc import Iterable

from .types import (
    AiExplainResult,
    AiResult,
    AiSyncResult,
    AiTestResult,
    CiResult,
    ColumnLineageResult,
    CompileResult,
    ConformanceResult,
    DiscoverResult,
    DoctorResult,
    HistoryResult,
    MetricsResult,
    ModelHistoryResult,
    ModelLineageResult,
    OptimizeResult,
    PlanResult,
    RunResult,
    StateResult,
    TestResult,
    ValidateMigrationResult,
)

# Default subprocess timeout for any single Rocky CLI invocation. One hour is
# generous enough for full pipeline runs but still bounds runaway processes.
DEFAULT_TIMEOUT_SECONDS = 3600

# HTTP read timeout for the optional ``rocky serve`` fallback used by
# compile/lineage/metrics. Kept short — these endpoints are read-only.
DEFAULT_HTTP_TIMEOUT_SECONDS = 30

# Minimum Rocky binary version this dagster-rocky release is compatible with.
# Checked lazily on first CLI invocation. If the binary is older, the resource
# raises a ``dg.Failure`` with a clear message pointing at the install URL.
MIN_ROCKY_VERSION = "1.0.0"


def _forward_stderr_to_context(
    stderr: Iterable[str] | None,
    context: dg.AssetExecutionContext | dg.OpExecutionContext,
    sink: list[str],
) -> None:
    """Reader-thread body that forwards rocky stderr lines to ``context.log``.

    Reads ``stderr`` line-by-line until EOF. Each non-empty line is:

    1. Appended to ``sink`` (a list shared with the parent thread for
       use in failure metadata — the parent grabs the tail to surface
       in ``dg.Failure``).
    2. Logged via ``context.log.info`` so the Dagster run viewer
       streams progress in real time. Rocky's tracing layer writes to
       stderr (see ``engine/crates/rocky-observe/src/tracing_setup.rs``)
       so this captures every ``info!()`` / ``warn!()`` macro emission.

    Silently swallows any exceptions reading the pipe so a crashed
    reader thread doesn't take down the parent. The trade-off is that
    on a corrupt pipe we lose live streaming but the subprocess still
    completes and the final stdout parsing still runs.
    """
    if stderr is None:
        return
    with contextlib.suppress(Exception):
        # Outer suppress catches a broken pipe or any other read error so
        # the reader thread exits cleanly without taking down the parent.
        for raw in stderr:
            line = raw.rstrip("\n")
            if not line:
                continue
            sink.append(line)
            with contextlib.suppress(Exception):
                # context.log can raise if the run is being torn down;
                # don't crash the reader thread on shutdown races.
                context.log.info(f"rocky: {line}")


class RockyResource(dg.ConfigurableResource):
    """Dagster resource that invokes the Rocky CLI binary.

    Args:
        binary_path: Path to the ``rocky`` binary. Defaults to ``"rocky"`` (on PATH).
        config_path: Path to the ``rocky.toml`` config file.
        state_path: Path to the state store file.
        models_dir: Path to the directory containing model files
            (used by compile/lineage/test/ci).
        contracts_dir: Optional directory containing contract files.
        server_url: Optional URL for a running ``rocky serve`` instance. When set,
            ``compile()``, ``lineage()`` and ``metrics()`` use the HTTP API instead
            of a subprocess.
        timeout_seconds: Subprocess timeout for any one CLI invocation.
    """

    binary_path: str = "rocky"
    config_path: str = "rocky.toml"
    state_path: str = ".rocky-state.redb"
    models_dir: str = "models"
    contracts_dir: str | None = None
    server_url: str | None = None
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS

    # Instance-level cache for the version check (not a Dagster config field).
    _version_checked: bool = False

    # ------------------------------------------------------------------ #
    # Version compatibility check                                        #
    # ------------------------------------------------------------------ #

    def _verify_engine_version(self) -> None:
        """Check that the rocky binary meets the minimum version requirement.

        Called lazily on the first CLI invocation. Caches the result so
        subsequent calls are free. Raises ``dg.Failure`` with a helpful
        message if the binary is too old or missing.
        """
        if self._version_checked:
            return

        binary = shutil.which(self.binary_path) or self.binary_path
        try:
            result = subprocess.run(
                [binary, "--version"],
                capture_output=True,
                text=True,
                check=False,
                timeout=10,
            )
        except FileNotFoundError:
            raise dg.Failure(
                description=(
                    f"Rocky binary not found at '{self.binary_path}'. "
                    "Install from: https://github.com/rocky-data/rocky/releases"
                ),
            ) from None
        except (subprocess.TimeoutExpired, OSError):
            # Version check is best-effort — don't block the actual command
            # if the binary is slow to respond or the environment is weird.
            object.__setattr__(self, "_version_checked", True)
            return

        # rocky --version outputs "rocky X.Y.Z" or just "X.Y.Z"
        version_str = result.stdout.strip().removeprefix("rocky ").strip()

        if not version_str:
            # Can't determine version — warn but don't block (CI edge cases)
            object.__setattr__(self, "_version_checked", True)
            return

        try:
            detected = tuple(int(p) for p in version_str.split(".")[:3])
            required = tuple(int(p) for p in MIN_ROCKY_VERSION.split(".")[:3])
        except ValueError:
            # Non-semver output — skip check
            object.__setattr__(self, "_version_checked", True)
            return

        if detected < required:
            raise dg.Failure(
                description=(
                    f"Rocky binary version {version_str} is below the minimum "
                    f"required version {MIN_ROCKY_VERSION} for this dagster-rocky release. "
                    f"Update: https://github.com/rocky-data/rocky/releases"
                ),
                metadata={
                    "detected_version": dg.MetadataValue.text(version_str),
                    "min_version": dg.MetadataValue.text(MIN_ROCKY_VERSION),
                    "binary_path": dg.MetadataValue.text(binary),
                },
            )

        object.__setattr__(self, "_version_checked", True)

    # ------------------------------------------------------------------ #
    # Subprocess + HTTP plumbing                                         #
    # ------------------------------------------------------------------ #

    def _run_rocky(self, args: list[str], *, allow_partial: bool = False) -> str:
        """Execute the Rocky CLI and return stdout.

        Args:
            args: CLI arguments after the global flags. ``--config``,
                ``--state-path`` and ``--output json`` are inserted automatically.
            allow_partial: If ``True``, return stdout even on a non-zero exit
                when stdout starts with valid JSON (Rocky's partial-success
                semantics, where some tables succeed and some fail).
        """
        self._verify_engine_version()
        cmd = self._build_cmd(args)
        try:
            result = subprocess.run(  # noqa: S603 - cmd is fully constructed from typed args
                cmd,
                capture_output=True,
                text=True,
                check=False,
                timeout=self.timeout_seconds,
            )
        except FileNotFoundError:
            raise dg.Failure(
                description=(
                    f"Rocky binary not found at '{self.binary_path}'. "
                    "Install from: https://github.com/rocky-data/rocky/releases"
                ),
            ) from None
        except subprocess.TimeoutExpired:
            raise dg.Failure(
                description=f"Rocky command timed out after {self.timeout_seconds}s",
            ) from None

        if result.returncode == 0:
            return result.stdout

        # Partial-success: Rocky exits non-zero but still emits a valid JSON
        # payload describing the successful materializations alongside errors.
        if allow_partial and result.stdout.lstrip().startswith("{"):
            return result.stdout

        raise dg.Failure(
            description=f"Rocky command failed (exit {result.returncode})",
            metadata={"stderr": dg.MetadataValue.text(result.stderr)},
        )

    def _build_cmd(self, args: list[str]) -> list[str]:
        binary = shutil.which(self.binary_path) or self.binary_path
        return [
            binary,
            "--config",
            self.config_path,
            "--state-path",
            self.state_path,
            "--output",
            "json",
            *args,
        ]

    def _run_rocky_streaming(
        self,
        args: list[str],
        context: dg.AssetExecutionContext | dg.OpExecutionContext,
        *,
        allow_partial: bool = False,
    ) -> str:
        """Execute the Rocky CLI with live stderr streaming to ``context.log``.

        This is the Pipes-style alternative to :meth:`_run_rocky`. Instead
        of buffering everything via ``subprocess.run``, it spawns the
        binary via ``subprocess.Popen`` with separated stdout/stderr,
        starts a reader thread on stderr that forwards each line to
        ``context.log.info`` for live progress visibility, and captures
        stdout in memory for the final JSON parse after the subprocess
        exits.

        Rocky's tracing logs go to stderr (see
        ``engine/crates/rocky-observe/src/tracing_setup.rs``) and the
        structured ``--output json`` payload goes to stdout, so the
        separation is clean: users see human-readable progress lines
        in the Dagster run viewer in real time, and the integration
        gets the typed result back at the end.

        Note: this is "Pipes-style" rather than full Dagster Pipes
        because rocky-cli does not yet emit Dagster Pipes protocol
        messages. When the engine adds that, this method can be
        upgraded to use ``PipesSubprocessClient`` for per-model
        materialization-event streaming. Until then, what users get is
        live log streaming + final result parsing — a meaningful
        improvement over the buffered ``run()`` path.

        Args:
            args: CLI arguments after the global flags. ``--config``,
                ``--state-path`` and ``--output json`` are inserted
                automatically by :meth:`_build_cmd`.
            context: Dagster execution context. Either an
                ``AssetExecutionContext`` (when called from a
                ``@multi_asset``) or an ``OpExecutionContext`` (when
                called from a ``@op``).
            allow_partial: Same semantics as :meth:`_run_rocky` —
                accept a non-zero exit when stdout starts with valid
                JSON (Rocky partial-success).

        Returns:
            The captured stdout as a string, ready to be parsed via
            ``RunResult.model_validate_json``.

        Raises:
            dg.Failure: When the binary is missing, the subprocess
                times out, or the run fails without partial-success
                semantics.
        """
        self._verify_engine_version()
        cmd = self._build_cmd(args)
        try:
            proc = subprocess.Popen(  # noqa: S603 - cmd is fully constructed from typed args
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,  # line-buffered so the reader thread sees lines as they're written
            )
        except FileNotFoundError:
            raise dg.Failure(
                description=(
                    f"Rocky binary not found at '{self.binary_path}'. "
                    "Install from: https://github.com/rocky-data/rocky/releases"
                ),
            ) from None

        # Start a daemon thread to forward stderr lines to context.log in
        # real time. Daemon=True so it doesn't block process exit on a
        # crash.
        stderr_lines: list[str] = []
        reader = threading.Thread(
            target=_forward_stderr_to_context,
            args=(proc.stderr, context, stderr_lines),
            daemon=True,
            name="rocky-stderr-forwarder",
        )
        reader.start()

        try:
            stdout, _ = proc.communicate(timeout=self.timeout_seconds)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            reader.join(timeout=1.0)
            raise dg.Failure(
                description=f"Rocky command timed out after {self.timeout_seconds}s",
                metadata={"stderr_tail": dg.MetadataValue.text("\n".join(stderr_lines[-20:]))},
            ) from None

        # Wait briefly for the stderr reader to drain remaining lines
        # before checking exit status. The communicate() call already
        # closed the stderr pipe so the reader will exit shortly.
        reader.join(timeout=2.0)

        if proc.returncode == 0:
            return stdout

        # Partial-success: same semantics as the buffered path.
        if allow_partial and stdout.lstrip().startswith("{"):
            return stdout

        raise dg.Failure(
            description=f"Rocky command failed (exit {proc.returncode})",
            metadata={"stderr_tail": dg.MetadataValue.text("\n".join(stderr_lines[-20:]))},
        )

    def _http_get(self, path: str) -> str:
        """GET a JSON document from the ``rocky serve`` HTTP API."""
        if self.server_url is None:
            raise dg.Failure(
                description="HTTP fallback called but server_url is not configured",
            )
        url = f"{self.server_url.rstrip('/')}{path}"
        try:
            with urllib.request.urlopen(  # noqa: S310 - URL built from validated config
                url, timeout=DEFAULT_HTTP_TIMEOUT_SECONDS
            ) as resp:
                return resp.read().decode("utf-8")
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            raise dg.Failure(
                description=f"Rocky server request failed: {url}",
                metadata={"error": dg.MetadataValue.text(str(exc))},
            ) from None

    # ------------------------------------------------------------------ #
    # Discovery & execution (always CLI)                                 #
    # ------------------------------------------------------------------ #

    def discover(self) -> DiscoverResult:
        """Run ``rocky discover`` and return the parsed result."""
        return DiscoverResult.model_validate_json(self._run_rocky(["discover"]))

    def plan(self, filter: str) -> PlanResult:
        """Run ``rocky plan --filter <key=value>`` and return the parsed result."""
        return PlanResult.model_validate_json(self._run_rocky(["plan", "--filter", filter]))

    def run(
        self,
        filter: str,
        governance_override: dict | None = None,
        *,
        run_models: bool = False,
        partition: str | None = None,
        partition_from: str | None = None,
        partition_to: str | None = None,
        latest: bool = False,
        missing: bool = False,
        lookback: int | None = None,
        parallel: int | None = None,
    ) -> RunResult:
        """Run ``rocky run --filter <key=value>`` and return the parsed result.

        Partial success: if some tables fail but others succeed, the resulting
        JSON is still parsed and returned so callers can emit ``MaterializeResult``
        events for the successful tables and report failures for the rest.

        Args:
            filter: Component filter (e.g. ``"client=acme"``).
            governance_override: Optional per-run governance config (workspace_ids,
                grants), merged additively with ``rocky.toml`` defaults.
            run_models: If ``True``, also execute compiled models
                (passes ``--models`` and ``--all``).
            partition: Single partition key (canonical Rocky format, e.g.
                ``"2026-04-07"`` for daily, ``"2026-04-07T13"`` for hourly).
                Mutually exclusive with the other partition selection flags.
            partition_from: Lower bound of a closed inclusive partition range.
                Requires ``partition_to``. Mutually exclusive with ``partition``,
                ``latest``, ``missing``.
            partition_to: Upper bound of a closed inclusive partition range.
                Requires ``partition_from``.
            latest: Run the partition containing ``now()`` (UTC). Default for
                ``time_interval`` models when no other selection flag is set.
            missing: Run the partitions missing from the state store
                (computed from ``first_partition`` → now). Errors if
                ``first_partition`` is unset.
            lookback: Recompute the previous N partitions in addition to
                the selected ones. Overrides the model's TOML ``lookback``.
            parallel: Run N partitions concurrently (warehouse-query
                parallelism only). Defaults to 1 on the engine side.
        """
        args = self._build_run_args(
            filter,
            governance_override=governance_override,
            run_models=run_models,
            partition=partition,
            partition_from=partition_from,
            partition_to=partition_to,
            latest=latest,
            missing=missing,
            lookback=lookback,
            parallel=parallel,
        )
        return RunResult.model_validate_json(self._run_rocky(args, allow_partial=True))

    def run_streaming(
        self,
        context: dg.AssetExecutionContext | dg.OpExecutionContext,
        filter: str,
        governance_override: dict | None = None,
        *,
        run_models: bool = False,
        partition: str | None = None,
        partition_from: str | None = None,
        partition_to: str | None = None,
        latest: bool = False,
        missing: bool = False,
        lookback: int | None = None,
        parallel: int | None = None,
    ) -> RunResult:
        """Pipes-style ``rocky run`` with live stderr streaming to ``context.log``.

        Same semantics as :meth:`run` but spawns the binary via
        ``subprocess.Popen`` and forwards rocky's stderr (where the
        engine's tracing layer writes ``info!()`` / ``warn!()`` macros)
        to ``context.log.info`` line-by-line as the run progresses.

        Use this method from inside a Dagster ``@multi_asset`` /
        ``@op`` for runs longer than a few seconds — users will see
        progress in the run viewer as it happens, instead of waiting
        for the full output to buffer at the end.

        Background: this is "Pipes-style" rather than full Dagster
        Pipes (``PipesSubprocessClient``) because rocky-cli does not
        yet emit the Dagster Pipes message protocol. When the engine
        adds Pipes message emission, this method can be upgraded to
        use ``PipesSubprocessClient`` for per-model materialization
        events streaming live (instead of all at once at the end).
        Until then, what users get from this method is:

        * Live stderr → ``context.log.info`` streaming (every Rust
          ``info!()`` / ``warn!()`` macro shows up in the run viewer
          as it executes).
        * Final ``RunResult`` parsing from stdout after the subprocess
          exits cleanly.
        * Partial-success handling (non-zero exit + valid JSON stdout
          still returns the parsed result).
        * Captured stderr tail in any ``dg.Failure`` raised on errors.

        Args:
            context: Dagster execution context. Either an
                ``AssetExecutionContext`` (called from a multi_asset)
                or an ``OpExecutionContext`` (called from an op).
                Used as the destination for streamed log lines.
            filter: Component filter (e.g. ``"client=acme"``).
            governance_override: Same as :meth:`run`.
            run_models: Same as :meth:`run`.
            partition / partition_from / partition_to / latest /
            missing / lookback / parallel: Same as :meth:`run` —
                Phase 3 partition selection flags.

        Returns:
            The parsed :class:`RunResult` from stdout after the
            subprocess exits.
        """
        args = self._build_run_args(
            filter,
            governance_override=governance_override,
            run_models=run_models,
            partition=partition,
            partition_from=partition_from,
            partition_to=partition_to,
            latest=latest,
            missing=missing,
            lookback=lookback,
            parallel=parallel,
        )
        return RunResult.model_validate_json(
            self._run_rocky_streaming(args, context, allow_partial=True)
        )

    def _build_run_args(
        self,
        filter: str,
        *,
        governance_override: dict | None,
        run_models: bool,
        partition: str | None,
        partition_from: str | None,
        partition_to: str | None,
        latest: bool,
        missing: bool,
        lookback: int | None,
        parallel: int | None,
    ) -> list[str]:
        """Shared argv builder used by both :meth:`run` and :meth:`run_streaming`.

        Single source of truth for the partition flag plumbing so
        adding a new flag is a one-place change. The flags are
        defensive: ``partition_from`` without ``partition_to`` emits
        neither (rocky requires both for range mode), and the engine
        enforces mutual-exclusion via clap so we trust it to error
        helpfully if multiple selection flags are passed simultaneously.
        """
        args = ["run", "--filter", filter]
        if governance_override:
            args.extend(["--governance-override", json.dumps(governance_override)])
        if run_models:
            args.extend(["--models", self.models_dir, "--all"])
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
        return args

    def run_pipes(
        self,
        context: dg.AssetExecutionContext | dg.OpExecutionContext,
        filter: str,
        governance_override: dict | None = None,
        *,
        run_models: bool = False,
        partition: str | None = None,
        partition_from: str | None = None,
        partition_to: str | None = None,
        latest: bool = False,
        missing: bool = False,
        lookback: int | None = None,
        parallel: int | None = None,
        pipes_client: dg.PipesSubprocessClient | None = None,
    ) -> dg.PipesClientCompletedInvocation:
        """Full Dagster Pipes execution: structured events streamed via the protocol.

        Spawns ``rocky run`` via :class:`dagster.PipesSubprocessClient`,
        which sets the ``DAGSTER_PIPES_CONTEXT`` /
        ``DAGSTER_PIPES_MESSAGES`` env vars and tails the messages
        channel for structured events. Rocky's Pipes emitter (added
        engine-side in commit ef08cae) writes one Pipes message per
        materialization, asset check, and log line, so the run viewer
        gets:

        * ``MaterializationEvent`` per copied table (with rocky/strategy,
          duration_ms, rows_copied, target_table_full_name, sql_hash,
          partition_key when set)
        * ``AssetCheckEvaluation`` per Rocky check
        * ``log`` events for the rocky run starting / completing
          messages and any drift actions

        Use this method from a ``@dg.asset`` or ``@dg.multi_asset``
        when you want **structured** Dagster events from the rocky
        run, not just stderr forwarding. The canonical pattern::

            @dg.asset
            def my_warehouse_data(
                context: dg.AssetExecutionContext,
                rocky: RockyResource,
            ):
                yield from rocky.run_pipes(
                    context, filter="tenant=acme",
                ).get_results()

        ``run_pipes()`` returns the same
        :class:`PipesClientCompletedInvocation` shape as a direct
        ``PipesSubprocessClient.run()`` call, so users can chain
        ``.get_results()`` to extract the materialization events
        Dagster constructed from the Pipes messages.

        Args:
            context: Dagster execution context. Same as
                :meth:`run_streaming` — required for Pipes context
                injection (run id, partition key, asset keys, etc.).
            filter: Component filter (e.g. ``"tenant=acme"``).
            governance_override / run_models / partition / ... :
                Same as :meth:`run` and :meth:`run_streaming`. Threaded
                into the rocky CLI command via :meth:`_build_run_args`.
            pipes_client: Optional pre-configured
                ``PipesSubprocessClient`` (for tests or for users who
                need custom env / cwd / message_reader / context_injector).
                When ``None``, a fresh client is constructed with
                Dagster defaults.

        Returns:
            A :class:`PipesClientCompletedInvocation` ready for
            ``.get_results()`` to extract materialization events.

        Raises:
            dg.Failure: If the subprocess exits non-zero. Pipes also
                propagates a Failure when the subprocess crashes
                without sending a Pipes ``closed`` message.
        """
        args = self._build_run_args(
            filter,
            governance_override=governance_override,
            run_models=run_models,
            partition=partition,
            partition_from=partition_from,
            partition_to=partition_to,
            latest=latest,
            missing=missing,
            lookback=lookback,
            parallel=parallel,
        )
        client = pipes_client or dg.PipesSubprocessClient()
        return client.run(
            context=context,
            command=self._build_cmd(args),
        )

    def state(self) -> StateResult:
        """Run ``rocky state`` and return the parsed result."""
        return StateResult.model_validate_json(self._run_rocky(["state"]))

    # ------------------------------------------------------------------ #
    # Compiler (HTTP when server_url is set, CLI otherwise)              #
    # ------------------------------------------------------------------ #

    def compile(self, model_filter: str | None = None) -> CompileResult:
        """Run ``rocky compile`` and return the parsed result.

        When ``server_url`` is configured, fetches from the HTTP API instead.

        Args:
            model_filter: Optional model name to filter diagnostics.
        """
        if self.server_url is not None:
            return CompileResult.model_validate_json(self._http_get("/api/v1/compile"))

        args = ["compile", "--models", self.models_dir]
        if self.contracts_dir is not None:
            args.extend(["--contracts", self.contracts_dir])
        if model_filter is not None:
            args.extend(["--model", model_filter])
        return CompileResult.model_validate_json(self._run_rocky(args, allow_partial=True))

    def lineage(
        self,
        target: str,
        column: str | None = None,
    ) -> ModelLineageResult | ColumnLineageResult:
        """Run ``rocky lineage`` and return the parsed result.

        When ``server_url`` is configured, fetches from the HTTP API instead.

        Args:
            target: Model name (e.g. ``"customer_orders"``).
            column: Optional column name to trace.
        """
        if self.server_url is not None:
            if column is not None:
                output = self._http_get(f"/api/v1/models/{target}/lineage/{column}")
                return ColumnLineageResult.model_validate_json(output)
            output = self._http_get(f"/api/v1/models/{target}/lineage")
            return ModelLineageResult.model_validate_json(output)

        args = ["lineage", "--models", self.models_dir, target]
        if column is not None:
            args.extend(["--column", column])
        output = self._run_rocky(args)
        if column is not None:
            return ColumnLineageResult.model_validate_json(output)
        return ModelLineageResult.model_validate_json(output)

    # ------------------------------------------------------------------ #
    # Local testing (always CLI)                                         #
    # ------------------------------------------------------------------ #

    def test(self, model_filter: str | None = None) -> TestResult:
        """Run ``rocky test`` and return the parsed result.

        Executes models locally via DuckDB without warehouse credentials.

        Args:
            model_filter: Optional model name to test.
        """
        args = ["test", "--models", self.models_dir]
        if self.contracts_dir is not None:
            args.extend(["--contracts", self.contracts_dir])
        if model_filter is not None:
            args.extend(["--model", model_filter])
        return TestResult.model_validate_json(self._run_rocky(args, allow_partial=True))

    def ci(self) -> CiResult:
        """Run ``rocky ci`` (compile + test) and return the parsed result."""
        args = ["ci", "--models", self.models_dir]
        if self.contracts_dir is not None:
            args.extend(["--contracts", self.contracts_dir])
        return CiResult.model_validate_json(self._run_rocky(args, allow_partial=True))

    # ------------------------------------------------------------------ #
    # Observability (HTTP when server_url is set, CLI otherwise)         #
    # ------------------------------------------------------------------ #

    def history(
        self,
        model: str | None = None,
        since: str | None = None,
    ) -> HistoryResult | ModelHistoryResult:
        """Run ``rocky history`` and return the parsed result.

        Args:
            model: Optional model name to filter to.
            since: Optional date filter (ISO 8601 or ``YYYY-MM-DD``).
        """
        args: list[str] = ["history"]
        if model is not None:
            args.extend(["--model", model])
        if since is not None:
            args.extend(["--since", since])
        output = self._run_rocky(args)
        if model is not None:
            return ModelHistoryResult.model_validate_json(output)
        return HistoryResult.model_validate_json(output)

    def metrics(
        self,
        model: str,
        *,
        trend: bool = False,
        column: str | None = None,
        alerts: bool = False,
    ) -> MetricsResult:
        """Run ``rocky metrics`` and return the parsed result.

        When ``server_url`` is configured, fetches from the HTTP API instead.

        Args:
            model: Model name.
            trend: If ``True``, show trend over recent runs.
            column: Optional column to filter null rate trends.
            alerts: If ``True``, include quality alerts.
        """
        if self.server_url is not None:
            return MetricsResult.model_validate_json(
                self._http_get(f"/api/v1/models/{model}/metrics")
            )

        args = ["metrics", model]
        if trend:
            args.append("--trend")
        if column is not None:
            args.extend(["--column", column])
        if alerts:
            args.append("--alerts")
        return MetricsResult.model_validate_json(self._run_rocky(args))

    def optimize(self, model: str | None = None) -> OptimizeResult:
        """Run ``rocky optimize`` and return the parsed result.

        Args:
            model: Optional model name to filter analysis.
        """
        args: list[str] = ["optimize"]
        if model is not None:
            args.extend(["--model", model])
        return OptimizeResult.model_validate_json(self._run_rocky(args))

    # ------------------------------------------------------------------ #
    # AI Level 3 commands                                                #
    # ------------------------------------------------------------------ #

    def ai(self, intent: str, format: str = "rocky") -> AiResult:
        """Generate a model from a natural-language intent."""
        return AiResult.model_validate_json(self._run_rocky(["ai", intent, "--format", format]))

    def ai_sync(
        self,
        *,
        apply: bool = False,
        model: str | None = None,
        with_intent: bool = False,
    ) -> AiSyncResult:
        """Detect schema changes and propose intent-guided updates."""
        args = ["ai-sync", "--models", self.models_dir]
        if apply:
            args.append("--apply")
        if model is not None:
            args.extend(["--model", model])
        if with_intent:
            args.append("--with-intent")
        return AiSyncResult.model_validate_json(self._run_rocky(args))

    def ai_explain(
        self,
        model: str | None = None,
        *,
        all: bool = False,
        save: bool = False,
    ) -> AiExplainResult:
        """Generate intent descriptions from existing model code."""
        args: list[str] = ["ai-explain", "--models", self.models_dir]
        if model is not None:
            args.append(model)
        if all:
            args.append("--all")
        if save:
            args.append("--save")
        return AiExplainResult.model_validate_json(self._run_rocky(args))

    def ai_test(
        self,
        model: str | None = None,
        *,
        all: bool = False,
        save: bool = False,
    ) -> AiTestResult:
        """Generate test assertions from intent."""
        args: list[str] = ["ai-test", "--models", self.models_dir]
        if model is not None:
            args.append(model)
        if all:
            args.append("--all")
        if save:
            args.append("--save")
        return AiTestResult.model_validate_json(self._run_rocky(args))

    # ------------------------------------------------------------------ #
    # Hook commands                                                      #
    # ------------------------------------------------------------------ #

    def hooks_list(self) -> str:
        """List configured hooks (returns raw stdout)."""
        return self._run_rocky(["hooks", "list"])

    def hooks_test(self, event: str) -> str:
        """Fire a test hook event (returns raw stdout)."""
        return self._run_rocky(["hooks", "test", event])

    # ------------------------------------------------------------------ #
    # Migration and adapter commands                                     #
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
        return ValidateMigrationResult.model_validate_json(self._run_rocky(args))

    def test_adapter(
        self,
        adapter: str | None = None,
        command: str | None = None,
    ) -> ConformanceResult:
        """Run adapter conformance tests."""
        args: list[str] = ["test-adapter"]
        if adapter is not None:
            args.extend(["--adapter", adapter])
        if command is not None:
            args.extend(["--command", command])
        return ConformanceResult.model_validate_json(self._run_rocky(args))

    # ------------------------------------------------------------------ #
    # Doctor and resume commands                                         #
    # ------------------------------------------------------------------ #

    def doctor(self) -> DoctorResult:
        """Run ``rocky doctor`` and return the parsed health-check results."""
        return DoctorResult.model_validate_json(self._run_rocky(["doctor"]))

    def resume_run(
        self,
        run_id: str | None = None,
        *,
        filter: str = "",
        governance_override: dict | None = None,
    ) -> RunResult:
        """Resume a failed run from where it left off.

        Args:
            run_id: Specific run ID to resume. If ``None``, resumes the latest.
            filter: Optional filter expression.
            governance_override: Optional governance overrides.
        """
        args: list[str] = ["run"]
        if run_id is not None:
            args.extend(["--resume", run_id])
        else:
            args.append("--resume-latest")
        if filter:
            args.extend(["--filter", filter])
        if governance_override:
            args.extend(["--governance-override", json.dumps(governance_override)])
        return RunResult.model_validate_json(self._run_rocky(args, allow_partial=True))
