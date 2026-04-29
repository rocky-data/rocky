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
from typing import IO, TYPE_CHECKING, Annotated, Any, Literal, TypeVar

import dagster as dg
from pydantic import BaseModel, ConfigDict, ValidationError

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
    RetentionStatusOutput,
    RunResult,
    StateHealthResult,
    StateResult,
    TestResult,
    ValidateMigrationResult,
)

_log = logging.getLogger(__name__)

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

# Number of bytes of stdout/stderr surfaced back to the operator when a Rocky
# command returns malformed or schema-violating JSON. Enough to spot a stray
# tracing line or truncated blob without dumping potentially MB of noise.
_JSON_ERROR_PREVIEW_BYTES = 500

# Maximum bytes of stderr embedded into ``dg.Failure.metadata`` when a Rocky
# subprocess fails. Rocky's tracing layer can emit megabytes of context for a
# pathological run; the Dagster UI renders metadata inline and isn't designed
# to host blobs that large, so we clip and append a marker advertising the
# original byte count for operators who need the full payload.
_STDERR_METADATA_CAP_BYTES = 8192

# Argv flags whose immediately-following positional value is credential-bearing
# or otherwise sensitive. When we log the constructed argv (e.g. for live
# debugging in :meth:`_run_rocky_streaming`) we replace the value of any
# matching flag with ``"***"`` so credentials don't leak into Dagster logs or
# the operator's terminal. The subprocess itself still receives the real
# value — only the *log line* is redacted.
_REDACTED_ARGV_FLAGS = frozenset(
    {
        # `--governance-override <json>` carries a JSON blob with workspace IDs
        # and grant tuples. Not strictly a "credential" but treated as
        # sensitive so it never appears in shared logs or screenshots.
        "--governance-override",
        # `--idempotency-key <opaque>` is a caller-chosen token used to dedup
        # runs. Treat as opaque secret material.
        "--idempotency-key",
    }
)

_TModel = TypeVar("_TModel", bound=BaseModel)


def _redact_argv(argv: list[str]) -> list[str]:
    """Return a copy of ``argv`` with credential-bearing flag values masked.

    Walks the list left-to-right; whenever a token equals one of
    :data:`_REDACTED_ARGV_FLAGS`, the *next* token (the value) is replaced
    with ``"***"``. The flag itself is kept verbatim so log lines stay
    diagnostic ("we passed ``--idempotency-key ***``"). Useful for log
    output only — never for the argv handed to the subprocess.
    """
    out: list[str] = []
    redact_next = False
    for token in argv:
        if redact_next:
            out.append("***")
            redact_next = False
            continue
        out.append(token)
        if token in _REDACTED_ARGV_FLAGS:
            redact_next = True
    return out


def _truncate_stderr_for_metadata(stderr: str) -> str:
    """Cap ``stderr`` at :data:`_STDERR_METADATA_CAP_BYTES` for ``dg.Failure``.

    The Dagster UI renders ``dg.Failure.metadata`` inline; a multi-megabyte
    stderr blob would overwhelm both the renderer and the database row.
    When the input exceeds the cap, the prefix is preserved and a single
    marker line is appended advertising the original size so operators who
    need the full payload know to look in the Dagster compute logs / their
    own subprocess capture instead.
    """
    if len(stderr) <= _STDERR_METADATA_CAP_BYTES:
        return stderr
    original_bytes = len(stderr)
    return stderr[:_STDERR_METADATA_CAP_BYTES] + (
        f"\n... [truncated, original {original_bytes} bytes]"
    )


def _validate_governance_override(override: dict | None) -> None:
    """Pre-flight guard against the silent full-revoke footgun (FR-009).

    Mirrors the engine-side check in ``rocky-core`` / ``rocky run`` so
    dagster-rocky callers see the error as a :class:`dagster.Failure`
    **before** the subprocess is spawned — faster feedback, a cleaner
    stack trace, and no half-applied warehouse state on a catalog that
    was only ever meant to be a no-op.

    Semantics mirror the engine exactly:

    * ``None`` / argument omitted → no-op (supported).
    * ``workspace_ids`` key absent → no-op (caller doesn't want to touch
      workspace bindings on this run — FR-005 reconciler is skipped).
    * ``workspace_ids`` not a list → :class:`dagster.Failure` (type
      error; the engine would reject it during JSON deserialization).
    * ``workspace_ids = []`` without ``allow_empty_workspace_ids = True``
      → :class:`dagster.Failure` (rejects the footgun; otherwise the
      reconciler would revoke every existing workspace binding on the
      target catalog).
    * ``workspace_ids = []`` with ``allow_empty_workspace_ids = True``
      → no-op (explicit consent to fully revoke).
    * ``workspace_ids`` non-empty list → no-op (normal reconcile).

    Args:
        override: The ``governance_override`` dict (or ``None``) that
            the caller passed to :meth:`RockyResource.run`,
            :meth:`RockyResource.run_streaming`, or
            :meth:`RockyResource.run_pipes`.

    Raises:
        dagster.Failure: When ``override`` is neither ``None`` nor a
            dict, or when its ``workspace_ids`` shape would cause a
            silent full revoke.
    """
    if override is None:
        return
    if not isinstance(override, dict):
        raise dg.Failure(
            description=(
                "governance_override must be a dict (or None), got "
                f"{type(override).__name__}. See RockyResource.run docs for the "
                "expected shape."
            )
        )
    if "workspace_ids" not in override:
        # Key absent → engine treats as "skip binding reconciliation".
        # Intentional and supported; validator stays out of the way.
        return
    ws_ids = override["workspace_ids"]
    if not isinstance(ws_ids, list):
        raise dg.Failure(
            description=(
                f"governance_override.workspace_ids must be a list, got {type(ws_ids).__name__}."
            )
        )
    if not ws_ids and not override.get("allow_empty_workspace_ids"):
        raise dg.Failure(
            description=(
                "governance_override.workspace_ids is empty. Rocky's binding "
                "reconciler would revoke every workspace binding on the target "
                "catalog. Pass allow_empty_workspace_ids=True if that's "
                "intentional, or omit the workspace_ids key to skip binding "
                "reconciliation."
            )
        )


def _parse_rocky_json(output: str, model_cls: type[_TModel], *, command: str) -> _TModel:
    """Parse Rocky CLI JSON output into a Pydantic model with operator-friendly errors.

    Pydantic's ``model_validate_json`` raises ``ValidationError`` for both
    malformed JSON and schema-drift; the default message is unreadable for
    anyone debugging a Dagster run (a 2 000-character error with nested
    pydantic paths). This wrapper catches those failures, attaches a preview
    of the raw stdout, and re-raises as ``dg.Failure`` so the operator sees a
    clear cause + a scannable excerpt of what Rocky actually wrote.

    Roadmap §P2.12.
    """
    try:
        return model_cls.model_validate_json(output)
    except ValidationError as exc:
        preview = (output or "")[:_JSON_ERROR_PREVIEW_BYTES]
        raise dg.Failure(
            description=(
                f"rocky {command} output failed schema validation — "
                "see metadata for the raw stdout preview and pydantic error"
            ),
            metadata={
                "command": dg.MetadataValue.text(command),
                "stdout_preview": dg.MetadataValue.text(preview),
                "stdout_bytes": dg.MetadataValue.int(len(output or "")),
                "validation_error": dg.MetadataValue.text(str(exc)),
            },
        ) from exc
    except json.JSONDecodeError as exc:
        # Defense in depth — pydantic wraps JSON errors in ValidationError in
        # modern versions, but this catches any direct ``json.loads`` usage or
        # future pydantic behaviour change.
        preview = (output or "")[:_JSON_ERROR_PREVIEW_BYTES]
        raise dg.Failure(
            description=(
                f"rocky {command} returned malformed JSON — see metadata for the stdout preview"
            ),
            metadata={
                "command": dg.MetadataValue.text(command),
                "stdout_preview": dg.MetadataValue.text(preview),
                "stdout_bytes": dg.MetadataValue.int(len(output or "")),
                "json_error": dg.MetadataValue.text(str(exc)),
            },
        ) from exc


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

    This function is the **sole reader** of ``proc.stderr``. Running it
    alongside a ``proc.communicate(timeout=…)`` call (which also reads
    the pipe via raw ``os.read``) violates CPython's documented
    subprocess contract and was the root cause of the 2026-04-18 /
    2026-04-19 production hangs — the timeout intermittently failed to
    fire under stderr traffic. See :meth:`RockyResource._run_rocky_streaming`
    for the single-reader + watchdog pattern that replaces that race.

    On unexpected read errors the reader thread logs the error at WARN
    via the module logger and exits cleanly so it doesn't take down the
    parent. We still lose live streaming for the rest of the run, but
    the subprocess completes and the final stdout parsing still runs.
    """
    if stderr is None:
        return
    try:
        for raw in stderr:
            line = raw.rstrip("\n")
            if not line:
                continue
            sink.append(line)
            # `context.log` can raise mid-line if the run is being torn
            # down — suppress and keep reading so `sink` stays populated
            # for failure metadata.
            with contextlib.suppress(Exception):
                context.log.info(f"rocky: {line}")
    except (OSError, ValueError) as exc:
        # OSError covers broken-pipe / EBADF / closed-file on the subprocess
        # stderr handle. ValueError covers "I/O operation on closed file"
        # from CPython. Anything else escapes so we notice the real bug.
        _log.warning("rocky stderr reader terminated: %s", exc)


def _accumulate_stdout(stdout: IO[str] | None, sink: list[str]) -> None:
    """Reader-thread body that accumulates rocky stdout lines into ``sink``.

    Counterpart to :func:`_forward_stderr_to_context` for the stdout
    pipe. Runs as the **sole reader** of ``proc.stdout`` so the parent
    thread never touches the pipe FD — eliminating the two-readers
    race that caused the 2026-04-18 / 2026-04-19 production hangs.

    Reads line-by-line (inheriting the parent's ``bufsize=1``
    line-buffering) and appends every line, **including blank ones**,
    so the final concatenation reconstructs rocky's exact JSON output
    byte-for-byte ready for ``_parse_rocky_json``.

    On unexpected read errors the reader thread logs at WARN and exits
    cleanly; the parent will see whatever was buffered so far and will
    surface a JSON parse failure if the payload is truncated.
    """
    if stdout is None:
        return
    try:
        for line in stdout:
            sink.append(line)
    except (OSError, ValueError) as exc:
        _log.warning("rocky stdout accumulator terminated: %s", exc)


class RockyPipesMessageReader(dg.PipesTempFileMessageReader):
    """Pipes message reader that translates + filters Rocky asset keys in flight.

    The Rocky CLI emits Pipes messages with asset keys keyed by the
    engine's native ``[source_type, *components, table]`` path (see
    ``engine/crates/rocky-cli/src/commands/run.rs::emit_pipes_events``),
    slash-joined per the Dagster wire convention. That is the correct
    shape for a direct ``@dg.asset`` callsite that picks matching
    keys, but :class:`RockyComponent` remaps those paths to Dagster
    asset keys via :class:`RockyDagsterTranslator` before declaring
    the multi-asset.

    Without this reader the Pipes path would deliver materialization /
    check events on keys Dagster's asset graph doesn't recognise, so
    the run viewer would show "unexpected asset key" errors or drop
    events silently. Intercepting in the reader lets us rewrite keys
    and drop events for tables outside the selected subset *before*
    the events reach Dagster's handler — so nothing leaks through and
    nothing races.

    The interception point is :meth:`handle_message` on the handler
    wrapper we hand to :class:`PipesFileMessageReader._reader_thread`.
    Subclassing :class:`PipesTempFileMessageReader` is a small private
    API dependency (the reader-thread hook is prefixed with an
    underscore in Dagster); if that contract ever changes, the
    dagster bump check in CI will surface it before release.

    Two transformations happen on each message:

    1. ``asset_key_fn`` rewrites the wire ``asset_key`` string. It
       receives the slash-split path (``list[str]``) — the exact tuple
       the engine emits — and returns a :class:`dagster.AssetKey` or
       ``None``. Returning ``None`` drops the event (the key isn't
       known to this component).
    2. ``include_keys`` filters on the *resolved* :class:`AssetKey`.
       Events whose resolved key isn't in the set are dropped.

    Both hooks are optional — the default reader is the unmodified
    file-based tempfile reader.
    """

    def __init__(
        self,
        *,
        asset_key_fn: Callable[[list[str]], dg.AssetKey | None] | None = None,
        include_keys: set[dg.AssetKey] | None = None,
        include_stdio_in_messages: bool = False,
    ) -> None:
        super().__init__(include_stdio_in_messages=include_stdio_in_messages)
        self._asset_key_fn = asset_key_fn
        self._include_keys = include_keys

    @contextlib.contextmanager
    def read_messages(self, handler):  # type: ignore[override]
        """Wrap the upstream handler with a filter + asset-key rewriter.

        We build a tiny proxy whose ``handle_message`` inspects each
        Pipes envelope, transforms the ``asset_key`` where applicable,
        then either forwards it to the real handler or drops it. Every
        other method is delegated to the underlying handler so the
        upstream reader thread sees the full interface (opened /
        closed counts, framework exception reporting, etc.).
        """
        proxy = _PipesHandlerProxy(
            handler,
            asset_key_fn=self._asset_key_fn,
            include_keys=self._include_keys,
        )
        with super().read_messages(proxy) as params:
            yield params


class _PipesHandlerProxy:
    """Duck-typed handler wrapper that filters Rocky Pipes messages.

    Kept private: this wraps a single call site —
    :meth:`PipesFileMessageReader._reader_thread` invoking
    ``handler.handle_message(message)``. Every other attribute is
    forwarded unchanged via :meth:`__getattr__` so the reader thread's
    exception-reporting helpers still work.
    """

    # Messages whose ``params`` dict carries an asset key. Other
    # messages (``opened``, ``closed``, ``log``, ``log_external_stream``,
    # ``report_custom_message``) pass through untouched.
    _ASSET_KEYED_METHODS = frozenset(("report_asset_materialization", "report_asset_check"))

    def __init__(
        self,
        inner,
        *,
        asset_key_fn: Callable[[list[str]], dg.AssetKey | None] | None,
        include_keys: set[dg.AssetKey] | None,
    ) -> None:
        self._inner = inner
        self._asset_key_fn = asset_key_fn
        self._include_keys = include_keys

    def handle_message(self, message) -> None:
        method = message.get("method")
        if method in self._ASSET_KEYED_METHODS:
            transformed = self._transform_asset_key(message)
            if transformed is None:
                # Event dropped (either asset_key_fn rejected it or the
                # resolved key isn't in include_keys). Silent by design
                # — Rocky always runs at source granularity so emitting
                # a log per dropped event would flood on any partial
                # subset run.
                return
            message = transformed

        self._inner.handle_message(message)

    def _transform_asset_key(self, message):
        params = message.get("params") or {}
        raw_key = params.get("asset_key")
        if not isinstance(raw_key, str) or not raw_key:
            # Defensive — let the inner handler raise if the envelope
            # is malformed. We only transform well-formed wire values.
            return message

        path = raw_key.split("/")

        if self._asset_key_fn is not None:
            resolved = self._asset_key_fn(path)
            if resolved is None:
                return None
            # Dagster expects slash-escaped user strings on the wire
            # (the handler calls AssetKey.from_escaped_user_string).
            new_key_str = resolved.to_user_string()
        else:
            resolved = dg.AssetKey(path)
            new_key_str = raw_key

        if self._include_keys is not None and resolved not in self._include_keys:
            return None

        # Only rebuild the envelope when we actually changed the key —
        # otherwise the inner handler's check.str_param is happy with
        # the original dict.
        if new_key_str == raw_key:
            return message

        new_params = dict(params)
        new_params["asset_key"] = new_key_str
        new_message = dict(message)
        new_message["params"] = new_params
        return new_message

    def __getattr__(self, name):
        # Forward every attribute we don't handle explicitly — the
        # upstream reader needs report_pipes_framework_exception,
        # on_launched, properties, and internal state.
        return getattr(self._inner, name)


def _kill_process_group(proc: subprocess.Popen[str]) -> None:
    """Terminate ``proc`` and any children via its POSIX process group.

    Called from the watchdog thread when the wall-clock timeout
    elapses. On POSIX, uses ``os.killpg(os.getpgid(pid), SIGKILL)`` so
    child processes rocky spawned also die — requires that the Popen
    was launched with ``start_new_session=True``. On Windows
    (``os.name == "nt"``) falls back to ``proc.kill()`` which is the
    best we can do without a process group.

    ``ProcessLookupError`` and ``OSError`` are swallowed silently: the
    subprocess may have exited between ``proc.wait()`` returning and
    the kill call, which is benign. Any other exception surfaces to
    the thread's default handler so we notice real bugs.
    """
    try:
        if os.name == "nt":
            proc.kill()
        else:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except (ProcessLookupError, OSError):
        # Process already reaped, pgid lookup raced with exit, or the
        # kernel simply refused — nothing useful we can do from here.
        pass


def _collect_supplied_run_kwargs(
    *,
    filter: str,
    shadow_suffix: str | None,
    governance_override: dict | None,
    idempotency_key: str | None,
) -> dict[str, Any]:
    """Build the ``kwargs`` dict handed to :meth:`RockyResource._apply_resolvers`.

    The three resolver-eligible kwargs (``shadow_suffix``,
    ``governance_override``, ``idempotency_key``) have typed signatures on
    the public run methods, so at runtime we can't distinguish "caller
    passed ``None``" from "caller didn't pass anything". We follow the
    spec's caller-wins rule by treating **only non-``None`` values as
    supplied** — explicit ``None`` and omission both let the resolver
    fire. The tradeoff is documented on each public run method.

    ``filter`` is always included because :class:`ResolverContext` exposes
    it to resolvers for disambiguation (e.g. governance-override lookup
    keyed on the filter value).
    """
    kwargs: dict[str, Any] = {"filter": filter}
    if shadow_suffix is not None:
        kwargs["shadow_suffix"] = shadow_suffix
    if governance_override is not None:
        kwargs["governance_override"] = governance_override
    if idempotency_key is not None:
        kwargs["idempotency_key"] = idempotency_key
    return kwargs


class ResolverContext(BaseModel):
    """Read-only snapshot handed to each per-call kwarg resolver.

    Resolvers are closures users register on :class:`RockyResource` to inject
    kwargs derived from Dagster run context on every ``run`` / ``run_streaming``
    / ``run_pipes`` call. The context is **frozen** — signature stability
    matters because resolvers are user-authored closures imported across
    module boundaries.

    Attributes:
        context: The Dagster execution context for the call. ``None`` when
            the resolver fires from :meth:`RockyResource.run` (which doesn't
            accept a context param). Resolvers that need a context must
            handle ``None`` (typically by returning ``None`` to no-op).
        filter: The positional ``filter`` kwarg passed to the run method.
        method: Which run method triggered the resolver.
        supplied_kwargs: Snapshot of the kwargs the caller explicitly
            supplied. Lets resolvers bail early when the caller already set
            a value (though ``_apply_resolvers`` already skips resolution
            for present kwargs — this field is for resolvers that want to
            branch on *other* caller-supplied kwargs).
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    # Typed as ``Any`` rather than ``AssetExecutionContext | OpExecutionContext``
    # because Pydantic's is-instance validation would reject test doubles
    # (MagicMock) — and because Dagster occasionally hands callers wrapper
    # context objects that aren't instance-of the public classes. Resolvers
    # should treat ``context`` as a duck-typed Dagster context and only reach
    # for attributes the Dagster docs promise (run, tags, log, ...).
    context: Any = None
    filter: str | None = None
    method: Literal["run", "run_streaming", "run_pipes"]
    supplied_kwargs: dict[str, Any]


#: Type alias for user-authored per-call kwarg resolvers. Each resolver is a
#: callable taking a :class:`ResolverContext` and returning either a value
#: for the target kwarg or ``None`` to leave it unset.
Resolver = Callable[[ResolverContext], Any]


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
        shadow_suffix_fn: Optional resolver that produces ``shadow_suffix`` per
            call when the caller doesn't supply one. See :class:`ResolverContext`
            for the closure signature. Returning ``None`` is a no-op.
        governance_override_fn: Optional resolver for ``governance_override``.
            Same semantics as ``shadow_suffix_fn``.
        idempotency_key_fn: Optional resolver for ``idempotency_key``.
            Same semantics as ``shadow_suffix_fn``.
    """

    binary_path: str = "rocky"
    config_path: str = "rocky.toml"
    state_path: str = ".rocky-state.redb"
    models_dir: str = "models"
    contracts_dir: str | None = None
    server_url: str | None = None
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    #: Run ``rocky doctor`` at resource startup and gate execution on
    #: the result. Defaults to ``False`` — doctor is *not* invoked, and
    #: startup cost stays zero for users who don't opt in. When
    #: ``True``, :meth:`setup_for_execution` runs doctor once per
    #: resource initialization and may raise :class:`dagster.Failure`
    #: based on :attr:`strict_doctor_checks`.
    strict_doctor: bool = False
    #: Per-check allowlist for the strict doctor gate. Only meaningful
    #: when :attr:`strict_doctor` is ``True``.
    #:
    #: * Empty list (default) — fail on *any* critical check. The
    #:   fail-fast-on-anything-critical shape.
    #: * Non-empty — fail only when a critical check whose ``name``
    #:   appears in this list fires. Critical checks outside the list
    #:   are logged as warnings so operators still see them.
    #:
    #: Non-critical severities (``healthy``, ``warning``) never raise
    #: regardless of this list — warnings are logged at ``warning``
    #: level. A failure here blocks the Dagster run from launching
    #: rather than producing a mid-run error.
    strict_doctor_checks: list[str] = []

    #: Optional resolver that produces a ``shadow_suffix`` per ``run`` /
    #: ``run_streaming`` / ``run_pipes`` call. Fires only when the caller
    #: didn't supply ``shadow_suffix`` (or supplied ``None``). See
    #: :class:`ResolverContext` for the closure signature. Returning ``None``
    #: leaves the kwarg unset. Pair with
    #: :func:`.branch_deploy.shadow_suffix_resolver` for the common branch-
    #: deploy case.
    # Callable fields aren't valid Dagster config schema entries, so we
    # mark them as ``resource_dependency`` via ``typing.Annotated`` —
    # Dagster's canonical escape hatch for non-config resource attrs
    # (see ``_is_annotated_as_resource_type`` in
    # ``dagster/_config/pythonic_config/resource.py``). Using the
    # literal string marker instead of ``dg.ResourceDependency[...]``
    # keeps the annotation robust under ``from __future__ import annotations``,
    # where the generic-alias form fails Pydantic validation because
    # the string annotation isn't resolved back to the marker type.
    #
    # This also ensures the resolver closures survive Dagster's resource
    # lifecycle: at execution time Dagster rebuilds the resource via
    # ``self.__class__(**public_field_values)`` (see
    # ``ConfigurableResourceFactory._with_updated_values``). ``Annotated``
    # resource-dependency fields participate in that rebuild as public
    # fields, so the registered resolvers carry through to the per-asset
    # call. ``PrivateAttr``-backed fields did *not* survive that rebuild
    # and would silently drop resolvers mid-materialize — the end-to-end
    # test ``test_resolvers_survive_dagster_materialize_lifecycle`` pins
    # this regression.
    shadow_suffix_fn: Annotated[Resolver | None, "resource_dependency"] = None
    governance_override_fn: Annotated[Resolver | None, "resource_dependency"] = None
    idempotency_key_fn: Annotated[Resolver | None, "resource_dependency"] = None

    # Instance-level cache for the version check (not a Dagster config field).
    _version_checked: bool = False

    # ------------------------------------------------------------------ #
    # Per-call kwarg resolvers                                           #
    # ------------------------------------------------------------------ #

    def _apply_resolvers(
        self,
        context: dg.AssetExecutionContext | dg.OpExecutionContext | None,
        method: Literal["run", "run_streaming", "run_pipes"],
        kwargs: dict[str, Any],
    ) -> dict[str, Any]:
        """Inject resolver-produced values into ``kwargs`` in place.

        For each ``(kwarg_name, resolver_fn)`` pair, fire the resolver only
        when the caller didn't supply the kwarg (i.e. it's absent from
        ``kwargs``). Caller-supplied values always win. Resolvers that
        return ``None`` are treated as a no-op so they can conditionally
        opt in (e.g. :func:`branch_deploy_shadow_suffix` returns ``None``
        outside a branch deploy).

        Exceptions raised by a resolver propagate as :class:`dg.Failure`
        with the resolver's ``__qualname__`` in the description. A
        ``dg.Failure`` raised directly by a resolver is preserved so the
        resolver can surface its own operator-friendly error.

        Args:
            context: Dagster execution context for the call, or ``None``
                for :meth:`run` (which has no context param).
            method: Which run method triggered this resolver invocation.
                Passed into the :class:`ResolverContext` so resolvers can
                specialize behaviour per method.
            kwargs: The mutable kwargs dict being assembled for
                :meth:`_build_run_args`. This function mutates the dict
                in place and also returns it for convenience.

        Returns:
            The same ``kwargs`` dict, with resolver-produced values added
            for any absent keys whose resolver returned a non-``None``
            value.
        """
        rc = ResolverContext(
            context=context,
            filter=kwargs.get("filter"),
            method=method,
            supplied_kwargs=dict(kwargs),
        )
        for kw, fn in (
            ("shadow_suffix", self.shadow_suffix_fn),
            ("governance_override", self.governance_override_fn),
            ("idempotency_key", self.idempotency_key_fn),
        ):
            if fn is None or kw in kwargs:
                continue
            try:
                value = fn(rc)
            except dg.Failure:
                # Preserve resolver-raised dg.Failure so the resolver can
                # surface its own operator-friendly message (e.g.
                # "Could not determine target client ...").
                raise
            except Exception as exc:
                qualname = getattr(fn, "__qualname__", repr(fn))
                raise dg.Failure(
                    description=(f"resolver {qualname!r} for {kw!r} raised: {exc}"),
                ) from exc
            if value is not None:
                kwargs[kw] = value
        return kwargs

    # ------------------------------------------------------------------ #
    # Startup hook — opt-in strict rocky doctor gate                     #
    # ------------------------------------------------------------------ #

    def setup_for_execution(self, context: dg.InitResourceContext) -> None:
        """Run the optional :meth:`rocky doctor` startup gate.

        Invoked by Dagster once per resource initialization before any
        asset / op body executes. When :attr:`strict_doctor` is
        ``False`` (the default) this is a cheap no-op — we skip the
        subprocess entirely to keep cold-start overhead zero for users
        who don't opt in.

        When enabled, ``rocky doctor`` runs and its result is triaged:

        * Non-critical severities (``healthy``, ``warning``) are never
          failed on. Warnings are logged at ``warning`` so operators
          still see them.
        * Critical checks in :attr:`strict_doctor_checks` (or *all*
          critical checks when the list is empty) raise a
          :class:`dagster.Failure`, preventing the run from starting.
        * Critical checks *outside* the allowlist are logged at
          ``warning`` so they're visible without being fatal — this is
          the point of per-check strictness.

        The binary itself failing (missing, timeout, malformed JSON)
        also raises :class:`dagster.Failure`: when the user has opted
        into a strict gate, "couldn't run doctor" is more aligned with
        fail-fast than "silently pretend everything's fine."
        """
        if not self.strict_doctor:
            return

        log = context.log if context is not None and context.log is not None else _log

        try:
            report = self.doctor()
        except dg.Failure as exc:
            # User opted in to strict doctor but the binary can't run —
            # the whole point of this hook is to fail fast on
            # infrastructure problems, so surface the failure.
            description = str(exc.description or exc)
            raise dg.Failure(
                description=(
                    "rocky doctor could not execute during RockyResource startup "
                    f"(strict_doctor=True, checks={self.strict_doctor_checks!r}): "
                    f"{description}"
                ),
            ) from exc

        log.info(
            f"rocky doctor: overall={report.overall}, {len(report.checks)} check(s) "
            f"(strict_doctor=True, checks={self.strict_doctor_checks!r})"
        )

        strict_failures: list[str] = []
        warn_failures: list[str] = []
        for check in report.checks:
            status = check.status.value if hasattr(check.status, "value") else str(check.status)
            if status.lower() != "critical":
                # healthy / warning — surface warnings but never fail.
                if status.lower() == "warning":
                    log.warning(f"rocky doctor [{check.name}]: {check.message}")
                continue

            if self._is_strict_check(check.name):
                strict_failures.append(f"{check.name}: {check.message}")
            else:
                warn_failures.append(f"{check.name}: {check.message}")
                log.warning(f"rocky doctor [{check.name}] critical (non-strict): {check.message}")

        if strict_failures:
            raise dg.Failure(
                description=(
                    "rocky doctor reported critical issues on strict checks: "
                    + "; ".join(strict_failures)
                ),
                metadata={
                    "strict_failures": dg.MetadataValue.text("\n".join(strict_failures)),
                    "non_strict_warnings": dg.MetadataValue.text(
                        "\n".join(warn_failures) if warn_failures else "(none)"
                    ),
                    "strict_doctor_checks": dg.MetadataValue.text(repr(self.strict_doctor_checks)),
                },
            )

    def _is_strict_check(self, check_name: str) -> bool:
        """Return ``True`` when ``check_name`` is treated as strict.

        * Empty allowlist → every critical check is strict (fail-on-any).
        * Non-empty → only listed names are strict.
        """
        if not self.strict_doctor_checks:
            return True
        return check_name in self.strict_doctor_checks

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

        # Strip pre-release / build suffix before semver compare so dev
        # builds like ``1.17.4-dev``, ``1.17.4-pre``, ``1.17.4-rc.2`` and
        # ``1.17.4+sha.abc`` all gate against the matching release. Without
        # this, ``int("4-dev")`` fails the parse and the version check
        # silently skips — which is exactly how a too-old dev build slips
        # past the gate.
        core_version = version_str.split("-", 1)[0].split("+", 1)[0]

        try:
            detected = tuple(int(p) for p in core_version.split(".")[:3])
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
            metadata={
                "stderr": dg.MetadataValue.text(_truncate_stderr_for_metadata(result.stderr)),
            },
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
        starts dedicated reader threads on each pipe, and relies on an
        external watchdog thread (not ``communicate(timeout=)``) to
        enforce the wall-clock timeout.

        Rocky's tracing logs go to stderr (see
        ``engine/crates/rocky-observe/src/tracing_setup.rs``) and the
        structured ``--output json`` payload goes to stdout, so the
        separation is clean: users see human-readable progress lines
        in the Dagster run viewer in real time, and the integration
        gets the typed result back at the end.

        Concurrency model (fixes the 2026-04-18 / 2026-04-19 prod hangs)::

            Popen(..., start_new_session=True)   # rocky gets its own pgid
              ├── stderr forwarder thread         # SOLE reader of proc.stderr
              ├── stdout accumulator thread       # SOLE reader of proc.stdout
              ├── watchdog thread                 # os.killpg on timeout
              └── main thread: proc.wait()        # blocks until external kill

        The previous implementation combined the stderr forwarder with
        ``proc.communicate(timeout=…)``, which also reads the stderr
        pipe via raw ``os.read``. Two concurrent readers on the same
        pipe FD violates CPython's documented ``subprocess`` contract
        ("the process must have been started with the stream set to
        ``PIPE`` and the stream must not be read from otherwise") —
        under stderr traffic the ``TimeoutExpired`` path intermittently
        failed to fire and the subprocess hung for hours. The watchdog
        pattern kills the process externally via
        ``os.killpg(SIGKILL)``, so enforcement is pipe-FD-independent.

        For full Dagster Pipes integration with structured per-model
        materialization and check events, use
        :meth:`RockyResource.run_pipes` instead. This method provides
        live log streaming + final result parsing without requiring
        the Pipes protocol.

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
        # POSIX-only process group isolation. Lets os.killpg reach any
        # child processes rocky spawns (adapter subprocesses, hook
        # scripts, …) when the watchdog fires. Windows has no direct
        # equivalent; ``proc.kill()`` fallback in ``_kill_process_group``
        # handles the single-process case, which is all rocky does on
        # Windows today.
        popen_kwargs: dict[str, object] = {
            "stdout": subprocess.PIPE,
            "stderr": subprocess.PIPE,
            "text": True,
            "bufsize": 1,  # line-buffered so the readers see lines as they're written
        }
        if os.name != "nt":
            popen_kwargs["start_new_session"] = True

        t0 = time.monotonic()
        try:
            proc = subprocess.Popen(  # noqa: S603 - cmd is fully constructed from typed args
                cmd,
                **popen_kwargs,
            )
        except FileNotFoundError:
            raise dg.Failure(
                description=(
                    f"Rocky binary not found at '{self.binary_path}'. "
                    "Install from: https://github.com/rocky-data/rocky/releases"
                ),
            ) from None

        _log.info(
            "rocky subprocess started: pid=%s timeout_s=%s cmd=%s",
            proc.pid,
            self.timeout_seconds,
            # Redact credential-bearing argv values before they hit the log.
            # The subprocess itself was already spawned with the real ``cmd``;
            # this only sanitises what gets written into Dagster logs / the
            # operator's terminal. See :func:`_redact_argv`.
            " ".join(_redact_argv(cmd)),
        )

        # Sole-reader threads for the two pipes. Must be started before
        # proc.wait() so they drain concurrently with the subprocess
        # writing to its pipe buffers (otherwise a large write blocks
        # rocky and we deadlock).
        stderr_lines: list[str] = []
        stdout_chunks: list[str] = []
        stderr_reader = threading.Thread(
            target=_forward_stderr_to_context,
            args=(proc.stderr, context, stderr_lines),
            daemon=True,
            name="rocky-stderr-forwarder",
        )
        stdout_reader = threading.Thread(
            target=_accumulate_stdout,
            args=(proc.stdout, stdout_chunks),
            daemon=True,
            name="rocky-stdout-accumulator",
        )
        stderr_reader.start()
        stdout_reader.start()

        # Watchdog: if ``fired`` is not set within ``timeout_seconds``,
        # hard-kill the process group. Uses an Event so the main
        # thread can dismiss the watchdog after a clean exit without
        # racing on a shared flag.
        fired = threading.Event()

        def _watchdog() -> None:
            if not fired.wait(self.timeout_seconds):
                _kill_process_group(proc)
                fired.set()

        watchdog = threading.Thread(
            target=_watchdog,
            daemon=True,
            name="rocky-watchdog",
        )
        watchdog.start()

        try:
            # No timeout on wait(): enforcement comes from the watchdog
            # killing the process externally. This is the critical
            # change — `communicate(timeout=)` was racing with the
            # stderr reader on the same pipe FD and failing to fire.
            proc.wait()
        finally:
            # Order matters: dismiss the watchdog first (so it stops
            # waiting on the timeout), then drain the reader threads.
            # ``proc.wait()`` has already closed the pipes on the
            # subprocess side, so the readers will hit EOF and exit.
            fired.set()
            watchdog.join(timeout=1.0)
            stderr_reader.join(timeout=2.0)
            stdout_reader.join(timeout=2.0)

        duration_ms = int((time.monotonic() - t0) * 1000)
        stdout = "".join(stdout_chunks)

        # On POSIX, a subprocess killed by SIGKILL has returncode == -SIGKILL.
        # On Windows, proc.kill() leaves returncode == 1 (no way to
        # distinguish from a native failure), but the watchdog firing
        # on Windows is still reliable because fired.wait() + proc.kill()
        # behave identically. We treat the POSIX signal marker as the
        # canonical timeout signal.
        killed_by_watchdog = os.name != "nt" and proc.returncode == -signal.SIGKILL

        outcome: str
        if killed_by_watchdog:
            outcome = "timeout-killed"
        elif proc.returncode == 0:
            outcome = "success"
        elif allow_partial and stdout.lstrip().startswith("{"):
            outcome = "partial-success"
        else:
            outcome = "failure"

        _log.info(
            "rocky subprocess ended: pid=%s returncode=%s duration_ms=%d outcome=%s",
            proc.pid,
            proc.returncode,
            duration_ms,
            outcome,
        )

        if killed_by_watchdog:
            raise dg.Failure(
                description=(
                    f"Rocky command timed out after {self.timeout_seconds}s (watchdog-killed)"
                ),
                metadata={
                    "stderr_tail": dg.MetadataValue.text(
                        _truncate_stderr_for_metadata("\n".join(stderr_lines[-20:]))
                    ),
                    "duration_ms": dg.MetadataValue.int(duration_ms),
                    "pid": dg.MetadataValue.int(proc.pid),
                },
            )

        if proc.returncode == 0:
            return stdout

        # Partial-success: same semantics as the buffered path.
        if allow_partial and stdout.lstrip().startswith("{"):
            return stdout

        raise dg.Failure(
            description=f"Rocky command failed (exit {proc.returncode})",
            metadata={
                "stderr_tail": dg.MetadataValue.text(
                    _truncate_stderr_for_metadata("\n".join(stderr_lines[-20:]))
                ),
                "duration_ms": dg.MetadataValue.int(duration_ms),
            },
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

    def discover(self, *, pipeline: str | None = None) -> DiscoverResult:
        """Run ``rocky discover`` and return the parsed result.

        Args:
            pipeline: Pipeline name (required when multiple pipelines are
                defined in ``rocky.toml``).
        """
        args = ["discover"]
        if pipeline is not None:
            args.extend(["--pipeline", pipeline])
        return _parse_rocky_json(self._run_rocky(args), DiscoverResult, command="discover")

    def plan(self, filter: str) -> PlanResult:
        """Run ``rocky plan --filter <key=value>`` and return the parsed result."""
        return _parse_rocky_json(
            self._run_rocky(["plan", "--filter", filter]),
            PlanResult,
            command="plan",
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
            shadow_suffix: When set, enables shadow mode and uses the given
                suffix for shadow table names (e.g. ``"_dagster_pr_42"``).
                Typically derived from :func:`.branch_deploy.branch_deploy_shadow_suffix`.
            idempotency_key: Caller-supplied opaque key used to dedup this
                run against prior runs with the same key. If a prior run with
                this key completed successfully, this call returns a
                :class:`RunResult` with ``status = "skipped_idempotent"`` and
                ``skipped_by_run_id`` set to the prior ``run_id``; no rocky
                subprocess work is performed beyond the short-circuit
                output. If another caller currently holds the key's
                in-flight claim, exits with ``status = "skipped_in_flight"``.

                Defense-in-depth below Dagster's ``run_key`` — catches pod
                retries, Kafka re-delivery, webhook duplicates, cron races.
                Works on state backends ``local``, ``valkey``, ``tiered``,
                ``s3``, and ``gcs``.

                ⚠️ Keys are stored verbatim in the state store; do NOT put
                secrets in idempotency keys.

        Note on resolver interaction:
            Per-call resolvers registered on the resource (``shadow_suffix_fn``,
            ``governance_override_fn``, ``idempotency_key_fn``) fire for the
            matching kwarg only when that kwarg is **absent** from the call.
            Because Python can't distinguish "caller explicitly passed
            ``None``" from "caller didn't pass anything", an explicit
            ``None`` is treated as absent and resolvers fire. Pass the
            non-``None`` value you want to win against the resolver.
        """
        resolved = self._apply_resolvers(
            context=None,
            method="run",
            kwargs=_collect_supplied_run_kwargs(
                filter=filter,
                shadow_suffix=shadow_suffix,
                governance_override=governance_override,
                idempotency_key=idempotency_key,
            ),
        )
        _validate_governance_override(resolved.get("governance_override"))
        args = self._build_run_args(
            filter,
            governance_override=resolved.get("governance_override"),
            pipeline=pipeline,
            run_models=run_models,
            partition=partition,
            partition_from=partition_from,
            partition_to=partition_to,
            latest=latest,
            missing=missing,
            lookback=lookback,
            parallel=parallel,
            shadow_suffix=resolved.get("shadow_suffix"),
            idempotency_key=resolved.get("idempotency_key"),
        )
        return _parse_rocky_json(
            self._run_rocky(args, allow_partial=True), RunResult, command="run"
        )

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
        shadow_suffix: str | None = None,
        idempotency_key: str | None = None,
    ) -> RunResult:
        """``rocky run`` with live stderr streaming to ``context.log``.

        Same semantics as :meth:`run` but spawns the binary via
        ``subprocess.Popen`` and forwards rocky's stderr (where the
        engine's tracing layer writes ``info!()`` / ``warn!()`` macros)
        to ``context.log.info`` line-by-line as the run progresses.

        Use this method from inside a Dagster ``@multi_asset`` /
        ``@op`` for runs longer than a few seconds — users will see
        progress in the run viewer as it happens, instead of waiting
        for the full output to buffer at the end. For full Dagster
        Pipes integration with structured materialization and check
        events, use :meth:`run_pipes` instead.

        What users get from this method:

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
            shadow_suffix: Same as :meth:`run`.

        Returns:
            The parsed :class:`RunResult` from stdout after the
            subprocess exits.
        """
        resolved = self._apply_resolvers(
            context=context,
            method="run_streaming",
            kwargs=_collect_supplied_run_kwargs(
                filter=filter,
                shadow_suffix=shadow_suffix,
                governance_override=governance_override,
                idempotency_key=idempotency_key,
            ),
        )
        _validate_governance_override(resolved.get("governance_override"))
        args = self._build_run_args(
            filter,
            governance_override=resolved.get("governance_override"),
            run_models=run_models,
            partition=partition,
            partition_from=partition_from,
            partition_to=partition_to,
            latest=latest,
            missing=missing,
            lookback=lookback,
            parallel=parallel,
            shadow_suffix=resolved.get("shadow_suffix"),
            idempotency_key=resolved.get("idempotency_key"),
        )
        return _parse_rocky_json(
            self._run_rocky_streaming(args, context, allow_partial=True),
            RunResult,
            command="run (streaming)",
        )

    def _build_run_args(
        self,
        filter: str,
        *,
        governance_override: dict | None,
        pipeline: str | None = None,
        run_models: bool,
        partition: str | None,
        partition_from: str | None,
        partition_to: str | None,
        latest: bool,
        missing: bool,
        lookback: int | None,
        parallel: int | None,
        shadow_suffix: str | None = None,
        idempotency_key: str | None = None,
    ) -> list[str]:
        """Shared argv builder used by :meth:`run`, :meth:`run_streaming`, and :meth:`run_pipes`.

        Single source of truth for the flag plumbing so adding a new
        flag is a one-place change. The flags are defensive:
        ``partition_from`` without ``partition_to`` emits neither
        (rocky requires both for range mode), and the engine enforces
        mutual-exclusion via clap so we trust it to error helpfully if
        multiple selection flags are passed simultaneously.
        """
        args = ["run", "--filter", filter]
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
        shadow_suffix: str | None = None,
        idempotency_key: str | None = None,
        pipes_client: dg.PipesSubprocessClient | None = None,
        asset_key_fn: Callable[[list[str]], dg.AssetKey | None] | None = None,
        include_keys: set[dg.AssetKey] | None = None,
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
        * ``AssetCheckEvaluation`` per Rocky check and drift observation
        * ``log`` events for the rocky run starting / completing
          messages and drift action details

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
            governance_override / run_models / partition /
            shadow_suffix / ... :
                Same as :meth:`run` and :meth:`run_streaming`. Threaded
                into the rocky CLI command via :meth:`_build_run_args`.
            pipes_client: Optional pre-configured
                ``PipesSubprocessClient`` (for tests or for users who
                need custom env / cwd / message_reader / context_injector).
                When supplied, ``asset_key_fn`` / ``include_keys`` are
                ignored — the caller's client wins. When ``None``, a
                fresh client is constructed with Dagster defaults, plus
                a :class:`RockyPipesMessageReader` when either
                ``asset_key_fn`` or ``include_keys`` is non-``None``.
            asset_key_fn: Optional transform applied to each Pipes
                event's asset key at the reader layer. The function
                receives the slash-split path the engine emits (a
                ``[source_type, *components, table]`` list) and returns
                a :class:`dagster.AssetKey` — or ``None`` to drop the
                event. Used by :class:`RockyComponent` to translate
                engine-native paths into Dagster-translated keys before
                they reach the handler.
            include_keys: Optional allowlist of Dagster asset keys.
                Events whose resolved key is not in the set are dropped
                before reaching Dagster's materialization bookkeeping.
                Intended for subset-aware multi-assets that must ignore
                events for tables outside the requested subset.

        Returns:
            A :class:`PipesClientCompletedInvocation` ready for
            ``.get_results()`` to extract materialization events.

        Raises:
            dg.Failure: If the subprocess exits non-zero. Pipes also
                propagates a Failure when the subprocess crashes
                without sending a Pipes ``closed`` message.
        """
        resolved = self._apply_resolvers(
            context=context,
            method="run_pipes",
            kwargs=_collect_supplied_run_kwargs(
                filter=filter,
                shadow_suffix=shadow_suffix,
                governance_override=governance_override,
                idempotency_key=idempotency_key,
            ),
        )
        _validate_governance_override(resolved.get("governance_override"))
        args = self._build_run_args(
            filter,
            governance_override=resolved.get("governance_override"),
            run_models=run_models,
            partition=partition,
            partition_from=partition_from,
            partition_to=partition_to,
            latest=latest,
            missing=missing,
            lookback=lookback,
            parallel=parallel,
            shadow_suffix=resolved.get("shadow_suffix"),
            idempotency_key=resolved.get("idempotency_key"),
        )
        if pipes_client is not None:
            client = pipes_client
        elif asset_key_fn is not None or include_keys is not None:
            # Custom reader only when the caller actually asked for
            # translation / filtering — keeps the default path identical
            # to canonical Dagster Pipes and avoids regressions for
            # users who don't use :class:`RockyComponent`.
            client = dg.PipesSubprocessClient(
                message_reader=RockyPipesMessageReader(
                    asset_key_fn=asset_key_fn,
                    include_keys=include_keys,
                ),
            )
        else:
            client = dg.PipesSubprocessClient()
        return client.run(
            context=context,
            command=self._build_cmd(args),
        )

    def state(self) -> StateResult:
        """Run ``rocky state`` and return the parsed result."""
        return _parse_rocky_json(self._run_rocky(["state"]), StateResult, command="state")

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
            return _parse_rocky_json(
                self._http_get("/api/v1/compile"), CompileResult, command="compile"
            )

        args = ["compile", "--models", self.models_dir]
        if self.contracts_dir is not None:
            args.extend(["--contracts", self.contracts_dir])
        if model_filter is not None:
            args.extend(["--model", model_filter])
        return _parse_rocky_json(
            self._run_rocky(args, allow_partial=True),
            CompileResult,
            command="compile",
        )

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
                return _parse_rocky_json(output, ColumnLineageResult, command="lineage")
            output = self._http_get(f"/api/v1/models/{target}/lineage")
            return _parse_rocky_json(output, ModelLineageResult, command="lineage")

        args = ["lineage", "--models", self.models_dir, target]
        if column is not None:
            args.extend(["--column", column])
        output = self._run_rocky(args)
        if column is not None:
            return _parse_rocky_json(output, ColumnLineageResult, command="lineage")
        return _parse_rocky_json(output, ModelLineageResult, command="lineage")

    # ------------------------------------------------------------------ #
    # DAG + per-model execution                                          #
    # ------------------------------------------------------------------ #

    def dag(self, *, column_lineage: bool = False) -> DagResult:
        """Run ``rocky dag`` and return the full unified DAG.

        Returns a :class:`DagResult` containing all pipeline stages as nodes
        with enriched metadata (target, strategy, freshness, partition shape,
        upstream dependencies). Consumers can build a complete Dagster asset
        graph from a single call.

        Args:
            column_lineage: When ``True``, include column-level lineage
                edges (requires model compilation; slower).
        """
        args = ["dag", "--models", self.models_dir]
        if self.contracts_dir is not None:
            args.extend(["--contracts", self.contracts_dir])
        if column_lineage:
            args.append("--column-lineage")
        return _parse_rocky_json(self._run_rocky(args), DagResult, command="dag")

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

        ``--model`` is an alternative execution path to ``--filter`` — it
        skips the replication phase and executes only the named model.
        Dagster uses this for per-asset materialization when it controls
        the DAG scheduling.

        Args:
            model_name: Name of the compiled model to execute.
            filter: Optional source filter (passed alongside ``--model``).
            partition: Single partition key for time-interval models.
            partition_from: Start of a closed partition range (inclusive).
            partition_to: End of a closed partition range (inclusive).
            latest: Run the partition containing now() (UTC).
            missing: Run partitions missing from the state store.
            lookback: Recompute previous N partitions.
            parallel: Run N partitions concurrently.
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
        return _parse_rocky_json(
            self._run_rocky(args, allow_partial=True), RunResult, command="run"
        )

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
        return _parse_rocky_json(
            self._run_rocky(args, allow_partial=True), TestResult, command="test"
        )

    def ci(self) -> CiResult:
        """Run ``rocky ci`` (compile + test) and return the parsed result."""
        args = ["ci", "--models", self.models_dir]
        if self.contracts_dir is not None:
            args.extend(["--contracts", self.contracts_dir])
        return _parse_rocky_json(self._run_rocky(args, allow_partial=True), CiResult, command="ci")

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
        """Run ``rocky metrics`` and return the parsed result.

        When ``server_url`` is configured, fetches from the HTTP API instead.

        Args:
            model: Model name.
            trend: If ``True``, show trend over recent runs.
            column: Optional column to filter null rate trends.
            alerts: If ``True``, include quality alerts.
        """
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
        return _parse_rocky_json(self._run_rocky(args), MetricsResult, command="metrics")

    def optimize(self, model: str | None = None) -> OptimizeResult:
        """Run ``rocky optimize`` and return the parsed result.

        Args:
            model: Optional model name to filter analysis.
        """
        args: list[str] = ["optimize"]
        if model is not None:
            args.extend(["--model", model])
        return _parse_rocky_json(self._run_rocky(args), OptimizeResult, command="optimize")

    def cost(self, run_id: str = "latest") -> CostOutput:
        """Run ``rocky cost <run_id>`` and return the historical cost attribution.

        Reads the persisted ``RunRecord`` from the embedded state store and
        rolls per-model duration / bytes_scanned / bytes_written / cost_usd up
        from the stored materialization records. The same formula
        ``rocky_core::cost::compute_observed_cost_usd`` applied at the end of a
        live run is re-applied here, so the historical surface stays consistent
        with the per-run summary on ``RunOutput``.

        Args:
            run_id: Specific run ID to attribute, or the literal ``"latest"``
                (the default) to look up the most recent recorded run.
        """
        return _parse_rocky_json(self._run_rocky(["cost", run_id]), CostOutput, command="cost")

    # ------------------------------------------------------------------ #
    # AI Level 3 commands                                                #
    # ------------------------------------------------------------------ #

    def ai(self, intent: str, format: str = "rocky") -> AiResult:
        """Generate a model from a natural-language intent."""
        return _parse_rocky_json(
            self._run_rocky(["ai", intent, "--format", format]),
            AiResult,
            command="ai",
        )

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
        return _parse_rocky_json(self._run_rocky(args), AiSyncResult, command="ai sync")

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
        return _parse_rocky_json(self._run_rocky(args), AiExplainResult, command="ai explain")

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
        return _parse_rocky_json(self._run_rocky(args), AiTestResult, command="ai test")

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
        return _parse_rocky_json(
            self._run_rocky(args),
            ValidateMigrationResult,
            command="validate",
        )

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
        return _parse_rocky_json(self._run_rocky(args), ConformanceResult, command="conformance")

    # ------------------------------------------------------------------ #
    # Doctor and resume commands                                         #
    # ------------------------------------------------------------------ #

    def doctor(self, *, check: str | None = None) -> DoctorResult:
        """Run ``rocky doctor`` and return the parsed health-check results.

        Args:
            check: Optional single-check id (e.g. ``"state_rw"``) forwarded
                as ``--check <id>``. When set, the engine runs only that
                check — the output is still a :class:`DoctorResult` with
                the same schema, just fewer entries in ``checks``. The set
                of valid ids lives on the engine side; invalid values are
                surfaced by the engine rather than pre-validated here.
        """
        args = ["doctor"]
        if check is not None:
            args.extend(["--check", check])
        return _parse_rocky_json(self._run_rocky(args), DoctorResult, command="doctor")

    def state_health(self, *, probe_write: bool = False) -> StateHealthResult:
        """Return a live snapshot of Rocky's state-backend health.

        Aggregates two already-shipped observability signals — the
        configured ``[state] backend`` plus the most recent run
        recorded in the state store — into a single typed snapshot
        that Dagster sensors / schedules / asset checks can gate on
        without having to shell out to ``rocky doctor`` on every
        tick or log-scrape state-sync events.

        The cheap path (the default, ``probe_write=False``) does one
        ``rocky history`` subprocess plus a ``tomllib`` read of
        :attr:`config_path` — bounded sub-second on any backend. When
        ``probe_write=True`` we additionally invoke
        ``rocky doctor --check state_rw`` (which reuses the engine's
        ``probe_state_backend`` helper to do a bounded put/get/delete
        round-trip against the configured backend) and translate its
        ``state_rw`` check into the
        :attr:`~.types.StateHealthResult.probe_outcome` /
        :attr:`~.types.StateHealthResult.probe_duration_ms` /
        :attr:`~.types.StateHealthResult.probe_error` fields.

        Tolerance: the accessor is deliberately designed to survive a
        sensor tick even when the underlying rocky binary is missing
        or the state store is unreadable — in both cases the recent-run
        fields degrade to ``None`` rather than raising. The probe
        branch surfaces failures through the ``probe_*`` fields too
        (``probe_outcome="error"``, ``probe_error=<description>``) so
        a caller has a single source of truth for state-backend
        liveness without wrapping this method in try/except.

        Delegates to :func:`dagster_rocky.health.state_health` to keep
        the resource thin and match the standalone
        :func:`~.health.rocky_healthcheck` pattern.

        Args:
            probe_write: When ``True``, run the ``state_rw`` put/get/delete
                probe. Default ``False`` — the cheap path.

        Returns:
            A :class:`~.types.StateHealthResult` describing the current
            state-backend health.
        """
        # Import locally to avoid a module-level cycle between
        # ``resource`` and ``health`` (health uses ``RockyResource`` via
        # ``TYPE_CHECKING``).
        from .health import state_health as _state_health

        return _state_health(self, probe_write=probe_write)

    def compliance(self, *, env: str | None = None) -> ComplianceOutput:
        """Run ``rocky compliance`` and return the parsed governance rollup.

        A thin wrapper over the engine's Wave B governance surface
        (``rocky compliance``) — a static resolver over the project's
        ``[classification]`` sidecars + ``[mask]`` / ``[mask.<env>]``
        policy blocks that answers "are all classified columns masked
        wherever policy says they should be?". The command makes no
        warehouse calls and is safe to invoke per materialization batch.

        Args:
            env: Optional environment label forwarded as ``--env <env>``.
                When set, masking status is evaluated only against that
                environment's ``[mask.<env>]`` block. When unset, the
                engine expands across the union of defaults and every
                named override block.

        Returns:
            :class:`~.types.ComplianceOutput` — the parsed rollup. See
            :meth:`~.types.ComplianceOutput.exceptions` for the
            unmasked-where-expected list and
            :meth:`~.types.ComplianceOutput.summary` for aggregate tallies.
        """
        args = ["compliance", "--output", "json"]
        if env is not None:
            args.extend(["--env", env])
        return _parse_rocky_json(self._run_rocky(args), ComplianceOutput, command="compliance")

    def retention_status(self, *, env: str | None = None) -> RetentionStatusOutput:
        """Run ``rocky retention-status`` and return the parsed per-model status.

        Reports which models declare a ``retention = "<N>[dy]"`` sidecar
        value and — once the engine's ``--drift`` warehouse probe lands
        — whether the warehouse's current retention matches. The
        command reads the project's model sidecars + state store only;
        no warehouse round-trip in v1, so it's safe to invoke per
        materialization batch.

        Args:
            env: Optional environment label forwarded as ``--env <env>``.

        Returns:
            :class:`~.types.RetentionStatusOutput` — one
            :class:`~.types.ModelRetentionStatus` entry per model.
            ``warehouse_days`` is always ``None`` until the engine's
            v2 ``--drift`` probe ships.
        """
        args = ["retention-status", "--output", "json"]
        if env is not None:
            args.extend(["--env", env])
        return _parse_rocky_json(
            self._run_rocky(args), RetentionStatusOutput, command="retention-status"
        )

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
        # FR-009 pre-flight: mirror the guard from :meth:`run` so a
        # resumed run catches the empty-workspace_ids footgun in-process
        # instead of paying a subprocess round-trip for the error.
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
        return _parse_rocky_json(
            self._run_rocky(args, allow_partial=True), RunResult, command="run"
        )
