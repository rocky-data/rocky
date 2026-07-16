"""RockyResource — Dagster adapter over the Rocky SDK client.

The resource is a thin Dagster wrapper around :class:`rocky_sdk.client.RockyClient`.
It exposes one Python method per Rocky CLI command; each delegates to the client
and translates the client's :class:`rocky_sdk.exceptions.RockyError` hierarchy into
``dagster.Failure`` (preserving the metadata operators are used to). The Dagster-
specific concerns stay here: per-call resolvers, the strict-doctor startup gate,
the governance pre-flight, live ``context.log`` streaming, and full Dagster Pipes
integration.

Three execution modes for ``rocky run``:

* :meth:`RockyResource.run` — buffered. Returns a ``RunResult``. For non-Dagster
  callers, prefer ``rocky_sdk.RockyClient`` directly.
* :meth:`RockyResource.run_streaming` — forwards the engine's live stderr to
  ``context.log`` line-by-line. Returns a ``RunResult``.
* :meth:`RockyResource.run_pipes` — full Dagster Pipes: structured
  materialization / check events streamed via the protocol.
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Annotated, Any, Literal

import dagster as dg
from pydantic import BaseModel, ConfigDict
from rocky_sdk import RockyClient
from rocky_sdk.client import DEFAULT_TIMEOUT_SECONDS, MIN_ROCKY_VERSION, ApplyResult
from rocky_sdk.exceptions import (
    RockyBinaryNotFoundError,
    RockyCommandError,
    RockyError,
    RockyOutputParseError,
    RockyServerError,
    RockyTimeoutError,
    RockyVersionError,
)

from .types import (
    AiContractOutput,
    AiExplainResult,
    AiResult,
    AiSyncResult,
    AiTestResult,
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
    StateHealthResult,
    StateResult,
    TestResult,
    ValidateMigrationResult,
)

_log = logging.getLogger(__name__)

# Re-exported for backward compatibility (``from dagster_rocky import MIN_ROCKY_VERSION``).
# Owned by the SDK so the resource and a standalone client agree on the floor.
__all__ = ["MIN_ROCKY_VERSION", "Resolver", "ResolverContext", "RockyResource"]

# Maximum bytes of stderr embedded into ``dg.Failure.metadata`` when a Rocky
# subprocess fails. Rocky's tracing layer can emit megabytes of context for a
# pathological run; the Dagster UI renders metadata inline and isn't designed
# to host blobs that large, so we clip and append a marker advertising the
# original byte count for operators who need the full payload.
_STDERR_METADATA_CAP_BYTES = 8192

# Bytes of stdout surfaced back to the operator when a Rocky command returns
# malformed or schema-violating JSON. Enough to spot a stray tracing line or
# truncated blob without dumping potentially MB of noise.
_JSON_ERROR_PREVIEW_BYTES = 500


def _truncate_stderr_for_metadata(stderr: str) -> str:
    """Cap ``stderr`` at :data:`_STDERR_METADATA_CAP_BYTES` for ``dg.Failure``.

    When the input exceeds the cap, the prefix is preserved and a single marker
    line is appended advertising the original size so operators who need the full
    payload know to look in the Dagster compute logs instead.
    """
    if len(stderr) <= _STDERR_METADATA_CAP_BYTES:
        return stderr
    original_bytes = len(stderr)
    return stderr[:_STDERR_METADATA_CAP_BYTES] + (
        f"\n... [truncated, original {original_bytes} bytes]"
    )


def _parse_error_to_failure(exc: RockyOutputParseError) -> dg.Failure:
    """Rebuild the operator-friendly ``dg.Failure`` for a parse failure."""
    command = exc.command or ""
    stdout = exc.stdout or ""
    preview = stdout[:_JSON_ERROR_PREVIEW_BYTES]
    if exc.kind == "validation":
        return dg.Failure(
            description=(
                f"rocky {command} output failed schema validation — "
                "see metadata for the raw stdout preview and pydantic error"
            ),
            metadata={
                "command": dg.MetadataValue.text(command),
                "stdout_preview": dg.MetadataValue.text(preview),
                "stdout_bytes": dg.MetadataValue.int(len(stdout)),
                "validation_error": dg.MetadataValue.text(exc.parse_error),
            },
        )
    if exc.kind == "json":
        return dg.Failure(
            description=(
                f"rocky {command} returned malformed JSON — see metadata for the stdout preview"
            ),
            metadata={
                "command": dg.MetadataValue.text(command),
                "stdout_preview": dg.MetadataValue.text(preview),
                "stdout_bytes": dg.MetadataValue.int(len(stdout)),
                "json_error": dg.MetadataValue.text(exc.parse_error),
            },
        )
    # Apply envelope problems.
    if exc.inner_result_preview is not None:
        return dg.Failure(
            description=(
                f"rocky {command} reported success=false on the apply envelope — "
                "the engine ran the plan but signalled the apply did not succeed. "
                "See metadata for the inner result payload."
            ),
            metadata={
                "command": dg.MetadataValue.text(command),
                "plan_id": dg.MetadataValue.text(exc.plan_id or ""),
                "plan_kind": dg.MetadataValue.text(exc.plan_kind or ""),
                "inner_result_preview": dg.MetadataValue.text(exc.inner_result_preview),
            },
        )
    return dg.Failure(
        description=(
            f"rocky {command} apply envelope missing inner result (expected dict under `result`)"
        ),
        metadata={
            "command": dg.MetadataValue.text(command),
            "plan_id": dg.MetadataValue.text(exc.plan_id or ""),
            "plan_kind": dg.MetadataValue.text(exc.plan_kind or ""),
        },
    )


def _rocky_error_to_failure(exc: RockyError) -> dg.Failure:
    """Translate a :class:`RockyError` into the ``dg.Failure`` operators expect.

    The single boundary between the SDK's typed exceptions and Dagster's failure
    surface. Each subtype rebuilds the description + metadata that the resource
    has historically produced, so the run-viewer experience is unchanged.
    """
    if isinstance(exc, RockyBinaryNotFoundError):
        return dg.Failure(description=str(exc))
    if isinstance(exc, RockyVersionError):
        return dg.Failure(
            description=(
                f"Rocky binary version {exc.detected_version} is below the minimum "
                f"required version {exc.min_version} for this dagster-rocky release. "
                f"Update: https://github.com/rocky-data/rocky/releases"
            ),
            metadata={
                "detected_version": dg.MetadataValue.text(exc.detected_version),
                "min_version": dg.MetadataValue.text(exc.min_version),
                "binary_path": dg.MetadataValue.text(exc.binary_path),
            },
        )
    if isinstance(exc, RockyTimeoutError):
        return dg.Failure(
            description=f"Rocky command timed out after {exc.timeout_seconds}s (watchdog-killed)",
            # A watchdog timeout is a deterministic "too slow", not a transient
            # error: re-running the same over-budget command at the same budget
            # re-fails by construction. Opt out of Dagster's retry machinery so a
            # blanket op RetryPolicy doesn't turn one over-budget run into 4x the
            # budget in futile retries. The remedy for a genuinely borderline run
            # is a larger budget (per-call via `timeout_fn`), not a same-budget
            # re-run. The retryable path (a transient circuit-breaker breach) is a
            # distinct raise in `component._quota_breach_failure` and is unaffected.
            allow_retries=False,
            metadata={
                "stderr_tail": dg.MetadataValue.text(
                    _truncate_stderr_for_metadata(exc.stderr_tail)
                ),
                "duration_ms": dg.MetadataValue.int(exc.duration_ms),
                "pid": dg.MetadataValue.int(exc.pid if exc.pid is not None else 0),
            },
        )
    if isinstance(exc, RockyOutputParseError):
        return _parse_error_to_failure(exc)
    if isinstance(exc, RockyServerError):
        return dg.Failure(
            description=f"Rocky server request failed: {exc.url}",
            metadata={"error": dg.MetadataValue.text(exc.error)},
        )
    if isinstance(exc, RockyCommandError):
        # Covers RockyPartialFailure (a subclass) — both surface as "failed (exit N)".
        return dg.Failure(
            description=f"Rocky command failed (exit {exc.returncode})",
            metadata={
                "stderr_tail": dg.MetadataValue.text(
                    _truncate_stderr_for_metadata(exc.stderr_tail)
                ),
                "duration_ms": dg.MetadataValue.int(exc.duration_ms),
            },
        )
    # RockyGovernanceError and any future RockyError — surface the message as-is.
    return dg.Failure(description=str(exc))


@contextlib.contextmanager
def _translating() -> Iterator[None]:
    """Re-raise any SDK :class:`RockyError` as the equivalent ``dagster.Failure``."""
    try:
        yield
    except RockyError as exc:
        raise _rocky_error_to_failure(exc) from exc


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
    * ``workspace_ids`` not a list → :class:`dagster.Failure`.
    * ``workspace_ids = []`` without ``allow_empty_workspace_ids = True``
      → :class:`dagster.Failure` (rejects the footgun; otherwise the
      reconciler would revoke every existing workspace binding).
    * ``workspace_ids = []`` with ``allow_empty_workspace_ids = True``
      → no-op (explicit consent to fully revoke).
    * ``workspace_ids`` non-empty list → no-op (normal reconcile).

    Raises:
        dagster.Failure: When ``override`` is neither ``None`` nor a dict, or
            when its ``workspace_ids`` shape would cause a silent full revoke.
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


def _collect_supplied_run_kwargs(
    *,
    filter: str,
    shadow_suffix: str | None,
    governance_override: dict | None,
    idempotency_key: str | None,
    timeout_seconds: int | None = None,
) -> dict[str, Any]:
    """Build the ``kwargs`` dict handed to :meth:`RockyResource._apply_resolvers`.

    The three resolver-eligible kwargs have typed signatures on the public run
    methods, so at runtime we can't distinguish "caller passed ``None``" from
    "caller didn't pass anything". We follow the spec's caller-wins rule by
    treating **only non-``None`` values as supplied** — explicit ``None`` and
    omission both let the resolver fire.
    """
    kwargs: dict[str, Any] = {"filter": filter}
    if shadow_suffix is not None:
        kwargs["shadow_suffix"] = shadow_suffix
    if governance_override is not None:
        kwargs["governance_override"] = governance_override
    if idempotency_key is not None:
        kwargs["idempotency_key"] = idempotency_key
    if timeout_seconds is not None:
        kwargs["timeout_seconds"] = timeout_seconds
    return kwargs


class RockyPipesMessageReader(dg.PipesTempFileMessageReader):
    """Pipes message reader that translates + filters Rocky asset keys in flight.

    The Rocky CLI emits Pipes messages with asset keys keyed by the engine's
    native ``[source_type, *components, table]`` path, slash-joined per the
    Dagster wire convention. That is the correct shape for a direct ``@dg.asset``
    callsite, but :class:`RockyComponent` remaps those paths to Dagster asset
    keys via :class:`RockyDagsterTranslator` before declaring the multi-asset.

    Without this reader the Pipes path would deliver materialization / check
    events on keys Dagster's asset graph doesn't recognise. Intercepting in the
    reader lets us rewrite keys and drop events for tables outside the selected
    subset *before* the events reach Dagster's handler.

    Two transformations happen on each message:

    1. ``asset_key_fn`` rewrites the wire ``asset_key`` string. It receives the
       slash-split path (``list[str]``) and returns a :class:`dagster.AssetKey`
       or ``None`` (which drops the event).
    2. ``include_keys`` filters on the *resolved* :class:`AssetKey`.

    Both hooks are optional — the default reader is the unmodified tempfile reader.
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
        """Wrap the upstream handler with a filter + asset-key rewriter."""
        proxy = _PipesHandlerProxy(
            handler,
            asset_key_fn=self._asset_key_fn,
            include_keys=self._include_keys,
        )
        with super().read_messages(proxy) as params:
            yield params


class _PipesHandlerProxy:
    """Duck-typed handler wrapper that filters Rocky Pipes messages.

    Wraps a single call site — ``PipesFileMessageReader._reader_thread`` invoking
    ``handler.handle_message(message)``. Every other attribute is forwarded
    unchanged via :meth:`__getattr__` so the reader thread's exception-reporting
    helpers still work.
    """

    # Messages whose ``params`` dict carries an asset key. Other messages
    # (``opened``, ``closed``, ``log``, …) pass through untouched.
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
                # Event dropped (asset_key_fn rejected it or the resolved key
                # isn't in include_keys). Silent by design — Rocky always runs
                # at source granularity so logging per dropped event would flood.
                return
            message = transformed

        self._inner.handle_message(message)

    def _transform_asset_key(self, message):
        params = message.get("params") or {}
        raw_key = params.get("asset_key")
        if not isinstance(raw_key, str) or not raw_key:
            # Defensive — let the inner handler raise if the envelope is malformed.
            return message

        path = raw_key.split("/")

        if self._asset_key_fn is not None:
            resolved = self._asset_key_fn(path)
            if resolved is None:
                return None
            # Dagster expects slash-escaped user strings on the wire.
            new_key_str = resolved.to_user_string()
        else:
            resolved = dg.AssetKey(path)
            new_key_str = raw_key

        if self._include_keys is not None and resolved not in self._include_keys:
            return None

        # Only rebuild the envelope when we actually changed the key.
        if new_key_str == raw_key:
            return message

        new_params = dict(params)
        new_params["asset_key"] = new_key_str
        new_message = dict(message)
        new_message["params"] = new_params
        return new_message

    def __getattr__(self, name):
        # Forward every attribute we don't handle explicitly.
        return getattr(self._inner, name)


class ResolverContext(BaseModel):
    """Read-only snapshot handed to each per-call kwarg resolver.

    Resolvers are closures users register on :class:`RockyResource` to inject
    kwargs derived from Dagster run context on every ``run`` / ``run_streaming``
    / ``run_pipes`` call. The context is **frozen** — signature stability matters
    because resolvers are user-authored closures imported across module
    boundaries.

    Attributes:
        context: The Dagster execution context for the call. ``None`` when the
            resolver fires from :meth:`RockyResource.run`.
        filter: The positional ``filter`` kwarg passed to the run method.
        method: Which run method triggered the resolver.
        supplied_kwargs: Snapshot of the kwargs the caller explicitly supplied.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    # Typed as ``Any`` rather than the concrete context classes because
    # Pydantic's is-instance validation would reject test doubles (MagicMock).
    context: Any = None
    filter: str | None = None
    method: Literal["run", "run_streaming", "run_pipes"]
    supplied_kwargs: dict[str, Any]


#: Type alias for user-authored per-call kwarg resolvers. Each resolver takes a
#: :class:`ResolverContext` and returns either a value for the target kwarg or
#: ``None`` to leave it unset.
Resolver = Callable[[ResolverContext], Any]


class RockyResource(dg.ConfigurableResource):
    """Dagster resource that runs Rocky CLI commands via :class:`rocky_sdk.RockyClient`.

    Args:
        binary_path: Path to the ``rocky`` binary. Defaults to ``"rocky"`` (on PATH).
        config_path: Path to the ``rocky.toml`` config file.
        state_path: Path to the state store file.
        models_dir: Directory containing model files for model-aware commands,
            including compile, lineage, test, CI, AI, and compliance.
        contracts_dir: Optional directory containing contract files.
        server_url: Optional URL for a running ``rocky serve`` instance. When set,
            ``compile()``, ``lineage()`` and ``metrics()`` use the HTTP API.
        timeout_seconds: Default subprocess timeout for any one CLI invocation.
        shadow_suffix_fn: Optional resolver that produces ``shadow_suffix`` per
            call when the caller doesn't supply one. Returning ``None`` is a no-op.
        governance_override_fn: Optional resolver for ``governance_override``.
        idempotency_key_fn: Optional resolver for ``idempotency_key``.
        timeout_fn: Optional resolver that produces a per-call ``timeout_seconds``
            watchdog budget. Fires when the caller omits ``timeout_seconds``;
            returning ``None`` falls back to the static :attr:`timeout_seconds`.
            Lets a tenant-collapsed pipeline grant only its heavy tenants a larger
            copy budget while keeping a tight hang-detection watchdog everywhere
            else — e.g. ``lambda ctx: 5400 if ctx.filter in HEAVY else 900``.
    """

    binary_path: str = "rocky"
    config_path: str = "rocky.toml"
    state_path: str = ".rocky-state.redb"
    #: Optional per-namespace state file (engine ``--state-namespace``). When set
    #: to a SQL identifier, this resource's commands route to a per-namespace
    #: redb file so independent fan-out runs don't serialize on a single writer
    #: lock. Mutually exclusive with ``state_path``: when set, ``--state-namespace``
    #: is sent and the explicit ``--state-path`` is omitted. Default ``None`` keeps
    #: the single global file.
    state_namespace: str | None = None
    models_dir: str = "models"
    contracts_dir: str | None = None
    server_url: str | None = None
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    #: Run ``rocky doctor`` at resource startup and gate execution on the result.
    #: Defaults to ``False`` — doctor is not invoked and startup cost stays zero
    #: for users who don't opt in. When ``True``, :meth:`setup_for_execution`
    #: runs doctor once and may raise based on :attr:`strict_doctor_checks`.
    strict_doctor: bool = False
    #: Per-check allowlist for the strict doctor gate. Only meaningful when
    #: :attr:`strict_doctor` is ``True``.
    #:
    #: * Empty list (default) — fail on *any* critical check.
    #: * Non-empty — fail only when a critical check whose ``name`` appears in
    #:   this list fires. Critical checks outside the list are logged as warnings.
    strict_doctor_checks: list[str] = []

    #: Optional resolver that produces a ``shadow_suffix`` per run call. Fires only
    #: when the caller didn't supply ``shadow_suffix``. Callable fields aren't
    #: valid Dagster config schema entries, so they're marked as
    #: ``resource_dependency`` via ``typing.Annotated`` — Dagster's canonical
    #: escape hatch for non-config resource attrs. Using the literal string marker
    #: keeps the annotation robust under ``from __future__ import annotations``.
    #: ``Annotated`` resource-dependency fields survive Dagster's resource rebuild
    #: at execution time (``PrivateAttr``-backed fields did not — see
    #: ``test_resolvers_survive_dagster_materialize_lifecycle``).
    shadow_suffix_fn: Annotated[Resolver | None, "resource_dependency"] = None
    governance_override_fn: Annotated[Resolver | None, "resource_dependency"] = None
    idempotency_key_fn: Annotated[Resolver | None, "resource_dependency"] = None
    #: Optional resolver that produces a per-call ``timeout_seconds`` watchdog
    #: budget. Unlike the three resolvers above (which inject CLI-argument
    #: kwargs), the resolved value is threaded to the SDK watchdog as a per-call
    #: override — ``timeout_seconds`` is not a CLI flag. Fires only when the
    #: caller omits ``timeout_seconds``; ``None`` falls back to the static field.
    timeout_fn: Annotated[Resolver | None, "resource_dependency"] = None

    # Lazily-built SDK client, cached per resource instance. Dagster rebuilds the
    # resource per asset; each rebuild gets a fresh client (re-resolves the
    # binary + re-checks the version on first use), matching the prior behavior.
    # ``None`` means "not yet built". Not a Dagster config field.
    _client: RockyClient | None = None
    # Instance-level cache so the Pipes-timeout warning fires exactly once per
    # resource lifetime instead of on every materialize. Not a config field.
    _pipes_timeout_warned: bool = False

    # ------------------------------------------------------------------ #
    # SDK client                                                         #
    # ------------------------------------------------------------------ #

    def _get_client(self) -> RockyClient:
        """Build (once) and return the SDK client mirroring this resource's config.

        The client carries the dagster logger so subprocess lifecycle lines keep
        their ``dagster_rocky`` provenance, and ``mirror_stderr=True`` so the
        engine's stderr is preserved by Dagster's compute-log capture (which only
        sees the step process's real fds).
        """
        cached = self._client
        if cached is not None:
            return cached
        client = RockyClient(
            config_path=self.config_path,
            binary_path=self.binary_path,
            state_path=self.state_path,
            state_namespace=self.state_namespace,
            models_dir=self.models_dir,
            contracts_dir=self.contracts_dir,
            server_url=self.server_url,
            timeout_seconds=self.timeout_seconds,
            logger=_log,
            mirror_stderr=True,
        )
        # ``ConfigurableResource`` is pydantic-immutable at runtime, so set via
        # ``object.__setattr__`` — the same pattern the prior cache flags used.
        object.__setattr__(self, "_client", client)
        return client

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

        For each ``(kwarg_name, resolver_fn)`` pair, fire the resolver only when
        the caller didn't supply the kwarg. Caller-supplied values always win.
        Resolvers returning ``None`` are a no-op. A ``dg.Failure`` raised by a
        resolver is preserved; any other exception is wrapped with the resolver's
        ``__qualname__`` in the description.
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
            # ``timeout_seconds`` is not a CLI flag: the run methods read the
            # resolved value out of ``kwargs`` and thread it to the SDK watchdog
            # as a per-call override, never to ``_build_run_args``.
            ("timeout_seconds", self.timeout_fn),
        ):
            if fn is None or kw in kwargs:
                continue
            try:
                value = fn(rc)
            except dg.Failure:
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
        """Run the optional ``rocky doctor`` startup gate.

        Invoked by Dagster once per resource initialization before any asset / op
        body executes. A cheap no-op when :attr:`strict_doctor` is ``False`` (the
        default). When enabled, ``rocky doctor`` runs and its result is triaged:
        non-critical severities never fail; critical checks in
        :attr:`strict_doctor_checks` (or *all* critical checks when the list is
        empty) raise a :class:`dagster.Failure`; critical checks outside the
        allowlist are logged at ``warning``.
        """
        if not self.strict_doctor:
            return

        log = context.log if context is not None and context.log is not None else _log

        try:
            report = self.doctor()
        except dg.Failure as exc:
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
            status_l = status.lower()
            if status_l == "healthy":
                continue
            if status_l == "warning":
                log.warning(f"rocky doctor [{check.name}]: {check.message}")
                continue

            # `critical` and any status we don't recognise (a future severity
            # above `warning`) are treated as gate-worthy — a strict-doctor gate
            # must fail closed on an unknown-but-severe status, not skip it.
            if self._is_strict_check(check.name):
                strict_failures.append(f"{check.name}: {check.message}")
            else:
                warn_failures.append(f"{check.name}: {check.message}")
                log.warning(f"rocky doctor [{check.name}] {status_l} (non-strict): {check.message}")

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

        Empty allowlist → every critical check is strict (fail-on-any). Non-empty
        → only listed names are strict.
        """
        if not self.strict_doctor_checks:
            return True
        return check_name in self.strict_doctor_checks

    # ------------------------------------------------------------------ #
    # Thin delegating helpers (subprocess + argv now live in the client) #
    # ------------------------------------------------------------------ #

    def _resolve_binary(self) -> str:
        """Resolve and memoize the absolute path to the rocky binary."""
        return self._get_client()._resolve_binary()

    def _verify_engine_version(self) -> None:
        """Check the binary meets the minimum version (raises ``dg.Failure``)."""
        with _translating():
            self._get_client()._verify_engine_version()

    def _build_cmd(self, args: list[str]) -> list[str]:
        """Prefix ``args`` with the binary + global flags."""
        return self._get_client()._build_cmd(args)

    def _build_run_args(self, filter: str, **kwargs: Any) -> list[str]:
        """Build the argv for a fused ``rocky run`` (delegates to the client)."""
        return self._get_client()._build_run_args(filter, **kwargs)

    def _build_plan_args(self, filter: str, **kwargs: Any) -> list[str]:
        """Build the argv for ``rocky plan`` (delegates to the client)."""
        return self._get_client()._build_plan_args(filter, **kwargs)

    def _run_rocky(
        self, args: list[str], *, allow_partial: bool = False, timeout_seconds: int | None = None
    ) -> str:
        """Execute a Rocky CLI command and return stdout (errors → ``dg.Failure``).

        Streams the binary's stderr to the ``dagster_rocky`` module logger
        line-by-line. Used for read-only invocations without a Dagster context;
        for live ``context.log`` streaming see :meth:`_run_rocky_streaming`.

        ``timeout_seconds`` overrides the client's static watchdog budget for this
        one invocation; forwarded only when set so the default path is unchanged.
        """
        with _translating():
            if timeout_seconds is None:
                return self._get_client().run_cli(args, allow_partial=allow_partial)
            return self._get_client().run_cli(
                args, allow_partial=allow_partial, timeout_seconds=timeout_seconds
            )

    def _run_rocky_streaming(
        self,
        args: list[str],
        context: dg.AssetExecutionContext | dg.OpExecutionContext,
        *,
        allow_partial: bool = False,
    ) -> str:
        """Execute the Rocky CLI with live stderr streaming to ``context.log``."""

        def _sink(line: str) -> None:
            # ``context.log`` can raise mid-line during run teardown — suppress
            # and keep reading so the run still completes cleanly.
            with contextlib.suppress(Exception):
                context.log.info(f"rocky: {line}")

        with _translating():
            return self._get_client().run_cli(args, allow_partial=allow_partial, log_callback=_sink)

    # ------------------------------------------------------------------ #
    # Discovery & execution                                             #
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
        with _translating():
            return self._get_client().discover(
                pipeline=pipeline, emit_fivetran_state_to=emit_fivetran_state_to
            )

    def plan(
        self,
        filter: str | None = None,
        *,
        pipeline: str | None = None,
        env: str | None = None,
    ) -> PlanResult:
        """Run ``rocky plan`` and return the parsed result.

        Every project shape content-addresses a plan and persists it to
        ``.rocky/plans/<plan_id>.json``. Call :meth:`apply` with the returned
        ``plan_id`` to execute it.
        """
        with _translating():
            return self._get_client().plan(filter, pipeline=pipeline, env=env)

    def apply(self, plan_id: str) -> ApplyResult:
        """Run ``rocky apply <plan-id>`` and return the parsed result.

        ``rocky apply`` prints the plan-kind's own output (no wrapping
        envelope), so the return type is the :data:`~rocky_sdk.client.ApplyResult`
        union: run-shaped plans yield a :class:`RunResult`, a ``gc`` plan a
        ``GcApplyOutput``, compact / archive / promote plans their own outputs.
        """
        with _translating():
            return self._get_client().apply(plan_id)

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
        timeout_seconds: int | None = None,
    ) -> RunResult:
        """Run ``rocky run --filter <key=value>`` and return the parsed result.

        Partial success: if some tables fail but others succeed, the resulting
        JSON is still parsed and returned so callers can emit ``MaterializeResult``
        events for the successful tables and report failures for the rest.

        Args:
            filter: Component filter (e.g. ``"client=acme"``).
            governance_override: Optional per-run governance config (workspace_ids,
                grants), merged additively with ``rocky.toml`` defaults.
            run_models: If ``True``, also execute compiled models.
            partition / partition_from / partition_to / latest / missing /
                lookback / parallel: Partition selection flags.
            shadow_suffix: Enable shadow mode with this table-name suffix.
            idempotency_key: Caller-supplied opaque dedup token. Stored verbatim —
                do NOT put secrets in idempotency keys.
            defer / defer_to: Resolve unbuilt ``ref()`` upstreams against an
                existing schema instead of rebuilding.
            timeout_seconds: Per-call watchdog budget (positive seconds) for this
                one run. Overrides both :attr:`timeout_seconds` and any
                :attr:`timeout_fn` resolver. When omitted, ``timeout_fn`` (if set)
                resolves the budget, else the static :attr:`timeout_seconds`.

        Note on resolver interaction:
            Per-call resolvers registered on the resource fire for the matching
            kwarg only when that kwarg is **absent** from the call. An explicit
            ``None`` is treated as absent and resolvers fire. Pass the non-``None``
            value you want to win against the resolver.
        """
        resolved = self._apply_resolvers(
            context=None,
            method="run",
            kwargs=_collect_supplied_run_kwargs(
                filter=filter,
                shadow_suffix=shadow_suffix,
                governance_override=governance_override,
                idempotency_key=idempotency_key,
                timeout_seconds=timeout_seconds,
            ),
        )
        _validate_governance_override(resolved.get("governance_override"))
        with _translating():
            return self._get_client().run(
                filter,
                governance_override=resolved.get("governance_override"),
                pipeline=pipeline,
                timeout_seconds=resolved.get("timeout_seconds"),
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
                defer=defer,
                defer_to=defer_to,
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
        defer: bool = False,
        defer_to: str | None = None,
        timeout_seconds: int | None = None,
    ) -> RunResult:
        """``rocky run`` with live stderr streaming to ``context.log``.

        Same semantics as :meth:`run` but forwards rocky's stderr (where the
        engine's tracing layer writes ``info!()`` / ``warn!()`` macros) to
        ``context.log.info`` line-by-line as the run progresses. Use from inside a
        Dagster ``@multi_asset`` / ``@op`` for runs longer than a few seconds. For
        full Dagster Pipes integration with structured events, use
        :meth:`run_pipes`.

        ``timeout_seconds`` (and the :attr:`timeout_fn` resolver) apply here in
        full: the whole ``rocky run`` — copy plus finalize — is watchdog-bound.
        """
        resolved = self._apply_resolvers(
            context=context,
            method="run_streaming",
            kwargs=_collect_supplied_run_kwargs(
                filter=filter,
                shadow_suffix=shadow_suffix,
                governance_override=governance_override,
                idempotency_key=idempotency_key,
                timeout_seconds=timeout_seconds,
            ),
        )
        _validate_governance_override(resolved.get("governance_override"))

        def _sink(line: str) -> None:
            with contextlib.suppress(Exception):
                context.log.info(f"rocky: {line}")

        with _translating():
            return self._get_client().run(
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
                defer=defer,
                defer_to=defer_to,
                timeout_seconds=resolved.get("timeout_seconds"),
                log_callback=_sink,
            )

    @staticmethod
    def _extract_plan_id(plan_stdout: str) -> str | None:
        """Pull ``plan_id`` out of a ``rocky plan`` JSON payload.

        Returns ``None`` when ``rocky plan`` did not persist a ``plan_id`` (engine
        version skew or a malformed payload). :meth:`run_pipes` treats a ``None``
        here as a hard error rather than executing a plan it cannot correlate.
        """
        import json

        try:
            payload = json.loads(plan_stdout)
        except json.JSONDecodeError:
            return None
        if not isinstance(payload, dict):
            return None
        plan_id = payload.get("plan_id")
        if not isinstance(plan_id, str):
            return None
        return plan_id

    def _maybe_warn_pipes_timeout_ignored(
        self, context: dg.AssetExecutionContext | dg.OpExecutionContext | None
    ) -> None:
        """Emit a one-time warning when ``run_pipes`` is called with a non-default
        ``timeout_seconds``.

        ``dagster.PipesSubprocessClient`` owns its subprocess and exposes no kill
        hook, so the resource cannot enforce a watchdog around the apply step in
        Pipes mode. The warning fires exactly once per resource lifetime.
        """
        if self.timeout_seconds == DEFAULT_TIMEOUT_SECONDS or self._pipes_timeout_warned:
            return
        msg = (
            f"RockyResource.timeout_seconds={self.timeout_seconds} is "
            "ignored by run_pipes — dagster.PipesSubprocessClient owns "
            "the apply subprocess and exposes no kill hook. The plan "
            "step is still watchdog-bound, but a warehouse hang during "
            "apply will pin the Dagster step process indefinitely. "
            "Configure a Dagster-side run timeout for hard bounding, or "
            "use RockyResource.run_streaming() if you need the "
            "resource-level watchdog."
        )
        if context is not None and getattr(context, "log", None) is not None:
            with contextlib.suppress(Exception):
                context.log.warning(msg)
        _log.warning(msg)
        object.__setattr__(self, "_pipes_timeout_warned", True)

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
        defer: bool = False,
        defer_to: str | None = None,
        timeout_seconds: int | None = None,
        pipes_client: dg.PipesSubprocessClient | None = None,
        asset_key_fn: Callable[[list[str]], dg.AssetKey | None] | None = None,
        include_keys: set[dg.AssetKey] | None = None,
    ) -> dg.PipesClientCompletedInvocation:
        """Full Dagster Pipes execution: structured events streamed via the protocol.

        Spawns ``rocky apply`` via :class:`dagster.PipesSubprocessClient`, which
        sets the Pipes env vars and tails the messages channel for structured
        events (``MaterializationEvent`` per copied table, ``AssetCheckEvaluation``
        per Rocky check / drift observation, ``log`` events). The canonical
        pattern::

            @dg.asset
            def my_warehouse_data(context: dg.AssetExecutionContext, rocky: RockyResource):
                yield from rocky.run_pipes(context, filter="tenant=acme").get_results()

        **Timeout contract.** Neither ``timeout_seconds`` nor the
        :attr:`timeout_fn` resolver bounds the apply step in Pipes mode —
        ``PipesSubprocessClient`` owns the subprocess and exposes no kill hook.
        Both bound only the plan step, which routes through :meth:`_run_rocky`
        and is watchdog-bound. Configure a Dagster-side run timeout for hard
        bounding, or use :meth:`run_streaming` if you need a resource-level
        watchdog around the copy.

        Args:
            context: Dagster execution context (required for Pipes injection).
            filter: Component filter (e.g. ``"tenant=acme"``).
            governance_override / run_models / partition / shadow_suffix / …:
                Same as :meth:`run`. Threaded into the rocky CLI via
                :meth:`_build_plan_args`.
            pipes_client: Optional pre-configured ``PipesSubprocessClient``. When
                supplied, ``asset_key_fn`` / ``include_keys`` are ignored.
            asset_key_fn: Optional transform applied to each Pipes event's asset
                key at the reader layer. Returns an :class:`AssetKey` or ``None``
                (drops the event).
            include_keys: Optional allowlist of Dagster asset keys; events whose
                resolved key is not in the set are dropped.
        """
        self._maybe_warn_pipes_timeout_ignored(context)
        resolved = self._apply_resolvers(
            context=context,
            method="run_pipes",
            kwargs=_collect_supplied_run_kwargs(
                filter=filter,
                shadow_suffix=shadow_suffix,
                governance_override=governance_override,
                idempotency_key=idempotency_key,
                timeout_seconds=timeout_seconds,
            ),
        )
        _validate_governance_override(resolved.get("governance_override"))
        build_kwargs: dict[str, Any] = {
            "governance_override": resolved.get("governance_override"),
            "run_models": run_models,
            "partition": partition,
            "partition_from": partition_from,
            "partition_to": partition_to,
            "latest": latest,
            "missing": missing,
            "lookback": lookback,
            "parallel": parallel,
            "shadow_suffix": resolved.get("shadow_suffix"),
            "idempotency_key": resolved.get("idempotency_key"),
            "defer": defer,
            "defer_to": defer_to,
        }
        # Deliberate asymmetry: unlike run / run_streaming (a single fused
        # ``rocky run``), run_pipes keeps the two-step ``rocky plan`` +
        # ``rocky apply <plan_id>`` shape. The plan persists a content-addressed
        # ``plan_id`` we surface as Pipes ``extras`` so operators can correlate
        # the materialization back to ``.rocky/plans/<plan_id>.json`` — a field a
        # fused ``rocky run`` cannot provide. Do not "consistency-fix" this.
        plan_args = self._build_plan_args(filter, **build_kwargs)
        if pipes_client is not None:
            client = pipes_client
        elif asset_key_fn is not None or include_keys is not None:
            # Custom reader only when the caller asked for translation / filtering
            # — keeps the default path identical to canonical Dagster Pipes.
            client = dg.PipesSubprocessClient(
                message_reader=RockyPipesMessageReader(
                    asset_key_fn=asset_key_fn,
                    include_keys=include_keys,
                ),
            )
        else:
            client = dg.PipesSubprocessClient()
        # Plan step is buffered and a separate subprocess (the engine's Pipes
        # emitter is a no-op on ``rocky plan`` — only ``rocky apply`` reports).
        # The per-call timeout bounds this watchdog-backed plan step (the apply
        # step below is Pipes-owned and unbounded — see the timeout contract).
        plan_stdout = self._run_rocky(plan_args, timeout_seconds=resolved.get("timeout_seconds"))
        plan_id = self._extract_plan_id(plan_stdout)
        if plan_id is None:
            raise dg.Failure(
                description=(
                    "rocky plan did not emit a plan_id — this dagster integration "
                    "requires engine-v1.34+ which content-addresses every plan, "
                    "including replication-only projects. Upgrade the rocky binary "
                    "or pin dagster-rocky<1.33 if a downgrade is needed."
                ),
                metadata={
                    "plan_stdout_tail": dg.MetadataValue.text(
                        _truncate_stderr_for_metadata(plan_stdout)
                    ),
                },
            )
        return client.run(
            context=context,
            command=self._build_cmd(["apply", plan_id]),
            # Surface the plan id on the Dagster run as Pipes extras for correlation.
            extras={"plan_id": plan_id},
        )

    def state(self) -> StateResult:
        """Run ``rocky state`` and return the parsed result."""
        with _translating():
            return self._get_client().state()

    # ------------------------------------------------------------------ #
    # Branch approval / promote                                          #
    # ------------------------------------------------------------------ #

    def branch_approve(
        self,
        name: str,
        *,
        message: str | None = None,
        out: str | None = None,
    ) -> ApproveOutput:
        """Run ``rocky branch approve <name>`` and return the parsed artifact."""
        with _translating():
            return self._get_client().branch_approve(name, message=message, out=out)

    def branch_promote(
        self,
        name: str,
        *,
        filter: str | None = None,
        skip_approval: bool = False,
    ) -> BranchPromoteOutput:
        """Run ``rocky branch promote <name>`` and return the parsed result."""
        with _translating():
            return self._get_client().branch_promote(
                name, filter=filter, skip_approval=skip_approval
            )

    def plan_promote(
        self,
        name: str,
        *,
        base: str = "main",
        allow_breaking: bool = False,
        filter: str | None = None,
    ) -> PromotePlan:
        """Run ``rocky plan promote <name>`` and return the parsed plan."""
        with _translating():
            return self._get_client().plan_promote(
                name, base=base, allow_breaking=allow_breaking, filter=filter
            )

    # ------------------------------------------------------------------ #
    # Compiler / analysis (HTTP when server_url is set, CLI otherwise)   #
    # ------------------------------------------------------------------ #

    def compile(self, model_filter: str | None = None) -> CompileResult:
        """Run ``rocky compile`` (or the HTTP API when ``server_url`` is set)."""
        with _translating():
            return self._get_client().compile(model_filter)

    def lineage(
        self,
        target: str,
        column: str | None = None,
    ) -> ModelLineageResult | ColumnLineageResult:
        """Run ``rocky lineage`` (or the HTTP API when ``server_url`` is set)."""
        with _translating():
            return self._get_client().lineage(target, column)

    def catalog(self, *, out: str | None = None) -> CatalogOutput:
        """Run ``rocky catalog`` and return the project-wide lineage snapshot."""
        with _translating():
            return self._get_client().catalog(out=out)

    def dag(self, *, column_lineage: bool = False) -> DagResult:
        """Run ``rocky dag`` and return the full unified DAG."""
        with _translating():
            return self._get_client().dag(column_lineage=column_lineage)

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
        """Run ``rocky run --model <name>`` for a single compiled model."""
        with _translating():
            return self._get_client().run_model(
                model_name,
                filter=filter,
                partition=partition,
                partition_from=partition_from,
                partition_to=partition_to,
                latest=latest,
                missing=missing,
                lookback=lookback,
                parallel=parallel,
            )

    # ------------------------------------------------------------------ #
    # Local testing                                                      #
    # ------------------------------------------------------------------ #

    def test(self, model_filter: str | None = None) -> TestResult:
        """Run ``rocky test`` (local DuckDB execution, no warehouse creds)."""
        with _translating():
            return self._get_client().test(model_filter)

    def ci(self) -> CiResult:
        """Run ``rocky ci`` (compile + test) and return the parsed result."""
        with _translating():
            return self._get_client().ci()

    # ------------------------------------------------------------------ #
    # Observability (HTTP when server_url is set, CLI otherwise)         #
    # ------------------------------------------------------------------ #

    def history(
        self,
        model: str | None = None,
        since: str | None = None,
    ) -> HistoryResult | ModelHistoryResult:
        """Run ``rocky history`` and return the parsed run history."""
        with _translating():
            return self._get_client().history(model, since)

    def metrics(
        self,
        model: str,
        *,
        trend: bool = False,
        column: str | None = None,
        alerts: bool = False,
    ) -> MetricsResult:
        """Run ``rocky metrics`` (or the HTTP API when ``server_url`` is set)."""
        with _translating():
            return self._get_client().metrics(model, trend=trend, column=column, alerts=alerts)

    def optimize(self, model: str | None = None) -> OptimizeResult:
        """Run ``rocky optimize`` and return materialization recommendations."""
        with _translating():
            return self._get_client().optimize(model)

    def cost(self, run_id: str = "latest") -> CostOutput:
        """Run ``rocky cost <run_id>`` and return historical cost attribution."""
        with _translating():
            return self._get_client().cost(run_id)

    # ------------------------------------------------------------------ #
    # AI Level 3 commands                                                #
    # ------------------------------------------------------------------ #

    def ai(self, intent: str, format: str = "rocky") -> AiResult:
        """Generate a model from a natural-language intent."""
        with _translating():
            return self._get_client().ai(intent, format)

    def ai_sync(
        self,
        *,
        apply: bool = False,
        model: str | None = None,
        with_intent: bool = False,
    ) -> AiSyncResult:
        """Detect schema changes and propose intent-guided updates."""
        with _translating():
            return self._get_client().ai_sync(apply=apply, model=model, with_intent=with_intent)

    def ai_explain(
        self,
        model: str | None = None,
        *,
        all: bool = False,
        save: bool = False,
    ) -> AiExplainResult:
        """Generate intent descriptions from existing model code."""
        with _translating():
            return self._get_client().ai_explain(model, all=all, save=save)

    def ai_test(
        self,
        model: str | None = None,
        *,
        all: bool = False,
        save: bool = False,
    ) -> AiTestResult:
        """Generate test assertions from intent."""
        with _translating():
            return self._get_client().ai_test(model, all=all, save=save)

    def ai_contract(
        self,
        model: str,
        *,
        save: bool = False,
    ) -> AiContractOutput:
        """AI-draft a data contract from a model's observed data (DuckDB only)."""
        with _translating():
            return self._get_client().ai_contract(model, save=save)

    # ------------------------------------------------------------------ #
    # Hook commands                                                      #
    # ------------------------------------------------------------------ #

    def hooks_list(self) -> str:
        """List configured hooks (returns raw stdout)."""
        with _translating():
            return self._get_client().hooks_list()

    def hooks_test(self, event: str) -> str:
        """Fire a test hook event (returns raw stdout)."""
        with _translating():
            return self._get_client().hooks_test(event)

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
        with _translating():
            return self._get_client().validate_migration(
                dbt_project, rocky_project, sample_size=sample_size
            )

    def test_adapter(
        self,
        adapter: str | None = None,
        command: str | None = None,
    ) -> ConformanceResult:
        """Run adapter conformance tests."""
        with _translating():
            return self._get_client().test_adapter(adapter, command)

    # ------------------------------------------------------------------ #
    # Doctor and resume commands                                         #
    # ------------------------------------------------------------------ #

    def doctor(self, *, check: str | None = None) -> DoctorResult:
        """Run ``rocky doctor`` and return the parsed health-check results."""
        with _translating():
            return self._get_client().doctor(check=check)

    def state_health(self, *, probe_write: bool = False) -> StateHealthResult:
        """Return a live snapshot of Rocky's state-backend health.

        Aggregates the configured ``[state] backend`` plus the most recent run
        recorded in the state store into one typed snapshot Dagster sensors /
        schedules / asset checks can gate on. The cheap path (``probe_write=False``,
        the default) does one ``rocky history`` subprocess plus a ``tomllib`` read;
        ``probe_write=True`` additionally runs ``rocky doctor --check state_rw``.

        Tolerance: designed to survive a sensor tick even when the binary is
        missing or the store is unreadable — recent-run fields degrade to ``None``
        rather than raising, and the probe surfaces failures through the
        ``probe_*`` fields. Delegates to :func:`dagster_rocky.health.state_health`.
        """
        from .health import state_health as _state_health

        return _state_health(self, probe_write=probe_write)

    def compliance(self, *, env: str | None = None) -> ComplianceOutput:
        """Run ``rocky compliance`` and return the governance rollup."""
        with _translating():
            return self._get_client().compliance(env=env)

    def retention_status(self, *, env: str | None = None) -> RetentionStatusOutput:
        """Run ``rocky retention-status`` and return per-model retention status."""
        with _translating():
            return self._get_client().retention_status(env=env)

    def resume_run(
        self,
        run_id: str | None = None,
        *,
        filter: str = "",
        governance_override: dict | None = None,
    ) -> RunResult:
        """Resume a failed run from where it left off (latest when ``run_id`` is None)."""
        # FR-009 pre-flight: mirror the guard from :meth:`run` so a resumed run
        # catches the empty-workspace_ids footgun in-process.
        _validate_governance_override(governance_override)
        with _translating():
            return self._get_client().resume_run(
                run_id, filter=filter, governance_override=governance_override
            )
