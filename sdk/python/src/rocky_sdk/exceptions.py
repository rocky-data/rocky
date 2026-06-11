"""Typed exceptions raised by :class:`rocky_sdk.client.RockyClient`.

Every failure the client can surface is a :class:`RockyError` subclass carrying
structured fields (exit code, stderr tail, stdout preview, version strings, …)
rather than an opaque message. Callers catch the specific subclass they care
about; the ``dagster-rocky`` adapter catches :class:`RockyError` at one boundary
and rebuilds a ``dagster.Failure`` with equivalent metadata.

The install URL is centralised here so the "binary missing / too old" messages
stay consistent across the SDK and any adapter that re-renders them.
"""

from __future__ import annotations

#: Where the messages point users to install or upgrade the engine binary.
INSTALL_URL = "https://github.com/rocky-data/rocky/releases"


class RockyError(Exception):
    """Base class for every error raised by the Rocky SDK.

    Args:
        message: Human-readable description.
        command: The friendly CLI command label (e.g. ``"discover"``) when the
            error is tied to a specific invocation. ``None`` for setup-time
            errors (binary missing / too old).
    """

    def __init__(self, message: str, *, command: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.command = command


class RockyBinaryNotFoundError(RockyError):
    """The ``rocky`` binary could not be found or executed.

    Raised when ``shutil.which`` / ``subprocess`` cannot locate the configured
    ``binary_path`` (a bare name missing from ``$PATH`` or a path that does not
    exist).
    """

    def __init__(self, binary_path: str) -> None:
        super().__init__(f"Rocky binary not found at '{binary_path}'. Install from: {INSTALL_URL}")
        self.binary_path = binary_path


class RockyVersionError(RockyError):
    """The installed binary is older than the SDK's minimum supported version."""

    def __init__(self, detected_version: str, min_version: str, binary_path: str) -> None:
        super().__init__(
            f"Rocky binary version {detected_version} is below the minimum required "
            f"version {min_version} for this rocky-sdk release. Update: {INSTALL_URL}"
        )
        self.detected_version = detected_version
        self.min_version = min_version
        self.binary_path = binary_path


class RockyTimeoutError(RockyError):
    """The subprocess exceeded ``timeout_seconds`` and was watchdog-killed."""

    def __init__(
        self,
        timeout_seconds: int,
        *,
        stderr_tail: str = "",
        duration_ms: int = 0,
        pid: int | None = None,
    ) -> None:
        super().__init__(f"Rocky command timed out after {timeout_seconds}s (watchdog-killed)")
        self.timeout_seconds = timeout_seconds
        self.stderr_tail = stderr_tail
        self.duration_ms = duration_ms
        self.pid = pid


class RockyCommandError(RockyError):
    """The subprocess exited non-zero (and the output was not an accepted partial).

    ``stderr_tail`` carries the last lines the binary wrote to stderr — the
    engine's tracing layer routes ``info!()`` / ``warn!()`` there, so the tail
    usually explains the failure.
    """

    def __init__(
        self,
        returncode: int,
        *,
        stderr_tail: str = "",
        duration_ms: int = 0,
        command: str | None = None,
    ) -> None:
        super().__init__(f"Rocky command failed (exit {returncode})", command=command)
        self.returncode = returncode
        self.stderr_tail = stderr_tail
        self.duration_ms = duration_ms


class RockyPartialFailure(RockyCommandError):  # noqa: N818 - "Failure" reads truer than "Error" for a partial success
    """A run exited non-zero but returned a parseable (partial) JSON payload.

    Some tables/models succeeded and others failed. By default the client
    *returns* the partial result (``allow_partial=True``) so callers can act on
    the tables that did materialize; this is raised only when a caller opts into
    strict handling (``allow_partial=False``). ``stdout`` holds the raw partial
    JSON. A subclass of :class:`RockyCommandError`, so callers that only care
    about "did it fail" catch the base.
    """

    def __init__(
        self,
        returncode: int,
        *,
        stdout: str = "",
        stderr_tail: str = "",
        duration_ms: int = 0,
        command: str | None = None,
    ) -> None:
        super().__init__(
            returncode,
            stderr_tail=stderr_tail,
            duration_ms=duration_ms,
            command=command,
        )
        self.stdout = stdout


class RockyOutputParseError(RockyError):
    """The binary's stdout was not the JSON shape the SDK expected.

    ``kind`` distinguishes malformed JSON (``"json"``), schema/validation drift
    (``"validation"``), and a malformed apply envelope (``"envelope"``).
    ``stdout`` is the raw payload; ``parse_error`` is the underlying error text.
    """

    def __init__(
        self,
        command: str,
        *,
        stdout: str,
        parse_error: str,
        kind: str = "json",
        plan_id: str | None = None,
        plan_kind: str | None = None,
        inner_result_preview: str | None = None,
    ) -> None:
        if kind == "validation":
            message = (
                f"rocky {command} output failed schema validation — "
                "see attributes for the raw stdout and pydantic error"
            )
        elif kind == "envelope":
            message = f"rocky {command} apply envelope was malformed: {parse_error}"
        else:
            message = f"rocky {command} returned malformed JSON: {parse_error}"
        super().__init__(message, command=command)
        self.stdout = stdout
        self.parse_error = parse_error
        self.kind = kind
        self.plan_id = plan_id
        self.plan_kind = plan_kind
        self.inner_result_preview = inner_result_preview


class RockyServerError(RockyError):
    """A request to the optional ``rocky serve`` HTTP API failed."""

    def __init__(self, url: str, *, error: str = "") -> None:
        super().__init__(f"Rocky server request failed: {url}")
        self.url = url
        self.error = error


class RockyGovernanceError(RockyError):
    """A ``governance_override`` argument would cause a silent full-revoke footgun.

    Mirrors the engine-side guard so the error surfaces before the subprocess is
    spawned (faster feedback, no half-applied warehouse state).
    """
