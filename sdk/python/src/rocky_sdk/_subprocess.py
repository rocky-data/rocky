"""Low-level subprocess plumbing shared by :class:`rocky_sdk.client.RockyClient`.

Single-reader-per-pipe + external watchdog. Two dedicated threads are the *sole*
readers of ``proc.stdout`` / ``proc.stderr`` while a third watchdog thread
enforces the wall-clock timeout by killing the process group — never
``communicate(timeout=)``, which races with a concurrent pipe reader and was the
root cause of intermittent multi-hour hangs in production. See
:meth:`RockyClient._run_with_log_sink` for how the pieces fit together.
"""

from __future__ import annotations

import contextlib
import logging
import os
import signal
import subprocess
import sys
from collections.abc import Callable, Iterable
from typing import IO

_log = logging.getLogger("rocky_sdk")

#: Argv flags whose immediately-following value is credential-bearing or
#: otherwise sensitive. When the constructed argv is logged, the value of any
#: matching flag is masked. The subprocess itself still receives the real value
#: — only the log line is redacted.
_REDACTED_ARGV_FLAGS = frozenset({"--governance-override", "--idempotency-key"})


def redact_argv(argv: list[str]) -> list[str]:
    """Return a copy of ``argv`` with credential-bearing flag values masked.

    Walks left-to-right; whenever a token is in :data:`_REDACTED_ARGV_FLAGS`,
    the *next* token (its value) is replaced with ``"***"``. For log output
    only — never for the argv handed to the subprocess.
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


def forward_stderr_to_sink(
    stderr: Iterable[str] | None,
    log_line: Callable[[str], None],
    sink: list[str],
    *,
    mirror_to_stderr: bool = False,
) -> None:
    """Reader-thread body: forward rocky stderr lines to ``log_line`` and ``sink``.

    Reads ``stderr`` line-by-line until EOF. Each non-empty line is appended to
    ``sink`` (shared with the parent for failure-tail metadata) and handed to
    ``log_line`` (the caller's destination — a logger, ``print``, Dagster's
    ``context.log.info``, …). This is the **sole reader** of ``proc.stderr``.

    When ``mirror_to_stderr`` is set, each line is additionally written to this
    process's ``sys.stderr`` so an outer capture that only sees the real fds
    (e.g. Dagster's compute-log capture) preserves rocky's tracing output.
    Mirroring is best-effort: ``OSError`` / ``ValueError`` from a closed fd are
    swallowed so the reader thread never dies on teardown.
    """
    if stderr is None:
        return
    try:
        for raw in stderr:
            line = raw.rstrip("\n")
            if not line:
                continue
            sink.append(line)
            log_line(line)
            if mirror_to_stderr:
                with contextlib.suppress(OSError, ValueError):
                    print(line, file=sys.stderr, flush=True)
    except (OSError, ValueError) as exc:
        _log.warning("rocky stderr reader terminated: %s", exc)


def accumulate_stdout(stdout: IO[str] | None, sink: list[str]) -> None:
    """Reader-thread body: accumulate every rocky stdout line into ``sink``.

    Sole reader of ``proc.stdout``. Appends every line **including blank ones**
    so the final concatenation reconstructs rocky's exact JSON output
    byte-for-byte. On an unexpected read error logs at WARN and exits cleanly;
    the parent surfaces a parse failure if the payload was truncated.
    """
    if stdout is None:
        return
    try:
        for line in stdout:
            sink.append(line)
    except (OSError, ValueError) as exc:
        _log.warning("rocky stdout accumulator terminated: %s", exc)


def kill_process_group(proc: subprocess.Popen[str]) -> None:
    """Terminate ``proc`` and any children via its POSIX process group.

    Called from the watchdog when the wall-clock timeout elapses. On POSIX,
    ``os.killpg(os.getpgid(pid), SIGKILL)`` reaps children too (requires the
    Popen was launched with ``start_new_session=True``). On Windows, falls back
    to ``proc.kill()``. ``ProcessLookupError`` / ``OSError`` are swallowed: the
    process may have exited between ``wait()`` returning and the kill call.
    """
    try:
        if os.name == "nt":
            proc.kill()
        else:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except (ProcessLookupError, OSError):
        pass
