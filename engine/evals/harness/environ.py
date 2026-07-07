"""Environment resolution and skip detection.

The live agent loop is *not* creds-free: driving a scripted Claude Code session
needs a model API key. This module resolves the three external dependencies the
live suite needs — the `rocky` binary, the `claude` CLI, an `ANTHROPIC_API_KEY`
(plus the `duckdb` CLI used to seed the fixture) — and reports, precisely, which
are missing so the runner can *skip cleanly* (exit 0 with a message) rather than
fail when a contributor has no key. The label-gated CI job supplies the key as a
repository secret; without it the job skips green.
"""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from pathlib import Path

# engine/evals/harness/environ.py -> engine/evals -> engine
_EVALS_DIR = Path(__file__).resolve().parent.parent
_ENGINE_DIR = _EVALS_DIR.parent

#: Env var the runner reads the model key from. Named explicitly so the CI job
#: and the docs reference one canonical name.
API_KEY_ENV = "ANTHROPIC_API_KEY"


def resolve_rocky_bin() -> Path | None:
    """Locate the `rocky` binary: ``$ROCKY_BIN``, then a local build, then PATH.

    Prefers a release build over debug (faster MCP calls), then falls back to
    whatever `rocky` is on PATH.
    """
    override = os.environ.get("ROCKY_BIN")
    if override:
        candidate = Path(override)
        return candidate if candidate.exists() else None
    for candidate in (
        _ENGINE_DIR / "target" / "release" / "rocky",
        _ENGINE_DIR / "target" / "debug" / "rocky",
    ):
        if candidate.exists():
            return candidate
    found = shutil.which("rocky")
    return Path(found) if found else None


@dataclass(frozen=True)
class Environment:
    """The resolved external dependencies for a suite run."""

    rocky_bin: Path | None
    claude_bin: str | None
    duckdb_bin: str | None
    has_model_auth: bool

    def skip_reasons(self) -> list[str]:
        """Human-readable reasons the live suite cannot run, or ``[]``."""
        reasons: list[str] = []
        if self.rocky_bin is None:
            reasons.append(
                "the `rocky` binary was not found (set $ROCKY_BIN, or build it with "
                "`cargo build` in engine/)"
            )
        if self.claude_bin is None:
            reasons.append(
                "the `claude` CLI was not found on PATH (install @anthropic-ai/claude-code)"
            )
        if self.duckdb_bin is None:
            reasons.append("the `duckdb` CLI was not found on PATH (needed to seed the fixture)")
        if not self.has_model_auth:
            # Plain literal on purpose: interpolating the API_KEY_ENV constant
            # here makes static taint analysis treat a KEY-named symbol as data
            # reaching output. The suite never emits the key's value.
            reasons.append(
                "$ANTHROPIC_API_KEY is not set (the scripted agent loop needs a model "
                "key; this is expected on forks and PRs without the secret)"
            )
        return reasons

    def can_run_live(self) -> bool:
        return not self.skip_reasons()


def _env_is_set(name: str) -> bool:
    """True if env var ``name`` is set to a non-empty value.

    Reads only presence via a comparison, never binding the value into anything
    that flows onward — so the key itself cannot reach output. (The suite records
    only whether a key is present, never its value; this also keeps static taint
    analysis from treating a ``*KEY*``-named env read as data reaching a log.)
    """
    return os.environ.get(name, "") != ""


def resolve_environment() -> Environment:
    return Environment(
        rocky_bin=resolve_rocky_bin(),
        claude_bin=shutil.which("claude"),
        duckdb_bin=shutil.which("duckdb"),
        has_model_auth=_env_is_set(API_KEY_ENV),
    )
