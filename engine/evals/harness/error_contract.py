"""Structured-error contract check — creds-free, no model call.

The MCP surface is the interface agents work through, so its *failure* paths get
the same rigor as its success paths: every failing tool call must come back as a
tool-result error (``isError: true``) whose ``structuredContent`` is a
``{code, message, remediation_hint, policy_rule?}`` envelope — a stable machine
code plus an *actionable* hint the agent can route on, the tool-layer analog of
Rocky's diagnostic codes.

This module asserts that contract end-to-end by driving a real ``rocky mcp``
server over stdio with deliberately bad input and checking the envelope shape and
hint quality with **deterministic assertions**. It needs only the ``rocky``
binary — no ``$ANTHROPIC_API_KEY``, no ``claude`` CLI, no warehouse, no live
model — so it is the half of the suite that actually *runs* on the no-secret CI
path (the live authoring scenarios skip there).

It also pins the complementary shape: a genuine **compile error** is *not* an
envelope — it is a successful call reporting ``has_errors: true`` with
diagnostics (each already carrying code/message/span/suggestion — the same house
style). Forcing it into the error envelope would be wrong, so the two shapes are
asserted separately.
"""

from __future__ import annotations

import json
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

# The MCP protocol version the client advertises. Any version the server accepts
# works; it negotiates down. Pinned so the handshake is reproducible.
_PROTOCOL_VERSION = "2025-06-18"
# Overall wall budget for one server round-trip (spawn + handshake + one call).
# Generous: a cold binary on a loaded CI box, never interactive.
_CALL_TIMEOUT_S = 60


# --------------------------------------------------------------------------
# minimal MCP stdio client (stdlib only)
# --------------------------------------------------------------------------


def _mcp_call_tool(
    rocky_bin: Path,
    project_dir: Path,
    tool: str,
    arguments: dict,
    config: str = "rocky.toml",
) -> dict:
    """Drive one ``tools/call`` against a fresh ``rocky mcp`` server.

    Batches the three JSON-RPC messages the MCP handshake needs (initialize →
    initialized notification → the tool call) onto stdin, closes it, and reads
    the server's line-delimited responses off stdout. Closing stdin gives the
    stdio server EOF so it exits on its own — no interactive read loop to
    deadlock. Returns the ``CallToolResult`` object for the tool call.
    """
    requests = [
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": _PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "rocky-error-contract-eval", "version": "0"},
            },
        },
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": tool, "arguments": arguments},
        },
    ]
    payload = "".join(json.dumps(r) + "\n" for r in requests)

    proc = subprocess.Popen(
        [str(rocky_bin), "mcp", "--config", config],
        cwd=str(project_dir),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        out, err = proc.communicate(input=payload, timeout=_CALL_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        proc.kill()
        out, err = proc.communicate()
        raise RuntimeError(
            f"`rocky mcp` did not answer tools/call within {_CALL_TIMEOUT_S}s; "
            f"stderr tail: {err[-500:]!r}"
        ) from None

    # The server logs to stderr; stdout is the JSON-RPC wire. Accept only the
    # line that parses to the response for our tool call (id == 2) — a stray log
    # line on stdout can never be mistaken for the result.
    for line in out.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue
        if msg.get("id") != 2:
            continue
        if "result" in msg:
            return msg["result"]
        if "error" in msg:
            # A JSON-RPC *protocol* error is the wrong shape: the contract is a
            # tool-*result* error, so surface this as a contract failure.
            raise RuntimeError(
                f"tool '{tool}' returned a JSON-RPC protocol error, not a tool-result "
                f"error envelope: {msg['error']}"
            )
    raise RuntimeError(
        f"no tools/call response from `rocky mcp` for '{tool}'. stdout={out[-500:]!r} "
        f"stderr={err[-500:]!r}"
    )


# --------------------------------------------------------------------------
# fixtures — minimal, self-contained projects per error case
# --------------------------------------------------------------------------

_MINIMAL_CONFIG = """\
[adapter]
type = "duckdb"
path = "poc.duckdb"

[pipeline.poc]
strategy = "full_refresh"

[pipeline.poc.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.poc.target]
catalog_template = "poc"
schema_template = "demo"
"""


def _model_sidecar(name: str) -> str:
    return (
        f'name = "{name}"\n\n'
        '[strategy]\ntype = "full_refresh"\n\n'
        f'[target]\ncatalog = "poc"\nschema = "demo"\ntable = "{name}"\n'
    )


def _write_valid_project(project: Path) -> None:
    """A minimal, compilable DuckDB project (no warehouse connection needed).

    Uses a per-model TOML sidecar (not just `_defaults.toml`): the `rocky`
    model loader requires each `.sql` to carry frontmatter or a sidecar, so a
    bare `.sql` would fail to load rather than compile.
    """
    (project / "models").mkdir(parents=True, exist_ok=True)
    (project / "rocky.toml").write_text(_MINIMAL_CONFIG)
    # A self-contained model with no source refs — compiles on a cold cache.
    (project / "models" / "orders.sql").write_text("SELECT 1 AS id, 'COMPLETE' AS status\n")
    (project / "models" / "orders.toml").write_text(_model_sidecar("orders"))


def _write_broken_config_project(project: Path) -> None:
    """A project whose `rocky.toml` is present but syntactically invalid."""
    (project / "models").mkdir(parents=True, exist_ok=True)
    # Unterminated table header — a hard TOML parse error, so config load fails.
    (project / "rocky.toml").write_text('[adapter\ntype = "duckdb"\n')


def _write_compile_error_project(project: Path) -> None:
    """A project with one model that fails to compile (a real compile *error*).

    The model references an unbound run variable (`@var(threshold)`) with no
    supplied value and no inline default — an E028 error diagnostic. This fires
    on a cold cache (no warm source schema needed) and, unlike a raw SQL parse
    error (which is a *pipeline* failure surfaced as the error envelope), it is a
    successful compile call reporting `has_errors: true` with diagnostics — the
    exact shape this case is meant to pin.
    """
    (project / "models").mkdir(parents=True, exist_ok=True)
    (project / "rocky.toml").write_text(_MINIMAL_CONFIG)
    (project / "models" / "bad.sql").write_text("SELECT 1 AS id WHERE 1 = @var(threshold)\n")
    (project / "models" / "bad.toml").write_text(_model_sidecar("bad"))


def _write_parse_error_project(project: Path) -> None:
    """A project whose model SQL does not parse — a compile *pipeline* failure
    (distinct from an E-code diagnostic), which surfaces as the error envelope."""
    (project / "models").mkdir(parents=True, exist_ok=True)
    (project / "rocky.toml").write_text(_MINIMAL_CONFIG)
    (project / "models" / "bad.sql").write_text("SELECT FROM\n")
    (project / "models" / "bad.toml").write_text(_model_sidecar("bad"))


#: A `[policy]` block that denies an agent authoring (`propose`-class) into any
#: scope — the deterministic lever for the `draft_model` policy-deny path.
_DENY_PROPOSE_POLICY = """
[policy]
version = 1
default_agent_effect = "require_review"

[[policy.rules]]
principal = "agent"
capability = "propose"
scope = { any = true }
effect = "deny"
"""


def _write_policy_denied_draft_project(project: Path) -> None:
    """A project whose policy denies agent authorship, so `draft_model` must
    return a structured ``policy_denied`` envelope AND leave no draft on disk.

    A `_defaults.toml` supplies the target so the draft would otherwise compile —
    the deny, not a compile failure, is what stops it. This pins the deny
    ordering (write → compile → policy → rollback) end-to-end through the real
    binary, creds-free.
    """
    (project / "models").mkdir(parents=True, exist_ok=True)
    (project / "rocky.toml").write_text(_MINIMAL_CONFIG + _DENY_PROPOSE_POLICY)
    (project / "models" / "_defaults.toml").write_text(
        '[target]\ncatalog = "poc"\nschema = "demo"\n'
    )


# --------------------------------------------------------------------------
# cases
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class EnvelopeCase:
    """A tool call that must fail with the structured error envelope."""

    id: str
    setup: Callable[[Path], None]
    tool: str
    arguments: dict
    expected_code: str
    #: Substrings the remediation_hint must contain (all of them) — the
    #: deterministic hint-quality bar: the hint must point somewhere actionable.
    hint_contains_all: tuple[str, ...] = ()
    #: Substrings the remediation_hint must contain at least one of.
    hint_contains_any: tuple[str, ...] = ()
    #: Project-relative paths that must NOT exist after the call — the write-side
    #: assertion (a policy-denied draft leaves no artifact on disk).
    absent_after: tuple[str, ...] = ()


ENVELOPE_CASES: tuple[EnvelopeCase, ...] = (
    EnvelopeCase(
        id="invalid_argument__unknown_dialect",
        setup=_write_valid_project,
        tool="compile",
        arguments={"target_dialect": "redshift"},
        expected_code="invalid_argument",
        # The hint must name the accepted dialects, not just say "bad value".
        hint_contains_all=("duckdb", "snowflake"),
    ),
    EnvelopeCase(
        id="invalid_argument__unknown_list_kind",
        setup=_write_valid_project,
        tool="list",
        arguments={"kind": "frobnicate"},
        expected_code="invalid_argument",
        hint_contains_all=("models",),
    ),
    EnvelopeCase(
        id="config_invalid__malformed_rocky_toml",
        setup=_write_broken_config_project,
        tool="governance_preview",
        arguments={},
        expected_code="config_invalid",
        hint_contains_all=("rocky.toml",),
    ),
    EnvelopeCase(
        id="model_not_found__unknown_model",
        setup=_write_valid_project,
        tool="dependents",
        arguments={"model": "ghost"},
        expected_code="model_not_found",
        # The hint must route the agent to a discovery tool.
        hint_contains_any=("list", "inspect_schema"),
    ),
    EnvelopeCase(
        id="compile_failed__unparseable_sql",
        setup=_write_parse_error_project,
        tool="compile",
        arguments={},
        expected_code="compile_failed",
        hint_contains_all=("compile",),
    ),
    EnvelopeCase(
        id="policy_denied__draft_into_denied_scope",
        setup=_write_policy_denied_draft_project,
        tool="draft_model",
        arguments={
            "name": "revenue_draft",
            "sql": "SELECT 1 AS id",
            "intent": "a draft the policy denies",
        },
        expected_code="policy_denied",
        # The hint must route the agent to a reroute, not a retry.
        hint_contains_any=("Re-scope", "different"),
        # THE PIN: the denied draft is rolled back — no file left on disk.
        absent_after=("models/revenue_draft.sql", "models/revenue_draft.toml"),
    ),
)


@dataclass
class CaseResult:
    id: str
    passed: bool
    detail: str


def _nonempty_str(obj: dict, key: str) -> str | None:
    value = obj.get(key)
    if isinstance(value, str) and value.strip():
        return value
    return None


def _check_envelope_case(rocky_bin: Path, case: EnvelopeCase) -> CaseResult:
    with TemporaryDirectory(prefix=f"rocky-errctr-{case.id}-") as tmp:
        project = Path(tmp)
        case.setup(project)
        try:
            result = _mcp_call_tool(rocky_bin, project, case.tool, case.arguments)
        except RuntimeError as exc:
            return CaseResult(case.id, False, str(exc))

        # Write-side assertion, checked before the temp project is cleaned up: a
        # policy-denied draft must leave no artifact on disk.
        leaked = [rel for rel in case.absent_after if (project / rel).exists()]
        if leaked:
            return CaseResult(
                case.id, False, f"paths that must not exist were left behind: {leaked}"
            )

    if result.get("isError") is not True:
        return CaseResult(case.id, False, f"expected isError=true, got {result.get('isError')!r}")

    envelope = result.get("structuredContent")
    if not isinstance(envelope, dict):
        return CaseResult(case.id, False, f"no structuredContent envelope: {result!r}")

    code = envelope.get("code")
    if code != case.expected_code:
        return CaseResult(case.id, False, f"code={code!r}, expected {case.expected_code!r}")

    if _nonempty_str(envelope, "message") is None:
        return CaseResult(case.id, False, "message is empty or missing")

    hint = _nonempty_str(envelope, "remediation_hint")
    if hint is None:
        return CaseResult(case.id, False, "remediation_hint is empty or missing")

    missing_all = [s for s in case.hint_contains_all if s not in hint]
    if missing_all:
        return CaseResult(case.id, False, f"hint missing required text {missing_all}: {hint!r}")
    if case.hint_contains_any and not any(s in hint for s in case.hint_contains_any):
        return CaseResult(
            case.id, False, f"hint contains none of {case.hint_contains_any}: {hint!r}"
        )

    return CaseResult(case.id, True, f"code={code} hint={hint!r}")


def _check_compile_error_shape(rocky_bin: Path) -> CaseResult:
    """A genuine compile error is a *successful* call with diagnostics — NOT the
    error envelope. Assert that complementary shape so the two never blur."""
    case_id = "compile_error__diagnostics_not_envelope"
    with TemporaryDirectory(prefix="rocky-errctr-compile-") as tmp:
        project = Path(tmp)
        _write_compile_error_project(project)
        try:
            result = _mcp_call_tool(rocky_bin, project, "compile", {})
        except RuntimeError as exc:
            return CaseResult(case_id, False, str(exc))

    if result.get("isError") is True:
        return CaseResult(case_id, False, "a compile error must NOT be a tool-result error")
    body = result.get("structuredContent")
    if not isinstance(body, dict):
        return CaseResult(case_id, False, f"no structuredContent: {result!r}")
    if body.get("has_errors") is not True:
        return CaseResult(
            case_id, False, f"expected has_errors=true, got {body.get('has_errors')!r}"
        )
    diagnostics = body.get("diagnostics")
    if not isinstance(diagnostics, list) or not diagnostics:
        return CaseResult(case_id, False, "expected non-empty diagnostics")
    # Every diagnostic carries the actionable house style: a code and a message.
    for diag in diagnostics:
        if _nonempty_str(diag, "code") is None or _nonempty_str(diag, "message") is None:
            return CaseResult(case_id, False, f"diagnostic missing code/message: {diag!r}")
    return CaseResult(case_id, True, f"{len(diagnostics)} diagnostic(s), each with code+message")


def run(rocky_bin: Path) -> tuple[int, list[CaseResult]]:
    """Run every error-contract case. Returns (failure_count, results)."""
    results = [_check_envelope_case(rocky_bin, case) for case in ENVELOPE_CASES]
    results.append(_check_compile_error_shape(rocky_bin))
    failures = sum(1 for r in results if not r.passed)
    return failures, results


def main() -> int:
    """Creds-free entry point: needs only the `rocky` binary; skips clean without
    it (exit 0), fails hard on a broken contract (exit 1)."""
    # Imported lazily so this module stays importable without the rest of the
    # harness on the path.
    from harness.environ import resolve_rocky_bin

    rocky_bin = resolve_rocky_bin()
    if rocky_bin is None:
        print(
            "error-contract: SKIP — the `rocky` binary was not found "
            "(set $ROCKY_BIN or build it with `cargo build` in engine/)",
            file=sys.stderr,
        )
        return 0

    failures, results = run(rocky_bin)
    for r in results:
        status = "ok   " if r.passed else "FAIL "
        print(f"{status} {r.id}: {r.detail}", file=sys.stderr)
    if failures:
        print(f"\nerror-contract: {failures} failure(s)", file=sys.stderr)
        return 1
    print(f"\nerror-contract: all {len(results)} cases passed", file=sys.stderr)
    return 0
