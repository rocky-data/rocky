"""Execute the ``Example:`` snippets in ``RockyClient``'s core docstrings.

Docstring examples are the first thing a new SDK user copies, and nothing else
checks them: a renamed field or a changed shape leaves the prose compiling
(it's a string) and wrong. These tests extract the snippets from the live
docstrings and RUN them with the subprocess layer stubbed, so every attribute
access resolves against a real parsed model.

Writing this caught two real errors in the examples it documents (#898):
``TableError`` has no ``.table`` — the field is ``asset_key``, and it is a
``list[str]`` rather than a string.
"""

from __future__ import annotations

import ast
import json
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from rocky_sdk import RockyClient, RockyCommandError

DOCUMENTED_METHODS = (
    "apply",
    "discover",
    "plan",
    "run",
    "product_verify",
    "product_compile",
    "product_approve",
    "product_status",
    "product_list",
    "product_journal",
)

# The four core methods, executed exactly as before (#1387): one fixture each,
# no output assertion. The six `product_*` methods are NOT run this way: see
# `PRODUCT_CASES` below, which drives each documented branch separately and
# asserts on stdout, per the #2108 red-team finding that a single un-asserted
# execution does not catch a renamed field once it feeds an optional attribute
# or an untaken branch.
CORE_METHODS = ("apply", "discover", "plan", "run")

# Minimal but SCHEMA-VALID engine payloads. Every required field is present —
# a fixture that fails validation would make these tests fail for a reason
# that has nothing to do with the examples.
DISCOVER_PAYLOAD: dict[str, Any] = {
    "version": "1",
    "command": "discover",
    "sources": [
        {
            "id": "acme",
            "components": {"client": "acme"},
            "source_type": "postgres",
            "tables": [{"name": "orders", "row_count": 42}],
        }
    ],
    "failed_sources": [],
}

PLAN_PAYLOAD: dict[str, Any] = {
    "version": "1",
    "command": "plan",
    "filter": "client=acme",
    "statements": [
        {
            "purpose": "create table",
            "target": "wh.raw.orders",
            "sql": "CREATE TABLE wh.raw.orders (...)",
        }
    ],
    "plan_id": "a1b2c3d4",
    "plan_kind": "run",
}

RUN_PAYLOAD: dict[str, Any] = {
    "version": "1",
    "command": "run",
    "filter": "client=acme",
    "duration_ms": 10,
    "tables_copied": 1,
    "tables_failed": 1,
    "status": "PartialFailure",
    "materializations": [],
    "check_results": [],
    "permissions": {
        "grants_added": 0,
        "grants_revoked": 0,
        "catalogs_created": 0,
        "schemas_created": 0,
    },
    "drift": {"tables_checked": 0, "tables_drifted": 0, "actions_taken": []},
    # `asset_key` is a list of path segments, and the run example joins it.
    "errors": [{"asset_key": ["raw", "orders"], "error": "boom", "failure_kind": "query"}],
}

# `rocky apply` of a run-shaped plan prints a RunOutput, so it reuses the payload.
PAYLOADS = {
    "discover": DISCOVER_PAYLOAD,
    "plan": PLAN_PAYLOAD,
    "run": RUN_PAYLOAD,
    "apply": RUN_PAYLOAD,
}

# Snippets that continue an earlier one (no `RockyClient(...)` of their own)
# get the earlier one's bindings rather than being skipped.
PRELUDE = {
    "discover": "from rocky_sdk import RockyClient\n"
    "client = RockyClient()\n"
    "result = client.discover()\n",
    "run": "from rocky_sdk import RockyClient\nclient = RockyClient()\n",
}


def _client_source() -> str:
    import rocky_sdk.client

    return Path(rocky_sdk.client.__file__).read_text(encoding="utf-8")


def _docstrings() -> dict[str, str]:
    tree = ast.parse(_client_source())
    found = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name in DOCUMENTED_METHODS:
            doc = ast.get_docstring(node)
            if doc is not None:
                found[node.name] = doc
    return found


def _snippets(doc: str) -> list[str]:
    """Extract the indented code blocks introduced by a reST ``::`` line."""
    out: list[str] = []
    cur: list[str] = []
    base: int | None = None
    for line in doc.splitlines():
        if base is None:
            if line.rstrip().endswith("::"):
                base = -1  # the next non-blank line sets the indent
            continue
        if not line.strip():
            if cur:
                cur.append("")
            continue
        indent = len(line) - len(line.lstrip())
        if base == -1:
            base = indent
        if indent >= base:
            cur.append(line[base:])
        else:
            if cur:
                out.append("\n".join(cur).rstrip())
                cur = []
            base = -1 if line.rstrip().endswith("::") else None
    if cur:
        out.append("\n".join(cur).rstrip())
    return [s for s in out if s.strip()]


def _fake_run_cli(self: RockyClient, args: list[str], **_kwargs: Any) -> str:
    return json.dumps(PAYLOADS[args[0]])


@pytest.mark.parametrize("method", DOCUMENTED_METHODS)
def test_core_method_has_a_runnable_example(method: str) -> None:
    """#898/#1802: each documented method carries a worked example.

    Asserted separately from execution so deleting an example fails loudly
    instead of silently reducing what the executing test covers to nothing.
    """
    doc = _docstrings().get(method)
    assert doc is not None, f"{method} has no docstring"
    assert "Example:" in doc, f"{method} has no Example: block"
    assert _snippets(doc), f"{method}'s Example: block has no extractable code"


@pytest.mark.parametrize("method", CORE_METHODS)
def test_docstring_examples_execute_against_real_models(method: str) -> None:
    """Run each snippet with IO stubbed — a wrong field name fails here.

    Compiling is not enough: `error.table` parses fine and raises
    `AttributeError` only when executed, which is exactly the class of mistake
    a copied example inflicts on a new user.
    """
    doc = _docstrings()[method]
    snippets = _snippets(doc)
    assert snippets, f"probe bug: nothing extracted for {method}"

    with mock.patch.object(RockyClient, "run_cli", _fake_run_cli):
        for snippet in snippets:
            code = textwrap.dedent(snippet)
            namespace: dict[str, Any] = {"__name__": "__doc_example__"}
            if "RockyClient(" not in code:
                exec(PRELUDE.get(method, ""), namespace)  # noqa: S102 - trusted docstring
            exec(code, namespace)  # noqa: S102 - trusted docstring


# --------------------------------------------------------------------------- #
# product_* branch cases (#2108 red-team fix)                                 #
# --------------------------------------------------------------------------- #
#
# The single-fixture-per-method approach above caught a wrong field name only
# when the executed branch's own attribute access raised. It missed two real
# classes of bug, both found by an independent review of #2108:
#
# 1. An optional field is required to drive a branch (`paste_block`,
#    `spec_matches_approval`, `spec_error`). Renaming that key in the fixture
#    does not raise: pydantic silently defaults the model's field to `None`,
#    so the branch that reads it either prints "None" or is silently
#    skipped. Nothing failed, but nothing was proven either.
# 2. `product_journal`'s `except RockyCommandError as exc: ... exc.stderr_tail`
#    was never exercised at all: every fixture returned a successful payload,
#    so the `except` block's body, the very thing added to fix the previous
#    red-team finding, never ran, and a typo there (`stderr_tail` ->
#    `stderr_tial`) passed clean.
#
# Each case below drives one specific branch and asserts the printed output
# contains (or, for the untaken branch, omits) the exact value that branch's
# fixture supplied, so a renamed or misspelled field fails a stdout assertion
# instead of silently not being exercised.


class _Raises:
    """Sentinel: the mocked ``run_cli`` should raise this instead of returning JSON."""

    def __init__(self, exc: Exception) -> None:
        self.exc = exc


PRODUCT_VERIFY_PASS: dict[str, Any] = {
    "version": "1",
    "command": "product verify",
    "product_id": "product:orders_mart",
    "spec_digest": "sha256:cafefeed",
    "status": "pass",
    "reason": "posture verified",
    "output_model": "orders_mart",
}

PRODUCT_VERIFY_NEEDS_INPUT: dict[str, Any] = {
    "version": "1",
    "command": "product verify",
    "product_id": "product:orders_mart",
    "spec_digest": "sha256:aa",
    "status": "needs_input",
    "paste_block": '[policy]\nagent_apply = "deny"',
    "reason": "classification tag unresolved",
    "output_model": "orders_mart",
}

PRODUCT_VERIFY_FAIL: dict[str, Any] = {
    "version": "1",
    "command": "product verify",
    "product_id": "product:orders_mart",
    "spec_digest": "sha256:aa",
    "status": "fail",
    "reason": "agent apply resolves allow",
    "output_model": "orders_mart",
}

PRODUCT_COMPILE_MATCHES: dict[str, Any] = {
    "version": "1",
    "command": "product compile",
    "product_id": "product:orders_mart",
    "spec_digest": "sha256:aa",
    "spec_path": "products/orders_mart.toml",
    "output_model": "orders_mart",
    "phase": "lowered_contract",
    "manifest_path": "products/.rocky/orders_mart.manifest.json",
    "artifacts": [{"path": "products/orders_mart.sql", "sha256": "sha256:bb"}],
    "spec_matches_approval": True,
}

PRODUCT_COMPILE_SUPERSEDED: dict[str, Any] = {
    **PRODUCT_COMPILE_MATCHES,
    "spec_matches_approval": False,
}

PRODUCT_APPROVE_FRESH: dict[str, Any] = {
    "version": "1",
    "command": "product approve",
    "product_id": "product:orders_mart",
    "spec_digest": "sha256:aa",
    "output_model": "orders_mart",
    "already_approved": False,
    "approved_at": "2026-09-22T00:00:00Z",
    "approver": "hugo",
    "snapshot_path": "products/.rocky/orders_mart.snapshot.json",
    "state": "spec_approved",
}

PRODUCT_STATUS_PRESENT: dict[str, Any] = {
    "version": "1",
    "command": "product status",
    "product": "orders_mart",
    "spec_present": True,
    "staging_journal_present": False,
    "journal_rows": 3,
    "artifact_problems": [],
    "committed_phase": "lowered_contract",
}

PRODUCT_STATUS_ABSENT: dict[str, Any] = {
    "version": "1",
    "command": "product status",
    "product": "orders_mart",
    "spec_present": False,
    "staging_journal_present": False,
    "journal_rows": 0,
    "artifact_problems": [],
    "spec_error": "products/orders_mart.toml: invalid TOML at line 4",
}

PRODUCT_LIST_ZERO: dict[str, Any] = {
    "version": "1",
    "command": "product list",
    "count": 1,
    "products": [
        {
            "name": "orders_mart",
            "spec_present": True,
            "staging_journal_present": False,
            "journal_rows": 3,
            "artifact_problems": 0,
            "committed_phase": "lowered_contract",
            "fulfill_state": "spec_approved",
        }
    ],
}

PRODUCT_LIST_NONZERO: dict[str, Any] = {
    **PRODUCT_LIST_ZERO,
    "products": [{**PRODUCT_LIST_ZERO["products"][0], "artifact_problems": 2}],
}

PRODUCT_JOURNAL_ROWS: dict[str, Any] = {
    "version": "1",
    "command": "product journal",
    "product": "orders_mart",
    "product_id": "product:orders_mart",
    "count": 1,
    "rows": [
        {
            "seq": 0,
            "event": "spec approved",
            "to_state": "spec_approved",
            "at": "2026-09-22T00:00:00Z",
        }
    ],
}

PRODUCT_JOURNAL_EMPTY: dict[str, Any] = {
    **PRODUCT_JOURNAL_ROWS,
    "count": 0,
    "rows": [],
}

PRODUCT_JOURNAL_UNKNOWN = _Raises(
    RockyCommandError(1, stderr_tail="unknown product: orders_mart", command="product_journal")
)


@dataclass(frozen=True)
class ProductCase:
    """One documented branch of one `product_*` method's example."""

    method: str
    case_id: str
    run_cli: Any  # a JSON-able payload dict, or a `_Raises` instance
    expect: tuple[str, ...] = ()
    """Substrings that MUST appear in stdout: the branch's driving value."""
    forbid: tuple[str, ...] = ()
    """Substrings that must NOT appear: proves the untaken branch stayed untaken."""


PRODUCT_CASES: list[ProductCase] = [
    ProductCase("product_verify", "pass", PRODUCT_VERIFY_PASS, expect=("sha256:cafefeed",)),
    ProductCase(
        "product_verify",
        "needs_input",
        PRODUCT_VERIFY_NEEDS_INPUT,
        expect=('agent_apply = "deny"',),
    ),
    ProductCase(
        "product_verify", "fail", PRODUCT_VERIFY_FAIL, expect=("agent apply resolves allow",)
    ),
    ProductCase(
        "product_compile",
        "matches",
        PRODUCT_COMPILE_MATCHES,
        expect=("lowered_contract",),
        forbid=("moved past the approval",),
    ),
    ProductCase(
        "product_compile",
        "superseded",
        PRODUCT_COMPILE_SUPERSEDED,
        expect=("moved past the approval",),
    ),
    ProductCase("product_approve", "fresh", PRODUCT_APPROVE_FRESH, expect=("sha256:aa", "hugo")),
    ProductCase("product_status", "present", PRODUCT_STATUS_PRESENT, expect=("lowered_contract",)),
    ProductCase(
        "product_status",
        "absent",
        PRODUCT_STATUS_ABSENT,
        expect=("invalid TOML at line 4",),
    ),
    ProductCase(
        "product_list",
        "zero",
        PRODUCT_LIST_ZERO,
        expect=("orders_mart",),
        forbid=("byte-verification problems",),
    ),
    ProductCase(
        "product_list",
        "nonzero",
        PRODUCT_LIST_NONZERO,
        expect=("byte-verification problems: 2",),
    ),
    ProductCase("product_journal", "rows", PRODUCT_JOURNAL_ROWS, expect=("spec approved",)),
    ProductCase(
        "product_journal",
        "empty",
        PRODUCT_JOURNAL_EMPTY,
        expect=("has no fulfillment history yet",),
    ),
    ProductCase(
        "product_journal",
        "unknown_product",
        PRODUCT_JOURNAL_UNKNOWN,
        expect=("unknown product: orders_mart",),
    ),
]


@pytest.mark.parametrize("case", PRODUCT_CASES, ids=lambda c: f"{c.method}__{c.case_id}")
def test_product_example_branch_prints_the_driving_value(
    case: ProductCase, capsys: pytest.CaptureFixture[str]
) -> None:
    """#2108 red-team fix: assert on stdout, not just on "did it raise".

    Each case mocks ``run_cli`` to return (or raise) exactly the fixture that
    drives one documented branch, then asserts the branch's own value shows up
    in the printed output. A renamed fixture key, a flipped condition, or a
    misspelled attribute inside a branch that only an exception path reaches
    (`product_journal`'s ``except ... as exc: exc.stderr_tail``) now fails
    here instead of passing silently.
    """
    doc = _docstrings()[case.method]
    snippet = _snippets(doc)[0]
    code = textwrap.dedent(snippet)

    def fake_run_cli(self: RockyClient, args: list[str], **_kwargs: Any) -> str:
        if isinstance(case.run_cli, _Raises):
            raise case.run_cli.exc
        return json.dumps(case.run_cli)

    with mock.patch.object(RockyClient, "run_cli", fake_run_cli):
        exec(code, {"__name__": "__doc_example__"})  # noqa: S102 - trusted docstring

    out = capsys.readouterr().out
    for token in case.expect:
        assert token in out, f"{case.method}:{case.case_id} expected {token!r} in: {out!r}"
    for token in case.forbid:
        msg = f"{case.method}:{case.case_id} forbidden {token!r} found in: {out!r}"
        assert token not in out, msg
