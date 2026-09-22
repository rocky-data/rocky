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
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from rocky_sdk import RockyClient

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

PRODUCT_VERIFY_PAYLOAD: dict[str, Any] = {
    "version": "1",
    "command": "product verify",
    "product_id": "product:orders_mart",
    "spec_digest": "sha256:aa",
    # `needs_input` exercises the example's `paste_block` branch.
    "status": "needs_input",
    "paste_block": '[policy]\nagent_apply = "deny"',
    "reason": "classification tag unresolved",
    "output_model": "orders_mart",
}

PRODUCT_COMPILE_PAYLOAD: dict[str, Any] = {
    "version": "1",
    "command": "product compile",
    "product_id": "product:orders_mart",
    "spec_digest": "sha256:aa",
    "spec_path": "products/orders_mart.toml",
    "output_model": "orders_mart",
    "phase": "lowered_contract",
    "manifest_path": "products/.rocky/orders_mart.manifest.json",
    "artifacts": [{"path": "products/orders_mart.sql", "sha256": "sha256:bb"}],
    # `False` exercises the example's supersession-warning branch.
    "spec_matches_approval": False,
}

PRODUCT_APPROVE_PAYLOAD: dict[str, Any] = {
    "version": "1",
    "command": "product approve",
    "product_id": "product:orders_mart",
    "spec_digest": "sha256:aa",
    "output_model": "orders_mart",
    # `False` exercises the example's fresh-approval branch.
    "already_approved": False,
    "approved_at": "2026-09-22T00:00:00Z",
    "approver": "hugo",
    "snapshot_path": "products/.rocky/orders_mart.snapshot.json",
    "state": "spec_approved",
}

PRODUCT_STATUS_PAYLOAD: dict[str, Any] = {
    "version": "1",
    "command": "product status",
    "product": "orders_mart",
    "spec_present": True,
    "staging_journal_present": False,
    "journal_rows": 3,
    "artifact_problems": [],
    "committed_phase": "lowered_contract",
}

PRODUCT_LIST_PAYLOAD: dict[str, Any] = {
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

PRODUCT_JOURNAL_PAYLOAD: dict[str, Any] = {
    "version": "1",
    "command": "product journal",
    "product": "orders_mart",
    "product_id": "product:orders_mart",
    "count": 1,
    # A non-empty journal exercises the `for row in result.rows` branch.
    # The example's `except RockyCommandError as exc` branch reads
    # `exc.stderr_tail`, checked separately since this fixture returns a
    # successful payload instead of raising.
    "rows": [
        {
            "seq": 0,
            "event": "spec approved",
            "to_state": "spec_approved",
            "at": "2026-09-22T00:00:00Z",
        }
    ],
}

# `rocky apply` of a run-shaped plan prints a RunOutput, so it reuses the payload.
# `product <verb>` payloads are keyed by method name (`_payload_key` maps the
# `["product", "<verb>", ...]` argv to it) since `args[0]` alone is `"product"`
# for all six.
PAYLOADS = {
    "discover": DISCOVER_PAYLOAD,
    "plan": PLAN_PAYLOAD,
    "run": RUN_PAYLOAD,
    "apply": RUN_PAYLOAD,
    "product_verify": PRODUCT_VERIFY_PAYLOAD,
    "product_compile": PRODUCT_COMPILE_PAYLOAD,
    "product_approve": PRODUCT_APPROVE_PAYLOAD,
    "product_status": PRODUCT_STATUS_PAYLOAD,
    "product_list": PRODUCT_LIST_PAYLOAD,
    "product_journal": PRODUCT_JOURNAL_PAYLOAD,
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


def _payload_key(args: list[str]) -> str:
    """Map CLI argv to its ``PAYLOADS`` key.

    ``args[0]`` alone (``"discover"``, ``"plan"``, ``"run"``, ``"apply"``) is
    enough for the four core commands. ``rocky product <verb>`` needs the
    subverb too, since ``args[0]`` is just ``"product"`` for all six. The key
    becomes the method name, e.g. ``["product", "verify", "orders_mart"]`` ->
    ``"product_verify"``.
    """
    if args[0] == "product":
        return f"product_{args[1]}"
    return args[0]


def _fake_run_cli(self: RockyClient, args: list[str], **_kwargs: Any) -> str:
    return json.dumps(PAYLOADS[_payload_key(args)])


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


@pytest.mark.parametrize("method", DOCUMENTED_METHODS)
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
