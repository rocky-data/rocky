"""The extraction seam: version pins checked before any fulfillment run.

The framework sits on two moving surfaces: the ``rocky`` CLI (driven through
``rocky-sdk``) and the engine's MCP server (the worker agent's whole world).
This module pins what the framework requires from each, in one place, so a
version mismatch fails at startup with a clear pairing error instead of
failing somewhere in the middle of a run.

Every pin here is frozen by ``tests/test_seam.py``. Raising a pin is a
deliberate, reviewed change made in the same diff that starts depending on
the newer surface — never a side effect.
"""

from __future__ import annotations

from dataclasses import dataclass

SPEC_VERSION = "0"
"""Product-spec schema version this framework reads.

A spec file may pin ``spec_version = 0``; a spec with no ``spec_version``
key means the current version. A spec pinned to any other version is
refused, never shimmed.
"""


@dataclass(frozen=True, slots=True)
class McpToolRequirement:
    """One tool the worker-profile MCP server must expose.

    ``args`` is the minimum set of argument fields the framework may pass.
    The startup check requires the live server to list a tool with this
    name whose input schema accepts at least these fields.
    """

    name: str
    args: frozenset[str]


def _tool(name: str, *args: str) -> McpToolRequirement:
    return McpToolRequirement(name=name, args=frozenset(args))


REQUIRED_MCP_TOOLS: tuple[McpToolRequirement, ...] = (
    # Read/inspect: the grounding steps of the drafting loop.
    _tool("list", "kind"),
    _tool("inspect_schema"),
    _tool("sample_rows", "model"),
    # Verification: the compile/test loop and impact checks.
    _tool("compile", "model"),
    _tool("test"),
    _tool("breaking_change", "base"),
    _tool("dependents", "model"),
    # Drafting: the only writes the worker is trusted with.
    _tool("draft_model", "name", "sql", "intent"),
    _tool("draft_check", "model", "spec"),
)
"""Minimum tool surface of the worker-profile MCP server.

Checked against the live server's tool listing before any worker task is
dispatched. A missing tool or argument field is a version-pairing error,
raised at startup — never a mid-loop surprise.

This is a floor, not the profile. The worker profile itself is defined in
the engine as an allowlist, and it must never grow approval or apply
surfaces. This manifest must never require ``propose``, ``review_queue``,
``pause_schedule``, or ``draft_contract``: proposing and applying belong to
the trusted runner and the human, and the contract and metadata are owned
by the spec, not the worker. ``tests/test_seam.py`` pins that exclusion.
"""

MIN_ROCKY_VERSION = "1.70.1"
"""Minimum ``rocky`` CLI version the framework accepts.

``rocky-sdk`` carries its own floor; this is the framework's stricter pin:
1.70.1 is the released engine every tool in ``REQUIRED_MCP_TOOLS`` was
verified against. Raise it in the same change that starts depending on a
newer engine verb.
"""
