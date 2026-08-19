"""Freeze the extraction seam (:mod:`rocky_fulfillment._seam`).

These tests pin the seam's shape so any change to a version pin or to the
worker tool manifest shows up as a deliberate, visible test diff — never
as drift.
"""

from __future__ import annotations

import re

from rocky_fulfillment import (
    MIN_ROCKY_VERSION,
    REQUIRED_MCP_TOOLS,
    SPEC_VERSION,
    McpToolRequirement,
)

_IDENTIFIER = re.compile(r"[a-z][a-z0-9_]*")


def test_spec_version_is_frozen() -> None:
    assert SPEC_VERSION == "0"


def test_min_rocky_version_is_frozen() -> None:
    # Exact, not a floor: the docstring's contract is that raising this pin is
    # a deliberate diff made alongside the change that needs the newer engine.
    # A `>=` here would let the pin drift without a visible test change.
    assert MIN_ROCKY_VERSION == "1.70.1"


def test_manifest_is_frozen_exactly() -> None:
    # The golden manifest: every name and every argument field, frozen. Any
    # add, removal, or argument change is a deliberate, visible diff here.
    assert {req.name: req.args for req in REQUIRED_MCP_TOOLS} == {
        "list": frozenset({"kind"}),
        "inspect_schema": frozenset(),
        "sample_rows": frozenset({"model"}),
        "compile": frozenset({"model"}),
        "test": frozenset(),
        "breaking_change": frozenset({"base"}),
        "dependents": frozenset({"model"}),
        "draft_model": frozenset({"name", "sql", "intent"}),
        "draft_check": frozenset({"model", "spec"}),
    }


def test_manifest_entries_are_well_formed() -> None:
    for requirement in REQUIRED_MCP_TOOLS:
        assert isinstance(requirement, McpToolRequirement)
        assert _IDENTIFIER.fullmatch(requirement.name), requirement.name
        for arg in requirement.args:
            assert _IDENTIFIER.fullmatch(arg), (requirement.name, arg)


def test_manifest_names_are_unique() -> None:
    names = [requirement.name for requirement in REQUIRED_MCP_TOOLS]
    assert len(names) == len(set(names))


def test_manifest_requires_the_drafting_loop_tools() -> None:
    drafting_loop = {
        "compile",
        "test",
        "breaking_change",
        "dependents",
        "draft_model",
        "draft_check",
    }
    names = {requirement.name for requirement in REQUIRED_MCP_TOOLS}
    assert drafting_loop <= names


def test_manifest_never_requires_approval_or_spec_owned_surfaces() -> None:
    # Two-actor rule: proposing and applying belong to the trusted runner and
    # the human, and the contract and metadata are spec-owned. The worker
    # manifest must never require these surfaces — even if the engine exposes
    # them on other profiles.
    forbidden = {
        "propose",
        "apply",
        "review_queue",
        "pause_schedule",
        "draft_contract",
        "draft_metadata",
    }
    names = {requirement.name for requirement in REQUIRED_MCP_TOOLS}
    assert not (forbidden & names)
