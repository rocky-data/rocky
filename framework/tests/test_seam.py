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


def test_min_rocky_version_parses_as_major_minor_patch() -> None:
    match = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)", MIN_ROCKY_VERSION)
    assert match is not None
    major, minor, patch = (int(part) for part in match.groups())
    # The manifest below was verified against released rocky 1.70.1; the
    # floor may only move forward from there.
    assert (major, minor, patch) >= (1, 70, 1)


def test_manifest_is_non_empty() -> None:
    assert len(REQUIRED_MCP_TOOLS) > 0


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
