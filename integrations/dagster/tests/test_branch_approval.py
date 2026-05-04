"""Pydantic + parser coverage for ``rocky branch approve`` / ``promote``.

The playground POC is replication-only and doesn't naturally exercise the
approval gate, so these fixtures are hand-crafted to mirror the JSON the
engine emits for a happy-path approve and an enforced promote. Refresh by
running the CLI manually if the schema changes; the assertions check the
load-bearing fields the dagster surface depends on.
"""

from __future__ import annotations

import json

from dagster_rocky.types import (
    ApprovalArtifact,
    ApproveOutput,
    AuditEvent,
    AuditEventKind,
    BranchPromoteOutput,
    parse_rocky_output,
)

_APPROVE_JSON = json.dumps(
    {
        "version": "1.24.0",
        "command": "branch approve",
        "artifact": {
            "approval_id": "20260503T120000000000-aaaaaaaa",
            "branch": "fix-price",
            "branch_state_hash": "deadbeef" * 8,
            "approver": {
                "email": "alice@example.com",
                "name": "Alice",
                "host": "host-1",
                "source": "local",
            },
            "signed_at": "2026-05-03T12:00:00Z",
            "message": "ship it",
            "signature": {
                "algorithm": "blake3_canonical_json",
                "digest": "f" * 64,
            },
        },
        "artifact_path": ".rocky/approvals/fix-price/20260503T120000000000-aaaaaaaa.json",
    }
)


_PROMOTE_JSON = json.dumps(
    {
        "version": "1.24.0",
        "command": "branch promote",
        "branch": "fix-price",
        "branch_state_hash": "deadbeef" * 8,
        "approvals_used": [
            {
                "approval_id": "20260503T120000000000-aaaaaaaa",
                "branch": "fix-price",
                "branch_state_hash": "deadbeef" * 8,
                "approver": {
                    "email": "alice@example.com",
                    "name": None,
                    "host": "host-1",
                    "source": "local",
                },
                "signed_at": "2026-05-03T12:00:00Z",
                "message": None,
                "signature": {
                    "algorithm": "blake3_canonical_json",
                    "digest": "0" * 64,
                },
            }
        ],
        "approvals_rejected": [],
        "targets": [
            {
                "target": "warehouse.public.orders",
                "source": "warehouse.branch__fix-price.orders",
                "statement": (
                    "CREATE OR REPLACE TABLE warehouse.public.orders AS "
                    "SELECT * FROM warehouse.branch__fix-price.orders"
                ),
                "succeeded": True,
                "error": None,
            }
        ],
        "audit": [
            {
                "kind": "promote_started",
                "at": "2026-05-03T12:00:01Z",
                "actor": {
                    "email": "alice@example.com",
                    "name": None,
                    "host": "host-1",
                    "source": "local",
                },
                "branch": "fix-price",
                "branch_state_hash": "deadbeef" * 8,
                "reason": None,
            },
            {
                "kind": "promote_completed",
                "at": "2026-05-03T12:00:02Z",
                "actor": {
                    "email": "alice@example.com",
                    "name": None,
                    "host": "host-1",
                    "source": "local",
                },
                "branch": "fix-price",
                "branch_state_hash": "deadbeef" * 8,
                "reason": None,
            },
        ],
        "success": True,
    }
)


def test_approve_output_parses() -> None:
    result = ApproveOutput.model_validate_json(_APPROVE_JSON)
    assert result.command == "branch approve"
    assert result.artifact.branch == "fix-price"
    assert result.artifact.signature.algorithm.root.value == "blake3_canonical_json"
    assert result.artifact.approver.email == "alice@example.com"


def test_promote_output_parses_happy_path() -> None:
    result = BranchPromoteOutput.model_validate_json(_PROMOTE_JSON)
    assert result.success is True
    assert result.branch == "fix-price"
    assert len(result.targets) == 1
    target = result.targets[0]
    assert target.target == "warehouse.public.orders"
    assert target.source == "warehouse.branch__fix-price.orders"
    assert target.succeeded is True

    # Audit events must be parseable as the discriminated enum.
    kinds = [event.kind for event in result.audit]
    assert AuditEventKind.promote_started in kinds
    assert AuditEventKind.promote_completed in kinds


def test_parse_rocky_output_dispatches_branch_commands() -> None:
    approve = parse_rocky_output(_APPROVE_JSON)
    assert isinstance(approve, ApproveOutput)
    promote = parse_rocky_output(_PROMOTE_JSON)
    assert isinstance(promote, BranchPromoteOutput)


def test_approval_artifact_round_trips() -> None:
    """The on-disk artifact (with the signature already stamped) must
    serialize and deserialize without losing fields — `branch promote`
    relies on this round-trip when loading every file under
    `.rocky/approvals/<branch>/` for verification."""
    artifact = ApproveOutput.model_validate_json(_APPROVE_JSON).artifact
    encoded = artifact.model_dump_json()
    decoded = ApprovalArtifact.model_validate_json(encoded)
    assert decoded.approval_id == artifact.approval_id
    assert decoded.signature.digest == artifact.signature.digest
    assert decoded.branch_state_hash == artifact.branch_state_hash


def test_audit_event_round_trips() -> None:
    payload = json.loads(_PROMOTE_JSON)["audit"][0]
    event = AuditEvent.model_validate(payload)
    assert event.kind == AuditEventKind.promote_started
    assert event.actor.source.value == "local"
