"""Schema-parity guard: the runtime dispatch targets must not drop wire fields.

Rocky's ``--output json`` payloads are validated by hand-written Pydantic models
in :mod:`rocky_sdk.types` (the runtime dispatch targets that
``parse_rocky_output`` and ``RockyClient``'s methods route to). Those models use
``extra="ignore"`` for forward-compat, which means any wire field NOT declared
on the model is silently dropped — a consumer mapping it (e.g. dagster-rocky
surfacing a run's containment blast radius) would never see anything. That is
exactly how the shadow-drift bug cluster (#924 / #1061) hid at scale.

This test is the drift guard the codegen pipeline lacks for the hand-written
layer: for every runtime dispatch target it loads the corresponding
``schemas/<command>.schema.json`` and asserts every top-level schema property
has a matching field on the target model, recursing one level into the known
nested command-output objects (materialization items, check results, execution
summary).

Generated-type dispatch targets (soft-swapped aliases like ``TestResult`` /
``HistoryResult``) are included too: they trivially pass because they are
generated FROM the schema, so the guard also catches a future regression that
un-aliases one back to a hand-written shadow.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from rocky_sdk import types as sdk_types
from rocky_sdk.client import (
    ApplyResult,  # noqa: F401  (documents the apply union is intentionally not a single model)
)
from rocky_sdk.types import _SIMPLE_DISPATCH


def _schemas_dir() -> Path:
    """Locate the repo-root ``schemas/`` directory from this test file."""
    here = Path(__file__).resolve()
    for parent in here.parents:
        candidate = parent / "schemas"
        if (candidate / "run.schema.json").exists():
            return candidate
    raise FileNotFoundError("could not locate schemas/ above this test file")


SCHEMAS = _schemas_dir()


def _schema_top_level_props(schema_file: str) -> set[str]:
    schema = json.loads((SCHEMAS / schema_file).read_text(encoding="utf-8"))
    return _node_props(schema)


def _definition_props(schema_file: str, definition: str) -> set[str]:
    schema = json.loads((SCHEMAS / schema_file).read_text(encoding="utf-8"))
    return _node_props(schema["definitions"][definition])


def _node_props(node: dict) -> set[str]:
    """Union of ``properties`` across a node and any ``anyOf`` / ``oneOf`` variants.

    Several outputs (notably ``CheckResult``) carry a shared base plus a set of
    variant property blocks; the wire can emit fields from any variant, so the
    model must declare all of them.
    """
    props: set[str] = set(node.get("properties", {}))
    for variant in node.get("anyOf", []) + node.get("oneOf", []):
        props |= set(variant.get("properties", {}))
    return props


# Maps every ``parse_rocky_output`` dispatch command (and the two
# shape-discriminated commands handled outside ``_SIMPLE_DISPATCH``) to the
# schema file whose top-level shape the dispatch target must cover. Keeping this
# explicit — command names are not 1:1 with schema filenames (``validate-migration``
# → ``validate_migration``; ``ai_sync`` → ``ai_sync``) — while the coverage
# assertion below pins it against the live dispatch table.
_COMMAND_SCHEMA: dict[str, str] = {
    "discover": "discover.schema.json",
    "run": "run.schema.json",
    "plan": "plan.schema.json",
    "state": "state.schema.json",
    "state-clear-schema-cache": "state_clear_schema_cache.schema.json",
    "compile": "compile.schema.json",
    "test": "test.schema.json",
    "ci": "ci.schema.json",
    "ci-diff": "ci_diff.schema.json",
    "lineage-diff": "lineage_diff.schema.json",
    "metrics": "metrics.schema.json",
    "optimize": "optimize.schema.json",
    "cost": "cost.schema.json",
    "backfill": "backfill.schema.json",
    "ai": "ai.schema.json",
    "ai_sync": "ai_sync.schema.json",
    "ai_explain": "ai_explain.schema.json",
    "ai_test": "ai_test.schema.json",
    "ai_contract": "ai_contract.schema.json",
    "validate-migration": "validate_migration.schema.json",
    "doctor": "doctor.schema.json",
    "dag": "dag.schema.json",
    "compliance": "compliance.schema.json",
    "retention-status": "retention_status.schema.json",
    "catalog": "catalog.schema.json",
    "branch approve": "branch_approve.schema.json",
    "branch promote": "branch_promote.schema.json",
    "plan promote": "plan_promote.schema.json",
    "compact apply": "compact_apply.schema.json",
    "archive apply": "archive_apply.schema.json",
}

# Client-only parse targets not routed by ``parse_rocky_output`` (their payloads
# either carry no ``command`` key, or are shape-discriminated).
_CLIENT_ONLY: dict[str, tuple[str, type]] = {
    "apply-gc": ("gc_apply.schema.json", sdk_types.GcApplyOutput),
    "apply-restore": ("restore_apply.schema.json", sdk_types.RestoreApplyOutput),
    "test-adapter": ("test_adapter.schema.json", sdk_types.ConformanceResult),
    "lineage-model": ("lineage.schema.json", sdk_types.ModelLineageResult),
    "lineage-column": ("column_lineage.schema.json", sdk_types.ColumnLineageResult),
    "history-all": ("history.schema.json", sdk_types.HistoryResult),
    "history-model": ("model_history.schema.json", sdk_types.ModelHistoryResult),
}


def test_dispatch_table_is_fully_covered():
    """Every ``_SIMPLE_DISPATCH`` command must have a parity schema mapping.

    Adding a new dispatch command without a parity entry fails here, forcing the
    author to wire the drift guard for it.
    """
    uncovered = set(_SIMPLE_DISPATCH) - set(_COMMAND_SCHEMA)
    assert not uncovered, f"dispatch commands missing a parity schema mapping: {sorted(uncovered)}"


def _model_fields(model: type) -> set[str]:
    return set(model.model_fields)


_DISPATCH_CASES = [
    pytest.param(cmd, _COMMAND_SCHEMA[cmd], _SIMPLE_DISPATCH[cmd], id=f"dispatch:{cmd}")
    for cmd in sorted(_SIMPLE_DISPATCH)
]
_CLIENT_CASES = [
    pytest.param(key, schema, model, id=f"client:{key}")
    for key, (schema, model) in sorted(_CLIENT_ONLY.items())
]


@pytest.mark.parametrize(("command", "schema_file", "model"), _DISPATCH_CASES + _CLIENT_CASES)
def test_top_level_schema_props_have_model_fields(command: str, schema_file: str, model: type):
    schema_props = _schema_top_level_props(schema_file)
    missing = schema_props - _model_fields(model)
    assert not missing, (
        f"{model.__name__} (command {command!r}, schema {schema_file}) is missing "
        f"fields for wire properties {sorted(missing)} — they would be silently "
        f'dropped by Pydantic\'s extra="ignore". Declare them on the model.'
    )


# One-level recursion into the known nested command-output objects the spec
# calls out. Each tuple: (schema_file, definition_name, target_nested_model).
_NESTED_CASES = [
    pytest.param(
        "run.schema.json",
        "MaterializationOutput",
        sdk_types.MaterializationInfo,
        id="nested:materialization",
    ),
    pytest.param("run.schema.json", "CheckResult", sdk_types.CheckResult, id="nested:check_result"),
    pytest.param(
        "run.schema.json",
        "ExecutionSummary",
        sdk_types.ExecutionSummary,
        id="nested:execution_summary",
    ),
]


@pytest.mark.parametrize(("schema_file", "definition", "model"), _NESTED_CASES)
def test_nested_schema_props_have_model_fields(schema_file: str, definition: str, model: type):
    schema_props = _definition_props(schema_file, definition)
    missing = schema_props - _model_fields(model)
    assert not missing, (
        f"{model.__name__} (nested {definition} in {schema_file}) is missing fields "
        f"for wire properties {sorted(missing)} — they would be silently dropped."
    )
