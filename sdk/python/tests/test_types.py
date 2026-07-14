"""Tests for the typed surface — ``parse_rocky_output`` dispatch and the
``_parse_rocky_json`` / ``_parse_run_or_apply`` error paths.
"""

from __future__ import annotations

import json

import pytest

from rocky_sdk import CiResult, CompileResult, DiscoverResult, TestResult, parse_rocky_output
from rocky_sdk.client import _parse_rocky_json, _parse_run_or_apply
from rocky_sdk.exceptions import RockyOutputParseError
from rocky_sdk.types import CheckResult, GcApplyOutput, RestoreApplyOutput, RunResult

DISCOVER_JSON = '{"version": "1.0.0", "command": "discover", "sources": []}'


def test_parse_rocky_output_dispatches_discover():
    result = parse_rocky_output(DISCOVER_JSON)
    assert isinstance(result, DiscoverResult)
    assert result.sources == []


@pytest.mark.parametrize(
    ("marker", "counter", "extra", "model"),
    [
        ("restored", "restored_count", {}, RestoreApplyOutput),
        ("evicted", "evicted_count", {"bytes_refused": 0}, GcApplyOutput),
    ],
)
def test_parse_rocky_output_dispatches_apply_by_shape(marker, counter, extra, model):
    payload = {
        "version": "1.64.0",
        "command": "apply",
        "plan_id": "a" * 64,
        marker: [],
        "refused": [],
        f"already_{marker}": [],
        counter: 0,
        "refused_count": 0,
        f"bytes_{marker}": 0,
        "notes": [],
        **extra,
    }
    assert isinstance(parse_rocky_output(json.dumps(payload)), model)


def test_parse_rocky_output_rejects_unknown_apply_shape():
    with pytest.raises(ValueError, match="Unknown Rocky apply output shape"):
        parse_rocky_output('{"command": "apply", "refused": []}')


def test_parse_compile_surfaces_model_governance_tags():
    # A model's resolved [tags] (own + inherited from its config group) ride
    # on `models_detail[].tags` — the seam dagster-rocky reads to project
    # governance tags onto the derived asset.
    payload = (
        '{"version": "1.0.0", "command": "compile", "models": 1, '
        '"execution_layers": 1, "diagnostics": [], "has_errors": false, '
        '"models_detail": [{"name": "fct_orders", '
        '"strategy": {"type": "full_refresh"}, '
        '"target": {"catalog": "wh", "schema": "mart_emea", "table": "fct_orders"}, '
        '"tags": {"domain": "finance", "tier": "silver"}}]}'
    )
    result = parse_rocky_output(payload)
    assert isinstance(result, CompileResult)
    assert result.models_detail[0].tags == {"domain": "finance", "tier": "silver"}


def test_parse_compile_model_without_tags_defaults_none():
    payload = (
        '{"version": "1.0.0", "command": "compile", "models": 1, '
        '"execution_layers": 1, "diagnostics": [], "has_errors": false, '
        '"models_detail": [{"name": "stg", "strategy": {"type": "full_refresh"}, '
        '"target": {"catalog": "wh", "schema": "s", "table": "stg"}}]}'
    )
    result = parse_rocky_output(payload)
    assert result.models_detail[0].tags is None


def test_parse_test_result_failures_are_objects():
    # `rocky test --output json` serializes `failures` as [{name, error}]
    # objects (the engine maps its (name, error) tuples to the `TestFailure`
    # struct), and carries `model_results` (per-model outcomes, passes too).
    # The old hand-written `TestResult` declared `failures: list[list[str]]`
    # and lacked `model_results` — it could not parse this payload at all and
    # raised on the first failing test. Captured shape from the live binary.
    payload = (
        '{"version": "1.0.0", "command": "test", "total": 2, "passed": 1, '
        '"failed": 1, "failures": [{"name": "fct_orders", "error": '
        '"DuckDB error: Binder Error"}], "model_results": ['
        '{"model": "fct_orders", "status": "fail", "error": "DuckDB error: Binder Error"}, '
        '{"model": "stg_orders", "status": "pass"}]}'
    )
    result = parse_rocky_output(payload)
    assert isinstance(result, TestResult)
    assert result.failures[0].name == "fct_orders"
    assert "Binder Error" in result.failures[0].error
    assert {(m.model, m.status) for m in result.model_results} == {
        ("fct_orders", "fail"),
        ("stg_orders", "pass"),
    }


def test_parse_ci_result_failures_are_objects():
    # `rocky ci` shares the same `failures` shape: [{name, error}] objects, not
    # positional tuples. Same drift as `TestResult`; same soft-swap fix.
    payload = (
        '{"version": "1.0.0", "command": "ci", "compile_ok": true, '
        '"tests_ok": false, "models_compiled": 2, "tests_passed": 1, '
        '"tests_failed": 1, "exit_code": 1, "diagnostics": [], '
        '"failures": [{"name": "fct_orders", "error": "type mismatch"}]}'
    )
    result = parse_rocky_output(payload)
    assert isinstance(result, CiResult)
    assert result.failures[0].name == "fct_orders"
    assert result.failures[0].error == "type mismatch"


def test_check_result_preserves_severity():
    # Shadow-drift regression: the runtime ``RunResult`` routes check results
    # through this hand-written wide ``CheckResult`` (not the generated
    # per-variant models). Without ``severity`` declared here, Pydantic's
    # default ``extra="ignore"`` would silently drop the wire value, and any
    # consumer mapping it (dagster-rocky's ``AssetCheckSeverity``) would only
    # ever see the default. Captured shape from the live binary.
    advisory = CheckResult.model_validate(
        {"name": "null_rate", "passed": False, "severity": "warning"}
    )
    assert advisory.severity == "warning"

    # Omitted severity (older binary) defaults to "error".
    assert CheckResult.model_validate({"name": "row_count", "passed": True}).severity == "error"


def test_run_result_threads_check_severity_through_table_checks():
    # The whole path the orchestrator reads: run output → check_results →
    # checks[].severity must survive into the wide model.
    result = RunResult.model_validate(
        {
            "version": "1.0.0",
            "command": "run",
            "filter": "tenant=acme",
            "duration_ms": 1,
            "tables_copied": 0,
            "materializations": [],
            "check_results": [
                {
                    "asset_key": ["fivetran", "acme", "shopify", "orders"],
                    "checks": [{"name": "null_rate", "passed": False, "severity": "warning"}],
                }
            ],
            "permissions": {
                "grants_added": 0,
                "grants_revoked": 0,
                "catalogs_created": 0,
                "schemas_created": 0,
            },
            "drift": {"tables_checked": 0, "tables_drifted": 0, "actions_taken": []},
        }
    )
    assert result.check_results[0].checks[0].severity == "warning"


def test_run_result_threads_containment_blast_radius():
    # Shadow-drift regression: `contained[]` is present on the *generated*
    # `RunOutput`, but `parse_rocky_output` dispatches `run` to the hand-written
    # `RunResult`. Without the field declared there, Pydantic's default
    # `extra="ignore"` silently drops the wire value, so a consumer surfacing the
    # containment blast radius (dagster-rocky) never sees it. This is the same
    # class of drift the `CheckResult.severity` test above pins.
    payload = json.dumps(
        {
            "version": "1.0.0",
            "command": "run",
            "filter": "all",
            "duration_ms": 42,
            "tables_copied": 1,
            "tables_failed": 1,
            "materializations": [],
            "check_results": [],
            "errors": [
                {"asset_key": ["stg_orders"], "error": "boom", "failure_kind": "query-rejected"}
            ],
            "contained": [
                {
                    "model": "fct_orders",
                    "blocked_by": ["stg_orders"],
                    "unblock_hint": "resolve the stg_orders failure, then re-run",
                },
                {
                    "model": "mart_revenue",
                    "blocked_by": ["fct_orders"],
                    "unblock_hint": "resolve the stg_orders failure, then re-run",
                },
            ],
            "permissions": {
                "grants_added": 0,
                "grants_revoked": 0,
                "catalogs_created": 0,
                "schemas_created": 0,
            },
            "drift": {"tables_checked": 0, "tables_drifted": 0, "actions_taken": []},
        }
    )
    result = parse_rocky_output(payload)
    assert isinstance(result, RunResult)
    assert [c.model for c in result.contained] == ["fct_orders", "mart_revenue"]
    assert result.contained[0].blocked_by == ["stg_orders"]
    assert result.contained[1].blocked_by == ["fct_orders"]
    assert "re-run" in result.contained[0].unblock_hint


def test_run_result_containment_defaults_empty_when_omitted():
    # A default fail-fast / successful run omits `contained` on the wire; it must
    # surface as an empty list, not raise.
    payload = json.dumps(
        {
            "version": "1.0.0",
            "command": "run",
            "filter": "all",
            "duration_ms": 1,
            "tables_copied": 0,
            "materializations": [],
            "check_results": [],
            "permissions": {
                "grants_added": 0,
                "grants_revoked": 0,
                "catalogs_created": 0,
                "schemas_created": 0,
            },
            "drift": {"tables_checked": 0, "tables_drifted": 0, "actions_taken": []},
        }
    )
    result = parse_rocky_output(payload)
    assert isinstance(result, RunResult)
    assert result.contained == []


def test_parse_rocky_output_rejects_non_object():
    with pytest.raises(ValueError, match="not a JSON object"):
        parse_rocky_output("[1, 2, 3]")


def test_parse_rocky_output_rejects_unknown_command():
    with pytest.raises(ValueError, match="Unknown Rocky command"):
        parse_rocky_output('{"command": "teleport"}')


def test_parse_rocky_json_malformed():
    # pydantic's model_validate_json wraps malformed JSON in ValidationError, so
    # _parse_rocky_json reports it as a validation failure (matching the original
    # dagster-rocky behavior — the json.JSONDecodeError branch is defense-in-depth).
    with pytest.raises(RockyOutputParseError) as exc:
        _parse_rocky_json("not json at all", DiscoverResult, command="discover")
    assert exc.value.kind == "validation"
    assert exc.value.command == "discover"


def test_parse_rocky_json_schema_drift():
    # Valid JSON, wrong shape for DiscoverResult (sources must be a list).
    with pytest.raises(RockyOutputParseError) as exc:
        _parse_rocky_json('{"command": "discover"}', DiscoverResult, command="discover")
    assert exc.value.kind == "validation"


def test_parse_run_or_apply_malformed_json():
    with pytest.raises(RockyOutputParseError) as exc:
        _parse_run_or_apply("definitely not json", command="run")
    assert exc.value.kind == "json"


def test_parse_run_or_apply_envelope_missing_inner_result():
    payload = '{"command": "apply", "plan_id": "abc", "plan_kind": "run", "result": null}'
    with pytest.raises(RockyOutputParseError) as exc:
        _parse_run_or_apply(payload, command="apply")
    assert exc.value.kind == "envelope"
    assert exc.value.plan_id == "abc"


def test_parse_run_or_apply_envelope_success_false():
    payload = (
        '{"command": "apply", "plan_id": "def", "plan_kind": "run", '
        '"success": false, "result": {"command": "run"}}'
    )
    with pytest.raises(RockyOutputParseError) as exc:
        _parse_run_or_apply(payload, command="apply")
    assert exc.value.kind == "envelope"
    assert exc.value.plan_id == "def"
    assert exc.value.inner_result_preview is not None
