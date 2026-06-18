"""Tests for the typed surface — ``parse_rocky_output`` dispatch and the
``_parse_rocky_json`` / ``_parse_run_or_apply`` error paths.
"""

from __future__ import annotations

import pytest

from rocky_sdk import CiResult, CompileResult, DiscoverResult, TestResult, parse_rocky_output
from rocky_sdk.client import _parse_rocky_json, _parse_run_or_apply
from rocky_sdk.exceptions import RockyOutputParseError

DISCOVER_JSON = '{"version": "1.0.0", "command": "discover", "sources": []}'


def test_parse_rocky_output_dispatches_discover():
    result = parse_rocky_output(DISCOVER_JSON)
    assert isinstance(result, DiscoverResult)
    assert result.sources == []


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
