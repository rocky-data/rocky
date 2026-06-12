"""Tests for the typed surface — ``parse_rocky_output`` dispatch and the
``_parse_rocky_json`` / ``_parse_run_or_apply`` error paths.
"""

from __future__ import annotations

import pytest

from rocky_sdk import DiscoverResult, parse_rocky_output
from rocky_sdk.client import _parse_rocky_json, _parse_run_or_apply
from rocky_sdk.exceptions import RockyOutputParseError

DISCOVER_JSON = '{"version": "1.0.0", "command": "discover", "sources": []}'


def test_parse_rocky_output_dispatches_discover():
    result = parse_rocky_output(DISCOVER_JSON)
    assert isinstance(result, DiscoverResult)
    assert result.sources == []


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
