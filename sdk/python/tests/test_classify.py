"""Tests for ``rocky_sdk.classify``: rules, sidecar edits, and ``suggest``.

The Laya path is not exercised here (its weights are 2.3 GB); ``suggest`` takes
a stand-in ``model_classifier`` instead.
"""

from __future__ import annotations

import json
import tomllib
from unittest.mock import patch

import pytest

from rocky_sdk import RockyClient
from rocky_sdk.classify import (
    ColumnEvidence,
    Suggestion,
    apply_accepted,
    existing_classification,
    rules_classifier,
    suggest,
)
from rocky_sdk.classify._laya import render
from rocky_sdk.classify._suggest import rocky_type_name


def _client() -> RockyClient:
    client = RockyClient(binary_path="rocky", models_dir="models")
    client._version_checked = True
    return client


def _col(column, values=(), type_="String", table="customers"):
    return ColumnEvidence(table=table, column=column, type=type_, values=tuple(values))


# --------------------------------------------------------------------- rules
@pytest.mark.parametrize(
    ("column", "values", "type_", "kind"),
    [
        ("email", (), "String", "email"),
        ("contact", ("a@x.com", "b@y.org", "c@z.net", "d@w.io", "e@v.de"), "String", "email"),
        ("email_verified", ("true",) * 5, "Boolean", "none"),
        ("iban", (), "String", "financial"),
        (
            "card",
            (
                "4111111111111111",
                "5500005555555559",
                "340000000000009",
                "6011000000000004",
                "4012888888881881",
            ),
            "String",
            "financial",
        ),
        ("product_name", ("Boots",) * 5, "String", "none"),
        ("status", ("open", "closed"), "String", "none"),
    ],
)
def test_rules_classifier(column, values, type_, kind):
    assert rules_classifier(_col(column, values, type_))[0] == kind


@pytest.mark.parametrize(
    ("warehouse", "rocky"),
    [
        ("VARCHAR", "String"),
        ("BIGINT", "Int64"),
        ("DATE", "Date"),
        ("TIMESTAMP WITH TIME ZONE", "Timestamp"),
        ("BOOLEAN", "Boolean"),
        ("DECIMAL(18,2)", "Decimal"),
        ("GEOMETRY", "GEOMETRY"),
    ],
)
def test_a_number_that_fails_luhn_is_not_financial():
    assert rules_classifier(_col("card", ("1234567890123456",) * 5))[0] != "financial"


def test_rocky_type_name(warehouse, rocky):
    assert rocky_type_name(warehouse) == rocky


def test_render_matches_the_measured_input_shape():
    text = render(_col("email", ("a@x.com", "b@y.org")))
    assert text == (
        "Table: customers. Column: email. Type: String. Sample values: a@x.com; b@y.org."
    )


# --------------------------------------------------------------------- sidecar
def _s(column, tag="pii"):
    return Suggestion(column=column, kind="email", tag=tag, source="rules", probability=1.0)


def test_apply_adds_a_new_block_and_keeps_the_rest(tmp_path):
    side = tmp_path / "m.toml"
    original = 'name = "m"\n\n[strategy]\ntype = "full_refresh"\n'
    side.write_text(original)
    assert apply_accepted(side, [_s("email"), _s("odd col", "confidential")]) == [
        "email",
        "odd col",
    ]
    text = side.read_text()
    assert text.startswith(original)
    assert tomllib.loads(text)["classification"] == {"email": "pii", "odd col": "confidential"}


def test_apply_never_overwrites_an_existing_tag(tmp_path):
    side = tmp_path / "m.toml"
    side.write_text('[classification]\nemail = "restricted"\n\n[target]\nschema = "s"\n')
    assert apply_accepted(side, [_s("email"), _s("phone")]) == ["phone"]
    data = tomllib.loads(side.read_text())
    assert data["classification"] == {"email": "restricted", "phone": "pii"}
    assert data["target"] == {"schema": "s"}


def test_apply_with_nothing_new_leaves_the_file_alone(tmp_path):
    side = tmp_path / "m.toml"
    side.write_text('[classification]\nemail = "pii"\n')
    before = side.stat().st_mtime_ns
    assert apply_accepted(side, [_s("email")]) == []
    assert side.stat().st_mtime_ns == before


def test_apply_refuses_a_dotted_key_classification(tmp_path):
    side = tmp_path / "m.toml"
    side.write_text('classification.email = "pii"\n')
    with pytest.raises(ValueError, match="by hand"):
        apply_accepted(side, [_s("phone")])
    assert side.read_text() == 'classification.email = "pii"\n'


def test_existing_classification_missing_file(tmp_path):
    assert existing_classification(tmp_path / "absent.toml") == {}


# --------------------------------------------------------------------- suggest
def _profile_payload(columns, unavailable=None):
    payload = {
        "version": "1.77.0",
        "command": "profile",
        "model": "stg_customers",
        "profiled_table": "staging.stg_customers",
        "columns": columns,
    }
    if unavailable:
        payload["unavailable"] = unavailable
        payload["columns"] = []
    return json.dumps(payload)


def _stats(name, type_, samples):
    return {
        "name": name,
        "type": type_,
        "rows": 5,
        "nulls": 0,
        "null_rate": 0.0,
        "distinct": len(samples),
        "observed_values": [],
        "sample_values": list(samples),
    }


def test_suggest_runs_rules_then_the_model_on_untagged_columns(tmp_path):
    side = tmp_path / "stg_customers.toml"
    side.write_text('[classification]\nssn = "confidential"\n')
    payload = _profile_payload(
        [
            _stats("email", "VARCHAR", ["a@x.com"] * 5),
            _stats("author", "VARCHAR", ["Ana Silva", "Tom Hughes"]),
            _stats("hire_date", "DATE", ["2024-01-02"]),
            _stats("status", "VARCHAR", ["open"]),
            _stats("ssn", "VARCHAR", ["123-45-6789"]),
        ]
    )
    seen = []

    def model(col):
        seen.append(col.column)
        answers = {"author": ("name", 0.91), "hire_date": ("phone", 0.7)}
        return answers.get(col.column, ("none", 0.9))

    client = _client()
    with patch.object(client, "run_cli", return_value=payload) as run_cli:
        out = suggest(client, "stg_customers", sidecar=side, use_model=True, model_classifier=model)
    run_cli.assert_called_once_with(
        ["profile", "stg_customers", "--models", "models", "--sample", "5"]
    )
    by_col = {s.column: s for s in out}
    assert set(by_col) == {"email", "author", "hire_date"}
    assert by_col["email"].source == "rules" and by_col["email"].warning is None
    assert by_col["author"].source == "model" and by_col["author"].tag == "pii"
    assert "dates" in by_col["hire_date"].warning
    # The model never sees columns the rules already flagged or the sidecar tags.
    assert seen == ["author", "hire_date", "status"]
    # Writing nothing until a person accepts.
    assert existing_classification(side) == {"ssn": "confidential"}


def test_suggest_rules_only_by_default():
    payload = _profile_payload([_stats("author", "VARCHAR", ["Ana Silva"])])
    client = _client()
    with patch.object(client, "run_cli", return_value=payload):
        assert suggest(client, "stg_customers") == []


def test_suggest_returns_nothing_when_profile_is_unavailable():
    payload = _profile_payload([], unavailable="DuckDB only")
    client = _client()
    with patch.object(client, "run_cli", return_value=payload):
        assert suggest(client, "stg_customers") == []


def test_suggest_needs_samples():
    with pytest.raises(ValueError, match="sample"):
        suggest(_client(), "m", sample=0)


def test_custom_kind_to_tag_drops_unmapped_kinds():
    payload = _profile_payload(
        [
            _stats("email", "VARCHAR", ["a@x.com"]),
            _stats("iban", "VARCHAR", ["GB82WEST12345698765432"]),
        ]
    )
    client = _client()
    with patch.object(client, "run_cli", return_value=payload):
        out = suggest(client, "m", kind_to_tag={"email": "contact"})
    assert [(s.column, s.tag) for s in out] == [("email", "contact")]
