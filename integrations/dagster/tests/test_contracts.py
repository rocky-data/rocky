"""Tests for the dagster_rocky.contracts module."""

from __future__ import annotations

from pathlib import Path

import dagster as dg
import pytest

from dagster_rocky.contracts import (
    CONTRACT_COLUMN_CONSTRAINTS_CHECK,
    CONTRACT_PROTECTED_COLUMNS_CHECK,
    CONTRACT_REQUIRED_COLUMNS_CHECK,
    ContractParseError,
    ContractRules,
    contract_check_results_from_diagnostics,
    contract_check_specs_for_model,
    discover_contract_rules,
)
from dagster_rocky.types import Diagnostic, Severity, SourceSpan

# ---------------------------------------------------------------------------
# discover_contract_rules
# ---------------------------------------------------------------------------


def test_discover_contract_rules_empty_dir(tmp_path: Path):
    """Empty directory returns empty dict — no error."""
    assert discover_contract_rules(tmp_path) == {}


def test_discover_contract_rules_nonexistent_dir(tmp_path: Path):
    """Non-existent directory returns empty dict (defensive)."""
    nonexistent = tmp_path / "does_not_exist"
    assert discover_contract_rules(nonexistent) == {}


def test_discover_contract_rules_finds_required_rule(tmp_path: Path):
    contract_file = tmp_path / "orders.contract.toml"
    contract_file.write_text(
        """
[rules]
required = ["id", "amount"]
""",
        encoding="utf-8",
    )

    result = discover_contract_rules(tmp_path)
    assert "orders" in result
    assert result["orders"].has_required is True
    assert result["orders"].has_protected is False
    assert result["orders"].has_column_constraints is False


def test_discover_contract_rules_finds_protected_rule(tmp_path: Path):
    contract_file = tmp_path / "orders.contract.toml"
    contract_file.write_text(
        """
[rules]
protected = ["customer_id"]
""",
        encoding="utf-8",
    )

    result = discover_contract_rules(tmp_path)
    assert result["orders"].has_protected is True


def test_discover_contract_rules_finds_column_constraints(tmp_path: Path):
    """Column-level type or nullability rules become has_column_constraints=True."""
    contract_file = tmp_path / "orders.contract.toml"
    contract_file.write_text(
        """
[[columns]]
name = "id"
type = "Int64"
nullable = false
""",
        encoding="utf-8",
    )

    result = discover_contract_rules(tmp_path)
    assert result["orders"].has_column_constraints is True


def test_discover_contract_rules_no_new_nullable_flag(tmp_path: Path):
    """Schema-level no_new_nullable flag also marks column constraints present."""
    contract_file = tmp_path / "orders.contract.toml"
    contract_file.write_text(
        """
[rules]
no_new_nullable = true
""",
        encoding="utf-8",
    )

    result = discover_contract_rules(tmp_path)
    assert result["orders"].has_column_constraints is True


def test_discover_contract_rules_skips_empty_contract(tmp_path: Path):
    """A contract file with no rules at all is silently ignored."""
    contract_file = tmp_path / "orders.contract.toml"
    contract_file.write_text("# completely empty\n", encoding="utf-8")

    result = discover_contract_rules(tmp_path)
    assert result == {}


def test_discover_contract_rules_multiple_files(tmp_path: Path):
    (tmp_path / "orders.contract.toml").write_text('[rules]\nrequired = ["id"]\n', encoding="utf-8")
    (tmp_path / "customers.contract.toml").write_text(
        '[rules]\nprotected = ["email"]\n', encoding="utf-8"
    )

    result = discover_contract_rules(tmp_path)
    assert set(result.keys()) == {"orders", "customers"}
    assert result["orders"].has_required is True
    assert result["customers"].has_protected is True


def test_discover_contract_rules_invalid_toml_raises(tmp_path: Path):
    contract_file = tmp_path / "broken.contract.toml"
    contract_file.write_text("this = is = not = valid = toml\n", encoding="utf-8")

    with pytest.raises(ContractParseError) as excinfo:
        discover_contract_rules(tmp_path)
    assert "broken.contract.toml" in str(excinfo.value)


# ---------------------------------------------------------------------------
# contract_check_specs_for_model
# ---------------------------------------------------------------------------


def test_check_specs_only_for_declared_rules():
    asset_key = dg.AssetKey(["orders"])
    rules = ContractRules(
        has_required=True,
        has_protected=False,
        has_column_constraints=True,
    )

    specs = list(contract_check_specs_for_model(asset_key, rules))

    names = {s.name for s in specs}
    assert names == {
        CONTRACT_REQUIRED_COLUMNS_CHECK,
        CONTRACT_COLUMN_CONSTRAINTS_CHECK,
    }
    # protected was not declared, so no spec
    assert CONTRACT_PROTECTED_COLUMNS_CHECK not in names


def test_check_specs_empty_rules_yields_nothing():
    asset_key = dg.AssetKey(["orders"])
    rules = ContractRules(
        has_required=False,
        has_protected=False,
        has_column_constraints=False,
    )

    assert list(contract_check_specs_for_model(asset_key, rules)) == []


def test_check_specs_target_correct_asset_key():
    asset_key = dg.AssetKey(["fivetran", "acme", "orders"])
    rules = ContractRules(has_required=True, has_protected=False, has_column_constraints=False)

    specs = list(contract_check_specs_for_model(asset_key, rules))

    assert len(specs) == 1
    assert specs[0].asset_key == asset_key


# ---------------------------------------------------------------------------
# contract_check_results_from_diagnostics
# ---------------------------------------------------------------------------


def _diag(code: str, model: str, message: str, severity: Severity = Severity.error) -> Diagnostic:
    return Diagnostic(
        severity=severity,
        code=code,
        message=message,
        model=model,
        span=SourceSpan(file="x.sql", line=1, col=1),
    )


def test_results_pass_when_no_diagnostics_for_check():
    asset_key = dg.AssetKey(["orders"])
    rules = ContractRules(has_required=True, has_protected=True, has_column_constraints=True)

    results = list(
        contract_check_results_from_diagnostics(
            diagnostics=[],
            asset_key=asset_key,
            model_name="orders",
            rules=rules,
        )
    )

    assert len(results) == 3
    assert all(r.passed for r in results)


def test_results_fail_when_e010_present():
    """E010 (required column missing) → contract_required_columns fails."""
    asset_key = dg.AssetKey(["orders"])
    rules = ContractRules(has_required=True, has_protected=False, has_column_constraints=False)

    results = list(
        contract_check_results_from_diagnostics(
            diagnostics=[_diag("E010", "orders", "required column 'id' missing")],
            asset_key=asset_key,
            model_name="orders",
            rules=rules,
        )
    )

    assert len(results) == 1
    r = results[0]
    assert r.check_name == CONTRACT_REQUIRED_COLUMNS_CHECK
    assert r.passed is False
    assert r.severity == dg.AssetCheckSeverity.ERROR
    assert "required column" in r.metadata["rocky/violation_0"].text
    assert r.metadata["rocky/violation_count"].value == 1


def test_results_fail_when_e013_present():
    """E013 (protected column removed) → contract_protected_columns fails."""
    asset_key = dg.AssetKey(["orders"])
    rules = ContractRules(has_required=False, has_protected=True, has_column_constraints=False)

    results = list(
        contract_check_results_from_diagnostics(
            diagnostics=[_diag("E013", "orders", "protected column 'email' removed")],
            asset_key=asset_key,
            model_name="orders",
            rules=rules,
        )
    )

    assert len(results) == 1
    assert results[0].check_name == CONTRACT_PROTECTED_COLUMNS_CHECK
    assert results[0].passed is False


def test_results_e011_e012_map_to_column_constraints():
    """E011 (type mismatch) and E012 (nullability) → contract_column_constraints fails."""
    asset_key = dg.AssetKey(["orders"])
    rules = ContractRules(has_required=False, has_protected=False, has_column_constraints=True)

    results = list(
        contract_check_results_from_diagnostics(
            diagnostics=[
                _diag("E011", "orders", "column 'id' type mismatch"),
                _diag("E012", "orders", "column 'email' must be non-nullable"),
            ],
            asset_key=asset_key,
            model_name="orders",
            rules=rules,
        )
    )

    assert len(results) == 1
    r = results[0]
    assert r.check_name == CONTRACT_COLUMN_CONSTRAINTS_CHECK
    assert r.passed is False
    assert r.severity == dg.AssetCheckSeverity.ERROR
    # Both violations are present
    assert r.metadata["rocky/violation_count"].value == 2


def test_results_w010_alone_yields_warn_severity():
    """W010 is a warning. When it's the only failing diagnostic, severity is WARN."""
    asset_key = dg.AssetKey(["orders"])
    rules = ContractRules(has_required=False, has_protected=False, has_column_constraints=True)

    results = list(
        contract_check_results_from_diagnostics(
            diagnostics=[
                _diag("W010", "orders", "contract column 'phone' not found", Severity.warning),
            ],
            asset_key=asset_key,
            model_name="orders",
            rules=rules,
        )
    )

    assert len(results) == 1
    assert results[0].passed is False
    assert results[0].severity == dg.AssetCheckSeverity.WARN


def test_results_filter_by_model_name():
    """Diagnostics for OTHER models are ignored."""
    asset_key = dg.AssetKey(["orders"])
    rules = ContractRules(has_required=True, has_protected=False, has_column_constraints=False)

    results = list(
        contract_check_results_from_diagnostics(
            diagnostics=[
                _diag("E010", "customers", "required column missing"),  # different model
                _diag("E010", "orders", "required column 'id' missing"),  # this model
            ],
            asset_key=asset_key,
            model_name="orders",
            rules=rules,
        )
    )

    assert len(results) == 1
    assert results[0].passed is False
    # Only one violation (the orders one), not two
    assert results[0].metadata["rocky/violation_count"].value == 1


def test_results_ignore_non_contract_codes():
    """Diagnostics with non-contract codes (e.g. typecheck errors) are ignored."""
    asset_key = dg.AssetKey(["orders"])
    rules = ContractRules(has_required=True, has_protected=False, has_column_constraints=False)

    results = list(
        contract_check_results_from_diagnostics(
            diagnostics=[
                _diag("T001", "orders", "type error somewhere"),
                _diag("E010", "orders", "required column 'id' missing"),
            ],
            asset_key=asset_key,
            model_name="orders",
            rules=rules,
        )
    )

    # Only the E010 counted
    assert results[0].metadata["rocky/violation_count"].value == 1


def test_component_pre_declares_contract_specs_when_contracts_dir_set(tmp_path: Path):
    """End-to-end: RockyComponent picks up contract files from contracts_dir
    and pre-declares matching AssetCheckSpec instances on assets whose
    table name matches a contract."""
    import json

    from dagster_rocky.component import RockyComponent

    # Set up a contracts dir with a contract for `orders`
    contracts_dir = tmp_path / "contracts"
    contracts_dir.mkdir()
    (contracts_dir / "orders.contract.toml").write_text(
        """
[rules]
required = ["id", "amount"]
protected = ["customer_id"]

[[columns]]
name = "id"
type = "Int64"
nullable = false
""",
        encoding="utf-8",
    )

    # State file with discover output that includes an `orders` table
    state_file = tmp_path / "state.json"
    state_file.write_text(
        json.dumps(
            {
                "discover": {
                    "version": "0.1.0",
                    "command": "discover",
                    "sources": [
                        {
                            "id": "src_001",
                            "components": {
                                "tenant": "acme",
                                "region": "us_west",
                                "source": "shopify",
                            },
                            "source_type": "fivetran",
                            "last_sync_at": "2026-04-08T10:00:00Z",
                            "tables": [
                                {"name": "orders", "row_count": 100},
                                {"name": "payments", "row_count": 50},
                            ],
                        }
                    ],
                }
            }
        ),
        encoding="utf-8",
    )

    component = RockyComponent(
        config_path="rocky.toml",
        contracts_dir=str(contracts_dir),
    )
    defs = component.build_defs_from_state(context=None, state_path=state_file)

    orders_key = dg.AssetKey(["fivetran", "acme", "us_west", "shopify", "orders"])
    payments_key = dg.AssetKey(["fivetran", "acme", "us_west", "shopify", "payments"])

    # Collect check specs by asset key
    checks_by_key: dict[dg.AssetKey, set[str]] = {}
    for asset_def in defs.assets or []:
        if isinstance(asset_def, dg.AssetsDefinition):
            for cs in asset_def.check_specs:
                checks_by_key.setdefault(cs.asset_key, set()).add(cs.name)

    # `orders` has all 4 default checks PLUS 3 contract checks
    assert CONTRACT_REQUIRED_COLUMNS_CHECK in checks_by_key[orders_key]
    assert CONTRACT_PROTECTED_COLUMNS_CHECK in checks_by_key[orders_key]
    assert CONTRACT_COLUMN_CONSTRAINTS_CHECK in checks_by_key[orders_key]

    # `payments` has only the 4 default checks (no contract file)
    assert CONTRACT_REQUIRED_COLUMNS_CHECK not in checks_by_key[payments_key]
    assert CONTRACT_PROTECTED_COLUMNS_CHECK not in checks_by_key[payments_key]
    assert CONTRACT_COLUMN_CONSTRAINTS_CHECK not in checks_by_key[payments_key]


def test_results_only_yield_for_declared_rules():
    """Even if diagnostics exist for an undeclared rule kind, no result is yielded."""
    asset_key = dg.AssetKey(["orders"])
    rules = ContractRules(
        has_required=True,
        has_protected=False,  # NOT declared
        has_column_constraints=False,  # NOT declared
    )

    results = list(
        contract_check_results_from_diagnostics(
            diagnostics=[
                _diag("E013", "orders", "protected col removed"),  # protected, undeclared
                _diag("E011", "orders", "type mismatch"),  # constraints, undeclared
            ],
            asset_key=asset_key,
            model_name="orders",
            rules=rules,
        )
    )

    # Only the required check is yielded; it passes (no E010 diagnostics)
    assert len(results) == 1
    assert results[0].check_name == CONTRACT_REQUIRED_COLUMNS_CHECK
    assert results[0].passed is True
