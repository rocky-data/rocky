"""Contract check translation: Rocky `.contract.toml` files → Dagster check specs.

Rocky validates derived-model output schemas against ``.contract.toml`` files
loaded from a ``contracts_dir``. Each file is named ``{model_name}.contract.toml``
and declares column constraints (`type`, `nullable`) plus schema-level rules
(`required`, `protected`, `no_new_nullable`).

This module provides:

* :func:`discover_contract_rules` — walk a contracts directory and return a
  ``{model_name: ContractRules}`` map. ``ContractRules`` is a small dataclass
  with booleans for which rule kinds are present in the contract file.

* :func:`contract_check_specs_for_model` — yields one :class:`AssetCheckSpec`
  per rule kind present in the contract. Names are stable (`contract_required_columns`,
  `contract_protected_columns`, `contract_column_constraints`).

* :func:`contract_check_results_from_diagnostics` — yields one
  :class:`AssetCheckResult` per check spec, mapping compiler diagnostics with
  contract-related codes (E010-E013, W010) to pass/fail status.

Mapping from compiler diagnostic codes to dagster check names:

  E010 (required column missing)         → contract_required_columns
  E013 (protected column removed)        → contract_protected_columns
  E011 (column type mismatch)            → contract_column_constraints
  E012 (column nullability violated)     → contract_column_constraints
  W010 (contract column not in model)    → contract_column_constraints (warn)

These are pure-function builders, decoupled from :class:`RockyComponent` so
users with hand-rolled multi_assets can adopt them today. The component
auto-wires them when ``contracts_dir`` is configured.
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import dagster as dg

if TYPE_CHECKING:
    from collections.abc import Iterator

    from .types import Diagnostic


#: Canonical contract check names. Pre-declare these as ``AssetCheckSpec`` so
#: the Dagster UI shows the contract checks before any compile or run.
CONTRACT_REQUIRED_COLUMNS_CHECK: str = "contract_required_columns"
CONTRACT_PROTECTED_COLUMNS_CHECK: str = "contract_protected_columns"
CONTRACT_COLUMN_CONSTRAINTS_CHECK: str = "contract_column_constraints"

#: Compiler diagnostic codes that contract validation emits. Maps each code
#: to the dagster check name it should fail. ``W010`` is a warning, not an
#: error, but still attaches to the column-constraints check so users see it.
_CONTRACT_CODE_TO_CHECK: dict[str, str] = {
    "E010": CONTRACT_REQUIRED_COLUMNS_CHECK,
    "E013": CONTRACT_PROTECTED_COLUMNS_CHECK,
    "E011": CONTRACT_COLUMN_CONSTRAINTS_CHECK,
    "E012": CONTRACT_COLUMN_CONSTRAINTS_CHECK,
    "W010": CONTRACT_COLUMN_CONSTRAINTS_CHECK,
}


@dataclass(frozen=True)
class ContractRules:
    """Which rule kinds are present in a model's ``.contract.toml`` file.

    A boolean per check kind. Used to drive AssetCheckSpec pre-declaration:
    only checks corresponding to rules actually declared in the contract get
    a spec, so the Dagster UI doesn't show empty contract slots on every asset.
    """

    has_required: bool
    has_protected: bool
    has_column_constraints: bool

    @property
    def is_empty(self) -> bool:
        """``True`` when no rule kinds are present (no specs to declare)."""
        return not (self.has_required or self.has_protected or self.has_column_constraints)


def discover_contract_rules(contracts_dir: Path) -> dict[str, ContractRules]:
    """Walk a contracts directory and return per-model rule presence.

    Looks for ``*.contract.toml`` files. Each filename is interpreted as
    ``{model_name}.contract.toml`` — the model name is the filename stem
    minus the ``.contract`` suffix. The file is parsed with stdlib
    :mod:`tomllib`; only the rule-kind presence is extracted, not the
    actual rule values (those drive engine-side validation, not the
    dagster spec shape).

    Args:
        contracts_dir: Directory containing ``.contract.toml`` files.
            Returns an empty dict if the directory does not exist (so
            callers can pass an unconditional path without guarding).

    Returns:
        ``{model_name: ContractRules}`` for every contract file found.
        Models without contract files are absent from the dict.

    Raises:
        ContractParseError: If a contract file is not valid TOML. The
            error message includes the offending file path so users can
            fix it directly.
    """
    out: dict[str, ContractRules] = {}
    if not contracts_dir.is_dir():
        return out

    for path in sorted(contracts_dir.glob("*.contract.toml")):
        # Filename: foo.contract.toml → model name "foo"
        stem = path.name.removesuffix(".contract.toml")
        if not stem:
            continue
        rules = _parse_contract_rules(path)
        if not rules.is_empty:
            out[stem] = rules
    return out


class ContractParseError(Exception):
    """Raised when a ``.contract.toml`` file cannot be parsed.

    The message includes the file path so users can find and fix the
    offending contract.
    """


def _parse_contract_rules(path: Path) -> ContractRules:
    """Parse a single contract file into a :class:`ContractRules` summary.

    Reads the TOML, looks at ``[rules]`` and ``[[columns]]`` sections to
    determine which rule kinds are present. Does NOT validate the actual
    rule values — that's the engine's job.
    """
    try:
        with path.open("rb") as f:
            data = tomllib.load(f)
    except tomllib.TOMLDecodeError as exc:
        raise ContractParseError(f"failed to parse {path}: {exc}") from None

    rules_section = data.get("rules") or {}
    has_required = bool(rules_section.get("required"))
    has_protected = bool(rules_section.get("protected"))
    # Column constraints are present when at least one column declares a
    # type or nullability expectation, OR when the no_new_nullable
    # schema-level flag is set.
    has_column_constraints = bool(rules_section.get("no_new_nullable"))
    if not has_column_constraints:
        for col in data.get("columns") or []:
            if isinstance(col, dict) and ("type" in col or "nullable" in col):
                has_column_constraints = True
                break

    return ContractRules(
        has_required=has_required,
        has_protected=has_protected,
        has_column_constraints=has_column_constraints,
    )


def contract_check_specs_for_model(
    asset_key: dg.AssetKey,
    rules: ContractRules,
) -> Iterator[dg.AssetCheckSpec]:
    """Yield one ``AssetCheckSpec`` per rule kind present in the contract.

    Skips check kinds whose corresponding rule is not declared in the
    contract — e.g. a model with only ``[[columns]]`` constraints
    doesn't get a ``contract_required_columns`` spec.

    Args:
        asset_key: The Dagster asset key the checks belong to.
        rules: ContractRules describing which rule kinds are present.

    Yields:
        ``dg.AssetCheckSpec`` instances. Empty when ``rules.is_empty``.
    """
    if rules.has_required:
        yield dg.AssetCheckSpec(
            name=CONTRACT_REQUIRED_COLUMNS_CHECK,
            asset=asset_key,
            description="Required columns from .contract.toml are present in model output",
        )
    if rules.has_protected:
        yield dg.AssetCheckSpec(
            name=CONTRACT_PROTECTED_COLUMNS_CHECK,
            asset=asset_key,
            description="Protected columns from .contract.toml have not been removed",
        )
    if rules.has_column_constraints:
        yield dg.AssetCheckSpec(
            name=CONTRACT_COLUMN_CONSTRAINTS_CHECK,
            asset=asset_key,
            description=(
                "Column types and nullability constraints from .contract.toml are satisfied"
            ),
        )


def contract_check_results_from_diagnostics(
    diagnostics: list[Diagnostic],
    *,
    asset_key: dg.AssetKey,
    model_name: str,
    rules: ContractRules,
) -> Iterator[dg.AssetCheckResult]:
    """Yield ``AssetCheckResult`` per declared contract check based on compiler diagnostics.

    Walks the supplied diagnostics for entries matching ``model_name`` and
    a known contract code. Each declared check kind (per ``rules``) gets
    exactly one result:

    * ``passed=True`` if no failing diagnostics for that check kind.
    * ``passed=False`` with metadata listing the failing diagnostics if
      any are present.

    Severity:

    * E010-E013 → ``AssetCheckSeverity.ERROR``
    * W010      → ``AssetCheckSeverity.WARN``

    Args:
        diagnostics: Compile-time diagnostics from :class:`CompileResult`.
            Both contract codes and other codes are tolerated; non-contract
            codes are ignored.
        asset_key: The Dagster asset key the checks belong to.
        model_name: The compiled model name to filter diagnostics by.
        rules: ContractRules — only check kinds with declared rules
            produce results.

    Yields:
        ``dg.AssetCheckResult`` events. The number of yields equals the
        number of declared rule kinds in ``rules``.
    """
    # Group diagnostics by which check they belong to
    by_check: dict[str, list[Diagnostic]] = {
        CONTRACT_REQUIRED_COLUMNS_CHECK: [],
        CONTRACT_PROTECTED_COLUMNS_CHECK: [],
        CONTRACT_COLUMN_CONSTRAINTS_CHECK: [],
    }
    for diag in diagnostics:
        if diag.model != model_name:
            continue
        check_name = _CONTRACT_CODE_TO_CHECK.get(diag.code)
        if check_name is None:
            continue
        by_check[check_name].append(diag)

    if rules.has_required:
        yield _result_for_check(
            check_name=CONTRACT_REQUIRED_COLUMNS_CHECK,
            asset_key=asset_key,
            diagnostics=by_check[CONTRACT_REQUIRED_COLUMNS_CHECK],
        )
    if rules.has_protected:
        yield _result_for_check(
            check_name=CONTRACT_PROTECTED_COLUMNS_CHECK,
            asset_key=asset_key,
            diagnostics=by_check[CONTRACT_PROTECTED_COLUMNS_CHECK],
        )
    if rules.has_column_constraints:
        yield _result_for_check(
            check_name=CONTRACT_COLUMN_CONSTRAINTS_CHECK,
            asset_key=asset_key,
            diagnostics=by_check[CONTRACT_COLUMN_CONSTRAINTS_CHECK],
        )


def _result_for_check(
    *,
    check_name: str,
    asset_key: dg.AssetKey,
    diagnostics: list[Diagnostic],
) -> dg.AssetCheckResult:
    """Build one AssetCheckResult from a filtered diagnostics list.

    ``passed=True`` when the list is empty. When non-empty, the result
    fails with severity matching the worst diagnostic (W010 → WARN, all
    others → ERROR) and metadata listing each diagnostic message.
    """
    if not diagnostics:
        return dg.AssetCheckResult(
            asset_key=asset_key,
            check_name=check_name,
            passed=True,
        )

    # WARN if every failing diagnostic is W010, ERROR otherwise
    only_warnings = all(d.code == "W010" for d in diagnostics)
    severity = dg.AssetCheckSeverity.WARN if only_warnings else dg.AssetCheckSeverity.ERROR

    metadata: dict[str, dg.MetadataValue] = {
        f"rocky/violation_{i}": dg.MetadataValue.text(f"[{d.code}] {d.message}")
        for i, d in enumerate(diagnostics)
    }
    metadata["rocky/violation_count"] = dg.MetadataValue.int(len(diagnostics))

    return dg.AssetCheckResult(
        asset_key=asset_key,
        check_name=check_name,
        passed=False,
        severity=severity,
        metadata=metadata,
    )
