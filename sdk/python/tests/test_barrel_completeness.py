"""Guard that the ``types_generated`` barrel exports every command output type.

``just codegen-sdk`` regenerates every ``types_generated/<command>_schema.py``
module, but the curated barrel (``types_generated/__init__.py``) is restored
from git HEAD on each run — the codegen pipeline does NOT re-derive it. So a
new CLI command's generated module can silently fail to reach the barrel's
public surface (the exact bug that left the whole governor surface —
``audit`` / ``policy`` / ``review`` — and a dozen older commands unexported).

This test is the enforcement point the pipeline lacks: it walks every generated
module by AST and asserts every top-level command-output type (a class whose
name ends in ``Output`` or ``Result``, excluding datamodel-code-generator's
numbered enum-variant duplicates like ``PolicyEffect15``) has its NAME present
in ``types_generated.__all__``.

Name-based (not identity-based) so it tolerates the handful of shared nested
types the generator materialises into more than one module (e.g.
``ExcludedTableOutput`` in both ``discover_schema`` and ``run_schema``): the
barrel re-exports one canonical binding and the name satisfies both.

The two config-file schemas (``rocky_project_schema`` for ``rocky.toml`` and
``adapter_config_schema``) define no ``*Output`` / ``*Result`` command types, so
they are naturally out of scope — they are not CLI ``--output json`` payloads.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

import rocky_sdk.types_generated as barrel

_GENERATED_DIR = Path(barrel.__file__).parent
_VARIANT_SUFFIX = re.compile(r"\d+$")


def _module_files() -> list[Path]:
    return sorted(p for p in _GENERATED_DIR.glob("*.py") if p.stem != "__init__")


def _terminal_output_classes(module_path: Path) -> list[str]:
    """Top-level command-output class names defined in ``module_path``.

    A command output is a class whose name ends in ``Output`` or ``Result`` and
    is not one of datamodel-code-generator's numbered enum-variant duplicates
    (``FooKind3``, ``PolicyEffect15``, …), which always carry a trailing digit.
    """
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    return [
        node.name
        for node in tree.body
        if isinstance(node, ast.ClassDef)
        and (node.name.endswith("Output") or node.name.endswith("Result"))
        and not _VARIANT_SUFFIX.search(node.name)
    ]


@pytest.mark.parametrize("module_path", _module_files(), ids=lambda p: p.stem)
def test_module_command_outputs_are_barrel_exported(module_path: Path):
    exported = set(barrel.__all__)
    unexported = [name for name in _terminal_output_classes(module_path) if name not in exported]
    assert not unexported, (
        f"{module_path.name} defines command-output type(s) missing from "
        f"types_generated.__all__: {sorted(unexported)}. Add an import + an "
        f"__all__ entry to types_generated/__init__.py (the barrel is restored "
        f"from git HEAD by `just codegen-sdk`, so it must be hand-maintained)."
    )


def test_all_exported_names_are_bound():
    """Every name in ``__all__`` must resolve to a real attribute."""
    unbound = [name for name in barrel.__all__ if not hasattr(barrel, name)]
    assert not unbound, f"types_generated.__all__ lists unbound names: {unbound}"


def test_all_has_no_duplicates():
    """``__all__`` must not list the same name twice."""
    seen: set[str] = set()
    dupes: set[str] = set()
    for name in barrel.__all__:
        if name in seen:
            dupes.add(name)
        seen.add(name)
    assert not dupes, f"types_generated.__all__ has duplicate entries: {sorted(dupes)}"


def test_governor_surface_is_exported():
    """Pin the specific regression: the governor command outputs must be present."""
    exported = set(barrel.__all__)
    governor = {
        "AuditOutput",
        "AuditForOutput",
        "AuditScorecardOutput",
        "PolicyCheckOutput",
        "PolicyTestOutput",
        "PolicyFreezeOutput",
        "ReviewOutput",
        "ReviewQueueOutput",
    }
    assert governor <= exported, (
        f"governor outputs missing from barrel: {sorted(governor - exported)}"
    )
