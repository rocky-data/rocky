"""Tests for the column_lineage builder."""

from __future__ import annotations

import dagster as dg

from dagster_rocky.column_lineage import build_column_lineage
from dagster_rocky.types import (
    ColumnDef,
    LineageEdge,
    ModelLineageResult,
    QualifiedColumn,
    TransformKind,
)


def _lineage(*edges: LineageEdge) -> ModelLineageResult:
    return ModelLineageResult(
        version="0.3.0",
        command="lineage",
        model="fct_orders",
        columns=[ColumnDef(name="order_id"), ColumnDef(name="amount")],
        upstream=["stg_orders"],
        downstream=[],
        edges=list(edges),
    )


def _edge(
    src_model: str,
    src_col: str,
    tgt_model: str,
    tgt_col: str,
) -> LineageEdge:
    return LineageEdge(
        source=QualifiedColumn(model=src_model, column=src_col),
        target=QualifiedColumn(model=tgt_model, column=tgt_col),
        transform=TransformKind.direct,
    )


def test_build_column_lineage_groups_edges_by_target_column():
    lineage = _lineage(
        _edge("stg_orders", "id", "fct_orders", "order_id"),
        _edge("stg_orders", "value", "fct_orders", "amount"),
    )
    result = build_column_lineage(lineage)

    assert isinstance(result, dg.TableColumnLineage)
    assert "order_id" in result.deps_by_column
    assert "amount" in result.deps_by_column
    assert len(result.deps_by_column["order_id"]) == 1
    assert len(result.deps_by_column["amount"]) == 1


def test_build_column_lineage_uses_model_to_key_mapping():
    lineage = _lineage(_edge("stg_orders", "id", "fct_orders", "order_id"))
    model_to_key = {"stg_orders": dg.AssetKey(["staging", "stg_orders"])}

    result = build_column_lineage(lineage, model_to_key=model_to_key)

    dep = result.deps_by_column["order_id"][0]
    assert dep.asset_key == dg.AssetKey(["staging", "stg_orders"])
    assert dep.column_name == "id"


def test_build_column_lineage_falls_back_to_literal_model_name():
    """Edges referencing models not in model_to_key still produce deps,
    using AssetKey([model_name]) as a fallback."""
    lineage = _lineage(_edge("stg_orders", "id", "fct_orders", "order_id"))
    result = build_column_lineage(lineage, model_to_key={})

    dep = result.deps_by_column["order_id"][0]
    assert dep.asset_key == dg.AssetKey(["stg_orders"])
    assert dep.column_name == "id"


def test_build_column_lineage_skips_self_edges():
    """Self-edges (target column = source column on same model) are not
    lineage in the dependency sense — drop them."""
    lineage = _lineage(
        _edge("fct_orders", "order_id", "fct_orders", "order_id"),  # self
        _edge("stg_orders", "id", "fct_orders", "order_id"),  # real
    )
    result = build_column_lineage(lineage)

    # Only the real upstream edge survives
    assert len(result.deps_by_column["order_id"]) == 1
    assert result.deps_by_column["order_id"][0].asset_key == dg.AssetKey(["stg_orders"])


def test_build_column_lineage_empty_lineage_returns_empty():
    lineage = _lineage()
    result = build_column_lineage(lineage)
    assert result.deps_by_column == {}


def test_build_column_lineage_multiple_upstreams_per_column():
    """A column derived from multiple upstream columns should have all
    of them in deps_by_column."""
    lineage = _lineage(
        _edge("stg_a", "amount", "fct_orders", "total"),
        _edge("stg_b", "tax", "fct_orders", "total"),
    )
    result = build_column_lineage(lineage)

    deps = result.deps_by_column["total"]
    assert len(deps) == 2
    upstream_models = {dep.asset_key for dep in deps}
    assert dg.AssetKey(["stg_a"]) in upstream_models
    assert dg.AssetKey(["stg_b"]) in upstream_models
