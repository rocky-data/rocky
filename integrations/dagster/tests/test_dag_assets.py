"""Tests for the DAG-driven asset builder."""

from __future__ import annotations

import dagster as dg

from dagster_rocky.dag_assets import (
    DagAssetGroup,
    build_dag_specs,
    split_dag_specs_by_group,
)
from dagster_rocky.translator import RockyDagsterTranslator
from dagster_rocky.types import DagResult


def _make_dag_result(
    nodes: list[dict] | None = None,
    edges: list[dict] | None = None,
    column_lineage: list[dict] | None = None,
) -> DagResult:
    """Build a minimal DagResult from dicts."""
    return DagResult.model_validate({
        "version": "1.0.0",
        "command": "dag",
        "nodes": nodes or [],
        "edges": edges or [],
        "execution_layers": [],
        "summary": {
            "total_nodes": len(nodes or []),
            "total_edges": len(edges or []),
            "execution_layers": 0,
            "counts_by_kind": {},
        },
        "column_lineage": column_lineage or [],
    })


# ---------------------------------------------------------------------------
# build_dag_specs
# ---------------------------------------------------------------------------


def test_build_dag_specs_one_per_node():
    """One AssetSpec per non-test node."""
    dag = _make_dag_result(nodes=[
        {"id": "source:raw", "kind": "source", "label": "raw (source)", "pipeline": "raw"},
        {"id": "load:raw", "kind": "load", "label": "raw (load)", "pipeline": "raw",
         "depends_on": ["source:raw"]},
        {"id": "transformation:stg_orders", "kind": "transformation", "label": "stg_orders",
         "pipeline": "transform",
         "target": {"catalog": "warehouse", "schema": "staging", "table": "stg_orders"},
         "depends_on": ["load:raw"]},
    ])
    specs, node_map = build_dag_specs(dag, translator=RockyDagsterTranslator())
    assert len(specs) == 3
    assert len(node_map) == 3


def test_build_dag_specs_resolves_dependencies():
    """Upstream dependencies resolved to AssetKeys."""
    dag = _make_dag_result(nodes=[
        {"id": "source:raw", "kind": "source", "label": "raw (source)", "pipeline": "raw"},
        {"id": "load:raw", "kind": "load", "label": "raw (load)", "pipeline": "raw",
         "depends_on": ["source:raw"]},
    ])
    specs, node_map = build_dag_specs(dag, translator=RockyDagsterTranslator())
    load_spec = next(s for s in specs if "load" in str(s.key))
    assert load_spec.deps is not None
    assert len(load_spec.deps) == 1


def test_build_dag_specs_transformation_uses_target_as_key():
    """Transformation nodes use [catalog, schema, table] as the AssetKey."""
    dag = _make_dag_result(nodes=[
        {"id": "transformation:fct_revenue", "kind": "transformation",
         "label": "fct_revenue", "pipeline": "transform",
         "target": {"catalog": "analytics", "schema": "marts", "table": "fct_revenue"}},
    ])
    specs, node_map = build_dag_specs(dag, translator=RockyDagsterTranslator())
    assert len(specs) == 1
    assert specs[0].key == dg.AssetKey(["analytics", "marts", "fct_revenue"])


def test_build_dag_specs_test_nodes_excluded():
    """Test nodes are not turned into assets."""
    dag = _make_dag_result(nodes=[
        {"id": "transformation:orders", "kind": "transformation", "label": "orders",
         "target": {"catalog": "w", "schema": "s", "table": "orders"}},
        {"id": "test:orders::not_null_id", "kind": "test", "label": "orders::not_null_id",
         "depends_on": ["transformation:orders"]},
    ])
    specs, node_map = build_dag_specs(dag, translator=RockyDagsterTranslator())
    assert len(specs) == 1  # only the transformation, not the test
    assert "test:orders::not_null_id" not in node_map


def test_build_dag_specs_full_lineage_chain():
    """Full source -> load -> stg -> fct chain with correct dependencies."""
    dag = _make_dag_result(nodes=[
        {"id": "source:raw", "kind": "source", "label": "raw (source)", "pipeline": "raw"},
        {"id": "load:raw", "kind": "load", "label": "raw (load)", "pipeline": "raw",
         "depends_on": ["source:raw"]},
        {"id": "transformation:stg_orders", "kind": "transformation", "label": "stg_orders",
         "target": {"catalog": "w", "schema": "staging", "table": "stg_orders"},
         "depends_on": ["load:raw"]},
        {"id": "transformation:fct_revenue", "kind": "transformation", "label": "fct_revenue",
         "target": {"catalog": "w", "schema": "marts", "table": "fct_revenue"},
         "depends_on": ["transformation:stg_orders"]},
    ])
    specs, node_map = build_dag_specs(dag, translator=RockyDagsterTranslator())
    assert len(specs) == 4

    # Check fct_revenue depends on stg_orders.
    fct_spec = next(s for s in specs if s.key.path[-1] == "fct_revenue")
    stg_key = node_map["transformation:stg_orders"]
    fct_dep_keys = {dep.asset_key for dep in fct_spec.deps}
    assert stg_key in fct_dep_keys

    # Check stg_orders depends on load:raw.
    stg_spec = next(s for s in specs if s.key.path[-1] == "stg_orders")
    load_key = node_map["load:raw"]
    stg_dep_keys = {dep.asset_key for dep in stg_spec.deps}
    assert load_key in stg_dep_keys


def test_build_dag_specs_freshness_auto_mapped():
    """Freshness config maps to FreshnessPolicy."""
    dag = _make_dag_result(nodes=[
        {"id": "transformation:orders", "kind": "transformation", "label": "orders",
         "target": {"catalog": "w", "schema": "s", "table": "orders"},
         "freshness": {"max_lag_seconds": 86400}},
    ])
    specs, _ = build_dag_specs(dag, translator=RockyDagsterTranslator())
    assert specs[0].freshness_policy is not None


def test_build_dag_specs_seed_node():
    """Seed nodes get an AssetKey of ["seed", name]."""
    dag = _make_dag_result(nodes=[
        {"id": "seed:ref_countries", "kind": "seed", "label": "ref_countries"},
    ])
    specs, node_map = build_dag_specs(dag, translator=RockyDagsterTranslator())
    assert len(specs) == 1
    assert specs[0].key == dg.AssetKey(["seed", "ref_countries"])


def test_build_dag_specs_column_lineage_attached():
    """Column lineage metadata attached when edges are present."""
    dag = _make_dag_result(
        nodes=[
            {"id": "transformation:stg_orders", "kind": "transformation",
             "label": "stg_orders",
             "target": {"catalog": "w", "schema": "s", "table": "stg_orders"}},
            {"id": "transformation:fct_revenue", "kind": "transformation",
             "label": "fct_revenue",
             "target": {"catalog": "w", "schema": "m", "table": "fct_revenue"},
             "depends_on": ["transformation:stg_orders"]},
        ],
        column_lineage=[
            {
                "source": {"model": "stg_orders", "column": "amount"},
                "target": {"model": "fct_revenue", "column": "total_amount"},
                "transform": "expression",
            },
        ],
    )
    specs, _ = build_dag_specs(dag, translator=RockyDagsterTranslator())
    fct_spec = next(s for s in specs if s.key.path[-1] == "fct_revenue")
    assert "dagster/column_lineage" in (fct_spec.metadata or {})


# ---------------------------------------------------------------------------
# split_dag_specs_by_group
# ---------------------------------------------------------------------------


def test_split_dag_specs_groups_by_kind_and_shape():
    """Specs are grouped by (kind, partition_shape)."""
    dag = _make_dag_result(nodes=[
        {"id": "source:raw", "kind": "source", "label": "raw", "pipeline": "raw"},
        {"id": "transformation:m1", "kind": "transformation", "label": "m1",
         "target": {"catalog": "w", "schema": "s", "table": "m1"}},
        {"id": "transformation:m2", "kind": "transformation", "label": "m2",
         "target": {"catalog": "w", "schema": "s", "table": "m2"},
         "partition_shape": {"granularity": "daily"}},
    ])
    specs, _ = build_dag_specs(dag, translator=RockyDagsterTranslator())
    groups = split_dag_specs_by_group(specs, dag)
    # Should have 3 groups: source_unpartitioned, transformation_unpartitioned,
    # transformation_daily.
    assert len(groups) == 3
    names = {g.name for g in groups}
    assert "source_unpartitioned" in names
    assert "transformation_unpartitioned" in names
    assert "transformation_daily" in names


def test_split_empty_returns_empty():
    """Empty spec list returns no groups."""
    dag = _make_dag_result()
    groups = split_dag_specs_by_group([], dag)
    assert groups == []


# ---------------------------------------------------------------------------
# Backward compatibility
# ---------------------------------------------------------------------------


def test_dag_result_parses_from_json():
    """DagResult can be parsed from a rocky dag JSON payload."""
    import json

    payload = json.dumps({
        "version": "1.0.0",
        "command": "dag",
        "nodes": [
            {"id": "source:raw", "kind": "source", "label": "raw (source)",
             "pipeline": "raw"},
        ],
        "edges": [],
        "execution_layers": [["source:raw"]],
        "summary": {
            "total_nodes": 1,
            "total_edges": 0,
            "execution_layers": 1,
            "counts_by_kind": {"source": 1},
        },
    })
    result = DagResult.model_validate_json(payload)
    assert len(result.nodes) == 1
    assert result.nodes[0].kind == "source"


def test_dag_result_in_parse_rocky_output():
    """parse_rocky_output auto-detects the dag command."""
    import json

    from dagster_rocky.types import parse_rocky_output

    payload = json.dumps({
        "version": "1.0.0",
        "command": "dag",
        "nodes": [],
        "edges": [],
        "execution_layers": [],
        "summary": {
            "total_nodes": 0,
            "total_edges": 0,
            "execution_layers": 0,
            "counts_by_kind": {},
        },
    })
    result = parse_rocky_output(payload)
    assert isinstance(result, DagResult)
