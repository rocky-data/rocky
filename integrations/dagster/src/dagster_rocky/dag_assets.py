"""DAG-driven asset creation from Rocky's unified DAG.

Builds the full connected asset graph from a single ``rocky dag`` call.
Every :class:`DagNodeOutput` becomes one Dagster :class:`AssetSpec`.
Edges in the DAG become Dagster asset dependencies. Source, Load,
Transformation, Seed, Quality, and Snapshot nodes are all represented.

Test nodes are mapped to :class:`AssetCheckSpec` on their parent model
rather than becoming standalone assets.

Usage::

    from dagster_rocky import RockyResource, RockyDagsterTranslator
    from dagster_rocky.dag_assets import build_dag_specs

    rocky = RockyResource(config_path="rocky.toml")
    dag = rocky.dag(column_lineage=True)
    specs, node_map = build_dag_specs(dag, translator=RockyDagsterTranslator())
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import dagster as dg

from .column_lineage import build_column_lineage
from .freshness import freshness_policy_from_model

if TYPE_CHECKING:
    from .contracts import ContractRules
    from .translator import RockyDagsterTranslator
    from .types import (
        DagNodeOutput,
        DagResult,
        LineageEdgeRecord,
        ModelLineageResult,
        OptimizeResult,
    )


@dataclass
class DagAssetGroup:
    """One bucket of DAG-node specs sharing the same execution kind and
    partition shape.

    All specs in a ``DagAssetGroup`` are passed to a single ``multi_asset``
    decorator. The group key is used to namespace the multi-asset's name
    so multiple groups coexist without collision.
    """

    name: str
    specs: list[dg.AssetSpec] = field(default_factory=list)
    node_ids: list[str] = field(default_factory=list)
    partitions_def: dg.PartitionsDefinition | None = None
    shape_key: str = "unpartitioned"


def _asset_key_for_node(
    node: DagNodeOutput,
    translator: RockyDagsterTranslator,
) -> dg.AssetKey:
    """Derive a Dagster AssetKey from a DAG node based on its kind."""
    if node.kind == "transformation" and node.target is not None:
        return dg.AssetKey([
            node.target.catalog,
            node.target.schema_,
            node.target.table,
        ])
    if node.kind == "seed":
        return dg.AssetKey(["seed", node.label])
    if node.kind == "source":
        return dg.AssetKey([node.pipeline or "default", "source", node.label])
    if node.kind == "load":
        return dg.AssetKey([node.pipeline or "default", node.label])
    if node.kind == "quality":
        return dg.AssetKey(["quality", node.pipeline or "default", node.label])
    if node.kind == "snapshot":
        return dg.AssetKey(["snapshot", node.pipeline or "default", node.label])
    # Fallback: use the node ID as-is.
    return dg.AssetKey([node.id])


def _group_name_for_node(node: DagNodeOutput) -> str:
    """Derive a Dagster group name from a DAG node."""
    if node.kind == "transformation" and node.target is not None:
        return node.target.schema_
    if node.pipeline is not None:
        return node.pipeline
    return "default"


def _partitions_def_for_node(
    node: DagNodeOutput,
) -> dg.PartitionsDefinition | None:
    """Create a Dagster PartitionsDefinition from a node's partition shape."""
    if node.partition_shape is None:
        return None
    grain = node.partition_shape.granularity
    start = node.partition_shape.first_partition or "2024-01-01"
    if grain == "daily":
        return dg.DailyPartitionsDefinition(start_date=start)
    if grain == "hourly":
        return dg.HourlyPartitionsDefinition(start_date=start)
    if grain == "monthly":
        return dg.MonthlyPartitionsDefinition(start_date=start)
    if grain == "yearly":
        # Dagster doesn't have a built-in YearlyPartitionsDefinition.
        # Fall back to unpartitioned.
        return None
    return None


def _freshness_for_node(node: DagNodeOutput) -> dg.FreshnessPolicy | None:
    """Map a node's freshness config to a Dagster FreshnessPolicy."""
    if node.freshness is None:
        return None
    return freshness_policy_from_model(node.freshness)


def _shape_key_for_node(node: DagNodeOutput) -> str:
    """Derive a partition-shape key for grouping nodes."""
    if node.partition_shape is None:
        return "unpartitioned"
    return node.partition_shape.granularity


def _build_column_lineage_metadata(
    node: DagNodeOutput,
    column_lineage_edges: list[LineageEdgeRecord],
    node_id_to_key: dict[str, dg.AssetKey],
) -> dict[str, dg.TableColumnLineage] | None:
    """Build Dagster column lineage metadata from Rocky's lineage edges."""
    if not column_lineage_edges:
        return None

    # Filter edges targeting this node's model.
    model_edges = [
        e for e in column_lineage_edges if e.target.model == node.label
    ]
    if not model_edges:
        return None

    # Build a synthetic ModelLineageResult-like object for the existing
    # build_column_lineage helper.
    deps_by_column: dict[str, list[dg.TableColumnDep]] = {}
    for edge in model_edges:
        # Find the asset key for the source model.
        source_node_id = f"transformation:{edge.source.model}"
        source_key = node_id_to_key.get(source_node_id)
        if source_key is None:
            # Try seed.
            source_key = node_id_to_key.get(f"seed:{edge.source.model}")
        if source_key is None:
            continue

        col = edge.target.column
        if col not in deps_by_column:
            deps_by_column[col] = []
        deps_by_column[col].append(
            dg.TableColumnDep(asset_key=source_key, column_name=edge.source.column)
        )

    if not deps_by_column:
        return None

    return {"dagster/column_lineage": dg.TableColumnLineage(deps_by_column=deps_by_column)}


def build_dag_specs(
    dag_result: DagResult,
    *,
    translator: RockyDagsterTranslator,
    optimize_result: OptimizeResult | None = None,
    contract_rules_by_model: dict[str, ContractRules] | None = None,
) -> tuple[list[dg.AssetSpec], dict[str, dg.AssetKey]]:
    """Build one :class:`AssetSpec` per DAG node from ``rocky dag`` output.

    Returns:
        A tuple of (specs, node_id_to_asset_key_map). Test nodes are excluded
        from specs (they map to asset checks instead).
    """
    node_id_to_key: dict[str, dg.AssetKey] = {}
    column_lineage_edges = dag_result.column_lineage or []

    # First pass: compute all asset keys so we can resolve dependencies.
    for node in dag_result.nodes:
        if node.kind == "test":
            continue
        key = _asset_key_for_node(node, translator)
        node_id_to_key[node.id] = key

    # Second pass: build specs with resolved dependencies.
    specs: list[dg.AssetSpec] = []
    for node in dag_result.nodes:
        if node.kind == "test":
            continue

        key = node_id_to_key[node.id]

        # Resolve upstream dependencies.
        deps_list = node.depends_on or []
        deps = [
            node_id_to_key[dep_id]
            for dep_id in deps_list
            if dep_id in node_id_to_key
        ]

        # Build metadata.
        metadata: dict[str, object] = {
            "rocky/node_id": node.id,
            "rocky/kind": node.kind,
        }
        if node.pipeline is not None:
            metadata["rocky/pipeline"] = node.pipeline

        # Column lineage.
        col_lineage_meta = _build_column_lineage_metadata(
            node, column_lineage_edges, node_id_to_key,
        )
        if col_lineage_meta:
            metadata.update(col_lineage_meta)

        spec = dg.AssetSpec(
            key=key,
            deps=deps if deps else None,
            group_name=_group_name_for_node(node),
            tags={
                "rocky/kind": node.kind,
            },
            metadata=metadata,
            freshness_policy=_freshness_for_node(node),
            kinds={"rocky", node.kind},
        )
        specs.append(spec)

    return specs, node_id_to_key


def split_dag_specs_by_group(
    specs: list[dg.AssetSpec],
    dag_result: DagResult,
) -> list[DagAssetGroup]:
    """Group specs by (execution kind, partition shape) for multi_asset compat.

    Dagster's ``multi_asset`` requires all specs to share one
    ``PartitionsDefinition``. This function groups specs by their partition
    shape so each group can become one ``multi_asset``.
    """
    # Build node lookup.
    node_by_id: dict[str, DagNodeOutput] = {}
    for node in dag_result.nodes:
        node_by_id[node.id] = node

    groups: dict[str, DagAssetGroup] = {}
    for spec in specs:
        node_id = spec.metadata.get("rocky/node_id", "") if spec.metadata else ""
        node = node_by_id.get(str(node_id))

        if node is not None:
            shape_key = _shape_key_for_node(node)
            kind = node.kind
        else:
            shape_key = "unpartitioned"
            kind = "unknown"

        group_key = f"{kind}_{shape_key}"
        if group_key not in groups:
            pdef = _partitions_def_for_node(node) if node else None
            groups[group_key] = DagAssetGroup(
                name=group_key,
                partitions_def=pdef,
                shape_key=shape_key,
            )
        groups[group_key].specs.append(spec)
        if node is not None:
            groups[group_key].node_ids.append(node.id)

    return list(groups.values())
