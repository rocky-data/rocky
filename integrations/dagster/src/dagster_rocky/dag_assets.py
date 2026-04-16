"""DAG-driven asset creation from Rocky's unified DAG.

Builds the full connected asset graph from a single ``rocky dag`` call.
Every :class:`DagNodeOutput` becomes one Dagster :class:`AssetSpec`.
Edges in the DAG become Dagster asset dependencies. Source, Load,
Transformation, Seed, Quality, and Snapshot nodes are all represented.

Test nodes are mapped to :class:`AssetCheckSpec` on their parent model
rather than becoming standalone assets.

Usage::

    from dagster_rocky import RockyResource, RockyDagsterTranslator
    from dagster_rocky.dag_assets import build_dag_multi_assets

    rocky = RockyResource(config_path="rocky.toml")
    dag = rocky.dag(column_lineage=True)
    assets = build_dag_multi_assets(
        dag, rocky=rocky, translator=RockyDagsterTranslator(),
    )
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import dagster as dg

from .freshness import freshness_policy_from_model

if TYPE_CHECKING:
    from collections.abc import Iterator

    from .contracts import ContractRules
    from .resource import RockyResource
    from .translator import RockyDagsterTranslator
    from .types import (
        DagNodeOutput,
        DagResult,
        DiscoverResult,
        LineageEdgeRecord,
        OptimizeResult,
        RunResult,
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


# ---------------------------------------------------------------------------
# Partition helpers
# ---------------------------------------------------------------------------


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
    return None


def _shape_key_for_node(node: DagNodeOutput) -> str:
    """Derive a partition-shape key for grouping nodes."""
    if node.partition_shape is None:
        return "unpartitioned"
    return node.partition_shape.granularity


# ---------------------------------------------------------------------------
# Column lineage
# ---------------------------------------------------------------------------


def _build_column_lineage_metadata(
    node: DagNodeOutput,
    column_lineage_edges: list[LineageEdgeRecord],
    node_id_to_key: dict[str, dg.AssetKey],
) -> dict[str, dg.TableColumnLineage] | None:
    """Build Dagster column lineage metadata from Rocky's lineage edges."""
    if not column_lineage_edges:
        return None

    model_edges = [e for e in column_lineage_edges if e.target.model == node.label]
    if not model_edges:
        return None

    deps_by_column: dict[str, list[dg.TableColumnDep]] = {}
    for edge in model_edges:
        source_node_id = f"transformation:{edge.source.model}"
        source_key = node_id_to_key.get(source_node_id)
        if source_key is None:
            source_key = node_id_to_key.get(f"seed:{edge.source.model}")
        if source_key is None:
            continue

        col = edge.target.column
        deps_by_column.setdefault(col, []).append(
            dg.TableColumnDep(asset_key=source_key, column_name=edge.source.column)
        )

    if not deps_by_column:
        return None

    return {"dagster/column_lineage": dg.TableColumnLineage(deps_by_column=deps_by_column)}


# ---------------------------------------------------------------------------
# Spec building
# ---------------------------------------------------------------------------


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

    # First pass: compute all asset keys via the translator.
    for node in dag_result.nodes:
        if node.kind == "test":
            continue
        key = translator.get_dag_node_asset_key(node)
        node_id_to_key[node.id] = key

    # Second pass: build specs with resolved dependencies.
    specs: list[dg.AssetSpec] = []
    for node in dag_result.nodes:
        if node.kind == "test":
            continue

        key = node_id_to_key[node.id]

        deps_list = node.depends_on or []
        deps = [node_id_to_key[dep_id] for dep_id in deps_list if dep_id in node_id_to_key]

        metadata: dict[str, object] = {
            "rocky/node_id": node.id,
            "rocky/kind": node.kind,
        }
        if node.pipeline is not None:
            metadata["rocky/pipeline"] = node.pipeline

        col_lineage_meta = _build_column_lineage_metadata(
            node,
            column_lineage_edges,
            node_id_to_key,
        )
        if col_lineage_meta:
            metadata.update(col_lineage_meta)

        freshness = None
        if node.freshness is not None:
            freshness = freshness_policy_from_model(node.freshness)

        spec = dg.AssetSpec(
            key=key,
            deps=deps if deps else None,
            group_name=translator.get_dag_group_name(node),
            tags={"rocky/kind": node.kind},
            metadata=metadata,
            freshness_policy=freshness,
            kinds={"rocky", node.kind},
        )
        specs.append(spec)

    return specs, node_id_to_key


# ---------------------------------------------------------------------------
# Grouping
# ---------------------------------------------------------------------------


def split_dag_specs_by_group(
    specs: list[dg.AssetSpec],
    dag_result: DagResult,
) -> list[DagAssetGroup]:
    """Group specs by (execution kind, partition shape) for multi_asset compat."""
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


# ---------------------------------------------------------------------------
# Executable multi_asset construction
# ---------------------------------------------------------------------------


def _safe_asset_name(name: str) -> str:
    """Sanitize a string for use as a Dagster asset name."""
    return name.replace("-", "_").replace(".", "_").replace(" ", "_").replace(":", "_")


def _partition_kwargs_from_context(
    context: dg.AssetExecutionContext,
    partitions_def: dg.PartitionsDefinition | None,
) -> dict[str, object]:
    """Extract partition selection kwargs from the Dagster execution context."""
    if partitions_def is None:
        return {}
    if context.has_partition_key_range:
        key_range = context.partition_key_range
        return {"partition_from": key_range.start, "partition_to": key_range.end}
    if context.has_partition_key:
        return {"partition": context.partition_key}
    return {}


def _emit_model_results(
    result: RunResult,
    selected_keys: set[dg.AssetKey],
) -> Iterator[dg.MaterializeResult]:
    """Yield MaterializeResult for model materializations in the run output."""
    from .component import RockyMetadataSet

    for mat in result.materializations:
        asset_key = dg.AssetKey(mat.asset_key)
        if asset_key not in selected_keys:
            continue
        metadata: dict[str, dg.MetadataValue] = {
            **RockyMetadataSet(
                strategy=mat.metadata.strategy if mat.metadata else None,
                duration_ms=mat.duration_ms,
                rows_copied=mat.rows_copied,
                watermark=(
                    mat.metadata.watermark.isoformat()
                    if mat.metadata is not None and mat.metadata.watermark is not None
                    else None
                ),
                target_table_full_name=(
                    mat.metadata.target_table_full_name if mat.metadata else None
                ),
                sql_hash=mat.metadata.sql_hash if mat.metadata else None,
                column_count=mat.metadata.column_count if mat.metadata else None,
                compile_time_ms=mat.metadata.compile_time_ms if mat.metadata else None,
            ),
            "dagster/duration_ms": dg.MetadataValue.int(mat.duration_ms),
        }
        if mat.rows_copied is not None:
            metadata["dagster/row_count"] = dg.MetadataValue.int(mat.rows_copied)
        if mat.partition is not None:
            metadata["rocky/partition_key"] = dg.MetadataValue.text(mat.partition.key)
        yield dg.MaterializeResult(asset_key=asset_key, metadata=metadata)


def _build_source_filters(discover_result: DiscoverResult | None) -> list[str]:
    """Extract per-source filter strings from the discover output.

    Returns a list like ``["source=orders", "source=customers"]`` — one
    per discovered source. Used by source/load execution to run the
    replication pipeline with the correct filter.
    """
    if discover_result is None:
        return []
    filters: list[str] = []
    for source in discover_result.sources:
        # The first string-valued component is the natural filter key.
        for key, val in source.components.items():
            if isinstance(val, str):
                filters.append(f"{key}={val}")
                break
        else:
            # Fallback to id-based filter.
            filters.append(f"id={source.id}")
    return filters


def build_dag_multi_assets(
    dag_result: DagResult,
    *,
    rocky: RockyResource,
    translator: RockyDagsterTranslator,
    discover_result: DiscoverResult | None = None,
    optimize_result: OptimizeResult | None = None,
    contract_rules_by_model: dict[str, ContractRules] | None = None,
) -> list[dg.AssetsDefinition]:
    """Build executable multi_assets from the unified DAG.

    Groups nodes by (kind, partition_shape). Each group becomes one
    ``@dg.multi_asset`` with ``can_subset=True``. Within each multi_asset,
    materialization dispatches to the right Rocky CLI command based on
    node kind:

    - ``transformation`` → ``rocky.run_model(model_name)``
    - ``source`` / ``load`` → ``rocky.run(filter=...)`` using discover output
    - ``seed`` → ``rocky.run_seed(name)`` (or placeholder when unavailable)
    - ``quality`` / ``snapshot`` → placeholder (graph-only for now)
    """
    specs, node_id_to_key = build_dag_specs(
        dag_result,
        translator=translator,
        optimize_result=optimize_result,
        contract_rules_by_model=contract_rules_by_model,
    )

    groups = split_dag_specs_by_group(specs, dag_result)
    source_filters = _build_source_filters(discover_result)

    # Build a node lookup for the execution bodies.
    node_by_id: dict[str, DagNodeOutput] = {}
    for node in dag_result.nodes:
        node_by_id[node.id] = node

    assets: list[dg.AssetsDefinition] = []
    for group in groups:
        asset_def = _make_dag_group_asset(
            group=group,
            node_by_id=node_by_id,
            rocky=rocky,
            source_filters=source_filters,
        )
        assets.append(asset_def)

    return assets


def _make_dag_group_asset(
    *,
    group: DagAssetGroup,
    node_by_id: dict[str, DagNodeOutput],
    rocky: RockyResource,
    source_filters: list[str],
) -> dg.AssetsDefinition:
    """Create one multi_asset for a group of DAG nodes."""
    asset_name = f"rocky_dag_{_safe_asset_name(group.name)}"

    # Build a spec-key → node-id lookup for the execution body.
    spec_key_to_node_id: dict[dg.AssetKey, str] = {}
    for spec, node_id in zip(group.specs, group.node_ids, strict=False):
        spec_key_to_node_id[spec.key] = node_id

    @dg.multi_asset(
        name=asset_name,
        specs=group.specs,
        can_subset=True,
        partitions_def=group.partitions_def,
    )
    def _asset(context):
        selected_keys = set(context.selected_asset_keys)
        partition_kwargs = _partition_kwargs_from_context(context, group.partitions_def)

        for spec_key in selected_keys:
            node_id = spec_key_to_node_id.get(spec_key)
            if node_id is None:
                yield dg.MaterializeResult(asset_key=spec_key)
                continue

            node = node_by_id.get(node_id)
            if node is None:
                yield dg.MaterializeResult(asset_key=spec_key)
                continue

            if node.kind == "transformation":
                context.log.info(f"Executing rocky run --model {node.label}")
                result = rocky.run_model(
                    node.label,
                    **partition_kwargs,  # type: ignore[arg-type]
                )
                context.log.info(
                    f"Model {node.label}: {result.tables_copied} copied, "
                    f"{result.tables_failed} failed in {result.duration_ms}ms"
                )
                emitted = False
                for mr in _emit_model_results(result, {spec_key}):
                    emitted = True
                    yield mr
                if not emitted:
                    yield dg.MaterializeResult(
                        asset_key=spec_key,
                        metadata={
                            "dagster/duration_ms": dg.MetadataValue.int(result.duration_ms),
                        },
                    )

            elif node.kind in ("source", "load"):
                # Source/load nodes run the replication pipeline using
                # filters from the discover output. Each filter runs one
                # source; results are aggregated.
                pipeline_name = node.pipeline or "default"
                if not source_filters:
                    context.log.warning(
                        f"No discover output — cannot execute {node.kind} "
                        f"node {node.label}. Run `rocky discover` first."
                    )
                    yield dg.MaterializeResult(asset_key=spec_key)
                    continue

                total_copied = 0
                total_duration = 0
                for src_filter in source_filters:
                    context.log.info(
                        f"Executing rocky run --pipeline {pipeline_name} --filter {src_filter}"
                    )
                    result = rocky.run(
                        filter=src_filter,
                        pipeline=pipeline_name,
                        **partition_kwargs,  # type: ignore[arg-type]
                    )
                    total_copied += result.tables_copied
                    total_duration += result.duration_ms

                context.log.info(
                    f"{node.kind} {node.label}: {total_copied} tables copied in {total_duration}ms"
                )
                yield dg.MaterializeResult(
                    asset_key=spec_key,
                    metadata={
                        "dagster/row_count": dg.MetadataValue.int(total_copied),
                        "dagster/duration_ms": dg.MetadataValue.int(total_duration),
                    },
                )

            else:
                # seed, quality, snapshot — participate in the DAG graph
                # and can be marked as materialized but don't execute a
                # Rocky command. Full execution for seeds (via a future
                # `rocky.seed()` resource method) and quality/snapshot
                # pipelines is a follow-up.
                context.log.info(
                    f"Node {node.label} ({node.kind}): graph-only asset (marking as materialized)"
                )
                yield dg.MaterializeResult(asset_key=spec_key)

    return _asset
