"""RockyDagsterTranslator — maps Rocky sources/tables/models to Dagster asset keys and tags."""

from __future__ import annotations

import dagster as dg

from .types import ModelDetail, SourceInfo, TableInfo


class RockyDagsterTranslator:
    """Translates Rocky source/table/model info into Dagster asset keys, tags, and groups.

    Override methods to customize the mapping for your pipeline. The base
    implementation builds source-replication keys from the source's
    components dict and derived-model keys from each model's
    ``[target] catalog.schema.table`` triple.
    """

    def get_asset_key(self, source: SourceInfo, table: TableInfo) -> dg.AssetKey:
        """Returns the Dagster asset key for a table.

        Default: [source_type, *component_values, table_name]
        """
        parts = [source.source_type]
        for v in source.components.values():
            if isinstance(v, list):
                parts.append("__".join(v))
            else:
                parts.append(str(v))
        parts.append(table.name)
        return dg.AssetKey(parts)

    def get_group_name(self, source: SourceInfo) -> str:
        """Returns the Dagster group name for a source's tables.

        Default: the first component whose value is a string, using
        ``source.components`` dict iteration order. Python 3.7+ preserves
        dict insertion order, and Pydantic 2 preserves the order from the
        JSON parse — so the "first" is deterministic as long as the
        upstream producer (Rocky's engine) emits components in a stable
        order. That's the contract today.

        If every component value is a list (a rare but valid shape
        representing multi-valued axes like ``regions=[...]``), there is
        no natural string axis to group by and this method falls back to
        ``source.source_type`` so downstream Dagster UI groupings remain
        non-empty.

        If you need a *specific* component to drive grouping (rather than
        "whichever string comes first"), subclass the translator and
        override this method to read the named key directly. For example,
        if every source in your pipeline carries a ``tenant`` component
        and you want grouping pinned to that regardless of how the engine
        orders the components dict::

            class TenantTranslator(RockyDagsterTranslator):
                def get_group_name(self, source: SourceInfo) -> str:
                    tenant = source.components.get("tenant")
                    if isinstance(tenant, str):
                        return tenant
                    return super().get_group_name(source)

        The override is deterministic regardless of upstream component
        ordering, and it still falls back to the base behavior when the
        named key is missing or non-scalar.
        """
        for v in source.components.values():
            if isinstance(v, str):
                return v
        return source.source_type

    def get_tags(self, source: SourceInfo, table: TableInfo) -> dict[str, str]:
        """Returns Dagster tags for a table asset."""
        tags = {"rocky/source_type": source.source_type}
        for k, v in source.components.items():
            if isinstance(v, str):
                tags[f"rocky/{k}"] = v
        return tags

    def get_metadata(self, source: SourceInfo, table: TableInfo) -> dict[str, str]:
        """Returns Dagster metadata for a table asset."""
        meta: dict[str, str] = {
            "source_id": source.id,
            "source_type": source.source_type,
        }
        if source.last_sync_at:
            meta["last_sync_at"] = source.last_sync_at.isoformat()
        if table.row_count is not None:
            meta["row_count"] = str(table.row_count)
        return meta

    def get_asset_deps(self, source: SourceInfo, table: TableInfo) -> list[dg.AssetKey]:
        """Returns upstream dependency keys for a table asset.

        Override to declare lineage from source assets (e.g., Fivetran → Rocky).
        Default: no dependencies.
        """
        return []

    # ------------------------------------------------------------------ #
    # Derived-model translation (T-derived-models)                       #
    # ------------------------------------------------------------------ #

    def get_model_asset_key(self, model: ModelDetail) -> dg.AssetKey:
        """Returns the Dagster asset key for a derived model.

        Default: ``[catalog, schema, table]`` from the model's
        ``[target]`` block. Override to namespace differently (e.g.,
        prefix with ``warehouse`` or fold the catalog into a tag).
        """
        target = model.target
        return dg.AssetKey(
            [
                str(target["catalog"]),
                str(target["schema"]),
                str(target["table"]),
            ]
        )

    def get_model_group_name(self, model: ModelDetail) -> str:
        """Returns the Dagster group name for a derived model.

        Default: the target schema. Models in the same target schema
        share a group, which usually corresponds to a logical layer
        (raw / staging / marts).
        """
        return str(model.target.get("schema", "models"))

    def get_model_tags(self, model: ModelDetail) -> dict[str, str]:
        """Returns Dagster tags for a derived model asset.

        Default tags: ``rocky/strategy`` (the materialization type),
        ``rocky/target_catalog``, ``rocky/target_schema``,
        ``rocky/model_name``.
        """
        tags: dict[str, str] = {
            "rocky/model_name": model.name,
            "rocky/target_catalog": str(model.target.get("catalog", "")),
            "rocky/target_schema": str(model.target.get("schema", "")),
        }
        strategy_type = model.strategy.get("type") if isinstance(model.strategy, dict) else None
        if strategy_type:
            tags["rocky/strategy"] = str(strategy_type)
        return tags

    def get_model_metadata(self, model: ModelDetail) -> dict[str, str]:
        """Returns Dagster metadata for a derived model asset."""
        target = model.target
        return {
            "rocky/target_table": (
                f"{target.get('catalog', '')}.{target.get('schema', '')}.{target.get('table', '')}"
            ),
            "rocky/strategy": (
                str(model.strategy.get("type", "")) if isinstance(model.strategy, dict) else ""
            ),
        }
