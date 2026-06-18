"""RockyDagsterTranslator — maps Rocky sources/tables/models to Dagster asset keys and tags."""

from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING

import dagster as dg

from .types import ModelDetail, SourceInfo, TableInfo

if TYPE_CHECKING:
    from .types import DagNodeOutput

_INVALID_CHARS = re.compile(r"[^A-Za-z0-9_]")
# Dagster's strict tag charset is identical for key segments and strict values:
# ``[A-Za-z0-9_.-]`` (dagster._utils.tags ``VALID_TAG_KEY_REGEX`` /
# ``VALID_STRICT_TAG_VALUE_REGEX``). A single complement class drives both the
# key and value sanitizers below; if Dagster ever diverges the two charsets,
# split this into separate constants.
_INVALID_TAG_CHARS = re.compile(r"[^A-Za-z0-9_.\-]")


def _sanitize_key_part(s: str) -> str:
    """Replace characters invalid in Dagster names with underscores."""
    return _INVALID_CHARS.sub("_", s)


def _sanitize_tag_key(s: str) -> str:
    """Coerce an arbitrary governance tag key into a Dagster-valid tag key.

    Dagster tag keys allow ``[A-Za-z0-9_.-]`` plus one optional ``/``
    namespace separator (which this never introduces). Disallowed characters
    — including ``/`` and whitespace — collapse to ``_``, and the result is
    truncated to Dagster's 63-character key limit. Two keys that differ only
    in disallowed characters therefore collapse to the same sanitized key
    (last write wins); governance keys are expected to be simple identifiers,
    so this is rare. Returns ``""`` only for an empty input, in which case the
    caller drops the tag.
    """
    return _INVALID_TAG_CHARS.sub("_", s)[:63]


def _sanitize_tag_value(s: str) -> str:
    """Coerce an arbitrary governance tag value into a Dagster-valid strict value.

    Dagster validates tag *values* against ``^[A-Za-z0-9_.-]{0,63}$`` at
    ``AssetSpec`` construction (``normalize_tags(strict=True)``). Disallowed
    characters — ``@``, ``:``, ``/``, whitespace, etc. — collapse to ``_`` and
    the result is truncated to 63 characters. Unlike keys, an empty value is
    valid in Dagster and is preserved as-is. This is the value-side counterpart
    of :func:`_sanitize_tag_key`; without it, a governance value like
    ``data-eng@corp.com`` would raise at spec construction rather than degrade
    gracefully.
    """
    return _INVALID_TAG_CHARS.sub("_", s)[:63]


def strip_tenant_component(source: SourceInfo, tenant_component: str) -> SourceInfo:
    """Return a copy of ``source`` with the tenant component removed.

    Single source of truth for the tenant-as-partition collapse key shape:
    the component (when building collapsed specs) and the sensor (when
    selecting collapsed assets for a tenant partition) both derive keys by
    calling the *stock* translator on the stripped source, so they can
    never disagree on which collapsed asset a tenant maps to. Dropping the
    tenant entry yields ``[source_type, *non_tenant, table]`` and omits the
    per-tenant ``rocky/{component}`` tag automatically — no translator API
    change required.
    """
    remaining = {k: v for k, v in source.components.items() if k != tenant_component}
    return source.model_copy(update={"components": remaining})


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
        """Returns Dagster metadata for a table asset.

        Adapter-namespaced metadata from ``source.metadata`` (keys like
        ``fivetran.service``, ``fivetran.custom_reports``) is forwarded
        verbatim — non-string values are JSON-encoded so Dagster's metadata
        dict stays ``str``-keyed/``str``-valued while downstream consumers
        can ``json.loads(...)`` the projected JSON blobs for richer
        structure (e.g. custom-reports lists).
        """
        meta: dict[str, str] = {
            "source_id": source.id,
            "source_type": source.source_type,
        }
        if source.last_sync_at:
            meta["last_sync_at"] = source.last_sync_at.isoformat()
        if table.row_count is not None:
            meta["row_count"] = str(table.row_count)
        for key, value in source.metadata.items():
            if isinstance(value, str):
                meta[key] = value
            else:
                meta[key] = json.dumps(value, sort_keys=True, default=str)
        return meta

    def get_asset_deps(self, source: SourceInfo, table: TableInfo) -> list[dg.AssetKey]:
        """Returns upstream dependency keys for a table asset.

        Override to declare lineage from source assets (e.g., Fivetran → Rocky).
        Default: no dependencies.
        """
        return []

    def get_partition_key(self, source: SourceInfo) -> str | None:
        """Returns the tenant partition key for a source, or ``None``.

        Only consulted on the tenant-as-partition collapse path (when
        ``RockyComponent.tenant`` is configured). It is the **single
        chokepoint** for deriving a source's tenant partition key: the
        component, the sensor (dynamic-partition sync + ``RunRequest``),
        and the execution ``--filter`` all route through it, so a custom
        derivation can never drift between "which partition" and "which
        tenant gets materialized."

        Default: ``None`` — meaning "use the raw value of the configured
        tenant component verbatim" (the component supplies
        ``source.components[tenant.component]``). Rocky's ``discover``
        already emits component values as clean, lowercased SQL
        identifiers, which are valid Dagster partition keys, so the raw
        value is correct out of the box.

        Override **only** when the raw component value is not a valid /
        canonical partition key — e.g. to fold case variants onto a
        canonical client code. If you do, the component keeps a
        ``partition_key → original component value`` map so the execution
        ``--filter`` still targets the real per-tenant catalog; your
        returned key is what the partition set and ``RunRequest`` use.
        """
        return None

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

        Two kinds of tag are emitted:

        * **Governance tags** — the model's own ``[tags]`` block, inherited
          from its config group, projected as *first-class* Dagster tags so
          they're usable in asset selection (e.g. ``tag:domain=finance``).
          Keys are sanitized to the Dagster-valid charset via
          :func:`_sanitize_tag_key` and values via :func:`_sanitize_tag_value`
          (both required — Dagster validates keys *and* values at ``AssetSpec``
          construction); a key that sanitizes to empty is dropped, an empty
          value is kept.
        * **Synthesized metadata tags** — ``rocky/model_name``,
          ``rocky/target_catalog``, ``rocky/target_schema``, and
          ``rocky/strategy`` (the materialization type).

        The synthesized keys always contain a ``/``; sanitized governance
        keys never do, so a governance tag can never clobber Rocky's own
        metadata. Override this method to namespace governance tags
        differently or drop the synthesized ones.
        """
        tags: dict[str, str] = {}
        # `getattr` (not `model.tags`) so an older rocky-sdk without the field
        # degrades to "no governance tags" instead of crashing — same defensive
        # access the rest of this module uses for newer ModelDetail fields.
        for key, value in (getattr(model, "tags", None) or {}).items():
            sanitized = _sanitize_tag_key(str(key))
            if sanitized:
                tags[sanitized] = _sanitize_tag_value(str(value))
        # Synthesized values are warehouse identifiers / strategy enums and are
        # virtually always already valid, but sanitize them too so an
        # unexpected value can never raise at AssetSpec construction.
        tags["rocky/model_name"] = _sanitize_tag_value(model.name)
        tags["rocky/target_catalog"] = _sanitize_tag_value(str(model.target.get("catalog", "")))
        tags["rocky/target_schema"] = _sanitize_tag_value(str(model.target.get("schema", "")))
        strategy_type = model.strategy.get("type") if isinstance(model.strategy, dict) else None
        if strategy_type:
            tags["rocky/strategy"] = _sanitize_tag_value(str(strategy_type))
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

    # ------------------------------------------------------------------ #
    # DAG-mode translation (unified DAG nodes)                           #
    # ------------------------------------------------------------------ #

    def get_dag_node_asset_key(self, node: DagNodeOutput) -> dg.AssetKey:
        """Returns the Dagster asset key for a unified DAG node.

        Dispatches by node kind:

        - ``transformation`` → ``[catalog, schema, table]`` from target
        - ``seed`` → ``["seed", label]``
        - ``source`` → ``[pipeline, "source", label]``
        - ``load`` → ``[pipeline, label]``
        - ``quality`` → ``["quality", pipeline, label]``
        - ``snapshot`` → ``["snapshot", pipeline, label]``

        Override to customize key derivation for your pipeline layout.
        """
        pipeline = node.pipeline or "default"
        if node.kind == "transformation" and node.target is not None:
            return dg.AssetKey([node.target.catalog, node.target.schema_, node.target.table])
        if node.kind == "seed":
            return dg.AssetKey(["seed", node.label])
        if node.kind == "source":
            return dg.AssetKey([pipeline, "source"])
        if node.kind == "load":
            return dg.AssetKey([pipeline, "load"])
        if node.kind == "quality":
            return dg.AssetKey(["quality", pipeline])
        if node.kind == "snapshot":
            return dg.AssetKey(["snapshot", pipeline])
        return dg.AssetKey([_sanitize_key_part(node.id)])

    def get_dag_group_name(self, node: DagNodeOutput) -> str:
        """Returns the Dagster group name for a unified DAG node.

        Default: target schema for transformation nodes, pipeline name
        for everything else, ``"default"`` as fallback.
        """
        if node.kind == "transformation" and node.target is not None:
            return node.target.schema_
        if node.pipeline is not None:
            return node.pipeline
        return "default"
