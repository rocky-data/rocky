"""Derived-model surfacing — convert Rocky compile output into Dagster assets.

`RockyComponent` historically only surfaced source-replication tables (one
``AssetSpec`` per ``(source, table)`` pair). This module adds the second
half: turning the derived-model entries from ``CompileResult.models_detail``
into their own Dagster assets, grouped by partitioning shape so that each
multi-asset has a single, consistent ``PartitionsDefinition``.

Why grouping by partition shape matters: Dagster's ``multi_asset`` requires
all specs inside it to share one ``PartitionsDefinition``. A project that
mixes ``time_interval`` (daily) models with ``incremental`` (unpartitioned)
models cannot put them in the same multi-asset. This module splits by
shape automatically:

  - One multi-asset per partitioning shape (unpartitioned, daily, hourly,
    monthly, yearly).
  - All models inside a multi-asset share the same partition definition
    (or none).
  - Cross-shape dependencies still render correctly because deps are
    asset-key references.

The module is split in two halves so callers that prefer hand-rolled
multi-assets can use only the spec builder:

* :func:`build_model_specs` — pure function that turns a ``CompileResult``
  into a list of ``AssetSpec`` decorated with freshness, partitions,
  optimize metadata, contract specs, and inter-model deps.

* :func:`split_model_specs_by_partition_shape` — groups the specs into
  ``ModelGroup`` buckets, one per shape. Each bucket has a stable
  ``shape_key`` derived from the partitions definition (or ``None``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import dagster as dg

from .freshness import freshness_policy_from_model
from .observability import optimize_metadata_for_keys
from .partitions import partitions_def_for_model_detail

if TYPE_CHECKING:
    from .contracts import ContractRules
    from .translator import RockyDagsterTranslator
    from .types import CompileResult, ModelDetail, OptimizeResult


@dataclass
class ModelGroup:
    """One bucket of derived-model specs sharing the same partition shape.

    All specs in a ``ModelGroup`` are passed to a single ``multi_asset``
    decorator. The shape key is used to namespace the multi-asset's name
    so multiple groups (one per shape) coexist without collision.
    """

    name: str
    specs: list[dg.AssetSpec] = field(default_factory=list)
    partitions_def: dg.PartitionsDefinition | None = None
    #: Stable identifier for this group's partition shape, used for the
    #: multi-asset's ``name`` suffix. ``"unpartitioned"`` for the no-partition
    #: bucket; ``"daily"`` / ``"hourly"`` / etc. for time-window buckets.
    shape_key: str = "unpartitioned"


def build_model_specs(
    compile_result: CompileResult,
    *,
    translator: RockyDagsterTranslator,
    optimize_result: OptimizeResult | None = None,
    contract_rules_by_model: dict[str, ContractRules] | None = None,
) -> list[dg.AssetSpec]:
    """Build one ``AssetSpec`` per model in ``compile_result.models_detail``.

    Each spec is decorated with:

    * **Freshness policy** — from the model's ``[freshness]`` frontmatter
      via :func:`freshness_policy_from_model`. ``None`` when not declared.
    * **Partitions definition** — from the model's ``time_interval``
      strategy via :func:`partitions_def_for_model_detail`. ``None`` for
      ``full_refresh`` / ``incremental`` / ``merge``. Stored on
      ``AssetSpec.partitions_def`` so :func:`split_model_specs_by_partition_shape`
      can group consistent shapes together.
    * **Optimize metadata** — from ``rocky optimize`` recommendations
      via :func:`optimize_metadata_for_keys` when ``optimize_result`` is
      passed. Merged into ``spec.metadata``. Skipped silently if no
      recommendation matches the model name.
    * **Inter-model dependencies** — for each entry in ``model.depends_on``,
      the corresponding asset key is looked up in the ``model_to_key`` map
      built from ``compile_result.models_detail``. Cross-model lineage
      arrows render in the asset graph.

    Contract check specs are NOT added here — they're handled at the
    component level in ``_build_check_specs`` so they integrate with the
    existing default-checks pipeline. The ``contract_rules_by_model``
    parameter is accepted for future extension but currently unused.

    Args:
        compile_result: Cached ``rocky compile`` output. Models come
            from ``compile_result.models_detail``.
        translator: Translator instance for asset key / group / tag
            derivation. Defaults are reasonable; override
            :class:`RockyDagsterTranslator.get_model_*` methods to
            customize.
        optimize_result: Optional ``rocky optimize`` output. When
            present, recommendations are merged into matching models'
            metadata at load time.
        contract_rules_by_model: Reserved for future use (contracts
            integration). Currently ignored — contract specs are added
            in the component layer, not here.

    Returns:
        A list of ``dg.AssetSpec``, one per derived model. Empty when
        ``compile_result.models_detail`` is empty.
    """
    del contract_rules_by_model  # noqa: F841 — reserved for future use

    # Build a name → asset_key map up front so depends_on can be resolved.
    model_to_key: dict[str, dg.AssetKey] = {}
    for model in compile_result.models_detail:
        model_to_key[model.name] = translator.get_model_asset_key(model)

    optimize_meta: dict[dg.AssetKey, dict[str, dg.MetadataValue]] = {}
    if optimize_result is not None:
        optimize_meta = optimize_metadata_for_keys(optimize_result, model_to_key=model_to_key)

    specs: list[dg.AssetSpec] = []
    for model in compile_result.models_detail:
        asset_key = model_to_key[model.name]
        deps = _resolve_model_deps(model, model_to_key)
        metadata: dict[str, dg.MetadataValue | str] = {
            **translator.get_model_metadata(model),
        }
        # Optimize metadata wins over translator defaults when keys collide.
        for k, v in optimize_meta.get(asset_key, {}).items():
            metadata[k] = v

        partitions_def = partitions_def_for_model_detail(model)
        freshness_policy = freshness_policy_from_model(model.freshness)

        specs.append(
            dg.AssetSpec(
                key=asset_key,
                deps=deps or None,
                group_name=translator.get_model_group_name(model),
                tags=translator.get_model_tags(model),
                metadata=metadata,
                kinds={"rocky", "model"},
                freshness_policy=freshness_policy,
                partitions_def=partitions_def,
            )
        )

    return specs


def _resolve_model_deps(
    model: ModelDetail,
    model_to_key: dict[str, dg.AssetKey],
) -> list[dg.AssetKey]:
    """Resolve a model's ``depends_on`` list to AssetKey references.

    Each entry is looked up in ``model_to_key``. Entries that don't
    correspond to a known model are silently dropped — they're either
    references to source replication tables (which the source-asset
    surface handles) or typos.
    """
    deps: list[dg.AssetKey] = []
    depends_on_field = getattr(model, "depends_on", None) or []
    for entry in depends_on_field:
        key = model_to_key.get(str(entry))
        if key is not None:
            deps.append(key)
    return deps


def split_model_specs_by_partition_shape(
    specs: list[dg.AssetSpec],
) -> list[ModelGroup]:
    """Group ``AssetSpec`` instances by partitioning shape.

    Dagster's ``multi_asset`` requires every spec inside it to share one
    ``PartitionsDefinition``. This function buckets the input specs by
    shape so callers can build one multi-asset per bucket.

    The shape key is derived from the partitions definition class name
    (`"daily"`, `"hourly"`, `"monthly"`, `"timewindow"`) so two daily
    definitions with different start dates still share a bucket. This is
    pragmatically what users want: one multi-asset for "all daily
    models", regardless of when each one started.

    Args:
        specs: AssetSpec instances, possibly with mixed
            ``partitions_def`` shapes.

    Returns:
        A list of :class:`ModelGroup`. There is exactly one bucket per
        distinct shape, ordered by appearance in ``specs``. The
        ``unpartitioned`` bucket is always present last (or absent if
        every spec is partitioned).
    """
    by_shape: dict[str, ModelGroup] = {}
    for spec in specs:
        shape_key = _shape_key_for_spec(spec)
        if shape_key not in by_shape:
            by_shape[shape_key] = ModelGroup(
                name=f"models_{shape_key}",
                shape_key=shape_key,
                partitions_def=spec.partitions_def,
            )
        by_shape[shape_key].specs.append(spec)
    return list(by_shape.values())


def _shape_key_for_spec(spec: dg.AssetSpec) -> str:
    """Compute the stable shape key for a spec's partitions definition.

    Returns a string identifier suitable for use as a multi-asset name
    suffix. ``"unpartitioned"`` when the spec has no partitions def.
    """
    pdef = spec.partitions_def
    if pdef is None:
        return "unpartitioned"
    cls_name = type(pdef).__name__
    # Strip the "PartitionsDefinition" suffix and lowercase
    if cls_name.endswith("PartitionsDefinition"):
        cls_name = cls_name[: -len("PartitionsDefinition")]
    return cls_name.lower() or "custom"
