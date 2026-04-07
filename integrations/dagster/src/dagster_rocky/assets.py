"""Asset loading from Rocky discover output."""

from __future__ import annotations

import dagster as dg

from .freshness import freshness_policy_from_checks
from .resource import RockyResource
from .translator import RockyDagsterTranslator


def load_rocky_assets(
    rocky: RockyResource,
    translator: RockyDagsterTranslator | None = None,
) -> list[dg.AssetSpec]:
    """Calls `rocky discover` and returns a list of AssetSpec for each enabled table.

    When the discovered pipeline has ``[checks.freshness]`` configured in
    ``rocky.toml``, every returned ``AssetSpec`` is given a matching
    ``FreshnessPolicy`` so the Dagster UI surfaces stale-data badges and the
    declarative-automation freshness conditions trigger correctly.

    Args:
        rocky: The RockyResource to use for discovery.
        translator: Optional translator for custom asset key/tag mapping.

    Returns:
        A list of AssetSpec, one per enabled table across all sources.
    """
    if translator is None:
        translator = RockyDagsterTranslator()

    result = rocky.discover()
    freshness_policy = freshness_policy_from_checks(result.checks)
    specs: list[dg.AssetSpec] = []

    for source in result.sources:
        group = translator.get_group_name(source)
        for table in source.tables:
            specs.append(
                dg.AssetSpec(
                    key=translator.get_asset_key(source, table),
                    group_name=group,
                    tags=translator.get_tags(source, table),
                    metadata=translator.get_metadata(source, table),
                    freshness_policy=freshness_policy,
                )
            )

    return specs
