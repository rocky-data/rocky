"""Tests for ``RockyDagsterTranslator``.

The default translator is deliberately generic — it has no awareness of
any specific source type or component hierarchy. These tests exercise
it against several source-shape families (different source types,
different component layouts, scalar vs list-valued components) to pin
down the generic behavior and ensure future refactors don't silently
break users whose pipelines don't match the example shapes.

Scenario coverage (see ``SCENARIOS`` below):

- ``fivetran_deep`` — 3-level hierarchy {tenant, region, source} with
  all scalar components. A common multi-tenant layout.
- ``fivetran_list_valued_region`` — same shape but with a list-valued
  ``regions`` component, which the translator joins with ``__`` in
  asset keys and skips in tags (list components can't be serialized
  as tag values).
- ``airbyte_flat`` — 2-level flat {environment, connector} hierarchy
  representing an adapter that doesn't model tenants or regions.
- ``manual_single_component`` — smallest valid hierarchy: a single
  {dataset} component. Exercises the degenerate path.
"""

from __future__ import annotations

import dagster as dg
import pytest

from dagster_rocky.translator import RockyDagsterTranslator
from dagster_rocky.types import SourceInfo, TableInfo


def _source() -> SourceInfo:
    """Canonical 3-level hierarchical source. Kept for the legacy
    standalone tests below that assert specific values without going
    through the parametrized matrix."""
    return SourceInfo(
        id="src_001",
        components={"tenant": "acme", "region": "us_west", "source": "shopify"},
        source_type="fivetran",
        last_sync_at=None,
        tables=[
            TableInfo(name="orders", row_count=15000),
            TableInfo(name="payments"),
        ],
    )


# ---------------------------------------------------------------------------
# Parametrized scenarios — the genericity proof
# ---------------------------------------------------------------------------

SCENARIOS = [
    pytest.param(
        SourceInfo(
            id="src_fivetran_deep",
            components={"tenant": "acme", "region": "us_west", "source": "shopify"},
            source_type="fivetran",
            tables=[TableInfo(name="orders", row_count=15000), TableInfo(name="payments")],
        ),
        dg.AssetKey(["fivetran", "acme", "us_west", "shopify", "orders"]),
        "acme",
        {
            "rocky/source_type": "fivetran",
            "rocky/tenant": "acme",
            "rocky/region": "us_west",
            "rocky/source": "shopify",
        },
        id="fivetran_deep",
    ),
    pytest.param(
        SourceInfo(
            id="src_fivetran_list",
            components={
                "tenant": "initech",
                "regions": ["europe", "france"],
                "source": "stripe",
            },
            source_type="fivetran",
            tables=[TableInfo(name="charges")],
        ),
        dg.AssetKey(["fivetran", "initech", "europe__france", "stripe", "charges"]),
        "initech",  # first *string* component wins; the list is skipped
        {
            "rocky/source_type": "fivetran",
            "rocky/tenant": "initech",
            "rocky/source": "stripe",
            # rocky/regions is absent — get_tags skips list-valued components
        },
        id="fivetran_list_valued_region",
    ),
    pytest.param(
        SourceInfo(
            id="src_airbyte_flat",
            components={"environment": "prod", "connector": "stripe"},
            source_type="airbyte",
            tables=[TableInfo(name="charges"), TableInfo(name="customers")],
        ),
        dg.AssetKey(["airbyte", "prod", "stripe", "charges"]),
        "prod",
        {
            "rocky/source_type": "airbyte",
            "rocky/environment": "prod",
            "rocky/connector": "stripe",
        },
        id="airbyte_flat",
    ),
    pytest.param(
        SourceInfo(
            id="src_manual_single",
            components={"dataset": "sales_leads"},
            source_type="manual",
            tables=[TableInfo(name="leads")],
        ),
        dg.AssetKey(["manual", "sales_leads", "leads"]),
        "sales_leads",
        {
            "rocky/source_type": "manual",
            "rocky/dataset": "sales_leads",
        },
        id="manual_single_component",
    ),
]


@pytest.mark.parametrize(
    ("source", "expected_first_key", "expected_group", "expected_tag_subset"),
    SCENARIOS,
)
def test_default_translator_produces_consistent_shapes(
    source: SourceInfo,
    expected_first_key: dg.AssetKey,
    expected_group: str,
    expected_tag_subset: dict[str, str],
) -> None:
    """Default translator produces the expected asset key, group name,
    and tag surface across every scenario — proof that nothing in the
    generic path assumes fivetran or any specific hierarchy shape."""
    t = RockyDagsterTranslator()

    # Asset key for the first table
    assert t.get_asset_key(source, source.tables[0]) == expected_first_key

    # Group name (depends on dict iteration order — see the dedicated
    # test below for the "all components are lists" fallback path)
    assert t.get_group_name(source) == expected_group

    # Tags are a superset of expected. We assert subset rather than
    # equality so the test is robust against future tag additions.
    tags = t.get_tags(source, source.tables[0])
    for key, value in expected_tag_subset.items():
        assert tags.get(key) == value, (
            f"expected tag {key!r}={value!r} but got {tags.get(key)!r} (full tags: {tags})"
        )


# ---------------------------------------------------------------------------
# Edge cases — pinning tests for documented generic behavior
# ---------------------------------------------------------------------------


def test_get_group_name_falls_back_to_source_type_when_all_components_are_lists():
    """When every component is list-valued, there's no natural string
    axis to group by, so ``get_group_name`` falls back to
    ``source.source_type``. This is a rare but documented code path —
    pin it so a future refactor that removes the loop doesn't regress it.
    """
    t = RockyDagsterTranslator()
    src = SourceInfo(
        id="src_only_lists",
        components={"regions": ["us_east", "us_west"], "tags": ["prod", "critical"]},
        source_type="fivetran",
        tables=[TableInfo(name="tbl")],
    )
    # No string components → fall back to source_type
    assert t.get_group_name(src) == "fivetran"
    # Asset key still joins list components with `__`
    assert t.get_asset_key(src, src.tables[0]) == dg.AssetKey(
        ["fivetran", "us_east__us_west", "prod__critical", "tbl"]
    )
    # Tags skip list components — only source_type survives
    tags = t.get_tags(src, src.tables[0])
    assert tags == {"rocky/source_type": "fivetran"}


def test_get_group_name_uses_first_string_component_in_dict_iteration_order():
    """``get_group_name`` default is "first component value that's a
    string, in dict iteration order". Python 3.7+ preserves dict
    insertion order and Pydantic 2 preserves JSON parse order, so the
    "first" is deterministic — but it's determined by the upstream
    producer (Rocky's engine), not by the Dagster side. Users who need
    a different grouping axis should subclass the translator."""
    t = RockyDagsterTranslator()
    src = SourceInfo(
        id="src_order",
        components={"tenant": "hooli", "vertical": "media", "region": "emea"},
        source_type="fivetran",
        tables=[TableInfo(name="tbl")],
    )
    # The first string component ("tenant" → "hooli") wins — not
    # alphabetical ("region" → "emea"), not last-inserted ("region").
    assert t.get_group_name(src) == "hooli"


# ---------------------------------------------------------------------------
# Legacy tests — specific values that don't belong in the parametrized set
# ---------------------------------------------------------------------------


def test_asset_key():
    t = RockyDagsterTranslator()
    src = _source()
    key = t.get_asset_key(src, src.tables[0])
    assert key == dg.AssetKey(["fivetran", "acme", "us_west", "shopify", "orders"])


def test_asset_key_with_list_component():
    t = RockyDagsterTranslator()
    src = SourceInfo(
        id="src_002",
        components={"tenant": "globex", "regions": ["europe", "france"], "source": "stripe"},
        source_type="fivetran",
        tables=[TableInfo(name="tbl")],
    )
    key = t.get_asset_key(src, src.tables[0])
    assert key == dg.AssetKey(["fivetran", "globex", "europe__france", "stripe", "tbl"])


def test_group_name():
    t = RockyDagsterTranslator()
    assert t.get_group_name(_source()) == "acme"


def test_tags():
    t = RockyDagsterTranslator()
    src = _source()
    tags = t.get_tags(src, src.tables[0])
    assert tags["rocky/tenant"] == "acme"
    assert tags["rocky/source"] == "shopify"
    assert tags["rocky/source_type"] == "fivetran"


def test_metadata():
    t = RockyDagsterTranslator()
    src = _source()
    meta = t.get_metadata(src, src.tables[0])
    assert meta["source_id"] == "src_001"
    assert meta["row_count"] == "15000"


def test_metadata_forwards_adapter_namespaced_fields():
    """FR-002: adapter-namespaced metadata keys (``fivetran.service``,
    ``fivetran.custom_reports``, ...) flow into ``AssetSpec.metadata`` so
    downstream logic can branch on connector type without re-parsing
    connector strings."""
    import json

    t = RockyDagsterTranslator()
    src = SourceInfo(
        id="conn_ads",
        components={"tenant": "acme", "source": "fb_ads"},
        source_type="facebook_ads",
        last_sync_at=None,
        tables=[TableInfo(name="ads_insights")],
        metadata={
            "fivetran.service": "facebook_ads",
            "fivetran.connector_id": "conn_ads",
            "fivetran.custom_reports": [
                {"name": "custom_report_revenue", "report_type": "ads_insights"}
            ],
        },
    )
    meta = t.get_metadata(src, src.tables[0])
    # Core fields are unchanged.
    assert meta["source_id"] == "conn_ads"
    # String adapter fields pass through verbatim.
    assert meta["fivetran.service"] == "facebook_ads"
    assert meta["fivetran.connector_id"] == "conn_ads"
    # Structured adapter fields are JSON-encoded so Dagster's
    # ``dict[str, str]`` contract holds; consumers ``json.loads()`` for
    # richer structure.
    reports = json.loads(meta["fivetran.custom_reports"])
    assert reports[0]["name"] == "custom_report_revenue"


def test_metadata_no_opt_in_preserves_legacy_surface():
    """Adapters that haven't opted in to ``SourceInfo.metadata`` must
    produce the same ``get_metadata`` output as before — regression
    guard for backward compat."""
    t = RockyDagsterTranslator()
    src = _source()  # default source has empty metadata
    meta = t.get_metadata(src, src.tables[0])
    # No adapter keys leaked in.
    for key in meta:
        assert "." not in key, f"unexpected namespaced key {key!r}"
