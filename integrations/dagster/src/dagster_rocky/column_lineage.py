"""Column-level lineage builder bridging Rocky lineage to Dagster primitives.

Rocky's compiler tracks column-level lineage through SQL transformations and
exposes it via ``rocky lineage <model> --column <col>`` (returning a
:class:`ColumnLineageResult`) or ``rocky lineage <model>`` (returning a
:class:`ModelLineageResult` with edges across all columns of a model).

Dagster 1.12+ accepts column lineage as a structured metadata value via
:class:`dagster.TableColumnLineage` attached to ``MaterializeResult.metadata``,
which the asset detail page renders as an interactive column-level
dependency graph.

This module provides one pure-function builder that converts a Rocky
:class:`ModelLineageResult` (model-level shape) into a
:class:`dagster.TableColumnLineage`. Callers wire it into their
materialization metadata via something like::

    metadata["dagster/column_lineage"] = build_column_lineage(
        rocky.lineage(target=model_name),
        translator=my_translator,
    )

Single-column lineage (the ``ColumnLineageResult`` shape) is intentionally
not handled here — its trace is a path through transforms, not a per-column
dependency map, and wouldn't fit ``TableColumnLineage`` cleanly.
"""

from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING

import dagster as dg

if TYPE_CHECKING:
    from .types import ModelLineageResult


def build_column_lineage(
    lineage: ModelLineageResult,
    *,
    model_to_key: dict[str, dg.AssetKey] | None = None,
) -> dg.TableColumnLineage:
    """Build a Dagster ``TableColumnLineage`` from a Rocky model-lineage result.

    For every column on the target model, walks the model's edges and
    collects the (upstream-model, upstream-column) pairs that feed it.
    The output is a ``TableColumnLineage`` whose ``deps_by_column`` map
    has one entry per target column.

    Args:
        lineage: A :class:`ModelLineageResult` from
            ``RockyResource.lineage(target=...)`` (no ``column``
            parameter — the model-level shape).
        model_to_key: Optional mapping of upstream model name → Dagster
            ``AssetKey``. Edges referencing models not in the map fall
            back to ``AssetKey([model_name])`` so the lineage still
            renders, just with the literal model name as the key.

    Returns:
        A :class:`dagster.TableColumnLineage` ready to attach to
        ``MaterializeResult.metadata``.
    """
    deps_by_column: dict[str, list[dg.TableColumnDep]] = defaultdict(list)
    resolver = model_to_key or {}

    for edge in lineage.edges:
        # Skip self-edges (target column = source column on the same
        # model). They're not lineage in the dependency sense.
        if edge.source.model == edge.target.model:
            continue
        target_column = edge.target.column
        upstream_key = resolver.get(edge.source.model) or dg.AssetKey([edge.source.model])
        deps_by_column[target_column].append(
            dg.TableColumnDep(
                asset_key=upstream_key,
                column_name=edge.source.column,
            )
        )

    return dg.TableColumnLineage(deps_by_column=dict(deps_by_column))
