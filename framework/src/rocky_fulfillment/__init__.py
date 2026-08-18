"""rocky-fulfillment — spec-driven fulfillment runtime for Rocky.

Working name; nothing here is published yet. The runtime logic lands in
later work packages. What ships from day one is the extraction seam: the
version pins in :mod:`rocky_fulfillment._seam` that pair this package with
the engine's CLI and MCP surfaces.
"""

from __future__ import annotations

from rocky_fulfillment._seam import (
    MIN_ROCKY_VERSION,
    REQUIRED_MCP_TOOLS,
    SPEC_VERSION,
    McpToolRequirement,
)

__all__ = [
    "MIN_ROCKY_VERSION",
    "REQUIRED_MCP_TOOLS",
    "SPEC_VERSION",
    "McpToolRequirement",
]
