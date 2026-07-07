"""Agent conformance eval harness for the Rocky MCP interface.

The suite drives a scripted agent session (the Claude Code CLI in headless
`--print` mode, by default) against a fresh `rocky mcp` server on a pinned
DuckDB fixture, then scores the session with deterministic assertions that are
pure functions of the captured transcript and the fixture's on-disk state after
the run. See ``engine/evals/README.md`` for the full contract.
"""

from harness.version import HARNESS_VERSION

__all__ = ["HARNESS_VERSION"]
