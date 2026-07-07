"""Parse a Claude Code ``stream-json`` transcript into scored signals.

The transcript is newline-delimited JSON: a `system` init line, alternating
`assistant` / `user` message lines whose `message.content` carries `tool_use`
and `tool_result` blocks, and a final `result` line with usage and status. We
extract the ordered tool-call sequence (the grounding / propose signals) plus
the model usage and terminal status. Parsing is a pure function of the file, so
re-scoring a captured transcript is deterministic.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

#: MCP grounding tools — the reconcile-discipline signal ("look at the data
#: before writing SQL"). Names are ``mcp__<server>__<tool>`` where the server is
#: registered as ``rocky`` in the driver's generated config.
GROUNDING_TOOLS = frozenset(
    {
        "mcp__rocky__sample_rows",
        "mcp__rocky__profile_column",
        "mcp__rocky__inspect_schema",
    }
)
PROPOSE_TOOL = "mcp__rocky__propose"
#: Built-in harness tools that write model files (authoring signal).
FILE_WRITE_TOOLS = frozenset({"Write", "Edit", "MultiEdit", "NotebookEdit"})


@dataclass
class ToolCall:
    index: int
    name: str
    input: dict
    tool_use_id: str = ""


@dataclass
class ToolResult:
    tool_use_id: str
    is_error: bool
    text: str


@dataclass
class ParsedTranscript:
    tool_calls: list[ToolCall] = field(default_factory=list)
    tool_results: dict[str, ToolResult] = field(default_factory=dict)
    model_usage: list[str] = field(default_factory=list)
    result_is_error: bool = False
    num_turns: int | None = None
    lines_parsed: int = 0

    # -- derived signals (pure functions of the parse) --------------------

    def calls_named(self, name: str) -> list[ToolCall]:
        return [c for c in self.tool_calls if c.name == name]

    def grounding_calls(self) -> list[ToolCall]:
        return [c for c in self.tool_calls if c.name in GROUNDING_TOOLS]

    def first_index(self, name: str) -> int | None:
        for call in self.tool_calls:
            if call.name == name:
                return call.index
        return None

    def grounded_before_propose(self) -> bool:
        """True if a grounding tool was called before the first ``propose``.

        Scored against *propose*, not against the first file write: agents
        legitimately draft, then sample, then rewrite, and scoring on first-write
        would false-negative that valid loop. If the agent never proposed, any
        grounding call still counts as grounding (the propose gate is scored
        separately).
        """
        grounding_indices = [c.index for c in self.grounding_calls()]
        if not grounding_indices:
            return False
        propose_index = self.first_index(PROPOSE_TOOL)
        if propose_index is None:
            return True
        return min(grounding_indices) < propose_index

    def proposed_plan_result(self) -> ToolResult | None:
        """The result of the first successful ``propose`` call that returned a
        ``plan_id``, if any. Correlates each propose call to its result by the
        `tool_use_id` recorded during parsing.
        """
        for call in self.calls_named(PROPOSE_TOOL):
            result = self.tool_results.get(call.tool_use_id)
            if result and not result.is_error and '"plan_id"' in result.text:
                return result
        return None


def parse_transcript(path: Path) -> ParsedTranscript:
    parsed = ParsedTranscript()
    if not path.is_file():
        return parsed

    index = 0
    for raw in path.read_text().splitlines():
        raw = raw.strip()
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError:
            continue
        parsed.lines_parsed += 1
        obj_type = obj.get("type")

        if obj_type == "result":
            parsed.result_is_error = bool(obj.get("is_error"))
            parsed.num_turns = obj.get("num_turns")
            usage = obj.get("modelUsage")
            if isinstance(usage, dict):
                parsed.model_usage = sorted(usage.keys())
            continue

        message = obj.get("message")
        if not isinstance(message, dict):
            continue
        content = message.get("content")
        if not isinstance(content, list):
            continue
        for block in content:
            if not isinstance(block, dict):
                continue
            block_type = block.get("type")
            if block_type == "tool_use":
                parsed.tool_calls.append(
                    ToolCall(
                        index=index,
                        name=block.get("name", ""),
                        input=block.get("input") or {},
                        tool_use_id=block.get("id", ""),
                    )
                )
                index += 1
            elif block_type == "tool_result":
                tool_use_id = block.get("tool_use_id", "")
                parsed.tool_results[tool_use_id] = ToolResult(
                    tool_use_id=tool_use_id,
                    is_error=bool(block.get("is_error")),
                    text=_result_text(block.get("content")),
                )
    return parsed


def _result_text(content: object) -> str:
    """Flatten a tool_result content payload to text for substring checks."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if isinstance(block, dict) and block.get("type") == "text":
                parts.append(str(block.get("text", "")))
            elif isinstance(block, str):
                parts.append(block)
        return "\n".join(parts)
    return ""
