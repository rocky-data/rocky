"""Agent driver — drives a scripted session against a fresh `rocky mcp` server.

The default (and only, in v0) driver shells out to the **Claude Code CLI** in
headless `--print` mode. That is the faithful analog of how a human uses Rocky
through an agent (VS Code agent mode spawns the same `rocky mcp` over stdio), and
it means the suite rides the frontier harness rather than reimplementing a tool
loop. The driver is kept behind a small protocol so an Anthropic-SDK API loop
can be swapped in if the CLI dependency ever becomes a problem in CI.

Isolation (must hold for the eval to measure anything real):
  * `--strict-mcp-config` + a generated `.mcp.json` → the agent sees *only* the
    `rocky` MCP server, nothing from the user's global config.
  * `CLAUDE_CONFIG_DIR` is pointed at a fresh empty dir → the agent-under-test
    does not inherit the operator's global CLAUDE.md, memory, or skills, and is
    forced to authenticate from `ANTHROPIC_API_KEY` alone (the CI condition).
  * `--disallowedTools Bash WebSearch WebFetch` → the agent cannot route around
    the MCP grounding tools with a raw `duckdb` shell (which would make the
    grounding measurement meaningless), cannot reach the network, and — since
    `rocky apply` is only reachable via a shell and there is no MCP apply tool —
    *cannot mutate the warehouse*. "No direct mutation" is therefore structural.
"""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

#: Tools the agent is denied. Bash is the load-bearing one (see module docstring).
_DISALLOWED_TOOLS = ("Bash", "WebSearch", "WebFetch")


@dataclass
class AgentRun:
    """The outcome of one scripted agent session."""

    transcript_path: Path
    returncode: int
    timed_out: bool
    wall_s: float
    model_requested: str


class Driver(Protocol):
    def run(
        self,
        project_dir: Path,
        intent: str,
        system_prompt: str,
        model: str,
        timeout_s: int,
    ) -> AgentRun: ...


class ClaudeCliDriver:
    """Drive the Claude Code CLI headlessly against `rocky mcp`."""

    def __init__(self, claude_bin: str, rocky_bin: Path) -> None:
        self._claude_bin = claude_bin
        self._rocky_bin = rocky_bin

    def _write_mcp_config(self, project_dir: Path) -> Path:
        config = {
            "mcpServers": {
                "rocky": {
                    "command": str(self._rocky_bin),
                    "args": ["mcp", "--config", "rocky.toml"],
                }
            }
        }
        path = project_dir / ".eval-mcp.json"
        path.write_text(json.dumps(config))
        return path

    def _neutralized_env(self, config_dir: Path) -> dict[str, str]:
        env = dict(os.environ)
        # Force a clean config surface: no inherited CLAUDE.md/memory/skills, and
        # auth resolves from ANTHROPIC_API_KEY only (verified: works with no
        # OAuth session, which is the CI condition).
        env["CLAUDE_CONFIG_DIR"] = str(config_dir)
        # Belt-and-braces: drop an OAuth token if one is exported, so the key is
        # unambiguously the auth path being exercised.
        env.pop("ANTHROPIC_AUTH_TOKEN", None)
        return env

    def run(
        self,
        project_dir: Path,
        intent: str,
        system_prompt: str,
        model: str,
        timeout_s: int,
    ) -> AgentRun:
        mcp_config = self._write_mcp_config(project_dir)
        config_dir = Path(tempfile.mkdtemp(prefix="rocky-eval-cfg-"))
        transcript_path = project_dir / "transcript.jsonl"

        cmd = [
            self._claude_bin,
            "-p",
            intent,
            "--append-system-prompt",
            system_prompt,
            "--strict-mcp-config",
            "--mcp-config",
            str(mcp_config),
            "--disallowedTools",
            *_DISALLOWED_TOOLS,
            "--permission-mode",
            "bypassPermissions",
            "--output-format",
            "stream-json",
            "--verbose",
            "--model",
            model,
        ]

        start = time.monotonic()
        timed_out = False
        try:
            completed = subprocess.run(
                cmd,
                cwd=project_dir,
                env=self._neutralized_env(config_dir),
                capture_output=True,
                text=True,
                timeout=timeout_s,
                check=False,
            )
            returncode = completed.returncode
            transcript_path.write_text(completed.stdout)
            stderr_tail = completed.stderr[-2000:]
        except subprocess.TimeoutExpired as exc:
            timed_out = True
            returncode = -1
            # Partial stdout captured up to the kill is still worth scoring.
            partial = exc.stdout or ""
            if isinstance(partial, bytes):
                partial = partial.decode("utf-8", "replace")
            transcript_path.write_text(partial)
            stderr_tail = "timed out"
        finally:
            import shutil

            shutil.rmtree(config_dir, ignore_errors=True)

        wall_s = round(time.monotonic() - start, 2)
        # Persist a stderr tail next to the transcript for debugging failed runs.
        (project_dir / "driver-stderr.txt").write_text(stderr_tail)

        return AgentRun(
            transcript_path=transcript_path,
            returncode=returncode,
            timed_out=timed_out,
            wall_s=wall_s,
            model_requested=model,
        )
