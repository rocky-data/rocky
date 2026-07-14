#!/usr/bin/env python3
"""Collect bounded, credential-free input for the privileged AI review.

This script runs in the pull-request trust domain. Its output is deliberately
treated as attacker-controlled by ``fetch_ai_review_context.py``; the limits
here keep normal runs economical, while the trusted consumer independently
enforces the security boundary.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any


SHA_RE = re.compile(r"[0-9a-f]{40}")
PROJECT_RELATIVE_PATH = Path(
    "examples/playground/pocs/06-developer-experience/10-pr-preview-and-data-diff"
)
TRUSTED_CONFIG_PATH = Path(__file__).resolve().parents[2] / PROJECT_RELATIVE_PATH / "rocky.toml"
DIFF_LIMIT = 32 * 1024
COMMAND_OUTPUT_LIMIT = 64 * 1024
ERROR_LIMIT = 2 * 1024
COMMAND_TIMEOUT_SECONDS = 120


def require_sha(name: str) -> str:
    value = os.environ.get(name, "")
    if not SHA_RE.fullmatch(value):
        raise SystemExit(f"{name} must be a lowercase 40-character Git SHA")
    return value


def bounded_text(value: bytes | None, limit: int) -> str:
    raw = value or b""
    clipped = raw[:limit]
    text = clipped.decode("utf-8", errors="replace")
    if len(raw) > limit:
        text += f"\n\n...[truncated to {limit} bytes]"
    return text


def run_command(
    command: list[str], *, command_env: dict[str, str], output_limit: int, project: Path
) -> dict[str, Any]:
    try:
        completed = subprocess.run(
            command,
            cwd=project,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            env=command_env,
            timeout=COMMAND_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as error:
        return {
            "status": "failed",
            "content": bounded_text(error.stdout, output_limit),
            "error": (
                f"command timed out after {COMMAND_TIMEOUT_SECONDS}s\n"
                + bounded_text(error.stderr, ERROR_LIMIT)
            ),
        }

    return {
        "status": "ok" if completed.returncode == 0 else "failed",
        "content": bounded_text(completed.stdout, output_limit),
        "error": bounded_text(completed.stderr, ERROR_LIMIT),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository-root", type=Path, required=True)
    args = parser.parse_args(argv)

    base_sha = require_sha("BASE_SHA")
    head_sha = require_sha("HEAD_SHA")
    pr_number_text = os.environ.get("PR_NUMBER", "")
    if not pr_number_text.isdecimal() or int(pr_number_text) < 1:
        raise SystemExit("PR_NUMBER must be a positive integer")

    output = Path(os.environ.get("REVIEW_CONTEXT_PATH", ""))
    if not output.is_absolute():
        raise SystemExit("REVIEW_CONTEXT_PATH must be an absolute runner-temp path")
    repository_root = args.repository_root.resolve()
    project = (repository_root / PROJECT_RELATIVE_PATH).resolve()
    if not project.is_relative_to(repository_root) or not project.is_dir():
        raise SystemExit(f"review project is missing or escaped its checkout: {project}")
    if TRUSTED_CONFIG_PATH.is_symlink() or not TRUSTED_CONFIG_PATH.is_file():
        raise SystemExit("trusted review configuration is missing or unsafe")
    # Candidate rocky.toml is configuration with filesystem/import semantics,
    # not inert model input. Compile only the candidate model files under this
    # immutable DuckDB configuration.
    shutil.copyfile(TRUSTED_CONFIG_PATH, project / "rocky.toml")

    safe_home = output.parent / "home"
    safe_home.mkdir(mode=0o700, parents=True, exist_ok=True)
    safe_home.chmod(0o700)
    command_env = {
        "HOME": str(safe_home),
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "NO_COLOR": "1",
        "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
        "TMPDIR": str(safe_home),
    }

    context = {
        "schema_version": 1,
        "pull_request": {
            "number": int(pr_number_text),
            "base_sha": base_sha,
            "head_sha": head_sha,
        },
        "sections": {
            "diff": run_command(
                ["git", "diff", f"{base_sha}...{head_sha}"],
                command_env=command_env,
                output_limit=DIFF_LIMIT,
                project=project,
            ),
            "compile": run_command(
                ["rocky", "compile", "--models", "models", "--output", "json"],
                command_env=command_env,
                output_limit=COMMAND_OUTPUT_LIMIT,
                project=project,
            ),
            "ci_diff": run_command(
                [
                    "rocky",
                    "ci-diff",
                    base_sha,
                    "--models",
                    "models",
                    "--output",
                    "json",
                ],
                command_env=command_env,
                output_limit=COMMAND_OUTPUT_LIMIT,
                project=project,
            ),
            "lineage": run_command(
                [
                    "rocky",
                    "lineage-diff",
                    base_sha,
                    "--models",
                    "models",
                    "--output",
                    "json",
                ],
                command_env=command_env,
                output_limit=COMMAND_OUTPUT_LIMIT,
                project=project,
            ),
        },
    }

    output.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    output.write_text(json.dumps(context, ensure_ascii=False, separators=(",", ":")))
    output.chmod(0o600)
    print(f"wrote bounded review context to {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
