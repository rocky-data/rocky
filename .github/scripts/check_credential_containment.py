#!/usr/bin/env python3
"""Reject pull-request workflows that can expose credentials to mutable code.

The checker intentionally understands only the small GitHub Actions structure
needed by this policy. Unknown credential-bearing PR jobs fail closed instead
of being treated as safe. It has no third-party parser dependency, so the same
trusted checker can run in the repository's minimal policy job.
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
from dataclasses import dataclass
from pathlib import Path


SECRET_RE = re.compile(r"\bsecrets\b", re.IGNORECASE)
ENVIRONMENT_RE = re.compile(r"(?m)^\s*(?:environment|'environment'|\"environment\")\s*:")
JOB_RE = re.compile(
    r"^  (?:(?P<bare>[A-Za-z0-9_-]+)|'(?P<single>[^']+)'|\"(?P<double>[^\"]+)\"):\s*$"
)
PINNED_CHECKOUT_RE = re.compile(r"actions/checkout@[0-9a-f]{40}")
PINNED_REMOTE_ACTION_RE = re.compile(
    r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.\-/]+@[0-9a-f]{40}"
)
CHECKOUT_SOURCE = "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0"
GITHUB_SCRIPT_SOURCE = (
    "actions/github-script@3a2844b7e9c422d3c10d287c895573f7108da1b3"
)
UPLOAD_ARTIFACT_SOURCE = (
    "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a"
)
RUST_TOOLCHAIN_SOURCE = (
    "dtolnay/rust-toolchain@29eef336d9b2848a0b548edc03f92a220660cdb8"
)
RUST_CACHE_SOURCE = (
    "Swatinem/rust-cache@c19371144df3bb44fab255c43d04cbc2ab54d1c4"
)
SETUP_UV_SOURCE = "astral-sh/setup-uv@fac544c07dec837d0ccb6301d7b5580bf5edae39"
SETUP_NODE_SOURCE = (
    "actions/setup-node@48b55a011bda9f5d6aeb4c2d9c7362e8dae4041e"
)
MODEL_SECRET_RE = re.compile(r"\bANTHROPIC_API_KEY\b", re.IGNORECASE)
CANDIDATE_JOBS = {
    ("ai-review.yml", "context"),
    ("ci-security-policy-tests.yml", "policy-tests"),
    ("engine-evals.yml", "eval-contract"),
    ("ci-security-policy.yml", "credential-containment"),
    ("preview.yml", "context"),
}
REQUIRED_JOBS = CANDIDATE_JOBS | {
    ("ai-review.yml", "invalidate-approval"),
    ("ai-review.yml", "ai-review"),
    ("engine-evals-live.yml", "evals"),
    ("preview-comment.yml", "comment"),
}
PRIVILEGED_EVENT_JOBS = {
    "ai-review.yml": {"invalidate-approval", "context", "ai-review"},
    "ci-security-policy.yml": {"credential-containment"},
    "preview-comment.yml": {"comment"},
}
CANDIDATE_CHECKOUT_MAPS = {
    ("ai-review.yml", "context"): (
        {
            "ref": "${{ github.workflow_sha }}",
            "path": "trusted",
            "fetch-depth": "1",
            "persist-credentials": "false",
        },
        {
            "repository": "${{ github.event.pull_request.head.repo.full_name }}",
            "ref": "${{ github.event.pull_request.head.sha }}",
            "path": "candidate",
            "fetch-depth": "0",
            "persist-credentials": "false",
            "allow-unsafe-pr-checkout": "true",
        },
    ),
    ("engine-evals.yml", "eval-contract"): (
        {
            "persist-credentials": "false",
        },
    ),
    ("ci-security-policy-tests.yml", "policy-tests"): (
        {"persist-credentials": "false"},
    ),
    ("preview.yml", "context"): (
        {
            "fetch-depth": "0",
            "persist-credentials": "false",
        },
    ),
}
UNPRIVILEGED_LOCAL_ACTIONS = {
    ("preview.yml", "context"): {"./.github/actions/rocky-preview"},
}
PROTECTED_CANDIDATE_WORKFLOWS = {name for name, _ in CANDIDATE_JOBS}
TRUSTED_PRIVILEGED_JOBS = {
    ("ai-review.yml", "invalidate-approval"),
    ("ai-review.yml", "ai-review"),
    ("ci-security-policy.yml", "credential-containment"),
    ("preview-comment.yml", "comment"),
}
REQUIRED_WORKFLOWS = {
    "ai-review.yml",
    "ci-security-policy.yml",
    "ci-security-policy-tests.yml",
    "engine-evals.yml",
    "engine-evals-live.yml",
    "preview-comment.yml",
    "preview.yml",
}
FROZEN_TRUST_ROOTS = (
    ".github/actions/rocky-ai-review/action.yml",
    ".github/scripts/check_credential_containment.py",
    ".github/scripts/collect_ai_review_context.py",
    ".github/scripts/fetch_ai_review_context.py",
    ".github/scripts/fetch_policy_candidate.py",
    ".github/scripts/preview_comment.py",
    ".github/scripts/verify_pr_approval.py",
    ".github/tests/test_credential_containment.py",
)
TRUSTED_SCRIPT_ENTRIES = frozenset(
    {
        "check_credential_containment.py",
        "collect_ai_review_context.py",
        "fetch_ai_review_context.py",
        "fetch_policy_candidate.py",
        "preview_comment.py",
        "verify_pr_approval.py",
    }
)
AI_CONTEXT_RUN_SCRIPT_HASHES = (
    "78672ec07a7cd17336620941cf25b0c67685361c3d2c5626cd16d670319bdab8",
    "1acde9916b033fbff68dcbfccd32cb549e1d45a14e4e7e31257effa34336fe26",
    "b591eb8aa9539063f1f4937178b9705853c32edf532c81a68245c1c1738a464c",
    "8bfc0d6008dc300268cf8a9d52f996a9fcea008d3d70aa4bc74675bc59ab65ad",
)
LIVE_EVAL_RUN_SCRIPT_HASHES = (
    "bcc1fc2a2f26a4c70d47bf952209bc567c57169b787520e0a320c72a59354cc1",
    "768174df2e1667e0ab05237911fc6cd69e10e01d6784f4788e78e2f91700a355",
    "70ae26327016cb662cf067d773701168c860980f5d6786ba9867c29f2f27bc16",
    "24c3c51fdfbc7d24831cf51d002af269b21939bef8038ebd539e617f3e3bf2bf",
    "cfe6045336ed47c1aaa4927d8c335b05aaf079f5d65cf10cf5cfa460133d856b",
    "ddca91f8195bd32178ea6df55db0651c0f30090ce0be1cf695f4b1f19a47ba76",
    "fd05fce7fc7ea59f92aff062ccbf6156c06a4b4ddcdcadbdcf76bdbe3f645af5",
)
TOKEN_CONTEXT_RE = re.compile(
    r"\bgithub\s*(?:\.\s*token|\[\s*['\"]token['\"]\s*\])"
    r"|\bsecrets\s*(?:\.\s*GITHUB_TOKEN|\[\s*['\"]GITHUB_TOKEN['\"]\s*\])",
    re.IGNORECASE,
)
WHOLE_GITHUB_CONTEXT_RE = re.compile(r"\btoJSON\s*\(\s*github\s*\)", re.IGNORECASE)
COMPUTED_GITHUB_CONTEXT_RE = re.compile(
    r"\bgithub\s*\[|\bgithub\s*\.\s*\*|\$\{\{\s*github\s*\}\}",
    re.IGNORECASE,
)
YAML_CHARACTER_ESCAPE_RE = re.compile(
    r"\\(?:x[0-9A-Fa-f]{2}|u[0-9A-Fa-f]{4}|U[0-9A-Fa-f]{8})"
)
YAML_QUOTED_KEY_RE = re.compile(
    r"(?m)^[ \t]*(?:-[ \t]+)?['\"][^'\"\r\n]+['\"][ \t]*:"
)
YAML_EXPLICIT_KEY_RE = re.compile(r"(?m)^[ \t]*(?:-[ \t]+)?[?][ \t]+")
YAML_KEY_SPACING_RE = re.compile(
    r"(?m)^[ \t]*(?:-[ \t]+)?[A-Za-z0-9_.-]+[ \t]+:"
)
YAML_FLOW_MAPPING_SEQUENCE_RE = re.compile(r"(?m)^[ \t]*-[ \t]*\{")
YAML_INLINE_STEPS_RE = re.compile(r"(?m)^[ \t]*steps:[ \t]*\S")
YAML_ANCHOR_ALIAS_RE = re.compile(
    r"(?:^|[\s\[\]{},:])[&*](?![&*])[^\s\[\]{},]+"
)
YAML_TAG_RE = re.compile(
    r"(?m)(?:^\s*|:\s+|[\[,]\s*|-\s+)!(?:<[^>]+>|[^\s\[\]{},]+)"
)
BLOCK_SCALAR_KEY_RE = re.compile(
    r"^(?P<indent>\s*)(?:-\s+)?"
    r"(?:[A-Za-z0-9_.-]+|'[^']+'|\"[^\"]+\")\s*:\s*[|>][0-9+-]*\s*(?:#.*)?$"
)


def _yaml_structural_text(text: str) -> str:
    """Return YAML structure while omitting comments and block-scalar bodies."""

    structural: list[str] = []
    scalar_indent: int | None = None
    for line in text.splitlines():
        indent = len(line) - len(line.lstrip())
        if scalar_indent is not None:
            if not line.strip() or indent > scalar_indent:
                continue
            scalar_indent = None
        if line.lstrip().startswith("#"):
            continue
        structural.append(line)
        match = BLOCK_SCALAR_KEY_RE.fullmatch(line)
        if match:
            scalar_indent = len(match.group("indent"))
    return "\n".join(structural)


@dataclass(frozen=True)
class Workflow:
    path: Path
    text: str

    @property
    def executable_text(self) -> str:
        return "\n".join(
            line for line in self.text.splitlines() if not line.lstrip().startswith("#")
        )

    @property
    def structural_text(self) -> str:
        return _yaml_structural_text(self.text)

    def canonical_event_names(self) -> set[str] | None:
        """Return events only for the policy's strict block-mapping grammar.

        GitHub accepts many equivalent YAML spellings. Supporting a subset is
        deliberate: an unfamiliar trigger representation must fail closed
        before the checker decides whether candidate code has PR credentials.
        """

        lines = self.text.splitlines()
        on_indices = [index for index, line in enumerate(lines) if line == "on:"]
        if len(on_indices) != 1:
            return None

        events: set[str] = set()
        for line in lines[on_indices[0] + 1 :]:
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            indent = len(line) - len(line.lstrip())
            if indent == 0:
                break
            if indent == 2:
                match = re.fullmatch(r"  ([A-Za-z0-9_-]+):", line)
                if match is None or match.group(1) in events:
                    return None
                events.add(match.group(1))
            elif indent < 4:
                return None
        return events or None

    def event_block(self, event: str) -> str | None:
        lines = self.text.splitlines()
        for index, line in enumerate(lines):
            match = re.fullmatch(r"(?:on|'on'|\"on\")\s*:\s*(.*)", line)
            if not match:
                continue
            inline = match.group(1)
            if inline:
                event_pattern = rf"(?<![A-Za-z0-9_]){re.escape(event)}(?![A-Za-z0-9_])"
                if inline in {"|", "|-", ">", ">-"}:
                    scalar: list[str] = []
                    for candidate in lines[index + 1 :]:
                        if candidate and not candidate[0].isspace():
                            break
                        scalar.append(candidate)
                    rendered = "\n".join(scalar)
                    return rendered if re.search(event_pattern, rendered) else None
                return inline if re.search(event_pattern, inline) else None

            block: list[str] = []
            for candidate in lines[index + 1 :]:
                if candidate and not candidate[0].isspace() and not candidate.startswith("#"):
                    break
                block.append(candidate)
            for event_index, candidate in enumerate(block):
                if re.fullmatch(
                    rf"  -\s*(?:{re.escape(event)}|'{re.escape(event)}'|\"{re.escape(event)}\")\s*",
                    candidate,
                ):
                    return candidate
                if not re.fullmatch(
                    rf"  (?:{re.escape(event)}|'{re.escape(event)}'|\"{re.escape(event)}\"):\s*",
                    candidate,
                ):
                    continue
                event_lines = [candidate]
                for nested in block[event_index + 1 :]:
                    if re.match(r"^  [A-Za-z0-9_-]+:\s*", nested):
                        break
                    event_lines.append(nested)
                return "\n".join(event_lines)
            return None
        return None

    def jobs(self) -> dict[str, str]:
        lines = self.text.splitlines()
        try:
            jobs_index = lines.index("jobs:")
        except ValueError:
            return {}

        jobs: dict[str, list[str]] = {}
        current: str | None = None
        for line in lines[jobs_index + 1 :]:
            if line and not line[0].isspace() and not line.startswith("#"):
                break
            match = JOB_RE.fullmatch(line)
            if match:
                current = match.group("bare") or match.group("single") or match.group("double")
                jobs[current] = [line]
            elif re.match(r"^  \S[^:]*:", line) and not line.lstrip().startswith("#"):
                current = f"<unparsed:{line.strip()}>"
                jobs[current] = [line]
            elif current is not None:
                jobs[current].append(line)
        return {name: "\n".join(block) for name, block in jobs.items()}


def _is_credential_job(job: str) -> bool:
    executable = "\n".join(
        line for line in job.splitlines() if not line.lstrip().startswith("#")
    )
    return bool(SECRET_RE.search(executable) or ENVIRONMENT_RE.search(executable))


def _key_block(text: str, key: str, *, indent: int) -> str | None:
    lines = text.splitlines()
    prefix = " " * indent
    rendered_key = rf"(?:{re.escape(key)}|'{re.escape(key)}'|\"{re.escape(key)}\")"
    pattern = re.compile(rf"^{re.escape(prefix)}{rendered_key}\s*:\s*(.*)$")
    for index, line in enumerate(lines):
        if not pattern.fullmatch(line):
            continue
        block = [line]
        for nested in lines[index + 1 :]:
            nested_indent = len(nested) - len(nested.lstrip())
            if nested.strip() and nested_indent <= indent:
                break
            block.append(nested)
        return "\n".join(block)
    return None


def _scalar_mapping(text: str, key: str, *, indent: int) -> dict[str, str] | None:
    """Parse the deliberately simple scalar maps used by trusted jobs.

    Returning ``None`` for aliases, flow maps, duplicate keys, block values,
    or unexpected indentation makes the privileged-job validators fail closed.
    """

    block = _key_block(text, key, indent=indent)
    if block is None:
        return None
    lines = block.splitlines()
    if not re.fullmatch(rf"{' ' * indent}{re.escape(key)}:\s*", lines[0]):
        return None
    entries: dict[str, str] = {}
    child_indent = " " * (indent + 2)
    entry_re = re.compile(
        rf"^{re.escape(child_indent)}"
        r"(?:(?P<bare>[A-Za-z0-9_-]+)|'(?P<single>[^']+)'|\"(?P<double>[^\"]+)\")"
        r":\s*(?P<value>\S.*?)\s*$"
    )
    for line in lines[1:]:
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        match = entry_re.fullmatch(line)
        if not match:
            return None
        entry = match.group("bare") or match.group("single") or match.group("double")
        if entry in entries:
            return None
        entries[entry] = match.group("value")
    return entries


def _mapping_keys(text: str, key: str, *, indent: int) -> list[str] | None:
    block = _key_block(text, key, indent=indent)
    if block is None:
        return None
    lines = block.splitlines()
    if not re.fullmatch(rf"{' ' * indent}{re.escape(key)}:\s*", lines[0]):
        return None
    child_indent = " " * (indent + 2)
    entry_re = re.compile(rf"^{re.escape(child_indent)}([A-Za-z0-9_-]+):(?:\s.*)?$")
    entries: list[str] = []
    for line in lines[1:]:
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        current_indent = len(line) - len(line.lstrip())
        if current_indent > indent + 2:
            continue
        match = entry_re.fullmatch(line)
        if not match or match.group(1) in entries:
            return None
        entries.append(match.group(1))
    return entries


def _permission_state(text: str, *, indent: int) -> str:
    """Classify a canonical permissions declaration as read-only/write/invalid."""

    block = _key_block(text, "permissions", indent=indent)
    if block is None:
        return "missing"
    lines = block.splitlines()
    prefix = " " * indent
    inline = re.fullmatch(rf"{re.escape(prefix)}permissions:\s*(.*?)\s*", lines[0])
    if inline is None:
        return "invalid"
    value = inline.group(1)
    if value in {"{}", "read-all"}:
        return "read"
    if value == "write-all":
        return "write"
    if value:
        return "invalid"
    mapping = _scalar_mapping(text, "permissions", indent=indent)
    if mapping is None or not mapping:
        return "invalid"
    state = "read"
    for permission in mapping.values():
        normalized = permission.strip("'\"")
        if normalized == "write":
            state = "write"
        elif normalized not in {"read", "none"}:
            return "invalid"
    return state


def _step_blocks(job: str) -> list[str]:
    steps: list[list[str]] = []
    current: list[str] | None = None
    for line in job.splitlines():
        if re.match(r"^      - ", line):
            current = [line]
            steps.append(current)
        elif current is not None:
            current.append(line)
    return ["\n".join(step) for step in steps]


def _step_action_source(step: str) -> str | None:
    sources: list[str] = []
    for index, line in enumerate(step.splitlines()):
        pattern = r"^      -\s+uses:\s*([^\s#]+)" if index == 0 else (
            r"^        uses:\s*([^\s#]+)"
        )
        match = re.match(pattern, line)
        if match:
            sources.append(match.group(1).strip("'\""))
    return sources[0] if len(sources) == 1 else None


def _uses_sources(job: str) -> list[str]:
    return [
        source
        for step in _step_blocks(job)
        if (source := _step_action_source(step)) is not None
    ]


def _step_name(step: str) -> str | None:
    first = step.splitlines()[0] if step else ""
    match = re.fullmatch(r"\s+-\s+name:\s*(.*?)\s*", first)
    return match.group(1).strip("'\"") if match else None


def _job_keys(job: str) -> list[str]:
    keys: list[str] = []
    for line in job.splitlines()[1:]:
        match = re.match(r"^    ([A-Za-z0-9_-]+)\s*:", line)
        if match:
            keys.append(match.group(1))
    return keys


def _step_keys(step: str) -> list[str] | None:
    keys: list[str] = []
    for index, line in enumerate(step.splitlines()):
        if index == 0:
            match = re.fullmatch(
                r"      -\s+(?:(?P<bare>[A-Za-z0-9_-]+)|"
                r"'(?P<single>[^']+)'|\"(?P<double>[^\"]+)\"):\s*.*",
                line,
            )
        else:
            match = re.fullmatch(
                r"        (?:(?P<bare>[A-Za-z0-9_-]+)|"
                r"'(?P<single>[^']+)'|\"(?P<double>[^\"]+)\"):\s*.*",
                line,
            )
        if match:
            keys.append(
                match.group("bare") or match.group("single") or match.group("double")
            )
        elif index == 0:
            return None
    return keys


def _block_scalar(text: str, key: str, *, indent: int) -> str | None:
    lines = text.splitlines()
    pattern = re.compile(rf"^{' ' * indent}{re.escape(key)}:\s*([|>](?:-)?)\s*$")
    for index, line in enumerate(lines):
        if not pattern.fullmatch(line):
            continue
        body: list[str] = []
        for nested in lines[index + 1 :]:
            if nested.strip() and len(nested) - len(nested.lstrip()) <= indent:
                break
            body.append(nested)
        nonempty_indents = [
            len(item) - len(item.lstrip()) for item in body if item.strip()
        ]
        trim = min(nonempty_indents, default=indent + 2)
        return "\n".join(item[trim:] for item in body).strip()
    return None


def _run_scripts(job: str) -> list[str]:
    lines = job.splitlines()
    scripts: list[str] = []
    for index, line in enumerate(lines):
        match = re.match(
            r"^(\s+)(?:-\s+)?(?:run|'run'|\"run\"):\s*(.*?)\s*$", line
        )
        if not match:
            continue
        indent = len(match.group(1))
        scalar = match.group(2)
        if scalar not in {"|", "|-", ">", ">-"}:
            scripts.append(scalar)
            continue

        body: list[str] = []
        for nested in lines[index + 1 :]:
            if nested.strip() and len(nested) - len(nested.lstrip()) <= indent:
                break
            body.append(nested)
        nonempty_indents = [
            len(item) - len(item.lstrip()) for item in body if item.strip()
        ]
        trim = min(nonempty_indents, default=indent + 2)
        scripts.append("\n".join(item[trim:] for item in body).strip())
    return scripts


def _check_ai_review(workflow: Workflow, job: str) -> list[str]:
    violations: list[str] = []
    top_level_keys = re.findall(
        r"(?m)^([A-Za-z0-9_-]+)\s*:", workflow.structural_text
    )
    if top_level_keys != ["name", "on", "permissions", "concurrency", "jobs"]:
        violations.append("AI review workflow keys are not exact")
    if _permission_state(workflow.text, indent=0) != "read" or not re.search(
        r"(?m)^permissions:\s*\{\}\s*$", workflow.text
    ):
        violations.append("AI review workflow permissions are not exact")
    if _scalar_mapping(workflow.text, "concurrency", indent=0) != {
        "group": "rocky-ai-review-${{ github.event.pull_request.number }}",
        "cancel-in-progress": "true",
    }:
        violations.append("AI review concurrency is not exact")
    if _job_keys(job) != [
        "name",
        "needs",
        "runs-on",
        "environment",
        "permissions",
        "steps",
    ]:
        violations.append("AI review credential job keys are not exact")
    target = workflow.event_block("pull_request_target")
    normalized_target = "\n".join(
        line
        for line in (target or "").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    )
    if normalized_target != (
        "  pull_request_target:\n"
        "    types: [labeled, synchronize]\n"
        "    branches: [main]"
    ) or workflow.event_block("pull_request") is not None:
        violations.append("AI review must use pull_request_target only")
    on_block = _key_block(workflow.text, "on", indent=0) or ""
    if set(re.findall(r"(?m)^  ([A-Za-z0-9_-]+):", on_block)) != {
        "pull_request_target"
    }:
        violations.append("AI review event set is not exact")
    if not re.search(
        r"(?m)^    environment:\s*\n      name:\s*credentialed-pr-review\s*$",
        job,
    ):
        violations.append("AI review must use credentialed-pr-review")
    if not re.search(r"(?m)^    runs-on:\s*ubuntu-latest\s*$", job):
        violations.append("AI review credential job must use the isolated hosted runner")
    if not re.search(r"(?m)^    needs:\s*context\s*$", job):
        violations.append("AI review credential job must depend on context")
    if _scalar_mapping(job, "permissions", indent=4) != {
        "actions": "read",
        "contents": "read",
        "pull-requests": "write",
    }:
        violations.append("AI review credential job permissions are not exact")
    for forbidden in (
        r"(?m)^    (?:container|services|defaults|env):",
        r"(?m)^\s+shell:",
        r"(?m)^\s+working-directory:",
        r"(?m)^\s+repository:",
    ):
        if re.search(forbidden, job):
            violations.append("AI review credential job changes its execution context")

    executable = "\n".join(
        line for line in job.splitlines() if not line.lstrip().startswith("#")
    )
    expected_secret = "${{ secrets.ANTHROPIC_API_KEY }}"
    if executable.count(expected_secret) != 1:
        violations.append("AI review must reference the model key exactly once")
    if SECRET_RE.search(executable.replace(expected_secret, "")):
        violations.append("AI review credential job references an unexpected secret")
    expected_token = "${{ github.token }}"
    if executable.count(expected_token) != 2 or TOKEN_CONTEXT_RE.search(
        executable.replace(expected_token, "")
    ) or WHOLE_GITHUB_CONTEXT_RE.search(executable) or COMPUTED_GITHUB_CONTEXT_RE.search(
        executable
    ):
        violations.append("AI review credential job token references are not exact")

    sources = _uses_sources(job)
    allowed_local_action = "./trusted/.github/actions/rocky-ai-review"
    if sources != [CHECKOUT_SOURCE, allowed_local_action]:
        violations.append("AI review executes a non-allow-listed action source")

    steps = _step_blocks(job)
    expected_names = [
        "Check out trusted review tooling",
        "Fetch and validate current-run review context",
        "Run Rocky AI review",
    ]
    if [_step_name(step) for step in steps] != expected_names:
        violations.append("AI review credential job steps are not exact")
    if len(steps) == 3:
        checkout_mapping = _scalar_mapping(steps[0], "with", indent=8)
        if checkout_mapping is None or checkout_mapping.get("ref") != (
            "${{ github.workflow_sha }}"
        ):
            violations.append("AI review trusted checkout must use github.workflow_sha")
        if _step_keys(steps[0]) != ["name", "uses", "with"] or checkout_mapping != {
            "ref": "${{ github.workflow_sha }}",
            "path": "trusted",
            "fetch-depth": "1",
            "persist-credentials": "false",
        }:
            violations.append("AI review trusted checkout is not isolated")
        if _step_keys(steps[1]) != ["name", "env", "run"] or _scalar_mapping(
            steps[1], "env", indent=8
        ) != {
            "GH_TOKEN": "${{ github.token }}",
            "APPROVAL_LABEL": "ai-review",
            "APPROVED_BASE_REF": "main",
            "APPROVED_BASE_REPOSITORY": "${{ github.repository }}",
            "APPROVED_BASE_SHA": "${{ github.event.pull_request.base.sha }}",
            "APPROVED_HEAD_REPOSITORY": "${{ github.event.pull_request.head.repo.full_name }}",
            "APPROVED_HEAD_SHA": "${{ github.event.pull_request.head.sha }}",
            "APPROVER_LOGIN": "${{ github.actor }}",
            "CONTEXT_RUN_ID": "${{ github.run_id }}",
            "PR_NUMBER": "${{ github.event.pull_request.number }}",
            "REVIEW_CONTEXT_DIR": "${{ runner.temp }}/rocky-ai-review-context",
        }:
            violations.append("AI review context-fetch environment is not exact")
        if _step_keys(steps[2]) != ["name", "uses", "with"] or _scalar_mapping(
            steps[2], "with", indent=8
        ) != {
            "approved_base_ref": "main",
            "approved_base_repository": "${{ github.repository }}",
            "approved_base_sha": "${{ github.event.pull_request.base.sha }}",
            "approved_head_repository": "${{ github.event.pull_request.head.repo.full_name }}",
            "approved_head_sha": "${{ github.event.pull_request.head.sha }}",
            "base_ref": "${{ github.event.pull_request.base.sha }}",
            "context_directory": "${{ runner.temp }}/rocky-ai-review-context",
            "pull_request_number": "${{ github.event.pull_request.number }}",
            "anthropic_api_key": expected_secret,
            "github_token": "${{ github.token }}",
        }:
            violations.append("AI review trusted action inputs are not exact")

    expected_script = (
        "python3 trusted/.github/scripts/fetch_ai_review_context.py \\\n"
        '  --output-directory "$REVIEW_CONTEXT_DIR"'
    )
    scripts = _run_scripts(job)
    if scripts != [expected_script]:
        violations.append("AI review credential job contains a non-allow-listed run script")
    return violations


def _check_ai_context(job: str) -> list[str]:
    violations: list[str] = []
    if _job_keys(job) != ["name", "if", "runs-on", "permissions", "steps"]:
        violations.append("AI context job keys are not exact")
    expected_condition = """\
    if: >-
      github.event.action == 'labeled' &&
      github.event.label.name == 'ai-review'"""
    if _key_block(job, "if", indent=4) != expected_condition:
        violations.append("AI context condition is not exact")
    if not re.search(r"(?m)^    runs-on:\s*ubuntu-latest\s*$", job):
        violations.append("AI context must use the hosted runner")
    if _scalar_mapping(job, "permissions", indent=4) != {"contents": "read"}:
        violations.append("AI context permissions are not exact")
    for forbidden in (
        r"(?m)^    (?:container|services|defaults|env|environment):",
        r"(?m)^\s+shell:",
        r"(?m)^\s+working-directory:",
    ):
        if re.search(forbidden, job):
            violations.append("AI context changes its execution context")
    steps = _step_blocks(job)
    if [_step_name(step) for step in steps] != [
        "Check out trusted context tooling",
        "Check out exact candidate head in the unprivileged job",
        "Fetch exact base commit into candidate repository",
        "Reject candidate filesystem indirection",
        "Install released Rocky binary with trusted installer",
        "Collect bounded context with trusted collector",
        "Upload untrusted context artifact",
    ]:
        violations.append("AI context steps are not exact")
        return violations
    if _uses_sources(job) != [CHECKOUT_SOURCE, CHECKOUT_SOURCE, UPLOAD_ARTIFACT_SOURCE]:
        violations.append("AI context action sources are not exact")
    expected_keys = [
        ["name", "uses", "with"],
        ["name", "uses", "with"],
        ["name", "env", "run"],
        ["name", "run"],
        ["name", "env", "run"],
        ["name", "env", "run"],
        ["name", "uses", "with"],
    ]
    if [_step_keys(step) for step in steps] != expected_keys:
        violations.append("AI context step shapes are not exact")
    if _scalar_mapping(steps[0], "with", indent=8) != CANDIDATE_CHECKOUT_MAPS[
        ("ai-review.yml", "context")
    ][0]:
        violations.append("AI context trusted checkout is not exact")
    if _scalar_mapping(steps[1], "with", indent=8) != CANDIDATE_CHECKOUT_MAPS[
        ("ai-review.yml", "context")
    ][1]:
        violations.append("AI context candidate checkout is not exact")
    if _scalar_mapping(steps[2], "env", indent=8) != {
        "BASE_REPOSITORY": "${{ github.repository }}",
        "BASE_SHA": "${{ github.event.pull_request.base.sha }}",
    }:
        violations.append("AI context base-fetch environment is not exact")
    if _scalar_mapping(steps[4], "env", indent=8) != {
        "ROCKY_INSTALL_DIR": "${{ runner.temp }}/rocky-bin"
    }:
        violations.append("AI context installer environment is not exact")
    if _scalar_mapping(steps[5], "env", indent=8) != {
        "BASE_SHA": "${{ github.event.pull_request.base.sha }}",
        "HEAD_SHA": "${{ github.event.pull_request.head.sha }}",
        "PR_NUMBER": "${{ github.event.pull_request.number }}",
        "REVIEW_CONTEXT_PATH": "${{ runner.temp }}/rocky-ai-review-context/context.json",
    }:
        violations.append("AI context collector environment is not exact")
    if _scalar_mapping(steps[6], "with", indent=8) != {
        "name": "rocky-ai-review-context-${{ github.event.pull_request.head.sha }}",
        "path": "${{ runner.temp }}/rocky-ai-review-context/context.json",
        "if-no-files-found": "error",
        "retention-days": "1",
    }:
        violations.append("AI context artifact upload is not exact")
    script_hashes = tuple(
        hashlib.sha256(script.encode()).hexdigest() for script in _run_scripts(job)
    )
    if script_hashes != AI_CONTEXT_RUN_SCRIPT_HASHES:
        violations.append("AI context run scripts are not exact")
    return violations


def _check_invalidate_approval(job: str) -> list[str]:
    violations: list[str] = []
    if _job_keys(job) != ["name", "if", "runs-on", "permissions", "steps"]:
        violations.append("approval invalidation job keys are not exact")
    expected_condition = """\
    if: >-
      github.event.action == 'synchronize' &&
      contains(github.event.pull_request.labels.*.name, 'ai-review')"""
    if _key_block(job, "if", indent=4) != expected_condition:
        violations.append("approval invalidation condition is not exact")
    if not re.search(r"(?m)^    runs-on:\s*ubuntu-latest\s*$", job):
        violations.append("approval invalidation must use the hosted runner")
    if _scalar_mapping(job, "permissions", indent=4) != {"issues": "write"}:
        violations.append("approval invalidation permissions are not exact")
    for forbidden in (
        r"(?m)^    (?:container|services|defaults|env|environment):",
        r"(?m)^\s+shell:",
        r"(?m)^\s+working-directory:",
        r"(?m)^\s+repository:",
    ):
        if re.search(forbidden, job):
            violations.append("approval invalidation changes its execution context")
    steps = _step_blocks(job)
    if [_step_name(step) for step in steps] != [
        "Remove approval label after a head change"
    ]:
        violations.append("approval invalidation steps are not exact")
        return violations
    step = steps[0]
    if _step_keys(step) != ["name", "uses", "with"]:
        violations.append("approval invalidation step shape is not exact")
    if _mapping_keys(step, "with", indent=8) != ["script"]:
        violations.append("approval invalidation inputs are not exact")
    if _uses_sources(step) != [GITHUB_SCRIPT_SOURCE]:
        violations.append("approval invalidation action source is not exact")
    executable = "\n".join(
        line for line in job.splitlines() if not line.lstrip().startswith("#")
    )
    if (
        SECRET_RE.search(executable)
        or TOKEN_CONTEXT_RE.search(executable)
        or WHOLE_GITHUB_CONTEXT_RE.search(executable)
        or COMPUTED_GITHUB_CONTEXT_RE.search(executable)
    ):
        violations.append("approval invalidation consumes an unexpected credential")
    expected_script = """\
const { owner, repo } = context.repo;
const issue_number = context.payload.pull_request.number;
try {
  await github.rest.issues.removeLabel({
    owner,
    repo,
    issue_number,
    name: 'ai-review',
  });
} catch (error) {
  if (error.status !== 404) throw error;
  core.info('ai-review label was already absent');
}"""
    if _block_scalar(step, "script", indent=10) != expected_script:
        violations.append("approval invalidation script is not exact")
    return violations


def _check_preview_producer(workflow: Workflow, job: str) -> list[str]:
    violations: list[str] = []
    top_level_keys = re.findall(r"(?m)^([A-Za-z0-9_-]+)\s*:", workflow.structural_text)
    if top_level_keys != ["name", "on", "permissions", "concurrency", "jobs"]:
        violations.append("preview producer workflow keys are not exact")
    expected_trigger = """\
  pull_request:
    types: [opened, synchronize, reopened]
    branches: [main]
    paths:
      - 'examples/playground/pocs/06-developer-experience/10-pr-preview-and-data-diff/models/**'
      - '.github/actions/rocky-preview/**'
      - '.github/scripts/preview_comment.py'
      - '.github/workflows/preview.yml'"""
    if (workflow.event_block("pull_request") or "").strip() != expected_trigger.strip():
        violations.append("preview producer trigger is not exact")
    if workflow.event_block("pull_request_target") is not None:
        violations.append("preview producer must not use pull_request_target")
    on_block = _key_block(workflow.text, "on", indent=0) or ""
    if set(re.findall(r"(?m)^  ([A-Za-z0-9_-]+):", on_block)) != {"pull_request"}:
        violations.append("preview producer event set is not exact")
    if _scalar_mapping(workflow.text, "permissions", indent=0) != {"contents": "read"}:
        violations.append("preview producer permissions are not exact")
    if _job_keys(job) != ["name", "runs-on", "steps"]:
        violations.append("preview producer job keys are not exact")
    if not re.search(r"(?m)^    runs-on:\s*ubuntu-latest\s*$", job):
        violations.append("preview producer must use the hosted runner")
    for forbidden in (
        r"(?m)^    (?:container|services|defaults|env|environment):",
        r"(?m)^\s+shell:",
        r"(?m)^\s+working-directory:",
        r"(?m)^\s+repository:",
    ):
        if re.search(forbidden, job):
            violations.append("preview producer changes its execution context")
    steps = _step_blocks(job)
    if [_step_name(step) for step in steps] != [
        "Check out pull-request revision",
        "Render Rocky preview",
        "Stage bounded preview artifact",
        "Upload bounded preview artifact",
    ]:
        violations.append("preview producer steps are not exact")
        return violations
    if _uses_sources(job) != [
        CHECKOUT_SOURCE,
        "./.github/actions/rocky-preview",
        UPLOAD_ARTIFACT_SOURCE,
    ]:
        violations.append("preview producer action sources are not exact")
    if _step_keys(steps[0]) != ["name", "uses", "with"] or _scalar_mapping(
        steps[0], "with", indent=8
    ) != {
        "fetch-depth": "0",
        "persist-credentials": "false",
    }:
        violations.append("preview producer checkout is not exact")
    if _step_keys(steps[1]) != ["name", "id", "uses", "with"] or _scalar_mapping(
        steps[1], "with", indent=8
    ) != {
        "base_ref": "${{ github.event.pull_request.base.sha }}",
        "branch_name": "${{ github.event.pull_request.head.ref }}",
        "working_directory": (
            "examples/playground/pocs/06-developer-experience/"
            "10-pr-preview-and-data-diff"
        ),
        "models_dir": "models",
    }:
        violations.append("preview producer action inputs are not exact")
    if _step_keys(steps[2]) != ["name", "env", "run"] or _scalar_mapping(
        steps[2], "env", indent=8
    ) != {
        "BASE_REPOSITORY": "${{ github.repository }}",
        "BASE_SHA": "${{ github.event.pull_request.base.sha }}",
        "BODY_PATH": "${{ steps.preview.outputs.body_path }}",
        "HEAD_REPOSITORY": "${{ github.event.pull_request.head.repo.full_name }}",
        "HEAD_SHA": "${{ github.event.pull_request.head.sha }}",
        "PR_NUMBER": "${{ github.event.pull_request.number }}",
        "WORKFLOW_HEAD_SHA": "${{ github.event.pull_request.head.sha }}",
    }:
        violations.append("preview artifact metadata environment is not exact")
    if _step_keys(steps[3]) != ["name", "uses", "with"] or _scalar_mapping(
        steps[3], "with", indent=8
    ) != {
        "name": "rocky-preview-comment",
        "path": "${{ runner.temp }}/rocky-preview-comment-upload/",
        "if-no-files-found": "error",
        "retention-days": "1",
    }:
        violations.append("preview artifact upload is not exact")
    if _run_scripts(job) != [
        "python3 .github/scripts/preview_comment.py stage\n"
        '--source "$BODY_PATH"\n'
        '--output-directory "$RUNNER_TEMP/rocky-preview-comment-upload"'
    ]:
        violations.append("preview producer run script is not exact")
    return violations


def _check_preview_comment(workflow: Workflow, job: str) -> list[str]:
    violations: list[str] = []
    top_level_keys = re.findall(r"(?m)^([A-Za-z0-9_-]+)\s*:", workflow.structural_text)
    if top_level_keys != ["name", "on", "permissions", "jobs"]:
        violations.append("preview comment workflow keys are not exact")
    expected_trigger = """\
  workflow_run:
    workflows: [rocky-preview]
    types: [completed]"""
    if (workflow.event_block("workflow_run") or "").strip() != expected_trigger.strip():
        violations.append("preview comment trigger is not exact")
    on_block = _key_block(workflow.text, "on", indent=0) or ""
    if set(re.findall(r"(?m)^  ([A-Za-z0-9_-]+):", on_block)) != {"workflow_run"}:
        violations.append("preview comment event set is not exact")
    if _scalar_mapping(workflow.text, "permissions", indent=0) != {
        "actions": "read",
        "contents": "read",
        "pull-requests": "write",
    }:
        violations.append("preview comment permissions are not exact")
    if _job_keys(job) != ["name", "if", "runs-on", "steps"]:
        violations.append("preview comment job keys are not exact")
    expected_condition = """\
    if: >-
      github.event.workflow_run.event == 'pull_request' &&
      github.event.workflow_run.conclusion == 'success'"""
    if _key_block(job, "if", indent=4) != expected_condition:
        violations.append("preview comment condition is not exact")
    if not re.search(r"(?m)^    runs-on:\s*ubuntu-latest\s*$", job):
        violations.append("preview comment job must use the hosted runner")
    for forbidden in (
        r"(?m)^    (?:container|services|defaults|env|environment|permissions):",
        r"(?m)^\s+shell:",
        r"(?m)^\s+working-directory:",
        r"(?m)^\s+repository:",
    ):
        if re.search(forbidden, job):
            violations.append("preview comment job changes its execution context")
    executable = "\n".join(
        line for line in job.splitlines() if not line.lstrip().startswith("#")
    )
    expected_token = "${{ github.token }}"
    if (
        executable.count(expected_token) != 1
        or TOKEN_CONTEXT_RE.search(executable.replace(expected_token, ""))
        or WHOLE_GITHUB_CONTEXT_RE.search(executable)
        or COMPUTED_GITHUB_CONTEXT_RE.search(executable)
        or SECRET_RE.search(executable)
    ):
        violations.append("preview comment token reference is not exact")
    steps = _step_blocks(job)
    if [_step_name(step) for step in steps] != [
        "Check out trusted comment tooling",
        "Authenticate producer artifact and upsert preview comment",
    ]:
        violations.append("preview comment job steps are not exact")
        return violations
    if _uses_sources(job) != [CHECKOUT_SOURCE]:
        violations.append("preview comment action sources are not exact")
    if _step_keys(steps[0]) != ["name", "uses", "with"] or _scalar_mapping(
        steps[0], "with", indent=8
    ) != {
        "ref": "${{ github.workflow_sha }}",
        "path": "trusted",
        "fetch-depth": "1",
        "persist-credentials": "false",
    }:
        violations.append("preview trusted checkout is not isolated")
    if _step_keys(steps[1]) != ["name", "env", "run"] or _scalar_mapping(
        steps[1], "env", indent=8
    ) != {
        "GH_TOKEN": "${{ github.token }}",
        "SOURCE_RUN_ID": "${{ github.event.workflow_run.id }}",
    }:
        violations.append("preview commenter environment is not exact")
    if _run_scripts(job) != [
        "python3 trusted/.github/scripts/preview_comment.py post-workflow-run"
    ]:
        violations.append("preview comment job contains a non-allow-listed run script")
    return violations


def _check_policy_workflow(workflow: Workflow, job: str) -> list[str]:
    violations: list[str] = []
    if _job_keys(job) != ["name", "runs-on", "steps"]:
        violations.append("security policy job keys are not exact")
    on_block = _key_block(workflow.text, "on", indent=0) or ""
    event_names = {
        match.group(1)
        for match in re.finditer(r"(?m)^  ([A-Za-z0-9_-]+):", on_block)
    }
    if event_names != {"pull_request_target"}:
        violations.append("security policy events are not exact")
    expected_target = """\
  pull_request_target:
    types: [opened, reopened, synchronize]
    branches: [main]"""
    target = workflow.event_block("pull_request_target")
    if target is None or target.strip() != expected_target.strip():
        violations.append("security policy trigger is not exact")
    if workflow.event_block("pull_request") is not None:
        violations.append("security policy must not use pull_request")
    if _scalar_mapping(workflow.text, "permissions", indent=0) != {
        "contents": "read"
    }:
        violations.append("security policy permissions are not exact")
    if not re.search(r"(?m)^    runs-on:\s*ubuntu-latest\s*$", job):
        violations.append("security policy must use the hosted runner")
    for forbidden in (
        r"(?m)^    (?:container|services|defaults|env|environment):",
        r"(?m)^\s+shell:",
    ):
        if re.search(forbidden, job):
            violations.append("security policy changes its execution context")

    steps = _step_blocks(job)
    if [_step_name(step) for step in steps] != [
        "Check out trusted policy tooling",
        "Fetch exact candidate policy data",
        "Enforce credential-containment policy",
    ]:
        violations.append("security policy steps are not exact")
        return violations
    if _uses_sources(job) != [CHECKOUT_SOURCE]:
        violations.append("security policy action sources are not exact")
    if _step_keys(steps[0]) != ["name", "uses", "with"] or _scalar_mapping(
        steps[0], "with", indent=8
    ) != {
        "ref": "${{ github.workflow_sha }}",
        "path": "trusted",
        "persist-credentials": "false",
    }:
        violations.append("security policy trusted checkout is not exact")
    if _step_keys(steps[1]) != ["name", "env", "run"] or _scalar_mapping(
        steps[1], "env", indent=8
    ) != {
        "CANDIDATE_REPOSITORY": "${{ github.event.pull_request.head.repo.full_name }}",
        "CANDIDATE_SHA": "${{ github.event.pull_request.head.sha }}",
        "GH_TOKEN": "${{ github.token }}",
    }:
        violations.append("security policy candidate fetch environment is not exact")
    if _step_keys(steps[2]) != ["name", "run"]:
        violations.append("security policy enforcement step is not exact")
    if _run_scripts(job) != [
        "python3 trusted/.github/scripts/fetch_policy_candidate.py "
        '"$GITHUB_WORKSPACE/candidate"',
        "python3 trusted/.github/scripts/check_credential_containment.py\n"
        '"$GITHUB_WORKSPACE/candidate"',
    ]:
        violations.append("security policy run scripts are not exact")
    executable = "\n".join(
        line for line in job.splitlines() if not line.lstrip().startswith("#")
    )
    if (
        SECRET_RE.search(executable)
        or WHOLE_GITHUB_CONTEXT_RE.search(executable)
        or COMPUTED_GITHUB_CONTEXT_RE.search(executable)
    ):
        violations.append("security policy consumes an unexpected credential")
    expected_token = "${{ github.token }}"
    if executable.count(expected_token) != 1 or TOKEN_CONTEXT_RE.search(
        executable.replace(expected_token, "")
    ):
        violations.append("security policy token reference is not exact")
    return violations


def _check_policy_tests_workflow(workflow: Workflow, job: str) -> list[str]:
    violations: list[str] = []
    top_level_keys = re.findall(r"(?m)^([A-Za-z0-9_-]+)\s*:", workflow.structural_text)
    if top_level_keys != ["name", "on", "permissions", "jobs"]:
        violations.append("security policy test workflow keys are not exact")
    expected_trigger = """\
  pull_request:
    types: [opened, reopened, synchronize]
    branches: [main]"""
    if (workflow.event_block("pull_request") or "").strip() != expected_trigger.strip():
        violations.append("security policy test trigger is not exact")
    if workflow.event_block("pull_request_target") is not None:
        violations.append("security policy tests must not use pull_request_target")
    on_block = _key_block(workflow.text, "on", indent=0) or ""
    if set(re.findall(r"(?m)^  ([A-Za-z0-9_-]+):", on_block)) != {"pull_request"}:
        violations.append("security policy test events are not exact")
    if _scalar_mapping(workflow.text, "permissions", indent=0) != {"contents": "read"}:
        violations.append("security policy test permissions are not exact")
    if _job_keys(job) != ["name", "runs-on", "steps"]:
        violations.append("security policy test job keys are not exact")
    if not re.search(r"(?m)^    runs-on:\s*ubuntu-latest\s*$", job):
        violations.append("security policy tests must use the hosted runner")
    steps = _step_blocks(job)
    if [_step_name(step) for step in steps] != [
        "Check out pull-request revision",
        "Run executable workflow-policy regressions",
    ]:
        violations.append("security policy test steps are not exact")
        return violations
    if _uses_sources(job) != [CHECKOUT_SOURCE]:
        violations.append("security policy test action sources are not exact")
    if _step_keys(steps[0]) != ["name", "uses", "with"] or _scalar_mapping(
        steps[0], "with", indent=8
    ) != {"persist-credentials": "false"}:
        violations.append("security policy test checkout is not exact")
    if _step_keys(steps[1]) != ["name", "run"]:
        violations.append("security policy test runner step is not exact")
    if _run_scripts(job) != [
        "python3 -m unittest discover -s .github/tests -p 'test_*.py' -v"
    ]:
        violations.append("security policy test run script is not exact")
    return violations


def _check_engine_eval_workflow(workflow: Workflow, job: str) -> list[str]:
    violations: list[str] = []
    top_level_keys = re.findall(r"(?m)^([A-Za-z0-9_-]+)\s*:", workflow.structural_text)
    if top_level_keys != ["name", "on", "permissions", "concurrency", "jobs"]:
        violations.append("PR eval workflow keys are not exact")
    expected_trigger = """\
  pull_request:
    types: [opened, reopened, synchronize]
    branches: [main]
    paths:
      - 'engine/evals/**'
      - 'engine/crates/rocky-mcp/**'
      - '.github/workflows/engine-evals.yml'"""
    if (workflow.event_block("pull_request") or "").strip() != expected_trigger.strip():
        violations.append("PR eval trigger is not exact")
    if workflow.event_block("pull_request_target") is not None:
        violations.append("PR evals must not use pull_request_target")
    on_block = _key_block(workflow.text, "on", indent=0) or ""
    if set(re.findall(r"(?m)^  ([A-Za-z0-9_-]+):", on_block)) != {"pull_request"}:
        violations.append("PR eval event set is not exact")
    if _scalar_mapping(workflow.text, "permissions", indent=0) != {"contents": "read"}:
        violations.append("PR eval permissions are not exact")
    if _job_keys(job) != ["name", "runs-on", "steps"]:
        violations.append("PR eval job keys are not exact")
    if not re.search(r"(?m)^    runs-on:\s*ubuntu-latest\s*$", job):
        violations.append("PR evals must use the hosted runner")
    return violations


def _check_unprivileged_job(
    workflow: Workflow,
    job_name: str,
    job: str,
    *,
    allowed_checkout_maps: tuple[dict[str, str], ...] | None,
) -> list[str]:
    violations: list[str] = []
    top_permission_state = _permission_state(workflow.text, indent=0)
    job_permission_state = _permission_state(job, indent=4)
    if top_permission_state == "missing":
        violations.append(f"candidate job {job_name} inherits unspecified token permissions")
    elif top_permission_state == "write":
        violations.append(f"candidate job {job_name} inherits write token permissions")
    elif top_permission_state == "invalid":
        violations.append(f"candidate job {job_name} has non-canonical token permissions")
    if job_permission_state == "write":
        violations.append(f"candidate job {job_name} requests write token permissions")
    elif job_permission_state == "invalid":
        violations.append(f"candidate job {job_name} requests non-canonical token permissions")

    executable = "\n".join(
        line for line in job.splitlines() if not line.lstrip().startswith("#")
    )
    if (
        TOKEN_CONTEXT_RE.search(executable)
        or WHOLE_GITHUB_CONTEXT_RE.search(executable)
        or COMPUTED_GITHUB_CONTEXT_RE.search(executable)
    ):
        violations.append(f"candidate job {job_name} explicitly consumes the GitHub token")

    if _key_block(job, "uses", indent=4) is not None:
        violations.append(f"candidate job {job_name} must not call a reusable workflow")
    action_sources = _uses_sources(job)
    uses_occurrences = len(
        re.findall(r"(?m)^\s+(?:-\s+)?uses:\s*", _yaml_structural_text(job))
    )
    if uses_occurrences != len(action_sources):
        violations.append(f"candidate job {job_name} uses a non-canonical action step")
    allowed_local = UNPRIVILEGED_LOCAL_ACTIONS.get(
        (workflow.path.name, job_name), set()
    )
    for source in action_sources:
        if source.startswith("./"):
            if source not in allowed_local:
                violations.append(
                    f"candidate job {job_name} uses a non-allow-listed local action"
                )
        elif not PINNED_REMOTE_ACTION_RE.fullmatch(source):
            violations.append(
                f"candidate job {job_name} action source is not pinned to a full SHA"
            )

    checkout_steps = [
        step
        for step in _step_blocks(job)
        if (_step_action_source(step) or "").startswith("actions/checkout@")
    ]
    checkout_occurrences = len(
        re.findall(r"actions/checkout@", _yaml_structural_text(job))
    )
    if checkout_occurrences != len(checkout_steps):
        violations.append(f"candidate job {job_name} uses a non-canonical checkout step")
    checkout_maps: list[dict[str, str]] = []
    for step in checkout_steps:
        source = _step_action_source(step)
        mapping = _scalar_mapping(step, "with", indent=8)
        if source is None or not PINNED_CHECKOUT_RE.fullmatch(source):
            violations.append(f"candidate job {job_name} checkout action is not pinned")
        if mapping is None:
            violations.append(f"candidate job {job_name} checkout inputs are non-canonical")
            violations.append(
                f"candidate job {job_name} checkout persists or omits GitHub credentials"
            )
            continue
        checkout_maps.append(mapping)
        if mapping.get("persist-credentials") != "false":
            violations.append(
                f"candidate job {job_name} checkout persists or omits GitHub credentials"
            )
        if {"token", "ssh-key", "github-server-url"}.intersection(mapping):
            violations.append(f"candidate job {job_name} checkout overrides a credential input")
    if allowed_checkout_maps is not None:
        if any(_step_action_source(step) != CHECKOUT_SOURCE for step in checkout_steps):
            violations.append(f"candidate job {job_name} checkout source is not exact")
        if tuple(checkout_maps) != allowed_checkout_maps:
            violations.append(f"candidate job {job_name} has no exact candidate checkout")
    return violations


def _check_live_evals(workflow: Workflow, job: str) -> list[str]:
    violations: list[str] = []
    top_level_keys = re.findall(
        r"(?m)^([A-Za-z0-9_-]+)\s*:", workflow.structural_text
    )
    if top_level_keys != ["name", "on", "permissions", "concurrency", "jobs"]:
        violations.append("live eval workflow keys are not exact")
    if _job_keys(job) != ["name", "if", "runs-on", "environment", "env", "steps"]:
        violations.append("live eval job keys are not exact")
    if workflow.event_block("pull_request") is not None or workflow.event_block(
        "pull_request_target"
    ) is not None:
        violations.append("live evals must never have a pull-request trigger")
    on_block = _key_block(workflow.text, "on", indent=0) or ""
    event_names = {
        match.group(1)
        for match in re.finditer(r"(?m)^  ([A-Za-z0-9_-]+):", on_block)
    }
    if event_names != {"push", "workflow_dispatch"}:
        violations.append("live eval triggers are not exact")
    push = workflow.event_block("push")
    if push is None or not re.search(r"(?m)^    branches:\s*\[main\]\s*$", push):
        violations.append("live evals must run only for main pushes")
    if _scalar_mapping(workflow.text, "permissions", indent=0) != {
        "contents": "read"
    }:
        violations.append("live eval workflow permissions are not exact")
    if not re.search(r"(?m)^    environment:\s*\n      name:\s*credentialed-ci\s*$", job):
        violations.append("live evals must use credentialed-ci")
    if _key_block(job, "if", indent=4) != "    if: github.ref == 'refs/heads/main'":
        violations.append("live eval main-ref guard is not exact")
    if not re.search(r"(?m)^    runs-on:\s*ubuntu-latest\s*$", job):
        violations.append("live evals must use the hosted runner")
    if _key_block(job, "permissions", indent=4) is not None:
        violations.append("live eval job must inherit the exact read-only permissions")
    if _scalar_mapping(job, "env", indent=4) != {
        "DISABLE_AUTOUPDATER": "'1'",
        "DISABLE_UPDATES": "'1'",
    }:
        violations.append("live eval job environment is not exact")
    for forbidden in (
        r"(?m)^    (?:container|services|defaults):",
        r"(?m)^\s+shell:",
        r"(?m)^\s+repository:",
    ):
        if re.search(forbidden, job):
            violations.append("live eval job changes its execution context")

    executable = workflow.executable_text
    expected_secret = "${{ secrets.ANTHROPIC_API_KEY }}"
    if executable.count(expected_secret) != 1 or SECRET_RE.search(
        executable.replace(expected_secret, "")
    ):
        violations.append("live eval secret reference is not exact")
    if (
        TOKEN_CONTEXT_RE.search(executable)
        or WHOLE_GITHUB_CONTEXT_RE.search(executable)
        or COMPUTED_GITHUB_CONTEXT_RE.search(executable)
    ):
        violations.append("live eval job consumes an unexpected GitHub token")

    expected_sources = [
        CHECKOUT_SOURCE,
        RUST_TOOLCHAIN_SOURCE,
        RUST_CACHE_SOURCE,
        SETUP_UV_SOURCE,
        SETUP_NODE_SOURCE,
        UPLOAD_ARTIFACT_SOURCE,
    ]
    if _uses_sources(job) != expected_sources:
        violations.append("live eval action sources are not exact")
    steps = _step_blocks(job)
    expected_names = [
        "Check out exact trusted revision",
        None,
        None,
        None,
        None,
        "Configure swap",
        "Build rocky (debug)",
        "Install the DuckDB CLI",
        "Install the Claude Code CLI",
        "Harness self-test",
        "Structured-error contract",
        "Run the agent conformance suite",
        "Upload scorecard",
    ]
    expected_keys = [
        ["name", "uses", "with"],
        ["uses"],
        ["uses", "with"],
        ["uses"],
        ["uses", "with"],
        ["name", "run"],
        ["name", "working-directory", "run"],
        ["name", "run"],
        ["name", "run"],
        ["name", "working-directory", "run"],
        ["name", "working-directory", "env", "run"],
        ["name", "working-directory", "env", "run"],
        ["name", "if", "uses", "with"],
    ]
    if [_step_name(step) for step in steps] != expected_names or [
        _step_keys(step) for step in steps
    ] != expected_keys:
        violations.append("live eval steps are not exact")
        return violations

    if _scalar_mapping(steps[0], "with", indent=8) != {
        "ref": "${{ github.sha }}",
        "persist-credentials": "false",
    }:
        violations.append("live eval trusted checkout is not exact")
    if _scalar_mapping(steps[2], "with", indent=8) != {
        "workspaces": "engine",
        "key": "evals-live",
    }:
        violations.append("live eval Rust cache inputs are not exact")
    if _scalar_mapping(steps[4], "with", indent=8) != {"node-version": "22"}:
        violations.append("live eval Node setup inputs are not exact")
    for index in (6, 9, 10, 11):
        if _key_block(steps[index], "working-directory", indent=8) != (
            "        working-directory: engine"
            if index == 6
            else "        working-directory: engine/evals"
        ):
            violations.append("live eval working directories are not exact")
            break
    if _scalar_mapping(steps[10], "env", indent=8) != {
        "ROCKY_BIN": "${{ github.workspace }}/engine/target/debug/rocky"
    }:
        violations.append("live eval error-contract environment is not exact")
    if _scalar_mapping(steps[11], "env", indent=8) != {
        "ROCKY_BIN": "${{ github.workspace }}/engine/target/debug/rocky",
        "ANTHROPIC_API_KEY": expected_secret,
    }:
        violations.append("live eval model-call environment is not exact")
    if _key_block(steps[12], "if", indent=8) != "        if: always()" or (
        _mapping_keys(steps[12], "with", indent=8)
        != ["name", "path", "if-no-files-found"]
    ) or _key_block(steps[12], "name", indent=10) != (
        "          name: agent-eval-scorecard-${{ github.sha }}"
    ) or _block_scalar(steps[12], "path", indent=10) != (
        "engine/evals/results/scorecard.json\n"
        "engine/evals/results/scorecard.md\n"
        "engine/evals/results/transcripts/"
    ) or _key_block(steps[12], "if-no-files-found", indent=10) != (
        "          if-no-files-found: warn"
    ):
        violations.append("live eval artifact upload is not exact")
    script_hashes = tuple(
        hashlib.sha256(script.encode()).hexdigest() for script in _run_scripts(job)
    )
    if script_hashes != LIVE_EVAL_RUN_SCRIPT_HASHES:
        violations.append("live eval run scripts are not exact")
    return violations


def check_workflow(workflow: Workflow) -> list[str]:
    violations: list[str] = []
    structural = workflow.structural_text
    if YAML_CHARACTER_ESCAPE_RE.search(workflow.text):
        violations.append("workflow uses an unsupported escaped YAML character")
    if YAML_QUOTED_KEY_RE.search(structural):
        violations.append("workflow uses an unsupported quoted YAML key")
    if YAML_EXPLICIT_KEY_RE.search(structural):
        violations.append("workflow uses an unsupported explicit YAML key")
    if YAML_KEY_SPACING_RE.search(structural):
        violations.append("workflow uses unsupported spacing before a YAML key colon")
    if YAML_FLOW_MAPPING_SEQUENCE_RE.search(structural):
        violations.append("workflow uses an unsupported flow-style sequence mapping")
    if YAML_INLINE_STEPS_RE.search(structural):
        violations.append("workflow steps must use canonical block form")
    if YAML_ANCHOR_ALIAS_RE.search(structural):
        violations.append("workflow uses an unsupported YAML anchor or alias")
    if YAML_TAG_RE.search(structural):
        violations.append("workflow uses an unsupported YAML tag")
    top_level_keys = re.findall(r"(?m)^([A-Za-z0-9_-]+)\s*:", structural)
    if len(top_level_keys) != len(set(top_level_keys)):
        violations.append("workflow contains duplicate top-level YAML keys")
    on_block = _key_block(workflow.text, "on", indent=0) or ""
    event_keys = re.findall(r"(?m)^  ([A-Za-z0-9_-]+)\s*:", on_block)
    if len(event_keys) != len(set(event_keys)):
        violations.append("workflow contains duplicate event keys")
    event_names = workflow.canonical_event_names()
    if event_names is None:
        violations.append("workflow trigger must use a canonical block mapping")
        event_names = set()
    pr_triggered = bool(event_names & {"pull_request", "pull_request_target"})
    pull_request_target = "pull_request_target" in event_names
    workflow_run = "workflow_run" in event_names
    jobs = workflow.jobs()
    if pull_request_target and workflow.path.name not in {
        "ai-review.yml",
        "ci-security-policy.yml",
    }:
        violations.append("pull_request_target workflow is not allow-listed")
    if workflow_run and workflow.path.name != "preview-comment.yml":
        violations.append("workflow_run workflow is not allow-listed")
    if (pull_request_target or workflow_run) and workflow.path.name in PRIVILEGED_EVENT_JOBS:
        expected_privileged_jobs = PRIVILEGED_EVENT_JOBS[workflow.path.name]
        if set(jobs) != expected_privileged_jobs:
            violations.append("privileged-event workflow jobs are not exact")
    if pr_triggered and ("jobs:" not in workflow.text.splitlines() or not jobs):
        violations.append("PR-triggered jobs must use canonical block form")
    if pr_triggered and any(name.startswith("<unparsed:") for name in jobs):
        violations.append("PR-triggered job definitions must use canonical block form")
    job_names = [
        match.group("bare") or match.group("single") or match.group("double")
        for line in workflow.text.splitlines()
        if (match := JOB_RE.fullmatch(line))
    ]
    if len(job_names) != len(set(job_names)):
        violations.append("workflow contains duplicate job names")
    for job_name, job in jobs.items():
        keys = _job_keys(job)
        if len(keys) != len(set(keys)):
            violations.append(f"job {job_name} contains duplicate YAML keys")
    credential_jobs = {name for name, job in jobs.items() if _is_credential_job(job)}
    if workflow.path.name in PROTECTED_CANDIDATE_WORKFLOWS and re.search(
        r"(?m)^(?:env|defaults)\s*:", structural
    ):
        violations.append("candidate workflows must not define top-level env or defaults")
    if pr_triggered and COMPUTED_GITHUB_CONTEXT_RE.search(workflow.executable_text):
        violations.append("PR-triggered workflows must not compute over the GitHub context")
    expected_jobs = {
        job_name for name, job_name in REQUIRED_JOBS if name == workflow.path.name
    }
    for expected_job in expected_jobs - jobs.keys():
        violations.append(f"required candidate job {expected_job} is missing")

    parsed_executable = "\n".join(
        line
        for job in jobs.values()
        for line in job.splitlines()
        if not line.lstrip().startswith("#")
    )
    if pr_triggered and (
        len(SECRET_RE.findall(workflow.executable_text))
        > len(SECRET_RE.findall(parsed_executable))
        or len(ENVIRONMENT_RE.findall(workflow.executable_text))
        > len(ENVIRONMENT_RE.findall(parsed_executable))
    ):
        violations.append("PR-triggered credentials exist outside a classified job")
    if pr_triggered and (
        len(TOKEN_CONTEXT_RE.findall(workflow.executable_text))
        > len(TOKEN_CONTEXT_RE.findall(parsed_executable))
        or len(WHOLE_GITHUB_CONTEXT_RE.findall(workflow.executable_text))
        > len(WHOLE_GITHUB_CONTEXT_RE.findall(parsed_executable))
    ):
        violations.append("PR-triggered GitHub token context exists outside a classified job")
    if pr_triggered and not credential_jobs and (
        SECRET_RE.search(workflow.executable_text)
        or ENVIRONMENT_RE.search(workflow.executable_text)
    ):
        violations.append("PR-triggered credentials could not be classified into jobs")
    if MODEL_SECRET_RE.search(workflow.executable_text) and workflow.path.name not in {
        "ai-review.yml",
        "engine-evals-live.yml",
    }:
        violations.append("model key appears outside an allow-listed workflow")

    for job_name, job in jobs.items():
        job_identity = (workflow.path.name, job_name)
        if job_identity == ("ai-review.yml", "ai-review"):
            violations.extend(_check_ai_review(workflow, job))
        elif job_identity == ("ai-review.yml", "context"):
            violations.extend(_check_ai_context(job))
        elif job_identity == ("ai-review.yml", "invalidate-approval"):
            violations.extend(_check_invalidate_approval(job))
        elif job_identity == ("preview.yml", "context"):
            violations.extend(_check_preview_producer(workflow, job))
        elif job_identity == ("preview-comment.yml", "comment"):
            violations.extend(_check_preview_comment(workflow, job))
        elif job_identity == ("ci-security-policy.yml", "credential-containment"):
            violations.extend(_check_policy_workflow(workflow, job))
        elif job_identity == ("ci-security-policy-tests.yml", "policy-tests"):
            violations.extend(_check_policy_tests_workflow(workflow, job))
        elif job_identity == ("engine-evals.yml", "eval-contract"):
            violations.extend(_check_engine_eval_workflow(workflow, job))
        if pr_triggered and job_identity not in TRUSTED_PRIVILEGED_JOBS:
            violations.extend(
                _check_unprivileged_job(
                    workflow,
                    job_name,
                    job,
                    allowed_checkout_maps=CANDIDATE_CHECKOUT_MAPS.get(job_identity),
                )
            )
        if MODEL_SECRET_RE.search(job) and (workflow.path.name, job_name) not in {
            ("ai-review.yml", "ai-review"),
            ("engine-evals-live.yml", "evals"),
        }:
            violations.append(f"{job_name} references the model key outside an allow-listed job")
        if not pr_triggered or not _is_credential_job(job):
            continue
        if job_identity not in {
            ("ai-review.yml", "ai-review"),
            ("ci-security-policy.yml", "credential-containment"),
        }:
            violations.append(
                f"PR-triggered credential job {job_name} is not allow-listed"
            )

    if workflow.path.name == "engine-evals-live.yml":
        if set(jobs) != {"evals"}:
            violations.append("live eval workflow jobs are not exact")
        eval_job = jobs.get("evals")
        if eval_job is None:
            violations.append("live eval workflow omitted its evals job")
        else:
            violations.extend(_check_live_evals(workflow, eval_job))
    return violations


def check_repository(repository_root: Path) -> list[str]:
    github_directory = repository_root / ".github"
    workflows = github_directory / "workflows"
    violations: list[str] = []
    if github_directory.is_symlink() or not github_directory.is_dir():
        return [".github must be a real directory"]
    if workflows.is_symlink() or not workflows.is_dir():
        return [".github/workflows must be a real directory"]
    if (workflows / ".git").exists() or (workflows / ".git").is_symlink():
        violations.append(".github/workflows must not be a gitlink")
    scripts = github_directory / "scripts"
    if scripts.is_symlink() or not scripts.is_dir():
        violations.append(".github/scripts must be a real directory")
    else:
        script_entries = {entry.name for entry in scripts.iterdir()}
        for name in sorted(script_entries - TRUSTED_SCRIPT_ENTRIES):
            violations.append(f".github/scripts/{name}: unexpected trusted-script entry")
        for name in sorted(TRUSTED_SCRIPT_ENTRIES - script_entries):
            violations.append(f".github/scripts/{name}: required trusted script is missing")
        for name in sorted(script_entries & TRUSTED_SCRIPT_ENTRIES):
            entry = scripts / name
            if entry.is_symlink() or not entry.is_file():
                violations.append(f".github/scripts/{name}: trusted script is unsafe")
    trusted_repository_root = Path(__file__).resolve().parents[2]
    for relative_path in FROZEN_TRUST_ROOTS:
        candidate = repository_root / relative_path
        trusted = trusted_repository_root / relative_path
        current = repository_root
        unsafe_parent = None
        for part in Path(relative_path).parts[:-1]:
            current /= part
            if current.is_symlink() or not current.is_dir():
                unsafe_parent = current
                break
        if unsafe_parent is not None:
            violations.append(
                f"{relative_path}: frozen trust-root parent is missing or unsafe"
            )
            continue
        if candidate.is_symlink() or not candidate.is_file():
            violations.append(f"{relative_path}: frozen trust root is missing or unsafe")
            continue
        if not trusted.is_file() or candidate.read_bytes() != trusted.read_bytes():
            violations.append(f"{relative_path}: frozen trust root differs from trusted base")
    for name in sorted(REQUIRED_WORKFLOWS):
        path = workflows / name
        if path.is_symlink() or not path.is_file():
            violations.append(f".github/workflows/{name}: required workflow is missing")
    for path in sorted(workflows.iterdir()):
        if path.suffix not in {".yml", ".yaml"}:
            continue
        if path.is_symlink():
            violations.append(
                f"{path.relative_to(repository_root)}: workflow must not be a symlink"
            )
            continue
        if not path.is_file():
            continue
        for violation in check_workflow(Workflow(path=path, text=path.read_text())):
            violations.append(f"{path.relative_to(repository_root)}: {violation}")
    return violations


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("repository_root", nargs="?", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)
    violations = check_repository(args.repository_root.resolve())
    for violation in violations:
        print(f"::error::{violation}", file=sys.stderr)
    if violations:
        print(f"credential-containment policy: {len(violations)} violation(s)", file=sys.stderr)
        return 1
    print("credential-containment policy: passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
