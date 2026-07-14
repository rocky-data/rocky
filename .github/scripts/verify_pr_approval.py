#!/usr/bin/env python3
"""Verify that an AI-review approval still names the live pull-request head."""

from __future__ import annotations

import json
import os
import re
import sys
import urllib.error
import urllib.request
from typing import Any


SHA_RE = re.compile(r"[0-9a-f]{40}")
REPOSITORY_RE = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")
MAX_API_RESPONSE_BYTES = 1024 * 1024


class ApprovalError(RuntimeError):
    """The current PR state no longer matches the reviewed approval event."""


def github_json(*, api_url: str, path: str, token: str) -> Any:
    request = urllib.request.Request(
        f"{api_url.rstrip('/')}{path}",
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read(MAX_API_RESPONSE_BYTES + 1)
    except (OSError, urllib.error.HTTPError) as error:
        raise ApprovalError(f"GitHub API request failed: {error}") from error
    if len(body) > MAX_API_RESPONSE_BYTES:
        raise ApprovalError("GitHub API response exceeded the safety limit")
    try:
        return json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ApprovalError("GitHub API returned invalid JSON") from error


def verify_approval(
    payload: Any,
    *,
    approved_head_sha: str,
    approval_label: str | None,
    approved_base_sha: str | None = None,
    approved_base_ref: str | None = None,
    approved_base_repository: str | None = None,
    approved_head_repository: str | None = None,
) -> None:
    if not isinstance(payload, dict):
        raise ApprovalError("GitHub PR response was not an object")
    if payload.get("state") != "open":
        raise ApprovalError("pull request is no longer open")

    head = payload.get("head")
    base = payload.get("base")
    if not isinstance(head, dict) or not isinstance(base, dict):
        raise ApprovalError("GitHub PR response omitted head/base metadata")

    live_head_sha = head.get("sha")
    if live_head_sha != approved_head_sha:
        raise ApprovalError(
            "approval is stale: live head "
            f"{live_head_sha!r} does not match approved head {approved_head_sha}"
        )
    if approved_base_sha is not None and base.get("sha") != approved_base_sha:
        raise ApprovalError("approval is stale because the pull-request base changed")
    if approved_base_ref is not None and base.get("ref") != approved_base_ref:
        raise ApprovalError("approval is invalid because the pull-request base ref is untrusted")

    if approved_base_repository is not None:
        base_repository = base.get("repo")
        live_base_repository = (
            base_repository.get("full_name")
            if isinstance(base_repository, dict)
            else None
        )
        if live_base_repository != approved_base_repository:
            raise ApprovalError(
                "approval is invalid because the pull-request base repository is untrusted"
            )

    if approved_head_repository is not None:
        repository = head.get("repo")
        live_repository = repository.get("full_name") if isinstance(repository, dict) else None
        if live_repository != approved_head_repository:
            raise ApprovalError("approval is stale because the head repository changed")

    if approval_label is not None:
        labels = payload.get("labels")
        if not isinstance(labels, list):
            raise ApprovalError("GitHub PR response omitted label metadata")
        label_names = {
            item.get("name")
            for item in labels
            if isinstance(item, dict) and isinstance(item.get("name"), str)
        }
        if approval_label not in label_names:
            raise ApprovalError(f"required approval label {approval_label!r} is absent")


def load_and_verify_from_environment() -> None:
    repository = os.environ.get("GITHUB_REPOSITORY", "")
    if not REPOSITORY_RE.fullmatch(repository):
        raise ApprovalError("GITHUB_REPOSITORY is invalid")
    pr_number = os.environ.get("PR_NUMBER", "")
    if not pr_number.isdecimal() or int(pr_number) < 1:
        raise ApprovalError("PR_NUMBER must be a positive integer")
    approved_head_sha = os.environ.get("APPROVED_HEAD_SHA", "")
    if not SHA_RE.fullmatch(approved_head_sha):
        raise ApprovalError("APPROVED_HEAD_SHA must be a lowercase full Git SHA")

    approved_base_sha = os.environ.get("APPROVED_BASE_SHA", "")
    if not SHA_RE.fullmatch(approved_base_sha):
        raise ApprovalError("APPROVED_BASE_SHA must be a lowercase full Git SHA")
    approved_base_ref = os.environ.get("APPROVED_BASE_REF", "")
    if approved_base_ref != "main":
        raise ApprovalError("APPROVED_BASE_REF must be main")
    approved_base_repository = os.environ.get("APPROVED_BASE_REPOSITORY", "")
    if approved_base_repository != repository:
        raise ApprovalError("APPROVED_BASE_REPOSITORY must match GITHUB_REPOSITORY")
    approved_head_repository = os.environ.get("APPROVED_HEAD_REPOSITORY", "")
    if not REPOSITORY_RE.fullmatch(approved_head_repository):
        raise ApprovalError("APPROVED_HEAD_REPOSITORY is invalid")

    token = os.environ.get("GH_TOKEN", "")
    if not token:
        raise ApprovalError("GH_TOKEN is required")
    approval_label = os.environ.get("APPROVAL_LABEL", "ai-review")
    if approval_label != "ai-review":
        raise ApprovalError("unexpected approval label")

    payload = github_json(
        api_url=os.environ.get("GITHUB_API_URL", "https://api.github.com"),
        path=f"/repos/{repository}/pulls/{pr_number}",
        token=token,
    )
    verify_approval(
        payload,
        approved_head_sha=approved_head_sha,
        approved_base_sha=approved_base_sha,
        approved_base_ref=approved_base_ref,
        approved_base_repository=approved_base_repository,
        approved_head_repository=approved_head_repository,
        approval_label=approval_label,
    )
    print(f"approval is current for pull request #{pr_number} at {approved_head_sha}")


def main() -> int:
    try:
        load_and_verify_from_environment()
    except ApprovalError as error:
        print(f"::error::{error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
