#!/usr/bin/env python3
"""Fetch this workflow run's AI-review artifact and validate it as data."""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import stat
import sys
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path
from typing import Any

from verify_pr_approval import (
    ApprovalError,
    REPOSITORY_RE,
    SHA_RE,
    github_json,
    verify_approval,
)


TRUSTED_WORKFLOW_PATH = ".github/workflows/ai-review.yml"
TRUSTED_BASE_REF = "main"
MAX_ARTIFACT_BYTES = 512 * 1024
MAX_ARCHIVE_DOWNLOAD_BYTES = 1024 * 1024
MAX_CONTEXT_JSON_BYTES = 320 * 1024
MAX_DIFF_BYTES = 34 * 1024
MAX_SECTION_BYTES = 66 * 1024
MAX_ERROR_BYTES = 3 * 1024
LOGIN_RE = re.compile(r"[A-Za-z0-9_.\-\[\]]{1,100}")


class ContextError(RuntimeError):
    """The current-run artifact failed authentication or validation."""


def authenticate_current_run(
    payload: Any,
    *,
    expected_run_id: int,
    repository: str,
    approved_base_sha: str,
    approver_login: str,
) -> None:
    if not isinstance(payload, dict):
        raise ContextError("workflow-run response was not an object")
    run_path = payload.get("path")
    trusted_run_paths = {
        TRUSTED_WORKFLOW_PATH,
        f"{TRUSTED_WORKFLOW_PATH}@{TRUSTED_BASE_REF}",
    }
    actor = payload.get("actor")
    actor_login = actor.get("login") if isinstance(actor, dict) else None
    head_repository = payload.get("head_repository")
    head_repository_name = (
        head_repository.get("full_name") if isinstance(head_repository, dict) else None
    )
    # Each field fails closed independently, and the raised error names the exact
    # mismatch. This keeps the first live pull_request_target run debuggable: that
    # event's run object reports the base branch as head_sha/head_repository (the
    # version of the workflow being run), which is the load-bearing assumption
    # this repository has not yet exercised. The pinned REST API currently returns
    # a suffixless path, while newer GitHub documentation shows `path@ref`; accept
    # only those exact trusted representations and treat any other path or ref as
    # a mismatch.
    mismatches: list[str] = []
    if payload.get("id") != expected_run_id:
        mismatches.append("run id")
    if payload.get("event") != "pull_request_target":
        mismatches.append("event")
    if run_path not in trusted_run_paths:
        mismatches.append("workflow path")
    if payload.get("head_sha") != approved_base_sha:
        mismatches.append("head_sha (expected the approved PR base SHA)")
    if actor_login != approver_login:
        mismatches.append("actor login")
    if head_repository_name != repository:
        mismatches.append("head repository")
    if mismatches:
        raise ContextError(
            "current workflow-run metadata did not match the approval event: "
            + ", ".join(mismatches)
        )


def select_artifact(artifacts_payload: Any, *, expected_name: str) -> dict[str, Any]:
    if not isinstance(artifacts_payload, dict):
        raise ContextError("artifacts response was not an object")
    artifacts = artifacts_payload.get("artifacts")
    if not isinstance(artifacts, list):
        raise ContextError("artifacts response omitted artifacts")
    matches = [
        item
        for item in artifacts
        if isinstance(item, dict)
        and item.get("name") == expected_name
        and item.get("expired") is False
    ]
    if len(matches) != 1:
        raise ContextError("expected exactly one current-run exact-head context artifact")
    artifact = matches[0]
    size = artifact.get("size_in_bytes")
    if not isinstance(size, int) or size < 1 or size > MAX_ARTIFACT_BYTES:
        raise ContextError("context artifact size is outside the allowed bounds")
    if not isinstance(artifact.get("id"), int):
        raise ContextError("context artifact omitted its numeric id")
    return artifact


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self,
        request: Any,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> None:
        return None


def download_artifact(
    *, api_url: str, repository: str, artifact_id: int, token: str
) -> bytes:
    url = f"{api_url.rstrip('/')}/repos/{repository}/actions/artifacts/{artifact_id}/zip"
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    opener = urllib.request.build_opener(_NoRedirect)
    try:
        response = opener.open(request, timeout=30)
    except urllib.error.HTTPError as error:
        if error.code not in {301, 302, 303, 307, 308}:
            raise ContextError(f"artifact download failed with HTTP {error.code}") from error
        location = error.headers.get("Location")
        if not location:
            raise ContextError("artifact download redirect omitted Location") from error
        parsed = urllib.parse.urlparse(location)
        if parsed.scheme != "https" or not parsed.netloc:
            raise ContextError("artifact download redirect was not an absolute HTTPS URL")
        # The object-store URL is signed. Never forward the GitHub token.
        response = urllib.request.urlopen(urllib.request.Request(location), timeout=30)
    except OSError as error:
        raise ContextError(f"artifact download failed: {error}") from error

    with response:
        body = response.read(MAX_ARCHIVE_DOWNLOAD_BYTES + 1)
    if len(body) > MAX_ARCHIVE_DOWNLOAD_BYTES:
        raise ContextError("artifact archive exceeded the download limit")
    return body


def extract_context_json(archive: bytes) -> bytes:
    try:
        with zipfile.ZipFile(io.BytesIO(archive)) as bundle:
            members = [member for member in bundle.infolist() if not member.is_dir()]
            if len(members) != 1 or members[0].filename != "context.json":
                raise ContextError("context archive must contain only context.json")
            member = members[0]
            mode = (member.external_attr >> 16) & 0o170000
            if mode not in {0, stat.S_IFREG}:
                raise ContextError("context.json must be a regular file")
            if member.file_size < 1 or member.file_size > MAX_CONTEXT_JSON_BYTES:
                raise ContextError("context.json size is outside the allowed bounds")
            return bundle.read(member)
    except (zipfile.BadZipFile, RuntimeError) as error:
        if isinstance(error, ContextError):
            raise
        raise ContextError("context artifact was not a valid ZIP archive") from error


def _bounded_string(value: Any, *, field: str, max_bytes: int) -> str:
    if not isinstance(value, str):
        raise ContextError(f"{field} must be a string")
    if "\x00" in value or len(value.encode("utf-8")) > max_bytes:
        raise ContextError(f"{field} exceeded its content safety bound")
    return value


def validate_context(
    raw: bytes,
    *,
    approved_base_sha: str,
    approved_head_sha: str,
    pr_number: int,
) -> dict[str, dict[str, str]]:
    if len(raw) > MAX_CONTEXT_JSON_BYTES:
        raise ContextError("context JSON exceeded the validation limit")
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ContextError("context.json contained invalid JSON") from error
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        raise ContextError("unsupported context schema")

    metadata = payload.get("pull_request")
    if not isinstance(metadata, dict):
        raise ContextError("context omitted pull_request metadata")
    # Current-run identity selects/authenticates the artifact. Body metadata is
    # only a consistency check and never grants eligibility.
    if metadata != {
        "number": pr_number,
        "base_sha": approved_base_sha,
        "head_sha": approved_head_sha,
    }:
        raise ContextError("context body metadata does not match the current run")

    sections = payload.get("sections")
    if not isinstance(sections, dict) or set(sections) != {
        "diff",
        "compile",
        "ci_diff",
        "lineage",
    }:
        raise ContextError("context sections were missing or unexpected")

    validated: dict[str, dict[str, str]] = {}
    for name, value in sections.items():
        if not isinstance(value, dict) or set(value) != {"status", "content", "error"}:
            raise ContextError(f"context section {name} had an invalid shape")
        status_value = value.get("status")
        if status_value not in {"ok", "failed"}:
            raise ContextError(f"context section {name} had an invalid status")
        content_limit = MAX_DIFF_BYTES if name == "diff" else MAX_SECTION_BYTES
        validated[name] = {
            "status": status_value,
            "content": _bounded_string(
                value.get("content"),
                field=f"sections.{name}.content",
                max_bytes=content_limit,
            ),
            "error": _bounded_string(
                value.get("error"),
                field=f"sections.{name}.error",
                max_bytes=MAX_ERROR_BYTES,
            ),
        }
    return validated


def materialize_context(
    sections: dict[str, dict[str, str]], output_directory: Path
) -> None:
    runner_temp_text = os.environ.get("RUNNER_TEMP", "")
    if not runner_temp_text:
        raise ContextError("RUNNER_TEMP is required")
    runner_temp = Path(runner_temp_text).resolve()
    destination = output_directory.resolve()
    if not runner_temp.is_dir() or not destination.is_relative_to(runner_temp):
        raise ContextError("context output directory must be below RUNNER_TEMP")
    if destination.exists() and any(destination.iterdir()):
        raise ContextError("context output directory was unexpectedly non-empty")
    destination.mkdir(mode=0o700, parents=True, exist_ok=True)

    filenames = {
        "diff": ("diff.txt", "diff.err", "diff_failed"),
        "compile": ("compile.json", "compile.err", "compile_failed"),
        "ci_diff": ("ci.json", "ci.err", "ci_failed"),
        "lineage": ("lineage.json", "lineage.err", "lineage_failed"),
    }
    for name, section in sections.items():
        content_name, error_name, failed_name = filenames[name]
        (destination / content_name).write_text(section["content"])
        (destination / error_name).write_text(section["error"])
        (destination / failed_name).write_text(
            "failed\n" if section["status"] == "failed" else ""
        )
    for path in destination.iterdir():
        path.chmod(0o600)


def fetch_context(*, output_directory: Path) -> None:
    repository = os.environ.get("GITHUB_REPOSITORY", "")
    head_repository = os.environ.get("APPROVED_HEAD_REPOSITORY", "")
    base_repository = os.environ.get("APPROVED_BASE_REPOSITORY", "")
    base_ref = os.environ.get("APPROVED_BASE_REF", "")
    approver_login = os.environ.get("APPROVER_LOGIN", "")
    head_sha = os.environ.get("APPROVED_HEAD_SHA", "")
    base_sha = os.environ.get("APPROVED_BASE_SHA", "")
    pr_number_text = os.environ.get("PR_NUMBER", "")
    run_id_text = os.environ.get("CONTEXT_RUN_ID", "")
    approval_label = os.environ.get("APPROVAL_LABEL", "ai-review")
    token = os.environ.get("GH_TOKEN", "")
    api_url = os.environ.get("GITHUB_API_URL", "https://api.github.com")

    if (
        not REPOSITORY_RE.fullmatch(repository)
        or not REPOSITORY_RE.fullmatch(head_repository)
        or base_repository != repository
    ):
        raise ContextError("repository metadata is invalid")
    if base_ref != TRUSTED_BASE_REF:
        raise ContextError("pull-request base ref must be main")
    if not LOGIN_RE.fullmatch(approver_login):
        raise ContextError("APPROVER_LOGIN is invalid")
    if not SHA_RE.fullmatch(head_sha) or not SHA_RE.fullmatch(base_sha):
        raise ContextError("approved revisions must be lowercase full Git SHAs")
    if not pr_number_text.isdecimal() or int(pr_number_text) < 1:
        raise ContextError("PR_NUMBER must be a positive integer")
    if not run_id_text.isdecimal() or int(run_id_text) < 1:
        raise ContextError("CONTEXT_RUN_ID must be a positive integer")
    if not token:
        raise ContextError("GH_TOKEN is required")
    pr_number = int(pr_number_text)
    run_id = int(run_id_text)

    pr_payload = github_json(
        api_url=api_url,
        path=f"/repos/{repository}/pulls/{pr_number}",
        token=token,
    )
    try:
        verify_approval(
            pr_payload,
            approved_head_sha=head_sha,
            approved_base_sha=base_sha,
            approved_base_ref=base_ref,
            approved_base_repository=base_repository,
            approved_head_repository=head_repository,
            approval_label=approval_label,
        )
    except ApprovalError as error:
        raise ContextError(str(error)) from error

    run_payload = github_json(
        api_url=api_url,
        path=f"/repos/{repository}/actions/runs/{run_id}",
        token=token,
    )
    authenticate_current_run(
        run_payload,
        expected_run_id=run_id,
        repository=repository,
        approved_base_sha=base_sha,
        approver_login=approver_login,
    )
    artifacts_payload = github_json(
        api_url=api_url,
        path=f"/repos/{repository}/actions/runs/{run_id}/artifacts?per_page=100",
        token=token,
    )
    artifact = select_artifact(
        artifacts_payload,
        expected_name=f"rocky-ai-review-context-{head_sha}",
    )
    archive = download_artifact(
        api_url=api_url,
        repository=repository,
        artifact_id=artifact["id"],
        token=token,
    )
    raw_context = extract_context_json(archive)
    sections = validate_context(
        raw_context,
        approved_base_sha=base_sha,
        approved_head_sha=head_sha,
        pr_number=pr_number,
    )
    materialize_context(sections, output_directory)
    print(f"validated current-run artifact {artifact['id']} for approved head {head_sha}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-directory", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        fetch_context(output_directory=args.output_directory)
    except (ApprovalError, ContextError) as error:
        print(f"::error::{error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
