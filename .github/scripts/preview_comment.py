#!/usr/bin/env python3
"""Move a bounded preview artifact across the PR trust boundary and post it.

The ``stage`` command runs in the credential-free candidate job. The
``post-workflow-run`` command runs in a fresh write-token workflow, authenticates
the producer run, revalidates the live pull request, and treats the downloaded
artifact only as bounded Markdown data.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import stat
import sys
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from verify_pr_approval import (
    ApprovalError,
    REPOSITORY_RE,
    SHA_RE,
    verify_approval,
)


COMMENT_MARKER = "<!-- rocky-preview -->"
COMMENT_FILE = "body.md"
METADATA_FILE = "metadata.json"
ARTIFACT_NAME = "rocky-preview-comment"
TRUSTED_PREVIEW_WORKFLOW_PATH = ".github/workflows/preview.yml"
MAX_COMMENT_BYTES = 60 * 1024
MAX_METADATA_BYTES = 4 * 1024
MAX_ARTIFACT_BYTES = 128 * 1024
MAX_ARCHIVE_DOWNLOAD_BYTES = 256 * 1024
MAX_API_RESPONSE_BYTES = 1024 * 1024
MAX_COMMENT_PAGES = 10


class PreviewCommentError(RuntimeError):
    """The preview artifact or live pull-request state was unsafe."""


@dataclass(frozen=True)
class PreviewRunIdentity:
    pr_number: int | None
    base_repository: str
    base_sha: str | None
    head_repository: str
    head_sha: str


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


def _runner_temp() -> Path:
    value = os.environ.get("RUNNER_TEMP", "")
    if not value:
        raise PreviewCommentError("RUNNER_TEMP is required")
    root = Path(value).resolve()
    if not root.is_dir():
        raise PreviewCommentError("RUNNER_TEMP is not a directory")
    return root


def _require_within_runner_temp(path: Path, *, must_exist: bool) -> Path:
    root = _runner_temp()
    try:
        resolved = path.resolve(strict=must_exist)
        resolved.relative_to(root)
    except (OSError, ValueError) as error:
        raise PreviewCommentError("preview path escaped RUNNER_TEMP") from error
    return resolved


def _read_regular_file(path: Path) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise PreviewCommentError("preview body could not be opened safely") from error
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise PreviewCommentError("preview body is not a regular file")
        if metadata.st_size > MAX_COMMENT_BYTES:
            raise PreviewCommentError("preview body exceeded the byte limit")
        with os.fdopen(descriptor, "rb", closefd=False) as source:
            body = source.read(MAX_COMMENT_BYTES + 1)
    finally:
        os.close(descriptor)
    if len(body) > MAX_COMMENT_BYTES:
        raise PreviewCommentError("preview body exceeded the byte limit")
    return body


def validate_body(body: bytes) -> str:
    if not body:
        raise PreviewCommentError("preview body is empty")
    if len(body) > MAX_COMMENT_BYTES:
        raise PreviewCommentError("preview body exceeded the byte limit")
    if b"\0" in body:
        raise PreviewCommentError("preview body contains a NUL byte")
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as error:
        raise PreviewCommentError("preview body is not UTF-8") from error
    if not text.startswith(f"{COMMENT_MARKER}\n"):
        raise PreviewCommentError("preview body omitted the exact leading marker")
    if text.count(COMMENT_MARKER) != 1:
        raise PreviewCommentError("preview body contains duplicate markers")
    return text


def stage_body(source: Path, output_directory: Path) -> Path:
    _require_within_runner_temp(source, must_exist=True)
    _require_within_runner_temp(output_directory.parent, must_exist=True)
    if output_directory.exists() or output_directory.is_symlink():
        raise PreviewCommentError("preview staging directory already exists")
    body = validate_body(_read_regular_file(source))

    output_directory.mkdir(mode=0o700)
    os.chmod(output_directory, 0o700)
    target = output_directory / COMMENT_FILE
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor = os.open(target, flags, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb", closefd=False) as destination:
            destination.write(body.encode("utf-8"))
            destination.flush()
            os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return target


def _write_private_json(path: Path, payload: dict[str, Any]) -> None:
    encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    if len(encoded) > MAX_METADATA_BYTES:
        raise PreviewCommentError("preview metadata exceeded the byte limit")
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor = os.open(path, flags, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb", closefd=False) as destination:
            destination.write(encoded)
            destination.flush()
            os.fsync(descriptor)
    finally:
        os.close(descriptor)


def stage_artifact(source: Path, output_directory: Path) -> Path:
    target = stage_body(source, output_directory)
    repository = os.environ.get("GITHUB_REPOSITORY", "")
    base_repository = os.environ.get("BASE_REPOSITORY", "")
    head_repository = os.environ.get("HEAD_REPOSITORY", "")
    base_sha = os.environ.get("BASE_SHA", "")
    head_sha = os.environ.get("HEAD_SHA", "")
    workflow_head_sha = os.environ.get("WORKFLOW_HEAD_SHA", "")
    pr_number_text = os.environ.get("PR_NUMBER", "")
    if (
        not REPOSITORY_RE.fullmatch(repository)
        or base_repository != repository
        or not REPOSITORY_RE.fullmatch(head_repository)
    ):
        raise PreviewCommentError("preview repository metadata is invalid")
    if not all(SHA_RE.fullmatch(value) for value in (base_sha, head_sha, workflow_head_sha)):
        raise PreviewCommentError("preview revision metadata is invalid")
    if not pr_number_text.isdecimal() or int(pr_number_text) < 1:
        raise PreviewCommentError("PR_NUMBER must be a positive integer")
    _write_private_json(
        output_directory / METADATA_FILE,
        {
            "schema_version": 1,
            "pull_request": {
                "number": int(pr_number_text),
                "base_repository": base_repository,
                "base_sha": base_sha,
                "head_repository": head_repository,
                "head_sha": head_sha,
            },
            "workflow_head_sha": workflow_head_sha,
        },
    )
    return target


def _validate_metadata(raw: bytes) -> dict[str, Any]:
    if not raw or len(raw) > MAX_METADATA_BYTES:
        raise PreviewCommentError("preview metadata size is outside the allowed bounds")
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PreviewCommentError("preview metadata was not valid JSON") from error
    if not isinstance(payload, dict) or set(payload) != {
        "schema_version",
        "pull_request",
        "workflow_head_sha",
    }:
        raise PreviewCommentError("preview metadata shape is invalid")
    if payload.get("schema_version") != 1:
        raise PreviewCommentError("preview metadata schema is unsupported")
    pull_request = payload.get("pull_request")
    if not isinstance(pull_request, dict) or set(pull_request) != {
        "number",
        "base_repository",
        "base_sha",
        "head_repository",
        "head_sha",
    }:
        raise PreviewCommentError("preview pull-request metadata is invalid")
    if not isinstance(pull_request.get("number"), int) or pull_request["number"] < 1:
        raise PreviewCommentError("preview pull-request number is invalid")
    if not REPOSITORY_RE.fullmatch(str(pull_request.get("base_repository", ""))):
        raise PreviewCommentError("preview base repository is invalid")
    if not REPOSITORY_RE.fullmatch(str(pull_request.get("head_repository", ""))):
        raise PreviewCommentError("preview head repository is invalid")
    for field in ("base_sha", "head_sha"):
        if not SHA_RE.fullmatch(str(pull_request.get(field, ""))):
            raise PreviewCommentError(f"preview {field} is invalid")
    if not SHA_RE.fullmatch(str(payload.get("workflow_head_sha", ""))):
        raise PreviewCommentError("preview workflow head SHA is invalid")
    return payload


def extract_preview_artifact(archive: bytes) -> tuple[str, dict[str, Any]]:
    try:
        with zipfile.ZipFile(io.BytesIO(archive)) as bundle:
            members = {
                member.filename: member
                for member in bundle.infolist()
                if not member.is_dir()
            }
            if set(members) != {COMMENT_FILE, METADATA_FILE}:
                raise PreviewCommentError(
                    "preview artifact must contain only body.md and metadata.json"
                )
            for name, limit in (
                (COMMENT_FILE, MAX_COMMENT_BYTES),
                (METADATA_FILE, MAX_METADATA_BYTES),
            ):
                member = members[name]
                mode = (member.external_attr >> 16) & 0o170000
                if mode not in {0, stat.S_IFREG} or member.file_size > limit:
                    raise PreviewCommentError(f"preview artifact member {name} is unsafe")
            body = validate_body(bundle.read(members[COMMENT_FILE]))
            metadata = _validate_metadata(bundle.read(members[METADATA_FILE]))
            return body, metadata
    except (zipfile.BadZipFile, RuntimeError) as error:
        if isinstance(error, PreviewCommentError):
            raise
        raise PreviewCommentError("preview artifact was not a valid ZIP archive") from error


def _github_request_json(
    *,
    api_url: str,
    path: str,
    token: str,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
) -> Any:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        f"{api_url.rstrip('/')}{path}",
        data=data,
        method=method,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read(MAX_API_RESPONSE_BYTES + 1)
    except urllib.error.HTTPError as error:
        raise PreviewCommentError(
            f"GitHub API request failed with HTTP {error.code}"
        ) from error
    except OSError as error:
        raise PreviewCommentError("GitHub API request failed") from error
    if len(body) > MAX_API_RESPONSE_BYTES:
        raise PreviewCommentError("GitHub API response exceeded the safety limit")
    try:
        return json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PreviewCommentError("GitHub API returned invalid JSON") from error


def _repository_from_api_url(value: Any, *, api_url: str) -> str | None:
    if not isinstance(value, str):
        return None
    prefix = f"{api_url.rstrip('/')}/repos/"
    repository = value[len(prefix) :] if value.startswith(prefix) else ""
    return repository if REPOSITORY_RE.fullmatch(repository) else None


def authenticate_preview_run(
    payload: Any, *, expected_run_id: int, repository: str, api_url: str
) -> PreviewRunIdentity:
    if not isinstance(payload, dict):
        raise PreviewCommentError("preview workflow-run response was not an object")
    run_repository = payload.get("repository")
    run_repository_name = (
        run_repository.get("full_name") if isinstance(run_repository, dict) else None
    )
    head_repository = payload.get("head_repository")
    head_repository_name = (
        head_repository.get("full_name")
        if isinstance(head_repository, dict)
        else None
    )
    workflow_head_sha = payload.get("head_sha")
    run_path = payload.get("path")
    path_is_exact = run_path == TRUSTED_PREVIEW_WORKFLOW_PATH or (
        isinstance(run_path, str)
        and run_path.startswith(f"{TRUSTED_PREVIEW_WORKFLOW_PATH}@")
        and len(run_path) > len(TRUSTED_PREVIEW_WORKFLOW_PATH) + 1
    )
    if (
        payload.get("id") != expected_run_id
        or payload.get("name") != "rocky-preview"
        or payload.get("event") != "pull_request"
        or payload.get("status") != "completed"
        or payload.get("conclusion") != "success"
        or not path_is_exact
        or run_repository_name != repository
        or not REPOSITORY_RE.fullmatch(str(head_repository_name or ""))
        or not isinstance(workflow_head_sha, str)
        or not SHA_RE.fullmatch(workflow_head_sha)
    ):
        raise PreviewCommentError("preview workflow-run metadata is not trusted")
    pull_requests = payload.get("pull_requests")
    if not isinstance(pull_requests, list) or len(pull_requests) > 1:
        raise PreviewCommentError("preview run has ambiguous pull-request associations")
    if not pull_requests:
        # GitHub omits this association for some fork runs. The bounded
        # artifact supplies the PR number/base SHA only as lookup data; the
        # live PR must later bind them to this authenticated head identity.
        return PreviewRunIdentity(
            pr_number=None,
            base_repository=repository,
            base_sha=None,
            head_repository=head_repository_name,
            head_sha=workflow_head_sha,
        )
    pull_request = pull_requests[0]
    pr_number = pull_request.get("number") if isinstance(pull_request, dict) else None
    if not isinstance(pr_number, int) or pr_number < 1:
        raise PreviewCommentError("preview run omitted its pull-request number")
    head = pull_request.get("head")
    base = pull_request.get("base")
    if not isinstance(head, dict) or not isinstance(base, dict):
        raise PreviewCommentError("preview run omitted its pull-request revisions")
    head_repo = head.get("repo")
    base_repo = base.get("repo")
    head_repository = _repository_from_api_url(
        head_repo.get("url") if isinstance(head_repo, dict) else None,
        api_url=api_url,
    )
    base_repository = _repository_from_api_url(
        base_repo.get("url") if isinstance(base_repo, dict) else None,
        api_url=api_url,
    )
    head_sha = head.get("sha")
    base_sha = base.get("sha")
    if (
        base_repository != repository
        or head_repository is None
        or head_repository != head_repository_name
        or not isinstance(head_sha, str)
        or not isinstance(base_sha, str)
        or not SHA_RE.fullmatch(head_sha)
        or not SHA_RE.fullmatch(base_sha)
        or workflow_head_sha != head_sha
    ):
        raise PreviewCommentError("preview run pull-request identity is invalid")
    return PreviewRunIdentity(
        pr_number=pr_number,
        base_repository=base_repository,
        base_sha=base_sha,
        head_repository=head_repository,
        head_sha=head_sha,
    )


def select_preview_artifact(
    payload: Any, *, source_run_id: int, workflow_head_sha: str
) -> dict[str, Any]:
    if not isinstance(payload, dict) or not isinstance(payload.get("artifacts"), list):
        raise PreviewCommentError("preview artifacts response was invalid")
    matches = [
        item
        for item in payload["artifacts"]
        if isinstance(item, dict)
        and item.get("name") == ARTIFACT_NAME
        and item.get("expired") is False
    ]
    if len(matches) != 1:
        raise PreviewCommentError("expected exactly one current-run preview artifact")
    artifact = matches[0]
    size = artifact.get("size_in_bytes")
    artifact_run = artifact.get("workflow_run")
    if (
        not isinstance(artifact.get("id"), int)
        or not isinstance(size, int)
        or size < 1
        or size > MAX_ARTIFACT_BYTES
        or not isinstance(artifact_run, dict)
        or artifact_run.get("id") != source_run_id
        or artifact_run.get("head_sha") != workflow_head_sha
    ):
        raise PreviewCommentError("preview artifact identity or size is invalid")
    return artifact


def validate_preview_run_metadata(
    metadata: dict[str, Any], *, identity: PreviewRunIdentity
) -> None:
    pull_request = metadata.get("pull_request")
    if not isinstance(pull_request, dict):
        raise PreviewCommentError("preview artifact metadata omitted pull-request data")
    expected_pull_request = {
        "number": pull_request.get("number"),
        "base_repository": identity.base_repository,
        "base_sha": pull_request.get("base_sha"),
        "head_repository": identity.head_repository,
        "head_sha": identity.head_sha,
    }
    if identity.pr_number is not None:
        expected_pull_request["number"] = identity.pr_number
    if identity.base_sha is not None:
        expected_pull_request["base_sha"] = identity.base_sha
    if (
        pull_request != expected_pull_request
        or metadata.get("workflow_head_sha") != identity.head_sha
    ):
        raise PreviewCommentError("preview artifact metadata does not match its workflow run")


def download_preview_artifact(
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
            raise PreviewCommentError(
                f"preview artifact download failed with HTTP {error.code}"
            ) from error
        location = error.headers.get("Location")
        parsed = urllib.parse.urlparse(location or "")
        if parsed.scheme != "https" or not parsed.netloc:
            raise PreviewCommentError(
                "preview artifact redirect was not an absolute HTTPS URL"
            ) from error
        response = urllib.request.urlopen(urllib.request.Request(location), timeout=30)
    except OSError as error:
        raise PreviewCommentError("preview artifact download failed") from error
    with response:
        archive = response.read(MAX_ARCHIVE_DOWNLOAD_BYTES + 1)
    if len(archive) > MAX_ARCHIVE_DOWNLOAD_BYTES:
        raise PreviewCommentError("preview artifact archive exceeded the download limit")
    return archive


def upsert_comment(
    *,
    api_url: str,
    repository: str,
    pr_number: int,
    token: str,
    body: str,
) -> str:
    matches: list[dict[str, Any]] = []
    for page in range(1, MAX_COMMENT_PAGES + 1):
        payload = _github_request_json(
            api_url=api_url,
            path=f"/repos/{repository}/issues/{pr_number}/comments?per_page=100&page={page}",
            token=token,
        )
        if not isinstance(payload, list):
            raise PreviewCommentError("GitHub comments response was not a list")
        for item in payload:
            if not isinstance(item, dict):
                continue
            author = item.get("user")
            if (
                isinstance(item.get("body"), str)
                and item["body"].startswith(COMMENT_MARKER)
                and isinstance(author, dict)
                and author.get("login") == "github-actions[bot]"
            ):
                matches.append(item)
        if len(payload) < 100:
            break
    else:
        raise PreviewCommentError("pull request has too many comments to scan safely")

    if len(matches) > 1:
        raise PreviewCommentError("multiple Rocky preview comments already exist")
    if matches:
        comment_id = matches[0].get("id")
        if not isinstance(comment_id, int) or comment_id < 1:
            raise PreviewCommentError("existing preview comment has an invalid ID")
        result = _github_request_json(
            api_url=api_url,
            path=f"/repos/{repository}/issues/comments/{comment_id}",
            token=token,
            method="PATCH",
            payload={"body": body},
        )
    else:
        result = _github_request_json(
            api_url=api_url,
            path=f"/repos/{repository}/issues/{pr_number}/comments",
            token=token,
            method="POST",
            payload={"body": body},
        )
    if not isinstance(result, dict) or not isinstance(result.get("html_url"), str):
        raise PreviewCommentError("GitHub comment response omitted its URL")
    return result["html_url"]


def post_workflow_run_from_environment() -> str:
    repository = os.environ.get("GITHUB_REPOSITORY", "")
    if not REPOSITORY_RE.fullmatch(repository):
        raise PreviewCommentError("GITHUB_REPOSITORY is invalid")
    source_run_id_text = os.environ.get("SOURCE_RUN_ID", "")
    if not source_run_id_text.isdecimal() or int(source_run_id_text) < 1:
        raise PreviewCommentError("SOURCE_RUN_ID must be a positive integer")
    source_run_id = int(source_run_id_text)
    token = os.environ.get("GH_TOKEN", "")
    if not token:
        raise PreviewCommentError("GH_TOKEN is required")
    api_url = os.environ.get("GITHUB_API_URL", "https://api.github.com")

    run_payload = _github_request_json(
        api_url=api_url,
        path=f"/repos/{repository}/actions/runs/{source_run_id}",
        token=token,
    )
    identity = authenticate_preview_run(
        run_payload,
        expected_run_id=source_run_id,
        repository=repository,
        api_url=api_url,
    )
    artifacts_payload = _github_request_json(
        api_url=api_url,
        path=f"/repos/{repository}/actions/runs/{source_run_id}/artifacts?per_page=100",
        token=token,
    )
    artifact = select_preview_artifact(
        artifacts_payload,
        source_run_id=source_run_id,
        workflow_head_sha=identity.head_sha,
    )
    archive = download_preview_artifact(
        api_url=api_url,
        repository=repository,
        artifact_id=artifact["id"],
        token=token,
    )
    body, metadata = extract_preview_artifact(archive)
    validate_preview_run_metadata(metadata, identity=identity)
    pull_request = metadata["pull_request"]

    pr_payload = _github_request_json(
        api_url=api_url,
        path=f"/repos/{repository}/pulls/{pull_request['number']}",
        token=token,
    )
    verify_approval(
        pr_payload,
        approved_head_sha=pull_request["head_sha"],
        approved_base_sha=pull_request["base_sha"],
        approved_base_ref="main",
        approved_base_repository=repository,
        approved_head_repository=pull_request["head_repository"],
        approval_label=None,
    )
    return upsert_comment(
        api_url=api_url,
        repository=repository,
        pr_number=pull_request["number"],
        token=token,
        body=body,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    stage = subparsers.add_parser("stage")
    stage.add_argument("--source", type=Path, required=True)
    stage.add_argument("--output-directory", type=Path, required=True)
    subparsers.add_parser("post-workflow-run")
    args = parser.parse_args(argv)

    try:
        if args.command == "stage":
            target = stage_artifact(args.source, args.output_directory)
            print(f"staged bounded preview body at {target}")
        elif args.command == "post-workflow-run":
            url = post_workflow_run_from_environment()
            print(f"preview comment is current: {url}")
    except (ApprovalError, PreviewCommentError) as error:
        print(f"::error::{error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
