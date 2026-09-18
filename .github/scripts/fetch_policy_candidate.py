#!/usr/bin/env python3
"""Materialize a bounded candidate ``.github`` tree as non-executable data.

The credential-containment workflow runs under ``pull_request_target`` so its
checker must come from the trusted workflow revision.  It must also inspect the
candidate policy without checking out candidate Git objects into the runner's
default-branch cache domain.  This helper reads the exact candidate commit via
GitHub's Git Data API, rejects non-regular entries and unsafe paths, and writes
only a bounded ``.github`` snapshot for the trusted checker.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import http.client
import json
import os
import re
import shutil
import ssl
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any


MAX_TREE_ENTRIES = 256
MAX_BLOB_BYTES = 512 * 1024
MAX_TOTAL_BYTES = 4 * 1024 * 1024
MAX_API_RESPONSE_BYTES = 8 * 1024 * 1024
MAX_PATH_BYTES = 512
REQUEST_TIMEOUT_SECONDS = 30
# A transient GitHub API error used to fail the whole required check before it
# looked at any diff, costing a human re-run each time. Retrying does not weaken
# the gate: the candidate is still fetched exactly once, every validation below
# still runs on it, and a failure that persists still fails the job.
REQUEST_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = (2.0, 6.0)
# 5xx is the server failing, 429 is the documented rate-limit status, and 403
# carries the rate limit when the quota is exhausted. A 401, 404 or 422 is the
# request being wrong, which repeating cannot fix.
RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})

REPOSITORY_RE = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")
SHA_RE = re.compile(r"[0-9a-fA-F]{40}")


class PolicyCandidateError(RuntimeError):
    """The candidate snapshot could not be authenticated or materialized."""


@dataclass(frozen=True)
class CandidateBlob:
    path: str
    sha: str
    size: int


JsonFetcher = Callable[[str, str, str], dict[str, Any]]


class _RejectRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> None:
        return None


def _validate_api_url(value: str) -> str:
    parsed = urllib.parse.urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or any(part == ".." for part in parsed.path.split("/"))
    ):
        raise PolicyCandidateError("GITHUB_API_URL must be a plain HTTPS API origin")
    return value.rstrip("/")


def _validate_repository(value: str) -> str:
    if not REPOSITORY_RE.fullmatch(value) or ".." in value.split("/"):
        raise PolicyCandidateError("candidate repository identity is invalid")
    return value


def _validate_sha(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not SHA_RE.fullmatch(value):
        raise PolicyCandidateError(f"{field} must be a full Git SHA")
    return value.lower()


def _is_rate_limited(error: urllib.error.HTTPError) -> bool:
    """Tell a rate-limited 403 from a 403 that means the token cannot do this."""

    headers = getattr(error, "headers", None)
    if headers is None:
        return False
    return headers.get("x-ratelimit-remaining") == "0" or bool(
        headers.get("retry-after")
    )


def _describe_failure(error: BaseException) -> tuple[bool, str]:
    """Say whether one failed attempt is worth repeating, and name what happened.

    The description is what reaches the log. Every failure used to print the
    same line, so a 502, a DNS failure and an exhausted rate limit were
    indistinguishable and nobody could tell whether a retry would have helped.
    """

    if isinstance(error, urllib.error.HTTPError):
        if error.code == 403 and _is_rate_limited(error):
            return True, "HTTP 403 (rate limit exhausted)"
        return error.code in RETRYABLE_STATUSES, f"HTTP {error.code}"
    reason = getattr(error, "reason", None)
    # A certificate that does not verify is not a blip. Retrying would say
    # "transient" in the log for a failed TLS identity check, and would give an
    # intermittent interceptor three chances instead of one.
    if isinstance(error, ssl.SSLCertVerificationError) or isinstance(
        reason, ssl.SSLCertVerificationError
    ):
        return False, f"TLS certificate verification failed: {reason or error}"
    detail = f"{type(error).__name__}: {reason}" if reason else type(error).__name__
    # A connection that never produced a response: DNS, TLS, reset, timeout.
    return True, detail


def _request_json(api_url: str, endpoint: str, token: str) -> dict[str, Any]:
    url = f"{api_url}/{endpoint.lstrip('/')}"
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "rocky-credential-containment-policy",
        },
    )
    opener = urllib.request.build_opener(_RejectRedirects())
    attempts: list[str] = []
    for attempt in range(1, REQUEST_ATTEMPTS + 1):
        try:
            with opener.open(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                payload = response.read(MAX_API_RESPONSE_BYTES + 1)
            break
        # http.client.HTTPException is NOT an OSError, so a response that dies
        # mid-body (IncompleteRead) would otherwise escape as a traceback: no
        # retry, and no line saying which call failed. That is the failure this
        # change exists to remove, so it must be caught here too.
        except (OSError, urllib.error.URLError, http.client.HTTPException) as error:
            retryable, detail = _describe_failure(error)
            attempts.append(f"attempt {attempt}: {detail}")
            if not retryable or attempt == REQUEST_ATTEMPTS:
                raise PolicyCandidateError(
                    f"GitHub API request failed for {endpoint} "
                    f"({'; '.join(attempts)})"
                ) from error
            time.sleep(RETRY_BACKOFF_SECONDS[attempt - 1])
    if len(payload) > MAX_API_RESPONSE_BYTES:
        raise PolicyCandidateError("GitHub API response exceeds the byte limit")
    try:
        decoded = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PolicyCandidateError("GitHub API returned invalid JSON") from error
    if not isinstance(decoded, dict):
        raise PolicyCandidateError("GitHub API response must be an object")
    return decoded


def _tree_entries(
    payload: dict[str, Any], *, expected_sha: str
) -> list[dict[str, Any]]:
    if _validate_sha(payload.get("sha"), field="tree SHA") != expected_sha:
        raise PolicyCandidateError("GitHub API returned a different tree")
    if payload.get("truncated") is not False:
        raise PolicyCandidateError("candidate tree response is truncated")
    entries = payload.get("tree")
    if not isinstance(entries, list):
        raise PolicyCandidateError("candidate tree entries are missing")
    if len(entries) > MAX_TREE_ENTRIES:
        raise PolicyCandidateError("candidate tree exceeds the entry limit")
    if not all(isinstance(entry, dict) for entry in entries):
        raise PolicyCandidateError("candidate tree contains an invalid entry")
    return entries


def _validate_relative_path(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise PolicyCandidateError("candidate tree path is invalid")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as error:
        raise PolicyCandidateError("candidate tree path is not UTF-8") from error
    parts = value.split("/")
    if (
        len(encoded) > MAX_PATH_BYTES
        or value.startswith("/")
        or "\\" in value
        or "\x00" in value
        or any(part in {"", ".", ".."} for part in parts)
        or any(len(part.encode("utf-8")) > 255 for part in parts)
    ):
        raise PolicyCandidateError("candidate tree path is unsafe")
    return value


def _candidate_blobs(
    payload: dict[str, Any], *, expected_sha: str
) -> list[CandidateBlob]:
    entries = _tree_entries(payload, expected_sha=expected_sha)
    seen: set[str] = set()
    blobs: list[CandidateBlob] = []
    total_size = 0

    for entry in entries:
        path = _validate_relative_path(entry.get("path"))
        if path in seen:
            raise PolicyCandidateError("candidate tree contains a duplicate path")
        seen.add(path)
        entry_type = entry.get("type")
        mode = entry.get("mode")
        if entry_type == "tree":
            if mode != "040000":
                raise PolicyCandidateError("candidate tree directory mode is invalid")
            _validate_sha(entry.get("sha"), field="directory SHA")
            continue
        if entry_type != "blob" or mode not in {"100644", "100755"}:
            raise PolicyCandidateError("candidate tree contains a non-regular entry")
        size = entry.get("size")
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise PolicyCandidateError("candidate blob size is invalid")
        if size > MAX_BLOB_BYTES:
            raise PolicyCandidateError("candidate blob exceeds the byte limit")
        total_size += size
        if total_size > MAX_TOTAL_BYTES:
            raise PolicyCandidateError("candidate snapshot exceeds the byte limit")
        blobs.append(
            CandidateBlob(
                path=path,
                sha=_validate_sha(entry.get("sha"), field="blob SHA"),
                size=size,
            )
        )

    blob_paths = {blob.path for blob in blobs}
    for path in blob_paths:
        parts = path.split("/")
        if any("/".join(parts[:index]) in blob_paths for index in range(1, len(parts))):
            raise PolicyCandidateError(
                "candidate tree contains a file/directory conflict"
            )
    return sorted(blobs, key=lambda blob: blob.path)


def _decode_blob(payload: dict[str, Any], *, expected: CandidateBlob) -> bytes:
    if _validate_sha(payload.get("sha"), field="blob response SHA") != expected.sha:
        raise PolicyCandidateError("GitHub API returned a different blob")
    size = payload.get("size")
    if isinstance(size, bool) or size != expected.size:
        raise PolicyCandidateError("candidate blob size changed during fetch")
    if payload.get("encoding") != "base64" or not isinstance(
        payload.get("content"), str
    ):
        raise PolicyCandidateError("candidate blob encoding is invalid")
    content = payload["content"]
    if any(character.isspace() and character not in "\r\n" for character in content):
        raise PolicyCandidateError("candidate blob base64 contains invalid whitespace")
    try:
        decoded = base64.b64decode(
            content.replace("\r", "").replace("\n", ""), validate=True
        )
    except (binascii.Error, ValueError) as error:
        raise PolicyCandidateError("candidate blob base64 is invalid") from error
    if len(decoded) != expected.size or len(decoded) > MAX_BLOB_BYTES:
        raise PolicyCandidateError("candidate blob content does not match its size")
    return decoded


def _write_regular_file(path: Path, content: bytes) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.parent.chmod(0o700)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as destination:
            descriptor = -1
            destination.write(content)
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def fetch_policy_candidate(
    *,
    api_url: str,
    repository: str,
    commit_sha: str,
    token: str,
    output_root: Path,
    request_json: JsonFetcher = _request_json,
) -> int:
    api_url = _validate_api_url(api_url)
    repository = _validate_repository(repository)
    commit_sha = _validate_sha(commit_sha, field="candidate commit SHA")
    if not token or token != token.strip() or "\r" in token or "\n" in token:
        raise PolicyCandidateError("GitHub token is missing or invalid")
    if output_root.exists() or output_root.is_symlink():
        raise PolicyCandidateError("candidate output path already exists")
    parent = output_root.parent
    if parent.is_symlink() or not parent.is_dir():
        raise PolicyCandidateError("candidate output parent is missing or unsafe")

    encoded_repository = urllib.parse.quote(repository, safe="/")
    commit_endpoint = f"repos/{encoded_repository}/git/commits/{commit_sha}"
    commit = request_json(api_url, commit_endpoint, token)
    if _validate_sha(commit.get("sha"), field="commit response SHA") != commit_sha:
        raise PolicyCandidateError("GitHub API returned a different candidate commit")
    commit_tree = commit.get("tree")
    if not isinstance(commit_tree, dict):
        raise PolicyCandidateError("candidate commit tree is missing")
    root_tree_sha = _validate_sha(commit_tree.get("sha"), field="root tree SHA")

    root_tree = request_json(
        api_url,
        f"repos/{encoded_repository}/git/trees/{root_tree_sha}",
        token,
    )
    root_entries = _tree_entries(root_tree, expected_sha=root_tree_sha)
    github_entries = [entry for entry in root_entries if entry.get("path") == ".github"]
    if len(github_entries) != 1:
        raise PolicyCandidateError("candidate .github tree is missing or ambiguous")
    github_entry = github_entries[0]
    if github_entry.get("type") != "tree" or github_entry.get("mode") != "040000":
        raise PolicyCandidateError("candidate .github entry is not a directory")
    github_tree_sha = _validate_sha(github_entry.get("sha"), field=".github tree SHA")

    github_tree = request_json(
        api_url,
        f"repos/{encoded_repository}/git/trees/{github_tree_sha}?recursive=1",
        token,
    )
    blobs = _candidate_blobs(github_tree, expected_sha=github_tree_sha)

    staging = Path(tempfile.mkdtemp(prefix=".candidate-policy-", dir=parent))
    staging.chmod(0o700)
    try:
        github_directory = staging / ".github"
        github_directory.mkdir(mode=0o700)
        for blob in blobs:
            payload = request_json(
                api_url,
                f"repos/{encoded_repository}/git/blobs/{blob.sha}",
                token,
            )
            content = _decode_blob(payload, expected=blob)
            _write_regular_file(
                github_directory.joinpath(*blob.path.split("/")), content
            )
        staging.rename(output_root)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return len(blobs)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("output_root", type=Path)
    args = parser.parse_args()
    try:
        count = fetch_policy_candidate(
            api_url=os.environ.get("GITHUB_API_URL", ""),
            repository=os.environ.get("CANDIDATE_REPOSITORY", ""),
            commit_sha=os.environ.get("CANDIDATE_SHA", ""),
            token=os.environ.get("GH_TOKEN", ""),
            output_root=args.output_root,
        )
    except PolicyCandidateError as error:
        print(f"::error::{error}", file=sys.stderr)
        # The cause carries the status or the connection reason. Discarding it
        # is why three separate failures once produced one identical line and
        # nobody could tell which call had failed, or why.
        if error.__cause__ is not None:
            print(
                f"::error::caused by {type(error.__cause__).__name__}: "
                f"{error.__cause__}",
                file=sys.stderr,
            )
        return 1
    print(f"materialized {count} candidate policy files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
