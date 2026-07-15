#!/usr/bin/env python3
"""Extract one expected Rocky command object from bounded mixed stdout."""

from __future__ import annotations

import argparse
import json
import os
import stat
import sys
from pathlib import Path
from typing import Any


MAX_RAW_BYTES = 4 * 1024 * 1024
MAX_DECODE_ATTEMPTS = 4096
EXPECTED_COMMANDS = frozenset({"preview-create", "preview-diff", "preview-cost"})


class PreviewJsonError(RuntimeError):
    """Rocky stdout did not contain one bounded expected command object."""


def extract_command_object(raw: bytes, *, expected_command: str) -> dict[str, Any]:
    if expected_command not in EXPECTED_COMMANDS:
        raise PreviewJsonError("expected command is not allow-listed")
    if len(raw) > MAX_RAW_BYTES:
        raise PreviewJsonError("Rocky stdout exceeds the byte limit")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as error:
        raise PreviewJsonError("Rocky stdout is not UTF-8") from error

    decoder = json.JSONDecoder()
    matches: list[dict[str, Any]] = []
    offset = 0
    attempts = 0
    while offset < len(text):
        start = text.find("{", offset)
        if start < 0:
            break
        attempts += 1
        if attempts > MAX_DECODE_ATTEMPTS:
            raise PreviewJsonError("Rocky stdout contains too many JSON candidates")
        try:
            value, end = decoder.raw_decode(text, start)
        except json.JSONDecodeError:
            offset = start + 1
            continue
        if isinstance(value, dict) and value.get("command") == expected_command:
            matches.append(value)
        offset = max(end, start + 1)

    if len(matches) != 1:
        raise PreviewJsonError(
            f"Rocky stdout must contain exactly one {expected_command!r} object"
        )
    return matches[0]


def normalize_preview_json(
    source: Path,
    destination: Path,
    *,
    expected_command: str,
) -> None:
    if source.is_symlink() or not source.is_file():
        raise PreviewJsonError("Rocky stdout path is missing or unsafe")
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = -1
    try:
        descriptor = os.open(source, flags)
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise PreviewJsonError("Rocky stdout path is missing or unsafe")
        with os.fdopen(descriptor, "rb") as input_file:
            descriptor = -1
            raw = input_file.read(MAX_RAW_BYTES + 1)
    except OSError as error:
        raise PreviewJsonError("Rocky stdout could not be read") from error
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    payload = extract_command_object(raw, expected_command=expected_command)
    encoded = (
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n"
    ).encode()
    if len(encoded) > MAX_RAW_BYTES:
        raise PreviewJsonError("normalized Rocky JSON exceeds the byte limit")
    if destination.exists() or destination.is_symlink():
        raise PreviewJsonError("normalized JSON output path already exists")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(destination, flags, 0o600)
    except OSError as error:
        raise PreviewJsonError("normalized JSON output could not be created") from error
    try:
        with os.fdopen(descriptor, "wb") as output:
            descriptor = -1
            output.write(encoded)
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    parser.add_argument("expected_command", choices=sorted(EXPECTED_COMMANDS))
    args = parser.parse_args()
    try:
        normalize_preview_json(
            args.source,
            args.destination,
            expected_command=args.expected_command,
        )
    except PreviewJsonError as error:
        print(f"preview JSON normalization failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
