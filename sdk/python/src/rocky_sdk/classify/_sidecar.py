"""Read and add to the ``[classification]`` block of a model sidecar."""

from __future__ import annotations

import json
import os
import re
import tempfile
import tomllib
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._suggest import Suggestion

_HEADER = re.compile(r"[ \t]*\[[ \t]*classification[ \t]*\][ \t]*(#.*)?")
_BARE_KEY = re.compile(r"[A-Za-z0-9_-]+")


def _read(path: Path) -> str:
    raw = path.read_bytes()
    if raw.startswith(b"\xef\xbb\xbf"):
        raise ValueError(f"{path}: starts with a byte-order mark; remove it and try again")
    # newline="" semantics: keep CRLF as written, so the rest of the file is
    # left byte for byte.
    return raw.decode("utf-8")


def existing_classification(sidecar: str | Path) -> dict[str, str]:
    """The current ``[classification]`` block, or ``{}`` when there is none."""
    path = Path(sidecar)
    if not path.exists():
        return {}
    data = tomllib.loads(_read(path))
    block = data.get("classification", {})
    if not isinstance(block, dict):
        raise ValueError(f"{path}: 'classification' is not a table")
    return {str(k): str(v) for k, v in block.items()}


def _key(column: str) -> str:
    if _BARE_KEY.fullmatch(column):
        return column
    return json.dumps(column, ensure_ascii=False)


def apply_accepted(sidecar: str | Path, accepted: Iterable[Suggestion]) -> list[str]:
    """Add the accepted suggestions to the sidecar's ``[classification]`` block.

    Only adds keys. A column that already has a tag keeps it, whatever the
    suggestion says. The rest of the file is left byte for byte, line endings
    included. Returns the columns written. Refuses (``ValueError``) a file whose
    classification is not a plain ``[classification]`` table, rather than guess
    how to edit it. The sidecar must exist (``FileNotFoundError`` otherwise).
    The write is atomic: a crash leaves the old file or the new one.
    """
    path = Path(sidecar)
    text = _read(path)
    current = existing_classification(path)

    new: dict[str, str] = {}
    for s in accepted:
        if s.column not in current and s.column not in new:
            new[s.column] = s.tag
    if not new:
        return []

    eol = "\r\n" if "\r\n" in text else "\n"
    block = "".join(
        f"{_key(col)} = {json.dumps(tag, ensure_ascii=False)}{eol}" for col, tag in new.items()
    )
    src = text.splitlines(keepends=True)
    header_at = next(
        (i for i, line in enumerate(src) if _HEADER.fullmatch(line.rstrip("\r\n"))), None
    )
    if header_at is not None:
        header = src[header_at]
        if not header.endswith(("\n", "\r")):
            header += eol
        updated = "".join(src[:header_at]) + header + block + "".join(src[header_at + 1 :])
    elif current:
        raise ValueError(
            f"{path}: classification is not written as a [classification] table; "
            "add the tags by hand"
        )
    else:
        sep = "" if not text or text.endswith(("\n", "\r")) else eol
        updated = text + sep + eol + "[classification]" + eol + block

    try:
        parsed = tomllib.loads(updated)
    except tomllib.TOMLDecodeError as exc:
        raise ValueError(f"{path}: the edit would not be valid TOML; file unchanged") from exc
    got = parsed.get("classification", {})
    if any(got.get(col) != tag for col, tag in new.items()) or any(
        got.get(col) != tag for col, tag in current.items()
    ):
        raise ValueError(f"{path}: the edit did not produce the expected tags; file unchanged")

    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(updated.encode("utf-8"))
        os.chmod(tmp, path.stat().st_mode & 0o7777)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
    return list(new)
