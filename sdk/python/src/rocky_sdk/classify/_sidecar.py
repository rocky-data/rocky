"""Read and add to the ``[classification]`` block of a model sidecar."""

from __future__ import annotations

import json
import re
import tomllib
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._suggest import Suggestion

_HEADER = re.compile(r"^\s*\[\s*classification\s*\]\s*(#.*)?$")
_BARE_KEY = re.compile(r"^[A-Za-z0-9_-]+$")


def existing_classification(sidecar: str | Path) -> dict[str, str]:
    """The current ``[classification]`` block, or ``{}`` when there is none."""
    path = Path(sidecar)
    if not path.exists():
        return {}
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    block = data.get("classification", {})
    if not isinstance(block, dict):
        raise ValueError(f"{path}: 'classification' is not a table")
    return {str(k): str(v) for k, v in block.items()}


def _key(column: str) -> str:
    return column if _BARE_KEY.match(column) else json.dumps(column)


def apply_accepted(sidecar: str | Path, accepted: Iterable[Suggestion]) -> list[str]:
    """Add the accepted suggestions to the sidecar's ``[classification]`` block.

    Only adds keys. A column that already has a tag keeps it, whatever the
    suggestion says. The rest of the file is left byte for byte. Returns the
    columns written. Refuses (``ValueError``) a file whose classification is
    not a plain ``[classification]`` table, rather than guess how to edit it.
    """
    path = Path(sidecar)
    text = path.read_text(encoding="utf-8")
    current = existing_classification(path)

    new: dict[str, str] = {}
    for s in accepted:
        if s.column not in current and s.column not in new:
            new[s.column] = s.tag
    if not new:
        return []

    lines = [f"{_key(col)} = {json.dumps(tag)}" for col, tag in new.items()]
    src = text.splitlines(keepends=True)
    header_at = next((i for i, line in enumerate(src) if _HEADER.match(line)), None)
    if header_at is not None:
        block = "".join(line + "\n" for line in lines)
        updated = "".join(src[: header_at + 1]) + block + "".join(src[header_at + 1 :])
    elif current:
        raise ValueError(
            f"{path}: classification is not written as a [classification] table; "
            "add the tags by hand"
        )
    else:
        sep = "" if text.endswith("\n") or not text else "\n"
        updated = text + sep + "\n[classification]\n" + "".join(line + "\n" for line in lines)

    parsed = tomllib.loads(updated)
    got = parsed.get("classification", {})
    if any(got.get(col) != tag for col, tag in new.items()) or any(
        got.get(col) != tag for col, tag in current.items()
    ):
        raise ValueError(f"{path}: the edit did not produce the expected tags; file unchanged")
    path.write_text(updated, encoding="utf-8")
    return list(new)
