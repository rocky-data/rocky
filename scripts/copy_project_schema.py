#!/usr/bin/env python3
"""Copy the schemars-generated `rocky_project.schema.json` into the VS Code
extension's `schemas/` directory, prepending an auto-generation banner.

Invoked by the `codegen-vscode-project-schema` just recipe at the monorepo
root. The destination file (`editors/vscode/schemas/rocky-project.schema.json`)
is the path the extension's `package.json` points its `jsonValidation` at.
"""

from __future__ import annotations

import json
import pathlib
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
SRC = REPO_ROOT / "schemas" / "rocky_project.schema.json"
DST = REPO_ROOT / "editors" / "vscode" / "schemas" / "rocky-project.schema.json"

BANNER = (
    "AUTO-GENERATED — do not edit by hand. "
    "Source: schemas/rocky_project.schema.json (derived from rocky-core RockyConfig). "
    "Run `just codegen` from the monorepo root to regenerate."
)


def main() -> int:
    if not SRC.is_file():
        print(f"error: {SRC} not found — run `just codegen-rust` first", file=sys.stderr)
        return 1
    schema = json.loads(SRC.read_text())
    annotated = {"$comment": BANNER, **schema}
    DST.parent.mkdir(parents=True, exist_ok=True)
    DST.write_text(json.dumps(annotated, indent=2, ensure_ascii=False) + "\n")
    print(f"wrote {DST.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
