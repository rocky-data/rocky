"""Fixture setup — a hermetic, throwaway copy of a pinned eval project.

Each scenario attempt runs in its own temp directory *outside* the repo tree, so
(a) the agent's file writes never dirty the working copy, (b) CLAUDE.md / skill
discovery walking up from the cwd cannot reach the repo's agent guidance (the
agent-under-test must be isolated from it), and (c) runs cannot interfere with
each other. The fixture's `poc.duckdb` is seeded from `data/seed.sql` before the
agent starts so the MCP grounding tools have real rows to return.
"""

from __future__ import annotations

import shutil
import subprocess
import tempfile
from pathlib import Path

_FIXTURES_DIR = Path(__file__).resolve().parent.parent / "fixtures"


def fixture_source(name: str) -> Path:
    src = _FIXTURES_DIR / name
    if not src.is_dir():
        raise FileNotFoundError(f"unknown fixture '{name}' (looked in {_FIXTURES_DIR})")
    return src


def prepare_fixture(name: str, duckdb_bin: str) -> Path:
    """Copy fixture ``name`` into a fresh temp dir and seed its warehouse.

    Returns the path to the prepared project directory (containing `rocky.toml`,
    `models/`, `data/seed.sql`, and a seeded `poc.duckdb`). The caller owns the
    directory and should remove it when done via :func:`cleanup_fixture`.
    """
    src = fixture_source(name)
    workdir = Path(tempfile.mkdtemp(prefix=f"rocky-eval-{name}-"))
    project = workdir / name
    shutil.copytree(src, project)

    seed = project / "data" / "seed.sql"
    if seed.is_file():
        # Seed the persistent DuckDB file the MCP grounding tools read from.
        with seed.open() as handle:
            subprocess.run(
                [duckdb_bin, "poc.duckdb"],
                cwd=project,
                stdin=handle,
                capture_output=True,
                text=True,
                check=True,
            )
    return project


def cleanup_fixture(project: Path) -> None:
    """Remove the temp workdir that :func:`prepare_fixture` created."""
    # project is <workdir>/<name>; remove the whole workdir.
    shutil.rmtree(project.parent, ignore_errors=True)
