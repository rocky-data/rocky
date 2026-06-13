"""Drive the Rocky engine end-to-end with rocky-sdk.

Run it with no arguments to spin up a throwaway, pre-seeded DuckDB playground
(via ``rocky playground`` — no credentials) and exercise the client against it:

    python quickstart.py

Or point it at an existing project's ``rocky.toml``:

    python quickstart.py path/to/rocky.toml

Requires the ``rocky`` binary on PATH (engine v1.34.0+). The engine has DuckDB
embedded, so the playground path needs nothing else installed.
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from pathlib import Path

from rocky_sdk import RockyClient
from rocky_sdk.exceptions import RockyCommandError


def bootstrap_playground() -> Path:
    """Generate a throwaway, pre-seeded DuckDB playground; return its rocky.toml."""
    project = Path(tempfile.mkdtemp(prefix="rocky-sdk-demo-")) / "playground"
    subprocess.run(
        ["rocky", "playground", str(project), "--template", "quickstart"],
        check=True,
        capture_output=True,
        text=True,
    )
    return project / "rocky.toml"


def main() -> int:
    config = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else bootstrap_playground()
    # Run from the project directory so the relative paths in rocky.toml (the
    # DuckDB file, models/, data/) resolve.
    os.chdir(config.parent)

    client = RockyClient(config_path=config.name)

    # 1. Compile — type-check the models. Returns a typed CompileResult.
    compiled = client.compile()
    print(f"compile: {compiled.models} models, has_errors={compiled.has_errors}")
    for diag in compiled.diagnostics:
        print(f"  {diag.severity} {diag.code}: {diag.message}")

    # 2. Inspect the dependency graph.
    dag = client.dag()
    print(f"dag: {len(dag.nodes)} nodes, {len(dag.edges)} edges")

    # 3. Column-level lineage for one model.
    lineage = client.lineage("revenue_summary")
    print(f"lineage(revenue_summary): {type(lineage).__name__}")

    # 4. Run the pipeline — materialize the model DAG against DuckDB. An empty
    #    filter runs everything. For a transformation pipeline the signal is the
    #    materialization count (tables_copied is a replication-only metric).
    run = client.run(filter="")
    print(
        f"run: {len(run.materializations)} models materialized, "
        f"{run.tables_failed} failed, {run.duration_ms} ms"
    )
    for m in run.materializations:
        print(f"  materialized {'.'.join(m.asset_key)}")

    # 5. Errors are typed — branch on the cause, not a string.
    try:
        client.lineage("does_not_exist")
    except RockyCommandError as exc:
        print(f"expected typed error: {type(exc).__name__} (exit {exc.returncode})")

    print("ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
