"""Project bootstrap helper for new Rocky + Dagster projects.

The companion to ``dg scaffold defs dagster_rocky.RockyComponent`` — that
command (registered via the ``dagster_dg_cli.registry_modules`` entry
point in ``pyproject.toml``) writes a bare ``defs.yaml`` for the
component, but a complete Rocky project also needs a ``rocky.toml``,
a ``models/`` directory, and ideally a ``README.md`` with quickstart
instructions. :func:`init_rocky_project` writes all of those into a
target directory in one call.

Usage::

    from pathlib import Path
    from dagster_rocky.scaffold import init_rocky_project

    init_rocky_project(Path("my_pipeline"))

After running, ``my_pipeline/`` contains::

    my_pipeline/
    ├── defs.yaml          # type: dagster_rocky.RockyComponent + attributes
    ├── rocky.toml         # DuckDB-backed starter pipeline
    ├── models/            # empty, with .gitkeep
    │   └── .gitkeep
    ├── .gitignore         # the DuckDB file and state the engine creates
    └── README.md          # quickstart instructions

The default ``rocky.toml`` template uses the DuckDB local-execution
adapter so no warehouse credentials are required to validate the
scaffold runs end-to-end.
"""

from __future__ import annotations

from pathlib import Path

#: The default ``defs.yaml`` template emitted by :func:`init_rocky_project`.
#: Mirrors the canonical ``RockyComponent`` configuration with sensible
#: defaults that point at the sibling ``rocky.toml`` and ``models/`` dir.
DEFS_YAML_TEMPLATE: str = """\
# dagster-rocky component definition.
# See https://github.com/rocky-data/rocky for more details.
type: dagster_rocky.RockyComponent
attributes:
  binary_path: rocky
  config_path: rocky.toml
  models_dir: models

  # execution_mode: streaming (default) | pipes
  #   "streaming" — rocky stderr forwards to context.log, Dagster events
  #                 are built from rocky's JSON output.
  #   "pipes"     — Dagster Pipes protocol; rocky emits MaterializationEvent /
  #                 AssetCheckEvaluation directly over the wire.
  # execution_mode: streaming

  # strict_doctor (bool, default false) + strict_doctor_checks (list[str], default [])
  #   When strict_doctor is true, runs `rocky doctor` at startup and fails
  #   fast on critical checks. strict_doctor_checks scopes the gate:
  #     []                    → fail on any critical check
  #     [state_rw, auth, ...] → fail only on the listed critical checks;
  #                             others are logged as warnings.
  #   Default (false + []) preserves the tolerant behaviour — doctor is
  #   not run on startup.
  # strict_doctor: false
  # strict_doctor_checks: []
"""

#: Starter ``rocky.toml`` using the DuckDB local-execution adapter so the
#: scaffold runs end-to-end without warehouse credentials.
ROCKY_TOML_TEMPLATE: str = """\
# Rocky pipeline configuration.
# Validate with: rocky --config rocky.toml validate
# Run with:      rocky --config rocky.toml run

# One local DuckDB file next to this one holds both the source schemas and
# the replicated tables, so the scaffold runs end-to-end without warehouse
# credentials. Relative paths resolve against the directory `rocky` runs
# from, so run it from this directory.
[adapter]
type = "duckdb"
path = "warehouse.duckdb"

# Every table in a source schema named `src__<source>` is copied in full
# into `raw__<source>` on each run. No particular column is required.
[pipeline.main]
strategy = "full_refresh"

[pipeline.main.source.discovery]
adapter = "default"

[pipeline.main.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["source"]

# In DuckDB the catalog is the database file's stem: `warehouse.duckdb`
# is catalog `warehouse`. Rename the two together.
[pipeline.main.target]
catalog_template = "warehouse"
schema_template = "raw__{source}"

[pipeline.main.target.governance]
auto_create_schemas = true

[pipeline.main.checks]
row_count = true
column_match = true

# Once every `src__*` table carries a load timestamp, copy only new rows
# and fail the run when the newest row is older than a day. Both settings
# need that column on every source table.
#
# [pipeline.main]
# strategy = "incremental"
# timestamp_column = "_loaded_at"
#
# [pipeline.main.checks.freshness]
# threshold_seconds = 86400
"""

#: ``.gitignore`` for the scaffolded project: the files the engine creates
#: next to ``rocky.toml`` when the starter pipeline runs.
GITIGNORE_TEMPLATE: str = """\
# Created by `rocky run` on the starter pipeline.
warehouse.duckdb
.rocky/
.rocky-state.redb*
"""

#: README quickstart for the scaffolded project.
README_TEMPLATE: str = """\
# Rocky pipeline

A Rocky data pipeline managed via the [`dagster-rocky`](https://github.com/rocky-data/rocky)
integration.

## Quickstart

```bash
# Install the rocky binary (once)
curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash

# Check the pipeline config (DuckDB, no credentials required)
rocky --config rocky.toml validate

# Replicate every `src__*` schema in warehouse.duckdb into `raw__*`
rocky --config rocky.toml run

# Launch the Dagster UI
dg dev
```

## Layout

```
.
├── defs.yaml          # Dagster component definition
├── rocky.toml         # Rocky pipeline configuration
├── models/            # Rocky model files (.rocky / .toml)
├── .gitignore         # the DuckDB file and state the engine creates
└── README.md          # this file
```

## Next steps

1. Point `[pipeline.main.source.discovery]` at a connector adapter (e.g. Fivetran) — see the
   [Fivetran adapter docs](https://rocky-data.dev/reference/adapters/fivetran/).
2. Define your first model under `models/` and run `rocky test` to validate.
3. Open the asset graph in the Dagster UI and trigger a materialization.
"""


def init_rocky_project(target_dir: Path, *, overwrite: bool = False) -> Path:
    """Bootstrap a new Rocky + Dagster project skeleton in ``target_dir``.

    Creates the directory if it doesn't exist, then writes ``defs.yaml``,
    ``rocky.toml``, ``models/.gitkeep``, ``.gitignore``, and ``README.md``. By default,
    refuses to overwrite existing files — pass ``overwrite=True`` to
    replace any pre-existing files.

    Args:
        target_dir: Directory to bootstrap. May or may not exist; will
            be created if missing.
        overwrite: If ``True``, replaces existing files. Defaults to
            ``False`` so accidental re-runs don't clobber user edits.

    Returns:
        The (resolved) target directory path.

    Raises:
        FileExistsError: If ``overwrite=False`` and any of the target
            files already exist.
    """
    target_dir = target_dir.resolve()
    target_dir.mkdir(parents=True, exist_ok=True)
    models_dir = target_dir / "models"
    models_dir.mkdir(exist_ok=True)

    files: dict[Path, str] = {
        target_dir / "defs.yaml": DEFS_YAML_TEMPLATE,
        target_dir / "rocky.toml": ROCKY_TOML_TEMPLATE,
        target_dir / "README.md": README_TEMPLATE,
        target_dir / ".gitignore": GITIGNORE_TEMPLATE,
        models_dir / ".gitkeep": "",
    }

    if not overwrite:
        existing = [p for p in files if p.exists()]
        if existing:
            raise FileExistsError(
                f"Refusing to overwrite existing files: {[str(p) for p in existing]}. "
                "Pass overwrite=True to force."
            )

    for path, content in files.items():
        path.write_text(content, encoding="utf-8")

    return target_dir
