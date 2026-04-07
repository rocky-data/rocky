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
"""

#: Starter ``rocky.toml`` using the DuckDB local-execution adapter so the
#: scaffold runs end-to-end without warehouse credentials.
ROCKY_TOML_TEMPLATE: str = """\
# Rocky pipeline configuration.
# Run with: rocky run --config rocky.toml

[source]
type = "duckdb"
path = ".rocky/source.duckdb"

[source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["source"]

[warehouse]
type = "duckdb"
path = ".rocky/warehouse.duckdb"

[target]
catalog_template = "warehouse"
schema_template = "raw__{source}"

[replication]
strategy = "incremental"
timestamp_column = "_loaded_at"

[checks]
enabled = true
row_count = true
column_match = true

[checks.freshness]
threshold_seconds = 86400  # 24 hours
"""

#: README quickstart for the scaffolded project.
README_TEMPLATE: str = """\
# Rocky pipeline

A Rocky data pipeline managed via the [`dagster-rocky`](https://github.com/rocky-data/rocky)
integration.

## Quickstart

```bash
# Install the rocky binary (once)
curl -sSL https://github.com/rocky-data/rocky/releases/latest/download/install.sh | sh

# Validate the pipeline locally (DuckDB, no credentials required)
rocky run --config rocky.toml

# Launch the Dagster UI
dg dev
```

## Layout

```
.
├── defs.yaml          # Dagster component definition
├── rocky.toml         # Rocky pipeline configuration
├── models/            # Rocky model files (.rocky / .toml)
└── README.md          # this file
```

## Next steps

1. Add a connector under `[source]` (e.g. Fivetran) — see the
   [`rocky-fivetran` docs](https://github.com/rocky-data/rocky/blob/main/engine/crates/rocky-fivetran/README.md).
2. Define your first model under `models/` and run `rocky test` to validate.
3. Open the asset graph in the Dagster UI and trigger a materialization.
"""


def init_rocky_project(target_dir: Path, *, overwrite: bool = False) -> Path:
    """Bootstrap a new Rocky + Dagster project skeleton in ``target_dir``.

    Creates the directory if it doesn't exist, then writes ``defs.yaml``,
    ``rocky.toml``, ``models/.gitkeep``, and ``README.md``. By default,
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
