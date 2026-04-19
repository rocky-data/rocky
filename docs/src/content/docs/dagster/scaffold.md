---
title: Project Scaffold
description: Bootstrap a new Rocky + Dagster project
sidebar:
  order: 16
---

`dagster-rocky` 0.4 ships two ways to bootstrap a new project:

1. **`dg scaffold defs dagster_rocky.RockyComponent <name>`** — uses Dagster's
   built-in scaffolder via the registered entry point. Writes a bare
   `defs.yaml`.
2. **`init_rocky_project(target_dir)`** — Python helper that writes a complete
   skeleton with `defs.yaml`, `rocky.toml`, `models/`, and a `README.md`.

## `dg scaffold` (canonical)

After installing `dagster-rocky`, the `dg` CLI automatically discovers
`RockyComponent` via the
`[project.entry-points."dagster_dg_cli.registry_modules"]` entry in
`dagster-rocky`'s `pyproject.toml`.

```bash
uv add dagster-rocky
uv run dg list components | grep -i rocky
# dagster_rocky.RockyComponent

uv run dg scaffold defs dagster_rocky.RockyComponent my_pipeline
```

This writes a single `my_pipeline/defs.yaml` with `type: dagster_rocky.RockyComponent`.

## `init_rocky_project` (richer)

For a complete project skeleton, use the Python helper:

```python
from pathlib import Path
from dagster_rocky import init_rocky_project

init_rocky_project(Path("my_pipeline"))
```

After running, `my_pipeline/` contains:

```
my_pipeline/
├── defs.yaml          # type: dagster_rocky.RockyComponent + attributes
├── rocky.toml         # DuckDB-backed starter, freshness preconfigured
├── models/            # empty, with .gitkeep
│   └── .gitkeep
└── README.md          # quickstart instructions
```

The default `rocky.toml` uses the **DuckDB local-execution adapter** so the
scaffold runs end-to-end without warehouse credentials. A [freshness
policy](./freshness.md) is preconfigured so you can see it flow through to
Dagster assets immediately.

## Overwrite protection

`init_rocky_project` refuses to overwrite existing files by default — pass
`overwrite=True` to force:

```python
init_rocky_project(Path("my_pipeline"), overwrite=True)
```

This protects accidental re-runs from clobbering user edits to `rocky.toml`,
`defs.yaml`, or `README.md`.

## Quickstart for end users

After scaffolding, the README guides users through:

```bash
# Install the rocky binary (once)
curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash

# Validate the pipeline locally (DuckDB, no credentials required)
rocky run --config rocky.toml

# Launch the Dagster UI
dg dev
```
