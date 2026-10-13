---
title: Project Scaffold
description: Bootstrap a new Rocky + Dagster project
sidebar:
  order: 16
---

There are two ways to bootstrap a new project. Take the first for a bare
start, the second for a skeleton that already runs.

1. **`dg scaffold defs dagster_rocky.RockyComponent <name>`**: uses Dagster's
   built-in scaffolder via the registered entry point. Writes a bare
   `defs.yaml`.
2. **`init_rocky_project(target_dir)`**: Python helper that writes a complete
   skeleton with `defs.yaml`, `rocky.toml`, `.gitignore`, `models/`, and a
   `README.md`.

## `dg scaffold` (canonical)

Install `dagster-rocky` and the `dg` CLI finds `RockyComponent` on its
own. The discovery comes from the
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
├── rocky.toml         # DuckDB-backed full-refresh starter
├── .gitignore         # the DuckDB files and state the engine creates
├── models/            # empty, with .gitkeep
│   └── .gitkeep
└── README.md          # quickstart instructions
```

The default `rocky.toml` uses the **DuckDB local-execution adapter**. The
scaffold therefore runs end to end with no warehouse credentials. It copies
every table in each `src__<source>` schema of `warehouse.duckdb` into
`raw__<source>`, with `row_count` and `column_match` checks.

The file also carries a commented-out incremental strategy and a
`[pipeline.main.checks.freshness]` block. Turn them on once every source table
has a load timestamp column. The freshness block then gives your Dagster assets
a [freshness policy](/dagster/freshness/).

## Overwrite protection

`init_rocky_project` refuses to overwrite existing files by default; pass
`overwrite=True` to force:

```python
init_rocky_project(Path("my_pipeline"), overwrite=True)
```

This stops an accidental re-run from clobbering your own edits to
`rocky.toml`, `defs.yaml`, or `README.md`.

## Quickstart for end users

After scaffolding, the README guides users through:

```bash
# Install the rocky binary (once)
curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash

# Check the pipeline config (DuckDB, no credentials required)
rocky --config rocky.toml validate

# Run the pipeline locally
rocky --config rocky.toml run

# Launch the Dagster UI
dg dev
```
