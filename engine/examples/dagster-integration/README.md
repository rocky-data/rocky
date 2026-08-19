# Dagster Integration

Configuration for orchestrating this Rocky project from Dagster with the
`dagster-rocky` package. It holds a `rocky.toml`, two models, a `defs.yaml`,
and a `definitions.py`.

## How dagster-rocky reaches the engine

```
  Dagster process
      │
      ├── RockyComponent  ── builds assets from a cached state file
      │        │                  at code-location load time
      │        │
      │        └── write_state_to_path()  ── runs the CLI, caches JSON
      │                   │
      │                   ├──► rocky discover   sources → AssetSpecs
      │                   ├──► rocky compile    diagnostics → asset checks
      │                   ├──► rocky optimize   strategy hints → metadata
      │                   │                     (surface_optimize_metadata, on by default)
      │                   └──► rocky dag        full graph (dag_mode=True only)
      │
      └── RockyResource   ── runs the CLI inside an asset body
                   └──► rocky run / plan / apply / discover
```

`RockyComponent` splits the work in two. `write_state_to_path(state_path)`
shells out to the CLI and writes the JSON. `build_defs_from_state()` reads
that JSON on every code-location reload and builds the assets from it.

One opt-in changes that. With `discover_on_missing_state: true`, a load that
finds no state file calls `write_state_to_path` first, so that load does shell
out. The attribute defaults to `false` and `defs.yaml` here leaves it unset.

`RockyResource` is the imperative alternative. You call `rocky.run(...)` or
`rocky.plan(...)` from inside an `@dg.asset` function.

## Files

```
dagster-integration/
  rocky.toml               # Rocky pipeline config
  defs.yaml                # RockyComponent attributes
  definitions.py           # RockyResource assets
  models/
    stg_orders.rocky       + stg_orders.toml
    fct_order_summary.rocky + fct_order_summary.toml
```

`defs.yaml` sets three attributes:

```yaml
type: dagster_rocky.RockyComponent
attributes:
  binary_path: rocky
  config_path: rocky.toml
  state_path: .rocky-state.redb
```

## Check the Rocky side first

Run these from the repository root. Rocky reads `rocky.toml` from the working
directory by default, so the `cd` is what lets the rest omit `--config`.

```bash
cd engine/examples/dagster-integration
rocky compile
rocky dag
```

`rocky compile` reports two models. `rocky dag` prints the unified graph.

## This directory is not a runnable Dagster project

There is no `pyproject.toml` here, and none in any parent directory. So
`uv add dagster-rocky` fails from this directory:

```
error: No `pyproject.toml` found in current directory or any parent directory
```

`uv run dg dev` fails too, for a different reason. `uv run` does not need a
`pyproject.toml`. It fails because `dg` is not installed here:

```
error: Failed to spawn: `dg`
  Caused by: No such file or directory (os error 2)
```

Treat these files as fragments to copy into a Dagster project you have already
scaffolded.

To use them, create a `dg` project and add `dagster-rocky` to it. Copy
`rocky.toml`, `models/`, and `defs.yaml` across. Then point `config_path` at
your copy of `rocky.toml`.

## `rocky discover` returns nothing here

`RockyComponent` builds its assets from `rocky discover`. This example's
pipeline is a replication pipeline whose source schema pattern needs a
`raw__*` schema in DuckDB. A fresh database has none, so discover reports
zero sources and the component builds zero assets:

```bash
rocky --output json discover
```

```json
{
  "version": "...",
  "command": "discover",
  "sources": []
}
```

Point the pipeline at a warehouse that holds `raw__*` schemas to see assets
appear.

## Where `--config` goes

`--config` is a top-level flag, not a per-command flag. It comes before the
subcommand, never after:

```bash
rocky --config rocky.toml compile   # works
rocky compile --config rocky.toml   # error: unexpected argument '--config' found
```

The commands above omit it, because `--config` already defaults to
`rocky.toml` in the working directory.

`RockyComponent` and `RockyResource` build the argument list through
`rocky-sdk`, which places `--config` first. Set `config_path` and let the SDK
order the flags.
