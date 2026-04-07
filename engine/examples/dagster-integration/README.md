# Dagster Integration

Orchestrate a Rocky pipeline with Dagster using the `dagster-rocky` package.

## Architecture

```
Dagster (orchestrator)
  |
  +-- dagster-rocky.RockyComponent
  |     |
  |     +-- rocky discover  -->  AssetSpecs (cached, no API calls on reload)
  |     +-- rocky plan      -->  Preview SQL
  |     +-- rocky run       -->  Execute pipeline
  |
  +-- rocky.toml (Rocky config)
  +-- models/ (Rocky models)
```

## Project Structure

```
dagster-integration/
  rocky.toml              # Rocky pipeline config
  defs.yaml                  # Dagster component config
  definitions.py             # Dagster code location
  models/
    stg_orders.rocky         # Staging model
    stg_orders.toml
    fct_order_summary.rocky  # Fact model
    fct_order_summary.toml
```

## How It Works

### RockyComponent (recommended)

`RockyComponent` is a state-backed Dagster component that caches `rocky discover` output. Assets appear in the Dagster UI instantly on code location reload without calling any external APIs.

1. **`write_state_to_path()`** calls `rocky discover`, serializes to JSON, and saves to a state file
2. **`build_defs_from_state()`** reads the cached JSON and creates `AssetSpec` objects without API calls

### RockyResource

`RockyResource` wraps the Rocky CLI binary. Use it inside asset functions to run discover, plan, and run commands.

## Setup

```bash
# Install dagster-rocky
pip install dagster-rocky

# Ensure rocky binary is on PATH
cargo install rocky

# Start Dagster dev server
dagster dev -f definitions.py
```

## Running

```bash
# Preview what Dagster will see
dagster dev -f definitions.py

# Or use dg CLI
dg dev
```
