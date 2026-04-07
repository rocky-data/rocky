---
title: Introduction
description: What dagster-rocky is and how it integrates Rocky with Dagster
sidebar:
  order: 1
---

`dagster-rocky` is a Python package that bridges Rocky's Rust binary with Dagster orchestration. It lets you manage SQL transformations with Rocky while leveraging Dagster for scheduling, retries, alerting, and its asset-centric UI.

## What it provides

- **`RockyResource`** — A Dagster `ConfigurableResource` that wraps the Rocky CLI, invoking it via subprocess and parsing JSON output into strongly-typed Pydantic models.
- **`RockyDagsterTranslator`** — Controls how Rocky sources and tables map to Dagster asset keys, groups, tags, and metadata.
- **`load_rocky_assets()`** — Calls `rocky discover` and returns a list of Dagster `AssetSpec` objects, one per enabled table.
- **`emit_check_results()` / `emit_materializations()`** — Convert Rocky's check and materialization results into Dagster events that appear in the UI.
- **`RockyComponent`** — A state-backed Dagster component that caches discovery output, avoiding API calls on every code location reload.

## Architecture

The integration follows a simple pattern:

1. Dagster calls the `rocky` binary via subprocess (e.g., `rocky discover --output json`).
2. Rocky executes against your warehouse and sources, returning structured JSON.
3. `dagster-rocky` parses that JSON into Pydantic models.
4. The models are translated into Dagster events (asset materializations, check results, etc.).

Rocky handles the SQL transformation layer: DAG resolution, incremental logic, SQL generation, schema drift detection, and permission reconciliation. Dagster handles everything around it: scheduling, retries, alerting, lineage visualization, and operational monitoring.

## Requirements

- `dagster >= 1.12.0`
- `pydantic >= 2.0`
- The `rocky` binary must be available on `PATH` (or configured via `binary_path`)
