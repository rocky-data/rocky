---
title: Roadmap
description: What Rocky ships today, what's in Beta, and what's coming.
sidebar:
  order: 4
---

We are explicit about the line between what Rocky ships and what's still coming. The trust primitives (compiler, branches, replay, lineage, contracts, cost attribution) are production-grade on Databricks. Here is the honest state of everything else.

## Shipped and production-grade

- **Databricks** is the production target for 2026: SQL Statement API, Unity Catalog, OAuth M2M, adaptive concurrency, and `SHALLOW CLONE` for branches.
- **The compiler.** Typed column-level inference, the diagnostic codes, and the LSP all run the same in CI and in your editor.
- **Branches and replay.** Named branches as isolated schemas, and content-addressed replay that rebuilds a past run exactly.
- **Cost attribution.** Per-model cost on every run record, with `[budget]` blocks that fail a run on overspend.
- **The AI compile-validate loop.** `rocky ai` generates a model, compiles it, and auto-fixes parse errors before it lands.

## Beta

- **Snowflake, BigQuery, and Trino.** Connection, execution, and the core run loop work. Conformance coverage is still growing, and we test against live warehouses, so corner cases get reported and fixed quickly. If your enterprise warehouse is Snowflake or BigQuery and you need it production-grade today, [open a discussion](https://github.com/rocky-data/rocky/discussions). We want the failure reports.
- **Iceberg.** REST-catalog source discovery works. Content-addressed writes round-trip as Iceberg through Delta UniForm, end-to-end.

## On the 2026 roadmap

- **Iceberg-native writes** without the Delta intermediate.
- **The wider AI surface.** The loop is shipped; the larger workflow (rename a column, regenerate the downstream models and their assertions, run the tests, ship) is the next step.
- **A semantic layer.** Rocky's typed IR is the right home for one. Until it exists, integrate with Cube, the dbt Semantic Layer, or your existing metric store.

## Handled by an integration, not built in

- **Extraction from SaaS sources.** Use Fivetran, Airbyte, Stitch, or warehouse-native CDC. Rocky discovers what they land and takes over from there.
- **Orchestration beyond Dagster.** Dagster is first-class, and `rocky serve` covers small standalone teams. Airflow and Prefect call the `rocky` binary like any other CLI; native integrations are not shipped.

## Tell us where it hurts

The roadmap is shaped by where production pipelines are actually getting hurt. If one of these gaps blocks your team, [open a discussion](https://github.com/rocky-data/rocky/discussions) and tell us where.
