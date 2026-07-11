# POC Catalog

99 small POCs across 8 categories. Each is self-contained; `cd` into a POC folder and run `./run.sh`.

## Categories

- [00-foundations](00-foundations/): Playground default baseline + DSL syntax + replication + materialization basics + trust-arc 1 branches/replay/lineage + generic adapter exercise + config layering + branch approve/promote + file-format ingest + per-tenant routing + plan/apply workflow + project scaffolding + surrogate keys + config groups + per-run variables (17 POCs · DuckDB)
- [01-quality](01-quality/): Contracts, checks, anomaly detection, local testing, SCD-2 snapshots, standalone quality pipeline, freshness SLAs, cross-source overlap, named reusable tests, fixture-driven unit tests, fail-loud compile-error handling (11 POCs · DuckDB)
- [02-performance](02-performance/): Incremental, merge, drift, optimization, ephemeral CTE, delete+insert, adaptive concurrency, trust-arc 2 cost+budgets, strategy showcase, EXPLAIN cost estimation, skip-unchanged gate (14 POCs · DuckDB)
- [03-ai](03-ai/): AI generation, sync, test generation, trust-arc 5 schema-grounded validation, MCP data-grounding, agent-policy testing (7 POCs · `ANTHROPIC_API_KEY` for 01–05, DuckDB for 06–07)
- [04-governance](04-governance/): Unity Catalog grants, schema-pattern multi-tenant routing, isolation, tagging, classification + masking, retention, auto-create schemas, cross-team contracts, model-tags inheritance, recipe provenance, agent-policy enforcement (11 POCs · Databricks / DuckDB)
- [05-orchestration](05-orchestration/): Hooks, webhooks, state, resume, Valkey cache, trust-arc 3 circuit breaker, idempotency keys, state retention sweep, per-client state namespacing (11 POCs · DuckDB / docker)
- [06-developer-experience](06-developer-experience/): Lineage, serve, dbt migration, shadow, CI, hybrid dbt workflows, trust-arc 4 trace-Gantt, trust-arc 6 portability lint, trust-arc 7 SQL types, PR-preview, lineage-diff, catalog emit, watch inner-loop, dbt-import failure modes, semantic breaking-change gate, history rolling stats, trace+cost+replay combo, view strategy, dbt unit-test import, defer-against-prod, emit-sql exit path (21 POCs · DuckDB)
- [07-adapters](07-adapters/): Snowflake, Databricks, Fivetran, custom process adapter, BigQuery, Rust-native adapter skeleton, Trino-Docker (7 POCs · mixed)

## Credentials at a glance

| POCs | Credentials |
|---|---|
| 87 of 99 | None — local DuckDB (or docker-compose for MinIO / Valkey / Trino) |
| 5 (`03-ai/01..05`) | `ANTHROPIC_API_KEY` |
| 4 (`04-governance/01..04`) + 1 (`07-adapters/02`) | Databricks host + token (`04-governance/02` additionally needs the Fivetran credentials below) |
| 1 (`07-adapters/01`) | Snowflake account + auth |
| 2 (`07-adapters/03`, `04-governance/02`) | Fivetran API key (read-only) |
| `07-adapters/05` live tour only | GCP Service Account / ADC (the default compile smoke is credential-free) |

See the top-level [README.md](../README.md) for the full POC list with one-line descriptions.

## Running

```bash
cd <category>/<id>-<name>
./run.sh
```

Or from the playground root (`examples/playground/`), run all credential-free POCs in sequence:

```bash
./scripts/run-all-duckdb.sh
```
