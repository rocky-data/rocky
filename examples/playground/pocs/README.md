# POC Catalog

60 small POCs across 8 categories. Each is self-contained — `cd` into a POC folder and run `./run.sh`.

## Categories

- [00-foundations](00-foundations/) — DSL syntax + materialization basics + trust-arc 1 branches/replay/lineage (7 POCs · DuckDB)
- [01-quality](01-quality/) — Contracts, checks, anomaly detection, local testing, SCD-2 snapshots, standalone quality pipeline (6 POCs · DuckDB)
- [02-performance](02-performance/) — Incremental, merge, drift, optimization, ephemeral CTE, delete+insert, adaptive concurrency, trust-arc 2 cost+budgets (10 POCs · DuckDB)
- [03-ai](03-ai/) — AI generation, sync, test generation, trust-arc 5 schema-grounded validation (5 POCs · `ANTHROPIC_API_KEY`)
- [04-governance](04-governance/) — Unity Catalog grants, isolation, tagging, classification + masking, retention (6 POCs · Databricks / DuckDB)
- [05-orchestration](05-orchestration/) — Hooks, webhooks, state, resume, Valkey cache, trust-arc 3 circuit breaker, idempotency keys (9 POCs · DuckDB / docker)
- [06-developer-experience](06-developer-experience/) — Lineage, serve, dbt migration, shadow, CI, hybrid dbt workflows, trust-arc 4 trace-Gantt, trust-arc 6 portability lint, trust-arc 7 SQL types, PR-preview, lineage-diff (11 POCs · DuckDB)
- [07-adapters](07-adapters/) — Snowflake, Databricks, Fivetran, custom process adapter, BigQuery, Rust-native adapter skeleton (6 POCs · mixed)

## Credentials at a glance

| POCs | Credentials |
|---|---|
| 47 of 60 | None — local DuckDB (or docker-compose for MinIO/Valkey) |
| 5 (`03-ai/*`) | `ANTHROPIC_API_KEY` |
| 4 (`04-governance/01..04`) + 1 (`07-adapters/02`) | Databricks host + token |
| 1 (`07-adapters/01`) | Snowflake account + auth |
| 1 (`07-adapters/03`) | Fivetran API key (read-only) |
| 1 (`07-adapters/05`) | GCP Service Account / ADC |

See the top-level [README.md](../README.md) for the full POC list with one-line descriptions.

## Running

```bash
cd <category>/<id>-<name>
./run.sh
```

Or from the repo root, run all credential-free POCs in sequence:

```bash
./scripts/run-all-duckdb.sh
```
