# Rocky Playground

A curated catalog of small POCs that showcase the distinctive capabilities of [Rocky](https://github.com/rocky-data/rocky), plus the benchmark suite comparing Rocky against dbt-core, dbt-fusion, and PySpark.

This repo is a **learning / reference** companion to the official [rocky/examples/](https://github.com/rocky-data/rocky/tree/main/examples) starter projects. The starters show you the shape of a Rocky project; the POCs here show you *what Rocky can do that other tools can't*.

## When to look here vs `rocky/examples/`

| You want to... | Look at |
|---|---|
| Start your first Rocky project | [`quickstart`](https://github.com/rocky-data/rocky/tree/main/engine/examples/quickstart) |
| See a full Bronze/Silver/Gold architecture | [`multi-layer`](https://github.com/rocky-data/rocky/tree/main/engine/examples/multi-layer) |
| Migrate a dbt project | [`dbt-migration`](https://github.com/rocky-data/rocky/tree/main/engine/examples/dbt-migration) + `pocs/06-developer-experience/03-import-dbt-validate` |
| Wire Rocky into Dagster | [`dagster-integration`](https://github.com/rocky-data/rocky/tree/main/engine/examples/dagster-integration) |
| Explore one specific feature in isolation | `pocs/` (this repo) |
| See raw performance numbers | [`benchmarks/`](benchmarks/) (this repo) |

## Running a POC

Each POC is a self-contained folder with its own `rocky.toml`, `models/`, and `run.sh`. Most POCs run on local DuckDB with zero credentials.

```bash
git clone https://github.com/rocky-data/rocky.git
cd rocky/examples/playground

# Run any POC end-to-end
./pocs/02-performance/01-incremental-watermark/run.sh
```

Or, from inside a POC folder:

```bash
cd pocs/02-performance/01-incremental-watermark
./run.sh
```

**Prerequisites:** Rocky CLI on PATH. Most POCs only need the [DuckDB CLI](https://duckdb.org) for seeding (`brew install duckdb`).

**34 of 46 POCs run with no external credentials.** See each POC's README for prerequisites.

## The catalog

### 00 — Foundations (6 POCs · DuckDB)

DSL syntax, materialization basics, and the playground baseline.

| POC | Feature |
|---|---|
| [00-playground-default](pocs/00-foundations/00-playground-default) | Stock 3-model scaffold from `rocky playground` — baseline smoke test |
| [01-dsl-pipeline-syntax](pocs/00-foundations/01-dsl-pipeline-syntax) | Every Rocky DSL operator in one file (`from→where→derive→group→join→select→sort→take→distinct`) |
| [02-null-safe-operators](pocs/00-foundations/02-null-safe-operators) | `!=` lowering to `IS DISTINCT FROM` — side-by-side `.rocky` vs `.sql` |
| [03-date-literals-and-match](pocs/00-foundations/03-date-literals-and-match) | `@2025-01-01` date literals and `match { ... }` pattern matching |
| [04-window-functions](pocs/00-foundations/04-window-functions) | DSL window syntax with partition + sort + frame |
| [05-generic-adapter-exercise](pocs/00-foundations/05-generic-adapter-exercise) | Generic adapter exercise — building custom adapters via the process protocol |

### 01 — Quality (6 POCs · DuckDB)

Contracts, inline checks, anomaly detection, local testing, SCD-2 snapshots, standalone quality pipeline.

| POC | Feature |
|---|---|
| [01-data-contracts-strict](pocs/01-quality/01-data-contracts-strict) | Every contract rule (`required`, `protected`, columns) + a deliberately broken sibling that exercises every diagnostic code |
| [02-inline-checks](pocs/01-quality/02-inline-checks) | Built-in `[checks]` (row_count, column_match, freshness, null_rate) running inline during `rocky run` |
| [03-anomaly-detection](pocs/01-quality/03-anomaly-detection) | `rocky history` + `rocky metrics --alerts` driven by row count anomalies across runs |
| [04-local-test-with-duckdb](pocs/01-quality/04-local-test-with-duckdb) | `rocky test` with both passing and intentionally failing assertions |
| [05-snapshot-scd2](pocs/01-quality/05-snapshot-scd2) | `type = "snapshot"` pipeline — SCD Type 2 with `unique_key`, `updated_at`, `invalidate_hard_deletes` |
| [06-quality-pipeline-standalone](pocs/01-quality/06-quality-pipeline-standalone) | `type = "quality"` pipeline — standalone checks (row_count, freshness, null_rate) with `depends_on` chaining |

### 02 — Performance (9 POCs · DuckDB)

Incremental, merge, drift, optimization, ephemeral CTE, delete+insert, adaptive concurrency.

| POC | Feature |
|---|---|
| [01-incremental-watermark](pocs/02-performance/01-incremental-watermark) | `strategy = "incremental"` — full load on run #1, watermark-filtered INSERT on run #2 |
| [02-merge-upsert](pocs/02-performance/02-merge-upsert) | `strategy.type = "merge"` with `unique_key` + `update_columns` for SCD-1 upserts |
| [03-partition-checksum](pocs/02-performance/03-partition-checksum) | Partition checksum incremental that catches late-arriving corrections to historical data |
| [04-column-propagation](pocs/02-performance/04-column-propagation) | Column-level lineage pruning — `rocky plan` skips downstream models whose consumed columns didn't change |
| [05-optimize-recommendations](pocs/02-performance/05-optimize-recommendations) | `rocky optimize` + `profile-storage` + `compact --dry-run` after building run history |
| [06-schema-drift-recover](pocs/02-performance/06-schema-drift-recover) | Drift detection auto-widening `STRING→INT`, unsafe changes via `DROP+RECREATE` |
| [07-ephemeral-cte](pocs/02-performance/07-ephemeral-cte) | `strategy = "ephemeral"` — model inlined as CTE, never persisted as a table |
| [08-delete-insert-partitioned](pocs/02-performance/08-delete-insert-partitioned) | `strategy = "delete_insert"` with `partition_by` — atomic partition replacement without MERGE |
| [09-adaptive-concurrency](pocs/02-performance/09-adaptive-concurrency) | AIMD throttling — dynamic parallelism with `concurrency`, `error_rate_abort_pct`, `table_retries` |

### 03 — AI (4 POCs · `ANTHROPIC_API_KEY` required)

AI-powered model generation, intent extraction, schema sync, test generation.

| POC | Feature |
|---|---|
| [01-model-generation](pocs/03-ai/01-model-generation) | `rocky ai "intent..."` with the visible compile-verify retry loop |
| [02-ai-explain-bootstrap](pocs/03-ai/02-ai-explain-bootstrap) | `rocky ai-explain --all --save` reverse-engineers intent fields onto existing models |
| [03-ai-sync-schema-evolution](pocs/03-ai/03-ai-sync-schema-evolution) | `rocky ai-sync` proposes downstream updates after upstream schema changes |
| [04-ai-test-generation](pocs/03-ai/04-ai-test-generation) | `rocky ai-test --all --save` generates SQL assertions from intent + schema |

### 04 — Governance (4 POCs · Databricks required)

Unity Catalog grants, schema patterns, workspace isolation, tagging.

| POC | Feature |
|---|---|
| [01-unity-catalog-grants](pocs/04-governance/01-unity-catalog-grants) | `[[governance.grants]]` declarative RBAC with SHOW GRANTS before/after |
| [02-schema-patterns-multi-tenant](pocs/04-governance/02-schema-patterns-multi-tenant) | `source.schema_pattern` with `prefix`/`separator`/`components` (incl. variadic `regions...`) routing to per-tenant catalogs |
| [03-workspace-isolation](pocs/04-governance/03-workspace-isolation) | `[governance.isolation]` with workspace bindings + ISOLATED catalog mode |
| [04-tagging-lifecycle](pocs/04-governance/04-tagging-lifecycle) | `[governance.tags]` propagated via `ALTER ... SET TAGS` |

### 05 — Orchestration (6 POCs · DuckDB / docker)

Hooks, webhooks, remote state, checkpoint/resume, Valkey cache.

| POC | Feature |
|---|---|
| [01-shell-hooks](pocs/05-orchestration/01-shell-hooks) | `[[hook.<event>]]` with stdin-JSON context and `on_failure` semantics |
| [02-webhook-slack-preset](pocs/05-orchestration/02-webhook-slack-preset) | `[hook.webhooks.pipeline_error] preset = "slack"` against a `webhook.site` echo URL |
| [03-remote-state-s3](pocs/05-orchestration/03-remote-state-s3) | `[state] backend = "s3"` against MinIO via docker-compose; full upload/download round-trip |
| [04-checkpoint-resume](pocs/05-orchestration/04-checkpoint-resume) | `rocky run --resume-latest` after a deliberate mid-pipeline failure |
| [05-webhook-presets-multi](pocs/05-orchestration/05-webhook-presets-multi) | All 5 webhook presets (Slack, Teams, PagerDuty, Datadog, generic) on different lifecycle events |
| [06-valkey-distributed-cache](pocs/05-orchestration/06-valkey-distributed-cache) | Three-tier caching (memory → Valkey → source) + tiered state backend via docker-compose |

### 06 — Developer Experience (6 POCs · DuckDB)

Lineage, HTTP API, dbt import, shadow mode, CI, hybrid workflows.

| POC | Feature |
|---|---|
| [01-lineage-column-level](pocs/06-developer-experience/01-lineage-column-level) | `rocky lineage <model> --column <col> --output json` on a 4-model branching DAG |
| [02-rocky-serve-api](pocs/06-developer-experience/02-rocky-serve-api) | `rocky serve --watch` HTTP API with curl examples |
| [03-import-dbt-validate](pocs/06-developer-experience/03-import-dbt-validate) | `rocky import-dbt` on a real dbt project + `rocky validate-migration` correctness report |
| [04-shadow-mode-compare](pocs/06-developer-experience/04-shadow-mode-compare) | `rocky compare` shadow targets with row count + schema diffs |
| [05-doctor-and-ci](pocs/06-developer-experience/05-doctor-and-ci) | `rocky doctor` + `rocky ci --output json` + a GitHub Actions example |
| [06-hybrid-dbt-packages](pocs/06-developer-experience/06-hybrid-dbt-packages) | Rocky consuming dbt package tables (Fivetran facebook_ads, stripe) as external sources — no conversion needed |

### 07 — Adapters (5 POCs · mixed)

Snowflake, Databricks, Fivetran, custom process adapter, BigQuery.

| POC | Feature | Credentials |
|---|---|---|
| [01-snowflake-dynamic-table](pocs/07-adapters/01-snowflake-dynamic-table) | `MaterializationStrategy::DynamicTable { target_lag }` | Snowflake |
| [02-databricks-materialized-view](pocs/07-adapters/02-databricks-materialized-view) | `MaterializationStrategy::MaterializedView` on Databricks | Databricks |
| [03-fivetran-discover](pocs/07-adapters/03-fivetran-discover) | `rocky discover` against Fivetran REST API; metadata only | `FIVETRAN_API_KEY` |
| [04-custom-process-adapter](pocs/07-adapters/04-custom-process-adapter) | ~80-line Python adapter speaking JSON-RPC over stdio, registered via `[adapter] type = "process"` | none |
| [05-bigquery-native-queries](pocs/07-adapters/05-bigquery-native-queries) | BigQuery adapter — backtick quoting, time-interval partitions, DML transactions | GCP SA / ADC |

## Benchmarks

The [`benchmarks/`](benchmarks/) folder contains a reproducible suite comparing Rocky against dbt-core, dbt-fusion, and PySpark on identical transformation workloads. See [`benchmarks/REPORT_CURRENT.md`](benchmarks/REPORT_CURRENT.md) for the latest numbers.

Headline (50k models): Rocky compiles in **10.5s**, **15× faster** than dbt-core, **21× faster** than dbt-fusion, with **5.3× less memory**.

```bash
cd benchmarks
make bench          # Run the full suite
```

## Adding a POC

```bash
./scripts/new-poc.sh 02-performance 10-my-new-feature
```

See the monorepo [CONTRIBUTING.md](../../CONTRIBUTING.md) for conventions.

## License

Apache 2.0 — see [LICENSE](../../LICENSE).
