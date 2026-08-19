# Rocky Playground

A catalog of small POCs, each isolating one [Rocky](https://github.com/rocky-data/rocky) feature. Each one is a complete project you can run.

This catalog is a **learning / reference** companion to the [rocky/engine/examples/](https://github.com/rocky-data/rocky/tree/main/engine/examples) starter projects. A starter shows you the shape of a Rocky project. A POC here isolates one feature and demonstrates it end to end.

To start your own project, run `rocky playground my-project`. It scaffolds a throwaway DuckDB project you can change freely. The [`quickstart`](https://github.com/rocky-data/rocky/tree/main/engine/examples/quickstart) example is the same shape, checked into the repo to read rather than scaffold.

## When to look here vs `engine/examples/`

| You want to... | Look at |
|---|---|
| Start your first Rocky project | [`quickstart`](https://github.com/rocky-data/rocky/tree/main/engine/examples/quickstart) |
| See a full Bronze/Silver/Gold architecture | [`multi-layer`](https://github.com/rocky-data/rocky/tree/main/engine/examples/multi-layer) |
| Migrate a dbt project | [`dbt-migration`](https://github.com/rocky-data/rocky/tree/main/engine/examples/dbt-migration) + `pocs/06-developer-experience/03-import-dbt-validate` |
| Wire Rocky into Dagster | [`dagster-integration`](https://github.com/rocky-data/rocky/tree/main/engine/examples/dagster-integration) |
| Explore one specific feature in isolation | `pocs/` (this repo) |
| See raw performance numbers | [`benchmarks/`](benchmarks/) (this repo) |

## Running a POC

Every POC is one self-contained folder. Two files are always there: a `README.md` and a `run.sh`. `run.sh` drives the whole demo, so you never assemble the pieces by hand.

```
pocs/<category>/<id>-<name>/
├── README.md     always — the feature, and the output to expect
├── run.sh        always ──► the one entry point: runs the demo end to end
├── rocky.toml    the pipeline config, when the POC runs a pipeline
├── models/       when the POC ships models: .sql / .rocky + .toml sidecars
├── data/         when the POC needs seed rows, usually seed.sql
└── contracts/    only when contracts are part of the feature
```

Clone the repo, then run any POC from the playground root:

```bash
git clone https://github.com/rocky-data/rocky.git
cd rocky/examples/playground

# Run any POC end-to-end
./pocs/02-performance/01-incremental-watermark/run.sh
```

Or run it from inside the POC folder:

```bash
cd pocs/02-performance/01-incremental-watermark
./run.sh
```

**Prerequisites:** the Rocky CLI on your `PATH`. Many POCs also use the [DuckDB CLI](https://duckdb.org) to load seed data (`brew install duckdb`).

**88 of 100 POCs run with no external credentials.** See each POC's README for prerequisites.

## The catalog

### 00 — Foundations (17 POCs · DuckDB)

Start here. These POCs cover the language, the config layers, and the deploy loop you use in every project.

| POC | Feature |
|---|---|
| [00-playground-default](pocs/00-foundations/00-playground-default) | Stock 3-model scaffold from `rocky playground` — baseline smoke test |
| [01-replication-basics](pocs/00-foundations/01-replication-basics) | Minimal replication-shape pipeline — backs the Dagster integration test fixtures |
| [01-dsl-pipeline-syntax](pocs/00-foundations/01-dsl-pipeline-syntax) | Every Rocky DSL operator in one file (`from→where→derive→group→join→select→sort→take→distinct`) |
| [02-null-safe-operators](pocs/00-foundations/02-null-safe-operators) | `!=` lowering to `IS DISTINCT FROM` — side-by-side `.rocky` vs `.sql` |
| [03-date-literals-and-match](pocs/00-foundations/03-date-literals-and-match) | `@2025-01-01` date literals and `match { ... }` pattern matching |
| [04-window-functions](pocs/00-foundations/04-window-functions) | DSL window syntax with partition + sort + frame |
| [05-generic-adapter-exercise](pocs/00-foundations/05-generic-adapter-exercise) | Full generic adapter surface against DuckDB — validate, list, doctor, seed, discover, plan, compile, test in one credential-free flow |
| [06-branches-replay-lineage](pocs/00-foundations/06-branches-replay-lineage) | **Trust arc 1** — `rocky branch create/list/show`, `rocky run --branch`, `rocky replay`, `rocky lineage --downstream` |
| [07-config-layering](pocs/00-foundations/07-config-layering) | Three-layer config: `rocky.toml` + `_defaults.toml` + per-model sidecar, with env-var substitution at every layer |
| [08-branch-approve-promote](pocs/00-foundations/08-branch-approve-promote) | **Trust arc 1** — `[branch.approval] required = true` gates `rocky branch promote`; `rocky branch approve` writes a content-addressed signed artifact bound to the branch state hash |
| [09-files-to-duckdb](pocs/00-foundations/09-files-to-duckdb) | `rocky load` ingests Parquet + CSV + JSONL from one `data/` directory into DuckDB — format auto-detected by extension |
| [10-route-by-tenant](pocs/00-foundations/10-route-by-tenant) | One Parquet file with mixed-account rows is loaded then fanned out to per-tenant schemas (`account_acme.events`, `account_beta.events`, …) via per-model `target.schema` overrides |
| [11-plan-apply-workflow](pocs/00-foundations/11-plan-apply-workflow) | `rocky plan` persists a content-addressed plan to `.rocky/plans/<id>.json`; `rocky apply <id>` executes it. Re-planning the same intent yields the same `plan_id` |
| [12-init-scaffolding](pocs/00-foundations/12-init-scaffolding) | `rocky init --template duckdb` scaffolds a project that validates + compiles out of the box; `--template snowflake` shows the layout changes per warehouse |
| [13-surrogate-keys](pocs/00-foundations/13-surrogate-keys) | A model sidecar `[[surrogate_key]]` block injects a deterministic, dbt_utils-identical MD5 hash column into the materialized table at `rocky run` |
| [14-config-groups](pocs/00-foundations/14-config-groups) | One `models/groups/*.toml` fans `schema_template` + strategy + tags to N members via per-model `[args]`; `broken/` siblings show the `enforce` and pinned-schema-plus-args load guards |
| [15-run-variables](pocs/00-foundations/15-run-variables) | Per-run `@var(name)` / `@var(name, default)` markers resolved by `rocky run --var` (and `compile` / `emit-sql`); a required var with no value fails compile with E028 — distinct from config-time `${ENV}` |

### 01 — Quality (11 POCs · DuckDB)

How Rocky catches bad data. These POCs cover contracts, inline checks, named and unit tests, freshness SLAs, anomaly alerts, and SCD-2 history.

| POC | Feature |
|---|---|
| [01-data-contracts-strict](pocs/01-quality/01-data-contracts-strict) | Every contract rule (`required`, `protected`, columns) + a deliberately broken sibling that exercises every diagnostic code |
| [02-inline-checks](pocs/01-quality/02-inline-checks) | Built-in `[checks]` (row_count, column_match, freshness, null_rate) running inline during `rocky run` |
| [03-anomaly-detection](pocs/01-quality/03-anomaly-detection) | `rocky history` + `rocky metrics --alerts` driven by row count anomalies across runs |
| [04-local-test-with-duckdb](pocs/01-quality/04-local-test-with-duckdb) | `rocky test` with both passing and intentionally failing assertions |
| [05-snapshot-scd2](pocs/01-quality/05-snapshot-scd2) | `type = "snapshot"` pipeline — SCD Type 2 with `unique_key`, `updated_at`, `invalidate_hard_deletes` |
| [06-quality-pipeline-standalone](pocs/01-quality/06-quality-pipeline-standalone) | `type = "quality"` pipeline — standalone checks (row_count, freshness, null_rate) with `depends_on` chaining |
| [07-freshness-sla](pocs/01-quality/07-freshness-sla) | Per-model + project `[freshness]` SLAs (`expected_lag_seconds` alias); the **W005** coverage diagnostic fires on a temporal-output model with no freshness declaration (`compile --with-seed`), suppressed by a per-model block or a project default; also surfaces in the editor with an AI fix |
| [08-cross-source-overlap](pocs/01-quality/08-cross-source-overlap) | The same data arriving via two sibling sources — `cross_source_overlap` flags a business key shared across siblings (which per-table `unique` can't see) + `unique_expr` catches a derived-key duplicate, both at replication time |
| [09-named-tests](pocs/01-quality/09-named-tests) | Define a check once in `test_definitions.toml`, apply it across models via `[[use_test]]` (column/severity overrides) alongside inline `[[tests]]` |
| [10-unit-tests](pocs/01-quality/10-unit-tests) | Fixture-driven `[[test]]` blocks (`given` rows → `expect` rows) run by `rocky test` — multiset by default, positional with `ordered` |
| [11-fail-loud-on-compile-error](pocs/01-quality/11-fail-loud-on-compile-error) | A model that fails to compile (E020) makes `rocky run` exit non-zero with status `partial_failure` and a `compile-error`; the broken table is never built while the clean model's data still lands |

### 02 — Performance (14 POCs · DuckDB)

How Rocky avoids rebuilding what has not changed. These POCs cover the incremental, merge, ephemeral, and delete_insert strategies, plus schema drift, cost controls, and concurrency.

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
| [10-cost-budgets](pocs/02-performance/10-cost-budgets) | **Trust arc 2** — per-run `cost_summary` + `[budget]` block + `budget_breach` record |
| [11-strategy-showcase](pocs/02-performance/11-strategy-showcase) | Three strategies side-by-side on one source: `full_refresh` + `incremental` + `merge`, with a cheat-sheet README |
| [12-replication-table-overrides](pocs/02-performance/12-replication-table-overrides) | `[[table_overrides]]` — per-table `strategy`/`merge_keys`/`timestamp_column`/`enabled` overrides with glob matching and most-specific-match-wins resolution |
| [13-estimate-explain-cost](pocs/02-performance/13-estimate-explain-cost) | `rocky estimate` runs each model's SELECT through DuckDB `EXPLAIN` — row estimates, join strategy, filter pushdown — as a pure dry-run, no tables materialized |
| [14-skip-unchanged](pocs/02-performance/14-skip-unchanged) | `rocky run --skip-unchanged` — opt-in model-skip gate: unchanged logic + unchanged upstream data ⇒ SKIP (`tables_skipped: 1`); mutate the upstream ⇒ BUILD. Best-effort, default-off, fail-safe |

### 03 — AI (7 POCs · `ANTHROPIC_API_KEY` for 01–05, DuckDB for 06–07)

How Rocky keeps a language model honest. A generated model goes through the compiler first, and Rocky writes it only when it type-checks.

| POC | Feature |
|---|---|
| [01-model-generation](pocs/03-ai/01-model-generation) | `rocky ai "intent..."` with the visible compile-verify retry loop |
| [02-ai-explain-bootstrap](pocs/03-ai/02-ai-explain-bootstrap) | `rocky ai-explain --all --save` reverse-engineers intent fields onto existing models |
| [03-ai-sync-schema-evolution](pocs/03-ai/03-ai-sync-schema-evolution) | `rocky ai-sync` proposes downstream updates after upstream schema changes |
| [04-ai-test-generation](pocs/03-ai/04-ai-test-generation) | `rocky ai-test --all --save` generates SQL assertions from intent + schema |
| [05-schema-grounded-validation](pocs/03-ai/05-schema-grounded-validation) | **Trust arc 5** — `ValidationContext` schema grounding + compile-verify retry loop |
| [06-mcp-grounding](pocs/03-ai/06-mcp-grounding) | `rocky mcp` server — a schema-only model compiles but reconciles wrong; sampling the data via the MCP tools fixes it (creds-free `run.sh`) |
| [07-policy](pocs/03-ai/07-policy) | `rocky policy test` — scenario assertions pin an agent `[policy]` so a careless edit that opens a hole becomes a red CI check (creds-free `run.sh`) |

### 04 — Governance (11 POCs · Databricks / DuckDB)

Who may read what, and who proved it. The Databricks POCs need credentials; the rest run on local DuckDB.

| POC | Feature | Credentials |
|---|---|---|
| [01-unity-catalog-grants](pocs/04-governance/01-unity-catalog-grants) | `[[governance.grants]]` declarative RBAC with SHOW GRANTS before/after | Databricks |
| [02-schema-patterns-multi-tenant](pocs/04-governance/02-schema-patterns-multi-tenant) | `source.schema_pattern` with `prefix`/`separator`/`components` (incl. variadic `regions...`) routing to per-tenant catalogs | Databricks |
| [03-workspace-isolation](pocs/04-governance/03-workspace-isolation) | `[governance.isolation]` with workspace bindings + ISOLATED catalog mode | Databricks |
| [04-tagging-lifecycle](pocs/04-governance/04-tagging-lifecycle) | `[governance.tags]` propagated via `ALTER ... SET TAGS` | Databricks |
| [05-classification-masking-compliance](pocs/04-governance/05-classification-masking-compliance) | `[classification]` + `[mask]` policy + `rocky compliance --fail-on exception` for CI gating on unmasked PII | none |
| [06-retention-policies](pocs/04-governance/06-retention-policies) | Declarative `retention = "<N>[dy]"` sidecars + `rocky retention-status --drift` | none |
| [07-auto-create-schemas](pocs/04-governance/07-auto-create-schemas) | `[…target.governance] auto_create_schemas = true` on a transformation pipeline targeting a fresh schema (v1.29.0 parity fix) | none |
| [08-cross-team-contracts](pocs/04-governance/08-cross-team-contracts) | Consumer imports a producer's published IR snapshot via `[imports.<name>]`; `rocky compile` fails with E030 when the producer drops a column the consumer reads (E033 on pin mismatch) | none |
| [09-model-tags-inheritance](pocs/04-governance/09-model-tags-inheritance) | A config-group `[tags]` baseline inherited by members and overridden per-key by a model's own `[tags]`, surfaced on `rocky compile --output json` as `models_detail[].tags` | none |
| [10-recipe-provenance](pocs/04-governance/10-recipe-provenance) | Every materialization records a recipe-identity triple (`recipe_hash`, `input_hash`, `env_hash`); `rocky history --recipe <hash>` returns every execution of that exact program — the "what produced this?" audit one-liner, honest about `heuristic` vs `strong` input proof | none |
| [11-agent-policy](pocs/04-governance/11-agent-policy) | The agent-policy plane as an incident guardrail: an agent's breaking drop of a PII-classified column under a cross-team contract is DENIED at the `rocky apply` seam with the deciding rule named, the same plan applied by a human goes through, and every decision lands in a ledger queryable via `rocky audit` | none |

### 05 — Orchestration (12 POCs · DuckDB / docker)

How a run starts, where its state lives, and what happens when it fails. Some POCs need Docker for a local backing service.

| POC | Feature |
|---|---|
| [01-shell-hooks](pocs/05-orchestration/01-shell-hooks) | `[[hook.<event>]]` with stdin-JSON context and `on_failure` semantics |
| [02-webhook-slack-preset](pocs/05-orchestration/02-webhook-slack-preset) | `[hook.webhooks.pipeline_error] preset = "slack"` against a `webhook.site` echo URL |
| [03-remote-state-s3](pocs/05-orchestration/03-remote-state-s3) | `[state] backend = "s3"` against MinIO via docker-compose; full upload/download round-trip |
| [04-checkpoint-resume](pocs/05-orchestration/04-checkpoint-resume) | `rocky run --resume-latest` after a deliberate mid-pipeline failure |
| [05-webhook-presets-multi](pocs/05-orchestration/05-webhook-presets-multi) | All 5 webhook presets (Slack, Teams, PagerDuty, Datadog, generic) on different lifecycle events |
| [06-valkey-distributed-cache](pocs/05-orchestration/06-valkey-distributed-cache) | Three-tier caching (memory → Valkey → source) + tiered state backend via docker-compose |
| [07-dagster-dag-mode](pocs/05-orchestration/07-dagster-dag-mode) | `rocky run --dag` unified cross-pipeline DAG with Dagster orchestration |
| [08-circuit-breaker](pocs/05-orchestration/08-circuit-breaker) | **Trust arc 3** — `[adapter.retry]` exponential backoff + three-state `CircuitBreaker` |
| [09-idempotency-key](pocs/05-orchestration/09-idempotency-key) | `rocky run --idempotency-key` dedup — second run with the same key yields `status = "skipped_idempotent"` |
| [10-state-retention-sweep](pocs/05-orchestration/10-state-retention-sweep) | `[state.retention]` + `rocky state retention sweep` — manual + end-of-run auto-sweep of run history / lineage / audit domains |
| [11-state-namespacing](pocs/05-orchestration/11-state-namespacing) | `rocky --state-namespace <key>` routes each pipeline/client to its own `<models>/.rocky-state/<key>.redb` (own single-writer lock) — opt-in, byte-identical when off |
| [12-webhook-ingress](pocs/05-orchestration/12-webhook-ingress) | `rocky serve --scheduler` exposes `POST /api/v1/hooks/trigger/{pipeline}`; an `X-Rocky-Signature` HMAC-SHA256 body signature queues a durable at-most-once run demand, and an `X-Rocky-Delivery` id deduplicates a redelivery |

### 06 — Developer Experience (21 POCs · DuckDB)

The daily loop: edit, check, review, ship. Lineage, previews, CI gates, dbt import, and the plain-SQL exit path.

| POC | Feature |
|---|---|
| [01-lineage-column-level](pocs/06-developer-experience/01-lineage-column-level) | `rocky lineage <model> --column <col> --output json` on a 4-model branching DAG |
| [02-rocky-serve-api](pocs/06-developer-experience/02-rocky-serve-api) | `rocky serve --watch` HTTP API with curl examples |
| [03-import-dbt-validate](pocs/06-developer-experience/03-import-dbt-validate) | `rocky import-dbt` on a real dbt project + `rocky validate-migration` correctness report |
| [04-shadow-mode-compare](pocs/06-developer-experience/04-shadow-mode-compare) | `rocky compare` shadow targets with row count + schema diffs |
| [05-doctor-and-ci](pocs/06-developer-experience/05-doctor-and-ci) | `rocky doctor` + `rocky ci --output json` + a GitHub Actions example |
| [06-hybrid-dbt-packages](pocs/06-developer-experience/06-hybrid-dbt-packages) | Rocky consuming dbt package tables (Fivetran facebook_ads, stripe) as external sources — no conversion needed |
| [07-run-trace-gantt](pocs/06-developer-experience/07-run-trace-gantt) | **Trust arc 4** — `rocky trace latest` Gantt view + feature-gated OTLP exporter |
| [08-portability-lint](pocs/06-developer-experience/08-portability-lint) | **Trust arc 6** — `rocky compile --target-dialect bq`, `[portability]`, `-- rocky-allow` pragma |
| [09-sql-types-blast-radius](pocs/06-developer-experience/09-sql-types-blast-radius) | **Trust arc 7** — `rocky compile --with-seed` type grounding + blast-radius `SELECT *` lint |
| [10-pr-preview-and-data-diff](pocs/06-developer-experience/10-pr-preview-and-data-diff) | `rocky preview create / diff / cost` — column-level pruned re-run + sampled row diff + cost delta on a 5-model DAG |
| [11-lineage-diff](pocs/06-developer-experience/11-lineage-diff) | `rocky lineage-diff <base_ref>` — per-changed-column downstream blast-radius for PR review (Markdown drops into a PR comment) |
| [12-catalog-emit](pocs/06-developer-experience/12-catalog-emit) | `rocky catalog --format both` — column-level lineage emitted as `catalog.json` + `edges.parquet` + `assets.parquet`; readable by any Parquet client without going through the engine |
| [13-run-watch-inner-loop](pocs/06-developer-experience/13-run-watch-inner-loop) | `rocky run --watch` re-materialises on every save; 200 ms debounce window, FSEvents-safe directory watch, NDJSON-stream output, clean SIGINT exit |
| [14-import-dbt-failure-modes](pocs/06-developer-experience/14-import-dbt-failure-modes) | `rocky import-dbt` against malformed inputs (missing files, invalid YAML, unknown adapters) — exercises every diagnostic path the importer emits |
| [15-semantic-breaking-change-gate](pocs/06-developer-experience/15-semantic-breaking-change-gate) | `rocky ci-diff --semantic` (informational) + `rocky branch promote --base-ref / --allow-breaking` — pre-promote gate blocks column drops; override is recorded as a `BreakingChangesAllowed` audit event |
| [16-history-rolling-stats](pocs/06-developer-experience/16-history-rolling-stats) | `rocky history --model <name> --rolling-stats --window N` — per-model mean / std-dev / z-score / composite `health_score` over the rolling window |
| [17-trace-replay-cost-combo](pocs/06-developer-experience/17-trace-replay-cost-combo) | One `RunRecord`, three views — `rocky trace` + `rocky cost` + `rocky replay` all project from the same run id |
| [18-view-strategy](pocs/06-developer-experience/18-view-strategy) | `[strategy] type = "view"` — model compiles to `CREATE OR REPLACE VIEW <target>` on every warehouse |
| [19-import-dbt-unit-tests](pocs/06-developer-experience/19-import-dbt-unit-tests) | `rocky import-dbt --manifest` walks `manifest.unit_tests` into Rocky `[[test]]` sidecars; `data_tests:` accepted as alias for `tests:` on column tests (dbt 1.7+) |
| [20-defer-against-prod](pocs/06-developer-experience/20-defer-against-prod) | `rocky run --model <name> --defer --defer-to <schema>` — build only the changed model locally, resolve its unbuilt upstream `ref()`s against a production schema (default-off dev convenience) |
| [21-emit-sql-exit-path](pocs/06-developer-experience/21-emit-sql-exit-path) | `rocky emit-sql --out-dir` renders the model DAG to plain `.sql`, then runs it through DuckDB with no Rocky — the tested no-lock-in exit path |

### 07 — Adapters (7 POCs · mixed)

One POC each for Snowflake, Databricks, BigQuery, Trino, and Fivetran, plus two starters for writing your own adapter. Rocky ships more adapters than this section covers. Check the Credentials column before you run one.

| POC | Feature | Credentials |
|---|---|---|
| [01-snowflake-dynamic-table](pocs/07-adapters/01-snowflake-dynamic-table) | `MaterializationStrategy::DynamicTable { target_lag }` | Snowflake |
| [02-databricks-materialized-view](pocs/07-adapters/02-databricks-materialized-view) | `MaterializationStrategy::MaterializedView` on Databricks | Databricks |
| [03-fivetran-discover](pocs/07-adapters/03-fivetran-discover) | `rocky discover` against Fivetran REST API; metadata only | `FIVETRAN_API_KEY` |
| [04-custom-process-adapter](pocs/07-adapters/04-custom-process-adapter) | Python adapter speaking JSON-RPC over stdio, discovered through the engine as `rocky adapter list`/`info` via the `rocky-<name>` PATH convention | none |
| [05-bigquery-native-queries](pocs/07-adapters/05-bigquery-native-queries) | BigQuery adapter — backtick quoting, time-interval partitions, DML transactions | GCP SA / ADC |
| [06-rust-native-adapter-skeleton](pocs/07-adapters/06-rust-native-adapter-skeleton) | Out-of-tree warehouse adapter starter — `rocky-adapter-sdk` traits against an in-memory mock, ClickHouse-shaped (backtick quoting, no `MERGE`, partition replace) | none (Rust toolchain) |
| [07-trino-docker](pocs/07-adapters/07-trino-docker) | `rocky-trino` v0 adapter against a single-node `trinodb/trino` container — full_refresh CTAS from `tpch.tiny.nation` into the `memory` catalog | none (Docker) |

## Benchmarks

The [`benchmarks/`](benchmarks/) folder holds a suite that compares Rocky against dbt-core and dbt-fusion on a generated transformation DAG. `make bench` measures compile time and DAG resolution, and records peak memory for each. Startup, lineage, warm compile, and config validation are separate targets. Run `make help` to list them.

The suite uses the engine's release binary from this repository, and builds it when it is missing. So it needs the repository and a Rust toolchain, not just the Rocky CLI on your `PATH`. It also needs dbt-core installed. dbt-fusion is optional: the run warns and skips it when it is absent.

```bash
cd benchmarks
pip install -r requirements.txt   # dbt-core, dbt-duckdb, matplotlib, psutil
make bench                        # Compile + DAG, at SCALE=10000 models
```

[`benchmarks/REPORT_CURRENT.md`](benchmarks/REPORT_CURRENT.md) records one past run. It is dated, and it names the Rocky version it measured. Treat it as a snapshot, not as the current performance of the engine.

## Adding a POC

Scaffold the folder, write the POC, then update the counts on this page.

```bash
./scripts/new-poc.sh 02-performance 15-my-new-feature
# ... write models/, run.sh, README.md ...
bash ../../scripts/update-poc-counts.sh
```

The second command rewrites every count in this file from the folders on disk. Never edit those numbers by hand. CI recomputes them and fails the pull request if they drift.

See the monorepo [CONTRIBUTING.md](../../CONTRIBUTING.md) for conventions.

## License

Apache 2.0, see [LICENSE](../../LICENSE).
