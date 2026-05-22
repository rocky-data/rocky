<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/rocky-readme-dark.svg" />
    <img src="docs/rocky-readme-light.svg" alt="Rocky" />
  </picture>
</p>

[![Engine CI](https://github.com/rocky-data/rocky/actions/workflows/engine-ci.yml/badge.svg)](https://github.com/rocky-data/rocky/actions/workflows/engine-ci.yml)
[![Dagster CI](https://github.com/rocky-data/rocky/actions/workflows/dagster-ci.yml/badge.svg)](https://github.com/rocky-data/rocky/actions/workflows/dagster-ci.yml)
[![VS Code CI](https://github.com/rocky-data/rocky/actions/workflows/vscode-ci.yml/badge.svg)](https://github.com/rocky-data/rocky/actions/workflows/vscode-ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

**Rocky is the typed graph between your code and whichever warehouse, table format, or query engine you've chosen.**

Not a warehouse. Not a table format. Not a query engine. The trust plane for your data — a typed compiler, named branches, deterministic replay, column-level lineage, compile-time contracts, and per-model cost — running over your existing Databricks / Snowflake / BigQuery / DuckDB. Apache 2.0.

Rocky exists because the disasters that cost data teams real money — silent schema drift wrecking a revenue dashboard, a column rename quietly poisoning 47 downstream models, an auditor asking who touched `fct_revenue.amount` and when, a cost spike that no one can attribute to a model — all share a common shape. They're problems the warehouse can't see and the templating engine on top of it was never asked to. Rocky owns the graph between your code and the warehouse so those problems become compile errors, blocked PRs, and signed artifacts instead of pages and post-mortems.

It's built for the team running production-critical multi-tenant pipelines on Databricks today, on Snowflake or BigQuery tomorrow, who can't tolerate another silent failure. Storage and compute stay where they are. **Rocky works on the SQL you already have** — the `.rocky` DSL is an acceleration when you want it, not a gate.

<p align="center">
  <img src="docs/public/demo-quickstart.gif" alt="Rocky quickstart — create a project, compile, and run 3 models in under 15s" width="900" />
</p>

## The disasters Rocky prevents

| Disaster | What dbt does | What Rocky does |
|---|---|---|
| Upstream changes a column type | Silent — fails downstream, hours later | `E013` at compile, blocks the PR |
| Required column dropped from a contract | No contract concept | `E010` at compile, blocks the PR |
| Column rename with unknown blast radius | `dbt docs` post-hoc, table-level (dbt Cloud Enterprise has column lineage in their UI, also post-hoc, not PR-blocking) | `rocky lineage-diff` at PR time, column-level, downstream consumers listed, blocks the merge |
| `SELECT *` pulls a new column you didn't expect | Silent | `P002` warning, downstream consumers named |
| Snowflake-only function written for a Databricks project | Runs in dev, fails in prod | `P001` dialect-portability lint at compile |
| Run cost doubles, no one knows which model | Manual warehouse spelunking | `RunOutput.cost_summary` per model, every run |
| Auditor asks: who changed `fct_revenue.amount`, when, and why? | Git blame + screenshots | `rocky replay <run_id>` — signed, content-addressed artifact of the exact code + inputs + outputs |
| Sev-2 at 3 AM, half the pipeline already ran | Re-run everything | `rocky run --resume-latest` — checkpoint, three-state circuit breaker, skip what succeeded |

Each row is a real failure mode, with a Rocky command that turns it into a non-event. The same primitives — typed compiler, content-addressed state, column-level lineage, per-model cost — back every row.

**Already on dbt?** `rocky import-dbt` converts a vanilla dbt project to Rocky in one command. The "What dbt does" column above is dbt Core's behavior. **dbt Fusion** — dbt Labs' Rust rewrite of dbt Core (public beta) — catches some compile-time issues that dbt Core misses, but doesn't ship named branches, content-addressed deterministic replay, per-model cost attribution as a first-class column, dialect-portability lint across warehouses, or declarative governance + masking outside dbt platform's paid tiers. Those stay Rocky's surface, Apache 2.0.

## Try it in 60 seconds

```bash
# macOS / Linux
curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash

# Windows (PowerShell)
irm https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.ps1 | iex
```

```bash
rocky playground my-first-project
cd my-first-project
rocky compile && rocky test && rocky run
```

No credentials needed — the playground runs end-to-end on local DuckDB.

## Who Rocky is for

Rocky is built first for **data platform engineers running production-critical, multi-tenant pipelines on Databricks** — the team that's hit dbt's ceiling, where silent failures cost real money, and where Dagster is already the orchestrator. That's the launch wedge, and that's where Rocky is most battle-tested.

The next ring out: **Snowflake and BigQuery shops** currently evaluating SQLMesh, who want correctness moved to the compiler (not the planner) and prefer SQL by default over Python-first ergonomics. Adapters are Beta today; see [Where Rocky is today](#where-rocky-is-today) below.

## See it in action

Each demo below is a self-contained POC in [`examples/playground/pocs/`](examples/playground/) — `cd` in, run `./run.sh`, reproduce locally.

### Detects schema drift the moment it happens

A source column type changes upstream. On the next run, Rocky diffs source vs. target, drops the target, and recreates it. No silent data corruption, no dbt-style quiet divergence.

<p align="center">
  <img src="docs/public/demo-drift-recover.gif" alt="rocky run detects source type change and recreates the target" width="900" />
</p>

[POC — `02-performance/06-schema-drift-recover`](examples/playground/pocs/02-performance/06-schema-drift-recover/)

### Enforces data contracts at compile time

Missing required columns, protected columns being removed, or unsafe type changes surface as diagnostic codes (`E010`, `E013`) before a single row is written.

<p align="center">
  <img src="docs/public/demo-data-contracts.gif" alt="rocky compile flags E010 and E013 contract violations on broken_metrics" width="900" />
</p>

[POC — `01-quality/01-data-contracts-strict`](examples/playground/pocs/01-quality/01-data-contracts-strict/)

### Named branches for risk-free experiments

Create a branch, run against it in an isolated schema, inspect, then drop or promote. Column-level lineage shows the downstream blast radius before you ship.

<p align="center">
  <img src="docs/public/demo-branches-replay.gif" alt="rocky branch create, run on branch, and trace column lineage downstream" width="900" />
</p>

[POC — `00-foundations/06-branches-replay-lineage`](examples/playground/pocs/00-foundations/06-branches-replay-lineage/)

### Column-level lineage, not table-level

Trace a single column from a downstream fact back through its aggregations, all the way to the seed. Blast-radius analysis without reading every model.

<p align="center">
  <img src="docs/public/demo-column-lineage.gif" alt="rocky lineage --column traces fct_revenue.total back to seeds.orders.amount" width="900" />
</p>

[POC — `06-developer-experience/01-lineage-column-level`](examples/playground/pocs/06-developer-experience/01-lineage-column-level/)

### AI model generation with a compile-validate loop

Describe what you want in plain English. Rocky generates a Rocky DSL model, compiles it, and retries on parse failure — the `Attempts: 2` line shows the loop catching a first-pass error invisibly.

<p align="center">
  <img src="docs/public/demo-ai-model-generation.gif" alt="rocky ai generates a .rocky model from natural language intent, Attempts: 2" width="900" />
</p>

[POC — `03-ai/01-model-generation`](examples/playground/pocs/03-ai/01-model-generation/)

### PR-time blast-radius with `rocky lineage-diff`

Compare two git refs and get a per-changed-column readout of downstream consumers — pre-rendered Markdown drops straight into a GitHub PR comment. CODEOWNERS-style review tooling can't reach this granularity without a compiled engine.

<p align="center">
  <img src="docs/public/demo-lineage-diff.gif" alt="rocky lineage-diff main lists added and removed columns across two models with downstream consumers per change" width="900" />
</p>

[POC — `06-developer-experience/11-lineage-diff`](examples/playground/pocs/06-developer-experience/11-lineage-diff/)

### Classify columns, mask by environment, gate CI

Tag PII columns in the model sidecar; bind tags to mask strategies in `[mask]` / `[mask.<env>]`. `rocky compliance --env prod --fail-on exception` exits 1 the moment a classified column has no resolved strategy — a one-line CI gate against accidentally-unmasked data.

<p align="center">
  <img src="docs/public/demo-classification-masking.gif" alt="rocky compliance rolls up classification tags to mask strategies; --fail-on exception exits 1, gating CI on unmasked PII" width="900" />
</p>

[POC — `04-governance/05-classification-masking-compliance`](examples/playground/pocs/04-governance/05-classification-masking-compliance/)

### Incremental loads with persistent watermark state

`strategy = "incremental"` plus a `timestamp_column` is all it takes. Rocky writes the high-water mark to the embedded state store; subsequent runs only `INSERT … WHERE timestamp > watermark`. Append 25 rows after a 500-row load — run 2 still finishes in 0.2s.

<p align="center">
  <img src="docs/public/demo-incremental-watermark.gif" alt="rocky run with incremental strategy: run 1 copies 500 rows; appended 25 rows; run 2 only copies the delta in 0.2s" width="900" />
</p>

[POC — `02-performance/01-incremental-watermark`](examples/playground/pocs/02-performance/01-incremental-watermark/)

## Where Rocky is today

The trust primitives — compiler, branches, replay, lineage, contracts, cost attribution — are production-grade on Databricks. We're explicit about the rest:

- **Databricks is the production target for 2026.** Snowflake, BigQuery, and Trino adapters are Beta — connection, execution, and the core run loop work, but conformance coverage is still growing. If your enterprise warehouse is Snowflake or BigQuery and you need it production-grade today, talk to us.
- **AI is a growing surface, not a finished product.** The compile-validate loop (generate → type-check → auto-fix → land) is real and shipped; the broader story (mass refactor across the DAG, auto-migration from a column type change, schema-aware assertion generation) is on the roadmap, not the changelog.
- **Iceberg.** REST-catalog source discovery is Beta. Content-addressed writes round-trip as Iceberg through Delta UniForm — shipped end-to-end (Wave 2). First-class Iceberg-native writes without the Delta intermediate are on the 2026 roadmap.
- **No built-in semantic layer.** Rocky's typed IR is the right home for one. Today, integrate with Cube, the dbt Semantic Layer, or your existing metric store.
- **Orchestration: Dagster is first-class.** A `rocky serve` standalone path exists; native Airflow / Prefect integrations are not yet shipped — they're called from the CLI like any other binary.

If those gaps are blockers for your team, [open a discussion](https://github.com/rocky-data/rocky/discussions) — the roadmap is shaped by where production pipelines are actually getting hurt.

## Subprojects

| Path | Artifact | Language | Description |
|---|---|---|---|
| [`engine/`](engine/) | `rocky` CLI binary | Rust | Core SQL transformation engine — 22-crate Cargo workspace |
| [`integrations/dagster/`](integrations/dagster/) | `dagster-rocky` PyPI wheel | Python | Dagster resource and component wrapping the Rocky CLI |
| [`editors/vscode/`](editors/vscode/) | Rocky VSIX | TypeScript | VS Code extension — LSP client + commands for AI features |
| [`examples/playground/`](examples/playground/) | (config only) | TOML / SQL | Self-contained DuckDB sample pipeline used for smoke tests and benchmarks |

Each subproject has its own README with detailed usage. The [`engine/README.md`](engine/README.md) is the canonical product reference for the Rocky CLI.

## Adapters

| Role | Adapter | Status | Notes |
|------|---------|--------|-------|
| Warehouse | Databricks | Production | SQL Statement API · Unity Catalog · `SHALLOW CLONE` for branches |
| Warehouse | Snowflake | Beta | REST connector · zero-copy `CLONE` for branches · masking policies |
| Warehouse | BigQuery | Beta | REST connector · `CREATE TABLE … COPY` for branches |
| Warehouse | DuckDB | Local / Testing | Embedded · powers `rocky playground` (no credentials needed) |
| Warehouse | Trino | Beta | REST `/v1/statement` polling client · Basic + JWT auth · Docker conformance harness behind `trino-conformance` feature |
| Source | Fivetran | Production | REST connector + table discovery |
| Source | Airbyte | Beta | Catalog discovery |
| Source | Iceberg | Beta | REST catalog discovery of namespaces and tables |
| Source | Manual | Production | Schema/table lists inline in `rocky.toml` |

Building a warehouse Rocky doesn't ship in-tree (ClickHouse, Redshift, …)? See the [Adapter SDK guide](https://rocky-data.dev/guides/adapter-sdk/) and the [Rust-native skeleton POC](examples/playground/pocs/07-adapters/06-rust-native-adapter-skeleton/).

## Building from source

```bash
git clone https://github.com/rocky-data/rocky.git
cd rocky
just build       # builds engine + dagster wheel + vscode extension
just test        # runs all test suites
just lint        # cargo clippy/fmt + ruff + eslint
```

`just` is optional — you can also build each subproject directly. See [`CONTRIBUTING.md`](CONTRIBUTING.md) for per-subproject build commands.

## Releases

Each artifact is released independently using a tag-namespaced scheme:

- `engine-v*` → Rocky CLI binary (cross-compiled, on GitHub Releases)
- `dagster-v*` → `dagster-rocky` wheel
- `vscode-v*` → Rocky VSIX

See [`CONTRIBUTING.md`](CONTRIBUTING.md#releases) for the full release flow.

## Documentation

Full documentation: **[rocky-data.dev](https://rocky-data.dev)** — concepts, guides, CLI reference, Dagster integration, adapter SDK.

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md). Before opening a PR, please read the cross-project change guidance — schema and DSL changes must update consumers atomically.

## Sponsoring

Rocky is free and open source. If it saves your team time, consider [sponsoring the project](https://github.com/sponsors/hugocorreia90) so development can continue.

## License

[Apache 2.0](LICENSE)
