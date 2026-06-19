<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/rocky-readme-dark.svg" />
    <img src="docs/rocky-readme-light.svg" alt="Rocky" />
  </picture>
</p>

[![Engine CI](https://github.com/rocky-data/rocky/actions/workflows/engine-ci.yml/badge.svg)](https://github.com/rocky-data/rocky/actions/workflows/engine-ci.yml)
[![SDK CI](https://github.com/rocky-data/rocky/actions/workflows/sdk-ci.yml/badge.svg)](https://github.com/rocky-data/rocky/actions/workflows/sdk-ci.yml)
[![Dagster CI](https://github.com/rocky-data/rocky/actions/workflows/dagster-ci.yml/badge.svg)](https://github.com/rocky-data/rocky/actions/workflows/dagster-ci.yml)
[![VS Code CI](https://github.com/rocky-data/rocky/actions/workflows/vscode-ci.yml/badge.svg)](https://github.com/rocky-data/rocky/actions/workflows/vscode-ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

**Rocky is a tool that checks and runs your SQL data pipelines — and catches problems before they reach your warehouse.**

It works with Databricks, Snowflake, BigQuery, and DuckDB. You keep your warehouse and your existing SQL. Rocky reads your pipeline, understands what every column does, and tells you when something is wrong — before running anything. Apache 2.0.

The problems that cost data teams the most are the ones nobody notices until it's too late: a source table's columns change and break something downstream, a column gets renamed and three other models silently stop working, a query that works in dev fails in prod because it uses a function the production database doesn't support. Rocky catches all of these at the "check" step — before any data moves.

<p align="center">
  <img src="docs/public/demo-quickstart.gif" alt="Rocky quickstart: create a project, compile, and run 3 models in under 15s" width="900" />
</p>

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

No credentials needed — the playground runs entirely on a local database (DuckDB) so you can try everything offline.

`rocky run` runs your whole pipeline in one command, which is fine for local development and automation. For production or code-review deploys, you can split it into two steps: `rocky plan` (which saves a record of exactly what will change) followed by `rocky apply <plan-id>` (which executes it). This gives you a clear audit trail and a chance to review before anything runs.

## Who Rocky is for

Rocky is built first for **data engineers running critical, multi-tenant pipelines on Databricks** where silent failures cost real money and Dagster is already the scheduler. That's the primary audience, and where Rocky is most thoroughly tested.

The next group: **Snowflake and BigQuery teams** who want problems caught before pipelines run rather than after. Those adapters are in Beta today — see [Where Rocky is today](#where-rocky-is-today) below.

## See it in action

Each demo is a self-contained example in [`examples/playground/pocs/`](examples/playground/). `cd` in, run `./run.sh`, and reproduce it yourself.

### See what breaks before you merge — with `rocky lineage-diff`

Compare two versions of your project and get a report of which downstream tables and columns are affected by each change. The output drops straight into a GitHub PR comment, so reviewers can see the impact without having to dig through code.

<p align="center">
  <img src="docs/public/demo-lineage-diff.gif" alt="rocky lineage-diff main lists added and removed columns across two models with downstream consumers per change" width="900" />
</p>

[POC: `06-developer-experience/11-lineage-diff`](examples/playground/pocs/06-developer-experience/11-lineage-diff/)

### More demos

- [Schema drift recovery](examples/playground/pocs/02-performance/06-schema-drift-recover/): a source column's type changes upstream; Rocky detects it and rebuilds the affected table safely instead of letting it silently corrupt.
- [Data contracts at check time](examples/playground/pocs/01-quality/01-data-contracts-strict/): required columns that go missing, protected columns that get removed, or type changes that aren't safe all surface as errors (`E010` / `E013`) before a single row is written.
- [Native BigQuery, cost to the byte](examples/playground/pocs/07-adapters/05-bigquery-native-queries/): the same models run live against BigQuery; the run receipt's `bytes_scanned` matches BigQuery's own billing number exactly (requires credentials).
- [Named branches + replay](examples/playground/pocs/00-foundations/06-branches-replay-lineage/): run your pipeline against an isolated copy of your schema, inspect the results, then drop it or promote it to production.
- [Column-level lineage](examples/playground/pocs/06-developer-experience/01-lineage-column-level/): trace a single column in a downstream report back to the original source it came from.
- [Incremental loads](examples/playground/pocs/02-performance/01-incremental-watermark/): set `strategy = "incremental"` and a timestamp column; Rocky only processes new rows since the last run.
- [Data masking and compliance](examples/playground/pocs/04-governance/05-classification-masking-compliance/): tag columns that contain personal data, choose a masking strategy per environment, and fail the pipeline check if sensitive data would be exposed unmasked.
- [AI model generation](examples/playground/pocs/03-ai/01-model-generation/): describe what you want in plain English; Rocky generates the SQL, checks it, and retries automatically if there's a problem.

## In your editor

The same checker that runs in CI also runs as a language server inside VS Code. That means you see problems — column type mismatches, broken references, violated rules — while you're writing the code, not hours later in CI. Your `.rocky` model files produce SQL live as you type, with column types shown on hover and go-to-definition across all your models.

The Rocky Inspector shows you everything about a model in one place: its columns, where each column came from, what tests it has, how much it costs to run, and which columns contain sensitive data.

<p align="center">
  <img src="editors/vscode/media/demo-inspector.gif" alt="The Rocky Inspector's Overview as a model trust dashboard, its Governance card flagging two classified columns with one left unmasked" width="900" />
</p>

[Install the VS Code extension →](https://marketplace.visualstudio.com/items?itemName=rocky-data.rocky)

## Where Rocky is today

The core features (the checker, named pipeline branches, replay, column lineage tracking, rules enforcement, cost tracking per model) are production-ready on Databricks. Everything else is in progress:

- **Databricks is the main focus for 2026.** Snowflake, BigQuery, and Trino adapters work for connecting, running queries, and the core pipeline loop — but they haven't reached the same level of thoroughness as the Databricks adapter yet. If you need Snowflake or BigQuery in production today, [talk to us](https://github.com/rocky-data/rocky/discussions).
- **AI features are early.** The generate → check → fix loop is shipped. Bigger AI features — mass refactoring across your whole pipeline, automatic migration when a column type changes, auto-generated data quality assertions — are on the roadmap.
- **Iceberg support.** Reading from an Iceberg catalog works in Beta. Writing directly as Iceberg (without going through Delta format first) is planned for 2026.
- **No built-in metrics layer.** Rocky understands your columns and where they come from, but it doesn't define business metrics. For that, integrate with Cube, the dbt Semantic Layer, or whatever metrics tool you already use.
- **Dagster is the one built-in scheduler integration ([`dagster-rocky`](integrations/dagster/)).** Every other scheduler — Airflow, Prefect, Flyte, a cron script — can use the [`rocky-sdk`](sdk/python/) Python client to wrap Rocky in a task. There's also a `rocky serve` HTTP mode. Pre-built integrations for other schedulers aren't shipped yet, but the Python client is the building block.

If any of these gaps are a problem for your team, [open a discussion](https://github.com/rocky-data/rocky/discussions). The roadmap is driven by where real pipelines are actually breaking.

## How it compares to dbt Core

| Problem | What dbt Core does | What Rocky does |
|---|---|---|
| A source table's column type changes | Silent; shows up as a failure later in a downstream model | Caught at check time as error `E013` — blocks the PR |
| A required column disappears | Caught at build time if you've opted into `contract: enforced` | Caught at check time as error `E010` — blocks the PR |
| A column gets renamed and you don't know what breaks | `dbt docs` shows table-level lineage after the fact; dbt Cloud Enterprise adds column lineage in the UI, also after the fact | `rocky lineage-diff` at PR time shows exactly which downstream columns are affected, by name |
| `SELECT *` pulls in a new column you didn't ask for | Silent | Warning `P002` — names the downstream models that are affected |
| SQL that only works on Snowflake gets written for a Databricks project | No check; works in dev, fails in prod | `P001` database-portability warning at check time |
| A run costs twice as much as last week and no one knows which model | No per-model cost; you'd have to dig through warehouse query history | `RunOutput.cost_summary` gives you the cost per model, every run |
| An auditor asks who changed `fct_revenue.amount`, when, and why | Run history in dbt Cloud, but no record of the exact code that produced a given output | `rocky replay <run_id>` gives you a complete record of the code and the output it produced |
| A pipeline fails at 3 AM and half the models already ran | `dbt retry` resumes from the failed model | `rocky run --resume-latest` picks up from the last checkpoint; models that already succeeded are not re-run |

dbt Core created this category, and `rocky import-dbt` converts a vanilla dbt project in one command. In June 2026 dbt Labs open-sourced a new Rust-based runtime called Fusion as dbt Core v2.0 (Apache 2.0, alpha). Fusion adds SQL type-checking and column-level lineage, though it still uses Jinja templates and its safety checks are opt-in rather than enforced.

What neither dbt Core v2.0 nor Fusion includes: named pipeline branches, a record of the exact code and output for each run, cost per model as a built-in field, a cross-database SQL portability check, or declarative data access rules and masking. dbt's governance and cost features are part of its paid platform; Rocky's are all Apache 2.0.

## Subprojects

| Path | What ships | Language | What it does |
|---|---|---|---|
| [`engine/`](engine/) | `rocky` CLI binary | Rust | The core engine — SQL checking, schema drift detection, incremental loads, warehouse adapters. 23 Rust crates. |
| [`sdk/python/`](sdk/python/) | `rocky-sdk` (PyPI) | Python | A Python client that wraps the Rocky CLI — use it in notebooks, scripts, and custom schedulers |
| [`integrations/dagster/`](integrations/dagster/) | `dagster-rocky` (PyPI) | Python | Dagster resource that uses `rocky-sdk` under the hood; maps results to Dagster assets and checks |
| [`editors/vscode/`](editors/vscode/) | Rocky VS Code extension | TypeScript | VS Code extension — live checking, syntax highlighting, AI commands |
| [`examples/playground/`](examples/playground/) | (config only) | TOML / SQL | A self-contained sample pipeline on DuckDB — no credentials needed; used for testing and demos |

Each subproject has its own README with more detail. [`engine/README.md`](engine/README.md) is the main reference for the Rocky CLI.

## Adapters

| Role | Adapter | Status | Notes |
|------|---------|--------|-------|
| Warehouse | Databricks | Production | SQL Statement API · Unity Catalog · schema-prefix branches |
| Warehouse | Snowflake | Beta | REST connector · permission reconciliation · schema-prefix branches |
| Warehouse | BigQuery | Beta | REST connector · schema-prefix branches |
| Warehouse | DuckDB | Local / Testing | Embedded database · powers `rocky playground` (no credentials needed) |
| Warehouse | Trino | Beta | REST polling client · Basic + JWT auth |
| Source | Fivetran | Production | REST connector + table discovery |
| Source | Airbyte | Beta | Catalog discovery |
| Source | Iceberg | Beta | REST catalog — discovers namespaces and tables |
| Source | Manual | Production | List schemas and tables directly in `rocky.toml` |

Building a connector for a warehouse Rocky doesn't support yet (ClickHouse, Redshift, …)? See the [Adapter SDK guide](https://rocky-data.dev/guides/adapter-sdk/) and the [example skeleton POC](examples/playground/pocs/07-adapters/06-rust-native-adapter-skeleton/).

## Building from source

```bash
git clone https://github.com/rocky-data/rocky.git
cd rocky
just build       # builds engine + sdk + dagster wheels + vscode extension
just test        # runs all test suites
just lint        # cargo clippy/fmt + ruff + eslint
```

`just` is optional — you can build each subproject on its own too. See [`CONTRIBUTING.md`](CONTRIBUTING.md) for per-subproject build commands.

## Releases

Each piece ships independently, tagged separately:

- `engine-v*` → Rocky CLI binary (built for macOS, Linux, and Windows, available on GitHub Releases)
- `sdk-v*` → `rocky-sdk` Python package on PyPI
- `dagster-v*` → `dagster-rocky` Python package on PyPI
- `vscode-v*` → Rocky VS Code extension on the Marketplace

See [`CONTRIBUTING.md`](CONTRIBUTING.md#releases) for the full release process.

## Documentation

Full documentation is at **[rocky-data.dev](https://rocky-data.dev)**: concepts, guides, CLI reference, the Python SDK, Dagster integration, and the adapter SDK.

New to Rocky and want the whole thing explained in plain English? **[`ROCKY_EXPLAINED.md`](ROCKY_EXPLAINED.md)** is a single file that walks through every part of Rocky from the ground up — the checker, the pipeline model, how adapters work, incremental watermarks, data contracts, masking, column lineage, and more — with diagrams throughout.

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md). Before opening a PR, read the cross-project change guidance: changes to the output format or the Rocky DSL language need to update all the dependent pieces at the same time.

## Sponsoring

Rocky is free and open source. If it saves your team time, consider [sponsoring the project](https://github.com/sponsors/hugocorreia90) so development can continue.

## License

[Apache 2.0](LICENSE)
