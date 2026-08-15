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

**Rocky checks your whole SQL pipeline before it runs, and tells you what a change will break.**

Rocky works with Databricks, Snowflake, BigQuery, and DuckDB. You keep your warehouse and your existing SQL. Apache 2.0.

The failures that cost the most are the quiet ones. A source column changes type. Someone renames a column and three models stop working. A query works in dev and fails in production. Rocky finds all of these at check time.

```
   you edit SQL          rocky compile              rocky run
        │                      │                        │
        ▼                      ▼                        ▼
   ┌─────────┐        ┌──────────────────┐        ┌───────────┐
   │  model  │───────►│  check the whole │───────►│ warehouse │
   │  files  │        │  pipeline: types,│        │  writes   │
   └─────────┘        │  refs, contracts │        └───────────┘
                      └──────────────────┘
                               │
                               │ a problem is found here,
                               ▼ so nothing runs
                        E010: required column
                        `order_id` is missing
```

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

No credentials needed — the playground runs on local DuckDB.

For production deploys, use `rocky plan` (saves what will change) then `rocky apply <plan-id>` (runs it). For local work and automation, `rocky run` does it all in one step.

## Who Rocky is for

Rocky is built first for **data engineers on Databricks**, where a silent failure costs real money and Dagster runs the schedule. The **Snowflake and BigQuery** adapters are in Beta. See [Where Rocky is today](#where-rocky-is-today).

## See it in action

Each demo is in [`examples/playground/pocs/`](examples/playground/pocs/). Change into a demo directory and run `./run.sh`.

### See what breaks before you merge, with `rocky lineage-diff`

Compare two versions of your project. Rocky lists the downstream tables and columns that each change affects. Paste the list into a GitHub pull request comment.

<p align="center">
  <img src="docs/public/demo-lineage-diff.gif" alt="rocky lineage-diff main lists added and removed columns across two models with downstream consumers per change" width="900" />
</p>

[POC: `06-developer-experience/11-lineage-diff`](examples/playground/pocs/06-developer-experience/11-lineage-diff/)

### More demos

- [Schema drift recovery](examples/playground/pocs/02-performance/06-schema-drift-recover/): source column type changes upstream; Rocky detects it and rebuilds safely.
- [Data contracts](examples/playground/pocs/01-quality/01-data-contracts-strict/): missing required columns, dropped protected columns, or unsafe type changes surface as errors (`E010`, `E011`, `E013`) before a row is written.
- [BigQuery cost to the byte](examples/playground/pocs/07-adapters/05-bigquery-native-queries/): `bytes_scanned` in the run receipt matches BigQuery's billing number exactly (requires credentials).
- [Named branches + replay](examples/playground/pocs/00-foundations/06-branches-replay-lineage/): run against an isolated schema copy, inspect, then drop or promote.
- [Agent policy](examples/playground/pocs/03-ai/07-policy/): a `[policy]` block grades what an agent may do on its own; pinned scenarios catch a loosened rule in CI.
- [Column lineage](examples/playground/pocs/06-developer-experience/01-lineage-column-level/): trace a column in a downstream model back to its source.
- [Incremental loads](examples/playground/pocs/02-performance/01-incremental-watermark/): set `strategy = "incremental"` and Rocky only processes new rows each run.
- [Data masking](examples/playground/pocs/04-governance/05-classification-masking-compliance/): tag PII columns, set masking per environment, fail the check if anything goes out unmasked.
- [AI model generation](examples/playground/pocs/03-ai/01-model-generation/): describe what you want; Rocky writes the SQL, checks it, and retries if something's wrong.

## In your editor

The checker runs as a language server in VS Code. You see type mismatches and broken references while you write, not later in CI. Column types show when you hover. Go-to-definition works across all your models.

The Rocky Inspector shows a model's columns, where each column came from, its tests, its cost, and which columns hold sensitive data.

<p align="center">
  <img src="editors/vscode/media/demo-inspector.gif" alt="The Rocky Inspector's Overview as a model trust dashboard, its Governance card flagging two classified columns with one left unmasked" width="900" />
</p>

[Install the VS Code extension →](https://marketplace.visualstudio.com/items?itemName=rocky-data.rocky)

## When an AI agent writes your pipelines

Agents now write real pipeline changes. An agent that is trusted too much, with production access, can destroy real data in seconds. Rocky treats an agent as an operator with a controlled path to production.

Rocky type-checks every change an agent writes. The agent produces a plan. A plan never applies itself. It must first pass the rules you wrote, and every decision goes into a ledger you can query.

```
   an agent drafts a change
              │
              ▼
   ┌─────────────────────┐
   │ compiler            │   types and contracts are checked
   │                     │   as the agent writes
   └──────────┬──────────┘
              ▼
   ┌─────────────────────┐
   │ plan                │   a plan never applies itself
   └──────────┬──────────┘
              ▼
   ┌─────────────────────┐
   │ your [policy] rules │
   └──────────┬──────────┘
              │
     ┌────────┴────────┬──────────────────┐
     │ require review  │ allow            │ deny
     ▼                 │                  ▼
  ┌──────────────┐     │        ┌───────────────────┐
  │ a human      │     │        │ refused. Rocky    │
  │ approves     │     │        │ rolls the write   │
  └──────┬───────┘     │        │ back.             │
         │             │        └─────────┬─────────┘
         └──────┬──────┘                  │
                ▼                         │
     ┌────────────────────┐               │
     │ apply, then re-run │               │
     │ the checks         │               │
     └─────────┬──────────┘               │
               │                          │
               └────────────┬─────────────┘
                            ▼
              ┌──────────────────────────────┐
              │ every decision lands here:   │
              │ rocky audit · rocky brief    │
              └──────────────────────────────┘
```

- **You write the rules in `rocky.toml`.** A `[policy]` rule says what each principal may do, and where. The answer is allow, require review, or deny. If a change touches too many downstream models, Rocky downgrades an allow to require review. It does the same when it cannot work out how far the change reaches. You can test the rules: `[[policy.tests]]` scenarios run through the real evaluator, so `rocky policy test` in CI catches an edit that would have opened a hole.
- **A plan written by AI waits for a human.** The agent proposes. `rocky apply` refuses an unapproved AI-authored plan unless one of your `[policy]` rules grants that scope. The engine enforces this. It is not a convention you can forget.
- **You can ask the ledger what happened.** `rocky audit --for <table>` says who changed what, under whose authority, and what was verified. `rocky review --queue` ranks what waits on you. `rocky brief` is the morning digest, and every line cites the ledger.
- **Rocky tracks what an agent builds back to its recipe.** This applies on the content-addressed path. `rocky gc --derivable` lists artifacts whose recorded recipe matches their bytes. A review gates each eviction, and it leaves a tombstone. `rocky restore` then rebuilds the exact bytes, or it refuses. Restore works for a recipe that reads no recorded upstreams. It cannot yet rebuild a recipe with several inputs, so eviction is not reversible for every artifact.
- **The agent surface is MCP.** `rocky mcp` exposes 30 tools. They ground an agent in your real schemas and data, draft changes that compile in the same call, and propose the result. A denied draft leaves nothing on disk.

<p align="center">
  <img src="docs/public/demo-policy-enforce.gif" alt="an agent's change to a contracted model is planned, rocky apply run as the agent principal is denied by the policy plane with the rule named, and rocky audit shows the recorded decision" width="900" />
</p>

[POC: `04-governance/11-agent-policy`](examples/playground/pocs/04-governance/11-agent-policy/) drives this end to end, and the policy itself is regression-tested: `rocky policy test` runs pinned scenarios in CI and fails when an edit loosens a rule ([POC: `03-ai/07-policy`](examples/playground/pocs/03-ai/07-policy/)).

An agent earns freedom one step at a time. A retry after a known transient failure is free. You can let a provably additive schema change flow through under policy. Everything else waits for review until you grant it.

Budgets tighten when failures repeat. They recover only as those failures age out of the window you set. `rocky policy freeze` is the kill switch. To see how an agent writes, proposes, and passes each gate, read [Operating Rocky with agents](https://rocky-data.dev/concepts/operating-rocky-with-agents/).

## Where Rocky is today

These features are ready for production on Databricks: the checker, named branches, replay, column lineage, rule enforcement, and per-model cost. The rest is still in progress.

- **Databricks is the 2026 focus.** Snowflake, BigQuery, and Trino run the core loop, but they are less thorough. [Talk to us](https://github.com/rocky-data/rocky/discussions) if you need one of them in production now.
- **AI features are early.** Generate, check, and fix is shipped. `rocky ai-test` writes assertions for a model from its stated intent. Large refactors and automatic migration on a type change are still on the roadmap.
- **Replay re-runs your work, and says what it cannot re-run.** Every run leaves a content-addressed record. `rocky replay` reads that record and checks it against the ledger. For a deterministic content-addressed model, `rocky replay --execute --verify` runs the recorded recipe again and confirms the output is identical, byte for byte. It can do this locally, or on the live warehouse in a separate replay schema. If a model reads a source that can change, Rocky marks it non-replayable instead of quietly re-running it against today's data. Rocky also flags SQL that is not deterministic, so a difference is reported as expected rather than as a failure.
- **Iceberg.** Reading from a REST catalog is Beta. Today, content-addressed writes land as Iceberg-readable tables through Delta UniForm. Native Iceberg writes, with no Delta step in between, are on the roadmap.
- **No built-in metrics layer.** Use Cube, or whichever metrics layer you already run.
- **Dagster is the one built-in scheduler integration** ([`dagster-rocky`](integrations/dagster/)). For anything else, use the [`rocky-sdk`](sdk/python/) Python client or `rocky serve`. `rocky tick` can also run cron and freshness schedules with no orchestrator, but it is experimental.

[Open a discussion](https://github.com/rocky-data/rocky/discussions) if any of these are a blocker.

## You can leave

`rocky emit-sql` writes your transformation models out as plain SQL, in dependency order. It runs offline and needs no warehouse connection.

Two kinds of model produce no standalone SQL: ephemeral models, which Rocky inlines as CTEs, and strategies that need a live warehouse to render, such as a Snowflake dynamic table. Rocky reports those on stderr instead of dropping them quietly.

It is one command, not a rewrite. Adopting Rocky is not a one-way door. See [No lock-in](https://rocky-data.dev/guides/no-lock-in/).

Already have a project in another tool? `rocky import-dbt` converts a dbt Core project in one command. See the [import guide](https://rocky-data.dev/guides/migrate-from-dbt/).

## Subprojects

| Path | What ships | Language | What it does |
|---|---|---|---|
| [`engine/`](engine/) | `rocky` CLI and `rocky-lsp` | Rust | Core engine: SQL checking, drift detection, incremental loads, adapters |
| [`sdk/python/`](sdk/python/) | `rocky-sdk` (PyPI) | Python | Python client wrapping the CLI, for notebooks and scripts |
| [`integrations/dagster/`](integrations/dagster/) | `dagster-rocky` (PyPI) | Python | Dagster resource built on `rocky-sdk` |
| [`editors/vscode/`](editors/vscode/) | Rocky VS Code extension | TypeScript | Live checking, syntax highlighting, AI commands |
| [`examples/playground/`](examples/playground/) | (config only) | TOML / SQL | Sample DuckDB pipeline, no credentials needed |

## Adapters

| Role | Adapter | Status |
|------|---------|--------|
| Warehouse | Databricks | Production |
| Warehouse | Snowflake | Beta |
| Warehouse | BigQuery | Beta |
| Warehouse | DuckDB | Local / Testing |
| Warehouse | Trino | Beta |
| Source | Fivetran | Production |
| Source | Airbyte | Beta |
| Source | Iceberg | Beta |
| Source | Manual | Production |

Building a connector for ClickHouse, Redshift, or another warehouse? See the [Adapter SDK guide](https://rocky-data.dev/guides/adapter-sdk/) and the [skeleton POC](examples/playground/pocs/07-adapters/06-rust-native-adapter-skeleton/).

## Building from source

```bash
git clone https://github.com/rocky-data/rocky.git
cd rocky
just build   # engine + sdk + dagster + vscode
just test
just lint
```

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for per-subproject build commands.

## Releases

Each artifact ships independently via CI-driven tags:

- `engine-v*` → Rocky CLI binary on GitHub Releases (macOS, Linux, Windows)
- `sdk-v*` → `rocky-sdk` on PyPI
- `dagster-v*` → `dagster-rocky` on PyPI
- `vscode-v*` → Rocky extension on the VS Code Marketplace

## Documentation

Full docs at **[rocky-data.dev](https://rocky-data.dev)**.

New to Rocky? **[`ROCKY_EXPLAINED.md`](ROCKY_EXPLAINED.md)** is a plain-English walkthrough of the whole system, with diagrams.

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md). Schema or DSL changes need to update all dependent pieces at once — read the cross-project change guidance before opening a PR.

## Sponsoring

Rocky is free and open source. If it saves your team time, consider [sponsoring the project](https://github.com/sponsors/hugocorreia90).

## License

[Apache 2.0](LICENSE)
