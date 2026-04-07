---
name: rocky-poc
description: Authoring a new POC under `examples/playground/pocs/`. Use when demoing a single Rocky feature (drift, incremental, contracts, lineage, AI, hooks, etc.) that doesn't already exist in `engine/examples/`. Enforces the POC conventions so it runs via a single `./run.sh` with no credentials.
---

# Authoring a new Rocky POC

`examples/playground/` is a curated catalog of small, self-contained POCs — one per feature. Each POC is runnable with one `./run.sh`, defaults to DuckDB (no credentials), and lives in a category folder.

## When to use this skill

- Demoing a Rocky feature not already covered in `engine/examples/` (the *official* starter set lives there and should NOT be duplicated in playground)
- Building a runnable smoke test for a new capability
- Creating a benchmark fixture (anything under `benchmarks/` is off-limits unless explicitly asked)

## What's already covered in `engine/examples/`

Do NOT build a duplicate playground POC for these — link to them instead:

| `engine/examples/` path | What it shows |
|---|---|
| `quickstart/` | 3-model "hello world" pipeline |
| `multi-layer/` | Generic Bronze/Silver/Gold |
| `dbt-migration/` | dbt → Rocky before/after |
| `ai-intent/` | AI-intent feature walkthrough |
| `dagster-integration/` | Basic Dagster wiring |

Playground POCs should cover what those don't: incremental watermarks, drift diagnostics, contract validation, hooks, lineage, AI sync, custom adapters, partitions, column lineage, etc.

## POC categories

Pick the category whose id matches the feature group:

```
00-foundations          Fundamental concepts, playground baseline
01-quality              Contracts, checks, drift, data quality
02-performance          Incremental, partitioning, checksums, adaptive concurrency
03-ai                   AI intent, AI sync, AI test, AI explain
04-governance           Permissions, tags, workspace isolation
05-orchestration        Dagster, sensors, schedules, hooks
06-developer-experience LSP, lineage, VS Code features
07-adapters             Adapter-specific behavior
```

## Scaffold

Use the scaffolder — don't hand-create the directory:

```bash
cd examples/playground
./scripts/new-poc.sh <category> <id-name>
# e.g.
./scripts/new-poc.sh 02-performance 07-late-arriving-data
```

This copies `scripts/_poc-template/` into `pocs/<category>/<id-name>/` and chmods `run.sh`.

## POC structure

Every POC MUST contain:

```
README.md         # Structured: feature, why distinctive, layout, run, expected output
rocky.toml        # Minimal POC-specific config (DuckDB by default)
run.sh            # Executable end-to-end demo (chmod +x)
models/           # .sql / .rocky files + .toml sidecars
contracts/        # Only if contracts are part of the feature
seeds/            # CSV / SQL sample data — keep ≤1000 rows
data/seed.sql     # Optional — auto-loaded by `rocky test`
expected/         # Golden JSON output from run.sh (gitignored, regenerated each run)
```

## Minimal `rocky.toml` (DuckDB)

```toml
[adapter]
type = "duckdb"
path = "poc.duckdb"

[pipeline.poc]
strategy = "full_refresh"   # or "incremental"

[pipeline.poc.source]
schema_pattern = { prefix = "raw__", separator = "__", components = ["source"] }

[pipeline.poc.target]
catalog_template = "poc"
schema_template = "staging__{source}"
```

Defaults that can be omitted:
- `pipeline.type` defaults to `"replication"` — omit it.
- Unnamed `[adapter]` with a `type` key auto-wraps as `adapter.default`; pipeline adapter refs default to `"default"` — omit `adapter = "local"` lines.
- `[state]\nbackend = "local"` is the default — omit it.
- `auto_create_catalogs = false` / `auto_create_schemas = false` are defaults — omit unless intentionally `true`.
- Model sidecar `name` defaults to filename stem; `target.table` defaults to `name` — omit when redundant.
- `models/_defaults.toml` provides directory-level `[target]` defaults (`catalog`, `schema`) — use when 2+ models share values.

## Credential gating

Default to **DuckDB** so the POC runs with zero config. Only require credentials when the feature genuinely can't be demoed without them (Databricks governance, Snowflake dynamic tables, Fivetran, Anthropic API).

If credentials are required, fail fast at the top of `run.sh`:

```bash
: "${DATABRICKS_HOST:?Set DATABRICKS_HOST before running}"
: "${DATABRICKS_TOKEN:?Set DATABRICKS_TOKEN before running}"
```

And mark the credential requirement prominently in `README.md`.

## Runtime idioms (post-fix rocky 0.1.x)

| Goal | Command |
|---|---|
| Type-check models without a warehouse | `rocky compile --models models/ --contracts contracts/` |
| Run model tests against in-memory DuckDB (auto-loads `data/seed.sql`) | `rocky test --models models/ --contracts contracts/` |
| CI pipeline (compile + test) | `rocky ci --models models/ --contracts contracts/` |
| Validate the pipeline config | `rocky validate -c rocky.toml` |
| Discover sources from local DuckDB | `rocky -c rocky.toml discover` |
| Preview replication SQL | `rocky -c rocky.toml plan --filter source=<name>` |
| Execute the pipeline | `rocky -c rocky.toml run --filter source=<name>` |
| Inspect watermarks | `rocky -c rocky.toml state` |
| Column-level lineage | `rocky lineage <model> --models models/ [--column <col>]` |
| Schema-pattern aware diagnostics | `rocky doctor` |

For `discover`/`plan`/`run`, seed data must be manually loaded into the DuckDB file first — typically `duckdb poc.duckdb < data/seed.sql` at the top of `run.sh`. `rocky test` auto-loads it.

## `run.sh` conventions

- `set -euo pipefail`
- Print a header describing the feature
- Run the canonical command sequence (compile → test → run → inspect)
- Exit 0 on success; the POC is smoke-tested by `./scripts/run-all-duckdb.sh` with a 60s timeout

## Verification before committing

```bash
cd pocs/<cat>/<id-name>
./run.sh                          # Must exit 0
rocky validate -c rocky.toml      # If the POC uses a pipeline path
```

For the catalog as a whole:

```bash
cd examples/playground
./scripts/run-all-duckdb.sh       # All credential-free POCs, 60s timeout each
```

## README structure

Five sections, in this order:

1. **Feature** — one-sentence description of what this POC demonstrates
2. **Why it's distinctive** — why a user should care (what it proves about Rocky vs alternatives)
3. **Layout** — tree of files with one-line descriptions
4. **Run** — copy-pasteable commands to run it
5. **Expected output** — what the user should see (snippet of `run.sh` output, not the full golden file)

## Commit style

```
feat(02-performance/07-late-arriving-data): add late-arriving watermark POC
```

Scope by POC id when the change is POC-specific.

## Off-limits

- `benchmarks/` — Rocky vs dbt-core / dbt-fusion / PySpark perf suite. Don't touch unless explicitly asked.
- Duplicating anything already in `engine/examples/`. Link to it instead.
