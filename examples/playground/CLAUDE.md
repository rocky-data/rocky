# Rocky Playground

A curated catalog of small POCs that showcase Rocky's capabilities, plus the benchmark suite. This directory is part of the `rocky-data/rocky` monorepo, alongside `engine/`, `integrations/dagster/`, and `editors/vscode/`.

This file is for **agents authoring new POCs in this directory**. For the engine's own coding standards see `../../engine/CLAUDE.md`.

## What this directory is

- **`pocs/`** — One folder per feature, each fully self-contained and runnable.
- **`benchmarks/`** — Rocky vs dbt-core / dbt-fusion / PySpark performance suite. Don't touch unless explicitly asked.
- **`scripts/`** — POC scaffolder and CI helper.

The official starter examples live in `../../engine/examples/` (quickstart, multi-layer, dbt-migration, ai-intent, dagster-integration). **Do not duplicate them here.** This directory covers what they don't.

## Authoring conventions

### POC structure

Every POC under `pocs/<category>/<id>-<name>/` must contain:

```
README.md      # Structured: feature, why distinctive, layout, run, expected output
rocky.toml     # Minimal POC-specific config (DuckDB by default)
run.sh         # Executable end-to-end demo (chmod +x)
models/        # .sql / .rocky files + .toml sidecars
contracts/     # Only if contracts are part of the feature
seeds/         # CSV / SQL sample data — keep ≤1000 rows
expected/      # Captured golden output from run.sh (gitignored)
```

Use the scaffolder:

```bash
./scripts/new-poc.sh <category> <id-name>
```

### POC scope

- Each POC focuses on **one** feature group. If it grows beyond ~5 models or needs deep setup, split it.
- Default to **DuckDB**. Only require credentials when the feature genuinely requires Databricks / Snowflake / Anthropic API / Fivetran. Mark credential needs prominently in the README.
- POCs must be **runnable** with a single `./run.sh` invocation.
- The POC's `expected/` directory is for golden JSON output that committed runs would produce — gitignored, regenerated each run.

### Runtime conventions (post-fix rocky 0.1.x)

The rocky binary now supports DuckDB end-to-end. Use these idioms:

| Goal | Command |
|---|---|
| Type-check models without warehouse | `rocky compile --models models/ --contracts contracts/` |
| Run model tests against in-memory DuckDB (auto-loads `data/seed.sql`) | `rocky test --models models/ --contracts contracts/` |
| CI pipeline (compile + test) | `rocky ci --models models/ --contracts contracts/` |
| Validate the pipeline config | `rocky validate -c rocky.toml` |
| Discover sources from local DuckDB | `rocky -c rocky.toml discover` |
| Preview replication SQL | `rocky -c rocky.toml plan --filter source=<name>` |
| Execute the pipeline | `rocky -c rocky.toml run --filter source=<name>` |
| Inspect watermarks | `rocky -c rocky.toml state` |
| Column-level lineage | `rocky lineage <model> --models models/ [--column <col>]` |
| Schema-pattern aware diagnostics | `rocky doctor` |

### POC TOML schema (DuckDB pipeline)

```toml
[adapter]
type = "duckdb"
path = "poc.duckdb"

[pipeline.poc]
strategy = "full_refresh"

[pipeline.poc.source]
schema_pattern = { prefix = "raw__", separator = "__", components = ["source"] }

[pipeline.poc.target]
catalog_template = "poc"
schema_template = "staging__{source}"

[pipeline.poc.execution]
concurrency = 4
```

Minimal-config defaults that can be omitted:
- `pipeline.type` defaults to `"replication"` — omit it.
- Unnamed `[adapter]` with a `type` key auto-wraps as `adapter.default`. Pipeline source/target/discovery adapter refs default to `"default"` — omit `adapter = "local"` lines.
- `[state]\nbackend = "local"` is the default — omit it.
- `auto_create_catalogs = false` / `auto_create_schemas = false` are defaults — omit them (keep `= true` when intentional).
- Model sidecar `name` defaults to filename stem, `target.table` defaults to `name` — omit when redundant.
- `models/_defaults.toml` provides directory-level `[target]` defaults (`catalog`, `schema`) — use when 2+ models share the same values.

For seed data, ship `data/seed.sql` that creates `raw__<source>.<table>` schemas (matching the prefix). `rocky test` auto-loads it; `rocky discover/plan/run` need it manually loaded with `duckdb poc.duckdb < data/seed.sql`.

### What's off-limits because rocky/examples/ already covers it

Do not build POCs that simply replicate these — link to them in the POC's README instead:

| Already covered in rocky/examples | Don't build a duplicate POC for |
|---|---|
| `quickstart/` | A 3-model "hello world" pipeline |
| `multi-layer/` | Generic Bronze/Silver/Gold |
| `dbt-migration/` | Hand-written before/after dbt vs Rocky comparison |
| `ai-intent/` | Pre-generated AI test fixtures |
| `dagster-integration/` | Basic Dagster wiring |

POCs should cover features those examples don't show: incremental, drift, contract diagnostics, hooks, lineage, AI sync, custom adapters, etc.

## Verification before committing a new POC

```bash
cd pocs/<cat>/<id>-<name>
./run.sh                                    # Should exit 0
rocky validate -c rocky.toml                # If POC uses pipeline path
```

For the catalog as a whole:

```bash
./scripts/run-all-duckdb.sh                 # All credential-free POCs, 60s timeout each
```

Credential-gated POCs should fail fast with a clear `: "${VAR:?Set VAR before running}"` guard at the top of `run.sh`.

## Git conventions

- Match `../../engine/CLAUDE.md`: conventional commits (`feat:`, `fix:`, etc.), no `Co-Authored-By` trailers.
- Scope by POC when relevant: `feat(02-performance/01-incremental-watermark): add seed delta`
- Don't push to remote without explicit user confirmation.
