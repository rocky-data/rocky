# Captured fixtures

Files here are recorded from a real `rocky serve`, not written by hand. A
hand-written fixture agrees with whatever the test author believed; a
captured one disagrees when the engine changes.

`@rocky-fixtures` (see `vite.config.ts`) points at the dagster corpus, which
`just regen-fixtures` records from a **replication** project. Its DAG has two
nodes, both of kinds the model route cannot serve. Fixtures in this directory
cover what that corpus does not.

## `dag-mixed-kinds.json`

`GET /api/v1/dag` from a project that produces seven of the engine's eight
node kinds in one graph.

```
seed ──────────── country_codes
snapshot ──────── customer_history
source ────────▶ load          (ecommerce)
quality ───────── nightly_dq
transformation ─▶ transformation ─▶ transformation ─▶ test
  raw_orders       customer_orders   revenue_summary   revenue_summary::…
```

The eighth kind, `replication`, cannot appear: the parser expands a
replication pipeline into a `source` + `load` pair, and the variant survives
only to read stored DAGs (`unified_dag.rs`).

### To record it again

```bash
rocky playground /tmp/dagfix          # a transformation pipeline, 3 models
```

Then, in `/tmp/dagfix/rocky.toml`, add a replication pipeline (`source` +
`load`), a `type = "quality"` pipeline, and a `type = "snapshot"` pipeline.
Add a `[[tests]]` block to `models/revenue_summary.toml` for the `test` node,
and a `seeds/country_codes.csv` for the `seed` node. Then:

```bash
cd /tmp/dagfix && rocky serve --port 8137 &
curl -s http://127.0.0.1:8137/api/v1/dag | python3 -m json.tool > dag-mixed-kinds.json
```

The same project shape is built by `engine/rocky/tests/serve_ui.rs`, which
asserts against the live API what this fixture asserts in vitest.

## `dag-two-pipelines.json` and `model-list-two-pipelines.json`

`GET /api/v1/dag` and `GET /api/v1/models` from one server, on a project
with two transformation pipelines, each with its own models directory.

```
pipeline     directory    in the DAG         in the model list
playground   models/      raw_orders         yes
                          customer_orders    yes
                          revenue_summary    yes
reporting    reporting/   weekly_revenue     yes  → /models/weekly_revenue is 200
```

The DAG and the server's compile both read every transformation pipeline's own
models directory (#2011), so every drawn model is listed. The estate screen
reads the model list to know which nodes open.

### To record them again

```bash
rocky playground /tmp/twofix
```

Then, in `/tmp/twofix/rocky.toml`, add a second transformation pipeline:

```toml
[pipeline.reporting]
type = "transformation"
models = "reporting/**"

[pipeline.reporting.target.governance]
auto_create_schemas = true
```

Add `reporting/weekly_revenue.sql` (`SELECT 1 AS week, 2 AS revenue`) and a
`reporting/weekly_revenue.toml` sidecar with `name = "weekly_revenue"`, a
`full_refresh` strategy and a `playground.main.weekly_revenue` target. Then:

```bash
cd /tmp/twofix && rocky serve --port 8137 &
curl -s http://127.0.0.1:8137/api/v1/dag    | python3 -m json.tool > dag-two-pipelines.json
curl -s http://127.0.0.1:8137/api/v1/models | python3 -m json.tool > model-list-two-pipelines.json
```

Wait for `/api/v1/models` to answer `200` before recording: until the first
compile lands it answers `503 engine_not_ready`. `serve_ui.rs` builds the same
project and fails if either capture stops describing the live server.

## `dag-stale-compile.json` and `model-list-stale-compile.json`

The "not compiled" state, recorded from a real server: the DAG draws
`weekly_revenue`, the model list lacks it, `/models/weekly_revenue` answers
`404`, and the DAG node carries `"compiled": false`.

`/dag` reads the files on disk on every request. `/models` reads the last
compile. So a model written after the compile is drawn and not listed. This
is the only way to see the state from a real server on this project. A
pipeline-model load error (W014, for example a model name shared across
`models/` and `reporting/`) makes `/models` serve `models/` alone, but
`/dag` loads the same models and answers the same error with a `500`, so it
draws nothing and there is no pair to record.

### To record them again

Start from the two-pipeline project above, but leave `reporting/` empty:
`rocky playground /tmp/stalefix`, append the `[pipeline.reporting]` block, and
`mkdir /tmp/stalefix/reporting`. Then:

```bash
cd /tmp/stalefix && rocky serve --port 8137 &
# wait until /api/v1/models answers 200, then write the model AFTER the compile:
echo 'SELECT 1 AS week, 2 AS revenue' > reporting/weekly_revenue.sql
# and reporting/weekly_revenue.toml, the same sidecar as above
curl -s http://127.0.0.1:8137/api/v1/dag    | python3 -m json.tool > dag-stale-compile.json
curl -s http://127.0.0.1:8137/api/v1/models | python3 -m json.tool > model-list-stale-compile.json
```

`serve_ui.rs` (`a_model_written_after_the_compile_is_drawn_but_not_listed`)
builds the same project and fails if either capture stops describing the live
server.
