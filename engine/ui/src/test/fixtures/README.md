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
