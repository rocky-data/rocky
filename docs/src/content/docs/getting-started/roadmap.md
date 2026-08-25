---
title: Roadmap
description: What Rocky ships today, what is still Beta, and what comes next.
sidebar:
  order: 4
---

The trust primitives (compiler, branches, run records, lineage, contracts, cost
attribution) are production-grade on Databricks. Here is the state of everything
else.

## Shipped and production-grade

- **Databricks** is the production target for 2026: SQL Statement API, Unity
  Catalog, OAuth M2M, adaptive concurrency, and schema-prefix branches.
  Warehouse-native `SHALLOW CLONE` is a follow-up.
- **The compiler.** Typed column-level inference, the diagnostic codes, and the
  language server all behave the same in CI and in your editor.
- **Branches and run records.** A named branch is an isolated schema. Each run
  writes a content-addressed record, meaning the artifacts are keyed by the hash
  of their bytes. `rocky replay` inspects that record. For a deterministic
  model, `rocky replay --execute --verify` re-runs the recorded recipe locally
  on DuckDB and reproduces the output bit for bit. Add `--warehouse` to re-run
  it in an isolated replay schema on the live warehouse instead.
- **Cost attribution.** Per-model cost lands on every run record, and `[budget]`
  blocks fail a run on overspend.
- **The AI compile-validate loop.** `rocky ai` generates a model, compiles it,
  and auto-fixes parse errors before it lands.
- **Deterministic surrogate keys.** A model declares a
  [`[[surrogate_key]]`](/reference/model-format/#surrogate_key) block, and
  `rocky run` injects a dialect-correct hash column at materialization on
  DuckDB, Databricks, Snowflake, and BigQuery. On a given warehouse the value
  matches what dbt Core's `dbt_utils.generate_surrogate_key` produces over the
  same columns. A key therefore joins across a Rocky / dbt Core boundary either
  way.
- **Named and inline tests.** Define a data-quality assertion once in
  `models/test_definitions.toml` and apply it by name with
  [`[[use_test]]`](/reference/model-format/#use_test), overriding the column,
  severity, or filter at the use site. It is the declarative analogue of dbt
  Core's generic tests. Inline [`[[tests]]`](/reference/model-format/#tests)
  keep working alongside. References resolve when the project loads, so a typo
  fails the load instead of silently dropping a check. See
  [data quality checks](/concepts/data-quality-checks/#reusable-named-tests).
- **Fixture-driven unit tests.** A model's sidecar declares
  [`[[test]]`](/reference/model-format/#test) blocks with mocked upstream inputs
  (`given`) and an expected output (`expect`). `rocky test` seeds a fresh
  in-memory DuckDB, materializes the model against the fixtures, and compares
  the result. No warehouse connection is needed. See
  [testing](/concepts/testing/).
- **Config groups.** A [config group](/reference/model-format/#config-groups) in
  `models/groups/<name>.toml` fans shared routing and strategy out to every
  model that opts in with `group = "<name>"`, so a one-line change reaches the
  whole group. With `enforce = true` the group's fields become binding. A member
  that diverges locally from the group-controlled schema or strategy fails the
  load instead of drifting silently.
- **Model tags.** A model declares a [`[tags]`](/reference/model-format/#tags)
  block of governance attributes (`domain`, `tier`, `owner`). A config group can
  supply a `[tags]` baseline that every member inherits, and the model's own
  tags override it per key. Resolved tags appear on
  `rocky compile --output json` for orchestrators to read.
- **`rocky emit-sql`.** Renders the runnable SQL each transformation model would
  produce, offline and with no warehouse connection, to stdout or one file per
  model. A Rocky project always reduces to plain SQL in dependency order, so
  adopting Rocky is never a one-way door. See
  [No lock-in](/guides/no-lock-in/).
- **The agent policy plane.** A `[policy]` block grades what a principal (a
  person, CI, or an AI agent) may do, by capability and scope: allow, require
  review, or deny. Enforcement runs at `apply`, at `promote`, and on the MCP
  write tools, and every decision, including a denial, goes to the ledger.
  Blast-radius ceilings downgrade an allow to a review. `verify_after` holds an
  apply to its own run's checks. Autonomy budgets tighten on repeated failure
  and recover only as those failures age out of the configured window.
  `rocky policy freeze` is the kill switch, and `rocky policy test` runs pinned
  `[[policy.tests]]` scenarios through the real evaluator so CI catches a
  loosened rule. See
  [Operating Rocky with agents](/concepts/operating-rocky-with-agents/) and
  [Testing policies](/guides/testing-policies/).
- **Self-healing, rung by rung.** A proven-transient failure (a warming
  warehouse, a 429, a lock conflict) is retried within a small bounded budget,
  on by default. Opt-in containment withholds a failed model's downstream
  closure, still materializes the disjoint subgraphs, and reports an honest
  partial failure. Auto-apply is confined to provably additive source drift,
  behind an explicit opt-in plus a policy grant. A backfill is always a
  review-gated plan.
- **Reclamation with proof.** `rocky gc --derivable` inventories artifacts whose
  recorded recipe is bound to their exact bytes. Eviction is review-gated, even
  for a person, and leaves a tombstone. `rocky restore` re-derives the artifact
  and reinstates it only when the rebuilt bytes match the recorded hash exactly.
  Otherwise it refuses. Restore covers recipes that read no recorded upstreams;
  a multi-input recipe cannot be rebuilt yet, so eviction is not reversible for
  every artifact.
- **The governor's surface.** `rocky brief` renders an estate digest with every
  line cited to the ledger. `rocky audit --for <table|run|plan>` walks a custody
  chain end to end. `rocky audit --scorecard` reports acceptance and escalation
  rates per principal or rule, and `rocky review --queue` ranks pending
  escalations. A signal the ledger does not hold renders as *not recorded*,
  never as a fabricated number.
- **An embeddable engine.** `rocky serve` exposes `/api/v1` with outputs
  byte-identical to the CLI's `--output json`, an async job model, and a
  generated OpenAPI 3.1 document. The [Embedding guide](/guides/embedding/)
  covers the four integration patterns. `rocky mcp` is the agent-facing surface
  (31 tools). The [recipe-manifest spec](/reference/rocky-manifest-spec/) and
  the standalone `rocky-verify` binary let a third party check a manifest
  offline, with no engine installed.

## Beta

- **Snowflake, BigQuery, and Trino.** Connection, execution, and the core run
  loop work. Conformance coverage is still growing. We test against live
  warehouses, so corner cases get reported and fixed quickly. If your enterprise
  warehouse is Snowflake or BigQuery and you need it production-grade today,
  [open a discussion](https://github.com/rocky-data/rocky/discussions). We want
  the failure reports.
- **Iceberg.** REST-catalog source discovery works. Content-addressed writes
  round-trip as Iceberg through Delta UniForm, end to end.

## On the 2026 roadmap

- **Iceberg-native writes**, without the Delta intermediate.
- **The wider AI surface.** The compile-validate loop is shipped. The larger
  workflow comes next: rename a column, regenerate the downstream models and
  their assertions, run the tests, ship.
- **A semantic layer.** Rocky's typed IR is the right home for one. Until it
  exists, integrate with Cube, the dbt Semantic Layer, or your existing metric
  store.

## Where this is going

Data platforms are heading somewhere specific. Most pipeline changes will be
authored by agents rather than people. The question that matters shifts from
"can we build it" to "can we trust what was built."

Rocky's long arc is the governed estate. Humans declare the invariants:
contracts, policies, budgets. Agents do the operating: author, run, diagnose,
remediate. The engine checks each mutating step and records what it decided, so
any consumer, human or machine, can read a table's recorded custody before
relying on it. Read that record as best-effort: a failed ledger write warns and
lets the operation continue, and a refusal issued before the rules are
evaluated writes no row at all.

The pieces above are that arc's foundation, shipped in their first form: the
policy plane, the custody ledger, bit-exact replay, and the engine-free manifest
verifier. The [recipe-manifest spec](/reference/rocky-manifest-spec/) is
deliberately an open format. The proof a table carries should outlive any one
tool, including this one.

## Handled by an integration, not built in

- **Extraction from SaaS sources.** Use Fivetran, Airbyte, Stitch, or
  warehouse-native CDC. Rocky discovers what they land and takes over from
  there.
- **Orchestration beyond Dagster.** Dagster has a native integration, and
  `rocky serve` covers small standalone teams. Airflow and Prefect call the
  `rocky` binary like any other CLI; native integrations are not shipped.

## Tell us where it hurts

The roadmap is shaped by where production pipelines are getting hurt. If one of
these gaps blocks your team,
[open a discussion](https://github.com/rocky-data/rocky/discussions).
