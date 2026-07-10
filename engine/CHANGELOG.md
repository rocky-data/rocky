# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.62.0] - 2026-07-10

### Added

- **`rocky policy test` — scenario assertions for agent policy.** A policy file can declare `[[policy.tests]]` scenarios (`(principal, capability, target)` and the effect they must resolve to) that run through the real evaluator; `rocky policy test` exits non-zero if any resolved effect differs from its expectation, so a policy edit cannot silently open a hole in CI. (#1073)
- **Autonomy budgets + `rocky policy freeze`.** A `[policy]` rule may carry `autonomy_budget = { failures = N, window = "7d" }`: verify-after failures within the window burn the budget, and at exhaustion the rule auto-degrades from `allow` to `require_review` (surfaced in `rocky brief`). It only ever tightens, never widens. `rocky policy freeze [--principal] [--scope]` records a freeze decision that forces `deny` for matched actions at the enforcement seam; `rocky policy unfreeze` lifts it. Both are projections over the existing decision ledger — no state-schema change. (#1074)
- **Policy-gated auto-apply of additive source drift (`[resilience] auto_apply_additive_drift`, default-off).** The first self-healing rung that mutates a warehouse schema: when a replication run detects an additive, non-breaking upstream column and the opt-in is set and a `[policy]` rule grants `schema_change.additive` for the table's scope, the engine applies the `ALTER TABLE ADD COLUMN` migration itself, records a custody entry, and runs the `verify_after` gate. It is deliberately narrow and fail-closed: any change that is not provably additive — a drop, retype, narrowing, non-null add, contract-boundary crossing, or absent grant — is refused and left for review, and with the opt-in on but no `[policy]` block every drift is governed to require-review rather than mutating ungoverned. A failed `verify_after` halts (halt-only where no rollback substrate exists). (#1075)

### Changed

- State schema `v17 → v18`: a serde-defaulted auto-apply custody field on the policy-decision record. Additive — older state opens forward-compatibly.


## [1.61.0] - 2026-07-09

### Changed

- **Content-addressed column-level skip is now on by default (`[reuse] column_level`).** An unpartitioned content-addressed model whose logic, environment, and every provably-consumed upstream column are unchanged since its last successful build is now **skipped** by default — its SQL does not run and no new commit is written; the prior output stays authoritative. The decision is fail-closed: any unproven input (a non-deterministic model, a changed recipe/env, an un-enumerable consumed set, a missing or moved column hash, or an ambiguous producer-column key) forces a build. This affects **only** the content-addressed materialization path and is inert on the common non-content-addressed run. Set `column_level = false` under `[reuse]` to restore the always-build behavior. The default was flipped after a live S3/UniForm verification of the skip-on-unchanged / build-on-changed decision on a real content-addressed table.

## [1.60.0] - 2026-07-09

### Added

- **`rocky gc --derivable --dry-run` — a reclamation inventory for provably-rebuildable artifacts.** A read-only report that joins the content-addressed artifact ledger with replay-check verdicts, refcounts, and recipe-identity triples to inventory which managed artifacts are *derivable* — recorded with a strong recipe, replayable and deterministic, unreferenced, policy-eligible, and past an age threshold (`--min-age-days`, default 7). Each candidate prints why it is (or isn't) safe to reclaim, and the report headlines the aggregate "X bytes / Y% of managed storage is derivable". This phase has **no deletion path** — every eligibility check fails closed and the command only ever reports. (#1067)

- **`rocky backfill` — a scoped, review-gated recovery plan.** On a failure or gap (a freshness breach, or the withheld window of a contained run via `--from-last-run`; or an explicit `--model`), the engine composes a backfill plan: the affected models plus their downstream lineage closure, ordered dependency-first, scoped to the affected partition window (`--from`/`--to`), with a cost estimate from historical observations. The plan is persisted and emitted as JSON, and is **always review-gated regardless of policy** — a backfill re-runs existing recipes over a scoped window and can hide blast radius, so it always requires a human sign-off (`rocky review <plan> --approve`) before `rocky apply`. It never rewrites SQL. (#1068)

- **Agent-policy v1 conditions — `max_downstreams` blast-radius ceiling and `verify_after` post-apply gate.** A `[[policy.rules]]` rule may now carry `max_downstreams = N`: an `allow` only stands when the target model's *transitive* downstream blast radius is ≤ N; an oversized **or uncomputable** blast radius degrades the allow to `require_review` (fail closed), and the ceiling is a **sticky safety cap** — it cannot be bypassed by a more-specific sibling `allow` that omits it. A rule may also carry `verify_after = ["check", …]`: after an apply mutates, the named checks must have passed in that apply's own run or the apply halts; a check that failed **or did not run** fails the gate. Where no rollback substrate exists the failure is honest halt-only (the mutation stands and must be reverted manually — never a fabricated rollback). State schema bumps to v17 (additive per-check outcomes on the run record + the decision record's `verify_after`; pre-v17 records read back empty). (#1069)

### Fixed

- **Column-level sound-skip now fails closed on an ambiguous case-colliding model key.** The consumer-baseline builder resolves each consumed upstream through a lowercased lookup map keyed by both the bare model name and the 3-part `catalog.schema.table`. The map was built last-writer-wins, so two distinct models whose name or target folds to the same key case-insensitively (for example `Orders` and `orders`) silently aliased — a consumer reading that key could compare the *wrong* producer's column hashes and skip on a genuine content change. An ambiguous key is now poisoned (removed) rather than resolved, so the read yields no column baseline and the gate builds. The per-column skip gate remains default-off (`[reuse] column_level`). (#1066)

## [1.59.0] - 2026-07-09

### Added

- **Recipe-identity attestation travels in Databricks table metadata, verifiable offline.** On a Databricks (Delta) materialization Rocky writes the recipe-identity triple it already records — `program_hash`, `env_hash`, `hash_scheme`, plus the input-match keys when the run observed its inputs — into the table's `TBLPROPERTIES` under a vendor-neutral `recipe_manifest.*` namespace. The write is a post-create `ALTER TABLE … SET TBLPROPERTIES`, deliberately separate from the CREATE DDL so it never enters the IR the recipe hash is computed over (hash-neutral by construction). A generic `SHOW TBLPROPERTIES` read-back reconstitutes a standalone `rocky-manifest v0.1` document that the engine-free `rocky-verify` validates offline. DuckDB, Snowflake, and BigQuery inherit a silent no-op via the new `write_recipe_manifest` governance hook; only Databricks implements the carrier. On a managed (non-content-addressed) run the manifest carries the identity triple but no output byte-hashes, so offline verification is structural, not byte-identity. (#1062)

- **`rocky export-openapi` — an OpenAPI 3.1 document for the `rocky serve` HTTP API.** Assembles a single OpenAPI 3.1 document from the same typed JSON-schema registry that backs the CLI plus the `/api/v1` route table: shared components are deduplicated, Draft-07 schemas are bridged to the 2020-12 dialect, `$ref`s are rewritten, and the document is validated against the OpenAPI 3.1 meta-schema before it is written. The artifact is published at `docs/public/openapi.json` and drift-gated in CI alongside the other codegen. Paired with a new **Embedding Rocky** guide covering the four integration patterns (subprocess, SDK, MCP, serve API), the async job model, `/meta` feature detection, the schema-stability promise, and the single-tenant sidecar posture. (#1064)

### Fixed

- **Column-level sound-skip now fails closed on a case-folding producer-column collision.** The consumer-baseline builder folded producer output-column names to lowercase, so two columns differing only by case (for example `id` and `ID`) collapsed to one key and a consumed column could resolve to whichever entry won the insert. If the consumed column changed but its case-colliding sibling did not, the gate compared the unchanged sibling and could skip a genuinely-changed column. The baseline now fails closed on the collision — no column baseline for that upstream, so the model builds — mirroring the guard already present in the per-column hash comparison. The per-column skip gate remains default-off (`[reuse] column_level`). (#1063)

## [1.58.0] - 2026-07-08

### Added

- **`rocky audit --scorecard --by principal|rule|scope [--window]` — a trust scorecard.** Proposal acceptance / denial / escalation rates, computed per group over the persisted policy-decision ledger and windowed by `--window`. Metrics the ledger does not persist (verify-after outcomes, escalation-resolution latency) are rendered as *not recorded* with the reason rather than inferred into a number; a ledger read failure renders the whole scorecard unavailable rather than a smoothed-over zero. Reads only — it informs human judgment and is wired to no automatic policy change. (#1057)

- **Custody drill-down + review queue.** `rocky audit --for <table|run|plan>` renders the custody chain for a subject as one query — principal → decision → plan classification → verify-after → column-level blast radius — each link fail-closed (a signal the ledger does not persist reads as *not recorded*, never fabricated). `rocky review --queue` lists the pending `require_review` escalations ranked by blast-radius × change-class × staleness, each with the exact approve command. Both read the ledger; no schema bump. (#1055)

- **Model-failure containment (`[resilience] contain_failures`, default off).** When enabled, a model whose build fails no longer aborts the whole run: the failed model and its downstream closure are *withheld*, disjoint subgraphs still materialize, and the run reports a partial failure with a containment manifest — the withheld models on `contained[]` (each with `blocked_by` and an unblock hint) and the causes on `errors[]`. The closure is conservative and sourced from the resolved dependency graph (explicit `depends_on` merged with SQL `ref()` and physical-target reads), so a withheld model is never built on a failed upstream's stale output; on any doubt the closure contains more, never less. Default-off keeps the historical fail-fast path byte-identical. (#1058)

- **`draft_model` — policy-aware agent authoring over MCP.** `draft_model(name, sql, intent)` writes a model plus its intent sidecar into the config's model directory (path-gated — traversal and symlink escape are refused), compiles immediately and returns diagnostics inline, and points the agent at the compile → plan → propose → review flow. Policy-aware through the shared evaluator: a denied draft returns a structured error and leaves no file on disk; a `require_review` draft persists for a human. No delete or warehouse tool. (#1054)

- **MCP write-path tools + generator rename.** The `rocky mcp` surface now separates *generating* content from *writing* it. Two new policy-aware write tools join `draft_model`: `draft_contract(model, spec)` writes an agent-authored `.contract.toml` next to the model, and `draft_check(model, spec)` merges declarative `[[tests]]` checks into the model's sidecar — each writes into `models/`, **compiles in the same call** (a contract is validated against the model's inferred schema; a malformed check fails structurally), and is gated by the agent-policy plane exactly like `draft_model` (a denied draft leaves no file / restores the prior sidecar; a `require_review` draft persists for a human). The two LLM generators are renamed to the `ai_*` family to match the `rocky ai-*` CLI verbs: `draft_contract` → **`ai_contract`**, `generate_tests` → **`ai_test`**. A call to `draft_contract`/`draft_check` that omits `spec` (i.e. code reaching for the old generator) returns a structured `invalid_argument` error whose hint points at `ai_contract`/`ai_test`, so the migration is a single actionable hop. New agent-conformance eval scenarios author a contract/check via the write path and reroute a policy denial; the first per-release scorecard ships under `engine/evals/`.

- **Classified retry at the run loop (`[resilience]`).** A model whose materialization fails is now classified — via each adapter's own retryable judgement, hoisted into a unified `FailureClass { Transient(kind), Permanent, Unknown }` — and a *proven* transient failure is re-run within a small bounded budget with capped exponential backoff. Every retry is recorded as an attempt trail on the execution record and surfaced in the run's JSON output. **Behavior change:** this is on by default, so a transient failure that fails a run today (a warehouse warming up, a 429, a connection reset, a lock conflict) will now be retried instead. The default budget is deliberately small (`transient_max_retries = 2`); set `transient_max_retries = 0` or `enabled = false` under `[resilience]` to restore the prior single-attempt behavior (e.g. in CI where a fast-fail is preferred). **Permanent** failures (a rejected statement, a type/contract error) and **Unknown** failures (an error the adapter doesn't recognise) are never retried — fail closed. Retry is allow-by-default under the agent-policy plane; only an explicit `capability = "retry"` rule gates it. State schema bumps to v16 (additive `attempts` field on the execution record; pre-v16 records read back with an empty trail).

### Fixed

- **`rocky audit --scorecard --window` no longer panics on an out-of-range value.** An `i64`-parseable but out-of-range magnitude (e.g. `100000000d`) previously overflowed the window arithmetic and crashed the process (exit 101); it now returns a clean usage error (exit 1). (#1057)

## [1.57.0] - 2026-07-08

A large feature release: an enforcing **agent policy plane**, **recipe identity** on every execution, **replay** re-execution, sound **per-column content hashing**, an embedder-grade **engine API** with a job model, the **governor's brief**, and a standalone **recipe-manifest verifier**. All additive — absent the new opt-in config (`[policy]`, `[reuse] column_level`), behavior is unchanged.

### Added

- **Agent policy plane — "IAM for data agents."** A `[policy]` block declares `(principal, capability, scope) → effect` rules; the evaluator resolves them per-model with a deny-override, most-specific-wins (most-restrictive on incomparable), fail-closed classification. `rocky policy check` explains a verdict; enforcement wires into `apply`/`promote` (generalizing the AI-authored review gate) and the MCP `propose` tool, which returns a structured `{code, message, remediation_hint}` denial naming the rule. Every decision is recorded to a `POLICY_DECISIONS` ledger, queryable with `rocky audit`. A denied propose writes no plan; a legacy AI-authored plan still requires review. Absent `[policy]`, behavior is byte-identical to before. (#1043, #1048, #1050)
- **Recipe identity on every execution.** Each materialization records the `(recipe_hash, input_hash, env_hash)` triple plus a versioned `hash_scheme`, with an honest `input_proof_class` (`strong` vs `heuristic`) so a freshness-derived input hash is never presented as a content claim. Surfaced on `history`/`trace`/`catalog` records and via `rocky history --recipe <hash>` — every execution of that exact program. (#1033, #1039)
- **Replay re-execution.** `rocky replay --at <run> --check` classifies each recorded model as replayable or not from the ledger alone; `--execute --verify` re-runs a model (or, with no `--model`, the whole run in topological order, each downstream reading its *replayed* upstream) on an ephemeral engine and blake3-compares to the recording. Fail-safe verdicts; nothing materialized to production; nondeterministic models are flagged, not chased. (#1035, #1038, #1041)
- **Per-column content hashing (T2).** A consumed-column completeness guard (fail-closed on `SELECT *`/CTEs/ambiguity), producer-side output-column hashes and a consumer-side baseline recorded on the content-addressed path, and a **sound content-addressed column-level skip gate** — a rebuilt upstream is skipped for a downstream only when the columns it consumes are byte-identical to its baseline. Default-OFF behind `[reuse] column_level`; every uncertainty fails closed to BUILD. (#1034, #1037, #1042, #1046)
- **Engine API (`rocky serve --api`).** The `/api/v1` routes now serve canonical `*Output` bytes identical to the CLI's `--output json` (a golden parity test enforces it), with a `{code, message, remediation_hint}` error envelope, a computed `/meta` (engine + schema versions, capabilities), and a **job model** — `POST /api/v1/jobs/{run|plan|apply}` → `202`, `GET /jobs/{id}` with the canonical output embedded, one-mutating-job-at-a-time (`409 mutation_in_progress`), and persisted job status so a restarted sidecar reports honestly. (#1044, #1047)
- **`rocky brief` — the governor's estate digest.** A typed digest composed template-first from the ledger (agent activity, escalations, runs, drift, freshness, quality, cost/budget), every line carrying ledger citations, fail-closed sections, markdown or JSON. (#1052)
- **Recipe-manifest v0.1 spec + `rocky-verify`.** A vendor-neutral manifest schema (every field mapped to shipped recording code) and a dependency-light standalone verifier that validates a manifest and byte-checks recorded hashes offline — no engine required. (#1051)
- **Agent interface hardening.** A structured `{code, message, remediation_hint, policy_rule?}` error contract across the MCP tools, and a versioned agent-conformance eval suite (`engine/evals/`) scoring an agent's grounding/authoring over the MCP surface. (#1036, #1032)
- **Replication `prune_unchanged`.** Skip-unchanged source pruning on the replication path. (#1024)

### Fixed

- **`rocky plan` honors `merge` + `table_overrides` on replication.** The plan preview ignored per-table strategy and column overrides on the replication path. (#1023)

## [1.56.0] - 2026-07-02

### Added

- **A DSL `derive` alias can be referenced by a later pipeline step.** A `derive`d name used in a `group` aggregation, a `where` / `having`, a `select`, or a subsequent `derive` inlines to its expression, so `derive { total: amount * quantity }` followed by `group customer_id { revenue: sum(total) }` lowers to `SUM(amount * quantity)` and executes. A terminal `derive` preserves the source columns and appends the computed ones (`SELECT *, <expr> AS <name>`), matching the documented "adds computed columns without removing existing ones" contract. (#1009)

### Changed

- **Transformation `merge` with no explicit `update_columns` now resolves the column set on every adapter.** A `merge` model that omitted `update_columns` (meaning "all columns") emitted invalid SQL on BigQuery (`UPDATE SET target = source`) and errored on Snowflake/DuckDB; only Databricks worked. The transformation path now resolves the implicit set from the model's typed output columns, and BigQuery fails fast with a clear message if none can be resolved. (#1007)
- **A replication `rocky run --all` that fails at model-execution time now reports a partial failure.** A runtime model failure on the replication path exited 1 with empty stdout under `--output json`; it now emits the `RunOutput` JSON (copied tables plus the error) and exits 2, matching the model-only and transformation paths. (#1007)

### Fixed

- **DSL `derive` no longer drops the source columns.** A terminal `derive` previously projected only the computed columns, silently discarding every source column, and a `derive` alias referenced by a later `select` produced SQL referencing a non-existent column. Both now follow the documented contract. (#1007, #1009)
- **The cross-team-contract gate now flags breaking changes to columns read outside a consumer's projection.** `rocky compile` / `rocky ci` built the "columns the consumer reads" set from the top-level projection only (and only a function's first argument), so a producer change to a column read via a `WHERE` filter, a later function argument, arithmetic, or `CASE` passed the E030/E031/E032 gate silently. The read-set is now derived from a complete walk of the consumer's SQL. (#1007)
- **A configured `[[checks.custom]]` now fails when it should.** The custom-check evaluator inverted its comparison, so a violation-counting check reported as passing — and with the default `threshold = 0` it could never fail. It now passes only when the returned count is within the threshold. (#1007)
- **A global `[budget]` is now enforced on transformation runs.** The transformation path never evaluated the configured budget, making a cost/duration/scan cap a silent no-op there; it now runs the check and fails the run on an `on_breach = "error"` breach. (#1007)
- **`rocky import-dbt` validates manifest-derived model names and escapes generated sidecars.** A dbt `manifest.json` node name flowed unvalidated into output file paths and into the generated sidecar TOML; imports now reject unsafe file-name components (no path separators, `..`, or absolute paths) and escape every manifest-derived value. (#1007)
- **`rocky import-dbt` no longer corrupts model SQL containing `$`.** On the no-manifest path a `$` in an `is_incremental()` else block (a dollar-quoted literal, a positional parameter, a literal like `$100`) was interpreted as a regex capture-group reference and dropped; it is now copied verbatim. (#1007)
- **`rocky emit-sql` fails on a compile error and on an unknown `--model`.** It previously emitted (and with `--out-dir`, wrote) the missing-`@var` sentinel `NULL` when a required variable had no value, and silently succeeded when `--model` matched nothing. (#1007)
- **`@var(...)` inside a backslash-escaped string literal is now substituted.** The comment scanner treated `\'` as closing a string, so a `@var` after `--` inside such a literal (honored on Databricks/Snowflake/BigQuery) was misclassified as a comment and skipped. (#1007)
- **Webhook payloads escape data-derived values.** Templated webhook bodies (including the built-in Slack/PagerDuty/Datadog/Teams presets) substituted values such as error text and table names raw, so a value with a quote or newline could break or inject into the JSON payload; substituted values are now JSON-escaped. (#1007)
- **Content-addressed writes refuse an unsupported partitioned + row-tracking table** instead of silently corrupting Delta row-tracking metadata. (#1007)
- **A config load no longer panics on a long multibyte environment-variable value.** The substitution hint truncated on a fixed byte offset, panicking when it fell inside a multibyte character; it now truncates on a character boundary. (#1007)

## [1.55.2] - 2026-06-28

### Fixed

- **`rocky compile` now rejects managed-Iceberg `format_options` that Databricks rejects at the warehouse — `partition_by` + `cluster_by` set together, and engine-managed TBLPROPERTIES (e.g. `write.format.default`) — with a clear diagnostic instead of a runtime warehouse rejection.** The check fires at compile time (diagnostic E035, naming the offending option) before any warehouse call, and the lakehouse DDL generator carries the same guard so the run path fails with a clear Rocky error if compile is bypassed. (FR-044)
- **`rocky import-dbt` now ports dbt unit tests with null fixture cells instead of dropping them as UnserializableUnitTest** — TOML has no null, so null cells are omitted on emit and the run-side fixture builder unions the column set across rows, materializing absent cells as SQL NULL. (FR-045)
- **`rocky run --var` is now rejected with `--dag` / `--watch` instead of being silently dropped.** Those two dispatch paths compiled their sub-runs with an empty run-variable set, so a supplied `--var` was silently ignored — a model with inline `@var(name, default)` placeholders resolved to the default and the run still reported success (wrong data), and a required variable failed even though a value was supplied. The combination now errors clearly until `--var` is threaded through those paths.
- **`rocky import-dbt` now emits bare model references instead of dbt's compiled qualified relation names, so imported repos compile and unit-test** — a `SELECT * FROM {{ ref() }}` microbatch/incremental body no longer fails `rocky compile` with E020, and imported unit tests bind their fixtures instead of erroring with a missing catalog. Genuine `source()` references stay qualified. (FR-046)

## [1.55.1] - 2026-06-27

### Changed

- **Upgraded the `object_store` dependency (0.11 → 0.14).** The S3 conditional-put mode is now pinned to `ETagMatch` (`If-None-Match: *`), making the concurrent-writer `put_if_not_exists` guarantee explicit and version-independent on real S3. (#966)

### Fixed

- **`rocky run --output json` (including `--dag`) no longer prints a human summary line to stdout before the JSON payload.** Under the unified-DAG path each sub-run (replication load, transformation, seed) printed lines like "Copied N tables …", "transformation pipeline complete …", or "Seed complete …" to stdout ahead of the JSON document, forcing an orchestrator to slice from the first `{`. With `-o json`, stdout is now exactly the JSON document; that progress text goes to stderr. Human/table output is unchanged.

## [1.55.0] - 2026-06-27

### Added

- **`rocky run --var name=value` — per-run variables in model SQL.** A model can reference an explicit `@var(name)` (or `@var(name, default)`) placeholder that the compiler resolves to a caller-supplied string at compile time, parallel to dbt's `{{ var() }}`. The substitution is textual — the operator owns SQL quoting — and a referenced variable with no value and no inline default fails the run with a diagnostic naming it. (#976)
- **`[governance.tags]` propagation on transformation runs, with view-aware Unity Catalog tag DDL.** Model and pipeline governance tags are applied to transformation targets, emitting the correct tag DDL for a view versus a table. (#971)
- **Lakehouse-format initial DDL for incremental strategies.** The first materialization of an incremental model emits the configured lakehouse-format table DDL instead of an untyped create. (#970)
- **Seed pre/post-hooks.** `rocky seed` fires the same `pre_hook` / `post_hook` lifecycle as model materialization. (#969)
- **`rocky import-dbt` fidelity: `dbt_expectations` / `dbt_utils` test mapping, `profiles.yml` anchors + `env_var(...)`, and dropped-contract warnings.** Common `dbt_expectations` / `dbt_utils` generic tests map to native Rocky tests where there is a clear equivalent (carrying severity and row filter); `profiles.yml` YAML anchors / merge keys and `{{ env_var(...) }}` adapter types resolve instead of silently falling back to DuckDB; and a dbt model `contract: { enforced: true }` (with its column `data_type`s and `constraints`) no longer drops silently on import — the importer emits a per-model warning and a `contracts_dropped` count flagging the lost enforcement. (#972)
- **`rocky import-dbt --microbatch-as=time_interval`** translates a dbt microbatch model to a Rocky `time_interval` strategy (granularity and lookback derived from the dbt config); the default `merge` keeps the idempotent-merge mapping. (#974)
- **`rocky import-dbt --skip-unit-tests`** skips translating dbt `unit_tests`, counting them as dropped constructs. (#968)

### Changed

- **`rocky run` now fails when a model does not compile.** A model that fails to type-check was previously logged as a WARN, skipped, and the run still reported `status: "success"` / exit 0 with the model un-materialized. A compile error is now a first-class run failure: the run reports `status` `failure` or `partial_failure`, exits non-zero (1 or 2), counts the model in `tables_failed`, and surfaces the diagnostic in `--output json` `errors` with `failure_kind: "compile-error"`. This also reclassifies a transformation run where some models materialized and one failed at runtime from `failure` / exit 1 to `partial_failure` / exit 2. (#975)
- **`rocky run --parallel` now defaults to 4** (was serial / `1`). It stays overridable: `--parallel 1` forces serial, and adapters that do not support concurrent execution (e.g. DuckDB) stay serial regardless. Concurrent-capable warehouses (Databricks, Snowflake) now run up to 4 models or partitions per layer concurrently by default, relying on the adapters' adaptive throttling. (#977)

### Fixed

- **`rocky import-dbt` no longer aborts on an unserializable dbt unit test.** A unit test whose payload can't be serialized is counted as a dropped construct and reported, rather than failing the whole import. (#968)
- **`rocky compile` expands `SELECT *` over a derived table** during output-schema inference, so a model selecting `*` from a subquery or CTE infers the correct columns. (#973)

## [1.54.0] - 2026-06-23

### Added

- **Replication pipelines now execute `[[checks.custom]]` and `null_rate` checks.** Custom checks previously ran only on quality pipelines and `null_rate` ran nowhere, so a replication pipeline can now enforce inline custom-SQL and per-column null-rate guards without standing up a separate quality pipeline. Null-rate results are named per column (`null_rate:<column>`). (#955)
- **`rocky discover` projects the resolved check names a run will emit** under `checks.configured_checks`, so an orchestrator can pre-declare matching checks without re-reading `rocky.toml`. The projected names byte-match what the runner emits. (#955)

### Changed

- **`rocky validate` warns on data-quality config that won't run.** `V034` flags a configured check a pipeline type does not execute (for example `null_rate` on a quality pipeline); `V035` flags a replication `strategy` — at the pipeline level or in a `[[table_overrides]]` entry — that isn't recognized and would silently fall back to `full_refresh`. (#955, #956)

### Fixed

- **Null-rate checks no longer report a false pass on an unparseable count.** A sampled/null count that can't be parsed is skipped with a warning rather than treated as zero; the count parser also accepts integer aggregates returned as JSON floats. (#956)

## [1.53.0] - 2026-06-22

### Fixed

- **`rocky import-dbt` no longer silently mis-converts — it fixes or loudly flags each gap.** A migration tool that quietly emits wrong SQL is worse than one that errors, so this closes the silent failures: dbt **`alias`** now drives the sidecar `[target].table` (it was hardcoded to the node name, silently routing data to the wrong table); **`{{ this }}`** resolves to the model's real FQN (was a bogus `__this__`); **`{% for %}`/`{% set %}`** on the no-manifest path are **refused** rather than half-rendered into broken SQL (a `{% if %}` still emits with a TODO marker); dbt **`merge_update_columns`** carries into the `merge` strategy (was update-all); dbt **microbatch** maps to an idempotent `merge(unique_key)` instead of an append-only insert that re-inserts the lookback window every run; configured **`not_null`/`unique`** tests convert and carry `severity:`/`where:` (were dropped); dbt **tags** carry onto the model `[tags]` block; and the resources the importer skips (**snapshots, metrics, semantic models, exposures**) are now detected and counted (`constructs_dropped` + per-resource warnings) instead of vanishing. When a manifest carries no compiled SQL, the importer warns loudly to run `dbt compile` first rather than regex-rendering every model. (#944)

### Added

- **`rocky import-dbt` converts model-level `dbt_utils.unique_combination_of_columns`** to a Rocky `composite` uniqueness test (it was dropped as unsupported). The columns come from the test's own config, so no model schema is needed; the converted count surfaces in `tests_converted_custom` (previously always 0). A microbatch model without a `unique_key` now imports as an honest append-only `incremental` strategy rather than a `microbatch` label that would falsely imply Rocky enforces dbt's batch idempotency. (#945)

- **`rocky cost --by tenant` (and `--by model`) — grouped cost rollups.** `rocky cost <run|latest>` can now roll its per-model cost attribution up by a dimension: `--by tenant` groups executions by the discover-time schema-pattern `{tenant}` component (replication models with no recorded tenant collect in an `<unattributed>` bucket), `--by model` groups by model name. The grouped rollup is emitted as an additive `groups` array on the JSON output; `per_model` is always present, so a plain `rocky cost` is byte-for-byte unchanged. The tenant dimension is persisted on each `ModelExecution` as an additive, serde-defaulted field — no state-schema version bump, and pre-existing run records read back as unattributed.
- **Cross-team contract checks now flag type and nullability changes, not just dropped columns.** When a consumer project imports a producer snapshot with a `baseline`, `rocky compile` re-emits the producer's column-level changes as consumer-side diagnostics, filtered to the columns the consumer actually reads: **E031** (a referenced column's type was narrowed), **E032** (a referenced column went nullable → NOT NULL), **W031** (a referenced column's type was widened), and **W030** (a column was added, surfaced only to `SELECT *` consumers whose positional projection shifts). E030 (referenced column dropped) already shipped. (#942)
- **`rocky imports update` — advance vendored cross-team-contract baselines.** A consumer accepts a producer's reviewed change with one command: `rocky imports update` advances each `[imports.<name>]` baseline to the current snapshot (the explicit accept that clears the pending E030–E032 diagnostics), and `--check` is a read-only CI guard that fails when a baseline is behind or a pin is stale. Published snapshots now carry a `snapshot_version` header, so a consumer on an older build fails closed (**E034**) against a newer snapshot format instead of silently mis-reading it; pre-versioning snapshots still load unchanged (their recipe hash is identical, so existing pins keep matching). (#943)

### Changed

- **`rocky publish-ir` refuses to publish a contract that can never be enforced.** A snapshot whose models all have empty `typed_columns` (typically a self-contained DuckDB producer published without `--with-seed`) would look enforced to a consumer but detect no breaking change. Publishing an entirely degenerate snapshot now fails with guidance; a partially degenerate one warns and names the unenforceable targets. (#942)
- Added a live BigQuery MERGE-materialization regression test. The `Merge` strategy dispatches unconditionally to `BigQueryDialect::merge_into` (no per-dialect maintenance gate), so it is a BigQuery-reachable `rocky run` path; the new `#[ignore]`-gated test seeds a target, runs the emitted `MERGE … WHEN MATCHED THEN UPDATE … WHEN NOT MATCHED THEN INSERT ROW` against the sandbox, and asserts matched rows are updated and unmatched rows inserted. Mirrors the runner's `ColumnSelection::All` → explicit per-column resolution so it exercises the SQL replication actually emits.
- **The installers now also install the standalone `rocky-lsp` binary** next to `rocky`, and the Windows installer replaces an in-use `rocky.exe` (rename-then-replace) instead of failing the overwrite. The VS Code extension prefers `rocky-lsp` for the language server, so updating `rocky` no longer fails because the editor is running it. (#931)
- Refreshed locked workspace dependencies to their latest semver-compatible versions (`duckdb` 1.10504, plus transitive bumps). No `Cargo.toml` major bumps — the deliberate compat pins (duckdb↔arrow, the rustls dual-provider balance, the jsonwebtoken/tonic provider) are unchanged. Full suite green. (#939)

## [1.52.0] — 2026-06-19

### Added

- **Fixture-driven unit tests in `rocky test`.** A model sidecar can declare `[[test]]` blocks with mocked upstream inputs (`given`) and an `expect` table; `rocky test` seeds a fresh in-memory DuckDB, materializes the model against the fixtures, and compares the output (multiset comparison plus a row count). (#899)
- **Named test definitions.** Define a data-quality assertion once in `models/test_definitions.toml` and apply it across models by name via `[[use_test]]`, overriding the column, severity, or row filter at the use site — the declarative analogue of dbt's generic tests. Inline `[[tests]]` keep working alongside. (#919)
- **Config groups.** A `models/groups/<name>.toml` definition fans shared routing (`schema_template`, filled per model from its `[args]`) and strategy out to every model that opts in with `group = "<name>"`, so a one-line change flows to the whole group. (#917)
- **Enforced config groups.** `enforce = true` makes a group's fields binding rather than defaults: a member that locally pins the group-controlled target schema or strategy fails the load instead of silently diverging from the group. (#918)
- **Model-level `[tags]`, inheritable from config groups.** A model can declare a `[tags]` block of free-form governance attributes (`domain`, `tier`, `owner`, …), and a config group can declare `[tags]` that every member inherits as a shared baseline — a model's own tags override the group per key (sidecar > group). Resolved tags are surfaced on `rocky compile --output json` as `models_detail[].tags`, where orchestrators can read them. Merged at the shared `.sql` / `.rocky` resolution path so both model formats inherit. (#920)
- **Config-group misplacement guard.** A group member that pins its own `target.schema` **and** supplies `[args]` now fails the load: the pin bypasses the group's `schema_template`, so the args could only fill a template that's never used — a contradiction that usually masks a routing mistake. Pinning a schema with no args (a legitimate override) and routing via args with no pin both stay valid. (#923)
- **Deterministic surrogate keys.** A model sidecar can declare `[[surrogate_key]]` blocks (an output column name plus its input columns); `rocky run` injects each as a dialect-correct, dbt-value-identical MD5 hash column at materialization (DuckDB, Databricks, Snowflake, BigQuery), validates the spec at load, and treats a newly added key as a change so the model re-materializes rather than being skipped as unchanged. (#911, #913, #914)
- **Per-column documentation.** Column descriptions declared in a model sidecar `[columns.<name>]` table now surface in `rocky docs` (the HTML catalog) and `rocky catalog --output json`. (#900)
- **`rocky emit-sql` — a tested, dependency-ordered exit path.** Renders the runnable SQL each transformation model would produce, offline with no warehouse connection, either to stdout or as one `<model>.sql` file per model under `--out-dir`. A project always reduces to plain SQL, so adopting Rocky is never a one-way door. (#916)

### Changed

- **A config group's `schema_template` is validated as a SQL identifier once resolved against a model's `[args]`.** A bad `[args]` value (e.g. one carrying a hyphen) now fails at load with a clear message instead of surfacing later as broken SQL — the same `validate_identifier` the table-reference builder applies, so load and run agree. (#924)
- **Typos in a `[[surrogate_key]]` or `[[use_test]]` sidecar block now fail the load** (`deny_unknown_fields`) instead of being silently ignored. (#924)

### Fixed

- **`emit-sql --out-dir` skips a model whose name is not a safe filename** (reporting it) rather than joining the name into the output path. (#924)

## [1.51.1] — 2026-06-14

### Security

- **The BigQuery and DuckDB loaders validate SQL identifiers before interpolating them.** Table and column names that reach the bulk-load path are now checked against an allowlist rather than being formatted directly into the generated SQL. (#887)
- **`state.valkey_url` is redacted from the plan snapshot.** The Valkey/Redis connection string (which can carry a password) is masked in serialized plan output, so the credential never lands in the snapshot on disk or in logs. (#892)
- **`ci-diff`, `lineage-diff`, and `preview` reject an option-injecting `base_ref`.** A `base_ref` beginning with `-` can no longer be smuggled through to git as an option. (#893)

### Fixed

- **The LSP no longer panics on multi-byte UTF-8.** Truncation and indexing in the language server are now char-boundary-safe, so a document containing non-ASCII text doesn't crash `rocky lsp`. (#891)

### Changed

- **Compile and DAG model lookups are hash-map-backed.** Replacing the previous linear scans removes O(M²) behavior on large projects. (#890)

## [1.51.0] — 2026-06-08

### Added

- **State-schema deploy safety for rolling upgrades.** A rolling engine upgrade that crosses a redb state-schema version no longer strands orchestrated runs (an old-binary pod that read newer state through a shared backend used to hard-fail deterministically, burning an orchestrator's retry budget for ~an hour before failing with no work done). Four parts:
  - **Version-qualified remote state keys.** The tiered / S3 / GCS / Valkey state keys now carry the engine's *schema* version (`rocky:state:v9:…`, `rocky/state/v9/…`), so two engine versions that disagree on the schema never read or write each other's state through a shared backend. Qualified by **schema** version, not binary version — a patch bump that leaves the schema unchanged keeps sharing state, so watermarks are not reset on every upgrade. After a schema-changing bump the new version finds no state under its key and bootstraps once via the existing incremental path (one full refresh). (#865)
  - **`[state] on_schema_mismatch` — `"recreate"` (default) `| "fail"`.** When the engine opens a state store whose schema is *newer* than it supports, `recreate` logs a single WARN, bootstraps fresh local state (one full refresh), and **never writes the downgraded state back** to the shared tier — so the newer state that already-upgraded pods depend on stays intact and an old-binary pod degrades gracefully instead of stranding the run. `"fail"` preserves the prior hard-abort. Backward-compatibility (a newer binary reading older state) still auto-migrates forward as before. (#865)
  - **`rocky doctor --check state_schema`.** Returns `critical` (with both the supported and on-disk versions in the message) when the on-disk schema is newer than the binary supports — a deploy-time boot gate an orchestrator can add to its strict-startup checks so an incompatible pod fails fast and visibly at boot instead of mid-run. (#865)
  - **`rocky state show --output json` schema versions.** Adds `schema_version_supported` and `schema_version_on_disk` so external tooling can make a compatibility decision structurally instead of parsing the human error string. (#865)

## [1.50.1] — 2026-06-06

### Changed

- **Auditable reuse is now live-verified end-to-end (4/4) on Databricks-Iceberg.** The fail-closed *fall-back-to-BUILD when a prior run's file is no longer live* safety path — previously the one un-runnable leg of the reuse live suite — is now exercised by a self-contained live test. It tombstones the prior run's file with a `DELETE` of that run's rows (the append-only UniForm writer never lets `VACUUM RETAIN 0 HOURS` remove the live current version, and the test refuses to mutate shared warehouse config), so `add_path_is_live` returns false and the run correctly builds instead of pointing at a removed file. Reuse stays **experimental and default-off**; with `[reuse]` unset, `rocky run` is byte- and cost-identical. (#860)
- Corrected stale "Stage-1 / dormant / ONLY-BUILD" doc-comments across the reuse path (the `[reuse]` config, the reuse decision, and the content-addressed runner) that still described an earlier slice where a positive reuse verdict was only *logged*. With `[reuse]` enabled, an eligible input-match against a prior **strong** run now performs a real zero-copy point-to and skips the model's SQL. The `[reuse]` descriptions in `rocky-project.schema.json` and the regenerated Dagster / VS Code bindings were updated to match. (#860)

## [1.50.0] — 2026-06-06

### Added

- **Decision-support output for `plan` and `run` — reporting-only, opt-in, default-inert.** New surfaces explain *why* the engine made each build decision and what a schema change touches downstream. They are purely informational and **never change build behavior**; a default `rocky run` / `rocky plan` is byte-identical to before.
  - **`rocky run` per-model decision + reason.** `RunOutput.model_decisions` reports, for each transformation model, the **build / skip / reused** verdict the engine reached plus a short human-readable reason (e.g. "logic and upstream data unchanged since last build", "reused prior run's bytes (strong proof)"). Populated only when the `--skip-unchanged` gate is active or `[reuse]` is enabled; omitted entirely on a default run. Lets orchestrators (Dagster) and the VS Code extension explain skip/reuse decisions. (#853)
  - **`rocky plan --semantic` column-level impact + breaking-change verdict.** When `--semantic` is passed and a baseline ref is available, `plan` attaches a semantic verdict describing the changes vs. the baseline at column granularity — added/removed models, added/dropped columns, column type changes (with a `narrowing` flag), nullability changes, reordering, and materialization-strategy changes — i.e. a downstream-impact preview (#856) and a breaking-change verdict (#857). Omitted on a default `rocky plan` or when no usable baseline exists.

## [1.49.0] — 2026-06-05

### Added

- **`rocky run --defer` / `--defer-to <schema>`.** Build only the models you changed locally and resolve unbuilt upstream `ref()`s against an existing (production) schema — the dbt-Core-style dev-against-prod-upstreams convenience. Inert on a full run; meaningful when a subset is built. (#817, #819)
- **`rocky run --skip-unchanged` — an opt-in, default-off model-skip gate.** Skips re-materializing a transformation model when its logic (a cosmetic-invariant IR hash) **and** every upstream's data (`MAX(ts)` watermark, or `COUNT(*)` with `skip_rowcount_fallback`) both appear unchanged since the last successful build. A best-effort cost optimization, **not** a result-equivalence guarantee: it fails safe to BUILD, and models with CTEs/subqueries/`PIVOT`/set operations or non-deterministic SQL always rebuild. Tunable via `[run]`; `--force-rebuild` bypasses it. (#823, #828, #831)
- **Per-namespace state files — `--state-namespace <key>` / `[state] namespacing`.** Route each pipeline/client to its own `<models>/.rocky-state/<key>.redb` so independent fan-out runs don't serialize on a single redb writer lock. Opt-in and default-off — byte-identical to before when unset. (#829)
- **AI authoring over MCP.** The `rocky mcp` server now exposes the AI generators (`draft_contract`, `generate_tests`, `explain_model`), on-demand dialect lint, authoring prompt-trajectories, and read-only `governance_preview` / `drift_preview`, so a connected agent can draft → typecheck → preview governance/drift → propose behind the human-gate. (#816, #824)
- **`rocky load` gated on a typed data contract**, with native BigQuery load jobs and Databricks staged-file `COPY INTO` (CSV cast to the declared schema), and create-the-target-table-from-the-file-schema when it is missing. (#820, #821, #822, #826, #833, #834)
- **Cross-source collision detection at discover time.** `rocky discover` flags tables that collide across sources (`cross_source_overlap`) and reports first-seen tables via a `new_sources` diff. (#847, #850)
- **`unique_expr` assertion** for derived-key uniqueness, plus row-level assertions at replication time. (#842)
- **Auditable-reuse spine — experimental, default-off.** When `[reuse]` is enabled on the Databricks-Iceberg content-addressed path, a verified input-match performs a **zero-copy point-to** — the model's SQL is skipped and the prior run's bytes are reused — with a fail-closed fallback to BUILD and an offline-verifiable provenance record (logic key, upstream identities, output BLAKE3, proof class). **Not yet verified end-to-end against a live warehouse — leave it off in production until it is.** With `[reuse]` off (the default), `rocky run` is byte- and cost-identical. (#835, #838, #840, #841)
- **Versioned graph-export contract.** `rocky dag --output json` carries a `schema_version` so orchestrators can consume the typed graph across releases. (#818)

### Changed

- Contract type-matching is widening-aware and recognizes BigQuery type names and hyphenated catalog identifiers. (#827, #836, #837)
- Refreshed workspace dependencies to their latest Rust-1.88-compatible versions.

### Fixed

- **`--skip-unchanged` now works on the full-DAG transformation path.** It previously only skipped on the `--model`-scoped path: the full-DAG run executed against a non-canonical state path and never persisted a run record, so the gate had no baseline and silently rebuilt everything. The full-DAG and `--dag` paths now thread the canonical resolved state path and persist the run, so the gate skips correctly. (#849)
- `rocky` writes a `.gitignore` into `.rocky/` so the working directory stays out of users' repositories. (#832)

## [1.48.0] — 2026-06-02

### Added

- **`rocky mcp` grounds AI suggestions in live warehouse data.** The MCP server's data-grounding tools (`inspect_schema`, `sample_rows`, `profile_column`) now query the configured warehouse directly — sampling real rows, profiling a column's distribution and top values, and discovering raw source tables — across DuckDB, Snowflake, BigQuery, and Databricks. A new `suggest_freshness_block` tool drafts a `[freshness]` block from a source's observed load pattern. (#775)
- **Cross-team contracts via imported producer IR snapshots.** A consumer project can import an upstream producer's compiled IR snapshot and type-check its own models against the producer's published output schema, so a breaking change upstream surfaces at compile time downstream. (#774)

### Changed

- **`rocky run` is a first-class single-step alias again** — no longer deprecated. `run` is the fused plan+apply path for everyday use; `plan` + `apply` remain the canonical auditable two-step flow. (#763)
- **Non-blocking DuckDB execution and opt-in intra-layer transformation concurrency.** DuckDB queries now run on a blocking-safe thread, and independent models within a layer can be transformed in parallel with the new `--parallel` flag. (#790)
- **State-store writes are batched.** Per-run progress writes dropped from O(N²) to O(N), and the schema cache now fsyncs once per run instead of once per model. (#787)
- Bumped workspace dependencies to their latest minor versions. (#776)

### Fixed

- **Live data grounding emits correct SQL for BigQuery and Databricks.** BigQuery table references are backtick-quoted with hyphen-allowing project-id validation, and Databricks profile casts use Spark's `STRING` type instead of `VARCHAR` (which Spark rejects with `DATATYPE_MISSING_SIZE`). (#778)
- **Dialect adapter-gating and identifier quoting corrected across adapters**, verified against live warehouses. (#789)
- **BigQuery async job failures now surface instead of hanging**, with automatic retries on HTTP 429/503. (#796)
- **`rocky doctor` and logging no longer warn on a healthy default setup** — a clean local project reports clean.
- **The playground DuckDB is auto-seeded** so the first-run quickstart works without a manual seed step.
- CLI UX, diagnostics, and cross-platform fixes, plus dead-code cleanup. (#788)

### Security

- Addressed security and data-integrity findings from an internal audit. (#764)

## [1.47.1] — 2026-05-30

### Fixed

- **Snowflake `insert_overwrite_partition` now works against the live SQL API.** The connector sent `TRANSACTION_ABORT_ON_ERROR` as a `/api/v2/statements` request parameter, which the SQL API rejects (HTTP 400 / code 391917), so the multi-statement `DELETE` + `INSERT` transaction failed before executing. The setting is now enabled by prepending an `ALTER SESSION SET TRANSACTION_ABORT_ON_ERROR = TRUE` statement to the script. (#754)

## [1.47.0] — 2026-05-30

### Added

- **`rocky mcp` data-grounding tools reach raw sources and cold-start projects.** The MCP grounding tools now sample qualified `schema.table` sources (not just materialized models), discover an uncompiled project's schema on cold start via `inspect_schema`, default to a deterministic sample, and `profile_column` reports top values. (#752)

## [1.46.4] — 2026-05-29

### Fixed

- **`.rocky` DSL models are no longer invisible to non-run commands.** `validate`, `list`, `dag`, `optimize`, `compliance`, `preview`, `estimate`, `docs`, and branch scoping loaded models through a `.sql`-only path, so a project with `.rocky` DSL models had them silently dropped — most visibly, `rocky validate` reported a false `DAG error: unknown dependency` when a `.rocky` model sat between two `.sql` models in a transformation DAG. All these commands now load `.sql` and `.rocky` models through one shared loader.
- **Columns with no source no longer vanish from a model's schema.** A named projection with no column-level lineage — an aliased `COUNT(*)`, a literal, a computed expression like `total_revenue / order_count AS avg_order_value` — was dropped from the model's column set, so it never appeared in `rocky profile`, the Inspector Columns tab, or column lineage. These are now kept as source-less columns (present in the schema, with no upstream edge).

## [1.46.3] — 2026-05-28

### Changed

- **`rocky playground` now scaffolds a runnable transformation pipeline.** The quickstart project's models materialize via `rocky run` into the database's default schema (`playground.main`), so `rocky run` → `rocky preview rows` / `rocky profile` (and the VS Code Inspector) work end-to-end on a fresh scaffold. Previously the scaffold was a replication pipeline whose models only ran under `rocky test`.

### Fixed

- **`rocky preview rows` gives accurate guidance on replication pipelines.** When a referenced model isn't materialized, the error no longer unconditionally advises `rocky run` — on a replication pipeline (which copies sources rather than materializing transformation models) it now says so instead of suggesting a command that won't help.

## [1.46.2] — 2026-05-28

### Fixed

- **`rocky profile` reports the observed warehouse type instead of `Unknown`.** The `type` for each column came from the compiler's inferred schema, which is `Unknown` for raw-SQL models whose source schemas aren't resolved at compile time. Profile already queries the table, so it now reads the column types straight from the warehouse (`DESCRIBE`) — the authoritative source for a profile of what's actually there. Falls back to the inferred type only when a column isn't in the describe.

## [1.46.1] — 2026-05-28

### Fixed

- **`rocky profile <model>` no longer fails when the model isn't materialized.** Profile queried the model's declared target table directly, so profiling a model whose target doesn't exist yet — an unmaterialized transformation, or a replication pipeline that never runs its transformation models — failed with a raw catalog error. Profile now probes the declared target and, when it's missing, falls back to the model's first resolvable source table, recording the substitution in `ProfileOutput.profiled_table` and `fell_back_from`. With no source to fall back to it returns a clear "run `rocky run` first" message instead of the catalog error. This powers the VS Code Inspector's Profile tab and inline column profiling. (`rocky ai-contract` keeps refusing an unmaterialized target, since a contract drafted from source data would be misleading.)
- **`rocky test --model <name>` now scopes the run to one model.** The flag was accepted but ignored, so it reported every model. It now filters the reported results to the named model; the model's upstream dependencies still execute so its SQL resolves.

### Added

- **`rocky test` reports per-model results.** `TestOutput` gains `model_results`, one entry per model with a `pass` / `fail` status, so passes are surfaced individually instead of inferred from `total` minus the failure list. The VS Code Inspector's Tests tab uses this to show that a model passes even when it declares no `[[tests]]`.

## [1.46.0] — 2026-05-27

### Added

- **`rocky profile <model>`** — profile a model's columns directly: row count, null count and rate, distinct count, and min/max per column, from a single aggregate query (DuckDB today, no LLM round-trip). `--column <name>` narrows to one column; `--output json` emits `ProfileOutput`. Powers the VS Code Inspector's Profile tab and its inline column profiling.

## [1.45.1] — 2026-05-27

### Fixed

- **TLS no longer aborts the binary at startup** — when both rustls crypto backends are compiled into the dependency graph (`ring` via reqwest's TLS stack and `aws-lc-rs` via the JWT signer), rustls cannot determine a default provider from crate features, and the first HTTPS call (`discover`, `doctor`, any network path) panicked with *"Could not automatically determine the process-level CryptoProvider"* — aborting the process before any request was sent. The `rocky` and `rocky-lsp` binaries now install the aws-lc-rs provider as the process default at startup, before any TLS. A regression test exercises the exact panic site so it can't recur. (Affected 1.43.0–1.45.0.)

## [1.45.0] — 2026-05-26

Read-only MCP tools round out the agent surface, and a watermark bug that silently dropped the first incremental delta is fixed.

### Added

- **`rocky mcp`: read-only `catalog` / `history` / `metrics` / `optimize` tools** — four more tools on the MCP server, all offline (no warehouse connection, no credentials) and built on the engine's existing command cores. `catalog` returns the project-wide asset inventory in one call: every model and source with its typed columns and upstream/downstream model lists (the token-heavy column-edge set is dropped — agents use `lineage` for the column-level trace, `inspect_schema` for typed columns alone, `dependents` for one model's consumers). `history` reads the run ledger — the recent project runs, or a single model's executions (duration, rows, status, `sql_hash`) when `model` is set. `metrics` returns a model's quality snapshots (row count, freshness lag, per-column null rates) plus derived freshness / null-rate alerts. `optimize` returns the cost model's materialization recommendations (current vs recommended strategy, projected monthly savings, reasoning) computed from run history and the on-disk DAG. The tools ground an agent's proposals in the typed graph and operational reality before it reaches `propose`; none of them mutate the warehouse or engine state.

### Changed

- **`rocky-cli`: more typed-output cores extracted for reuse** — `history` / `metrics` / `optimize` now build their JSON output through reusable `history_runs_output` / `model_history_output` / `metrics_output` / `optimize_output` functions (the `run_*` handlers call the core, then print), and `compute_catalog_output` is widened to `pub`, so the MCP server and the CLI share one code path. No output-schema change.

### Fixed

- **Incremental bootstrap no longer drops the first delta** — an incremental pipeline's bootstrap run (the first run, or after a drift drop-and-recreate) recorded the watermark as the run's wall-clock time instead of `MAX(source.<timestamp>)`. Wall-clock is later than every existing row, so the next incremental run filtered them all out and copied zero new rows. The bootstrap now records the source data max, so the next tick picks up the delta.

## [1.44.0] — 2026-05-25

The AI-drivable surface lands end-to-end. `rocky mcp` exposes the engine's read-only tools over the Model Context Protocol so an agent can drive Rocky directly, a `build_model` prompt scripts the authoring loop, and AI-authored plans are gated behind `rocky review` before any `rocky apply`. Also ships `rocky ai-contract`, `rocky preview rows`, and a governance-safe ad-hoc SQL preview.

### Added

- **`rocky mcp`: read-only MCP server** — exposes the engine over the Model Context Protocol so an AI agent can drive Rocky through read-only tools (`compile`, `plan_preview`, `lineage`, `test`, `list`, `inspect_schema`, `sample_rows`, `profile_column`, `propose`, plus `breaking_change` and `dependents` for impact analysis). No tool mutates the warehouse or engine state: `sample_rows` / `profile_column` issue DuckDB-only `SELECT`s and return `{unavailable: true}` on any non-DuckDB adapter, and `propose` writes an `ai_authored` plan to `.rocky/plans/` that still requires `rocky review <plan-id> --approve` + `rocky apply <plan-id>` to materialize. Identifiers are validated through `rocky_sql::validation` before composing refs — no raw-input SQL injection.
- **`build_model` MCP prompt** — a server prompt that scripts the model-authoring loop (inspect → sample → profile → write SQL → compile → plan_preview → propose), stopping at the human review gate and carrying the reconcile discipline (check the data, not just the schema). Bumps the workspace MSRV to 1.88 (the rmcp 1.7 tree requires it).
- **`rocky ai-contract`: AI-drafted data contracts from observed data** — samples a model and infers a starter data contract (column types, nullability, observed ranges) the author can refine, rather than hand-writing one from scratch. Codegen cascade propagated to the dagster + VS Code bindings.
- **`rocky preview rows`: inline model / CTE row preview** — previews the output rows of a model or a named CTE, governance-aware (declared masks applied). Backs the VS Code inline-preview panel.
- **AI-authored-plan review gate (`rocky review`)** — adds a `PlanKind::AiAuthored` discriminator; a bare `rocky apply` refuses to execute an AI-authored plan until a human runs `rocky review <plan-id> --approve`, which records a sign-off marker after reporting the semantic breaking-change delta against a base ref so the approval is informed.
- **Ad-hoc SQL selection preview (governance-safe)** — previews the result of an ad-hoc SQL selection with governance masks applied, without registering a model.

### Changed

- **`rocky-cli`: typed-output cores extracted from command handlers** — JSON-output construction for `compile` / `plan_preview` / `lineage` / `test` / `list*` is factored into reusable `*_output` core functions so the MCP server and the CLI share one code path. No output-schema change.

### Fixed

- **`rocky-cli`: clippy `--all-targets` failures surfaced by clippy 1.95** — the MSRV→1.88 bump moved CI to clippy 1.95, which newly flags `collapsible_if` / `manual_is_multiple_of`. Resolved across the workspace so `cargo clippy --all-targets -- -D warnings` stays green.

- **`rocky-cli`: `branch promote` approvals no longer self-invalidate under the canonical state layout** — `models_fingerprint` (folded into `branch_state_hash` by the v1.43.0 model-byte approval binding) walked the `<config_dir>/models` tree but only skipped `.git` / `target` / `.DS_Store`. The canonical state path is `<models>/.rocky-state.redb` (+ its `.lock`), so the state DB lives *inside* the models tree — its mutable bytes were feeding the content hash. The result: the first state-writing command after `branch approve` (including `branch promote` itself) drifted `branch_state_hash`, so a freshly-signed approval failed its own `state_hash_mismatch` check before it could be honoured. `collect_model_files` now also skips any entry whose name starts with `.rocky-state.redb`, so state churn no longer voids approvals; a genuine model-source edit still drifts the hash (approval soundness preserved). Regression test `models_fingerprint_ignores_state_db_but_tracks_model_edits` pins both halves.

## [1.43.0] — 2026-05-24

### Added

- **`rocky-cli`: `rocky adapter list` / `rocky adapter info <name>` + PATH-based process-adapter discovery** — formalizes the existing `rocky_adapter_sdk::process::ProcessAdapter` (already shipped, used by `rocky test-adapter --command <path>`) into a canonical user-facing surface. Rocky now follows the `cargo`-subcommand convention: any executable on `$PATH` whose filename starts with `rocky-` is treated as a process adapter named after the suffix (e.g. `rocky-snowplow` registers as `snowplow`). `rocky adapter list` walks `$PATH`, spawns each candidate, and emits one entry per discovered adapter with its manifest (text columns: `NAME / VERSION / DIALECT / PATH`; `--output json` returns the full `AdapterManifest`). Adapters that fail to initialize still appear in the listing with an error message so broken installs are visible rather than silently skipped. `rocky-lsp` is filtered out so the bundled language server isn't mis-reported as an adapter. `rocky adapter info <name>` resolves `rocky-<name>` on `$PATH`, spawns it, and prints the manifest. `rocky test-adapter --adapter <name>` now falls back to the same PATH lookup when `<name>` isn't a compiled-in adapter (`databricks` / `snowflake` / `duckdb`), so the same `--adapter` flag works for shipped and installed adapters. The wire protocol (JSON-RPC 2.0, newline-delimited JSON over stdio, methods: `initialize` / `execute_statement` / `execute_query` / `describe_table` / `table_exists` / `shutdown`) is documented end-to-end in `engine/examples/process-adapter-echo/PROTOCOL.md`, with a working reference adapter at `engine/examples/process-adapter-echo/rocky-echo` (a ~150-line Python script that pretends `main.demo.events` exists). New integration test `process_adapter_round_trip` spawns the example and asserts the contract: manifest fields, `execute_statement` ack, `describe_table` columns, `table_exists` true/false. The `AdapterConfig` runtime path (i.e. `[adapter.foo] type = "process"` + `command = "..."` in `rocky.toml` flowing through `AdapterRegistry::from_config` into the pipeline runner) is **not** part of this cut — it's gated on the in-flight `AdapterConfig` reshape and ships as a follow-up.
- **`rocky-cli`: `branch promote` now walks transformation pipelines, not just replication** — `branch promote` (and `plan promote`) previously enumerated only the replication pipeline's discovery surface and surfaced a hard error on any other pipeline type. The enumeration is now pipeline-type-aware: replication pipelines still discover source connectors and resolve each table through the schema-pattern templates, and transformation pipelines walk the configured `models` glob (the same surface `rocky list models` reads), emitting one promote target per model from its sidecar `[target]` block. Both shapes feed the same `(prod, branch_source)` payload — the branch source rewrites the production schema to the branch's schema prefix — so the downstream `CREATE OR REPLACE TABLE prod AS SELECT * FROM branch__<name>.<table>` dispatch is unchanged. Ephemeral models are skipped (no physical table to promote). A new `--pipeline <name>` flag on `branch promote` and `plan promote` disambiguates multi-pipeline configs; omitting it on a single-pipeline project keeps working as before. On transformation pipelines `--filter` accepts `table` / `model` / `catalog` / `schema` keys (mirroring `rocky run`), and a stale replication-style key (e.g. `client=acme`) is rejected fast rather than silently matching zero models and shipping a no-op promote. Quality / snapshot / load pipelines remain unsupported with a clear error. New tests cover model-glob enumeration, the transformation filter surface, mixed-project per-pipeline routing under `--pipeline`, and an end-to-end DuckDB promote that copies branch-schema rows into production for every model.
- **`rocky-trino`: `WarehouseAdapter::fetch_arrow_batch` via spooled-protocol Arrow encoding** — `TrinoAdapter` overrides the default `Err` impl with a real implementation backed by a new `arrow_stream` module on the connector. The path negotiates Arrow IPC via the documented spooling-protocol headers (`X-Trino-Client-Capabilities: SPOOLING` + `X-Trino-Spooled-Segments-Accept-Encoding: arrow+zstd,arrow`), fetches each spooled segment as raw Arrow IPC bytes (inline base64 + presigned `uri` variants both supported), decodes via `arrow::ipc::reader::StreamReader`, concatenates per-segment batches with `arrow::compute::concat_batches`, and best-effort acks each segment via its `ackUri`. **Version-gated:** Apache Arrow IPC is a proposed spooled-protocol encoding (upstream PR [`trinodb/trino#26365`](https://github.com/trinodb/trino/pull/26365), closed stale Nov 2025) — no shipping Trino release yet honours `arrow`/`arrow+zstd`. When the coordinator falls back to inline JSON rows the adapter surfaces `TrinoError::ArrowEncodingUnavailable` (wrapped in `AdapterError`) rather than silently degrading; the negotiation wire is already in place for the day the encoding merges. Wiremock unit tests cover the inline-segment decode path, the spooled-URI fetch + ack path, multi-page Arrow concatenation across `nextUri` polls, the version-gated server-fallback error, and pin the negotiation header values on the wire. A new live `#[ignore]` conformance test asserts the version gate against the existing `trinodb/trino:latest` Docker harness (flips to a positive receipt once upstream Arrow encoding lands).
- **`rocky-bigquery`: `WarehouseAdapter::fetch_arrow_batch` via the BigQuery Storage Read API** — overrides the default `Err(...)` impl shipped in PR #631. Two-hop flow: (1) `jobs.query` runs the SQL and lands results in an anonymous destination table (BigQuery's standard behaviour for non-DDL query jobs); (2) `jobs.get` resolves `configuration.query.destinationTable` → `(project, dataset, table)`; (3) Storage Read API `CreateReadSession` + `ReadRows` over gRPC streams Arrow IPC schema + record-batch messages directly, which decode through workspace `arrow 58` `StreamReader` (schema-bytes-spliced-in-front-of-each-batch pattern) and concatenate via `arrow::compute::concat_batches` into a single `RecordBatch`. New `storage_read` module wraps the tonic-generated `BigQueryReadClient` from the `googleapis-tonic-google-cloud-bigquery-storage-v1` crate (version-aligned to the workspace `tonic 0.14` + `prost 0.14`), wires bearer auth via `tonic::service::interceptor`, and reuses the existing `BigQueryAuth` cache so the gRPC path shares OAuth tokens with the REST path (same scope `https://www.googleapis.com/auth/bigquery`). New workspace deps: `tonic`, `prost`, `googleapis-tonic-google-cloud-bigquery-storage-v1` — all version-pinned to majors already present in the lock graph (via `opentelemetry-otlp`), so no second copy of either crate enters the dependency tree. Live-verified on the BQ sandbox in EU (`SELECT 1 AS n, 'foo' AS s` → 1-row / 2-column workspace `RecordBatch` with `Int64` + `Utf8` columns, `totalBytesBilled = 0` because the query is constant-folded by the optimizer before scan). Caller IAM requires `bigquery.readSessions.create` (the bundled `BigQuery Read Session User` role supplies it) in addition to the usual `dataEditor` + `jobUser` grants.
- **`rocky-core`: VACUUM refcount primitives on the content-addressed artifact ledger** — new `StateStore::refcount_for_hash(blake3_hash) -> Result<u64, StateError>` method counts the ledger rows pointing at a content hash, and a new free function `partition_vacuum_candidates(candidates, refcount_fn) -> VacuumPartition` splits a batch of `ArtifactRecord` candidates into `safe_to_delete` (refcount == 1, where the candidate is the sole reference) and `still_referenced` (refcount > 1 OR refcount == 0 OR lookup failed — fail-closed semantics). The closure-based refcount lookup is transport-agnostic so callers can back it with the ledger, a pre-computed map, or a remote service; per-batch memoisation collapses duplicate hashes to one lookup. This is the Phase 6 refcount primitive that consumers of the `OUTPUT_ARTIFACTS` ledger (introduced in v1.40) will call before physically deleting content-addressed parquet bytes: a single-reference candidate is the last pointer so deletion is safe, while a multi-reference candidate must stay live because branches / replayed runs still reference the same bytes. **No call sites in tree today** — today's `rocky compact` / `rocky archive` emit `VACUUM <table> RETAIN <hours> HOURS` SQL that Delta itself executes against the warehouse, with no Rocky-side object-store deletion path to gate. The primitive lands ahead of the branch-aware materialization consumer so the integration step is a single `partition_vacuum_candidates(...)` call when the object-store deletion path comes online. Five new unit tests pin the refcount semantics: ledger row counting, single-reference deletion (baseline regression), multi-reference preservation, refcount-zero fail-closed, lookup-error graceful degradation (single bad hash does not nuke the sweep), and per-hash memoisation (one ledger scan per unique hash regardless of batch duplication).
- **`rocky-databricks`: `WarehouseAdapter::fetch_arrow_batch` via Statement Execution `EXTERNAL_LINKS` + `ARROW_STREAM`** — the Databricks adapter now overrides the default `Err("not supported")` impl with a real implementation that returns a workspace `arrow::record_batch::RecordBatch`. Databricks rejects `disposition=INLINE` paired with `format=ARROW_STREAM` (`INVALID_PARAMETER_VALUE`: "The format field must be JSON_ARRAY when the disposition field is INLINE"), so the only viable Arrow path is `disposition=EXTERNAL_LINKS`: the response carries `manifest.total_chunk_count` + a list of pre-signed cloud-storage URLs on `result.external_links`. The new `DatabricksConnector::execute_sql_arrow` submits through the existing breaker + retry + span-attribute path (`ResultFormat::ExternalArrow` threaded through `submit_and_wait_with_format`), pages forward via `GET /api/2.0/sql/statements/{id}/result/chunks/{i}` when Databricks did not inline every link, fetches chunks concurrently (cap: 4) on the bare `reqwest::Client` (no Bearer token — the storage layer rejects it on Azure/GCS and ignores it on S3), decodes each chunk via `arrow::ipc::reader::StreamReader`, and concatenates the per-chunk batches via `arrow::compute::concat_batches`. Two new terminal `ConnectorError` variants (`Arrow`, `NoArrowChunks`) cover IPC decode and empty-result-set cases. Live-verified end-to-end on the Databricks sandbox via `cargo test -p rocky-databricks --test fetch_arrow_batch_live -- --ignored` (asserts the same shape as the rocky-duckdb conformance test: 2 fields named `n`/`s` typed `Int32`/`Utf8`, 1 row, values `1` + `"foo"`).
- **`rocky-catalog-core`: `GovernanceCatalogClient` trait + `Securable` enum** — new parallel trait alongside `CatalogClient` for the multi-securable RBAC surface. `Securable::{Catalog, Schema, Table}` represents Unity / Polaris's three-level securable namespace; the trait exposes `apply_grants(securable, &[Grant])`, `revoke_grants(...)`, `list_grants(...)`. **Opt-in** — adapters that don't expose RBAC mutation over REST (Iceberg REST today, DuckDB, BigQuery, …) simply don't implement it; callers detect the absence at the binding site rather than pattern-matching on `CatalogError::UnsupportedOperation`. The existing `CatalogClient::apply_grant(table, grant)` / `revoke_grant(...)` singular table-scoped path stays untouched — both paths coexist. Trait + `Securable` are re-exported from the crate root.
- **`[freshness]` config block + W005 freshness-coverage diagnostic** — a model can now declare a TTL after which it is considered stale via a per-model `[freshness]` sidecar block (`expected_lag_seconds`, optional `time_column`, optional `severity`), and a project-wide default via the top-level `[freshness]` block on `rocky.toml`. Per-model fields win field-by-field over the project default; a model with no block at all inherits the project default when it carries an `expected_lag_seconds`. The compiler emits a new soft-warn **W005** for any model that has at least one temporal output column (DATE / TIMESTAMP / TIMESTAMP_NTZ) but no `freshness` declaration in scope — neither a per-model block nor a project-level default — nudging authors toward an explicit freshness expectation. W005 is suppressed by adding a `[freshness]` block (per-model or project) and is silenced globally when the project default declares an `expected_lag_seconds`. The diagnostic fires through `rocky compile`, `rocky plan`, `rocky run`'s governance preview, and the LSP. In the LSP it carries an AI code-action arm (`rocky.ai-freshness-fix.v1`) that proposes a `[freshness]` block appended to the model's sidecar `.toml`, picking `time_column` from the model's candidate temporal columns. The per-model `expected_lag_seconds` deserializes as an alias of the existing `max_lag_seconds` field, so existing sidecar fixtures and generated dagster / VS Code bindings keep working unchanged. Codegen cascade propagated to `rocky-project.schema.json` + downstream dagster Pydantic + VS Code TypeScript bindings.
- **`rocky-lang`: `SourceFile` carries `path` + `read_source` loader for the salsa pipeline** — `#[salsa::input] SourceFile` now stores `path: PathBuf` alongside `text: String`, and a new `read_source(&mut db, path)` helper loads `.rocky` files from disk into a `SourceFile` input, deduplicating by canonical path via a `HashMap<PathBuf, SourceFile>` field on `RockyDatabase`. A second `read_source` call with the same path returns the cached handle without re-reading disk — LSP / file-watcher integrations push fresh contents through `SourceFile::set_text` (which still bumps the salsa revision and re-runs `parse_file`). Missing or unreadable paths surface a new `ReadSourceError` (`io::Error` + offending path); the salsa database is not mutated on the error path. A `lookup_source(&db, &path)` helper lets `didChange`-style handlers fetch the existing input by canonical path without re-reading. Follows the canonical Salsa 0.26 `lazy-input` example (Option A: path-as-input-field + external dedup map). New tests pin: disk content round-trip, path dedup (same input handle, parse-count stays at 1 across two `parse_file` calls), distinct paths produce distinct inputs, `set_text` after `read_source` invalidates downstream queries, missing-file returns `Err` without mutating the dedup map. The original three-step memoization invariant (parse count 1 → 1 → 2 across `set_text`) is preserved.
- **`rocky-lang`: salsa incremental-computation spike (`incremental` module)** — opt-in, parallel parse API built on the [salsa 0.26](https://docs.rs/salsa) framework (the rust-analyzer team's incremental-computation library; sometimes referred to internally as "salsa-2026"). New `RockyDatabase`, `SourceFile` salsa input, and `parse_file` tracked query wrap the existing `parser::parse` entry point with memoization: repeated parses of an unchanged `SourceFile` are served from cache; `SourceFile::set_text` bumps the input revision and forces a re-parse. The main compile pipeline in `rocky-compiler` and the LSP server in `rocky-server` are intentionally **not** wired up — this PR proves the framework's behaviour against real Rocky parser inputs without committing to a migration. Includes a `ParsedAst(Arc<RockyFile>)` newtype with `unsafe impl salsa::Update` so the AST round-trips through the cache without touching `ast.rs`. Unit tests assert (a) the parser body runs exactly once across two `parse_file` calls with identical input, and (b) running it a third time after `set_text` re-invokes the parser, ruling out the "function never called" failure mode for the memoization receipt.
- **`rocky-core`: `WarehouseAdapter::fetch_arrow_batch` trait method (Arrow inter-adapter PoC)** — new async method returning `arrow::record_batch::RecordBatch` so warehouse adapters can hand back columnar results without going through the row-of-JSON `QueryResult` shape. **PoC scope:** only `rocky-duckdb` implements it today (via DuckDB's native `query_arrow` against workspace `arrow 58`); every other adapter inherits the default impl that returns `AdapterError::msg("fetch_arrow_batch not supported by this adapter")`. No existing call sites are migrated — the goal of this cut is to prove the trait shape before per-adapter rollout (Databricks, Snowflake, BigQuery, Trino, Iceberg, Airbyte, Fivetran follow-ups). Live-verified end-to-end via `cargo test -p rocky-duckdb -- --ignored fetch_arrow_batch`.

### Changed

- **`rocky-cli`: `branch promote` approvals now bind to model bytes, not just config** — `branch_state_hash` (the value an approval signs over, so an approval is valid only against the exact reviewed state) previously hashed only the branch metadata + the `rocky.toml` fingerprint. Editing a model's SQL after approval therefore left the stale approval valid — a soundness hole. The hash now also folds in a fingerprint over the project's `<config_dir>/models` tree: each file's relative path + bytes are streamed into a blake3 digest (the path is hashed *alongside* the bytes with `\0` separators so renames/moves register; entries are sorted by relative path so the digest is order-independent; `.git`/`target`/`.DS_Store` are skipped; an absent models directory yields `"none"`, an unreadable file an `<unreadable>` marker — never a panic). Editing, adding, renaming, or removing any model file now voids every existing approval. **Upgrade note: because `models_hash` is a new field in the signed payload, all pre-existing branch approvals become invalid on upgrade and must be re-signed (`rocky branch approve`).** Scope is the conventional `models/` tree (a non-default `models` glob location is still covered transitively — editing the glob declaration changes the `rocky.toml` fingerprint). Four new tests pin the invariants: stable-when-unchanged, model-edit, file-add, and rename (identical bytes / new path).
- **`rocky-airbyte`: documented as out-of-scope for the `WarehouseAdapter::fetch_arrow_batch` rollout** — the Cluster H Arrow PoC trait docstring in `rocky-core::traits` lists Airbyte alongside the warehouse adapters; that listing is aspirational and does not reflect this crate's shape. `rocky-airbyte` only implements `DiscoveryAdapter` (metadata-only Airbyte Configuration API), not `WarehouseAdapter` — Rocky never executes queries against Airbyte. Sync output lands in a downstream warehouse (Databricks, Snowflake, BigQuery, Trino, Iceberg, DuckDB), and Arrow reads of that data flow through the warehouse adapter for that destination. New crate-level + `adapter.rs` module-level docstrings make the intentional non-implementation explicit so the next reader doesn't try to "fill in" a stub `WarehouseAdapter` impl that would have no production caller. Doc-only — no behaviour change, no Cargo.lock churn.
- **`rocky-snowflake`: documented `WarehouseAdapter::fetch_arrow_batch` as out-of-scope** — Snowflake's public SQL REST API v2 (`/api/v2/statements`, the API this adapter speaks) is JSON-only: requesting `resultSetMetaData.format = "arrowv1"` on submit is silently downgraded to `"jsonv2"`, and fetching a large result's partition with `Accept: application/vnd.apache.arrow.stream` still returns gzip-compressed JSON with `Content-Type: application/json` — verified live against a 14-partition result, where the Arrow `Accept` header was ignored outright. Arrow over Snowflake exists only in the native driver protocol (Python / JDBC / Go / ADBC connectors), which this REST-based adapter does not use, so `SnowflakeWarehouseAdapter` keeps the default "not supported" `Err` for `fetch_arrow_batch`. New crate-level + `adapter.rs` module-level docstrings record the constraint and the live evidence so the next reader doesn't re-attempt a stub that would only hide a JSON round-trip behind the columnar trait. Doc-only — no behaviour change, no Cargo.lock churn.
- **`rocky-catalog-core` + `rocky-iceberg`: `TableCommit` extends to multi-ref + schema / partition-spec passthrough** — `TableCommit` gains three optional fields on top of the v1 (`expected_snapshot_id`, `new_snapshot_id`) pair: `branch: Option<String>` (defaults to `"main"` when `None`, preserving the v1 contract), and `extra_requirements` / `extra_updates` (`Vec<serde_json::Value>` — opaque Iceberg-spec `commit-table-update` JSON entries the adapter threads through alongside the synthesized `assert-ref-snapshot-id` + `set-snapshot-ref` pair). A new `TableCommit::new(table, expected, new)` constructor plus `with_branch` / `with_extra_requirements` / `with_extra_updates` builder setters keep the common single-branch case terse. The `IcebergCatalogClientAdapter::commit_transaction` impl honours the new fields end-to-end: the synthesized assert + advance fire against the selected ref (so multi-table multi-ref commits in one atomic batch work), and extras are appended to the wire body in declaration order *after* the synthesized entries so the CAS holds whatever the caller threaded through. Typed catalog-agnostic `MetadataUpdate` variants (e.g. `AddSchema`, `SetPartitionSpec`) would be a strict surface widening that pulls Iceberg's spec into `rocky-catalog-core` — that's a follow-up gated on Polaris / Nessie / Unity cross-catalog evidence; the passthrough payload satisfies the trait contract for the schema / partition-spec use cases today. New wiremock tests pin the multi-ref headline (`commit_transaction_advances_named_branch`), the passthrough payload order (`commit_transaction_threads_schema_and_partition_updates`), and per-table ref selection inside one atomic batch (`commit_transaction_multi_table_with_per_table_refs`); the existing single-ref + `main`-default wiremock tests still hold byte-for-byte after migration to `TableCommit::new`.
- **`rocky-databricks`: `UnityCatalogClient` implements `GovernanceCatalogClient` with multi-change PATCH batching** — a flat `Vec<Grant>` against a `Securable` now collapses into **one** HTTP PATCH per securable, grouping by principal so a single request carries every `(principal, [privilege, ...])` pair the caller asked for. Where the singular `CatalogClient::apply_grant` wraps one change in a one-entry `changes[]` array, the new path drains the wire shape Unity's permissions endpoint actually serves (`changes: Vec<PermissionChange>`). URL path dispatches on `Securable` variant: `permissions/catalog/{name}` / `permissions/schema/{catalog.schema}` / `permissions/table/{catalog.schema.table}`. Empty input short-circuits before any HTTP traffic. The singular `CatalogClient::apply_grant` / `revoke_grant` path is untouched. New wiremock test asserts `.expect(1)` on the PATCH endpoint when a 3-grant / 2-principal batch is applied — fails noisily if a future change reintroduces one-grant-per-call looping.
- **`rocky-cli`: `rocky run` opportunistically routes catalog- and schema-level grants through `GovernanceCatalogClient` when the adapter implements it** — the catalog-grant and schema-grant emit sites now prefer the REST-routed `GovernanceCatalogClient::apply_grants` path when the registry returns one for the target adapter, falling back to the existing `GovernanceAdapter::apply_grants` (SQL `GRANT`) path otherwise. No behavioural delta when the adapter doesn't implement the new trait — DuckDB / BigQuery / Snowflake / Iceberg continue to flow through the legacy path. Databricks gets the multi-change PATCH win. Multi-word privileges are rendered in Unity's REST `Privilege` enum form (`USE_CATALOG` / `USE_SCHEMA`) at the IR→trait boundary, distinct from the spaced SQL `GRANT` grammar (`USE CATALOG`) the legacy path emits. Errors on either path are best-effort `warn!` (matching the call site's existing failure semantics). New `AdapterRegistry::governance_catalog_client(name)` accessor returns `Option<Arc<dyn GovernanceCatalogClient>>`: `Some(UnityCatalogClient)` for Databricks, `None` for everything else.
- **`rocky-adapter-sdk`: close the three blockers `rocky-trino` v0 surfaced ([#427](https://github.com/rocky-data/rocky/pull/427)).** (a) `WarehouseAdapter` gains six additive default-impl methods that mirror the in-tree `rocky-core::traits::WarehouseAdapter` surface — `execute_statement_with_stats` (+ new `ExecutionStats` struct), `ping`, `explain` (+ new `ExplainResult` struct), `is_experimental`, `warehouse_name`, `list_tables`. Out-of-tree adapters now get the same method surface in-tree adapters get, without breaking the `rocky-core` trait signature that sibling adapter crates (Databricks / Snowflake / BigQuery / Trino / Airbyte / Iceberg) are mid-rollout against. The remaining non-additive divergences (`fetch_arrow_batch`, `clone_table_for_branch`, the `SqlDialect::merge_into` signature, and the duplicated `TableRef` / `ColumnInfo` / `Grant` / `MetadataColumn` types) are deferred — they require cross-crate type unification that this PR deliberately doesn't land. (b) `AdapterConfig` (in `rocky-core::config`) gains an `extra: BTreeMap<String, serde_json::Value>` escape hatch under `[adapter.<name>.extra]` so adapter-specific keys can flow through without disabling the existing `deny_unknown_fields` typo-detection on the top-level fields. (c) New `rocky_adapter_sdk::auth::AuthProvider` trait composes the `Authorization` header with a list of extra mandatory headers — the `X-Trino-User`-on-JWT case that broke Trino v0's auth model now has a canonical shape. A reference `StaticAuthProvider` ships alongside for tests and simple cases. No in-tree adapter required migration; the trait additions are default-impl, the `extra` field is `#[serde(default, skip_serializing_if = ...)]`, and the auth provider is a new abstraction nothing yet depends on.
- **`rocky-server`: LSP `didOpen` / `didChange` now seed and mutate the `rocky-lang` salsa `SourceFile` inputs** — every editor buffer that arrives via `textDocument/didOpen` is loaded into the shared `RockyDatabase` via `read_source` and immediately re-overwritten with the editor's text via `SourceFile::set_text`, so the salsa input matches what the user actually sees (not stale disk content). Subsequent `textDocument/didChange` notifications push the new buffer through `set_text`, bumping the input revision and invalidating downstream tracked queries. The buffer-hash short-circuit at the top of `did_change` still gates this — cursor-only "edits" don't churn the salsa cache. The synthetic E028 path in `publish_dsl_parse_diagnostics` now flows through the memoized `parse_file` tracked query when the file has a salsa input, so a compile failure burst during interactive editing re-parses each broken `.rocky` file at most once per buffer revision instead of once per compile attempt. Cold-start (no `didOpen` yet) keeps the original `read_to_string` + `rocky_lang::parse` fallback. `tokio::sync::Mutex<RockyDatabase>` wraps the db because every salsa entry point (`read_source`, `set_text`, `db.files` dedup map) needs `&mut RockyDatabase`. Non-`file:` URIs (e.g. `untitled:`, `vscode-remote:`) skip the salsa wiring with a debug log — the rest of the LSP still operates off `self.documents`. New tests pin (a) `did_open` populates the salsa input and `parse_file` returns the same `Arc<RockyFile>` instance across two reads (memoization), (b) `did_change` with new text invalidates the cache so the next `parse_file` produces a fresh `Arc`, and (c) buffer-hash short-circuit suppresses no-op `did_change` notifications from bumping the salsa revision.
- **`rocky-compiler`: route per-file compile diagnostics through `salsa::Accumulator`** — `rocky_compiler::salsa_compile` exports a new `#[salsa::accumulator] struct CompileDiagnostic(pub String)`; parse and lower errors emitted from inside `file_typecheck` push a `CompileDiagnostic` instead of stashing on the returned `FileTypecheck` (the `Vec<String> diagnostics` field is removed). Callers retrieve via `file_typecheck::accumulated::<CompileDiagnostic>(db, src)` — `project.rs::load_single_rocky_model_with_db` is updated accordingly. The wire shape (`"parse error: …"` / `"lower error: …"` string prefixes) is preserved so the prefix-discriminating branch that maps the leading diagnostic to `ProjectError::RockyParse` / `ProjectError::RockyLower` keeps working untouched. The win: diagnostics now participate in salsa's memoization + backdating — a second `file_typecheck` call against unchanged inputs returns the same diagnostic set without re-emitting (no duplicate pool growth), and a `set_text` that produces a token-equivalent AST (whitespace-only edits) backdates so the diagnostics are inherited from the cached run rather than re-emitted. New tests pin the four invariants: single-emit on first call, no re-emit on cache hit, backdate-on-token-equivalent-AST (the headline receipt), and clean invalidation on a genuine AST change (no stale diagnostics from the prior AST).
- **`rocky-compiler`: per-file typecheck via salsa tracked queries** — new `rocky_compiler::salsa_compile` module wraps the `.rocky` parse + lower + per-file column extraction in two salsa tracked queries (`file_typecheck(db, SourceFile) -> Arc<FileTypecheck>` and `source_signature(db, SourceFile) -> u64`), sharing the same `RockyDatabase` as the parser-side `rocky_lang::incremental::parse_file` query. The body of `file_typecheck` runs at most once per `SourceFile` revision: repeated compiles against the same database with unchanged inputs return the cached `Arc<FileTypecheck>` without re-entering the body, and structural `PartialEq` on the AST nodes lets salsa backdate when `set_text` produces a token-equivalent AST. Edit one file's text → only that file's body re-runs; independent files stay cached. Whole-project orchestration (`compile()` / `compile_project()` / `compile_incremental()`) is unchanged — cross-model passes (join keys, contracts, blast-radius, classification) stay in the plain orchestration layer and remain whole-project for now. `salsa = { workspace = true }` added to `rocky-compiler`'s dependencies. New integration tests pin the receipts: cold-cache baseline (3 files = 3 invocations), unchanged inputs (second compile = 0 additional invocations), one-file edit (only the changed file's body re-runs — others stay cached), and `source_signature` stability across no-op `set_text` calls. Scope-of-this-cut: `.sql` models still load through the existing non-tracked path; full cross-model transitive invalidation via salsa is a follow-up that requires routing `compute_model_typecheck`'s cross-cutting context (semantic graph, source schemas, mask config) into the database.
- **`rocky-lang`: derive `salsa::Update` on AST types directly; drop the `ParsedAst` newtype + `unsafe impl Update`** — the Salsa PoC's `incremental::parse_file` query now returns `Arc<RockyFile>` directly. `RockyFile` and all 17 transitively-reachable AST nodes derive `salsa::Update` (plus structural `PartialEq` so salsa can backdate when re-parsing produces an identical AST). The `unsafe impl salsa::Update for ParsedAst` block and the `Arc::ptr_eq`-based `PartialEq` are gone — no remaining `unsafe` in `rocky-lang`. Memoization invariants unchanged (the three in-module tests still hold: parse-count is 1 after the first call, still 1 after a second call with identical input, 2 after `set_text` mutation).
- **`rocky-duckdb`: bump `duckdb` to `1.10503.1`, drop the Arrow IPC stream bridge** — earlier `duckdb 1.4.x` pinned `arrow 56`, forcing `fetch_arrow_batch` to round-trip results through the IPC stream format to reach the workspace `arrow 58` types. `duckdb 1.10503.1` ships against `arrow 58` directly, so the bridge is gone: the renamed `arrow56` aliased dep is removed from `rocky-duckdb`, `query_arrow_ipc_bytes` is replaced by a direct `query_arrow_batch` that returns `arrow::record_batch::RecordBatch`, and the adapter's `fetch_arrow_batch` impl is a thin pass-through. `DuckDbError::Arrow` is retained to cover multi-batch `concat_batches` failures. Engine workspace MSRV bumped from `1.85` to `1.85.1` to match the new `duckdb` / `libduckdb-sys` requirement. Live `#[ignore]` conformance test renamed to `fetch_arrow_batch_returns_workspace_arrow_batch` and verified green end-to-end against the rebuilt path.
- **`rocky-cli`: warehouse-side circuit-breaker cooldown parity** — `TableErrorOutput.cooldown_seconds: Option<u64>` added to the `rocky run` JSON output, mirroring the Fivetran-side `FailedSourceOutput.cooldown_seconds` shipped in v1.42.0. Populated when a Databricks or Snowflake circuit breaker trips on a config with `retry.circuit_breaker_recovery_timeout_secs` set. Sourced from a new `pub fn recovery_timeout(&self) -> Option<Duration>` accessor on `rocky_core::circuit_breaker::CircuitBreaker`; the warehouse `ConnectorError::CircuitBreakerOpen` variants now carry `cooldown_seconds: Option<u64>` and a unified `classify_anyhow_error_with_cooldown` walker projects them onto `TableError`. **Behaviour change:** the warehouse `CircuitBreakerOpen → FailureKind` mapping flips from `Transient` → `QuotaExceeded` to match the enum's documented semantics ("Rate limit hit or a configured budget cap (retry budget, circuit breaker, account-level quota)") and the existing dagster handler keyed on `failure_kind == quota-exceeded`. Orchestrators that previously treated a tripped warehouse breaker as a fail-fast transient now get a retriable `dg.Failure` with the warehouse's configured `retry_after_seconds` instead.
- **`rocky-core`: `CircuitBreaker::recovery_timeout()` accessor** exposes the previously-private field so warehouse adapters can mirror it onto `ConnectorError::CircuitBreakerOpen.cooldown_seconds` without re-parsing config. `None` for manual-reset-only breakers preserves the original behaviour.
- **`rocky-observe`: canonical cross-adapter span-attribute schema** — new `rocky_observe::span_attrs` module ships 7 canonical attribute-key constants (`ADAPTER_NAME`, `WAREHOUSE_NAME`, `WAREHOUSE_QUERY_ID`, `WAREHOUSE_BYTES_SCANNED`, `WAREHOUSE_TABLE_REF`, `STATEMENT_KIND`, `RETRY_ATTEMPT`) under the existing `rocky.<resource>.<field>` shape, so OTel collectors see consistent keys across Databricks / Snowflake / BigQuery instead of bespoke per-adapter names. The Databricks, Snowflake, and BigQuery `statement.execute` spans now emit `rocky.adapter.name` + `rocky.warehouse.name` at span creation and record `rocky.warehouse.query_id` (+ `bytes_scanned` where the response payload carries it, + `retry.attempt`) on the success path via `Span::record`. The pre-existing bespoke `adapter` / `statement.kind` attributes are emitted **alongside** the canonical names for one release to keep existing dashboards green — follow-up PRs drop the aliases. Sampling and batching are unchanged; this is naming unification only.

### Fixed

- **`rocky-snowflake`: deflake the `statement.execute` span-capture wiremock test** — `test_statement_execute_span_emitted` asserted the canary span's `statement.kind == "query"`, but its `CREATE TABLE canary` statement classifies as `ddl` (`classify_statement_kind` tags `CREATE`/`ALTER`/`DROP`/`GRANT`/`REVOKE` as `ddl`). The stale assertion failed *while holding the process-wide `captured_spans()` mutex*, poisoning it and cascading `PoisonError` panics into sibling tests (`test_statement_failed`, `test_password_auth_omits_token_type_header`); the suite stayed green only because parallel execution let the `.find(statement.execute)` pick up a sibling test's `query`-kind span from the shared capture buffer. The test now asserts the canary's span *exists* (adapter `snowflake` + `statement.kind=ddl` on a single span) via `any()` instead of indexing the first entry, and drops the lock guard before asserting so a failed assertion can no longer poison the shared mutex. Verified green both in isolation (`--test-threads=1`) and across the full parallel suite.
- **`rocky-cli`: give the `branch promote` transformation e2e test a self-contained git identity** — `run_branch_promote_transformation_e2e_succeeds` `set_current_dir`s into a fresh `TempDir` and drives a real promote, which resolves the approver identity via `git config --get user.email` (cwd-sensitive: repo-local → global → system). On a host with no global/system git identity (CI runners), that lookup exits 1 and the whole promote fails with "`git config --get user.email` exited with status 1" — surfacing as an intermittent red `Test` job on unrelated PRs. The test now `git init`s the `TempDir` and sets a local `user.email` / `user.name`, so the identity resolves deterministically from the test's own repo regardless of ambient git config. Reproduced deterministically with `GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_SYSTEM=/dev/null` (RED before, GREEN after); the full `commands::branch` module stays green.

## [1.42.0] — 2026-05-21

Headline: **Fivetran 429-storms now trip the shared circuit breaker** instead of stalling each pod in a 6-minute in-pod backoff loop. Pairs with a structured retriable signal on the Dagster side so orchestrators learn the failure within seconds and reschedule the run past the breaker cooldown. Single-PR cut ([#624](https://github.com/rocky-data/rocky/pull/624)) — small, targeted, high-impact for downstream consumers running Fivetran replication on a shared Valkey-backed breaker.

### Changed

- **`rocky-fivetran`: sustained 429s now vote `Remote` to the shared circuit breaker** ([#624](https://github.com/rocky-data/rocky/pull/624)). Previously `is_remote_failure(RateLimited)` returned `false`, so the per-account shared breaker never tripped during 429-storms — each pod sat in its own exponential-backoff retry loop for several minutes before exiting non-zero with no retry signal to the orchestrator. Post-change, `RateLimited` only surfaces *after* `retry.max_retries` consecutive 429s on one envelope-fetch (the per-call retry loop absorbs single transients), so the post-retry classification is already a sustained-storm signal — exactly what the shared breaker is designed to act on. With `failure_threshold = 5` (default), the breaker trips on the 5th post-retry storm in a 60s window and short-circuits subsequent calls with `FivetranError::CircuitOpen` for the configured cooldown.
- **`rocky-fivetran`: `FivetranError::CircuitOpen` now carries `cooldown_seconds: u64`** so callers downstream of the adapter (Dagster, etc.) can derive a `retry_after` hint without re-parsing config. Sourced from `FivetranCircuitBreaker::cooldown_seconds()`, a new trait method with a default impl returning 300s for out-of-tree backends. The `InMemoryCircuit` + `ValkeyCircuit` implementations read it from `CircuitConfig.cooldown.as_secs()`.
- **`rocky-cli`: `FailedSourceOutput.cooldown_seconds: Option<u64>`** added to the `rocky discover` JSON output (and mirrored on `rocky-core::source::FailedSource`). Populated only by adapters whose failure mode carries a known cooldown — currently only Fivetran when the shared breaker trips. Optional with `skip_serializing_if = "Option::is_none"`, so fixtures captured before this cut stay byte-stable. Full codegen cascade propagated to dagster Pydantic `discover_schema.py` + VS Code `discover.ts`.
- **`rocky-core`: `RetryConfig::max_retries` doc-comment** now recommends `≤ 4` when the Fivetran shared circuit breaker is enabled (with a non-default backend), so a single 429-storm bursts at most ~5 attempts (`max_retries + 1`) per envelope-fetch before voting `Remote`. Higher values lengthen the storm without changing the outcome. The engine default (3) is unchanged.

## [1.41.0] — 2026-05-21

Headline of this cut: **the Arc 4 trust-system slice closes**. Run-time span data is now durable across process exits — JSONL-per-process spans land in `.rocky/traces/{ts}-{pid}.jsonl` with last-N retention, queryable via a new `read_trace(run_id)` API. Pairs with the `OUTPUT_ARTIFACTS` redb ledger that shipped in v1.40.0 to complete the artifact-tracking half — both halves of the Arc 4 observability surface are now in.

Alongside that: **catalog-first refactor enters its consumer-wiring phase**. `DatabricksWarehouseAdapter` now optionally composes `UnityCatalogClient` for read-side catalog ops (`describe_table` / `list_tables`); SQL fallback preserved. The full Phase 1 wiring task halted on a clean Phase 0 decision rule — the four governance methods (`set_tags` / grants) need a trait reshape; that work is captured in a Phase-2 design memo, gated on Polaris-impl evidence. The smaller Phase 1A cut ships here.

Plus three adapter-correctness PRs: **Snowflake replication-merge** dispatched `ColumnSelection::All` which Snowflake rejects (`UPDATE SET *` / `INSERT *`); the runner now resolves it against the discovered source schema, and the dialect emits explicit INSERT columns + double-quoted identifiers. **BigQuery cost-reconciliation** adds a non-circular live cross-check between `rocky run --output json` reported bytes and `bq show -j`'s `totalBytesBilled`. **LSP AI code actions** gain two new arms (W004 unresolved classification + E028 DSL parse errors) on top of the `code_action_resolve` + `AiContractActionData` pattern shipped in v1.37.

### Added

- **`rocky-observe`: JSONL span persistence with last-N retention** ([#616](https://github.com/rocky-data/rocky/pull/616)). New `tracing-subscriber::Layer` writes line-delimited JSON to `.rocky/traces/{YYYYMMDD-HHMMSS-mmm}-{pid}.jsonl` (Windows-safe — no colons in the filename), open-on-first-write append-only, no buffering. Default-on; OTLP exporter stays orthogonal. `init_tracing` peeks `args().nth(1)` and skips the layer for `rocky lsp` so long-lived language-server processes don't grow unbounded files. `ROCKY_TRACE_DISABLE=1` is a belt-and-suspenders escape hatch for shared-filesystem setups. The `run` span declares `fields(run_id)`; the value records onto the live span via `tracing::Span::current().record("run_id", ...)` immediately after mint, so subsequent nested events carry `run_id` in their `spans[]` chain. New `read_trace(run_id) -> Vec<TraceSpan>` scans the trace directory, returning all lines from any file whose lines reference the target `run_id` — file-per-process semantic means pre-mint spans return alongside their run's spans as context. **`SweepReport` schema cascade:** new `traces_deleted: u64` field joins the existing `runs_deleted` / `lineage_deleted` / `audit_deleted` shape 1:1; full codegen cascade hits `schemas/state_retention_sweep.schema.json`, dagster Pydantic `state_retention_sweep_schema.py`, and VS Code TypeScript `state_retention_sweep.ts`. Retention sweep restructured to fire before the existing redb-retention early-return gates so trace files don't grow unbounded when state-retention is disabled — has its own `ROCKY_TRACE_RETAIN_RUNS` env var (default 20) independent of redb retention policy.
- **`rocky-databricks`: delegate `describe_table` + `list_tables` through Unity REST when wired** ([#618](https://github.com/rocky-data/rocky/pull/618)). `DatabricksWarehouseAdapter` gains `catalog_client: Option<UnityCatalogClient>` via a new `with_catalog_client(client)` builder method (`::new(connector)` signature unchanged for back-compat). The two methods override their existing SQL paths to try `self.catalog_client.as_ref().map(|c| c.describe_table(...))` first; fall back to the existing `DESCRIBE TABLE` / `SHOW TABLES` SQL on `CatalogError::UnsupportedOperation` or transport error; surface other `CatalogError` as `AdapterError`. Three projection helpers (`ir_to_catalog_ref` / `catalog_schema_to_column_info` / `catalog_error_is_fallback`) carry the cross-type mapping with unit-test coverage. The governance methods (`set_tags` / `get_grants` / `apply_grant` / `revoke_grant`) stay on the existing SQL path pending a trait-shape decision. **Live-verified parity:** REST and SQL paths return identical column lists (by `(name, data_type)`) and identical lowercased table sets on the Databricks sandbox. Note one pre-existing divergence the live test captures: SQL `DESCRIBE TABLE` hard-codes `nullable = true` (the underlying SQL doesn't reliably report it); Unity REST returns declared nullability faithfully. Once REST is wired, callers get correct nullability; the SQL fallback stays lossy as it was before.
- **`rocky-server`: AI LSP code actions for W004 + `.rocky` DSL parse errors** ([#620](https://github.com/rocky-data/rocky/pull/620)). Two new arms on the `code_action_resolve` + `AiContractActionData` dispatcher established in v1.37 (PR #374). **W004 unresolved classification tag:** AI suggests the missing `[classifications]` block entry. As a critical upstream gap fix, `recompile()` now loads `rocky.toml` from `<models_dir>.parent()` and threads `mask` / `allow_unmasked` through (previously left empty, so W004 never reached `code_action` in production). New end-to-end test `w004_fires_through_compile_when_rocky_toml_declares_no_mask` drives the real compile pipeline. **`.rocky` DSL parse errors via new E028:** `ProjectError::RockyParse` aborts the compile pipeline before any `Diagnostic` is built (the compiler emits zero diagnostics on parse failure), so a new diagnostic code E028 is synthesized by `recompile()` on compile failure — it re-parses each `.rocky` file under `models_dir` and publishes a synthetic LSP diagnostic with span derived from `rocky_lang::ParseError`'s byte offset. The new arm hooks E028. AI mock matches the v1.37 precedent — no wiremock; the disabled-without-API-key path is exercised end-to-end. vscode cascade is a follow-up (tower-lsp / vscode-languageclient pass `CodeAction.data` through verbatim, so no client-side dispatch change is needed for the engine release to be usable).
- **`docs`: dbt-bundle migration guide covers `data_tests:` alias + manifest unit-tests bridge + POC 19** ([#596](https://github.com/rocky-data/rocky/pull/596)). `docs/src/content/docs/guides/migrate-from-dbt.md` §9 expanded with worked YAML→TOML examples covering both `tests:` / `data_tests:` column tests and `manifest.unit_tests` bridge, plus counter + warning surface. `engine/crates/rocky-compiler/src/import/dbt.rs::apply_dbt_tests` doc-comment mentions the alias and points at `apply_dbt_unit_tests`. New POC `examples/playground/pocs/06-developer-experience/19-import-dbt-unit-tests/` (no credentials, `./run.sh` exits 0, auto-discovered by the smoke matrix). POC 03 cross-links to POC 19.

### Fixed

- **`rocky-cli` + `rocky-snowflake`: replication-merge column threading + Snowflake dialect parity** ([#619](https://github.com/rocky-data/rocky/pull/619)). Snowflake's `MERGE` rejects `UPDATE SET *` and `INSERT *`; replication's `[merge]` strategy used to dispatch `ColumnSelection::All` from the runner. Two scoped commits land the fix end-to-end and let reviewers bisect dialect-vs-runner independently. **Dialect commit:** Snowflake `merge_into` emits an explicit INSERT column list + double-quoted identifiers (fixes the case-fold trap on unquoted Snowflake identifiers). **Runner commit:** `process_table` resolves `ColumnSelection::All` against the discovered source schema before dispatching to SQL-gen — `describe_table(&source_table)` is already awaited for drift; `source_cols` is already in scope; no new config surface or extra round-trip. Live `#[ignore]` test `live_merge_with_explicit_columns_is_idempotent` against the Snowflake sandbox was RED on `main` (Snowflake parser rejected the dispatched SQL) → GREEN on this branch, 3 idempotent runs → stable row count 3. `ir_golden` snapshots `03-replication-merge/snowflake.sql` + `05-transformation-merge/snowflake.sql` regenerated to match the new quoted INSERT form.

### Changed

- **`rocky-bigquery`: live cross-check of `bytes_scanned` vs `jobs.get` `totalBytesBilled`** ([#617](https://github.com/rocky-data/rocky/pull/617)). New live integration test plus a `docs/src/content/docs/reference/json-output.md` operator snippet showing the `rocky run --output json | jq '.statements[].warehouse_job_id'` → `bq show -j <job-id> --format=prettyjson | jq .statistics.query.totalBytesBilled` cross-check flow. The underlying `warehouse_job_id` plumbing was already shipped under PRs #324/#326/#329 in earlier releases — this PR closes the regression-surface gap by asserting `rocky.bytes_scanned == bq.totalBytesBilled` (±1% tolerance) against a real BQ job. The test exercises BQ's 10 MB billing floor (both sides report 10 485 760 bytes for a sub-floor query); the ±1% tolerance is not exercised against a non-trivial bill — that would require larger trial-credit spend than the regression surface justifies.

## [1.40.0] — 2026-05-20

Headline of this cut: **per-model cost ceilings, grounded in real catalog data**. Rocky now type-checks an optional `[budget]` block on a model sidecar at compile time, populates a typed `cost_ceiling: Option<CostBudget>` field on `ModelIr`, and — at `rocky plan` time — fetches real per-table size statistics from the warehouse catalog, propagates costs through the DAG, and emits `E027 budget exceeded` diagnostics for any model whose projected scan exceeds its declared ceiling. The full **Cluster 3 D series** lands across four PRs: typed IR field + sidecar parsing (D-1), `CatalogClient::table_stats` trait method + Iceberg snapshot-summary implementation (D-2 SOFT-GO — Unity REST has no stats surface by design), wiring into `rocky compile` against deterministic stubs (D-3 stage 1), and grounding the runtime check in real Iceberg snapshot summaries + Databricks `DESCRIBE DETAIL` (D-3 stage 2). Per-model `on_breach` policy is honoured — `"warn"` (default) emits Warning-severity diagnostics; `"error"` sets `PlanOutput::has_budget_errors`, signalling CI/orchestration to halt.

Plus an Arc 4 observability double: **`PipelineEvent` bus-emit completeness** — investigation found every OTel span event already had a matching call site; the actual gap was the inverse, 7 `record_span_event` sites with no matching `global_event_bus().emit()` (three of them `circuit_state_change` sites added in v1.38.0's Fivetran circuit breaker where the omission was consistent, not intentional). And **`OUTPUT_ARTIFACTS` state ledger** — a new redb side-table that persists the `blake3_hash` returned by `rocky_iceberg::uniform_writer::WriteResult` against the originating run + model, collapsing one of two prerequisites for the eventual Phase 6 VACUUM refcount sweep without pre-building refcount semantics.

### Added

- **`rocky-ir`: per-model `[budget]` cost ceiling IR field** ([#602](https://github.com/rocky-data/rocky/pull/602)). New `CostBudget { max_usd: Option<f64>, max_bytes_scanned: Option<u64> }` struct + `cost_ceiling: Option<CostBudget>` field on `ModelIr`. Model sidecar TOML `[budget]` blocks parse into the typed field through `engine/crates/rocky-core/src/models.rs::to_model_ir`; policy-only sidecars (both cost fields `None`) collapse to `cost_ceiling = None`. Diagnostic `E027 budget exceeded` registered in `rocky-compiler/src/diagnostic.rs` with `Diagnostic::budget_exceeded(model, projected_usd, ceiling_usd)` and `budget_exceeded_bytes(model, projected_bytes, ceiling_bytes)` builders.
- **`rocky-catalog-core`: `CatalogClient::table_stats` trait method + Iceberg snapshot-summary implementation** ([#600](https://github.com/rocky-data/rocky/pull/600)). New async trait method `table_stats(&self, &TableRef) -> CatalogResult<TableStats>` with `TableStats { row_count: Option<u64>, total_bytes: Option<u64>, file_count: Option<u64> }`. The Iceberg impl in `rocky-iceberg/src/catalog_client.rs` parses snapshot summary from the existing `load_table` REST response — no new round-trip. The Unity REST impl in `rocky-databricks/src/unity_catalog_client.rs` returns `UnsupportedOperation`: Unity catalog REST has no stats surface by design; Databricks bytes/rows route via `DESCRIBE DETAIL` SQL through the connector (see #605 and #606).
- **`rocky-compiler`: wire `E027 budget-exceeded` diagnostics into `rocky compile`** ([#605](https://github.com/rocky-data/rocky/pull/605)). New `cost_check::check_cost_ceilings(models, estimates) -> Vec<Diagnostic>` module compares propagated cost estimates against each model's `[budget]` ceiling and emits E027 for breaches in both USD and bytes-scanned dimensions. Wired into `rocky compile` so the check runs unconditionally and respects the `--model` filter; `result.has_errors` is set on any breach. Compile stays **offline-pure** — estimates are stub values (10k rows × 256 bytes per leaf) here; real catalog stats land at `rocky plan` time via #606. Companion `DatabricksConnector::describe_detail_stats(catalog, schema, table)` on `rocky-databricks` parses `sizeInBytes` from `DESCRIBE DETAIL` with graceful degradation on `TABLE_OR_VIEW_NOT_FOUND` — no `ANALYZE TABLE` prerequisite required, live-verified against the Databricks sandbox.
- **`rocky-cli`: wire real catalog stats into per-model cost ceiling at plan time** ([#606](https://github.com/rocky-data/rocky/pull/606)). `rocky plan` now grounds the `[budget]` ceiling check in actual catalog data instead of compile-time stubs. Per-leaf dispatch: Iceberg leaves → `CatalogClient::table_stats` (the trait method shipped in #600); Databricks leaves → `DatabricksConnector::describe_detail_stats` (the SQL stats path shipped in #605); Unity REST `UnsupportedOperation`, missing tables, network errors all degrade silently so missing data never blocks `plan`. Per-model `on_breach` policy honoured via new `check_cost_ceilings_plan` — `"warn"` (default) emits Warning-severity E027 (advisory only); `"error"` emits Error-severity E027 (sets `PlanOutput::has_budget_errors`, blocking signal for CI/orchestration). New `Diagnostic::budget_exceeded_warn` + `budget_exceeded_bytes_warn` builders mirror the existing Error-severity ones. Registry change: two independent `IcebergCatalogClient` instances per Iceberg adapter — one backs the existing `IcebergDiscoveryAdapter`, the other is exposed for plan-time `table_stats` lookups (cheap; avoids a Clone refactor on the discovery adapter). New `PlanOutput.budget_diagnostics` + `has_budget_errors` fields cascade through `schemas/plan.schema.json`, dagster Pydantic `plan_schema.py`, and the VS Code TypeScript `plan.ts`.
- **`rocky-core`: `OUTPUT_ARTIFACTS` state table for the content-addressed write ledger** ([#604](https://github.com/rocky-data/rocky/pull/604)). New `ArtifactRecord` struct + `record_artifact` / `list_artifacts_by_hash` / `list_artifacts_for_run` / `count_artifacts` methods on `StateStore`. Persists the `blake3_hash` returned by `rocky_iceberg::uniform_writer::WriteResult` (previously logged via `tracing::info!` and dropped on process exit) so a future VACUUM refcount sweep can answer "which runs reference hash X?" without re-reading the Delta log. Wired at the existing `execute_content_addressed_model` success branch — best-effort write that logs + continues on failure (the run is already durable in the Delta log; a missing ledger row is recoverable). State schema bumps v6 → v7; the upgrade is purely additive (v6 databases auto-create the empty table on next open and stamp themselves v7). Replicates across backends by default — recipe-canonical artifact metadata is not machine-local. Internal `rocky-core` symbol only; no JSON output schema cascade. **Known gap:** the partitioned write loop in `execute_content_addressed_model` only returns the *last* group's hash, so partitioned tables currently record one row per run instead of one row per partition group — TODO at the call site, picked up by the eventual Phase 6 refcount PR.

### Fixed

- **`rocky-fivetran`: emit `PipelineEvent` to global event bus at stampede + circuit-breaker sites** ([#601](https://github.com/rocky-data/rocky/pull/601)). 7 sites that called `record_span_event(...)` for OTel were not calling `global_event_bus().emit(PipelineEvent::new(...))` — meaning observability span events fired but the parallel pipeline-event surface (which Dagster, hooks, and downstream consumers depend on) stayed silent. Three of the 7 were `circuit_state_change` sites added in PR #589 (v1.38.0 Fivetran circuit breaker); `git show 05f0bb66` confirmed the omission was consistent across all three, not intentional. The remaining 4 were equivalent stampede-protection emit sites. No behavior change beyond making the parallel emission complete.

## [1.39.1] — 2026-05-20

Patch release fixing a packaging regression that made the v1.38.0 / v1.39.0 Fivetran adapter-resilience layers unreachable from the prebuilt CLI.

### Fixed

- **`rocky-cli`: enable the `valkey` feature on the `rocky-fivetran` + `rocky-cache` dep edges** ([bac858a](https://github.com/rocky-data/rocky/commit/bac858a)). The shipped `rocky` binary depended on `rocky-fivetran` and `rocky-cache` without enabling either crate's optional `valkey` cargo feature, and the release workflow builds with `cargo build --release --bin rocky` (no `--features`). Every prebuilt binary on the GitHub release page therefore fell through to the `#[cfg(not(feature = "valkey"))]` arm in the four `[adapter.fivetran.*]` resilience backends (`cache` / `ratelimit` / `stampede` / `circuit_breaker`) and refused to construct them whenever `backend` selected `tiered` or `valkey` — none of those backends were reachable from a stock release. Two-line fix on the `rocky-cli` dep edges; every future release picks it up with no workflow change. A new `release-build-smoke` job in `engine-ci.yml` mirrors the release-workflow flags and greps for the specific construction error so the regression can't reappear silently; `cargo test --all-features` couldn't catch it because workspace-level `--all-features` force-enables every feature regardless of how dep edges are wired.

## [1.39.0] — 2026-05-19

Three small but high-leverage changes this cut. **`rocky import-dbt` dbt-1.7+ catch-up.** dbt 1.7 renamed the column-level test YAML key from `tests:` to `data_tests:`; the importer's deserializer only knew the legacy spelling, so any `schema.yml` written against modern dbt round-tripped to zero converted column tests. A serde alias on `RawColumn.tests` + `RawSourceColumn.tests` restores the canonical-four (`unique` / `not_null` / `accepted_values` / `relationships`) conversion path for those projects without changing existing behavior. **`rocky import-dbt` unit-test bridge.** The importer now walks `manifest.unit_tests` and emits each entry as a Rocky `[[test]]` block in the matching model's sidecar TOML — closing the gap between *Rocky has dbt-style unit tests* (true: `rocky-core/unit_test.rs`) and *Rocky imports dbt unit tests* (false until now). CSV fixtures, `overrides:`, and non-`dict` `expect.format` are deferred to follow-ups. **Fivetran tolerance for connectors with no schema config.** `FivetranClient::fetch_envelope` previously errored when a connector lacked a `schema` config block on the API side; it now logs and continues, so a single misconfigured connector no longer blocks the rest of the envelope.

### Added

- **`rocky import-dbt`: import dbt unit tests from manifest** ([#594](https://github.com/rocky-data/rocky/pull/594)). New `DbtManifestUnitTest` / `DbtUnitTestGiven` / `DbtUnitTestExpect` types parsed from `manifest.unit_tests` in `engine/crates/rocky-compiler/src/import/dbt_manifest.rs`. New `apply_dbt_unit_tests` converter in `dbt.rs`, invoked automatically from `import_from_manifest`; `strip_ref_wrapper` peels `ref('foo')` / `source('a','b')` / bare / quoted forms down to a bare ref. Each converted unit test lands in the matching model's sidecar TOML as a `[[test]]` block via `emit.rs`. New `ImportResult` counters `unit_tests_found / unit_tests_converted / unit_tests_skipped` (also surfaced on `ImportDbtOutput` and in the generated `MIGRATION-NOTES.md`) and two new warning categories: `OrphanUnitTest` (unit test targets a model that wasn't imported) and `UnsupportedUnitTestFormat` (`expect.format = "csv" | "sql"`, fixture references, or any other format Rocky's `UnitTestDef` doesn't yet model). Cascades through `just codegen` to `schemas/import_dbt.schema.json`, dagster Pydantic, and VS Code TypeScript. Note: emitted `[[test]]` blocks deserialize via `UnitTestDef` but aren't yet wired into the runtime test runner — running them via `rocky test` is a follow-up.

### Fixed

- **`rocky import-dbt`: accept `data_tests:` as alias for `tests:` in dbt YAML** ([#593](https://github.com/rocky-data/rocky/pull/593)). `RawColumn.tests` in `engine/crates/rocky-compiler/src/import/dbt_tests.rs` and `RawSourceColumn.tests` in `dbt_sources.rs` gain `#[serde(default, alias = "data_tests")]`. dbt 1.7+ renamed the column-level test key (the legacy `tests:` form is still accepted by dbt itself); before this fix, any schema.yml using the new key produced `tests_found: 0` even when `dbt compile` reported thousands of column-level data tests. Behavior with both keys present matches dbt's own validation — serde-yaml 0.9 errors on the duplicate, pinned by regression test. Pure structural fix; no JSON output schemas touched, no codegen cascade.
- **`rocky-snowflake`: classify Snowflake statement spans** ([#540](https://github.com/rocky-data/rocky/pull/540)). OTLP span attributes on Snowflake SQL execution now distinguish statement kinds (DDL vs DML vs query) so downstream observability tooling can filter and aggregate consistently with the Databricks / DuckDB / BigQuery adapters.
- **`rocky-fivetran`: tolerate connectors with no schema config in `fetch_envelope`** ([#592](https://github.com/rocky-data/rocky/pull/592)). A single connector missing its `schema` block on the Fivetran API side no longer aborts the whole envelope build; the missing config is logged and the connector is included with an empty schema map.

## [1.38.0] — 2026-05-19

Three headlines this cut. **Fivetran adapter at scale.** Layered on top of v1.37.0's FR-A pluggable cache backends + FR-B Phase 1 per-host rate-limit budget, this cut ships three more orthogonal mitigations: distributed cache stampede protection (10 cold-start pods → 1 API call per TTL window), cross-pod rate-limit coordination via Valkey (FR-B Phase 2), and a per-account circuit breaker so a Fivetran outage doesn't pound the API for 90 seconds. Plus the canonical `FivetranStateEnvelope` contract that `rocky discover --emit-fivetran-state-to <PATH>` writes (FR-C) — the orchestrator hook that lets a downstream consumer share Rocky's view of a destination without re-fetching it. **View / MaterializedView / DynamicTable strategies end-to-end.** Three new `StrategyConfig` variants are declarable from a model sidecar TOML and dispatch correctly through both the replication and transformation runners; the `SqlDialect` trait grows `view_ddl` / `materialized_view_ddl` / `dynamic_table_ddl` with per-warehouse coverage across DuckDB / Databricks / Snowflake / BigQuery / Trino. The replication runner's silent "MV → CTAS" bug is fixed in the process. **`rocky import-dbt` v2** maps `view` / `materialized_view` / `microbatch` correctly onto the new strategy variants, fixes a parse bug that treated `incremental_strategy='merge'` as a timestamp column name, and emits structured warnings for un-translated dbt-databricks config (`databricks_tags` / `post_hook` / `on_schema_change`). **Plus** five POC infrastructure improvements: a new smoke step that parse-validates credential-gated POCs with mock env vars (the gap that hid a silent V033 cluster), the V033 sweep itself across 5 governance/adapter POCs, and an env-var alignment fix on the schema-patterns POC.

### Added

- **Canonical `FivetranStateEnvelope` contract** ([#583](https://github.com/rocky-data/rocky/pull/583); FR-C). New `engine/crates/rocky-fivetran/src/envelope.rs` exposes a `JsonSchema`-deriving struct (`version: EnvelopeVersion::V1_0`, `fetched_at: DateTime<Utc>`, `destination`, sorted-by-id `connectors: Vec<FivetranConnectorSummary>`, `schemas: BTreeMap<String, FivetranSchemaConfig>`) plus an `envelope_hash(env) -> [u8; 32]` helper that excludes `fetched_at` for stable cross-run hashing. Schema cascades through `just codegen` to `schemas/rocky_fivetran_state.schema.json`, dagster Pydantic, and VS Code TypeScript. `FivetranClient` gains `fetch_envelope(refresh: bool)` with in-process memoization (Mutex-based) so a single CLI run fans out one HTTP burst instead of three (destination + connectors + per-connector schemas).
- **`rocky discover --emit-fivetran-state-to <PATH>`** ([#583](https://github.com/rocky-data/rocky/pull/583); FR-C). Writes the canonical envelope to `<PATH>` via atomic tmp+rename. Idempotent: a sibling `<PATH>.blake3` sentinel file holds the previous envelope's hash; same upstream state → no write, mtime stable, downstream `stat(2)` watchers don't fire. Multi-destination configs write to `<PATH>.<account_hash>.<destination_id>.json` per envelope (path builder strips `.json` suffix from the base before injection). `account_hash` reuses `rocky_fivetran::ratelimit::hash_account_id` for cross-feature consistency.
- **View / MaterializedView / DynamicTable strategies end-to-end** ([#585](https://github.com/rocky-data/rocky/pull/585)). New `MaterializationStrategy::View` IR variant and three new `StrategyConfig` user-facing variants (`view`, `materialized_view`, `dynamic_table { target_lag }`); `to_model_ir` maps each to the right IR variant. The `SqlDialect` trait grows `view_ddl` / `materialized_view_ddl` / `dynamic_table_ddl` methods with default impls (fail-loud-unsupported) and per-dialect overrides. Coverage matrix: DuckDB (`View`); Databricks (`View` + `MaterializedView`); Snowflake (`View` + `MaterializedView` + `DynamicTable` — warehouse threaded via new `WarehouseAdapter::warehouse_name()`); BigQuery (`View` + `MaterializedView`); Trino (`View`). The replication runner's "MV silently treated as full refresh" bug at `commands/run.rs:4861` is fixed — now dispatches to the dialect generators. New DuckDB POC at `examples/playground/pocs/06-developer-experience/18-view-strategy/` smokes the `view` strategy end-to-end. Cascades through `just codegen` to schemas + dagster Pydantic + VS Code TypeScript.
- **Fivetran distributed cache stampede protection** ([#589](https://github.com/rocky-data/rocky/pull/589); resilience layer 1). New `StampedeLock` trait in `engine/crates/rocky-fivetran/src/stampede.rs` plus `NoLock` (default) and `ValkeyLock` (gated behind `valkey` feature). The Valkey backend uses `SET <key>:lock <unique_id> NX EX <ttl_seconds>` to acquire, and `LockGuard::drop` runs an `EVAL` Lua script that does check-and-DEL (only deletes if the value still matches our `unique_id` — guards against deleting another holder's lock after TTL expiry). Wired into `FivetranClient::fetch_envelope`: leader fetches + writes the cache + releases lock; followers poll the cache key with exponential backoff (100ms → 2s, 30s total cap) until present, then return. Converts the cold-start herd from N API calls → 1 API call per TTL window. Layered test (`layered_concurrent_callers_collapse_to_one_http_call`) pins the invariant: 10 concurrent `fetch_envelope` calls produce exactly 1 wiremock-asserted HTTP call, 1 cache write, and 1 stampede acquisition. New config block: `[adapter.<name>.stampede]` with `backend = "none" | "valkey"`, `valkey_url`, `lock_ttl_seconds = 60`, `poll_timeout_seconds = 30`.
- **Fivetran cross-pod rate-limit budget** ([#589](https://github.com/rocky-data/rocky/pull/589); FR-B Phase 2, resilience layer 2). Refactored `engine/crates/rocky-fivetran/src/ratelimit.rs` to expose a `RatelimitBudget` trait; the v1.37.0 file-backed `${TMPDIR}/rocky-fivetran-ratelimit/<account_hash>.json` implementation becomes the `FileBudget` impl. New `ValkeyBudget` (in `ratelimit_valkey.rs`, behind the `valkey` feature) stores the `wake_at_epoch_ms` at `fivetran-ratelimit:<account_hash>` using an `EVAL` Lua script for atomic max-merge so multiple concurrent pods setting wake_at converge correctly. New config block: `[adapter.<name>.ratelimit]` with `backend = "file" | "valkey"` (default `"file"` for back-compat), `valkey_url`. Behaviour parity with FR-B Phase 1 — just shared storage. One 429 throttles N pods coordinated instead of N independent backoffs.
- **Fivetran per-account circuit breaker** ([#589](https://github.com/rocky-data/rocky/pull/589); resilience layer 3). New `FivetranCircuitBreaker` trait + `AlwaysClosed` (default) + `ValkeyCircuit` (behind `valkey` feature) in `engine/crates/rocky-fivetran/src/circuit_breaker.rs`. State machine: `Closed` (normal) → counts consecutive remote failures within `window_seconds`; on reaching `failure_threshold` → `Open` (refuse traffic with `FivetranError::CircuitOpen`, no HTTP); after `cooldown_seconds` → `HalfOpen` (single probe); probe success → `Closed`; probe failure → `Open` with cooldown exponentially extended (capped at `cooldown_max_seconds`). Wired into `FivetranClient::fetch_envelope` — state check happens before the HTTP request, so an Open circuit fails fast instead of waiting on the request timeout. Only remote failures (5xx, 429 storms, connection-refused) trip the breaker; local errors (parse, serialization) don't count. Renamed from the original `CircuitBreaker` to `FivetranCircuitBreaker` to avoid colliding with the existing `rocky_core::circuit_breaker::CircuitBreaker` warehouse-retry helper. OTLP span event `fivetran.circuit_state_change` (from_state, to_state, reason). New config block: `[adapter.<name>.circuit_breaker]` with `backend = "none" | "valkey"`, `valkey_url`, `failure_threshold = 5`, `window_seconds = 60`, `cooldown_seconds = 300`, `cooldown_max_seconds = 3600`.
- **`[adapter.<name>.retry]` config knob** ([#589](https://github.com/rocky-data/rocky/pull/589)). Threading `RetryConfig` from `AdapterConfig.retry` into `FivetranClient::with_retry(...)` at adapter init was already wired through serde; this cut adds a regression test pinning the wiring. Lets operators override the default `max_retries = 3` / `initial_backoff_ms = 1000` / `max_backoff_ms = 30000` from `rocky.toml` without recompiling. Useful in conjunction with the new cross-pod budget — bigger retry windows are safe now that wake_at coordination prevents burst-fired backoff stomp.
- **`rocky import-dbt` v2 — correct dbt materialization mapping** ([#590](https://github.com/rocky-data/rocky/pull/590); Wave 2). `view` → `StrategyConfig::View`; `materialized_view` → `StrategyConfig::MaterializedView`; `microbatch` (+ `event_time`, `batch_size`) → `StrategyConfig::Microbatch`; `incremental` + `incremental_strategy='merge'` + `unique_key` → `StrategyConfig::Merge` (fixes the parse bug that previously read `'merge'` as a timestamp column name); `incremental` + `incremental_strategy='delete+insert'` → `StrategyConfig::DeleteInsert`; `incremental` + `incremental_strategy='insert_overwrite'` → `StrategyConfig::DeleteInsert` + warning suggesting `time_interval`. Regression test `test_incremental_strategy_merge_regression` pins the merge mapping. `ephemeral` stays at `FullRefresh` + warning (Rocky has no equivalent for dbt's CTE-inlining indirection).
- **`rocky import-dbt` v2 — structured warnings for un-translated config** ([#590](https://github.com/rocky-data/rocky/pull/590)). New `ImportDbtStructuredWarning` enum (cascades through `just codegen` to dagster Pydantic + VS Code TypeScript) variants: `UnsupportedMaterialization`, `DroppedDatabricksTags { tags: BTreeMap<String, String> }`, `DroppedHook { hook_kind, sql }`, `DroppedOnSchemaChange { dbt_value, rocky_equivalent: &'static str }`, `UnresolvableMacro { macro_name, first_call_site_line }`, plus `MicrobatchMissingEventTime`. Each variant carries the actual content the user needs to migrate — e.g. the actual `databricks_tags` key/value pairs to paste into Rocky's `[classification]` block. Existing string-warning surface stays for back-compat. Side fixes that landed in the same PR: `pre_hook` / `post_hook` accept string-or-list (dbt manifest variation); the dashed `pre-hook` / `post-hook` variants also parse; `unique_key` accepts string-or-list at the regex path.
- **POC smoke matrix parse-validate step** ([#587](https://github.com/rocky-data/rocky/pull/587)). New sibling script `examples/playground/scripts/run-all-parse-validate.sh` (112 LOC, POSIX/Bash 3.2 portable) walks every credential-gated POC, injects mock env vars for every `${VAR}` reference in its `rocky.toml`, and runs `rocky validate`. Wired as a third step in the `poc-smoke` job in `engine-weekly.yml` with `continue-on-error: true` (warning-only initially). This closes the gap that hid the V033 cluster + the silent parse failures on `materialized_view` / `dynamic_table` declarations.
- **Pluggable persistent state cache for the Fivetran adapter** ([#584](https://github.com/rocky-data/rocky/pull/584); FR-A). New `FivetranStateCache` trait in `rocky-fivetran::state_cache` plus five concrete backends:
  - `NoCache` — sentinel; default when `[adapter.<name>.cache]` is absent.
  - `FileCache` — `<root>/<account_hash>/<destination_id>.json` with atomic tmp+rename + hash-dedupe (mtime stays stable on no-op rewrites).
  - `ObjectStoreCache` — S3 / GCS / Azure / `file://` via the `object_store` crate. Credentials resolved through the SDK default chain (`AWS_*` / `GOOGLE_APPLICATION_CREDENTIALS` / IAM role / etc.) — no Rocky-specific credential surface. Single-part PUT only (max 5 MB envelope, asserted at write time); hash-dedupe via HEAD-then-conditional-PUT comparing `MD5(new_bytes)` against the existing object's ETag (strips the `"…"` quoting + optional `W/` weak-ETag prefix).
  - `ValkeyCache` — Redis / Valkey, gated behind a new `valkey` Cargo feature on `rocky-fivetran`. Keys namespaced under `fivetran-state:`; every `SET` carries the configured TTL (default 600s). Hash-dedupe via `GET`-before-`SET`.
  - `TieredCache` — composes a primary (typically Valkey) + secondary (typically `ObjectStoreCache`). Reads try primary first; on miss they fall through to secondary and write-back to primary. Writes go to both layers (secondary first, durable; primary best-effort).
- **`[adapter.<name>.cache]` config block** under any `type = "fivetran"` adapter ([#584](https://github.com/rocky-data/rocky/pull/584); FR-A). Fields: `backend` (one of `"none"` / `"file"` / `"object_store"` / `"valkey"` / `"tiered"`, defaults to `"none"`), `file_root`, `object_store_url`, `valkey_url`, `valkey_ttl_seconds`. Cross-field validation runs at `load_rocky_config` time (e.g. `backend = "tiered"` requires both `object_store_url` and `valkey_url`) and surfaces missing fields as a typed `ConfigError::FivetranCacheMissingField` so the operator gets a clear error before the first cache touch. All string fields support the existing `${VAR}` / `${VAR:-default}` env-var substitution.
- **`rocky discover --no-cache`** ([#584](https://github.com/rocky-data/rocky/pull/584); FR-A) — skip the persistent cache read on the current invocation and force a fresh API fetch. The fresh envelope is still written back to the cache so the next invocation sees the up-to-date data. Useful after rolling a Fivetran credential or when an operator suspects the cache is stale.
- **OTLP span events for cache decisions** ([#584](https://github.com/rocky-data/rocky/pull/584); FR-A). `fivetran.cache_hit` / `fivetran.cache_miss` / `fivetran.cache_write` / `fivetran.cache_write_skipped` / `fivetran.cache_write_failed`. Mirrors the existing `fivetran.rate_limit_observed` emission pattern from FR-B Phase 1. All events also published on the in-process pipeline event bus so sensor / orchestrator code that subscribes to `PipelineEvent` sees the same signal as the OTLP exporter.

### Notes

- **Volume-reduction caveat.** The state-cache layer dedupes correctness — it prevents redundant writes when the bytes haven't changed and serves repeat reads without hitting the API. The cold-start herd reduction (N concurrent processes converging on a single API fetch) depends on the per-host rate-limit budget shipped in engine-v1.37.0 (FR-B) serializing those processes. Cache + rate-limit together are what drive the steady-state reduction; either alone is insufficient.
- **FR-spec deviation.** The original FR proposed `[fivetran.cache]` as the TOML location. We nest under `[adapter.<name>.cache]` to align with Rocky's existing per-adapter config patterns — a project can have multiple Fivetran adapters with distinct cache configs.
- **`--no-cache` is `rocky discover`-only.** The flag is not yet exposed on `rocky run`; demand-gate the extension.

## [1.37.0] — 2026-05-19

Three headlines this cut. **Cluster 3 C closes — the v1 plan-store reader is removed** (C-7). The legacy inline-SQL plan-store envelope for `rocky compact` / `rocky archive` is no longer loadable, and the `[plan_store]` config block is dropped from `rocky.toml`. **Catalog-first foundation expands.** `IcebergCatalogClientAdapter` gains the four lifecycle methods deferred from v1.36.0, and a new `DatabricksCatalogClient` ships in `rocky-databricks` — the first concrete `CatalogClient` impl for a SQL warehouse, routing Unity Catalog ops through Unity REST (7 of 10 trait methods REST-backed; live-verified against the Databricks sandbox via OAuth M2M). **Replication operational hardening.** Three independent improvements: (1) the watermark filter migrates from a target-side correlated subquery to a source-side literal across all five dialects — watermarks now reflect what's been processed *from* source rather than what's *in* target; (2) per-source `[[table_overrides]]` with field-level most-specific-match-wins resolution; (3) filtered runs (`rocky run --filter id=X` / `--filter client=Y`) now tolerate out-of-scope source drift — an unrelated connector disappearing between a bulk run and a per-source-filtered follow-up no longer aborts, it logs a WARN and proceeds. Plus: Fivetran adapter honors `Retry-After` headers on 429 / 503 and gains a per-host shared rate-limit budget (FR-B Phase 1); `LineageOutput` nodes gain `target_schema` + `source_id` and `LineageColumnDef` gains `data_type`; `sqlparser` 0.62 lands with a source fix.

### Added

- **`DatabricksCatalogClient` — first concrete `CatalogClient` impl for a SQL warehouse** ([#578](https://github.com/rocky-data/rocky/pull/578)). New `rocky-databricks::unity_catalog_client` module wraps Unity Catalog REST. Trait method coverage: `describe_table` (`GET /api/2.1/unity-catalog/tables/{full_name}`), `list_tables` (`GET /api/2.1/unity-catalog/tables?catalog_name=…&schema_name=…`, paginated), `create_table` (`POST /tables`, managed Delta), `drop_table` (`DELETE /tables/{full_name}`), `get_grants` (`GET /permissions/table/{full_name}`), `apply_grant` (`PATCH /permissions/table/{full_name}` with `add[]`), `revoke_grant` (`PATCH /permissions/table/{full_name}` with `remove[]`). `tag_table` / `commit_transaction` / `list_branches` return `UnsupportedOperation` — Unity has no table-tag REST endpoint (SQL-only via `ALTER TABLE SET TAGS`), no generic multi-table commit REST surface, and no Iceberg-style branch model. HTTP-status → `CatalogError` mapping matches the `rocky-iceberg` adapter (401 → `AuthFailed`, 403 → `PermissionDenied`, 404 → `TableNotFound`, 409 → `CommitConflict`, 429 + 5xx → `Transport`). 34 unit tests + 18 wiremock tests + 2 `#[ignore]`-gated live tests covering `list_tables` / `describe_table` against a real Databricks Unity sandbox via OAuth M2M. The existing SQL-driven `CatalogManager` is untouched — `DatabricksCatalogClient` is purely additive; pipelines that don't construct one keep working unchanged.
- **`IcebergCatalogClientAdapter` lifecycle methods** (commit `b315e9af`). The four `UnsupportedOperation` stubs that shipped in v1.36.0 are now functional: `create_table` (`POST /v1/namespaces/{ns}/tables` with the `CreateTableRequest` body containing the catalog-agnostic `TableSchema` mapped onto Iceberg-shape field types), `drop_table` (`DELETE /v1/namespaces/{ns}/tables/{name}`), `commit_transaction` (`POST /v1/transactions/commit` with the multi-table commit envelope; falls back to per-table `POST /v1/namespaces/{ns}/tables/{name}/metrics` when the catalog declines the multi-table form), `list_branches` (reads `SnapshotReference.kind = "branch"` from the table's metadata; honors `BranchRef::main_only` to short-circuit). Governance methods (`tag_table` / `get_grants` / `apply_grant` / `revoke_grant`) keep returning `UnsupportedOperation` — Iceberg REST has no governance surface by spec.
- **Replication `[[table_overrides]]` with most-specific-match-wins resolution** (PR-B3, commit `56199841`). New optional `[[table_overrides]]` array on `ReplicationPipelineConfig` lets per-table settings override the pipeline-level defaults. Each entry carries `match.table_pattern` / `match.schema_pattern` / `match.connector_pattern` (glob, all optional — an empty match block matches every table); per-field overrides for `strategy`, `merge_keys`, `merge_keys_fallback`, `timestamp_column`, `target_table`, `target_schema`. Resolution is per-field most-specific-match-wins — for each field, the override with the most specific match block (most populated pattern fields) wins; ties resolve by config order (later wins). Fail-loud on glob compilation errors at `load_rocky_config` time. Cascades through `just codegen` to dagster Pydantic + VS Code TypeScript.
- **`LineageOutput` enrichment.** Lineage nodes gain `target_schema` and `source_id`; `LineageColumnDef` gains `data_type` (commits `cbfaed43`, `d959f924`). Additive JSON-shape changes; downstream consumers that parse `rocky lineage --output json` via the regenerated Pydantic / TypeScript bindings get the new fields without further code changes.
- **`rocky-fivetran` honors `Retry-After` on 429 and 503 responses**. The client now parses the `Retry-After` response header per RFC 9110 §10.2.3 — accepts both integer-seconds and HTTP-date forms (RFC 7231 §7.1.1.1) — and sleeps `max(retry_after, current_backoff)` before the retry attempt. When the header is absent the client falls back to the existing fixed-backoff schedule, so prior behaviour is preserved. Applies to 503 Service Unavailable in addition to 429, matching the RFC's `Retry-After` semantics for both status codes.
- **`rocky-fivetran` per-host shared rate-limit budget** for concurrent rocky-cli processes. When N rocky-cli processes share a Fivetran org rate-limit budget on the same host, the first process to hit a `Retry-After` records the `wake_at` window into `${TMPDIR}/rocky-fivetran-ratelimit/<account_hash>.json` (advisory-locked via `fs4`, written tmp+rename for crash safety) and every concurrent process consults the same file before issuing a fresh request. The file is account-scoped — the SHA-256-truncated hash of the API key — so distinct Fivetran orgs on the same host don't share budget. I/O errors in the coordination path are logged and swallowed (fail-open: a glitch in the shared file must never block live traffic).
- **OTLP span event `fivetran.rate_limit_observed`** is emitted whenever the client honors a `Retry-After` window or falls back to fixed backoff after a 429/503. Carries `retry_after_ms` (the actual sleep the client is about to perform), `header_retry_after_ms` (the unmodified value parsed from the upstream header — present only when `source = "header"`, so dashboards can distinguish "upstream said 100ms but we slept 1s because backoff dominated" from straight header compliance), `source` (`"header"` when the upstream signalled, `"fallback"` otherwise), `account_id` (hashed — never the raw API key), and `status` (`429` or `503`). Lets dashboards quantify how often the org is throttled and whether the upstream is signalling backoff cleanly.

### Removed

- **Drop v1 plan-store reader (Cluster 3 C — C-7).** The legacy inline-SQL plan-store envelope for `rocky compact` / `rocky archive` is no longer loadable. Plans written by Rocky < engine-v1.35.0 (the v1 default era) return a clear migration error at `rocky apply <plan-id>` pointing at the recipe — re-run `rocky compact` / `rocky archive` to write a fresh v2 plan. `Run` / `Replication` / `Promote` plans are untouched (they never carried the inline-SQL envelope). The `[plan_store]` config block (`format = "v1" | "v2"`) is also dropped — `RockyConfig`'s `deny_unknown_fields` now rejects projects still carrying the block. See [`concepts/plan-store-v1-to-v2`](https://docs.rocky-data.com/concepts/plan-store-v1-to-v2) for the migration guide. Closes the C-5 → C-6 → C-7 soft-cycle that introduced the typed-IR v2 envelope.

### Changed

- **`rocky apply` tolerates out-of-scope source drift on filtered replication plans.** When the persisted plan was created with `--filter id=<X>` or `--filter client=<X>` and the symmetric diff between the persisted source-state snapshot and a fresh live discover is *disjoint from the filter scope*, `rocky apply` now demotes the previous hard abort to a `WARN` log (`"source state has drifted outside filter scope (filter=id=X, drifted=[A, B]); continuing"`) and proceeds. When the diff *intersects* the filter scope — the filtered source itself was added, removed, or schema-changed — `rocky apply` still bails with the existing drift error (mentioning the in-scope connectors). Unfiltered applies keep today's strict semantics unchanged. Fixes the failure pattern where a bulk replication run completes, an upstream connector drops between invocations, and a follow-up per-source filtered apply aborts despite the filter target being unaffected. `--filter client=<X>` correctly expands to the union of connector ids that match the client component across both the persisted and live snapshots, so a removed in-scope connector is still classified as in-scope drift. Log-only surfacing — no CLI JSON output schema change.
- **Replication watermark filter is now source-side across all 5 dialect adapters** (`rocky-databricks`, `rocky-snowflake`, `rocky-bigquery`, `rocky-duckdb`, `rocky-trino`). The dialect's `watermark_where` no longer emits a correlated `SELECT MAX({ts}) FROM target` subquery — instead it substitutes the previous run's max source timestamp as a literal (`WHERE {ts} > TIMESTAMP '...'`). The runner reads the prior watermark from the state store before SQL generation, threads it into `sql_gen::generate_select_sql` / `generate_insert_sql`, and (after a successful execute on `Incremental` / `Microbatch` strategies) issues a fresh `SELECT MAX({ts}) FROM source` to record this run's watermark — replacing the previous `Utc::now()` semantics. The watermark now reflects what's been processed *from* source rather than what's *in* target. For monotonic sources the two values coincide; the difference surfaces for late-arriving data (target-side hid this) and for strategies that collapse N source rows into 1 target row (target-side undercounted). The trait signature changes to `watermark_where(timestamp_col: &str, last_watermark: Option<&DateTime<Utc>>)` — third-party adapters built against `rocky-adapter-sdk` need a one-line signature update. The `MaterializationMetadata.watermark` field surfaced via `rocky run --output json` now reflects the source-side max — dagster consumers that previously read it as "when did the run complete" wall-clock will see a different value (real data timestamps for incremental tables, `Utc::now()` for non-incremental strategies). State-store schema unchanged: `WatermarkState::last_value` keeps its `DateTime<Utc>` type, only the meaning of the stored value shifts. Existing watermarks (which previously stored `Utc::now()`) are valid lower bounds for the first run after upgrade — monotonic sources stay correct; non-monotonic sources gain the conceptual fix going forward.
- **`sqlparser` bumped 0.61.0 → 0.62.0** ([#569](https://github.com/rocky-data/rocky/pull/569)). New `SelectItem::ExprWithAliases` variant handled at the two exhaustive-match sites in `rocky-compiler::typecheck` and `rocky-sql::lineage` — treated symmetrically to the existing `SelectItem::ExprWithAlias` (singular) branch.

### Migration

- **Remove the `[plan_store]` block from `rocky.toml`** before upgrading. Projects that previously set `[plan_store] format = "v2"` should delete the block entirely — v2 is the only format now and the block is parsed as an unknown field by `deny_unknown_fields`. Projects that set `[plan_store] format = "v1"` and still need the legacy on-disk shape must regenerate plans (no v1-format upgrade path exists past this release).
- **Already-persisted v1 plans for `rocky compact` / `rocky archive`** become unloadable. Re-run `rocky compact` / `rocky archive` to regenerate fresh v2 plans. See `docs/concepts/plan-store-v1-to-v2.md` for the recipe.
- **Third-party adapters built against `rocky-adapter-sdk`** need to update `watermark_where` signature: the second argument becomes `last_watermark: Option<&DateTime<Utc>>` (was no argument). One-line change per dialect.

### Documentation

- **Per-table error containment + `failure_kind` consumption guide** (commit `1cb10252`). New section in the error-handling docs documenting that engine-side per-table errors don't fail the run as a whole, how the `failure_kind` discriminator from v1.34.0 lands on `RunOutput.errors[*]`, and the recommended downstream consumption pattern for Dagster / dbt-style consumers.

### Notes

- **Catalog-first foundation: no migration to a `CatalogClient`-routed pipeline yet.** The new `DatabricksCatalogClient` and the `IcebergCatalogClientAdapter` lifecycle methods are purely additive. No `WarehouseAdapter` or `CatalogManager` call site has been migrated to delegate to `CatalogClient` yet — that's a separate PR that ships when the trait surface has soaked.
- **Fivetran cross-pod rate-limit budget (per-cluster) is not in this release.** FR-B Phase 1 ships per-host coordination only — concurrent processes on different pods don't yet share state. Cross-pod coordination is part of FR-A (the pluggable cache backend), tracked separately.
- **`opentelemetry-otlp` 0.32 stays deferred.** The bump fails dep resolution because `tracing-opentelemetry 0.32.1` still pins `opentelemetry_sdk ^0.31`. The fix is merged on `tracing-opentelemetry`'s default branch but unreleased — the next cut will be `tracing-opentelemetry 0.33`. Rocky stays on `opentelemetry-otlp 0.31.1` until the chain lands on crates.io.

## [1.36.0] — 2026-05-18

Two headlines this cut. **The catalog-first foundation lands.** A new `rocky-catalog-core` crate defines the `CatalogClient` async trait — 10 methods (`list_tables`, `describe_table`, `create_table`, `drop_table`, `commit_transaction`, `list_branches`, `tag_table`, `get_grants`, `apply_grant`, `revoke_grant`) absorbing the convergent surface across Iceberg REST / Polaris / Unity / Nessie. The first concrete implementation ships for Iceberg REST inside `rocky-iceberg` (`IcebergCatalogClientAdapter`): `list_tables` routes to the existing client; `describe_table` adds a new `GET /v1/namespaces/{ns}/tables/{name}` call plus schema distillation onto the catalog-agnostic `TableSchema`. Lifecycle methods (`create_table` / `drop_table` / `commit_transaction` / `list_branches`) return `UnsupportedOperation` and are deferred to a follow-up that lands POST/DELETE infrastructure with full `TableMetadata` round-trips. Governance methods (`tag_table` / `get_grants` / `apply_grant` / `revoke_grant`) return `UnsupportedOperation` permanently — the Iceberg REST spec exposes no governance endpoints by design, so the gap is permanent rather than not-yet. Internal scaffolding; no user-facing CLI surface change across either PR. **Replication `strategy = "merge"` ships.** A third replication strategy lands alongside `full_refresh` and `incremental` — applies the watermarked delta via `MERGE INTO target USING (delta) ON merge_keys WHEN MATCHED UPDATE SET * WHEN NOT MATCHED INSERT *`, fixing the misbehaviour where Fivetran-style whole-table-resync sources accumulated duplicates under append-by-watermark `incremental` (up to ~49x after 24h). Reuses the `MaterializationStrategy::Merge` + `sql_gen::generate_merge_sql` + per-adapter `merge_into()` plumbing that already ships for transformation pipelines. Databricks / BigQuery / DuckDB work today; Snowflake replication+merge fails loud at SQL-gen time (`ColumnSelection::All` is rejected by Snowflake's `merge_into` — transformation users supply explicit `update_columns` in model TOML; replication has no equivalent surface yet) and is tracked as a follow-up. DuckDB native MERGE backs the local-test path — `rocky-duckdb::dialect::merge_into` now emits the standard MERGE statement DuckDB 0.10+ supports, replacing the previous stub error, and `AdapterCapabilities.merge` flips from `false` to `true` for the DuckDB adapter.

### Added

- **`rocky-catalog-core` crate + `CatalogClient` async trait** ([#558](https://github.com/rocky-data/rocky/pull/558)). New foundation crate exposing the trait surface that absorbs the convergent table-CRUD layer across the data-catalog ecosystem (Iceberg REST, Polaris, Unity OSS, Nessie, Snowflake Horizon). 10 methods: `list_tables`, `describe_table`, `create_table`, `drop_table`, `commit_transaction`, `list_branches`, `tag_table`, `get_grants`, `apply_grant`, `revoke_grant`. `CatalogError` via `thiserror` with `CatalogResult<T>` alias. Supporting value types: `TableRef`, `BranchRef` / `BranchKind`, `Grant`, `TableSchema` / `ColumnSchema`, `TableCommit`. Trait is `Send + Sync` + `#[async_trait]` and object-safe (`dyn CatalogClient`). Methods that are not universally supported return `CatalogError::UnsupportedOperation`, letting callers fall back to adapter-specific paths (typically SQL DDL) on that signal. An `InMemoryCatalogClient` test stub ships gated behind `cfg(test)` + the `testing` Cargo feature for downstream tests. No callers yet; no `WarehouseAdapter` / `GovernanceAdapter` trait changes; zero behaviour change for any current pipeline.
- **`IcebergCatalogClientAdapter` in `rocky-iceberg`** ([#560](https://github.com/rocky-data/rocky/pull/560)). First concrete `CatalogClient` implementation. Wraps the existing Iceberg REST client. `list_tables` routes to the existing `IcebergCatalogClient::list_tables`; `describe_table` adds a new `GET /v1/namespaces/{ns}/tables/{name}` call (`IcebergCatalogClient::load_table`) plus schema distillation onto the catalog-agnostic `TableSchema`. `From<IcebergError> for CatalogError` HTTP-status mapping: 401 → `AuthFailed`, 403 → `PermissionDenied`, 404 → `TableNotFound` (namespace-scoped call sites rewrite to `NamespaceNotFound`), 409 → `CommitConflict`, 429 + 5xx → `Transport`, unknown statuses → `InvalidResponse`, `IcebergError::UnexpectedResponse` → `InvalidResponse`. Lifecycle methods (`create_table` / `drop_table` / `commit_transaction` / `list_branches`) return `UnsupportedOperation` and are deferred to a follow-up. Governance methods (`tag_table` / `get_grants` / `apply_grant` / `revoke_grant`) return `UnsupportedOperation` permanently — Iceberg REST exposes no governance endpoints by spec. Eight new wiremock tests cover the adapter; existing `IcebergDiscoveryAdapter` callers are untouched.
- **Replication `strategy = "merge"`** ([#561](https://github.com/rocky-data/rocky/pull/561)). Third strategy on `ReplicationPipelineConfig` alongside `full_refresh` and `incremental`. New optional `merge_keys: Vec<String>` and `merge_keys_fallback: Vec<String>` fields on `ReplicationPipelineConfig`. `load_rocky_config` fails fast via the new `validate_replication_strategies` hook in `rocky-core/src/config.rs` when `strategy = "merge"` lacks both keys. New `build_replication_strategy(&ReplicationPipelineConfig) -> Result<MaterializationStrategy>` helper in `rocky-cli/src/commands/run.rs` replaces the previous inline `if/else` at the dispatch site. Plumbs through to the existing `MaterializationStrategy::Merge` + `sql_gen::generate_merge_sql` + per-adapter `merge_into()` path. The two new optional fields cascade through `just codegen` to `schemas/rocky_project.schema.json`, the dagster Pydantic `RockyProjectSchema`, and the VS Code TypeScript `rocky_project.ts`. Fivetran-style ingestion with whole-table-resync sources no longer silently duplicates under merge — MERGE upserts the delta by key, collapsing duplicates whichever source shape arrives.
- **DuckDB native MERGE** ([#559](https://github.com/rocky-data/rocky/pull/559)). `SqlDialect::merge_into` for `rocky-duckdb` now emits standard `MERGE INTO ... USING ... WHEN MATCHED ... WHEN NOT MATCHED` SQL (DuckDB 0.10+ engine support), replacing the previous stub error. `AdapterCapabilities.merge` flips from `false` to `true` for the DuckDB adapter, declared via the `test-adapter` conformance surface. DuckDB MERGE has two dialect-specific quirks Rocky honours: (1) no `UPDATE SET *` shorthand — DuckDB rejects the wildcard form; the SET clause must enumerate every target column (same posture as Snowflake); (2) no qualified column names on the SET LHS — DuckDB raises `Parser Error: Qualified column names in UPDATE .. SET not supported` for `t.col = s.col`, so the target column stays unqualified (`col = s.col`). E2E idempotency test in `rocky-core/tests/e2e.rs` runs the generated MERGE SQL three times against in-memory DuckDB with a synthetic whole-table-resync source and asserts row-count stability + value/timestamp convergence. Unblocks local POC testing of merge-strategy pipelines (the `02-merge-upsert` POC and the merge slice of `11-strategy-showcase`) without requiring a live warehouse.

### Notes

- **Snowflake replication+merge fails loud at SQL-gen time.** `ColumnSelection::All` is rejected by `rocky-snowflake/src/dialect.rs::merge_into` with `Snowflake MERGE does not support UPDATE SET *. Use explicit update_columns.`. Transformation users supply explicit `update_columns` in model TOML today; the replication runner has no equivalent surface yet. Tracked as a follow-up — threading explicit column lists through the runner is the same work that needs to land for the per-warehouse introspection path on DuckDB.
- **Watermark behaviour for `incremental` and `merge` is target-side across all dialects** (`WHERE {ts} > (SELECT MAX({ts}) FROM target_ref)`). For `incremental` this is the misbehaviour that motivates MERGE — a resync source advances `MAX(target.ts)` correctly but the filter still passes all source rows on the next tick because the source bumped its timestamps. MERGE preserves the filter and key-collapses any duplicates that pass through, so structural correctness lands now. Migrating to source-side watermarks is a separate follow-up that touches all 5 dialect impls + state-store semantics.
- **Silent fallback on strategy typos.** Any string other than `"incremental"` / `"merge"` falls back to `FullRefresh` (preserves prior behaviour). A typo of `"merge"` → `"mege"` silently maps to `FullRefresh`, defeating the safety the new validation adds. Tightening to a typed error is worth doing in a follow-up.

## [1.35.0] — 2026-05-18

Two headlines this cut. **Plan-store v2 is now the default** (Cluster 3 C — C-6). The writer-side flip lands behind the existing `[plan_store] format` config bit (introduced opt-in in `v1.33.0`). The reader continues to accept both formats unconditionally — plans persisted under v1 before this upgrade keep applying cleanly against a v1.35 binary. Projects that need the legacy on-disk shape must set `[plan_store] format = "v1"` explicitly. Stdout JSON is unchanged in both formats — the split remains purely between *persisted-to-disk* and *stdout* shapes. **Replication-only projects now get a real content-addressed `plan_id`** (Cluster 3 B — Phase 5b). `rocky plan` no longer emits `plan_id = null` for projects without a `models/` directory (or with zero compiled models). A new `PlanKind::Replication` variant runs source discovery at plan time and content-addresses by `blake3(kind + config + sorted-source-state)`. `rocky apply <plan-id>` re-runs discovery, asserts the source state matches the persisted snapshot, and aborts with an explicit diff if it has drifted before any SQL is emitted. Closes the dagster Phase 5 migration story — the dagster integration's `rocky run` fallback for replication-only projects is now removable on the consumer side.

### Added

- **Content-addressed `plan_id` for replication-only projects** (Cluster 3 B — Phase 5b). New `PlanKind::Replication` variant carries a `ReplicationPlan` payload: the full `RockyConfig` snapshot plus a sorted source-state snapshot (connectors + tables). Volatile fields are intentionally excluded for hash stability — `last_sync_at` (wall-clock, changes every sync) and the adapter-namespaced `metadata` map (can carry rate-limit counters etc.). `plan_id = blake3(kind + payload)`, so the same intent against the same source state produces the same id deterministically across machines.
- **Plan-time discovery for replication plans.** `rocky plan` already runs discovery to build its statement preview, so capturing the source-state snapshot at plan time is essentially free I/O-wise. Plan-time was picked over apply-time because apply-time would have given a cheaper `rocky plan` but only a config-only hash — no way to detect that the source moved between plan and apply. The trade is a strictly better symmetric drift check at apply time.

### Changed

- **`[plan_store] format` default flips from `"v1"` to `"v2"`** (Cluster 3 C — C-6). Projects without an explicit `[plan_store]` block now persist `rocky compact` / `rocky archive` plans in the typed-IR v2 envelope; `rocky apply` regenerates SQL at execution time via `rocky_core::sql_gen::{compact_from_ir, archive_from_ir}`. The reader continues to accept both formats unconditionally, so plans persisted under v1 before this upgrade keep applying cleanly. Projects that need to stay on the legacy on-disk shape must set `[plan_store] format = "v1"` explicitly. Stdout JSON is **unchanged in both formats** — `rocky compact --output json` / `rocky archive --output json` always carry inline SQL for human + CI consumers. `PromotePlan` and `RunPlan` remain untouched (promote keeps inline SQL as a documented governance-audit exception; run plans were already IR-only by construction). The default-change cascades through `just codegen` to the generated `rocky_project.schema.json`, the dagster `RockyProjectSchema` Pydantic model, and the VS Code `rocky_project.ts` interface. The plan-store migration guide is updated for the new default; a subsequent minor release (C-7) will drop the v1 reader entirely.
- **`rocky apply <plan-id>` fails fast on source drift for replication plans.** When the persisted source-state snapshot doesn't match what discovery returns at apply time, the apply path aborts with an explicit diff (`persisted snapshot: N connector(s); live snapshot: M connector(s)`, plus per-connector add/remove/change lines) and a "re-plan and re-apply" pointer — *before* any SQL is emitted. The symmetric inverse of plan-time discovery: the snapshot is captured at plan time and asserted at apply time, so source drift between the two never silently runs against a stale plan.

### Migration

- **Users who want to stay on plan-store v1 must explicitly opt in** via `[plan_store] format = "v1"` in `rocky.toml`. With nothing set, v1.35 writes v2.
- **The v1 reader stays for at least v1.35 + v1.36 minors.** C-7 drops the v1 reader path entirely; plans on disk written under `format = "v1"` after C-7 lands will return a clear regenerate-or-discard error. Migration window is open until then.
- **Replication-only callers of `rocky plan`** now get a non-null `plan_id`. Downstream tooling that previously branched on `plan_id == null` should switch to passing the new id through to `rocky apply <plan-id>`. The dagster integration already does this as of `dagster-rocky v1.33.0`.

## [1.34.0] — 2026-05-18

Two headlines this cut. **Arc 4 public-demo polish complete.** Landing-page copy + cross-links + the trace+cost+replay screencast asset itself all shipped, closing out the public-facing surface for the trace / cost / replay trio. **`failure_kind` discriminator on `RunOutput.errors[*]` ships fully wired.** A new typed enum mapped from `ConnectorError` variants (kebab-case wire form: `connection-failed`, `auth-failed`, `query-rejected`, `transient`, `quota-exceeded`, `not-found`, `unknown`) cascades through `just codegen` to the dagster Pydantic models + VS Code TypeScript bindings, paired with a 23-site refactor in `run.rs` that converts the lossy `anyhow!("{e}")` wrap to a typed-preserving form so the classifier reaches the typed error in production instead of falling through to `unknown`. Plus the plan-store v1→v2 migration guide ships ahead of the C-6 default-flip soak, and three smaller items: `rocky ai generate --unique-key` for merge materialization, an L001/L002 lint fix that compares against pre-substitution declared strings, and single-adapter `[source.discovery]` auto-wire.

### Added

- **`failure_kind` discriminator on `RunOutput.errors[*]`**. New typed enum mapped from `ConnectorError` variants — wire-form kebab-case: `connection-failed`, `auth-failed`, `query-rejected`, `transient`, `quota-exceeded`, `not-found`, `unknown`. Additive JSON-shape change; cascades through `just codegen` to dagster `RunOutput.errors[*]` Pydantic models and the VS Code TS bindings (`src/types/generated/run.ts`). Downstream consumers parsing run-output errors can now branch on a stable classification instead of grepping the error string.
- **`rocky ai generate --unique-key c1,c2` for `--materialization merge`**. Repeatable or comma-list; propagates through `SidecarMaterialization::Merge` → emitted sidecar TOML's `[strategy].unique_key`. Closes the runtime-rejection gap from `v1.26.0` where the merge materialization wrote an incomplete sidecar that `rocky run` then refused.
- **Single-adapter `[source.discovery]` auto-wire**. When `rocky.toml` defines exactly one adapter and `[pipeline.<name>.source.discovery]` is omitted, discovery now resolves to the sole adapter — mirroring the existing single-adapter default behaviour for the `source` and `target` blocks.

### Changed

- **Typed `AdapterError` now survives the anyhow wrap in `run.rs`**. 23 sites converted from the lossy `anyhow::anyhow!("{e}")` pattern to either `anyhow::Error::from(e)` (bare) or `anyhow::Error::from(e).context(...)` (with prefix). The `FailureKind` classifier walks the source chain via `downcast_ref` and now reaches the typed error at every run-path site instead of seeing a stringified blob and falling through to `unknown`. Closes the production-reach gap acknowledged in the body of the `failure_kind` PR — without this, the discriminator was structurally present on the JSON shape but practically always `unknown` for the most common failure paths.

### Fixed

- **L001 / L002 sidecar lints no longer fire on env-substitution defaults**. Lints now compare against the pre-substitution declared strings; `table = "${ROCKY_TABLE_OVERRIDE:-customer_facts}"` with `name = "customer_facts"` no longer trips L002. Symptom for users: a sidecar that compiled and ran fine still failed `rocky validate` against the substituted-string mismatch.

## [1.33.0] — 2026-05-17

Two headlines this cut. **Plan/apply spine alias deprecation cycle complete.** `rocky run` and bare `rocky branch promote <name>` now emit a one-line `[deprecated]` notice to stderr pointing at the canonical `rocky plan` + `rocky apply <plan-id>` flow (suppressible with `ROCKY_SUPPRESS_DEPRECATION=1`); behaviour and stdout JSON are byte-stable. `rocky plan` picked up the full flag surface of `rocky run` (every flag except `--watch`) so the plan/apply pair is now a complete replacement for `rocky run` for every non-watch caller, with flags captured into the persisted `RunPlan` so `rocky apply <plan-id>` honours them at apply time. **Cluster 3 C IR foundation + opt-in v2 plan persistence.** New typed `CompactPlanIr` and `ArchivePlanIr` plus the `sql_gen::{compact_from_ir, archive_from_ir}` regeneration helpers land additively — today's emission path still writes inline SQL into stdout JSON and the persisted plan. A new top-level `[plan_store]` config table introduces `format = "v1" | "v2"` (default `"v1"`); when flipped to `"v2"`, persisted `.rocky/plans/<id>.json` payloads for `rocky compact` / `rocky archive` switch to the typed-IR shape and `rocky apply` regenerates SQL at execution time. The reader accepts both formats unconditionally regardless of writer config for a soft migration. `PromotePlan` and `RunPlan` are intentionally untouched — promote keeps inline SQL as a documented governance-audit exception, and run plans were already IR-only by construction. Stdout JSON is unchanged in both formats.

### Added

- **Plan-store v2 writer behind `[plan_store] format` config bit** (Cluster 3 C — C-5). New top-level `[plan_store]` config table introduces a single field, `format = "v1" | "v2"`, that selects the persisted-plan payload shape for `rocky compact` / `rocky archive`. **Default stays `"v1"`** — existing users see no behaviour change. When `format = "v2"` the persisted `.rocky/plans/<id>.json` body is a typed IR payload (`CompactPlanIr` / `ArchivePlanIr` envelope) and `rocky apply` regenerates SQL at execution time via `rocky_core::sql_gen::{compact_from_ir, archive_from_ir}`; under v1 the payload is the legacy `CompactOutput` / `ArchiveOutput` shape with inline `NamedStatement.sql`. Stdout JSON is **unchanged in both formats** — `rocky compact --output json` / `rocky archive --output json` always carry inline SQL for human + CI consumers; the split is purely between *persisted-to-disk* and *stdout* shapes. A new `format_version: u32` field on `PersistedPlan` (with `#[serde(default = 1)]` for backward-compat with pre-C-5 files that lack the tag) lets the apply path dispatch on payload shape: the reader accepts **both formats unconditionally regardless of the writer config**, so v1 plans on disk continue to apply against a binary configured for v2 (and vice versa) for the entire migration window. `PromotePlan` and `RunPlan` are intentionally untouched — promote keeps inline SQL as a documented governance-audit exception, and run plans were already IR-only by construction. The soft migration cycle follows the audit memo §Q1: v1.33 (now) ships v2 as opt-in; a future minor (C-6) flips the default to `"v2"`; a subsequent minor (C-7) drops the v1 reader path. The C-2/C-3 byte-equivalence tests over today's emission inputs guarantee v1 and v2 produce identical SQL at apply time. New `[plan_store]` table cascades through `just codegen` to the generated `rocky_project.schema.json`, the dagster `RockyProjectSchema` Pydantic model, and the VS Code `rocky_project.ts` interface.
- **`ArchivePlanIr` typed IR + `sql_gen::archive_from_ir` regeneration helper** (Cluster 3 C — C-3). Mechanical mirror of C-2 against the archive command. New `rocky_ir::ArchivePlanIr` captures the structured inputs that determine `rocky archive`'s DELETE/VACUUM SQL: optional `target_table` (the `None` arm reproduces today's degenerate `DELETE FROM *` emission when the user supplies neither `--model` nor `--catalog`), `older_than` (preserved verbatim for audit), `older_than_days` (drives the `DATEADD(DAY, -N, CURRENT_TIMESTAMP())` literal), `partition_column` (today's hardcoded `_fivetran_synced` watermark, modeled as a field so future per-model overrides do not break the persisted shape), and `vacuum_retention_hours` (`None` skips VACUUM; today's CLI populates `Some(0)`). The companion `rocky_core::sql_gen::archive_from_ir(ir, dialect)` regenerates byte-identical SQL from the IR; an equivalence test in `commands::archive` pins parity across single-model default older-than, custom older-than (`180d`, `1y`), two-part-table-ref, wildcard-target, and per-table-in-catalog-scope inputs. Cutoff handling stays at runtime-DATEADD per the C-3 option-`a` decision; plan-time cutoff resolution (literal-timestamp emission) is deferred to C-5 alongside the persistence flip. **C-3 is additive: persistence is not flipped yet** — today's emission path still writes `Some(sql)` into both stdout JSON and the persisted plan.
- **`CompactPlanIr` typed IR + `sql_gen::compact_from_ir` regeneration helper** (Cluster 3 C — C-2). New `rocky_ir::CompactPlanIr` captures the structured inputs that determine `rocky compact`'s OPTIMIZE/VACUUM SQL: `target_table`, `target_size_mb`, `vacuum_retention_hours`, plus forward-compatible `partition_columns` / `zorder_columns` vectors. The companion `rocky_core::sql_gen::compact_from_ir(ir, dialect)` regenerates byte-identical SQL from the IR; an equivalence test in `commands::compact` pins parity across single-model, custom-size, two-part-table-ref, and per-table-in-catalog-scope inputs. **C-2 is additive: persistence is not flipped yet** — today's emission path still writes `Some(sql)` into both stdout JSON and the persisted plan. The v2 persisted-plan format (gated behind `[plan_store] format = "v2"`) lands in C-5.
- **`NamedStatement.sql` is now `Option<String>`** (`#[serde(skip_serializing_if = "Option::is_none")]`). Today's emission path always writes `Some(sql)`, so stdout JSON and persisted plans are byte-stable. The optionality leaves room for the v2 plan format to write `None` and rely on the apply path to regenerate SQL from typed IR. Apply paths in `rocky compact apply` / `rocky archive apply` surface a clear "this plan was written by a newer engine that persists typed-IR plans; this binary cannot regenerate SQL from IR yet — upgrade and re-apply" error if they ever encounter `None` (impossible until C-5 ships). The change cascades through `just codegen` to the dagster Pydantic + VS Code TypeScript bindings (`compact_schema.py`, `archive_schema.py`, `compact.ts`, `archive.ts`).

- **`rocky plan` flag-surface parity with `rocky run`.** `rocky plan` now accepts the full flag surface of `rocky run` (`--resume`, `--resume-latest`, `--shadow`, `--shadow-suffix`, `--shadow-schema`, `--branch`, `--partition`, `--from`, `--to`, `--latest`, `--missing`, `--lookback`, `--parallel`, `--all`, `--dag`, `--idempotency-key`, `--governance-override`, `--model`, `--models`) — the plan/apply pair is now a complete replacement for `rocky run`. Flags are captured into the persisted `RunPlan` payload so `rocky apply <plan-id>` honours them at apply time, including branch → shadow resolution. `--watch` is the one exception: its re-run-loop semantics have no plan/apply analogue and it remains exclusive to `rocky run`.

### Deprecated

- **`rocky run`** — Phase 4 of the plan/apply spine. Bare `rocky run` now emits a one-line `[deprecated]` notice to stderr pointing at the canonical `rocky plan` + `rocky apply <plan-id>` flow. Behaviour is unchanged. JSON-on-stdout is untouched. Suppress with `ROCKY_SUPPRESS_DEPRECATION=1`. With the flag-surface parity above, `rocky run` is now fully migratable; only `--watch` callers (re-run loop) need to keep using it.
- **`rocky branch promote <name>`** (without `--plan`) — same Phase 4 cycle. Emits a stderr notice pointing at `rocky plan promote <name>` + `rocky apply <plan-id>`. The `--plan` form is the canonical "review the plan in CI, apply on merge" UX. Suppress with `ROCKY_SUPPRESS_DEPRECATION=1`.

The dagster integration sets `ROCKY_SUPPRESS_DEPRECATION=1` on every subprocess invocation as of `dagster-rocky 1.31.0`, so existing `RockyResource.materialize()` / `RockyResource.run()` callers see no change.

## [1.32.0] — 2026-05-15

Headline: **Cluster 3 B plan/apply spine fully shipped end-to-end.** Three PRs land the unified plan/apply UX across `compact` / `archive` / `run` / `branch promote`. Plus a small LSP semantic-token column fix.

### Added

- **Persistent plan store.** New `plan_store` module persists plans at `./.rocky/plans/<plan_id>.json`, keyed by a full 64-character blake3 hex digest of the canonical `{kind, payload}` envelope. Plans are content-addressed — same intent → same plan_id. Extensible `PlanKind` enum (Compact / Archive / Run / Promote). PR [#522](https://github.com/rocky-data/rocky/pull/522).
- **`rocky compact apply <plan-id>` / `rocky archive apply <plan-id>`.** Phase 1 wires the `apply` subcommand for the two simplest verbs. Today's `rocky compact` / `rocky archive` already generate SQL but don't execute — users copy-paste. Now they save the plan and apply it. Per-statement `StatementResult { purpose, sql, success, duration_ms, error }` so partial failures roll up cleanly. DuckDB rejects OPTIMIZE/VACUUM cleanly via the per-statement error path; no adapter-specific branching. PR [#522](https://github.com/rocky-data/rocky/pull/522).
- **`rocky plan` + `rocky apply` unified entry.** Phase 2 introduces the top-level plan/apply spine. `rocky plan` compiles + emits a content-addressed `RunPlan` JSON; `rocky apply <plan-id>` dispatches by `PlanKind` (Compact → existing apply, Archive → existing apply, Run → existing run path). `--inline` flag skips persistence. `rocky run` is now an alias for `rocky apply --inline` (behavior preserved exactly). New `PlanOutput` evolved additively. PR [#523](https://github.com/rocky-data/rocky/pull/523).
- **`rocky plan promote <branch>` + `branch promote --plan <plan-id>`.** Phase 3 folds `branch promote` into the plan/apply spine. `rocky plan promote` runs the approval gate + breaking-change classifier at plan time and persists a `PromotePlan` (with `branch_state_hash`, `approvals_used`, `breaking_changes`, `targets[]`). `branch promote --plan <plan-id>` skips the plan step — the canonical "review the plan in CI, apply on merge" UX. Bare `branch promote <branch>` keeps working as an alias that internally chains plan + apply (byte-stable JSON output preserved). New `AuditEventKind::PromotePlanCreated` variant. PR [#527](https://github.com/rocky-data/rocky/pull/527).

### Fixed

- **LSP semantic tokens off-by-one column** (`rocky-server`). `sqlparser` reports identifier spans with 1-indexed lines *and* columns, but the LSP token collector only converted the line (`line - 1`) and emitted the column as-is. Identifiers at the end of a line ended up with `endChar = startChar + len` overflowing the line length, causing VS Code to silently drop the tokens. Fix subtracts 1 from `column` on every emitted token in `collect_tokens_from_table_factor` and the `Identifier` / `CompoundIdentifier` / `Function` arms of `collect_tokens_from_expr`. PR [#524](https://github.com/rocky-data/rocky/pull/524).
- **Fixture-determinism: `created_at` wall-clock field now sentinelled.** Push-fix to PR #523 added `created_at` to `scripts/_normalize_fixture.py::WALL_CLOCK_FIELDS` so the regen-fixtures corpus stays byte-stable across runs.

### Dependencies

- `cargo update` SemVer-compatible bumps: `aws-lc-rs` 1.16.3→1.17.0, `aws-lc-sys` 0.40.0→0.41.0, `clap_complete` 4.6.4→4.6.5, `filetime` 0.2.28→0.2.29, `jsonschema` 0.46.4→0.46.5, `kqueue-sys` 1.1.1→1.1.2, `pin-project` 1.1.12→1.1.13, `pin-project-internal` 1.1.12→1.1.13, `referencing` 0.46.4→0.46.5, `winnow` 1.0.2→1.0.3, `zerofrom` 0.1.7→0.1.8.

## [1.31.0] — 2026-05-13

**Cluster 3 E — semantic breaking-change diff — shipped end-to-end** across three PRs. Rocky now produces typed structural classifications of changes between two `ProjectIr` snapshots, exposes them informationally via `rocky ci-diff --semantic`, and uses them as a failing pre-promote gate on `rocky branch promote`. Strictly more powerful than text-shaped diff (SQLMesh's approach): operates on the typed IR rather than parsing emitted SQL strings.

### Added

- **`rocky_core::breaking_change` classifier** ([#508](https://github.com/rocky-data/rocky/pull/508)). New module exposing a typed `BreakingChange` enum (16 variants: model add/remove, column add/drop/type/nullability/reorder, materialization strategy + key swap, partition-by, target rename, source change, column mask, lakehouse format, SQL body), `BreakingSeverity { Breaking, Warning, Info }`, `BreakingFinding { change, severity }`, and `pub fn diff_project_ir(old: &ProjectIr, new: &ProjectIr) -> Vec<BreakingFinding>`. Covers all 10 `MaterializationStrategy` variants including per-variant key fields (Merge `unique_key` + `update_columns`, Incremental `timestamp_column`, TimeInterval `time_column` + `granularity`, ContentAddressed `storage_prefix` + `partition_columns`, etc.). Type-narrowing detection is hand-coded for the canonical pairs (Int64 → Int32, Float64 → Float32, Decimal precision shrink, Timestamp → Date) and defaults to widening for unknown transitions so the classifier doesn't over-alarm.
- **`rocky ci-diff --semantic` flag** ([#509](https://github.com/rocky-data/rocky/pull/509)). When set, `rocky ci-diff` compiles both refs into `ProjectIr` values, runs the classifier, and surfaces the findings in `CiDiffOutput.breaking_findings`. **Informational only — no exit-code change**; the failing gate is `branch promote`. `CompileResult` doesn't carry `ProjectIr`, so a new internal helper stitches `type_check.typed_models` onto each `Model::to_model_ir()` — without that merge the classifier would silently see empty schemas.
- **`rocky branch promote` pre-promote gate** ([#510](https://github.com/rocky-data/rocky/pull/510)). After the existing approval gate (and before `PromoteStarted`), compiles the project at `--base-ref` (default `main`) and HEAD, runs the classifier, and **fails fast on any `Breaking`-severity finding unless `--allow-breaking` is set**. New flags: `--allow-breaking`, `--base-ref <ref>`, `--models <path>` (the last two mirror `ci-diff` conventions). Three new `AuditEventKind` variants: `BreakingChangesBlocked` (gate blocked promote), `BreakingChangesAllowed` (operator overrode with `--allow-breaking`), `BreakingChangesGateSkipped` (gate couldn't run — base ref didn't compile under current Rocky; **fail-open** so stale-history doesn't surprise users). New `AuditEvent.breaking_changes: Option<Vec<BreakingFinding>>` field carries findings on Blocked/Allowed events. New top-level `BranchPromoteOutput.breaking_changes` field surfaces gate results even on a clean run.

### Changed

- **`extract_base_compile` consolidated as the canonical base-ref compile helper** ([#510](https://github.com/rocky-data/rocky/pull/510)). The previous `compile_base_ref` (added in #509) is removed; everything routes through `extract_base_compile(...) -> Result<CompileResult, String>` so callers get the failure reason when needed (e.g. for `BreakingChangesGateSkipped` audit events). `compute_ci_diff` continues to consume an `Option<CompileResult>` via `.ok()` — no behaviour change in `ci-diff`.

## [1.30.0] — 2026-05-12

Wave 2 feature drop: content-addressed UniForm Iceberg writer end-to-end across Phases 1–5. Headline: **`materialization = "content_addressed"`** is now a first-class strategy in the IR ([#496](https://github.com/rocky-data/rocky/pull/496)), accepted by the TOML sidecar ([#497](https://github.com/rocky-data/rocky/pull/497)), wired through `rocky run` ([#498](https://github.com/rocky-data/rocky/pull/498)), live-tested end-to-end against a Delta/Iceberg sandbox ([#499](https://github.com/rocky-data/rocky/pull/499)), partition-aware ([#500](https://github.com/rocky-data/rocky/pull/500)), and covering the full `RockyType` surface ([#501](https://github.com/rocky-data/rocky/pull/501)). The underlying `rocky-iceberg` writer ships in five phases: scaffold ([#488](https://github.com/rocky-data/rocky/pull/488)), bootstrap-commit discovery ([#489](https://github.com/rocky-data/rocky/pull/489)), `write_batch()` emitting content-addressed Parquet + Delta commits ([#491](https://github.com/rocky-data/rocky/commit/18361934)), `sync_iceberg_metadata()` with full round-trip ([#492](https://github.com/rocky-data/rocky/pull/492)), partitioned-table support ([#494](https://github.com/rocky-data/rocky/pull/494)), rowTracking writer surface ([#502](https://github.com/rocky-data/rocky/pull/502)), and post-ALTER schema evolution ([#503](https://github.com/rocky-data/rocky/pull/503)).

Internal cycle also extracts a dedicated **`rocky-ir` crate** out of `rocky-core` (Phase G1 + G1b) and deletes the legacy `Plan` enum (Typed-IR Phase 3 milestone, [#490](https://github.com/rocky-data/rocky/pull/490)). The dbt importer is promoted from experimental framing to GA ([#484](https://github.com/rocky-data/rocky/pull/484)). Adapter-conformance dialect check is now part of the standard test surface ([#398](https://github.com/rocky-data/rocky/pull/398)). Dependency floor moves to arrow + parquet 58.

### Added

- **`MaterializationStrategy::ContentAddressed` IR variant** ([#496](https://github.com/rocky-data/rocky/pull/496)). New variant on the typed IR alongside `FullRefresh` / `Incremental` / `Merge` / `MaterializedView` / `DynamicTable` / `TimeInterval`. Models declaring `materialization = "content_addressed"` in their sidecar now compile through the typed IR without the legacy free-form fallback.
- **TOML sidecar accepts `materialization = "content_addressed"`** ([#497](https://github.com/rocky-data/rocky/pull/497)). The model sidecar parser maps the new string to `MaterializationStrategy::ContentAddressed`. Existing materialization values (`view`, `table`, `incremental`, `merge`, `materialized_view`, `dynamic_table`, `time_interval`) are unchanged.
- **`rocky run` wires the content-addressed strategy** ([#498](https://github.com/rocky-data/rocky/pull/498), [#500](https://github.com/rocky-data/rocky/pull/500), [#501](https://github.com/rocky-data/rocky/pull/501)). The runner picks the content-addressed path when a model declares it, drives the rocky-iceberg writer through partitioned + unpartitioned + rowTracking paths, and now covers the full `RockyType` surface (numeric / string / temporal / boolean / decimal). Live e2e test in [#499](https://github.com/rocky-data/rocky/pull/499) executes the round-trip against a real Delta sandbox.
- **`rocky-iceberg` content-addressed UniForm writer** (Phases 1–5):
  - Phase 1 — scaffold ([#488](https://github.com/rocky-data/rocky/pull/488)). New `rocky-iceberg` writer module with the trait surface and a `discover()` stub.
  - Phase 1 — discover ([#489](https://github.com/rocky-data/rocky/pull/489)). `discover()` reads the bootstrap Delta commit and surfaces the existing snapshot's schema + partition spec + rowTracking config.
  - Phase 1 — write ([commit 18361934](https://github.com/rocky-data/rocky/commit/18361934)). `write_batch()` emits content-addressed Parquet files and appends a Delta commit referencing them.
  - Phase 1 — sync + round-trip ([#492](https://github.com/rocky-data/rocky/pull/492)). `sync_iceberg_metadata()` plus a full discover-write-read round-trip against the sandbox.
  - Phase 2 — partitioned tables ([#494](https://github.com/rocky-data/rocky/pull/494)). The writer now handles `partitionValues` correctly (keyed by physical UUID, not logical column name — see also the in-flight column-mapping handling in the writer).
  - Phase 3 — rowTracking ([#502](https://github.com/rocky-data/rocky/pull/502)). Writer-surface support for Delta rowTracking (baseRowId + rowCommitVersion), unblocked once UniForm + Deletion Vectors were confirmed mutually exclusive.
  - Phase 5 — schema evolution ([#503](https://github.com/rocky-data/rocky/pull/503)). Subsequent writes adapt to schema changes (added columns, type widening) applied to the underlying Delta table between writes.
- **Adapter-conformance dialect check** ([#398](https://github.com/rocky-data/rocky/pull/398)). The conformance harness now exercises a dialect-level identifier-quoting probe against each adapter, catching divergences between the `SqlDialect` impl and the live warehouse's parser.

### Changed

- **`rocky-ir` extracted into its own crate** ([commit e6b147cb](https://github.com/rocky-data/rocky/commit/e6b147cb), [commit d75401d2](https://github.com/rocky-data/rocky/commit/d75401d2)). The typed IR (`ModelIr`, `MaterializationStrategy`, source/target descriptors) used to live in `rocky-core::ir`. It now lives in `rocky-ir`, and external consumers (`rocky-compiler`, `rocky-cli`, `rocky-engine`, the adapters) depend on `rocky-ir` directly. `rocky-core` continues to depend on `rocky-ir` for the SQL-generation surface. No public API changes for end users; internal crate boundary moves.
- **`Plan` enum + `*Plan` structs + `From<&Plan>` + `to_plan_compatible` deleted** ([#490](https://github.com/rocky-data/rocky/pull/490)). The legacy `Plan` enum was the bridge layer kept in place while the codebase migrated to the typed `ModelIr` accessors (Typed-IR Phase 3 PR-2/PR-3/PR-4 across [#483](https://github.com/rocky-data/rocky/pull/483), [#485](https://github.com/rocky-data/rocky/pull/485), [#486](https://github.com/rocky-data/rocky/pull/486)). With all construction and consumption sites migrated, the bridge is gone — `ModelIr` is now the single in-memory plan representation.
- **`rocky import-dbt` promoted to GA framing** ([#484](https://github.com/rocky-data/rocky/pull/484)). The dbt importer drops the "experimental" framing in docs + help text. Behaviour unchanged; this is a documentation + positioning change reflecting that the end-to-end emission path (`rocky.toml` + per-model SQL + sidecars + seeds + `MIGRATION-NOTES.md`) shipped in engine `v1.27.0` and has been stable since.
- **Dependency bumps**: arrow + parquet `54 → 58` ([#471](https://github.com/rocky-data/rocky/pull/471)), tokio `1.52.1 → 1.52.3` ([#465](https://github.com/rocky-data/rocky/pull/465)), clap_complete `4.6.3 → 4.6.4` ([#463](https://github.com/rocky-data/rocky/pull/463)), and a workspace-wide `cargo update` ([#473](https://github.com/rocky-data/rocky/pull/473)).

### Fixed

- **`engine/install.ps1` parses on PowerShell 5.1** ([#470](https://github.com/rocky-data/rocky/pull/470)). The installer script used PowerShell 7-only syntax (ternary operator and null-coalescing) that PowerShell 5.1 — still the default on Windows Server 2019/2022 and stock Windows 10 — refused to parse. The fix is a syntactic rewrite using `if/else` and `if ($null -eq …)`; no behaviour change for users on PowerShell 7.

## [1.29.0] — 2026-05-09

Pipeline-correctness + security cycle. Headline: **`auto_create_schemas` now actually works on transformation pipelines** ([#448](https://github.com/rocky-data/rocky/pull/448)) — the setting under `[pipeline.<name>.target.governance]` was a no-op for transformation pipelines (only the replication path was wired), so models targeting a fresh schema failed at execute time with `Schema with name X does not exist`. Fix threads `auto_create_schemas: bool` through `execute_models` and pre-creates target schemas after compile, mirroring the per-source loop on the replication path. Pairs with **DuckDB MERGE upsert no longer parser-errors on `UPDATE SET t.col = s.col`** ([#449](https://github.com/rocky-data/rocky/pull/449)) — DuckDB rejects qualified column names on the SET LHS; the dialect now emits the unqualified form, with a new live-execute regression test that runs the generated MERGE against an in-memory DuckDB instead of asserting string content.

The cycle also closes a smaller correctness gap on the model-only run path ([#453](https://github.com/rocky-data/rocky/pull/453)) and bundles the actionable findings from the 2026-05-08 monorepo security audit ([#454](https://github.com/rocky-data/rocky/pull/454)).

### Added

- **Same-origin guard on Trino `nextUri` and Airbyte `next` follow-up URLs** ([#454](https://github.com/rocky-data/rocky/pull/454)). The Trino connector polls `/v1/statement` follow-ups by GETting the server-supplied `nextUri` with the same headers; the Airbyte client iterates pagination via the server-supplied `next` URL with the shared bearer-auth client. Both now validate that the follow-up URL is on the same scheme + host + port as the configured coordinator / base URL before re-issuing with the `Authorization` header. Mismatch surfaces as new error variants `TrinoError::UntrustedNextUri` / `AirbyteError::UntrustedNextUrl`. Closes the credential-forwarding hazard if the configured upstream is compromised, on-path, or mistyped to point at an attacker-controlled host. New shared `same_origin` helper plus 9 unit tests covering matching/different-host/scheme-downgrade/different-port/unparseable/default-port-normalization across both adapters.
- **`rocky lsp` caps `textDocument/didChange` at 100 MB** ([#454](https://github.com/rocky-data/rocky/pull/454)). tower-lsp doesn't bound JSON-RPC payload size, so a buggy or malicious LSP client could OOM the language server with a single oversized notification. Updates whose `text.len()` exceeds the cap are dropped with a `tracing::warn`; realistic hand-written `.rocky`/`.sql` models are several orders of magnitude smaller.
- **`rocky-bigquery` warns when `GOOGLE_APPLICATION_CREDENTIALS` is group/world-readable** ([#454](https://github.com/rocky-data/rocky/pull/454)). The service-account JSON file holds an RSA private key; mode `0600` (or `0400`) is the conventional setting. The adapter now logs a `tracing::warn` (Unix only) when `metadata.permissions().mode() & 0o077 != 0`. Doesn't refuse to load — that would break shared CI runners that mount keys as `0644` — but the misconfiguration surfaces in logs.
- **Path-traversal containment on transformation `models = "..."`** ([#454](https://github.com/rocky-data/rocky/pull/454)). `engine/crates/rocky-cli/src/scope.rs` now canonicalizes the project root and bails if a `models` glob resolves outside it. Closes the case where a checked-in `rocky.toml` with `models = "../../etc"` would read files outside the project tree and surface them through `rocky list models`, LSP hovers, or error output. The project root is required to canonicalize (errors out with context if it doesn't exist); a models directory that doesn't exist yet falls through to `load_models_from_dir`, which surfaces the natural error.
- **2026-05-08 monorepo security audit report** ([#454](https://github.com/rocky-data/rocky/pull/454)). New `docs/security-audit-2026-05-08.md` captures the static review across 7 concern areas (secrets, SQL, subprocess, fs/deserialization, network/TLS, LSP/IPC, AI). Zero critical/high findings; covers what was fixed in this cycle plus 12 positive observations (centralized identifier validation, `RedactedString` everywhere, loopback-only HTTP-server default, constant-time bearer-token compare, CSP+nonces, etc.).

### Changed

- **`rocky run --model X` propagates state-store open errors instead of silently disabling persistence** ([#453](https://github.com/rocky-data/rocky/pull/453)). The model-only entry point used `.ok()` on `StateStore::open(state_path)`, so a corrupted or unreadable state file silently dropped state persistence (lost watermarks, missing run history) — diverging from the full-pipeline path which already used `?` with `.context(...)`. Bring the two paths to parity. **Behavior change:** `rocky run --model X` against a corrupted state file now hard-errors with `failed to open state store at <path>` instead of running to completion in degraded mode. This is the right call — corrupted state should surface, not be masked — but anyone catching the prior degraded behavior in scripts will see the new error.
- **`WebhookConfig.secret` is `Option<RedactedString>`** ([#454](https://github.com/rocky-data/rocky/pull/454)). The HMAC signing secret used for outbound `X-Rocky-Signature` was stored as a plain `String`. Now wrapped in the existing `RedactedString` newtype so a stray `Debug` print of the hook config can't leak the secret into logs. JSON schema is unchanged (`RedactedString` delegates `JsonSchema` to `String`); no codegen drift.
- **Snowflake + Fivetran upstream error bodies truncated to 1 KB** ([#454](https://github.com/rocky-data/rocky/pull/454)). `AuthError::ApiError` (Snowflake) and `FivetranError::Api` (Fivetran) previously surfaced upstream response bodies verbatim. Both paths now truncate at 1024 bytes on a UTF-8 char boundary with a `…(truncated)` marker. Defensive against an unexpectedly large or attacker-shaped body if upstream response shape ever changes.
- **`rocky-ai` HTTP client now sets connect + request timeouts** ([#454](https://github.com/rocky-data/rocky/pull/454)). 10 s connect / 120 s request, matching every other adapter. A stalled Anthropic call would previously pin `rocky ai` indefinitely.
- **Hooks doc explicitly forbids interpolating untrusted values into `command`** ([#454](https://github.com/rocky-data/rocky/pull/454)). The runtime context is delivered to the script via stdin JSON — not interpolated into the command line — so the values Rocky exposes can't inject shell metacharacters. The new doc note in `docs/concepts/hooks.md` makes the trust boundary on the `command` field itself explicit: never build a hook command by string-formatting untrusted input (webhook payloads, API responses, row values).

### Fixed

- **`auto_create_schemas` now applies on transformation pipelines** ([#448](https://github.com/rocky-data/rocky/pull/448)). The setting under `[pipeline.<name>.target.governance]` was a no-op for transformation pipelines — `execute_models` never received the flag, so models targeting a fresh schema failed at execute time with `DuckDB error: Catalog Error: Schema with name X does not exist`. Replication pipelines have honoured this since v1.0; this brings the transformation path to parity. Threads `auto_create_schemas: bool` through `execute_models` and adds a pre-create pass after compile and before per-model execution that collects unique `(catalog, schema)` pairs from the resolved model set and calls `dialect.create_schema_sql()` on each. Three call sites updated: `run_local.rs` (transformation pipeline) and the replication-side `--all`/`--models` branch in `run.rs` both pass `pipeline.target.governance.auto_create_schemas`; the model-only entry point (`--model X`) passes `false` (no pipeline context). New end-to-end test `transformation_auto_create_schemas_materializes_fresh_schema` drives `super::run()` against a transformation pipeline whose single model targets `warehouse.mart.summary` — a schema that does not exist in a freshly-created DuckDB file. Pre-fix the run errors with the schema-not-exist message; post-fix it succeeds.
- **DuckDB MERGE no longer parser-errors on qualified columns in `UPDATE SET`** ([#449](https://github.com/rocky-data/rocky/pull/449)). DuckDB rejects qualified column names on the left side of `UPDATE .. SET` inside a MERGE statement (`Parser Error: Qualified column names in UPDATE .. SET not supported`). The dialect's `merge_into` was generating `UPDATE SET t.<col> = s.<col>` for every column in `update_columns` — the string compiled fine and passed every string-content unit test but blew up at execute time. The DuckDB form (`<col> = s.<col>`) is what's now emitted. Snowflake / Databricks goldens stay as-is since `t.<col> = s.<col>` is valid in those dialects. New live-execute test `test_merge_executes_against_live_duckdb` builds the MERGE SQL through `dialect.merge_into()` and runs it against an in-memory DuckDB with a seeded target + delta source — pre-fix this fails with the parser error, post-fix it succeeds and verifies UPDATE-on-match + INSERT-on-no-match semantics. The two affected IR-golden fixtures (`03-replication-merge/duckdb.sql`, `05-transformation-merge/duckdb.sql`) were regenerated to match.

## [1.28.0] — 2026-05-08

`rocky-trino` graduates from Experimental to Beta. Headline: **`rocky init --template trino`** ([#441](https://github.com/rocky-data/rocky/pull/441)) joins duckdb / databricks / snowflake / bigquery as a fourth-class scaffold target, emitting a runnable Trino project (env-var-driven `${TRINO_HOST}` / `${TRINO_USER}` / `${TRINO_PASSWORD}` / `${TRINO_JWT}` — never inline secrets — plus a self-contained `models/welcome.{sql,toml}` that compiles with no source tables). Pairs with the **Docker conformance harness behind the `trino-conformance` cargo feature** ([#442](https://github.com/rocky-data/rocky/pull/442)) — a `cargo test -p rocky-trino --features trino-conformance -- --ignored` invocation drives the adapter end-to-end against a real `trinodb/trino` coordinator over `/v1/statement` polling, exercising the dialect's identifier-quoting contract and a full `WarehouseAdapter` round-trip via the writable `memory` connector. With a self-test path against a live coordinator now in place, the adapter **drops the `is_experimental` override** ([#443](https://github.com/rocky-data/rocky/pull/443)): the CLI no longer logs a startup warning when a `type = "trino"` block is registered, and the root + engine README adapter tables move Trino from `Experimental` to `Beta` alongside Snowflake / BigQuery.

### Added

- **`rocky init --template trino`** ([#441](https://github.com/rocky-data/rocky/pull/441)). Fourth-class scaffold target alongside `duckdb` / `databricks` / `snowflake` / `bigquery`. Generates `rocky.toml` with the `trino` adapter wired to `${TRINO_HOST}` / `${TRINO_USER}` / `${TRINO_PASSWORD}` (HTTP Basic) or `${TRINO_JWT}` (JWT bearer) env-var placeholders — credentials never inlined; `models/_defaults.toml` with shared target catalog/schema; `models/welcome.{sql,toml}` selecting from literals so `rocky compile` succeeds against the freshly scaffolded project without any source tables. Inline TOML comments document both auth modes and explain why `username` is required even on the JWT path.
- **Docker conformance harness for `rocky-trino` behind the `trino-conformance` cargo feature** ([#442](https://github.com/rocky-data/rocky/pull/442)). Opt-in integration test at `engine/crates/rocky-trino/tests/conformance.rs` driving the adapter against a real `trinodb/trino` coordinator over the `/v1/statement` polling state machine. Coverage: `TrinoDialect::format_table_ref` against the live coordinator (so the dialect's identifier-quoting contract has to match Trino's parser) plus a full `WarehouseAdapter` round-trip via the writable `memory` connector (`SELECT 1`, then `CREATE TABLE AS` / `INSERT INTO` / `SELECT *` / `DROP TABLE` against `memory.default.<unique_table>`, table name salted with `SystemTime::now()` so re-runs against a long-lived container don't collide). The harness uses the JWT-bearer auth path with a dummy token (Trino doesn't validate JWT bearers against a JWKS in the upstream image's default config) and supplies `X-Trino-User` via `TrinoClientConfig::with_user`. Coordinator URL reads from `${TRINO_HOST:-localhost}` and `${TRINO_PORT:-8080}`. The default `cargo test -p rocky-trino` run stays credential- and network-free; CI is unchanged. The live-coordinator tests are additionally marked `#[ignore]` (see Fixed below) so `cargo test --all-features` skips them by default — invoke via `cargo test -p rocky-trino --features trino-conformance -- --ignored`.

### Changed

- **`rocky-trino` drops the experimental flag** ([#443](https://github.com/rocky-data/rocky/pull/443)). With the Docker conformance harness landed (`trino-conformance` cargo feature), the adapter now advertises `is_experimental() == false` (the trait default) and the CLI no longer logs a startup warning when a `type = "trino"` block is registered. Adapter tables in the root and engine READMEs move from `Experimental` to `Beta`. No behavioral change for users beyond the warning suppression.

### Fixed

- **`rocky-trino` conformance tests no longer break `cargo test --all-features`** ([#444](https://github.com/rocky-data/rocky/pull/444)). The two live-coordinator tests in `tests/conformance.rs` (`round_trip_select_one`, `round_trip_create_insert_select_drop`) are now marked `#[ignore]` so the `trino-conformance` feature gates compilation while `--ignored` gates execution. Without this pairing, CI's `cargo test --all-features` enabled `trino-conformance` and tried to run the network-dependent tests against a non-existent `localhost:8080`, surfacing as `ConnectionRefused` failures (which broke `main` briefly between [#442](https://github.com/rocky-data/rocky/pull/442) and the fix). The pure-dialect test stays runnable since it doesn't open a connection. README + module-doc invocation strings updated in lockstep — local invocation now requires both the feature flag and `--ignored`.

## [1.27.0] — 2026-05-07

Feature release on three independent fronts. Headline: **`rocky import-dbt` now emits a runnable Rocky repo** ([#428](https://github.com/rocky-data/rocky/pull/428)) — the command was previously a validation report; it now writes `rocky.toml` (with `${VAR}` placeholders for connection fields, never inlined secrets), per-model `.sql` + `.toml` sidecars, a verbatim copy of `seeds/`, and a `MIGRATION-NOTES.md` under `--output-dir`. **Generic-test mapping** ([#433](https://github.com/rocky-data/rocky/pull/433)) lands in the same window, translating dbt's `unique` / `not_null` / `accepted_values` / `relationships` schema tests into Rocky `[[checks]]` blocks on the per-model sidecar. Bundles **`rocky-trino` v0** ([#427](https://github.com/rocky-data/rocky/pull/427)) — first warehouse adapter built natively against `rocky-adapter-sdk` (the four first-party adapters predated the SDK and were retrofitted), shipping the four core modules (REST connector + dialect + HTTP Basic / JWT auth + `WarehouseAdapter` impl) with v0 limits flagged at validate time (no MERGE, no OAuth/Kerberos, no governance, no checksum bisection). **`rocky run --watch`** ([#423](https://github.com/rocky-data/rocky/pull/423)) wraps the run path in a filesystem-watcher loop for the inner-loop editor workflow; under `--output json`, each iteration emits one `RunOutput` per line (newline-delimited stream).

### Added

- **`rocky-trino` warehouse adapter (v0, experimental)** ([#427](https://github.com/rocky-data/rocky/pull/427)). New crate `engine/crates/rocky-trino` exercising `rocky-adapter-sdk` from outside the first-party adapters. The v0 slice ships the four core modules — `connector.rs` (async REST client driving Trino's `POST /v1/statement` + `nextUri` polling state machine, hand-rolled over `reqwest`), `dialect.rs` (`SqlDialect` impl with double-quoted identifiers, three-part `<catalog>.<schema>.<table>` references, `DESCRIBE <table>` column introspection, ANSI `INSERT INTO` / `CREATE TABLE AS`, `TABLESAMPLE BERNOULLI`), `auth.rs` (HTTP Basic + JWT bearer with `RedactedString`-wrapped credentials), and `adapter.rs` (`WarehouseAdapter` impl: `dialect`, `execute_statement`, `execute_query`, `describe_table`). The `trino` arm wires into `rocky-cli`'s adapter registry alongside `databricks` / `snowflake` / `bigquery` (reuses the existing `host` / `username` / `password` / `token` / `database` `AdapterConfig` fields — no new TOML keys). The adapter advertises `is_experimental: true`, so the registry's startup loop logs a warning when it's selected. **v0 limitations:** no MERGE (Trino's MERGE support is connector-dependent — `merge_into` returns "not supported in v0" so `strategy = "merge"` fails loudly at validate time), no OAuth/Kerberos auth (Basic + JWT only), no governance / loader / batch-checks, no checksum-bisection support (`row_hash_expr` falls through to the trait-default error). Follow-up PRs cover the `rocky init trino` template, the Docker-Trino conformance harness behind the `trino-conformance` feature flag, and the playground POC.
- **`rocky import-dbt` now emits a runnable Rocky repo (v0)** ([#428](https://github.com/rocky-data/rocky/pull/428)). The command was previously a validation report — it scanned a dbt project, translated each model body, and stopped. With this change the importer also writes a self-contained Rocky directory layout under `--output-dir`: `rocky.toml` (parsed from `dbt_project.yml` + `profiles.yml`, with connection fields written as `${VAR}` env-var placeholders — never inlined secrets), `models/<name>.sql` + `models/<name>.toml` sidecars per dbt model, `models/_defaults.toml` for the directory-level catalog/schema defaults, a verbatim copy of `<dbt_project>/seeds/`, and a `MIGRATION-NOTES.md` listing the v0 limitations + required env vars + adapter mapping. New flags: `--target-adapter` (override the profile-derived adapter — `duckdb` / `databricks` / `snowflake` / `bigquery`) and `--overwrite` (replace contents of a non-empty output directory; default refuses). v0 mapping: `materialized=view → ephemeral`, `table → full_refresh`, `incremental` (with/without `unique_key`) → `merge` / `incremental`, anything else → `full_refresh` with a TODO line in `MIGRATION-NOTES.md`. `{{ ref(...) }}` and `{{ source(...) }}` translate to plain identifiers; other Jinja is left verbatim with a `# TODO: dbt-jinja-not-translated` comment. Profile types Rocky doesn't natively support (e.g. `redshift`, `postgres`) stub a DuckDB `[adapter]` so the project still compiles, with the original type preserved under "Not Translated" in the migration notes. `ImportDbtOutput` gains an optional `emission` block (`out_dir`, `rocky_toml_path`, `migration_notes_path`, `models_translated_count`, `models_skipped_count`, `seeds_copied_count`, `adapter_type`, `original_dbt_adapter_type`, `required_env_vars`); the JSON schema, dagster Pydantic model, and vscode TypeScript interface are regenerated via `just codegen`. The dbt fixture used by the POC at `examples/playground/pocs/06-developer-experience/03-import-dbt-validate/` is unchanged; integration coverage for the new emission path lives at `engine/crates/rocky-cli/tests/import_dbt_emit.rs` against a richer fixture under `engine/crates/rocky-cli/tests/fixtures/dbt-rich/` that exercises `view`, `incremental`, and `table` materializations plus a seeds/ directory.
- **`rocky import-dbt` translates dbt generic schema tests into Rocky checks** ([#433](https://github.com/rocky-data/rocky/pull/433)). The four built-in dbt generic tests — `unique`, `not_null`, `accepted_values`, `relationships` — are parsed out of `schema.yml` and emitted as `[[checks]]` entries on the matching per-model `.toml` sidecar. `unique` and `not_null` map to the corresponding native Rocky check kinds; `accepted_values` becomes an `[[checks]]` block with `kind = "expression"` and a generated `WHERE`-shaped predicate; `relationships` becomes a foreign-key check against the referenced model. Tests on columns Rocky doesn't yet model (e.g. column-level tests where the column wasn't translated) are listed in `MIGRATION-NOTES.md` under "Not Translated" rather than silently dropped. Singular tests in `tests/`, custom generic tests, and macro-defined tests remain v0-deferred. The migrate-from-dbt walkthrough is updated in lockstep ([#434](https://github.com/rocky-data/rocky/pull/434)).
- **`rocky run --watch` for the inner-loop developer workflow** ([#423](https://github.com/rocky-data/rocky/pull/423)). Wraps the existing run path in a filesystem-watcher loop so an interactive editor session re-materializes the pipeline on every save. The watcher monitors the parent directory of `rocky.toml` (filtered to `rocky.toml` itself) plus the resolved `models/` directory recursively, with a 200 ms debounce window that coalesces editor save bursts (e.g. vim's atomic-rename, VSCode's two-phase write) into a single re-run. Failed runs are logged and the loop continues; only adapter-init / config-parse errors at startup exit non-zero. Ctrl-C completes the in-flight run, then exits 0. The directory-watch + filename-filter shape is FSEvents-safe on macOS — atomic-rename saves trigger correctly where a file-level watch can miss the new inode. Banner / "detected change" lines write to `stderr` so `stdout` stays parseable; with `--output json`, each iteration emits one `RunOutput` JSON object (newline-delimited stream). **v0 limitations** rejected at clap parse time: `--watch` is mutually exclusive with `--dag`, `--resume`, `--resume-latest`, `--idempotency-key`, and `--model`. Implementation lives in a dedicated `engine/crates/rocky-cli/src/commands/run_watch.rs` (kept out of the 5621-line `run.rs`); covered by 4 unit tests on the event filter + debounce coalescing plus 1 Unix-only integration test that spawns the binary, touches a model file, and verifies the second run + clean SIGINT exit.

### Fixed

- **`rocky run --watch -o json` now honours its NDJSON-stream contract** ([#423](https://github.com/rocky-data/rocky/pull/423) follow-up). Each watch iteration's `RunOutput` previously serialized as multi-line pretty-printed JSON, which broke any tool piping `rocky run --watch -o json` through line-oriented JSON readers (`jq -c`, `tail -f | jq`, log shippers expecting NDJSON). Watch mode now emits one compact JSON object per line, matching what the doc-comment on the `run_watch` module already promised. Non-watch one-shot commands (`rocky run`, `rocky compile`, `rocky catalog`, …) keep pretty-printing — the streaming contract is the watch-specific add. Implementation: a process-global `COMPACT_JSON` `AtomicBool` in `crates/rocky-cli/src/output.rs::print_json` that the watch wrapper toggles on once at startup. The integration test in `engine/rocky/tests/run_watch.rs` now collects stdout per-line and asserts every line parses as a standalone JSON object with `command == "run"`, so a regression that re-introduces pretty-print fails the suite.
- **`rocky validate` now recognises `trino`, `bigquery`, `airbyte`, and `iceberg` as known adapter types** ([#435](https://github.com/rocky-data/rocky/pull/435)). The hand-maintained known-types list inside `validate` had drifted behind the registry: configs targeting any of the four adapters above tripped a "Unknown adapter type" diagnostic even though the registry resolved them at run time. Symptom for users: a project that compiled and ran fine still failed `rocky validate` against a stale taxonomy. The list is now driven straight from the adapter registry (see refactor below), so future adapters register once and propagate without a follow-up validate edit.

### Internal

- **`rocky validate` known-types now driven from the adapter registry** ([#436](https://github.com/rocky-data/rocky/pull/436)). Removes the parallel hand-maintained list. The validate diagnostic walks the registry-declared adapter set instead, which closes the drift class that produced [#435](https://github.com/rocky-data/rocky/pull/435).

## [1.26.0] — 2026-05-05

Feature release tightening up the AI authoring surface and closing the remaining gap on per-model env-var injection. Headline: **`rocky ai "<intent>"` now writes both the body and a `.toml` sidecar to disk** ([#414](https://github.com/rocky-data/rocky/pull/414)) — the command previously only printed the body to stdout, leaving the user to hand-author a matching sidecar before Rocky's loader would pick the model up. The sidecar carries the materialization strategy + target FQN, so a generated model is loadable on the next `rocky run` without manual editing. Bundles **`${VAR}` / `${VAR:-default}` substitution in model sidecar TOML** ([#415](https://github.com/rocky-data/rocky/pull/415)) — the same env-var syntax `rocky.toml` already supported now resolves in per-model `.toml` sidecars, `models/_defaults.toml`, and inline `---toml` frontmatter, unblocking the Dagster asset-factory pattern with per-asset env injection. **AI generation prompts tightened** ([#413](https://github.com/rocky-data/rocky/pull/413)) so the model generator can't suggest dbt-shaped output: the SQL prompt forbids Jinja and `{{ config(...) }}` headers; the DSL prompt clarifies `.rocky` files are bare pipeline bodies (no `model { }` wrapper) and documents the `replicate <source>` step.

### Added

- **`rocky ai "<intent>"` emits a `.toml` sidecar alongside the body** ([#414](https://github.com/rocky-data/rocky/pull/414)). The command now writes two files into the models directory — the source body and a sidecar carrying `[strategy]` + `[target]` — instead of only printing the body to stdout. New flags drive the sidecar shape: `--materialization <full_refresh|incremental|merge|ephemeral>` (defaults to `full_refresh`), `--watermark <col>` (required when `--materialization=incremental`, serialized as `[strategy] timestamp_column`), `--target <catalog.schema.table>` (defaults to `generated.ai.<name>`, matching the in-memory default in `build_generated_model`), and `--overwrite` to allow replacing existing files. `AiGenerateOutput` gains `body_path` + `sidecar_path` (Optional, only populated on success); the JSON schema, dagster Pydantic model, and vscode TypeScript interface are regenerated via `just codegen`. The sidecar is emitted from a lean local serializer (`SidecarToml`) rather than serializing `ModelConfig` directly, so the on-disk file stays compact (no empty `depends_on = []` / `sources = []` / `classification = {}` blocks) — covered by three loader-roundtrip tests for full_refresh / incremental / ephemeral. **v1 limitations:** `--materialization merge` ships incomplete — no `--unique-key` flag yet, so the resulting sidecar is missing the merge key and must be hand-edited before `rocky run` will accept it. `time_interval`, `delete_insert`, and `microbatch` materializations are out of scope for this first cut and need richer flag plumbing; `materialized_view` and `dynamic_table` are not variants of `StrategyConfig` (they live in the IR `MaterializationStrategy` only) and would emit invalid sidecar TOML, so they are intentionally not exposed by `--materialization`.
- **`${VAR}` / `${VAR:-default}` substitution in model sidecar TOML** ([#415](https://github.com/rocky-data/rocky/pull/415)). The same env-var substitution syntax `rocky.toml` already supported now resolves in per-model sidecar `.toml`, `models/_defaults.toml`, and inline `---toml` frontmatter. Closes the per-model env injection gap (FR-001 Option A) — Dagster's asset-factory pattern can now inject per-asset configuration via env vars rather than templating sidecars at codegen time. Substitution applies to the frontmatter only; the SQL body is left literal so `${VAR}` references in queries (e.g. an in-warehouse expression that uses `${`) are not silently rewritten.

### Changed

- **AI generation prompts tightened to refuse dbt-shaped output** ([#413](https://github.com/rocky-data/rocky/pull/413)). The SQL prompt now explicitly forbids Jinja templating and `{{ config(...) }}` headers — Rocky's loader rejects both, and the model generator was occasionally proposing them. The DSL prompt clarifies `.rocky` files are a bare pipeline body (no `model { }` wrapper, which Rocky's parser does not accept), documents the `replicate <source>` step (which the prior prompt didn't surface), and adds two few-shot DSL examples so the generator can pattern-match the expected shape. Behavioural change for `rocky ai "<intent>"` users: generated SQL no longer carries dbt config blocks; generated DSL no longer wraps the pipeline in a `model` declaration.

## [1.25.0] — 2026-05-04

Feature release closing out Typed-IR Option B Phase 2 and shipping the file-based branch approval gate. Headline: **`rocky branch approve` + `rocky branch promote` with a file-based approval gate** ([#388](https://github.com/rocky-data/rocky/pull/388)) — `branch approve` signs a content-addressed approval artifact (one JSON file per approver under `./.rocky/approvals/<branch>/<approval_id>.json`, blake3 over canonical JSON, bound to the local git identity and a `branch_state_hash`); `branch promote` enumerates the configured replication pipeline's production targets and dispatches `CREATE OR REPLACE TABLE prod.<x> AS SELECT * FROM branch__<name>.<x>` per target with dialect-correct identifier quoting. The gate is opt-in via `[branch.approval]` (`required` / `min_approvers` / `allowed_signers` / `max_age_seconds`) and disabled by default; pass `--skip-approval` (or set `ROCKY_BRANCH_APPROVAL_SKIP=1`) to bypass an enabled gate, which records an `ApprovalSkipped` audit event in the JSON output. Both commands emit typed JSON output (`BranchApproveOutput` / `BranchPromoteOutput`) for the dagster + vscode integrations. **Typed-IR Option B Phase 2 lands end-to-end** — Phase 2a extends `ModelIr` with the materialization/governance fields the production sites need plus a `Plan → ModelIr` shim; Phase 2b.1 constructs `ModelIr` at every `Plan` construction site so the typed IR coexists with the legacy enum at every callsite; Phase 2b ([#378](https://github.com/rocky-data/rocky/pull/378)) flips `sql_gen` to consume `&ModelIr` directly, completing the migration. **`rocky completions` shell-completion generator** ([#373](https://github.com/rocky-data/rocky/pull/373)) emits bash / zsh / fish / powershell / elvish completion scripts via clap_complete. **AI-powered LSP code-action fallback for E010 + E013** ([#374](https://github.com/rocky-data/rocky/pull/374)) — when the deterministic code-action for the contract diagnostics doesn't have enough context, the LSP server now falls back to a Claude-backed action that can rewrite the contract or the model body, gated on `ANTHROPIC_API_KEY`. **`rocky init bigquery` template** ([#377](https://github.com/rocky-data/rocky/pull/377)) adds a credentials-clean BigQuery starter alongside the existing duckdb/databricks/snowflake templates.

### Added

- **`rocky branch approve` + `rocky branch promote` with file-based approval gate** ([#388](https://github.com/rocky-data/rocky/pull/388)). New verbs under `rocky branch` for the branch lifecycle. `rocky branch approve <name>` writes a content-addressed approval artifact at `./.rocky/approvals/<branch>/<approval_id>.json` — blake3 over canonical JSON, signed under the local git identity (`git config user.email` + `user.name`), and bound to a `branch_state_hash` derived from the branch metadata + `rocky.toml` bytes so the approval automatically becomes void if the branch's state advances. The `algorithm` discriminator on disk is the upgrade hook for future asymmetric-signing variants. `rocky branch promote <name>` enumerates the configured replication pipeline's production targets, validates approvals against `[branch.approval]` (off by default — opt in with `required = true` plus optional `min_approvers` / `allowed_signers` / `max_age_seconds`), and dispatches `CREATE OR REPLACE TABLE prod.<x> AS SELECT * FROM branch__<name>.<x>` per target. `--skip-approval` (or `ROCKY_BRANCH_APPROVAL_SKIP=1`) bypasses an enabled gate and emits an `ApprovalSkipped` audit event capturing the skip origin. The typed `BranchPromoteOutput` exposes `branch_state_hash`, `approvals_used`, `approvals_rejected` (with reasons: `bad_signature` / `state_hash_mismatch` / `expired` / `signer_not_allowed` / `parse_error`), `targets`, `audit` (`PromoteStarted` / `PromoteCompleted` / `PromoteFailed` / `ApprovalSkipped`), and `success`. Schemas exported, dagster Pydantic models + vscode TypeScript bindings regenerated. Companion landing in dagster `v1.23.0` and vscode `v1.13.0`. v1 limitations: promote enumeration is replication-only (transformation pipelines need the model-glob walk follow-up); `branch_state_hash` covers `rocky.toml` bytes only, so model-file edits don't void existing approvals; no revoke flow (approvals void only when the state hash advances or the artifact file is removed).
- **`rocky completions <shell>` shell-completion generator** ([#373](https://github.com/rocky-data/rocky/pull/373)). New top-level command that emits completion scripts for bash, zsh, fish, powershell, and elvish via `clap_complete`. Output goes to stdout so users can redirect / source per their shell convention; e.g., `rocky completions zsh > ~/.zfunc/_rocky`. Surfaces every CLI verb + flag without a separate maintenance burden — driven straight off the `clap` definitions.
- **`rocky init bigquery` template** ([#377](https://github.com/rocky-data/rocky/pull/377)). Fourth scaffold target alongside duckdb/databricks/snowflake. The template ships a credentials-clean `rocky.toml` using `${VAR:-default}` env-var substitution for the GCP service-account / project / dataset triple, plus a starter model. No live credentials are baked into the template.
- **AI-powered LSP code-action fallback for E010 + E013** ([#374](https://github.com/rocky-data/rocky/pull/374)). When the deterministic code-action for the two contract diagnostics (E010 missing-required-column, E013 protected-column-violation) lacks enough context to produce a confident edit, the LSP server now falls back to a Claude-backed action that synthesizes a multi-line edit on the contract or model body. Gated on `ANTHROPIC_API_KEY` — without a key the deterministic path remains the only surface, so the fallback is opt-in by environment.
- **LSP code-action for E010 + E013 contract diagnostics**. Deterministic code-action surface for the two contract diagnostics, registered as the primary code-action handler before the AI fallback above kicks in.

### Changed

- **Typed-IR Option B Phase 2 lands end-to-end.** Phase 2a extends `ModelIr` with the materialization and governance fields the production construction sites carry on `Plan` (target FQN + materialization strategy + governance config), plus a one-way `Plan → ModelIr` shim used by the construction sites until they're flipped over. Phase 2b.1 constructs `ModelIr` at every `Plan` construction site so the typed IR coexists with the legacy enum at every callsite. Phase 2b ([#378](https://github.com/rocky-data/rocky/pull/378)) flips `sql_gen` to consume `&ModelIr` directly — the old `&Plan` overload is removed. The four production construction sites (`compile`, `plan`, `run`, `preview`) now produce both representations; subsequent phases will retire the legacy `Plan` enum once the remaining downstream consumers (state-store recipe-hash, runtime executor) migrate.
- **Doc-comment hygiene sweep + codegen regen** ([#386](https://github.com/rocky-data/rocky/pull/386)). Strip internal "Arc N / wave X" framing from `///` doc comments on `JsonSchema`-deriving structs so the generated TS / Pydantic / JSON-schema descriptions don't propagate internal taxonomy. No behaviour change; codegen regen is the only diff outside of the doc edits.

### Internal

- **Typed-IR contract spec + multi-dialect golden tests** ([#387](https://github.com/rocky-data/rocky/pull/387)). Documents the typed-IR invariants ratified during the Option B sign-off (mask resolution, watermark strip, partition-window invariants, type relocation) and adds golden-file tests covering the four warehouse dialects (`duckdb`, `databricks`, `snowflake`, `bigquery`) so the IR → SQL pipeline is pinned per-dialect.

## [1.24.0] — 2026-05-03

Feature release closing out the `rocky catalog` command and continuing the OTel + Typed-IR threads. Headline: **`rocky catalog` is now feature-complete** — Parquet emit (`edges.parquet` + `assets.parquet` alongside `catalog.json`) plus state-store enrichment that fills the previously-stubbed `last_run_id` / `last_materialized_at` fields, plus a `--catalog <name>` scope flag mirroring `compact` / `archive`. The artifact contract the AI review workflow consumes is now the full design-memo shape. **Per-adapter `statement.execute` spans land on BigQuery and Snowflake**, completing the OTel tracing surface where it was previously asymmetric — Databricks alone inherited statement-level spans via the `materialize.table` parent. Internal: **`ModelIr` + `ProjectIr` structs land in `rocky-core::ir`** as PR-2 of Typed-IR Option B Phase 1, coexisting with the existing `Plan` enum until Phase 2 migrates the construction sites.

### Added

- **`rocky catalog --format json|parquet|both` ([#366](https://github.com/rocky-data/rocky/pull/366)).** The catalog command now emits `edges.parquet` + `assets.parquet` alongside `catalog.json` by default. Flat schemas per the design memo — one row per column-lineage edge (`source_model`, `source_column`, `target_model`, `target_column`, `transform`, `confidence`, `generated_at`, `config_hash`, `last_run_id`) and one row per asset/column (`asset_fqn`, `model_name`, `asset_kind`, `column_name`, `data_type`, `is_nullable`, `has_star`, `intent`, `last_materialized_at`, `generated_at`, `config_hash`). Parquet output is byte-stable across runs (rows sorted by their natural key before emit) so consumers can JOIN against warehouse queries or load directly into Pandas / DuckDB. New `parquet = "54"` workspace dep alongside the existing `arrow = "54"`. `--format parquet` skips JSON for "give me only the analytics file" use cases; `--format json` matches PR-1 behaviour.
- **`rocky catalog` state-store enrichment + `--catalog <name>` scope ([#368](https://github.com/rocky-data/rocky/pull/368)).** Two finishing touches on the catalog command. (1) State-store enrichment: opens the local state store read-only via `StateStore::open_read_only` so the command never blocks an in-flight `rocky run`, walks the most recent 50 runs, and fills `CatalogOutput.last_run_id` (most recent successful run) plus per-asset `CatalogAsset.last_run_id` + `last_materialized_at` (newest successful execution per model). PR-1 + PR-2 left these fields stubbed as `None`. Best-effort: a missing or unreadable state store leaves all state-derived fields as `None` so the command works pre-first-run. (2) `--catalog <name>` flag filters the snapshot to assets whose target FQN sits in the named warehouse catalog. Reuses the same `scope::resolve_managed_tables_in_catalog` helper that backs `rocky compact --catalog` and `rocky archive --catalog`, so resolution semantics (managed-table set, catalog-name normalisation, "no matches" diagnostic) are identical across the three commands. Edges referencing dropped assets are pruned so the JSON / Parquet artifacts never have dangling references.
- **Per-adapter `statement.execute` OTel spans on BigQuery and Snowflake ([#365](https://github.com/rocky-data/rocky/pull/365)).** Statement-level tracing was previously asymmetric — Databricks queries inherited a `statement.execute` parent via the `materialize.table` span (renamed from `dag_node` in v1.23.0), but BigQuery and Snowflake had no per-statement child span. This PR adds `info_span!("statement.execute", adapter = "bigquery"|"snowflake", statement.kind = "query")` around the central submit+poll path on both adapters: `BigQueryConnector::run_query` (inherited by `execute_statement`, `execute_statement_with_stats`, `execute_query`, `describe_table`, `list_tables`, …) and `SnowflakeConnector::submit_and_wait` (split into a span-wrapping outer + retry-loop inner so retries become events on the parent span, not orphan siblings). `rocky-iceberg` / `rocky-airbyte` / `rocky-fivetran` are intentionally not touched — they implement `DiscoveryAdapter` only with no SQL execution surface, so a `statement.execute` span would be semantically wrong. Snowflake gains `tracing-subscriber` as a dev-dep for the new wiremock-based span-emission canary test.

### Internal

- **`ModelIr` + `ProjectIr` typed-IR structs land in `rocky-core::ir` ([#367](https://github.com/rocky-data/rocky/pull/367)).** PR-2 of Typed-IR Option B Phase 1. Adds `ModelIr` (per-model: SQL, types, lineage slice, schema, materialization strategy, governance, resolved column masks) and `ProjectIr` (thin wrapper: `models`, `dag`, `lineage`) alongside the existing `Plan` enum — coexists rather than replacing, with the next phase set to migrate the four production construction sites. Three load-bearing refinements ratified in the design memo landed here: (a) `WatermarkState` strips out of `MaterializationStrategy::Incremental` because it's runtime state and would have made `recipe_hash` change every successful run, destroying content-addressed write semantics — runtime continues to read watermarks from the state store via `get_watermark` / `set_watermark` (no behaviour change). (b) Resolved column-mask plan moves into `ModelIr.column_masks` so masks-as-applied participate in the recipe-hash; policy stays in `RockyConfig`. (c) Canonical-JSON convention applied uniformly — every `Option` field uses `#[serde(default, skip_serializing_if = "Option::is_none")]` so the JSON encoding is deterministic for hashing. New `recipe_hash()` method on `ModelIr` returns a `blake3::Hash` of the canonical JSON; tests assert determinism and divergence on input changes. New `blake3` dep on `rocky-core`. The next phase will absorb the gap that `ModelIr` doesn't yet carry `ReplicationPlan` source/target or snapshot-specific fields — all four construction sites currently route through `Plan` for those.

## [1.23.0] — 2026-05-02

Feature release bundling the `rocky catalog` JSON emit with an OTel-tracing wave and the per-model side of the pre-merge budget surface. Headline: **`rocky catalog` JSON emit** — a new top-level command that walks the SemanticGraph and writes a project-wide column-level lineage snapshot to `./.rocky/catalog/catalog.json`. The artifact is byte-stable across runs (edges sorted, pretty-printed) so it diffs cleanly in git, and unblocks the AI review workflow's artifact contract. Bundles **end-to-end OTLP tracing** — `tracing-opentelemetry` is now registered on the subscriber, every existing `info_span!` reaches OTLP automatically, `PipelineEvent`s surface as span events, retry-loop terminal failures stamp the active span with `Status::Error`, and the per-DAG-node span is renamed to `materialize.table` per OTel `<verb>.<resource>` conventions. **Per-model `[budget]` blocks** land alongside per-model cost-projection coverage in `rocky preview cost`. **`rocky.table_duration_*` / `rocky.query_duration_*` gauges flip to `u64_histogram` instruments** so OTLP backends can compute percentiles natively across hosts and runs. Bundles an end-of-run auto-sweep for the `[state.retention]` policy that landed in 1.22.0 — the policy now "just works" without an external cron — plus an internal-only relocation of the typed-program primitives (`TypedColumn`, `RockyType`, `LineageEdge`) from `rocky-compiler` to `rocky-core`. No public API change from the relocation; existing call sites continue compiling unchanged via re-exports.

**Operator callout (OTel metrics shape change).** Dashboards watching `rocky.table_duration_p50_ms` / `_p95_ms` / `_max_ms` (or the query-side equivalents) need to migrate to the new `rocky.table_duration_ms` / `rocky.query_duration_ms` histogram instruments and use the backend's percentile aggregations. The in-process JSON snapshot (consumed by the dagster integration and terminal output) is unchanged.

### Added

- **`rocky catalog` JSON emit ([#356](https://github.com/rocky-data/rocky/pull/356)).** New top-level `rocky catalog` command that emits a project-wide column-level lineage snapshot. Defaults follow the design memo's signed-off shape: one verb (no subcommands), `./.rocky/catalog/` directory, JSON only, three-bucket `High|Medium|Low` confidence enum (Medium for star-expanded edges), sources emitted as assets, pretty-printed JSON, first-pipeline selection in multi-pipeline projects. Edges are sorted by `(source_model, source_column, target_model, target_column)` before emit so the artifact is byte-stable across runs and diffable in git. Stdout shows a one-screen summary; the structured payload is the file. New `CatalogOutput` family (`CatalogAsset`, `CatalogColumn`, `CatalogEdge`, `CatalogStats`, `AssetKind`) lands in the JSON-output cascade — `RockyResource.catalog()` exposes it through the dagster typed-resource layer and the VS Code extension picks up the regenerated TS interfaces. PR-2/3/4 follow-ups (Parquet emit, `--catalog <name>` scoping, `RunRecord` enrichment) are tracked separately. The `assets_with_star` stat surfaces the partial-coverage signal for lineage extraction holes (window functions, CTEs, set operations, `CASE WHEN`, join keys) that are out of scope for PR-1. Reuses 100% of the existing compile + `SemanticGraph` path; touches no state store.
- **`tracing-opentelemetry` layer registered on the subscriber ([#360](https://github.com/rocky-data/rocky/pull/360)).** With the `otel` Cargo feature and `OTEL_EXPORTER_OTLP_ENDPOINT` set, every existing `info_span!` site (`dag_node`, `discover_sources`, `governance_setup`, `state.{download,upload,probe}`, …) now reaches OTLP gRPC automatically — no source-level changes at the instrumentation sites. The OTel tracer provider is installed inside `init_tracing` (returning a `TracingGuard` held for process lifetime) so it's live before the first captured span; `OtelGuard` demoted to a metrics-only lifecycle wrapper, metrics flush behaviour unchanged. W3C `TraceContextPropagator` installed globally; `extract_remote_context()` parses an upstream `TRACEPARENT` env var (Dagster, Airflow OpenLineage, OTel Operator) so subprocess-as-child runs join the parent trace. Sampler is `ParentBased(AlwaysOn)` — honours upstream sampler decisions when traceparent propagation lands, falls back to "always sample" stand-alone. `tracing-opentelemetry = "0.32"` paired with `opentelemetry = "0.31"`.
- **Per-`PipelineEvent` OTLP span events at the 8 production emit sites ([#362](https://github.com/rocky-data/rocky/pull/362)).** `circuit_breaker_recovered` / `circuit_breaker_tripped` / `statement_retry` (rocky-databricks + rocky-snowflake), `http_retry` (rocky-fivetran), and `budget_breach` (rocky-cli) now annotate the active `tracing::Span` with an OTLP span event in addition to the existing `EventBus` emit, so retries / circuit-breaker transitions / budget breaches show up natively in trace viewers (Jaeger, Honeycomb, Datadog, Grafana Tempo) without operators wiring up an `EventBus` subscriber separately. Two new helpers in `rocky-observe`: `events::record_span_event(&PipelineEvent)` translates the event type to a span-event name and the structured fields (`target` / `error` / `attempt` / `max_attempts` / `error_class` / metadata) to OTel `KeyValue` attributes under the `rocky.event.*` namespace; `events::set_current_span_error(message)` marks the active span as `Status::Error(description)` and is invoked at retry-loop terminal-failure sites (Databricks + Snowflake retry-budget exhaustion + retry exhaustion) so the active adapter span — and, by inheritance, the parent `materialize.table` span — carries the right OTel status. Both helpers are no-ops without the `otel` feature so call sites don't need cfg gates.
- **Per-model `[budget]` blocks with cost-projection coverage ([#361](https://github.com/rocky-data/rocky/pull/361)).** Model sidecars now accept a `[budget]` block with the same field set as the project-level `[budget]` (`max_usd`, `max_duration_ms`, `max_bytes_scanned`, `on_breach`) — every field optional. Per-model fields are the local authority: explicitly set values override the project-level config for that one model, missing fields inherit. The precedence call extends to `on_breach` — a per-model `warn` overrides a project-level `error`, and vice versa. Implemented as a new `ModelBudgetConfig` type next to `BudgetConfig` with a `resolve(&BudgetConfig) -> BudgetConfig` API that performs field-level inheritance. `rocky preview cost` now extends `PreviewCostOutput` with a strictly-additive `projected_per_model_budget_breaches: Vec<PerModelBudgetBreachOutput>` field — each entry carries `model_name`, `limit_type`, the resolved `limit` / `actual` pair, and the resolved `on_breach`. Existing `projected_budget_breaches` continues to surface only project-level breaches, so direct JSON consumers do not break. Markdown rendering (consumed by `.github/actions/rocky-preview/`) extends the existing "Budget projection" section with a per-model breach subtable; the section header flips to "would fail the run" when any per-model breach has `on_breach = "error"` even when the project-level breach is advisory. `rocky preview cost` gains a `--models` flag (default `models`) so it can load sidecar `[budget]` blocks; missing or malformed model directories silently degrade to project-level-only projection.
- **Snowflake checksum-bisection support ([#357](https://github.com/rocky-data/rocky/pull/357)).** `SnowflakeSqlDialect::row_hash_expr` (`COALESCE(HASH("col_a", "col_b", ...), 0)` — Snowflake's proprietary 64-bit non-cryptographic hash, variadic, positionally NULL-aware; the `COALESCE` guards the all-NULL row case so `BITXOR_AGG`'s NULL-skipping doesn't drop legitimate chunk contributions) and `SnowflakeSqlDialect::quote_identifier` (double quotes — survives Snowflake's case-fold-on-unquoted-identifiers rule) plus a Snowflake-specific `WarehouseAdapter::checksum_chunks` override land. The override emits Snowflake-canonical SQL — `BITXOR_AGG` (Snowflake's aggregate equivalent of `BIT_XOR`, which Snowflake does not have), `INTEGER` cast (Snowflake's alias for `NUMBER(38, 0)`), the `LEAST(K-1, FLOOR(...))` clamp matching the kernel's last-chunk-absorbs-remainder contract. Live-verified end-to-end against a real Snowflake instance: noop diff bottoms out at `K` chunk checksums per side; planted change at row 4,242 of a 10k-row fixture is found by every run.
- **Snowflake adapter — PAT auth + four latent bug fixes ([#357](https://github.com/rocky-data/rocky/pull/357)).** New `pat: Option<RedactedString>` field on the Snowflake `AdapterConfig` plumbs Snowflake's `PROGRAMMATIC_ACCESS_TOKEN` auth flow through Rocky — set `pat = "${SNOWFLAKE_PAT}"` in `rocky.toml`. New `AuthInner::Pat` + `TokenType::Pat` (→ `PROGRAMMATIC_ACCESS_TOKEN` token-type header) + highest-priority detection in `Auth::from_config`. Auth priority is now PAT > OAuth > Key-pair JWT > Password. Companion adapter fixes (surfaced by live verification): HTTP clients now send a non-empty `User-Agent` (`rocky/<crate-version>`); `check_terminal` recognises Snowflake's `09xxxx` success-with-info codes (e.g. `090001` for DDL "Statement executed successfully") in addition to ANSI `00000`; `format_table_ref` now quotes each component via `quote_identifier` so case-preserving identifiers survive Snowflake's case-fold-on-unquoted rule. Test schema-prefix renamed `hc_*` → `rocky_bisection_*` across all three adapters' `bisection_live.rs`.
- **Snowflake zero-copy `CLONE` override for `clone_table_for_branch` ([#364](https://github.com/rocky-data/rocky/pull/364)).** Completes the Phase 5 warehouse-native clones sweep — Databricks `SHALLOW CLONE` (#305), BigQuery `CREATE TABLE … COPY` (#309), and now Snowflake `CREATE TABLE … CLONE`. `rocky branch create` on Snowflake is metadata-only at create time (Snowflake shares micropartitions with the source until copy-on-write divergence), making branch creation effectively free regardless of source size. SQL form: `CREATE TABLE "<db>"."<branch_schema>"."<table>" CLONE "<db>"."<src_schema>"."<table>"`. Uses `CREATE TABLE` (not `CREATE OR REPLACE TABLE`) — pre-existing target signals a caller bug (e.g. branch not torn down between runs); silent overwrite would mask the issue. Identifier components validated through `rocky_sql::validation::validate_identifier` and double-quoted per Snowflake convention. Live-verified end-to-end against a real Snowflake instance.
- **End-of-run auto-sweep for state-store retention ([#355](https://github.com/rocky-data/rocky/pull/355)).** `rocky run` now invokes `StateStore::sweep_retention` at the end of every run so the policy declared in `[state.retention]` (introduced in 1.22.0) "just works" without an external cron. The sweep is gated by a `last_retention_sweep_at` metadata key in the state store: subsequent runs within `sweep_interval_seconds` (default 3600 = 1h) skip without doing any work, so a project running every minute does not pay sweep cost on every invocation. A `sweep_budget_ms` knob (default 5000 = 5s) controls the soft budget — the sweep always runs to completion, but exceeding the budget flips the per-run log line from `tracing::debug` to `tracing::warn` so operators can spot a state store grown large enough to warrant a manual `rocky state retention sweep` outside the normal run loop. Auto-sweep failures (cannot open state store, sweep itself errors) are swallowed as `tracing::warn` and never mask the run's exit code. Setting `applies_to = []` disables the auto-sweep without touching the manual subcommand. The two new `[state.retention]` fields (`sweep_interval_seconds`, `sweep_budget_ms`) are additive and default-populated — existing configs upgrade without changes.

### Changed

- **`dag_node` span renamed to `materialize.table` ([#362](https://github.com/rocky-data/rocky/pull/362)).** `crates/rocky-core/src/dag_executor.rs` previously emitted the per-DAG-node span as `info_span!("dag_node", id, kind)`. Renamed to `info_span!("materialize.table", rocky.model.fqn, rocky.model.kind)` following OTel `<verb>.<resource>` span-name and `<vendor>.<resource>.<name>` attribute conventions, so dashboards across this wave's instrumentation pivot consistently. Operators with span-name filters watching for `dag_node` need to migrate to `materialize.table`.

### Fixed

- **`rocky.table_duration_*` / `rocky.query_duration_*` recorded as histograms not gauges ([#358](https://github.com/rocky-data/rocky/pull/358)).** The OTLP metrics exporter previously emitted in-process `p50` / `p95` / `max` aggregations as `u64_gauge` instruments per duration metric (six gauges across table + query). Two issues: (1) information loss at the OTLP boundary — backends received only the in-process aggregations and could not recompute percentiles across hosts, runs, or time windows; (2) conflicts with OTel semantic conventions — duration metrics are spec'd as histogram instruments, gauges break out-of-the-box visualisations and aggregation queries downstream. Switched to two `u64_histogram` instruments (`rocky.table_duration_ms`, `rocky.query_duration_ms`) that record raw per-observation values. A per-exporter cursor over the existing `Mutex<Vec<u64>>` in `RunMetrics` keeps the periodic flush idempotent (no double-counting between flushes) without draining the source slice — the run-end JSON snapshot, which percentile-folds the same `Vec`, is unchanged for non-OTel consumers (dagster integration, terminal output). New non-draining accessors `RunMetrics::read_table_durations` / `read_query_durations` plus a small `advance_cursor` helper.

### Internal

- **Typed-program primitives relocated from `rocky-compiler` to `rocky-core` ([#354](https://github.com/rocky-data/rocky/pull/354)).** PR-1 of Typed-IR Option B Phase 1. Moves `TypedColumn`, `RockyType`, `StructField`, `common_supertype`, `is_assignable` (now in `rocky-core/src/types.rs`) and `LineageEdge`, `QualifiedColumn` (now in `rocky-core/src/lineage.rs`) so the upcoming `ModelIr` / `ProjectIr` types in `rocky-core` can reference them without creating a `rocky-compiler` → `rocky-core` dependency cycle. `rocky-compiler` re-exports keep all 12 external import sites compiling unchanged. **No public API change**; no behaviour change; no edits to `rocky-cli/output.rs` or any downstream caller.

## [1.22.0] — 2026-05-02

Feature release. Headline: **exhaustive checksum-bisection diff** — `rocky preview diff --algorithm bisection` covers every row in a target table at bounded scan cost, ending the sampling false-negative ceiling. Live-verified on DuckDB, BigQuery, and Databricks; Snowflake stays on the sampled fallback until a consumer drives the override. Bundles pre-merge budget projection on `rocky preview cost`, rolling z-score + health score on `rocky history`, a state-store retention sweep, and a tagged-enum restructure of `PreviewDiffOutput` (the breaking shape change downstream consumers need to migrate for).

**Migration callout (breaking JSON-shape change).** `PreviewDiffOutput.models[*].sampled` and `.sampling_window` move under a new `algorithm` discriminated union. Direct JSON readers must migrate from `model.sampled.rows_changed` to `model.algorithm.sampled.rows_changed` (with a `kind` discriminator check first). The dagster typed-resource layer and the VS Code extension's adapter absorb the shape change automatically through `dagster-rocky` 1.20.0's regenerated bindings — only third-party scripts that read the JSON directly need to update.

### Changed

- **`PreviewDiffOutput.models[*].algorithm` tagged enum (breaking JSON-shape restructure).** Per-model row-level diff is now carried in a `PreviewModelDiffAlgorithm` discriminated union with two variants: `{ "kind": "sampled", "sampled": …, "sampling_window": … }` and `{ "kind": "bisection", "diff": …, "bisection_stats": … }`. The legacy `model.sampled` and `model.sampling_window` fields move under `model.algorithm` — direct JSON consumers (shell scripts, custom dashboards) must migrate to `model.algorithm.sampled.*` / `model.algorithm.bisection.*` with a `kind` discriminator check. The dagster typed-resource layer absorbs the change automatically through `dagster_rocky.types_generated`; the VS Code extension's adapter regenerates via `just codegen` and stays consumer-clean. `summary.any_coverage_warning` keeps its name and widens semantically — it now fires on `Sampled { sampling_window.coverage_warning: true }` *or* `Bisection { bisection_stats.depth_capped: true }`. The `--algorithm=bisection` CLI flag (previously `tracing`-log-only) now surfaces results as the typed `Bisection` variant on the JSON output.

### Added

- **State-store retention policy.** New `[state.retention]` config block (`max_age_days = 365`, `min_runs_kept = 100`, `applies_to = ["history", "lineage", "audit"]` defaults) bounds the size of `state.redb`. New `rocky state retention sweep [--dry-run]` subcommand drops run-history, DAG-snapshot, and quality-snapshot rows that exceed the policy, while always preserving the most recent `min_runs_kept` rows in each domain. Operational tables (schema cache, watermarks, partitions, idempotency keys) are never touched. `RetentionSweepOutput` lands in the JSON-output cascade. Auto-sweep at end-of-run is deferred to a follow-up — the manual subcommand is shippable on its own.
- **Databricks checksum-bisection support.** `DatabricksSqlDialect::row_hash_expr` (`xxhash64(\`col_a\`, \`col_b\`, ...)` — Spark's multi-arg form, positional NULL-aware + type-aware, no `concat_ws` collision risk) and `DatabricksSqlDialect::quote_identifier` (backticks — works under both `ANSI_MODE = on` and `off`) plus a Databricks-specific `WarehouseAdapter::checksum_chunks` override land. The override emits Databricks-canonical SQL — backtick-quoted columns, `BIGINT` cast (Spark's native integer type), the `LEAST(K-1, FLOOR(...))` clamp matching the kernel's last-chunk-absorbs-remainder contract. Unit-tested via 5 new dialect tests; live verification gates merge — `tests/bisection_live.rs` is `#[ignore]`-gated and runs locally with `DATABRICKS_HOST` + `DATABRICKS_HTTP_PATH` + `DATABRICKS_TOKEN` + `DATABRICKS_TEST_CATALOG` set.
- **BigQuery checksum-bisection support.** `BigQueryDialect::row_hash_expr` (`FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(...)))`) and `BigQueryDialect::quote_identifier` (backticks) plus a BQ-specific `WarehouseAdapter::checksum_chunks` override land. The override emits BigQuery-canonical SQL — backtick-quoted columns, `FLOAT64` / `INT64` casts, the `LEAST(K-1, FLOOR(...))` clamp matching the kernel's last-chunk-absorbs-remainder contract. Live-verified against a real BigQuery sandbox: noop diff bottoms out at `K` chunk checksums per side, planted change at row 4,242 of a 10k-row fixture is found by every run. `tests/bisection_live.rs` is `#[ignore]`-gated; runs locally with `BIGQUERY_TEST_PROJECT` + `GOOGLE_APPLICATION_CREDENTIALS` set.
- **`SqlDialect::quote_identifier` on `rocky-core`.** New trait method that returns the dialect's identifier-quoting style. Default is double quotes (DuckDB / Snowflake / Databricks); BigQuery overrides to backticks. The kernel's checksum-bisection SQL (chunk checksums + leaf-row fetch + null-PK count) now routes column quoting through this method instead of hardcoding `"col"`. Required because BigQuery treats `"col"` as a STRING literal — running the kernel default on BQ would silently compare the literal text `"id"` to a numeric and throw a type-mismatch at SQL plan time.
- **Hidden `--algorithm=bisection` flag on `rocky preview diff`.** Wires the checksum-bisection kernel through to the preview flow — for every model in the branch run that declares a single-column `unique_key` on a Merge strategy, Rocky now runs an exhaustive row-level diff between the branch schema (`branch__<branch_name>`) and the model's declared base schema. Result is logged via `tracing::info` (target `rocky::preview::bisection`); the `PreviewDiffOutput` JSON shape is unchanged in this change. PK column comes from the model sidecar's `unique_key`; non-PK column list is discovered via `DESCRIBE TABLE` on the branch side (falling back to base). Models without a single-column integer PK skip with a `tracing::warn`. The flag is hidden from `--help` because the surface is opt-in until the output-schema swap lands.
- **Pre-merge budget projection on `rocky preview cost`.** `PreviewCostOutput.projected_budget_breaches` reports run-level `[budget]` breaches (`max_usd` / `max_duration_ms` / `max_bytes_scanned`) projected against the branch totals — populated only when the project declares a `[budget]` block. Mirrors the `RunOutput.budget_breaches` shape so the same downstream consumers (PR-comment templates, JSON listeners) can process both with one code path. The `PreviewCostSummary` gains `total_branch_duration_ms` + `total_branch_bytes_scanned` so all three budget dimensions can be projected at preview time. Markdown rendering surfaces a "Budget projection" section only when breaches exist; framing flips from "warning" to "would fail the run" based on `[budget].on_breach`. Pure read-only helper `project_budget_breaches` lives at `rocky_cli::commands::preview` for reuse outside the preview flow.
- **Checksum-bisection diff kernel.** `rocky_core::compare::bisection::bisection_diff` walks the `K`-fanout chunk lattice over a single-column integer / numeric primary key, recurses into mismatched chunks until the leaf threshold, then materializes-and-diffs the leaves row-by-row. Datafold-style: a no-op diff bottoms out at `K` chunk checksums per side; a single-row change recurses to the row in `O(K · log_K(N))` chunks examined. Conformance suite at `engine/crates/rocky-duckdb/tests/bisection_conformance.rs` pins determinism, the no-op cost bound, and a 100k-row planted-change fixture.
- **Adapter trait surface for bisection.** `WarehouseAdapter::checksum_chunks` (with `recommended_leaf_size` + `recommended_uuid_threshold` getters) on both `rocky-core` and the public-stable `rocky-adapter-sdk`. The in-tree default emits a portable `BIT_XOR(<row_hash>)`-over-`FLOOR((pk - lo) / step)` query that composes with `SqlDialect::row_hash_expr`; the SDK default returns "not supported" so out-of-tree adapters surface a clear error until they wire their native hash. New `ChunkChecksum`, `PkRange`, `SplitStrategy` types are re-exported from both crates.
- **`SqlDialect::row_hash_expr`** on `rocky-core::traits::SqlDialect`. Returns the per-dialect SQL fragment for a single-row hash usable under `BIT_XOR(...)`. Default is an explicit not-supported error so adapters that haven't overridden surface a helpful message rather than emitting broken SQL. DuckDB ships with the override (`CAST(hash(...) AS HUGEINT)`); the other in-tree adapters add their native hash in follow-up changes.
- **`rocky history --model <name> --rolling-stats [--window N]`**. When `--rolling-stats` is set, `ModelHistoryOutput` gains a `rolling_stats` block with per-dimension statistics (`mean`, `std_dev`, `latest_z_score`) for `duration_ms` and `rows_affected`, plus a composite `health_score`. Health score is `1.0 - clamp((max(|z_rows|, |z_dur|) - 2) / 4, 0, 1)` — `1.0` when both z-scores are within 2σ, `0.0` at 6σ or beyond. Only successful executions contribute; `--window` (default 20) controls the look-back depth.

### Notes for downstream consumers

- **`rocky-adapter-sdk` trait surface gained one method** (`checksum_chunks`) with a default-error impl and two utility getters with safe defaults (`recommended_leaf_size`, `recommended_uuid_threshold`). Out-of-tree adapters that don't override get a "not supported" error from `checksum_chunks`, which is the intended state until each adapter wires its native hash. Treat as a minor SDK bump at the next engine release cut; no source-level breaking changes.

## [1.21.0] — 2026-05-01

Feature release. Headline: **BigQuery adapter promoted out of experimental** — every BQ-specific dialect surface is now live-verified end-to-end (full-refresh, time-interval DML transactions, MERGE bootstrap, region-qualified discovery, drift across all three actions, cost cross-check). `MaterializationOutput.job_ids` lets consumers cross-check rocky-reported bytes against the warehouse console. Bundles the FR-021 explicit-separator grammar in schema-pattern templates.

### Added

- **`MaterializationOutput.job_ids: Vec<String>`** ([#337](https://github.com/rocky-data/rocky/pull/337)). Each materialization now surfaces the warehouse-side job IDs of the statements it issued, accumulated alongside `bytes_scanned` / `bytes_written`. Lets orchestrators cross-check rocky's reported figures against the warehouse console (`bq show -j`, Snowflake query history, Databricks SQL warehouse history) without having to scrape stderr. Empty `[]` for adapters that don't surface a job id.
- **`BigQueryDiscoveryAdapter` + replication-from-BigQuery** ([#327](https://github.com/rocky-data/rocky/pull/327)). BigQuery now satisfies both the `WarehouseAdapter` and `DiscoveryAdapter` traits — `adapter_capability.rs` reports `BOTH`. Discovery enumerates datasets via region-qualified `<project>.region-<location>.INFORMATION_SCHEMA.SCHEMATA` because the unqualified form silently misses cross-region datasets. Replication-from-BQ pipelines work end-to-end (`pipeline.type = "replication"` against a BQ source).
- **`is_safe_type_widening` + `alter_column_type_sql` on the `SqlDialect` trait** ([#332](https://github.com/rocky-data/rocky/pull/332)). Default impls live on the trait; each adapter declares its own widening semantics + SQL emit. The BigQuery dialect override accepts only the strict lossless promotions BQ supports via `ALTER COLUMN SET DATA TYPE`: `INT64 → NUMERIC`, `INT64 → BIGNUMERIC`, `NUMERIC → BIGNUMERIC`. Lossy conversions (`… → FLOAT64`) and unsupported targets (`… → STRING`, despite being lossless at the value level — BQ's ALTER rejects this) fall through to `drop_and_recreate`.
- **`{name:SEP}` placeholder grammar in schema-pattern templates** ([#339](https://github.com/rocky-data/rocky/pull/339)). `ParsedSchema::resolve_template` now accepts an optional explicit join separator at the use site: `{name:SEP}` joins multi-valued components with the literal string `SEP` instead of the caller-supplied default. Closes a footgun where the same placeholder rendered differently across call sites — `target.{catalog,schema}_template` joins with `target.separator`, while `metadata_columns.value` joins with `pattern.separator`, so `{regions}` could resolve to `us_west_us_east` in one TOML field and `us_west__us_east` in another. Pinning the separator at the use site (`{regions:_}`) makes the rendered value stable across config changes — important for templates feeding hash functions (RLS keys, audit hashes). Bare `{name}` is unchanged: every existing template renders identically. The grammar applies uniformly to `catalog_template`, `schema_template`, and `metadata_columns.value`.

### Changed

- **BigQuery adapter no longer marked experimental** ([#335](https://github.com/rocky-data/rocky/pull/335)). The `is_experimental` override is dropped from the adapter trait impl; `rocky validate` / `rocky run` no longer print the `experimental adapter` warning for BQ pipelines. Every BQ-specific dialect surface is now live-verified end-to-end via the smoke-test suite at `examples/playground/pocs/07-adapters/05-bigquery-native-queries/live/`.
- **`detect_drift` populates `added_columns: Vec<ColumnInfo>`** ([#331](https://github.com/rocky-data/rocky/pull/331)). Prior `detect_drift` only reported drifted (type-changed) columns. The runtime now issues `ALTER TABLE ADD COLUMN` for each added source column before the next INSERT instead of failing the run. `add_columns` is the third action alongside `drop_and_recreate` and `alter_column_types`. Signature change: `detect_drift` takes `&dyn SqlDialect` so each adapter declares its own `add_column_sql`.

### Fixed

- **BigQuery time-interval emits a single semicolon-joined script** ([#323](https://github.com/rocky-data/rocky/pull/323)). The BigQuery REST API is stateless: `BEGIN TRANSACTION` / `COMMIT TRANSACTION` issued as separate `jobs.query` calls are rejected with `Transaction control statements are supported only in scripts or sessions`. The dialect now emits the full `BEGIN TRANSACTION; DELETE; INSERT; COMMIT TRANSACTION` flow as one statement so BQ runs it as a script. Other dialects (Databricks, Snowflake, DuckDB) keep the per-statement shape.
- **`generate_transformation_initial_ddl` wired for MERGE first-run bootstrap** ([#324](https://github.com/rocky-data/rocky/pull/324)). MERGE on a fresh target needs a CREATE TABLE bootstrap before the first MERGE statement runs; the helper existed but had no caller in the transformation path. The runtime now invokes it on the first-run path so MERGE pipelines no longer error with `table not found` on their initial run.
- **BigQuery `bytes_scanned` reflects `totalBytesBilled`, not `totalBytesProcessed`** ([#330](https://github.com/rocky-data/rocky/pull/330)). `execute_statement_with_stats` now follows up `jobs.query` with a `jobs.get` call to enrich the response with the full `statistics` block (where `totalBytesBilled` lives, with the 10 MB minimum-bill floor applied). Sub-10 MB queries now report the floor (which matches the actual GCP charge) instead of the few-hundred-byte raw scan figure.
- **BigQuery transformation pipelines populate `cost_usd` / `bytes_scanned`** ([#326](https://github.com/rocky-data/rocky/pull/326)). The transformation run path on BQ was dropping `ExecutionStats` on the floor; the per-model `MaterializationOutput` now carries real cost figures, matching the replication path.
- **`alter_column_types` drift action wired through the runtime** ([#333](https://github.com/rocky-data/rocky/pull/333)). The detection branch existed in `drift::detect_drift` but the runtime in `run.rs` only wired `drop_and_recreate`; safe widenings fell through to the next `INSERT` and could fail. The handler now has three branches (`DropAndRecreate` / `AlterColumnTypes` / `add_columns`) with `job_ids_acc` accumulators alongside bytes.
- **`rocky archive --model <name>` validates the model name** ([#321](https://github.com/rocky-data/rocky/pull/321)). Was passing the user string straight into SQL formatting; now goes through `validate_identifier`. The `excluded_tables` field is also marked optional in the dagster Pydantic model to match the engine's `Option<Vec<String>>`.

### Docs

- **`CONFORMANCE.md` for `rocky-bigquery`** ([#334](https://github.com/rocky-data/rocky/pull/334)). New per-adapter doc mapping each `rocky_adapter_sdk::conformance` test category to the live driver under `examples/playground/pocs/07-adapters/05-bigquery-native-queries/live/` that exercises it.

## [1.20.1] — 2026-05-01

Patch release. Root-cause fix for Fivetran auto-rename collisions surfacing as duplicate `DiscoveredTable` records in `rocky discover` output.

### Fixed

- **Fivetran discover: dedupe table records per source.** The Fivetran `/v1/connectors/{id}/schemas` response can list two distinct schema-entry keys that resolve to the same destination table name (e.g. an auto-rename leaves the original logical key alongside a fresh entry whose `name_in_destination` matches the renamed table). `SchemaConfig::enabled_tables()` faithfully forwards both rows, which left duplicate `DiscoveredTable` records in the discover output and broke downstream consumers — Dagster's `multi_asset` rejects duplicate `AssetCheckSpec`s, which crashed the entire `RockyComponent` build for affected pipelines. The Fivetran adapter now dedupes the per-connector table list by name (preserving first occurrence) with a WARN log carrying the connector id and the duplicate count, so the upstream-config quirk stays visible to operators rather than getting silently swallowed.

## [1.20.0] — 2026-05-01

Feature release. Headline: **`rocky compact --catalog <name>` and `rocky archive --catalog <name>`** turn the two storage-maintenance commands from per-table-only into per-catalog-aware. Multi-catalog deployments that drove weekly compaction sweeps had to reimplement catalog → schema → table enumeration in the orchestrator and fan out one subprocess per table; now one CLI invocation per catalog returns aggregated SQL. Bundled with a clap UX cleanup so `rocky compact` with no scope errors with every valid alternative listed.

### Added

- **`rocky compact --catalog <name>` and `rocky archive --catalog <name>`** ([#315](https://github.com/rocky-data/rocky/pull/315)). New scope flag that resolves the set of Rocky-managed tables in a single catalog from the pipeline config (replication discovery or transformation model files — no warehouse round trip) and aggregates per-table OPTIMIZE/VACUUM (compact) or DELETE/VACUUM (archive) SQL into one JSON envelope. Replaces the consumer-side `SHOW SCHEMAS`/`SHOW TABLES` enumeration that multi-catalog deployments otherwise have to reimplement to drive a weekly maintenance sweep — one subprocess call per catalog instead of one per table. Mutually exclusive with `--model` (and, for compact, `--measure-dedup`). Empty matches error with the available catalogs listed so consumer typos don't silently turn into no-ops.
- **`CompactOutput` / `ArchiveOutput` carry per-catalog aggregation fields** ([#315](https://github.com/rocky-data/rocky/pull/315)). New optional `catalog`, `scope`, `tables` (per-FQN statement bundles), and `totals` (`table_count` + `statement_count`) fields populated only on `--catalog` invocations. Single-model envelopes are unchanged (the new fields are `skip_serializing_if = "Option::is_none"`); the flat top-level `statements` list still carries every SQL statement across every table for consumers that just iterate it.

### Changed

- **`CompactOutput.model` is now optional** ([#315](https://github.com/rocky-data/rocky/pull/315)). Was required (`String`); now `Option<String>` so the catalog-scope path can omit it. JSON schema relaxes from required to optional; existing single-model JSON output is byte-stable. Pydantic + TypeScript bindings regenerated accordingly.
- **`rocky-cli/src/scope.rs`** ([#315](https://github.com/rocky-data/rocky/pull/315)). The managed-table resolver previously private to `commands/compact.rs` (`resolve_managed_tables`, `resolve_replication_managed_tables`, `resolve_transformation_managed_tables`, `managed_catalog_set`) lifted into a shared module so both `compact` (`--measure-dedup`, `--catalog`) and `archive` (`--catalog`) reuse one implementation. Pure refactor for the dedup path — its tests come along.
- **`rocky compact` no-scope error message lists every valid scope** ([#316](https://github.com/rocky-data/rocky/pull/316)). Replaces clap's `required_unless_present_any` + per-flag `conflicts_with` declarations with a single `ArgGroup` (`compact_scope`) over `model` / `catalog` / `measure_dedup`. Running `rocky compact` with no scope now errors with `<MODEL|--catalog <CATALOG>|--measure-dedup>` instead of just `<MODEL>`, so the user sees that `--catalog` and `--measure-dedup` are valid alternatives. Mutual exclusion is unchanged — the group's `multiple(false)` enforces it.

## [1.19.2] — 2026-04-30

CI-only re-cut of `1.19.1`. Source code is identical to `1.19.1`; binary semantics unchanged. Adds the missing Windows archive (and the `SHA256SUMS` checksums file gated on it) that the `1.19.1` matrix run failed to produce because `aws-lc-sys` (transitive dep of `jsonwebtoken` via the `aws_lc_rs` feature flag) needs NASM on the Windows runner to assemble AWS-LC's optimized crypto kernels — the macOS / Linux toolchains fall back to a YASM-equivalent that ships by default. The release workflow now installs NASM on the Windows job before `cargo build`. macOS / Linux v1.19.1 binaries are correct and stay valid; this release just fills in the cross-platform set.

## [1.19.1] — 2026-04-30

Patch release. Headline: **`clone_table_for_branch` warehouse-native overrides on Databricks (`SHALLOW CLONE`) and BigQuery (`CREATE TABLE … COPY`)** turn the per-PR branch substrate from a portable CTAS that re-scans bytes into a metadata-only operation that's effectively zero-cost at create time. Snowflake stays on the CTAS default (still correct, just slower) until a Snowflake consumer asks for the native `CLONE` path.

### Added

- **`WarehouseAdapter::clone_table_for_branch` trait method + DuckDB CTAS default** ([#303](https://github.com/rocky-data/rocky/pull/303)). New trait surface that `rocky preview create` uses to populate the per-PR branch schema for every model not in the prune set. Default impl is a portable `CREATE OR REPLACE TABLE "{branch}"."{table}" AS SELECT * FROM "{src_schema}"."{table}"` that works on DuckDB and any warehouse whose CTAS accepts the two-part `schema.table` form without a catalog prefix. Adapters with native zero-copy primitives override. Identifier-validation guard on every component via `rocky_sql::validation`.
- **Databricks `SHALLOW CLONE` override of `clone_table_for_branch`** ([#305](https://github.com/rocky-data/rocky/pull/305)). Replaces the portable CTAS with `CREATE OR REPLACE TABLE … SHALLOW CLONE …` so the per-PR branch lands in `<source.catalog>.<branch_schema>.<source.table>` as a metadata-only reference to the source's underlying files. Live-verified end-to-end against a real Databricks workspace (~12s wall clock for the integration test).
- **BigQuery `CREATE TABLE … COPY` override of `clone_table_for_branch`** ([#309](https://github.com/rocky-data/rocky/pull/309)). BigQuery's native metadata-only table copy primitive — strictly dominates the CTAS default, which would re-scan the source bytes. Three-part backtick quoting (`` `project`.`dataset`.`table` ``); branch table lands in the same GCP project as the source (BigQuery's `COPY` is single-project-scoped). Live-verified end-to-end against a real BigQuery sandbox (~12s wall clock).
- **`rocky run --idempotency-key` env-var fallback** ([#307](https://github.com/rocky-data/rocky/pull/307)). The CLI flag now falls back to `ROCKY_IDEMPOTENCY_KEY` when not given. Useful for orchestrators that already plumb an idempotency key through env (cron wrappers, pod templates, ad-hoc CI scripts). Workspace `clap` features grew `["env"]`.
- **`IcebergError` taxonomy on the discovery adapter's `failed_sources` classifier** ([#307](https://github.com/rocky-data/rocky/pull/307)). The classifier was hard-coded to `Unknown` pending wire-error classification; now mirrors the Fivetran taxonomy (`Transient | Timeout | RateLimit | Auth | Unknown`) keyed on `IcebergError` variants and HTTP status codes. Downstream consumers of `failed_sources` get the same back-pressure / alerting signals from Iceberg as they do from Fivetran. Three new wiremock integration tests cover `401 → Auth`, `403 → Auth`, and `404 → Unknown` paths.
- **`rocky_sql::validation::validate_gcp_project_id`** ([#310](https://github.com/rocky-data/rocky/pull/310)). New validator honouring GCP's actual project-ID rules (`^[a-z][a-z0-9-]{4,28}[a-z0-9]$`). The strict `validate_identifier` rejected hyphens, which made the BigQuery adapter unusable against any real GCP project.

### Fixed

- **`jsonwebtoken` 10.x crypto-provider panic on first JWT signing** ([#310](https://github.com/rocky-data/rocky/pull/310)). Workspace pin grew `["aws_lc_rs", "use_pem"]` features so the BigQuery service-account auth path doesn't panic with `CryptoProvider::install_default()` on its first call.
- **BigQuery adapter rejects real GCP project IDs containing hyphens** ([#310](https://github.com/rocky-data/rocky/pull/310)). `BigQueryAdapter::{describe_table, list_tables, clone_table_for_branch}` and `BigQueryDialect::{format_table_ref, create_schema_sql, list_tables_sql}` now use `validate_gcp_project_id` for the project component. Dataset and table names stay on the strict identifier rule.

### Changed

- **Schema-cache doc-comment cleanup** ([#307](https://github.com/rocky-data/rocky/pull/307)). Internal task-tracking labels stripped from ~25 doc-comments and inline comments across the schema-cache codebase. Surrounding descriptive prose preserved. Description text on a few `JsonSchema`-deriving structs (`SchemaCacheConfig`, `CacheConfig`, `schemas_cached`) propagated through the codegen cascade — pure description-text drift, no field added / removed / renamed.

## [1.19.0] — 2026-04-30

Feature release. Headline: **`rocky lineage-diff`** — a new top-level CLI verb that combines `rocky ci-diff`'s structural per-column diff with `rocky lineage --downstream`'s blast-radius walk, emitting a PR-comment-ready Markdown summary of which downstream columns each PR change reaches. Closes the launch-thread commitment to `xiaoher-c` ([HN item 47935246](https://news.ycombinator.com/item?id=47935246)).

Bundled with the public adapter SDK guide + Rust-native skeleton POC that close the launch-thread commitment to `hasyimibhar` (community-driven adapter contributions are now genuinely *"tractable through the Adapter SDK without engine patching"*).

### Added

- **`rocky lineage-diff [<base_ref>]`** ([#298](https://github.com/rocky-data/rocky/pull/298)). New top-level CLI verb. Internally factors a `pub(crate) compute_ci_diff` out of `commands/ci_diff.rs::run_ci_diff` returning `CiDiffData { summary, results, head_compile, changed_file_count }` so both `ci-diff` and `lineage-diff` run git + compile once and share the diff result. `commands/lineage_diff.rs::run_lineage_diff` enriches each `ColumnDiff` via `semantic_graph::trace_column_downstream(model, col)` and dedupes consumers by `(target.model, target.column)`. New `LineageDiffOutput` schema in `output.rs` with sibling `LineageDiffResult` + `LineageColumnChange`; reuses `LineageQualifiedColumn`. Pre-rendered Markdown lives on `LineageDiffOutput.markdown` so a CI-side caller posts the PR comment without re-parsing. v1 trace direction is downstream-from-HEAD only — removed columns report empty consumer sets and the Markdown row reads `_(removed; not traceable on HEAD)_`. Codegen cascade clean (1 new schema → 1 Pydantic file → 1 TypeScript file).
- **Adapter SDK guide + Rust-native skeleton POC** ([#300](https://github.com/rocky-data/rocky/pull/300)). New `docs/src/content/docs/guides/adapter-sdk.md` (eight sections: when-to-use, full trait surface table, worked example, auth + connection, testing, distribution, gotchas, next steps) + standalone POC at `examples/playground/pocs/07-adapters/05-rust-native-adapter-skeleton/`. POC is intentionally NOT a workspace member — models the out-of-tree consumer experience. ClickHouse-shaped: backtick quoting, two-part names, no `MERGE`, partition replace via `ALTER TABLE ... DELETE` + `INSERT`. 11 unit tests; `cargo run --example demo` prints generated SQL end-to-end against an in-memory `MockBackend`. The guide is honestly upfront about three current SDK gaps (two-trait-surface confusion between `rocky-adapter-sdk` and `rocky-core`; static adapter registry; `conformance::run_conformance` is a checklist, not a runner) — tracked as backlog items for future SDK work.
- **3 new demo GIFs + lineage-diff POC** ([#301](https://github.com/rocky-data/rocky/pull/301)). VHS-rendered `demo-classification-masking.gif` (governance), `demo-incremental-watermark.gif`, `demo-lineage-diff.gif` embedded in the top-level README + relevant POC READMEs. New POC at `examples/playground/pocs/06-developer-experience/11-lineage-diff/` sets up a self-contained scratch git repo with baseline + feature branch and runs `rocky lineage-diff main` to surface 5 column changes across 2 modified models.

### Changed

- **`commands/ci_diff.rs::run_ci_diff` refactored to share its body via `compute_ci_diff`** ([#298](https://github.com/rocky-data/rocky/pull/298)). Pure refactor — JSON output and human-readable messaging are byte-identical to v1.18.0 (the "no changed model files detected" vs "X file(s) changed, but no model files affected" distinction is preserved via the new `changed_file_count` field on `CiDiffData`). `lineage-diff` is the second consumer.

## [1.18.0] — 2026-04-29

Feature + audit-sweep release. Headline: **`rocky preview`** ships end-to-end — a new top-level command that builds a per-PR branch pre-populated from the base ref, computes a structural + sampled-data diff against base, runs a cost-delta projection, and a companion GitHub Action posts the result as a single PR comment. Also bundles a security audit closeout (#285–#287, #290–#293), a `[budget].max_bytes_scanned` gate, and Snowflake / BigQuery auth fixes.

### Added

- **`rocky preview` workflow** ([#279](https://github.com/rocky-data/rocky/pull/279), [#280](https://github.com/rocky-data/rocky/pull/280), [#282](https://github.com/rocky-data/rocky/pull/282)). Three new subcommands — `preview create`, `preview diff`, `preview cost` — orchestrate PR-time preview environments. `create` materializes a per-PR branch and pre-populates unchanged models from the base ref via DuckDB CTAS (the Fivetran "Smart Run" technique; warehouse-native `SHALLOW CLONE` / zero-copy `CLONE` lift in a follow-up). `diff` compares branch vs. base on schema + sampled rows (built on top of the existing `rocky_core::compare` / `shadow` kernel and `ComparisonResult`). `cost` reads the latest base `RunRecord` per model from the state store (the Arc 2 wave 2 surface) and emits a per-model + total cost delta. New `*Output` schemas: `PreviewCreateOutput`, `PreviewDiffOutput`, `PreviewCostOutput` — fully typed, codegen'd Pydantic + TypeScript bindings shipped alongside.
- **`rocky-preview` GitHub Action + reference workflow** ([#281](https://github.com/rocky-data/rocky/pull/281)). Composite action at `.github/actions/rocky-preview/` that wires the three subcommands into a single PR-level summary comment, with a `<!-- rocky-preview -->` magic-string upsert so subsequent PR pushes update the same comment instead of stacking new ones.
- **`max_bytes_scanned` threshold on the `[budget]` block** ([#288](https://github.com/rocky-data/rocky/pull/288)). New optional `u64` field alongside `max_usd` and `max_duration_ms` that gates a run on the aggregate `bytes_scanned` summed across every materialization in `RunCostSummary`. Closes the post-launch user-feedback gap that "fail CI when this run scanned more than N TB" wasn't expressible — `max_usd` is correlated with scan volume but a regression that stops pruning partitions on a flat-rate warehouse can blow scan up without changing the dollar figure. New `BudgetLimitType::MaxBytesScanned` variant; the limit_type tag on each `BudgetBreach` / `BudgetBreachOutput` is `"max_bytes_scanned"`. `RunCostSummary` now carries `total_bytes_scanned: Option<u64>` so consumers can read the aggregate without re-walking `materializations`. Skipped (rather than treated as zero) when no adapter reports a byte count, matching `max_usd`.
- **`rocky serve --token`, `--allowed-origin`, `--host` flags** ([#291](https://github.com/rocky-data/rocky/pull/291)). New `auth` module gates every route except `/api/v1/health` behind a Bearer-token middleware (constant-time compare). Token sources: `--token <secret>` flag, then the `ROCKY_SERVE_TOKEN` env var. Default bind moved to `127.0.0.1:8080`; non-loopback bind without a token is refused with a clear error. CORS goes from `permissive()` to an empty-by-default allowlist populated by repeatable `--allowed-origin <ORIGIN>` flags; methods restricted to GET/POST/OPTIONS, headers to Authorization/Content-Type. Closes the LAN-leak / CSRF audit class flagged before public 1.0.

### Changed

- **`rocky-server` redb work moved off the async runtime** ([#291](https://github.com/rocky-data/rocky/pull/291)). Three handlers (`api::list_runs`, `api::model_history`, `api::model_metrics`) plus `state::recompile` and `state::load_cached_source_schemas` now run via `tokio::task::spawn_blocking`. Mirrors the LSP fix shipped in 1.17.2 (PR #263) — the same redb-flock retry path applies and we don't want it on the runtime.
- **`ProcessAdapter::call` serializes concurrent callers** ([#290](https://github.com/rocky-data/rocky/pull/290)). New `call_lock: Mutex<()>` held across the entire write→read pair so concurrent callers don't swap JSON-RPC ids. Prior behaviour could race two callers' write-then-read on the same stdin/stdout pair under heavy load; now each call is observed end-to-end.
- **BigQuery auth caches the OAuth token** ([#292](https://github.com/rocky-data/rocky/pull/292)). `BigQueryAuth::ServiceAccount` becomes a struct variant with `cached_token: Arc<RwLock<Option<CachedToken>>>` and a 60s `REFRESH_SLACK`, mirroring the Databricks pattern. `expires_in` plumbed end-to-end; new `invalidate_cache()` for post-401 retries. Constructor moved from `BigQueryAuth::ServiceAccount(key)` to `BigQueryAuth::service_account(key)`.

### Fixed

- **SQL identifier validation across the engine** ([#293](https://github.com/rocky-data/rocky/pull/293)). Threads `validate_identifier` through every `format!`-with-SQL site in the warehouse adapters and execution engine. `rocky-bigquery` `describe_table` / `list_tables`, `rocky-databricks` `format_target` / `format_options_clause`, `rocky-snowflake` `format_target` / `create_external_stage_sql` / `put_file_sql` / `file_format_clause`, `rocky-engine` `executor` / `profile.generate_profile_sql` — all now refuse injection-bearing input rather than escaping it. New `validate_cloud_uri` / CSV-delimiter guards reject embedded quotes, backslashes, and newlines in URI / delimiter literal contexts. `rocky-lang/lower.rs` is unchanged because `Token::Ident`'s lexer regex is strictly tighter than `validate_identifier` (documented in the module header).
- **`rocky-snowflake` auth-token-type header per mode** ([#283](https://github.com/rocky-data/rocky/pull/283)). The connector previously hard-coded `KEYPAIR_JWT` for every auth mode, so OAuth and password-mode connections silently sent the wrong `X-Snowflake-Authorization-Token-Type` header and Snowflake rejected them with a misleading auth error. Header now matches the active auth strategy.
- **`rocky-lang` parser recursion depth bounded at 256** ([#286](https://github.com/rocky-data/rocky/pull/286)). Prevents stack overflow on deeply-nested adversarial input (closes a WASM crash class).
- **`rocky-ai` `api_key` redaction + `testgen` path canonicalization + retry-token cap** ([#287](https://github.com/rocky-data/rocky/pull/287)). `api_key` no longer surfaces in `Debug` / `Serialize` output; `testgen` canonicalizes write paths; retry budget caps total tokens spent on a single request.
- **Hook subprocess `kill_on_drop(true)` + HTTP timeouts on Fivetran/Airbyte/Iceberg/SCIM clients + branch HashSet diff** ([#285](https://github.com/rocky-data/rocky/pull/285)). Hook subprocesses now actually die when the parent drops; source-adapter HTTP clients gain explicit timeouts so a hung remote can no longer block a discover run forever. `branch.rs` HashSet diff replaces an O(n²) Vec scan.
- **SQL transpile word-boundary + comment / string-literal awareness, WASM UTF-8 panic, `explain` TOML clobber, partial-success exit code 2** ([#292](https://github.com/rocky-data/rocky/pull/292)). The transpiler's substring rewriter now tracks comment + string-literal state and uses word boundaries, so `INT` no longer rewrites the `INT` inside `BIGINT` (and identical bugs in NVL / INT64). `rocky-wasm` clamps offsets to char boundaries before slicing source text. `rocky ai explain` refuses to clobber an unparseable sidecar TOML rather than silently overwriting it. `rocky run` returns exit code 2 on partial success (downstream Dagster + CI consumers can now distinguish "all failed" from "some failed").
- **Source-adapter URL path injection** ([#290](https://github.com/rocky-data/rocky/pull/290)). `rocky-fivetran` / `rocky-airbyte` / `rocky-iceberg` HTTP clients now percent-encode every interpolated path segment using an RFC 3986 unreserved-charset minus `.`, so connector ids / namespace parts can't break out of the URL path.

## [1.17.4] — 2026-04-26

Trust-system patch + minor data-cost fix. Closes a data-integrity hazard in `rocky discover` where a transient Fivetran 5xx or rate-limit window on a single connector could silently drop that connector from output, indistinguishable from "removed upstream" to downstream diff-based reconcilers (the asset-graph-shrinkage failure mode that took down a Dagster sensor in production with `DagsterInvalidSubsetError`). Same shape of bug Gold patched on the dbt path in commit `c43394d`. Bundled with an Arc 2 wave 3 residual: derived transformation models now report real `bytes_scanned` / `bytes_written` to `rocky cost` instead of dropping the warehouse-reported `ExecutionStats` on the floor.

### Added

- **`DiscoverOutput.failed_sources`** ([#270](https://github.com/rocky-data/rocky/pull/270)). New optional field on `rocky discover --output json`: a list of sources the adapter attempted to fetch metadata for and failed on, distinct from `sources` (succeeded) and `excluded_tables` (filtered post-success). Each entry carries `id` / `schema` / `source_type` / `error_class` / `message`. The `error_class` is a coarse 5-class enum — `transient` / `timeout` / `rate_limit` / `auth` / `unknown` — so consumers can branch on operating-mode without parsing free-form messages. Empty when discovery completes cleanly; absent from the JSON when empty (`skip_serializing_if = "Vec::is_empty"`) so existing fixtures stay byte-stable. Consumers diffing successive discover snapshots MUST treat sources present in `failed_sources` but absent from `sources` as "unknown state, do not delete" — that's the contract that distinguishes a fetch failure from a deletion. The dagster-rocky sensor now logs a warning when `failed_sources` is non-empty and leaves cursor entries for failed ids untouched.
- **Stats accumulation on plain transformation models** ([#270](https://github.com/rocky-data/rocky/pull/270)). The `execute_models` plain-transformation loop now calls `WarehouseAdapter::execute_statement_with_stats` and pushes a `MaterializationOutput` per model with accumulated `bytes_scanned` / `bytes_written` across statements, mirroring the time_interval and replication paths. Derived BQ models via full_refresh / incremental / merge will now feed real `bytes_scanned` to `rocky cost` (BigQuery on-demand pricing path) instead of dropping the warehouse-reported `ExecutionStats`. Strategy is mapped to a stable string (`full_refresh` / `incremental` / `merge` / `materialized_view` / `dynamic_table` / `delete_insert` / `microbatch` / `ephemeral`) so downstream metadata is consistent across paths.

### Changed

- **BREAKING: `DiscoveryAdapter::discover` returns `DiscoveryResult`** instead of `Vec<DiscoveredConnector>` ([#270](https://github.com/rocky-data/rocky/pull/270)). The trait return type changed in both `rocky_core::traits::DiscoveryAdapter` AND the public `rocky_adapter_sdk::DiscoveryAdapter`. `DiscoveryResult { connectors: Vec<DiscoveredConnector>, failed: Vec<FailedSource> }` carries successfully-fetched connectors plus any per-source failures the adapter classified onto `FailedSourceErrorClass`. Out-of-tree adapter authors must update: return `DiscoveryResult::ok(connectors)` for clean cases, or build `DiscoveryResult { connectors, failed }` when partial-failure classification is meaningful (per-connector parallel fetch, per-namespace listings). `rocky-fivetran` and `rocky-iceberg` had the silent-drop pattern in-tree and are fixed; `rocky-airbyte` and `rocky-duckdb` are single-shot surfaces and use `::ok(...)`. Wire format is unaffected — the new `failed_sources` field on `DiscoverOutput` is additive.
- **`rocky run` warns when discover surfaces failed sources.** A failed-source list at run time means the run still executes the healthy subset, but the run output won't carry materializations for the failed connectors. The warning surfaces the count so the consumer knows missing materializations are not the same as deletions.

### Fixed

- **`rocky-iceberg::discover` no longer silently drops namespaces on `list_tables` failure** ([#270](https://github.com/rocky-data/rocky/pull/270)). Same shape as the Fivetran fix: per-namespace `list_tables` errors at `adapter.rs:77-83` previously logged a warn and skipped the namespace; now they surface as `FailedSource` entries in `DiscoveryResult.failed`. Classifier hard-coded to `Unknown` pending an `IcebergError` taxonomy.

## [1.17.3] — 2026-04-25

Patch release. Fixes the `redis::Client::open("rediss://...")` → `valkey error: can't connect with TLS, the feature is not enabled - InvalidClientConfig` failure that hit any deployment pointing Rocky's Valkey/Redis state-sync or cache at a TLS-required endpoint (e.g. AWS ElastiCache with in-transit encryption). The prebuilt 1.17.2 binary's `redis` crate was compiled without any TLS feature — `rocky-core`'s sync state/idempotency client and `rocky-cache`'s async `ConnectionManager` both rejected the `rediss://` scheme at parse time.

### Fixed

- **TLS support enabled in the bundled `redis` crate.** `rocky-core` now pulls `redis` with `tls-rustls` + `tls-rustls-webpki-roots`, and `rocky-cache` adds `tokio-rustls-comp` + `tls-rustls-webpki-roots` on top of the existing `tokio-comp` + `connection-manager` features. Matches the workspace's existing rustls-only stance (`reqwest` already uses `rustls-tls`); no OpenSSL or system-cert dependency is introduced. `webpki-roots` bundles Mozilla's CA set so the static Linux binary works inside minimal containers without a populated `/etc/ssl/certs`. `redis://` URLs are unchanged. Both sync paths (`StateSync::Valkey` in `rocky-core/src/state_sync.rs`, idempotency keys in `rocky-core/src/idempotency.rs`) and the async `ValkeyCache::connect` in `rocky-cache/src/valkey.rs` now accept `rediss://`.

## [1.17.2] — 2026-04-25

Patch release. Two LSP-vs-CLI concurrency fixes that surface as a better VS Code extension experience: the extension's commands no longer randomly fail with a raw `Database already open. Cannot acquire lock.` error from redb, and the LSP no longer briefly stalls on every keystroke when a CLI process is running against the same state file.

### Fixed

- **`StateStore::open_read_only` now retries transient redb flock contention** ([#262](https://github.com/rocky-data/rocky/pull/262)). The docstring promised concurrent reader access but the underlying `Database::create` call inherits redb 2.x's unconditional `flock(LOCK_EX | LOCK_NB)` — there is no read-only path in redb that bypasses it. The LSP fires a schema-cache read ~300ms after every keystroke (`rocky-server::lsp::Server::did_change`), which races every CLI process the VS Code extension spawns; without retry the user saw the raw redb error verbatim. New `open_redb_with_retry` helper retries up to 5 × 50ms (~250ms total) on `DatabaseError::DatabaseAlreadyOpen`; other redb errors still propagate immediately. After exhaustion the caller sees a new `StateError::Busy` variant ("state store at <path> is busy — another rocky process is holding the database lock. Retry in a moment.") instead of the raw text. Both writer and reader open paths route through the same helper. Writer-vs-writer is unchanged: that race still fails fast with `LockHeldByOther`. Known boundary: this does NOT cover the case where a long-running `rocky run` holds the lock for seconds-to-minutes — inspection commands will still hit `Busy` in that scenario, but with an actionable message instead of a confusing redb error.
- **LSP schema-cache redb work moved onto the Tokio blocking pool** ([#263](https://github.com/rocky-data/rocky/pull/263)). `Server::load_cached_source_schemas` does sync redb work (`StateStore::open_read_only` + cache scan) inside an `async fn` called from a `tokio::spawn`'d task on every debounced keystroke. With the new flock-retry helper that open can sleep up to ~250ms during contention; doing that on a Tokio worker would intermittently starve the LSP just as the user is most actively typing. The open + scan now run via `tokio::task::spawn_blocking`; the throttled info log stays on the runtime because it awaits a tokio mutex. Independently useful even without the retry — `Database::create` itself does file I/O that should not run on the runtime — but the motivation is bounding the worst-case LSP responsiveness regression the retry would otherwise carry.

## [1.17.1] — 2026-04-24

Patch release. `rocky lineage <model> --format dot` now actually emits DOT when the global `--output` is left at its `json` default (#260). Previously the CLI's `--output json` default took precedence over the lineage-local `--format dot`, so the flag was silently ignored and stdout was a JSON blob. `--format dot` now wins inside the lineage command; `rocky lineage foo` without `--format` is still JSON by default. The VS Code extension's "Show Lineage" webview was the primary casualty — it fed the JSON to viz.js and rendered as an error complaining about character `{`.

## [1.17.0] — 2026-04-24

Governance waveplan polish wave. Five follow-ups on top of v1.16.0 plus one breaking data-integrity guardrail. Highlights: `rocky run --governance-override` now rejects `workspace_ids = []` without an explicit opt-in (breaking; set `allow_empty_workspace_ids: true` to keep the old "revoke everything" behaviour); `rocky run` + `rocky plan` accept `--env <name>` and plumb it into the masking resolver; `rocky plan` previews classification / mask / retention actions alongside SQL statements; `rocky compile` emits `W004` for classification tags that don't resolve to any masking strategy; Databricks role-graph reconciliation goes from log-only to real — SCIM group creation + per-catalog GRANT emission; `rocky retention-status --drift` now probes the warehouse (Databricks + Snowflake) instead of always returning `warehouse_days = null`.

### Added

- **Databricks SCIM client + per-catalog GRANT emission for `reconcile_role_graph`.** Completes the Wave C-1 role-graph reconciler that shipped log-only in engine 1.16.0 (#243). New `rocky_databricks::scim` module wraps the workspace-level `/api/2.0/preview/scim/v2/Groups` surface with idempotent `create_group` (POST-first; falls back to `GET ?filter=displayName eq "<name>"` on 409 Conflict) and `get_group_by_name`. `DatabricksGovernanceAdapter::reconcile_role_graph` now actually creates `rocky_role_<name>` UC groups (catalog-independent, once per role) and emits `GRANT <permission> ON CATALOG <catalog> TO \`rocky_role_<name>\`` per `(role, catalog, permission)` triple against every catalog the current `rocky run` touched. ADD-ONLY v1: groups are never deleted and grants are never revoked by this path; a role removed from `rocky.toml` leaves its group + grants in place until a future reconcile mode adds delete semantics. Adapters constructed via `without_workspace()` (no SCIM client) fall back to log-only behaviour so pipelines without SCIM configured keep working.
- **`rocky retention-status --drift` now probes the warehouse.** Completes the Wave C-2 surface shipped in 1.16.0 where `warehouse_days` on `ModelRetentionStatus` was always `None`. `GovernanceAdapter` gains an additive `read_retention_days` method (default impl returns `Ok(None)`); Databricks parses `SHOW TBLPROPERTIES (… 'delta.deletedFileRetentionDuration' …)` and Snowflake parses `SHOW PARAMETERS LIKE 'DATA_RETENTION_TIME_IN_DAYS' IN TABLE …`. DuckDB and BigQuery inherit the default `Ok(None)` — `--drift` degrades to "no warehouse observation" on those targets rather than erroring. `in_sync` now compares the probed value against the sidecar declaration instead of collapsing to `None == None`. JSON shape unchanged.
- **`--env <name>` flag on `rocky run` and `rocky plan`.** Closes the env-plumbing gap on the masking resolver shipped in 1.16.0: `RockyConfig::resolve_mask_for_env(Option<&str>)` already accepted an env, but callers passed `None`. `rocky run --env prod` now resolves `[mask.prod]` overrides on top of the workspace `[mask]` defaults during the post-DAG governance reconcile; `rocky plan --env prod` previews the same resolution in `mask_actions`. Role-graph reconcile remains env-invariant — Rocky's role config has no `[role.<env>]` override shape — so `--env` does NOT flow into `reconcile_role_graph`. Classification tagging and retention policies are also env-invariant. Matches the `--env` flag already accepted by `rocky compliance`.
- **`rocky plan` preview of classification / mask / retention actions.** `PlanOutput` gains three additive fields: `classification_actions` (one row per `(model, column, tag)` from `[classification]` sidecars), `mask_actions` (one row per `(model, column, tag, resolved_strategy)` where the tag resolves under the active env; unresolved tags are a `rocky compliance` diagnostic, not a preview row), and `retention_actions` (one row per model whose sidecar declares `retention = "<N>[dy]"`, carrying the parsed `duration_days` plus a `warehouse_preview` — Databricks renders `delta.logRetentionDuration` + `delta.deletedFileRetentionDuration`, Snowflake renders `DATA_RETENTION_TIME_IN_DAYS`, other adapters emit `null`). All three fields use `skip_serializing_if = "Vec::is_empty"` so `rocky plan --output json` on projects without governance config is byte-stable with the pre-1.16 shape. `PlanOutput.env` carries the active `--env <name>` under the same `skip_serializing_if` treatment. Existing JSON consumers written against the 1.16.0 shape are unaffected.
- **W004 compiler warning for unresolved classification tags.** `rocky compile` (and the post-DAG governance compile in `rocky run`) now emit one Warning per `(model, column, tag)` where the tag on a model's `[classification]` sidecar doesn't resolve to any `[mask]` / `[mask.<env>]` strategy. Example: `warning[W004]: classification tag 'audit_only' on column 'audit_note' has no matching [mask] strategy`. Suppress by adding a `[mask.<tag>]` entry to `rocky.toml` or listing the tag in `[classifications.allow_unmasked]`. Complements #241 by catching the "I tagged a column but forgot the strategy" class of silent config error at compile time instead of `rocky run`.

### Changed

- **`GovernanceAdapter::reconcile_role_graph` takes a `catalogs: &[&str]` parameter.** The trait is in-process only (consumed by `rocky run`; no FFI, no JSON-schema surface, no generated bindings), so the signature shift is internal — out-of-tree callers implementing the trait must add the new parameter. Rationale: SCIM group creation is catalog-independent but GRANT emission is per-catalog, so keeping catalog iteration inside the adapter avoids N× redundant SCIM round-trips. An empty `catalogs` slice is valid — groups are still created, zero GRANTs are emitted. `NoopGovernanceAdapter` accepts the extra arg and returns `Ok(())`; all other adapters inherit the trait default.
- **BREAKING: `rocky run --governance-override` rejects `workspace_ids = []` without an explicit opt-in** (FR-009). The post-DAG workspace-binding reconciler shipped in engine 1.14.0 treats `workspace_ids` as declarative state — empty list = "revoke every binding on the target catalog." That's the right contract when the list is intentional, but it also means a misconfigured permission store or an off-by-one serializer can silently strip every workspace off the catalog.

  `rocky run` now fails fast with a clear error before touching any catalog when the `--governance-override` payload contains `{"workspace_ids": []}` and the new `allow_empty_workspace_ids` opt-in is not set. The key semantics:

  | Payload | Behaviour |
  |---|---|
  | key omitted | Skip binding reconciliation (unchanged) |
  | `{"workspace_ids": [ids...]}` | Reconcile to exactly that set (unchanged) |
  | `{"workspace_ids": []}` | **Error** — refuses the silent full revoke |
  | `{"workspace_ids": [], "allow_empty_workspace_ids": true}` | Explicit full revoke (new path) |

  **Migration.** Any pipeline that was relying on an empty `workspace_ids` list to drop every binding must add `"allow_empty_workspace_ids": true` to the override payload. Callers that pass `None` / omit the key / send a non-empty list are unaffected.

  Types: `GovernanceOverride::workspace_ids` is now `Option<Vec<WorkspaceBindingConfig>>` (was `Vec<WorkspaceBindingConfig>` with `#[serde(default)]`). The option is what lets the engine distinguish "no override" (`None`) from "reconcile to empty" (`Some(vec![])`) — JSON `{}` and `{"workspace_ids": []}` were indistinguishable before. A new `allow_empty_workspace_ids: bool` field (default `false`, `#[serde(default)]`) carries the opt-in so the intent is auditable per-run via the `RunRecord` audit trail (`target_catalog`, `triggering_identity`) introduced in engine 1.16.0.

## [1.16.0] — 2026-04-23

Ships the governance waveplan — column classification + masking, audit trail on every run, `rocky compliance` rollup, role-graph reconciliation, and data retention policies. Five Waveplan PRs on top of three FR-004 / state-path follow-ups. Eight PRs since v1.15.0.

### Added

- **Column classification + masking (Wave A, [#241](https://github.com/rocky-data/rocky/pull/241)).** Model sidecars gain a `[classification]` block mapping column → free-form tag. Project `rocky.toml` gains `[mask]` (workspace default) + `[mask.<env>]` (per-env overrides) keyed by classification tag with strategies `"hash"` (SHA-256), `"redact"` (`'***'`), `"partial"` (keep first/last 2 chars), `"none"` (explicit identity). `GovernanceAdapter` gains `apply_column_tags` + `apply_masking_policy`; Databricks implements both via Unity Catalog column tags + `CREATE MASK` / `SET MASKING POLICY` (one statement per column — UC rejects multi-column DDL). `rocky run` applies after a successful DAG, best-effort (mirrors `apply_grants` semantics).
- **`rocky history --audit` + 8-field audit trail on `RunRecord` (Wave A, [#240](https://github.com/rocky-data/rocky/pull/240)).** Every `rocky run` now stamps `triggering_identity` / `session_source` (`Cli` / `Dagster` / `Lsp` / `HttpApi`, auto-detected) / `git_commit` / `git_branch` / `idempotency_key` / `target_catalog` / `hostname` / `rocky_version`. Redb schema v5→v6 with forward-deserialize (no in-place blob rewrite); defaults on v5 rows are `hostname="unknown"` / `rocky_version="<pre-audit>"` / `session_source=Cli`. New `--audit` flag on `rocky history` expands fields in text + JSON.
- **`rocky compliance` governance rollup (Wave B, [#242](https://github.com/rocky-data/rocky/pull/242)).** Static resolver that answers *"are all classified columns masked wherever policy says they should be?"* Thin rollup over Wave A state (no warehouse calls). Flags: `--env <name>`, `--exceptions-only`, `--fail-on exception` (CI gate; exit 1 on any exception). `ComplianceOutput` with `summary` / `per_column` / `exceptions`; `MaskStrategy::None` counts as masked (explicit-identity is a policy decision, not a gap); `[classifications.allow_unmasked]` suppresses exceptions without pretending columns are enforced.
- **Role-graph reconciliation (Wave C-1, [#243](https://github.com/rocky-data/rocky/pull/243)).** `[role.<name>] inherits = […] permissions = […]` blocks in `rocky.toml`; Rocky flattens the DAG (DFS with cycle detection + unknown-parent errors) into `BTreeMap<String, ResolvedRole>` and reconciles via new `GovernanceAdapter::reconcile_role_graph`. v1 Databricks impl is log-only — validates each `rocky_role_<name>` principal-syntax and emits `debug!`; SCIM group creation + per-catalog GRANT emission deferred as a follow-up. Resolver still catches cycles + unknown parents at config-load time regardless of adapter capability.
- **Data retention policies + `rocky retention-status` (Wave C-2, [#244](https://github.com/rocky-data/rocky/pull/244)).** Model sidecars gain `retention = "<N>[dy]"` parsed into `RetentionPolicy { duration_days: u32 }`. New `GovernanceAdapter::apply_retention_policy` implemented on Databricks (Delta `TBLPROPERTIES` — paired `delta.logRetentionDuration` + `delta.deletedFileRetentionDuration`) and Snowflake (`DATA_RETENTION_TIME_IN_DAYS`). BigQuery + DuckDB default-unsupported (no first-class retention knob). New `rocky retention-status` report with `--model <name>` + `--drift` flags; `--drift` warehouse probe deferred to v2 with stable schema (`warehouse_days: Option<u32>`).

### Changed

- **Unified post-DAG governance reconcile loop.** `rocky run` now iterates models once after a successful DAG and fires classification-tagging + masking-policy + retention-policy applies in a single pass. All three are best-effort — failures emit `warn!` and the pipeline continues, mirroring the `apply_grants` semantics from FR-005.
- **CLI and LSP state-path unification ([#238](https://github.com/rocky-data/rocky/pull/238)).** The CLI's `--state-path` default (previously `.rocky-state.redb` in CWD) now resolves through the new `rocky_core::state::resolve_state_path` helper, matching the LSP's `<models>/.rocky-state.redb` convention. Fresh projects land on the canonical `<models>/.rocky-state.redb` path; existing users with a CWD state file keep working and see a one-time deprecation warning on stderr pointing at the migration path. The inlay-hint cache-hit path (PR #228) and the schema-cache write tap (PR #230) now observe the same state file end-to-end — the known follow-up called out in the 1.14.0 release notes. Passing `--state-path` explicitly remains a hard override. Behaviour when both `<models>/.rocky-state.redb` and a legacy CWD state file exist: CWD wins (to preserve existing watermarks / branches / partitions) and a louder warning asks you to reconcile. Merge is lossy, so delete one copy to silence the warning.

### Fixed

- **Idempotency finalize on the error path (FR-004 F1, [#237](https://github.com/rocky-data/rocky/pull/237)).** A `rocky run --idempotency-key K` that errored out left the `InFlight` claim live in the state store because no error-path finalizer ran. Retrying the same key inside `in_flight_ttl_hours` then returned `skipped_in_flight` indefinitely. Now every `rocky run` error return goes through a guard that stamps `Failed` on the claim before surfacing the error.
- **Success-path idempotency finalize for non-replication dispatches (FR-004 F2, [#239](https://github.com/rocky-data/rocky/pull/239)).** A `rocky run --idempotency-key K` against a Transformation / Quality / Snapshot / Load pipeline successfully returned `Ok(())` but left the `InFlight` claim in place — the four non-replication dispatch arms returned directly from `run()` without calling `finalize_idempotency`, and the error-path wrapper from [#237](https://github.com/rocky-data/rocky/pull/237) only fires on `is_err()`. A retry with the same key inside `in_flight_ttl_hours` (default 24h) then returned `skipped_in_flight` for up to 24h instead of `skipped_idempotent`. Each of the four arms now stamps `Succeeded` on its happy-path exit via a new `finalize_idempotency_on_success` helper; the one-shot `.take()` on the shared `IdempotencyCtx` keeps the error-path wrapper a no-op when the success path already drained. Replication was never affected — it already finalized correctly at its main exit in [#235](https://github.com/rocky-data/rocky/pull/235).

## [1.15.0] — 2026-04-23

Ships FR-004 `rocky run --idempotency-key` for state-store-backed run dedup across every state backend (Phase 1 local/valkey/tiered + Phase 2 S3 + Phase 3 GCS), plus a routine `rand` dependency bump. Two PRs since v1.14.0.

### Added

- **`rocky run --idempotency-key <KEY>`** (FR-004, [#235](https://github.com/rocky-data/rocky/pull/235)) — caller-supplied opaque key that dedups this run against prior runs with the same key. Three outcomes:
  - **Seen, succeeded** (or any terminal status under `dedup_on = "any"`) → exit 0 with `status = "SkippedIdempotent"` and prior `run_id` surfaced as `skipped_by_run_id`. No work done.
  - **Seen, in-flight within TTL** (another caller is running this key) → exit 0 with `status = "SkippedInFlight"` and the in-flight `run_id`.
  - **Unseen** (or prior `InFlight` past TTL — treated as crashed-pod corpse via `AdoptStale`) → proceed normally, stamp `InFlight` at claim time, update to `Succeeded` / `Failed` at every terminal exit.

  Works on all five state backends via each backend's native atomic primitive:

  | Backend | Primitive |
  |---|---|
  | `local` | redb write-txn inside the existing `state.redb.lock` file lock |
  | `valkey` / `tiered` | `SET NX EX` on `{prefix}:idempotency:<key>` |
  | `s3` | `PutMode::Create` → `If-None-Match: "*"` on `PutObject` |
  | `gcs` | `x-goog-if-generation-match: 0` precondition on `insertObject` |

  Defence-in-depth below Dagster's `run_key` — catches pod retries, Kafka re-delivery, webhook duplicates, cron races. Works for non-Dagster callers (CI, cron, webhooks) equally. `--idempotency-key` + `--resume{,-latest}` is a clap-level error (resume is an explicit override). DAG sub-runs ignore the outer key so sibling pipelines don't short-circuit on a single stamp.

- **New `[state.idempotency]` TOML block** — tuning knobs for the dedup subsystem. All fields are optional with the shown defaults:

  ```toml
  [state.idempotency]
  retention_days = 30         # Stamp lifetime before GC
  dedup_on = "success"        # "success" (default) | "any"
  in_flight_ttl_hours = 24    # Stale-InFlight reaping window
  ```

- **New `IDEMPOTENCY_KEYS` redb table** + state-store schema version bump **4 → 5**. Additive migration (new table appears on first open at v5; no data shuffle). Deliberately NOT in `LOCAL_ONLY_TABLE_NAMES` — idempotency stamps must replicate on tiered backends so sibling pods see the same entry.

- **`rocky-core::idempotency` module** — public surface for downstream consumers: `IdempotencyBackend::from_state_config`, `check_and_claim`, `finalize`, `sweep_expired`, plus `IdempotencyEntry` / `IdempotencyState` / `DedupPolicy` / `FinalOutcome` / `IdempotencyCheck` types.

- **`ObjectStoreProvider::put_if_not_exists`** — atomic create-if-absent wrapper over `object_store::PutMode::Create`. Returns `PutIfNotExistsOutcome::{Created, AlreadyExists}`. Backs the S3 + GCS conditional-write paths but exposed on the provider directly so any caller needing the same primitive can use it.

- **POC**: [`examples/playground/pocs/05-orchestration/09-idempotency-key/`](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/05-orchestration/09-idempotency-key) demonstrates the flag acceptance + short-circuit behaviour end-to-end.

### Changed

- **`RunStatus` enum** (`rocky-core/src/state.rs`) extended with `SkippedIdempotent` + `SkippedInFlight` variants; ripples through the codegen cascade to Pydantic + TypeScript via `schemars`.
- **`RunOutput`** gains three fields: `status: RunStatus` (explicit — no longer derived-from-counts only), `skipped_by_run_id: Option<String>`, `idempotency_key: Option<String>` (echoed back for operator cross-reference in logs / `rocky history`).
- **GC during state upload sweep** — expired idempotency stamps + stale `InFlight` corpses are swept inside `state_sync::upload_state_with_excluded_tables` (same post-run cadence as `schema_cache`); no new cron. Structured `swept_count` + `retention_days` + `in_flight_ttl_hours` fields on the tracing `info!` event.

### Dependencies

- **`rand` bumped from 0.8.5 → 0.8.6** ([#234](https://github.com/rocky-data/rocky/pull/234), dependabot).

### Known follow-ups

- **Error-path finalize (F1 in FR-004 plan §11).** A `rocky run` that errors before `persist_run_record()` leaves its `InFlight` entry until the 24h `in_flight_ttl_hours` sweep reaps it. Worst-case latency to retry a crashed run with the same key is 24h. Tighten via `run_inner()` extraction OR a `tokio::spawn`-based drop guard on `IdempotencyCtx`; dedicated follow-up PR.

## [1.14.0] — 2026-04-23

Big batch of arcs shipping together: Arc 7 wave 2 wave-2 (cached DESCRIBE end-to-end), Arc 2 wave 3 (`bytes_scanned` plumbing → real $ cost on BigQuery + Databricks), three inbound governance / adapter feature requests (Unity Catalog workspace bindings, Fivetran connector metadata), plus a dedup of the retry backoff helper. Twelve engine PRs since v1.13.0.

### Added

#### Arc 7 wave 2 wave-2 — cached DESCRIBE

Leaf models now typecheck against real warehouse types without a live `DESCRIBE TABLE` round-trip on every `rocky compile` / `rocky lsp` invocation. Five PRs land end-to-end:

- **`rocky-core::schema_cache` + `SCHEMA_CACHE` redb table** (#223, PR 1a) — `SchemaCacheEntry` / `StoredColumn` / `schema_cache_key` / `is_expired` helpers in `rocky-core`; adapter-neutral strings keep the core clear of `RockyType`. New `[cache.schemas]` config (`enabled = true`, `ttl_seconds = 86400`, `replicate = false`). `StateStore` gains CRUD on the new table (schema version 3 → 4). `state_sync::upload_state_with_excluded_tables` filters local-only tables out of remote state by default. `rocky-compiler::schema_cache::load_source_schemas_from_cache` TTL-filtered loader.
- **Typecheck callsites wired against the cache** (#228, PR 1b) — 9 of 10 `CompilerConfig.source_schemas: HashMap::new()` callsites in `rocky-cli` + `rocky-server/LSP` now read from `StateStore::open_read_only` (no write lock, safe alongside live `rocky run`). `ai.rs:112` (ValidationContext) and `bench.rs:268` (synthetic tempdir) left unwired by design. LSP throttle module gates the cache loads per `models_dir`; honours `[cache.schemas] enabled`.
- **`rocky run` write tap** (#230, PR 2) — `SchemaCacheWriteTap` persists every successful `batch_describe_schema` result into `SCHEMA_CACHE`. Dedup within one run; best-effort (cache-write failures logged at `warn`, never fail the run). Gated on `[cache.schemas] enabled`.
- **`rocky discover --with-schemas`** (#231, PR 3) — explicit warm-up path for CI / scripted cache priming. Walks each `(catalog, schema)` pair via the source's `BatchCheckAdapter`, persists every returned table. Setting the flag with `[cache.schemas] enabled = false` errors with a clear message instead of silently no-op'ing. New `schemas_cached: usize` on `DiscoverOutput` (`skip_serializing_if = is_zero` keeps existing fixtures byte-stable).
- **`rocky state clear-schema-cache [--dry-run]`** (#232, PR 4) — explicit flush. Missing state store treated as no-op (CI-safe on ephemeral runners). New `ClearSchemaCacheOutput` through the codegen cascade. `rocky state` becomes a subcommand group; bare `rocky state` (watermarks view) is preserved.
- **`rocky --cache-ttl <seconds>` global CLI flag** (#232, PR 4) — overrides `[cache.schemas] ttl_seconds` for this invocation. Precedence: `--cache-ttl` > `rocky.toml` > built-in default (86400 s / 24 h). `--cache-ttl 0` treats every cache entry as instantly stale. To disable the cache entirely, set `[cache.schemas] enabled = false`. Applies to CLI read paths (`rocky compile`, `rocky run`, …); the `rocky lsp` / `rocky serve` daemons keep the config-derived TTL because daemon lifetimes outlive a single-invocation flag.

**Known follow-up** (not in these five PRs): CLI default state_path (`.rocky-state.redb` in CWD) and LSP default (`models_dir.join(".rocky-state.redb")`) diverge — until they're unified, LSP inlay-hints don't observe what `rocky run` wrote. Needs a migration story for existing users' state files; tracked as a separate PR. **Resolved in [Unreleased]** via `rocky_core::state::resolve_state_path`.

#### Arc 2 wave 3 — `bytes_scanned` adapter plumbing

Billing-relevant bytes flow through `MaterializationOutput.bytes_scanned` on BigQuery and Databricks, so `rocky cost` produces real dollar numbers end-to-end.

- **Non-breaking `WarehouseAdapter::execute_statement_with_stats` trait method** (#219) — default impl returns all-`None`. BigQuery override parses `statistics.query.totalBytesBilled` from the REST response. New `ExecutionStats { bytes_scanned, bytes_written, rows_affected }` public type. `MaterializationOutput` gains `bytes_scanned` / `bytes_written`. `populate_cost_summary` now consumes `mat.bytes_scanned`. Naming note: `bytes_scanned` carries *billing-relevant* bytes (BQ returns `totalBytesBilled` with a 10 MB floor), not literal scan volume.
- **Databricks override** (#221) — `manifest.total_byte_count` maps to `bytes_scanned`. Databricks cost is DBU-priced, so bytes aren't the primary cost driver; surfaced as-is for observability parity with BQ.
- **Snowflake deferred-by-design** (#220) — 21-line adapter comment explaining why Snowflake does NOT override: `QUERY_HISTORY` round-trip cost + Snowflake cost is duration × DBU, never consumes `bytes_scanned`. Serverless pricing would flip the trade-off; batched-lookup-at-finalise is the future path if demand materialises.
- **Adapter-keyed `bytes_scanned` docstring cascade** (#222) — six field-declaration sites carry billing-semantic doc through `schemars` to 12 generated files (JSON schema + Pydantic + TypeScript). VS Code hover and Dagster Pydantic IDE consumers see the BQ-console-comparison nugget in place.

**Residual gaps surfaced but not addressed this release** (demand-gated): plain transformation models don't push `MaterializationOutput` in the `execute_models` loop today, so derived BQ models via `full_refresh` / `incremental` / `merge` still report `None`; `snapshot_scd2` uses multi-statement `execute_query`.

#### Unity Catalog workspace-binding reconcile

- **`GovernanceAdapter::list_workspace_bindings` + `remove_workspace_binding`** (#226) — additive trait extension; new `reconcile_access` combined-pass entry in `permissions.rs` (grants + bindings in one pass; `rocky plan` groups them). Databricks-only implementation via existing `WorkspaceManager`; non-Databricks adapters silently no-op. **Behaviour change to flag for users:** the reconciler removes hand-added bindings not in `rocky.toml`. Consider a `reconcile_bindings = false` default or `--dry-run` before a policy-sensitive rollout.

#### Fivetran connector metadata

- **`SourceOutput.metadata` + `DiscoveredConnector.metadata`** (#225) — adapter-namespaced `IndexMap<String, Value>`. Fivetran populates `fivetran.service` / `fivetran.connector_id` / `fivetran.schema_prefix` / `fivetran.custom_tables` / `fivetran.custom_reports`. Other adapters (Airbyte, DuckDB, Iceberg) pass empty maps; `skip_serializing_if = "IndexMap::is_empty"` keeps DuckDB playground fixtures byte-stable. `IndexMap` over `HashMap` for insertion-stable iteration (deterministic fixture bytes). Dagster translator forwards adapter-namespaced keys as asset metadata.

### Internal

- **`compute_backoff` dedup** (#217) — lifted three identical copies from `rocky-core::state_sync` / `rocky-databricks::connector` / `rocky-snowflake::connector` into a new `rocky_core::retry` module. Net −99 lines. All three bodies were byte-identical (cosmetic drift only), so zero unification choices. Closes the follow-up left open by #213.

## [1.13.0] — 2026-04-22

Reliability hardening for the state backend. Closes the last mutation path in `rocky-core` without retry / circuit-breaker parity with the adapter layer, and turns `rocky doctor` into a real smoke test of state-backend connectivity instead of a credentials-only check. Two PRs since v1.12.0.

### Added

- **`[state.retry]` block on `StateConfig`** (#213) — shape identical to `[adapter.databricks.retry]`, so operators reason about both layers with a single mental model. Fields: `max_retries` (default 3), `initial_backoff_ms` (1000), `max_backoff_ms` (30000), `backoff_multiplier` (2.0), `jitter` (true), `circuit_breaker_threshold` (5), `circuit_breaker_recovery_timeout_secs` (None), `max_retries_per_run` (None). Reuses the existing `rocky_core::circuit_breaker::CircuitBreaker` + `rocky_core::retry_budget::RetryBudget` already battle-tested by the Databricks adapter. The retry loop is wrapped by the existing `with_transfer_timeout`, so `transfer_timeout_seconds` (default 300 s) remains the total wall-clock cap — retries *share* the budget rather than extend it. No liveness regression vs v1.12.0.
- **`[state] on_upload_failure = "skip" | "fail"`** (#213) — policy applied after retries + circuit are exhausted. Default `skip` matches the de-facto behaviour of existing callers that `warn + continue` on upload failure; `fail` is the opt-in for strict environments where state durability trumps liveness. New `StateUploadFailureMode` enum exported from `rocky_core::config`.
- **New `StateSyncError::CircuitOpen` + `RetryBudgetExhausted` variants** (#213) — terminal outcomes that were previously masquerading as generic upload/timeout errors now surface with attribution, so log-grep and `match`-based error handling can distinguish "breaker tripped" from "network blip".
- **`outcome` field on every terminal `state.upload` / `state.download` event** (#213) — `ok` / `absent` / `timeout` / `error_then_fresh` / `skipped_after_failure` / `transient_exhausted` / `circuit_open` / `budget_exhausted`. Terminal success events also carry a `retries` counter. Lets operators diagnose incidents from structured log lines instead of free-form message regex.
- **`rocky doctor` gains a 7th check: `state_rw`** (#214) — runs a put/get/delete round-trip against the configured state backend (S3 / GCS / Valkey / Tiered). Uses a distinct marker key (`doctor-probe-{pid}-{nanos}.marker`, never `state.redb`), honours `transfer_timeout_seconds`, no retries — probes produce a single-pass pass/fail signal, not resilient writes. Local backend is a no-op. Tiered probes both legs; either failing fails the probe. Complements the existing `state_sync` check (backend-type inspection only) by surfacing IAM / reachability problems at cold start instead of at end-of-run upload time.
- **`rocky_core::state_sync::probe_state_backend` public helper** (#214) — exposes the RW probe so downstream `doctor`-like tooling (Dagster asset checks, scheduled sensors) can reuse it without shelling out to `rocky doctor`.

### Internal

- **`compute_backoff` + `is_transient` for state sync intentionally duplicated** from `rocky-databricks::connector` and `rocky-snowflake::connector`. A follow-up PR should hoist all three copies into a shared `rocky-core` helper; this release kept scope narrow.

## [1.12.0] — 2026-04-21

Arc 1 wave 2 shipped + a short cleanup wave around it. Eight PRs since v1.11.0. `record_run` wiring finally makes the run-history query surfaces (`rocky history` / `replay` / `trace` / `cost`) return real data end-to-end, plus two new commands (`rocky cost`, `rocky branch compare`) and a SIGPIPE fix.

### Added

- **`rocky cost <run_id|latest>`** (#202) — historical cost rollup over stored runs. Recomputes per-model cost via `rocky_core::cost::compute_observed_cost_usd` from `ModelExecution.duration_ms` / `bytes_scanned`. New `CostOutput` + `PerModelCostHistorical` through the codegen cascade. BigQuery cost now computes from stored bytes even when the live `rocky run` path still emits `None`. Arc 2 wave 2 first PR.
- **`rocky branch compare <name>`** (#200) — diff a named branch's targets against main. Delegates to the existing `compare::compare` entry point with the branch's `schema_prefix` routed through `ShadowConfig.schema_override` — same mechanism `rocky run --branch` uses for writes, so compare hits exactly the tables the branch produced. Zero JSON-schema drift.
- **`rocky run` now persists `RunRecord` to the state store** (#203) — `StateStore::record_run` wired at every exit path (happy / interrupted / model-only) after `populate_cost_summary`. `rocky history`, `rocky replay`, `rocky trace`, and `rocky cost` now return real data instead of reading from an empty store. Arc 1 wave 2.
- **Real per-model `started_at` on `MaterializationOutput`** (#206) — every materialization now carries a `DateTime<Utc>` stamped at the moment the engine begins executing it. `RunOutput::to_run_record` uses it directly (deriving per-model `finished_at` as `started_at + duration_ms`), so parallel runs preserve their real ordering on the persisted `RunRecord` instead of the finish-relative collapse PR #203 had to leave behind.
- **`OptimizeRecommendation`** (#203) gained `compute_cost_per_run`, `storage_cost_per_month`, `downstream_references` — projected through from `rocky_core::optimize::MaterializationCost` so Dagster `checks.py` can surface the values as asset metadata without re-deriving.
- **Configurable `state.transfer_timeout_seconds`** — wall-clock budget for each state download/upload operation, defaulting to 300 s. Replaces the hard-coded constant in `rocky_core::state_sync`. Raise for very large state files or slow networks; lower to fail faster in CI. Surfaces in `rocky.toml` under `[state]`.
- **`state.upload` / `state.download` tracing spans** around every state transfer, carrying `backend`, `bucket`, and `size_bytes` fields. Events emitted inside the transfer — including the new timeout warning — inherit those fields automatically, so hung transfers are diagnosable from stderr logs alone (which dagster-rocky streams into the Dagster run viewer).
- **Structured `tracing::warn!` on transfer-timeout elapse** with a `duration_ms` field and message `"state transfer exceeded timeout budget"`. Operators now see a single, searchable event instead of a silent `StateSyncError::Timeout`.
- **`rocky_core::object_store` now honours `AWS_ALLOW_HTTP` / `AZURE_ALLOW_HTTP` / `GOOGLE_STORAGE_ALLOW_HTTP`** in `default_client_options()`. These are standard `object_store`-crate env vars; the flag is always off in production. Integration tests (see `tests/state_sync_timeout_test.rs`) use it to front-end the SDK with a plain-HTTP wiremock without bypassing the real credential chain.

### Fixed

- **SIGPIPE now handled gracefully** (#199) — `rocky <cmd> | head` / `| jq | head` no longer SIGABRTs. Rust's default was to ignore `SIGPIPE` at the kernel level and panic from `println!` on the resulting EPIPE; restored the POSIX default in `main()` before `Cli::parse()` so `--help` / `--version` are also covered. `#[cfg(not(unix))]` stub preserves Windows builds.
- **`target_dialect = "bq"` rejected by the project schema** (#201) — `examples/playground/pocs/06-developer-experience/08-portability-lint/rocky.toml` had the CLI-flag short form where only the long form (`bigquery`) is valid in `[portability]`. Updated the POC; `every_committed_poc_matches_project_schema` is green again.
- **`HistoryResult` Pydantic drift** (#203) — hand-written class mirrored the state-store `RunRecord` shape (`finished_at`, `config_hash`, `models_executed` as a list) rather than what `rocky history --json` actually emits. Completed the Phase 2 soft-swap that history had been missing: `HistoryResult = HistoryOutput`, `ModelHistoryResult = ModelHistoryOutput`. Invisible until runs stopped being empty.
- **Valkey state transfers now honour the transfer-timeout budget.** The sync `redis::Client::get_connection()` and `redis::cmd(...).query()` calls in `upload_to_valkey` / `download_from_valkey` ran on the tokio runtime thread, so a dead Valkey peer could stall the run forever — no outer `tokio::time::timeout` could rescue a blocking socket read. Both paths now run under `tokio::task::spawn_blocking` inside `with_transfer_timeout`, closing the same class of hang the object-store paths were already protected against.

### Internal

- **`scripts/_normalize_fixture.py`** (#203) gained `WALL_CLOCK_ID_FIELDS = {"run_id"}` and `DERIVED_FROM_WALL_CLOCK_FIELDS = {"compute_cost_per_run", "estimated_monthly_savings"}` so dagster test fixtures stay byte-stable across regens now that `run_id` and wall-clock-derived cost numbers appear in non-empty arrays.

## [1.11.0] — 2026-04-20

Closes the **first wave of every trust-system arc** (Arcs 1–7) plus two
wave-2 follow-ups landed the same day. Nine feature PRs since v1.10.0,
organized by the seven arcs of the trust-system direction.

### Added — Trust-system Arc 1 (first wave) · #170

- `rocky lineage --column <col> --downstream` walks the column-level graph forward (existing `--column` continues to walk upstream). A new `edges_by_source_model` index backs the transitive walker so the cost scales with fan-out rather than total edges. `ColumnLineageOutput` gains a `direction` field so consumers can distinguish the two shapes.
- `rocky branch create|delete|list|show` — named virtual branches persisted in the state store's new `BRANCHES` table. A branch records a `schema_prefix` (default `branch__<name>`); `BranchOutput`, `BranchListOutput`, and `BranchDeleteOutput` are registered with the codegen cascade.
- `rocky run --branch <name>` applies a previously-created branch by routing through existing shadow-mode machinery (internally `--shadow --shadow-schema <branch.schema_prefix>`). Mutually exclusive with `--shadow` / `--shadow-schema`.
- `rocky replay <run_id|latest>` surfaces the state store's `RunRecord` — per-model SQL hash, row counts, bytes, and timings as captured at execution time. Optional `--model` filter. Inspection-only.

### Added — Trust-system Arc 2 (first wave) · #171

- `RunOutput.cost_summary` carries per-run total cost; per-materialization `cost_usd` flows through `MaterializationMetadata`.
- `[budget]` block in `rocky.toml` declares run-level cost / row / byte limits with `on_breach = "warn" | "error"` semantics.
- `budget_breach` PipelineEvent + `HookEvent::BudgetBreach` fire when limits trip; non-zero exit code on `on_breach = "error"`.

### Added — Trust-system Arc 3 (first wave) · #172

- Three-state `CircuitBreaker` (Closed / Open / HalfOpen) with timed auto-recovery — Databricks + Snowflake adapters consolidated onto a single shared implementation in `rocky_core::poison`.
- New `circuit_breaker_tripped` / `_recovered` PipelineEvents emitted via `TransitionOutcome` so observers see state transitions in real time.

### Added — Trust-system Arc 4 (first wave) · #173

- `rocky trace <run_id|latest>` renders a Gantt timeline of a run with lane assignment to fit parallel materializations on the terminal.
- Feature-gated `otel` cascade wires `rocky_observe::otel::OtelExporter` via an `OtelGuard` RAII handle so `rocky run` exports OTLP metrics when built with `--features otel`.

### Added — Trust-system Arc 5 (first wave) · #174

- Schema-grounded prompt builder for `rocky ai`: AI requests now ship the project's typed model schema as context so first-attempt accuracy improves without an extra round-trip.
- `rocky_ai::generate::ValidationContext` hook lets the AI generator typecheck against the live project before returning SQL.
- `rocky ai --models <a,b,c>` flag scopes prompt context to a subset of models.

### Added — Trust-system Arc 6 (first wave) · #184

- `rocky compile --target-dialect <dbx|sf|bq|duckdb>` rejects SQL constructs that don't run on the chosen warehouse, emitting error-severity **P001** diagnostics. AST-based (sqlparser visitor) catalog mirrors what `rocky_sql::transpile` already knows about: NVL / IFNULL / DATEADD / DATE_ADD / TO_VARCHAR / LEN / CHARINDEX / ARRAY_SIZE / DATE_FORMAT / QUALIFY / ILIKE / FLATTEN.

### Added — Trust-system Arc 7 (first wave) · #185

- Always-on, warning-severity **P002** blast-radius lint for `SELECT *`. Fires when a model uses `SELECT *` AND a downstream model references specific columns of its output (leaf `SELECT *` is intentionally not flagged). Diagnostic names the affected downstream consumers + the columns they reference (capped at 3 per consumer for legibility), with an actionable suggestion to switch to an explicit column list. Wired in `rocky-compiler::compile_project` + `compile_incremental` so both CLI and LSP surfaces see it.

### Added — Trust-system Arc 6 (wave 2) · #186

- `[portability]` block in `rocky.toml` (top-level `target_dialect` + `allow` list) replaces the wave-1 flag-only UX with project-level configuration.
- Per-model SQL pragma `-- rocky-allow: NVL, QUALIFY` (case-insensitive, comma-separated) for targeted exemptions.
- New generic `rocky-sql/src/pragma.rs` module so a future `[lints]` block driving P002 toggles reuses the same parser.
- `Dialect` enum gains `Serialize / Deserialize / JsonSchema` (lowercase variants) so `target_dialect = "bigquery"` round-trips cleanly through TOML.
- Precedence: CLI flag > `[portability] target_dialect` config > unset.

### Added — Trust-system Arc 7 (wave 2 wave-1) · #187

- Opt-in `rocky compile --with-seed` flag runs the project's `data/seed.sql` against an in-memory DuckDB and uses `information_schema` as the source-of-truth for `source_schemas`. Leaf `.sql` models pick up real types instead of `Unknown`. Reuses the existing sync `DuckDbConnector` + one `information_schema.columns` round-trip; feature-gated behind `duckdb`.
- Trust outcome verified on the playground default POC: `raw_orders.incrementality_hint` confidence jumps from `medium` (1 signal: name pattern) to `high` (2 signals — including "column type is temporal (Date), suitable as a watermark," impossible without typed source).

### Fixed · #169

- `engine/install.sh` and `engine/install.ps1` now resolve "latest" by semver instead of lexicographic order across the `engine-v*` tag namespace.

### Deferred (carry forward from this release)

- Arc 1 wave 2: warehouse-native clone (Delta `SHALLOW CLONE`, Snowflake zero-copy); `rocky replay` re-execution; `rocky branch compare`; Exp 4 live spike.
- Arc 2 wave 2: per-model budgets; adapter-reported `bytes_scanned` plumbing for BigQuery cost; PR cost-projection GitHub Action; `rocky cost` historical command.
- Arc 3 wave 2: event→hook bridge so adapter-emitted events fire configured hooks; transactional checkpoint atomicity audit.
- Arc 4 wave 2: OTel *span* coverage on `HookEvent::Before/AfterMaterialize` sites; freshness SLO enforcement; Dagster UI timeline hook.
- Arc 5 wave 2: auto-fix suggestions on failed runs; contract-aware generation; typechecker tightening so the validator becomes a hard gate.
- Arc 6 wave 3: lint on expanded SQL when `--expand-macros` is set; sharper source spans (per-construct byte offsets).
- Arc 7 wave 2 wave-2: cached `DESCRIBE TABLE` from `rocky discover`/`run` flowing into compile (real-warehouse audience); needs design pass for new persistent cache + invalidation.

## [1.10.0] — 2026-04-20

Closes the active arc of the perf-resilience roadmap. Thirteen release PRs over two days — P3.1 incremental compiler, P3.4 range-protocol half, P3.11 split `rocky-lsp` binary, full 15-event hook lifecycle wired into `rocky run`, cross-adapter shared `RetryBudget`, and a run of `Arc<str>` / `CiKey` alloc-reduction passes through the compiler hot path. Plus two adjacent Databricks fixes.

### Added — split `rocky-lsp` binary (§P3.11)

New `rocky-lsp` binary ships alongside the main `rocky` CLI from this release onwards. Purpose-built for IDE integration: links only `rocky-server` (compiler + LSP surface) + tokio — no adapter graph. Sizes measured on macOS ARM64 release:

| Binary | Size | Contents |
|---|---|---|
| `rocky` | 47 MB | CLI + all adapters (Databricks, Snowflake, BigQuery, DuckDB, Fivetran, Airbyte, Iceberg) |
| `rocky-lsp` | 6.1 MB | LSP only |

The release workflow publishes `rocky-lsp-<target>.{tar.gz|zip}` archives alongside the existing `rocky-*` archives on every platform. The matching VS Code extension release (v1.5.0) prefers `rocky-lsp` when it's on PATH or alongside `rocky.server.path`, falling back to `rocky lsp` otherwise.

### Added — 15-event hook lifecycle in `rocky run` (§P2.6)

`HookRegistry::fire` is now wired into every lifecycle event `rocky run` emits. Hook subscribers — shell commands or webhooks declared in `rocky.toml` — can observe every phase without polling output JSON:

- **Pipeline:** `pipeline_start`, `pipeline_complete`, `pipeline_error`, `wait_async_webhooks` drain before exit.
- **Discovery / compile:** `discover_complete` (with `connector_count`), `compile_complete` (with `model_count`).
- **Per-table:** `before_materialize`, `after_materialize` (with `duration_ms`, `row_count`), `materialize_error`.
- **Per-model:** `before_model_run`, `after_model_run` (with `duration_ms`), `model_error`.
- **Checks / state:** `before_checks`, `check_result` (per individual check), `after_checks` (roll-up), `drift_detected`, `anomaly_detected`, `state_synced`.

Every error path after `pipeline_start` fires now emits a terminal `pipeline_error` or `pipeline_complete` — including previously-silent `?`-propagations through `execute_models`. Subscribers no longer need a timeout heuristic to detect orphaned runs.

### Added — cross-adapter shared `RetryBudget` via `[retry]` config (§P2.7 extension)

New optional top-level `[retry]` block:

```toml
[retry]
max_retries_per_run = 50
```

When set, `AdapterRegistry` builds one shared `Arc<AtomicI64>` budget and wires it through every adapter (Databricks, Snowflake, Fivetran). Once exhausted, no adapter retries further — prevents one failing endpoint from burning the whole budget pool others could have used. Omit the block for per-adapter budgets (the existing behaviour from v1.9.0).

### Added — `semanticTokens/range` on the LSP server (§P3.4)

The LSP server now advertises `range: Some(true)` and implements `textDocument/semanticTokens/range`. Editors (vscode-languageclient) switch to range requests on scroll instead of always requesting full-document tokens. Response bytes shrink to viewport-only; server compute stays the same on a cache hit. Matches the roadmap's 10–20% per-keystroke estimate on large files.

Cache shape changed from `Vec<SemanticToken>` (post-delta) to `Vec<(u32, u32, u32, u32)>` (pre-delta) so full + range paths share one cache.

### Performance — incremental compiler reuses previous typecheck (§P3.1)

The LSP's per-keystroke compile path is now genuinely incremental. New `rocky_compiler::compile::compile_incremental`:

- Loads the new project + rebuilds the semantic graph (cheap).
- Computes the affected set against the new graph — changed files, transitive dependents, new-to-project models, and models whose upstream set shifted.
- Falls through to full `compile()` when the affected blast radius exceeds 50% of the graph, or projects under 10 models (bookkeeping isn't worth it).
- Otherwise runs `typecheck_project_incremental`, which reuses `previous.typed_models` entries for non-affected models. `reference_map` is seeded from the previous result (dropping entries recorded FROM affected files) and merged with fresh refs — no SQL reparse of the non-affected remainder.

On the `single_file_change/500_models_edit_one_mart` bench: ~24 ms → ~18 ms (~25% faster). Real LSP workloads with complex SQL should see a larger relative win.

**Correctness fix bundled:** the old `compile_incremental` in `rocky-server/src/lsp.rs` wrote back `ReferenceMap::default()` on its "no model files changed" short-circuit, silently breaking Find References / Rename after any incremental compile. The new path preserves the reference map correctly.

### Performance — `Arc`/`CiKey` across the compiler hot path (§P3.5–P3.8, §P4.2)

- **P3.5** `Diagnostic::{code, message}` → `Arc<str>` (5-crate ripple; serde `rc` feature preserves JSON wire format).
- **P3.6** DSL `Token` content variants → `Arc<str>` (logos callbacks wrap once via `Arc::from`; AST boundary preserved via `.to_string()`).
- **P3.7** AST sub-expressions (`BinaryOp`, `UnaryOp`, `IsNull`, `InList`, `Match`) → `Arc<Expr>`. Cloning a DSL `Expr` is now a refcount bump per branch instead of a deep tree copy. `Arc` (not `Rc`) because `CompileResult` moves across `spawn_blocking`. Zero ripple outside rocky-lang — `Deref` preserves consumer surface.
- **P3.8** `TypeScope` uses `CiKey<'static>` + nested `HashMap<CiKey, HashMap<CiKey, V>>` so lookups via `CiStr::new(name)` are alloc-free. ~1M lookup allocations removed per 100-model full compile.
- **P4.2** `ir.rs` identifier-list fields (`unique_key`, `partition_by`, `ColumnSelection::Explicit`, `columns_to_drop`) → `Vec<Arc<str>>`. Coordinated `SqlDialect::merge_into` trait change across 5 dialect impls (databricks/snowflake/duckdb/bigquery) + 8 rocky-core mocks, all in one atomic commit so there's no per-call `.to_string()` conversion left behind. JSON wire format preserved.

### Added — `PipelineEvent` retry emit sites + cross-dagster bindings

`PipelineEvent` schema (added in v1.9.0 via P2.8) now has emit sites in every adapter's retry loop. Databricks + Snowflake + Fivetran emit `statement_retry` / `http_retry` events with `attempt`, `max_attempts`, and `error_class` fields; subscribers can distinguish "retry 3/5" from "final failure" without string-matching error messages. Pydantic + TypeScript bindings regenerated.

### Added — batched watermark commits + other batch-3 wins

Carried forward from the batch-3 roadmap push (PR #145):

- **P1.6** batched watermark commits — `state_store` flushes watermarks in bulk per run instead of per-table; reduces redb transaction count by ~100× on wide pipelines.
- **P1.9** `CiKey`/`CiStr` zero-alloc case-insensitive column lookups (`rocky-core/src/column_map.rs`) — replaces per-column `.to_lowercase()` allocs; drift / checks / contracts all lookup via `CiStr::new(name)`.
- **P2.6** webhook global timeout + async tracking — webhooks run with a per-run watchdog; fire-and-forget handles drain before exit via `wait_async_webhooks()`.
- **P2.9** env-var-aware TOML parse errors — surfaces `${VAR}` / `${VAR:-default}` substitution failures at parse time with the env-var name in the error message.
- **P2.10** webhook method validated at config load — unknown HTTP verbs fail early with a sourced diagnostic instead of at fire time.
- **P2.11** `fail_fast` gates watermark commits — a failing table in `fail_fast` mode no longer advances the watermark for tables still in-flight.
- **P2.12** dagster JSON parse error UX — unparseable CLI output surfaces the offending byte offset + line instead of a generic `JSONDecodeError`.
- **P2.13** BigQuery HTTP tuning — bounded connect/read timeouts + retries on transient errors.
- **P3.2** LSP buffer-hash short-circuit — `didChange` skips recompile when the incoming buffer text hashes to the same value as the last compiled version. ~30% spurious compile cycles eliminated.
- **P3.3** `spawn_blocking` the full compile — long parse+compile runs on the blocking pool so hover / completion / semantic-token handlers stay responsive.
- **P3.9** parser error messages → `Cow<'static, str>` — no per-error `format!` when the message is static.
- **P3.10** fast `rocky --version` / `--help` — cold start ~100 ms → ~10–20 ms. Version and help paths no longer load the adapter registry.
- **P4.1** column_map exclude set hoisted — signature change: `exclude: &HashSet<String>` (callers keep a prepared set).
- **P4.3** streaming `sql_fingerprint` — feeds statements into the hasher incrementally instead of `join(";\n")` then hash. Bit-identical output verified by a 6-case test.
- **P4.4** single-file-change criterion bench — guards the P3.1 incremental-compile gains against regression.

### Fixed — Databricks OAuth 403 "Invalid Token" → token refresh

`rocky-databricks/src/connector.rs::is_transient` previously classified only 401 as transient-and-refreshable. Databricks's SQL Statement Execution API returns **HTTP 403 "Invalid Token"** where 401 would be more idiomatic, which meant long-running operations that crossed the 1-hour OAuth M2M TTL boundary mid-execution died on the first post-expiry call with no retry and no token refresh.

- `is_transient` now treats both 401 and 403 as transient.
- The retry loop's `invalidate_cache` branch matches both 401 and 403, so the next attempt triggers a fresh `client_credentials` exchange.
- Bad credentials still re-fail every attempt and exhaust the retry budget — no new attack surface.
- Surfaced by a multi-hour `rocky compact --measure-dedup` sweep against a production Databricks workspace.

### Fixed — `rocky compact --measure-dedup` on Databricks Unity Catalog

The Layer 0 dedup-measurement command had three defects that made it unusable against Unity Catalog. All three land together since any real Databricks sweep hits each one in sequence:

- **Unqualified `information_schema.tables` query.** Unity Catalog has no workspace-wide `information_schema`; every query must be `<catalog>.information_schema.tables`. `enumerate_tables` now accepts an explicit catalog list (derived from the managed-table set via the new `managed_catalog_set` helper) and fans out per-catalog with catalog-qualified queries. The unqualified form is kept as the `None` fall-through for DuckDB (single-DB scope). Catalogs that fail enumeration (missing, no `USE CATALOG` grant, etc.) are skipped with a warning rather than aborting the sweep.
- **ANSI-only `table_type` filter.** `WHERE table_type IN ('BASE TABLE', 'TABLE')` excluded every Delta table on Databricks (which uses `MANAGED`, `EXTERNAL`, `STREAMING_TABLE`, `FOREIGN`). Flipped to `NOT IN ('VIEW', 'MATERIALIZED_VIEW', 'MATERIALIZED VIEW')` — broadly portable across warehouses.
- **No tolerance for empty tables or single-table failures.** An empty table's `HASH` aggregate returns NULL, which aborted the entire sweep. Similarly, one `describe_table` or `hash_table` failure killed all prior + future work. Now all three degrade gracefully: NULL-checksum rows are skipped, per-table failures warn and continue, and the summary log reports skipped catalogs + skipped tables so results can be interpreted as partial coverage rather than silently incomplete.

Known gap (not fixed here): `--all-tables` on Databricks still hits the unqualified `information_schema` query and errors out. The fix is to add `SHOW CATALOGS` discovery for the `None` catalog path, deferred as it needs adapter-level plumbing.

## [1.9.0] — 2026-04-19

Perf-resilience batch 2. Four items off the roadmap's near-term bucket — two perf foundations, two 1.0 auth-resilience closeouts — plus a refactor pass consolidating the auth-cache pattern across adapters.

### Added — predecessor map in DAG execution phases

`execution_phases` in `rocky-core/src/unified_dag.rs` now builds a `HashMap<NodeId, Vec<NodeId>>` of predecessors once in the same edge-walk as the existing `dependents` map, replacing the per-node `dag.edges.iter().filter(...)` inner loop. Sub-ms on today's graphs; guardrails quadratic blow-up on projects with 500+ models.

### Changed — adaptive state-sync cadence

`rocky run` no longer flushes watermarks to remote object storage on a fixed 30-second cadence. Cadence now scales with an estimated run duration (`tables * 3s / concurrency`):

- Runs estimated under 60 s skip the periodic upload entirely — the end-of-run upload handles state persistence. Short runs interrupted with Ctrl-C no longer emit an intermediate PUT.
- Longer runs target one upload per quarter of the estimated duration, floored at 10 s and capped at 30 s.

Eliminates ~9 redundant object-store PUTs per 5-minute run without sacrificing durability for long pipelines.

### Fixed — Snowflake keypair JWT caching + 401 recovery

The keypair-auth path in `rocky-snowflake/src/auth.rs` previously minted a fresh 60-second-TTL JWT on **every** `get_token` call and classified all 401 responses as permanent errors. Long-running pipelines that crossed any server-side expiry boundary silently failed.

- The keypair branch now caches the minted JWT with a 59-minute TTL (Snowflake's documented maximum) and refreshes 60 s before expiry, mirroring the existing password-path cache.
- New `Auth::invalidate_cache()` drops the cached token. The connector retry loop calls it on 401 so the next attempt re-mints a fresh JWT; `is_transient(401)` now returns `true` to make the retry path reachable. Bad credentials still re-fail every attempt and exhaust the configured retry budget — no new attack surface.

### Fixed — Databricks OAuth 401 → token refresh + retry

`rocky-databricks/src/connector.rs::is_transient` classified every 401 as permanent, so OAuth M2M pipelines running past the hourly token TTL silently failed the first time Databricks rejected the cached token.

- New `Auth::invalidate_cache()` drops the cached OAuth token (PAT is a no-op).
- The retry loop calls it before retrying a 401 so the next attempt triggers a fresh `client_credentials` exchange; `is_transient(401)` now returns `true`.

### Refactored — shared auth-cache scaffolding

Post-review cleanup that landed in the same PR:

- `JWT_TTL_SECS` and `REFRESH_SLACK` promoted to module-scope constants in both auth crates, replacing six magic `Duration::from_secs(60)` literals with documented names.
- `CachedToken::is_fresh()` predicate + module-level `read_fresh_token(&RwLock<Option<CachedToken>>)` helper collapse three near-identical double-checked-locking cache-check blocks (Snowflake Password, Snowflake KeyPair, Databricks OAuth) into three lines each.
- `run.rs` tightened: dead `checked_div(max(1)).unwrap_or(0)` replaced with plain division; `state_sync_handle.as_ref()` becomes `&state_sync_handle`.

No behavior change from the refactor.

### Test coverage caveat

`Auth::invalidate_cache()` is covered in isolation for every auth variant (keypair, password, OAuth M2M, PAT, pre-supplied OAuth). The wire-up that `submit_and_wait` actually calls `invalidate_cache` on a live 401 response is **not** covered end-to-end — that needs a `wiremock`/`mockito` harness and is tracked as a follow-up. The behavior is still indirectly exercised: the 401 transience classification is unit-tested on both connectors.

## [1.8.1] — 2026-04-18

Patch release. One bug fix addressing an indefinite hang on remote state sync.

### Fixed — bounded HTTP timeouts on object store access

`ObjectStoreProvider::from_uri` previously built `AmazonS3Builder` /
`GoogleCloudStorageBuilder` / `MicrosoftAzureBuilder` without a `ClientOptions`,
so the underlying HTTP client had no per-request timeout. A stuck TCP connection
therefore hung the await indefinitely, and `object_store`'s own `RetryConfig`
(180s default) never got a chance to trigger because each attempt never
returned.

Observed in production on 2026-04-18: a `rocky run` emitted
`uploading state to object store` and then produced no further output for ~7h
until the outer scheduler cancelled the run.

Two fixes:

1. Every cloud builder is now constructed with
   `ClientOptions::new().with_timeout(60s).with_connect_timeout(10s)`, so any
   single request that stalls is aborted and retried under the existing
   `RetryConfig` budget.
2. `state_sync::{upload,download}_to_object_store` wrap the transfer in
   `tokio::time::timeout(300s, …)` as a belt-and-suspenders bound on the
   total operation. A new `StateSyncError::Timeout` variant makes the outer
   expiry distinguishable from per-request client timeouts for operators
   triaging hangs.

Both values are intentionally generous for the `state.redb` workload
(sub-20MB transfers) while still giving operators a definitive hang
guarantee.

## [1.8.0] — 2026-04-17

Perf-resilience sprint. Five engine changes that close the roadmap's "Top 5" pre-1.0 gaps plus two BigQuery quality-of-life fixes — broken out into this release because together they change Rocky's behaviour under failure, under concurrency, and against wide DAGs.

### Added — within-layer DAG parallelism

`DagExecutor::execute` now runs independent nodes in a single Kahn layer concurrently via `tokio::task::JoinSet` bounded by the existing `max_concurrency` knob. `NodeFuture` gained a `+ Send` bound. On a 50-wide bench (10 ms per node) this is 615 ms → 35 ms — **~17×** wall-clock speedup.

Two `.entered()` tracing spans in `run.rs` (`governance_setup`, `batched_checks`) held across awaits were converted to plain `Span` declarations to satisfy the `Send` bound; events inside those blocks no longer parent to those spans. Minor observability regression, no behaviour change.

### Added — graceful Ctrl-C / SIGTERM handling in `rocky run`

First SIGINT or SIGTERM now triggers in-process cleanup instead of a hard bail:

- Watermarks for completed tables are flushed.
- Tables that were in-flight or not yet started are written as `TableStatus::Interrupted` in the state store, so `rocky run --resume-latest` retries them without redoing `Success` rows.
- Partial JSON is emitted with the new `RunOutput.interrupted: bool` flag (always serialised).
- Process exits with code 130 via `rocky_cli::commands::Interrupted` (downcast in `rocky/src/main.rs`).

A watcher armed on the first signal hard-exits via `process::exit(130)` on a second Ctrl-C, so the user always has an escape hatch if cleanup itself hangs. SIGTERM is handled on Unix via a `#[cfg(unix)]` branch in the same `tokio::select!`; Windows falls back to default termination.

Also adds `PartitionStatus::Interrupted` for symmetry in partition-based pipelines.

### Added — advisory file lock on the state store

`StateStore::open` now takes an exclusive `fs4` advisory lock on `<state>.redb.lock` before opening the redb database. Two concurrent `rocky run` invocations against the same state path previously silently raced on writes; the second call now fails fast with `StateError::LockHeldByOther { path }`.

- New `StateStore::open_read_only` skips the lock for inspection paths (`rocky state`, `history`, `metrics`, `optimize`, `doctor`, the server APIs). Readers never block a live writer — redb's MVCC handles correctness.
- Writers (`run`, `load`, `run_local`) keep calling `open`.

### Added — `batch_describe_schema` for Snowflake and BigQuery

`BatchCheckAdapter::batch_describe_schema` now has real implementations on `rocky-snowflake` and `rocky-bigquery`, replacing N per-table `DESCRIBE` round-trips with a single `INFORMATION_SCHEMA.COLUMNS` query per schema/dataset. At 100 ms RPC latency on a 100-table schema, this shaves ~10 s off pre-flight.

- Snowflake scopes `{db}.information_schema.columns` with case-insensitive `UPPER(table_schema)` match.
- BigQuery scopes to `` `{project}`.`{dataset}`.INFORMATION_SCHEMA.COLUMNS `` (per-dataset by design).
- `batch_row_counts` / `batch_freshness` return "not yet implemented" for both — `run.rs` already falls back to per-table queries on error. Follow-ups.
- `AdapterRegistry::batch_check_adapter` tries Databricks → Snowflake → BigQuery → `None`. New `snowflake_connectors` / `bigquery_adapters` maps hold the typed clients so the batch path can reach them.

### Fixed — BigQuery async job polling

Any BigQuery query slower than the server's sync window previously returned `jobComplete: false` with an empty rows array and the adapter silently treated it as an empty result. `run_query` now polls `jobs.getQueryResults` when the initial response is async, using a Databricks-style ladder (100/200/500/1000/2000 ms fixed → exponential cap at 5 s), bounded by the adapter's `timeout_secs`.

Each poll asks BigQuery to hold the connection up to 10 s server-side via `timeoutMs`, so fast jobs don't pay a round-trip penalty while slow jobs re-check the client-side deadline promptly. On deadline, returns `BigQueryError::Timeout` instead of stale data.

### Schema cascade

`just codegen` regenerated `schemas/run.schema.json`, `integrations/dagster/.../run_schema.py`, and `editors/vscode/.../run.ts` for the new `RunOutput.interrupted` field. The dagster test fixtures under `tests/fixtures_generated/` were refreshed via `just regen-fixtures`.

### Upgrading

All changes are additive to the JSON output schema except `RunOutput.interrupted` which is always serialised (no skip-if-false). Pydantic and TypeScript consumers pick up the new field automatically after regenerating bindings. No config changes required.

## [1.7.0] — 2026-04-17

Closes the `rocky-project.schema.json` autogen arc. `editors/vscode/schemas/rocky-project.schema.json` is now fully generated from Rust types by `just codegen` — up from a 696-line hand-maintained file to 2462 lines of complete coverage across every variant, assertion kind, quarantine setting, governance sub-block, and load option. Additive to runtime parsing; one deliberate validation-tightening behavior change (see below).

### Changed — stricter config parsing (minor breaking)

`deny_unknown_fields` was added to `RockyConfig` and its top-level non-pipeline children (`StateConfig`, `CostSection`, `SchemaEvolutionConfig`, `AdapterConfig`, `RetryConfig`, `ExecutionConfig`, `GovernanceConfig`, `ChecksConfig`, `QuarantineConfig`, `FreshnessConfig`, `NullRateConfig`, `CustomCheckConfig`, `IsolationConfig`, `WorkspaceBindingConfig`, `GrantConfig`, `SchemaPatternConfig`, `MetadataColumnConfig`, `DiscoveryConfig`, and every pipeline-target subtype). A typo in `[state]`, `[cost]`, `[adapter.*]`, `[pipeline.x.target.governance]`, `[pipeline.x.execution]`, etc. now fails parse with a clear diagnostic instead of being silently ignored.

**Not applied** to the 5 pipeline variant structs themselves (`ReplicationPipelineConfig`, `TransformationPipelineConfig`, `QualityPipelineConfig`, `SnapshotPipelineConfig`, `LoadPipelineConfig`) and to `HooksConfig` — the former is incompatible with the schema's `type`-discriminator injection, the latter with `serde(flatten)` on the arbitrary-event-name HashMap. The omission is documented inline in `rocky-core::config`.

Empirical pre-flight: 47 committed POCs, zero unknown fields before 1.7.0 → zero regressions under the tightened parser.

### Added — `rocky-project.schema.json` autogenerated from Rust (non-pipeline portion)

`editors/vscode/schemas/rocky-project.schema.json` is now generated by `just codegen` from the `RockyConfig` schemars derive in `rocky-core`, replacing a 696-line hand-maintained file. The previous file had rotted — it knew only `replication` pipelines and was missing newer fields across `[state]`, `[hook]`, DQX assertions, quarantine, schema_evolution, and adapter additions.

- New schema export entry `rocky_project` in `engine/crates/rocky-cli/src/commands/export_schemas.rs`.
- `RockyConfig`, `StateConfig`, `StateBackend`, `CostSection`, `SchemaEvolutionConfig`, `RetryConfig`, `AdapterConfig`, `WebhookConfig`, `HookConfig`, `HooksConfig`, `HookEvent`, `FailureAction`, `WebhookConfigOrList`, `HookConfigOrList` all derive `JsonSchema`. `deny_unknown_fields` added everywhere it doesn't conflict with `serde(flatten)`.
- `AdaptersFieldSchema` / `PipelinesFieldSchema` schema-only helpers expose the deserializer's flat-vs-named shorthand (`[adapter]` vs `[adapter.foo]`) to the IDE schema, so the 38 of 46 committed POCs that use the flat form continue to validate cleanly.
- New `engine/crates/rocky-core/tests/project_schema.rs` validates every committed POC `rocky.toml` against both `AdapterConfig` and `RockyConfig` schemas — supersedes `tests/adapter_schema.rs`.
- New `scripts/copy_project_schema.py` and `just codegen-vscode-project-schema` recipe copy `schemas/rocky_project.schema.json` into the editor directory with a generated-file banner.
- `codegen-drift.yml` now also gates on `editors/vscode/schemas/rocky-project.schema.json`.
- POC `examples/playground/pocs/05-orchestration/06-valkey-distributed-cache/rocky.toml` had its unused top-level `[cache]` block removed — `CacheConfig` is defined but never wired into `RockyConfig`. The Valkey caching surface in this POC is the `tiered` state backend; standalone metadata caching is tracked separately.

### Added — `rocky-project.schema.json` autogenerated from Rust (pipeline portion)

Completes the schema-autogen arc started above. `[pipeline.*]` is now covered by a hand-written `JsonSchema` impl on `PipelineConfig` that mirrors the custom `Deserialize` impl — five `anyOf` arms, one per variant, with `type` optional (`const: "replication"`) for the back-compat default and `type` required for the other four (`transformation`, `quality`, `snapshot`, `load`). Schema grew from 724 lines (PR-a) to 2462 lines (this change) — the full surface of every variant, assertion kind, quarantine, governance sub-block, and load option.

- 5 variant structs (`ReplicationPipelineConfig`, `TransformationPipelineConfig`, `QualityPipelineConfig`, `SnapshotPipelineConfig`, `LoadPipelineConfig`) + their per-variant subtypes (`PipelineSourceConfig`, `DiscoveryConfig`, `PipelineTargetConfig`, `TransformationTargetConfig`, `QualityTargetConfig`, `TableRef`, `SnapshotSourceConfig`, `SnapshotTargetConfig`, `LoadTargetConfig`, `LoadOptionsConfig`, `LoadFileFormat`) all derive `JsonSchema`.
- Supporting types in `rocky-core::config` also gain `JsonSchema` + `deny_unknown_fields` where compatible: `ExecutionConfig`, `SchemaPatternConfig`, `MetadataColumnConfig`, `ChecksConfig`, `AggregateCheckToggle`, `QualityAssertion`, `QuarantineMode`, `QuarantineConfig`, `FreshnessConfig`, `NullRateConfig`, `CustomCheckConfig`, `GovernanceConfig`, `BindingType`, `WorkspaceBindingConfig`, `IsolationConfig`, `GrantConfig`.
- Hand-written `JsonSchema` impl on `ConcurrencyMode` — the custom `Deserialize` accepts `"adaptive"` or a positive integer; schema mirrors as `anyOf: [{const: "adaptive"}, {type: "integer", minimum: 1}]`.
- Hand-written `JsonSchema` impl on `tests::AggregateCmp` — schemars' derive drops serde `alias` attributes, so the auto-derived schema missed the symbolic forms (`<`, `<=`, `>`, `>=`, `==`, `!=`) that the deserializer accepts alongside `lt`/`lte`/etc.
- The 5 variant pipeline structs intentionally omit `serde(deny_unknown_fields)`. Runtime parsing is unaffected (never had it); but it's intentionally *not added* because the hand-written `PipelineConfig::json_schema` injects the `type` discriminator via an `allOf` branch, and under JSON Schema Draft-07 semantics `additionalProperties: false` on a sibling `allOf` branch rejects `type`. Trade-off: pipeline-block typos aren't flagged by the IDE schema; every other config section stays strict.
- POC fix: `examples/playground/pocs/04-governance/03-workspace-isolation/rocky.toml` had `binding_type = "BINDING_TYPE_READ_WRITE"`, which does not deserialize under the existing `BindingType` enum (`rename_all = "SCREAMING_SNAKE_CASE"` → expects `"READ_WRITE"`). The POC had always failed to parse at runtime; the schema-validation test surfaced it. Corrected to `binding_type = "READ_WRITE"`.
- `PipelineConfigSchemaPlaceholder` + its `Serialize`/`Deserialize`/`JsonSchema` trio are removed; `PipelinesFieldSchema`'s enum arms now reference `PipelineConfig` directly.

The `tests/project_schema.rs` validation test now walks every committed POC `rocky.toml` against the full `RockyConfig` schema, including pipelines.

### Added — shared schema artifacts

New `schemas/rocky_project.schema.json` (emitted by `rocky export-schemas`) is now part of the 38-file `schemas/` set alongside the CLI output schemas. The Pydantic `rocky_project_schema.py` (`integrations/dagster/src/dagster_rocky/types_generated/`) and TypeScript `rocky_project.ts` (`editors/vscode/src/types/generated/`) bindings are generated alongside it, though they're not currently consumed at runtime — they're published for parity with the CLI output bindings.

## [1.6.0] — 2026-04-17

Closes out DQX quality parity Phase 4 — five new row-level / table-level assertion kinds plus per-check `filter`. Additive; no breaking change.

### Added — Per-check `filter`

`TestDecl.filter: Option<String>` — a SQL boolean predicate that scopes an assertion to a subset of rows. Rows where `(filter)` evaluates to `TRUE` are subject to the assertion; rows where it's `FALSE` or `NULL` pass unconditionally.

- Applied to every `TestType` kind — wraps the generated WHERE clause (`(filter) AND ...`) for predicate-based kinds, pre-GROUP for set-based kinds.
- Quarantine integration: `CASE WHEN (filter) THEN base_valid_pred ELSE TRUE END` — out-of-scope rows stay in `__valid` even when `base_valid_pred` would fail them.

### Added — `TestType::InRange { min, max }`

Numeric range assertion (inclusive, half-open either side). NULL column values pass (matches existing `NOT IN` / `NOT (expr)` semantics). Bounds parse as `f64` and emit as SQL numeric literals — no user-SQL interpolation on the value path. Lowers into quarantine predicates.

**Temporal ranges stay on `NotInFuture` / `OlderThanNDays` (below) or `expression`.**

### Added — `TestType::RegexMatch { pattern }`

Dialect-specific regex match via the new `SqlDialect::regex_match_predicate` trait method. Default impl returns an error; the four production dialects override:

- **DuckDB:** `regexp_matches(col, 'pat')`
- **Databricks:** `col RLIKE 'pat'`
- **Snowflake:** `REGEXP_LIKE(col, 'pat')`
- **BigQuery:** `REGEXP_CONTAINS(col, r'pat')`

Patterns are validated against a strict allowlist (no single quotes, backticks, semicolons). NULL column values pass. Lowers into quarantine predicates.

### Added — `TestType::Aggregate { op, cmp, value }`

Table-level aggregate assertion: `SUM` / `COUNT(*)` / `AVG` / `MIN` / `MAX` with a comparison threshold (`lt` / `lte` / `gt` / `gte` / `eq` / `ne`, plus symbolic aliases `<`, `<=`, `>`, `>=`, `==`, `!=`). Emits `SELECT CASE WHEN <op> <cmp> <value> THEN 0 ELSE 1 END FROM t`. Not quarantinable (table-scoped).

### Added — `TestType::Composite { kind: "unique", columns }`

Multi-column uniqueness (requires at least two columns — single-column uniqueness stays on `Unique`). Emits `GROUP BY c1, c2, ... HAVING COUNT(*) > 1`. Not quarantinable (set-based).

### Added — `TestType::NotInFuture` + `TestType::OlderThanNDays { days }`

First-class sugar for timestamp bounds:

- `NotInFuture`: `col <= CURRENT_TIMESTAMP`.
- `OlderThanNDays { days }`: `col <= CURRENT_DATE - N days`.

Row-level, NULL-permissive, quarantinable. Two new `SqlDialect` trait methods with ANSI default impls:

- `current_timestamp_expr()` — default `CURRENT_TIMESTAMP` (keyword form; works for Databricks, Snowflake, DuckDB); BigQuery overrides to `CURRENT_TIMESTAMP()`.
- `date_minus_days_expr(days)` — default `CURRENT_DATE - INTERVAL 'N' DAY` (works for Databricks, Snowflake, DuckDB); BigQuery overrides to `DATE_SUB(CURRENT_DATE(), INTERVAL N DAY)`.

**DuckDB quirk:** rejects `CURRENT_TIMESTAMP()` with parens; default impl uses the keyword form.

### Other

- `rocky test` and `run_quality` both route through the new dialect-aware `generate_test_sql_with_dialect`, so every new kind works via both paths.
- 34 new rocky-core unit tests (19 Phase 4a, 14 Phase 4b, 1 `is_quarantinable` coverage).
- POC 06 (`examples/playground/pocs/01-quality/06-quality-pipeline-standalone`) extended to exercise every Phase 4 addition — 14 checks total, 196 valid / 4 quarantined rows on DuckDB.

## [1.5.0] — 2026-04-17

### Breaking — Quality pipelines fail on error-severity check failures by default

Quality pipelines now exit non-zero when any error-severity check fails. The previous behavior (always `Ok(())`) silently swallowed failures and forced orchestrators to parse the JSON output to detect them.

- Each `CheckResult` now carries `severity: "error" | "warning"` (default `error`).
- `ChecksConfig.fail_on_error` (default `true`) gates the exit behavior. Set `fail_on_error = false` to restore the pre-1.5.0 always-succeed semantics while still surfacing failing checks in the JSON output.
- Per-check severity on aggregate checks via the new `AggregateCheckToggle` untagged enum — `row_count = true` (legacy) and `row_count = { enabled = true, severity = "warning" }` (new) both parse.
- Freshness / null-rate / custom check configs all grow a `severity` field.

**Migration:** Configs with silently failing checks will start failing the run. If you want to keep the old behavior, add `fail_on_error = false` to every `[pipeline.x.checks]` block. Prefer to audit your checks and upgrade failure handling in the orchestrator.

### Added — Unified row-level assertions in the quality pipeline

`[[pipeline.x.checks.assertions]]` now accepts every declarative test type (`not_null`, `unique`, `accepted_values`, `relationships`, `expression`, `row_count_range`) via the same `TestDecl` surface used by `rocky test` model sidecars. Each entry targets a single table by name and carries its own `severity`.

```toml
[[pipeline.nightly_dq.checks.assertions]]
name     = "orders_customer_id_required"
table    = "orders"
type     = "not_null"
column   = "customer_id"
severity = "error"
```

Optional `name` disambiguates assertions that share table + kind + column. `SqlDialect::list_tables_sql(catalog, schema)` supports schema-level targeting (omit `table`) with DuckDB + BigQuery overrides.

### Added — Row quarantine (`split` / `tag` / `drop`)

`[pipeline.x.checks.quarantine]` compiles error-severity row-level assertions into a boolean predicate per table and emits CTAS statements that split the source:

- `mode = "split"` (default) — writes `<table>__valid` (passing rows) and `<table>__quarantine` (failing rows with per-assertion `_error_<name>` label columns).
- `mode = "tag"` — rewrites the source in-place with `_error_<name>` columns populated on failing rows.
- `mode = "drop"` — writes only the valid half; failing rows discarded.

Only `NotNull`, `AcceptedValues`, `Expression` at error severity drive the split (aggregate / set-based kinds like `Unique`, `Relationships`, `RowCountRange` stay observational). NULL-permissive predicates match existing `rocky test` semantics. Quarantine statements run before the valid statement so a partial failure leaves a stray quarantine table rather than a stale valid one. New `RunOutput.quarantine` field reports per-table mode, target tables, row counts, and error state.

### Added — `JsonSchema` on `AdapterConfig`

`AdapterConfig` / `AdapterKind` / `RetryConfig` now derive `JsonSchema`. The schema ships as `schemas/adapter_config.schema.json` and drives a new integration test that walks all 46 POC `rocky.toml` files and validates every `[adapter.*]` block against the generated schema. `RedactedString` got a manual `JsonSchema` impl (surface as plain `string`, never leak values). Groundwork for the broader `rocky-project.schema.json` autogen effort.

### Added — `[adapter.*] kind` field

`kind = "data" | "discovery"` declares the role the adapter plays. Required on discovery-only types (`fivetran`, `airbyte`, `iceberg`, `manual`); optional on warehouse types (default `"data"`); optional on dual-role `duckdb` (absent registers both roles). Backed by a canonical `rocky_core::adapter_capability` table, surfaced by `rocky validate` as V032 / V033 structured diagnostics.

### Refactored — `run.rs` decoupled from `rocky-databricks`

`AdaptiveThrottle` moved to `rocky-adapter-sdk::throttle` as a generic AIMD implementation; `BatchCheckAdapter` gained `batch_describe_schema`; `GovernanceAdapter` became registry-constructible as `Box<dyn GovernanceAdapter>`; `BatchTableRef` replaced with the canonical `rocky_core::ir::TableRef`. Zero ripple into other adapters. `rg "rocky_databricks::" run.rs` → 0.

### Fixed — Loader `rows_loaded` accuracy (Databricks + Snowflake)

The Databricks and Snowflake loaders now report the actual number of rows written by their COPY INTO statements instead of always reporting `0` in `LoadFileOutput.rows_loaded`.

- **Databricks** parses `num_affected_rows` from the COPY INTO response's single-row result set. `DatabricksLoaderAdapter::load` switched from `execute_statement` to `execute_sql` (returns the full `QueryResult`); the redundant `SELECT COUNT(*)` follow-up was removed.
- **Snowflake** parses `ROWS_LOADED` from the per-file COPY INTO result set and sums across files. Both the cloud-URI and local-file paths capture the count.

Both implementations fall back to `0` rather than erroring when the column is missing from the response.

### Fixed — Codegen-drift determinism

Fixtures previously captured wall-clock `watermark` / `last_value` timestamps from the full-refresh path, failing `codegen-drift.yml` CI on every re-run. Added both fields to `WALL_CLOCK_FIELDS` sanitization; `DagSummaryOutput.counts_by_kind` swapped from `HashMap` to `BTreeMap` for deterministic iteration. All 35 fixtures regenerated against the 1.5.0 output schema.

## [1.3.0] — 2026-04-16

### Added — Adapter `kind` field

`[adapter.*]` blocks in `rocky.toml` now accept `kind = "data" | "discovery"` to declare the role the adapter plays. Required on discovery-only types (`fivetran`, `airbyte`, `iceberg`, `manual`) so the role is self-evident in the raw config file without knowing the Rust trait surface of each adapter. Optional (defaults to `"data"`) for warehouse types (`databricks`, `snowflake`, `bigquery`); optional for the dual-role `duckdb` type, where absent means "register both roles".

A new canonical `rocky_core::adapter_capability` table backs the validation and replaces the discovery-only string match that used to live in `bridge::adapter_capabilities`.

**Breaking:** existing `rocky.toml` files with `type = "fivetran"` / `"airbyte"` / `"iceberg"` / `"manual"` adapter blocks must add `kind = "discovery"`. Parse-time error points at the exact fix. Data-only and DuckDB configs are unaffected.

### Added — Structured V032/V033 diagnostics in `rocky validate`

`rocky validate` now emits dedicated codes for adapter-kind issues:

- **V032** — `[adapter.*]` kind invariants (missing required `kind = "discovery"`, or a declared kind that the adapter's type doesn't support). `field` points at `adapter.<name>.kind`.
- **V033** — pipeline cross-reference role mismatches (`source.adapter` references an adapter whose role excludes data movement, or `source.discovery.adapter` references an adapter whose role excludes discovery). `field` points at the offending pipeline key.

A config with multiple independent kind issues now surfaces every one in a single `rocky validate` run instead of bailing on the first through the V001 catch-all. Production code paths still fail fast via `load_rocky_config`; the new public `rocky_core::config::parse_rocky_config` is the lenient counterpart used by the validate command.

### Changed — `validate_adapter_kinds` signature

`rocky_core::config::validate_adapter_kinds(&RockyConfig)` now returns `Vec<ConfigError>` (all issues) instead of `Result<(), ConfigError>` (first). `load_rocky_config` preserves fail-fast behaviour by taking the first error from the vec.

**Breaking:** direct callers of `validate_adapter_kinds` in the public Rust API must adapt. Only consumers embedding the engine as a library are affected; CLI users see no difference.

### Changed — Bridge capabilities driven from canonical table

`rocky_core::bridge::adapter_capabilities` now derives `can_export` / `can_import` from `rocky_core::adapter_capability::capability_for` instead of a hard-coded match on adapter-type strings. `cloud_storage` stays as a small local `matches!` (orthogonal bridge-specific concern). Behaviour is unchanged for every adapter.

### Fixed — Clippy 1.95 lints on `main`

- `rocky-sql::transpile`: collapse nested `if` inside a match arm into a match guard (`collapsible_match`).
- `rocky-core::state`: replace `sort_by(|a, b| b.x.cmp(&a.x))` descending-sort helpers in `get_run_history` and `get_quality_history` with `sort_by_key(|r| std::cmp::Reverse(r.x))` (`unnecessary_sort_by`).

## [1.2.0] — 2026-04-16

### Added — `LoaderAdapter` widened to support cloud URIs

`LoaderAdapter::load_file(&Path, ...)` is replaced by `load(&LoadSource, ...)`,
where `LoadSource` is `LocalFile(PathBuf)` or `CloudUri(String)`. Recognized
schemes: `s3://`, `s3a://`, `gs://`, `az://`, `abfs://`, `abfss://`. DuckDB's
httpfs extension consumes the URI verbatim; cloud warehouse loaders generate
COPY-style SQL pointing at the URI directly.

### Added — Cloud object store provider (`ObjectStoreProvider`)

New `rocky_core::object_store::ObjectStoreProvider` wraps Apache `object_store`
with one async API for S3, GCS, Azure, local files, and an in-memory backend.
Credential resolution follows the standard provider chains (`AWS_*` env vars,
`GOOGLE_APPLICATION_CREDENTIALS`, etc.) — Rocky does not re-implement
credential handling.

### Added — Databricks `LoaderAdapter` (`COPY INTO`)

New `rocky_databricks::loader::DatabricksLoaderAdapter` generates
`COPY INTO catalog.schema.table FROM '<uri>' FILEFORMAT = ...` SQL for sources
already in cloud storage. Supports CSV, Parquet, JSON. Local-file sources are
rejected with a clear error message — Databricks reads from cloud URIs
directly, so the user is expected to upload first.

### Added — Snowflake `LoaderAdapter` (stage + `COPY INTO`)

New `rocky_snowflake::loader::SnowflakeLoaderAdapter` and
`rocky_snowflake::stage` module. Two execution paths:

- **Local file:** `CREATE TEMPORARY STAGE → PUT file://... @stg → COPY INTO → DROP`
- **Cloud URI:** `CREATE TEMPORARY STAGE ... URL = 's3://...' → COPY INTO → DROP`

Temporary stages auto-expire at session end, so failed loads still clean up.
Explicit `DROP STAGE IF EXISTS` after each load keeps long-running sessions
tidy. Stage names are validated as SQL identifiers and generated with unique
timestamp suffixes to avoid concurrent collisions.

### Added — BigQuery `LoaderAdapter` (INSERT fallback)

New `rocky_bigquery::loader::BigQueryLoaderAdapter` streams local CSVs through
`CsvBatchReader` + `generate_batch_insert_sql` and dispatches the batches via
the existing BigQuery REST connector. **Status: starter / dev-scale only** —
INSERT-over-REST is orders of magnitude slower than native bulk load.
Production-scale BigQuery loading needs the Storage Write API (gRPC streaming
via `tonic`), deferred until demand appears. Cloud URIs and Parquet/JSON are
also not supported in this iteration.

### Added — `state_sync` migrated to `object_store`, plus GCS backend

`state_sync` no longer shells out to `aws s3 cp`. The S3 path goes through
`ObjectStoreProvider::download_file` / `upload_file`, eliminating the runtime
dependency on the AWS CLI binary. New `StateBackend::Gcs` variant plus
`gcs_bucket` / `gcs_prefix` fields on `StateConfig` add Google Cloud Storage
as a first-class state backend.

### Added — DAG-driven execution (`rocky run --dag`)

`rocky run --dag` walks the unified DAG via `execution_phases()` and
dispatches every node to its matching per-pipeline executor (replication,
transformation, quality, snapshot, load, seed). Skip-downstream-on-failure:
when a node fails, every node whose ancestor set contains the failed node is
reported as `Skipped` rather than executed. Unrelated branches still run to
completion.

Layers respect Kahn topology; nodes within a layer run sequentially on the
current task — within-layer parallelism is a follow-up (blocked on
`tracing::EnteredSpan` not being `Send` in `run.rs`).

New JSON output `DagRunOutput` is registered in `export_schemas`; the codegen
cascade refreshed `integrations/dagster/.../types_generated/` and
`editors/vscode/src/types/generated/`.

### Added — DAG status tracking + `GET /api/v1/dag/status`

New `DagStatusStore` on `ServerState` records the most recent DAG execution
result. The HTTP endpoint returns `DagStatus { completed_at, result }` (full
per-node breakdown), or `503` when no DAG run has been recorded.

### Added — Runtime cross-pipeline dependency inference

`unified_dag::infer_runtime_dependencies(&mut dag, &sql_by_name)` augments a
DAG with `DataDependency` edges discovered by parsing each model's SQL and
matching `FROM` references against producing nodes. Idempotent;
case-insensitive; ignores self-refs; gracefully tolerates invalid SQL. Lets
users skip per-model `depends_on` declarations when the SQL already makes
the dependency obvious.

### Added — `rocky run` Load pipeline dispatch

`rocky run --pipeline X` now supports Load pipelines by delegating to the
`rocky load` command. Previously hit an explicit bail; this is also the
prerequisite that makes Load nodes in DAG execution work.

### Notes

- The DAG executor composes over the existing per-pipeline executors, so
  `commands/run.rs` retains its direct `rocky_databricks::throttle` /
  `::batch` / `::governance` imports for the replication path. Deeper
  decoupling behind the adapter trait boundary is deferred.

### Added — Dagster Pipes protocol emitter (T2)

`rocky run` now speaks the [Dagster Pipes](https://docs.dagster.io/concepts/dagster-pipes)
wire protocol when invoked from a Dagster `PipesSubprocessClient`. New module
`crates/rocky-cli/src/pipes.rs` (~400 LOC, hand-rolled — no `dagster_pipes_rust`
dependency, only adds `base64` to `rocky-cli`):

- `PipesEmitter::detect()` reads `DAGSTER_PIPES_CONTEXT` + `DAGSTER_PIPES_MESSAGES`
  at the top of `run` / `run_local`. When either is unset (the common case for
  CLI invocation), returns `None` — zero overhead, zero behavior change.
- Supported messages-channel shapes: `{"path": "..."}` (file, append mode) and
  `{"stdio": "stderr"}`. `stdout` is rejected (reserved for the JSON `RunOutput`
  payload). S3/GCS variants are rejected (would pull extra deps).
- Emits one JSON-line message per event: `log` at run start/end, `closed` at
  end-of-run, `report_asset_materialization` per `output.materializations`
  entry, `report_asset_check` per `output.check_results` entry, and `log` at
  WARN level per `output.drift.actions_taken` entry.
- Asset keys are slash-joined per the Pipes wire convention to match the
  `Vec<String>` paths the engine emits in `MaterializationOutput`.
- Wired into both execution paths: `commands/run.rs` (Databricks) and
  `commands/run_local.rs` (DuckDB / non-Databricks). The shared
  `pub(super) emit_pipes_events()` helper keeps the wire format in sync across
  both paths.
- Emission is **batched at end of run**, not per-event streaming. Dagster's
  `PipesSubprocessClient` tails the messages file regardless of timing, so
  events still appear as individual run-viewer entries. Per-event streaming
  (threading the emitter through `process_table` / `run_one_partition`) is a
  follow-up that can land without changing the consumer side.

6 new `pipes::tests::*` unit tests in `rocky-cli`. The dagster half of T2
(`RockyResource.run_pipes()`) lives in `integrations/dagster/CHANGELOG.md`.
See `docs/dagster/pipes.md` for the integration guide.

### Added — Richer `MaterializationMetadata` fields (T1.4)

`MaterializationMetadata` (in `crates/rocky-cli/src/output.rs`) gains four new
optional fields, three of which are populated today:

- **`target_table_full_name: Option<String>`** — fully-qualified
  `catalog.schema.table` for click-through links. Always set when the
  materialization targets a known table. Wired in `run.rs` (replication path),
  `run.rs` (`time_interval` per-partition path), and `run_local.rs`.
- **`sql_hash: Option<String>`** — 16-char hex fingerprint (stdlib
  `DefaultHasher` / SipHash-1-3) of the SQL statements the engine sent to the
  warehouse, computed via the new module-level `output::sql_fingerprint()`
  helper. Lets users detect "what changed?" between runs without diffing full
  SQL bodies. Stable within a Rust release; not for cross-release persistence.
  Currently populated for `time_interval` materializations (where `stmts` is
  in scope from the SQL generation step); the replication path leaves it
  `None` because adapter dispatch is opaque to the call site.
- **`column_count: Option<u64>`** and **`compile_time_ms: Option<u64>`** —
  scaffolded but always `None` until the derived-model materialization path
  threads typed schema + per-model `PhaseTimings` slices through. Adding them
  later is a one-line change in two places.

Schema codegen consumers picked these up automatically: `schemas/run.schema.json`,
`integrations/dagster/.../types_generated/run_schema.py`, and
`editors/vscode/.../generated/` were all regenerated via `just codegen`.

### Added — `time_interval` materialization strategy

A fourth materialization strategy for partition-keyed tables. Idempotent
re-runs, late-arriving data correction, and per-partition observability —
the gap between Rocky's `incremental` and what dbt's `incremental +
partition_by` covers.

**Usage:**

```toml
[strategy]
type = "time_interval"
time_column = "order_date"   # column on the model output
granularity = "day"          # hour | day | month | year
lookback = 0                 # optional: recompute previous N partitions
first_partition = "2024-01-01"
```

```sql
SELECT DATE_TRUNC('day', order_at) AS order_date, ...
FROM stg_orders
WHERE order_at >= @start_date AND order_at < @end_date
GROUP BY 1
```

**New CLI flags on `rocky run`:**
- `--partition KEY` — run a single partition by canonical key
- `--from FROM --to TO` — run a closed inclusive range
- `--latest` — run the partition containing `now()` (UTC; default for time_interval)
- `--missing` — diff expected vs recorded, run the gaps (requires `first_partition`)
- `--lookback N` — also recompute the previous N partitions
- `--parallel N` — run N partitions concurrently (state writes still serialize)

**New JSON output fields:**
- `RunOutput.partition_summaries: Vec<PartitionSummary>` — one per partitioned model touched
- `MaterializationOutput.partition: Option<PartitionInfo>` — per-partition window info
- `CompileOutput.models_detail[].strategy` — full `StrategyConfig` discriminator (already shipping)

**Per-warehouse SQL:**
- **Databricks (Delta):** single `INSERT INTO ... REPLACE WHERE ...` (atomic)
- **Snowflake:** 4-statement vec (`BEGIN; DELETE; INSERT; COMMIT;`) — Snowflake's REST API runs one statement per call by default, so the runtime issues each separately and rolls back on failure
- **DuckDB:** same shape as Snowflake for symmetry

**State store:** new `PARTITIONS` redb table tracks per-partition lifecycle
(`Computed` / `Failed` / `InProgress`), keyed by `(model_name, partition_key)`.
`--missing` consults this table; `RUN_HISTORY` remains the source of truth
for whole-run success.

**Compiler diagnostics** (codes E020-E026, W003): the `validate_time_interval_models()`
typecheck pass enforces `time_column` exists in the output schema, has a
date/timestamp type (when known), is non-nullable, passes SQL identifier
validation; both `@start_date` and `@end_date` placeholders are present;
granularity matches the column type; and `first_partition` parses to a
canonical key for the grain. Type-shape checks (E021/E022/E025) are skipped
when the column type is `Unknown` (e.g., source schema not declared in
`source_schemas`) so the runtime catches mismatches at SQL execution time
instead of falsely blocking compile.

**Working demo:** `examples/playground/pocs/02-performance/03-partition-checksum/`
ships a runnable end-to-end POC against in-process DuckDB, including the
late-arriving-data scenario. Promoted from "spec only" status.

**Tests:** 41 new unit tests across `rocky-core` (TimeGrain arithmetic,
partition_key_to_window, expected_partitions, plan_partitions selection
modes, sql_gen Vec<String> migration), 16 in `rocky-compiler` (the 8
diagnostic codes), 7 in `rocky-cli` (PartitionRunOptions::to_selection),
and 7 end-to-end against in-process DuckDB exercising the full path.

**Documentation:** see [`docs/features/time-interval`](features/time-interval/)
for the full reference.

### Added — Phase 2: schema codegen pipeline

Every CLI command that emits `--output json` is now backed by a typed Rust output struct deriving `JsonSchema` (via the `schemars` crate). The schemas are exported to `../schemas/*.schema.json` by `rocky export-schemas` and consumed by:
- `integrations/dagster` — autogenerates Pydantic v2 models in `src/dagster_rocky/types_generated/` via `datamodel-code-generator`.
- `editors/vscode` — autogenerates TypeScript interfaces in `src/types/generated/` via `json-schema-to-typescript`.

28 typed command outputs covered: `discover`, `run`, `plan`, `state`, `doctor`, `drift`, `compile`, `test`, `ci`, `lineage` (+ `column_lineage`), `history` (+ `model_history`), `metrics`, `optimize`, `compare`, `compact`, `archive`, `profile-storage`, `import-dbt`, `validate-migration`, `test-adapter`, `hooks list`, `hooks test`, `ai`, `ai-sync`, `ai-explain`, `ai-test`. The ad-hoc `serde_json::json!()` pattern has been removed from every command file.

New cargo dependency: `schemars = "0.8"` at the workspace level (with `chrono` and `indexmap2` features), pulled into `rocky-cli`, `rocky-core`, `rocky-observe`, and `rocky-compiler`.

New CLI subcommand: `rocky export-schemas <dir>` (default: `schemas/`). Drives the dagster + vscode codegen pipelines.

New unit tests in `crates/rocky-cli/src/commands/export_schemas.rs::tests`:
- `every_entry_produces_a_valid_schema` — guards against misbehaving JsonSchema derives.
- `schema_names_are_unique` — catches duplicate registrations.
- `registered_schemas_match_committed_files` — catches the case where a new struct is registered but the regenerated schemas/ files aren't committed (or vice versa).

### Added — `rocky discover` checks projection

`DiscoverOutput` now includes an optional `checks` field that projects `rocky_core::config::ChecksConfig` (currently the freshness threshold) into the discover envelope. Downstream orchestrators consume it without re-parsing `rocky.toml` themselves.

### Changed — `Severity` JSON casing reverted to PascalCase

The `rocky_compiler::diagnostic::Severity` enum was briefly serialized as lowercase (`"error"`/`"warning"`/`"info"`) in an earlier draft of the schemars work but is now back to PascalCase (`"Error"`/`"Warning"`/`"Info"`) to stay compatible with the existing dagster fixtures and the hand-written Pydantic enum.

See [GitHub Releases](https://github.com/rocky-data/rocky/releases) for detailed release notes.
