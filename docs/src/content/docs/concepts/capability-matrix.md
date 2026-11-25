---
title: Who Enforces What
description: Each Rocky capability classified by who enforces it, so you know what the engine guarantees, what one adapter does, and what stays yours.
sidebar:
  order: 2.5
---

Rocky makes promises with three owners. Read this page before you build a compliance story, an approval workflow or an incident plan on Rocky. [Architecture of Trust](/concepts/architecture-of-trust/) grades the same primitives by maturity.

| Owner | Meaning |
|---|---|
| **Rocky-guaranteed** | The engine enforces it at compile or apply time, the same on every warehouse. |
| **Adapter-dependent** | The engine asks the warehouse adapter. The result depends on the adapter. |
| **External control** | Your warehouse or operations own it. Rocky records what it saw. |

Four words keep one meaning here: **Enforced** (the build or apply fails when the condition does not hold), **Attempted** (best-effort: Rocky tries, warns on failure, continues), **Recorded when available** (written when the execution path supplies it; a missing value is normal), **Durable** (the write must persist, or the operation fails closed).

## The matrix

| Capability | Who enforces it |
|---|---|
| [Contract columns, types, required, protected](#contracts-columns-types-required-protected) | Enforced at compile (E010–E014, E062, E063) and on every `rocky run` route, `--dag` included: a model with a contract error is not written. |
| [Classification tag completeness](#classification-tag-completeness) | Not enforced: Rocky warns (W004). Nothing blocks. |
| [Masking application](#masking-application) | Adapter-dependent: Databricks only, attempted |
| [Freshness](#freshness) | Declared metadata, not enforced. One opt-in run-time check, replication pipelines only. |
| [Human review of AI-authored plans](#human-review-of-ai-authored-plans) | Rocky-guaranteed: enforced floor at apply. The marker is checked, the approver is not authenticated. |
| [Audit ledger](#audit-ledger) | Rocky records it: attempted for ordinary rules, durable for budget and `verify_after` decisions. Some refusals produce no row. |
| [Byte-identical replay](#byte-identical-replay) | One write path only: content-addressed S3/UniForm. Elsewhere, recorded when available. |
| [Rollback after a bad apply](#rollback-after-a-bad-apply) | No automatic restoration. Rocky halts; recovery is yours. |
| [Grain and uniqueness](#grain-and-uniqueness) | Test-time assertion, not enforced at build |

## Contracts: columns, types, required, protected

**Rocky-guaranteed: enforced at compile.** `rocky compile` checks the model's inferred schema against its `.contract.toml` before anything runs. The codes are `E010` to `E014`, `E062` and `E063` (opt-in rules `[rules] no_new_nullable` for `E014` and `[rules] no_extra_columns` for `E063`). See the [diagnostic codes](/concepts/compiler/#diagnostic-codes).

Every `rocky run` reads the project `contracts/` directory and checks each contract in the compile that supplies the model SQL. A contract error keeps that model's existing table and withholds its downstream models. `rocky run --pipeline <NAME> --model <NAME> --contracts <DIR>` also requires the selected model to have a contract. A failed run can still record state and history.

Limits:

- `E011` runs only when Rocky infers a concrete type. For `Unknown` it does not run, and the column is `I003` (info, exit code unchanged). Give the compiler source schemas: `rocky compile`, `rocky test` and `rocky ci` read `data/seed.sql` when present. For a replication pipeline, `rocky discover --with-schemas` (or `rocky run`) fills the schema cache `rocky compile` reads. `discover` refuses a transformation-only pipeline: use a seed. A warehouse-dependent result such as `AVG` over a `DECIMAL` stays `Unknown` either way, and a `CAST` over an untypeable input clears `I003` only for some target types (`INT`, `BIGINT`, `TIMESTAMP` stay `Unknown`). See [The Rocky Compiler](/concepts/compiler/#4-type-check).
- `rocky compile --strict-contracts`, `rocky ci --strict-contracts` or `[contracts] strict = true` (also read by `rocky run` and `rocky test`) turns each `I003` into `E059`. `rocky run` describes the sources in the warehouse first, so it refuses only what stays unknown.
- `rocky compile` reads a source column as nullable unless a source schema says otherwise, so `nullable = false` on it is `E012`. Before any write, `rocky run` describes each source behind an `E012` and drops it when the warehouse reports NOT NULL. `rocky plan` stores those descriptions and `rocky apply` reuses them. A plan saved by an older Rocky has none, so its apply keeps the `E012`.
- A bare `Decimal` in a contract matches any precision and scale. `Decimal(18,2)` must match exactly.

Checked in `validate_contract` and `type_name_matches`, `engine/crates/rocky-compiler/src/contracts.rs`. Workflow: [Checks and Tests](/concepts/data-quality-checks/) and [Cross-team contracts](/concepts/cross-team-contracts/).

## Classification tag completeness

**Not enforced. Rocky warns; nothing blocks.** A column with a classification tag (for example `pii`) that no `[mask]` or `[mask.<env>]` block resolves raises warning `W004`. A tag in `[classifications] allow_unmasked` silences it. The compiler has no switch that turns it into an error. To enforce "every tagged column has a masking strategy", fail CI on `W004` in the compile output, or gate on `rocky compliance --fail-on exception`.

Checked in `check_classification_tags`,
`engine/crates/rocky-compiler/src/typecheck.rs`. Tag and mask semantics:
[Governance](/guides/governance/), sections "Allowed unmasked tags" and
"Compliance Rollup".

## Masking application

**Adapter-dependent: Databricks only, attempted there.** After a clean full-replication `--all`/`--models` model phase, Rocky reconciles masking best-effort: it walks each model's `[classification]` block and calls the governance adapter's masking hooks. Other paths (a single `--model` run, a backfill, a transformation pipeline) do not reconcile masks. On Databricks, Rocky writes Unity Catalog column tags and issues masking DDL, one statement per column. On DuckDB, Snowflake and BigQuery the hooks do nothing today.

When a masking statement fails, Rocky logs a warning and the run continues. A masking failure is not a build error and rolls nothing back. Do not state a compliance guarantee on this on any warehouse. Elsewhere, masking is an external control: use the warehouse's own tools.

See [Governance](/guides/governance/), "How apply works".

## Freshness

**Checked on demand by `rocky freshness`; not a gate on `rocky run`.** Two declarations describe how stale data may get: a model's `[freshness]` block (`warn_after`, `error_after`, `time_column`), and a transformation pipeline's `[[pipeline.<name>.sources]]` entry with a `freshness` block (`loaded_at_field`, `warn_after`, `error_after`, `filter`). A source is a table the models read but do not build.

```
  source  MAX(loaded_at_field) [WHERE filter]   ─┐  age = now - max
  model   MAX(time_column), or last good build  ─┘  age > error_after: error · age > warn_after: warn · else pass
```

The command exits 1 on any `error` or `runtime_error`. A `warn` alone exits 0. An empty source counts as never loaded. A model's one TTL is its warn threshold, or its error threshold under `severity = "error"`.

Other consumers: the scheduler (`rocky tick`, `rocky serve --scheduler`) reruns a pipeline with a freshness schedule once too much time passes since its last successful run. `rocky validate` checks the declaration is well-formed. The compiler warns `W005` for a temporal column with no declaration in scope, raises `E050` for a declaration it cannot evaluate, and `W050` for a column that is not a date or time.

`rocky run` does not check the declarations and does not skip a model downstream of a stale source. `rocky test` does not evaluate them. To gate a run, call `rocky freshness` first and stop on a non-zero exit.

One opt-in run-time check exists, for replication pipelines only: `[checks] freshness = { error_after = "..." }`. It measures lag with SQL after the run and records a failed check when the lag exceeds the threshold. Other pipeline types do not run it; `rocky validate` flags it there as inert (V034). The failure is advisory: run status comes from copied and failed tables, so the exit code does not change.

Code: `ModelFreshnessConfig` (`engine/crates/rocky-core/src/models.rs`), `PipelineSourceConfig` (`engine/crates/rocky-core/src/source_freshness.rs`), command (`engine/crates/rocky-cli/src/commands/freshness.rs`), compile checks (`engine/crates/rocky-compiler/src/freshness.rs`), scheduler (`engine/crates/rocky-core/src/schedule/demand.rs`), replication check `check_freshness` (`engine/crates/rocky-core/src/checks.rs`).

## Human review of AI-authored plans

**Rocky-guaranteed: enforced at apply, as a floor.** A plan an agent proposes is marked AI-authored. `rocky apply` refuses to execute it unless an approval marker is present that parses and names that exact plan. `rocky review <plan-id> --approve` writes the marker. Apply enforces the marker check, not the approver's identity:

- **A floor.** The check runs on every AI-authored apply, whatever `[policy]` says. A policy `allow` cannot waive it. A policy rule can only add restrictions.
- **Parse-and-match, not file-exists.** A truncated, malformed or mispasted marker is refused with its own error.
- **Not cryptographic.** The marker records who approved (best-effort git identity) and when (`rocky review <plan-id> --status` reports it). It has no signature: it proves an approval was recorded on this machine, not who wrote the bytes. Signed approvals are planned.

Engine v1.70.1 and earlier check only that a marker file exists, and skip even that when a `[policy]` rule resolves to `allow`. Use v1.71.0 or later.

Gate: `run_apply_ai_authored_plan`,
`engine/crates/rocky-cli/src/commands/apply.rs`. Marker shape and the
absent/approved/invalid states: `ReviewMarkerState`,
`engine/crates/rocky-cli/src/commands/review.rs`. Workflow:
[Operating Rocky with agents](/concepts/operating-rocky-with-agents/), "The
three gates".

## Audit ledger

**Rocky recording: attempted for ordinary rules, durable for the two gating kinds.** The mutating seams are `rocky apply`, promote, an agent's `rocky run`, and the `draft_*` and `propose` tools. They run the same evaluator into the same sink: a `policy_decisions` table in the embedded redb state store. For ordinary rules a failed write warns and continues. Some refusals never reach the table, because the gate can return a verdict before any rule runs. Decisions tied to an autonomy budget or to `verify_after` are durable: when they cannot persist, the apply fails closed. Treat the ledger as a best-effort record, not proof that a decision happened.

`rocky audit` lists the decisions. `rocky audit --for <table|run|plan>` assembles the custody chain for one subject: who proposed, what policy decided, what the plan changed, which runs materialized it, and what verification found. A link with no recorded signal renders as `unavailable`. Rocky never fabricates a value. The ledger is evidence, not enforcement, and is only as safe as your state store.

Ledger tables: `engine/crates/rocky-core/src/state.rs`. Command:
`engine/crates/rocky-cli/src/commands/audit.rs`. Reading the ledger without
Rocky: [Verify a Run Without Rocky](/guides/verify-a-run/).

## Byte-identical replay

**One write path only.** On the content-addressed write path (S3-backed Delta/UniForm), Rocky names each output Parquet file after the BLAKE3 hash of its own bytes, records the hash in the ledger, and pins byte-stability with an engine test. There, `rocky replay --execute --verify` re-derives a past run and compares hashes. A general run on DuckDB, Databricks, Snowflake or BigQuery records recipe identity, plus SQL hashes and row counts only when the execution path supplies them (a replication copy supplies neither). It emits no hash-named artifacts, so there is no byte-level replay proof. Treat a byte-identical claim as unverified on an adapter until you have run the verification there.

Scope and walkthrough: [Verify a Run Without Rocky](/guides/verify-a-run/).
Byte-stability test: `build_parquet_is_byte_stable_across_runs`,
`engine/crates/rocky-iceberg/src/uniform_writer/parquet_builder.rs`. Replay
grading: [Architecture of Trust](/concepts/architecture-of-trust/).

## Rollback after a bad apply

**Does not exist.** When a post-apply check (`verify_after` in a policy rule) fails, Rocky halts and reports. The mutation has already landed and stays. A transactional statement failure may roll back an uncommitted partition, but that is mid-run hygiene, not a restore. Plan recovery with the warehouse's own tools: time travel, snapshots, or a re-run from upstream data.

Halt-only behavior and its rationale: the `verify_after` gate in
`engine/crates/rocky-cli/src/commands/apply.rs` (the failure message states
the mutation has already landed).

## Grain and uniqueness

**Test-time assertion, not a declared constraint.** Rocky has no compile-time grain declaration. Declare tests: `type = "unique"` for one column, or a composite unique test for a multi-column key. `rocky test` runs them as SQL against the materialized data, so a duplicate is caught after it lands, and Rocky does not prevent one landing between runs.

Test kinds: `engine/crates/rocky-core/src/tests.rs`. Workflow:
[Checks and Tests](/concepts/data-quality-checks/).

## How to read a gap

Build controls on the Rocky-guaranteed rows, name the adapter when you rely on an adapter-dependent row, and own the external rows in your runbook.
