---
title: Who Enforces What
description: Each Rocky capability classified by who enforces it, so you know what the engine guarantees, what one adapter does, and what stays yours.
sidebar:
  order: 2.5
---

Rocky makes promises with three different owners. Some promises the engine
enforces itself, the same way on every warehouse. Some depend on which
warehouse adapter you run. Some belong to your warehouse or your operations,
and Rocky only observes or records.

This page classifies each capability by its owner. Read it before you build a
compliance story, an approval workflow, or an incident plan on top of Rocky.
Each section names the file or the page where you can check the claim. The
[Architecture of Trust](/concepts/architecture-of-trust/) page grades the same
primitives by maturity. This page answers a different question: when this
check matters, who actually runs it?

The three classes:

- **Rocky-guaranteed.** The engine enforces it at compile time or apply time.
  It behaves the same on DuckDB, Databricks, Snowflake, and BigQuery.
- **Adapter-dependent.** The engine asks the warehouse adapter to do it. What
  happens depends on the adapter, so the section names the warehouses.
- **External control.** Your warehouse or your operations own it. Rocky
  records what it saw, and nothing more.

Four more words appear across the rows, always with the same meaning:

- **Enforced.** The build or the apply fails when the condition does not
  hold.
- **Attempted.** Best-effort: Rocky tries, warns on failure, and continues.
- **Recorded when available.** Rocky writes the value when the execution
  path supplies it. A missing value is normal, not an error.
- **Durable.** The write must persist, or the operation fails closed.

## The matrix

| Capability | Who enforces it |
|---|---|
| [Contract columns, types, required, protected](#contracts-columns-types-required-protected) | Rocky-guaranteed: enforced at compile (E010–E013), with two stated limits |
| [Classification tag completeness](#classification-tag-completeness) | Not enforced: Rocky warns (W004). Nothing blocks. |
| [Masking application](#masking-application) | Adapter-dependent: Databricks only, attempted |
| [Freshness](#freshness) | Declared metadata, not enforced. One opt-in run-time check, replication pipelines only. |
| [Human review of AI-authored plans](#human-review-of-ai-authored-plans) | Rocky-guaranteed: enforced floor at apply. The marker is checked, the approver is not authenticated. |
| [Audit ledger](#audit-ledger) | Rocky records it: attempted for ordinary rules, durable for budget and `verify_after` decisions. Some refusals produce no row. |
| [Byte-identical replay](#byte-identical-replay) | One write path only: content-addressed S3/UniForm. Elsewhere, recorded when available. |
| [Rollback after a bad apply](#rollback-after-a-bad-apply) | No automatic restoration. Rocky halts; recovery is yours. |
| [Grain and uniqueness](#grain-and-uniqueness) | Test-time assertion, not enforced at build |

## Contracts: columns, types, required, protected

**Rocky-guaranteed: enforced at compile.** A `.contract.toml` declares what a model must produce.
`rocky compile` checks the model's inferred schema against it, before anything
runs, and fails on any of these four errors:

- `E010`: a required column is missing.
- `E011`: a column's type does not match.
- `E012`: the contract says non-nullable, the model output is nullable.
- `E013`: a protected column was removed.

Two limits, stated plainly:

- The type check (`E011`) applies only when Rocky infers a concrete type for
  the column. When inference returns `Unknown`, the check is skipped by
  design, so an unresolvable expression does not fail its contract.
- `Decimal` matches on the name alone. A contract that says `Decimal(18,2)`
  accepts any precision and scale today
  ([#1466](https://github.com/rocky-data/rocky/issues/1466)).

Checked in `validate_contract` and `type_name_matches`,
`engine/crates/rocky-compiler/src/contracts.rs`. Workflow:
[Testing](/concepts/testing/) and
[Cross-team contracts](/concepts/cross-team-contracts/).

## Classification tag completeness

**Not enforced. Rocky warns; nothing blocks.** When a column carries a classification tag
(for example `pii`) that no `[mask]` or `[mask.<env>]` block resolves, the
compiler emits warning `W004`. A tag listed in
`[classifications] allow_unmasked` silences it. The warning does not fail the
compile, and the compiler has no switch that turns it into an error. If your
policy is "every tagged column must have a masking strategy", enforce that in
CI: fail on `W004` in the compile output, or gate on
`rocky compliance --fail-on exception`.

Checked in `check_classification_tags`,
`engine/crates/rocky-compiler/src/typecheck.rs`. Tag and mask semantics:
[Governance](/guides/governance/), sections "Allowed unmasked tags" and
"Compliance Rollup".

## Masking application

**Adapter-dependent: Databricks only, attempted there.** After a clean
full-replication `--all`/`--models` model phase, Rocky reconciles masking
best-effort: it walks each model's `[classification]` block and calls the
governance adapter's masking hooks. Other execution paths (a single
`--model` run, a backfill, a transformation pipeline) do not reconcile
masks today. On Databricks, Rocky writes Unity Catalog column tags and
issues masking DDL, one statement per column. On DuckDB, Snowflake, and
BigQuery the masking hooks do nothing today.

Attempted means: when a masking statement fails, Rocky logs a warning and
the run continues. A masking failure is not a build error and does not roll
anything back. Do not state a compliance guarantee on top of this on any
warehouse. On warehouses other than Databricks, masking is an external
control: apply it with the warehouse's own tools.

Described, with the same caveats, in [Governance](/guides/governance/),
section "How apply works", and graded in the
[Architecture of Trust](/concepts/architecture-of-trust/).

## Freshness

**Declared metadata, not a gate.** A model's `[freshness]` block
(`expected_lag_seconds`, `time_column`) declares how stale the model may get.
Three things consume the declaration:

- The scheduler (`rocky tick`, `rocky serve --scheduler`) turns the declared
  budget into demand: a pipeline with a freshness schedule runs again once
  too much time passes since its last successful run.
- `rocky validate` checks the declaration itself is well-formed.
- The compiler warns (`W005`) when a model has a temporal column but no
  freshness declaration in scope.

What the declaration does **not** do: `rocky test` does not evaluate it, and
no materialization is blocked by it. Declaring `[freshness]` on a model does
not create a run-time staleness alarm. Detecting stale data in production is
an observation job that you own.

One separate, opt-in run-time check exists, for replication pipelines only:
`[checks] freshness = { threshold_seconds = ... }` measures the real lag with
SQL after the pipeline runs, and records the check as failed when the lag
exceeds the threshold. Other pipeline types do not execute that check today;
`rocky validate` flags it there as inert (V034). It is a pipeline check with
its own key, not the model's `[freshness]` declaration.

That recorded failure is advisory. The replication runner does not stop on it.
The run's status comes from copied and failed tables, never from check results,
so the exit code does not change. Rocky reports the outcome and leaves the
decision to your orchestrator.

Declaration: `ModelFreshnessConfig`, `engine/crates/rocky-core/src/models.rs`
(its own doc comment states the compiler does not enforce it). Scheduler
demand: `engine/crates/rocky-core/src/schedule/demand.rs`. Run-time check:
`check_freshness`, `engine/crates/rocky-core/src/checks.rs`.

## Human review of AI-authored plans

**Rocky-guaranteed: enforced at apply, as a floor.** A plan proposed by an agent is marked
AI-authored. `rocky apply` refuses to execute it unless an approval marker is
present that parses and names that exact plan. `rocky review <plan-id> --approve`
is the command that writes that marker. What apply enforces is the marker check,
not the identity of the approver. Three properties of that gate, as it ships
today:

- **It is a floor.** The marker check runs on every AI-authored apply,
  whatever your `[policy]` rules say. A policy `allow` cannot waive it; a
  policy rule can only add restrictions on top.
- **It is parse-and-match, not file-exists.** The marker must parse and must
  name the exact plan being applied. A truncated, malformed, or mispasted
  marker is refused with its own error. It never counts as an approval.
- **It is not cryptographic.** The marker records who approved (best-effort
  git identity) and when, and `rocky review <plan-id> --status` reports it.
  The marker carries no signature. It proves an approval was recorded on this
  machine, not who wrote the bytes. Signed approvals are a planned
  hardening, not a shipped one.

The first two properties arrived in engine v1.71.0. Releases up to and
including v1.70.1 check only that a marker file exists, and skip even that
when a `[policy]` rule resolves to `allow`. Upgrade to v1.71.0 or later before
you rely on the floor.

Gate: `run_apply_ai_authored_plan`,
`engine/crates/rocky-cli/src/commands/apply.rs`. Marker shape and the
absent/approved/invalid states: `ReviewMarkerState`,
`engine/crates/rocky-cli/src/commands/review.rs`. Workflow:
[Operating Rocky with agents](/concepts/operating-rocky-with-agents/), "The
three gates".

## Audit ledger

**Rocky recording: attempted for ordinary rules, durable for the two gating kinds.**
Policy-decision recording is best-effort for ordinary rules. At a mutating seam
— `rocky apply` and promote, plus the `draft_*` and `propose` tools, which run
the same evaluator into the same sink — Rocky writes each evaluated decision to
a `policy_decisions` table in the embedded redb state store, and when that write
fails it warns and continues. Some refusals never reach the table at all,
because the gate can return a verdict before any rule is evaluated. Decisions
relevant to an autonomy budget or to `verify_after` are durable instead: when
they cannot be persisted, the apply fails closed. Treat the ledger as a
best-effort record rather than proof that a decision happened. `rocky audit` lists the decisions.
`rocky audit --for <table|run|plan>` assembles the custody chain for one
subject: who proposed, what policy decided, what the plan changed, which runs
materialized it, and what verification found. A link with no recorded signal
renders as `unavailable`. Rocky does not fabricate a value to complete the
chain.

The ledger records what Rocky did. It is evidence, not enforcement: it stops
nothing by itself, and it lives in whatever state store you configure. The
ledger is only as safe as that store.

Ledger tables: `engine/crates/rocky-core/src/state.rs`. Command:
`engine/crates/rocky-cli/src/commands/audit.rs`. Reading the ledger without
Rocky: [Verify a Run Without Rocky](/guides/verify-a-run/).

## Byte-identical replay

**One write path only.** On the content-addressed write path (S3-backed
Delta/UniForm materialization), Rocky names each output Parquet file after
the BLAKE3 hash of its own bytes and records the hash in the ledger. An
engine test pins that the writer is byte-stable across runs. On that path,
`rocky replay --execute --verify` re-derives a past run and compares hashes.

A general run against DuckDB, Databricks, Snowflake, or BigQuery records
less. When persistence succeeds, successful materializations record recipe
identity; SQL hashes and row counts are recorded only when the execution
path supplies them (a replication copy, for example, supplies neither). A
general run emits no hash-named artifacts, so there is no byte-level replay
proof on those targets. Treat any byte-identical claim as unverified on a
given adapter until you have run the verification against that adapter
yourself.

Scope and walkthrough: [Verify a Run Without Rocky](/guides/verify-a-run/).
Byte-stability test: `build_parquet_is_byte_stable_across_runs`,
`engine/crates/rocky-iceberg/src/uniform_writer/parquet_builder.rs`. Replay
grading: [Architecture of Trust](/concepts/architecture-of-trust/).

## Rollback after a bad apply

**Does not exist.** When a post-apply verification check
(`verify_after` in a policy rule) fails, Rocky halts and reports. The
mutation has already landed in the warehouse and it stays there. Rocky has
no automatic restoration after a failed `verify_after` check; transactional
statement failures may still roll back an uncommitted partition. That
narrower mechanism is mid-run hygiene on transactional warehouses, not a
restore. No restore substrate exists, because a plain warehouse table has
no engine-owned prior version.

Recovery after a bad apply is an external control. Plan for it with the
warehouse's own tools: time travel, snapshots, or a re-run from upstream
data. Rocky's role ends at halting loudly and recording what happened.

Halt-only behavior and its rationale: the `verify_after` gate in
`engine/crates/rocky-cli/src/commands/apply.rs` (the failure message states
the mutation has already landed).

## Grain and uniqueness

**Test-time assertion, not a declared constraint.** Rocky has no compile-time
grain declaration. To assert a model's grain, you declare tests: `type =
"unique"` for one column, or a composite unique test for a multi-column key.
`rocky test` runs them as SQL against the materialized data, so a duplicate
is caught after the data exists, not before. Rocky does not prevent a
duplicate from landing between test runs.

Test kinds: `engine/crates/rocky-core/src/tests.rs`. Workflow:
[Testing](/concepts/testing/).

## How to read a gap

A row that says "adapter-dependent" or "external control" is not a defect
list. It is the boundary of what the engine can honestly promise. Build your
controls on the rows marked Rocky-guaranteed, name the adapter when you rely
on an adapter-dependent row, and own the external rows in your runbook.
