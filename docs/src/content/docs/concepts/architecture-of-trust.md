---
title: The Architecture of Trust
description: Every trust claim Rocky makes, graded against what ships today.
sidebar:
  order: 2
---

This page grades each of Rocky's trust primitives against what ships today. Where
a primitive is partial or still on the roadmap, the grade says so.

## The failures worth designing against

The expensive failures on a mature data platform are trust failures, not slow
queries. They share a shape. A change happens somewhere, nothing breaks loudly,
and the damage surfaces days later in a number someone trusted. The
[Introduction](/getting-started/introduction/) lists the specific ones.

Every primitive below exists to turn one of those failures into something you
catch before you merge.

## The mental model: code, typed IR, warehouse

Rocky sits between your code and your warehouse as a typed compiler.

```
  your code                Rocky                 your warehouse
  ─────────                ─────                 ──────────────
  .sql / .rocky  ────────► typed IR    ────────► storage
  .toml sidecars           every model,          compute
  contracts                every column,         the tables
                           every type
                               │
                               │ before anything runs
                               ▼
                   E### errors, W### warnings,
                   lineage, drift, cost, contracts
```

Your SQL and config compile down to a typed intermediate representation, or
[IR](/reference/glossary/#ir-intermediate-representation). The IR knows every
model, every column, and every type across the full dependency graph. The
warehouse still owns storage and compute. Rocky owns the graph, and the compiler
is the trust boundary.

That single decision is what makes the rest possible. Once the graph is typed and
known before execution, three things follow. A schema change becomes a type
error. A missing contract column becomes a diagnostic with a code. A column's
downstream blast radius becomes a query you can run in CI. None of that is
reachable from a template that substitutes strings, because a string template
never has to know what a column is.

Rocky is not a warehouse, not a table format, and not a query engine. It is the
typed layer above whichever of those you have chosen, and it stays
warehouse-neutral on purpose.

## The trust primitives, graded honestly

Each primitive below names its real CLI surface or diagnostic code. The inline
grade tells you how far it ships today, so you can plan around the edges instead
of finding them later.

### Compile-time types and diagnostics

Rocky infers column-level types across the whole DAG. It reports problems as
diagnostic codes you can grep in a CI log. The error codes run from `E001` to
`E036`, with `W` warnings and `P` lints alongside.

Compilation fails on any error-level diagnostic. That is the whole point: the
failure becomes a non-zero exit code at PR time, not a wrong number in
production.

**Shipped.** This is the foundation the other primitives build on.

### Compile-time column-level lineage

`rocky lineage <model>` traces a model's inputs and outputs.
`rocky lineage <model>.<column>` traces a single column through every
transformation that touches it. The edges come from the compiler's semantic
analysis, so Rocky computes lineage at compile time rather than reconstructing it
afterwards.

`rocky lineage-diff main` turns that into a blast-radius report for PR review.
Change a column, and see exactly which downstream columns it affects before you
merge.

The lineage graph is intra-project. It knows the columns inside one Rocky
project, not across project boundaries.

**Shipped, within a project.**

### Branches

`rocky branch create` and `rocky run --branch <name>` give you isolated branches
for development and review. A branch today is a schema prefix: models for branch
`feature_x` materialize under a `branch__feature_x` namespace, so a branch never
touches a production table. `rocky branch approve` writes an artifact under
`.rocky/approvals/<branch>/` by default. `--out` sends it somewhere else, and
the gate only reads the default directory, so an artifact written elsewhere
does not count.

The gate that reads those artifacts is off by default. Set
`[branch.approval] required = true` to turn it on. With it off, the promote
path does not read the directory at all.

With it on, the gate runs when the promote plan is built. That is `rocky plan
promote`, and bare `rocky branch promote <name>`, which builds a plan first.
Applying a promote plan that already exists does not repeat the check, so the
approvals are the ones that were valid at plan time. The gate loads every
artifact in that directory and counts the valid ones against `min_approvers`.

An artifact is valid when its blake3 digest still matches its own contents, its
recorded branch state hash matches the branch now, it is not dated in the
future, and it is not older than `max_age_seconds`. When `allowed_signers` is
not empty, the approver's email must also be on that list.

`min_approvers` counts artifact FILES, not distinct people. Nothing
de-duplicates by identity, so one approver can satisfy a threshold of two by
writing two artifacts. It defaults to 1, and setting it to 0 makes the count
pass with no approvals at all.

Two things bypass the gate even when it is on. `rocky branch promote
--skip-approval` skips it, and so does the `ROCKY_BRANCH_APPROVAL_SKIP`
environment variable. Both record the reason as an audit event rather than
failing.

That digest is not a cryptographic signature, and it is not a tamper boundary.
It is unkeyed, so anything that can write the file can change the artifact and
recompute the digest together. What it catches is a modification that was not
re-hashed. It authenticates nobody: the approver's email is a self-asserted git
identity hashed with the rest of the artifact, and a writer can set it to
anything.

What you get today is schema-prefix isolation, not a warehouse-native zero-copy
clone. Delta `SHALLOW CLONE` and Snowflake zero-copy `CLONE` would make branch
creation near-instant and free of storage cost. That integration is a follow-up,
not what runs now.

**Partial.** Schema-prefix branches and promotion ship today. The approval gate ships too, but it is off until you set `[branch.approval] required = true`, and its check is a digest, not a signature. Warehouse-native clones and signed approvals do not ship.

### Per-model cost

Rocky records per-model cost on every run, which makes cost a property of a model
rather than a line on an invoice.

The accuracy depends on the warehouse:

- **BigQuery.** Bytes scanned maps directly to billing, so the figure is billing-exact.
- **Databricks and Snowflake.** The figure is a duration × DBU-rate estimate. A DBU is Databricks' compute billing unit. Databricks also reports warehouse-scanned bytes, surfaced for observability rather than pricing, because Databricks prices by DBU. Snowflake's bytes plumbing is still a follow-up.
- **DuckDB.** Zero.

**Partial.** Per-model cost populates on every run. It is billing-exact on BigQuery and a duration-based estimate on Databricks and Snowflake. Databricks surfaces scanned bytes for observability. Snowflake's bytes plumbing is the follow-up.

### Compile-time contracts

A `.contract.toml` declares what a model must produce. The compiler checks the
model's inferred schema against it. Four codes cover the intra-project case:

- `E010`: a required column is missing from the model output.
- `E011`: a column's type does not match the contract.
- `E012`: the contract says non-nullable and the model output is nullable.
- `E013`: a protected column has been removed.

Any of these fails compilation, so a broken contract is a red CI check rather
than a production surprise.

Those four are intra-project: they check a model against a contract inside one
Rocky project. Enforcement across a project boundary also ships, through a
**vendored snapshot**. Vendored means the consuming team keeps its own committed
copy of the producing team's compiled schema, and diffs against that copy.

The flow has three parts. A producer runs `rocky publish-ir` to publish a
snapshot of its compiled IR. A consumer vendors that snapshot and declares an
`[imports.<name>]` block, with `baseline`, `snapshot`, and an optional `pin`,
maintained by `rocky imports update [--check]`. The consumer's `rocky compile`
then diffs the baseline against the snapshot and fails on a producer's breaking
change:

- `E030`: a column the consumer reads was dropped.
- `E031`: that column's type narrowed.
- `E032`: it went from nullable to NOT NULL.
- `E033`: the snapshot drifted from the pinned recipe hash.
- `E034`: the snapshot format is newer than this build can read.

`W030` and `W031` cover the non-breaking cases: an added column and a widened
type. See [Cross-Team Contracts](/concepts/cross-team-contracts/) for the full
workflow.

**Shipped.** Intra-project (`E010`–`E013`) and cross-team via published-IR snapshots (`E030`–`E034`, enforced at the consumer's compile).

### Declarative governance

Rocky models governance as code through a `GovernanceAdapter`. The surface covers
tag management, grant and revoke, workspace bindings, column tags, masking
policies bound to classification tags, and role-graph reconciliation.

How much of that is real depends entirely on the warehouse:

- **Databricks** implements the full surface through Unity Catalog.
- **Snowflake** reconciles object tags (`ALTER … SET TAG`) and `GRANT`/`REVOKE` role grants, plus retention policy. Workspace binding and masking are not driven.
- **BigQuery** reconciles tags as labels (`ALTER … SET OPTIONS(labels=…)`). Grants map to IAM, so `apply_grants` and `revoke_grants` currently log and do nothing. Real IAM integration is a follow-up.
- **DuckDB** does nothing, because it has no governance model to drive.

So governance at depth is a Databricks capability today. The skeleton is
warehouse-neutral. The depth is not yet portable.

**Partial.** Full on Databricks; tags + `GRANT`/`REVOKE` on Snowflake; label-based tagging only on BigQuery (grants no-op, IAM follow-up); no-op on DuckDB.

### Schema drift handling

When a source schema changes under a materialized model, Rocky does not quietly
keep going. It picks one of three responses: ignore the change, apply a safe
column-type widening, or drop and recreate the table. A grace period runs before
any destructive action. Drift becomes an explicit, graded decision instead of a
silent divergence.

**Shipped.**

### Content-addressed writes and replay

Replay means two distinct things. Being precise about which one ships matters.

**The first is deterministic recording with ledger verification.** Rocky records
each run's per-model SQL hashes, row counts, bytes, and timings when the
execution path supplies them. Deterministic content-addressed materializations
produce hash-named artifacts; ordinary warehouse runs do not provide byte-level
replay proof. On the content-addressed path, each file is named by the hash of
its own bytes, so the same inputs and code produce the same physical files.
`rocky replay <run_id>` inspects that record and verifies it against the ledger.
That ships today.

Every materialization also stamps a **recipe-identity triple**: three hashes that
together answer "what produced this row set".

- `recipe_hash` fingerprints the model's canonical typed IR, so the same program hashes the same no matter when it ran.
- `input_hash` covers the inputs it read.
- `env_hash` covers the engine, adapter, and dialect it ran under.

`rocky history --recipe <hash>` answers the audit question directly: what
produced this, and every other time this exact program ran. The triple is honest
about its own strength. An `input_hash` proven by an observed freshness signature
is tagged `heuristic` and is never presented as a claim about byte content. A
content-addressed input is tagged `strong`. This is an identity and audit
primitive, not a reproducibility claim.

**The second is re-execution from the pinned record.** To replay a past run,
Rocky rebuilds each model's recipe from provenance, never from the working tree.
It then re-runs that recipe to reproduce the output from scratch.
`rocky replay --execute --verify` does this and compares the re-derived BLAKE3
hash against the recorded one.

It runs on a local DuckDB engine by default, or against the live warehouse with
`--warehouse`. The warehouse path materializes into an isolated
`hcv2_replay_<run>` schema, never the recorded target's production location. It
encodes the recomputed artifact with the target table's own physical column
mapping. So a `bit_exact` verdict means the warehouse reproduced the recorded
bytes exactly.

Re-execution is scoped honestly. It covers deterministic, content-addressed
models. A model that reads a mutable source is classified `non_replayable`
instead of being re-run against current data. A non-deterministic recipe is
flagged, so a `diverged` verdict there is expected rather than a failure.

**Shipped for deterministic content-addressed models.** Recording and ledger verification ship; re-execution ships for the deterministic content-addressed case (mutable-source models classified `non_replayable`, non-deterministic recipes flagged).

Content-addressed materialization itself ships for single-writer Delta and
UniForm, a Delta feature that also publishes Iceberg metadata. It writes
blake3-hashed Parquet files plus a Delta log commit, and Iceberg-compatible
readers see the same snapshot. It is single-writer. It does not yet cover
multi-writer concurrency, broad schema evolution, or deletion vectors. A deletion
vector is a Delta feature that records deleted rows in a side file rather than
rewriting the Parquet.

**Partial.** Single-writer content-addressed Delta/UniForm ships; multi-writer, broad schema evolution, and deletion vectors do not.

### VS Code trust overlays

The VS Code extension draws the lineage graph and paints four trust signals onto
it. Each one is backed by CLI output.

1. **Drift**: schema drift against the warehouse. This overlay expects a dedicated drift command. There is no standalone `rocky drift` subcommand yet, because drift is detected inside `rocky run` and `rocky plan`. The overlay degrades to unavailable until that surface lands.
2. **Breaking**: breaking changes from the semantic CI diff.
3. **Replay**: the last recorded run for each model.
4. **Governance**: compliance and masking status.

**Shipped (four overlays).**

## The honesty grade

Every load-bearing claim, in one table. The partial rows are where teams get
surprised.

| Claim | Grade | What that means |
|---|---|---|
| Compile-time column-level types and diagnostics (`E###` errors) | Shipped | Compilation fails on any error-level diagnostic. |
| Compile-time column-level lineage + `lineage-diff` blast radius | Shipped | Intra-project; computed at compile time. |
| Compile-time contracts (`E010`–`E013`) | Shipped | Intra-project contract validation against inferred schema. |
| Schema drift handling (ignore / safe widen / drop-and-recreate) | Shipped | Explicit graded response with a grace period. |
| Dialect-divergence lint (`P001`) | Shipped | Opt-in via `--target-dialect`; error severity. |
| VS Code trust overlays | Shipped | Exactly four: Drift, Breaking, Replay, Governance. |
| Branches | Partial | Schema-prefix isolation with promotion. The approval gate is opt-in (`[branch.approval] required = true`) and checks an unkeyed digest: an integrity checksum, not a tamper boundary, and it authenticates nobody. No warehouse-native zero-copy clones yet. |
| Replay | Partial | Deterministic recording + ledger verification, plus re-execution (`rocky replay --execute --verify`, local or `--warehouse`) for deterministic content-addressed models; mutable-source models are `non_replayable`, non-deterministic recipes flagged. |
| Content-addressed writes | Partial | Single-writer Delta/UniForm; no multi-writer, broad schema evolution, or deletion vectors yet. |
| Per-model cost | Partial | Billing-exact on BigQuery; a duration × DBU-rate estimate on Databricks and Snowflake; zero on DuckDB. Databricks surfaces scanned bytes for observability; Snowflake's warehouse-reported-bytes plumbing is the follow-up. |
| Declarative governance | Partial | Full on Databricks (Unity Catalog); tags + `GRANT`/`REVOKE` on Snowflake; label-based tagging only on BigQuery (grants no-op, IAM follow-up); no-op on DuckDB. |
| Cross-team / cross-project contract enforcement | Shipped | Producer `rocky publish-ir` → consumer `[imports.<name>]` vendored snapshot → `E030`–`E034` enforced at the consumer's `rocky compile`. |

## What to lead with

If you are deciding whether Rocky is worth your team's time, lead with the
enforcement plane: branches, content-addressed replay, per-model cost,
declarative governance, the dialect-divergence lint (`P001`), and compile-time
contracts. The lint alone is useful the day you start a warehouse migration, and
essential the day you finish one.

Rocky being written in Rust matters for speed, and for the existence of a real
LSP. It is not the reason to choose it. The reason is that the failure modes
above become compile errors and CI gates.

## Where Rocky sits next to the adjacent tools

You are probably holding Rocky up against something already in your stack. Here
is the honest framing for two of them.

### Databricks LakeFlow (head-to-head, with a caveat)

LakeFlow is coupled to the warehouse and comes free with the platform. Rocky
differentiates if portability across warehouses matters to you, and if a real
compiler with serious tooling matters. If neither does, the warehouse-native
option may simply be good enough for your team. That is a legitimate answer.

### Polaris and the open table formats (category clarification)

This one is a category question, not a head-to-head. Polaris is Snowflake's
Iceberg REST catalog. Iceberg and Delta are open table formats. Rocky is none of
those. Rocky targets them. It writes content-addressed Delta and UniForm that an
Iceberg-compatible reader can consume. It treats the format and the catalog as
the substrate it sits above.

---

Rocky is the typed graph between your code and whichever warehouse, table format,
or query engine you've chosen.
