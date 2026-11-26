---
title: The Architecture of Trust
description: Every trust claim Rocky makes, graded against what ships today.
sidebar:
  order: 2
---

This page grades each of Rocky's trust primitives against what ships today. Where one is partial or on the roadmap, the grade says so.

## The mental model: code, typed IR, warehouse

The expensive failures on a data platform are trust failures: a change happens, nothing breaks loudly, and a wrong number surfaces days later. The [Introduction](/getting-started/introduction/) lists them. Each primitive below turns one into something you catch before you merge.

Rocky is a typed compiler between your code and your warehouse:

```
  .sql / .rocky + .toml + contracts ──▶ typed IR (every model, column, type) ──▶ warehouse
                                           │ before anything runs                  (storage, compute)
                                           ▼
                              E### errors, W### warnings, lineage, drift, cost, contracts
```

Your SQL and config compile to a typed [IR](/reference/glossary/#ir-intermediate-representation) across the dependency graph. The warehouse owns storage and compute. Rocky owns the graph, and the compiler is the trust boundary. A schema change becomes a type error, a missing contract column becomes a coded diagnostic, and a column's blast radius becomes a query you can run in CI. Rocky is not a warehouse, table format or query engine: it is the typed layer above whichever you choose.

## The trust primitives, graded honestly

### Compile-time types, diagnostics and lineage

Rocky infers column-level types across the whole DAG. Errors carry an `E` code, warnings a `W` code, lints a `P` code. Any error-level diagnostic fails the compile with a non-zero exit at PR time. See [The Rocky Compiler](/concepts/compiler/).

`rocky lineage <model>` traces a model's inputs and outputs. `rocky lineage <model>.<column>` traces one column through every transformation. `rocky lineage-diff main` gives a blast-radius report for PR review. The edges come from the compiler's semantic analysis, so lineage is computed at compile time. The graph is intra-project.

### Branches

`rocky branch create` and `rocky run --branch <name>` give isolated branches. A branch is a schema prefix: models for branch `feature_x` materialize under `branch__feature_x`, so a branch never touches a production table. Delta `SHALLOW CLONE` and Snowflake zero-copy `CLONE` are a follow-up, not what runs now.

`rocky branch approve` writes an artifact under `.rocky/approvals/<branch>/`. `--out` writes elsewhere, but the gate reads only the default directory.

The approval gate is off by default: set `[branch.approval] required = true`. With it off, promote does not read the directory. With it on, the check runs when the promote plan is built (`rocky plan promote`, or bare `rocky branch promote <name>`). Applying an existing promote plan does not repeat it. It counts valid artifacts against `min_approvers`. An artifact is valid when:

- its blake3 digest matches its contents
- its recorded branch state hash matches the branch now
- it is not dated in the future, and not older than `max_age_seconds`
- the approver's email is on `allowed_signers`, when that list is not empty

`min_approvers` counts artifact FILES, not people: one approver can satisfy two with two artifacts. It defaults to 1. Setting it to 0 passes with no approvals. `rocky branch promote --skip-approval` and `ROCKY_BRANCH_APPROVAL_SKIP` bypass the gate. Both record the reason as an audit event.

The digest is not a signature and not a tamper boundary. It is unkeyed: anything that can write the file can change the artifact and recompute the digest. It authenticates nobody, because the approver's email is a self-asserted git identity.

`.rocky/plans/` is a trusted input for the same reason. A plan's id is the blake3 digest of its kind and payload. Rocky recomputes it on every read and refuses a mismatch, but the id is unkeyed, so it catches an edit, not a plan written whole. The `AiAuthored` review marker is unsigned too. Anyone who can write `.rocky/plans/` can author a plan that `rocky apply` runs with the applier's warehouse credentials. A promote plan runs the SQL it records, and apply checks the table names recorded beside the SQL, not names parsed from it. The policy gate still runs at apply, against the applying principal, and judges the plan by those recorded names.

Rocky writes a `.gitignore` in `.rocky/` unless one exists, and ignores a failed write. Check what your repository excludes, and protect write access to the directory as you would the project.

### Per-model cost

Rocky records per-model cost on every run. Accuracy depends on the warehouse:

- **BigQuery.** Bytes scanned maps to billing, so the figure is billing-exact.
- **Databricks and Snowflake.** A duration × DBU-rate estimate. A DBU is Databricks' compute billing unit. Databricks also reports scanned bytes, for observability only. Snowflake's bytes plumbing is a follow-up.
- **DuckDB.** Zero.

### Compile-time contracts

A `.contract.toml` declares what a model must produce. `E010` to `E014`, `E062` and `E063` fail the compile, so a broken contract is a red CI check. The two opt-in rules, `[rules] no_new_nullable` (`E014`) and `[rules] no_extra_columns` (`E063`), are off by default. Format and codes: [Checks and Tests](/concepts/data-quality-checks/#data-contracts).

Across a project boundary, enforcement uses a **vendored snapshot**: a producer runs `rocky publish-ir`, a consumer declares an `[imports.<name>]` block, and its `rocky compile` fails on a breaking change with `E030` to `E034` (`W030` and `W031` for an added column and a widened type). See [Cross-Team Contracts](/concepts/cross-team-contracts/).

### Declarative governance

A `GovernanceAdapter` models governance as code: tags, grants, workspace bindings, column tags, masking policies bound to classification tags, and role-graph reconciliation. `revoke_grants` exists on the trait but `rocky run` never calls it: Rocky grants and never revokes. Databricks implements the full surface through Unity Catalog. Snowflake, BigQuery and DuckDB implement less (see the grade table). The skeleton is warehouse-neutral, but the depth is a Databricks capability today. See [Who Enforces What](/concepts/capability-matrix/).

### Schema drift handling

Rocky checks drift as it copies each table whose target exists, and applies a graded response in the same run. The mechanism, the `auto_apply_additive_drift` gate, and the limits (no grace period before a drop, no detection of a column removed from the source, a `drift` output that can omit a drop whose rebuild failed) are in [Schema Drift](/concepts/schema-drift/) and [Failure modes](/advanced/failure-modes/).

### Content-addressed writes and replay

Replay means two things.

**1. Deterministic recording with ledger verification.** Rocky records each run's per-model SQL hashes, row counts, bytes and timings when the execution path supplies them. On the content-addressed path each file is named by the hash of its own bytes. Ordinary warehouse runs give no byte-level replay proof. `rocky replay <run_id>` verifies the record against the ledger. Every materialization also stamps a **recipe-identity triple**: `recipe_hash` (the model's canonical typed IR), `input_hash` (the inputs it read) and `env_hash` (engine, adapter and dialect). `rocky history --recipe <hash>` shows what produced a row set and every other run of the same program. An `input_hash` proven by an observed freshness signature is tagged `heuristic`. A content-addressed input is tagged `strong`. The triple is an identity and audit primitive, not a reproducibility claim.

**2. Re-execution from the pinned record.** `rocky replay --execute --verify` rebuilds each model's recipe from provenance, never from the working tree, re-runs it, and compares the re-derived BLAKE3 hash with the recorded one. It runs on local DuckDB by default, or on the live warehouse with `--warehouse`, which writes into an isolated `hcv2_replay_<run>` schema and encodes the artifact with the target table's own column mapping, so `bit_exact` means the warehouse reproduced the recorded bytes. A model that reads a mutable source is `non_replayable`. A non-deterministic recipe is flagged, so a `diverged` verdict there is expected.

Content-addressed materialization ships for single-writer Delta and UniForm. Multi-writer concurrency, broad schema evolution and deletion vectors are not covered. See [Content-Addressed Materialization](/concepts/content-addressed/).

## The honesty grade

Every load-bearing claim, in one table. The partial rows are where teams get surprised.

| Claim | Grade | What that means |
|---|---|---|
| Compile-time types and diagnostics (`E###`) | Shipped | Compilation fails on any error-level diagnostic. |
| Compile-time column-level lineage + `lineage-diff` | Shipped | Intra-project. |
| Compile-time contracts (`E010`–`E014`) | Shipped | Intra-project, against the inferred schema. |
| Schema drift handling | Shipped | Graded response in the run that detects it. Limits: [Schema Drift](/concepts/schema-drift/). |
| Dialect-divergence lint (`P001`) | Shipped | Opt-in via `--target-dialect` or `[portability] target_dialect`; error severity. |
| VS Code trust overlays | Shipped | Five: Cost, Freshness, Breaking, Last run, Governance. No drift overlay. |
| Branches | Partial | Schema-prefix isolation with promotion. The approval gate is opt-in (`[branch.approval] required = true`) and checks an unkeyed digest: an integrity checksum, not a tamper boundary, and it authenticates nobody. No warehouse-native zero-copy clones yet. |
| Replay | Partial | Recording plus ledger verification, and re-execution for deterministic content-addressed models. Mutable-source models are `non_replayable`. |
| Content-addressed writes | Partial | Single-writer Delta/UniForm only. |
| Per-model cost | Partial | Billing-exact on BigQuery; a duration × DBU-rate estimate on Databricks and Snowflake; zero on DuckDB. |
| Declarative governance | Partial | Full on Databricks (Unity Catalog); tags + `GRANT` on Snowflake; label tagging only on BigQuery (grants no-op); no-op on DuckDB. Snowflake reconciles `ALTER … SET TAG`, `GRANT` role grants and retention policy, not workspace binding or masking. |
| Cross-team / cross-project contract enforcement | Shipped | Producer `rocky publish-ir` → consumer `[imports.<name>]` vendored snapshot → `E030`–`E034` enforced at the consumer's `rocky compile`. |
