---
title: The Architecture of Trust
description: Every trust claim Rocky makes, graded against what ships today.
sidebar:
  order: 2
---

This page grades each of Rocky's trust primitives against what ships today. Where
a primitive is partial or still on the roadmap, the grade says so.

## The mental model: code, typed IR, warehouse

The expensive failures on a data platform are trust failures: a change happens, nothing breaks loudly, and a wrong number surfaces days later. The [Introduction](/getting-started/introduction/) lists them. Each primitive below turns one into something you catch before you merge.

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

Your SQL and config compile to a typed [IR](/reference/glossary/#ir-intermediate-representation) that knows every model, column and type across the dependency graph. The warehouse owns storage and compute. Rocky owns the graph, and the compiler is the trust boundary.

Because the graph is typed before execution, a schema change becomes a type error, a missing contract column becomes a coded diagnostic, and a column's blast radius becomes a query you can run in CI. A string template cannot do this: it never has to know what a column is.

Rocky is not a warehouse, a table format or a query engine. It is the typed layer above whichever you choose. The [grade table](#the-honesty-grade) at the end says how far each primitive ships today.

## The trust primitives, graded honestly

### Compile-time types and diagnostics

Rocky infers column-level types across the whole DAG. Errors carry an `E` code, warnings a `W` code, lints a `P` code. Compilation fails on any error-level diagnostic, so the failure is a non-zero exit at PR time. See [The Rocky Compiler](/concepts/compiler/).

### Compile-time column-level lineage

`rocky lineage <model>` traces a model's inputs and outputs. `rocky lineage <model>.<column>` traces one column through every transformation. The edges come from the compiler's semantic analysis, so Rocky computes lineage at compile time.

`rocky lineage-diff main` gives a blast-radius report for PR review. The graph is intra-project: it does not cross project boundaries.

### Branches

`rocky branch create` and `rocky run --branch <name>` give isolated branches. A branch is a schema prefix: models for branch `feature_x` materialize under `branch__feature_x`, so a branch never touches a production table. Delta `SHALLOW CLONE` and Snowflake zero-copy `CLONE` are a follow-up, not what runs now.

`rocky branch approve` writes an artifact under `.rocky/approvals/<branch>/`. `--out` writes elsewhere, but the gate reads only the default directory.

The approval gate is off by default. Set `[branch.approval] required = true` to turn it on. With it off, promote does not read the directory.

```
  gate on ─► runs when the promote plan is built
             (`rocky plan promote`, or bare `rocky branch promote <name>`)
          ─► applying an existing promote plan does not repeat the check
          ─► counts valid artifacts against `min_approvers`
```

An artifact is valid when:

- its blake3 digest matches its contents
- its recorded branch state hash matches the branch now
- it is not dated in the future, and not older than `max_age_seconds`
- the approver's email is on `allowed_signers`, when that list is not empty

`min_approvers` counts artifact FILES, not people. One approver can satisfy a threshold of two with two artifacts. It defaults to 1. Setting it to 0 passes with no approvals.

`rocky branch promote --skip-approval` and the `ROCKY_BRANCH_APPROVAL_SKIP` environment variable bypass the gate. Both record the reason as an audit event.

The digest is not a signature and not a tamper boundary. It is unkeyed: anything that can write the file can change the artifact and recompute the digest. It authenticates nobody, because the approver's email is a self-asserted git identity.

`.rocky/plans/` is a trusted input for the same reason. A plan's id is the blake3 digest of its kind and payload. Rocky recomputes it on every read and refuses a mismatch. The id is unkeyed, so it catches an edit, not a plan written whole. The `AiAuthored` review marker is unsigned too. Anyone who can write `.rocky/plans/` can author a plan that `rocky apply` runs with the applier's warehouse credentials. A promote plan runs the SQL it records. Apply checks the table names recorded beside the SQL, not names parsed from it. The policy gate still runs at apply, against the applying principal, and judges the plan by those recorded names.

Rocky writes a `.gitignore` in `.rocky/` unless one exists, and ignores a failed write. Check what your repository excludes, and protect write access to the directory as you would the project.

### Per-model cost

Rocky records per-model cost on every run. Accuracy depends on the warehouse:

- **BigQuery.** Bytes scanned maps to billing, so the figure is billing-exact.
- **Databricks and Snowflake.** A duration × DBU-rate estimate. A DBU is Databricks' compute billing unit. Databricks also reports scanned bytes, for observability only. Snowflake's bytes plumbing is a follow-up.
- **DuckDB.** Zero.

### Compile-time contracts

A `.contract.toml` declares what a model must produce. The compiler checks the inferred schema against it. `E010` to `E014`, `E062` and `E063` fail the compile, so a broken contract is a red CI check. [Checks and Tests](/concepts/data-quality-checks/#data-contracts) has the format and every code. The two opt-in rules, `[rules] no_new_nullable` (`E014`) and `[rules] no_extra_columns` (`E063`), are off by default.

Those codes are intra-project. Across a project boundary, enforcement uses a **vendored snapshot**: the consumer keeps a committed copy of the producer's compiled schema and diffs against it. A producer runs `rocky publish-ir`. A consumer declares an `[imports.<name>]` block, and its `rocky compile` fails on a breaking change with `E030` to `E034`. `W030` and `W031` cover an added column and a widened type. See [Cross-Team Contracts](/concepts/cross-team-contracts/).

### Declarative governance

Rocky models governance as code through a `GovernanceAdapter`: tags, grants, workspace bindings, column tags, masking policies bound to classification tags, and role-graph reconciliation. The trait has `revoke_grants`, but `rocky run` does not call it. Rocky grants and never revokes.

- **Databricks** implements the full surface through Unity Catalog.
- **Snowflake** reconciles object tags (`ALTER … SET TAG`), `GRANT` role grants and retention policy. Workspace binding and masking are not driven.
- **BigQuery** reconciles tags as labels (`ALTER … SET OPTIONS(labels=…)`). Grants map to IAM, so `apply_grants` and `revoke_grants` log and do nothing today.
- **DuckDB** does nothing: it has no governance model.

The skeleton is warehouse-neutral. The depth is a Databricks capability today. See [Who Enforces What](/concepts/capability-matrix/).

### Schema drift handling

Rocky checks for drift as it copies each table whose target exists. By default it applies the graded response in the same run: add a column, widen a safe type, or drop and rebuild for any other change. With `auto_apply_additive_drift = true` under `[resilience]`, only a nullable new column proceeds, and only when `[policy]` resolves `schema_change.additive` to `allow`. With no `[policy]` block, nothing is granted. Every other change, a safe widening included, is refused before it touches the target. See [Schema Drift](/concepts/schema-drift/).

Limits:

- The drop happens in the run that finds the change. No grace period runs first.
- Rocky does not detect a column that disappeared from the source.
- The run's `drift` output lists only the actions of an attempt that finished. If the rebuild fails after the drop, that entry is lost. A retry (one by default) can rebuild the table and report it as materialized, with no sign of the drop. See [Failure modes](/advanced/failure-modes/).

### Content-addressed writes and replay

Replay means two things.

**1. Deterministic recording with ledger verification.** Rocky records each run's per-model SQL hashes, row counts, bytes and timings when the execution path supplies them. On the content-addressed path each file is named by the hash of its own bytes, so the same inputs and code give the same files. Ordinary warehouse runs give no byte-level replay proof. `rocky replay <run_id>` inspects the record and verifies it against the ledger.

Every materialization also stamps a **recipe-identity triple**:

- `recipe_hash` fingerprints the model's canonical typed IR.
- `input_hash` covers the inputs it read.
- `env_hash` covers the engine, adapter and dialect.

`rocky history --recipe <hash>` shows what produced a row set, and every other run of the same program. An `input_hash` proven by an observed freshness signature is tagged `heuristic`. A content-addressed input is tagged `strong`. The triple is an identity and audit primitive, not a reproducibility claim.

**2. Re-execution from the pinned record.** `rocky replay --execute --verify` rebuilds each model's recipe from provenance, never from the working tree. It re-runs the recipe and compares the re-derived BLAKE3 hash with the recorded one.

It runs on local DuckDB by default, or on the live warehouse with `--warehouse`. The warehouse path writes into an isolated `hcv2_replay_<run>` schema, never the recorded target. It encodes the artifact with the target table's own column mapping, so `bit_exact` means the warehouse reproduced the recorded bytes.

Scope: deterministic, content-addressed models. A model that reads a mutable source is `non_replayable`. A non-deterministic recipe is flagged, so a `diverged` verdict there is expected.

Content-addressed materialization ships for single-writer Delta and UniForm (a Delta feature that also publishes Iceberg metadata). It writes blake3-hashed Parquet plus a Delta log commit. It does not yet cover multi-writer concurrency, broad schema evolution, or deletion vectors (a Delta side file that records deleted rows). See [Content-Addressed Materialization](/concepts/content-addressed/).

### VS Code trust overlays

The VS Code extension draws the lineage graph and paints five trust signals from Rocky output: **Cost** (compile-time estimate), **Freshness** (declared policy), **Breaking** (semantic CI diff), **Last run**, and **Governance** (classified columns and unmasked exceptions). There is no drift overlay: Rocky has no `rocky drift` command, and detects drift inside `rocky run` and `rocky plan`.

## The honesty grade

Every load-bearing claim, in one table. The partial rows are where teams get
surprised.

| Claim | Grade | What that means |
|---|---|---|
| Compile-time column-level types and diagnostics (`E###` errors) | Shipped | Compilation fails on any error-level diagnostic. |
| Compile-time column-level lineage + `lineage-diff` blast radius | Shipped | Intra-project; computed at compile time. |
| Compile-time contracts (`E010`–`E014`) | Shipped | Intra-project contract validation against inferred schema. |
| Schema drift handling (add column / safe widen / drop-and-recreate) | Shipped | Graded response, applied in the run that detects it by default; with `auto_apply_additive_drift`, only a nullable addition that policy resolves to `allow`. No grace period before a drop. A column removed from the source is not detected. The `drift` output can omit a drop whose first rebuild failed. |
| Dialect-divergence lint (`P001`) | Shipped | Opt-in via `--target-dialect` or `[portability] target_dialect`; error severity. |
| VS Code trust overlays | Shipped | Five: Cost, Freshness, Breaking, Last run, Governance. No drift overlay. |
| Branches | Partial | Schema-prefix isolation with promotion. The approval gate is opt-in (`[branch.approval] required = true`) and checks an unkeyed digest: an integrity checksum, not a tamper boundary, and it authenticates nobody. No warehouse-native zero-copy clones yet. |
| Replay | Partial | Deterministic recording + ledger verification, plus re-execution (`rocky replay --execute --verify`, local or `--warehouse`) for deterministic content-addressed models; mutable-source models are `non_replayable`, non-deterministic recipes flagged. |
| Content-addressed writes | Partial | Single-writer Delta/UniForm; no multi-writer, broad schema evolution, or deletion vectors yet. |
| Per-model cost | Partial | Billing-exact on BigQuery; a duration × DBU-rate estimate on Databricks and Snowflake; zero on DuckDB. Databricks surfaces scanned bytes for observability; Snowflake's warehouse-reported-bytes plumbing is the follow-up. |
| Declarative governance | Partial | Full on Databricks (Unity Catalog); tags + `GRANT` on Snowflake; label-based tagging only on BigQuery (grants no-op, IAM follow-up); no-op on DuckDB. |
| Cross-team / cross-project contract enforcement | Shipped | Producer `rocky publish-ir` → consumer `[imports.<name>]` vendored snapshot → `E030`–`E034` enforced at the consumer's `rocky compile`. |

## Where Rocky sits

- **Databricks LakeFlow** is coupled to the warehouse and free with the platform. Rocky differs on portability across warehouses and on a real compiler with tooling. If neither matters to you, the native option may be enough.
- **Polaris, Iceberg and Delta** are a catalog and open table formats. Rocky is none of those. It targets them: it writes content-addressed Delta and UniForm that Iceberg-compatible readers consume.
