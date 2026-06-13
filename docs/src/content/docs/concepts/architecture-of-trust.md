---
title: The Architecture of Trust
description: How Rocky's typed graph turns the failure modes serious data teams fear into compile errors, CI gates, and audit artifacts, graded honestly against what ships today.
sidebar:
  order: 2
---

This page is for the engineer who has already lost a weekend to a silent schema change, and who reads vendor docs with a healthy suspicion. It walks through how Rocky earns trust, primitive by primitive, and grades every load-bearing claim against what actually ships. Where something is partial, this page says so. Where something is roadmap, this page says that too.

## The failures worth designing against

The expensive failures on a mature data platform are rarely slow queries. They are trust failures, and they share a shape: a change happens somewhere, nothing breaks loudly, and the damage surfaces days later in a number someone trusted.

Four are worth naming because they recur on every team that reaches real scale:

- **Silent schema drift.** A source column changes type or disappears upstream. The pipeline keeps running. A downstream join quietly starts producing wrong numbers, and a dashboard diverges for three days before anyone notices.
- **Unattributable cost.** Warehouse spend jumps in a month. Nobody can point at the model that caused it, because cost was never a property of a model, only a line item on an invoice.
- **The un-auditable change.** An auditor asks who altered `fct_revenue.amount`, when, and on whose approval. The honest answer involves `git blame`, a Slack thread, and a screenshot.
- **The contract broken without warning.** A model promises a column to its consumers. Someone removes it, or changes its type, or relaxes its nullability. The consumers find out in production.

These are not edge cases. They are the load-bearing risks that make a platform team cautious, and they are the failures Rocky is built to convert from production incidents into things you catch before merge.

## The mental model: code, typed IR, warehouse

Rocky sits between your code and your warehouse as a typed compiler. Your SQL and config compile down to a typed intermediate representation that knows every model, every column, and every type across the full dependency graph. The warehouse still owns storage and compute. Rocky owns the graph, and the compiler is the trust boundary.

That single decision is what makes the rest possible. Once the graph is typed and known before execution, a schema change is a type error, a missing contract column is a diagnostic with a code, and a column's downstream blast radius is a query you can run in CI. None of that is reachable for a string-templating engine, because a string template never has to know what a column is.

Rocky is not a warehouse, not a table format, and not a query engine. It is the typed layer above whichever of those you have chosen, and it stays warehouse-neutral on purpose.

## The trust primitives, graded honestly

Each primitive below is tied to its real CLI surface or diagnostic code. The inline grade tells you how far it ships today, so you can plan around the edges instead of discovering them.

### Compile-time types and diagnostics

Rocky infers column-level types across the whole DAG and surfaces problems as diagnostic codes you can grep in CI logs. The codes run from `E001` through `E027` for errors, with `W` warnings and `P` lints alongside. Compilation fails on any error-level diagnostic, which is the entire point: the failure mode becomes a non-zero exit code at PR time rather than a wrong number in production.

**Shipped.** This is the foundation the other primitives build on.

### Compile-time column-level lineage

`rocky lineage <model>` traces a model's inputs and outputs; `rocky lineage <model>.<column>` traces a single column through every transformation that touches it. The edges come from the compiler's semantic analysis, so lineage is computed at compile time rather than reconstructed after the fact. `rocky lineage-diff main` turns this into a blast-radius report for PR review: change a column, see exactly which downstream columns are affected before you merge.

The lineage graph is intra-project. It knows the columns inside one Rocky project, not across project boundaries.

**Shipped, within a project.**

### Branches

`rocky branch create` and `rocky run --branch <name>` give you isolated branches for development and review. Today a branch is implemented as a schema prefix: models for branch `feature_x` materialize under a `branch__feature_x` namespace, so a branch never touches the production tables. Approval writes a signed artifact under `.rocky/approvals/<branch>/`, and promotion verifies that signature before merging the branch forward.

The branch isolation you get today is schema-prefix isolation, not a warehouse-native zero-copy clone. Delta `SHALLOW CLONE` and Snowflake zero-copy `CLONE` would make branch creation near-instant and storage-free; that integration is a follow-up, not what runs now.

**Partial.** Schema-prefix branches with signed approval and promotion ship today. Warehouse-native clones do not.

### Per-model cost

Rocky records per-model cost on every run, which makes cost a property of a model rather than a line on an invoice. On BigQuery, bytes-scanned maps directly to billing, so the figure is billing-exact. On Databricks and Snowflake it is a duration × DBU-rate estimate (warehouse-reported bytes plumbing is a follow-up); on DuckDB it is zero.

**Partial.** Per-model cost populates on every run; it is billing-exact on BigQuery and a duration-based estimate on Databricks/Snowflake until their bytes plumbing lands.

### Compile-time contracts

A `.contract.toml` declares what a model must produce, and the compiler checks the model's inferred schema against it. The relevant codes are concrete:

- `E010`: a required column is missing from the model output.
- `E011`: a column's type does not match the contract.
- `E012`: the contract says non-nullable and the model output is nullable.
- `E013`: a protected column has been removed.

Any of these fails compilation, so a broken contract is a red CI check, not a production surprise.

These contracts are intra-project. They validate a model against a contract inside the same Rocky project. There is no cross-project or cross-team contract-enforcement mechanism in Rocky today. A team publishing a contract that another team's separate project must honor, enforced at compile time across that boundary, is something Rocky is shaped to support but does not yet ship.

**Shipped intra-project (`E010`–`E013`). Cross-team / cross-project contract enforcement is not yet.**

### Declarative governance

Rocky models governance as code through a `GovernanceAdapter`: tag management, grant and revoke, workspace bindings, column tags, masking policies bound to classification tags, and role-graph reconciliation. How much of that surface is real depends entirely on the warehouse.

- **Databricks** implements the full surface through Unity Catalog.
- **Snowflake** and **BigQuery** support `GRANT` and `REVOKE` reconciliation, not the deeper tag and masking surface.
- **DuckDB** is a no-op, since it has no governance model to drive.

So declarative governance at depth is a Databricks capability today. The skeleton is warehouse-neutral; the depth is not yet portable.

**Partial.** Full on Databricks; `GRANT`/`REVOKE` only on Snowflake and BigQuery; no-op on DuckDB.

### Schema drift handling

When a source schema changes under a materialized model, Rocky does not silently keep going. Drift handling chooses between ignoring the change, applying safe column-type widenings, and a full drop-and-recreate, with a grace period before destructive action. The point is that drift becomes an explicit, graded decision instead of a silent divergence.

**Shipped.**

### Content-addressed writes and replay

Replay is two distinct things, and being precise about which one ships matters.

The first is deterministic recording with ledger verification. Rocky records each run's per-model SQL hashes, row counts, bytes, and timings, and content-addresses the written artifacts so that the same inputs and code produce the same physical files. `rocky replay <run_id>` inspects that record and verifies it against the ledger. That ships today.

The second is re-execution from the pinned record: replaying a past run by feeding the recorded inputs back through the engine to reproduce its outputs from scratch. That is the follow-up. It arrives on top of the content-addressed write path, and it is not what `rocky replay` does now.

In short: deterministic recording and content-addressed verification today, re-execution from the record next.

**Partial.** Recording and ledger verification ship; re-execution from the record is the follow-up.

Content-addressed materialization itself ships for single-writer Delta and UniForm: blake3-hashed Parquet files plus a Delta log commit, with Iceberg-compatible readers seeing the same snapshot. It is single-writer and does not yet cover multi-writer concurrency, broad schema evolution, or deletion vectors.

**Partial.** Single-writer content-addressed Delta/UniForm ships; multi-writer, broad schema evolution, and deletion vectors do not.

### VS Code trust overlays

The VS Code extension renders the lineage graph and overlays four trust signals onto it, each backed by a real CLI command:

1. **Drift**: schema drift against the live warehouse.
2. **Breaking**: breaking changes from the semantic CI diff.
3. **Replay**: the last recorded run for each model.
4. **Governance**: compliance and masking status.

Four overlays, not more. Each maps to a command the extension already drives.

**Shipped (four overlays).**

## The honesty grade

Every load-bearing claim, in one table. Read the partial and not-yet rows carefully; they are where teams get surprised, and naming them is the whole point of this page.

| Claim | Grade | What that means |
|---|---|---|
| Compile-time column-level types and diagnostics (`E###` errors) | Shipped | Compilation fails on any error-level diagnostic. |
| Compile-time column-level lineage + `lineage-diff` blast radius | Shipped | Intra-project; computed at compile time. |
| Compile-time contracts (`E010`–`E013`) | Shipped | Intra-project contract validation against inferred schema. |
| Schema drift handling (ignore / safe widen / drop-and-recreate) | Shipped | Explicit graded response with a grace period. |
| Dialect-divergence lint (`P001`) | Shipped | Opt-in via `--target-dialect`; error severity. |
| VS Code trust overlays | Shipped | Exactly four: Drift, Breaking, Replay, Governance. |
| Branches | Partial | Schema-prefix isolation with signed approval/promotion; no warehouse-native zero-copy clones yet. |
| Replay | Partial | Deterministic recording + ledger verification today; re-execution from the pinned record is the follow-up. |
| Content-addressed writes | Partial | Single-writer Delta/UniForm; no multi-writer, broad schema evolution, or deletion vectors yet. |
| Per-model cost | Partial | Billing-exact on BigQuery; a duration × DBU-rate estimate on Databricks and Snowflake; zero on DuckDB. Warehouse-reported-bytes plumbing on the non-BigQuery adapters is the follow-up. |
| Declarative governance | Partial | Full on Databricks (Unity Catalog); `GRANT`/`REVOKE` only on Snowflake and BigQuery; no-op on DuckDB. |
| Cross-team / cross-project contract enforcement | Not yet | Contracts are intra-project today; cross-boundary enforcement is the shape Rocky is built toward, not a current capability. |

## What to lead with

If you are deciding whether Rocky is worth your team's time, the differentiation worth weighing is concrete:

- **Branches** for isolated development and review without touching production tables.
- **Replay** as deterministic recording plus content-addressed verification, with re-execution coming.
- **Per-model cost** as a property of the model rather than an invoice line.
- **Declarative governance** as code, deepest on Databricks.
- **Dialect-divergence lint** (`P001`): the day you start a warehouse migration it is useful, and the day you finish one it is essential.
- **Compile-time contracts** that turn a broken promise into a red CI check.

Rocky being written in Rust matters for speed and for the existence of a real LSP, but it is not the reason to choose it. The reason is that the failure modes above become compile errors and CI gates.

## Where Rocky sits next to the adjacent tools

A sophisticated reader will already be holding Rocky up against a few specific things. Here is the honest framing for each.

### dbt Fusion (head-to-head)

In June 2026 dbt Labs open-sourced the Fusion runtime as dbt Core v2.0 (Rust, Apache 2.0, alpha); the recommended Fusion distribution is a genuine compiler with multi-dialect SQL validation, a real LSP, and column-level lineage in the editor, and it is the closest thing in the dbt ecosystem to what Rocky does. The differentiation is in the enforcement plane: named branches, content-addressed recording and ledger verification, per-model cost budgets that fail the build, a dialect-portability lint, and declarative governance and masking under Apache 2.0 rather than gated behind a paid platform tier. Fusion still uses Jinja templating, so its strictest, build-failing analysis is opt-in; Rocky keeps SQL first-class with no Jinja, and offers an optional typed DSL only where SQL does not fit.

Always read "dbt" with the qualifier. dbt Core 1.x is a templating engine and cannot catch the failures above at compile time by design. dbt Core v2.0 is a faster Rust binary but still renders Jinja; the SQL comprehension that catches some of these (type-checking and column-level lineage) lives in the Fusion extension and requires opting into its `strict` mode (the default `baseline` mode is lighter and warn-only). Fusion is the actual head-to-head. They are structurally different tools.

### Databricks LakeFlow (head-to-head, with a caveat)

LakeFlow is warehouse-coupled and comes free with the platform. If portability across warehouses and a real compiler with serious tooling matter to you, that is where Rocky differentiates. If they do not, the warehouse-native option may simply be good enough for your team, and that is a legitimate answer.

### Polaris and the open table formats (category clarification)

This one is a category question, not a head-to-head. Polaris is Snowflake's Iceberg REST catalog; Iceberg and Delta are open table formats. Rocky is none of those. Rocky targets them. It writes content-addressed Delta and UniForm that Iceberg-compatible readers can consume, and it treats the format and catalog as the substrate it sits above. Confusing Rocky with a table format or a catalog gets the layering wrong, which is exactly what the next sentence is meant to fix.

---

Rocky is the typed graph between your code and whichever warehouse, table format, or query engine you've chosen.
