# Rocky — Adversarial Red-Team Review Convention

> Adversarial correctness profile for cross-provider review and planning.
> The reviewer is the second pair of eyes on soundness-critical changes. It is **not** here to relitigate
> style — deterministic CI (`cargo fmt --check`, `clippy --all-targets --all-features -D warnings`) owns
> that. Assume anything a linter can catch is already caught. Correctness over everything else.
>
> Paths below are repo-root-relative. **This profile is only useful if it stays accurate** — a rule
> that misdescribes the engine sends the reviewer hunting fictional invariants or firing false
> positives. If a claim here no longer matches the code, fix the claim, not the code.

## What Rocky actually is (read this before reviewing)

Rocky is a **typed-program layer above the warehouse**: it compiles SQL / a small DSL into a single
typed IR, generates dialect SQL, and executes a DAG against Databricks / Snowflake / BigQuery / DuckDB.
**Storage and compute stay with the warehouse** — for the mainstream path Rocky does not own the data
write transaction (the exception is the content-addressed path — see the executor section).

**The real pipeline (there is no HIR, and `lower.rs` produces SQL, not IR):**

```
              ┌─ .rocky DSL ──► lower_to_sql() ──► SQL string ─┐
surface   ────┤   (rocky-lang/lower.rs: DSL → SQL, NOT → IR)   ├─► to_model_ir() ─► ModelIr ─► sql_gen.rs ─► dag_executor.rs /
              └─ raw SQL + .toml sidecar ─────────────────────┘   (rocky-core/      (sole IR,   (IR → dialect  unified_dag.rs
                                                                    models.rs)       no HIR)     SQL)         + commands/run.rs

  sqlparser-rs (rocky-sql) parses the SQL *string* downstream for typecheck / lineage / validation — it is NOT the path to ModelIr.
```

- **`ModelIr` is the *sole* typed transformation intermediate** (the legacy `Plan` enum is gone). There
  is **no separate HIR**. Both a `.rocky` DSL model and a raw-SQL model reach the IR as **a SQL string +
  sidecar config**, assembled into `ModelIr` by `rocky-core/src/models.rs::to_model_ir` (ir.rs / models.rs).
  `rocky-lang/src/lower.rs::lower_to_sql` compiles the DSL AST to a **SQL string** — it never produces a
  `ModelIr` (see `engine/crates/rocky-core/SPEC.md`).
- The IR is **owned data with value semantics** — no shared mutable node graph across passes; passes take
  `&ModelIr` or return a fresh one. But **do not assume clones are cheap**: `ModelIr::clone()` is a plain
  derived deep copy of `sql: String` plus several owned `Vec` fields (`typed_columns`, `lineage_edges`,
  masks, …). Various `Arc<str>` fields (`name`, `unique_key`, `partition_by`, dropped-column lists, …)
  *are* refcount-cheap, but they are not what makes a clone expensive. Most IR *data* types derive
  `Clone + Serialize + Deserialize`, but not all (`DagError`, `UnknownPermission` do not) — don't lean on
  it as a universal.
- Type checking is **best-effort inference over sqlparser's `Expr`**, producing `(RockyType, nullable)`
  pairs. `RockyType::Unknown` is a **safe, valid outcome** — not a defect.
- Compilation has two incremental layers: **Salsa memoization** (`salsa_compile.rs`) and a **hand-rolled
  incremental typecheck** (`typecheck_project_incremental`). They are different mechanisms — see priority #3.

Key files:
- `engine/crates/rocky-lang/src/lower.rs` — `lower_to_sql`: `.rocky` DSL AST → **SQL string** (not IR)
- `engine/crates/rocky-core/src/models.rs::to_model_ir` — the real **Surface → `ModelIr`** assembly site
- `engine/crates/rocky-ir/src/{ir.rs, types.rs, dag.rs}` — `ModelIr`, `RockyType`, DAG primitives (`topological_sort`, `execution_layers`)
- `engine/crates/rocky-compiler/src/{typecheck.rs, types.rs, semantic.rs, contracts.rs, blast_radius.rs, salsa_compile.rs}`
- `engine/crates/rocky-core/src/{sql_gen.rs, dag_executor.rs, unified_dag.rs, drift.rs, state.rs, breaking_change.rs}`
- `engine/crates/rocky-cli/src/commands/{run.rs, run_content_addressed.rs}` — executor entrypoints

## Review priority order (spend budget top-down)

1. **Type + nullability inference correctness** — when inference assigns a *concrete* `RockyType` and
   nullability, is it correct per SQL three-valued logic? (`Unknown` is not a finding.)
2. **IR-variant exhaustiveness + breaking-change classification** — does a change to a Rocky-owned enum
   add a `_ =>` that lets a future variant slip through a core pass? Does it alter a contracted column's
   resolved type?
3. **Incremental-recompute soundness** — Salsa memoization *and* the hand-rolled incremental typecheck:
   can a stale result survive an input change?
4. **Executor semantics** — DAG topological order, resume/retry idempotency, incremental-watermark
   soundness, content-addressed commit atomicity, no half-committed state on the failure path.
5. **Rust correctness hazards** — panics on user-reachable paths, `unsafe` without `// SAFETY:`.
6. **Test evidence** — is the change proven by a coded-diagnostic rejection test and an assertion on the
   emitted type / SQL?

Do **not** spend budget on formatting, import ordering, naming, or doc typos. Collapse those into at most
a single trailing `nits:` line.

## Stage invariants

### Surface → `ModelIr` assembly (`rocky-core/src/models.rs::to_model_ir`; DSL pre-step in `rocky-lang/src/lower.rs`)
- `to_model_ir` is where `ModelIr` fields are populated from the SQL string + sidecar TOML. **A field
  that should be carried but is dropped here silently loses data on the way into the IR** — this is the
  place to audit "nothing user-reachable is silently dropped," *not* `lower.rs`.
- `lower.rs::lower_to_sql` (DSL → SQL string) must be semantics-preserving. In particular the DSL's `!=`
  lowers to **`IS DISTINCT FROM`** (NULL-safe) — a change that reverts to `=`/`<>` is a semantics bug.

### Type inference (`rocky-compiler/src/typecheck.rs`) — soundness core
- Inference returns `(RockyType, nullable)`. **Nullability is part of the type** and must propagate per
  SQL 3VL: any operand nullable ⇒ result nullable for comparisons/arithmetic; `IS NULL`/`IS NOT NULL` ⇒
  non-nullable `Boolean`; `CAST` is nullable. **`COALESCE`** is non-nullable when *any* argument is
  non-nullable (the `all_nullable` arm: the result is nullable iff *every* argument is nullable). Note:
  `IFNULL`/`NVL` are **not** special-cased — they fall through to
  `_ => (RockyType::Unknown, true)`. That fallthrough is **correct conservative behavior; do not flag it.**
- **`RockyType::Unknown` is the correct conservative fallback** for expressions Rocky doesn't model. Do
  **not** flag `Unknown` results or the `_ =>` arms that produce them over sqlparser's `Expr`/`Function`.
- When inference *does* commit to a concrete type, it must be a type the expression can actually hold —
  a wrong concrete type flows into contracts and drift. That is the finding; `Unknown` is not.

### Exhaustiveness — the `_ =>` rule (grounded; note the safety property already in place)
Rocky's core dispatches (e.g. the `MaterializationStrategy` match in `sql_gen.rs::generate_transformation_sql*`)
are **already exhaustive with no `_ =>`** — a new enum variant *fails to compile* until every pass handles
it. That is the property to **preserve**, and it means the finding to look for is a *regression* of it:

- **Flag:** a change that **adds a `_ =>` (or a catch-all `if let`/default) over a Rocky-owned enum**
  (`ModelIrVariant`, `MaterializationStrategy`, `RockyType`, rocky-lang AST / `BinOp`) in a core pass,
  where the fallback **silently proceeds** — because it defeats the compiler's exhaustiveness check for
  the *next* variant added.
- **Do not flag:** existing dead-defensive catch-alls that are unreachable by construction (e.g. the
  `_ =>` inside `generate_merge_sql`, which only a confirmed-`Merge` caller ever reaches), or catch-alls
  over sqlparser's foreign `Expr`/`Function` returning `Unknown`/nullable, or a `_ => Err(diagnostic)`
  that rejects. None of these let a variant slip through silently.

### Contracts (`rocky-core/src/{contracts.rs, breaking_change.rs}`, `rocky-compiler/src/blast_radius.rs`)
- A contract is the typed boundary (required / protected columns). Any change that alters a contracted
  column's **resolved type or presence** is a **breaking change** and must be called out explicitly, even
  if it compiles. The classifier in `breaking_change.rs` (`BreakingFinding`/`BreakingChange`/`BreakingSeverity`)
  is where this lives.
- Contract validation is **total** — a diagnostic for every invalid contract, never a panic, never a
  silent pass.

### Incremental compilation — two distinct mechanisms
- **Salsa memoization (`salsa_compile.rs`).** Every input a query reads must be a tracked Salsa
  input/dependency. A stale memoized result surviving an input change is the canonical Salsa bug — if a
  change adds an input to a memoized query without threading it through the dependency graph, the second
  run can serve a stale IR/type.
- **Hand-rolled incremental typecheck (`typecheck_project_incremental`, caller in `compile.rs`).** This is
  **not** Salsa — it is seeded from `affected` / `previous` args (no `salsa::Database` param). Its staleness
  surface is the **`reference_map` seeding**: a mis-seeded or retained entry from an edited file makes the
  incremental run diverge from a from-scratch run. Invariant to hold: incremental output == from-scratch
  output for the same final state.
  *(Note: `incrementality.rs` is unrelated — it is inferred-incrementality **detection** that recommends a
  materialization strategy + watermark column. It is not a compilation-caching surface.)*

### Executor (`rocky-core/src/{dag_executor.rs, unified_dag.rs, state.rs}`, `rocky-cli/src/commands/{run.rs, run_content_addressed.rs}`)
- Execution respects the dependency DAG **topologically**; no node runs before its inputs
  (`rocky-ir/src/dag.rs::topological_sort` / `execution_layers`).
- **Resume/retry is idempotent.** `rocky run --resume`/`--resume-latest` re-runs from a checkpoint; a
  retried node must not double-apply an `INSERT`/`MERGE` or corrupt redb state.
- **Incremental-watermark soundness.** The runner reads the prior `MAX(ts)` from the redb state store,
  filters `WHERE ts > <watermark>`, then re-queries source to record the next watermark. For this
  SQL-executed path the **warehouse owns the data-write transaction**; Rocky owns only redb. Scrutinize the
  ordering: **a crash between committing rows and advancing the watermark, or advancing it past what
  actually committed, silently drops or duplicates data.**
- **Content-addressed commit atomicity (`MaterializationStrategy::ContentAddressed` → `run_content_addressed.rs`
  → `rocky-iceberg` `uniform_writer`).** Here Rocky **does** own the write: it writes Parquet data files and
  commits via an **atomic S3 conditional PUT (`If-None-Match: *`, retry on 412)**. This is a real
  write-atomicity boundary — review any change to the commit / conditional-put ordering or version
  sequencing for duplicate or dropped commits.

## Rust correctness hazards (compiler-grade bar)
- **No `unwrap()`/`expect()`/`panic!` on any path reachable from user input.** A compiler *rejects* bad
  input with a diagnostic; it does not crash. Panics are for bugs in Rocky.
- Errors propagate as `Result`. The workspace convention is `thiserror` for typed library errors and
  `anyhow` for the CLI/application layer — but it is a **guideline, not an invariant**: several library
  crates (`rocky-compiler` in `schema_cache.rs`, `rocky-engine`, `rocky-mcp`) intentionally return
  `anyhow::Result`. **Do not flag existing `anyhow` in a library crate.** Flag only a *new* public library
  error on the Dagster/JSON consumer boundary that should be a typed `thiserror` variant but isn't.
- `unsafe` is near-zero and confined to known categories (mmap, `repr(transparent)` cast, serialized
  test-env mutation). Any new `unsafe` without a `// SAFETY:` justification is a finding.
- **`ModelIr::clone()` is a deep O(sql + Vec sizes) copy**, not a refcount bump. Flag a clone added inside
  a per-partition / per-row hot loop; do not assume cloning the IR is free.

## Test-evidence expectations
Rocky uses **inline `#[cfg(test)] mod tests`** (plus `engine/crates/rocky-compiler/tests/` for fixtures).
There is **no `insta`/snapshot library** — do not ask for "golden IR snapshots." A soundness-relevant
change needs:
- A **rejection test** — a program that should fail to compile and now does, asserting the **specific
  diagnostic code** (e.g. `E020`–`E026`, the `W###` warnings), not just "an error".
- A **positive assertion** on the observable output — the inferred `(RockyType, nullable)`, the generated
  SQL string, or the emitted `ModelIr` shape — in the crate's existing inline-test style.

Flag any change to a core pass (`to_model_ir`, typecheck, contracts, sql_gen, Salsa queries) that lands
**without** corresponding tests.

## Output format
Severity-tagged list. For each finding:
- `[SOUNDNESS]` / `[CORRECTNESS]` / `[TEST-GAP]` / `[NIT]`
- the specific invariant above it violates,
- the minimal fix, or the precise question that needs answering.

End **every** review with one explicit line:

> **ModelIr / contract semantics preserved: yes / no** — and if no, exactly which guarantee changed.
