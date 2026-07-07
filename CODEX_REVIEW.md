# Rocky — Codex Red-Team Review Convention

> Adversarial correctness profile for cross-provider review + planning via `codex-plugin-cc`.
> Codex is the second pair of eyes on soundness-critical changes. It is **not** here to relitigate
> style — deterministic CI (`cargo fmt --check`, `clippy --all-targets --all-features -D warnings`) owns
> that. Assume anything a linter can catch is already caught. Correctness over everything else.
> Paths below are repo-root-relative.

## What Rocky actually is (read this before reviewing)

Rocky is a **typed-program layer above the warehouse**: it compiles SQL / a small DSL into a single
typed IR, generates dialect SQL, and executes a DAG against Databricks / Snowflake / BigQuery / DuckDB.
**Storage and compute stay with the warehouse** — Rocky does not extract data or own the write transaction.

**The real pipeline (there is no HIR):**

```
surface syntax                          lowering              typed IR            codegen           execution
──────────────                          ────────              ────────            ───────           ─────────
SQL   → sqlparser-rs typed AST  ─┐
                                 ├──►  (direct)         ──►  ModelIr        ──►  dialect SQL   ──►  DAG executor
.rocky DSL → logos lexer          │    rocky-lang/           (rocky-ir,           rocky-core/        rocky-core/dag
  → parser → ast.rs              ─┘    lower.rs              single IR)          sql_gen.rs         + commands/run.rs
```

- **`ModelIr` is the *sole* transformation intermediate** (the legacy `Plan` enum is gone). There is
  **no separate HIR**. Do not review for "AST → HIR → typed IR" invariants — that pipeline does not exist.
- `ModelIr` and all IR types are **owned, value-semantics data** — every type derives
  `Clone + Serialize + Deserialize`. `Arc<str>` appears only as *immutable string interning* so cloning a
  plan is refcount-cheap. There is **no shared mutable node graph** across passes.
- Type checking is **best-effort inference over sqlparser's `Expr`**, producing `(RockyType, nullable)`
  pairs. `RockyType::Unknown` is a **safe, valid outcome** — not a defect.
- Compilation is **incremental via Salsa** (rust-analyzer's memoization framework). This is Rocky's
  single hardest correctness surface — see priority #3.

Key files:
- `engine/crates/rocky-ir/src/{ir.rs, types.rs, dag.rs}` — the IR, `RockyType`, DAG primitives
- `engine/crates/rocky-lang/src/lower.rs` — `.rocky` DSL AST → `ModelIr`
- `engine/crates/rocky-compiler/src/{typecheck.rs, types.rs, semantic.rs, contracts.rs, blast_radius.rs, salsa_compile.rs, incrementality.rs}`
- `engine/crates/rocky-core/src/{sql_gen.rs, dag.rs, drift.rs, state.rs, breaking_change.rs}`
- `engine/crates/rocky-cli/src/commands/run.rs` — the executor entrypoint (watermarks, resume)

## Review priority order (spend budget top-down)

1. **Type + nullability inference correctness** — when inference assigns a *concrete* `RockyType` and
   nullability, is it correct per SQL three-valued logic? (`Unknown` is not a finding.)
2. **IR-variant exhaustiveness + breaking-change classification** — does a change to a Rocky-owned enum
   silently escape a consuming match? Does it alter a contracted column's resolved type?
3. **Salsa incremental-recompute soundness** — can a stale memoized result survive an input change?
4. **Executor semantics** — DAG topological order, resume/retry idempotency, incremental-watermark
   soundness, no half-committed state on the failure path.
5. **Rust correctness hazards** — panics on user-reachable paths, `unsafe` without `// SAFETY:`.
6. **Test evidence** — is the change proven by a coded-diagnostic rejection test and an assertion on the
   emitted type / SQL?

Do **not** spend budget on formatting, import ordering, naming, or doc typos. Collapse those into at most
a single trailing `nits:` line.

## Stage invariants

### Surface → `ModelIr` lowering (`engine/crates/rocky-lang/src/lower.rs`; sqlparser AST for SQL)
- Nothing user-reachable is silently dropped on the way to `ModelIr`. A construct Rocky can't lower should
  produce a **diagnostic**, not a silent no-op.
- Desugaring is semantics-preserving. In particular the DSL's `!=` lowers to **`IS DISTINCT FROM`**
  (NULL-safe) — a change here that reverts to `=`/`<>` is a semantics bug.

### Type inference (`engine/crates/rocky-compiler/src/typecheck.rs`) — soundness core
- Inference returns `(RockyType, nullable)`. **Nullability is part of the type** and must propagate per
  SQL 3VL: any operand nullable ⇒ result nullable for comparisons/arithmetic; `IS NULL`/`IS NOT NULL` ⇒
  non-nullable `Boolean`; `CAST` is nullable; `COALESCE`/`IFNULL` should *clear* nullability when a later
  argument is provably non-null. Scrutinize new inference arms against these rules.
- **`RockyType::Unknown` is the correct conservative fallback** for expressions Rocky doesn't model. Do
  **not** flag `Unknown` results or the `_ =>` arms that produce them over sqlparser's `Expr`/`Function`.
- When inference *does* commit to a concrete type, it must be a type the expression can actually hold —
  a wrong concrete type flows into contracts and drift. That is the finding; `Unknown` is not.

### Exhaustiveness — the `_ =>` rule (grounded)
Flag a `_ =>` catch-all **only** when both hold:
1. it matches over a **Rocky-owned enum** (`ModelIrVariant`, `MaterializationStrategy`, `RockyType`,
   rocky-lang AST / `BinOp`), **and**
2. the fallback **silently proceeds** (returns a default / picks a code path) instead of emitting a
   diagnostic or being provably total.

- **Archetype to flag:** in `engine/crates/rocky-core/src/sql_gen.rs`, `_ => generate_create_table_as_sql(...)`
  over the materialization strategy — a new strategy variant would quietly emit `CREATE TABLE AS` instead
  of forcing the author to handle it. Silent-proceed over an owned enum = soundness risk.
- **Archetype to exempt:** in `typecheck.rs::infer_expr_type`, `_ => (RockyType::Unknown, true)` over
  sqlparser's `Expr`. That enum is foreign and huge; a conservative `Unknown`/nullable fallback is correct
  and must not churn on every `sqlparser` bump. A `_ => Err(diagnostic)` that *rejects* is also fine.

### Contracts (`engine/crates/rocky-core/src/{contracts.rs, breaking_change.rs}`, `rocky-compiler/src/blast_radius.rs`)
- A contract is the typed boundary (required / protected columns). Any change that alters a contracted
  column's **resolved type or presence** is a **breaking change** and must be called out explicitly, even
  if it compiles. The classifier in `breaking_change.rs` (`BreakingFinding`/`BreakingChange`/`BreakingSeverity`)
  is where this lives.
- Contract validation is **total** — a diagnostic for every invalid contract, never a panic, never a
  silent pass.

### Salsa incremental compilation (`engine/crates/rocky-compiler/src/{salsa_compile.rs, incrementality.rs}`, `typecheck_project_incremental`)
- Every input a query reads must be a tracked Salsa input/dependency. **A stale memoized result surviving
  an input change is the canonical incremental bug** — if a change adds a new data source to a compile
  query without threading it through Salsa's dependency graph, the second run can serve a stale IR/type.
- Incremental and from-scratch compilation must agree: `typecheck_project_incremental` must produce the
  same diagnostics and IR as `typecheck_project` for the same final state.

### Executor (`engine/crates/rocky-core/src/{dag.rs, state.rs}`, `rocky-cli/src/commands/run.rs`) — executor, not orchestrator
- Execution respects the dependency DAG **topologically**; no node runs before its inputs
  (`rocky-ir/src/dag.rs::topological_sort` / `execution_layers`).
- **Resume/retry is idempotent.** `rocky run --resume`/`--resume-latest` re-runs from a checkpoint; a
  retried node must not double-apply an `INSERT`/`MERGE` or corrupt redb state.
- **Incremental-watermark soundness (the real atomicity surface).** The runner reads the prior
  `MAX(ts)` from the redb state store, filters `WHERE ts > <watermark>`, then re-queries source to record
  the next watermark. Scrutinize the ordering: **a crash between committing rows and advancing the
  watermark, or advancing it past what actually committed, silently drops or duplicates data.** There is
  no Iceberg-level transaction to fall back on — the warehouse owns the data-write transaction; Rocky owns
  only redb. (`rocky-iceberg` is metadata/snapshot management, not a write-atomicity boundary.)

## Rust correctness hazards (compiler-grade bar)
- **No `unwrap()`/`expect()`/`panic!` on any path reachable from user input.** A compiler *rejects* bad
  input with a diagnostic; it does not crash. Panics are for bugs in Rocky.
- Errors propagate as `Result` with a `thiserror` type in library crates; `anyhow` only in the CLI binary.
- `unsafe` is near-zero and confined to known categories (mmap, `repr(transparent)` cast, serialized
  test-env mutation). Any new `unsafe` without a `// SAFETY:` justification is a finding.
- **Do not** flag `Clone` on `ModelIr` as a perf cliff — it is cheap by design (`Arc<str>` interning).
  Flag only a *new* heavy owned field (e.g. a large `Vec`/`String` where an `Arc<str>` was the pattern).

## Test-evidence expectations
Rocky uses **inline `#[cfg(test)] mod tests`** (plus `engine/crates/rocky-compiler/tests/` for fixtures).
There is **no `insta`/snapshot library** — do not ask for "golden IR snapshots." A soundness-relevant
change needs:
- A **rejection test** — a program that should fail to compile and now does, asserting the **specific
  diagnostic code** (e.g. `E020`–`E026`, the `W###` warnings), not just "an error".
- A **positive assertion** on the observable output — the inferred `(RockyType, nullable)`, the generated
  SQL string, or the emitted `ModelIr` shape — in the crate's existing inline-test style.

Flag any change to a core pass (lowering, typecheck, contracts, sql_gen, Salsa queries) that lands
**without** corresponding tests.

## Output format
Severity-tagged list. For each finding:
- `[SOUNDNESS]` / `[CORRECTNESS]` / `[TEST-GAP]` / `[NIT]`
- the specific invariant above it violates,
- the minimal fix, or the precise question that needs answering.

End **every** review with one explicit line:

> **ModelIr / contract semantics preserved: yes / no** — and if no, exactly which guarantee changed.
