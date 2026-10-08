# ADR-CONTRACTS — What a contract guarantees, where it is enforced, and how it may change

**Status:** Proposed — awaiting ratification

**Work package:** WP-03 (compiler and contract soundness) and WP-04 (governance semantics)
**Closes (once ratified and implemented):** audit findings **RD-011** (contract compatibility ignores nested and parameterized structure), **RD-012** (breaking-change classification defaults to safe), **RD-013** (promotion fails open when the semantic comparison cannot run). Sets the contract semantics that **WP-04** builds on.
**Sibling:** ADR-TYPES (not yet written) owns decimal arithmetic, numeric literal parsing, backend precision caps, and the call on the unmerged WP-03 decimal-inference work. This ADR does not decide any of those. It only consumes the `RockyType` that inference produces.

> Scope boundary. This ADR owns the *contract* question: what a declared contract promises, which gate checks it, how `Unknown` and nullability are treated at that gate, and how a type or contract change is classified. It does not change how types are inferred. A wrong concrete inferred type is an ADR-TYPES / `typecheck.rs` defect, not a contract defect.

---

## Context

### The problem

A contract is a promise about a table's shape: which columns exist, their types, and whether they can hold NULL. Readers of that table rely on the promise. Today Rocky has three contract surfaces, written at different times, with different comparison rules:

| Surface | Declared in | Checked by | Comparison |
|---|---|---|---|
| Model contract | `<model>.contract.toml` (sidecar or `--contracts <DIR>`) | `rocky-compiler/src/contracts.rs::validate_contract` | Exact type name match |
| Load contract | `[load] contract` in `rocky.toml` (`ContractConfig`) | `rocky-core/src/contracts.rs::validate_contract_typed` | Landed type must be *assignable to* the declared type |
| Cross-team contract | `[imports.<name>]` vendored snapshots | `rocky-cli/src/commands/imports_check.rs` over `rocky-core/src/breaking_change.rs::diff_project_ir` | Classifier `is_type_narrowing` |

The branch promotion gate (`rocky-cli/src/commands/branch.rs::evaluate_breaking_change_gate`) reuses the same classifier.

Three defects remain in these surfaces. Each one lets an incompatible change pass in silence.

1. **Nested types are only partly compared (RD-011).** In `type_name_matches`, any inferred `Array(_)` matches a contract that says `Array` *or starts with* `Array<`. So `Array<Int64>` in a contract accepts `Array(String)`. `Map` works the same way. A `Struct` matches only the bare word `Struct`, so a contract cannot state field names or types at all.
2. **The breaking-change classifier is inconsistent (RD-012).** `is_type_narrowing` ends in `_ => false`. A flat change it does not list is classified `Info`. So `Int32 → Boolean`, `Date → Int64` and `Decimal → String` are `Info`. The decimal arm compares only `np < op || ns < os`. It misses loss of integer digits: `Decimal(10,2) → Decimal(10,4)` drops from 8 integer digits to 6 and is classified as not narrowing. A change from `Unknown` to a concrete type also falls to `_ => false`. Nested types go the other way. The arm `(Array(_) | Map(..) | Struct(_) | Variant, _) if !matches!(new, Variant) => true` flags every change away from a nested type as narrowing, even a widening one, so nested types are over-reported today. The cross-team `E031` inherits all of this, because `imports_check.rs` reads the same `narrowing` flag.
3. **Promotion skips the gate when it cannot compile (RD-013).** `evaluate_breaking_change_gate` returns `Ok(None)` and records `BreakingChangesGateSkipped` when the models directory is missing, when HEAD's `compile::compile` returns `Err`, or when the base cannot be compiled. The caller lets the promote proceed on `None` (doc comment on `run_breaking_change_gate_for_plan`). The gate also never reads `has_errors`. A HEAD that compiles to `Ok` with error diagnostics still gets diffed, and its broken models may type as `Unknown`, which the classifier then treats as safe. The gate compiles with `contracts_dir: None`, so contracts in an explicit directory take no part.

### What is already closed — do not re-do

These were fixed in-tree after the audit. They are the base this ADR builds on.

- **`Unknown` no longer passes the model contract by accident (#1240).** `validate_contract` branches on `RockyType::Unknown` before calling `type_name_matches` and reports `I003` (info). `type_name_matches` returns `false` for `Unknown`, so a future caller that forgets the branch fails closed. The reason `I003` is info and not a warning is written on the `I003` constant in `rocky-compiler/src/diagnostic.rs`: `rocky test` and `rocky ci` compile with no source schemas.
- **The load gate treats the two sides of `Unknown` differently (#1614, #1646, #1721, #1856).** In `validate_contract_typed`, an unreadable *landed* type is a violation (`unverifiable_landed_type`) and refuses the load. An unreadable *declared* type is a warning. `landed_type_conforms` returns `false` for `Unknown` on either side, even though `rocky-ir`'s `is_assignable` treats `Unknown` as assignable both ways.
- **Decimal digits are checked in the model contract (#1489).** `decimal_type_matches` requires an exact match for `Decimal(p,s)` and treats a bare `Decimal` as "any digits".
- **`no_new_nullable` is enforced (#1467).** It emits `E014`, and refuses a contract that sets it with no `[[columns]]` baseline.
- **An unreadable config refuses the promote gate (#1702).** `evaluate_breaking_change_gate` returns `Err`, not `None`, when the project config fails to load.
- **A selected-model run can check an explicit contract (#2182).** `rocky run --pipeline <P> --model <M> --contracts <DIR>` validates the contract in the run's own compile (`run.rs::run_with_explicit_contracts`, `CompilerConfig::required_explicit_contract_model`).

### Where contracts are enforced today

```
  authoring                    write                        review / release
  ─────────                    ─────                        ────────────────
  rocky compile ─┐             rocky run (execute_models)   branch promote / apply
  rocky ci      ─┤ E010-E014   ├ sidecar contract error     ├ breaking-change gate
  rocky test    ─┤ I003 info   │  excludes the model and    │  skips on compile Err
  lsp / serve   ─┘ (sidecar    │  its descendants           │  or missing base
                   only in LSP)│  (run, pipeline, backfill, └ ignores has_errors
                               │  --dag, apply)
                               ├ --contracts <DIR>
                               │  (one selected model)
                               └ rocky load
                                  validate_contract_typed
                                  refuses before promote
```

- **Compile.** `compile.rs` always loads sidecar contracts (`discover_contracts_from_models`) and merges an explicit directory when one is set. `validate_all_contracts` runs every contract.
- **Run.** `run.rs::execute_models_with_explicit_contracts` compiles with sidecar contracts. It collects every model with an error diagnostic (`is_error()`), excludes those models, and blocks their descendants (`compile_error_descendant_blocks`). So a sidecar contract error already stops the write to that model and to everything downstream. This serves the main model run, the pipeline arm and backfill. `--dag` sub-runs (`run_dag_exec.rs`) and `rocky apply` both call `run::run`, so they reach the same path. The explicit `--contracts <DIR>` route adds directory contracts for one selected `full_refresh` model only. That is by design. Branch and shadow routes were not traced. Not verified.
- **LSP.** `lsp.rs` passes `contracts_dir: None`, but compile still merges sidecar contracts. So sidecar contract diagnostics reach the editor. Contracts in an explicit directory do not.
- **Load.** `load.rs::load_with_contract_gate` loads into a staging table, runs `validate_contract_typed`, and drops staging without touching the target on failure. `protected_columns` and `allowed_type_changes` are parsed but only reported as warnings, because a load has no prior target snapshot.
- **Dead code.** `rocky-core/src/contracts.rs::validate_contract` (the untyped, string-equality variant) has no production caller. It is `pub` in a `pub mod`, so it is public API of `rocky-core`. (`rocky-compiler` has its own, separate `validate_contract`, which is live.)

### Nullability today

The model contract checks nullability one way only. `ContractColumn.nullable = Some(false)` against an inferred nullable column is `E012`. `nullable = true` and an absent `nullable` check nothing.

The gate is only as sound as nullability inference. Per `AGENT_REVIEW.md`, inference follows SQL three-valued logic (3VL): any nullable operand makes a comparison or arithmetic result nullable; `IS NULL` is non-nullable; `CAST` is nullable (the code is narrower: a plain `CAST` keeps its operand's nullability, and ADR-TYPES settles which rule holds); `COALESCE` is nullable only when every argument is nullable. Unmodelled expressions fall to `(RockyType::Unknown, true)`.

This gives an asymmetry worth naming. For an unmodelled expression, the *type* check is skipped (`I003`, info), but the *nullability* check fails (`E012`, error), because the fallback says nullable. So nullability already fails closed on unresolved expressions, and type does not.

`RockyType` can carry nested nullability only for struct fields (`StructField.nullable`). `Array(Box<RockyType>)` and `Map(Box<RockyType>, Box<RockyType>)` have no element or value nullability.

### Versioning today

A contract file has no version field. Nothing diffs an old contract against a new one. Contract structs (`CompilerContract`, `ContractColumn`, `ContractRules`, `ContractConfig`) do not set `deny_unknown_fields`, so a misspelled key such as `nullble = false` parses and is ignored. The cross-team snapshot does carry a format version (`snapshot_version`, `E034` fails closed on a newer format).

---

## Decision

### 1. What a contract guarantees

A contract is a promise to the table's readers. When a model or load passes its contract, these hold for every declared column:

- **Presence.** A column listed in `required` or `protected` exists. A column listed only in `[[columns]]` is not promised to exist: when it is missing, `validate_contract` emits warning `W010`, not an error. List it in `required` to make presence part of the promise.
- **Type.** The column's resolved type matches the declared type, compared as defined in §2.
- **Nullability.** If the contract says `nullable = false`, the column cannot hold NULL, as proven by 3VL inference (§4).

A contract makes **no** promise about a column it does not declare, about values (ranges, uniqueness; those are checks), or about a type it could not resolve (§3).

A contract file is parsed strictly. Unknown keys are a parse error, on both the model contract and the load contract. Before 2.0 there is no compatibility shim for an old misspelling.

### 2. One recursive type comparison

The model contract compares the complete normalized type, recursively:

- `Decimal(p,s)`: exact precision and scale (unchanged).
- `Array<T>`: element types compared recursively.
- `Map<K,V>`: key and value types compared recursively.
- `Struct<...>`: field names, field order, field types (recursively), and field nullability. Field order counts, because `RockyType::Struct(Vec<StructField>)` equality is order-sensitive.

A bare parameterless spelling (`Decimal`, `Array`, `Map`, `Struct`) is an explicit wildcard over the parameters. This keeps today's bare-`Decimal` behaviour and extends it the same way. A parameter block that does not parse never matches, as `decimal_type_matches` already does.

One parser turns a contract type string into a `RockyType`. It replaces the prefix checks in `type_name_matches`. The parser is round-trip tested against every `RockyType` variant, using the contract spelling (`Int64`, `Array<Int64>`). That needs its own formatter: `Display for RockyType` writes a different spelling (`INT64`, `ARRAY<INT64>`). The match over `RockyType` in the comparison stays exhaustive, with no `_ =>` arm (the `AGENT_REVIEW.md` exhaustiveness rule).

Element and value nullability inside `Array` and `Map` is not representable in `RockyType`. A contract cannot state it. The docs say so.

### 3. `Unknown` is "not verified", never "verified"

Every contract gate keeps today's rule: `Unknown` is never a match, and never silently a pass. Each gate must report an unverified declared type with its own code or violation. Omitting `type` is the explicit wildcard: the column is checked for presence and nullability only.

What an unverified type *does* to the exit code at each gate is open. See Open question A.

### 4. Nullability follows 3VL, and over-approximation is the safe direction

- Inference may say "nullable" for a column that never holds NULL. It must never say "non-nullable" for a column that can hold NULL. A wrong non-nullable result is a soundness bug in `typecheck.rs`, and a contract that passes on it is a false guarantee.
- So `E012` stays fail-closed on unresolved expressions. The fix a user is told to apply is `COALESCE` or a `WHERE ... IS NOT NULL` filter, never a `CAST` (a cast never removes nullability; it only changes the type).
- `nullable = true` in a contract means "may be NULL". It checks nothing and promises nothing.
- Struct field nullability is compared as part of §2.

### 5. Where a contract is enforced

| Stage | Role | Required behaviour |
|---|---|---|
| Compile (`rocky compile`, `ci`, `test`, LSP, serve) | Early feedback | Report every violation with its code. Not the guarantee on its own. |
| Write (`rocky run`, `rocky load`) | **The guarantee** | A contract error stops the write to that table before the warehouse is touched. |
| Review / release (branch promote, `rocky apply` of a promote plan) | Change control | Classify the change (§6). Refuse when the comparison cannot run (§7). |

The guarantee holds only where a write is gated. Sidecar contracts already gate the main run, pipeline, backfill, `--dag` and `apply` routes, including descendants. The known gaps are the explicit `--contracts <DIR>` (model-only by design) and `rocky load` for directory contracts. Branch and shadow routes are not verified. Every user-facing page names the routes that gate a write and the routes that do not. See Open question D.

### 6. Classifying a type or contract change

**Type changes on a column (`breaking_change.rs`).** `is_type_narrowing` becomes an exhaustive table over `(RockyType, RockyType)` pairs with no default arm. Any pair not proven safe is breaking. The blanket arm that flags every nested change as narrowing is removed. The recursive comparison below replaces it, so a nested widening is no longer flagged. In particular:

- Decimal compares integer digits (`precision - scale`) and scale separately. Losing either is breaking.
- `Unknown` on either side is **not** `Info`. It is reported as unverified, and a gate treats it as breaking until resolved.
- Nested types compare recursively, under the same rules as §2.
- A change to a *contracted* column's resolved type or presence is breaking, even if the type widens. This is the rule in `AGENT_REVIEW.md`'s Contracts section. Widening on an uncontracted column may stay `Info`.
- The fix covers `E031` on cross-team imports too, because it reads the same flag.

**Nullability changes.** Two directions break two different parties.

```
  NOT NULL ──▶ nullable   breaks READERS (they relied on "no NULL")
  nullable ──▶ NOT NULL   breaks DATA AT REST (existing NULL rows cannot be stored)
```

Today `diff_columns` classifies `nullable → NOT NULL` as `Warning` (its own comment calls it breaking) and `NOT NULL → nullable` as `Info`. Cross-team imports already raise the first as error `E032`. For a contracted column, both directions are breaking. For an uncontracted column, `nullable → NOT NULL` becomes breaking and the other direction may stay `Info`.

**Contract changes.** A change to the contract file is classified by whether it weakens the promise:

- **Weakening = breaking.** Removing a `required` or `protected` column, changing `nullable = false` to true or removing it, changing or removing a declared `type`, or replacing a parameterized type with a bare wildcard.
- **Strengthening = compatible for readers.** Adding a column, adding `nullable = false`, or adding parameters. A strengthened contract may fail the model's own compile; that is the point.

There is no version field inside a contract file. Compatibility is decided by diffing the old contract against the new one at the review boundary (CI and promote), the same way the cross-team baseline is diffed today.

### 7. Promotion fails closed

The breaking-change gate refuses a promote, instead of skipping, when:

- HEAD's compile returns `Err`, **or** returns `Ok` with any error diagnostic (`has_errors`). A type-invalid HEAD is the change that most needs review.
- The models directory is missing.
- The base cannot be compiled or is unavailable.

The gate compiles with the project's contracts, including an explicit contracts directory, not `contracts_dir: None`.

A break-glass exception may exist. If it does, it records the compiler failure text and both commit identities (base and HEAD) in the audit trail. A `BreakingChangesGateSkipped` event stops being a silent pass. Whether to have one, and who may grant it, is Open question E.

---

## Open questions for ratification

**A. What does an unverified declared type do to the exit code?**

- *Option 1 — keep `I003` info everywhere.* No noise. But a contract with a declared type can pass a write without that type ever being checked.
- *Option 2 — fail at write and release gates, stay info at authoring.* `rocky run`, `rocky load` (already fails on the landed side) and promote refuse an unverified declared type. `rocky compile`, `ci` and `test` keep `I003` info, because they often run with no source schemas.
- *Option 3 — fail everywhere unless `type` is omitted.* Strictest. But `rocky test` and `rocky ci` compile with empty source schemas (per the `I003` doc), so most declared types would fail in every project until those commands gain a source-schema producer.

**Recommendation: Option 2 now, Option 3 once `rocky test` and `rocky ci` can read source schemas.** A write is where the guarantee is spent, so that is where an unproven type must stop.

**B. Exact match (model contract) versus assignable (load contract).**

- *Option 1 — keep both, and name them.* A model contract states the model's declared output type, so it is exact. A load contract states the type that landed data must fit in, so it accepts narrower data. Document the two questions side by side.
- *Option 2 — make both exact.* One rule. Breaks load contracts that rely on `INT` data under a `BIGINT` contract.
- *Option 3 — make both assignable.* One rule. A model contract would no longer pin the output type it declares.

**Recommendation: Option 1.** The two gates answer different questions. The fix is to say so, not to merge them.

**C. `protected_columns` and `allowed_type_changes` on the load contract.**

Today they are parsed, never enforced, and reported as warnings. A parsed-and-ignored field tells the operator a guard exists when it does not.

- *Option 1 — enforce them* against the existing target's schema read before the promote.
- *Option 2 — remove them* from `ContractConfig` before 2.0, so the parse fails.

**Recommendation: Option 1 if the target schema is already read on that path; otherwise Option 2.** Keeping a warning-only field is not an option.

**D. Close the two known gaps in write enforcement?**

Sidecar contracts already gate the main run, pipeline, backfill, `--dag` and `apply` routes (see Context). The gaps:

- Explicit `--contracts <DIR>` checks one selected `full_refresh` model only. This is by design (#2182).
- `rocky load` checks the load contract, not a model contract in a directory.
- Branch and shadow routes are not verified.

*Option 1 — close the gaps.* Trace the branch and shadow routes. Decide whether a directory contract should gate all selected models. *Option 2 — document them* as unguarded.

**Recommendation: trace the branch and shadow routes first, then choose.** Do not state they are guarded or unguarded until traced.

**E. Break-glass for a refused promote.**

Who may approve a promote the gate refused, and how, depends on a trust ADR (ADR-TRUST) that does not exist yet.

- *Option 1 — no break-glass.* A refused promote stays refused until the cause is fixed.
- *Option 2 — break-glass after ADR-TRUST lands.* Until then, Option 1 applies.

**Recommendation: Option 2.** Ship fail-closed first. Add the exception when its authorisation is defined.

---

## Consequences

### What changes

- `rocky-compiler/src/contracts.rs`: a recursive type parser and comparison replace the prefix match in `type_name_matches`. Contract structs reject unknown keys.
- `rocky-core/src/breaking_change.rs`: `is_type_narrowing` becomes an exhaustive, default-breaking table. Nullability severities change as §6 says. Contracted columns are classified more strictly. **Open prerequisite, no owner yet:** `diff_project_ir` sees only two `ProjectIr`s of `TypedColumn`s and does not know which columns are contracted. The classifier must be handed the contract set (contracted column names per model). Today nothing passes it. Someone must decide where that set comes from and own the work. Until then the contracted-column rules in §6 cannot be built.
- `rocky-cli/src/commands/branch.rs`: `evaluate_breaking_change_gate` refuses where it skips today, reads `has_errors`, and compiles with contracts.
- A new contract-to-contract diff at CI and promote.
- `rocky-core/src/contracts.rs::validate_contract` (untyped, no production caller) is deleted. It is `pub` API of `rocky-core`, so this is a **breaking public change**. It needs a changelog entry.

### Deliberate behaviour changes (call these out for sign-off)

1. Contracts that say `Array<T>` or `Map<K,V>` with the wrong `T`, `K` or `V` start failing with `E011`.
2. A contract with a misspelled key stops parsing.
3. Type changes that were `Info` become breaking. Promotes and imports that passed before can now be blocked.
4. A promote with a type-invalid HEAD or an unavailable base is refused. Whether any override exists is Open question E.

### Migration and compatibility

- **No back-compat shim before 2.0.** Each change ships with a changelog entry naming what now fails and how to fix it.
- **Codegen cascade.** `ContractConfig` and `ContractResult` derive `JsonSchema`, and `ContractResult` is part of a CLI output struct in `rocky-cli/src/output.rs`. Any field change there needs `just codegen` and `just regen-fixtures`. A change to `BreakingFinding` severities alters JSON output and the dagster fixtures in the same way.
- **New diagnostic codes** for "unverified type at a write gate" and "contract weakened" need entries in `diagnostic.rs` and the docs reference.

### What it does and does NOT close

- **Closes, once implemented:** RD-011 (recursive comparison and the `Unknown` policy), RD-012 (exhaustive, default-breaking classification), RD-013 (promotion fails closed).
- **Does NOT close:**
  - **RD-010 and decimal inference.** Wrong decimal precision or scale from inference is ADR-TYPES. A contract can only be as right as the type it is handed.
  - **The unmerged WP-03 decimal-inference work.** ADR-TYPES decides whether to open or archive it. Part of its load-gate `Unknown` work may already be covered on `main` by #1721; that needs checking when ADR-TYPES is written.
  - **The rest of WP-04.** Per-model check identity, missing verification evidence, durable policy audit writes, and the hostile-agent boundary are not contract semantics. This ADR gives them a stable contract and classifier to build on.
  - **Who may approve a break-glass promote.** Open question E, which depends on ADR-TRUST (not yet written).

### What it unblocks

- **Recursive contract compatibility (RD-011).** The comparison rules and the `Unknown` policy are fixed, so the compiler work can start.
- **Exhaustive breaking classification (RD-012).** The default-breaking rule and the nullability directions are fixed, so the table can be written and tested pair by pair.
- **Fail-closed promotion (RD-013).** The refuse-not-skip rule is fixed. Any break-glass waits on Open question E.
- **Governance semantics (WP-04).** WP-04 requires that missing or uncompilable evidence never produces a normal approval or promotion. §3, §6 and §7 define what "evidence" means for a contract.

---

## Alternatives considered

| Alternative | Why rejected |
|---|---|
| **Keep prefix matching for nested types** (status quo) | `Array<Int64>` accepts `Array(String)`. A contract that names a type it does not check is worse than one that names nothing. This *is* RD-011. |
| **Make `Unknown` a match again** (`is_assignable` semantics at the gate) | Reopens #1240 and #1721. "Rocky could not tell" is not evidence the data conforms. |
| **Keep `_ => false` in `is_type_narrowing`** to avoid over-alarming | A default-safe classifier hides exactly the changes nobody thought of. Over-reporting is visible and fixable; under-reporting ships a break. Matches the cross-team `SELECT *` rule, which already over-reports by design. (Nested types already over-report today; the recursive comparison makes that precise.) |
| **Semantic version field inside each contract file** | Authors would have to bump it by hand, and a forgotten bump is a silent break. A diff of old versus new at review time needs no author action. Revisit at 2.0 if contracts are published outside a repository. |
| **Classify nullability from one side only** | Each direction breaks a different party (readers or data at rest). Picking one hides the other. |
| **Skip the promote gate when HEAD fails to compile, but record it** (status quo) | The audit event records the skip, and the promote proceeds anyway. The change that most needs review gets none. |

---

## Validation

Every assertion must fail with the fix reverted (`scripts/mutation-check.sh`, per `AGENT_REVIEW.md`).

**Recursive comparison (§2)**
- `Array<Int64>` contract against inferred `Array(String)` ⇒ `E011`. Against `Array(Int64)` ⇒ pass. Bare `Array` ⇒ pass.
- `Map<String,Int64>` against `Map(String, Int32)` ⇒ `E011`.
- Struct: missing field, extra field, reordered fields, changed field type, and field `nullable` changed ⇒ each `E011`.
- An unparseable parameter block ⇒ no match.
- Parser round-trip over every `RockyType` variant, including nested `Decimal` inside `Array` and `Struct`.
- A contract with an unknown key ⇒ parse error naming the key.

**`Unknown` (§3, Open question A)**
- A declared type over an `Unknown` column ⇒ `I003` at compile, and the chosen behaviour at each write and release gate.
- The `Unknown` arm of the comparison returns "not a match" when called directly.

**Nullability (§4)**
- `nullable = false` over an unmodelled expression (fallback `(Unknown, true)`) ⇒ `E012`.
- `nullable = false` over `COALESCE(x, 0)` with `x` nullable ⇒ pass. Over `CAST(x AS ...)` with `x` nullable ⇒ `E012`.

**Classification (§6)**
- A generated test over every `(RockyType, RockyType)` pair, each with an explicit expected classification. A new `RockyType` variant fails to compile until the table handles it.
- Decimal boundaries: `(10,2) → (10,4)` breaking (integer digits 8 → 6), `(10,2) → (12,2)` safe, `(10,2) → (10,1)` breaking.
- `Unknown → Int64` and `Int64 → Unknown` ⇒ not `Info`.
- Contracted column widened ⇒ breaking. Uncontracted column widened ⇒ `Info`.
- Both nullability directions on a contracted column ⇒ breaking.
- Contract diff: each weakening in §6 ⇒ breaking; each strengthening ⇒ compatible.
- Cross-team: `E031` fires for a pair that was `_ => false` before.
- Nested: `Array(Int32) -> Array(Int64)` (widening) is not narrowing, and `Array(Int64) -> Array(Int32)` is. Today both are flagged by the blanket nested arm.

**Promotion (§7)**
- HEAD `compile` returns `Err` ⇒ refused. HEAD returns `Ok` with an error diagnostic ⇒ refused.
- Missing models directory, missing base ref, base that fails to compile ⇒ each refused.
- If Open question E adopts a break-glass: the same cases with a valid approval ⇒ proceed, and the audit record holds the compiler failure and both commit identities.
- A contract in an explicit contracts directory participates in the gate.
