# ADR-TYPES — What an inferred type promises, how decimals are sized, and how a type crosses a warehouse

**Status:** Proposed — awaiting ratification

**Work package:** WP-03 (compiler and contract soundness)
**Closes (once ratified and implemented):** audit findings **RD-010** (decimal arithmetic and numeric literal inference are unsound) and the inference half of **RD-028** (some expression and function types come from incomplete rules). Decides the fate of the unmerged branch `feat/wp03-pr1-decimal-inference` (Open question F).
**Sibling:** ADR-CONTRACTS owns what a contract gate does with a type: the recursive comparison, the `Unknown` gate policy, exact versus assignable at each gate, and breaking-change classification. This ADR owns how the type is produced. It does not change any gate rule.

> Scope boundary. This ADR owns the *inference* question: which `RockyType` and nullable bit Rocky writes for a column, how decimal digits grow, how a warehouse type string becomes a `RockyType`, and when a change to any of that is a public change. A gate that passes on a wrong inferred type is an ADR-TYPES defect. A gate that passes on a correct type it should have refused is an ADR-CONTRACTS defect.

---

## Context

### The problem

Every column in a compiled model carries a pair: a `RockyType` and a nullable bit. Contracts, drift, breaking-change classification, the model-detail API and the content-addressed writer all read that pair. If the pair is wrong, each of them is wrong in the same direction.

The governing rule, already stated in `AGENT_REVIEW.md`, is one sentence: **`Unknown` is a safe answer; a wrong concrete type is a defect.** Today some concrete answers are wrong, and some rules that decide them are not written down.

### Where an inferred type reaches a consumer today

This is the most important fact for sizing the work. Not every inference rule reaches a model's output columns.

```
  model SQL
     │
     ├─ lineage (rocky-sql/src/lineage.rs::extract_expr_lineage)
     │     Identifier / CompoundIdentifier ─▶ Direct
     │     Cast over a column              ─▶ Cast / TryCast
     │     Function over a column          ─▶ Aggregation(name), for ANY
     │       (first column argument)           function name, not only aggregates
     │     CAST over a column or function  ─▶ Cast / TryCast (overwrites an
     │                                          inner Aggregation edge)
     │     anything else (a + b, CASE,     ─▶ no edge ─▶ Expression
     │       literals, CAST(a + b AS ..))
     ▼
  typecheck.rs::compute_model_typecheck, Step 1      ◀── this builds the
     Direct         ─▶ upstream (type, nullable)          typed columns
     Cast           ─▶ (Unknown, upstream nullable)       every consumer reads
     TryCast        ─▶ (Unknown, true)
     Aggregation(f) ─▶ infer_aggregation_type(f, upstream type)
     Expression     ─▶ (Unknown, true)
     │
     ├─ enhanced_inference: a Cast / TryCast column whose input is known
     │     takes its type from infer_expr_type ─▶ sql_type_to_rocky(target)
     └─ a nullable-bit correction for outer joins (infer_select_types_with_lookup)

  infer_expr_type / infer_binary_op_type / infer_function_type / infer_case_type
     reach only: operand_check.rs (E042/W042, E043/W043), check_join_keys,
     the nullable-bit correction, and cast refinement.
```

So on `main`:

- **Live concrete-type producers** (they write a model's typed columns):
  - Direct passthrough of an upstream column.
  - The target of an infallible or fallible cast over a column, through `sql_type_to_rocky`.
  - `infer_aggregation_type`: `COUNT` and `COUNT_DISTINCT` (`Int64`, non-null), `SUM`, `AVG`, `MIN`, `MAX`. Every other function name returns `(Unknown, true)`.
  - Project UDFs (`udf::apply_direct_call_types`).
- **Latent rules** (they shape diagnostics, not a model's typed columns): arithmetic operators, numeric literals, `COALESCE`, `CASE`, `IF`, `GREATEST`, date functions. A wrong answer here produces a wrong `E042`/`E043`/join-key diagnostic. It does not reach a contract.

The unmerged WP-03 branch states the same fact in its own doc comment ("arithmetic types do not reach `ModelIr::typed_columns`"). One consequence: a user cannot give an arithmetic column a type with `CAST(a + b AS DECIMAL(12,2))`, because the cast has no lineage edge to refine.

### What is wrong today

**Live defects (a wrong concrete type reaches the typed columns).**

1. **`SUM` over a decimal keeps the input digits.** `infer_aggregation_type` returns `Decimal(p,s)` for `SUM` over `Decimal(p,s)`. A sum of ten `DECIMAL(10,2)` values can need 11 integer digits. The type is too narrow.
2. **`SUM` over an integer is `Int64`, and `SUM` over `Float32` is `Float32`.** Whether that bounds what the warehouse writes depends on the warehouse. Inference has no dialect (the `AVG` arm's own comment says so).
3. **Declared decimal digits wrap.** `sql_type_to_rocky` narrows sqlparser's `u64` precision and signed scale with `as u8`. `DECIMAL(300,2)` becomes a different type. `DECIMAL(10,-2)` becomes scale 254.
3a. **A function or cast wrapped around another function reads the wrong input.** Lineage traces the first column argument through nested calls and then overwrites the edge kind with the outer one. Read from `extract_expr_lineage` and Step 1 of `compute_model_typecheck`; not executed. Three results:
   - `MAX(LENGTH(name))` and `SUM(LENGTH(name))` take `name`'s type (`String`), not the function's result. `SUM(CAST(y AS DOUBLE))` takes `y`'s pre-cast type. These are wrong concrete types.
   - `CAST(NULLIF(x, 0) AS INT)` gets a `Cast` edge, which keeps `x`'s nullable bit. If `x` is non-null, the column is non-null, but `NULLIF` can return NULL. The same holds for `CAST(MAX(x) AS BIGINT)`. This is a wrong non-nullable result, the direction §4 forbids.
   - `COUNT(*)` has no column argument, so it gets no edge and is `(Unknown, true)`, not `(Int64, false)`. The `COUNT_DISTINCT` arm of `infer_aggregation_type` is dead: lineage names the function `COUNT` and the `DISTINCT` flag is separate.
4. **String-read decimal digits are not range-checked.** `decimal_family_type` (in both `rocky-compiler/src/compile.rs` and `rocky-core/src/contracts.rs`) parses precision and scale as `u8`. It accepts precision 0, scale greater than precision, and precision above every warehouse limit.

**Latent defects (diagnostics only, today).**

5. **All five arithmetic operators share one rule.** `infer_binary_op_type` sends `+ - * / %` to `common_supertype`. Multiply and divide need a different rule.
6. **Decimal unification drops integer digits.** `common_supertype` takes `max(p)` and `max(s)` independently. `Decimal(10,0)` with `Decimal(10,10)` gives `Decimal(10,10)`, which has no integer digits. `Int32` with `Decimal(5,2)` gives `Decimal(10,2)`, which holds 8 integer digits, not 10.
7. **Every numeric literal is `Int64`.** `infer_expr_type` types `Value::Number` as `Int64`, including `1.5` and `1e10`.
8. **An unmodelled literal is non-nullable.** The `Value` arm ends in `_ => (RockyType::Unknown, false)`. That is a "never NULL" claim over evidence Rocky did not read.
9. **`IF` / `IFF` ignore the else branch**, and branch unification is a left fold with `Unknown` as the seed. Once a cap exists (Decision §3), a fold is order-dependent.

**Mapping and assignability.**

10. **`is_assignable(Int64, Float64)` is `true`.** It falls through to `common_supertype`, whose doc comment says it "allows widening conversions that might lose precision". A value above 2^53 changes. This is the only integer-to-float pair that is wrongly assignable. `Int32` or `Int64` to `Float32` and `Float64` to `Float32` are already `false` today, because their supertype is `Float64`. Also today, `is_assignable(TimestampNtz, Timestamp)` is `true` and the reverse is `false`.
    `is_assignable` has two production callers, and both see any change to it: the load gate (`landed_type_conforms` in `rocky-core/src/contracts.rs`) and the UDF argument check (`rocky-compiler/src/udf.rs`, which raises `E051`).
11. **`Timestamp` and `TimestampNtz` unify to `Timestamp`.** One is an instant, the other is a wall-clock reading. Mixing them is not a widening.
12. **The per-adapter `TypeMapper` impls have no production caller.** `rocky-core/src/traits.rs::TypeMapper` is string-to-string. It is also public SDK surface: `rocky-adapter-sdk` re-exports it, and `rocky init-adapter` scaffolds an impl for third-party adapters. Its DuckDB, Databricks and Snowflake impls are called only from their own tests. The DuckDB `types_compatible` treats any two `DECIMAL…` strings as compatible. The `rocky-ir/src/types.rs` module doc says `TypeMapper` maps between warehouse types and `RockyType`. It does not.

### What is already closed — do not re-do

- **Bare decimal names no longer invent digits (#1646, #1721).** `sql_type_to_rocky` maps a bare `DECIMAL`/`NUMERIC` cast target to `Unknown`. Both `decimal_family_type` functions map a bare `DECIMAL`/`NUMERIC` to `Unknown`. `DECIMAL(p)` reads as `DECIMAL(p,0)` in all three parsers.
- **Dialect-aware reading at the load gate (#1856).** `rocky-core/src/contracts.rs::gate_type_to_rocky` reads a bare BigQuery `NUMERIC` as `NUMERIC(38,9)`, on both sides, only when `GateDialect::BigQuery`. A bare Snowflake `NUMBER` is `NUMBER(38,0)` in `rocky-core`'s `decimal_family_type` (the load gate). The compiler's `default_type_mapper` does not know `NUMBER` and maps it to `Unknown`.
- **Decimal `AVG` is `Unknown` (#1238).** `infer_aggregation_type` returns `Unknown` for `AVG` over a decimal. Other inputs give `Float64`.
- **Decimal capacity in assignment (#1342).** `is_assignable` compares scale and integer digits (`precision - scale`) separately, with `Int32` as 10 integer digits and `Int64` as 19.
- **`TRY_CAST` / `SAFE_CAST` are nullable (#1148).** Lineage tracks `TryCast` separately, and it stays sticky through an outer `CAST`.
- **An unverifiable landed type refuses the load (#1614, #1856).** `validate_contract_typed` reports `unverifiable_landed_type`. `landed_type_conforms` returns `false` for `Unknown` on either side. ADR-CONTRACTS records this.

### Cross-dialect type handling today

| Path | Function | Dialect? | Role |
|---|---|---|---|
| Source schema to `RockyType` (compiler) | `rocky-compiler/src/compile.rs::default_type_mapper` | No | Types source columns. Knows fewer names than the gate (no `INT64`, `NUMBER`, `BIGNUMERIC`), on purpose (#1646). |
| Landed and declared type at the load gate | `rocky-core/src/contracts.rs::warehouse_type_to_rocky`, `gate_type_to_rocky` | Only BigQuery `NUMERIC` | Compares a landed table to a load contract. |
| Drift: can `ALTER` keep the stored values? | `SqlDialect::is_safe_type_widening`; default `rocky-core/src/drift.rs::default_is_safe_type_widening` | Yes, per adapter | Allows numeric to `STRING` by default. BigQuery allows only `INT64`/`NUMERIC`/`BIGNUMERIC` steps. ClickHouse, Databricks and Trino return `false`. |
| `RockyType` to physical values | `rocky-cli/src/commands/run_content_addressed.rs` | Arrow | Builds `Decimal128Array` with the inferred precision and scale. A wrong inferred type changes what is written. |
| Warehouse string comparison | `TypeMapper::types_compatible` | Per adapter | No production caller. |

### Versioning today

`RockyType`'s serde shape is a public wire contract from engine 1.74.0 (its doc comment in `rocky-ir/src/types.rs`). Adding a variant is additive. Renaming, removing, or changing a payload is breaking and needs the codegen cascade. Nothing says that a change in *which* type is inferred for the same SQL is a public change. The engine version is part of `EnvIdentity` in `rocky-core/src/recipe_identity.rs`, so any release already changes the content-addressed recipe identity.

---

## Decision

### 1. The governing rule: an inferred type is a sound bound, not a prediction

For every column, the inferred pair `(T, nullable)` promises:

- **Type.** Every value the target warehouse can write into the column is a value of `T`. `T` may be wider than what the warehouse reports. It must never be narrower.
- **Nullability.** If `nullable` is `false`, the column cannot hold NULL on any supported warehouse. Inference may say "nullable" for a column that never holds NULL. It must never say the reverse.

When Rocky cannot prove a bound, the answer is `(Unknown, true)`. It never guesses, clamps, or wraps.

An over-wide type can make a correct model fail a contract. That is visible and the user can fix it. An over-narrow type can make a wrong model pass a contract. That is silent. So the rule picks the visible failure.

### 2. The lattice

`RockyType` keeps its current variants. This ADR adds none.

| Group | Variants | Notes |
|---|---|---|
| Exact numeric | `Int32`, `Int64`, `Decimal { precision, scale }` | `Int32` holds 10 integer digits, `Int64` holds 19 (as `is_assignable` already assumes). |
| Approximate numeric | `Float32`, `Float64` | |
| Other scalar | `Boolean`, `String`, `Binary`, `Date`, `Timestamp`, `TimestampNtz` | `Timestamp` is an instant. `TimestampNtz` is a wall-clock reading. |
| Nested | `Array(T)`, `Map(K, V)`, `Struct(fields)` | Only struct fields carry nullability. |
| Semi-structured | `Variant` | |
| No evidence | `Unknown` | See below. |

**What `Unknown` means.** `Unknown` means "Rocky has no bound for this column". It is not an error, and it is not a type. It has two roles, kept separate:

- **In inference, `Unknown` absorbs.** Any operator or function with an `Unknown` operand that the result depends on returns `Unknown`. A branch set (`COALESCE`, `CASE`, `IF`, `GREATEST`, `LEAST`) with any `Unknown` branch returns `Unknown`. A `NULL` literal is the one exception: it adds only nullability, not a type. This replaces today's rule, where `common_supertype` treats `Unknown` as the identity and lets the other branch win. An `Unknown` branch makes `common_supertype` return `Some(Unknown)`, not `None`. `None` still means "incompatible", which `check_join_keys` reports.
- **At a gate, `Unknown` is "not verified".** That rule is ADR-CONTRACTS §3. This ADR does not restate or change it.

`is_assignable` keeps answering `true` for `Unknown`. It is an inference helper. Gates do not call it on `Unknown`; they branch first (`landed_type_conforms`, `validate_contract`).

### 3. Decimal sizing

**Work in `(integer_digits, scale)`, not `(precision, scale)`.** Integer digits are `precision - scale`. `Int32` is `(10, 0)`. `Int64` is `(19, 0)`. All rules below use this pair. One function builds an inferred decimal from the pair, after a checked range test.

**Caps.**

- An **inferred** decimal has precision at most **38**. If a rule needs more, the result is `Unknown`. Rocky never clamps the scale to fit, because a clamped type cannot hold the inputs.
- A **declared** decimal (`CAST`, contract, source schema) is valid when `1 ≤ precision ≤ 76` and `0 ≤ scale ≤ precision`. 76 is BigQuery `BIGNUMERIC`'s limit, so a legal declaration is never refused. A declaration outside that range is refused with an error (§7). Per-adapter limits tighter than 76 are checked where the adapter is known (§5).

**Arithmetic.** With operands `(i1, s1)` and `(i2, s2)`, and at least one a `Decimal`:

| Operator | Integer digits | Scale | Why it is a bound |
|---|---|---|---|
| `+`, `-` | `max(i1, i2) + 1` | `max(s1, s2)` | One carry digit. |
| `*` | `i1 + i2` | `s1 + s2` | The exact product has exactly that many digits. |
| `%` | `min(i1, i2)` | `max(s1, s2)` | The remainder is smaller than both operands. |
| `/` | — | — | `Unknown`. The exact quotient does not terminate in general, and warehouses round in different ways. |

A float operand on either side gives `Float64`. A non-numeric operand gives `Unknown`.

**Integer pairs.** `Int32` and `Int64` operands with no decimal:

- Two `Int32` operands: `+`, `-`, `*` give `Int64`. Today `Int32 + Int32` gives `Int32`. That is narrower than what Snowflake (`NUMBER(38,0)`) and BigQuery (`INT64`) write. The product of two `Int32` values fits in `Int64`, so this is a sound bound on every adapter.
- Any `Int64` operand: `+`, `-`, `*` have the same dialect problem as `SUM(Int64)`. Snowflake writes `NUMBER(38,0)` for `Int64 * Int64`, which `Int64` does not hold. So the result is Open question A, not a fixed `Int64`. The interim answer is `Int64`, published as a known under-bound together with the `SUM` one.
- `/` gives `Unknown`. Warehouses return a float or a decimal, not an integer.
- `%` gives `Int64` (the remainder is smaller than the divisor, so it fits).

**Unification** (`COALESCE`, `CASE`, `IF`, `GREATEST`, `LEAST`, join keys) is not arithmetic. It takes `max(i)` and `max(s)` over the whole branch set, with no carry digit. Branches are unified as a set, not by a fold, so the order of arguments cannot change the result. Above the cap, the result is `Unknown`.

**Aggregates.**

| Function | Input | Result | Nullable |
|---|---|---|---|
| `COUNT`, `COUNT(DISTINCT …)` | any | `Int64` | no |
| `SUM` | `Decimal(p, s)`, `p ≤ 38` | `Decimal(38, s)` | yes |
| `SUM` | `Decimal(p, s)`, `p > 38` | `Unknown` | yes |
| `SUM` | `Int32`, `Int64` | Open question A | yes |
| `SUM` | `Float32`, `Float64` | `Float64` (today `Float32` stays `Float32`) | yes |
| `AVG` | `Decimal` | `Unknown` (unchanged, #1238) | yes |
| `AVG` | integer or float | Open question A | yes |
| `MIN`, `MAX` | `T` | `T` | yes |
| any other | any | `Unknown` | yes |

`SUM` over `Decimal(p, s)` with `p ≤ 38` becomes `Decimal(38, s)`. With `p > 38` (a BigQuery `BIGNUMERIC` source, declared up to 76) a fixed 38 would be narrower than the input, so the result is `Unknown`. A sum of values with scale `s` has at most `s` fractional digits. Warehouses widen a decimal sum's precision, most to 38. Postgres `NUMERIC` has no precision cap, so on Postgres `Decimal(38, s)` is not a proven bound. Dialect-aware inference (Open question A, Option 3) closes that gap. This is a deliberate change: a contract that declares `Decimal(10,2)` for `SUM(amount)` now fails `E011` and must declare `Decimal(38,2)`.

**Casts.** A cast's type is its target type, read by one shared parser:

- `DECIMAL(p, s)` and `DECIMAL(p)` (= scale 0) are checked against the declared range first. Out of range is an error, never a wrapped value.
- A bare `DECIMAL` / `NUMERIC` / `NUMBER` target follows §5: `Unknown` unless the dialect pins its digits.
- `DEC`, `BIGNUMERIC` and `BIGDECIMAL` are decimal-family names. A bare `BIGNUMERIC` is `Unknown`, because its default (`BIGNUMERIC(76.76, 38)`) has a fractional precision that `RockyType` cannot hold.

**Numeric literals.** Typed from their written form:

| Form | Example | Type |
|---|---|---|
| Integer within `i64` | `42` | `Int64` |
| Integer beyond `i64`, at most 38 digits | `99999999999999999999` | `Decimal(digits, 0)` |
| Fractional | `1.50` | `Decimal(total digits, fractional digits)`, i.e. `Decimal(3,2)` |
| Exponent | `1e10`, `2.5E-3` | `Float64` |
| Anything else | | `Unknown` |

Fractional literals are `Decimal`, not `Float64`, because `0.1` is exact as a decimal and not as a binary float. This is a dialect fact, not a universal one: BigQuery types `1.50` as `FLOAT64`. Open question G covers it. A leading minus sign is a unary operator and keeps its operand's type.

### 4. Nullability

Inference follows SQL three-valued logic. These rules are binding. Each one only ever widens toward "nullable".

- Comparison, boolean logic, `IN` list: nullable if any operand is nullable.
- Arithmetic `+ - *`: nullable if any operand is nullable. `/` and `%`: always nullable. DuckDB returns NULL for division by zero, and Databricks with `ANSI_MODE = off` does the same (also for decimal overflow). The live cross-check in Validation must confirm the DuckDB case before this ships. Whether `+ - *` over decimals is also fallible on a non-ANSI adapter is part of Open question B.
- `IS NULL`, `IS NOT NULL`, `EXISTS`: non-nullable `Boolean`.
- `COALESCE`: nullable only when every argument is nullable.
- `CASE` with no `ELSE`, `NULLIF`, `LAG`, `LEAD`, `NTH_VALUE`, `SUM`, `AVG`, `MIN`, `MAX`: nullable.
- `TRY_CAST`, `SAFE_CAST`: nullable.
- Plain `CAST` / `::`: Open question B. A cast over a function result is nullable unless the function is proven non-null (Context, defect 3a).
- Any unmodelled expression or literal: `(Unknown, true)`. The `_ => (RockyType::Unknown, false)` arm in `infer_expr_type`'s `Value` match becomes `true`.
- Outer joins: the null-extended side's columns are nullable (today's correction in `compute_model_typecheck`, kept).

**Cross-reference note on ADR-CONTRACTS and `AGENT_REVIEW.md`.** Both say "`CAST` is nullable" (ADR-CONTRACTS Context and §4, and the nullability rule in `AGENT_REVIEW.md`). That is not what `main` does. In `compute_model_typecheck` Step 1 and in `infer_expr_type`, a plain `CAST` keeps its input's nullable bit. Only `TRY_CAST` / `SAFE_CAST` are always nullable. So those two texts describe Option 1 of Open question B, while the code is Option 3. This ADR does not edit either text. Whichever option B takes, both need one edit to match it. ADR-CONTRACTS' validation line (`nullable = false` over `CAST(x AS ...)` ⇒ `E012`) is wrong for `main` today. The remedy text ("use `COALESCE` or a `WHERE … IS NOT NULL` filter, never a `CAST`") stays correct under every option.

### 5. Cross-dialect mapping

**Reading a warehouse type string into a `RockyType`.**

- A string maps to a concrete type only when its meaning is the same on every warehouse that can report it, or when the reader knows the dialect and the dialect pins it.
- A bare decimal-family name is `Unknown` unless the dialect pins its digits. Pinned today: Snowflake `NUMBER` = `(38,0)` in the load-gate reader only, and BigQuery `NUMERIC` = `(38,9)` at the load gate. A new pin needs a cited vendor default and a live-adapter test.
- A string outside the declared decimal range (§3) is `Unknown`, and the reader reports it where it has a diagnostic channel.
- The compiler's `default_type_mapper` stays dialect-free. It may know fewer names than the gate (#1646). It must never read one string differently from the gate when both give a concrete type. A test pins the two mappers equal on every string both of them type.

**Writing a `RockyType` to a warehouse or to Arrow.** Each adapter classifies every `RockyType` variant it can write as one of:

| Class | Meaning | Rocky does |
|---|---|---|
| **Exact** | Every value of the type round-trips. | Writes it. |
| **Widening** | The warehouse type holds every value, but reports a different type (for example `Int32` to Snowflake `NUMBER(38,0)`). | Writes it. A contract over the read-back type follows ADR-CONTRACTS. |
| **Lossy** | Some value changes (a precision above the adapter's cap, a timezone dropped, `Int64` into `FLOAT64`). | **Refuses** with a diagnostic naming the column, the type and the adapter. It never clamps or silently converts. |

The classification is an exhaustive match per adapter, with no `_ =>` arm (the `AGENT_REVIEW.md` exhaustiveness rule). Writing the per-adapter tables is required work; this ADR fixes only the classes and the refusal.

**Drift is a different question.** `is_safe_type_widening` asks "can `ALTER` keep the stored values?". It allows numeric to `STRING` on the default dialect. A reader's question ("does my query still get the same type?") is ADR-CONTRACTS §6. The two answers can differ, and both stay.

**The dead `TypeMapper` impls.** `TypeMapper` is removed, or rewritten so it returns a `RockyType` and is the one per-adapter reader. Open question E.

### 6. Assignability versus exact match

- `is_assignable(from, to)` means "every value of `from` is a value of `to`, unchanged". It is value-preserving.
- So `Int64` to `Float64` is **not** assignable. This is the one change from today. `Int32` or `Int64` to `Float32` and `Float64` to `Float32` are already not assignable, and stay so. `Int32` to `Float64` stays assignable: `Float64` holds every 32-bit integer exactly.
- Both callers see this change: the load gate and the UDF argument check (`E051`). See Deliberate behaviour changes 3 and 7.
- `Timestamp` and `TimestampNtz` are not assignable to each other and have no common supertype. Unifying them gives `Unknown`. Open question D covers whether to keep today's behaviour for a transition period.
- Which gate uses `is_assignable` and which uses exact match is ADR-CONTRACTS Open question B. This ADR defines only what "assignable" means.

### 7. Failure semantics and diagnostics

| Situation | Result | Diagnostic |
|---|---|---|
| A rule needs more than 38 digits | `Unknown` | None at inference. The gate reports it (ADR-CONTRACTS §3). |
| A declared decimal is outside `1 ≤ p ≤ 76`, `0 ≤ s ≤ p` | `Unknown` | **Error**, new code. Must not reuse `E036`, which `main` uses for the target-collision check in `compile.rs`. |
| A bare dialect-dependent cast target (`CAST(x AS DECIMAL)`) | `Unknown` | Warning naming the cast and the fix (declare the digits). |
| A warehouse string Rocky cannot read | `Unknown` | The load gate already reports it (`unverifiable_landed_type`, or a warning for the declared side). |
| A lossy write (§5) | Write refused | Error naming column, type and adapter. |

An out-of-range declaration is an error, not a warning, because degrading it to `Unknown` silently would turn it into an `I003` skip at the model contract gate.

### 8. Versioning: an inferred type is public behaviour

A change in the inferred `(RockyType, nullable)` for unchanged SQL and unchanged source schemas is a **public behaviour change**. That includes a concrete type becoming `Unknown`, and the reverse.

Why: it can flip a contract result (`E011`, `E012`, `I003`), a breaking-change finding, the model-detail API output, a join-key or operand diagnostic (`check_join_keys`, `operand_check.rs`), and the decimal digits the content-addressed writer uses.

Each such change ships with:

- A `Changed` entry in `engine/CHANGELOG.md` that names the expression, the old pair and the new pair, and what a user must edit.
- A golden test. A corpus of SQL expressions, with the expected `(RockyType, nullable)` for each, is asserted in `rocky-compiler`. A change to an expected value is the reviewable diff.
- No compatibility shim before 2.0. There is no flag that restores the old inference.

A change to the `RockyType` enum itself already follows its wire-contract doc comment and the codegen cascade.

---

## Open questions for ratification

**A. `SUM` and `AVG` over integers and floats need a dialect.**

`SUM(Int64)` is `BIGINT` on Databricks and `INT64` on BigQuery. It is `NUMBER(38,0)` on Snowflake. DuckDB returns a 128-bit integer. `AVG(Int64)` is a float on most warehouses and a scaled `NUMBER` on Snowflake. Inference has no dialect today.

- *Option 1 — `Unknown`.* Sound. But every contract over `SUM(qty)` becomes `I003` (not checked) until source schemas and a dialect exist.
- *Option 2 — keep `Int64` and `Float64`, and publish them as known under-bounds.* No user-visible change. Breaks §1 for two common functions, in writing.
- *Option 3 — dialect-aware inference.* Pass the pipeline's target dialect into typecheck, and let each adapter supply aggregate result types. `Unknown` only when no dialect is known (for example `rocky test` with no target).

**Recommendation: Option 3, with Option 2 as the interim.** The interim is honest only if the under-bounds are listed in the docs and in a test that fails when they change. Do not take Option 1: it hides most numeric contracts at once.

**B. Is a plain `CAST` over a non-null input nullable?**

Today `CAST` keeps its input's nullability. On Databricks with `ANSI_MODE = off` (a workspace setting, see `rocky-databricks/src/dialect.rs`), a failed cast returns NULL instead of an error. So a "never NULL" claim through a `CAST` is false there.

- *Option 1 — always nullable.* Matches the short form in `AGENT_REVIEW.md` and ADR-CONTRACTS §4. Some `nullable = false` contracts over a cast start failing `E012`.
- *Option 2 — nullable unless the conversion is total.* A widening cast (`Int32` to `Int64`, any type to `String`) keeps the input's nullability. A narrowing or parsing cast is nullable.
- *Option 3 — keep today's rule, and document that it assumes ANSI mode.*

**Recommendation: Option 2.** It is sound on every adapter and keeps the common case (`CAST(id AS BIGINT)`) non-null. Then update the one-line rule in `AGENT_REVIEW.md` to match, and edit ADR-CONTRACTS (see the cross-reference note in §4). Under Option 2 the ADR-CONTRACTS validation line holds only for a non-total cast.

The same ANSI-off argument covers other fallible operations: decimal `+ - *` overflow returns NULL on Databricks with `ANSI_MODE = off`. If the owner accepts Option 2 for that reason, the arithmetic rule in §4 must follow it. Do not ratify one without the other.

Option 2 needs the input type. For `CAST(a + b AS …)` (Open question C, Option 1) the input is `Unknown`, so that cast stays nullable.

**C. A typed escape hatch for an arithmetic column.**

Arithmetic, `CASE` and `COALESCE` columns are `Unknown` at the model level, and `CAST(a + b AS …)` does not refine them (Context). So a user cannot make such a column checkable.

- *Option 1 — add a lineage edge for a top-level `CAST` over any expression*, and type the column from the cast target. Nullability stays `true` unless proven otherwise.
- *Option 2 — route every output column through `infer_expr_type`*, so the §3 rules become live, not latent.
- *Option 3 — leave it.* Document that only direct, cast-over-column and aggregate columns are typed.

**Recommendation: Option 1 first.** It is small and gives users a fix. Option 2 is the real end state, but it makes every rule in §3 live at once. Do it only after the golden corpus (§8) exists, so the flip is one reviewable diff.

**D. `Timestamp` and `TimestampNtz`.**

- *Option 1 — no common supertype, not assignable* (§6). Some `CASE` and join-key results become `Unknown` or a diagnostic.
- *Option 2 — keep `Timestamp` as their supertype*, and document it as lossy.

**Recommendation: Option 1.** A wall-clock reading is not an instant. Mixing them silently is the class of bug a type system exists to stop.

**E. The per-adapter `TypeMapper` trait.**

- *Option 1 — delete it.* It has no production caller, and its DuckDB impl accepts any decimal pair. It is public `rocky-adapter-sdk` surface and a scaffold target of `rocky init-adapter`, so deleting it breaks third-party adapters and needs a changelog entry and a template change.
- *Option 2 — rewrite it* to return a `RockyType` and own the dialect pins in §5, replacing `GateDialect`.

Option 2 is the same kind of break for any third-party impl, since the return type changes.

**Recommendation: Option 2 if Open question A takes Option 3** (the adapter then needs a typed hook anyway). Otherwise Option 1. Either way, fix the `rocky-ir/src/types.rs` module doc.

**F. The unmerged branch `feat/wp03-pr1-decimal-inference`.**

Sixteen commits from July 2026. It is not on `origin`. Its merge base is about 585 commits behind `main` (counted 2026-10-08).

What it holds:

- **A sound decimal algebra in `rocky-ir`**: `decimal_from_parts`, `decimal_parts_of`, `validate_decimal_params`, `arithmetic_result_type`, set-wide `common_supertype_of`. Its rules match §3 for decimals.
- **Literal typing and the `as u8` fix** in `typecheck.rs`, and the two string parsers.
- **A DuckDB live-execute cross-check** (`decimal_inference_conformance.rs`).
- **A three-way contract verdict** (`ContractVerdict`, `UNVERIFIABLE_POLICY = Warn`) that warns and *permits* when both sides are in the decimal family but undecidable, with a planned flip in 1.69.0.

Why it cannot merge as it is:

- A trial merge conflicts in every core file: `rocky-ir/src/types.rs`, `typecheck.rs`, `compile.rs`, `diagnostic.rs`, and both `contracts.rs`.
- Its new error code `E036` is now taken on `main`.
- Its contract-verdict half is superseded. `main` already refuses an unverifiable landed type (#1614, #1856), and ADR-CONTRACTS §3 owns that policy. Merging "warn and permit" would reopen a fail-open that `main` closed. This also answers ADR-CONTRACTS' note: the branch's "`Unknown` is not a permit at `rocky load`" work is covered on `main`.
- Most of its algebra is latent today (Context), so it does not close the live defects alone. It does not touch `SUM`.

Options:

- *Option 1 — rebase and open one PR.* Large conflict work, and the gate half must be removed anyway.
- *Option 2 — archive it, and re-implement from this ADR alone.*
- *Option 3 — archive it, and port its algebra and tests into fresh PRs on `main`.* Port the `rocky-ir` functions, the literal typing, the parameter validation and the conformance test. Drop the contract verdict and the 1.69.0 flip. Give the declaration error a new code.

**Recommendation: Option 3.** The algebra was red-teamed once and matches §3. The gate half is the part `main` has moved past. Archive with a tag (for example `archive/wp03-pr1-decimal-inference` at `899e2f4f`) pushed to `origin`, so the commits stay reachable after the local branch is deleted.

**G. Fractional literal type per dialect.**

§3 types `1.50` as `Decimal(3,2)`. DuckDB, Snowflake, Databricks and Trino read it as a decimal. BigQuery reads it as `FLOAT64`. Literal typing is latent today, so nothing breaks yet. It breaks the day Open question C (Option 2) makes literals reach typed columns.

- *Option 1 — `Decimal` everywhere.* Simple. Wrong type name on BigQuery, so a landed-type comparison there can fail.
- *Option 2 — `Unknown` for a fractional literal until dialect-aware inference exists* (Open question A, Option 3).
- *Option 3 — `Decimal`, with BigQuery as a pinned exception once the adapter hook exists.*

**Recommendation: Option 3, with Option 2 as the interim for any rule that makes literals live.** Do not take Open question C, Option 2 before this is settled.

---

## Consequences

### What changes

- `rocky-ir/src/types.rs`: an `(integer_digits, scale)` decimal algebra with checked arithmetic. Operator-specific arithmetic. Set-wide unification where `Unknown` absorbs. `is_assignable` becomes value-preserving. The module doc stops claiming `TypeMapper` maps to `RockyType`.
- `rocky-compiler/src/typecheck.rs`, `rocky-sql/src/lineage.rs`: a function-wrapped function or cast reads the right input type and nullable bit (defect 3a). `COUNT(*)` is typed. The dead `COUNT_DISTINCT` arm goes.
- `rocky-compiler/src/udf.rs`: the argument check follows the new `is_assignable` (`E051` changes).
- `rocky-compiler/src/typecheck.rs`: `infer_binary_op_type` uses the operator table. `infer_expr_type` types literals by form and makes unmodelled literals nullable. `sql_type_to_rocky` validates digits before narrowing. `infer_aggregation_type` changes `SUM`. `IF`/`IFF` unify both branches.
- `rocky-compiler/src/compile.rs` and `rocky-core/src/contracts.rs`: both `decimal_family_type` functions apply the declared range.
- Each adapter: an exhaustive write classification (§5).
- `rocky-compiler/src/diagnostic.rs`: a new error code for an out-of-range declaration, and a warning for a bare dialect-dependent cast target.
- A golden inference corpus test (§8).

### Deliberate behaviour changes (call these out for sign-off)

1. A contract declaring `Decimal(p,s)` for `SUM` over `Decimal(p,s)` fails `E011`. The fix is to declare `Decimal(38,s)`.
2. A `DECIMAL(300,2)` or `DECIMAL(5,10)` declaration stops compiling.
3. A load contract declaring `DOUBLE` stops accepting landed `BIGINT` (§6). This is the only integer-to-float pair that changes.
4. Some join-key and operand diagnostics change, because `common_supertype` changes.
5. Depending on Open questions B and D: some `nullable = false` contracts over a cast fail `E012`, and mixed timestamp branches become `Unknown`.
6. `SUM(Float32)` becomes `Float64`. A contract declaring `Float32` for it fails `E011`.
7. A UDF call that passes an `Int64` argument to a `DOUBLE` parameter starts raising `E051` (§6). Today it passes.
8. A load contract that declares `TIMESTAMP` against a landed `TIMESTAMP_NTZ` column stops being accepted if Open question D takes Option 1. Today `is_assignable(TimestampNtz, Timestamp)` is `true`.
9. `CAST(NULLIF(x, 0) AS …)`, `CAST(MAX(x) AS …)` and similar casts over a function become nullable, so a `nullable = false` contract over them fails `E012`. `MAX(LENGTH(n))`, `SUM(CAST(y AS DOUBLE))` and `COUNT(*)` change type (defect 3a).
10. `/` and `%` columns become nullable even over non-null operands.

### Migration and compatibility

- **No back-compat shim before 2.0.** Each change has a changelog entry naming what now fails and how to fix it.
- **No codegen cascade from this ADR alone.** No `RockyType` variant is added or changed. If Open question E rewrites `TypeMapper`, check whether any output struct changes.
- **Dagster fixtures.** A changed inferred type changes `rocky compile` JSON. Run `just regen-fixtures` after the engine change.

### What it does and does NOT close

- **Closes, once implemented:** the nested function and cast defects (3a), RD-010 (operator rules, literal typing, digit validation, unification), and the inference half of RD-028 (`IF`/`IFF` branches, aggregate rules). The live `SUM` decimal under-bound.
- **Does NOT close:**
  - **Contract gate rules.** `Unknown` at a gate, exact versus assignable per gate, and breaking-change classification are ADR-CONTRACTS.
  - **Date-function typing.** `DATE_TRUNC`, `DATE_ADD` and `MONTHS_BETWEEN` (RD-028) are latent today. They need per-function signatures, which this ADR does not list.
  - **Nested element nullability.** `Array` and `Map` cannot carry it. Adding it is a `RockyType` payload change and needs its own record.
  - **`BIGNUMERIC`'s fractional default precision.** `RockyType` cannot represent it. A bare `BIGNUMERIC` stays `Unknown`.

### What it unblocks

- WP-03 staged PR 1 (checked decimal and literal inference) and PR 4 (conservative function inference) can start against fixed rules.
- ADR-CONTRACTS' recursive comparison can rely on a decimal pair that is a sound bound.

---

## Alternatives considered

| Alternative | Why rejected |
|---|---|
| **Predict the warehouse's result type** instead of a bound | Inference has no dialect, and warehouses disagree (decimal division alone has five answers). A prediction that is wrong on four adapters is a wrong concrete type on four adapters. |
| **Clamp to 38 digits** when a rule overflows, as Spark and Trino do | A clamped type cannot hold the inputs. That is a narrower-than-true type, the one direction §1 forbids. |
| **Type fractional literals as `Float64`** | `0.1` is not a binary float. The decimal type is exact, and it lets `price * 1.1` stay decimal. |
| **Keep `Unknown` as the identity in unification** | One unresolved branch then lets another branch's type stand for both. That is a concrete claim about a value Rocky did not read. |
| **Add a `RockyType::Null` variant** for the `NULL` literal | A public wire-contract addition for a case the unifier can handle by skipping the literal. Revisit if more bottom-type cases appear. |
| **One dialect-free mapper for every path** | BigQuery `NUMERIC` at the load gate needs the dialect (#1856). Forcing one reading would refuse valid BigQuery contracts or invent digits elsewhere. |
| **A version field on inference** that users pin | Before 2.0 there is no back-compat promise. A changelog entry and a golden corpus give the same visibility with no extra surface. |

---

## Validation

Every assertion must fail with the fix reverted (`scripts/mutation-check.sh`, per `AGENT_REVIEW.md`).

**Decimal algebra (§3)**
- Operator matrix over `Int32`, `Int64`, `Decimal` pairs for `+ - * / %`, asserting the `(RockyType, nullable)` pair. `Decimal(10,2) + Decimal(10,2)` ⇒ `Decimal(11,2)`. `*` ⇒ `Decimal(20,4)`. `/` ⇒ `Unknown`.
- Unification: `Decimal(10,0)` with `Decimal(10,10)` ⇒ `Decimal(20,10)`. `Int32` with `Decimal(5,2)` ⇒ `Decimal(12,2)`. Over the cap ⇒ `Unknown`. Every permutation of a three-branch `COALESCE` gives the same result.
- An upper-bound property test: for random operands, the exact result of the operation fits the inferred type.
- A live cross-check on DuckDB: the value DuckDB writes fits the inferred type for each operator. It also checks nullability: `x / 0` and `x % 0` on a non-null `x`.
- `SUM` over `Decimal(60, 2)` ⇒ `Unknown`, not `Decimal(38,2)`.

**Declarations and literals (§3, §7)**
- `DECIMAL(300,2)`, `DECIMAL(10,-2)`, `DECIMAL(5,10)`, `DECIMAL(0,0)` ⇒ the new error, in a cast, a source schema and a contract.
- `DECIMAL(76,38)` ⇒ accepted. `DECIMAL(10)` ⇒ `Decimal(10,0)` in all three parsers.
- Literals: `42`, `1.50`, `1e10`, a 25-digit integer, a 40-digit integer, malformed text.

**Nested functions and casts (defect 3a)**
- `MAX(LENGTH(n))` ⇒ `Int64`, nullable. `SUM(CAST(y AS DOUBLE))` ⇒ `Float64`. `COUNT(*)` ⇒ `Int64`, non-null.
- `CAST(NULLIF(x, 0) AS INT)` with non-null `x` ⇒ nullable. `CAST(x AS BIGINT)` with non-null `x` ⇒ the rule chosen in Open question B.

**Aggregates (§3, Open question A)**
- `SUM` over `Decimal(10,2)` reaches the model's typed columns as `Decimal(38,2)`, nullable.
- Whichever option A takes: an asserted pair for `SUM` and `AVG` over `Int32`, `Int64`, `Float32`.

**Nullability (§4, Open question B)**
- An unmodelled literal variant ⇒ nullable.
- `TRY_CAST` over a non-null column ⇒ nullable. Plain `CAST` ⇒ the chosen rule.
- `IF(c, a, b)` with `b` nullable ⇒ nullable.

**Mapping and assignability (§5, §6)**
- `is_assignable(Int64, Float64)` ⇒ `false`. `is_assignable(Int32, Float64)` ⇒ `true`. `is_assignable(Int64, Float32)` and `is_assignable(Float64, Float32)` stay `false`.
- The UDF argument check and the load gate each have a test for the `Int64` to `Float64` change.
- `default_type_mapper` and `warehouse_type_to_rocky` agree on every string that both map to a concrete type.
- Per adapter: an exhaustive write classification over every `RockyType` variant. A new variant fails to compile until each adapter classifies it. A lossy write is refused with the diagnostic.

**Versioning (§8)**
- The golden corpus runs in `rocky-compiler` tests. A PR that changes an expected pair shows the change in the diff and carries a `Changed` changelog entry.
