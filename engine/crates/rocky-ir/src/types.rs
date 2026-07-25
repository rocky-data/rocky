//! Rocky's unified type system for compile-time type checking.
//!
//! `RockyType` is the canonical type representation that all warehouse-specific
//! types map to. The `TypeMapper` trait (defined in `rocky-core/traits.rs`)
//! handles bidirectional mapping between warehouse types and `RockyType`.
//!
//! Lives in `rocky-core` so that IR types (`rocky-core::ir`) can reference
//! typed-column primitives without inducing a `rocky-core → rocky-compiler`
//! dependency cycle (rocky-compiler depends on rocky-core, one-way).
//! `rocky-compiler::types` re-exports these names so existing call sites
//! continue to compile unchanged.

use serde::{Deserialize, Serialize};

/// Rocky's unified column type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RockyType {
    // Numeric
    Boolean,
    Int32,
    Int64,
    Float32,
    Float64,
    Decimal {
        precision: u8,
        scale: u8,
    },

    // String
    String,

    // Temporal
    Date,
    /// Timestamp with timezone.
    Timestamp,
    /// Timestamp without timezone (common in Databricks).
    TimestampNtz,

    // Binary
    Binary,

    // Complex
    Array(Box<RockyType>),
    Map(Box<RockyType>, Box<RockyType>),
    Struct(Vec<StructField>),

    // Semi-structured (Snowflake VARIANT, etc.)
    Variant,

    // Type not yet inferred — not an error, just limits checking.
    Unknown,
}

/// A field in a struct type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructField {
    pub name: std::string::String,
    pub data_type: RockyType,
    pub nullable: bool,
}

/// A column with its inferred type and nullability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypedColumn {
    pub name: std::string::String,
    pub data_type: RockyType,
    pub nullable: bool,
}

impl std::fmt::Display for RockyType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RockyType::Boolean => f.write_str("BOOL"),
            RockyType::Int32 => f.write_str("INT32"),
            RockyType::Int64 => f.write_str("INT64"),
            RockyType::Float32 => f.write_str("FLOAT32"),
            RockyType::Float64 => f.write_str("FLOAT64"),
            RockyType::Decimal { precision, scale } => write!(f, "DECIMAL({precision},{scale})"),
            RockyType::String => f.write_str("STRING"),
            RockyType::Date => f.write_str("DATE"),
            RockyType::Timestamp => f.write_str("TIMESTAMP"),
            RockyType::TimestampNtz => f.write_str("TIMESTAMP_NTZ"),
            RockyType::Binary => f.write_str("BINARY"),
            RockyType::Array(inner) => write!(f, "ARRAY<{inner}>"),
            RockyType::Map(k, v) => write!(f, "MAP<{k},{v}>"),
            RockyType::Struct(fields) => {
                f.write_str("STRUCT<")?;
                for (i, field) in fields.iter().enumerate() {
                    if i > 0 {
                        f.write_str(",")?;
                    }
                    write!(f, "{}:{}", field.name, field.data_type)?;
                }
                f.write_str(">")
            }
            RockyType::Variant => f.write_str("VARIANT"),
            RockyType::Unknown => f.write_str("?"),
        }
    }
}

impl RockyType {
    /// Returns true if this type is numeric (can participate in arithmetic).
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            RockyType::Int32
                | RockyType::Int64
                | RockyType::Float32
                | RockyType::Float64
                | RockyType::Decimal { .. }
        )
    }

    /// Returns true if this is an integer type.
    pub fn is_integer(&self) -> bool {
        matches!(self, RockyType::Int32 | RockyType::Int64)
    }

    /// Returns true if this is a floating-point type.
    pub fn is_float(&self) -> bool {
        matches!(self, RockyType::Float32 | RockyType::Float64)
    }

    /// Returns true if this is a fixed-point decimal type.
    pub fn is_decimal(&self) -> bool {
        matches!(self, RockyType::Decimal { .. })
    }

    /// Returns true if this type is temporal.
    pub fn is_temporal(&self) -> bool {
        matches!(
            self,
            RockyType::Date | RockyType::Timestamp | RockyType::TimestampNtz
        )
    }
}

/// Widest precision Rocky will assert for an **inferred** decimal type.
///
/// 38 is the minimum maximum-precision across the five supported adapters
/// (Databricks/Spark, Snowflake, BigQuery `NUMERIC`, DuckDB, Trino). Inferring
/// anything wider would name a type some supported target cannot hold, so a
/// result that needs more precision degrades to [`RockyType::Unknown`].
pub const MAX_INFERRED_PRECISION: u32 = 38;

/// Widest precision Rocky will accept in a **user-written declaration**.
///
/// 76 is BigQuery `BIGNUMERIC`'s maximum, which is a legal declaration; a
/// blanket [`MAX_INFERRED_PRECISION`] cap here would false-reject it. The
/// asymmetry is the cap policy in one line: *validation uses the loosest bound
/// so a legal declaration is never rejected, inference uses the strictest bound
/// so Rocky never asserts a type some supported adapter cannot hold.*
pub const MAX_DECLARED_PRECISION: u32 = 76;

/// An arithmetic operator whose decimal result type Rocky derives.
///
/// Kept as a Rocky-owned enum (rather than reusing sqlparser's
/// `BinaryOperator`) so `rocky-ir` needs no SQL-parser dependency for the type
/// algebra.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecimalOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
}

/// Build a [`RockyType::Decimal`] from an integer-digit count and a scale, or
/// degrade to [`RockyType::Unknown`] when the result cannot be represented.
///
/// This is the **only** construction site for an inferred decimal. Every rule
/// in this module is stated in `(integer_digits, scale)` space and funnels
/// through here, because working in `(precision, scale)` space is precisely the
/// mistake that let integer digits disappear (`Decimal(10,0)` unified with
/// `Decimal(10,10)` used to yield `Decimal(10,10)` — zero integer digits).
///
/// Callers do all intermediate arithmetic in `u32` with checked operations; the
/// narrowing to the `u8` fields happens here, *after* the range check, so no
/// value can silently wrap.
///
/// Returns [`RockyType::Unknown`] when the scale alone exceeds the cap, when
/// `integer_digits + scale` overflows, when the total precision is zero, or
/// when the total precision exceeds [`MAX_INFERRED_PRECISION`].
pub fn decimal_from_parts(int_digits: u32, scale: u32) -> RockyType {
    if scale > MAX_INFERRED_PRECISION {
        return RockyType::Unknown;
    }
    let Some(precision) = int_digits.checked_add(scale) else {
        return RockyType::Unknown;
    };
    if precision == 0 || precision > MAX_INFERRED_PRECISION {
        return RockyType::Unknown;
    }
    let (Ok(precision), Ok(scale)) = (u8::try_from(precision), u8::try_from(scale)) else {
        return RockyType::Unknown;
    };
    RockyType::Decimal { precision, scale }
}

/// Decompose an exact-numeric type into `(integer_digits, scale)`.
///
/// - `Int32` → `(10, 0)` — `i32::MAX` is 2147483647, so ten integer digits hold
///   every value.
/// - `Int64` → `(19, 0)` — `i64::MIN`'s magnitude is 9223372036854775808, so
///   nineteen integer digits hold every value. (Spark promotes `LONG` to
///   `DECIMAL(20,0)`; Rocky is deliberately one digit tighter, and DuckDB's
///   measured `BIGINT + DECIMAL(10,2) = DECIMAL(22,2)` agrees with 19.)
/// - `Decimal { precision, scale }` → `(precision - scale, scale)`.
///
/// Returns `None` for any other type, and for a *malformed* decimal — zero
/// precision, or a scale greater than the precision. Such a value cannot be
/// produced by Rocky's own parsers (they validate first), but `RockyType` is a
/// public enum with public fields, so the algebra must not underflow on one.
pub fn decimal_parts_of(ty: &RockyType) -> Option<(u32, u32)> {
    match ty {
        RockyType::Int32 => Some((10, 0)),
        RockyType::Int64 => Some((19, 0)),
        RockyType::Decimal { precision, scale } => {
            let precision = u32::from(*precision);
            let scale = u32::from(*scale);
            if precision == 0 || scale > precision {
                return None;
            }
            Some((precision - scale, scale))
        }
        _ => None,
    }
}

/// Validate a user-written decimal precision/scale pair before narrowing it to
/// the `u8` fields of [`RockyType::Decimal`].
///
/// sqlparser hands back `Precision(u64)` and `PrecisionAndScale(u64, i64)` — the
/// scale is *signed*, and the parser accepts a leading `-`. Casting either
/// straight to `u8` wraps silently (`DECIMAL(300,2)` became `DECIMAL(44,2)`,
/// `DECIMAL(10,-2)` became `DECIMAL(10,254)`), which changes what the
/// declaration means. This is the single predicate all three of Rocky's decimal
/// parsers share:
///
/// ```text
/// valid(p, s)  ⟺  1 ≤ p ≤ MAX_DECLARED_PRECISION  ∧  0 ≤ s ≤ p
/// ```
///
/// Returns `None` when the pair is invalid; the caller degrades to
/// [`RockyType::Unknown`] (and, where it has a channel, emits a diagnostic).
///
/// Per-adapter bounds tighter than this (Snowflake's `s ≤ 37`, BigQuery
/// `NUMERIC`'s `s ≤ 9`) are deliberately *not* enforced: the target adapter is
/// not reachable at typecheck time, and enforcing the strictest bound would
/// false-reject declarations that are legal on three of the five adapters.
pub fn validate_decimal_params(precision: u64, scale: i64) -> Option<(u8, u8)> {
    if !(1..=u64::from(MAX_DECLARED_PRECISION)).contains(&precision) {
        return None;
    }
    let scale = u64::try_from(scale).ok()?;
    if scale > precision {
        return None;
    }
    // Both provably fit u8 now (precision ≤ 76, scale ≤ precision); `try_from`
    // states that rather than asserting it with a cast.
    Some((u8::try_from(precision).ok()?, u8::try_from(scale).ok()?))
}

/// Compute the result type of an arithmetic operation on exact numerics.
///
/// # The governing position
///
/// **Rocky's inferred decimal type is a sound upper bound on the exact
/// mathematical result. It is not a prediction of what the target warehouse
/// will produce.** The two are different targets, and measurement forces the
/// choice: DuckDB types `DECIMAL(10,2) * DECIMAL(10,2)` as `DECIMAL(18,4)`,
/// which has fourteen integer digits where the exact product needs sixteen, and
/// DuckDB's own `DECIMAL(10,0) + DECIMAL(10,10)` result type cannot hold its own
/// inputs (it raises a range error at run time). An over-wide inferred type only
/// ever produces a false *reject* at a contract gate, which is recoverable; a
/// too-narrow one produces a false *permit*, which is not.
///
/// # Rules
///
/// With operands reduced to `(i1, s1)` and `(i2, s2)` by [`decimal_parts_of`]:
///
/// | operator | result integer digits | result scale | derivation |
/// |---|---|---|---|
/// | `+` `-` | `max(i1,i2) + 1` | `max(s1,s2)` | `\|a±b\| < 2·10^max(i1,i2) ≤ 10^(max+1)` |
/// | `*` | `i1 + i2` | `s1 + s2` | `\|a·b\| < 10^i1 · 10^i2`; the exact product has exactly `s1+s2` fractional digits |
/// | `%` | `min(i1,i2)` | `max(s1,s2)` | `\|a mod b\| < \|b\|` and `≤ \|a\|`; `a - b·trunc(a/b)` has scale at most `max(s1,s2)` |
/// | `/` | — | — | [`RockyType::Unknown`], always — see below |
///
/// Each is a theorem about exact decimal arithmetic, not a vendor average, so
/// it holds on every engine. All four are symmetric in their operands, so the
/// *result type* of `a op b` equals that of `b op a` even where the values
/// differ.
///
/// # Why division is always `Unknown`
///
/// The exact quotient of two decimals is generally non-terminating (`1/3`), so
/// there is no bound to derive. Every engine instead invents a rounding policy
/// and they all disagree: Spark `DECIMAL(23,13)`, Snowflake `NUMBER(18,8)`,
/// Trino `DECIMAL(10,2)`, BigQuery unparameterised `NUMERIC`, DuckDB `DOUBLE` —
/// a different type *family*. The target adapter is not reachable at typecheck
/// time, so any concrete choice would be wrong on at least four of five.
///
/// # Scope
///
/// The operator-specific calculus applies **only** when at least one operand is
/// [`RockyType::Decimal`] and both reduce via [`decimal_parts_of`]. Every other
/// operand pair — integer/integer, float on either side, string, `Unknown` —
/// keeps the pre-existing [`common_supertype`] behaviour.
///
/// For a float operand that is also the sound answer: all five engines return a
/// float, division included. **For integer/integer it is not.** `Int32 + Int32`
/// yields `Int32`, which overflows; `Int64 * Int64` likewise; and `Int64 /
/// Int64` yields `Int64`, where the exact quotient of `1 / 2` is not an integer
/// at all. Those are genuine under-bounds, unchanged by this function and
/// deliberately out of its scope: they are not part of the decimal finding, no
/// live defect depends on them (arithmetic types do not reach
/// `ModelIr::typed_columns` — lineage extraction has no `BinaryOp` arm), and
/// widening them flips a second class of results. Whoever closes them should
/// extend the exact-numeric rules above to integer pairs rather than special-case
/// them. `test_arithmetic_result_is_an_upper_bound` states the same boundary
/// executably by excluding integer/integer pairs.
///
/// An `Unknown` operand paired with a `Decimal` yields `Unknown`: an unbounded
/// operand cannot be bounded. (That differs from [`common_supertype`], where
/// `Unknown` is correctly the bottom element — the two families differ.)
pub fn arithmetic_result_type(op: DecimalOp, a: &RockyType, b: &RockyType) -> RockyType {
    // Not a decimal expression: keep the pre-existing unification behaviour.
    if !a.is_decimal() && !b.is_decimal() {
        return common_supertype(a, b).unwrap_or(RockyType::Unknown);
    }
    // Float on either side: every supported engine returns a float here
    // (division included), so unification is both the agreed and the sound
    // answer. Routed through `common_supertype`'s Float↔Decimal arm.
    if a.is_float() || b.is_float() {
        return common_supertype(a, b).unwrap_or(RockyType::Unknown);
    }
    let (Some((i1, s1)), Some((i2, s2))) = (decimal_parts_of(a), decimal_parts_of(b)) else {
        return RockyType::Unknown;
    };

    match op {
        DecimalOp::Add | DecimalOp::Subtract => {
            let Some(int_digits) = i1.max(i2).checked_add(1) else {
                return RockyType::Unknown;
            };
            decimal_from_parts(int_digits, s1.max(s2))
        }
        DecimalOp::Multiply => {
            let (Some(int_digits), Some(scale)) = (i1.checked_add(i2), s1.checked_add(s2)) else {
                return RockyType::Unknown;
            };
            decimal_from_parts(int_digits, scale)
        }
        DecimalOp::Modulo => decimal_from_parts(i1.min(i2), s1.max(s2)),
        DecimalOp::Divide => RockyType::Unknown,
    }
}

/// Compute the common supertype of two types (for COALESCE, CASE, GREATEST,
/// LEAST, and join-key compatibility).
///
/// This is **unification**, not arithmetic: it answers "what type holds values
/// from both branches". There is no carry digit and no scale addition —
/// `COALESCE(a, b)` over two `DECIMAL(10,2)` columns is `DECIMAL(10,2)`, where
/// `a + b` is `DECIMAL(11,2)`. For the arithmetic family see
/// [`arithmetic_result_type`].
///
/// For the exact-numeric pairs, unification is `max` of the integer digits and
/// `max` of the scales, computed in `(integer_digits, scale)` space so integer
/// digits are never dropped. When the unified type would exceed
/// [`MAX_INFERRED_PRECISION`] the result is `Some(RockyType::Unknown)`, not a
/// scale-reduced `Decimal(38, …)`: Spark and Trino clamp there, but a clamped
/// type cannot hold values from the higher-scale branch, which is a *lossy*
/// unification.
///
/// Returns `None` if the types are incompatible.
pub fn common_supertype(a: &RockyType, b: &RockyType) -> Option<RockyType> {
    if a == b {
        return Some(a.clone());
    }

    // Unknown is compatible with anything
    if *a == RockyType::Unknown {
        return Some(b.clone());
    }
    if *b == RockyType::Unknown {
        return Some(a.clone());
    }

    // Numeric promotion
    match (a, b) {
        // Int32 widens to Int64
        (RockyType::Int32, RockyType::Int64) | (RockyType::Int64, RockyType::Int32) => {
            Some(RockyType::Int64)
        }
        // Float32 widens to Float64
        (RockyType::Float32, RockyType::Float64) | (RockyType::Float64, RockyType::Float32) => {
            Some(RockyType::Float64)
        }
        // Int → Float
        (i, RockyType::Float64) | (RockyType::Float64, i) if i.is_integer() => {
            Some(RockyType::Float64)
        }
        (i, RockyType::Float32) | (RockyType::Float32, i) if i.is_integer() => {
            Some(RockyType::Float64) // promote to Float64 for safety
        }
        // Int ⊔ Decimal and Decimal ⊔ Decimal.
        //
        // Both promote to `(integer_digits, scale)` first and unify by `max` on
        // each independently. The previous rule took `max` of the *precisions*,
        // which does not preserve integer digits: `Decimal(10,0) ⊔
        // Decimal(10,10)` yielded `Decimal(10,10)` — a type with zero integer
        // digits, which cannot hold either operand.
        (RockyType::Int32 | RockyType::Int64, RockyType::Decimal { .. })
        | (RockyType::Decimal { .. }, RockyType::Int32 | RockyType::Int64)
        | (RockyType::Decimal { .. }, RockyType::Decimal { .. }) => {
            let (Some((i1, s1)), Some((i2, s2))) = (decimal_parts_of(a), decimal_parts_of(b))
            else {
                // A malformed decimal (scale > precision) cannot be reasoned
                // about; `Unknown` is the conservative unification, and keeping
                // it `Some` avoids turning a malformed type into a hard
                // join-key incompatibility error.
                return Some(RockyType::Unknown);
            };
            Some(decimal_from_parts(i1.max(i2), s1.max(s2)))
        }
        // Float ⊔ Decimal → Float64.
        //
        // Without this arm the pair is *incompatible*, and `check_join_keys`
        // turns that into a hard E001 on a join whose key is DOUBLE upstream in
        // one model and DECIMAL in another — a join every engine resolves fine
        // as a float. Note this does not widen `is_assignable`, which no longer
        // consults this function: `Decimal → Float64` stays false because a
        // `DECIMAL(38,2)` does not fit exactly in a 53-bit mantissa.
        (RockyType::Float32 | RockyType::Float64, RockyType::Decimal { .. })
        | (RockyType::Decimal { .. }, RockyType::Float32 | RockyType::Float64) => {
            Some(RockyType::Float64)
        }
        // Timestamp variants
        (RockyType::Timestamp, RockyType::TimestampNtz)
        | (RockyType::TimestampNtz, RockyType::Timestamp) => Some(RockyType::Timestamp),

        _ => None,
    }
}

/// Unify a **set** of branch types — `COALESCE`, `CASE`, `GREATEST`, `LEAST`.
///
/// Order-independent by construction: every step is a function of the multiset
/// (`all` / `any` / `max`), never of an accumulator, so permuting the branches
/// cannot change the result. Pairwise reduction cannot achieve that once
/// [`common_supertype`] has a cap: an intermediate that exceeds
/// [`MAX_INFERRED_PRECISION`] is `Unknown`, and `Unknown` is the *bottom*
/// element, so the next branch replaces it instead of absorbing it. Measured on
/// the pairwise fold this replaces:
///
/// ```text
/// COALESCE(DEC(30,0), DEC(30,30), DEC(5,2))  =>  DECIMAL(5,2)
/// COALESCE(DEC(5,2),  DEC(30,0), DEC(30,30)) =>  ?
/// ```
///
/// Three integer digits where the first branch alone needs thirty, and a
/// different answer per argument order.
///
/// # What it guarantees, and what it does not
///
/// For a branch set that is entirely exact-numeric, the result contains **every**
/// branch (`max_i ≥ i_k` and `max_s ≥ s_k` for all k) or is
/// [`RockyType::Unknown`], which claims nothing. Order-independence holds for
/// **every** input; containment holds for **exact-numeric sets only**. When any
/// branch is a float the result is `Float64`, which cannot hold a
/// `DECIMAL(30,30)` exactly — the pre-existing deliberate lossy arm that
/// [`common_supertype`] already had, and every engine's answer for that
/// expression. It is not a containment claim, and
/// [`contract_verdict`]`(Decimal, Float64)` is [`ContractVerdict::Violated`], so
/// it cannot become a false permit at a gate.
///
/// `Unknown` branches are dropped before unifying: an unresolved branch carries
/// no constraint, and dropping it is what makes its *position* irrelevant. A set
/// with no known branch is `Unknown`.
pub fn common_supertype_of(branches: &[RockyType]) -> RockyType {
    let known: Vec<&RockyType> = branches
        .iter()
        .filter(|t| **t != RockyType::Unknown)
        .collect();
    let Some(first) = known.first() else {
        return RockyType::Unknown;
    };
    if known.iter().all(|t| *t == *first) {
        // Set-wide form of `common_supertype`'s `a == b` short circuit: the
        // unification of one repeated type is that type, whatever it is.
        return (*first).clone();
    }
    if known.iter().all(|t| t.is_numeric()) {
        if known.iter().any(|t| t.is_float()) {
            // Matches every pairwise float arm: Float32 ⊔ Float64, Int ⊔ Float,
            // and Float ⊔ Decimal all resolve to Float64.
            return RockyType::Float64;
        }
        if known.iter().all(|t| t.is_integer()) {
            // Not all equal, so this is Int32 ⊔ Int64.
            return RockyType::Int64;
        }
        let (mut max_int_digits, mut max_scale) = (0u32, 0u32);
        for ty in &known {
            let Some((int_digits, scale)) = decimal_parts_of(ty) else {
                // A malformed decimal cannot be reasoned about, and unlike the
                // pairwise fold there is no accumulator for the next branch to
                // overwrite it in.
                return RockyType::Unknown;
            };
            max_int_digits = max_int_digits.max(int_digits);
            max_scale = max_scale.max(scale);
        }
        // `Unknown` exactly when no representable common decimal exists.
        return decimal_from_parts(max_int_digits, max_scale);
    }
    // Outside the numeric families the only cross-type unification
    // `common_supertype` performs is the timestamp pair; every other mix is
    // incompatible, which the pairwise fold also reported as `Unknown` (via
    // `try_fold`'s `None` short circuit).
    if known
        .iter()
        .all(|t| matches!(t, RockyType::Timestamp | RockyType::TimestampNtz))
    {
        return RockyType::Timestamp;
    }
    RockyType::Unknown
}

/// The verdict of a **governance** type check — a contract gate, not inference.
///
/// Three outcomes, not two, because "unresolved" is not "compatible".
/// [`RockyType::Unknown`] is a valid conservative *inference* result, but at a
/// validation gate an unresolved type is unresolved *evidence*, and evidence
/// that has not been produced must not read as evidence of safety.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContractVerdict {
    /// Every value of the landed type is representable in the declared type.
    Satisfied,
    /// Some value of the landed type is not representable in the declared type.
    Violated,
    /// At least one side is [`RockyType::Unknown`], so containment cannot be
    /// decided. **Not a permit.** How a gate acts on it is [`UnverifiablePolicy`].
    Unverifiable,
}

/// How a governance gate acts on [`ContractVerdict::Unverifiable`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnverifiablePolicy {
    /// Report the diagnostic and permit. The migration phase.
    Warn,
    /// Report the diagnostic and reject.
    Reject,
}

/// **THE MIGRATION SWITCH. Changing this one line is the entire flip.**
///
/// `Warn` → `Reject` makes an unresolved type at a contract gate fail closed.
/// Nothing else changes: both gates already compute the
/// [`ContractVerdict::Unverifiable`] verdict, already thread this policy, and
/// already emit their diagnostic under both arms. Flipping it turns `W032` into
/// `E037` at the compiler contract gate, and into a `ContractViolation` with
/// rule `required_column_type_unverifiable` at the `rocky load` gate. Both arms
/// are exercised by tests today (each gate's `*_with_policy` entry point takes
/// the policy as an argument), so the rejecting behaviour is covered before it
/// ships.
///
/// # When it flips
///
/// **Targeted for engine v1.69.0** — one minor release after the v1.68.0 that
/// introduces it. This date is the forcing function: a warning that never
/// becomes an error trains people to ignore it, at which point the gate is a
/// permit with extra text. If v1.69.0 ships with this still on `Warn`, that is
/// a decision someone has to make on the record, not a default that holds by
/// nobody remembering.
///
/// The flip is one line here, plus two documentation surfaces that become
/// *false* rather than merely incomplete when it lands, and are therefore
/// deliberately left alone until then (both are `schemars` descriptions, so
/// editing them requires a `just codegen` cascade that does not belong in the
/// change that only adds the warning):
/// - `rocky_core::contracts::RequiredColumn::data_type` — *"A type the
///   normalizer doesn't recognize is treated as unknown and never fails the
///   type check"*.
/// - the `CHANGELOG` entry that announced the warning.
///
/// # Three known gaps that should be closed before, or with, the flip
///
/// All three are pre-existing and none is a *new* fail-open, but the flip is the
/// natural moment to settle them, because after it the gates reject and a gap
/// that merely under-warns today starts to look like an arbitrary exemption:
///
/// - **Every project that compiles without source schemas fails its contracts
///   at the flip.** With `[cache.schemas]` disabled or a cold cache the
///   compiler's `source_schemas` is empty, so no column in any model resolves;
///   `rocky_compiler::contracts` reports that as one collapsed diagnostic per
///   contracted model, which becomes one **error** per contracted model here.
///   Measured: every POC in this repository is in that state, every column. That
///   is the correct fail-closed reading — a contract cannot be checked against
///   evidence that was never produced — and it is a much larger migration than a
///   per-column warning suggests. Closing it means making a schema cache
///   reliably available to `rocky compile`, or offering an explicit opt-out that
///   is itself recorded.
/// - `rocky_compiler::contracts::type_name_matches` accepts a declared
///   `Decimal(p,s)` but the compile-time check never enforces `p`/`s` — any
///   decimal satisfies any `Decimal(…)` declaration there. Closing it turns
///   currently-clean contracts into `E011` errors.
/// - The Databricks source-discovery producers hand Rocky a bare `decimal`
///   (`information_schema.columns.data_type` is the simple type name;
///   `full_data_type` carries the parameters), so Databricks decimal source
///   columns resolve to `Unknown` and will be *rejected* at the flip. Fixing the
///   producer is the right repair; it needs a live Databricks to verify.
pub const UNVERIFIABLE_POLICY: UnverifiablePolicy = UnverifiablePolicy::Warn;

/// Decide whether a landed/inferred type satisfies a declared contract type.
///
/// This is the **gate** predicate. [`is_assignable`] is the *inference-side*
/// containment predicate and keeps `Unknown`-passes-everything, which is correct
/// there and a false permit here: degrading a type to [`RockyType::Unknown`] at a
/// validation gate is a fail-*open*, because `Unknown` satisfies
/// [`is_assignable`] unconditionally in both directions.
///
/// Off the `Unknown` axis the two agree exactly —
/// `contract_verdict(a, b) == Satisfied ⟺ is_assignable(a, b)` for every pair of
/// known types (`test_contract_verdict_agrees_with_is_assignable_off_unknown`),
/// so this splits one axis and freezes every other verdict.
pub fn contract_verdict(landed: &RockyType, declared: &RockyType) -> ContractVerdict {
    if *landed == RockyType::Unknown || *declared == RockyType::Unknown {
        return ContractVerdict::Unverifiable;
    }
    if is_assignable(landed, declared) {
        ContractVerdict::Satisfied
    } else {
        ContractVerdict::Violated
    }
}

/// Check if a value of type `from` can be assigned to a column of type `to`.
///
/// This is a **containment** predicate — *can every value of `from` be
/// represented in `to`* — and it is deliberately independent of
/// [`common_supertype`]. It used to be defined as
/// `common_supertype(from, to) == to`, which is a proxy that fails exactly where
/// scale matters, and produced five false permits at the `rocky load` contract
/// gate (`Int32 → Decimal(10,2)`, `Int64 → Decimal(19,2)`,
/// `Decimal(10,0) → Decimal(10,10)`, `Decimal(12,2) → Decimal(12,4)`,
/// `Decimal(38,0) → Decimal(38,2)`). Splitting it also decouples the gate from
/// this module's `Some(Unknown)` cap behaviour, which under the old definition
/// would have turned a capped unification into a spurious violation.
///
/// For an exact-numeric source and a `Decimal` target, containment is checked in
/// `(integer_digits, scale)` space: the source's integer digits must fit the
/// target's, *and* the source's scale must fit the target's. A malformed decimal
/// (scale > precision, or zero precision) on either side is not assignable —
/// fail-closed on a governance predicate.
///
/// # Exactly where the containment claim holds, and where it does not
///
/// The claim above is **not** true of this function in general. Two arms are
/// deliberately retained exceptions, both pre-existing and both outside the
/// decimal finding this rewrite closes. They are named here rather than left for
/// a reader to discover:
///
/// - **`Int64`/`Int32`/`Float32` → `Float64` is a genuine false permit.**
///   `9007199254740993` (`2^53 + 1`) is an `i64` that binary64 cannot represent
///   exactly, so `Int64 → Float64` claims an assignment that loses information.
///   The arm is kept because this rewrite freezes every non-decimal verdict (see
///   `test_is_assignable_verdicts_preserved_for_non_decimals`) so the change has
///   one flip surface rather than two; removing it flips a second, unrelated
///   class of contracts. `test_is_assignable_known_float_widening_exception`
///   pins it with the counterexample so it cannot be mistaken for an oversight.
/// - **Identical types short-circuit before validity is checked**, so a
///   malformed `Decimal(5,10)` is assignable to itself. That is trivially sound
///   — assigning a type to itself loses nothing whatever the type — but it does
///   mean "malformed is never assignable" holds only for *distinct* types.
///
/// Every other non-decimal pair also reproduces the previous verdict exactly,
/// pinned against a frozen copy of the old rule over the full cross-product of
/// non-decimal types.
///
/// # `Unknown` passes here, and that is why this must not be used at a gate
///
/// [`RockyType::Unknown`] on either side is assignable, which is a **false
/// permit** at a validation gate: unresolved evidence would read as evidence of
/// safety. Governance call sites must use [`contract_verdict`], which reports
/// [`ContractVerdict::Unverifiable`] for the same inputs.
///
/// Stated precisely, because a grep will otherwise contradict a looser claim:
/// after the gate split this function has **no callers outside this module** —
/// `contract_verdict` and the tests are the only consumers, and the two agree
/// everywhere except the `Unknown` axis. The permissive behaviour is retained
/// because it is this predicate's own long-standing contract and because
/// freezing it keeps the change to one flip surface
/// (`test_is_assignable_verdicts_preserved_for_non_decimals`), **not** because
/// some inference call site depends on it — there is no such call site today.
/// A future inference consumer is exactly who this contract is for; a future
/// governance consumer must not use it.
pub fn is_assignable(from: &RockyType, to: &RockyType) -> bool {
    if from == to || *from == RockyType::Unknown || *to == RockyType::Unknown {
        return true;
    }

    match (from, to) {
        // Exact numeric → Decimal: direct containment.
        (_, RockyType::Decimal { .. }) if from.is_integer() || from.is_decimal() => {
            let (Some((from_int, from_scale)), Some((to_int, to_scale))) =
                (decimal_parts_of(from), decimal_parts_of(to))
            else {
                return false;
            };
            from_int <= to_int && from_scale <= to_scale
        }
        // Int32 widens to Int64.
        (RockyType::Int32, RockyType::Int64) => true,
        // Pre-existing lossy widening into Float64. NOT exact containment:
        // 2^53 + 1 is an i64 binary64 cannot hold. Retained deliberately so this
        // change has one flip surface; see the doc comment above.
        (RockyType::Int32 | RockyType::Int64 | RockyType::Float32, RockyType::Float64) => true,
        // A timezone-naive timestamp is assignable to a zoned one.
        (RockyType::TimestampNtz, RockyType::Timestamp) => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_same_types() {
        assert_eq!(
            common_supertype(&RockyType::Int64, &RockyType::Int64),
            Some(RockyType::Int64)
        );
    }

    #[test]
    fn test_int_widening() {
        assert_eq!(
            common_supertype(&RockyType::Int32, &RockyType::Int64),
            Some(RockyType::Int64)
        );
    }

    #[test]
    fn test_float_widening() {
        assert_eq!(
            common_supertype(&RockyType::Float32, &RockyType::Float64),
            Some(RockyType::Float64)
        );
    }

    #[test]
    fn test_int_to_float() {
        assert_eq!(
            common_supertype(&RockyType::Int64, &RockyType::Float64),
            Some(RockyType::Float64)
        );
    }

    /// Shorthand for a decimal literal in the tables below.
    fn dec(precision: u8, scale: u8) -> RockyType {
        RockyType::Decimal { precision, scale }
    }

    // ---------------------------------------------------------------------
    // T1 — `arithmetic_result_type`
    // ---------------------------------------------------------------------

    /// The full arithmetic matrix: add, subtract, multiply, divide, modulo,
    /// scale growth, integer promotion, and overflow past the inference cap.
    ///
    /// Every row is a *derived* bound on the exact mathematical result, not a
    /// copy of any engine's formula — see `arithmetic_result_type`.
    #[test]
    fn test_arithmetic_result_matrix() {
        use DecimalOp::{Add, Divide, Modulo, Multiply, Subtract};

        let cases: &[(&str, RockyType, DecimalOp, RockyType, RockyType)] = &[
            // T1.1/T1.2 — add and subtract grow one integer digit for the carry.
            ("T1.1 add same", dec(10, 2), Add, dec(10, 2), dec(11, 2)),
            (
                "T1.2 sub same",
                dec(10, 2),
                Subtract,
                dec(10, 2),
                dec(11, 2),
            ),
            // T1.3 — scale growth that PRESERVES integer digits. The old
            // max-precision/max-scale rule returned Decimal(10,10) here: zero
            // integer digits, unable to hold either operand.
            (
                "T1.3 scale growth",
                dec(10, 0),
                Add,
                dec(10, 10),
                dec(21, 10),
            ),
            // T1.4 — multiply adds integer digits and adds scales.
            (
                "T1.4 mul same",
                dec(10, 2),
                Multiply,
                dec(10, 2),
                dec(20, 4),
            ),
            // T1.5 — multiply past the cap degrades rather than truncating.
            (
                "T1.5 mul overflow",
                dec(30, 2),
                Multiply,
                dec(30, 2),
                RockyType::Unknown,
            ),
            // T1.6 — divide is always Unknown for exact numerics.
            (
                "T1.6 div",
                dec(10, 2),
                Divide,
                dec(10, 2),
                RockyType::Unknown,
            ),
            // T1.7/T1.8 — modulo takes min integer digits, max scale.
            ("T1.7 mod same", dec(10, 2), Modulo, dec(10, 2), dec(10, 2)),
            (
                "T1.8 mod mixed",
                dec(20, 10),
                Modulo,
                dec(10, 2),
                dec(18, 10),
            ),
            // T1.9/T1.10 — integer promotion: Int32 → 10 digits, Int64 → 19.
            (
                "T1.9 int32 add",
                RockyType::Int32,
                Add,
                dec(10, 2),
                dec(13, 2),
            ),
            (
                "T1.10 int64 add",
                RockyType::Int64,
                Add,
                dec(10, 2),
                dec(22, 2),
            ),
            (
                "T1.11 int64 mul",
                RockyType::Int64,
                Multiply,
                dec(10, 2),
                dec(29, 2),
            ),
            (
                "T1.12 int64 mul overflow",
                RockyType::Int64,
                Multiply,
                dec(30, 8),
                RockyType::Unknown,
            ),
            // T1.13 — add overflow: 38 integer digits + carry = 39 > cap.
            (
                "T1.13 add overflow",
                dec(38, 0),
                Add,
                dec(1, 0),
                RockyType::Unknown,
            ),
            // T1.14 — exactly at the cap must NOT degrade.
            (
                "T1.14 mul at cap",
                dec(19, 0),
                Multiply,
                dec(19, 0),
                dec(38, 0),
            ),
            // T1.15 — operands with ZERO integer digits. Guards the boundary
            // where `precision - scale` is 0: the carry digit must still be
            // added, and the result must not come out with precision 0.
            // (T1.3 is the canary for doing the whole calculus in (p,s) space:
            // `max(p)+1` there would give Decimal(11,10) — one integer digit.)
            (
                "T1.15 zero int digits",
                dec(1, 1),
                Add,
                dec(1, 1),
                dec(2, 1),
            ),
            (
                "T1.15b zero int digits mul",
                dec(1, 1),
                Multiply,
                dec(1, 1),
                dec(2, 2),
            ),
            // T1.17 — a float operand keeps the float answer, division included.
            (
                "T1.17 float add",
                RockyType::Float64,
                Add,
                dec(10, 2),
                RockyType::Float64,
            ),
            (
                "T1.17 float div",
                RockyType::Float64,
                Divide,
                dec(10, 2),
                RockyType::Float64,
            ),
            (
                "T1.17 float32 mul",
                RockyType::Float32,
                Multiply,
                dec(10, 2),
                RockyType::Float64,
            ),
            // T1.18 — a non-numeric operand yields Unknown.
            (
                "T1.18 string",
                RockyType::String,
                Add,
                dec(10, 2),
                RockyType::Unknown,
            ),
            // An Unknown operand cannot be bounded, so the result is Unknown —
            // this differs from `common_supertype`, where Unknown is bottom.
            (
                "unknown operand",
                RockyType::Unknown,
                Add,
                dec(10, 2),
                RockyType::Unknown,
            ),
            // A malformed decimal (scale > precision) degrades safely.
            (
                "malformed decimal",
                dec(5, 10),
                Add,
                dec(10, 2),
                RockyType::Unknown,
            ),
            // T1.19 — integer division is UNCHANGED. Pins the scope boundary:
            // `Divide → Unknown` applies only when an operand is Decimal.
            (
                "T1.19 int div unchanged",
                RockyType::Int64,
                Divide,
                RockyType::Int64,
                RockyType::Int64,
            ),
            (
                "int add unchanged",
                RockyType::Int32,
                Add,
                RockyType::Int64,
                RockyType::Int64,
            ),
        ];

        for (label, a, op, b, expected) in cases {
            assert_eq!(
                &arithmetic_result_type(*op, a, b),
                expected,
                "{label}: {a} {op:?} {b}"
            );
        }
    }

    /// T1.16 — the result type is symmetric in its operands for `+`, `-`, `*`
    /// and `%` alike (`max`, `min` and `+` all are). This is a property of the
    /// *type*, not of the operation: `a - b ≠ b - a` numerically, but both land
    /// in the same type.
    #[test]
    fn test_arithmetic_result_is_operand_symmetric() {
        let operands = [
            dec(10, 2),
            dec(10, 0),
            dec(10, 10),
            dec(1, 1),
            dec(38, 0),
            dec(30, 2),
            RockyType::Int32,
            RockyType::Int64,
            RockyType::Float64,
            RockyType::String,
            RockyType::Unknown,
        ];
        let ops = [
            DecimalOp::Add,
            DecimalOp::Subtract,
            DecimalOp::Multiply,
            DecimalOp::Modulo,
            DecimalOp::Divide,
        ];

        for op in ops {
            for a in &operands {
                for b in &operands {
                    assert_eq!(
                        arithmetic_result_type(op, a, b),
                        arithmetic_result_type(op, b, a),
                        "asymmetric: {a} {op:?} {b}"
                    );
                }
            }
        }
    }

    /// Every concrete decimal this function returns must be able to hold the
    /// exact result. Checked structurally: for add/subtract/multiply/modulo the
    /// emitted `(integer_digits, scale)` must be at least the derived bound.
    ///
    /// **Scope of the claim, stated exactly.** This holds only for pairs where
    /// at least one operand is a `Decimal`. Integer/integer pairs are excluded
    /// because they keep the pre-existing `common_supertype` behaviour, which is
    /// *not* an upper bound (`Int32 + Int32` stays `Int32`, which overflows).
    /// That is a separate finding with no live defect behind it, deliberately
    /// out of this change's scope.
    #[test]
    fn test_arithmetic_result_is_an_upper_bound() {
        let operands = [
            dec(1, 0),
            dec(1, 1),
            dec(5, 2),
            dec(10, 0),
            dec(10, 2),
            dec(10, 10),
            dec(19, 0),
            dec(38, 0),
            dec(38, 38),
            RockyType::Int32,
            RockyType::Int64,
        ];

        for a in &operands {
            for b in &operands {
                if !a.is_decimal() && !b.is_decimal() {
                    continue;
                }
                let (i1, s1) = decimal_parts_of(a).expect("test operand is well formed");
                let (i2, s2) = decimal_parts_of(b).expect("test operand is well formed");
                let required = [
                    (DecimalOp::Add, i1.max(i2) + 1, s1.max(s2)),
                    (DecimalOp::Subtract, i1.max(i2) + 1, s1.max(s2)),
                    (DecimalOp::Multiply, i1 + i2, s1 + s2),
                    (DecimalOp::Modulo, i1.min(i2), s1.max(s2)),
                ];
                for (op, need_int, need_scale) in required {
                    match arithmetic_result_type(op, a, b) {
                        RockyType::Unknown => {} // conservative degradation is always allowed
                        RockyType::Decimal { precision, scale } => {
                            let (got_int, got_scale) =
                                decimal_parts_of(&dec(precision, scale)).expect("well formed");
                            assert!(
                                got_int >= need_int && got_scale >= need_scale,
                                "{a} {op:?} {b} inferred DECIMAL({precision},{scale}) \
                                 = ({got_int} int, {got_scale} scale), needs \
                                 ({need_int} int, {need_scale} scale)"
                            );
                        }
                        other => panic!("{a} {op:?} {b} produced non-decimal {other}"),
                    }
                }
            }
        }
    }

    // ---------------------------------------------------------------------
    // T2 — `common_supertype` unification
    // ---------------------------------------------------------------------

    /// T2.2 — replaces the old `test_int_to_decimal`, which pinned the bug
    /// (`Int64 ⊔ Decimal(10,2)` used to yield `Decimal(19,2)`: nineteen digits
    /// of *precision*, only seventeen of which are integer digits, so it cannot
    /// hold `i64::MAX`).
    #[test]
    fn test_unification_matrix() {
        let cases: &[(&str, RockyType, RockyType, Option<RockyType>)] = &[
            // T2.1 — scale growth preserving integer digits.
            ("T2.1", dec(10, 0), dec(10, 10), Some(dec(20, 10))),
            // T2.2 — Int64 needs 19 integer digits, so with scale 2 it needs 21.
            ("T2.2", RockyType::Int64, dec(10, 2), Some(dec(21, 2))),
            // T2.3 — Int32 needs 10 integer digits.
            ("T2.3", RockyType::Int32, dec(5, 2), Some(dec(12, 2))),
            ("T2.4", dec(12, 2), dec(12, 4), Some(dec(14, 4))),
            // T2.5 — arm-0 regression guard: identical types are unchanged, and
            // unification adds NO carry digit (that belongs to arithmetic).
            ("T2.5", dec(10, 2), dec(10, 2), Some(dec(10, 2))),
            // T2.6 — cap overflow is Some(Unknown), NOT None and NOT a lossy
            // scale-reduced Decimal(38, 8) the way Spark/Trino clamp.
            ("T2.6", dec(30, 0), dec(30, 30), Some(RockyType::Unknown)),
            // T2.7 — the new Float↔Decimal arm.
            (
                "T2.7a",
                RockyType::Float64,
                dec(10, 2),
                Some(RockyType::Float64),
            ),
            (
                "T2.7b",
                RockyType::Float32,
                dec(10, 2),
                Some(RockyType::Float64),
            ),
            // T2.8 — genuinely incompatible stays None.
            ("T2.8", RockyType::String, dec(10, 2), None),
            // A malformed decimal unifies to Unknown rather than None, so it
            // never becomes a hard join-key incompatibility error.
            (
                "malformed",
                dec(5, 10),
                dec(10, 2),
                Some(RockyType::Unknown),
            ),
        ];

        for (label, a, b, expected) in cases {
            assert_eq!(&common_supertype(a, b), expected, "{label}: {a} ⊔ {b}");
        }
    }

    /// T2.9 — unification is symmetric over every numeric pair.
    #[test]
    fn test_unification_is_symmetric() {
        let operands = [
            dec(10, 2),
            dec(10, 0),
            dec(10, 10),
            dec(30, 30),
            dec(38, 0),
            RockyType::Int32,
            RockyType::Int64,
            RockyType::Float32,
            RockyType::Float64,
            RockyType::String,
            RockyType::Unknown,
        ];
        for a in &operands {
            for b in &operands {
                assert_eq!(
                    common_supertype(a, b),
                    common_supertype(b, a),
                    "asymmetric: {a} ⊔ {b}"
                );
            }
        }
    }

    /// T2.10 — the two families are genuinely distinct: unification has no
    /// carry digit and no scale addition. If someone ever routes arithmetic
    /// back through `common_supertype`, this fails.
    #[test]
    fn test_arithmetic_and_unification_diverge() {
        let a = dec(10, 2);
        assert_eq!(common_supertype(&a, &a), Some(dec(10, 2)));
        assert_eq!(arithmetic_result_type(DecimalOp::Add, &a, &a), dec(11, 2));
        assert_eq!(
            arithmetic_result_type(DecimalOp::Multiply, &a, &a),
            dec(20, 4)
        );
    }

    /// Every concrete decimal `common_supertype` returns must hold values from
    /// *both* branches — that is what unification means.
    #[test]
    fn test_unification_contains_both_branches() {
        let operands = [
            dec(1, 1),
            dec(5, 2),
            dec(10, 0),
            dec(10, 2),
            dec(10, 10),
            dec(38, 0),
            dec(38, 38),
            RockyType::Int32,
            RockyType::Int64,
        ];
        for a in &operands {
            for b in &operands {
                let Some(sup) = common_supertype(a, b) else {
                    panic!("{a} ⊔ {b} unexpectedly incompatible");
                };
                if sup == RockyType::Unknown {
                    continue; // conservative degradation
                }
                assert!(
                    is_assignable(a, &sup) && is_assignable(b, &sup),
                    "{a} ⊔ {b} = {sup} does not contain both branches"
                );
            }
        }
    }

    #[test]
    fn test_incompatible_types() {
        assert_eq!(
            common_supertype(&RockyType::String, &RockyType::Int64),
            None
        );
        assert_eq!(
            common_supertype(&RockyType::Boolean, &RockyType::Date),
            None
        );
    }

    #[test]
    fn test_unknown_compatible_with_all() {
        assert_eq!(
            common_supertype(&RockyType::Unknown, &RockyType::Int64),
            Some(RockyType::Int64)
        );
        assert_eq!(
            common_supertype(&RockyType::String, &RockyType::Unknown),
            Some(RockyType::String)
        );
    }

    // ---------------------------------------------------------------------
    // T2.11–T2.15 — `common_supertype_of`, the set-wide fold
    // ---------------------------------------------------------------------

    /// The pairwise fold the four `typecheck.rs` sites used before
    /// `common_supertype_of`, kept verbatim so the tests below can state what
    /// changed rather than assert it.
    fn pairwise_fold_frozen_old(branches: &[RockyType]) -> RockyType {
        branches
            .iter()
            .try_fold(RockyType::Unknown, |acc, t| common_supertype(&acc, t))
            .unwrap_or(RockyType::Unknown)
    }

    fn permutations(branches: &[RockyType]) -> Vec<Vec<RockyType>> {
        if branches.len() <= 1 {
            return vec![branches.to_vec()];
        }
        let mut out = Vec::new();
        for i in 0..branches.len() {
            let mut rest = branches.to_vec();
            let head = rest.remove(i);
            for mut permutation in permutations(&rest) {
                permutation.insert(0, head.clone());
                out.push(permutation);
            }
        }
        out
    }

    /// The branch sets every fold test runs over. Each is a `COALESCE` /
    /// `CASE` / `GREATEST` argument list that is legal SQL.
    fn fold_corpus() -> Vec<(&'static str, Vec<RockyType>)> {
        vec![
            // The measured regression: pairwise gives DECIMAL(5,2) in one order
            // and `?` in another, and DECIMAL(5,2) has three integer digits
            // where the first branch alone needs thirty.
            (
                "dec30_dec30x30_dec5",
                vec![dec(30, 0), dec(30, 30), dec(5, 2)],
            ),
            // Worse: pairwise gives three different answers, one of them Int64.
            (
                "dec30_dec30x30_int64",
                vec![dec(30, 0), dec(30, 30), RockyType::Int64],
            ),
            // The second B4 trigger — `decimal_parts_of` failing on a malformed
            // decimal, which the pairwise fold absorbs into the accumulator.
            ("malformed_dec", vec![dec(5, 10), dec(10, 2)]),
            // The named float exception (T2.13).
            (
                "float_and_wide_decimals",
                vec![RockyType::Float64, dec(30, 0), dec(30, 30)],
            ),
            // Cap overflow with only two branches.
            ("cap_overflow_pair", vec![dec(20, 0), dec(20, 20)]),
            // Ordinary sets that must keep their existing answers.
            ("widening_decimals", vec![dec(10, 2), dec(12, 4)]),
            (
                "mixed_ints_and_float",
                vec![RockyType::Int32, RockyType::Int64, RockyType::Float32],
            ),
            ("int_and_decimal", vec![RockyType::Int64, dec(10, 2)]),
            ("incompatible", vec![RockyType::String, RockyType::Int64]),
            (
                "timestamps",
                vec![RockyType::Timestamp, RockyType::TimestampNtz],
            ),
            ("unknown_and_known", vec![RockyType::Unknown, dec(10, 2)]),
            ("all_equal", vec![dec(10, 2), dec(10, 2), dec(10, 2)]),
        ]
    }

    /// T2.11 — the fold is a function of the branch *multiset*.
    ///
    /// Measured on the pairwise fold this replaces, over the same corpus:
    /// `{DEC(30,0), DEC(30,30), DEC(5,2)}` yields two distinct results
    /// depending on argument order, and `{DEC(30,0), DEC(30,30), INT64}`
    /// yields three. Same expression, reordered arguments, different type.
    #[test]
    fn test_fold_is_order_independent() {
        for (label, branches) in fold_corpus() {
            let mut results: Vec<RockyType> = permutations(&branches)
                .iter()
                .map(|permutation| common_supertype_of(permutation))
                .collect();
            results.dedup();
            assert_eq!(
                results.len(),
                1,
                "{label}: fold is order-dependent, results = {}",
                results
                    .iter()
                    .map(std::string::ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(" / ")
            );
        }
    }

    /// T2.12 — for an exact-numeric branch set the result contains every
    /// branch, or is `Unknown` (which claims nothing).
    ///
    /// The pairwise fold fails this: `COALESCE(DEC(30,0), DEC(30,30), DEC(5,2))`
    /// typed as `DECIMAL(5,2)`, and `is_assignable(DEC(30,0), DEC(5,2))` is
    /// false — a *confidently wrong* narrower type, the one outcome the WP-03
    /// brief rules out.
    #[test]
    fn test_fold_never_narrows_exact_numerics() {
        for (label, branches) in fold_corpus() {
            let exact_numeric = branches
                .iter()
                .all(|t| t.is_integer() || t.is_decimal() || *t == RockyType::Unknown);
            if !exact_numeric {
                continue;
            }
            let result = common_supertype_of(&branches);
            if result == RockyType::Unknown {
                continue;
            }
            for branch in &branches {
                assert!(
                    is_assignable(branch, &result),
                    "{label}: fold = {result} does not contain branch {branch}"
                );
            }
        }
    }

    /// T2.13 — the float exception, stated executably so it cannot be mistaken
    /// for an oversight: a float branch makes the result `Float64`, which does
    /// **not** contain a high-scale decimal. What stops that being a false
    /// permit is the gate — `contract_verdict` reports `Violated` for exactly
    /// that pair, because `is_assignable(Decimal, Float64)` is false.
    #[test]
    fn test_fold_float_exception_is_named() {
        let result = common_supertype_of(&[RockyType::Float64, dec(30, 30)]);
        assert_eq!(result, RockyType::Float64);
        assert!(!is_assignable(&dec(30, 30), &RockyType::Float64));
        assert_eq!(
            contract_verdict(&dec(30, 30), &RockyType::Float64),
            ContractVerdict::Violated
        );
    }

    /// The blast radius of the fold change, stated as a test rather than as a
    /// claim: outside the cap-overflow and malformed-decimal classes,
    /// `common_supertype_of` returns exactly what the pairwise fold returned.
    #[test]
    fn test_fold_matches_pairwise_outside_the_bug_class() {
        let unchanged = [
            "widening_decimals",
            "mixed_ints_and_float",
            "int_and_decimal",
            "incompatible",
            "timestamps",
            "unknown_and_known",
            "all_equal",
            "float_and_wide_decimals",
            "malformed_dec",
        ];
        for (label, branches) in fold_corpus() {
            if !unchanged.contains(&label) {
                continue;
            }
            for permutation in permutations(&branches) {
                assert_eq!(
                    common_supertype_of(&permutation),
                    pairwise_fold_frozen_old(&permutation),
                    "{label}: fold changed a result outside the bug class"
                );
            }
        }
        // The empty set: no branches, nothing to unify.
        assert_eq!(common_supertype_of(&[]), RockyType::Unknown);
        assert_eq!(
            common_supertype_of(&[RockyType::Unknown, RockyType::Unknown]),
            RockyType::Unknown
        );
    }

    // ---------------------------------------------------------------------
    // G1/G2 — `contract_verdict`, the gate predicate
    // ---------------------------------------------------------------------

    /// G1 — an unresolved type at a gate is `Unverifiable`, in both directions.
    ///
    /// Measured on `is_assignable`, the predicate the `rocky load` gate used
    /// for these same three inputs: `true`, `true`, `true`. That is the
    /// fail-open — degrading a type to `Unknown` at a validation gate made the
    /// gate *more* permissive, not more conservative.
    #[test]
    fn test_contract_verdict_unknown_is_unverifiable() {
        assert_eq!(
            contract_verdict(&RockyType::Unknown, &dec(10, 2)),
            ContractVerdict::Unverifiable
        );
        assert_eq!(
            contract_verdict(&dec(38, 0), &RockyType::Unknown),
            ContractVerdict::Unverifiable
        );
        assert_eq!(
            contract_verdict(&RockyType::Unknown, &RockyType::Unknown),
            ContractVerdict::Unverifiable
        );
        // `is_assignable` keeps its own contract — this is a split, not a
        // change to the inference-side predicate.
        assert!(is_assignable(&RockyType::Unknown, &dec(10, 2)));
        assert!(is_assignable(&dec(38, 0), &RockyType::Unknown));
    }

    /// G2 — off the `Unknown` axis the gate predicate and the inference
    /// predicate agree exactly, over the full cross-product of the scalar
    /// variants plus a decimal grid. The split touches one axis and freezes
    /// every other verdict.
    #[test]
    fn test_contract_verdict_agrees_with_is_assignable_off_unknown() {
        let mut universe = vec![
            RockyType::Boolean,
            RockyType::Int32,
            RockyType::Int64,
            RockyType::Float32,
            RockyType::Float64,
            RockyType::String,
            RockyType::Date,
            RockyType::Timestamp,
            RockyType::TimestampNtz,
            RockyType::Binary,
            RockyType::Variant,
            RockyType::Array(Box::new(RockyType::Int64)),
        ];
        for precision in [1u8, 5, 10, 19, 38, 76] {
            for scale in [0u8, 2, 10, 38] {
                if scale <= precision {
                    universe.push(dec(precision, scale));
                }
            }
        }
        // A malformed decimal on either side must also agree.
        universe.push(dec(5, 10));

        for from in &universe {
            for to in &universe {
                let verdict = contract_verdict(from, to);
                assert_ne!(
                    verdict,
                    ContractVerdict::Unverifiable,
                    "no known pair may be Unverifiable: ({from}, {to})"
                );
                assert_eq!(
                    verdict == ContractVerdict::Satisfied,
                    is_assignable(from, to),
                    "gate predicate diverged from is_assignable off the Unknown axis: \
                     ({from}, {to})"
                );
            }
        }
    }

    #[test]
    fn test_is_assignable_widening() {
        assert!(is_assignable(&RockyType::Int32, &RockyType::Int64));
        assert!(!is_assignable(&RockyType::Int64, &RockyType::Int32));
    }

    /// T3.3 — `Unknown` on either side is assignable. `rocky_core::contracts`
    /// documents this as deliberate ("never a false failure"), so the rewrite
    /// must not change it.
    #[test]
    fn test_is_assignable_unknown() {
        assert!(is_assignable(&RockyType::Unknown, &RockyType::String));
        assert!(is_assignable(&RockyType::Int64, &RockyType::Unknown));
    }

    // ---------------------------------------------------------------------
    // T3 — `is_assignable` containment (the governance surface)
    // ---------------------------------------------------------------------

    /// T3.1 — the decimal containment matrix, including the five permits the
    /// old `common_supertype(from, to) == to` definition got wrong.
    #[test]
    fn test_is_assignable_decimal_containment() {
        let cases: &[(&str, RockyType, RockyType, bool)] = &[
            // Was TRUE: Int32 needs 10 integer digits, the target has 8.
            ("int32 → dec(10,2)", RockyType::Int32, dec(10, 2), false),
            // Was TRUE: Int64 needs 19 integer digits, the target has 17.
            ("int64 → dec(19,2)", RockyType::Int64, dec(19, 2), false),
            // Was FALSE: 19 integer digits fit exactly.
            ("int64 → dec(21,2)", RockyType::Int64, dec(21, 2), true),
            // Was TRUE: ten integer digits reduced to zero.
            ("dec(10,0) → dec(10,10)", dec(10, 0), dec(10, 10), false),
            // Was TRUE: ten integer digits reduced to eight.
            ("dec(12,2) → dec(12,4)", dec(12, 2), dec(12, 4), false),
            // Was TRUE — the Snowflake case. `DESCRIBE` returns NUMBER(38,0)
            // for every Snowflake integer column, and 38 integer digits do not
            // fit a DECIMAL(38,2)'s 36.
            ("dec(38,0) → dec(38,2)", dec(38, 0), dec(38, 2), false),
            // Genuine widening, unchanged.
            ("dec(10,2) → dec(12,4)", dec(10, 2), dec(12, 4), true),
            ("dec(10,2) → dec(10,2)", dec(10, 2), dec(10, 2), true),
            ("int32 → dec(12,2)", RockyType::Int32, dec(12, 2), true),
            ("int32 → dec(10,0)", RockyType::Int32, dec(10, 0), true),
            // Exactness is lost in both float directions — unchanged.
            ("dec(10,2) → float64", dec(10, 2), RockyType::Float64, false),
            ("float64 → dec(10,2)", RockyType::Float64, dec(10, 2), false),
            // A malformed decimal is not assignable (fail-closed).
            ("dec(5,10) → dec(38,10)", dec(5, 10), dec(38, 10), false),
            ("dec(10,2) → dec(5,10)", dec(10, 2), dec(5, 10), false),
        ];

        for (label, from, to, expected) in cases {
            assert_eq!(
                is_assignable(from, to),
                *expected,
                "{label}: is_assignable({from}, {to})"
            );
        }
    }

    /// The `Int → Float64` arm is a **known, deliberately retained false
    /// permit**, not an oversight.
    ///
    /// `2^53 + 1` is an `i64` that binary64 cannot represent exactly, so
    /// `is_assignable(Int64, Float64) == true` claims an assignment that loses
    /// information. It is kept because this change freezes every non-decimal
    /// verdict so it has one flip surface rather than two — removing the arm
    /// flips a second, unrelated class of load contracts (a landed `BIGINT`
    /// against a `DOUBLE` contract). Fixing it is a separate change.
    ///
    /// This test exists so the exception is pinned and greppable. If it starts
    /// failing, someone tightened the arm — that is a *behaviour* change to a
    /// governance predicate and needs its own review, not a test update.
    #[test]
    fn test_is_assignable_known_float_widening_exception() {
        let not_exactly_representable: i64 = 9_007_199_254_740_993; // 2^53 + 1
        assert_ne!(
            not_exactly_representable as f64 as i64, not_exactly_representable,
            "premise of this test: the value survives neither round trip"
        );

        assert!(
            is_assignable(&RockyType::Int64, &RockyType::Float64),
            "known retained false permit — see the doc comment on is_assignable"
        );
        assert!(is_assignable(&RockyType::Int32, &RockyType::Float64));
        assert!(is_assignable(&RockyType::Float32, &RockyType::Float64));
        // The decimal directions are NOT permitted, in either orientation.
        assert!(!is_assignable(&dec(10, 2), &RockyType::Float64));
        assert!(!is_assignable(&RockyType::Float64, &dec(10, 2)));
    }

    /// Identical types short-circuit before decimal validity is checked, so a
    /// malformed decimal is assignable to itself. That is trivially sound —
    /// assigning a type to itself loses nothing — but it bounds the
    /// "malformed is never assignable" rule to *distinct* types, so it is pinned
    /// explicitly rather than left implicit.
    #[test]
    fn test_is_assignable_identity_short_circuit() {
        assert!(is_assignable(&dec(5, 10), &dec(5, 10)));
        assert!(is_assignable(&dec(0, 0), &dec(0, 0)));
        // Distinct malformed pairs are rejected in both directions.
        assert!(!is_assignable(&dec(5, 10), &dec(6, 10)));
        assert!(!is_assignable(&dec(10, 2), &dec(5, 10)));
        assert!(!is_assignable(&dec(5, 10), &dec(10, 2)));
        // Same for unification: `a ⊔ a` is `a` by definition, whatever `a` is.
        assert_eq!(common_supertype(&dec(5, 10), &dec(5, 10)), Some(dec(5, 10)));
        assert_eq!(
            common_supertype(&dec(5, 10), &dec(10, 2)),
            Some(RockyType::Unknown)
        );
    }

    /// The old `is_assignable`, frozen verbatim as ground truth for T3.2.
    ///
    /// This is `common_supertype(from, to) == to` against the *pre-fix*
    /// `common_supertype`, so the test proves the rewrite touches only the
    /// decimal pairs it means to.
    fn is_assignable_frozen_old(from: &RockyType, to: &RockyType) -> bool {
        fn common_supertype_frozen_old(a: &RockyType, b: &RockyType) -> Option<RockyType> {
            if a == b {
                return Some(a.clone());
            }
            if *a == RockyType::Unknown {
                return Some(b.clone());
            }
            if *b == RockyType::Unknown {
                return Some(a.clone());
            }
            match (a, b) {
                (RockyType::Int32, RockyType::Int64) | (RockyType::Int64, RockyType::Int32) => {
                    Some(RockyType::Int64)
                }
                (RockyType::Float32, RockyType::Float64)
                | (RockyType::Float64, RockyType::Float32) => Some(RockyType::Float64),
                (i, RockyType::Float64) | (RockyType::Float64, i) if i.is_integer() => {
                    Some(RockyType::Float64)
                }
                (i, RockyType::Float32) | (RockyType::Float32, i) if i.is_integer() => {
                    Some(RockyType::Float64)
                }
                (RockyType::Int32, RockyType::Decimal { precision, scale })
                | (RockyType::Decimal { precision, scale }, RockyType::Int32) => {
                    Some(RockyType::Decimal {
                        precision: (*precision).max(10),
                        scale: *scale,
                    })
                }
                (RockyType::Int64, RockyType::Decimal { precision, scale })
                | (RockyType::Decimal { precision, scale }, RockyType::Int64) => {
                    Some(RockyType::Decimal {
                        precision: (*precision).max(19),
                        scale: *scale,
                    })
                }
                (
                    RockyType::Decimal {
                        precision: p1,
                        scale: s1,
                    },
                    RockyType::Decimal {
                        precision: p2,
                        scale: s2,
                    },
                ) => Some(RockyType::Decimal {
                    precision: (*p1).max(*p2),
                    scale: (*s1).max(*s2),
                }),
                (RockyType::Timestamp, RockyType::TimestampNtz)
                | (RockyType::TimestampNtz, RockyType::Timestamp) => Some(RockyType::Timestamp),
                _ => None,
            }
        }

        if from == to || *from == RockyType::Unknown || *to == RockyType::Unknown {
            return true;
        }
        common_supertype_frozen_old(from, to).is_some_and(|sup| sup == *to)
    }

    /// T3.2 — verdict preservation, exhaustively.
    ///
    /// Full cross-product over every non-`Decimal` `RockyType` shape against the
    /// frozen old rule. `Decimal` is excluded on both sides because those
    /// verdicts change *deliberately* (T3.1). This is deterministic closure over
    /// a finite type set, not a hand-picked sample — it is the evidence that the
    /// `is_assignable` split changed decimals and nothing else.
    #[test]
    fn test_is_assignable_verdicts_preserved_for_non_decimals() {
        let universe = [
            RockyType::Boolean,
            RockyType::Int32,
            RockyType::Int64,
            RockyType::Float32,
            RockyType::Float64,
            RockyType::String,
            RockyType::Date,
            RockyType::Timestamp,
            RockyType::TimestampNtz,
            RockyType::Binary,
            RockyType::Variant,
            RockyType::Unknown,
            RockyType::Array(Box::new(RockyType::String)),
            RockyType::Array(Box::new(RockyType::Int64)),
            RockyType::Map(Box::new(RockyType::String), Box::new(RockyType::Int64)),
            RockyType::Map(Box::new(RockyType::String), Box::new(RockyType::String)),
            RockyType::Struct(vec![StructField {
                name: "a".to_string(),
                data_type: RockyType::Int64,
                nullable: false,
            }]),
            RockyType::Struct(vec![StructField {
                name: "b".to_string(),
                data_type: RockyType::String,
                nullable: true,
            }]),
        ];

        for from in &universe {
            for to in &universe {
                assert_eq!(
                    is_assignable(from, to),
                    is_assignable_frozen_old(from, to),
                    "verdict changed for a non-decimal pair: is_assignable({from}, {to})"
                );
            }
        }
    }

    /// T3.4 — monotonicity. For a fixed target, if a decimal is assignable then
    /// every *narrower* decimal is too. The old rule violated this: it permitted
    /// `Int32 → Decimal(10,2)` while rejecting the strictly wider
    /// `Int64 → Decimal(10,2)`.
    #[test]
    fn test_is_assignable_is_monotonic_in_source_width() {
        let targets = [dec(10, 2), dec(21, 2), dec(38, 2), dec(38, 0), dec(20, 10)];
        for to in &targets {
            for precision in 2u8..=38 {
                for scale in 0u8..precision {
                    let wide = dec(precision, scale);
                    let narrow = dec(precision - 1, scale);
                    if is_assignable(&wide, to) {
                        assert!(
                            is_assignable(&narrow, to),
                            "{wide} → {to} permitted but narrower {narrow} → {to} rejected"
                        );
                    }
                }
            }
        }
    }

    /// Soundness spot-check on the containment predicate itself: for every
    /// permitted decimal pair, the target must have at least as many integer
    /// digits and at least as much scale as the source.
    #[test]
    fn test_is_assignable_permits_only_containment() {
        for from_p in 1u8..=38 {
            for from_s in 0u8..=from_p {
                for to_p in 1u8..=38 {
                    for to_s in 0u8..=to_p {
                        let from = dec(from_p, from_s);
                        let to = dec(to_p, to_s);
                        if from == to || !is_assignable(&from, &to) {
                            continue;
                        }
                        let (fi, fs) = decimal_parts_of(&from).expect("well formed");
                        let (ti, ts) = decimal_parts_of(&to).expect("well formed");
                        assert!(
                            fi <= ti && fs <= ts,
                            "false permit: {from} → {to} ({fi} int/{fs} scale into {ti} int/{ts} scale)"
                        );
                    }
                }
            }
        }
    }

    // ---------------------------------------------------------------------
    // Declared precision/scale validation (defect D)
    // ---------------------------------------------------------------------

    #[test]
    fn test_validate_decimal_params() {
        // Legal.
        assert_eq!(validate_decimal_params(10, 2), Some((10, 2)));
        assert_eq!(validate_decimal_params(1, 0), Some((1, 0)));
        assert_eq!(validate_decimal_params(38, 38), Some((38, 38)));
        // BigQuery BIGNUMERIC's maximum is a legal declaration, so validation
        // uses the loosest bound and must not reject it.
        assert_eq!(validate_decimal_params(76, 38), Some((76, 38)));
        assert_eq!(
            validate_decimal_params(u64::from(MAX_DECLARED_PRECISION), 0),
            Some((76, 0))
        );

        // Rejected — each of these used to wrap through `as u8` into a
        // different, meaningless type.
        assert_eq!(validate_decimal_params(256, 2), None); // was Decimal(0,2)
        assert_eq!(validate_decimal_params(300, 2), None); // was Decimal(44,2)
        assert_eq!(validate_decimal_params(10, 1000), None); // was Decimal(10,232)
        assert_eq!(validate_decimal_params(10, -2), None); // was Decimal(10,254)
        assert_eq!(validate_decimal_params(5, 10), None); // scale > precision
        assert_eq!(validate_decimal_params(0, 0), None); // zero precision
        assert_eq!(validate_decimal_params(77, 0), None); // past the declared cap
        assert_eq!(validate_decimal_params(u64::MAX, i64::MAX), None);
        assert_eq!(validate_decimal_params(u64::MAX, i64::MIN), None);
    }

    /// No input can make `validate_decimal_params` return a pair that does not
    /// satisfy its own contract — this is the "no silent wrap" property.
    #[test]
    fn test_validate_decimal_params_never_wraps() {
        for precision in [0u64, 1, 37, 38, 39, 76, 77, 255, 256, 300, 1000, u64::MAX] {
            for scale in [
                i64::MIN,
                -1000,
                -2,
                -1,
                0,
                1,
                2,
                37,
                38,
                76,
                77,
                1000,
                i64::MAX,
            ] {
                let Some((p, s)) = validate_decimal_params(precision, scale) else {
                    continue;
                };
                assert_eq!(u64::from(p), precision, "precision changed value");
                assert_eq!(i64::from(s), scale, "scale changed value");
                assert!(p >= 1 && u32::from(p) <= MAX_DECLARED_PRECISION);
                assert!(s <= p);
            }
        }
    }

    #[test]
    fn test_decimal_from_parts_caps_at_inferred_precision() {
        assert_eq!(decimal_from_parts(36, 2), dec(38, 2));
        assert_eq!(decimal_from_parts(37, 2), RockyType::Unknown);
        assert_eq!(decimal_from_parts(0, 38), dec(38, 38));
        assert_eq!(decimal_from_parts(0, 39), RockyType::Unknown);
        assert_eq!(decimal_from_parts(0, 0), RockyType::Unknown);
        assert_eq!(decimal_from_parts(u32::MAX, 1), RockyType::Unknown);
        assert_eq!(decimal_from_parts(1, u32::MAX), RockyType::Unknown);
    }

    #[test]
    fn test_decimal_parts_of() {
        assert_eq!(decimal_parts_of(&RockyType::Int32), Some((10, 0)));
        assert_eq!(decimal_parts_of(&RockyType::Int64), Some((19, 0)));
        assert_eq!(decimal_parts_of(&dec(10, 2)), Some((8, 2)));
        assert_eq!(decimal_parts_of(&dec(10, 10)), Some((0, 10)));
        // Malformed / non-exact-numeric.
        assert_eq!(decimal_parts_of(&dec(5, 10)), None);
        assert_eq!(decimal_parts_of(&dec(0, 0)), None);
        assert_eq!(decimal_parts_of(&RockyType::Float64), None);
        assert_eq!(decimal_parts_of(&RockyType::Unknown), None);
        assert_eq!(decimal_parts_of(&RockyType::String), None);
    }

    #[test]
    fn test_is_numeric() {
        assert!(RockyType::Int32.is_numeric());
        assert!(RockyType::Float64.is_numeric());
        assert!(
            RockyType::Decimal {
                precision: 10,
                scale: 2
            }
            .is_numeric()
        );
        assert!(!RockyType::String.is_numeric());
        assert!(!RockyType::Boolean.is_numeric());
    }

    #[test]
    fn test_display_compact() {
        assert_eq!(RockyType::Int64.to_string(), "INT64");
        assert_eq!(
            RockyType::Decimal {
                precision: 10,
                scale: 2
            }
            .to_string(),
            "DECIMAL(10,2)"
        );
        assert_eq!(
            RockyType::Array(Box::new(RockyType::String)).to_string(),
            "ARRAY<STRING>"
        );
        assert_eq!(
            RockyType::Map(Box::new(RockyType::String), Box::new(RockyType::Int64)).to_string(),
            "MAP<STRING,INT64>"
        );
        assert_eq!(
            RockyType::Struct(vec![
                StructField {
                    name: "id".to_string(),
                    data_type: RockyType::Int64,
                    nullable: false,
                },
                StructField {
                    name: "name".to_string(),
                    data_type: RockyType::String,
                    nullable: true,
                },
            ])
            .to_string(),
            "STRUCT<id:INT64,name:STRING>"
        );
        assert_eq!(RockyType::Unknown.to_string(), "?");
    }

    #[test]
    fn test_timestamp_variants() {
        assert_eq!(
            common_supertype(&RockyType::Timestamp, &RockyType::TimestampNtz),
            Some(RockyType::Timestamp)
        );
    }
}
