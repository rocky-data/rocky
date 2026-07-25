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
/// keeps the pre-existing [`common_supertype`] behaviour, which for a float
/// operand is also the sound answer (all five engines return a float, division
/// included). `Int64 / Int64` therefore still yields `Int64`; that is wrong on
/// four of five engines but is a separate finding with no live defect behind it.
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
/// target's, *and* the source's scale must fit the target's. A malformed
/// decimal on either side is not assignable (fail-closed on a governance
/// predicate).
///
/// Every non-decimal pair reproduces the previous verdict exactly, including the
/// intentionally lossy integer/float widenings — see `test_is_assignable_verdicts_preserved`,
/// which pins that against a frozen copy of the old rule over the full
/// cross-product of non-decimal types.
///
/// [`RockyType::Unknown`] on either side is assignable: an un-normalizable type
/// is never a false failure (see `rocky_core::contracts`).
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
        // Pre-existing intentional lossy widening into Float64.
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
