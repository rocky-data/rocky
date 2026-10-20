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

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Rocky's unified column type.
///
/// Derives `JsonSchema` because it is served verbatim on the HTTP model
/// detail route (`TypedColumnOutput::data_type`): the structured form is the
/// only lossless one — the `Display` rendering drops a struct field's
/// nullability.
///
/// # Wire contract
///
/// The serde shape — externally tagged, with these exact variant names — is
/// published in `schemas/model_detail.schema.json` and the generated Python
/// and TypeScript bindings. From engine 1.74.0 it is a public contract:
///
/// - **Adding** a variant is additive. Consumers must treat an unknown tag as
///   "a type this build does not know", never as an error.
/// - **Renaming or removing** a variant, or changing a variant's payload, is
///   a breaking change to the model-detail route and every binding. It needs
///   a `Changed` changelog entry that names the old and new tags, and the
///   codegen cascade in the same PR.
///
/// The `Display` rendering (`data_type_display`) is a label and carries no
/// such promise.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
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
    /// A decimal whose precision Rocky cannot bound (#2356), and whose
    /// scale it may know (#2398).
    ///
    /// The value is an exact decimal, but its precision depends on the
    /// warehouse: `SUM` over `DECIMAL(10,2)` is `DECIMAL(38,2)` on DuckDB
    /// and `DECIMAL(20,2)` on Databricks. The scale is the same on both, so
    /// it is kept. Rocky keeps the type family so a bare `Decimal` contract
    /// is still checked, and a `Decimal(p,s)` contract with another scale is
    /// a mismatch. The precision it does not claim, so a `Decimal(p,s)`
    /// contract with the same scale is reported as not checked.
    DecimalUnsized {
        /// The scale, when every warehouse agrees on it. `None` when it
        /// differs by warehouse too, as for a bare `CAST(x AS DECIMAL)`.
        scale: Option<u8>,
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
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
            // Not `DECIMAL`: a bare `DECIMAL` is a SQL spelling with
            // dialect-dependent digits, and this is a label, not SQL.
            RockyType::DecimalUnsized { scale: Some(scale) } => write!(f, "DECIMAL(?,{scale})"),
            RockyType::DecimalUnsized { scale: None } => f.write_str("DECIMAL(?,?)"),
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
                | RockyType::DecimalUnsized { .. }
        )
    }

    /// Returns true if this is a decimal type, sized or not.
    pub fn is_decimal(&self) -> bool {
        matches!(
            self,
            RockyType::Decimal { .. } | RockyType::DecimalUnsized { .. }
        )
    }

    /// The scale of a decimal type: `Some(s)` for `Decimal { s, .. }` and for
    /// a `DecimalUnsized` whose scale is known, `None` for anything else.
    pub fn decimal_scale(&self) -> Option<u8> {
        match self {
            RockyType::Decimal { scale, .. } => Some(*scale),
            RockyType::DecimalUnsized { scale } => *scale,
            RockyType::Boolean
            | RockyType::Int32
            | RockyType::Int64
            | RockyType::Float32
            | RockyType::Float64
            | RockyType::String
            | RockyType::Date
            | RockyType::Timestamp
            | RockyType::TimestampNtz
            | RockyType::Binary
            | RockyType::Array(_)
            | RockyType::Map(_, _)
            | RockyType::Struct(_)
            | RockyType::Variant
            | RockyType::Unknown => None,
        }
    }

    /// Returns true if this is an integer type.
    pub fn is_integer(&self) -> bool {
        matches!(self, RockyType::Int32 | RockyType::Int64)
    }

    /// Returns true if this is a floating-point type.
    pub fn is_float(&self) -> bool {
        matches!(self, RockyType::Float32 | RockyType::Float64)
    }

    /// Returns true if this type is temporal.
    pub fn is_temporal(&self) -> bool {
        matches!(
            self,
            RockyType::Date | RockyType::Timestamp | RockyType::TimestampNtz
        )
    }
}

/// Widest precision Rocky claims for an **inferred** decimal.
///
/// ADR-TYPES §3, "Caps" (line 163): a rule that needs more digits gives
/// [`RockyType::Unknown`], and the result is nullable ([`InferredType`]).
/// Rocky never clamps the scale to fit, because a clamped type cannot hold
/// the inputs.
pub const MAX_INFERRED_PRECISION: u32 = 38;

/// An inferred type, and whether the rule that built it went over
/// [`MAX_INFERRED_PRECISION`].
///
/// ADR-TYPES §1: when Rocky cannot prove a bound, the answer is
/// `(Unknown, true)`. A result over the cap has no proven bound, and a
/// warehouse that caps decimals at 38 digits may not hold it: Databricks
/// with `ANSI_MODE = off` writes NULL for a decimal overflow (Open question
/// B, not ratified). So an over-cap result is `Unknown` *and* nullable, even
/// over non-null operands.
///
/// The bit is set where the cap is applied and travels with the type, so a
/// caller cannot take the `Unknown` without it. Every other `Unknown` here
/// (a non-numeric operand, `/`) has `over_cap: false`: its nullability is
/// the caller's rule, not this one.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct InferredType {
    /// The inferred type: a sound bound, or [`RockyType::Unknown`].
    pub ty: RockyType,
    /// `true` when the rule needed more than [`MAX_INFERRED_PRECISION`]
    /// digits. `ty` is then [`RockyType::Unknown`], and the caller must make
    /// the result nullable whatever its operands are.
    pub over_cap: bool,
}

impl InferredType {
    /// A type built within the cap.
    fn typed(ty: RockyType) -> Self {
        Self {
            ty,
            over_cap: false,
        }
    }

    /// A result that needs more than [`MAX_INFERRED_PRECISION`] digits.
    fn over_cap() -> Self {
        Self {
            ty: RockyType::Unknown,
            over_cap: true,
        }
    }
}

/// An arithmetic operator whose result type [`arithmetic_result_type`]
/// derives.
///
/// A Rocky-owned enum, so `rocky-ir` needs no SQL parser for the algebra.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecimalOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
}

/// Build an inferred [`RockyType::Decimal`] from `(integer_digits, scale)`.
///
/// The one construction site for an inferred decimal, and the one place the
/// cap is applied (ADR-TYPES §3, line 159). Above
/// [`MAX_INFERRED_PRECISION`] the result is `Unknown` with
/// [`InferredType::over_cap`] set, so it is nullable. A precision of zero is
/// `Unknown` with no overflow. The `u8` narrowing happens only after the
/// range check, so no value can wrap.
fn decimal_from_parts(integer_digits: u32, scale: u32) -> InferredType {
    let Some(precision) = integer_digits.checked_add(scale) else {
        return InferredType::over_cap();
    };
    if precision > MAX_INFERRED_PRECISION {
        return InferredType::over_cap();
    }
    if precision == 0 {
        return InferredType::typed(RockyType::Unknown);
    }
    let (Ok(precision), Ok(scale)) = (u8::try_from(precision), u8::try_from(scale)) else {
        return InferredType::over_cap();
    };
    InferredType::typed(RockyType::Decimal { precision, scale })
}

/// The `(integer_digits, scale)` pair of an exact numeric whose digits are
/// known (ADR-TYPES §3, line 159).
///
/// - `Int32` is `(10, 0)` and `Int64` is `(19, 0)` (ADR-TYPES §2, line 137).
/// - `Decimal { precision, scale }` is `(precision - scale, scale)`.
///
/// `None` for [`RockyType::DecimalUnsized`], which claims no precision, for
/// a malformed decimal (precision 0, or a scale above the precision), and
/// for every other type.
pub fn decimal_parts_of(ty: &RockyType) -> Option<(u32, u32)> {
    match ty {
        RockyType::Int32 => Some((10, 0)),
        RockyType::Int64 => Some((19, 0)),
        RockyType::Decimal { precision, scale } => {
            let (precision, scale) = (u32::from(*precision), u32::from(*scale));
            if precision == 0 || scale > precision {
                return None;
            }
            Some((precision - scale, scale))
        }
        RockyType::DecimalUnsized { .. }
        | RockyType::Boolean
        | RockyType::Float32
        | RockyType::Float64
        | RockyType::String
        | RockyType::Date
        | RockyType::Timestamp
        | RockyType::TimestampNtz
        | RockyType::Binary
        | RockyType::Array(_)
        | RockyType::Map(_, _)
        | RockyType::Struct(_)
        | RockyType::Variant
        | RockyType::Unknown => None,
    }
}

/// The result type of `a <op> b` (ADR-TYPES §3, lines 166-182).
///
/// The result is a sound bound on the exact value, not a prediction of the
/// type a warehouse reports:
///
/// | Operands | `+` `-` | `*` | `%` | `/` |
/// |---|---|---|---|---|
/// | a sized decimal: `(i1,s1)`, `(i2,s2)` | `(max(i)+1, max(s))` | `(i1+i2, s1+s2)` | `(min(i), max(s))` | `Unknown` |
/// | a `DecimalUnsized` and no float | `DecimalUnsized { scale: None }` | same | same | `Unknown` |
/// | two integers | `Int64` | `Int64` | `Int64` | `Unknown` |
/// | a float and a number | `Float64` | `Float64` | `Float64` | `Float64` |
/// | a non-numeric or `Unknown` | `Unknown` | `Unknown` | `Unknown` | `Unknown` |
///
/// A sized result above [`MAX_INFERRED_PRECISION`] is `Unknown` with
/// [`InferredType::over_cap`] set, so the caller makes it nullable. An
/// unsized operand keeps the result unsized with no scale: a warehouse that
/// caps the precision may cut the scale (ADR-TYPES §2, line 151). `Int64`
/// for an `Int64` operand is the published interim under-bound of Open
/// question A (line 180).
pub fn arithmetic_result_type(op: DecimalOp, a: &RockyType, b: &RockyType) -> InferredType {
    if !a.is_numeric() || !b.is_numeric() {
        return InferredType::typed(RockyType::Unknown);
    }
    if a.is_float() || b.is_float() {
        return InferredType::typed(RockyType::Float64);
    }
    if a.is_integer() && b.is_integer() {
        return InferredType::typed(match op {
            DecimalOp::Add | DecimalOp::Subtract | DecimalOp::Multiply | DecimalOp::Modulo => {
                RockyType::Int64
            }
            DecimalOp::Divide => RockyType::Unknown,
        });
    }
    // At least one operand is a decimal, sized or not.
    if op == DecimalOp::Divide {
        return InferredType::typed(RockyType::Unknown);
    }
    if matches!(a, RockyType::DecimalUnsized { .. })
        || matches!(b, RockyType::DecimalUnsized { .. })
    {
        return InferredType::typed(RockyType::DecimalUnsized { scale: None });
    }
    let (Some((i1, s1)), Some((i2, s2))) = (decimal_parts_of(a), decimal_parts_of(b)) else {
        return InferredType::typed(RockyType::Unknown);
    };
    match op {
        DecimalOp::Add | DecimalOp::Subtract => decimal_from_parts(i1.max(i2) + 1, s1.max(s2)),
        DecimalOp::Multiply => decimal_from_parts(i1 + i2, s1 + s2),
        DecimalOp::Modulo => decimal_from_parts(i1.min(i2), s1.max(s2)),
        DecimalOp::Divide => InferredType::typed(RockyType::Unknown),
    }
}

/// Whether the members whose digits are known (integers and sized
/// decimals, [`decimal_parts_of`]) need more than [`MAX_INFERRED_PRECISION`]
/// digits together: `max(integer_digits) + max(scale)`. Every other member
/// is skipped, so it cannot hide the bit. Read over the whole set, so the
/// order of the members cannot change it.
fn known_digits_over_cap<'a>(members: impl IntoIterator<Item = &'a RockyType>) -> bool {
    let (mut max_integer_digits, mut max_scale) = (0u32, 0u32);
    for (integer_digits, scale) in members.into_iter().filter_map(decimal_parts_of) {
        max_integer_digits = max_integer_digits.max(integer_digits);
        max_scale = max_scale.max(scale);
    }
    decimal_from_parts(max_integer_digits, max_scale).over_cap
}

/// Unify exact numerics, at least one a decimal (ADR-TYPES §2 line 151,
/// §3 line 184).
///
/// First the cap: when the integers and sized decimals need more than
/// [`MAX_INFERRED_PRECISION`] digits together, the result is `Unknown` with
/// [`InferredType::over_cap`] set, even with a `DecimalUnsized` member. A
/// warehouse casts every branch to the common type, and a branch that does
/// not fit can overflow to NULL just as arithmetic can.
///
/// Otherwise, with a `DecimalUnsized` member the result is
/// `DecimalUnsized`, which claims no precision. It keeps a scale only when
/// every decimal member has that same known scale; integers have no
/// fraction digits to disagree with. Without one, the result is
/// `max(integer_digits)` and `max(scale)`, with no carry digit, or
/// `Unknown` for a malformed decimal.
fn unify_exact_numerics(members: &[&RockyType]) -> InferredType {
    if known_digits_over_cap(members.iter().copied()) {
        return InferredType::over_cap();
    }
    if members
        .iter()
        .any(|member| matches!(member, RockyType::DecimalUnsized { .. }))
    {
        let mut decimal_scales = members
            .iter()
            .filter(|member| member.is_decimal())
            .map(|member| member.decimal_scale());
        let first = decimal_scales.next().flatten();
        let scale = first.filter(|s| decimal_scales.all(|other| other == Some(*s)));
        return InferredType::typed(RockyType::DecimalUnsized { scale });
    }
    let (mut max_integer_digits, mut max_scale) = (0u32, 0u32);
    for member in members {
        let Some((integer_digits, scale)) = decimal_parts_of(member) else {
            return InferredType::typed(RockyType::Unknown);
        };
        max_integer_digits = max_integer_digits.max(integer_digits);
        max_scale = max_scale.max(scale);
    }
    decimal_from_parts(max_integer_digits, max_scale)
}

/// Compute the common supertype of two types (for `COALESCE`, `CASE`, join
/// keys).
///
/// Returns `None` if the types are incompatible. An `Unknown` side gives
/// `Some(Unknown)`: Rocky has no bound for that side, so it has none for
/// the pair (ADR-TYPES §2, line 146). Exact numerics unify in
/// `(integer_digits, scale)` space, so no integer digit is dropped (§3,
/// line 184): `Decimal(10,0)` with `Decimal(10,10)` is `Decimal(20,10)`.
/// A pair that needs more than [`MAX_INFERRED_PRECISION`] digits is
/// `Unknown` with [`InferredType::over_cap`] set. An `Unknown` side leaves
/// one known type, and one type is never over the cap, so that early return
/// drops no bit. For a whole branch set, use [`common_supertype_of`]: a
/// pairwise fold loses the digits of a member once it meets an `Unknown`,
/// `DecimalUnsized` or float member.
pub fn common_supertype(a: &RockyType, b: &RockyType) -> Option<InferredType> {
    if a == b {
        return Some(InferredType::typed(a.clone()));
    }

    if *a == RockyType::Unknown || *b == RockyType::Unknown {
        return Some(InferredType::typed(RockyType::Unknown));
    }

    // Numeric promotion
    let supertype = match (a, b) {
        // Int32 widens to Int64
        (RockyType::Int32, RockyType::Int64) | (RockyType::Int64, RockyType::Int32) => {
            RockyType::Int64
        }
        // Float32 widens to Float64
        (RockyType::Float32, RockyType::Float64) | (RockyType::Float64, RockyType::Float32) => {
            RockyType::Float64
        }
        // Int → Float
        (i, RockyType::Float64) | (RockyType::Float64, i) if i.is_integer() => RockyType::Float64,
        (i, RockyType::Float32) | (RockyType::Float32, i) if i.is_integer() => {
            RockyType::Float64 // promote to Float64 for safety
        }
        // An integer or a decimal with a decimal, sized or not (#2356,
        // #2398). A float with a decimal stays incompatible: `is_assignable`
        // falls through to this function, and a decimal does not fit a float.
        (a, b) if (a.is_integer() || a.is_decimal()) && (b.is_integer() || b.is_decimal()) => {
            return Some(unify_exact_numerics(&[a, b]));
        }
        // Timestamp variants
        (RockyType::Timestamp, RockyType::TimestampNtz)
        | (RockyType::TimestampNtz, RockyType::Timestamp) => RockyType::Timestamp,

        _ => return None,
    };
    Some(InferredType::typed(supertype))
}

/// Unify a whole branch set: `COALESCE`, `CASE`, `GREATEST`, `LEAST`.
///
/// ADR-TYPES §3, line 184: branches are unified as a set, not by a fold, so
/// the order of arguments cannot change the result. A pairwise fold cannot
/// promise that once a cap exists. Each step here reads the whole set
/// (`any`, `all`, `max`), never an accumulator.
///
/// - A set of one repeated type is that type.
/// - When the integers and sized decimals in the set need more than
///   [`MAX_INFERRED_PRECISION`] digits together: `Unknown` with
///   [`InferredType::over_cap`] set, whatever the other branches are. So
///   `[w, z, x]` is over the cap when `[w, z]` is, for an `Unknown`,
///   `DecimalUnsized`, float or any other `x`, in any order.
/// - Any `Unknown` branch gives `Unknown` (§2, line 146). The caller drops a
///   `NULL` literal first: it adds only nullability, not a type.
/// - Exact numerics with a decimal: as [`common_supertype`], over the set.
///   Above [`MAX_INFERRED_PRECISION`]: `Unknown` with
///   [`InferredType::over_cap`] set.
/// - `Int32` with `Int64`: `Int64`. Integers with floats, or two float
///   widths: `Float64`.
/// - `Timestamp` with `TimestampNtz`: `Timestamp`, as [`common_supertype`].
/// - A float with a decimal, or any other mix [`common_supertype`] calls
///   incompatible: `Unknown`.
///
/// An empty set is `Unknown`.
pub fn common_supertype_of(branches: &[RockyType]) -> InferredType {
    let Some(first) = branches.first() else {
        return InferredType::typed(RockyType::Unknown);
    };
    if branches.iter().all(|t| t == first) {
        return InferredType::typed(first.clone());
    }
    // Before any other rule: an `Unknown`, unsized or float branch must not
    // hide the digits the sized branches need together.
    if known_digits_over_cap(branches) {
        return InferredType::over_cap();
    }
    if branches.contains(&RockyType::Unknown) {
        return InferredType::typed(RockyType::Unknown);
    }
    if branches.iter().all(RockyType::is_numeric) {
        let any_float = branches.iter().any(RockyType::is_float);
        let any_decimal = branches.iter().any(RockyType::is_decimal);
        return match (any_float, any_decimal) {
            (true, true) => InferredType::typed(RockyType::Unknown),
            (true, false) => InferredType::typed(RockyType::Float64),
            // Not all equal and no float: `Int32` with `Int64`.
            (false, false) => InferredType::typed(RockyType::Int64),
            (false, true) => unify_exact_numerics(&branches.iter().collect::<Vec<_>>()),
        };
    }
    if branches
        .iter()
        .all(|t| matches!(t, RockyType::Timestamp | RockyType::TimestampNtz))
    {
        return InferredType::typed(RockyType::Timestamp);
    }
    InferredType::typed(RockyType::Unknown)
}

/// Check if a value of type `from` can be assigned to a column of type `to`.
///
/// More permissive than `common_supertype` — allows widening conversions
/// that might lose precision (e.g., Int64 → Float64).
pub fn is_assignable(from: &RockyType, to: &RockyType) -> bool {
    if from == to || *from == RockyType::Unknown || *to == RockyType::Unknown {
        return true;
    }

    let decimal_fits = |from_precision: u8, from_scale: u8, to_precision: u8, to_scale: u8| {
        from_scale <= to_scale
            && from_precision
                .checked_sub(from_scale)
                .zip(to_precision.checked_sub(to_scale))
                .is_some_and(|(from_integer, to_integer)| from_integer <= to_integer)
    };

    match (from, to) {
        (
            RockyType::Decimal {
                precision: from_precision,
                scale: from_scale,
            },
            RockyType::Decimal {
                precision: to_precision,
                scale: to_scale,
            },
        ) => decimal_fits(*from_precision, *from_scale, *to_precision, *to_scale),
        (RockyType::Int32, RockyType::Decimal { precision, scale }) => {
            decimal_fits(10, 0, *precision, *scale)
        }
        (RockyType::Int64, RockyType::Decimal { precision, scale }) => {
            decimal_fits(19, 0, *precision, *scale)
        }
        // A sized decimal or an integer fits "some decimal". The reverse is
        // not proven: the unsized value's digits may exceed the target's
        // (#2356). A known target scale must hold the source's fraction
        // digits: an integer has none, and a source of unknown scale is not
        // proven to fit (#2398).
        (from, RockyType::DecimalUnsized { scale: to_scale }) => {
            let fraction_fits = match (from.is_integer(), from.decimal_scale(), to_scale) {
                (true, _, _) | (false, _, None) => true,
                (false, Some(from_scale), Some(to_scale)) => from_scale <= *to_scale,
                (false, None, Some(_)) => false,
            };
            (from.is_integer() || from.is_decimal()) && fraction_fits
        }
        (RockyType::DecimalUnsized { .. }, _) => false,
        _ => common_supertype(from, to).is_some_and(|sup| sup.ty == *to),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The type half of [`common_supertype`]. The over-cap bit has its own
    /// test, `over_cap_is_set_where_the_cap_is_applied`.
    fn sup(a: &RockyType, b: &RockyType) -> Option<RockyType> {
        common_supertype(a, b).map(|inferred| inferred.ty)
    }

    /// The type half of [`common_supertype_of`].
    fn sup_of(branches: &[RockyType]) -> RockyType {
        common_supertype_of(branches).ty
    }

    /// The type half of [`arithmetic_result_type`].
    fn arith(op: DecimalOp, a: &RockyType, b: &RockyType) -> RockyType {
        arithmetic_result_type(op, a, b).ty
    }

    #[test]
    fn test_same_types() {
        assert_eq!(
            sup(&RockyType::Int64, &RockyType::Int64),
            Some(RockyType::Int64)
        );
    }

    #[test]
    fn test_int_widening() {
        assert_eq!(
            sup(&RockyType::Int32, &RockyType::Int64),
            Some(RockyType::Int64)
        );
    }

    #[test]
    fn test_float_widening() {
        assert_eq!(
            sup(&RockyType::Float32, &RockyType::Float64),
            Some(RockyType::Float64)
        );
    }

    #[test]
    fn test_int_to_float() {
        assert_eq!(
            sup(&RockyType::Int64, &RockyType::Float64),
            Some(RockyType::Float64)
        );
    }

    #[test]
    fn test_int_to_decimal() {
        let result = sup(
            &RockyType::Int64,
            &RockyType::Decimal {
                precision: 10,
                scale: 2,
            },
        );
        // 19 integer digits from `Int64`, 2 fraction digits from the decimal.
        assert_eq!(
            result,
            Some(RockyType::Decimal {
                precision: 21,
                scale: 2
            })
        );
    }

    #[test]
    fn test_incompatible_types() {
        assert_eq!(sup(&RockyType::String, &RockyType::Int64), None);
        assert_eq!(sup(&RockyType::Boolean, &RockyType::Date), None);
    }

    /// ADR-TYPES §2: an `Unknown` side absorbs. It is compatible (`Some`),
    /// so a join key is not reported as incompatible, but it claims no type.
    #[test]
    fn test_unknown_absorbs_in_unification() {
        assert_eq!(
            sup(&RockyType::Unknown, &RockyType::Int64),
            Some(RockyType::Unknown)
        );
        assert_eq!(
            sup(&RockyType::String, &RockyType::Unknown),
            Some(RockyType::Unknown)
        );
    }

    #[test]
    fn test_is_assignable_widening() {
        assert!(is_assignable(&RockyType::Int32, &RockyType::Int64));
        assert!(!is_assignable(&RockyType::Int64, &RockyType::Int32));
    }

    #[test]
    fn test_is_assignable_decimal_capacity() {
        let decimal = |precision, scale| RockyType::Decimal { precision, scale };

        assert!(is_assignable(&decimal(10, 2), &decimal(10, 2)));
        assert!(!is_assignable(&decimal(10, 2), &decimal(10, 4)));
        assert!(is_assignable(&decimal(10, 2), &decimal(12, 4)));
        assert!(!is_assignable(&decimal(12, 4), &decimal(10, 2)));
        assert!(!is_assignable(&decimal(2, 4), &decimal(10, 2)));
        assert!(!is_assignable(&decimal(10, 2), &decimal(2, 4)));
        assert!(!is_assignable(&RockyType::Int32, &decimal(10, 2)));
        assert!(is_assignable(&RockyType::Int32, &decimal(12, 2)));
        assert!(!is_assignable(&RockyType::Int64, &decimal(19, 10)));
        assert!(is_assignable(&RockyType::Int64, &decimal(29, 10)));
    }

    /// A sized decimal is never assignable to an integer: the fallthrough
    /// unifies them in `(integer_digits, scale)` space, and the union keeps
    /// the decimal's digits, so it is never the integer itself. This holds
    /// even for a scale-0 decimal that fits the integer's digits.
    #[test]
    fn test_is_assignable_decimal_to_integer_is_false() {
        let decimal = |precision, scale| RockyType::Decimal { precision, scale };
        for from in [
            decimal(10, 2),
            decimal(10, 0),
            decimal(5, 0),
            decimal(38, 0),
        ] {
            for to in [RockyType::Int32, RockyType::Int64] {
                assert!(!is_assignable(&from, &to), "{from} -> {to}");
            }
        }
        assert_eq!(
            sup(&decimal(10, 2), &RockyType::Int64),
            Some(decimal(21, 2))
        );
        assert_eq!(
            sup(&decimal(10, 0), &RockyType::Int64),
            Some(decimal(19, 0))
        );
        assert_eq!(sup(&decimal(5, 0), &RockyType::Int32), Some(decimal(10, 0)));
    }

    #[test]
    fn test_is_assignable_unknown() {
        assert!(is_assignable(&RockyType::Unknown, &RockyType::String));
        assert!(is_assignable(&RockyType::Int64, &RockyType::Unknown));
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

    /// #2356: `DecimalUnsized` is a decimal with a precision Rocky does
    /// not claim. It absorbs exact numerics in unification, takes sized
    /// decimals and integers on assignment, and is never assignable to a
    /// sized decimal, whose digits it may exceed.
    #[test]
    fn decimal_unsized_unifies_and_assigns_soundly() {
        let unsized_decimal = RockyType::DecimalUnsized { scale: None };
        let d10_2 = RockyType::Decimal {
            precision: 10,
            scale: 2,
        };
        for other in [
            d10_2.clone(),
            RockyType::Int32,
            RockyType::Int64,
            unsized_decimal.clone(),
        ] {
            assert_eq!(
                sup(&unsized_decimal, &other),
                Some(unsized_decimal.clone()),
                "{other:?}"
            );
            assert_eq!(
                sup(&other, &unsized_decimal),
                Some(unsized_decimal.clone()),
                "{other:?}"
            );
        }
        for other in [RockyType::String, RockyType::Float64, RockyType::Date] {
            assert_eq!(sup(&unsized_decimal, &other), None, "{other:?}");
        }

        assert!(is_assignable(&d10_2, &unsized_decimal));
        assert!(is_assignable(&RockyType::Int64, &unsized_decimal));
        assert!(is_assignable(&unsized_decimal, &unsized_decimal));
        assert!(!is_assignable(&unsized_decimal, &d10_2));
        assert!(!is_assignable(
            &unsized_decimal,
            &RockyType::Decimal {
                precision: 38,
                scale: 2
            }
        ));
        assert!(!is_assignable(&unsized_decimal, &RockyType::Int64));
        assert!(!is_assignable(&RockyType::String, &unsized_decimal));

        assert!(unsized_decimal.is_numeric());
        assert!(unsized_decimal.is_decimal());
        assert!(!unsized_decimal.is_integer());
        assert_eq!(unsized_decimal.to_string(), "DECIMAL(?,?)");
    }

    /// #2398: a known scale survives unification with an integer and with
    /// a decimal of the same scale. Any other decimal drops it, because a
    /// warehouse that caps the precision may cut the scale too.
    #[test]
    fn decimal_unsized_keeps_a_scale_only_when_it_is_shared() {
        let scale = |s: Option<u8>| RockyType::DecimalUnsized { scale: s };
        let d = |precision: u8, scale: u8| RockyType::Decimal { precision, scale };
        let cases = [
            (scale(Some(2)), RockyType::Int32, scale(Some(2))),
            (scale(Some(2)), RockyType::Int64, scale(Some(2))),
            (scale(Some(2)), d(10, 2), scale(Some(2))),
            (scale(Some(2)), d(10, 4), scale(None)),
            (scale(Some(2)), d(10, 0), scale(None)),
            (scale(Some(2)), scale(Some(4)), scale(None)),
            (scale(Some(2)), scale(None), scale(None)),
            (scale(None), RockyType::Int64, scale(None)),
            (scale(None), d(10, 2), scale(None)),
        ];
        for (a, b, want) in cases {
            assert_eq!(sup(&a, &b), Some(want.clone()), "{a:?} {b:?}");
            assert_eq!(sup(&b, &a), Some(want), "{b:?} {a:?}");
        }

        assert_eq!(scale(Some(2)).to_string(), "DECIMAL(?,2)");
        assert_eq!(scale(Some(2)).decimal_scale(), Some(2));
        assert_eq!(scale(None).decimal_scale(), None);
        assert_eq!(d(10, 3).decimal_scale(), Some(3));
        assert_eq!(RockyType::Int64.decimal_scale(), None);

        // A known target scale must hold the source's fraction digits.
        assert!(is_assignable(&d(10, 2), &scale(Some(2))));
        assert!(is_assignable(&d(10, 1), &scale(Some(2))));
        assert!(!is_assignable(&d(10, 4), &scale(Some(2))));
        assert!(is_assignable(&RockyType::Int64, &scale(Some(2))));
        assert!(!is_assignable(&scale(None), &scale(Some(2))));
        assert!(is_assignable(&scale(Some(2)), &scale(None)));
        assert!(!is_assignable(&scale(Some(2)), &d(38, 2)));
    }

    /// The wire shape is part of the model-detail contract.
    #[test]
    fn decimal_unsized_serializes_with_its_scale() {
        for (ty, want) in [
            (
                RockyType::DecimalUnsized { scale: Some(2) },
                r#"{"DecimalUnsized":{"scale":2}}"#,
            ),
            (
                RockyType::DecimalUnsized { scale: None },
                r#"{"DecimalUnsized":{"scale":null}}"#,
            ),
        ] {
            let json = serde_json::to_string(&ty).unwrap();
            assert_eq!(json, want);
            let back: RockyType = serde_json::from_str(&json).unwrap();
            assert_eq!(back, ty);
        }
    }

    #[test]
    fn test_timestamp_variants() {
        assert_eq!(
            sup(&RockyType::Timestamp, &RockyType::TimestampNtz),
            Some(RockyType::Timestamp)
        );
    }

    fn dec(precision: u8, scale: u8) -> RockyType {
        RockyType::Decimal { precision, scale }
    }

    fn unsized_dec(scale: Option<u8>) -> RockyType {
        RockyType::DecimalUnsized { scale }
    }

    /// A spread of every lattice family, for the exhaustive pair checks.
    fn lattice() -> Vec<RockyType> {
        vec![
            RockyType::Boolean,
            RockyType::Int32,
            RockyType::Int64,
            RockyType::Float32,
            RockyType::Float64,
            dec(10, 2),
            dec(10, 0),
            dec(10, 10),
            dec(38, 0),
            dec(30, 30),
            dec(2, 4),
            unsized_dec(Some(2)),
            unsized_dec(Some(4)),
            unsized_dec(None),
            RockyType::String,
            RockyType::Date,
            RockyType::Timestamp,
            RockyType::TimestampNtz,
            RockyType::Binary,
            RockyType::Array(Box::new(RockyType::Int64)),
            RockyType::Variant,
            RockyType::Unknown,
        ]
    }

    #[test]
    fn decimal_parts_of_reads_integer_digits_and_scale() {
        assert_eq!(decimal_parts_of(&RockyType::Int32), Some((10, 0)));
        assert_eq!(decimal_parts_of(&RockyType::Int64), Some((19, 0)));
        assert_eq!(decimal_parts_of(&dec(10, 2)), Some((8, 2)));
        assert_eq!(decimal_parts_of(&dec(10, 10)), Some((0, 10)));
        // No precision claimed, malformed, or not an exact numeric.
        assert_eq!(decimal_parts_of(&unsized_dec(Some(2))), None);
        assert_eq!(decimal_parts_of(&dec(2, 4)), None);
        assert_eq!(decimal_parts_of(&dec(0, 0)), None);
        assert_eq!(decimal_parts_of(&RockyType::Float64), None);
        assert_eq!(decimal_parts_of(&RockyType::Unknown), None);
    }

    /// ADR-TYPES §3, the operator table and the integer-pair rules, with the
    /// Validation section's named cases.
    #[test]
    fn arithmetic_result_type_follows_the_operator_table() {
        use DecimalOp::{Add, Divide, Modulo, Multiply, Subtract};
        let cases = [
            // Sized decimals.
            (Add, dec(10, 2), dec(10, 2), dec(11, 2)),
            (Subtract, dec(10, 2), dec(10, 2), dec(11, 2)),
            (Multiply, dec(10, 2), dec(10, 2), dec(20, 4)),
            (Modulo, dec(10, 2), dec(10, 2), dec(10, 2)),
            (Divide, dec(10, 2), dec(10, 2), RockyType::Unknown),
            (Add, dec(10, 0), dec(10, 10), dec(21, 10)),
            (Modulo, dec(10, 2), dec(10, 0), dec(10, 2)),
            // An integer keeps its integer digits.
            (Add, RockyType::Int32, dec(10, 2), dec(13, 2)),
            (Add, RockyType::Int64, dec(10, 2), dec(22, 2)),
            (Multiply, RockyType::Int32, dec(5, 2), dec(15, 2)),
            (Modulo, RockyType::Int64, dec(10, 2), dec(10, 2)),
            (Divide, RockyType::Int64, dec(10, 2), RockyType::Unknown),
            // Above the cap: `Unknown`, never a clamped scale.
            (Add, dec(38, 0), dec(38, 0), RockyType::Unknown),
            (Multiply, dec(20, 2), dec(20, 2), RockyType::Unknown),
            (
                Multiply,
                RockyType::Int64,
                RockyType::Int64,
                RockyType::Int64,
            ),
            // Integer pairs.
            (Add, RockyType::Int32, RockyType::Int32, RockyType::Int64),
            (
                Multiply,
                RockyType::Int32,
                RockyType::Int32,
                RockyType::Int64,
            ),
            (Add, RockyType::Int32, RockyType::Int64, RockyType::Int64),
            (Modulo, RockyType::Int32, RockyType::Int32, RockyType::Int64),
            (
                Divide,
                RockyType::Int32,
                RockyType::Int32,
                RockyType::Unknown,
            ),
            (
                Divide,
                RockyType::Int64,
                RockyType::Int64,
                RockyType::Unknown,
            ),
            // A float on either side.
            (
                Add,
                RockyType::Float32,
                RockyType::Int32,
                RockyType::Float64,
            ),
            (Divide, RockyType::Float64, dec(10, 2), RockyType::Float64),
            (
                Multiply,
                unsized_dec(Some(2)),
                RockyType::Float32,
                RockyType::Float64,
            ),
            // An unsized operand: unsized, no scale; division unknown.
            (Add, unsized_dec(Some(2)), dec(10, 2), unsized_dec(None)),
            (
                Add,
                unsized_dec(Some(2)),
                RockyType::Int64,
                unsized_dec(None),
            ),
            (Multiply, unsized_dec(None), dec(10, 2), unsized_dec(None)),
            (
                Modulo,
                unsized_dec(Some(2)),
                unsized_dec(Some(2)),
                unsized_dec(None),
            ),
            (Divide, unsized_dec(Some(2)), dec(10, 2), RockyType::Unknown),
            // Non-numeric, unknown, or malformed.
            (Add, RockyType::String, RockyType::Int64, RockyType::Unknown),
            (
                Multiply,
                RockyType::Unknown,
                RockyType::Int64,
                RockyType::Unknown,
            ),
            (Add, RockyType::Unknown, dec(10, 2), RockyType::Unknown),
            (
                Add,
                RockyType::Boolean,
                RockyType::Int32,
                RockyType::Unknown,
            ),
            (Add, dec(2, 4), dec(10, 2), RockyType::Unknown),
        ];
        for (op, a, b, want) in cases {
            assert_eq!(arith(op, &a, &b), want, "{a} {op:?} {b}");
        }
    }

    /// Every rule in the table is symmetric in its operands, and an unsized
    /// operand never yields a sized decimal.
    #[test]
    fn arithmetic_result_type_is_symmetric_and_never_sizes_an_unsized_operand() {
        let ops = [
            DecimalOp::Add,
            DecimalOp::Subtract,
            DecimalOp::Multiply,
            DecimalOp::Divide,
            DecimalOp::Modulo,
        ];
        for a in lattice() {
            for b in lattice() {
                for op in ops {
                    assert_eq!(
                        arithmetic_result_type(op, &a, &b),
                        arithmetic_result_type(op, &b, &a),
                        "{a} {op:?} {b}"
                    );
                    let result = arith(op, &a, &b);
                    if matches!(a, RockyType::DecimalUnsized { .. })
                        || matches!(b, RockyType::DecimalUnsized { .. })
                    {
                        assert!(
                            !matches!(result, RockyType::Decimal { .. }),
                            "{a} {op:?} {b} gave a sized {result}"
                        );
                    }
                }
            }
        }
    }

    /// A decimal value as a scaled integer: `mantissa / 10^scale`.
    #[derive(Clone, Copy, Debug)]
    struct Value {
        mantissa: i128,
        scale: u32,
    }

    impl Value {
        fn rescale(self, scale: u32) -> i128 {
            self.mantissa * 10i128.pow(scale - self.scale)
        }

        /// Whether the value is a value of `ty` (an `Int32`, `Int64` or a
        /// sized decimal).
        fn fits(self, ty: &RockyType) -> bool {
            let (integer_digits, scale) = decimal_parts_of(ty).expect("an exact numeric");
            let mut value = self;
            while value.scale > scale {
                if value.mantissa % 10 != 0 {
                    return false;
                }
                value.mantissa /= 10;
                value.scale -= 1;
            }
            value.rescale(scale).abs() < 10i128.pow(integer_digits + scale)
        }
    }

    /// The boundary values of a type: its largest magnitude, its smallest
    /// step, and both signs.
    fn boundary_values(ty: &RockyType) -> Vec<Value> {
        let (integer_digits, scale) = decimal_parts_of(ty).expect("an exact numeric");
        let max = 10i128.pow(integer_digits + scale) - 1;
        [max, -max, 1, -1, max / 2 + 1]
            .into_iter()
            .map(|mantissa| Value { mantissa, scale })
            .collect()
    }

    /// ADR-TYPES Validation: the exact result of each operation fits the
    /// inferred type, or the type is `Unknown`. The exact result is computed
    /// with integers, not read from the rule under test.
    #[test]
    fn arithmetic_result_type_bounds_the_exact_result() {
        let mut operands = vec![RockyType::Int32];
        for precision in 1..=12u8 {
            for scale in 0..=precision {
                operands.push(dec(precision, scale));
            }
        }
        let mut checked = 0usize;
        for a in &operands {
            for b in &operands {
                if !a.is_decimal() && !b.is_decimal() {
                    continue;
                }
                for op in [
                    DecimalOp::Add,
                    DecimalOp::Subtract,
                    DecimalOp::Multiply,
                    DecimalOp::Modulo,
                ] {
                    let result = arith(op, a, b);
                    if result == RockyType::Unknown {
                        continue;
                    }
                    for x in boundary_values(a) {
                        for y in boundary_values(b) {
                            let common = x.scale.max(y.scale);
                            let exact = match op {
                                DecimalOp::Add => Value {
                                    mantissa: x.rescale(common) + y.rescale(common),
                                    scale: common,
                                },
                                DecimalOp::Subtract => Value {
                                    mantissa: x.rescale(common) - y.rescale(common),
                                    scale: common,
                                },
                                DecimalOp::Multiply => Value {
                                    mantissa: x.mantissa * y.mantissa,
                                    scale: x.scale + y.scale,
                                },
                                DecimalOp::Modulo => Value {
                                    mantissa: x.rescale(common) % y.rescale(common),
                                    scale: common,
                                },
                                DecimalOp::Divide => unreachable!("not in the list"),
                            };
                            assert!(
                                exact.fits(&result),
                                "{a} {op:?} {b}: {x:?} and {y:?} give {exact:?}, \
                                 which does not fit the inferred {result}"
                            );
                            checked += 1;
                        }
                    }
                }
            }
        }
        assert!(checked > 100_000, "only {checked} values checked");
    }

    /// ADR-TYPES §3 Unification and its Validation cases.
    #[test]
    fn unification_keeps_integer_digits_and_caps() {
        assert_eq!(sup(&dec(10, 0), &dec(10, 10)), Some(dec(20, 10)));
        assert_eq!(sup(&RockyType::Int32, &dec(5, 2)), Some(dec(12, 2)));
        // No carry digit: unification is not arithmetic.
        assert_eq!(sup(&dec(10, 2), &dec(12, 2)), Some(dec(12, 2)));
        // Over the cap: compatible, but no type.
        assert_eq!(sup(&dec(30, 0), &dec(30, 30)), Some(RockyType::Unknown));
        assert_eq!(
            sup_of(&[dec(10, 0), dec(10, 10), RockyType::Int32]),
            dec(20, 10)
        );
        // A float with a decimal stays incompatible.
        assert_eq!(sup(&RockyType::Float64, &dec(10, 2)), None);
        assert_eq!(
            sup_of(&[RockyType::Float64, dec(10, 2)]),
            RockyType::Unknown
        );
        assert_eq!(sup_of(&[]), RockyType::Unknown);
        assert_eq!(
            sup_of(&[RockyType::Int32, RockyType::Float32]),
            RockyType::Float64
        );
        assert_eq!(
            sup_of(&[RockyType::Int32, RockyType::Int64]),
            RockyType::Int64
        );
    }

    /// ADR-TYPES §1: a result over the cap is `(Unknown, true)`. The
    /// nullable half is `over_cap`, set by the one function that applies the
    /// cap, so every public rule reports it. No other `Unknown` sets it.
    #[test]
    fn over_cap_is_set_where_the_cap_is_applied() {
        use DecimalOp::{Add, Divide, Modulo, Multiply, Subtract};
        let over = InferredType {
            ty: RockyType::Unknown,
            over_cap: true,
        };
        let typed = |ty| InferredType {
            ty,
            over_cap: false,
        };
        // Arithmetic.
        assert_eq!(arithmetic_result_type(Add, &dec(38, 0), &dec(38, 0)), over);
        assert_eq!(
            arithmetic_result_type(Subtract, &dec(30, 0), &dec(30, 30)),
            over
        );
        assert_eq!(
            arithmetic_result_type(Multiply, &dec(30, 0), &dec(30, 0)),
            over
        );
        assert_eq!(
            arithmetic_result_type(Multiply, &dec(37, 0), &RockyType::Int32),
            over
        );
        assert_eq!(
            arithmetic_result_type(Add, &dec(37, 0), &dec(37, 0)),
            typed(dec(38, 0))
        );
        // `%` keeps the smaller integer part, so it never goes over here.
        assert_eq!(
            arithmetic_result_type(Modulo, &dec(38, 0), &dec(38, 38)),
            typed(dec(38, 38))
        );
        // Other `Unknown`s are not overflow.
        for (op, a, b) in [
            (Divide, dec(38, 0), dec(38, 0)),
            (Add, RockyType::String, RockyType::Int32),
            (Add, RockyType::Unknown, dec(10, 2)),
            (Add, dec(2, 4), dec(10, 2)),
        ] {
            assert_eq!(
                arithmetic_result_type(op, &a, &b),
                typed(RockyType::Unknown),
                "{a} {op:?} {b}"
            );
        }
        // Unification, pair and set.
        assert_eq!(
            common_supertype(&dec(30, 0), &dec(30, 30)),
            Some(over.clone())
        );
        assert_eq!(
            common_supertype(&dec(38, 0), &dec(38, 38)),
            Some(over.clone())
        );
        assert_eq!(
            common_supertype_of(&[dec(30, 0), dec(30, 30), dec(5, 2)]),
            over
        );
        assert_eq!(
            common_supertype(&dec(10, 0), &dec(10, 10)),
            Some(typed(dec(20, 10)))
        );
        assert_eq!(
            common_supertype(&RockyType::Unknown, &dec(10, 2)),
            Some(typed(RockyType::Unknown))
        );
        assert_eq!(
            common_supertype_of(&[RockyType::Float64, dec(10, 2)]),
            typed(RockyType::Unknown)
        );
        // An `Unknown`, unsized or float branch does not hide the sized
        // branches' bit, in any order. Without `dec(30, 30)` they fit.
        let (w, z, m) = (dec(30, 0), dec(30, 30), RockyType::Unknown);
        let unsized_decimal = RockyType::DecimalUnsized { scale: None };
        for x in [m.clone(), unsized_decimal.clone(), RockyType::Float64] {
            for set in [
                [w.clone(), z.clone(), x.clone()],
                [w.clone(), x.clone(), z.clone()],
                [z.clone(), w.clone(), x.clone()],
                [z.clone(), x.clone(), w.clone()],
                [x.clone(), w.clone(), z.clone()],
                [x.clone(), z.clone(), w.clone()],
            ] {
                assert_eq!(common_supertype_of(&set), over, "{set:?}");
            }
        }
        assert_eq!(
            unify_exact_numerics(&[&w, &unsized_decimal, &z]),
            over.clone()
        );
        assert_eq!(
            common_supertype_of(&[w.clone(), m.clone(), w.clone()]),
            typed(RockyType::Unknown)
        );
        assert_eq!(
            common_supertype_of(&[w.clone(), unsized_decimal.clone(), w.clone()]),
            typed(RockyType::DecimalUnsized { scale: None })
        );
        assert_eq!(
            common_supertype_of(&[m.clone(), m]),
            typed(RockyType::Unknown)
        );
        // The bit and the type agree on the whole lattice: over the cap is
        // always `Unknown`.
        for a in lattice() {
            for b in lattice() {
                for op in [Add, Subtract, Multiply, Divide, Modulo] {
                    let result = arithmetic_result_type(op, &a, &b);
                    assert!(!result.over_cap || result.ty == RockyType::Unknown);
                }
                let set = common_supertype_of(&[a.clone(), b.clone()]);
                assert!(!set.over_cap || set.ty == RockyType::Unknown);
            }
        }
    }

    /// ADR-TYPES §3: the order of the branches cannot change the result. The
    /// first triple needs 60 digits, so the set is `Unknown`. A pairwise fold
    /// that applies the cap and starts from `Unknown` would depend on the
    /// order: `DEC(30,0), DEC(30,30)` reaches `Unknown`, and `DEC(5,2)` then
    /// replaces it with `DECIMAL(5,2)`, three integer digits where the first
    /// branch needs 30. Main's fold had no cap: it took `max(precision)` and
    /// `max(scale)`, giving `DECIMAL(30,30)` in every order, which is
    /// independent of the order but holds no integer digit.
    #[test]
    fn common_supertype_of_is_order_independent() {
        let triples = [
            [dec(30, 0), dec(30, 30), dec(5, 2)],
            [dec(10, 0), dec(10, 10), RockyType::Int32],
            [unsized_dec(Some(2)), dec(10, 2), dec(10, 4)],
            [unsized_dec(Some(2)), dec(10, 2), RockyType::Int64],
            [RockyType::Int32, RockyType::Float32, dec(10, 2)],
            [RockyType::Unknown, dec(10, 2), RockyType::Int64],
            [
                RockyType::Timestamp,
                RockyType::TimestampNtz,
                RockyType::Date,
            ],
        ];
        for triple in triples {
            let want = sup_of(&triple);
            for permutation in [
                [0, 1, 2],
                [0, 2, 1],
                [1, 0, 2],
                [1, 2, 0],
                [2, 0, 1],
                [2, 1, 0],
            ] {
                let branches = permutation.map(|i| triple[i].clone());
                assert_eq!(sup_of(&branches), want, "{branches:?}");
            }
        }
        assert_eq!(
            sup_of(&[dec(30, 0), dec(30, 30), dec(5, 2)]),
            RockyType::Unknown
        );
        assert_eq!(
            sup_of(&[unsized_dec(Some(2)), dec(10, 2), RockyType::Int64]),
            unsized_dec(Some(2))
        );
        assert_eq!(
            sup_of(&[unsized_dec(Some(2)), dec(10, 2), dec(10, 4)]),
            unsized_dec(None)
        );
    }

    /// The set form and the pair form agree on every pair of the lattice, and
    /// any `Unknown` branch absorbs the set (ADR-TYPES §2).
    #[test]
    fn common_supertype_of_agrees_with_the_pair_form() {
        for a in lattice() {
            for b in lattice() {
                assert_eq!(
                    common_supertype_of(&[a.clone(), b.clone()]),
                    common_supertype(&a, &b).unwrap_or(InferredType::typed(RockyType::Unknown)),
                    "{a:?} {b:?}"
                );
                assert_eq!(
                    sup_of(&[a.clone(), RockyType::Unknown, b.clone()]),
                    RockyType::Unknown
                );
            }
        }
    }
}
