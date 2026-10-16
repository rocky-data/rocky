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

/// Compute the common supertype of two types (for COALESCE, CASE, UNION).
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
        // Int → Decimal
        (RockyType::Int32, RockyType::Decimal { precision, scale })
        | (RockyType::Decimal { precision, scale }, RockyType::Int32) => Some(RockyType::Decimal {
            precision: (*precision).max(10),
            scale: *scale,
        }),
        (RockyType::Int64, RockyType::Decimal { precision, scale })
        | (RockyType::Decimal { precision, scale }, RockyType::Int64) => Some(RockyType::Decimal {
            precision: (*precision).max(19),
            scale: *scale,
        }),
        // Decimal + Decimal
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
        // A decimal with unknown precision absorbs every exact numeric: the
        // result is a decimal, but its precision is as unknown as the
        // unsized side's (#2356). An integer keeps the decimal's scale. Two
        // decimals keep a scale only when both know it and it is the same:
        // with different scales a warehouse that caps the precision at 38
        // may also cut the scale, so Rocky does not claim one (#2398).
        (RockyType::DecimalUnsized { scale }, other)
        | (other, RockyType::DecimalUnsized { scale })
            if other.is_integer() || other.is_decimal() =>
        {
            let scale = if other.is_integer() {
                *scale
            } else {
                scale.filter(|s| other.decimal_scale() == Some(*s))
            };
            Some(RockyType::DecimalUnsized { scale })
        }
        // Timestamp variants
        (RockyType::Timestamp, RockyType::TimestampNtz)
        | (RockyType::TimestampNtz, RockyType::Timestamp) => Some(RockyType::Timestamp),

        _ => None,
    }
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
        _ => common_supertype(from, to).is_some_and(|sup| sup == *to),
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

    #[test]
    fn test_int_to_decimal() {
        let result = common_supertype(
            &RockyType::Int64,
            &RockyType::Decimal {
                precision: 10,
                scale: 2,
            },
        );
        assert!(matches!(
            result,
            Some(RockyType::Decimal {
                precision: 19,
                scale: 2
            })
        ));
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
            RockyType::Unknown,
        ] {
            assert_eq!(
                common_supertype(&unsized_decimal, &other),
                Some(unsized_decimal.clone()),
                "{other:?}"
            );
            assert_eq!(
                common_supertype(&other, &unsized_decimal),
                Some(unsized_decimal.clone()),
                "{other:?}"
            );
        }
        for other in [RockyType::String, RockyType::Float64, RockyType::Date] {
            assert_eq!(
                common_supertype(&unsized_decimal, &other),
                None,
                "{other:?}"
            );
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
            assert_eq!(common_supertype(&a, &b), Some(want.clone()), "{a:?} {b:?}");
            assert_eq!(common_supertype(&b, &a), Some(want), "{b:?} {a:?}");
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
            common_supertype(&RockyType::Timestamp, &RockyType::TimestampNtz),
            Some(RockyType::Timestamp)
        );
    }
}
