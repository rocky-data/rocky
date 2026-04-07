//! Rocky's unified type system for compile-time type checking.
//!
//! `RockyType` is the canonical type representation that all warehouse-specific
//! types map to. The `TypeMapper` trait (defined in `rocky-core/traits.rs`)
//! handles bidirectional mapping between warehouse types and `RockyType`.

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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypedColumn {
    pub name: std::string::String,
    pub data_type: RockyType,
    pub nullable: bool,
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

    common_supertype(from, to).is_some_and(|sup| sup == *to)
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
    fn test_timestamp_variants() {
        assert_eq!(
            common_supertype(&RockyType::Timestamp, &RockyType::TimestampNtz),
            Some(RockyType::Timestamp)
        );
    }
}
