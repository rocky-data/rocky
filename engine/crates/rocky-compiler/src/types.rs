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
