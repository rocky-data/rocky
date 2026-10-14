//! Snowflake type mapper for Rocky's type system.

use rocky_core::traits::TypeMapper;

/// The type string Rocky stores for a column Snowflake reports as
/// `reported`: a `DESCRIBE TABLE` type, or the full type
/// [`InformationSchemaType::describe_type`] rebuilds from
/// `INFORMATION_SCHEMA.COLUMNS`.
///
/// Every Snowflake float is 64-bit: `FLOAT`, `FLOAT4`, `FLOAT8`, `REAL`,
/// `DOUBLE` and `DOUBLE PRECISION` are one type, which `DESCRIBE` reports as
/// `FLOAT`. Rocky's dialect-free normaliser reads a bare `FLOAT` as 32-bit,
/// so the float names become `DOUBLE PRECISION` here, the spelling it reads
/// as 64-bit. That is the width the compiler gives a Snowflake
/// `CAST(x AS FLOAT)` (#2333), so a source schema, the schema cache and
/// drift agree with the model. The SQL Server adapter does the same for its
/// `float`. Every other type is returned trimmed, as reported.
pub fn canonical_type(reported: &str) -> String {
    let trimmed = reported.trim();
    match trimmed.to_ascii_uppercase().as_str() {
        "FLOAT" | "FLOAT4" | "FLOAT8" | "REAL" | "DOUBLE" | "DOUBLE PRECISION" => {
            "DOUBLE PRECISION".to_string()
        }
        _ => trimmed.to_string(),
    }
}

/// The type columns of one `INFORMATION_SCHEMA.COLUMNS` row.
///
/// `DATA_TYPE` holds the bare type name (`NUMBER`, `TEXT`,
/// `TIMESTAMP_NTZ`). The precision, scale and length sit in their own
/// columns. [`Self::describe_type`] puts them back together (#2344).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InformationSchemaType {
    /// `DATA_TYPE`.
    pub data_type: String,
    /// `CHARACTER_MAXIMUM_LENGTH`.
    pub character_maximum_length: Option<u64>,
    /// `NUMERIC_PRECISION`.
    pub numeric_precision: Option<u64>,
    /// `NUMERIC_SCALE`.
    pub numeric_scale: Option<u64>,
    /// `DATETIME_PRECISION`.
    pub datetime_precision: Option<u64>,
}

impl InformationSchemaType {
    /// The type `DESCRIBE TABLE` prints for this column.
    ///
    /// Drift compares the batch spelling with the `DESCRIBE` spelling, and
    /// the safe-widening check needs the precision or the length. So the
    /// batch describe must return what `DESCRIBE` returns. The parameters
    /// come from these columns:
    ///
    /// - `NUMBER`: `NUMERIC_PRECISION` and `NUMERIC_SCALE` give
    ///   `NUMBER(p,s)`.
    /// - `TEXT`: `CHARACTER_MAXIMUM_LENGTH` gives `VARCHAR(n)`. `DESCRIBE`
    ///   prints every text type as `VARCHAR`.
    /// - `BINARY`: `CHARACTER_MAXIMUM_LENGTH` gives `BINARY(n)`.
    /// - `DECFLOAT`: `NUMERIC_PRECISION` gives `DECFLOAT(p)`.
    /// - `TIME`, `TIMESTAMP_NTZ`, `TIMESTAMP_LTZ`, `TIMESTAMP_TZ`: the
    ///   fractional-seconds precision gives `TIMESTAMP_NTZ(p)`. Snowflake
    ///   documents `DATETIME_PRECISION` as not applicable, and
    ///   `SHOW COLUMNS` reports this precision as the type's `scale`, so
    ///   `NUMERIC_SCALE` is the fallback.
    ///
    /// A parameter that is NULL leaves the type bare, as `DATA_TYPE`
    /// reports it. No default is guessed. Every other type (`FLOAT`,
    /// `BOOLEAN`, `DATE`, `VARIANT`, `OBJECT`, `ARRAY`, `GEOGRAPHY`,
    /// `GEOMETRY`, `VECTOR`) is returned as reported.
    /// `INFORMATION_SCHEMA` has no column for a `VECTOR` element type and
    /// dimension, so that type stays bare. Pass the result through
    /// [`canonical_type`], as the `DESCRIBE` path does.
    pub fn describe_type(&self) -> String {
        let data_type = self.data_type.trim();
        let upper = data_type.to_ascii_uppercase();
        let built = match upper.as_str() {
            "NUMBER" => match (self.numeric_precision, self.numeric_scale) {
                (Some(p), Some(s)) => Some(format!("NUMBER({p},{s})")),
                _ => None,
            },
            "TEXT" => self
                .character_maximum_length
                .map(|n| format!("VARCHAR({n})")),
            "BINARY" => self
                .character_maximum_length
                .map(|n| format!("BINARY({n})")),
            "DECFLOAT" => self.numeric_precision.map(|p| format!("DECFLOAT({p})")),
            "TIME" | "TIMESTAMP_NTZ" | "TIMESTAMP_LTZ" | "TIMESTAMP_TZ" => self
                .datetime_precision
                .or(self.numeric_scale)
                .map(|p| format!("{upper}({p})")),
            _ => None,
        };
        built.unwrap_or_else(|| data_type.to_string())
    }
}

/// Snowflake type mapper.
#[derive(Debug, Clone, Default)]
pub struct SnowflakeTypeMapper;

impl TypeMapper for SnowflakeTypeMapper {
    fn normalize_type(&self, warehouse_type: &str) -> String {
        warehouse_type.trim().to_uppercase()
    }

    fn types_compatible(&self, type_a: &str, type_b: &str) -> bool {
        let a = self.normalize_type(type_a);
        let b = self.normalize_type(type_b);

        if a == b {
            return true;
        }

        let compatible_groups: &[&[&str]] = &[
            &["VARCHAR", "STRING", "TEXT", "CHAR"],
            &[
                "NUMBER", "NUMERIC", "DECIMAL", "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT",
                "BYTEINT",
            ],
            &[
                "FLOAT",
                "FLOAT4",
                "FLOAT8",
                "DOUBLE",
                "DOUBLE PRECISION",
                "REAL",
            ],
            &["BOOLEAN"],
            &["DATE"],
            &["TIMESTAMP_NTZ", "DATETIME", "TIMESTAMP WITHOUT TIME ZONE"],
            &["TIMESTAMP_TZ", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP_LTZ"],
            &["VARIANT"],
            &["BINARY", "VARBINARY"],
        ];

        for group in compatible_groups {
            let a_in = group
                .iter()
                .any(|t| a == *t || a.starts_with(&format!("{t}(")));
            let b_in = group
                .iter()
                .any(|t| b == *t || b.starts_with(&format!("{t}(")));
            if a_in && b_in {
                return true;
            }
        }

        // NUMBER(p1,s1) ≈ NUMBER(p2,s2)
        if (a.starts_with("NUMBER") || a.starts_with("DECIMAL"))
            && (b.starts_with("NUMBER") || b.starts_with("DECIMAL"))
        {
            return true;
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #2333: a Snowflake float column reads as `Float64`, the width a
    /// Snowflake `CAST(x AS FLOAT)` has, so a model and the table it wrote
    /// do not disagree.
    #[test]
    fn a_reported_float_reads_as_64_bit() {
        use rocky_core::contracts::warehouse_type_to_rocky;
        use rocky_ir::types::RockyType;
        for reported in [
            "FLOAT",
            "float",
            " FLOAT4 ",
            "FLOAT8",
            "REAL",
            "DOUBLE",
            "DOUBLE PRECISION",
        ] {
            assert_eq!(
                warehouse_type_to_rocky(&canonical_type(reported)),
                RockyType::Float64,
                "{reported}"
            );
        }
        assert_eq!(canonical_type(" NUMBER(38,0) "), "NUMBER(38,0)");
        assert_eq!(canonical_type("TIMESTAMP_NTZ(9)"), "TIMESTAMP_NTZ(9)");
        let mapper = SnowflakeTypeMapper;
        assert!(mapper.types_compatible(&canonical_type("FLOAT"), "FLOAT"));
    }

    #[test]
    fn test_varchar_aliases() {
        let mapper = SnowflakeTypeMapper;
        assert!(mapper.types_compatible("VARCHAR", "STRING"));
        assert!(mapper.types_compatible("VARCHAR", "TEXT"));
    }

    #[test]
    fn test_number_aliases() {
        let mapper = SnowflakeTypeMapper;
        assert!(mapper.types_compatible("NUMBER", "INT"));
        assert!(mapper.types_compatible("NUMBER(38,0)", "DECIMAL(38,0)"));
        assert!(mapper.types_compatible("INTEGER", "BIGINT"));
    }

    #[test]
    fn test_variant() {
        let mapper = SnowflakeTypeMapper;
        assert!(mapper.types_compatible("VARIANT", "VARIANT"));
    }

    #[test]
    fn test_incompatible() {
        let mapper = SnowflakeTypeMapper;
        assert!(!mapper.types_compatible("VARCHAR", "NUMBER"));
        assert!(!mapper.types_compatible("BOOLEAN", "VARIANT"));
    }

    #[test]
    fn test_timestamp_variants() {
        let mapper = SnowflakeTypeMapper;
        assert!(mapper.types_compatible("TIMESTAMP_NTZ", "DATETIME"));
        // TZ and NTZ are different groups intentionally
        assert!(!mapper.types_compatible("TIMESTAMP_NTZ", "TIMESTAMP_TZ"));
    }
}
