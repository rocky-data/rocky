//! Snowflake type mapper for Rocky's type system.

use rocky_core::traits::TypeMapper;

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
