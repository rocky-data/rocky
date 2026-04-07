//! DuckDB type mapper for Rocky's type system.

use rocky_core::traits::TypeMapper;

/// DuckDB type mapper.
#[derive(Debug, Clone, Default)]
pub struct DuckDbTypeMapper;

impl TypeMapper for DuckDbTypeMapper {
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
            &["VARCHAR", "TEXT", "STRING"],
            &["INTEGER", "INT", "INT4", "SIGNED"],
            &["BIGINT", "INT8", "LONG"],
            &["SMALLINT", "INT2", "SHORT"],
            &["TINYINT", "INT1"],
            &["FLOAT", "FLOAT4", "REAL"],
            &["DOUBLE", "FLOAT8", "DOUBLE PRECISION"],
            &["BOOLEAN", "BOOL", "LOGICAL"],
            &["TIMESTAMP", "DATETIME", "TIMESTAMP WITH TIME ZONE"],
        ];

        for group in compatible_groups {
            let a_in = group.iter().any(|t| a == *t);
            let b_in = group.iter().any(|t| b == *t);
            if a_in && b_in {
                return true;
            }
        }

        if a.starts_with("DECIMAL") && b.starts_with("DECIMAL") {
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
        let mapper = DuckDbTypeMapper;
        assert!(mapper.types_compatible("VARCHAR", "TEXT"));
        assert!(mapper.types_compatible("VARCHAR", "STRING"));
    }

    #[test]
    fn test_integer_aliases() {
        let mapper = DuckDbTypeMapper;
        assert!(mapper.types_compatible("INTEGER", "INT4"));
        assert!(mapper.types_compatible("BIGINT", "INT8"));
    }

    #[test]
    fn test_incompatible() {
        let mapper = DuckDbTypeMapper;
        assert!(!mapper.types_compatible("VARCHAR", "INTEGER"));
    }
}
