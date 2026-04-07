//! Databricks type mapper for Rocky's type system.
//!
//! Maps Databricks SQL types to Rocky's unified type representation.

use rocky_core::traits::TypeMapper;

/// Databricks type mapper.
#[derive(Debug, Clone, Default)]
pub struct DatabricksTypeMapper;

impl TypeMapper for DatabricksTypeMapper {
    fn normalize_type(&self, warehouse_type: &str) -> String {
        warehouse_type.trim().to_uppercase()
    }

    fn types_compatible(&self, type_a: &str, type_b: &str) -> bool {
        let a = self.normalize_type(type_a);
        let b = self.normalize_type(type_b);

        if a == b {
            return true;
        }

        // Known compatible pairs
        let compatible_groups: &[&[&str]] = &[
            &["STRING", "VARCHAR", "TEXT"],
            &["INT", "INTEGER"],
            &["BIGINT", "LONG"],
            &["FLOAT", "REAL"],
            &["DOUBLE", "DOUBLE PRECISION"],
            &["BOOLEAN", "BOOL"],
            &["TIMESTAMP", "TIMESTAMP_NTZ", "DATETIME"],
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

        // DECIMAL(p1,s1) ≈ DECIMAL(p2,s2) — compatible if same base type
        if a.starts_with("DECIMAL") && b.starts_with("DECIMAL") {
            return true;
        }

        false
    }
}

/// Parse a Databricks type string into a canonical Rocky type name.
///
/// This is used by the compiler to convert `ColumnInfo.data_type` strings
/// into `RockyType` values.
pub fn parse_databricks_type(type_str: &str) -> &'static str {
    let upper = type_str.trim().to_uppercase();
    match upper.as_str() {
        "BOOLEAN" | "BOOL" => "Boolean",
        "TINYINT" | "BYTE" => "Int32",
        "SMALLINT" | "SHORT" => "Int32",
        "INT" | "INTEGER" => "Int32",
        "BIGINT" | "LONG" => "Int64",
        "FLOAT" | "REAL" => "Float32",
        "DOUBLE" | "DOUBLE PRECISION" => "Float64",
        "STRING" | "VARCHAR" | "TEXT" => "String",
        "BINARY" => "Binary",
        "DATE" => "Date",
        "TIMESTAMP" => "Timestamp",
        "TIMESTAMP_NTZ" => "TimestampNtz",
        _ if upper.starts_with("DECIMAL") || upper.starts_with("NUMERIC") => "Decimal",
        _ if upper.starts_with("ARRAY") => "Array",
        _ if upper.starts_with("MAP") => "Map",
        _ if upper.starts_with("STRUCT") => "Struct",
        _ => "Unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize() {
        let mapper = DatabricksTypeMapper;
        assert_eq!(mapper.normalize_type("string"), "STRING");
        assert_eq!(mapper.normalize_type("DECIMAL(10,2)"), "DECIMAL(10,2)");
    }

    #[test]
    fn test_same_type_compatible() {
        let mapper = DatabricksTypeMapper;
        assert!(mapper.types_compatible("STRING", "STRING"));
        assert!(mapper.types_compatible("BIGINT", "BIGINT"));
    }

    #[test]
    fn test_alias_compatible() {
        let mapper = DatabricksTypeMapper;
        assert!(mapper.types_compatible("STRING", "VARCHAR"));
        assert!(mapper.types_compatible("INT", "INTEGER"));
        assert!(mapper.types_compatible("BIGINT", "LONG"));
    }

    #[test]
    fn test_decimal_compatible() {
        let mapper = DatabricksTypeMapper;
        assert!(mapper.types_compatible("DECIMAL(10,2)", "DECIMAL(38,6)"));
    }

    #[test]
    fn test_incompatible() {
        let mapper = DatabricksTypeMapper;
        assert!(!mapper.types_compatible("STRING", "INT"));
        assert!(!mapper.types_compatible("BOOLEAN", "BIGINT"));
    }

    #[test]
    fn test_parse_type() {
        assert_eq!(parse_databricks_type("STRING"), "String");
        assert_eq!(parse_databricks_type("BIGINT"), "Int64");
        assert_eq!(parse_databricks_type("DECIMAL(10,2)"), "Decimal");
        assert_eq!(parse_databricks_type("TIMESTAMP_NTZ"), "TimestampNtz");
        assert_eq!(parse_databricks_type("BOOLEAN"), "Boolean");
        assert_eq!(parse_databricks_type("unknown_type"), "Unknown");
    }
}
