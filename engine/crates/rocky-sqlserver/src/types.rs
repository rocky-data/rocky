//! SQL Server type names → the canonical spelling `describe_table` reports.
//!
//! `INFORMATION_SCHEMA.COLUMNS` splits a type into `DATA_TYPE` plus length /
//! precision / scale columns. The adapter rebuilds one upper-case name that
//! is BOTH valid T-SQL DDL (drift replays it in `ALTER TABLE … ALTER COLUMN`
//! and `ADD`) AND, where Rocky's vocabulary has the type, the name its mapper
//! reads, so a source column grounds type inference and contracts.
//!
//! - `float` is reported as `DOUBLE PRECISION` (a T-SQL synonym for
//!   `float(53)`): Rocky's mapper reads a bare `FLOAT` as 32-bit, and a SQL
//!   Server `float` is 64-bit. `float(1..24)` is stored as `real` and the
//!   catalog already reports it as `real`.
//! - `datetime` keeps its name (Rocky reads it as a timezone-naive
//!   timestamp, which it is).
//! - Deliberately `Unknown` to the compiler: `BIT` (Rocky has no 0/1 type
//!   and `BOOLEAN` is not T-SQL DDL), `DATETIME2(n)`, `DATETIMEOFFSET(n)`,
//!   `(N)VARCHAR(n)`, `MONEY`, `UNIQUEIDENTIFIER`. `Unknown` is the
//!   conservative outcome: a contract reports "not checked" rather than a
//!   false mismatch.

/// Rebuild a type name from `INFORMATION_SCHEMA.COLUMNS` parts.
///
/// `char_max_length` is `-1` for `(n)varchar(max)` / `varbinary(max)`.
#[must_use]
pub fn type_from_parts(
    data_type: &str,
    char_max_length: Option<&str>,
    numeric_precision: Option<&str>,
    numeric_scale: Option<&str>,
    datetime_precision: Option<&str>,
) -> String {
    let base = data_type.trim().to_ascii_lowercase();
    let len = || match char_max_length.map(str::trim) {
        Some("-1") => "(MAX)".to_string(),
        Some(n) if !n.is_empty() => format!("({n})"),
        _ => String::new(),
    };
    let frac = || match datetime_precision.map(str::trim) {
        Some(p) if !p.is_empty() => format!("({p})"),
        _ => String::new(),
    };
    match base.as_str() {
        "bit" => "BIT".into(),
        "tinyint" => "TINYINT".into(),
        "smallint" => "SMALLINT".into(),
        "int" => "INT".into(),
        "bigint" => "BIGINT".into(),
        "real" => "REAL".into(),
        "float" => match numeric_precision.and_then(|p| p.trim().parse::<u32>().ok()) {
            Some(p) if p <= 24 => "REAL".into(),
            _ => "DOUBLE PRECISION".into(),
        },
        "decimal" | "numeric" => match (numeric_precision, numeric_scale) {
            (Some(p), Some(s)) => format!("DECIMAL({},{})", p.trim(), s.trim()),
            (Some(p), None) => format!("DECIMAL({},0)", p.trim()),
            _ => "DECIMAL(18,0)".into(),
        },
        "money" => "MONEY".into(),
        "smallmoney" => "SMALLMONEY".into(),
        "char" | "varchar" | "nchar" | "nvarchar" | "binary" | "varbinary" => {
            format!("{}{}", base.to_ascii_uppercase(), len())
        }
        "date" => "DATE".into(),
        "datetime" => "DATETIME".into(),
        "smalldatetime" => "SMALLDATETIME".into(),
        "datetime2" | "datetimeoffset" | "time" => {
            format!("{}{}", base.to_ascii_uppercase(), frac())
        }
        other => other.to_ascii_uppercase(),
    }
}

/// Translate a portable type name (a model's `metadata_columns` type, say)
/// to the T-SQL type that holds the same values.
///
/// A bare `VARCHAR` would be `VARCHAR(30)` inside a T-SQL `CAST` — a silent
/// truncation — and `TIMESTAMP` is SQL Server's `rowversion`, not a date.
/// Anything else is returned unchanged.
#[must_use]
pub fn tsql_type(portable: &str, fabric: bool) -> String {
    let upper = portable.trim().to_ascii_uppercase();
    match upper.as_str() {
        "STRING" | "TEXT" | "VARCHAR" => {
            if fabric {
                "VARCHAR(8000)".into()
            } else {
                "NVARCHAR(4000)".into()
            }
        }
        "TIMESTAMP" | "TIMESTAMP_NTZ" => "DATETIME2(6)".into(),
        "TIMESTAMPTZ" | "TIMESTAMP_TZ" | "TIMESTAMP WITH TIME ZONE" => "DATETIMEOFFSET(6)".into(),
        "BOOLEAN" | "BOOL" => "BIT".into(),
        "DOUBLE" | "FLOAT64" => "FLOAT".into(),
        "INT64" | "LONG" => "BIGINT".into(),
        "INTEGER" | "INT32" => "INT".into(),
        _ => upper,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn information_schema_parts_rebuild() {
        let cases = [
            (("int", None, Some("10"), Some("0"), None), "INT"),
            (("bigint", None, Some("19"), Some("0"), None), "BIGINT"),
            (("bit", None, None, None, None), "BIT"),
            (("float", None, Some("53"), None, None), "DOUBLE PRECISION"),
            (("float", None, Some("24"), None, None), "REAL"),
            (
                ("decimal", None, Some("12"), Some("2"), None),
                "DECIMAL(12,2)",
            ),
            (
                ("numeric", None, Some("38"), Some("0"), None),
                "DECIMAL(38,0)",
            ),
            (("nvarchar", Some("200"), None, None, None), "NVARCHAR(200)"),
            (("nvarchar", Some("-1"), None, None, None), "NVARCHAR(MAX)"),
            (("varchar", Some("50"), None, None, None), "VARCHAR(50)"),
            (
                ("varbinary", Some("-1"), None, None, None),
                "VARBINARY(MAX)",
            ),
            (("datetime2", None, None, None, Some("7")), "DATETIME2(7)"),
            (
                ("datetimeoffset", None, None, None, Some("3")),
                "DATETIMEOFFSET(3)",
            ),
            (("datetime", None, None, None, Some("3")), "DATETIME"),
            (("date", None, None, None, Some("0")), "DATE"),
            (
                ("uniqueidentifier", None, None, None, None),
                "UNIQUEIDENTIFIER",
            ),
        ];
        for ((t, l, p, s, d), want) in cases {
            assert_eq!(type_from_parts(t, l, p, s, d), want, "{t}");
        }
    }

    /// The canonical names the compiler's mapper reads ground a SQL Server
    /// source column with the right concrete type; the rest stay Unknown.
    #[test]
    fn canonical_names_ground_the_compiler_mapper() {
        use rocky_core::contracts::warehouse_type_to_rocky;
        use rocky_ir::types::RockyType;
        let t = |dt: &str, p: Option<&str>, s: Option<&str>| {
            warehouse_type_to_rocky(&type_from_parts(dt, None, p, s, None))
        };
        assert_eq!(t("int", None, None), RockyType::Int32);
        assert_eq!(t("bigint", None, None), RockyType::Int64);
        assert_eq!(t("float", Some("53"), None), RockyType::Float64);
        assert_eq!(t("real", Some("24"), None), RockyType::Float32);
        assert_eq!(
            t("decimal", Some("12"), Some("2")),
            RockyType::Decimal {
                precision: 12,
                scale: 2
            }
        );
        assert_eq!(t("date", None, None), RockyType::Date);
        assert_eq!(t("datetime", None, None), RockyType::TimestampNtz);
        assert_eq!(t("bit", None, None), RockyType::Unknown);
        assert_eq!(t("uniqueidentifier", None, None), RockyType::Unknown);
    }

    #[test]
    fn portable_types_translate() {
        assert_eq!(tsql_type("VARCHAR", false), "NVARCHAR(4000)");
        assert_eq!(tsql_type("string", true), "VARCHAR(8000)");
        assert_eq!(tsql_type("TIMESTAMP", false), "DATETIME2(6)");
        assert_eq!(tsql_type("BOOLEAN", false), "BIT");
        assert_eq!(tsql_type("DECIMAL(10,2)", false), "DECIMAL(10,2)");
        assert_eq!(tsql_type("NVARCHAR(50)", false), "NVARCHAR(50)");
    }
}
