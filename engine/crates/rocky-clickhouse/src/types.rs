//! ClickHouse type names → the spelling `describe_table` reports.
//!
//! `system.columns.type` spells a column's full type, wrappers included
//! (`LowCardinality(Nullable(String))`). The adapter splits that into a
//! nullability flag and a type name that is BOTH valid ClickHouse DDL (drift
//! replays it in `ALTER TABLE … MODIFY COLUMN`) AND, where a Rocky type
//! means exactly the same thing, the vocabulary Rocky's type mapper reads
//! (`BIGINT`, `TEXT`, `DECIMAL(18,2)`, `TIMESTAMP`, …). Every canonical name
//! below is a documented, case-insensitive ClickHouse alias of the native
//! type, so replaying it builds the same column.
//!
//! | ClickHouse | Reported | Rocky type |
//! |---|---|---|
//! | `Int8` / `Int16` / `Int32` / `Int64` | `TINYINT` / `SMALLINT` / `INTEGER` / `BIGINT` | Int32 / Int32 / Int32 / Int64 |
//! | `Float32` / `Float64` | `REAL` / `DOUBLE` | Float32 / Float64 |
//! | `Bool` | `BOOLEAN` | Boolean |
//! | `String` | `TEXT` | String |
//! | `Date` | `DATE` | Date |
//! | `DateTime`, `DateTime('tz')` | `TIMESTAMP` | Timestamp |
//! | `DateTime64(p[, 'tz'])` | `DateTime64(p)` | Timestamp |
//! | `Decimal(p, s)` | `DECIMAL(p,s)` | Decimal |
//! | `Nullable(T)` | `T`, nullable | as `T` |
//! | `LowCardinality(T)` | `T` | as `T` |
//!
//! Everything else keeps its native spelling and stays `Unknown` to the
//! compiler, deliberately: unsigned and 128/256-bit integers (no Rocky type
//! holds them exactly), `FixedString(n)`, `Date32`, `UUID`, `Enum8(…)`,
//! `Array(…)`, `Map(…)`, `Tuple(…)`, `JSON`. `Unknown` is the conservative
//! outcome: a contract reports "not checked" rather than a false mismatch.
//!
//! A `DateTime` column's time zone is display metadata — the stored value
//! is an instant — so it is dropped from the reported name. `DateTime64`
//! keeps its precision, so drift sees a precision change.

/// A column type split into the reported name and its nullability.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnType {
    /// The canonical (or native) type name.
    pub data_type: String,
    /// Whether the column was `Nullable(…)`.
    pub nullable: bool,
}

/// Split a `system.columns.type` value into its canonical name and
/// nullability, peeling `LowCardinality(…)` and `Nullable(…)` in either
/// order.
#[must_use]
pub fn column_type(raw: &str) -> ColumnType {
    let mut inner = raw.trim();
    let mut nullable = false;
    loop {
        if let Some(rest) = unwrap_call(inner, "Nullable") {
            nullable = true;
            inner = rest;
        } else if let Some(rest) = unwrap_call(inner, "LowCardinality") {
            inner = rest;
        } else {
            break;
        }
    }
    ColumnType {
        data_type: canonical_type(inner),
        nullable,
    }
}

/// `Name(inner)` → `inner`, when the whole string is that one call.
fn unwrap_call<'a>(raw: &'a str, name: &str) -> Option<&'a str> {
    let rest = raw.strip_prefix(name)?.strip_prefix('(')?;
    let inner = rest.strip_suffix(')')?;
    // `Nullable(A) X(B)` is not one call: the parentheses must balance
    // only at the very end.
    let mut depth = 0i32;
    for c in inner.chars() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth < 0 {
                    return None;
                }
            }
            _ => {}
        }
    }
    (depth == 0).then(|| inner.trim())
}

/// Canonical name for one (unwrapped) ClickHouse type.
#[must_use]
pub fn canonical_type(raw: &str) -> String {
    let raw = raw.trim();
    match raw {
        "Int8" => return "TINYINT".into(),
        "Int16" => return "SMALLINT".into(),
        "Int32" => return "INTEGER".into(),
        "Int64" => return "BIGINT".into(),
        "Float32" => return "REAL".into(),
        "Float64" => return "DOUBLE".into(),
        "Bool" => return "BOOLEAN".into(),
        "String" => return "TEXT".into(),
        "Date" => return "DATE".into(),
        "DateTime" => return "TIMESTAMP".into(),
        _ => {}
    }
    if let Some(args) = unwrap_call(raw, "DateTime") {
        // `DateTime('Europe/Berlin')`: a time zone is the only argument.
        if args.starts_with('\'') {
            return "TIMESTAMP".into();
        }
    }
    if let Some(args) = unwrap_call(raw, "DateTime64") {
        // `DateTime64(3)` or `DateTime64(3, 'UTC')`.
        let precision = args.split(',').next().unwrap_or("").trim();
        if !precision.is_empty() && precision.chars().all(|c| c.is_ascii_digit()) {
            return format!("DateTime64({precision})");
        }
    }
    if let Some(args) = unwrap_call(raw, "Decimal") {
        let parts: Vec<&str> = args.split(',').map(str::trim).collect();
        if let [p, s] = parts.as_slice()
            && p.parse::<u8>().is_ok()
            && s.parse::<u8>().is_ok()
        {
            return format!("DECIMAL({p},{s})");
        }
    }
    raw.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrappers_peel_into_nullability() {
        let cases = [
            ("Int64", "BIGINT", false),
            ("Nullable(Int64)", "BIGINT", true),
            ("LowCardinality(String)", "TEXT", false),
            ("LowCardinality(Nullable(String))", "TEXT", true),
            ("Nullable(DateTime64(3, 'UTC'))", "DateTime64(3)", true),
            ("Array(Nullable(Int32))", "Array(Nullable(Int32))", false),
            ("Map(String, UInt64)", "Map(String, UInt64)", false),
        ];
        for (raw, ty, nullable) in cases {
            assert_eq!(
                column_type(raw),
                ColumnType {
                    data_type: ty.into(),
                    nullable
                },
                "{raw}"
            );
        }
    }

    #[test]
    fn native_names_canonicalize() {
        let cases = [
            ("Int8", "TINYINT"),
            ("Int16", "SMALLINT"),
            ("Int32", "INTEGER"),
            ("Int64", "BIGINT"),
            ("UInt64", "UInt64"),
            ("Int128", "Int128"),
            ("Float32", "REAL"),
            ("Float64", "DOUBLE"),
            ("Bool", "BOOLEAN"),
            ("String", "TEXT"),
            ("FixedString(3)", "FixedString(3)"),
            ("Date", "DATE"),
            ("Date32", "Date32"),
            ("DateTime", "TIMESTAMP"),
            ("DateTime('Europe/Berlin')", "TIMESTAMP"),
            ("DateTime64(6)", "DateTime64(6)"),
            ("Decimal(18, 2)", "DECIMAL(18,2)"),
            ("Decimal(76, 10)", "DECIMAL(76,10)"),
            ("UUID", "UUID"),
            ("Enum8('a' = 1)", "Enum8('a' = 1)"),
        ];
        for (raw, want) in cases {
            assert_eq!(canonical_type(raw), want, "{raw}");
        }
    }

    /// The canonical names are the ones the compiler's mapper reads, so a
    /// ClickHouse column grounds inference with a concrete type — and the
    /// ones that are not exact stay `Unknown`.
    #[test]
    fn canonical_names_ground_the_compiler_mapper() {
        use rocky_core::contracts::warehouse_type_to_rocky;
        use rocky_ir::types::RockyType;
        let rocky = |raw: &str| warehouse_type_to_rocky(&column_type(raw).data_type);
        assert_eq!(rocky("Int32"), RockyType::Int32);
        assert_eq!(rocky("Nullable(Int64)"), RockyType::Int64);
        assert_eq!(rocky("LowCardinality(String)"), RockyType::String);
        assert_eq!(rocky("Float64"), RockyType::Float64);
        assert_eq!(rocky("Float32"), RockyType::Float32);
        assert_eq!(rocky("Bool"), RockyType::Boolean);
        assert_eq!(rocky("Date"), RockyType::Date);
        assert_eq!(rocky("DateTime"), RockyType::Timestamp);
        assert_eq!(rocky("DateTime64(3, 'UTC')"), RockyType::Timestamp);
        assert_eq!(
            rocky("Decimal(12, 2)"),
            RockyType::Decimal {
                precision: 12,
                scale: 2
            }
        );
        assert_eq!(rocky("UInt64"), RockyType::Unknown);
        assert_eq!(rocky("Array(Int32)"), RockyType::Unknown);
    }
}
