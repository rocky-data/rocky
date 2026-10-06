//! Native type names → the canonical spelling `describe_table` reports.
//!
//! PostgreSQL's `format_type()` and Redshift's `svv_columns` spell types in
//! long form (`character varying(40)`, `timestamp without time zone`). The
//! adapter reports them in the short upper-case form that is BOTH valid DDL
//! on the warehouse (drift replays it in `ALTER COLUMN … TYPE`) AND the
//! vocabulary Rocky's type mappers read (`INTEGER`, `BIGINT`, `TEXT`,
//! `NUMERIC(12,2)`, `TIMESTAMP`, …), so a source column grounds type
//! inference and contracts.
//!
//! What stays `Unknown` to the compiler, deliberately: `VARCHAR(n)`,
//! `TIMESTAMPTZ`, bare `NUMERIC` (arbitrary precision on PostgreSQL — no
//! digits to claim), `JSONB`, `SUPER`, arrays. `Unknown` is the conservative
//! outcome: a contract reports "not checked" rather than a false mismatch.

/// Canonical spelling for a `format_type()` / `svv_columns` type name.
#[must_use]
pub fn canonical_type(raw: &str) -> String {
    let lower = raw.trim().to_ascii_lowercase();
    if let Some(element) = lower.strip_suffix("[]") {
        return format!("{}[]", canonical_type(element));
    }
    let (base, modifier) = match lower.split_once('(') {
        Some((b, rest)) => {
            // `timestamp(3) without time zone` carries words after `)`.
            let (args, tail) = rest.split_once(')').unwrap_or((rest, ""));
            (format!("{}{}", b.trim(), tail), Some(args.replace(' ', "")))
        }
        None => (lower.clone(), None),
    };
    let base = base.trim();
    let with_mod = |name: &str| match &modifier {
        Some(m) => format!("{name}({m})"),
        None => name.to_string(),
    };
    match base {
        "smallint" | "int2" => "SMALLINT".into(),
        "integer" | "int" | "int4" => "INTEGER".into(),
        "bigint" | "int8" => "BIGINT".into(),
        "boolean" | "bool" => "BOOLEAN".into(),
        "real" | "float4" => "REAL".into(),
        "double precision" | "float8" => "DOUBLE PRECISION".into(),
        "text" => "TEXT".into(),
        "character varying" | "varchar" => with_mod("VARCHAR"),
        "character" | "char" | "bpchar" => with_mod("CHAR"),
        "numeric" | "decimal" => with_mod("NUMERIC"),
        "date" => "DATE".into(),
        // Precision is dropped: Rocky's vocabulary has one TIMESTAMP, and
        // drift never replays a timestamp type (it is not on either
        // dialect's widening allowlist).
        "timestamp" | "timestamp without time zone" => "TIMESTAMP".into(),
        "timestamptz" | "timestamp with time zone" => "TIMESTAMPTZ".into(),
        "time" | "time without time zone" => "TIME".into(),
        "timetz" | "time with time zone" => "TIMETZ".into(),
        _ => match &modifier {
            Some(m) => format!("{}({m})", base.to_ascii_uppercase()),
            None => base.to_ascii_uppercase(),
        },
    }
}

/// Rebuild a type name from `information_schema`-style parts (Redshift's
/// `svv_columns`): `data_type` plus the length / precision / scale columns.
#[must_use]
pub fn type_from_parts(
    data_type: &str,
    char_max_length: Option<&str>,
    numeric_precision: Option<&str>,
    numeric_scale: Option<&str>,
) -> String {
    let lower = data_type.trim().to_ascii_lowercase();
    let raw = match lower.as_str() {
        "character varying" | "character" => match char_max_length {
            Some(n) => format!("{lower}({n})"),
            None => lower.clone(),
        },
        "numeric" => match (numeric_precision, numeric_scale) {
            (Some(p), Some(s)) => format!("numeric({p},{s})"),
            (Some(p), None) => format!("numeric({p},0)"),
            _ => lower.clone(),
        },
        _ => lower.clone(),
    };
    canonical_type(&raw)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_type_spellings_canonicalize() {
        let cases = [
            ("integer", "INTEGER"),
            ("bigint", "BIGINT"),
            ("smallint", "SMALLINT"),
            ("boolean", "BOOLEAN"),
            ("real", "REAL"),
            ("double precision", "DOUBLE PRECISION"),
            ("text", "TEXT"),
            ("character varying(40)", "VARCHAR(40)"),
            ("character varying", "VARCHAR"),
            ("character(3)", "CHAR(3)"),
            ("numeric(12,2)", "NUMERIC(12,2)"),
            ("numeric", "NUMERIC"),
            ("date", "DATE"),
            ("timestamp without time zone", "TIMESTAMP"),
            ("timestamp(3) without time zone", "TIMESTAMP"),
            ("timestamp with time zone", "TIMESTAMPTZ"),
            ("jsonb", "JSONB"),
            ("bytea", "BYTEA"),
            ("uuid", "UUID"),
            ("super", "SUPER"),
            ("integer[]", "INTEGER[]"),
            ("character varying(40)[]", "VARCHAR(40)[]"),
        ];
        for (raw, want) in cases {
            assert_eq!(canonical_type(raw), want, "{raw}");
        }
    }

    #[test]
    fn svv_columns_parts_rebuild() {
        assert_eq!(
            type_from_parts("character varying", Some("256"), None, None),
            "VARCHAR(256)"
        );
        assert_eq!(
            type_from_parts("numeric", None, Some("18"), Some("4")),
            "NUMERIC(18,4)"
        );
        assert_eq!(
            type_from_parts("integer", None, Some("32"), Some("0")),
            "INTEGER"
        );
        assert_eq!(
            type_from_parts("timestamp without time zone", None, None, None),
            "TIMESTAMP"
        );
    }

    /// The canonical names are the ones the compiler's mapper reads, so a
    /// Postgres source column grounds inference with a concrete type.
    #[test]
    fn canonical_names_ground_the_compiler_mapper() {
        use rocky_core::contracts::warehouse_type_to_rocky;
        use rocky_ir::types::RockyType;
        assert_eq!(
            warehouse_type_to_rocky(&canonical_type("integer")),
            RockyType::Int32
        );
        assert_eq!(
            warehouse_type_to_rocky(&canonical_type("bigint")),
            RockyType::Int64
        );
        assert_eq!(
            warehouse_type_to_rocky(&canonical_type("text")),
            RockyType::String
        );
        assert_eq!(
            warehouse_type_to_rocky(&canonical_type("double precision")),
            RockyType::Float64
        );
        assert_eq!(
            warehouse_type_to_rocky(&canonical_type("numeric(12,2)")),
            RockyType::Decimal {
                precision: 12,
                scale: 2
            }
        );
        assert_eq!(
            warehouse_type_to_rocky(&canonical_type("timestamp without time zone")),
            RockyType::Timestamp
        );
        // No digits on a bare PostgreSQL numeric: stays Unknown.
        assert_eq!(
            warehouse_type_to_rocky(&canonical_type("numeric")),
            RockyType::Unknown
        );
    }
}
