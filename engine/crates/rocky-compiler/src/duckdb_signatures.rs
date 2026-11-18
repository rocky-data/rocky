//! What the bundled DuckDB refuses at bind time, for the call checks
//! [`E068`](crate::diagnostic::E068) (argument count) and
//! [`E069`](crate::diagnostic::E069) (argument type) (#2659).
//!
//! The data is `data/duckdb_signatures.txt`. It is not written by hand: the
//! ignored test `bless_duckdb_signatures` probes the live engine and writes
//! it. Every line is one call shape that DuckDB refused with `No function
//! matches the given name and argument types`:
//!
//! - `arity <name> <k>`: `name` with `k` arguments, every argument `NULL`.
//! - `above <name> <n>`: every count above `n`. Written only when the
//!   function has no variadic overload and `n + 1` to `n + 3` were refused.
//! - `arg <name> <k> <p> <class>`: `name` with `k` arguments, a column of
//!   every DuckDB type in `class` at position `p` (0-based), `NULL` at the
//!   others. A `NULL` binds to any parameter type, so a refusal here is a
//!   refusal for any other argument.
//!
//! A class is refused only when every DuckDB type in it is refused, so a
//! column Rocky types `Int32` is judged the same whether DuckDB stores it as
//! `TINYINT` or `INTEGER`. The DuckDB types each class stands for are
//! [`ArgClass::duckdb_types`].
//!
//! Names are left out (never checked) when DuckDB also has a macro, window
//! or table function by that name, when the name is not `[a-z0-9_]+`, and
//! when it is a call-position form the parser rewrites. A name an extension
//! that is not loaded adds overloads to is a known gap: the file is written
//! with `core_functions`, `icu`, `json` and `parquet` loaded, the extensions
//! a DuckDB 2.0 build links or autoloads for these functions. A DuckDB macro
//! created outside Rocky that reuses a built-in name is the same gap as for
//! [`E057`](crate::diagnostic::E057).

use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

use crate::types::RockyType;

/// The DuckDB version the data was generated from. A different bundled
/// version fails `the_signature_file_matches_the_bundled_duckdb`.
pub const DUCKDB_VERSION: &str = "v2.0.0";

/// A family of column types that DuckDB binds alike for these checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArgClass {
    Numeric,
    Text,
    Boolean,
    Date,
    Timestamp,
}

impl ArgClass {
    pub const ALL: [ArgClass; 5] = [
        ArgClass::Numeric,
        ArgClass::Text,
        ArgClass::Boolean,
        ArgClass::Date,
        ArgClass::Timestamp,
    ];

    /// The name the data file uses.
    pub fn name(self) -> &'static str {
        match self {
            ArgClass::Numeric => "numeric",
            ArgClass::Text => "text",
            ArgClass::Boolean => "boolean",
            ArgClass::Date => "date",
            ArgClass::Timestamp => "timestamp",
        }
    }

    fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|c| c.name() == name)
    }

    /// Every DuckDB type a column of this class can be. A class is refused
    /// only when each of these is.
    pub fn duckdb_types(self) -> &'static [&'static str] {
        match self {
            ArgClass::Numeric => &[
                "TINYINT",
                "SMALLINT",
                "INTEGER",
                "BIGINT",
                "HUGEINT",
                "FLOAT",
                "DOUBLE",
                "DECIMAL(4,1)",
                "DECIMAL(18,2)",
                "DECIMAL(38,2)",
            ],
            ArgClass::Text => &["VARCHAR"],
            ArgClass::Boolean => &["BOOLEAN"],
            ArgClass::Date => &["DATE"],
            ArgClass::Timestamp => &["TIMESTAMP", "TIMESTAMPTZ"],
        }
    }

    /// The class of a Rocky type, when DuckDB binds it alike. Every other
    /// type is not checked.
    pub fn of(ty: &RockyType) -> Option<Self> {
        match ty {
            RockyType::Int32
            | RockyType::Int64
            | RockyType::Float32
            | RockyType::Float64
            | RockyType::Decimal { .. } => Some(ArgClass::Numeric),
            RockyType::String => Some(ArgClass::Text),
            RockyType::Boolean => Some(ArgClass::Boolean),
            RockyType::Date => Some(ArgClass::Date),
            RockyType::Timestamp => Some(ArgClass::Timestamp),
            RockyType::DecimalUnsized { .. }
            | RockyType::TimestampNtz
            | RockyType::Binary
            | RockyType::Array(_)
            | RockyType::Map(_, _)
            | RockyType::Struct(_)
            | RockyType::Variant
            | RockyType::Unknown => None,
        }
    }
}

#[derive(Debug, Default)]
struct Signatures {
    arity: HashSet<(String, usize)>,
    above: HashMap<String, usize>,
    arg: HashSet<(String, usize, usize, ArgClass)>,
}

static SIGNATURES: LazyLock<Signatures> =
    LazyLock::new(|| parse(include_str!("data/duckdb_signatures.txt")));

fn parse(text: &str) -> Signatures {
    let mut out = Signatures::default();
    for line in text.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        match fields.as_slice() {
            ["arity", name, k] => {
                if let Ok(k) = k.parse() {
                    out.arity.insert(((*name).to_string(), k));
                }
            }
            ["above", name, n] => {
                if let Ok(n) = n.parse() {
                    out.above.insert((*name).to_string(), n);
                }
            }
            ["arg", name, k, p, class] => {
                if let (Ok(k), Ok(p), Some(class)) =
                    (k.parse(), p.parse(), ArgClass::from_name(class))
                {
                    out.arg.insert(((*name).to_string(), k, p, class));
                }
            }
            _ => {}
        }
    }
    out
}

/// Whether DuckDB refuses `name` (lower case) called with `count` arguments,
/// whatever they are.
pub fn refuses_count(name: &str, count: usize) -> bool {
    let sigs = &*SIGNATURES;
    sigs.arity.contains(&(name.to_string(), count))
        || sigs.above.get(name).is_some_and(|n| count > *n)
}

/// Whether DuckDB refuses `name` (lower case) called with `count` arguments
/// when argument `position` (0-based) is a column of `class`, whatever the
/// other arguments are.
pub fn refuses_argument(name: &str, count: usize, position: usize, class: ArgClass) -> bool {
    SIGNATURES
        .arg
        .contains(&(name.to_string(), count, position, class))
}

/// The argument counts DuckDB accepts for `name`, up to `limit`, for the
/// message.
pub fn accepted_counts(name: &str, limit: usize) -> Vec<usize> {
    (0..=limit).filter(|k| !refuses_count(name, *k)).collect()
}

/// The classes DuckDB accepts at `position` of `name` with `count`
/// arguments, for the message.
pub fn accepted_classes(name: &str, count: usize, position: usize) -> Vec<ArgClass> {
    ArgClass::ALL
        .into_iter()
        .filter(|c| !refuses_argument(name, count, position, *c))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Write as _;

    const NO_MATCH: &str = "No function matches the given name and argument types";

    /// Names the call checks leave alone: rewritten by the parser, or handled
    /// as special forms by `function_check`.
    fn skipped_name(name: &str) -> bool {
        !name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
            || name.starts_with("__")
            || crate::function_check::is_special_form(name)
    }

    fn column_name(ty: &str) -> String {
        let mut out = String::from("c_");
        for ch in ty.chars() {
            if ch.is_ascii_alphanumeric() {
                out.push(ch.to_ascii_lowercase());
            } else {
                out.push('_');
            }
        }
        out
    }

    fn connection() -> duckdb::Connection {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        let mut columns = Vec::new();
        for class in ArgClass::ALL {
            for ty in class.duckdb_types() {
                columns.push(format!("{} {ty}", column_name(ty)));
            }
        }
        conn.execute_batch(&format!("CREATE TABLE t ({})", columns.join(", ")))
            .unwrap();
        conn
    }

    /// Whether DuckDB refuses the call at bind time because no overload
    /// matches. Any other error (a constant required, a lambda expected) is
    /// not counted as a refusal.
    fn no_match(conn: &duckdb::Connection, name: &str, args: &[String]) -> bool {
        let sql = format!("SELECT {name}({}) FROM t", args.join(", "));
        match conn.prepare(&sql) {
            Ok(_) => false,
            Err(e) => e.to_string().contains(NO_MATCH),
        }
    }

    fn version(conn: &duckdb::Connection) -> String {
        conn.query_row("SELECT version()", [], |r| r.get(0))
            .unwrap()
    }

    /// `(name, max parameters, any variadic overload)` for every name the
    /// checks cover.
    fn checked_names(conn: &duckdb::Connection) -> Vec<(String, usize, bool)> {
        let mut stmt = conn
            .prepare(
                "SELECT lower(function_name), \
                        bool_and(function_type IN ('scalar', 'aggregate')), \
                        max(len(parameters)), \
                        bool_or(varargs IS NOT NULL) \
                 FROM duckdb_functions() GROUP BY 1 ORDER BY 1",
            )
            .unwrap();
        let rows: Vec<(String, bool, i64, bool)> = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        rows.into_iter()
            .filter(|(name, only_calls, _, _)| *only_calls && !skipped_name(name))
            .map(|(name, _, max, varargs)| (name, usize::try_from(max).unwrap_or(0), varargs))
            .collect()
    }

    fn generate(conn: &duckdb::Connection) -> String {
        let mut out = String::new();
        writeln!(
            out,
            "# What DuckDB {} refuses at bind time. Generated by the ignored test\n\
             # `duckdb_signatures::tests::bless_duckdb_signatures`; do not edit.\n\
             # Format and meaning: crates/rocky-compiler/src/duckdb_signatures.rs.",
            version(conn)
        )
        .unwrap();
        let nulls = |k: usize| vec!["NULL".to_string(); k];
        for (name, max, varargs) in checked_names(conn) {
            let mut accepted = Vec::new();
            for k in 0..=max + 3 {
                if no_match(conn, &name, &nulls(k)) {
                    writeln!(out, "arity {name} {k}").unwrap();
                } else {
                    accepted.push(k);
                }
            }
            if !varargs && (max + 1..=max + 3).all(|k| !accepted.contains(&k)) {
                writeln!(out, "above {name} {}", max + 3).unwrap();
            }
            for k in accepted.into_iter().filter(|k| *k >= 1) {
                for p in 0..k {
                    for class in ArgClass::ALL {
                        let refused = class.duckdb_types().iter().all(|ty| {
                            let mut args = nulls(k);
                            args[p] = column_name(ty);
                            no_match(conn, &name, &args)
                        });
                        if refused {
                            writeln!(out, "arg {name} {k} {p} {}", class.name()).unwrap();
                        }
                    }
                }
            }
        }
        out
    }

    /// Rewrites `data/duckdb_signatures.txt` from the bundled DuckDB, with
    /// the extensions named in the module docs loaded. Run with
    /// `ROCKY_BLESS_DUCKDB_SIGNATURES=1 cargo test -p rocky-compiler --lib
    /// bless_duckdb_signatures -- --ignored` after a DuckDB bump, then
    /// update [`DUCKDB_VERSION`].
    #[test]
    #[ignore = "writes data/duckdb_signatures.txt; run after a DuckDB bump"]
    fn bless_duckdb_signatures() {
        if std::env::var("ROCKY_BLESS_DUCKDB_SIGNATURES").as_deref() != Ok("1") {
            return;
        }
        let conn = connection();
        for ext in ["icu", "json", "parquet"] {
            conn.execute_batch(&format!("LOAD {ext}"))
                .unwrap_or_else(|e| panic!("bless needs the {ext} extension: {e}"));
        }
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/data/duckdb_signatures.txt"
        );
        std::fs::write(path, generate(&conn)).unwrap();
    }

    /// Every refusal in the file is a refusal of the bundled DuckDB, and the
    /// file was generated from the bundled version. The file is written with
    /// more extensions loaded than the bundled build has, so the bundled
    /// build refuses at least as much; a DuckDB bump fails the version check
    /// before any overload it adds can turn into a false refusal.
    #[test]
    fn the_signature_file_matches_the_bundled_duckdb() {
        let conn = connection();
        assert_eq!(
            version(&conn),
            DUCKDB_VERSION,
            "the bundled DuckDB changed; regenerate data/duckdb_signatures.txt \
             (see bless_duckdb_signatures)"
        );
        let text = include_str!("data/duckdb_signatures.txt");
        assert!(
            text.lines()
                .next()
                .is_some_and(|l| l.contains(&format!("DuckDB {DUCKDB_VERSION} "))),
            "the file header must name DuckDB {DUCKDB_VERSION}"
        );
        let sigs = &*SIGNATURES;
        // Guard against a file that refuses nothing and so proves nothing.
        assert!(sigs.arity.len() > 1000, "arity lines: {}", sigs.arity.len());
        assert!(sigs.arg.len() > 1000, "arg lines: {}", sigs.arg.len());
        for (name, k) in &sigs.arity {
            assert!(
                no_match(&conn, name, &vec!["NULL".to_string(); *k]),
                "DuckDB accepts {name} with {k} arguments"
            );
        }
        for (name, n) in &sigs.above {
            for k in n + 1..=n + 3 {
                assert!(
                    no_match(&conn, name, &vec!["NULL".to_string(); k]),
                    "DuckDB accepts {name} with {k} arguments"
                );
            }
        }
        for (name, k, p, class) in &sigs.arg {
            for ty in class.duckdb_types() {
                let mut args = vec!["NULL".to_string(); *k];
                args[*p] = column_name(ty);
                assert!(
                    no_match(&conn, name, &args),
                    "DuckDB accepts {name}({}) ",
                    args.join(", ")
                );
            }
        }
    }

    /// The issue's calls are in the file.
    #[test]
    fn the_issue_calls_are_refused() {
        assert!(refuses_argument("lower", 1, 0, ArgClass::Numeric));
        assert!(refuses_argument("abs", 1, 0, ArgClass::Text));
        assert!(refuses_argument("md5", 1, 0, ArgClass::Numeric));
        assert!(refuses_count("lower", 2));
        assert!(refuses_count("lower", 0));
        assert!(refuses_count("round", 3));
        assert!(!refuses_count("round", 1));
        assert!(!refuses_count("round", 2));
        assert!(!refuses_argument("concat", 2, 0, ArgClass::Numeric));
        // `date_trunc` over a timestamp: TIMESTAMP binds, so the class is not
        // refused even if one member were.
        assert!(!refuses_argument("date_trunc", 2, 1, ArgClass::Timestamp));
        assert!(refuses_argument("date_trunc", 2, 1, ArgClass::Numeric));
    }
}
