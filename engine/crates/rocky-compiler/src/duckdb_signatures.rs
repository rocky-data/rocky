//! What the bundled DuckDB refuses at bind time, for the argument-count
//! check [`E068`](crate::diagnostic::E068) (#2690).
//!
//! The data is `data/duckdb_signatures.txt`. It is not written by hand: the
//! ignored test `bless_duckdb_signatures` probes the live engine and writes
//! it. Every line is one call shape that DuckDB refused with `No function
//! matches the given name and argument types`:
//!
//! - `arity <name> <k>`: `name` with `k` arguments, every argument `NULL`,
//!   and again with every argument a column, for one column type of each
//!   class. A call such as `struct_pack(x)` binds a column but not `NULL`.
//! - `above <name> <n>`: every count above `n`. Written only when the
//!   function has no variadic overload and `n + 1` to `n + 3` were refused.
//!
//! The column types probed are one of each family DuckDB binds alike
//! (number, text, boolean, date, timestamp), plus `DOUBLE`. A name for which
//! a `NULL` argument binds differently from a column (`struct_pack` names
//! its fields after its arguments) is left out.
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

/// The DuckDB version the data was generated from. A different bundled
/// version fails `the_signature_file_matches_the_bundled_duckdb`.
pub const DUCKDB_VERSION: &str = "v2.0.1";

#[derive(Debug, Default)]
struct Signatures {
    arity: HashSet<(String, usize)>,
    above: HashMap<String, usize>,
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
            _ => {}
        }
    }
    out
}

/// Whether DuckDB refused `name` (lower case) called with `count` arguments
/// in every probe: all `NULL`, and all one column, for each filler type.
pub fn refuses_count(name: &str, count: usize) -> bool {
    let sigs = &*SIGNATURES;
    sigs.arity.contains(&(name.to_string(), count))
        || sigs.above.get(name).is_some_and(|n| count > *n)
}

/// The argument counts the data shows DuckDB accepts for `name`, for the
/// message, and whether every count above them is refused. Only counts up
/// to the highest one probed are listed.
pub fn accepted_counts(name: &str) -> (Vec<usize>, bool) {
    let sigs = &*SIGNATURES;
    let above = sigs.above.get(name).copied();
    let probed = sigs
        .arity
        .iter()
        .filter(|(n, _)| n == name)
        .map(|(_, k)| *k)
        .chain(above)
        .max()
        .unwrap_or(0);
    let accepted = (0..=probed).filter(|k| !refuses_count(name, *k)).collect();
    (accepted, above.is_some())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Write as _;

    const NO_MATCH: &str = "No function matches the given name and argument types";

    /// A family of column types DuckDB binds alike, for the probes' fillers.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    enum ArgClass {
        Numeric,
        Text,
        Boolean,
        Date,
        Timestamp,
    }

    impl ArgClass {
        const ALL: [ArgClass; 5] = [
            ArgClass::Numeric,
            ArgClass::Text,
            ArgClass::Boolean,
            ArgClass::Date,
            ArgClass::Timestamp,
        ];

        /// Every DuckDB type a column of this class can be. A class is refused
        /// only when each of these is.
        fn duckdb_types(self) -> &'static [&'static str] {
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
    }

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

    /// One DuckDB type of each class, plus `DOUBLE`: the fillers the probes
    /// put in the other argument positions.
    fn fillers() -> Vec<String> {
        let mut out = vec!["NULL".to_string()];
        for class in ArgClass::ALL {
            out.push(column_name(class.duckdb_types()[0]));
        }
        out.push(column_name("DOUBLE"));
        out
    }

    /// Whether every probe of `name` with `k` arguments is refused: all
    /// `NULL`, and all one column, for each filler.
    fn refuses_every_count_probe(conn: &duckdb::Connection, name: &str, k: usize) -> bool {
        fillers()
            .iter()
            .all(|f| no_match(conn, name, &vec![f.clone(); k]))
    }

    /// Whether a `NULL` argument binds differently from a column for `name`:
    /// some count is refused with all `NULL` but accepted with columns
    /// (`struct_pack` names its fields after its arguments). Such a name is
    /// left out, since the probes assume `NULL` binds to anything.
    fn null_sensitive(conn: &duckdb::Connection, name: &str, max: usize) -> bool {
        (1..=max + 3).any(|k| {
            no_match(conn, name, &vec!["NULL".to_string(); k])
                && !refuses_every_count_probe(conn, name, k)
        })
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
        for (name, max, varargs) in checked_names(conn) {
            if null_sensitive(conn, &name, max) {
                continue;
            }
            let mut accepted = Vec::new();
            for k in 0..=max + 3 {
                if refuses_every_count_probe(conn, &name, k) {
                    writeln!(out, "arity {name} {k}").unwrap();
                } else {
                    accepted.push(k);
                }
            }
            if !varargs && (max + 1..=max + 3).all(|k| !accepted.contains(&k)) {
                writeln!(out, "above {name} {}", max + 3).unwrap();
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

    /// Every refusal in the file still holds with the `httpfs`, `iceberg`,
    /// `ducklake` and `spatial` extensions loaded (#2752). An extension that
    /// adds an overload to a checked name would turn its refusal false. It
    /// needs the network to install the extensions, so it is ignored by
    /// default. Run with `cargo test -p rocky-compiler --lib
    /// extension_functions_keep_every_refusal -- --ignored` after a DuckDB
    /// bump. With no network it fails, naming the extension.
    #[test]
    #[ignore = "installs DuckDB extensions from the network"]
    fn extension_functions_keep_every_refusal() {
        let conn = connection();
        for ext in ["icu", "json", "parquet"] {
            conn.execute_batch(&format!("LOAD {ext}"))
                .unwrap_or_else(|e| panic!("the replay needs the {ext} extension: {e}"));
        }
        for ext in ["httpfs", "iceberg", "ducklake", "spatial"] {
            conn.execute_batch(&format!("INSTALL {ext}; LOAD {ext}"))
                .unwrap_or_else(|e| panic!("the replay needs the {ext} extension: {e}"));
        }
        let sigs = &*SIGNATURES;
        let mut accepted: Vec<String> = Vec::new();
        for (name, k) in &sigs.arity {
            if !refuses_every_count_probe(&conn, name, *k) {
                accepted.push(format!("{name} with {k} arguments"));
            }
        }
        for (name, n) in &sigs.above {
            for k in n + 1..=n + 3 {
                if !refuses_every_count_probe(&conn, name, k) {
                    accepted.push(format!("{name} with {k} arguments (above)"));
                }
            }
        }
        accepted.sort();
        assert!(
            accepted.is_empty(),
            "with the extensions loaded DuckDB accepts calls the file refuses: {accepted:?}"
        );
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
        for (name, k) in &sigs.arity {
            assert!(
                refuses_every_count_probe(&conn, name, *k),
                "DuckDB accepts {name} with {k} arguments"
            );
        }
        for (name, n) in &sigs.above {
            for k in n + 1..=n + 3 {
                assert!(
                    refuses_every_count_probe(&conn, name, k),
                    "DuckDB accepts {name} with {k} arguments"
                );
            }
        }
    }

    /// The issue's calls are in the file.
    #[test]
    fn the_issue_calls_are_refused() {
        assert!(refuses_count("lower", 2));
        assert!(refuses_count("lower", 0));
        assert!(refuses_count("round", 3));
        assert!(!refuses_count("round", 1));
        assert!(!refuses_count("round", 2));
    }
}
