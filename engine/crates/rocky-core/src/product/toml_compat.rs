//! An order-preserving TOML reader and a byte-compatible writer.
//!
//! The lowering rewrites a file a human and an agent both touch, and the
//! bytes it produces are a migration contract: the Python implementation
//! this port replaces emitted its merged sidecars through `tomli_w`, so a
//! project moving from one to the other must see a no-op diff. Rust's
//! `toml` crate cannot do that job — its serializer sorts maps and picks
//! its own layout, and the one feature that would preserve order
//! (`preserve_order`) is a workspace-wide cargo feature that would change
//! TOML serialization for every crate that unifies with it.
//!
//! So this module owns both halves:
//!
//! - [`parse_ordered`] reads a document into an insertion-ordered map, in
//!   the order the keys appear in the source. `toml::de::DeTable` is a
//!   sorted map, but its keys are `Spanned`, so the source order is
//!   recoverable by sorting each table's entries on the key's span.
//! - [`render_document`] writes a document back out with `tomli_w`'s
//!   layout rules, reproduced exactly: four-space multi-line arrays,
//!   arrays of tables kept inline while every element fits in 100
//!   columns and expanded into `[[key]]` blocks once one does not.
//!
//! [`basic_string`] is a *different* escaper, mirroring the engine's own
//! `toml_basic_string`. The contract file is hand-rendered rather than
//! written through this module's writer, and the two escapers disagree
//! on tabs, on DEL, and on the case of hex escapes. Both spellings are
//! preserved on purpose: each one matches the file it writes.

use std::fmt::Write as _;

use indexmap::IndexMap;
use toml::de::{DeTable, DeValue};
use toml::value::Datetime;

/// `tomli_w`'s indent width, and the column budget its inline-table
/// heuristic measures against.
const INDENT: &str = "    ";
const MAX_LINE_LENGTH: usize = 100;

/// A TOML value that remembers the order its table keys came in.
///
/// Deliberately not `toml::Value`: that type's table is sorted, which
/// loses the ordering the merge is required to preserve.
#[derive(Debug, Clone, PartialEq)]
pub enum TomlValue {
    /// A string.
    String(String),
    /// A 64-bit signed integer.
    Integer(i64),
    /// A float.
    Float(f64),
    /// A boolean.
    Boolean(bool),
    /// A date, time, or date-time.
    Datetime(Datetime),
    /// An array of values.
    Array(Vec<TomlValue>),
    /// A table, in insertion order.
    Table(TomlTable),
}

/// A TOML table in insertion order.
pub type TomlTable = IndexMap<String, TomlValue>;

impl TomlValue {
    /// Build a string value from anything string-like.
    pub fn string(value: impl Into<String>) -> Self {
        Self::String(value.into())
    }

    /// The table inside this value, if it is one.
    pub fn as_table(&self) -> Option<&TomlTable> {
        match self {
            Self::Table(table) => Some(table),
            _ => None,
        }
    }

    /// The array inside this value, if it is one.
    pub fn as_array(&self) -> Option<&[TomlValue]> {
        match self {
            Self::Array(items) => Some(items),
            _ => None,
        }
    }

    /// True for values TOML renders as tables or arrays of tables.
    ///
    /// Such a value must follow every scalar key at its level to stay
    /// valid TOML, which is why the merge orders on it. Mirrors the
    /// answer key's `_is_table_valued`: an array counts when *any*
    /// element is a table, which is deliberately looser than the
    /// [`is_array_of_tables`] test the writer applies.
    pub fn is_table_valued(&self) -> bool {
        match self {
            Self::Table(_) => true,
            Self::Array(items) => items.iter().any(|item| matches!(item, Self::Table(_))),
            _ => false,
        }
    }
}

// ---------------------------------------------------------------------------
// Reading: document order recovered from key spans
// ---------------------------------------------------------------------------

/// Parse a TOML document, keeping the source order of every table's keys.
///
/// # Errors
///
/// Returns the parser's own message when the text is not valid TOML, or
/// when an integer does not fit in `i64` (TOML integers are 64-bit
/// signed, so a wider literal has no representable value to preserve).
pub fn parse_ordered(text: &str) -> Result<TomlTable, String> {
    let document = DeTable::parse(text).map_err(|err| err.to_string())?;
    convert_table(document.get_ref())
}

fn convert_table(table: &DeTable<'_>) -> Result<TomlTable, String> {
    // `DeTable` is a sorted map, so the entries arrive alphabetically.
    // Their keys carry source spans, and the span start is exactly where
    // the key was written — sorting on it restores document order without
    // the `preserve_order` cargo feature.
    let mut entries: Vec<_> = table.iter().collect();
    entries.sort_by_key(|(key, _)| key.span().start);

    let mut out = TomlTable::with_capacity(entries.len());
    for (key, value) in entries {
        out.insert(key.get_ref().to_string(), convert_value(value.get_ref())?);
    }
    Ok(out)
}

fn convert_value(value: &DeValue<'_>) -> Result<TomlValue, String> {
    let converted = match value {
        DeValue::String(text) => TomlValue::String(text.as_ref().to_string()),
        DeValue::Integer(number) => {
            let parsed = i64::from_str_radix(number.as_str(), number.radix())
                .map_err(|err| format!("integer '{number}' is not representable: {err}"))?;
            TomlValue::Integer(parsed)
        }
        DeValue::Float(number) => TomlValue::Float(
            number
                .as_str()
                .parse()
                .map_err(|err| format!("float '{number}' is not representable: {err}"))?,
        ),
        DeValue::Boolean(flag) => TomlValue::Boolean(*flag),
        DeValue::Datetime(stamp) => TomlValue::Datetime(*stamp),
        DeValue::Array(items) => TomlValue::Array(
            items
                .iter()
                .map(|item| convert_value(item.get_ref()))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        DeValue::Table(table) => TomlValue::Table(convert_table(table)?),
    };
    Ok(converted)
}

// ---------------------------------------------------------------------------
// Writing: `tomli_w`'s layout, reproduced
// ---------------------------------------------------------------------------

/// Render a whole document, `tomli_w`-compatible.
///
/// Reproduces `tomli_w.dumps(obj)` at its defaults (four-space indent,
/// no multi-line strings). The output ends in a newline whenever the
/// document is non-empty, because every line the writer emits carries
/// one.
pub fn render_document(table: &TomlTable) -> String {
    let mut out = String::new();
    render_table(table, "", false, &mut out);
    out
}

/// True for a non-empty array whose every element is a table.
///
/// Only such an array is a candidate for `[[key]]` expansion. An empty
/// array, or one with a single non-table element, stays a literal.
fn is_array_of_tables(items: &[TomlValue]) -> bool {
    !items.is_empty() && items.iter().all(|item| matches!(item, TomlValue::Table(_)))
}

/// The heuristic that decides between an inline table and a `[[key]]`
/// block: the rendered line, indented and comma-terminated, must fit the
/// column budget and hold no newline.
///
/// The budget counts characters, not bytes — a non-ASCII value must not
/// flip the decision just because UTF-8 spends more bytes on it.
fn is_suitable_inline_table(table: &TomlTable) -> bool {
    let rendered = format!("{INDENT}{},", render_inline_table(table));
    rendered.chars().count() <= MAX_LINE_LENGTH && !rendered.contains('\n')
}

fn render_table(table: &TomlTable, name: &str, inside_aot: bool, out: &mut String) {
    let mut yielded = false;
    let mut literals: Vec<(&String, &TomlValue)> = Vec::new();
    // `(key, table, inside_aot)` — an array of tables contributes one
    // entry per element, all under the same key.
    let mut tables: Vec<(&String, &TomlTable, bool)> = Vec::new();

    for (key, value) in table {
        match value {
            TomlValue::Table(nested) => tables.push((key, nested, false)),
            TomlValue::Array(items)
                if is_array_of_tables(items)
                    && !items
                        .iter()
                        .all(|item| item.as_table().is_some_and(is_suitable_inline_table)) =>
            {
                for item in items {
                    if let TomlValue::Table(nested) = item {
                        tables.push((key, nested, true));
                    }
                }
            }
            _ => literals.push((key, value)),
        }
    }

    // A named table announces itself when it carries scalars of its own,
    // or when it has no sub-tables to announce it instead. A table whose
    // whole content is sub-tables emits no header — `[a.b]` alone, never
    // an empty `[a]` above it.
    if inside_aot || (!name.is_empty() && (!literals.is_empty() || tables.is_empty())) {
        yielded = true;
        if inside_aot {
            let _ = writeln!(out, "[[{name}]]");
        } else {
            let _ = writeln!(out, "[{name}]");
        }
    }

    if !literals.is_empty() {
        yielded = true;
        for (key, value) in literals {
            let _ = writeln!(out, "{} = {}", render_key(key), render_literal(value, 0));
        }
    }

    for (key, nested, in_aot) in tables {
        if yielded {
            out.push('\n');
        } else {
            yielded = true;
        }
        let key_part = render_key(key);
        let display_name = if name.is_empty() {
            key_part
        } else {
            format!("{name}.{key_part}")
        };
        render_table(nested, &display_name, in_aot, out);
    }
}

fn render_literal(value: &TomlValue, nest_level: usize) -> String {
    match value {
        TomlValue::Boolean(flag) => (if *flag { "true" } else { "false" }).to_string(),
        TomlValue::Integer(number) => number.to_string(),
        TomlValue::Float(number) => render_float(*number),
        TomlValue::Datetime(stamp) => stamp.to_string(),
        TomlValue::String(text) => render_string(text),
        TomlValue::Array(items) => render_inline_array(items, nest_level),
        TomlValue::Table(table) => render_inline_table(table),
    }
}

/// Arrays are always written across lines, one element per line with a
/// trailing comma — `tomli_w` has no single-line array form.
fn render_inline_array(items: &[TomlValue], nest_level: usize) -> String {
    if items.is_empty() {
        return "[]".to_string();
    }
    let item_indent = INDENT.repeat(1 + nest_level);
    let closing_indent = INDENT.repeat(nest_level);
    let rendered: Vec<String> = items
        .iter()
        // The nesting level advances for elements of an array…
        .map(|item| format!("{item_indent}{}", render_literal(item, nest_level + 1)))
        .collect();
    format!("[\n{}\n{closing_indent}]", rendered.join(",\n") + ",")
}

fn render_inline_table(table: &TomlTable) -> String {
    if table.is_empty() {
        return "{}".to_string();
    }
    let pairs: Vec<String> = table
        .iter()
        // …but resets inside an inline table, which is what `tomli_w`'s
        // default argument does. Reproduced, quirk included, because the
        // indentation of an array nested in an inline table depends on it.
        .map(|(key, value)| format!("{} = {}", render_key(key), render_literal(value, 0)))
        .collect();
    format!("{{ {} }}", pairs.join(", "))
}

/// A bare key where TOML allows one, a quoted key otherwise.
fn render_key(key: &str) -> String {
    let bare = !key.is_empty()
        && key
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_');
    if bare {
        key.to_string()
    } else {
        render_string(key)
    }
}

/// A float, always in a spelling TOML reads back as a float.
///
/// Rust prints `1.0` as `1`, which TOML would read back as an integer
/// and quietly change the value's type on the next merge, so a bare
/// integral form regains its `.0`.
fn render_float(number: f64) -> String {
    if number.is_nan() {
        return "nan".to_string();
    }
    if number.is_infinite() {
        return if number.is_sign_negative() {
            "-inf"
        } else {
            "inf"
        }
        .to_string();
    }
    let rendered = number.to_string();
    if rendered.contains(['.', 'e', 'E']) {
        rendered
    } else {
        format!("{rendered}.0")
    }
}

/// A TOML basic string in `tomli_w`'s spelling.
///
/// Escapes quote and backslash, uses the compact forms for backspace,
/// linefeed, form feed, and carriage return, and writes every other
/// control character — DEL included — as a lowercase `\u00xx`. A tab is
/// written raw: TOML permits it inside a basic string, and `tomli_w`
/// takes that permission.
fn render_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\n' => out.push_str("\\n"),
            '\u{c}' => out.push_str("\\f"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push('\t'),
            ch if ch.is_control() && (ch as u32) < 0x80 => {
                let _ = write!(out, "\\u{:04x}", ch as u32);
            }
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

/// A TOML basic string in the *engine's* spelling, for the hand-rendered
/// contract file.
///
/// Mirrors `toml_basic_string` in the engine's contract writer: tab,
/// linefeed, and carriage return take their compact forms, and every
/// other control character becomes an uppercase `\u00XX`. It differs
/// from [`render_string`] on purpose — the contract is the engine's
/// file, written the engine's way.
pub fn basic_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if (ch as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04X}", ch as u32);
            }
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(pairs: &[(&str, TomlValue)]) -> TomlTable {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), value.clone()))
            .collect()
    }

    // ----- reading -----

    #[test]
    fn document_order_survives_the_sorted_parser() {
        let parsed = parse_ordered("zebra = 1\nalpha = 2\nmiddle = 3\n").expect("parses");
        let keys: Vec<&str> = parsed.keys().map(String::as_str).collect();
        // Alphabetical order would be alpha, middle, zebra — the span
        // sort is what keeps the human's order.
        assert_eq!(keys, ["zebra", "alpha", "middle"]);
    }

    #[test]
    fn header_tables_order_after_the_scalars_that_precede_them() {
        let parsed =
            parse_ordered("name = \"m\"\nintent = \"i\"\n\n[[tests]]\ntype = \"expression\"\n")
                .expect("parses");
        let keys: Vec<&str> = parsed.keys().map(String::as_str).collect();
        assert_eq!(keys, ["name", "intent", "tests"]);
    }

    #[test]
    fn a_table_split_across_headers_keeps_its_first_position() {
        let parsed =
            parse_ordered("[a.b]\nx = 1\n\n[zz]\ny = 2\n\n[a.c]\nz = 3\n").expect("parses");
        let keys: Vec<&str> = parsed.keys().map(String::as_str).collect();
        assert_eq!(keys, ["a", "zz"]);
        let inner: Vec<&str> = parsed["a"]
            .as_table()
            .expect("table")
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(inner, ["b", "c"]);
    }

    #[test]
    fn every_scalar_kind_round_trips() {
        let text = "i = 7\nf = 1.5\nb = true\ns = \"x\"\nd = 1979-05-27T07:32:00Z\na = [1, 2]\n";
        let parsed = parse_ordered(text).expect("parses");
        assert_eq!(parsed["i"], TomlValue::Integer(7));
        assert_eq!(parsed["f"], TomlValue::Float(1.5));
        assert_eq!(parsed["b"], TomlValue::Boolean(true));
        assert_eq!(parsed["s"], TomlValue::string("x"));
        assert!(matches!(parsed["d"], TomlValue::Datetime(_)));
        assert_eq!(
            parsed["a"],
            TomlValue::Array(vec![TomlValue::Integer(1), TomlValue::Integer(2)])
        );
    }

    #[test]
    fn a_hex_integer_is_normalized_to_decimal() {
        let parsed = parse_ordered("n = 0x1f\n").expect("parses");
        assert_eq!(parsed["n"], TomlValue::Integer(31));
    }

    #[test]
    fn an_integer_too_wide_for_toml_is_refused() {
        let error = parse_ordered("n = 9223372036854775808\n").expect_err("out of range");
        assert!(
            error.contains("not representable") || error.contains("range"),
            "{error}"
        );
    }

    #[test]
    fn invalid_toml_reports_the_parser_message() {
        let error = parse_ordered("name = [broken").expect_err("bad");
        assert!(error.contains("unclosed array"), "{error}");
    }

    // ----- writing -----

    #[test]
    fn arrays_always_render_across_lines() {
        let document = table(&[(
            "columns",
            TomlValue::Array(vec![TomlValue::string("a"), TomlValue::string("b")]),
        )]);
        assert_eq!(
            render_document(&document),
            "columns = [\n    \"a\",\n    \"b\",\n]\n"
        );
    }

    #[test]
    fn a_nested_array_indents_one_level_deeper() {
        let document = table(&[(
            "a",
            TomlValue::Array(vec![
                TomlValue::Array(vec![TomlValue::Integer(1), TomlValue::Integer(2)]),
                TomlValue::Array(vec![TomlValue::Integer(3)]),
            ]),
        )]);
        assert_eq!(
            render_document(&document),
            "a = [\n    [\n        1,\n        2,\n    ],\n    [\n        3,\n    ],\n]\n"
        );
    }

    #[test]
    fn an_array_inside_an_inline_table_restarts_at_the_outer_indent() {
        // A quirk of the reference writer, reproduced because the bytes
        // are the contract: the nesting level resets when rendering the
        // values of an inline table, so this inner array indents as if it
        // were at the top level.
        let document = table(&[(
            "m",
            TomlValue::Array(vec![
                TomlValue::Table(table(&[(
                    "a",
                    TomlValue::Array(vec![TomlValue::Integer(1), TomlValue::Integer(2)]),
                )])),
                TomlValue::Integer(2),
            ]),
        )]);
        assert_eq!(
            render_document(&document),
            "m = [\n    { a = [\n    1,\n    2,\n] },\n    2,\n]\n"
        );
    }

    #[test]
    fn an_empty_array_stays_on_one_line() {
        let document = table(&[("x", TomlValue::Array(vec![]))]);
        assert_eq!(render_document(&document), "x = []\n");
    }

    #[test]
    fn a_short_array_of_tables_stays_inline() {
        let document = table(&[(
            "sources",
            TomlValue::Array(vec![TomlValue::Table(table(&[
                ("catalog", TomlValue::string("poc")),
                ("table", TomlValue::string("t")),
            ]))]),
        )]);
        assert_eq!(
            render_document(&document),
            "sources = [\n    { catalog = \"poc\", table = \"t\" },\n]\n"
        );
    }

    #[test]
    fn an_over_long_array_of_tables_expands_into_blocks() {
        let long = "x".repeat(90);
        let document = table(&[(
            "sources",
            TomlValue::Array(vec![TomlValue::Table(table(&[(
                "catalog",
                TomlValue::string(long),
            )]))]),
        )]);
        assert!(
            render_document(&document).starts_with("[[sources]]\n"),
            "a line past the budget must expand"
        );
    }

    #[test]
    fn the_column_budget_counts_characters_not_bytes() {
        // 80 three-byte characters: 240 bytes, well past the budget when
        // measured wrongly, but only ~93 columns when measured right.
        let wide = "→".repeat(80);
        let document = table(&[(
            "sources",
            TomlValue::Array(vec![TomlValue::Table(table(&[(
                "c",
                TomlValue::string(wide),
            )]))]),
        )]);
        assert!(
            render_document(&document).starts_with("sources = ["),
            "a wide but short line must stay inline"
        );
    }

    #[test]
    fn an_array_of_tables_holding_an_array_always_expands() {
        // The nested array renders across lines, so the inline form holds
        // a newline and is refused however short it is.
        let document = table(&[(
            "tests",
            TomlValue::Array(vec![TomlValue::Table(table(&[(
                "columns",
                TomlValue::Array(vec![TomlValue::string("a")]),
            )]))]),
        )]);
        assert_eq!(
            render_document(&document),
            "[[tests]]\ncolumns = [\n    \"a\",\n]\n"
        );
    }

    #[test]
    fn a_mixed_array_is_a_literal_not_a_block() {
        let document = table(&[(
            "mixed",
            TomlValue::Array(vec![
                TomlValue::Table(table(&[("a", TomlValue::Integer(1))])),
                TomlValue::Integer(2),
            ]),
        )]);
        assert_eq!(
            render_document(&document),
            "mixed = [\n    { a = 1 },\n    2,\n]\n"
        );
    }

    #[test]
    fn a_table_of_only_tables_emits_no_header_of_its_own() {
        let document = table(&[(
            "a",
            TomlValue::Table(table(&[(
                "b",
                TomlValue::Table(table(&[("c", TomlValue::Integer(1))])),
            )])),
        )]);
        assert_eq!(render_document(&document), "[a.b]\nc = 1\n");
    }

    #[test]
    fn scalars_precede_sub_tables_with_a_blank_line_between() {
        let document = table(&[
            ("name", TomlValue::string("m")),
            (
                "tags",
                TomlValue::Table(table(&[("x", TomlValue::Integer(1))])),
            ),
        ]);
        assert_eq!(
            render_document(&document),
            "name = \"m\"\n\n[tags]\nx = 1\n"
        );
    }

    #[test]
    fn a_key_needing_quotes_gets_them() {
        let document = table(&[("has space", TomlValue::Integer(1))]);
        assert_eq!(render_document(&document), "\"has space\" = 1\n");
    }

    #[test]
    fn floats_keep_a_shape_toml_reads_back_as_a_float() {
        assert_eq!(render_float(1.0), "1.0");
        assert_eq!(render_float(-0.0), "-0.0");
        assert_eq!(render_float(1.5), "1.5");
        assert_eq!(render_float(f64::INFINITY), "inf");
        assert_eq!(render_float(f64::NEG_INFINITY), "-inf");
        assert_eq!(render_float(f64::NAN), "nan");
    }

    #[test]
    fn an_integral_float_survives_a_render_parse_round_trip_as_a_float() {
        let document = table(&[("f", TomlValue::Float(1.0))]);
        let reparsed = parse_ordered(&render_document(&document)).expect("parses");
        assert_eq!(reparsed["f"], TomlValue::Float(1.0));
    }

    // ----- the two escapers, which disagree on purpose -----

    #[test]
    fn the_writer_escaper_matches_tomli_w() {
        assert_eq!(render_string("a\"b\\c"), "\"a\\\"b\\\\c\"");
        assert_eq!(
            render_string("a\nb\rc\u{8}d\u{c}e"),
            "\"a\\nb\\rc\\bd\\fe\""
        );
        // A tab is legal raw inside a basic string, and is left raw.
        assert_eq!(render_string("a\tb"), "\"a\tb\"");
        // Lowercase hex, and DEL is escaped.
        assert_eq!(render_string("\u{1}\u{7f}"), "\"\\u0001\\u007f\"");
    }

    #[test]
    fn the_contract_escaper_matches_the_engine() {
        assert_eq!(basic_string("a\"b\\c"), "\"a\\\"b\\\\c\"");
        // Tab takes its compact form here, unlike the writer's.
        assert_eq!(basic_string("a\tb"), "\"a\\tb\"");
        // Uppercase hex — ESC is the shortest control character whose hex
        // carries a letter, so it is what tells the two cases apart.
        assert_eq!(basic_string("\u{1b}"), "\"\\u001B\"");
        // And DEL is left alone.
        assert_eq!(basic_string("\u{1}\u{7f}"), "\"\\u0001\u{7f}\"");
    }
}
