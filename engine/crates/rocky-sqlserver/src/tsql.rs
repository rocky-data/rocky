//! T-SQL text helpers: identifier quoting and CTE hoisting.
//!
//! **Why CTEs are hoisted.** T-SQL accepts a `WITH` clause only at the start
//! of a statement (`WITH … SELECT`, `WITH … INSERT`, `WITH … MERGE`, `WITH …
//! DELETE`) or at the start of a view body. It refuses one inside a derived
//! table (`FROM (WITH x AS (…) SELECT …) AS t`), inside a CTE definition, or
//! after `INSERT INTO t` — learn.microsoft.com/sql/t-sql/queries/
//! with-common-table-expression-transact-sql ("A CTE must be followed by a
//! single SELECT, INSERT, UPDATE, or DELETE statement"; "Specifying more
//! than one WITH clause in a CTE isn't allowed").
//!
//! Rocky composes statements by wrapping a model's SELECT
//! (`SELECT * FROM (<model>) AS rocky_incremental WHERE …`, the `MERGE …
//! USING (<model>)` source, a `delete_insert` partition subquery). A model
//! that starts with `WITH` — most do — would be rejected in every one of
//! those positions. [`hoist_ctes`] lifts every CTE, at any depth, to one
//! leading list and returns the CTE-free remainder, so the dialect can put
//! the `WITH` where T-SQL wants it.
//!
//! Lifting as written could change meaning when two CTEs share a name (an
//! inlined ephemeral model and its consumer both define `final`) or a
//! nested CTE name is also used unqualified elsewhere in the statement
//! (lifting it would make that reference resolve to the CTE). Then the
//! statement goes through the SQL AST first
//! ([`rocky_sql::cte_names::uniquify_cte_names`]): each colliding nested
//! CTE gets a distinct name (`final__2`) and only the references that bind
//! to it in its own scope are rewritten. The rewrite is refused (`None`)
//! only when even the renamed statement cannot be lifted, or the text does
//! not parse; `rocky compile` reports that as `E054`, and at run time the
//! caller sends the text unchanged so the server reports its own error.

/// `[name]`, with `]` doubled — T-SQL's delimited identifier.
#[must_use]
pub fn quote_ident(name: &str) -> String {
    format!("[{}]", name.replace(']', "]]"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// A bare word: keyword, identifier, number, `@variable`.
    Word,
    /// `[x]` or `"x"`.
    Quoted,
    Open,
    Close,
    Comma,
    Dot,
    Semicolon,
    Other,
}

#[derive(Debug, Clone, Copy)]
struct Tok {
    kind: Kind,
    start: usize,
    end: usize,
}

/// Significant tokens of `sql`; string literals and comments are skipped
/// (a literal is reported as `Other` so it still separates words).
fn scan(sql: &str) -> Vec<Tok> {
    let b = sql.as_bytes();
    let mut toks = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        let start = i;
        match c {
            b' ' | b'\t' | b'\r' | b'\n' => i += 1,
            b'-' if b.get(i + 1) == Some(&b'-') => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                // T-SQL block comments nest.
                let mut depth = 0usize;
                while i < b.len() {
                    if b[i] == b'/' && b.get(i + 1) == Some(&b'*') {
                        depth += 1;
                        i += 2;
                    } else if b[i] == b'*' && b.get(i + 1) == Some(&b'/') {
                        depth -= 1;
                        i += 2;
                        if depth == 0 {
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
            }
            b'\'' => {
                i += 1;
                while i < b.len() {
                    if b[i] == b'\'' {
                        if b.get(i + 1) == Some(&b'\'') {
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
                toks.push(Tok {
                    kind: Kind::Other,
                    start,
                    end: i,
                });
            }
            b'[' | b'"' => {
                let close = if c == b'[' { b']' } else { b'"' };
                i += 1;
                while i < b.len() {
                    if b[i] == close {
                        if b.get(i + 1) == Some(&close) {
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
                toks.push(Tok {
                    kind: Kind::Quoted,
                    start,
                    end: i,
                });
            }
            b'(' | b')' | b',' | b'.' | b';' => {
                let kind = match c {
                    b'(' => Kind::Open,
                    b')' => Kind::Close,
                    b',' => Kind::Comma,
                    b'.' => Kind::Dot,
                    _ => Kind::Semicolon,
                };
                i += 1;
                toks.push(Tok {
                    kind,
                    start,
                    end: i,
                });
            }
            _ if c.is_ascii_alphanumeric() || c == b'_' || c == b'@' || c == b'#' || c >= 0x80 => {
                while i < b.len()
                    && (b[i].is_ascii_alphanumeric()
                        || matches!(b[i], b'_' | b'@' | b'#' | b'$')
                        || b[i] >= 0x80)
                {
                    i += 1;
                }
                toks.push(Tok {
                    kind: Kind::Word,
                    start,
                    end: i,
                });
            }
            _ => {
                i += 1;
                toks.push(Tok {
                    kind: Kind::Other,
                    start,
                    end: i,
                });
            }
        }
    }
    toks
}

fn is_word(sql: &str, t: &Tok, word: &str) -> bool {
    t.kind == Kind::Word && sql[t.start..t.end].eq_ignore_ascii_case(word)
}

/// The identifier a token names, unquoted and lower-cased (SQL Server's
/// default collations compare identifiers case-insensitively).
fn ident_key(sql: &str, t: &Tok) -> Option<String> {
    let text = &sql[t.start..t.end];
    match t.kind {
        Kind::Word => Some(text.to_ascii_lowercase()),
        Kind::Quoted if text.len() >= 2 => {
            let inner = &text[1..text.len() - 1];
            let unescaped = if text.starts_with('[') {
                inner.replace("]]", "]")
            } else {
                inner.replace("\"\"", "\"")
            };
            Some(unescaped.to_ascii_lowercase())
        }
        _ => None,
    }
}

/// Index of the `Close` matching the `Open` at `open`.
fn matching_close(toks: &[Tok], open: usize) -> Option<usize> {
    let mut depth = 0usize;
    for (i, t) in toks.iter().enumerate().skip(open) {
        match t.kind {
            Kind::Open => depth += 1,
            Kind::Close => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

/// One parsed CTE: `name [(cols)] AS (inner)`.
struct CteSpan {
    key: String,
    /// Byte range of `name [(cols)] AS (` — the head kept verbatim.
    head: (usize, usize),
    /// Byte range of the definition between the parentheses.
    inner: (usize, usize),
}

/// Parse the CTE list whose `WITH` is token `with_idx`. Returns the CTEs
/// and the token index where the statement body starts.
fn parse_cte_list(sql: &str, toks: &[Tok], with_idx: usize) -> Option<(Vec<CteSpan>, usize)> {
    let mut i = with_idx + 1;
    let mut ctes = Vec::new();
    loop {
        let name = toks.get(i)?;
        let key = ident_key(sql, name)?;
        if name.kind == Kind::Word && is_word(sql, name, "AS") {
            return None;
        }
        i += 1;
        if toks.get(i)?.kind == Kind::Open {
            i = matching_close(toks, i)? + 1;
        }
        if !is_word(sql, toks.get(i)?, "AS") {
            return None;
        }
        i += 1;
        let open = i;
        if toks.get(open)?.kind != Kind::Open {
            return None;
        }
        let close = matching_close(toks, open)?;
        ctes.push(CteSpan {
            key,
            head: (name.start, toks[open].end),
            inner: (toks[open].end, toks[close].start),
        });
        i = close + 1;
        match toks.get(i) {
            Some(t) if t.kind == Kind::Comma => i += 1,
            _ => return Some((ctes, i)),
        }
    }
}

/// A nested `( WITH … )` group: the CTE names it defines and the byte span
/// of the parenthesised group.
type NestedGroup = (Vec<String>, (usize, usize));

/// Every nested `( WITH … )` group in `sql`.
fn nested_groups(sql: &str, toks: &[Tok]) -> Option<Vec<NestedGroup>> {
    let mut groups = Vec::new();
    for (i, t) in toks.iter().enumerate() {
        if t.kind == Kind::Open && toks.get(i + 1).is_some_and(|w| is_word(sql, w, "WITH")) {
            let (ctes, _) = parse_cte_list(sql, toks, i + 1)?;
            let close = matching_close(toks, i)?;
            groups.push((
                ctes.into_iter().map(|c| c.key).collect(),
                (t.start, toks[close].end),
            ));
        }
    }
    Some(groups)
}

/// The CTE-free form of a SELECT.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hoisted {
    /// `WITH a AS (…), b AS (…)` — empty when the text had no CTE.
    pub with_clause: String,
    /// The statement body with every CTE removed.
    pub body: String,
}

impl Hoisted {
    /// `with_clause` followed by a newline, or nothing.
    #[must_use]
    pub fn prefix(&self) -> String {
        if self.with_clause.is_empty() {
            String::new()
        } else {
            format!("{}\n", self.with_clause)
        }
    }
}

/// Lift every CTE in `sql` — leading or nested at any depth — into one
/// leading `WITH` list. A text with no CTE comes back unchanged with an
/// empty `with_clause`. Colliding nested CTE names are renamed in scope
/// through the SQL AST first. `None` when the lift could still change the
/// statement's meaning, or a `WITH` does not parse as a CTE list (see the
/// module docs).
#[must_use]
pub fn hoist_ctes(sql: &str) -> Option<Hoisted> {
    if let Some(hoisted) = hoist_as_written(sql) {
        return Some(hoisted);
    }
    let dialect = sqlparser::dialect::MsSqlDialect {};
    let renamed = rocky_sql::cte_names::uniquify_cte_names(sql, &dialect).ok()??;
    hoist_as_written(&renamed)
}

/// [`hoist_ctes`] without the rename: `None` on any name collision.
fn hoist_as_written(sql: &str) -> Option<Hoisted> {
    let trimmed = sql.trim_start_matches(|c: char| c.is_whitespace() || c == ';');
    let toks = scan(trimmed);

    // Safety checks on the original text.
    let groups = nested_groups(trimmed, &toks)?;
    let mut seen = std::collections::HashSet::new();
    let leading = match toks.first() {
        Some(t) if is_word(trimmed, t, "WITH") => Some(parse_cte_list(trimmed, &toks, 0)?),
        _ => None,
    };
    if let Some((ctes, _)) = &leading {
        for c in ctes {
            if !seen.insert(c.key.clone()) {
                return None;
            }
        }
    }
    for (names, (start, end)) in &groups {
        for name in names {
            if !seen.insert(name.clone()) {
                return None;
            }
            for (i, t) in toks.iter().enumerate() {
                let outside = t.start < *start || t.start >= *end;
                let qualified = i > 0 && toks[i - 1].kind == Kind::Dot;
                if outside && !qualified && ident_key(trimmed, t).as_deref() == Some(name.as_str())
                {
                    return None;
                }
            }
        }
    }

    let mut ctes = Vec::new();
    let body = hoist_into(trimmed, &mut ctes)?;
    Some(Hoisted {
        with_clause: if ctes.is_empty() {
            String::new()
        } else {
            format!("WITH {}", ctes.join(",\n"))
        },
        body,
    })
}

/// Push `text`'s CTEs (inner ones first, so each is defined before use) onto
/// `out`; return the text without them.
fn hoist_into(text: &str, out: &mut Vec<String>) -> Option<String> {
    let toks = scan(text);
    let (leading, body_start) = match toks.first() {
        Some(t) if is_word(text, t, "WITH") => {
            let (ctes, body_idx) = parse_cte_list(text, &toks, 0)?;
            let start = toks.get(body_idx).map_or(text.len(), |t| t.start);
            (ctes, start)
        }
        _ => (Vec::new(), 0),
    };
    for cte in &leading {
        let inner = hoist_into(&text[cte.inner.0..cte.inner.1], out)?;
        let inner = strip_unbounded_order_by(&inner);
        out.push(format!(
            "{}{}\n)",
            &text[cte.head.0..cte.head.1],
            trim_newlines(inner)
        ));
    }
    hoist_nested(&text[body_start..], out)
}

/// `inner` without a trailing `ORDER BY`, when T-SQL would refuse it.
///
/// T-SQL rejects `ORDER BY` in a CTE unless `TOP`, `OFFSET` or `FOR` appears
/// with it (error 1033). A CTE has no defined row order, so dropping a bare
/// one changes no result. An inlined ephemeral model is the usual source.
/// Only a top-level `ORDER BY` goes: one inside `OVER (…)` or a subquery sits
/// in parentheses. Any top-level `TOP`, `OFFSET`, `FETCH` or `FOR` keeps the
/// text as written.
fn strip_unbounded_order_by(inner: &str) -> &str {
    let toks = scan(inner);
    let mut depth = 0usize;
    let mut order_at = None;
    for (i, t) in toks.iter().enumerate() {
        match t.kind {
            Kind::Open => depth += 1,
            Kind::Close => depth = depth.saturating_sub(1),
            Kind::Word if depth == 0 => {
                if ["TOP", "OFFSET", "FETCH", "FOR"]
                    .iter()
                    .any(|w| is_word(inner, t, w))
                {
                    return inner;
                }
                if order_at.is_none()
                    && is_word(inner, t, "ORDER")
                    && toks.get(i + 1).is_some_and(|n| is_word(inner, n, "BY"))
                {
                    order_at = Some(t.start);
                }
            }
            _ => {}
        }
    }
    order_at.map_or(inner, |at| inner[..at].trim_end())
}

/// Replace every outermost `( WITH … )` group in `text` with its CTE-free
/// body, pushing the lifted CTEs onto `out`.
fn hoist_nested(text: &str, out: &mut Vec<String>) -> Option<String> {
    let toks = scan(text);
    let mut result = String::with_capacity(text.len());
    let mut copied = 0;
    let mut i = 0;
    while i < toks.len() {
        let t = toks[i];
        if t.kind == Kind::Open && toks.get(i + 1).is_some_and(|w| is_word(text, w, "WITH")) {
            let close = matching_close(&toks, i)?;
            let inner = hoist_into(&text[t.end..toks[close].start], out)?;
            result.push_str(&text[copied..t.end]);
            result.push('\n');
            result.push_str(trim_newlines(&inner));
            result.push('\n');
            copied = toks[close].start;
            i = close;
            continue;
        }
        // Any other token, `(` included: keep scanning, which reaches a
        // `( WITH` group at every depth.
        i += 1;
    }
    result.push_str(&text[copied..]);
    Some(result)
}

fn trim_newlines(s: &str) -> &str {
    s.trim_matches(|c: char| c == '\n' || c == '\r' || c == ' ' || c == '\t')
}

/// Split a table reference this dialect rendered (`[db].[schema].[table]`,
/// `[schema].[table]`) — or a bare `a.b.c` — into unquoted parts. `None`
/// when the text is not a plain dotted name.
#[must_use]
pub fn split_table_ref(table_ref: &str) -> Option<Vec<String>> {
    let toks = scan(table_ref.trim());
    let mut parts = Vec::new();
    let mut expect_name = true;
    for t in &toks {
        match (expect_name, t.kind) {
            (true, Kind::Word | Kind::Quoted) => {
                let text = &table_ref.trim()[t.start..t.end];
                let name = match t.kind {
                    Kind::Quoted => text[1..text.len() - 1].replace("]]", "]"),
                    _ => text.to_string(),
                };
                parts.push(name);
                expect_name = false;
            }
            (false, Kind::Dot) => expect_name = true,
            _ => return None,
        }
    }
    if expect_name || parts.is_empty() || parts.len() > 3 {
        return None;
    }
    Some(parts)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hoist(sql: &str) -> Hoisted {
        hoist_ctes(sql).expect("hoistable")
    }

    /// An ephemeral model is inlined as a CTE. T-SQL refuses `ORDER BY` in a
    /// CTE unless `TOP`, `OFFSET` or `FOR` is also given (error 1033), so the
    /// hoisted definition must not carry a bare trailing `ORDER BY`. A CTE's
    /// row order is not defined anyway.
    #[test]
    fn a_bare_order_by_in_a_cte_is_dropped() {
        let h = hoist(
            "WITH __rocky_ephemeral__e AS (SELECT a, b FROM t ORDER BY a DESC)\n\
             SELECT a FROM __rocky_ephemeral__e ORDER BY a",
        );
        assert!(
            !h.with_clause.to_uppercase().contains("ORDER BY"),
            "{}",
            h.with_clause
        );
        // The statement's own ORDER BY is untouched.
        assert!(h.body.contains("ORDER BY a"), "{}", h.body);
    }

    #[test]
    fn order_by_in_a_cte_is_kept_when_top_offset_or_window_needs_it() {
        for cte in [
            "SELECT TOP (5) a FROM t ORDER BY a",
            "SELECT a FROM t ORDER BY a OFFSET 0 ROWS FETCH NEXT 5 ROWS ONLY",
            "SELECT ROW_NUMBER() OVER (ORDER BY a) AS rn FROM t",
        ] {
            let h = hoist(&format!("WITH c AS ({cte}) SELECT * FROM c"));
            assert!(h.with_clause.contains(cte), "{}", h.with_clause);
        }
    }

    #[test]
    fn plain_select_is_unchanged() {
        let h = hoist("SELECT a FROM t");
        assert_eq!(h.with_clause, "");
        assert_eq!(h.body, "SELECT a FROM t");
        assert_eq!(h.prefix(), "");
    }

    #[test]
    fn leading_cte_is_split() {
        let h = hoist("WITH x AS (SELECT 1 AS a), y (b) AS (SELECT a FROM x)\nSELECT b FROM y");
        assert_eq!(
            h.with_clause,
            "WITH x AS (SELECT 1 AS a\n),\ny (b) AS (SELECT a FROM x\n)"
        );
        assert_eq!(h.body, "SELECT b FROM y");
    }

    #[test]
    fn leading_semicolon_and_comments() {
        let h = hoist(";WITH x AS (SELECT ')' AS p -- (WITH\n) SELECT p FROM x");
        assert_eq!(h.with_clause, "WITH x AS (SELECT ')' AS p -- (WITH\n)");
        assert_eq!(h.body, "SELECT p FROM x");
    }

    #[test]
    fn cte_inside_derived_table_is_lifted() {
        let sql = "SELECT * FROM (\nWITH base AS (SELECT id, ts FROM raw.orders)\nSELECT id, ts FROM base\n) AS rocky_incremental\nWHERE rocky_incremental.ts > 1";
        let h = hoist(sql);
        assert_eq!(
            h.with_clause,
            "WITH base AS (SELECT id, ts FROM raw.orders\n)"
        );
        assert_eq!(
            h.body,
            "SELECT * FROM (\nSELECT id, ts FROM base\n) AS rocky_incremental\nWHERE rocky_incremental.ts > 1"
        );
    }

    #[test]
    fn doubly_nested_and_cte_inside_cte() {
        let sql =
            "SELECT * FROM (SELECT * FROM (WITH a AS (SELECT 1 AS v) SELECT v FROM a) AS i) AS o";
        let h = hoist(sql);
        assert_eq!(h.with_clause, "WITH a AS (SELECT 1 AS v\n)");
        assert_eq!(
            h.body,
            "SELECT * FROM (SELECT * FROM (\nSELECT v FROM a\n) AS i) AS o"
        );

        let sql = "WITH outer_cte AS (WITH inner_cte AS (SELECT 1 AS v) SELECT v FROM inner_cte) SELECT v FROM outer_cte";
        let h = hoist(sql);
        assert_eq!(
            h.with_clause,
            "WITH inner_cte AS (SELECT 1 AS v\n),\nouter_cte AS (SELECT v FROM inner_cte\n)"
        );
        assert_eq!(h.body, "SELECT v FROM outer_cte");
    }

    #[test]
    fn leading_and_nested_combine_in_dependency_order() {
        let sql = "WITH a AS (SELECT 1 AS v) SELECT * FROM (WITH b AS (SELECT v FROM a) SELECT v FROM b) AS s";
        let h = hoist(sql);
        assert_eq!(
            h.with_clause,
            "WITH a AS (SELECT 1 AS v\n),\nb AS (SELECT v FROM a\n)"
        );
        assert_eq!(h.body, "SELECT * FROM (\nSELECT v FROM b\n) AS s");
    }

    #[test]
    fn colliding_nested_names_are_renamed_in_scope_before_lifting() {
        // Two CTEs named `x`: the nested one becomes `x__2`, and only the
        // read inside its own scope follows it.
        let h = hoist(
            "WITH x AS (SELECT 1 AS v) SELECT * FROM (WITH x AS (SELECT 2 AS v) SELECT v FROM x) AS s",
        );
        assert_eq!(
            h.with_clause,
            "WITH x AS (SELECT 1 AS v\n),\nx__2 AS (SELECT 2 AS v\n)"
        );
        assert_eq!(h.body, "SELECT * FROM (\nSELECT v FROM x__2 AS x\n) AS s");
        // The outer query reads a TABLE named `x`; lifting the nested CTE
        // `x` as written would redirect it, so the CTE is renamed instead.
        let h =
            hoist("SELECT * FROM x JOIN (WITH x AS (SELECT 2 AS v) SELECT v FROM x) AS s ON 1 = 1");
        assert_eq!(h.with_clause, "WITH x__2 AS (SELECT 2 AS v\n)");
        assert_eq!(
            h.body,
            "SELECT * FROM x JOIN (\nSELECT v FROM x__2 AS x\n) AS s ON 1 = 1"
        );
        // A qualified `[marts].[x]` is not captured by a CTE named `x`.
        let h = hoist("MERGE_TARGET [marts].[x] (WITH x AS (SELECT 2 AS v) SELECT v FROM x)");
        assert_eq!(h.with_clause, "WITH x AS (SELECT 2 AS v\n)");
    }

    /// The inliner's output for two ephemeral models that each end in
    /// `WITH final AS …`, read by a consumer with its own `final`.
    #[test]
    fn ephemeral_inlining_with_shared_cte_names_lifts_to_one_with() {
        let sql = "WITH __rocky_ephemeral__stg_a AS (WITH final AS (SELECT id, v FROM raw) SELECT * FROM final), \
                   __rocky_ephemeral__stg_b AS (WITH final AS (SELECT id, v + 1 AS w FROM raw) SELECT * FROM final), \
                   final AS (SELECT a.id, a.v, b.w FROM __rocky_ephemeral__stg_a AS a JOIN __rocky_ephemeral__stg_b AS b ON a.id = b.id) \
                   SELECT * FROM final";
        let h = hoist(sql);
        assert_eq!(
            h.with_clause,
            "WITH final__2 AS (SELECT id, v FROM raw\n),\n\
             __rocky_ephemeral__stg_a AS (SELECT * FROM final__2 AS final\n),\n\
             final__3 AS (SELECT id, v + 1 AS w FROM raw\n),\n\
             __rocky_ephemeral__stg_b AS (SELECT * FROM final__3 AS final\n),\n\
             final AS (SELECT a.id, a.v, b.w FROM __rocky_ephemeral__stg_a AS a JOIN __rocky_ephemeral__stg_b AS b ON a.id = b.id\n)"
        );
        assert_eq!(h.body, "SELECT * FROM final");
        assert_eq!(h.with_clause.matches("WITH").count(), 1);
    }

    #[test]
    fn unliftable_statements_are_refused() {
        // The nested CTE `v` shares its name with a column the outer query
        // reads unqualified; no CTE collides, so nothing is renamed and the
        // token check still refuses.
        assert!(
            hoist_ctes("SELECT v FROM (WITH v AS (SELECT 1 AS v) SELECT v FROM v) AS s").is_none()
        );
        // A collision in text the SQL parser cannot read.
        assert!(
            hoist_ctes(
                "WITH x AS (SELECT 1 AS v) SELECT * FROM (WITH x AS (SELECT 2 AS v) SELECT v FROM x) AS s ~~ ("
            )
            .is_none()
        );
    }

    #[test]
    fn table_hints_and_function_parens_are_not_ctes() {
        let sql = "SELECT * FROM t WITH (NOLOCK) WHERE x IN (SELECT 1)";
        let h = hoist(sql);
        assert_eq!(h.with_clause, "");
        assert_eq!(h.body, sql);
    }

    #[test]
    fn quoting_and_ref_splitting() {
        assert_eq!(quote_ident("order"), "[order]");
        assert_eq!(quote_ident("a]b"), "[a]]b]");
        assert_eq!(
            split_table_ref("[db].[raw].[orders]").unwrap(),
            vec!["db", "raw", "orders"]
        );
        assert_eq!(
            split_table_ref("raw.orders").unwrap(),
            vec!["raw", "orders"]
        );
        assert!(split_table_ref("raw.").is_none());
        assert!(split_table_ref("(SELECT 1)").is_none());
        assert!(split_table_ref("a.b.c.d").is_none());
    }
}
