//! SQL style linter for `rocky lint`.
//!
//! A small rule set modeled on common SQLFluff rules. The rules are about how
//! a query reads, not whether it is correct, so they are separate from the
//! compiler's correctness diagnostics (`E`/`W`/`P` codes). Every rule has a
//! stable code in the `S` series:
//!
//! | Code | Name | What it flags | Fix |
//! |---|---|---|---|
//! | `S001` | `ambiguous-column` | Unqualified column in a query that reads two or more tables | report only |
//! | `S002` | `implicit-inner-join` | Bare `JOIN` instead of `INNER JOIN` | inserts `INNER` |
//! | `S003` | `select-star` | `SELECT *` in the final result of a statement | report only |
//! | `S004` | `target-order` | A plain column listed after a calculated column | report only |
//! | `S005` | `keyword-case` | Keyword capitalisation that differs from the rest of the file | recases |
//! | `S006` | `trailing-whitespace` | Spaces or tabs at the end of a line | trims |
//! | `S007` | `tab-character` | Tab characters outside literals and comments | spaces |
//!
//! Text rules (`S002`, `S005`, `S006`, `S007`) read the token stream, so they
//! work on a file the parser cannot read. AST rules (`S001`, `S003`, `S004`)
//! need a parse; [`StyleReport::ast_rules_skipped`] is `true` when the parse
//! failed and those rules did not run. Fixes are byte edits on the source, so
//! they never reformat anything a rule did not flag. `S004` is report-only
//! because reordering columns changes the model's output schema.

use std::collections::HashSet;
use std::ops::ControlFlow;

use sqlparser::ast::{
    BinaryOperator, Expr, Join, JoinConstraint, JoinOperator, Query, Select, SelectItem, SetExpr,
    Spanned, TableFactor, Visit, Visitor,
};
use sqlparser::dialect::DatabricksDialect;
use sqlparser::tokenizer::{Token, TokenWithSpan, Tokenizer, Whitespace};

use crate::parser::parse_sql;

/// Static description of one rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuleInfo {
    /// Stable code, e.g. `S001`.
    pub code: &'static str,
    /// Short kebab-case name.
    pub name: &'static str,
    /// One-line description of what the rule flags.
    pub summary: &'static str,
    /// Whether `--fix` can rewrite the finding without changing behavior.
    pub fixable: bool,
}

/// Every rule, in code order.
pub const RULES: &[RuleInfo] = &[
    RuleInfo {
        code: "S001",
        name: "ambiguous-column",
        summary: "unqualified column in a query that reads two or more tables",
        fixable: false,
    },
    RuleInfo {
        code: "S002",
        name: "implicit-inner-join",
        summary: "bare JOIN instead of INNER JOIN",
        fixable: true,
    },
    RuleInfo {
        code: "S003",
        name: "select-star",
        summary: "SELECT * in the final result of a statement",
        fixable: false,
    },
    RuleInfo {
        code: "S004",
        name: "target-order",
        summary: "plain column listed after a calculated column",
        fixable: false,
    },
    RuleInfo {
        code: "S005",
        name: "keyword-case",
        summary: "keyword capitalisation differs from the rest of the file",
        fixable: true,
    },
    RuleInfo {
        code: "S006",
        name: "trailing-whitespace",
        summary: "spaces or tabs at the end of a line",
        fixable: true,
    },
    RuleInfo {
        code: "S007",
        name: "tab-character",
        summary: "tab character outside a string literal or comment",
        fixable: true,
    },
];

/// Look up a rule by code (case-insensitive).
pub fn rule_info(code: &str) -> Option<&'static RuleInfo> {
    RULES.iter().find(|r| r.code.eq_ignore_ascii_case(code))
}

/// One replacement on the source text, in byte offsets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextEdit {
    /// Start byte offset (inclusive).
    pub start: usize,
    /// End byte offset (exclusive). Equal to `start` for an insertion.
    pub end: usize,
    /// Replacement text.
    pub replacement: String,
}

/// One style finding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StyleFinding {
    /// Rule code, e.g. `S001`.
    pub code: &'static str,
    /// 1-based line.
    pub line: u64,
    /// 1-based column, counted in characters.
    pub col: u64,
    /// What is wrong.
    pub message: String,
    /// Short suggestion for the fix.
    pub hint: String,
    /// Edits that fix this finding. Empty when the rule is report-only.
    pub fix: Vec<TextEdit>,
}

/// The findings for one SQL text.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StyleReport {
    /// Findings, ordered by position then code.
    pub findings: Vec<StyleFinding>,
    /// `true` when the SQL did not parse, so `S001`, `S003` and `S004` did
    /// not run.
    pub ast_rules_skipped: bool,
}

/// Run every rule on `sql`.
pub fn lint_style(sql: &str) -> StyleReport {
    let index = LineIndex::new(sql);
    let tokens = Tokenizer::new(&DatabricksDialect, sql)
        .tokenize_with_location()
        .unwrap_or_default();

    let mut findings = Vec::new();
    lint_implicit_join(sql, &index, &tokens, &mut findings);
    lint_keyword_case(sql, &index, &tokens, &mut findings);
    lint_whitespace(sql, &index, &tokens, &mut findings);

    let ast_rules_skipped = match parse_sql(&mask_run_vars(sql)) {
        Ok(statements) => {
            let mut visitor = AstVisitor {
                depth: 0,
                findings: Vec::new(),
            };
            for stmt in &statements {
                let _ = stmt.visit(&mut visitor);
            }
            findings.append(&mut visitor.findings);
            false
        }
        Err(_) => true,
    };

    findings.sort_by(|a, b| (a.line, a.col, a.code).cmp(&(b.line, b.col, b.code)));
    StyleReport {
        findings,
        ast_rules_skipped,
    }
}

/// Replace each `@var(name)` or `@var(name, default)` marker with a `0`
/// padded by spaces to the same length. The compiler resolves these markers
/// before the SQL reaches a warehouse, so the parser should not see them. The
/// padding keeps every later line and column where the user wrote it.
fn mask_run_vars(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut rest = sql;
    while let Some(at) = rest.find("@var(") {
        let after = &rest[at..];
        let Some(close) = after.find(')') else { break };
        out.push_str(&rest[..at]);
        let marker = &after[..=close];
        out.push('0');
        out.extend(
            marker
                .chars()
                .skip(1)
                .map(|c| if c == '\n' { '\n' } else { ' ' }),
        );
        rest = &after[close + 1..];
    }
    out.push_str(rest);
    out
}

/// Apply `edits` to `sql`. Edits that overlap an earlier edit are skipped;
/// callers re-lint and apply again until nothing is left to fix.
pub fn apply_edits(sql: &str, edits: &[TextEdit]) -> String {
    let mut sorted: Vec<&TextEdit> = edits.iter().collect();
    sorted.sort_by_key(|e| (e.start, e.end));
    let mut out = String::with_capacity(sql.len());
    let mut cursor = 0;
    for edit in sorted {
        if edit.start < cursor || edit.end > sql.len() || edit.start > edit.end {
            continue;
        }
        out.push_str(&sql[cursor..edit.start]);
        out.push_str(&edit.replacement);
        cursor = edit.end;
    }
    out.push_str(&sql[cursor..]);
    out
}

/// Fix every fixable finding. Repeats until no fixable finding is left (at
/// most a few passes). Returns the new text and the number of findings fixed.
pub fn fix_style(sql: &str, enabled: &dyn Fn(&str) -> bool) -> (String, usize) {
    let mut text = sql.to_string();
    let mut fixed = 0;
    for _ in 0..5 {
        let report = lint_style(&text);
        let fixable: Vec<&StyleFinding> = report
            .findings
            .iter()
            .filter(|f| !f.fix.is_empty() && enabled(f.code))
            .collect();
        if fixable.is_empty() {
            break;
        }
        let edits: Vec<TextEdit> = fixable.iter().flat_map(|f| f.fix.clone()).collect();
        let next = apply_edits(&text, &edits);
        if next == text {
            break;
        }
        fixed += fixable.len();
        text = next;
    }
    (text, fixed)
}

// ---------------------------------------------------------------------------
// Positions
// ---------------------------------------------------------------------------

struct LineIndex {
    line_starts: Vec<usize>,
}

impl LineIndex {
    fn new(sql: &str) -> Self {
        let mut line_starts = vec![0];
        for (i, b) in sql.bytes().enumerate() {
            if b == b'\n' {
                line_starts.push(i + 1);
            }
        }
        Self { line_starts }
    }

    /// Byte offset of a 1-based (line, column-in-chars) position.
    fn offset(&self, sql: &str, line: u64, col: u64) -> Option<usize> {
        let start = *self
            .line_starts
            .get(usize::try_from(line).ok()?.checked_sub(1)?)?;
        let skip = usize::try_from(col).ok()?.checked_sub(1)?;
        let rest = &sql[start..];
        match rest.char_indices().nth(skip) {
            Some((i, _)) => Some(start + i),
            None => Some(sql.len()),
        }
    }
}

fn token_offset(sql: &str, index: &LineIndex, t: &TokenWithSpan) -> Option<usize> {
    index.offset(sql, t.span.start.line, t.span.start.column)
}

fn is_trivia(token: &Token) -> bool {
    matches!(token, Token::Whitespace(_))
}

/// Next non-whitespace, non-comment token after position `i`.
fn next_significant(tokens: &[TokenWithSpan], i: usize) -> Option<&Token> {
    tokens[i + 1..]
        .iter()
        .map(|t| &t.token)
        .find(|t| !is_trivia(t))
}

/// Previous non-whitespace, non-comment token before position `i`.
fn prev_significant(tokens: &[TokenWithSpan], i: usize) -> Option<&Token> {
    tokens[..i]
        .iter()
        .rev()
        .map(|t| &t.token)
        .find(|t| !is_trivia(t))
}

// ---------------------------------------------------------------------------
// S002 — implicit inner join
// ---------------------------------------------------------------------------

/// Words that, directly before `JOIN`, make the join type explicit.
const JOIN_TYPE_WORDS: &[&str] = &[
    "INNER",
    "LEFT",
    "RIGHT",
    "FULL",
    "OUTER",
    "CROSS",
    "NATURAL",
    "SEMI",
    "ANTI",
    "ASOF",
    "STRAIGHT_JOIN",
];

fn lint_implicit_join(
    sql: &str,
    index: &LineIndex,
    tokens: &[TokenWithSpan],
    out: &mut Vec<StyleFinding>,
) {
    for (i, t) in tokens.iter().enumerate() {
        let Token::Word(w) = &t.token else { continue };
        if w.quote_style.is_some() || !w.value.eq_ignore_ascii_case("JOIN") {
            continue;
        }
        if matches!(prev_significant(tokens, i), Some(Token::Period)) {
            continue;
        }
        if let Some(Token::Word(prev)) = prev_significant(tokens, i)
            && prev.quote_style.is_none()
            && JOIN_TYPE_WORDS
                .iter()
                .any(|k| prev.value.eq_ignore_ascii_case(k))
        {
            continue;
        }
        let Some(start) = token_offset(sql, index, t) else {
            continue;
        };
        let inner = if w.value.chars().all(|c| c.is_ascii_lowercase()) {
            "inner "
        } else if w.value.chars().all(|c| c.is_ascii_uppercase()) {
            "INNER "
        } else {
            "Inner "
        };
        out.push(StyleFinding {
            code: "S002",
            line: t.span.start.line,
            col: t.span.start.column,
            message: "bare JOIN does not say which kind of join it is".to_string(),
            hint: "write INNER JOIN".to_string(),
            fix: vec![TextEdit {
                start,
                end: start,
                replacement: inner.to_string(),
            }],
        });
    }
}

// ---------------------------------------------------------------------------
// S005 — keyword capitalisation
// ---------------------------------------------------------------------------

/// Structural keywords the capitalisation rule looks at. A short fixed list
/// keeps column and function names (`date`, `left(...)`) out of the rule.
const CASE_KEYWORDS: &[&str] = &[
    "SELECT",
    "FROM",
    "WHERE",
    "JOIN",
    "INNER",
    "LEFT",
    "RIGHT",
    "FULL",
    "OUTER",
    "CROSS",
    "ON",
    "AND",
    "OR",
    "NOT",
    "AS",
    "GROUP",
    "BY",
    "ORDER",
    "HAVING",
    "LIMIT",
    "OFFSET",
    "UNION",
    "INTERSECT",
    "EXCEPT",
    "ALL",
    "DISTINCT",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
    "IN",
    "IS",
    "NULL",
    "LIKE",
    "BETWEEN",
    "WITH",
    "OVER",
    "PARTITION",
    "ASC",
    "DESC",
    "USING",
    "EXISTS",
    "QUALIFY",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CaseStyle {
    Upper,
    Lower,
    Capitalised,
    Mixed,
}

fn case_style(word: &str) -> CaseStyle {
    if word == word.to_ascii_uppercase() {
        CaseStyle::Upper
    } else if word == word.to_ascii_lowercase() {
        CaseStyle::Lower
    } else {
        let mut chars = word.chars();
        let first_upper = chars.next().is_some_and(|c| c.is_ascii_uppercase());
        if first_upper && chars.as_str() == chars.as_str().to_ascii_lowercase() {
            CaseStyle::Capitalised
        } else {
            CaseStyle::Mixed
        }
    }
}

fn recase(word: &str, style: CaseStyle) -> String {
    match style {
        CaseStyle::Upper | CaseStyle::Mixed => word.to_ascii_uppercase(),
        CaseStyle::Lower => word.to_ascii_lowercase(),
        CaseStyle::Capitalised => {
            let lower = word.to_ascii_lowercase();
            let mut chars = lower.chars();
            match chars.next() {
                Some(c) => c.to_ascii_uppercase().to_string() + chars.as_str(),
                None => String::new(),
            }
        }
    }
}

fn style_name(style: CaseStyle) -> &'static str {
    match style {
        CaseStyle::Upper => "upper case",
        CaseStyle::Lower => "lower case",
        CaseStyle::Capitalised => "capitalised",
        CaseStyle::Mixed => "mixed case",
    }
}

fn lint_keyword_case(
    sql: &str,
    index: &LineIndex,
    tokens: &[TokenWithSpan],
    out: &mut Vec<StyleFinding>,
) {
    // (token index, style) of every structural keyword.
    let mut seen: Vec<(usize, CaseStyle)> = Vec::new();
    for (i, t) in tokens.iter().enumerate() {
        let Token::Word(w) = &t.token else { continue };
        if w.quote_style.is_some() {
            continue;
        }
        if !CASE_KEYWORDS
            .iter()
            .any(|k| w.value.eq_ignore_ascii_case(k))
        {
            continue;
        }
        // `t.end`, `x.select`: a field name, not a keyword.
        if matches!(prev_significant(tokens, i), Some(Token::Period)) {
            continue;
        }
        // `left(s, 3)` and `right(s, 3)` are function calls.
        if (w.value.eq_ignore_ascii_case("LEFT") || w.value.eq_ignore_ascii_case("RIGHT"))
            && matches!(next_significant(tokens, i), Some(Token::LParen))
        {
            continue;
        }
        seen.push((i, case_style(&w.value)));
    }

    let count = |s: CaseStyle| seen.iter().filter(|(_, x)| *x == s).count();
    let candidates = [
        (CaseStyle::Upper, count(CaseStyle::Upper)),
        (CaseStyle::Lower, count(CaseStyle::Lower)),
        (CaseStyle::Capitalised, count(CaseStyle::Capitalised)),
    ];
    // Majority style; upper case wins a tie, then lower case.
    let best = candidates.iter().map(|c| c.1).max().unwrap_or(0);
    let Some(&(majority, _)) = candidates.iter().find(|c| c.1 == best) else {
        return;
    };
    if best == 0 || seen.iter().all(|(_, s)| *s == majority) {
        return;
    }

    for (i, style) in seen {
        if style == majority {
            continue;
        }
        let t = &tokens[i];
        let Token::Word(w) = &t.token else { continue };
        let Some(start) = token_offset(sql, index, t) else {
            continue;
        };
        let fixed = recase(&w.value, majority);
        out.push(StyleFinding {
            code: "S005",
            line: t.span.start.line,
            col: t.span.start.column,
            message: format!(
                "keyword `{}` is {}; the rest of the file uses {}",
                w.value,
                style_name(style),
                style_name(majority)
            ),
            hint: format!("write `{fixed}`"),
            fix: vec![TextEdit {
                start,
                end: start + w.value.len(),
                replacement: fixed,
            }],
        });
    }
}

// ---------------------------------------------------------------------------
// S006 / S007 — whitespace
// ---------------------------------------------------------------------------

fn lint_whitespace(
    sql: &str,
    index: &LineIndex,
    tokens: &[TokenWithSpan],
    out: &mut Vec<StyleFinding>,
) {
    // Line ends that sit inside a multi-line literal. Trimming there would
    // change the value, so those lines are left alone.
    let mut protected: HashSet<u64> = HashSet::new();
    for t in tokens {
        if t.span.start.line != t.span.end.line && !matches!(t.token, Token::Whitespace(_)) {
            for line in t.span.start.line..t.span.end.line {
                protected.insert(line);
            }
        }
    }

    let mut tab_lines: HashSet<u64> = HashSet::new();
    for (n, raw) in sql.split('\n').enumerate() {
        let line_no = n as u64 + 1;
        if protected.contains(&line_no) {
            continue;
        }
        let line = raw.strip_suffix('\r').unwrap_or(raw);
        let trimmed = line.trim_end_matches([' ', '\t']);
        if trimmed.len() == line.len() {
            continue;
        }
        let start = index.line_starts[n] + trimmed.len();
        out.push(StyleFinding {
            code: "S006",
            line: line_no,
            col: trimmed.chars().count() as u64 + 1,
            message: "line ends with whitespace".to_string(),
            hint: "remove the trailing whitespace".to_string(),
            fix: vec![TextEdit {
                start,
                end: index.line_starts[n] + line.len(),
                replacement: String::new(),
            }],
        });
        tab_lines.insert(line_no);
    }

    // Tabs: one finding per line, at the first tab. Tabs in a trailing run
    // belong to S006.
    let mut by_line: Vec<(u64, Vec<&TokenWithSpan>)> = Vec::new();
    for t in tokens {
        if !matches!(t.token, Token::Whitespace(Whitespace::Tab)) {
            continue;
        }
        match by_line.last_mut() {
            Some((line, v)) if *line == t.span.start.line => v.push(t),
            _ => by_line.push((t.span.start.line, vec![t])),
        }
    }
    for (line_no, tabs) in by_line {
        let line_start = index.line_starts[usize::try_from(line_no).unwrap_or(1) - 1];
        let mut edits = Vec::new();
        let mut first: Option<&TokenWithSpan> = None;
        for t in tabs {
            let Some(at) = token_offset(sql, index, t) else {
                continue;
            };
            let rest = &sql[at + 1..];
            let line_rest = rest.split('\n').next().unwrap_or("");
            if line_rest.trim_end_matches(['\r', ' ', '\t']).is_empty() {
                continue; // trailing: S006
            }
            let leading = sql[line_start..at].chars().all(|c| c == ' ' || c == '\t');
            edits.push(TextEdit {
                start: at,
                end: at + 1,
                replacement: if leading { "    " } else { " " }.to_string(),
            });
            first.get_or_insert(t);
        }
        let Some(first) = first else { continue };
        out.push(StyleFinding {
            code: "S007",
            line: first.span.start.line,
            col: first.span.start.column,
            message: "tab character in the SQL text".to_string(),
            hint: "indent with four spaces".to_string(),
            fix: edits,
        });
    }
}

// ---------------------------------------------------------------------------
// S001 / S003 / S004 — AST rules
// ---------------------------------------------------------------------------

/// Bare identifiers that parse as columns but are session values.
const NOT_COLUMNS: &[&str] = &[
    "CURRENT_DATE",
    "CURRENT_TIMESTAMP",
    "CURRENT_TIME",
    "CURRENT_USER",
    "SESSION_USER",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "USER",
    "TRUE",
    "FALSE",
    "NULL",
];

struct AstVisitor {
    /// Query nesting depth. `0` is the outermost query of a statement.
    depth: usize,
    findings: Vec<StyleFinding>,
}

impl Visitor for AstVisitor {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        let top = self.depth == 0;
        self.analyse_set_expr(&query.body, query, top);
        self.depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
        self.depth = self.depth.saturating_sub(1);
        ControlFlow::Continue(())
    }
}

impl AstVisitor {
    fn analyse_set_expr(&mut self, body: &SetExpr, query: &Query, top: bool) {
        match body {
            SetExpr::Select(select) => {
                self.check_ambiguous(select, query);
                self.check_order(select);
                if top {
                    self.check_star(select);
                }
            }
            SetExpr::SetOperation { left, right, .. } => {
                self.analyse_set_expr(left, query, top);
                self.analyse_set_expr(right, query, top);
            }
            // A parenthesised query is visited as its own `Query`.
            _ => {}
        }
    }

    fn check_star(&mut self, select: &Select) {
        for item in &select.projection {
            if !matches!(
                item,
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..)
            ) {
                continue;
            }
            let start = item.span().start;
            if start.line == 0 {
                continue;
            }
            self.findings.push(StyleFinding {
                code: "S003",
                line: start.line,
                col: start.column,
                message: "SELECT * lets an upstream column change reach this model's output"
                    .to_string(),
                hint: "list the columns this model should output".to_string(),
                fix: Vec::new(),
            });
        }
    }

    fn check_order(&mut self, select: &Select) {
        if select.projection.len() < 2 {
            return;
        }
        let mut highest = 0u8;
        for item in &select.projection {
            let class = target_class(item);
            if class < highest {
                let start = item.span().start;
                if start.line != 0 {
                    self.findings.push(StyleFinding {
                        code: "S004",
                        line: start.line,
                        col: start.column,
                        message: "a plain column is listed after a calculated column".to_string(),
                        hint: "list wildcards first, then plain columns, then calculations"
                            .to_string(),
                        fix: Vec::new(),
                    });
                }
                return;
            }
            highest = highest.max(class);
        }
    }

    fn check_ambiguous(&mut self, select: &Select, query: &Query) {
        let tables = count_relations(select);
        if tables < 2 || has_natural_join(select) {
            return;
        }
        let mut collector = ColumnCollector {
            query_depth: 0,
            idents: Vec::new(),
            ignore: HashSet::new(),
        };
        // Names that are legitimately bare: output aliases (a later clause can
        // read an earlier alias) and the merged columns of `JOIN ... USING`.
        for item in &select.projection {
            match item {
                SelectItem::ExprWithAlias { alias, .. } => {
                    collector.ignore.insert(alias.value.to_ascii_lowercase());
                }
                SelectItem::ExprWithAliases { aliases, .. } => {
                    for a in aliases {
                        collector.ignore.insert(a.value.to_ascii_lowercase());
                    }
                }
                _ => {}
            }
        }
        for twj in &select.from {
            for join in &twj.joins {
                if let Some(JoinConstraint::Using(cols)) = join_constraint(join) {
                    for name in cols {
                        if let Some(last) = name.0.last().and_then(|p| p.as_ident()) {
                            collector.ignore.insert(last.value.to_ascii_lowercase());
                        }
                    }
                }
            }
        }
        let _ = select.visit(&mut collector);
        if let Some(order_by) = &query.order_by {
            let _ = order_by.visit(&mut collector);
        }

        let mut reported: HashSet<String> = HashSet::new();
        for (name, line, col) in collector.idents {
            let key = name.to_ascii_lowercase();
            if collector.ignore.contains(&key)
                || NOT_COLUMNS.iter().any(|k| k.eq_ignore_ascii_case(&name))
                || !reported.insert(key)
            {
                continue;
            }
            self.findings.push(StyleFinding {
                code: "S001",
                line,
                col,
                message: format!(
                    "column `{name}` has no table qualifier in a query that reads {tables} tables"
                ),
                hint: format!("write `<alias>.{name}`"),
                fix: Vec::new(),
            });
        }
    }
}

/// 0 = wildcard, 1 = plain column (optionally renamed), 2 = anything else.
fn target_class(item: &SelectItem) -> u8 {
    fn plain(expr: &Expr) -> bool {
        matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
    }
    match item {
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) => 0,
        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } if plain(e) => 1,
        _ => 2,
    }
}

fn join_constraint(join: &Join) -> Option<&JoinConstraint> {
    match &join.join_operator {
        JoinOperator::Join(c)
        | JoinOperator::Inner(c)
        | JoinOperator::Left(c)
        | JoinOperator::LeftOuter(c)
        | JoinOperator::Right(c)
        | JoinOperator::RightOuter(c)
        | JoinOperator::FullOuter(c)
        | JoinOperator::CrossJoin(c)
        | JoinOperator::Semi(c)
        | JoinOperator::LeftSemi(c)
        | JoinOperator::RightSemi(c)
        | JoinOperator::Anti(c)
        | JoinOperator::LeftAnti(c)
        | JoinOperator::RightAnti(c)
        | JoinOperator::StraightJoin(c)
        | JoinOperator::AsOf { constraint: c, .. } => Some(c),
        JoinOperator::CrossApply
        | JoinOperator::OuterApply
        | JoinOperator::ArrayJoin
        | JoinOperator::LeftArrayJoin
        | JoinOperator::InnerArrayJoin => None,
    }
}

fn has_natural_join(select: &Select) -> bool {
    select.from.iter().any(|twj| {
        twj.joins
            .iter()
            .any(|j| matches!(join_constraint(j), Some(JoinConstraint::Natural)))
    })
}

/// Number of tables and subqueries in the FROM clause. Table functions such
/// as `UNNEST` read from a table already counted, so they do not count.
fn count_relations(select: &Select) -> usize {
    fn factor(f: &TableFactor) -> usize {
        match f {
            TableFactor::Table { args: None, .. } | TableFactor::Derived { .. } => 1,
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                factor(&table_with_joins.relation)
                    + table_with_joins
                        .joins
                        .iter()
                        .map(|j| factor(&j.relation))
                        .sum::<usize>()
            }
            _ => 0,
        }
    }
    select
        .from
        .iter()
        .map(|twj| {
            factor(&twj.relation) + twj.joins.iter().map(|j| factor(&j.relation)).sum::<usize>()
        })
        .sum()
}

fn lambda_params(expr: &Expr, ignore: &mut HashSet<String>) {
    match expr {
        Expr::Identifier(id) => {
            ignore.insert(id.value.to_ascii_lowercase());
        }
        Expr::Nested(inner) => lambda_params(inner, ignore),
        Expr::Tuple(items) => items.iter().for_each(|e| lambda_params(e, ignore)),
        _ => {}
    }
}

/// Collects bare identifiers that belong to one `SELECT`, not to a subquery
/// or a lambda inside it.
struct ColumnCollector {
    query_depth: usize,
    idents: Vec<(String, u64, u64)>,
    ignore: HashSet<String>,
}

impl Visitor for ColumnCollector {
    type Break = ();

    fn pre_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
        self.query_depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
        self.query_depth = self.query_depth.saturating_sub(1);
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        if self.query_depth > 0 {
            return ControlFlow::Continue(());
        }
        match expr {
            Expr::Lambda(l) => {
                for p in l.params.iter() {
                    self.ignore.insert(p.name.value.to_ascii_lowercase());
                }
            }
            // This dialect reads `v -> v > 1` as a binary `->`. When the
            // right side is not a literal key (`payload -> 'k'`), the left
            // side is a lambda parameter, not a column.
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Arrow,
                right,
            } if !matches!(**right, Expr::Value(_)) => {
                lambda_params(left, &mut self.ignore);
            }
            Expr::Identifier(id) if id.span.start.line != 0 => {
                self.idents
                    .push((id.value.clone(), id.span.start.line, id.span.start.column));
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn codes(sql: &str) -> Vec<&'static str> {
        lint_style(sql).findings.iter().map(|f| f.code).collect()
    }

    fn has(sql: &str, code: &str) -> bool {
        codes(sql).contains(&code)
    }

    // --- S001 -------------------------------------------------------------

    #[test]
    fn s001_flags_unqualified_column_in_join() {
        let r =
            lint_style("SELECT o.id, amount FROM orders o INNER JOIN items i ON o.id = i.order_id");
        let f: Vec<_> = r.findings.iter().filter(|f| f.code == "S001").collect();
        assert_eq!(f.len(), 1);
        assert_eq!((f[0].line, f[0].col), (1, 14));
        assert!(f[0].message.contains("`amount`"));
    }

    #[test]
    fn s001_flags_comma_join() {
        assert!(has("SELECT a.x, y FROM a, b WHERE a.k = b.k", "S001"));
    }

    #[test]
    fn s001_silent_when_all_qualified() {
        assert!(!has(
            "SELECT o.id, i.amount FROM orders o INNER JOIN items i ON o.id = i.order_id",
            "S001"
        ));
    }

    #[test]
    fn s001_silent_for_single_table() {
        assert!(!has(
            "SELECT id, amount FROM orders WHERE amount > 0",
            "S001"
        ));
    }

    #[test]
    fn s001_silent_for_using_column_and_aliases() {
        assert!(!has(
            "SELECT id, a.x FROM a INNER JOIN b USING (id)",
            "S001"
        ));
        assert!(!has(
            "SELECT a.x + b.y AS total FROM a INNER JOIN b ON a.k = b.k ORDER BY total",
            "S001"
        ));
        assert!(!has(
            "SELECT a.x AS t FROM a INNER JOIN b ON a.k = b.k GROUP BY t",
            "S001"
        ));
    }

    #[test]
    fn s001_silent_for_natural_join_subquery_and_lambda() {
        assert!(!has("SELECT id FROM a NATURAL JOIN b", "S001"));
        // The subquery reads one table; its bare column is not ambiguous.
        assert!(!has(
            "SELECT a.x FROM a INNER JOIN b ON a.k = b.k WHERE a.x IN (SELECT y FROM c)",
            "S001"
        ));
        assert!(!has(
            "SELECT a.x, filter(a.arr, v -> v > 1) AS f FROM a INNER JOIN b ON a.k = b.k",
            "S001"
        ));
    }

    #[test]
    fn s001_silent_for_session_values_and_cte_single_table() {
        assert!(!has(
            "SELECT a.x, current_date FROM a INNER JOIN b ON a.k = b.k",
            "S001"
        ));
        assert!(!has(
            "WITH c AS (SELECT id FROM t) SELECT id FROM c",
            "S001"
        ));
    }

    #[test]
    fn s001_silent_for_unnest_lateral() {
        assert!(!has(
            "SELECT o.id, item FROM orders o CROSS JOIN UNNEST(o.items) AS t(item)",
            "S001"
        ));
    }

    // --- S002 -------------------------------------------------------------

    #[test]
    fn s002_flags_bare_join_and_fixes() {
        let sql = "SELECT a.x FROM a JOIN b ON a.k = b.k";
        let r = lint_style(sql);
        let f: Vec<_> = r.findings.iter().filter(|f| f.code == "S002").collect();
        assert_eq!(f.len(), 1);
        assert_eq!((f[0].line, f[0].col), (1, 19));
        let (fixed, n) = fix_style(sql, &|_| true);
        assert_eq!(fixed, "SELECT a.x FROM a INNER JOIN b ON a.k = b.k");
        assert_eq!(n, 1);
    }

    #[test]
    fn s002_keeps_case_of_the_join_word() {
        let (fixed, _) = fix_style("select a.x from a join b on a.k = b.k", &|_| true);
        assert_eq!(fixed, "select a.x from a inner join b on a.k = b.k");
    }

    #[test]
    fn s002_silent_for_explicit_join_types() {
        for j in [
            "INNER JOIN",
            "LEFT JOIN",
            "LEFT OUTER JOIN",
            "RIGHT JOIN",
            "FULL OUTER JOIN",
            "CROSS JOIN",
            "LEFT SEMI JOIN",
            "LEFT ANTI JOIN",
            "NATURAL JOIN",
            "left\n  join",
        ] {
            let sql = format!("SELECT a.x FROM a {j} b ON a.k = b.k");
            assert!(!has(&sql, "S002"), "{j}");
        }
    }

    #[test]
    fn s002_ignores_join_inside_string_comment_and_field() {
        assert!(!has("SELECT 'a join b' AS s FROM t", "S002"));
        assert!(!has("SELECT 1 FROM t -- join later\n", "S002"));
        assert!(!has("SELECT t.join FROM t", "S002"));
    }

    // --- S003 -------------------------------------------------------------

    #[test]
    fn s003_flags_final_select_star() {
        let r = lint_style("SELECT * FROM orders");
        assert!(r.findings.iter().any(|f| f.code == "S003"));
        assert!(has("SELECT o.* FROM orders o", "S003"));
    }

    #[test]
    fn s003_silent_for_cte_subquery_and_explicit_columns() {
        assert!(!has(
            "WITH c AS (SELECT * FROM t) SELECT id, name FROM c",
            "S003"
        ));
        assert!(!has(
            "SELECT id FROM t WHERE EXISTS (SELECT * FROM u WHERE u.id = t.id)",
            "S003"
        ));
        assert!(!has("SELECT id, name FROM t", "S003"));
        assert!(!has("SELECT count(*) AS n FROM t", "S003"));
    }

    // --- S004 -------------------------------------------------------------

    #[test]
    fn s004_flags_column_after_calculation() {
        let r = lint_style("SELECT a + b AS total, c FROM t");
        let f: Vec<_> = r.findings.iter().filter(|f| f.code == "S004").collect();
        assert_eq!(f.len(), 1);
        assert_eq!((f[0].line, f[0].col), (1, 24));
        assert!(has("SELECT amount * 2 AS d, * FROM t", "S004"));
    }

    #[test]
    fn s004_silent_for_ordered_targets() {
        assert!(!has(
            "SELECT *, a, b AS renamed, a + b AS total FROM t",
            "S004"
        ));
        assert!(!has("SELECT a + b AS total FROM t", "S004"));
        assert!(!has(
            "SELECT a, t.b, upper(c) AS u, d + 1 AS e FROM t",
            "S004"
        ));
    }

    // --- S005 -------------------------------------------------------------

    #[test]
    fn s005_flags_minority_style_and_fixes() {
        let sql = "SELECT a\nfrom t\nWHERE a > 1\nORDER BY a";
        let r = lint_style(sql);
        let f: Vec<_> = r.findings.iter().filter(|f| f.code == "S005").collect();
        assert_eq!(f.len(), 1);
        assert_eq!((f[0].line, f[0].col), (2, 1));
        let (fixed, n) = fix_style(sql, &|_| true);
        assert_eq!(fixed, "SELECT a\nFROM t\nWHERE a > 1\nORDER BY a");
        assert_eq!(n, 1);
    }

    #[test]
    fn s005_silent_for_consistent_files() {
        assert!(!has("SELECT a FROM t WHERE a > 1", "S005"));
        assert!(!has("select a from t where a > 1", "S005"));
        assert!(!has("Select a From t Where a > 1", "S005"));
    }

    #[test]
    fn s005_ignores_columns_functions_literals_and_comments() {
        // `left(...)` is a function; `end` after a period is a field; the
        // lower-case words in the string and the comment are not keywords.
        assert!(!has(
            "SELECT left(name, 3) AS l, t.end, 'from where' AS s FROM t -- select from\n",
            "S005"
        ));
        // A column named like a keyword that is not in the list.
        assert!(!has("SELECT date, name FROM t", "S005"));
    }

    // --- S006 / S007 ------------------------------------------------------

    #[test]
    fn s006_flags_trailing_whitespace_and_fixes() {
        let sql = "SELECT a  \nFROM t\t\n";
        let r = lint_style(sql);
        let f: Vec<_> = r.findings.iter().filter(|f| f.code == "S006").collect();
        assert_eq!(f.len(), 2);
        assert_eq!((f[0].line, f[0].col), (1, 9));
        let (fixed, n) = fix_style(sql, &|_| true);
        assert_eq!(fixed, "SELECT a\nFROM t\n");
        assert_eq!(n, 2);
        assert!(!has(&fixed, "S007"));
    }

    #[test]
    fn s006_silent_for_clean_lines_crlf_and_literals() {
        assert!(!has("SELECT a\nFROM t\n", "S006"));
        assert!(!has("SELECT a\r\nFROM t\r\n", "S006"));
        // Trailing spaces inside a multi-line string are data.
        let sql = "SELECT 'a  \nb' AS s FROM t";
        assert!(!has(sql, "S006"));
        assert_eq!(fix_style(sql, &|_| true).0, sql);
    }

    #[test]
    fn s006_flags_trailing_whitespace_in_comment() {
        assert!(has("-- note   \nSELECT 1", "S006"));
    }

    #[test]
    fn s007_flags_tabs_and_fixes() {
        let sql = "SELECT\n\ta,\tb\nFROM t";
        let r = lint_style(sql);
        let f: Vec<_> = r.findings.iter().filter(|f| f.code == "S007").collect();
        assert_eq!(f.len(), 1);
        assert_eq!((f[0].line, f[0].col), (2, 1));
        let (fixed, _) = fix_style(sql, &|_| true);
        assert_eq!(fixed, "SELECT\n    a, b\nFROM t");
    }

    #[test]
    fn s007_silent_for_tab_inside_string() {
        assert!(!has("SELECT 'a\tb' AS s FROM t", "S007"));
    }

    // --- shared -----------------------------------------------------------

    #[test]
    fn run_var_markers_do_not_stop_the_ast_rules() {
        let r = lint_style("SELECT *\nFROM t\nWHERE a >= @var(min_amount, 0) AND b = '@var(x)'");
        assert!(!r.ast_rules_skipped);
        let f = r.findings.iter().find(|f| f.code == "S003").unwrap();
        assert_eq!((f.line, f.col), (1, 8));
        // A marker keeps later columns where the user wrote them.
        let r = lint_style("SELECT a.x, @var(v) + 1 AS y, z FROM a JOIN b ON a.k = b.k");
        let s1 = r.findings.iter().find(|f| f.code == "S001").unwrap();
        assert_eq!((s1.line, s1.col), (1, 31));
    }

    #[test]
    fn json_arrow_on_a_column_is_still_checked() {
        assert!(has(
            "SELECT a.id, payload -> 'k' AS v FROM a INNER JOIN b ON a.k = b.k",
            "S001"
        ));
    }

    #[test]
    fn parse_failure_skips_only_ast_rules() {
        let r = lint_style("SELECT * FROM (  \n");
        assert!(r.ast_rules_skipped);
        assert!(r.findings.iter().any(|f| f.code == "S006"));
        assert!(!r.findings.iter().any(|f| f.code == "S003"));
    }

    #[test]
    fn fixed_sql_keeps_the_same_tokens() {
        let sql = "select a.x,\tb.y  \nFROM a JOIN b on a.k = b.k\n";
        let (fixed, _) = fix_style(sql, &|_| true);
        let norm = |s: &str| {
            s.split_whitespace()
                .filter(|w| !w.eq_ignore_ascii_case("inner"))
                .map(str::to_ascii_lowercase)
                .collect::<Vec<_>>()
        };
        assert_eq!(norm(sql), norm(&fixed));
        assert!(lint_style(&fixed).findings.is_empty());
    }

    #[test]
    fn rule_table_matches_emitted_codes() {
        for r in RULES {
            assert!(rule_info(r.code).is_some());
        }
        assert!(rule_info("s002").is_some());
        assert!(rule_info("S999").is_none());
    }
}
