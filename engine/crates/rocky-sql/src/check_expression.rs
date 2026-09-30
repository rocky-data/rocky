//! Content boundary for declarative `expression` checks (#1524).
//!
//! An expression check is interpolated verbatim into
//! `SELECT COUNT(*) FROM <table> WHERE NOT (<expression>)` and executed with
//! the project's warehouse credentials. Two callers run that statement: a
//! human at `rocky test --declarative`, and the fulfillment loop, unattended,
//! after every apply. So the expression's *content* is an execution surface,
//! and this module is the boundary on what it may be.
//!
//! The boundary is an **allowlist**, not a denylist, and the reason is worth
//! stating because the denylist is the obvious cheaper design. Rejecting
//! subqueries and table-valued functions misses read primitives that sit in
//! ordinary scalar position and look like any other call in the AST:
//!
//! | dialect    | scalar-position trigger              | reaches           |
//! |------------|--------------------------------------|-------------------|
//! | DuckDB     | `read_text('/etc/passwd') IS NULL`   | the filesystem    |
//! | Snowflake  | `GETVARIABLE('SECRET') IS NULL`      | session variables |
//! | BigQuery   | `project.ds.remote_fn(col) IS NULL`  | an HTTP endpoint  |
//! | Databricks | `secret('scope','key') IS NULL`      | the secret store  |
//! | Trino      | `catalog.schema.udf(col) IS NULL`    | any plugin        |
//!
//! The read primitive is not a shape; it is specific names, one per dialect.
//! Only a named set of pure scalar functions is a boundary.
//!
//! What is refused, in order:
//! 1. anything that does not parse as exactly one expression under the
//!    target dialect (trailing tokens included — `x) OR 1=1 --` must not
//!    be able to close the generated `NOT (` and add clauses);
//! 2. any nested query, wherever it appears;
//! 3. any qualified function name — that is how UDFs, remote functions and
//!    plugin functions are reached;
//! 4. any unqualified function not in [`CHECK_EXPRESSION_FUNCTIONS`];
//! 5. an allowlisted function called in the one shape whose result is not a
//!    function of its arguments alone: the bare one-argument form of
//!    `to_date`, `to_timestamp` and `to_char`; a `week`-shaped part (or a
//!    synonym) in `date_trunc` / `datediff` or the date/time form of
//!    `trunc` / `truncate`; and a `week`-, `dayofweek`- or
//!    `yearofweek`-shaped part (or a synonym) in `date_part`. These forms
//!    read a Snowflake session parameter (#1942, #2141);
//! 6. `EXTRACT(<part> FROM <expr>)` on the same three unsafe part families
//!    `date_part` refuses — `EXTRACT` is sqlparser's own AST node, not a
//!    `Function` call, so it needs its own gate rather than inheriting
//!    `date_part`'s (#2141).
//!
//! Refusal happens when the test SQL is generated, which is before execution
//! on every path, so a refused check is visible in the deferred/executed
//! counts the fulfillment loop reports rather than silently dropped. The
//! same validator runs before the sidecar write in the MCP `draft_check`
//! tool, so a bad expression is refused when written, not when run.

use std::ops::ControlFlow;

use sqlparser::ast::{
    DateTimeField, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, ObjectNamePart,
    Query, TableFactor, UnaryOperator, Value, Visit, Visitor,
};
use sqlparser::dialect::{
    BigQueryDialect, DatabricksDialect, Dialect, DuckDbDialect, GenericDialect, SnowflakeDialect,
};
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use crate::validation::ValidationError;

/// Pure scalar functions an `expression` check may call, lowercase.
///
/// Every entry must be a function of its arguments alone: no filesystem,
/// network, secret store, session variable, or identity read. Non-determinism
/// on its own is tolerated (`now()`), a read of anything outside the row is
/// not. Constructs the parser turns into dedicated AST nodes rather than
/// `Function` — `CAST`, `EXTRACT`, `TRIM`, `SUBSTRING`, `POSITION`, `CEIL`,
/// `FLOOR`, `CASE` — need no entry here.
///
/// Every `expression` check in the tree at the time of writing uses
/// comparisons only, so this list is a starting point sized to plausible
/// use, not a survey. A refusal names the function; extending the list is a
/// one-line change here.
///
/// # What the allowlist does not guarantee
///
/// The allowlist matches a function's **name**, not the code it runs. A
/// warehouse that lets a session rebind a built-in under an unqualified
/// call routes the allowlisted call to the rebound body instead. On a
/// persistent, file-backed DuckDB, creating that binding needs the same
/// file-write access that already lets you edit the data; Rocky's
/// in-memory DuckDB was not probed. Measured per target dialect (#1935):
///
/// | Dialect | Unqualified rebind wins? | Measured on |
/// |---|---|---|
/// | DuckDB | Yes — an unqualified `CREATE MACRO` shadows the built-in, even for a new session | v1.5.5, persistent file |
/// | Databricks | No — unqualified stays built-in; a qualified override exists but is already refused | Unity Catalog |
/// | Snowflake | Not probed (no sandbox) | — |
/// | BigQuery | Not probed (no sandbox) | — |
/// | Trino | Not probed | — |
pub const CHECK_EXPRESSION_FUNCTIONS: &[&str] = &[
    // null handling / conditionals
    "coalesce",
    "nullif",
    "ifnull",
    "nvl",
    "iff",
    "if",
    "greatest",
    "least",
    // numeric
    "abs",
    "sign",
    "mod",
    "power",
    "pow",
    "sqrt",
    "round",
    "trunc",
    "truncate",
    "ceiling",
    "isnan",
    "isinf",
    "isfinite",
    "is_nan",
    "is_inf",
    // string
    "length",
    "len",
    "char_length",
    "character_length",
    "lower",
    "upper",
    "ltrim",
    "rtrim",
    "btrim",
    "concat",
    "replace",
    "left",
    "right",
    "reverse",
    "repeat",
    "lpad",
    "rpad",
    "split_part",
    "strpos",
    "instr",
    "starts_with",
    "startswith",
    "ends_with",
    "endswith",
    "contains",
    "regexp_matches",
    "regexp_like",
    "regexp_contains",
    "rlike",
    "regexp_replace",
    "regexp_extract",
    // date / time
    "now",
    "current_date",
    "current_timestamp",
    "current_time",
    "localtime",
    "localtimestamp",
    "date",
    "datetime",
    "timestamp",
    "to_date",
    "to_timestamp",
    "year",
    "month",
    "day",
    "hour",
    "minute",
    "second",
    "date_trunc",
    "date_part",
    "datediff",
    "date_diff",
    "dateadd",
    "date_add",
    "date_sub",
    "epoch",
    // type
    "try_cast",
    "safe_cast",
    "typeof",
    // hashing (pure functions of their input)
    "hash",
    "md5",
    "sha1",
    "sha2",
    "sha256",
    // encoding (pure)
    "hex",
    "to_hex",
    "encode",
    "decode",
    // date formatting. A grouping key very often buckets a timestamp by a
    // formatted string.
    //
    // `to_char` is back on the list (#1942; removed by #1922). Snowflake's
    // one-argument form still falls back to the session's output-format
    // parameter, so it is not unconditionally safe — but its two-argument
    // form (`to_char(x, fmt)`) is a function of its arguments alone, and
    // `shape_refusal` below refuses the one-argument form specifically.
    "to_char",
    "date_format",
    "format_date",
    // string shaping (pure)
    "translate",
    "initcap",
    "ascii",
    "chr",
    "normalize",
    "array_to_string",
    "json_extract",
];

/// Names deliberately NOT on the list, and why. Kept next to it so the next
/// person does not "fix" an omission that is a decision.
///
/// | name | why not |
/// |---|---|
/// | `string_agg`, and aggregates generally | an aggregate is not a per-row value. A grouping key that aggregates is a defect, and a predicate cannot aggregate at all |
/// | `uuid`, `random` | not deterministic. A non-deterministic grouping key groups nothing; refusing it is the point |
/// | `collate` | a clause, not a scalar function |
///
/// These are exclusions by SEMANTICS, not by the read-boundary the rest of
/// the list is about — they are pure, and still wrong here.
const _NOT_ALLOWED_ON_PURPOSE: () = ();

/// Niladic session-identity functions that SQL lets you write WITHOUT
/// parentheses, lowercase.
///
/// Under `GenericDialect` sqlparser turns a bare `CURRENT_USER` into an
/// `Expr::Function` and the allowlist refuses it. Under the warehouse
/// dialects the same bare keyword falls through to a plain identifier —
/// indistinguishable from a column named `current_user` — and would pass
/// as a column reference. These are session-state reads, the class the
/// allowlist exists to exclude, so an UNQUOTED identifier with one of these
/// names is refused. A quoted `"current_user"` is unambiguously a column
/// and passes.
const SESSION_IDENTITY_KEYWORDS: &[&str] = &[
    "current_user",
    "session_user",
    "user",
    "current_role",
    "current_catalog",
    "current_schema",
    "current_database",
    "current_account",
    "current_warehouse",
    "current_session",
];

/// Date/time parts for `datediff` whose result does NOT
/// depend on Snowflake's `WEEK_START` session parameter (#1942), lowercase.
///
/// An allowlist, on purpose, matching the reasoning at the top of this
/// module: Snowflake documents that `WEEK_START` controls the output
/// "[w]hen `date_or_time_part` is `week` (or any of its variations)"
/// (`DATEDIFF`). Denying just `"week"` would miss its
/// documented synonyms (`w`, `wk`, `weekofyear`, `woy`, `wy`), so this is the
/// complete set of every OTHER documented `DATEDIFF` part and its synonyms instead —
/// covering every real key/predicate shape this repo uses
/// (`docs/src/content/docs/**`, `examples/**` grep clean of `week` parts as
/// of #1942) without having to enumerate week's aliases correctly.
///
/// The ISO week part (`week_iso` and its synonyms) is deliberately INCLUDED:
/// an ISO week is fixed to start on Monday by the ISO 8601 standard, so
/// Snowflake does not consult `WEEK_START` for it — only the plain `week`
/// part is session-dependent.
const SAFE_DATEDIFF_PARTS: &[&str] = &[
    // Year
    "year",
    "y",
    "yy",
    "yyy",
    "yyyy",
    "yr",
    "years",
    "yrs",
    // Quarter
    "quarter",
    "q",
    "qtr",
    "qtrs",
    "quarters",
    // Month
    "month",
    "mm",
    "mon",
    "mons",
    "months",
    // Week (ISO) — fixed to Monday-start, NOT WEEK_START-dependent.
    "week_iso",
    "weekiso",
    "weekofyeariso",
    "weekofyear_iso",
    // Day
    "day",
    "d",
    "dd",
    "days",
    "dayofmonth",
    // Hour
    "hour",
    "h",
    "hh",
    "hr",
    "hours",
    "hrs",
    // Minute
    "minute",
    "m",
    "mi",
    "min",
    "minutes",
    "mins",
    // Second
    "second",
    "s",
    "sec",
    "seconds",
    "secs",
    // Millisecond
    "millisecond",
    "ms",
    "msec",
    "milliseconds",
    // Microsecond
    "microsecond",
    "us",
    "usec",
    "microseconds",
    // Nanosecond
    "nanosecond",
    "ns",
    "nsec",
    "nanosec",
    "nsecond",
    "nanoseconds",
    "nanosecs",
    "nseconds",
];

/// DATE_TRUNC (and date/time TRUNC/TRUNCATE) also accepts the ISO-fixed
/// `yearofweekiso` part; Snowflake does not list it for DATEDIFF.
/// See the `yearofweekiso` row in "Supported date and time parts".
const SAFE_TRUNCATION_PARTS: &[&str] = &[
    // Year
    "year",
    "y",
    "yy",
    "yyy",
    "yyyy",
    "yr",
    "years",
    "yrs",
    // Quarter
    "quarter",
    "q",
    "qtr",
    "qtrs",
    "quarters",
    // Month
    "month",
    "mm",
    "mon",
    "mons",
    "months",
    // Week (ISO) — fixed to Monday-start, NOT WEEK_START-dependent.
    "week_iso",
    "weekiso",
    "weekofyeariso",
    "weekofyear_iso",
    // ISO year of week — fixed semantics; absent from DATEDIFF.
    "yearofweekiso",
    // Day
    "day",
    "d",
    "dd",
    "days",
    "dayofmonth",
    // Hour
    "hour",
    "h",
    "hh",
    "hr",
    "hours",
    "hrs",
    // Minute
    "minute",
    "m",
    "mi",
    "min",
    "minutes",
    "mins",
    // Second
    "second",
    "s",
    "sec",
    "seconds",
    "secs",
    // Millisecond
    "millisecond",
    "ms",
    "msec",
    "milliseconds",
    // Microsecond
    "microsecond",
    "us",
    "usec",
    "microseconds",
    // Nanosecond
    "nanosecond",
    "ns",
    "nsec",
    "nanosec",
    "nsecond",
    "nanoseconds",
    "nanosecs",
    "nseconds",
];

/// Date/time parts for `date_part` (and `EXTRACT`) whose result does NOT
/// depend on any Snowflake session parameter (#2141), lowercase.
///
/// `date_part` accepts every part `date_trunc`/`datediff` do (so this
/// superset repeats [`SAFE_TRUNCATION_PARTS`]'s entries rather than sharing
/// them — no `const fn` slice concatenation without a helper crate, and an
/// independent literal is what this module already does for that
/// list), plus parts `date_trunc`/`datediff` don't take at all:
/// `dayofweek(_iso)`, `dayofyear`, `yearofweek(iso)`, the `epoch_*` family
/// and `timezone_hour`/`timezone_minute`. Confirmed against Snowflake's
/// `DATE_PART` and "Supported date and time parts" pages (2026-09-23):
///
/// - `date_or_time_part` is `week` (or any of its variations) → controlled
///   by `WEEK_START`.
/// - `date_or_time_part` is `dayofweek` or `yearofweek` (or any of their
///   variations) → controlled by `WEEK_OF_YEAR_POLICY` and `WEEK_START`.
/// - `DAYOFWEEKISO`, `WEEKISO` and `YEAROFWEEKISO` (and their variations)
///   are NOT controlled by either parameter — ISO weeks always start on
///   Monday by the ISO 8601 standard.
///
/// So exactly three part families are excluded here and refused by
/// [`shape_refusal`]: `week` (`w`, `wk`, `weekofyear`, `woy`, `wy`),
/// `dayofweek` (`weekday`, `dow`, `dw`) and `yearofweek` (no documented
/// synonym). Every other documented part, including every ISO variant, is
/// safe and included.
const SAFE_DATE_PART_PARTS: &[&str] = &[
    // Year
    "year",
    "y",
    "yy",
    "yyy",
    "yyyy",
    "yr",
    "years",
    "yrs",
    // Quarter
    "quarter",
    "q",
    "qtr",
    "qtrs",
    "quarters",
    // Month
    "month",
    "mm",
    "mon",
    "mons",
    "months",
    // Day of week (ISO) — fixed to Monday-start, NOT WEEK_START/
    // WEEK_OF_YEAR_POLICY-dependent. `dayofweek` itself (and `weekday`,
    // `dow`, `dw`) is EXCLUDED — it is session-dependent.
    "dayofweek_iso",
    "dayofweekiso",
    "weekday_iso",
    "dow_iso",
    "dw_iso",
    // Day of year — not week-related at all.
    "dayofyear",
    "yearday",
    "doy",
    "dy",
    // Week (ISO) — fixed to Monday-start, NOT WEEK_START-dependent. Plain
    // `week` (and its synonyms `w`, `wk`, `weekofyear`, `woy`, `wy`) is
    // EXCLUDED — it is the session-dependent part this list exists to keep
    // out.
    "week_iso",
    "weekiso",
    "weekofyeariso",
    "weekofyear_iso",
    // Day
    "day",
    "d",
    "dd",
    "days",
    "dayofmonth",
    // Year of week (ISO) — fixed, NOT session-dependent. Plain `yearofweek`
    // (no documented synonym) is EXCLUDED for the same reason as `week` and
    // `dayofweek` above.
    "yearofweekiso",
    // Hour
    "hour",
    "h",
    "hh",
    "hr",
    "hours",
    "hrs",
    // Minute
    "minute",
    "m",
    "mi",
    "min",
    "minutes",
    "mins",
    // Second
    "second",
    "s",
    "sec",
    "seconds",
    "secs",
    // Millisecond
    "millisecond",
    "ms",
    "msec",
    "milliseconds",
    // Microsecond
    "microsecond",
    "us",
    "usec",
    "microseconds",
    // Nanosecond
    "nanosecond",
    "ns",
    "nsec",
    "nanosec",
    "nsecond",
    "nanoseconds",
    "nanosecs",
    "nseconds",
    // Epoch — a count of elapsed time from a fixed origin, not a calendar
    // field; no session parameter affects it.
    "epoch_second",
    "epoch",
    "epoch_seconds",
    "epoch_millisecond",
    "epoch_milliseconds",
    "epoch_microsecond",
    "epoch_microseconds",
    "epoch_nanosecond",
    "epoch_nanoseconds",
    // Timezone offset — a property of the value's offset, not the calendar.
    "timezone_hour",
    "tzh",
    "timezone_minute",
    "tzm",
];

/// Advice shared by `date_part` and `EXTRACT`'s week-family gate — see
/// [`SAFE_DATE_PART_PARTS`] for the exact list and the Snowflake pages it
/// is confirmed against (#2141).
const DATE_PART_UNSAFE_PART_ADVICE: &str = "with a date/time part other than `week` (or one of \
     its synonyms `w`, `wk`, `weekofyear`, `woy`, `wy`), `dayofweek` (or `weekday`, `dow`, `dw`), \
     or `yearofweek` — for example `date_part('day', x)` or `EXTRACT(day FROM x)`. Those three \
     depend on Snowflake's WEEK_START or WEEK_OF_YEAR_POLICY session parameters; every ISO-fixed \
     variant (`dayofweekiso`, `weekiso`, `yearofweekiso`, and their synonyms) does not and stays \
     admitted";

/// The number of positional arguments a parsed function call carries.
///
/// `FunctionArguments::None` (a niladic call with no parentheses, e.g. a bare
/// `CURRENT_TIMESTAMP`) and `FunctionArguments::Subquery` (an unparenthesised
/// subquery argument, irrelevant to every function this module shape-checks)
/// both count as zero: neither is the two-argument shape any of these rules
/// accept.
fn positional_arg_count(function: &Function) -> usize {
    match &function.args {
        FunctionArguments::List(list) => list.args.len(),
        FunctionArguments::None | FunctionArguments::Subquery(_) => 0,
    }
}

/// The literal text of a function call's indexed argument, if it is a bare
/// identifier (`WEEK`) or a quoted string literal (`'week'`) — the two
/// shapes a `date_or_time_part` argument is written in. A numeric literal,
/// nested call, or placeholder returns `None`; an identifier may also name
/// a column, which cannot be distinguished here without type information.
fn arg_literal(function: &Function, index: usize) -> Option<String> {
    let FunctionArguments::List(list) = &function.args else {
        return None;
    };
    let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = list.args.get(index)? else {
        return None;
    };
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::Value(value) => value.value.clone().into_string(),
        _ => None,
    }
}

/// A numeric scale, not a date/time part, in TRUNC's second argument.
/// Snowflake documents both literal scales and `TRUNC(n, scale)` with a
/// column scale. Its date/time overload takes a listed part, so a quoted or
/// qualified column reference, or an unquoted identifier outside that list,
/// can only be a numeric scale (or an invalid argument). Without column
/// types, listed bare parts stay on the date/time path, where
/// session-dependent `week` spellings are refused.
fn second_arg_is_numeric_scale(function: &Function) -> bool {
    let FunctionArguments::List(list) = &function.args else {
        return false;
    };
    let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))) = list.args.get(1) else {
        return false;
    };
    match expr {
        Expr::Value(value) => matches!(value.value, Value::Number(..)),
        Expr::Identifier(ident) if ident.quote_style.is_some() => true,
        Expr::Identifier(ident) => {
            let part = ident.value.to_ascii_lowercase();
            !SAFE_TRUNCATION_PARTS.contains(&part.as_str())
                && !["week", "w", "wk", "weekofyear", "woy", "wy"].contains(&part.as_str())
        }
        Expr::CompoundIdentifier(_) => true,
        Expr::UnaryOp {
            op: UnaryOperator::Plus | UnaryOperator::Minus,
            expr,
        } => {
            matches!(expr.as_ref(), Expr::Value(value) if matches!(value.value, Value::Number(..)))
        }
        _ => false,
    }
}

/// Per-function argument-shape rule for allowlisted functions whose
/// argument shape can read a Snowflake session parameter instead of being a
/// function of their arguments alone (#1942, #2141). Checked in addition
/// to, not instead of, [`CHECK_EXPRESSION_FUNCTIONS`] membership — a name
/// has to clear both. `EXTRACT(<part> FROM <expr>)` shares `date_part`'s
/// rule and safe-part list but is not a `Function` call at all, so it is
/// gated separately in the `Walker` below, not here.
///
/// Returns `Some(accepted_shape)` — the text
/// [`ValidationError::ExpressionFunctionShapeNotAllowed`] reports — when
/// `function`'s shape is refused, `None` when it is fine. A name absent from
/// this `match` has no shape restriction.
fn shape_refusal(name: &str, function: &Function) -> Option<&'static str> {
    match name {
        // Snowflake's one-argument form reads DATE_INPUT_FORMAT. An explicit
        // second argument makes the result depend only on its own arguments.
        "to_date" if positional_arg_count(function) < 2 => {
            Some("with an explicit format, e.g. `to_date(x, 'YYYY-MM-DD')`")
        }
        // Same shape, TIMESTAMP_INPUT_FORMAT.
        "to_timestamp" if positional_arg_count(function) < 2 => {
            Some("with an explicit format, e.g. `to_timestamp(x, 'YYYY-MM-DD HH24:MI:SS')`")
        }
        // Same shape; this is the function #1922 removed entirely and #1942
        // re-admits under it. Snowflake's one-argument form reads the
        // session's output-format parameter; the second argument only has to
        // be PRESENT to suppress that read, not a literal — so, like
        // `to_date` and `to_timestamp` above, arity is the whole rule. A
        // format built from an expression (`to_char(amount, fmt_col)`) is
        // just as safe from a session read as a literal one.
        "to_char" if positional_arg_count(function) < 2 => {
            Some("with an explicit format, e.g. `to_char(x, 'YYYY-MM-DD')`")
        }
        // `date_trunc(part, x)` always takes two arguments, so arity cannot
        // distinguish the safe shape here — the risk is in WHICH part.
        "date_trunc"
            if !arg_literal(function, 0).is_some_and(|part| {
                SAFE_TRUNCATION_PARTS.contains(&part.to_ascii_lowercase().as_str())
            }) =>
        {
            Some(
                "with a date part other than `week` (or one of its synonyms `w`, `wk`, \
                 `weekofyear`, `woy`, `wy`), for example `date_trunc('day', x)`. A `week` \
                 truncation depends on Snowflake's WEEK_START session parameter",
            )
        }
        // Snowflake's date/time TRUNC and TRUNCATE reverse DATE_TRUNC's
        // arguments. A literal number in the second slot is a numeric scale;
        // a noncomputed column reference is Snowflake's documented scale
        // shape. A date/time part must be a known safe string or identifier.
        // A computed second argument is ambiguous without types, so refuse it.
        "trunc" | "truncate"
            if positional_arg_count(function) >= 2
                && !second_arg_is_numeric_scale(function)
                && !arg_literal(function, 1).is_some_and(|part| {
                    SAFE_TRUNCATION_PARTS.contains(&part.to_ascii_lowercase().as_str())
                }) =>
        {
            Some(
                "with a safe date part (for example `trunc(x, 'day')`) or a numeric \
                 scale (for example `trunc(amount, scale)`). A `week` part depends on \
                 Snowflake's WEEK_START session parameter; a computed second argument \
                 cannot be proven to be a numeric scale here",
            )
        }
        // Same risk, on the date-part argument of a three-argument call.
        // `date_diff` (the BigQuery/DuckDB spelling) gets the identical rule:
        // it is the same allowlist entry for the same date-part-taking
        // function under a different dialect's name, so a Snowflake user
        // could otherwise reach the exact bug this closes by spelling
        // `datediff` with an underscore. Tightening, not the loosening this
        // change is scoped to avoid.
        "datediff" | "date_diff"
            if !arg_literal(function, 0).is_some_and(|part| {
                SAFE_DATEDIFF_PARTS.contains(&part.to_ascii_lowercase().as_str())
            }) =>
        {
            Some(
                "with a date part other than `week` (or one of its synonyms `w`, `wk`, \
                 `weekofyear`, `woy`, `wy`), for example `datediff('day', a, b)`. A `week` \
                 difference depends on Snowflake's WEEK_START session parameter",
            )
        }
        // `date_part(part, x)` accepts more parts than `date_trunc`/
        // `datediff` do (`dayofyear`, `dayofweekiso`,
        // `epoch_second`, ...), so it gets its own safe list
        // ([`SAFE_DATE_PART_PARTS`]) rather than reusing
        // [`SAFE_TRUNCATION_PARTS`], which would refuse those valid parts
        // outright. Three families are session-dependent here:  `week`,
        // `dayofweek` and `yearofweek` (#2141) — one more than
        // `date_trunc`/`datediff` refuse, because Snowflake's `DATE_PART`
        // page documents `dayofweek`/`yearofweek` as `WEEK_OF_YEAR_POLICY`-
        // and `WEEK_START`-dependent too, and neither part exists on
        // `date_trunc`/`datediff` at all.
        "date_part"
            if !arg_literal(function, 0).is_some_and(|part| {
                SAFE_DATE_PART_PARTS.contains(&part.to_ascii_lowercase().as_str())
            }) =>
        {
            Some(DATE_PART_UNSAFE_PART_ADVICE)
        }
        _ => None,
    }
}

/// Where a validated expression is going to be used.
///
/// The boundary is not the same in all four. A time function is fine in a
/// predicate evaluated once and wrong in a grouping key, so the caller has to
/// say which it is — there is deliberately NO `Default`, so a new call site
/// must choose rather than inherit the permissive answer. That is how this
/// class of gap comes back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpressionUse {
    /// A boolean predicate evaluated ONCE, in one statement.
    ///
    /// Volatile functions are allowed here: `created_at > now() - interval`
    /// is a legitimate freshness-shaped check, and refusing it would break
    /// working configs to close a bug that does not exist in this position.
    SinglePredicate,

    /// A per-check `filter` — scopes which rows an assertion applies to.
    /// Evaluated ONCE, in the same statement as the predicate it filters
    /// (`tests.rs`'s per-check `filter`; `quarantine.rs::wrap_filter`).
    ///
    /// Permits exactly what [`Self::SinglePredicate`] permits — same
    /// volatility and `COLLATE` rules — because a filter is spliced into
    /// that same statement and reaches exactly as far. It is a separate
    /// variant only because the two differ in what field they name, which
    /// [`Self::noun`] has to say correctly: `expression` is the check
    /// itself, `filter` scopes which rows it applies to. A refused filter
    /// used to be described as "an expression check" — the noun came from
    /// the mode, not the field (#1971). Do NOT collapse this back into
    /// `SinglePredicate`: the two must agree on rules by construction (one
    /// arm below), not by two call sites staying in sync by hand.
    Filter,

    /// A scalar value projected into the SELECT list, evaluated ONCE per
    /// statement (`metadata_columns[].value`).
    ///
    /// Permits exactly what [`ExpressionUse::SinglePredicate`] permits today;
    /// only the advice beside a refusal differs, because it has to describe a
    /// column value rather than a check (#1959). It is a separate variant
    /// because the two differ in what they ARE, not in what they currently
    /// permit: one is a boolean tested per row, the other a value written
    /// into a column. A future rule can apply to one and not the other — a
    /// type or nullability constraint belongs to the projection, a
    /// three-valued-logic rule to the predicate. Do NOT merge them back
    /// together on the grounds that the bodies match.
    ///
    /// Volatile is allowed and is the ordinary case: `_loaded_at` is
    /// `current_timestamp()`. Each of `select_clause`'s three callers renders
    /// it once per statement, and MERGE consumes that rendered SELECT as its
    /// source subquery rather than re-evaluating the expression in either
    /// branch, so the value cannot disagree with itself.
    ScalarProjection,

    /// An expression used as a GROUPING KEY (`unique_expr.key_expr`,
    /// `cross_source_overlap.key_expr`).
    ///
    /// A volatile value is not a key. `now()` takes its value from evaluation
    /// time rather than the row, so rows that should group together do not —
    /// or everything collapses into one group. `COLLATE` is refused here, and
    /// ONLY here: it changes what equality MEANS, so the grouping is no
    /// longer the one the key describes.
    GroupingKey,
    // There was a fourth mode, for a predicate evaluated in two statements.
    // Only quarantine `split` used it, and `split` now evaluates each
    // predicate once (#1937). If a caller ever evaluates a user expression in
    // more than one statement again, a volatile function can disagree with
    // itself there, and that caller needs a mode that refuses one.
}

impl ExpressionUse {
    /// Whether a value that can change between evaluations is acceptable.
    ///
    /// False only for a grouping key, where the value must come from the row.
    /// Exhaustive on purpose (no wildcard): a new variant must pick a side
    /// rather than silently inherit one. The refusal built from `false` here
    /// (`ValidationError::ExpressionVolatileInKey`, in the walker below)
    /// hardcodes the word "key" into its sentence, so a future variant that
    /// picks `false` for a reason other than "this position is a grouping
    /// key" needs that message split back out by noun, the same way
    /// `ExpressionFunctionNotAllowed` was (#1971).
    fn tolerates_volatility(self) -> bool {
        match self {
            ExpressionUse::SinglePredicate
            | ExpressionUse::Filter
            | ExpressionUse::ScalarProjection => true,
            ExpressionUse::GroupingKey => false,
        }
    }

    /// Whether `COLLATE` is refused.
    ///
    /// Deliberately NOT derived from [`Self::tolerates_volatility`]. An
    /// explicit collation is deterministic, so it is not a volatility
    /// question. What it changes is the meaning of equality, which matters
    /// only where the expression's value is compared against other rows'
    /// values to form groups. Exhaustive on purpose, same reason as above.
    fn refuses_collate(self) -> bool {
        match self {
            ExpressionUse::GroupingKey => true,
            ExpressionUse::SinglePredicate
            | ExpressionUse::Filter
            | ExpressionUse::ScalarProjection => false,
        }
    }

    /// What a refusal calls the expression in this position, at the start
    /// of a sentence.
    ///
    /// The advice beside a refusal has to describe the field being
    /// validated. One validator serves every user expression Rocky splices
    /// into generated SQL — assertion expressions and filters, quarantine
    /// predicates and filters, the two key expressions, metadata column
    /// values and the MCP `draft_check` tool — so the noun comes from the
    /// mode rather than from a sentence written for `[checks.assertions]`
    /// (#1959). `context` still names the exact field; this names its kind.
    pub(crate) fn noun(self) -> &'static str {
        match self {
            ExpressionUse::SinglePredicate => "An expression check",
            ExpressionUse::Filter => "A filter",
            ExpressionUse::ScalarProjection => "A metadata column value",
            ExpressionUse::GroupingKey => "A key expression",
        }
    }

    /// [`Self::noun`], mid-sentence.
    pub(crate) fn noun_lowercase(self) -> &'static str {
        match self {
            ExpressionUse::SinglePredicate => "an expression check",
            ExpressionUse::Filter => "a filter",
            ExpressionUse::ScalarProjection => "a metadata column value",
            ExpressionUse::GroupingKey => "a key expression",
        }
    }

    /// The shape this position accepts, with an example, for the message a
    /// parse failure carries.
    ///
    /// A predicate is a boolean. A metadata column value is not: `NULL`,
    /// `1` and `'rocky'` are all accepted there, so the boolean advice would
    /// send its author looking for a rule the validator never applies. A key
    /// is any expression over the row that rows can be grouped by. A filter
    /// is worded over "the row's columns" rather than "the model's columns"
    /// — it is still the model's columns, but the sentence is read right
    /// after `noun()` names the field, and "the row" matches how the rest of
    /// the filter docs describe it.
    pub(crate) fn accepted_shape(self) -> &'static str {
        match self {
            ExpressionUse::SinglePredicate => {
                "one boolean expression over the model's columns, e.g. `amount >= 0`"
            }
            ExpressionUse::Filter => {
                "one boolean expression over the row's columns, e.g. `amount >= 0`"
            }
            ExpressionUse::ScalarProjection => {
                "one scalar expression, e.g. `current_timestamp()`, `'rocky'` or `NULL`"
            }
            ExpressionUse::GroupingKey => {
                "one expression over the row's own columns, e.g. `lower(email)`"
            }
        }
    }

    /// [`Self::accepted_shape`] reduced to its kind, for the trailing-text
    /// refusal ("Only a single … is accepted"). Kept separate so the
    /// predicate wording of that refusal stays exactly what it was.
    pub(crate) fn single_kind(self) -> &'static str {
        match self {
            ExpressionUse::SinglePredicate | ExpressionUse::Filter => "boolean expression",
            ExpressionUse::ScalarProjection => "scalar expression",
            ExpressionUse::GroupingKey => "key expression",
        }
    }
}

/// The sqlparser dialect to parse an expression under, from a Rocky
/// `SqlDialect::name()`.
///
/// Trino has no sqlparser dialect; it parses under `GenericDialect`, which
/// accepts a superset of the syntax the others share. Unknown names take
/// the same route — parsing under a broader dialect can only *accept* more
/// syntax, and acceptance still has to clear the walker below.
pub fn dialect_for(name: &str) -> Box<dyn Dialect> {
    match name.to_ascii_lowercase().as_str() {
        "duckdb" => Box::new(DuckDbDialect),
        "snowflake" => Box::new(SnowflakeDialect),
        "bigquery" => Box::new(BigQueryDialect),
        "databricks" => Box::new(DatabricksDialect),
        _ => Box::new(GenericDialect),
    }
}

/// Refuse an `expression` check whose content is not one boolean expression
/// over the model's own columns calling only allowlisted scalar functions.
///
/// `context` names the check for the message (e.g. ``expression test
/// `expression` on wh.main.orders``).
///
/// # Errors
///
/// One of the `Expression*` variants of [`ValidationError`], in the order
/// listed in the module docs.
pub fn validate_check_expression(
    context: &str,
    expression: &str,
    dialect: &dyn Dialect,
    use_: ExpressionUse,
) -> Result<(), ValidationError> {
    let unparseable = |detail: String| ValidationError::ExpressionUnparseable {
        context: context.to_string(),
        detail,
        use_,
    };
    let mut parser = Parser::new(dialect)
        .try_with_sql(expression)
        .map_err(|err| unparseable(err.to_string()))?;
    let expr = parser
        .parse_expr()
        .map_err(|err| unparseable(err.to_string()))?;
    // `parse_expr` stops at the first token that cannot continue an
    // expression — a `)` that would close the generated `NOT (`, a comma,
    // a keyword. Anything left over is the fragment escaping its slot.
    if parser.peek_token().token != Token::EOF {
        return Err(ValidationError::ExpressionTrailingTokens {
            context: context.to_string(),
            use_,
        });
    }
    let mut walker = Walker { context, use_ };
    match expr.visit(&mut walker) {
        ControlFlow::Break(err) => Err(err),
        ControlFlow::Continue(()) => Ok(()),
    }
}

/// Walks every node of the parsed expression. `sqlparser`'s visitor
/// descends into nested expressions, function arguments and subqueries on
/// its own, so a refusal here does not depend on enumerating `Expr`
/// variants by hand — a variant this module has never heard of is still
/// walked, and any `Query` or `Function` inside it is still judged.
struct Walker<'a> {
    context: &'a str,
    use_: ExpressionUse,
}

impl Visitor for Walker<'_> {
    type Break = ValidationError;

    // Any query anywhere: a scalar subquery, `IN (SELECT ..)`, `EXISTS`,
    // `ANY`/`ALL` — they all carry a `Query` and all reach other tables.
    fn pre_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
        ControlFlow::Break(ValidationError::ExpressionSubquery {
            context: self.context.to_string(),
            use_: self.use_,
        })
    }

    // Defensive: a bare expression has no FROM, so a table factor can only
    // appear inside a query the arm above already refused. Kept so the
    // refusal does not depend on visitor ordering.
    fn pre_visit_table_factor(&mut self, _tf: &TableFactor) -> ControlFlow<Self::Break> {
        ControlFlow::Break(ValidationError::ExpressionSubquery {
            context: self.context.to_string(),
            use_: self.use_,
        })
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        match expr {
            // `COLLATE` changes what equality means, so a key that carries
            // one does not group by what it says it groups by. Deterministic,
            // so it is not a volatility question — it is refused in the
            // position where equality is the point, and nowhere else.
            // `COLLATE` is not a function, so "add it to
            // CHECK_EXPRESSION_FUNCTIONS" would be meaningless advice — its
            // own variant carries the real reason instead (#1971).
            Expr::Collate { .. } if self.use_.refuses_collate() => {
                ControlFlow::Break(ValidationError::ExpressionCollateInKey {
                    context: self.context.to_string(),
                })
            }
            Expr::Function(function) => {
                let parts = &function.name.0;
                let [ObjectNamePart::Identifier(ident)] = parts.as_slice() else {
                    // Either qualified (`a.b.f`) or a dialect-specific
                    // identifier-producing part. Both are how UDFs, remote
                    // functions and plugin functions are reached.
                    return ControlFlow::Break(ValidationError::ExpressionQualifiedFunction {
                        context: self.context.to_string(),
                        use_: self.use_,
                        function: function.name.to_string(),
                    });
                };
                let name = ident.value.to_ascii_lowercase();
                // Volatile in the sense Rocky already defines elsewhere:
                // `determinism::VOLATILE_FUNCTIONS`, reused rather than
                // restated so the two cannot disagree about what "volatile"
                // means.
                if !self.use_.tolerates_volatility()
                    && crate::determinism::VOLATILE_FUNCTIONS
                        .iter()
                        .any(|v| v.eq_ignore_ascii_case(&name))
                {
                    // The name may already be on the allowlist (`now` is);
                    // it is refused for the POSITION, not the name, so "add
                    // it to CHECK_EXPRESSION_FUNCTIONS" would be false advice
                    // either way — for an allowlisted name it is already
                    // there, and for one that is not, this check runs before
                    // the allowlist is even consulted, so adding it would not
                    // change the outcome (#1971).
                    return ControlFlow::Break(ValidationError::ExpressionVolatileInKey {
                        context: self.context.to_string(),
                        function: ident.value.clone(),
                    });
                }
                if !CHECK_EXPRESSION_FUNCTIONS.contains(&name.as_str()) {
                    return ControlFlow::Break(ValidationError::ExpressionFunctionNotAllowed {
                        context: self.context.to_string(),
                        use_: self.use_,
                        function: ident.value.clone(),
                    });
                }
                // On the allowlist by name, but a handful of names admit
                // only one argument shape (#1942) — see `shape_refusal`.
                if let Some(accepted_shape) = shape_refusal(&name, function) {
                    return ControlFlow::Break(
                        ValidationError::ExpressionFunctionShapeNotAllowed {
                            context: self.context.to_string(),
                            use_: self.use_,
                            function: ident.value.clone(),
                            accepted_shape,
                        },
                    );
                }
                ControlFlow::Continue(())
            }
            // A lambda is a function body the warehouse executes; the
            // functions it can call are the same question one level down,
            // and nothing an expression check needs is expressed as one.
            Expr::Lambda(_) => ControlFlow::Break(ValidationError::ExpressionFunctionNotAllowed {
                context: self.context.to_string(),
                use_: self.use_,
                function: "<lambda>".to_string(),
            }),
            // A bare, unquoted `current_user` is a session read spelled as
            // an identifier — see `SESSION_IDENTITY_KEYWORDS`.
            Expr::Identifier(ident)
                if ident.quote_style.is_none()
                    && SESSION_IDENTITY_KEYWORDS
                        .contains(&ident.value.to_ascii_lowercase().as_str()) =>
            {
                ControlFlow::Break(ValidationError::ExpressionFunctionNotAllowed {
                    context: self.context.to_string(),
                    use_: self.use_,
                    function: ident.value.clone(),
                })
            }
            // A placeholder — Snowflake `$name` / `$1`, or any dialect's bind
            // marker. Rocky never binds parameters into these expressions:
            // they are spliced as TEXT, so nothing here is ever filled in by a
            // driver. What reaches the warehouse instead is a session-variable
            // read, resolved at execution time out of state this validator
            // cannot see — the same class as `getvariable(..)` and
            // `current_user`, which are already refused, but spelled so it is
            // neither a function nor an identifier.
            //
            // Refusing every placeholder is deliberate and fail-closed: a
            // stray `?` in a spliced predicate is a defect whatever it meant.
            Expr::Value(value) if matches!(value.value, Value::Placeholder(_)) => {
                let name = match &value.value {
                    Value::Placeholder(p) => p.clone(),
                    _ => unreachable!("guarded by the match arm"),
                };
                ControlFlow::Break(ValidationError::ExpressionFunctionNotAllowed {
                    context: self.context.to_string(),
                    use_: self.use_,
                    function: name,
                })
            }
            // `EXTRACT(<part> FROM <expr>)` (or the comma form,
            // `EXTRACT(<part>, <expr>)`) is Snowflake's own documented
            // alternative spelling of `DATE_PART` — same parts, same
            // session-parameter risk — but sqlparser gives it a dedicated
            // `Expr::Extract` node rather than routing it through
            // `Expr::Function`, so it never reaches the allowlist or
            // `shape_refusal` above and was admitted unconditionally before
            // this arm existed (#2141). A quoted part is `Custom(Ident)`;
            // compare its value without the SQL quotes. Known keyword parts
            // still use their canonical display spelling. Continuing still
            // walks into the inner `expr` — the traversal descends into an
            // unhandled node's children automatically, same as every other
            // arm here.
            Expr::Extract { field, .. } => {
                let part = match field {
                    DateTimeField::Custom(ident) => ident.value.to_ascii_lowercase(),
                    _ => field.to_string().to_ascii_lowercase(),
                };
                if SAFE_DATE_PART_PARTS.contains(&part.as_str()) {
                    ControlFlow::Continue(())
                } else {
                    ControlFlow::Break(ValidationError::ExpressionFunctionShapeNotAllowed {
                        context: self.context.to_string(),
                        use_: self.use_,
                        function: "extract".to_string(),
                        accepted_shape: DATE_PART_UNSAFE_PART_ADVICE,
                    })
                }
            }
            _ => ControlFlow::Continue(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CTX: &str = "expression test `expression` on wh.main.orders";

    fn check(expression: &str) -> Result<(), ValidationError> {
        validate_check_expression(
            CTX,
            expression,
            &GenericDialect,
            ExpressionUse::SinglePredicate,
        )
    }

    /// `COLLATE` is refused in the KEY position and nowhere else.
    ///
    /// It was previously gated on `tolerates_volatility()`, which also
    /// refused it in a re-evaluated predicate. That was wrong for the reason
    /// the code comment beside it already gave: an explicit collation is
    /// DETERMINISTIC, so two evaluations of it agree. What it changes is the
    /// meaning of equality, which only matters where values are compared
    /// against each other to form groups.
    #[test]
    fn collate_is_refused_for_a_key_and_allowed_everywhere_else() {
        check_key("email COLLATE NOCASE").expect_err("a collated key is refused");

        check("email COLLATE NOCASE = 'a'").expect("a collated predicate is fine");
        validate_check_expression(
            CTX,
            "email COLLATE NOCASE",
            &GenericDialect,
            ExpressionUse::ScalarProjection,
        )
        .expect("a projected value is not compared against other rows");
    }

    /// `Filter` shares `SinglePredicate`'s rules exactly — a clock and a
    /// `COLLATE` are both fine, because a filter is evaluated once, in the
    /// same statement as the predicate it scopes. Only the noun a refusal
    /// uses differs; see `a_check_filter_is_described_as_a_filter` in
    /// `rocky-core/src/tests.rs` for that half (#1971).
    #[test]
    fn filter_permits_exactly_what_single_predicate_permits() {
        // Each case is run through BOTH modes and must agree, so the
        // equality is pinned rather than asserted one mode at a time — that
        // would stay green even if a future change split the two rule sets
        // without anyone noticing the drift. `expect_ok` also pins the
        // DIRECTION: two modes silently agreeing on the wrong answer would
        // pass an agreement-only check.
        let cases = [
            ("created_at > now()", true),         // a clock is fine in both
            ("email COLLATE NOCASE = 'a'", true), // COLLATE is fine in both
            ("amount >= 0", true),                // an ordinary predicate
            ("my_udf(a)", false),                 // off the allowlist in both
            ("amount > (SELECT 1)", false),       // a subquery in both
            ("amount > 0, 1", false),             // trailing tokens in both
        ];
        for (expr, expect_ok) in cases {
            let filter_ok =
                validate_check_expression(CTX, expr, &GenericDialect, ExpressionUse::Filter)
                    .is_ok();
            let predicate_ok = validate_check_expression(
                CTX,
                expr,
                &GenericDialect,
                ExpressionUse::SinglePredicate,
            )
            .is_ok();
            assert_eq!(
                filter_ok, predicate_ok,
                "`{expr}`: Filter and SinglePredicate disagree (filter={filter_ok}, \
                 predicate={predicate_ok})"
            );
            assert_eq!(
                filter_ok, expect_ok,
                "`{expr}`: expected ok={expect_ok}, got {filter_ok}"
            );
        }
    }

    /// The advice beside a refusal describes the position that was
    /// validated, not `[checks.assertions]` (#1959).
    ///
    /// A metadata column value that fails to parse used to be told to write
    /// "one boolean expression over the model's columns", a rule the
    /// projection mode never applies: `NULL`, `1` and `'rocky'` are all
    /// accepted there. Every one of the five refusals carries the noun, so
    /// each is checked against a substring only that variant emits, and the
    /// predicate wording of all five is pinned to what it was before #1959,
    /// so this cannot pass by rewording the check advice instead.
    #[test]
    fn a_refusal_describes_the_position_it_was_validated_for() {
        let refuse = |e: &str, use_: ExpressionUse| {
            validate_check_expression("metadata_columns[].value", e, &GenericDialect, use_)
                .expect_err("must be refused")
                .to_string()
        };

        let projected = refuse("1 +", ExpressionUse::ScalarProjection);
        assert!(
            projected.contains("does not parse as a single SQL expression")
                && projected.contains(
                    "A metadata column value is one scalar expression, e.g. \
                     `current_timestamp()`, `'rocky'` or `NULL`"
                )
                && !projected.contains("boolean"),
            "{projected}"
        );
        // `1, 2` parses `1` and stops at the comma, so this is the
        // trailing-text refusal and not a parse failure; the first
        // substring is what tells the two apart.
        let trailing = refuse("1, 2", ExpressionUse::ScalarProjection);
        assert!(
            trailing.contains("continues past the end of one expression")
                && trailing.contains("Only a single scalar expression is accepted")
                && !trailing.contains("boolean"),
            "{trailing}"
        );
        let subquery = refuse("(SELECT 1)", ExpressionUse::ScalarProjection);
        assert!(
            subquery.contains("A metadata column value may only read the row's own columns"),
            "{subquery}"
        );
        let off_list = refuse("my_udf(1)", ExpressionUse::ScalarProjection);
        assert!(
            off_list.contains("pure scalar functions a metadata column value may use"),
            "{off_list}"
        );
        let qualified = refuse("s.f(1)", ExpressionUse::ScalarProjection);
        assert!(
            qualified.contains("never allowed in a metadata column value"),
            "{qualified}"
        );

        // The check wording is unchanged for the field it was written for:
        // all five refusals, byte for byte where the sentence is quoted.
        let predicate = refuse("1 +", ExpressionUse::SinglePredicate);
        assert!(
            predicate.contains(
                "An expression check is one boolean expression over the model's columns, \
                 e.g. `amount >= 0`"
            ),
            "{predicate}"
        );
        let trailing_check = refuse("1, 2", ExpressionUse::SinglePredicate);
        assert!(
            trailing_check.contains(
                "Only a single boolean expression is accepted — no trailing clauses, commas \
                 or operators"
            ),
            "{trailing_check}"
        );
        let subquery_check = refuse("(SELECT 1)", ExpressionUse::SinglePredicate);
        assert!(
            subquery_check.contains(
                "An expression check may only read the row's own columns; it cannot read \
                 other tables"
            ),
            "{subquery_check}"
        );
        let off_list_check = refuse("my_udf(1)", ExpressionUse::SinglePredicate);
        assert!(
            off_list_check.contains("pure scalar functions an expression check may use"),
            "{off_list_check}"
        );
        let qualified_check = refuse("s.f(1)", ExpressionUse::SinglePredicate);
        assert!(
            qualified_check.contains("which are never allowed in an expression check"),
            "{qualified_check}"
        );

        // A key is grouped by, so it is neither a boolean nor a column value.
        let key = refuse("1 +", ExpressionUse::GroupingKey);
        assert!(
            key.contains("A key expression is one expression over the row's own columns")
                && !key.contains("boolean"),
            "{key}"
        );
    }

    /// `ScalarProjection` accepts the metadata-column case that gives it a
    /// reason to exist, and still refuses an off-list function.
    #[test]
    fn a_projected_value_allows_a_clock_and_still_refuses_an_off_list_name() {
        let project = |e: &str| {
            validate_check_expression(CTX, e, &GenericDialect, ExpressionUse::ScalarProjection)
        };
        // The reason metadata columns exist. A volatile-refusing mode here
        // would break the common `_loaded_at` column.
        project("current_timestamp()").expect("_loaded_at = current_timestamp() is the point");
        project("now()").expect("same, spelled differently");
        // The boundary still holds.
        project("read_text('/etc/passwd')").expect_err("an off-list read is refused");
        project("(SELECT 1)").expect_err("a subquery is refused");
    }

    /// Every clock-shaped name this module allows is known to be volatile.
    ///
    /// `determinism` refuses any function it does not recognise, so a clock
    /// name missing from `VOLATILE_FUNCTIONS` was still safe THERE. This
    /// module has its own allowlist, so that fallback does not cover it: a
    /// name on the allowlist but absent from `VOLATILE_FUNCTIONS` is accepted
    /// as a grouping key. `localtime` was
    /// exactly that — allowlisted here, absent there, while `localtimestamp`
    /// sat on both lists.
    ///
    /// This fails if either list moves without the other.
    ///
    /// **What it does NOT prove.** `CLOCK_SHAPED` is hand-written, so this is
    /// a consistency check between two lists, not a completeness proof: an
    /// out-of-row function added to the allowlist and not to `CLOCK_SHAPED`
    /// leaves this test green. It also has no validator control — it would
    /// pass against a validator that refused everything. Name matching alone
    /// could not see the argument-shape cases either (`to_date(x)`,
    /// `date_trunc('week', x)`) — `shape_refusal` and the tests below now
    /// cover those (#1942).
    #[test]
    fn every_clock_name_on_the_allowlist_is_known_volatile() {
        // Names that read a clock, a session, or a sequence. Adding one to
        // CHECK_EXPRESSION_FUNCTIONS without adding it here does not make
        // this pass — it makes the volatile-refusal tests below fail.
        const CLOCK_SHAPED: &[&str] = &[
            "now",
            "current_date",
            "current_timestamp",
            "current_time",
            "localtime",
            "localtimestamp",
        ];
        for name in CLOCK_SHAPED {
            assert!(
                CHECK_EXPRESSION_FUNCTIONS.contains(name),
                "{name} is listed here as clock-shaped but is not allowlisted; \
                 drop it from CLOCK_SHAPED or add it to the allowlist"
            );
            assert!(
                crate::determinism::VOLATILE_FUNCTIONS.contains(&name.to_uppercase().as_str()),
                "{name} is allowlisted and clock-shaped but missing from \
                 VOLATILE_FUNCTIONS, so it passes GroupingKey"
            );
        }
    }

    /// Each clock name is refused as a grouping key.
    ///
    /// The matrix below previously tested `localtimestamp()` and never
    /// `localtime()`, which is why the gap survived review.
    #[test]
    fn every_clock_name_is_refused_as_a_key() {
        for expr in [
            "now()",
            "current_timestamp()",
            "current_date()",
            "current_time()",
            "localtime()",
            "localtimestamp()",
        ] {
            check_key(expr).expect_err(&format!("{expr} must not pass as a grouping key"));
            // ...and is still fine where it is evaluated once.
            check(expr).unwrap_or_else(|e| panic!("{expr} must stay legal in a predicate: {e:?}"));
        }
    }

    /// The same check, in the GROUPING KEY position — where a volatile
    /// value and a COLLATE are refused.
    fn check_key(expression: &str) -> Result<(), ValidationError> {
        validate_check_expression(CTX, expression, &GenericDialect, ExpressionUse::GroupingKey)
    }

    fn check_on(dialect: &str, expression: &str) -> Result<(), ValidationError> {
        validate_check_expression(
            CTX,
            expression,
            dialect_for(dialect).as_ref(),
            ExpressionUse::SinglePredicate,
        )
    }

    /// A session-variable read spelled as a placeholder is refused, on every
    /// dialect that parses one.
    ///
    /// Snowflake `$name` / `$1` is neither a function, a query, a lambda, nor
    /// an identifier, so every arm of the walker used to pass it through and
    /// the whole expression validated clean. `getvariable('x')` and
    /// `identifier($foo)` were already refused — the same read, spelled as a
    /// function. This closes the spelling that was not.
    ///
    /// Rocky splices these expressions as TEXT and never binds a parameter, so
    /// a placeholder can never be something we supply. Refusing all of them is
    /// fail-closed on purpose.
    #[test]
    fn a_placeholder_session_read_is_refused_on_every_dialect() {
        // Each entry is a dialect and a placeholder its parser produces.
        // A precondition that `continue`s can empty the loop: if no dialect
        // parsed a placeholder, every case would skip and the test would pass
        // having asserted nothing. Counted and checked below.
        let mut exercised = 0usize;
        for (dialect, expr) in [
            ("snowflake", "$foo"),
            ("snowflake", "$1"),
            ("generic", "$1"),
            ("duckdb", "$1"),
            ("bigquery", "?"),
            ("databricks", "$1"),
        ] {
            let parsed = {
                let d = dialect_for(dialect);
                let mut parser = Parser::new(d.as_ref()).try_with_sql(expr).unwrap();
                parser.parse_expr()
            };
            // Precondition: this dialect really does parse it as a
            // placeholder. A dialect that rejects it outright proves nothing
            // about the walker, and the case would pass vacuously.
            let Ok(Expr::Value(v)) = &parsed else {
                continue;
            };
            if !matches!(v.value, Value::Placeholder(_)) {
                continue;
            }
            exercised += 1;
            let err = check_on(dialect, expr).expect_err(&format!(
                "{dialect}: `{expr}` parses as a placeholder and must be refused"
            ));
            assert!(
                matches!(err, ValidationError::ExpressionFunctionNotAllowed { .. }),
                "{dialect}: `{expr}` gave {err:?}"
            );
        }
        assert!(
            exercised >= 2,
            "only {exercised} case(s) actually parsed as a placeholder — the \
             skip-if-not-a-placeholder precondition has emptied this test"
        );
    }

    /// The allowlist was sized for boolean PREDICATES. Since the same
    /// validator now guards `key_expr`, it has to admit what a real grouping
    /// key uses — and a false refusal there breaks a working project, which
    /// is the one direction where over-tightening costs users.
    ///
    /// Measured rather than assumed: each of these was refused before the
    /// list was extended.
    #[test]
    fn the_allowlist_admits_what_a_real_key_expression_uses() {
        for expr in [
            "sha1(a)",
            "sha2(a, 256)",
            "hex(a)",
            "to_hex(a)",
            "encode(a, 'hex')",
            "decode(a, 'hex')",
            "date_format(t, 'y')",
            "format_date('%Y', t)",
            "translate(a, 'x', 'y')",
            "initcap(a)",
            "ascii(a)",
            "chr(a)",
            "normalize(a)",
            "array_to_string(a, ',')",
            "json_extract(a, '$.k')",
        ] {
            check(expr).unwrap_or_else(|e| panic!("a real key shape is refused: {expr} -> {e:?}"));
            // The name of this test claims the KEY position, so assert it.
            // Running only `check` (predicate mode) would stay green if one
            // of these became mode-gated and stopped working as a key.
            check_key(expr).unwrap_or_else(|e| {
                panic!("a real key shape is refused in KEY position: {expr} -> {e:?}")
            });
        }
    }

    /// A time function is fine in a predicate and wrong in a key.
    ///
    /// The allowlist has carried `now`, `current_timestamp` and friends since
    /// it was written for PREDICATES, where `created_at > now() - interval`
    /// is a legitimate freshness check. Widening the same list to guard
    /// grouping keys made those names reachable somewhere they are wrong: a
    /// key takes its value from evaluation time rather than the row, so rows
    /// that should group together do not.
    ///
    /// The exclusion doc already said a non-deterministic key groups nothing.
    /// It said it about `uuid` and `random` while `now` sat on the list.
    #[test]
    fn a_volatile_function_is_allowed_in_a_predicate_and_refused_in_a_key() {
        for expr in [
            "now()",
            "current_timestamp()",
            "current_date()",
            "localtimestamp()",
        ] {
            check(expr)
                .unwrap_or_else(|e| panic!("a predicate may use a time function: {expr} -> {e:?}"));
            check_key(expr).expect_err(&format!("a grouping key may not use {expr}"));
        }
        // The realistic predicate shape, which must keep working.
        check("created_at > now()").expect("a freshness-shaped predicate is legitimate");
    }

    /// `COLLATE` changes what equality MEANS, so a key carrying one does not
    /// group by what it says it groups by.
    ///
    /// The exclusions table claimed this was already refused "because it is a
    /// clause, not a scalar function". It was not: `Expr::Collate` is a node
    /// the walker passed straight through. The doc asserted a behaviour that
    /// had never been tested.
    #[test]
    fn collate_is_refused_in_a_key_and_allowed_in_a_predicate() {
        check_key("email COLLATE NOCASE").expect_err("a collated key is refused");
        // Deterministic, and equality is not the point in a predicate.
        check("email COLLATE NOCASE = 'a'").expect("a collated predicate is fine");
    }

    /// The control for both: an ordinary key is still accepted in key mode.
    /// Without it, the refusals above would also pass on a change that
    /// refused every grouping key.
    #[test]
    fn an_ordinary_key_is_still_accepted_in_key_mode() {
        for expr in [
            "lower(email)",
            "concat(a, '-', b)",
            "md5(id)",
            "customer_id",
        ] {
            check_key(expr).unwrap_or_else(|e| panic!("a plain key must pass: {expr} -> {e:?}"));
        }
    }

    /// The exclusions are decisions, not omissions, so they are pinned too.
    /// Without this, "extend the allowlist" would eventually swallow them.
    #[test]
    fn the_allowlist_still_refuses_what_it_should() {
        // An aggregate is not a per-row value.
        check("string_agg(a, ',')").expect_err("an aggregate is not a key");
        // A non-deterministic key groups nothing.
        check("uuid()").expect_err("uuid is not deterministic");
        check("random()").expect_err("random is not deterministic");
    }

    /// A legitimate SCALAR key expression passes.
    ///
    /// `key_expr` on `unique_expr` and `cross_source_overlap` is a SCALAR, not
    /// a boolean, and it goes through this same validator. Nothing here
    /// requires booleanness — it parses one expression and judges the nodes —
    /// but a validator sized only for boolean predicates would refuse every
    /// valid key, so the shapes a key author actually reaches for are pinned.
    #[test]
    fn a_legitimate_scalar_key_expression_passes() {
        for expr in [
            "lower(email)",
            "coalesce(tenant_id, 'none')",
            "concat(region, '-', lower(source))",
            "date_trunc('day', created_at)",
            "md5(concat(customer_id, order_id))",
            "CAST(order_id AS VARCHAR)",
            "customer_id",
        ] {
            check(expr)
                .unwrap_or_else(|e| panic!("a valid key must not be refused: {expr} -> {e:?}"));
        }
    }

    /// A placeholder INSIDE a larger expression is refused too: the walker
    /// descends, so it is not only the top-level node that is judged.
    #[test]
    fn a_nested_placeholder_is_refused() {
        check_on("snowflake", "customer_id = $tenant")
            .expect_err("a placeholder in a comparison is still a session read");
        check_on("snowflake", "coalesce(name, $fallback) IS NOT NULL")
            .expect_err("a placeholder inside an allowed function is still a session read");
    }

    /// The control: an ordinary literal comparison is untouched. Without it,
    /// the refusals above would also pass on a guard that refused every value
    /// node.
    #[test]
    fn an_ordinary_literal_is_not_a_placeholder() {
        check_on("snowflake", "status = 'shipped'").expect("a string literal is fine");
        check_on("snowflake", "total >= 0").expect("a number literal is fine");
        check_on("snowflake", "name IS NOT NULL").expect("a bare column is fine");
    }

    /// Every `expression` check in the tree at the time of writing, plus the
    /// ordinary shapes a contract author reaches for. All must pass — a
    /// boundary that refuses the ordinary case is a bug, not a control.
    #[test]
    fn ordinary_predicates_pass() {
        for expr in [
            "amount > 0",
            "amount >= 0",
            "revenue_eur >= 0",
            "client_id > 0",
            "status IN ('open', 'closed')",
            "email IS NOT NULL",
            "amount BETWEEN 0 AND 1000",
            "coalesce(discount, 0) <= amount",
            "length(trim(name)) > 0",
            "lower(status) = 'open'",
            "CASE WHEN qty > 0 THEN price > 0 ELSE TRUE END",
            "CAST(amount AS DECIMAL(10,2)) >= 0",
            "created_at <= now()",
            "date_trunc('day', created_at) IS NOT NULL",
            "amount > 0 AND NOT (status = 'void' OR status = 'refunded')",
            "regexp_like(email, '^[^@]+@[^@]+$')",
        ] {
            check(expr).unwrap_or_else(|err| panic!("{expr} must pass: {err}"));
        }
    }

    /// The read primitives the allowlist exists for, one per dialect. Each
    /// sits in scalar position and would parse cleanly under a denylist
    /// that only knew about subqueries and table functions.
    #[test]
    fn scalar_position_read_primitives_are_refused_by_name() {
        let cases = [
            ("duckdb", "read_text('/etc/passwd') IS NULL", "read_text"),
            ("snowflake", "GETVARIABLE('SECRET') IS NULL", "GETVARIABLE"),
            ("databricks", "secret('scope', 'key') IS NULL", "secret"),
            ("generic", "read_csv('/tmp/x.csv') IS NULL", "read_csv"),
        ];
        for (dialect, expr, function) in cases {
            match check_on(dialect, expr) {
                Err(ValidationError::ExpressionFunctionNotAllowed { function: f, .. }) => {
                    assert_eq!(f, function, "{expr} must name the function it refused");
                }
                other => panic!("{expr} on {dialect} must be refused by name: {other:?}"),
            }
        }
    }

    /// Session identity is a read of session state and has three spellings
    /// the parsers disagree on: a bare keyword that `GenericDialect` turns
    /// into a `Function`, the same bare keyword that the warehouse dialects
    /// turn into a plain identifier, and the parenthesised call. All three
    /// are refused; a QUOTED identifier of the same name is a column.
    #[test]
    fn session_identity_is_refused_in_every_spelling() {
        for (dialect, expr) in [
            ("generic", "current_user = 'admin'"),
            ("duckdb", "current_user = 'admin'"),
            ("snowflake", "CURRENT_USER = 'admin'"),
            ("databricks", "session_user = 'admin'"),
            ("snowflake", "CURRENT_USER() = 'admin'"),
            ("generic", "current_user() = 'admin'"),
        ] {
            assert!(
                check_on(dialect, expr).is_err(),
                "{expr} on {dialect} must be refused: {:?}",
                check_on(dialect, expr)
            );
        }
        check_on("duckdb", "\"current_user\" = 'admin'")
            .expect("a quoted identifier is unambiguously a column");
    }

    /// Qualified names are how BigQuery remote functions and Trino plugin
    /// functions are reached. Refused regardless of the last segment — a
    /// qualified `coalesce` is still someone else's `coalesce`.
    #[test]
    fn qualified_functions_are_refused() {
        for expr in [
            "project.dataset.remote_fn(col) IS NULL",
            "catalog.schema.udf(col) = 1",
            "myschema.coalesce(a, b) IS NOT NULL",
        ] {
            assert!(
                matches!(
                    check(expr),
                    Err(ValidationError::ExpressionQualifiedFunction { .. })
                ),
                "{expr} must be refused as qualified: {:?}",
                check(expr)
            );
        }
    }

    /// A subquery in any position reads another table.
    #[test]
    fn subqueries_are_refused_wherever_they_appear() {
        for expr in [
            "amount > (SELECT max(amount) FROM other)",
            "id IN (SELECT id FROM allowlist)",
            "EXISTS (SELECT 1 FROM t WHERE t.id = id)",
            "amount > ANY (SELECT amount FROM t)",
            "coalesce((SELECT 1), 0) = 1",
        ] {
            assert!(
                matches!(check(expr), Err(ValidationError::ExpressionSubquery { .. })),
                "{expr} must be refused as a subquery: {:?}",
                check(expr)
            );
        }
    }

    /// The fragment is spliced as `NOT (<expression>)`. A trailing `)` closes
    /// that group and the rest becomes new clauses on the generated
    /// statement. `parse_expr` stops at the `)`; the leftover is the escape.
    #[test]
    fn a_fragment_that_escapes_its_slot_is_refused() {
        for expr in [
            "amount > 0) OR 1=1 --",
            "amount > 0, 1",
            "amount > 0 amount",
        ] {
            assert!(
                matches!(
                    check(expr),
                    Err(ValidationError::ExpressionTrailingTokens { .. })
                        | Err(ValidationError::ExpressionUnparseable { .. })
                ),
                "{expr} must not be accepted as one expression: {:?}",
                check(expr)
            );
        }
    }

    #[test]
    fn unparseable_text_is_refused_with_the_parser_detail() {
        match check("amount >") {
            Err(ValidationError::ExpressionUnparseable { detail, .. }) => {
                assert!(!detail.is_empty(), "the parser's reason must be carried");
            }
            other => panic!("must be unparseable: {other:?}"),
        }
    }

    /// Allowlist matching is case-insensitive; SQL function names are.
    #[test]
    fn allowlist_is_case_insensitive() {
        check("COALESCE(a, 0) > 0").expect("upper-case allowlisted function");
        check("Coalesce(a, 0) > 0").expect("mixed-case allowlisted function");
    }

    /// Every allowlist entry is lowercase and unique, so a lookup by
    /// lowercased name cannot miss one that is present.
    #[test]
    fn allowlist_is_lowercase_and_deduplicated() {
        let mut seen = std::collections::BTreeSet::new();
        for name in CHECK_EXPRESSION_FUNCTIONS {
            assert_eq!(*name, name.to_ascii_lowercase(), "{name} must be lowercase");
            assert!(seen.insert(*name), "{name} is listed twice");
        }
    }

    /// The dialect mapper never fails: an unknown name parses under
    /// `GenericDialect`, and the walker still judges the result.
    #[test]
    fn unknown_dialect_falls_back_to_generic_and_still_judges() {
        assert!(check_on("trino", "amount > 0").is_ok());
        assert!(matches!(
            check_on("not-a-dialect", "read_text('x') IS NULL"),
            Err(ValidationError::ExpressionFunctionNotAllowed { .. })
        ));
    }

    // ---- Argument-shape rule (#1942) ---------------------------------
    //
    // Tested on the Snowflake dialect specifically, not the `check()`
    // helper's `GenericDialect` default — this is the one dialect the
    // session-parameter reads are documented against, and #1922 already
    // found call sites that stayed green under a default-name dialect and
    // would not have caught a dialect-specific regression.

    /// `to_date`'s Snowflake one-argument form reads `DATE_INPUT_FORMAT`.
    /// The two-argument form does not, and is admitted.
    #[test]
    fn to_date_admits_only_the_explicit_format_shape() {
        match check_on("snowflake", "to_date(order_date) IS NOT NULL") {
            Err(ValidationError::ExpressionFunctionShapeNotAllowed {
                function,
                accepted_shape,
                ..
            }) => {
                assert_eq!(function, "to_date");
                assert!(
                    accepted_shape.contains("to_date(x, 'YYYY-MM-DD')"),
                    "{accepted_shape}"
                );
            }
            other => panic!("bare to_date must be refused by shape: {other:?}"),
        }
        check_on("snowflake", "to_date(order_date, 'YYYY-MM-DD') IS NOT NULL")
            .expect("an explicit format makes to_date a function of its arguments alone");
    }

    /// Same rule, `TIMESTAMP_INPUT_FORMAT`.
    #[test]
    fn to_timestamp_admits_only_the_explicit_format_shape() {
        match check_on("snowflake", "to_timestamp(loaded_at) IS NOT NULL") {
            Err(ValidationError::ExpressionFunctionShapeNotAllowed { function, .. }) => {
                assert_eq!(function, "to_timestamp");
            }
            other => panic!("bare to_timestamp must be refused by shape: {other:?}"),
        }
        check_on(
            "snowflake",
            "to_timestamp(loaded_at, 'YYYY-MM-DD HH24:MI:SS') IS NOT NULL",
        )
        .expect("an explicit format makes to_timestamp a function of its arguments alone");
    }

    /// `to_char` was removed from the allowlist entirely by #1922 because its
    /// one-argument form reads the session output-format parameter. #1942
    /// re-admits it under the same explicit-format rule as `to_date` and
    /// `to_timestamp`.
    #[test]
    fn to_char_is_readmitted_only_with_an_explicit_format() {
        match check_on("snowflake", "to_char(amount) = '0'") {
            Err(ValidationError::ExpressionFunctionShapeNotAllowed { function, .. }) => {
                assert_eq!(function, "to_char");
            }
            other => panic!("bare to_char must be refused by shape, not by name: {other:?}"),
        }
        check_on("snowflake", "to_char(amount, 'FM999999.00') = '0'")
            .expect("an explicit format makes to_char a function of its arguments alone");
    }

    /// `date_trunc`'s `week` part (and every documented synonym, quoted or
    /// bare) reads `WEEK_START`. Every other part, including the fixed
    /// ISO week, does not and stays admitted.
    #[test]
    fn date_trunc_refuses_week_and_its_synonyms_and_admits_everything_else() {
        for part in ["week", "WEEK", "w", "wk", "weekofyear", "woy", "wy"] {
            let quoted = check_on(
                "snowflake",
                &format!("date_trunc('{part}', created_at) IS NOT NULL"),
            );
            assert!(
                matches!(
                    quoted,
                    Err(ValidationError::ExpressionFunctionShapeNotAllowed { .. })
                ),
                "'{part}' must be refused: {quoted:?}"
            );
        }
        for part in [
            "day",
            "month",
            "year",
            "quarter",
            "hour",
            "minute",
            "second",
            "week_iso",
            "yearofweekiso",
        ] {
            check_on(
                "snowflake",
                &format!("date_trunc('{part}', created_at) IS NOT NULL"),
            )
            .unwrap_or_else(|e| panic!("'{part}' must stay admitted: {e:?}"));
        }
        // The unchanged control from before #1942: a plain day truncation
        // used as a grouping key is still fine.
        check_key("date_trunc('day', created_at)")
            .expect("an ordinary date_trunc key expression is unaffected");
    }

    /// Snowflake's TRUNC/TRUNCATE date/time overload puts the part second;
    /// the numeric overload has a numeric scale in that slot.
    #[test]
    fn trunc_and_truncate_refuse_week_but_admit_safe_parts_and_numeric_scales() {
        for name in ["trunc", "truncate"] {
            for part in ["week", "WEEK", "wk", "weekofyear"] {
                let refused = check_on(
                    "snowflake",
                    &format!("{name}(created_at, '{part}') IS NOT NULL"),
                );
                match refused {
                    Err(ValidationError::ExpressionFunctionShapeNotAllowed {
                        function, ..
                    }) => {
                        assert_eq!(function, name);
                    }
                    other => panic!("{name}(created_at, '{part}') must be refused: {other:?}"),
                }
            }
            for part in ["week", "w", "wk", "weekofyear", "woy", "wy"] {
                let bare_week = check_on(
                    "snowflake",
                    &format!("{name}(created_at, {part}) IS NOT NULL"),
                );
                assert!(
                    matches!(
                        bare_week,
                        Err(ValidationError::ExpressionFunctionShapeNotAllowed { .. })
                    ),
                    "{name} with bare {part} must be refused: {bare_week:?}"
                );
            }
            let computed_week = check_on(
                "snowflake",
                &format!("{name}(created_at, lower('week')) IS NOT NULL"),
            );
            assert!(
                matches!(
                    computed_week,
                    Err(ValidationError::ExpressionFunctionShapeNotAllowed { .. })
                ),
                "{name} with a computed week part must be refused: {computed_week:?}"
            );
            check_on(
                "snowflake",
                &format!("{name}(created_at, 'day') IS NOT NULL"),
            )
            .unwrap_or_else(|e| panic!("{name} with a day part must pass: {e:?}"));
            check_on(
                "snowflake",
                &format!("{name}(created_at, 'week_iso') IS NOT NULL"),
            )
            .unwrap_or_else(|e| panic!("{name} with an ISO week part must pass: {e:?}"));
            check_on(
                "snowflake",
                &format!("{name}(created_at, 'yearofweekiso') IS NOT NULL"),
            )
            .unwrap_or_else(|e| panic!("{name} with an ISO year-of-week part must pass: {e:?}"));
            check_on("snowflake", &format!("{name}(amount, 2) > 0"))
                .unwrap_or_else(|e| panic!("{name} with a numeric scale must pass: {e:?}"));
            check_on("snowflake", &format!("{name}(amount, scale) > 0"))
                .unwrap_or_else(|e| panic!("{name} with a column scale must pass: {e:?}"));
            check_on("snowflake", &format!("{name}(amount, \"scale\") > 0"))
                .unwrap_or_else(|e| panic!("{name} with a quoted column scale must pass: {e:?}"));
            check_on("snowflake", &format!("{name}(amount, t.scale) > 0")).unwrap_or_else(|e| {
                panic!("{name} with a qualified column scale must pass: {e:?}")
            });
            let computed_scale = check_on("snowflake", &format!("{name}(amount, scale + 1) > 0"));
            assert!(
                matches!(
                    computed_scale,
                    Err(ValidationError::ExpressionFunctionShapeNotAllowed { .. })
                ),
                "{name} with a computed scale must be refused: {computed_scale:?}"
            );
            check_on("snowflake", &format!("{name}(amount, -2) > 0"))
                .unwrap_or_else(|e| panic!("{name} with a negative scale must pass: {e:?}"));
            check_on("snowflake", &format!("{name}(amount) > 0"))
                .unwrap_or_else(|e| panic!("{name} with no scale must pass: {e:?}"));
        }
    }

    /// `datediff` gets the identical `week` rule, and so does its `date_diff`
    /// spelling — the same allowlist entry under a different dialect's name,
    /// so leaving one ungated would reopen the bug through the other name.
    #[test]
    fn datediff_and_date_diff_share_the_week_rule() {
        for name in ["datediff", "date_diff"] {
            for part in ["week", "yearofweekiso"] {
                let refused = check_on("snowflake", &format!("{name}('{part}', a, b) > 0"));
                assert!(
                    matches!(
                        refused,
                        Err(ValidationError::ExpressionFunctionShapeNotAllowed { .. })
                    ),
                    "{name}('{part}', ..): {refused:?}"
                );
            }
            check_on("snowflake", &format!("{name}('day', a, b) > 0"))
                .unwrap_or_else(|e| panic!("{name}('day', ..) must stay admitted: {e:?}"));
        }
    }

    /// The shape rule applies at every nesting depth, not only when the
    /// function is the top-level node — the walker descends into every
    /// argument and branch, so a bad shape buried inside an allowed call or
    /// a `CASE` must still be caught.
    #[test]
    fn a_shape_refusal_fires_at_any_nesting_depth() {
        let nested_arity = check("coalesce(nullif(to_date(order_date), NULL), current_date)");
        match nested_arity {
            Err(ValidationError::ExpressionFunctionShapeNotAllowed { ref function, .. }) => {
                assert_eq!(function, "to_date");
            }
            other => panic!("a bare to_date two calls deep must still be refused: {other:?}"),
        }

        let nested_literal =
            check("CASE WHEN a > 0 THEN date_trunc('week', created_at) IS NOT NULL ELSE FALSE END");
        match nested_literal {
            Err(ValidationError::ExpressionFunctionShapeNotAllowed { ref function, .. }) => {
                assert_eq!(function, "date_trunc");
            }
            other => {
                panic!("a week date_trunc inside a CASE branch must still be refused: {other:?}")
            }
        }
    }

    /// `date_part` refuses three session-dependent part families — `week`,
    /// `dayofweek` and `yearofweek`, and every documented synonym of each —
    /// and admits everything else, including every ISO-fixed counterpart
    /// and the parts `date_trunc`/`datediff` don't take at all (#2141).
    #[test]
    fn date_part_refuses_week_dayofweek_yearofweek_families_and_admits_everything_else() {
        for part in [
            "week",
            "WEEK",
            "w",
            "wk",
            "weekofyear",
            "woy",
            "wy",
            "dayofweek",
            "weekday",
            "dow",
            "dw",
            "yearofweek",
        ] {
            let refused = check_on("snowflake", &format!("date_part('{part}', created_at) > 0"));
            assert!(
                matches!(
                    refused,
                    Err(ValidationError::ExpressionFunctionShapeNotAllowed { .. })
                ),
                "'{part}' must be refused: {refused:?}"
            );
        }
        for part in [
            "day",
            "month",
            "year",
            "quarter",
            "hour",
            "minute",
            "second",
            "week_iso",
            "weekiso",
            "dayofweekiso",
            "dayofweek_iso",
            "yearofweekiso",
            "dayofyear",
            "epoch_second",
            "epoch",
            "timezone_hour",
            "tzh",
        ] {
            check_on("snowflake", &format!("date_part('{part}', created_at) > 0"))
                .unwrap_or_else(|e| panic!("'{part}' must stay admitted: {e:?}"));
        }
    }

    /// `date_part`'s bare identifier form (no quotes) hits the same rule —
    /// `first_arg_literal` reads an `Expr::Identifier` the same way it reads
    /// a string literal.
    #[test]
    fn date_part_bare_identifier_part_is_gated_the_same_as_a_quoted_one() {
        let refused = check_on("snowflake", "date_part(week, created_at) > 0");
        assert!(
            matches!(
                refused,
                Err(ValidationError::ExpressionFunctionShapeNotAllowed { .. })
            ),
            "date_part(week, ..) must be refused: {refused:?}"
        );
        check_on("snowflake", "date_part(day, created_at) > 0")
            .expect("date_part(day, ..) must stay admitted");
    }

    /// `EXTRACT(<part> FROM <expr>)` is Snowflake's documented alternative
    /// spelling of `DATE_PART` and must be refused on the identical three
    /// part families. Before this gate existed, `Expr::Extract` was not
    /// matched anywhere in the walker, so every part — including `week` —
    /// fell through to the default `_ => ControlFlow::Continue(())` arm and
    /// was admitted unconditionally; this test's `week`/`dayofweek` cases
    /// pin that gap closed (#2141).
    #[test]
    fn extract_is_gated_on_the_same_three_part_families_as_date_part() {
        for part in ["week", "dayofweek", "weekday", "yearofweek"] {
            let refused = check_on("snowflake", &format!("EXTRACT({part} FROM created_at) > 0"));
            match refused {
                Err(ValidationError::ExpressionFunctionShapeNotAllowed {
                    ref function, ..
                }) => {
                    assert_eq!(function, "extract");
                }
                other => panic!("EXTRACT({part} FROM ..) must be refused: {other:?}"),
            }
        }
        for part in [
            "day",
            "year",
            "week_iso",
            "dayofweekiso",
            "yearofweekiso",
            "epoch",
        ] {
            check_on("snowflake", &format!("EXTRACT({part} FROM created_at) > 0"))
                .unwrap_or_else(|e| panic!("EXTRACT({part} FROM ..) must stay admitted: {e:?}"));
        }
    }

    /// The comma form (`EXTRACT(part, expr)`, Snowflake's own documented
    /// alternative to `EXTRACT(part FROM expr)` — sqlparser's
    /// `ExtractSyntax::Comma`, gated per-dialect and NOT supported under
    /// `BigQueryDialect`) is the same `Expr::Extract` AST node under a
    /// different concrete syntax and must be gated identically to the
    /// `FROM` form.
    #[test]
    fn extract_comma_syntax_is_gated_the_same_as_the_from_syntax() {
        let refused = check_on("snowflake", "EXTRACT(week, created_at) > 0");
        assert!(
            matches!(
                refused,
                Err(ValidationError::ExpressionFunctionShapeNotAllowed { .. })
            ),
            "EXTRACT(week, ..) must be refused: {refused:?}"
        );
        check_on("snowflake", "EXTRACT(day, created_at) > 0")
            .expect("EXTRACT(day, ..) must stay admitted");
    }

    #[test]
    fn extract_quoted_parts_use_the_identifier_value_in_both_syntaxes() {
        for expression in [
            "EXTRACT('dayofweek_iso' FROM created_at) > 0",
            "EXTRACT('dayofweek_iso', created_at) > 0",
        ] {
            check_on("snowflake", expression)
                .unwrap_or_else(|e| panic!("{expression} must pass: {e:?}"));
        }
        for expression in [
            "EXTRACT('week' FROM created_at) > 0",
            "EXTRACT('week', created_at) > 0",
        ] {
            assert!(
                matches!(
                    check_on("snowflake", expression),
                    Err(ValidationError::ExpressionFunctionShapeNotAllowed { .. })
                ),
                "{expression} must be refused"
            );
        }
    }

    /// The older DATE_TRUNC rule applies to every dialect. The #2141 gates
    /// use the same policy even when BigQuery gives WEEK a fixed meaning.
    #[test]
    fn week_part_policy_is_dialect_agnostic() {
        for dialect in ["snowflake", "bigquery", "not-a-dialect"] {
            for expression in [
                "date_trunc('week', created_at) IS NOT NULL",
                "date_part('week', created_at) > 0",
                "EXTRACT(WEEK FROM created_at) > 0",
            ] {
                assert!(
                    matches!(
                        check_on(dialect, expression),
                        Err(ValidationError::ExpressionFunctionShapeNotAllowed { .. })
                    ),
                    "{dialect}: {expression} must be refused"
                );
            }
        }
        for expression in [
            "EXTRACT(ISOWEEK FROM created_at) > 0",
            "EXTRACT(WEEK(MONDAY) FROM created_at) > 0",
        ] {
            assert!(
                matches!(
                    check_on("bigquery", expression),
                    Err(ValidationError::ExpressionFunctionShapeNotAllowed { .. })
                ),
                "BigQuery {expression} remains refused by the shared allowlist"
            );
        }
    }

    /// The shape rule applies at any nesting depth for `date_part`/`EXTRACT`
    /// too, same as the pre-existing `date_trunc` case above.
    #[test]
    fn date_part_and_extract_refusals_fire_at_any_nesting_depth() {
        let nested_date_part = check_on(
            "snowflake",
            "CASE WHEN a > 0 THEN date_part('week', created_at) > 0 ELSE FALSE END",
        );
        match nested_date_part {
            Err(ValidationError::ExpressionFunctionShapeNotAllowed { ref function, .. }) => {
                assert_eq!(function, "date_part");
            }
            other => {
                panic!("a week date_part inside a CASE branch must still be refused: {other:?}")
            }
        }

        let nested_extract = check_on(
            "snowflake",
            "coalesce(NULL, EXTRACT(dayofweek FROM created_at))",
        );
        match nested_extract {
            Err(ValidationError::ExpressionFunctionShapeNotAllowed { ref function, .. }) => {
                assert_eq!(function, "extract");
            }
            other => {
                panic!("a dayofweek EXTRACT nested in coalesce must still be refused: {other:?}")
            }
        }
    }
}
