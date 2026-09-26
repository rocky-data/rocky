//! Parse errors with source locations and miette rendering.

use std::borrow::Cow;

use thiserror::Error;

/// Errors during Rocky DSL parsing.
///
/// The `expected` and `found` fields use `Cow<'static, str>` so common
/// static-literal cases (e.g. `"identifier"`, `"number"`,
/// `"pipeline step"`) avoid a `String::from` allocation on the error
/// path — matters for LSP hot paths where parse errors land per
/// keystroke while the user is mid-typing (§P3.9).
#[derive(Debug, Error)]
pub enum ParseError {
    #[error("unexpected token at offset {offset}: expected {expected}, found {found}")]
    UnexpectedToken {
        expected: Cow<'static, str>,
        found: Cow<'static, str>,
        offset: usize,
    },

    #[error("unexpected end of file: expected {expected}")]
    UnexpectedEof { expected: Cow<'static, str> },

    #[error("invalid number: {value}")]
    InvalidNumber { value: String },

    #[error("empty file: no pipeline steps found")]
    EmptyFile,

    #[error("expression nested too deeply: depth {depth} exceeds limit {limit} at offset {offset}")]
    TooDeeplyNested {
        depth: usize,
        limit: usize,
        offset: usize,
    },

    /// A string literal holds a backslash (#1596). Diagnostic code
    /// [`BACKSLASH_IN_STRING_LITERAL`].
    ///
    /// The DSL defines no escape sequences, so the backslash is part of the
    /// value. The lowered SQL literal cannot carry it on every warehouse:
    /// Snowflake, Databricks and BigQuery read a backslash in `'...'` as an
    /// escape, DuckDB and Trino do not, and lowering runs before a warehouse
    /// is known. So no single SQL text is right on all of them.
    #[error(
        "{BACKSLASH_IN_STRING_LITERAL}: string literal at offset {offset} contains a \
         backslash. {BACKSLASH_REASON}"
    )]
    BackslashInStringLiteral { offset: usize, len: usize },
}

/// Diagnostic code for [`ParseError::BackslashInStringLiteral`]. Re-exported
/// by `rocky-compiler` as `E040`.
pub const BACKSLASH_IN_STRING_LITERAL: &str = "E040";

/// Why a backslash is refused, and the escape hatch.
const BACKSLASH_REASON: &str = "Rocky cannot lower it to one SQL literal that keeps \
    the value on every warehouse: Snowflake, Databricks and BigQuery read a backslash \
    as an escape. Write this model as a .sql model to control the literal yourself";

/// A Rocky DSL parse error enriched with the original source text and file
/// path, ready for miette rendering with source spans.
#[derive(Debug, Error, miette::Diagnostic)]
#[error("{message}")]
pub struct RichParseError {
    /// Human-readable summary.
    pub message: String,

    /// The Rocky DSL source text that failed to parse.
    #[source_code]
    pub src: miette::NamedSource<String>,

    /// Span pointing at the error location.
    #[label("here")]
    pub span: Option<miette::SourceSpan>,

    /// Actionable suggestion.
    #[help]
    pub help: Option<String>,
}

impl ParseError {
    /// Convert into a miette-compatible diagnostic with source spans.
    pub fn into_rich(self, source: &str, file: &str) -> RichParseError {
        let named = miette::NamedSource::new(file, source.to_string());

        match &self {
            ParseError::UnexpectedToken {
                expected,
                found,
                offset,
            } => {
                let len = found.len().max(1);
                RichParseError {
                    message: format!("expected {expected}, found {found}"),
                    src: named,
                    span: Some(miette::SourceSpan::new((*offset).into(), len)),
                    help: Some(format!("expected {expected} at this position")),
                }
            }
            ParseError::UnexpectedEof { expected } => {
                let offset = source.len().saturating_sub(1);
                RichParseError {
                    message: format!("unexpected end of file: expected {expected}"),
                    src: named,
                    span: Some(miette::SourceSpan::new(offset.into(), 1)),
                    help: Some(format!("add {expected} before the end of the file")),
                }
            }
            ParseError::InvalidNumber { value } => RichParseError {
                message: format!("invalid number: {value}"),
                src: named,
                span: None,
                help: Some("use a valid integer or decimal literal".to_string()),
            },
            ParseError::EmptyFile => RichParseError {
                message: "empty file: no pipeline steps found".to_string(),
                src: named,
                span: None,
                help: Some(
                    "add a pipeline starting with `from <model>` or `select { ... }`".to_string(),
                ),
            },
            ParseError::TooDeeplyNested {
                depth,
                limit,
                offset,
            } => RichParseError {
                message: format!(
                    "expression nested too deeply: depth {depth} exceeds limit {limit}"
                ),
                src: named,
                span: Some(miette::SourceSpan::new((*offset).into(), 1)),
                help: Some(
                    "simplify the expression — extract sub-expressions into `let` \
                     bindings or split the pipeline into smaller steps"
                        .to_string(),
                ),
            },
            ParseError::BackslashInStringLiteral { offset, len } => RichParseError {
                message: format!(
                    "{BACKSLASH_IN_STRING_LITERAL}: string literal contains a backslash"
                ),
                src: named,
                span: Some(miette::SourceSpan::new((*offset).into(), *len)),
                help: Some(BACKSLASH_REASON.to_string()),
            },
        }
    }
}
