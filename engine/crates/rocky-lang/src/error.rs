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
}

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
        }
    }
}
