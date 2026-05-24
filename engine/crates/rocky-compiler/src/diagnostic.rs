//! Compiler diagnostics with source spans and suggestions.
//!
//! Used by both the type checker and contract validator to report issues.
//! Diagnostics can be converted to miette `Report`s for rich terminal
//! rendering with source spans, underlines, and help text.
//!
//! §P3.5: `code` and `message` are `Arc<str>` so cloning a `Diagnostic`
//! (hot-path in the LSP publish loop) is a refcount bump instead of a
//! `String` allocation. The JSON wire format is unchanged because serde's
//! `rc` feature makes `Arc<str>` (de)serialize transparently as a string.

use std::sync::Arc;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Diagnostic code registry
// ---------------------------------------------------------------------------

// Errors — type checking
/// Unresolved model reference.
pub const E001: &str = "E001";

// Errors — contract validation
/// Required column missing from model output.
pub const E010: &str = "E010";
/// Column type mismatch (contract vs model output).
pub const E011: &str = "E011";
/// Nullability violation (contract says non-nullable, model says nullable).
pub const E012: &str = "E012";
/// Protected column has been removed.
pub const E013: &str = "E013";

// Errors — time_interval validation
/// Missing `@start_date` placeholder.
pub const E020: &str = "E020";
/// Missing `@end_date` placeholder.
pub const E021: &str = "E021";
/// `time_column` not in model output schema.
pub const E022: &str = "E022";
/// `time_column` has incompatible type for granularity.
pub const E023: &str = "E023";
/// `time_column` is nullable (must be NOT NULL).
pub const E024: &str = "E024";
/// Placeholder used but strategy is not `time_interval`.
pub const E025: &str = "E025";
/// Duplicate `@start_date`/`@end_date` placeholder.
pub const E026: &str = "E026";
/// Budget exceeded — projected spend exceeds the declared per-model cost ceiling.
///
/// Emitted by `rocky compile` when the DAG-propagated cost estimate for a
/// model exceeds either `max_usd` or `max_bytes_scanned` declared in the
/// model's `[budget]` sidecar block.  Estimates are computed by
/// [`rocky_core::cost::propagate_costs`] using catalog-sourced table stats
/// when available, falling back to conservative stub statistics.
pub const E027: &str = "E027";

// Warnings
/// Unused model (no downstream consumers).
pub const W001: &str = "W001";
/// Duplicate column in model output.
pub const W002: &str = "W002";
/// `time_column` type is not DATE for day/month/year granularity (TIMESTAMP works but DATE preferred).
pub const W003: &str = "W003";
/// Classification tag on a model column doesn't resolve to any `[mask]` /
/// `[mask.<env>]` strategy and isn't listed in `[classifications.allow_unmasked]`.
/// One diagnostic per unresolved `(model, column, tag)` triple.
pub const W004: &str = "W004";
/// Model has at least one temporal output column (DATE / TIMESTAMP /
/// TIMESTAMP_NTZ) but no `freshness` declaration in scope — neither a
/// per-model sidecar block nor a project-level `[freshness]` default.
/// Soft hint that the model would benefit from a freshness expectation.
/// Suppressed by adding a `[freshness]` block (per-model or project).
pub const W005: &str = "W005";
/// Contract defines a column not in model output (but not required).
pub const W010: &str = "W010";
/// Contract exists for a model not found in the project.
pub const W011: &str = "W011";

// Info
/// Model dependency inferred from SQL.
pub const I001: &str = "I001";
/// Model compiled with SELECT *.
pub const I002: &str = "I002";

// Lints — portability + blast-radius
/// Construct is not portable to the configured target dialect.
/// Error severity, opt-in via `--target-dialect`. Emitted by the CLI.
pub const P001: &str = "P001";
/// `SELECT *` model has downstream consumers that reference specific
/// columns of its output — a schema change in the star's source would
/// silently propagate. Warning severity, always-on.
pub const P002: &str = "P002";

/// Severity level of a diagnostic.
///
/// Serialized in PascalCase (`"Error"`, `"Warning"`, `"Info"`) to stay
/// compatible with existing dagster fixtures and the hand-written
/// `Severity` StrEnum in `integrations/dagster/src/dagster_rocky/types.py`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum Severity {
    Error,
    Warning,
    Info,
}

/// Location in a source file.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SourceSpan {
    pub file: String,
    pub line: usize,
    pub col: usize,
}

/// A compiler diagnostic (error, warning, or informational message).
///
/// `code` and `message` use `Arc<str>` (§P3.5) — cloning a `Diagnostic`
/// in the LSP publish loop becomes a refcount bump. Construction still
/// accepts any `Into<String>` / `&str` via the helper constructors below;
/// the arc wrap happens once at construction time.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Diagnostic {
    /// Severity level.
    pub severity: Severity,
    /// Diagnostic code (e.g., "E001", "W001").
    pub code: Arc<str>,
    /// Human-readable message.
    pub message: Arc<str>,
    /// Source location (if available).
    pub span: Option<SourceSpan>,
    /// Which model this diagnostic relates to.
    pub model: String,
    /// Suggested fix (if any).
    pub suggestion: Option<String>,
}

impl Diagnostic {
    /// Create an error diagnostic.
    pub fn error(code: &str, model: &str, message: impl Into<String>) -> Self {
        Self {
            severity: Severity::Error,
            code: Arc::from(code),
            message: Arc::from(message.into()),
            span: None,
            model: model.to_string(),
            suggestion: None,
        }
    }

    /// Create a warning diagnostic.
    pub fn warning(code: &str, model: &str, message: impl Into<String>) -> Self {
        Self {
            severity: Severity::Warning,
            code: Arc::from(code),
            message: Arc::from(message.into()),
            span: None,
            model: model.to_string(),
            suggestion: None,
        }
    }

    /// Create an info diagnostic.
    pub fn info(code: &str, model: &str, message: impl Into<String>) -> Self {
        Self {
            severity: Severity::Info,
            code: Arc::from(code),
            message: Arc::from(message.into()),
            span: None,
            model: model.to_string(),
            suggestion: None,
        }
    }

    /// Add a source span.
    #[must_use]
    pub fn with_span(mut self, span: SourceSpan) -> Self {
        self.span = Some(span);
        self
    }

    /// Add a suggestion.
    #[must_use]
    pub fn with_suggestion(mut self, suggestion: impl Into<String>) -> Self {
        self.suggestion = Some(suggestion.into());
        self
    }

    /// Build an E027 budget-exceeded diagnostic for a USD cost ceiling.
    ///
    /// Emitted when the DAG-propagated cost estimate for a model exceeds the
    /// `max_usd` value declared in the model's `[budget]` sidecar block.
    #[must_use]
    pub fn budget_exceeded(model: &str, projected_usd: f64, ceiling_usd: f64) -> Self {
        Self::error(
            E027,
            model,
            format!("budget exceeded — projected ${projected_usd:.4} > ceiling ${ceiling_usd:.4}",),
        )
        .with_suggestion(format!(
            "raise [budget] max_usd above ${ceiling_usd:.4} in the model sidecar, \
             or optimize the query to reduce scan volume"
        ))
    }

    /// Build an E027 budget-exceeded diagnostic for a bytes-scanned ceiling.
    ///
    /// Emitted when the DAG-propagated byte estimate for a model exceeds the
    /// `max_bytes_scanned` value declared in the model's `[budget]` sidecar
    /// block.
    #[must_use]
    pub fn budget_exceeded_bytes(model: &str, projected_bytes: u64, ceiling_bytes: u64) -> Self {
        Self::error(
            E027,
            model,
            format!(
                "budget exceeded — projected {projected_bytes} bytes > ceiling {ceiling_bytes} bytes scanned",
            ),
        )
        .with_suggestion(format!(
            "raise [budget] max_bytes_scanned above {ceiling_bytes} in the model sidecar, \
             or optimize the query to reduce scan volume"
        ))
    }

    /// Build a **warning**-severity E027 USD budget diagnostic.
    ///
    /// Used at plan time when the model's `on_breach = "warn"` policy means a
    /// ceiling breach should be surfaced as advisory rather than blocking.
    /// The message is identical to [`Self::budget_exceeded`]; only severity
    /// differs.
    #[must_use]
    pub fn budget_exceeded_warn(model: &str, projected_usd: f64, ceiling_usd: f64) -> Self {
        Self::warning(
            E027,
            model,
            format!("budget exceeded — projected ${projected_usd:.4} > ceiling ${ceiling_usd:.4}",),
        )
        .with_suggestion(format!(
            "raise [budget] max_usd above ${ceiling_usd:.4} in the model sidecar, \
             or optimize the query to reduce scan volume"
        ))
    }

    /// Build a **warning**-severity E027 bytes-scanned budget diagnostic.
    ///
    /// Used at plan time when the model's `on_breach = "warn"` policy means a
    /// ceiling breach should be surfaced as advisory rather than blocking.
    #[must_use]
    pub fn budget_exceeded_bytes_warn(
        model: &str,
        projected_bytes: u64,
        ceiling_bytes: u64,
    ) -> Self {
        Self::warning(
            E027,
            model,
            format!(
                "budget exceeded — projected {projected_bytes} bytes > ceiling {ceiling_bytes} bytes scanned",
            ),
        )
        .with_suggestion(format!(
            "raise [budget] max_bytes_scanned above {ceiling_bytes} in the model sidecar, \
             or optimize the query to reduce scan volume"
        ))
    }

    /// Is this an error?
    pub fn is_error(&self) -> bool {
        self.severity == Severity::Error
    }

    /// Render this diagnostic as a miette `Report` with rich source spans.
    ///
    /// If `source_text` is provided, the diagnostic will include an underlined
    /// source snippet pointing at the error location. Without source text,
    /// falls back to a plain message with file:line:col.
    pub fn to_miette(&self, source_text: Option<&str>) -> miette::Report {
        let severity_prefix = match self.severity {
            Severity::Error => "error",
            Severity::Warning => "warning",
            Severity::Info => "info",
        };

        if let (Some(span), Some(src)) = (&self.span, source_text) {
            // Convert line:col to byte offset for miette
            if let Some(offset) = line_col_to_offset(src, span.line, span.col) {
                let diag = RichDiagnostic {
                    message: format!("{severity_prefix}[{}]: {}", self.code, self.message),
                    src: miette::NamedSource::new(&span.file, src.to_string()),
                    span: Some(miette::SourceSpan::new(offset.into(), 1)),
                    help: self.suggestion.clone(),
                    code: self.code.to_string(),
                    severity: self.severity,
                };
                return miette::Report::new(diag);
            }
        }

        // Fallback: no source text or can't resolve offset — plain diagnostic
        let diag = RichDiagnostic {
            message: format!("{severity_prefix}[{}]: {}", self.code, self.message),
            src: miette::NamedSource::new(
                self.span
                    .as_ref()
                    .map(|s| s.file.as_str())
                    .unwrap_or(&self.model),
                String::new(),
            ),
            span: None,
            help: self.suggestion.clone(),
            code: self.code.to_string(),
            severity: self.severity,
        };
        miette::Report::new(diag)
    }
}

impl std::fmt::Display for Diagnostic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let severity = match self.severity {
            Severity::Error => "error",
            Severity::Warning => "warning",
            Severity::Info => "info",
        };

        write!(f, "{severity}[{}]: {}", self.code, self.message)?;

        if let Some(ref span) = self.span {
            write!(f, "\n --> {}:{}:{}", span.file, span.line, span.col)?;
        }

        if let Some(ref suggestion) = self.suggestion {
            write!(f, "\n = help: {suggestion}")?;
        }

        Ok(())
    }
}

/// A miette-compatible diagnostic that carries source text and spans.
///
/// This is the internal rendering type — callers create these via
/// `Diagnostic::to_miette()` or by constructing directly for parser errors.
#[derive(Debug, miette::Diagnostic, thiserror::Error)]
#[error("{message}")]
pub struct RichDiagnostic {
    /// Formatted message with severity prefix and code.
    pub message: String,

    /// The source code being diagnosed.
    #[source_code]
    pub src: miette::NamedSource<String>,

    /// Span highlighting the error location.
    #[label("here")]
    pub span: Option<miette::SourceSpan>,

    /// Actionable fix suggestion.
    #[help]
    pub help: Option<String>,

    /// Diagnostic code (e.g., "E001").
    code: String,

    /// Severity (not rendered by miette, but available for callers).
    #[allow(dead_code)]
    severity: Severity,
}

/// Convert 1-based line and column to a byte offset in source text.
fn line_col_to_offset(source: &str, line: usize, col: usize) -> Option<usize> {
    let mut current_line = 1;
    let mut line_start = 0;
    for (i, ch) in source.char_indices() {
        if current_line == line {
            let offset = line_start + col.saturating_sub(1);
            return if offset <= source.len() {
                Some(offset)
            } else {
                None
            };
        }
        if ch == '\n' {
            current_line += 1;
            line_start = i + 1;
        }
    }
    if current_line == line {
        let offset = line_start + col.saturating_sub(1);
        return if offset <= source.len() {
            Some(offset)
        } else {
            None
        };
    }
    None
}

/// Render a collection of diagnostics as rich miette output.
///
/// For each diagnostic that has a `span` with a matching `file` key in
/// `source_map`, the full source is embedded so miette can underline the
/// error. Diagnostics without source information render as plain messages.
pub fn render_diagnostics(
    diagnostics: &[Diagnostic],
    source_map: &std::collections::HashMap<String, String>,
) -> String {
    use std::fmt::Write;

    let mut buf = String::new();
    for d in diagnostics {
        let src = d
            .span
            .as_ref()
            .and_then(|s| source_map.get(&s.file).map(std::string::String::as_str));

        let report = d.to_miette(src);
        // Use miette's GraphicalReportHandler for pretty output.
        let mut rendered = String::new();
        let handler = miette::GraphicalReportHandler::new();
        if handler
            .render_report(&mut rendered, report.as_ref())
            .is_ok()
        {
            let _ = writeln!(buf, "{rendered}");
        } else {
            // Fallback to Display
            let _ = writeln!(buf, "  {d}");
        }
    }
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_line_col_to_offset() {
        let src = "SELECT *\nFROM foo\nWHERE x = 1";
        assert_eq!(line_col_to_offset(src, 1, 1), Some(0));
        assert_eq!(line_col_to_offset(src, 2, 1), Some(9));
        assert_eq!(line_col_to_offset(src, 3, 7), Some(24));
    }

    #[test]
    fn test_diagnostic_to_miette_with_source() {
        let src = "SELECT *\nFROM foo\nWHERE x = 1";
        let d = Diagnostic::error("E001", "my_model", "type mismatch on column 'x'")
            .with_span(SourceSpan {
                file: "models/my_model.sql".to_string(),
                line: 3,
                col: 7,
            })
            .with_suggestion("add explicit CAST to match types");

        let report = d.to_miette(Some(src));
        let output = format!("{report:?}");
        assert!(output.contains("E001"));
    }

    #[test]
    fn test_diagnostic_to_miette_without_source() {
        let d = Diagnostic::warning("W001", "my_model", "implicit coercion");
        let report = d.to_miette(None);
        let output = format!("{report:?}");
        assert!(output.contains("W001"));
    }

    #[test]
    fn test_render_diagnostics_basic() {
        let d = Diagnostic::error("E001", "test", "something broke").with_suggestion("fix it");
        let rendered = render_diagnostics(&[d], &std::collections::HashMap::new());
        assert!(rendered.contains("E001"));
        assert!(rendered.contains("something broke"));
    }

    #[test]
    fn test_budget_exceeded_constructs_correctly() {
        let d = Diagnostic::budget_exceeded("fct_orders", 12.50, 10.00);
        assert_eq!(d.severity, Severity::Error);
        assert_eq!(d.code.as_ref(), E027);
        assert_eq!(d.model, "fct_orders");
        assert!(
            d.message.contains("12.5000"),
            "message must include projected cost, got: {}",
            d.message
        );
        assert!(
            d.message.contains("10.0000"),
            "message must include ceiling cost, got: {}",
            d.message
        );
        assert!(
            d.suggestion.is_some(),
            "budget_exceeded must include a suggestion"
        );
        assert!(d.is_error());
    }

    #[test]
    fn test_budget_exceeded_bytes_constructs_correctly() {
        let d = Diagnostic::budget_exceeded_bytes("fct_orders", 5_000_000, 1_000_000);
        assert_eq!(d.severity, Severity::Error);
        assert_eq!(d.code.as_ref(), E027);
        assert_eq!(d.model, "fct_orders");
        assert!(
            d.message.contains("5000000"),
            "message must include projected bytes, got: {}",
            d.message
        );
        assert!(
            d.message.contains("1000000"),
            "message must include ceiling bytes, got: {}",
            d.message
        );
        assert!(d.suggestion.is_some());
        assert!(d.is_error());
    }

    #[test]
    fn test_budget_exceeded_serializes() {
        let d = Diagnostic::budget_exceeded("my_model", 5.0, 3.0);
        let json = serde_json::to_string(&d).unwrap();
        assert!(json.contains("E027"));
        assert!(json.contains("my_model"));
        // Round-trip
        let back: Diagnostic = serde_json::from_str(&json).unwrap();
        assert_eq!(back.code.as_ref(), E027);
    }

    #[test]
    fn test_e027_constant_value() {
        assert_eq!(E027, "E027");
    }
}
