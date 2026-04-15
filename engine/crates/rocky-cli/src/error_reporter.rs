//! Miette-based error reporter for rich CLI diagnostics.
//!
//! Converts `anyhow::Error` chains originating from `rocky-core` into
//! [`miette::Diagnostic`] values with source spans, pointer annotations,
//! and actionable suggestions.  Errors that don't carry span information
//! fall through unchanged — the caller renders them with the default
//! anyhow formatting.
//!
//! This module is the **only** place where `miette` types appear.  The
//! library crates (`rocky-core`, adapters) stay on plain `thiserror` +
//! `Option<Range<usize>>` spans; the conversion to `miette::SourceSpan`
//! happens here.

use std::path::Path;

use miette::SourceSpan;
use rocky_core::config::ConfigError;

/// A miette-compatible diagnostic wrapping a Rocky config error with
/// source context.
#[derive(Debug, miette::Diagnostic, thiserror::Error)]
#[error("{message}")]
pub struct ConfigDiagnostic {
    message: String,

    #[source_code]
    src: miette::NamedSource<String>,

    #[label("{label}")]
    span: Option<SourceSpan>,

    label: String,

    #[help]
    help: Option<String>,
}

/// Known adapter type names for "did you mean?" suggestions.
pub const KNOWN_ADAPTER_TYPES: &[&str] = &[
    "databricks",
    "duckdb",
    "snowflake",
    "bigquery",
    "fivetran",
    "airbyte",
    "iceberg",
    "manual",
];

/// Attempt to convert an `anyhow::Error` into a rich [`ConfigDiagnostic`].
///
/// Walks the error chain looking for a [`ConfigError`]. If one is found
/// **and** it carries enough context to produce a span-annotated
/// diagnostic, returns `Some`. Otherwise returns `None` and the caller
/// should fall through to the default anyhow rendering.
pub fn try_upgrade_config_error(
    err: &anyhow::Error,
    config_path: &Path,
) -> Option<ConfigDiagnostic> {
    // Walk the chain — ConfigError may be wrapped behind one or more
    // `.context()` layers.
    let config_err = err.downcast_ref::<ConfigError>()?;

    let source_text = std::fs::read_to_string(config_path).ok()?;
    let named_source = miette::NamedSource::new(config_path.display().to_string(), source_text);

    match config_err {
        ConfigError::MissingEnvVar { name, span } => {
            let miette_span = span.as_ref().map(|r| SourceSpan::from(r.start..r.end));

            Some(ConfigDiagnostic {
                message: format!("environment variable '{name}' is not set"),
                src: named_source,
                span: miette_span,
                label: format!("${{{name}}} referenced here"),
                help: Some(format!(
                    "Add {name} to your shell environment or .env file, \
                     or use a default: ${{{name}:-default_value}}"
                )),
            })
        }
        ConfigError::ParseToml(toml_err) => {
            let miette_span = toml_err.span().map(|r| SourceSpan::from(r.start..r.end));

            Some(ConfigDiagnostic {
                message: format!("invalid TOML syntax: {toml_err}"),
                src: named_source,
                span: miette_span,
                label: "parse error here".to_string(),
                help: Some(
                    "Check the TOML syntax — common mistakes include \
                     missing quotes around strings, unclosed brackets, \
                     or duplicate keys"
                        .to_string(),
                ),
            })
        }
        // ReadFile and InvalidPattern don't have span info — let the
        // caller fall through to standard anyhow rendering.
        _ => None,
    }
}

/// Find the best "did you mean?" suggestion for `input` among
/// `candidates` using Jaro-Winkler similarity.
///
/// Returns `Some(candidate)` if the best match exceeds the threshold
/// (0.8), otherwise `None`.
pub fn did_you_mean<'a>(input: &str, candidates: &[&'a str]) -> Option<&'a str> {
    let input_lower = input.to_lowercase();
    candidates
        .iter()
        .map(|c| (*c, strsim::jaro_winkler(&input_lower, &c.to_lowercase())))
        .filter(|(_, score)| *score > 0.8)
        .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
        .map(|(candidate, _)| candidate)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_env_var_produces_diagnostic() {
        let err = ConfigError::MissingEnvVar {
            name: "MY_TOKEN".to_string(),
            span: Some(10..22),
        };
        let anyhow_err: anyhow::Error = err.into();

        // Write a temp config to disk for the reporter to read.
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("rocky.toml");
        let config_text = r#"[adapter]
token = "${MY_TOKEN}"
type = "databricks"
"#;
        std::fs::write(&config_path, config_text).unwrap();

        let diag = try_upgrade_config_error(&anyhow_err, &config_path);
        assert!(
            diag.is_some(),
            "should produce a diagnostic for MissingEnvVar"
        );
        let diag = diag.unwrap();
        assert!(diag.message.contains("MY_TOKEN"));
        assert!(diag.help.as_ref().unwrap().contains(".env"));
        assert!(diag.span.is_some());
    }

    #[test]
    fn toml_parse_error_produces_diagnostic() {
        let bad_toml = "[adapter\ntype = 'databricks'";
        let toml_err = toml::from_str::<toml::Value>(bad_toml).unwrap_err();
        let err: anyhow::Error = ConfigError::ParseToml(toml_err).into();

        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("rocky.toml");
        std::fs::write(&config_path, bad_toml).unwrap();

        let diag = try_upgrade_config_error(&err, &config_path);
        assert!(diag.is_some(), "should produce a diagnostic for ParseToml");
        let diag = diag.unwrap();
        assert!(diag.message.contains("TOML"));
        assert!(diag.help.is_some());
    }

    #[test]
    fn io_error_returns_none() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "not found");
        let err: anyhow::Error = ConfigError::ReadFile(io_err).into();

        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("rocky.toml");
        std::fs::write(&config_path, "").unwrap();

        let diag = try_upgrade_config_error(&err, &config_path);
        assert!(diag.is_none(), "ReadFile should fall through to anyhow");
    }

    #[test]
    fn did_you_mean_finds_close_match() {
        assert_eq!(
            did_you_mean("databrik", KNOWN_ADAPTER_TYPES),
            Some("databricks")
        );
        assert_eq!(did_you_mean("duckbd", KNOWN_ADAPTER_TYPES), Some("duckdb"));
        assert_eq!(
            did_you_mean("snowflak", KNOWN_ADAPTER_TYPES),
            Some("snowflake")
        );
    }

    #[test]
    fn did_you_mean_returns_none_for_distant() {
        assert_eq!(did_you_mean("postgres", KNOWN_ADAPTER_TYPES), None);
        assert_eq!(did_you_mean("zzzzzzz", KNOWN_ADAPTER_TYPES), None);
    }

    #[test]
    fn did_you_mean_case_insensitive() {
        assert_eq!(
            did_you_mean("DATABRICKS", KNOWN_ADAPTER_TYPES),
            Some("databricks")
        );
        assert_eq!(did_you_mean("DuckDB", KNOWN_ADAPTER_TYPES), Some("duckdb"));
    }
}
