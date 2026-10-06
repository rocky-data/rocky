//! Compile-time checks for freshness declarations (E050 / W050).
//!
//! Two declaration sites:
//!
//! - a transformation pipeline's `[[pipeline.<name>.sources]]` `freshness`
//!   block ([`check_source_freshness`]), checked against the source schemas
//!   the compiler holds (seed or schema cache);
//! - a model's `[freshness]` block ([`check_model_freshness`]), checked
//!   against the model's own typed output.
//!
//! The runtime side lives in `rocky freshness`. These checks catch, before a
//! run, the declarations that command could never evaluate.
//!
//! False refusals are worse than misses, so only facts error (E050): a
//! malformed declaration, or a model column absent from a provably complete
//! output. Anything that depends on a possibly stale source schema, or on an
//! output Rocky cannot fully enumerate, at most warns (W050).

use std::collections::HashMap;

use indexmap::IndexMap;
use rocky_core::source_freshness::PipelineSourceConfig;

use crate::diagnostic::{Diagnostic, E050, W050};
use crate::semantic::SemanticGraph;
use crate::types::{RockyType, TypedColumn};

/// The diagnostic `model` field for a source: `source:<schema>.<table>`.
pub fn source_diagnostic_subject(source: &PipelineSourceConfig) -> String {
    format!("source:{}", source.full_name())
}

/// E050 / W050 for every declared source freshness block.
///
/// `source_schemas` is the compiler's map keyed `"<schema>.<table>"` (seed
/// and schema cache share that shape). A source absent from it is skipped
/// for the column checks: no schema, nothing to compare.
pub fn check_source_freshness(
    sources: &[PipelineSourceConfig],
    source_schemas: &HashMap<String, Vec<TypedColumn>>,
) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    for source in sources {
        let Some(freshness) = &source.freshness else {
            continue;
        };
        let subject = source_diagnostic_subject(source);

        if let Err(problems) = freshness.validate() {
            for problem in problems {
                diagnostics.push(
                    Diagnostic::error(
                        E050,
                        &subject,
                        format!("source '{}': {problem}", source.full_name()),
                    )
                    .with_suggestion(
                        "fix the `[pipeline.<name>.sources.freshness]` block: set `warn_after` \
                             and/or `error_after` as \"<N>s\", \"<N>h\" or \"<N>d\", with \
                             `error_after` >= `warn_after`",
                    ),
                );
            }
        }

        let wanted = format!("{}.{}", source.schema, source.table);
        let Some(columns) = source_schemas
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(&wanted))
            .map(|(_, cols)| cols)
        else {
            continue;
        };
        let field = &freshness.loaded_at_field;
        match columns.iter().find(|c| c.name.eq_ignore_ascii_case(field)) {
            None => diagnostics.push(
                Diagnostic::warning(
                    W050,
                    &subject,
                    format!(
                        "source '{}': `loaded_at_field` '{field}' is not in the known schema of \
                         the source",
                        source.full_name()
                    ),
                )
                .with_suggestion(format!(
                    "known columns: {}. The schema came from a seed or the schema cache and may \
                     be stale; `rocky freshness` reports the warehouse's answer",
                    columns
                        .iter()
                        .map(|c| c.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )),
            ),
            Some(col) => {
                if let Some(d) =
                    non_temporal_warning(&subject, "loaded_at_field", field, &col.data_type)
                {
                    diagnostics.push(d);
                }
            }
        }
    }
    diagnostics
}

/// E050 / W050 for every model `[freshness]` block that names a `time_column`.
///
/// A missing column is E050 only when the model's own sidecar declares it and
/// the model's output is provably complete
/// ([`crate::semantic::ModelSchema::schema_is_complete`]). An inherited
/// `time_column` (from `_defaults.toml` or the project `[freshness]`) that
/// the model does not output is W050: the model never asked for it. An
/// incomplete output reports nothing, since the absence may be an artefact of
/// enumeration.
pub fn check_model_freshness(
    models: &[rocky_core::models::Model],
    typed_models: &IndexMap<String, Vec<TypedColumn>>,
    semantic_graph: &SemanticGraph,
) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    for model in models {
        let name = model.config.name.as_str();
        let Some(column) = model
            .config
            .freshness
            .as_ref()
            .and_then(|f| f.time_column.as_deref())
        else {
            continue;
        };
        let Some(cols) = typed_models.get(name) else {
            continue;
        };
        match cols.iter().find(|c| c.name.eq_ignore_ascii_case(column)) {
            Some(col) => {
                if let Some(d) = non_temporal_warning(name, "time_column", column, &col.data_type) {
                    diagnostics.push(d);
                }
            }
            None => {
                let complete = semantic_graph
                    .model_schema(name)
                    .is_some_and(crate::semantic::ModelSchema::schema_is_complete);
                if !complete {
                    continue;
                }
                let declared = model
                    .config
                    .freshness
                    .as_ref()
                    .is_some_and(|f| f.declared_in_sidecar);
                let available = cols
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                if declared {
                    diagnostics.push(
                        Diagnostic::error(
                            E050,
                            name,
                            format!(
                                "model '{name}': `[freshness] time_column` '{column}' is not an \
                                 output column of the model"
                            ),
                        )
                        .with_suggestion(format!("output columns: {available}")),
                    );
                } else {
                    // Inherited from `_defaults.toml` or the project
                    // `[freshness]`: the model never asked for this column,
                    // so refusing its build would be a false refusal.
                    diagnostics.push(
                        Diagnostic::warning(
                            W050,
                            name,
                            format!(
                                "model '{name}': inherited freshness `time_column` '{column}' is \
                                 not an output column of the model; `rocky freshness` falls back \
                                 to the model's last successful build"
                            ),
                        )
                        .with_suggestion(format!(
                            "declare a `[freshness]` block in the model sidecar with one of: \
                             {available}"
                        )),
                    );
                }
            }
        }
    }
    diagnostics
}

/// W050 when `ty` is concrete and not temporal. `Unknown` is not a finding.
fn non_temporal_warning(
    subject: &str,
    key: &str,
    column: &str,
    ty: &RockyType,
) -> Option<Diagnostic> {
    if ty.is_temporal() || *ty == RockyType::Unknown {
        return None;
    }
    Some(
        Diagnostic::warning(
            W050,
            subject,
            format!(
                "{subject}: freshness `{key}` '{column}' has type {ty:?}, not DATE or TIMESTAMP"
            ),
        )
        .with_suggestion(
            "point the freshness column at a DATE or TIMESTAMP column; `rocky freshness` reports \
             `runtime_error` when MAX(column) does not read as a timestamp",
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_core::source_freshness::SourceFreshnessConfig;

    fn source(field: &str, warn: Option<&str>, error: Option<&str>) -> PipelineSourceConfig {
        PipelineSourceConfig {
            catalog: String::new(),
            schema: "raw".into(),
            table: "orders".into(),
            freshness: Some(SourceFreshnessConfig {
                loaded_at_field: field.into(),
                warn_after: warn.map(Into::into),
                error_after: error.map(Into::into),
                filter: None,
            }),
        }
    }

    fn schemas(cols: &[(&str, RockyType)]) -> HashMap<String, Vec<TypedColumn>> {
        HashMap::from([(
            "raw.orders".to_string(),
            cols.iter()
                .map(|(n, t)| TypedColumn {
                    name: (*n).into(),
                    data_type: t.clone(),
                    nullable: true,
                })
                .collect(),
        )])
    }

    fn codes(d: &[Diagnostic]) -> Vec<&str> {
        d.iter().map(|d| &*d.code).collect()
    }

    #[test]
    fn valid_declaration_is_clean() {
        let d = check_source_freshness(
            &[source("loaded_at", Some("12h"), Some("24h"))],
            &schemas(&[
                ("order_id", RockyType::Int64),
                ("loaded_at", RockyType::Timestamp),
            ]),
        );
        assert!(d.is_empty(), "{d:?}");
        // A DATE column is a valid load time too.
        let d = check_source_freshness(
            &[source("order_date", Some("2d"), None)],
            &schemas(&[("order_date", RockyType::Date)]),
        );
        assert!(d.is_empty(), "{d:?}");
    }

    #[test]
    fn error_after_shorter_than_warn_after_is_e050() {
        let d = check_source_freshness(
            &[source("loaded_at", Some("24h"), Some("12h"))],
            &HashMap::new(),
        );
        assert_eq!(codes(&d), vec![E050]);
        assert!(d[0].is_error());
        assert_eq!(d[0].model, "source:raw.orders");
    }

    #[test]
    fn missing_thresholds_and_bad_duration_are_e050() {
        let d = check_source_freshness(&[source("loaded_at", None, None)], &HashMap::new());
        assert_eq!(codes(&d), vec![E050]);
        let d = check_source_freshness(
            &[source("loaded_at", Some("12 hours"), None)],
            &HashMap::new(),
        );
        assert_eq!(codes(&d), vec![E050]);
    }

    #[test]
    fn non_temporal_loaded_at_field_is_w050() {
        let d = check_source_freshness(
            &[source("status", Some("12h"), None)],
            &schemas(&[("status", RockyType::String)]),
        );
        assert_eq!(codes(&d), vec![W050]);
        assert!(!d[0].is_error());
    }

    #[test]
    fn absent_loaded_at_field_in_a_known_schema_only_warns() {
        // The schema may be a stale seed or cache entry: never refuse.
        let d = check_source_freshness(
            &[source("loaded_at", Some("12h"), None)],
            &schemas(&[("order_id", RockyType::Int64)]),
        );
        assert_eq!(codes(&d), vec![W050]);
    }

    #[test]
    fn unknown_schema_or_unknown_type_is_silent() {
        let d = check_source_freshness(&[source("loaded_at", Some("12h"), None)], &HashMap::new());
        assert!(d.is_empty());
        let d = check_source_freshness(
            &[source("loaded_at", Some("12h"), None)],
            &schemas(&[("loaded_at", RockyType::Unknown)]),
        );
        assert!(d.is_empty());
    }

    #[test]
    fn a_source_without_freshness_is_not_checked() {
        let mut s = source("x", None, None);
        s.freshness = None;
        assert!(check_source_freshness(&[s], &HashMap::new()).is_empty());
    }
}
