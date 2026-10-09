//! Downstream-consumer records in the compile: load `consumers/`, check every
//! `depends_on` entry names a model (`E059`), and hand the records to the
//! project graph.
//!
//! The record and its loader live in [`rocky_core::consumers`]. This module
//! owns the part that needs the model set.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use rocky_core::consumers::{Consumer, ConsumerLoadError, LoadedConsumers};

use crate::diagnostic::{Diagnostic, E059, SourceSpan};

/// Load the `consumers/` directory beside `models_dir` and check it against
/// the project's models.
///
/// Returns the consumers that loaded, and one `E059` error per problem:
/// a file that does not parse, a name used twice, or a `depends_on` entry
/// that names no model. A consumer with an unknown `depends_on` entry stays
/// in the returned list without that entry, so the rest of its edges still
/// show in lineage and docs while the compile fails.
#[must_use]
pub fn load_and_check(
    models_dir: &Path,
    model_names: &BTreeSet<&str>,
) -> (Vec<Consumer>, Vec<Diagnostic>) {
    check(
        &rocky_core::consumers::load_consumers_for_models_dir(models_dir),
        model_names,
    )
}

/// Check loaded consumers against the model names. See [`load_and_check`].
#[must_use]
pub fn check(
    loaded: &LoadedConsumers,
    model_names: &BTreeSet<&str>,
) -> (Vec<Consumer>, Vec<Diagnostic>) {
    let mut diagnostics: Vec<Diagnostic> = loaded.errors.iter().map(load_error).collect();

    // Two consumers with one name: nothing says which one a selector or a
    // lineage view means, so every holder is refused.
    let mut holders: BTreeMap<&str, usize> = BTreeMap::new();
    for consumer in &loaded.consumers {
        *holders.entry(consumer.name.as_str()).or_default() += 1;
    }
    let mut consumers = Vec::new();
    for consumer in &loaded.consumers {
        if holders[consumer.name.as_str()] > 1 {
            diagnostics.push(
                Diagnostic::error(
                    E059,
                    &subject(&consumer.name),
                    format!(
                        "consumer `{}` is declared more than once; consumer names must be unique",
                        consumer.name
                    ),
                )
                .with_span(span(&consumer.file_path, None))
                .with_suggestion("rename one of the files, or set a distinct `name`"),
            );
            continue;
        }
        let mut kept = consumer.clone();
        kept.depends_on.clear();
        for dep in &consumer.depends_on {
            if model_names.contains(dep.as_str()) {
                kept.depends_on.push(dep.clone());
                continue;
            }
            let mut diagnostic = Diagnostic::error(
                E059,
                &subject(&consumer.name),
                format!(
                    "consumer `{}` depends on `{dep}`, which is not a model in this project",
                    consumer.name
                ),
            )
            .with_span(span(&consumer.file_path, Some(dep)));
            diagnostic = match nearest(dep, model_names) {
                Some(near) => diagnostic.with_suggestion(format!("did you mean `{near}`?")),
                None => diagnostic.with_suggestion(
                    "`depends_on` takes model names; a source table or a seed is not a model",
                ),
            };
            diagnostics.push(diagnostic);
        }
        consumers.push(kept);
    }
    (consumers, diagnostics)
}

/// The `model` field of a consumer diagnostic. A consumer is not a model, and
/// `rocky run` excludes a model from execution when an error diagnostic is
/// keyed on its name, so a consumer called `orders` must not read as the model
/// `orders`. `:` cannot appear in either name, so the keys never collide.
fn subject(consumer: &str) -> String {
    format!("consumer:{consumer}")
}

fn load_error(error: &ConsumerLoadError) -> Diagnostic {
    Diagnostic::error(E059, &subject(&error.name), error.message.clone())
        .with_span(span(&error.file_path, None))
}

/// Point at the line that names `needle` (quoted), else the top of the file.
fn span(file: &Path, needle: Option<&str>) -> SourceSpan {
    let line = needle
        .and_then(|name| {
            let text = std::fs::read_to_string(file).ok()?;
            let quoted = format!("\"{name}\"");
            text.lines()
                .position(|l| l.contains(&quoted))
                .map(|i| i + 1)
        })
        .unwrap_or(1);
    SourceSpan {
        file: file.display().to_string(),
        line,
        col: 1,
    }
}

fn nearest<'a>(name: &str, candidates: &BTreeSet<&'a str>) -> Option<&'a str> {
    let wanted = name.to_ascii_lowercase();
    let limit = (wanted.chars().count() / 3).clamp(1, 3);
    candidates
        .iter()
        .map(|c| (strsim::levenshtein(&wanted, &c.to_ascii_lowercase()), *c))
        .filter(|(distance, _)| *distance <= limit)
        .min()
        .map(|(_, c)| c)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn consumer(name: &str, deps: &[&str]) -> Consumer {
        Consumer {
            name: name.into(),
            kind: rocky_core::consumers::ConsumerKind::Dashboard,
            owner: None,
            url: None,
            description: None,
            depends_on: deps.iter().map(|d| (*d).to_string()).collect(),
            file_path: PathBuf::from(format!("consumers/{name}.toml")),
        }
    }

    fn models() -> BTreeSet<&'static str> {
        ["fct_orders", "dim_customers"].into_iter().collect()
    }

    #[test]
    fn known_models_are_clean() {
        let loaded = LoadedConsumers {
            consumers: vec![consumer("board", &["fct_orders", "dim_customers"])],
            errors: vec![],
        };
        let (consumers, diagnostics) = check(&loaded, &models());
        assert!(diagnostics.is_empty(), "{diagnostics:?}");
        assert_eq!(consumers[0].depends_on.len(), 2);
    }

    #[test]
    fn an_unknown_model_is_an_error_with_a_near_miss_hint() {
        let loaded = LoadedConsumers {
            consumers: vec![consumer("board", &["fct_order", "fct_orders"])],
            errors: vec![],
        };
        let (consumers, diagnostics) = check(&loaded, &models());
        assert_eq!(diagnostics.len(), 1);
        let d = &diagnostics[0];
        assert!(d.is_error());
        assert_eq!(&*d.code, "E059");
        // Keyed so it can never be mistaken for a model of the same name.
        assert_eq!(&*d.model, "consumer:board");
        assert!(d.message.contains("`fct_order`"), "{}", d.message);
        assert_eq!(d.suggestion.as_deref(), Some("did you mean `fct_orders`?"));
        // The valid edge survives so lineage still shows it.
        assert_eq!(consumers[0].depends_on, vec!["fct_orders"]);
    }

    #[test]
    fn a_duplicate_name_refuses_every_holder() {
        let loaded = LoadedConsumers {
            consumers: vec![
                consumer("board", &["fct_orders"]),
                consumer("board", &["dim_customers"]),
            ],
            errors: vec![],
        };
        let (consumers, diagnostics) = check(&loaded, &models());
        assert!(consumers.is_empty());
        assert_eq!(diagnostics.len(), 2);
        assert!(diagnostics.iter().all(Diagnostic::is_error));
    }

    #[test]
    fn a_file_that_does_not_load_is_an_error() {
        let loaded = LoadedConsumers {
            consumers: vec![],
            errors: vec![ConsumerLoadError {
                name: "typo".into(),
                file_path: PathBuf::from("consumers/typo.toml"),
                message: "invalid consumer file".into(),
            }],
        };
        let (_, diagnostics) = check(&loaded, &models());
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(&*diagnostics[0].code, "E059");
    }
}
