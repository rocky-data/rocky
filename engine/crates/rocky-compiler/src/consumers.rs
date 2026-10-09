//! Downstream-consumer records in the compile: load `consumers/`, check every
//! `depends_on` entry names a model (`E060`), and hand the records to the
//! project graph.
//!
//! The record and its loader live in [`rocky_core::consumers`]. This module
//! owns the part that needs the model set.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use rocky_core::consumers::{Consumer, ConsumerLoadError, LoadedConsumers};

use rocky_core::models::Model;

use crate::compile::CompilerConfig;
use crate::diagnostic::{Diagnostic, E060, SourceSpan};

/// Load and check the consumers of a compile.
///
/// With [`CompilerConfig::project`] set, the directory is the project root's
/// `consumers/` and a `depends_on` entry is valid when it names a model
/// anywhere in the project, not only one of the `models` this compile holds.
/// Without it, this is [`load_and_check`] over the compiled models.
#[must_use]
pub fn load_and_check_project(
    config: &CompilerConfig,
    models: &[Model],
) -> (Vec<Consumer>, Vec<Diagnostic>) {
    let mut names: BTreeSet<&str> = models.iter().map(|m| m.config.name.as_str()).collect();
    match &config.project {
        Some(project) => {
            names.extend(project.model_names.iter().map(String::as_str));
            check(
                &rocky_core::consumers::load_consumers_for_root(&project.root),
                &names,
            )
        }
        None => load_and_check(&config.models_dir, &names),
    }
}

/// The project a compile belongs to, for a command that holds the project's
/// `rocky.toml`.
///
/// `root` is the directory of the config file. The model names come from the
/// same loader `rocky dag` and `rocky run --dag` use, so a consumer is judged
/// against every transformation pipeline's models. They are read only when a
/// `consumers/` directory exists: a project with no consumers pays nothing,
/// and a loader failure leaves the set empty (the models themselves report
/// that failure).
#[must_use]
pub fn project_context(
    config_path: &Path,
    config: &rocky_core::config::RockyConfig,
) -> crate::compile::ProjectContext {
    let root = config_path
        .parent()
        .map_or_else(|| Path::new(".").to_path_buf(), Path::to_path_buf);
    let model_names = if rocky_core::consumers::consumers_dir_for_root(&root).exists() {
        match crate::models_loader::whole_project_models(config_path, config) {
            Ok(Some(models)) => model_name_set(&models),
            Ok(None) | Err(_) => BTreeSet::new(),
        }
    } else {
        BTreeSet::new()
    };
    crate::compile::ProjectContext { root, model_names }
}

/// Check a project's `consumers/` on its own, without compiling any model.
///
/// For a command that runs the project's models in many compiles (`rocky run
/// --dag`) and wants the consumer problems once.
#[must_use]
pub fn diagnose_project(project: &crate::compile::ProjectContext) -> Vec<Diagnostic> {
    let names: BTreeSet<&str> = project.model_names.iter().map(String::as_str).collect();
    check(
        &rocky_core::consumers::load_consumers_for_root(&project.root),
        &names,
    )
    .1
}

/// The names of `models`, as a [`ProjectContext`](crate::compile::ProjectContext)
/// holds them.
#[must_use]
pub fn model_name_set(models: &[Model]) -> BTreeSet<String> {
    models.iter().map(|m| m.config.name.clone()).collect()
}

/// Load the `consumers/` directory beside `models_dir` and check it against
/// the project's models.
///
/// Returns the consumers that loaded, and one `E060` error per problem:
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
                    E060,
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
                E060,
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
    format!("{SUBJECT_PREFIX}{consumer}")
}

const SUBJECT_PREFIX: &str = "consumer:";

/// Whether a diagnostic's `model` field names a consumer rather than a model.
///
/// Readers that count failures per model (`rocky run`) must use this instead
/// of a hand-written prefix test, so the writer's keying and the reader's
/// classification cannot drift apart.
#[must_use]
pub fn is_consumer_subject(model: &str) -> bool {
    model.starts_with(SUBJECT_PREFIX)
}

/// Whether the compile has an error that is not about a consumer record.
///
/// For a command that uses the compiled models and has no reason to stop for
/// a wrong dashboard record (`rocky docs`, `rocky emit-sql`). `rocky compile`
/// and `rocky ci` use `has_errors`, which includes the consumer errors.
#[must_use]
pub fn has_model_errors(result: &crate::compile::CompileResult) -> bool {
    result
        .diagnostics
        .iter()
        .any(|d| d.is_error() && !is_consumer_diagnostic(d))
}

/// Whether a diagnostic is about a consumer record.
#[must_use]
pub fn is_consumer_diagnostic(diagnostic: &Diagnostic) -> bool {
    &*diagnostic.code == E060 && is_consumer_subject(&diagnostic.model)
}

fn load_error(error: &ConsumerLoadError) -> Diagnostic {
    Diagnostic::error(E060, &subject(&error.name), error.message.clone())
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
        assert_eq!(&*d.code, "E060");
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
        assert_eq!(&*diagnostics[0].code, "E060");
    }
}
