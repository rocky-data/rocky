//! Downstream consumers as first-class project records.
//!
//! A *consumer* is something outside the model graph that reads models: a
//! dashboard, a notebook, an ML job, an application. Rocky does not run it.
//! The record exists so the project can answer "who breaks if I change this
//! model?" and so a selection can name a consumer's whole upstream.
//!
//! A project may ship a `consumers/` directory beside its `models/`
//! directory, one TOML file per consumer:
//!
//! ```text
//! consumers/
//! └── weekly_board.toml
//! ```
//!
//! ```toml
//! name = "weekly_board"            # optional; defaults to the file stem
//! kind = "dashboard"               # dashboard | notebook | ml | application | analysis | other
//! owner = "finance-analytics"
//! url = "https://bi.example.com/d/weekly-board"
//! description = "Revenue by region, reviewed every Monday"
//! depends_on = ["fct_orders", "dim_customers"]
//! ```
//!
//! This module owns loading and the shape of the record. The check that
//! every `depends_on` entry names a model lives in `rocky-compiler`
//! (`E059`), because only the compiler knows the model set.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use rocky_sql::validation::validate_identifier;

/// The `consumers/` directory that belongs to a models directory.
///
/// Same convention as `functions/` and `macros/`: a sibling of the models
/// directory. Returns `None` for an empty `models_dir` (a compile over
/// preloaded models with no directory), so a caller never resolves
/// `../consumers` against the process working directory by accident.
#[must_use]
pub fn consumers_dir_for(models_dir: &Path) -> Option<PathBuf> {
    if models_dir.as_os_str().is_empty() {
        return None;
    }
    Some(models_dir.join("../consumers"))
}

/// What kind of thing a consumer is. Informational: nothing in the engine
/// branches on it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConsumerKind {
    /// A BI dashboard or report.
    Dashboard,
    /// A notebook.
    Notebook,
    /// A machine-learning model or pipeline.
    Ml,
    /// An application or service that queries the warehouse.
    Application,
    /// An ad-hoc analysis.
    Analysis,
    /// Anything else.
    #[default]
    Other,
}

impl ConsumerKind {
    /// The spelling used in the TOML file and in command output.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Dashboard => "dashboard",
            Self::Notebook => "notebook",
            Self::Ml => "ml",
            Self::Application => "application",
            Self::Analysis => "analysis",
            Self::Other => "other",
        }
    }

    /// Map a free-form kind (for example a dbt exposure `type`) onto the
    /// closest kind. Anything unrecognised is [`Self::Other`].
    #[must_use]
    pub fn from_label(label: &str) -> Self {
        match label.trim().to_ascii_lowercase().as_str() {
            "dashboard" => Self::Dashboard,
            "notebook" => Self::Notebook,
            "ml" => Self::Ml,
            "application" => Self::Application,
            "analysis" => Self::Analysis,
            _ => Self::Other,
        }
    }
}

/// The on-disk shape of a consumer file.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumerConfig {
    /// Consumer name. Defaults to the file stem.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// What kind of consumer this is.
    #[serde(default)]
    pub kind: ConsumerKind,
    /// Who to ask about it: a team, a person, an email.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    /// Where to find it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// What it is for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Names of the models it reads.
    #[serde(default)]
    pub depends_on: Vec<String>,
}

/// A loaded consumer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Consumer {
    /// Resolved name (`name` from the file, else the file stem).
    pub name: String,
    /// What kind of consumer this is.
    pub kind: ConsumerKind,
    /// Who to ask about it.
    pub owner: Option<String>,
    /// Where to find it.
    pub url: Option<String>,
    /// What it is for.
    pub description: Option<String>,
    /// Names of the models it reads, sorted and deduplicated.
    pub depends_on: Vec<String>,
    /// The file this consumer was read from.
    pub file_path: PathBuf,
}

/// A consumer file that could not be loaded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerLoadError {
    /// The consumer's name, or the file stem when the name is unknown.
    pub name: String,
    /// The offending file (or directory).
    pub file_path: PathBuf,
    /// What is wrong.
    pub message: String,
}

/// Everything found in a `consumers/` directory.
#[derive(Debug, Clone, Default)]
pub struct LoadedConsumers {
    /// Consumers that parsed, sorted by file path.
    pub consumers: Vec<Consumer>,
    /// Files that did not.
    pub errors: Vec<ConsumerLoadError>,
}

/// Load every `*.toml` file under `dir` (subdirectories included).
///
/// A missing directory is an empty result. Malformed files are returned in
/// [`LoadedConsumers::errors`] rather than failing the scan, so the compiler
/// can report each one as a diagnostic.
#[must_use]
pub fn load_consumers_from_dir(dir: &Path) -> LoadedConsumers {
    let mut loaded = LoadedConsumers::default();
    if !dir.is_dir() {
        return loaded;
    }
    let (dirs, walk_errors) = crate::model_walk::walk_model_dirs(dir);
    for err in walk_errors {
        loaded.errors.push(ConsumerLoadError {
            name: dir.display().to_string(),
            file_path: dir.to_path_buf(),
            message: err.to_string(),
        });
    }
    for sub in dirs {
        let mut tomls: Vec<PathBuf> = match std::fs::read_dir(&sub) {
            Ok(entries) => entries
                .filter_map(Result::ok)
                .map(|e| e.path())
                .filter(|p| p.is_file() && p.extension().is_some_and(|e| e == "toml"))
                .collect(),
            Err(e) => {
                loaded.errors.push(ConsumerLoadError {
                    name: sub.display().to_string(),
                    file_path: sub.clone(),
                    message: format!("failed to read consumers directory: {e}"),
                });
                continue;
            }
        };
        tomls.sort();
        for toml_path in tomls {
            match load_consumer(&toml_path) {
                Ok(consumer) => loaded.consumers.push(consumer),
                Err(err) => loaded.errors.push(err),
            }
        }
    }
    loaded
}

/// Load the consumers that belong to `models_dir`, or none when the project
/// has no `consumers/` directory.
#[must_use]
pub fn load_consumers_for_models_dir(models_dir: &Path) -> LoadedConsumers {
    consumers_dir_for(models_dir)
        .map(|dir| load_consumers_from_dir(&dir))
        .unwrap_or_default()
}

fn load_consumer(toml_path: &Path) -> Result<Consumer, ConsumerLoadError> {
    let stem = toml_path
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    let fail = |message: String| ConsumerLoadError {
        name: stem.clone(),
        file_path: toml_path.to_path_buf(),
        message,
    };
    let text = std::fs::read_to_string(toml_path)
        .map_err(|e| fail(format!("failed to read {}: {e}", toml_path.display())))?;
    let config: ConsumerConfig = toml::from_str(&text).map_err(|e| {
        fail(format!(
            "invalid consumer file {}: {e}",
            toml_path.display()
        ))
    })?;
    let name = config.name.clone().unwrap_or_else(|| stem.clone());
    if validate_identifier(&name).is_err() {
        return Err(ConsumerLoadError {
            name: name.clone(),
            file_path: toml_path.to_path_buf(),
            message: format!("consumer name `{name}` must match [A-Za-z0-9_]+"),
        });
    }
    let mut depends_on = config.depends_on;
    depends_on.sort();
    depends_on.dedup();
    Ok(Consumer {
        name,
        kind: config.kind,
        owner: config.owner.filter(|s| !s.trim().is_empty()),
        url: config.url.filter(|s| !s.trim().is_empty()),
        description: config.description.filter(|s| !s.trim().is_empty()),
        depends_on,
        file_path: toml_path.to_path_buf(),
    })
}

impl Consumer {
    /// Render the consumer as the TOML a `consumers/<name>.toml` file holds.
    ///
    /// Every string goes through the TOML serializer, so a value with a quote
    /// or a newline cannot break the file or add keys.
    #[must_use]
    pub fn to_toml(&self) -> String {
        let config = ConsumerConfig {
            name: Some(self.name.clone()),
            kind: self.kind,
            owner: self.owner.clone(),
            url: self.url.clone(),
            description: self.description.clone(),
            depends_on: self.depends_on.clone(),
        };
        toml::to_string(&config).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(dir: &Path, file: &str, body: &str) {
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(dir.join(file), body).unwrap();
    }

    #[test]
    fn dir_is_the_models_sibling_and_absent_for_an_empty_models_dir() {
        assert_eq!(
            consumers_dir_for(Path::new("proj/models")),
            Some(PathBuf::from("proj/models/../consumers"))
        );
        assert_eq!(consumers_dir_for(Path::new("")), None);
    }

    #[test]
    fn loads_a_full_consumer_and_defaults_the_name_to_the_stem() {
        let tmp = tempfile::tempdir().unwrap();
        write(
            tmp.path(),
            "weekly_board.toml",
            "kind = \"dashboard\"\nowner = \"finance\"\nurl = \"https://bi/x\"\n\
             description = \"d\"\ndepends_on = [\"b\", \"a\", \"a\"]\n",
        );
        let loaded = load_consumers_from_dir(tmp.path());
        assert!(loaded.errors.is_empty(), "{:?}", loaded.errors);
        let c = &loaded.consumers[0];
        assert_eq!(c.name, "weekly_board");
        assert_eq!(c.kind, ConsumerKind::Dashboard);
        assert_eq!(c.owner.as_deref(), Some("finance"));
        assert_eq!(c.depends_on, vec!["a", "b"]);
    }

    #[test]
    fn a_missing_directory_is_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let loaded = load_consumers_from_dir(&tmp.path().join("nope"));
        assert!(loaded.consumers.is_empty() && loaded.errors.is_empty());
    }

    #[test]
    fn bad_files_are_reported_not_skipped() {
        let tmp = tempfile::tempdir().unwrap();
        write(tmp.path(), "typo.toml", "ownr = \"x\"\n");
        write(tmp.path(), "badkind.toml", "kind = \"spreadsheet\"\n");
        write(tmp.path(), "bad name.toml", "depends_on = []\n");
        let loaded = load_consumers_from_dir(tmp.path());
        assert!(loaded.consumers.is_empty());
        assert_eq!(loaded.errors.len(), 3, "{:?}", loaded.errors);
    }

    #[test]
    fn to_toml_round_trips_awkward_strings() {
        let tmp = tempfile::tempdir().unwrap();
        let consumer = Consumer {
            name: "board".into(),
            kind: ConsumerKind::Ml,
            owner: Some("a \"quoted\"\nowner".into()),
            url: None,
            description: Some("line one\nline two".into()),
            depends_on: vec!["m".into()],
            file_path: PathBuf::new(),
        };
        write(tmp.path(), "board.toml", &consumer.to_toml());
        let loaded = load_consumers_from_dir(tmp.path());
        assert!(loaded.errors.is_empty(), "{:?}", loaded.errors);
        let got = &loaded.consumers[0];
        assert_eq!(got.owner, consumer.owner);
        assert_eq!(got.description, consumer.description);
        assert_eq!(got.kind, ConsumerKind::Ml);
    }

    #[test]
    fn kind_labels_map_and_unknown_is_other() {
        assert_eq!(
            ConsumerKind::from_label("Dashboard"),
            ConsumerKind::Dashboard
        );
        assert_eq!(ConsumerKind::from_label("ml"), ConsumerKind::Ml);
        assert_eq!(ConsumerKind::from_label("report"), ConsumerKind::Other);
    }
}
