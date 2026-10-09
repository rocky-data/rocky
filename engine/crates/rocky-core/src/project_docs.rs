//! Project metadata for the documentation site and the metadata export.
//!
//! [`ProjectDocs`] wraps the [`DocIndex`] that `rocky docs` has always built
//! and adds what a navigable site needs on top of it: per-model detail
//! (source file, SQL, tags, column classifications, freshness, contract),
//! column-level lineage edges, and the external tables the models read.
//!
//! The type holds plain data. The CLI fills it from the compiler's output
//! (types, lineage, contracts); this module never re-infers anything. Both
//! the static site ([`crate::docs_site`]) and the Parquet export read it, so
//! the two always describe the same project.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::docs::DocIndex;
use crate::models::Model;
use crate::tests::{TestDecl, test_type_kind};

/// One column-level lineage edge: `source` feeds `target`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocColumnEdge {
    /// Upstream model or external table name.
    pub source_model: String,
    /// Upstream column.
    pub source_column: String,
    /// Downstream model.
    pub target_model: String,
    /// Downstream column.
    pub target_column: String,
    /// How the value is derived (`direct`, `cast`, `sum`, `expression`, ...).
    pub transform: String,
}

/// An external table that models read but the project does not define.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocSource {
    /// Table name as it appears in the model SQL.
    pub name: String,
    /// Columns of this table that models read (from column lineage).
    pub columns: Vec<String>,
    /// Models that read this table.
    pub used_by: Vec<String>,
}

/// Freshness expectation declared on (or inherited by) a model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocFreshness {
    /// Maximum lag in seconds before the model is stale.
    pub max_lag_seconds: u64,
    /// Timestamp column the check reads, when set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_column: Option<String>,
    /// `warning` or `error`, when set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub severity: Option<String>,
}

/// One column constraint of a contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocContractColumn {
    /// Column name.
    pub name: String,
    /// Expected type name, when constrained.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub type_name: Option<String>,
    /// Expected nullability, when constrained.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nullable: Option<bool>,
    /// Description from the contract file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// A model's contract, flattened for display.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocContract {
    /// Column constraints.
    pub columns: Vec<DocContractColumn>,
    /// Columns that must exist.
    pub required: Vec<String>,
    /// Columns that must never be removed.
    pub protected: Vec<String>,
    /// Whether new nullable columns are refused.
    pub no_new_nullable: bool,
}

/// A declarative test, flattened for display and export.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocTest {
    /// Test kind (`not_null`, `unique`, `accepted_values`, ...).
    pub kind: String,
    /// Column under test, when the test is column-scoped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
    /// `error` or `warning`.
    pub severity: String,
    /// Type-specific parameters as compact JSON (`{}` when there are none).
    pub params: String,
    /// Row filter, when set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
}

impl DocTest {
    /// Flatten a declared test.
    pub fn from_decl(decl: &TestDecl) -> Self {
        let mut params = serde_json::to_value(&decl.test_type)
            .unwrap_or_else(|_| serde_json::Value::Object(serde_json::Map::new()));
        if let serde_json::Value::Object(map) = &mut params {
            map.remove("type");
        }
        Self {
            kind: test_type_kind(&decl.test_type).to_string(),
            column: decl.column.clone(),
            severity: format!("{:?}", decl.severity).to_lowercase(),
            params: params.to_string(),
            filter: decl.filter.clone(),
        }
    }
}

/// Per-model detail that [`crate::docs::DocModel`] does not carry.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelDetail {
    /// Source file, relative to the models directory.
    pub file: String,
    /// Authored SQL (or the compiled SQL for `.rocky` models).
    pub sql: String,
    /// Free-form tags.
    pub tags: BTreeMap<String, String>,
    /// Column name to classification tag (for example `email` to `pii`).
    pub classification: BTreeMap<String, String>,
    /// Freshness expectation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub freshness: Option<DocFreshness>,
    /// Contract, when the model has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contract: Option<DocContract>,
    /// Tables declared in the model's `sources` list (`catalog.schema.table`).
    pub declared_sources: Vec<String>,
}

/// A downstream consumer (dashboard, notebook, ML job, application) that
/// reads models, flattened for display and export.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocConsumer {
    /// Consumer name.
    pub name: String,
    /// `dashboard`, `notebook`, `ml`, `application`, `analysis` or `other`.
    pub kind: String,
    /// Who to ask about it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    /// Where to find it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// What it is for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Models it reads, sorted.
    pub depends_on: Vec<String>,
}

impl DocConsumer {
    /// Flatten a loaded consumer.
    pub fn from_consumer(consumer: &crate::consumers::Consumer) -> Self {
        Self {
            name: consumer.name.clone(),
            kind: consumer.kind.as_str().to_string(),
            owner: consumer.owner.clone(),
            url: consumer.url.clone(),
            description: consumer.description.clone(),
            depends_on: consumer.depends_on.clone(),
        }
    }
}

/// Everything the documentation site and the metadata export describe.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectDocs {
    /// Models, counts and per-column types, as in the single-page catalog.
    pub index: DocIndex,
    /// Per-model detail, keyed by model name.
    pub details: BTreeMap<String, ModelDetail>,
    /// External tables the models read.
    pub sources: Vec<DocSource>,
    /// Column-level lineage edges, sorted.
    pub column_lineage: Vec<DocColumnEdge>,
    /// Downstream consumers of the documented models, sorted by name. Each
    /// lists only the documented models it reads.
    #[serde(default)]
    pub consumers: Vec<DocConsumer>,
    /// Whether the compile step supplied column types. When `false`, column
    /// tables are empty because the project did not compile.
    pub schema_available: bool,
}

impl ProjectDocs {
    /// Assemble the project view.
    ///
    /// `models` supplies file, SQL, tags, classification, freshness and
    /// declared sources. `contracts` is keyed by model name. `lineage` holds
    /// the compiler's column edges; pass an empty list when the project did
    /// not compile. `models_dir` makes file paths relative.
    pub fn build(
        index: DocIndex,
        models: &[Model],
        models_dir: &Path,
        contracts: &HashMap<String, DocContract>,
        mut lineage: Vec<DocColumnEdge>,
        schema_available: bool,
    ) -> Self {
        let mut details = BTreeMap::new();
        for model in models {
            let file = model
                .file_path
                .strip_prefix(models_dir)
                .unwrap_or(&model.file_path)
                .to_string_lossy()
                .replace('\\', "/");
            let freshness = model.config.freshness.as_ref().map(|f| DocFreshness {
                max_lag_seconds: f.max_lag_seconds,
                time_column: f.time_column.clone(),
                severity: f.severity.map(|s| format!("{s:?}").to_lowercase()),
            });
            details.insert(
                model.config.name.clone(),
                ModelDetail {
                    file,
                    sql: model.sql.clone(),
                    tags: model.config.tags.clone(),
                    classification: model.config.classification.clone(),
                    freshness,
                    contract: contracts.get(&model.config.name).cloned(),
                    declared_sources: model
                        .config
                        .sources
                        .iter()
                        .map(|s| format!("{}.{}.{}", s.catalog, s.schema, s.table))
                        .collect(),
                },
            );
        }

        // SQL can name a model by its physical target (`catalog.schema.table`
        // or `schema.table`) instead of its bare name. Fold those spellings
        // onto the model so it is not reported as an external source. A
        // `schema.table` spelling is used only when exactly one model has it.
        let mut aliases: HashMap<String, Option<String>> = HashMap::new();
        for m in &index.models {
            let mut parts = m.target.splitn(2, '.');
            let _catalog = parts.next();
            let short = parts.next().unwrap_or_default().to_string();
            for key in [m.target.clone(), short] {
                aliases
                    .entry(key)
                    .and_modify(|v| *v = None)
                    .or_insert_with(|| Some(m.name.clone()));
            }
        }
        let model_names: BTreeSet<&str> = index.models.iter().map(|m| m.name.as_str()).collect();
        let canonical = |name: &str| -> String {
            if model_names.contains(name) {
                return name.to_string();
            }
            aliases
                .get(name)
                .cloned()
                .flatten()
                .unwrap_or_else(|| name.to_string())
        };
        for edge in &mut lineage {
            edge.source_model = canonical(&edge.source_model);
        }
        for detail in details.values_mut() {
            for declared in &mut detail.declared_sources {
                *declared = canonical(declared);
            }
        }

        lineage.sort_by(|a, b| {
            (
                &a.target_model,
                &a.target_column,
                &a.source_model,
                &a.source_column,
            )
                .cmp(&(
                    &b.target_model,
                    &b.target_column,
                    &b.source_model,
                    &b.source_column,
                ))
        });
        lineage.dedup();

        let sources = derive_sources(&index, &details, &lineage);
        Self {
            index,
            details,
            sources,
            column_lineage: lineage,
            consumers: Vec::new(),
            schema_available,
        }
    }

    /// Attach downstream consumers. Each keeps only the entries of
    /// `depends_on` that name a documented model, and one left reading no
    /// documented model is dropped, so a selection never lists a consumer of
    /// models the page does not describe.
    #[must_use]
    pub fn with_consumers(mut self, consumers: &[crate::consumers::Consumer]) -> Self {
        let documented: BTreeSet<&str> =
            self.index.models.iter().map(|m| m.name.as_str()).collect();
        let mut kept: Vec<DocConsumer> = consumers
            .iter()
            .map(DocConsumer::from_consumer)
            .map(|mut c| {
                c.depends_on.retain(|m| documented.contains(m.as_str()));
                c
            })
            .filter(|c| !c.depends_on.is_empty())
            .collect();
        kept.sort_by(|a, b| a.name.cmp(&b.name));
        self.consumers = kept;
        self
    }

    /// Model-level edges `(upstream, downstream)`. The upstream is a model
    /// or an external table; sorted and deduplicated.
    pub fn model_edges(&self) -> Vec<(String, String)> {
        let mut edges = BTreeSet::new();
        for model in &self.index.models {
            for dep in &model.depends_on {
                edges.insert((dep.clone(), model.name.clone()));
            }
        }
        let model_names: BTreeSet<&str> =
            self.index.models.iter().map(|m| m.name.as_str()).collect();
        for edge in &self.column_lineage {
            if model_names.contains(edge.target_model.as_str()) {
                edges.insert((edge.source_model.clone(), edge.target_model.clone()));
            }
        }
        edges.into_iter().collect()
    }
}

/// External tables: lineage sources that are not project models, plus tables
/// models declare in `sources`.
fn derive_sources(
    index: &DocIndex,
    details: &BTreeMap<String, ModelDetail>,
    lineage: &[DocColumnEdge],
) -> Vec<DocSource> {
    let model_names: BTreeSet<&str> = index.models.iter().map(|m| m.name.as_str()).collect();
    let mut map: BTreeMap<String, (BTreeSet<String>, BTreeSet<String>)> = BTreeMap::new();
    for edge in lineage {
        if model_names.contains(edge.source_model.as_str()) {
            continue;
        }
        let entry = map.entry(edge.source_model.clone()).or_default();
        entry.0.insert(edge.source_column.clone());
        entry.1.insert(edge.target_model.clone());
    }
    for (model, detail) in details {
        for declared in &detail.declared_sources {
            if !model_names.contains(declared.as_str()) {
                map.entry(declared.clone())
                    .or_default()
                    .1
                    .insert(model.clone());
            }
        }
    }
    map.into_iter()
        .map(|(name, (columns, used_by))| DocSource {
            name,
            columns: columns.into_iter().collect(),
            used_by: used_by.into_iter().collect(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::docs::{DocIndex, DocModel};
    use crate::tests::{TestDecl, TestSeverity, TestType};

    fn model(name: &str, deps: &[&str]) -> DocModel {
        DocModel {
            name: name.into(),
            description: None,
            target: format!("c.s.{name}"),
            strategy: "full_refresh".into(),
            depends_on: deps.iter().map(|d| (*d).to_string()).collect(),
            columns: vec![],
            tests: vec![],
            governance: None,
        }
    }

    fn edge(sm: &str, sc: &str, tm: &str, tc: &str) -> DocColumnEdge {
        DocColumnEdge {
            source_model: sm.into(),
            source_column: sc.into(),
            target_model: tm.into(),
            target_column: tc.into(),
            transform: "direct".into(),
        }
    }

    #[test]
    fn consumers_keep_only_documented_models_and_drop_when_none_remain() {
        use crate::consumers::{Consumer, ConsumerKind};
        let consumer = |name: &str, deps: &[&str]| Consumer {
            name: name.into(),
            kind: ConsumerKind::Dashboard,
            owner: None,
            url: None,
            description: None,
            depends_on: deps.iter().map(|d| (*d).to_string()).collect(),
            file_path: std::path::PathBuf::new(),
        };
        let index = DocIndex {
            models: vec![model("a", &[])],
            pipeline_count: 0,
            adapter_count: 0,
        };
        let docs = ProjectDocs::build(
            index,
            &[],
            Path::new("models"),
            &HashMap::new(),
            vec![],
            true,
        )
        .with_consumers(&[
            consumer("zeta", &["a", "unselected"]),
            consumer("alpha", &["a"]),
            consumer("elsewhere", &["unselected"]),
        ]);
        let names: Vec<&str> = docs.consumers.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["alpha", "zeta"]);
        assert_eq!(docs.consumers[1].depends_on, vec!["a"]);
    }

    #[test]
    fn external_lineage_sources_become_sources() {
        let index = DocIndex {
            models: vec![model("a", &[]), model("b", &["a"])],
            pipeline_count: 0,
            adapter_count: 0,
        };
        let docs = ProjectDocs::build(
            index,
            &[],
            Path::new("models"),
            &HashMap::new(),
            vec![
                edge("raw.orders", "id", "a", "id"),
                edge("raw.orders", "total", "a", "total"),
                edge("a", "id", "b", "id"),
            ],
            true,
        );
        assert_eq!(docs.sources.len(), 1);
        assert_eq!(docs.sources[0].name, "raw.orders");
        assert_eq!(docs.sources[0].columns, vec!["id", "total"]);
        assert_eq!(docs.sources[0].used_by, vec!["a"]);
        let edges = docs.model_edges();
        assert!(edges.contains(&("a".into(), "b".into())));
        assert!(edges.contains(&("raw.orders".into(), "a".into())));
    }

    #[test]
    fn qualified_model_names_fold_onto_the_model() {
        let index = DocIndex {
            models: vec![model("a", &[]), model("b", &[])],
            pipeline_count: 0,
            adapter_count: 0,
        };
        let docs = ProjectDocs::build(
            index,
            &[],
            Path::new("models"),
            &HashMap::new(),
            vec![
                edge("c.s.a", "id", "b", "id"),
                edge("s.a", "id", "b", "id2"),
                edge("other.t", "id", "b", "id3"),
            ],
            true,
        );
        let names: Vec<_> = docs
            .column_lineage
            .iter()
            .map(|e| e.source_model.as_str())
            .collect();
        assert_eq!(names, vec!["a", "a", "other.t"]);
        assert_eq!(docs.sources.len(), 1);
        assert_eq!(docs.sources[0].name, "other.t");
    }

    #[test]
    fn ambiguous_short_name_is_not_folded() {
        let mut a = model("a", &[]);
        a.target = "c1.s.t".into();
        let mut b = model("b", &[]);
        b.target = "c2.s.t".into();
        let docs = ProjectDocs::build(
            DocIndex {
                models: vec![a, b],
                pipeline_count: 0,
                adapter_count: 0,
            },
            &[],
            Path::new("models"),
            &HashMap::new(),
            vec![edge("s.t", "id", "b", "id")],
            true,
        );
        assert_eq!(docs.column_lineage[0].source_model, "s.t");
    }

    #[test]
    fn doc_test_flattens_params_without_the_type_tag() {
        let decl = TestDecl {
            test_type: TestType::AcceptedValues {
                values: vec!["x".into()],
            },
            column: Some("status".into()),
            severity: TestSeverity::Warning,
            filter: None,
        };
        let flat = DocTest::from_decl(&decl);
        assert_eq!(flat.kind, "accepted_values");
        assert_eq!(flat.severity, "warning");
        assert_eq!(flat.params, r#"{"values":["x"]}"#);
        let not_null = DocTest::from_decl(&TestDecl {
            test_type: TestType::NotNull,
            column: Some("id".into()),
            severity: TestSeverity::Error,
            filter: None,
        });
        assert_eq!(not_null.params, "{}");
    }
}
