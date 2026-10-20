//! Data residency for AI requests (V062).
//!
//! Every AI request names the warehouse data it carries before
//! [`LlmClient::new`](crate::client::LlmClient::new) builds a client, and the
//! client refuses before any request is sent when that data may not go to the
//! `[ai]` endpoint's region:
//!
//! ```text
//!   caller ──(AiResidency: data + regions)──▶ LlmClient::new
//!                                              │ check_ai_residency
//!                                              ├─ refused ─▶ AiError::Residency (no request)
//!                                              └─ ok ──────▶ client ─▶ POST /v1/messages
//! ```
//!
//! What counts as data: the schema (column names and types), statistics or
//! values of a table. The SQL or config text a user wrote is not data here.
//! A model's data is in the region of the target of each pipeline that loads
//! it ([`ModelRegions`]). A table that is not a project model takes the
//! target and source regions of each replication, snapshot or load pipeline
//! that writes it, and the source region of each replication or snapshot
//! pipeline that reads it as its source ([`ModelRegions::for_table`]). Any
//! other table has no known region.

use std::path::Path;

use rocky_compiler::compile::CompileResult;
use rocky_compiler::model_targets::ModelRegions;
use rocky_core::config::{AiDataRegion, ConfigError, RockyConfig, check_ai_residency};

/// The residency verdict for one AI client: what its requests carry, judged
/// against `[ai] region` and `[residency]`.
#[derive(Debug)]
pub struct AiResidency {
    verdict: Result<(), ConfigError>,
}

impl AiResidency {
    /// The requests carry no warehouse data: only SQL, DSL or config text the
    /// user wrote, and Rocky diagnostics about it.
    pub fn no_warehouse_data() -> Self {
        Self { verdict: Ok(()) }
    }

    /// The requests carry `data`; judge it against the project `config`
    /// (`None` when there is no `rocky.toml`).
    pub fn for_data(config: Option<&RockyConfig>, data: &[AiDataRegion]) -> Self {
        Self {
            verdict: check_ai_residency(config, data),
        }
    }

    /// The refusal, if any. Consumed by `LlmClient::new`.
    pub(crate) fn into_verdict(self) -> Result<(), ConfigError> {
        self.verdict
    }
}

/// The data items of the project `models` and of `tables` that are not
/// project models.
///
/// Each model contributes one item per region of the pipelines that load it
/// (see [`ModelRegions`]). Each other table contributes one item per region
/// of the pipelines that write it or read it as their source, or one item of
/// no known region when none does ([`ModelRegions::for_table`]). With no
/// `config` nothing carries a label, so this returns no items.
pub fn data_regions(
    config: Option<&RockyConfig>,
    config_path: &Path,
    models: &[&str],
    tables: &[&str],
) -> Vec<AiDataRegion> {
    let Some(config) = config else {
        return Vec::new();
    };
    let regions = ModelRegions::resolve(config, config_path);
    let mut out = Vec::new();
    for model in models {
        for region in regions.for_model(model) {
            out.push(AiDataRegion {
                data: format!("the schema of model `{model}`"),
                region,
            });
        }
    }
    for table in tables {
        for region in regions.for_table(table) {
            out.push(AiDataRegion {
                data: format!("the schema of table `{table}`"),
                region,
            });
        }
    }
    out
}

/// The direct upstreams of `model` whose typed schema the compile knows,
/// split into project models and other tables. These are the schemas the
/// explain and sync prompts carry next to the model's own.
pub fn upstream_schemas(compile_result: &CompileResult, model: &str) -> (Vec<String>, Vec<String>) {
    let mut models = Vec::new();
    let mut tables = Vec::new();
    let Some(schema) = compile_result.semantic_graph.model_schema(model) else {
        return (models, tables);
    };
    for up in &schema.upstream {
        if !compile_result.type_check.typed_models.contains_key(up) {
            continue;
        }
        if compile_result.project.model(up).is_some() {
            models.push(up.clone());
        } else {
            tables.push(up.clone());
        }
    }
    (models, tables)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Write a project and load it the way every command does.
    fn cfg(dir: &Path, extra: &str) -> RockyConfig {
        let path = dir.join("rocky.toml");
        std::fs::write(
            &path,
            format!(
                r#"
[adapter]
type = "duckdb"
path = "x.duckdb"

[pipeline.xf]
type = "transformation"
models = "models/**"

[pipeline.xf.target]
adapter = "default"
region = "eu"
{extra}"#
            ),
        )
        .unwrap();
        rocky_core::config::load_optional_project_config(Some(&path))
            .expect("loads")
            .expect("present")
    }

    #[test]
    fn a_model_takes_its_pipeline_target_region_and_a_table_has_none() {
        let dir = tempfile::tempdir().unwrap();
        let config = cfg(dir.path(), "");
        let data = data_regions(
            Some(&config),
            &dir.path().join("rocky.toml"),
            &["orders"],
            &["raw.orders"],
        );
        assert_eq!(
            data,
            vec![
                AiDataRegion {
                    data: "the schema of model `orders`".into(),
                    region: Some("eu".into()),
                },
                AiDataRegion {
                    data: "the schema of table `raw.orders`".into(),
                    region: None,
                },
            ]
        );
    }

    /// A table that is not a model but that a replication or snapshot
    /// pipeline writes takes that pipeline's regions. Without this, an EU
    /// replicated table's schema went to a US AI endpoint unchecked.
    #[test]
    fn a_table_a_replication_or_snapshot_target_writes_takes_its_region() {
        let dir = tempfile::tempdir().unwrap();
        let config = cfg(
            dir.path(),
            r#"
[pipeline.bronze]
type = "replication"

[pipeline.bronze.source]
region = "eu"

[pipeline.bronze.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["source"]

[pipeline.bronze.target]
catalog_template = "wh"
schema_template = "raw__{source}"
region = "eu"

[pipeline.hist]
type = "snapshot"
unique_key = ["id"]
updated_at = "updated_at"

[pipeline.hist.source]
catalog = "wh"
schema = "raw__crm"
table = "customers"
region = "eu"

[pipeline.hist.target]
catalog = "wh"
schema = "silver__scd"
table = "customers_history"
region = "eu"

[ai]
region = "us"
"#,
        );
        let path = dir.path().join("rocky.toml");
        for table in ["raw__crm.orders", "silver__scd.customers_history"] {
            let data = data_regions(Some(&config), &path, &[], &[table]);
            assert!(
                !data.is_empty() && data.iter().all(|d| d.region.as_deref() == Some("eu")),
                "{table}: {data:?}"
            );
            let err = check_ai_residency(Some(&config), &data)
                .expect_err("an EU replicated table to a US endpoint");
            assert!(err.to_string().starts_with("V062"), "{err}");
        }
        // A table no pipeline writes still has no known region.
        let data = data_regions(Some(&config), &path, &[], &["other.orders"]);
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].region, None);
    }

    #[test]
    fn no_config_gives_no_items() {
        assert!(data_regions(None, Path::new("rocky.toml"), &["m"], &["t"]).is_empty());
    }
}
