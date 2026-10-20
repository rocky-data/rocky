//! The warehouses each model runs on, resolved from the project's pipelines.
//!
//! One resolver for every surface that compiles a project: `rocky compile`,
//! `ci`, `test`, `plan`, `run`, `docs`, `publish-ir`, `rocky serve` and the
//! language server. The compile types a `CAST` whose width differs between
//! warehouses for these targets (#2333), so two surfaces that resolved them
//! differently would type the same column differently (#2342).
//!
//! It reads only `rocky.toml` and the model files: no adapter registry, so
//! the adapter-free `rocky-lsp` binary can use it too.

use std::collections::HashMap;
use std::path::Path;

use rocky_core::config::{AdapterConfig, AdapterKind, RockyConfig, TransformationPipelineConfig};

use crate::models_loader;
use crate::operand_check::{OperandDialect, OperandTarget, TargetDialects};

/// The warehouse adapters each model runs on, resolved from the pipelines
/// that actually target them — not from every configured adapter.
///
/// ```text
///   model ──(matches pipeline.<p>.models glob)──▶ transformation pipeline
///         ──(pipeline.<p>.target.adapter)──────▶ warehouse adapter
/// ```
///
/// - A model that a transformation pipeline's `models` glob loads runs on
///   that pipeline's target. Several pipelines can load one model; it then
///   has several targets.
/// - A model no transformation pipeline loads can still run through
///   `rocky run --models` on any pipeline, so it gets every pipeline's
///   target. With no pipeline at all, it gets every warehouse adapter.
/// - A pipeline whose model set cannot be read adds its target to every
///   model (fail closed).
///
/// The compile-time refusals (E049, E051, E053, E054, E042/E043) refuse a
/// model when ANY of its targets refuses it. An adapter that no pipeline
/// targets no longer hides a refusal.
pub struct ModelTargets<'c> {
    by_model: HashMap<String, Vec<&'c AdapterConfig>>,
    unclaimed: Vec<&'c AdapterConfig>,
    everywhere: Vec<&'c AdapterConfig>,
}

impl<'c> ModelTargets<'c> {
    /// Resolve the targets of every model from `config`, whose file is at
    /// `config_path` (pipeline `models` globs are relative to it).
    ///
    /// Reads every transformation pipeline's model files from disk. A caller
    /// that compiles often (the language server) should keep the result.
    pub fn resolve(config: &'c RockyConfig, config_path: &Path) -> Self {
        let warehouse = |name: &str| config.adapters.get(name).filter(|a| is_warehouse(a));
        let mut pipelines: Vec<(&String, &rocky_core::config::PipelineConfig)> =
            config.pipelines.iter().collect();
        pipelines.sort_by(|a, b| a.0.cmp(b.0));

        let mut by_model: HashMap<String, Vec<&'c AdapterConfig>> = HashMap::new();
        let mut everywhere = Vec::new();
        let mut unclaimed = Vec::new();
        for (_, pipeline) in &pipelines {
            let Some(adapter) = warehouse(pipeline.target_adapter()) else {
                continue;
            };
            push_unique(&mut unclaimed, adapter);
            let Some(tx) = pipeline.as_transformation() else {
                continue;
            };
            match pipeline_model_names(tx, config_path) {
                Some(names) => {
                    for name in names {
                        push_unique(by_model.entry(name).or_default(), adapter);
                    }
                }
                None => push_unique(&mut everywhere, adapter),
            }
        }
        if pipelines.is_empty() {
            let mut names: Vec<&String> = config.adapters.keys().collect();
            names.sort();
            for name in names {
                if let Some(adapter) = warehouse(name) {
                    push_unique(&mut unclaimed, adapter);
                }
            }
        }
        Self {
            by_model,
            unclaimed,
            everywhere,
        }
    }

    /// The warehouses `model` can run on. Empty when nothing is configured.
    pub fn for_model(&self, model: &str) -> Vec<&'c AdapterConfig> {
        match self.by_model.get(model) {
            Some(claimed) => self.with_everywhere(claimed.clone()),
            None => self.for_unlisted_model(),
        }
    }

    /// The warehouses a model no pipeline lists by name can run on.
    pub fn for_unlisted_model(&self) -> Vec<&'c AdapterConfig> {
        self.with_everywhere(self.unclaimed.clone())
    }

    /// The warehouse each model runs on, for the compile itself
    /// ([`crate::compile::CompilerConfig::target_dialects`]).
    ///
    /// Only the adapters of the pipelines that load the model count.
    /// `--target-dialect` and `[portability] target_dialect` are not used:
    /// they name a dialect to lint the SQL against, not the warehouse that
    /// writes the column, so a type taken from them could be wrong. A model
    /// with no known adapter, or with one Rocky has no width table for, keeps
    /// such casts `Unknown`.
    pub fn target_dialects(&self) -> TargetDialects {
        let mut out = TargetDialects::uniform(adapter_target(&self.for_unlisted_model()));
        for model in self.by_model.keys() {
            out.set(model.clone(), adapter_target(&self.for_model(model)));
        }
        out
    }

    fn with_everywhere(&self, mut out: Vec<&'c AdapterConfig>) -> Vec<&'c AdapterConfig> {
        for adapter in &self.everywhere {
            push_unique(&mut out, adapter);
        }
        out
    }
}

/// [`ModelTargets::target_dialects`] of the project `config`, or no target
/// for any model when there is no project config.
pub fn target_dialects(config: Option<&RockyConfig>, config_path: &Path) -> TargetDialects {
    match config {
        Some(config) => ModelTargets::resolve(config, config_path).target_dialects(),
        None => TargetDialects::default(),
    }
}

/// Add `adapter` to `list` unless that same block is already there.
pub fn push_unique<'c>(list: &mut Vec<&'c AdapterConfig>, adapter: &'c AdapterConfig) {
    if !list.iter().any(|a| std::ptr::eq(*a, adapter)) {
        list.push(adapter);
    }
}

/// Whether an adapter block acts as a warehouse (the data role): every
/// block except a discovery-only type (`fivetran`, `airbyte`, …) or one
/// declared `kind = "discovery"`. An adapter type Rocky does not know counts
/// as a warehouse.
pub fn is_warehouse(adapter: &AdapterConfig) -> bool {
    adapter.kind != Some(AdapterKind::Discovery)
        && rocky_core::adapter_capability::capability_for(&adapter.adapter_type)
            .is_none_or(|cap| cap.supports_data)
}

/// The dialects of `adapters`, with no fallback to a configured lint
/// dialect. An adapter Rocky has no dialect for is `unruled`.
fn adapter_target(adapters: &[&AdapterConfig]) -> OperandTarget {
    if adapters.is_empty() {
        return OperandTarget::Unconfigured;
    }
    let mut dialects = Vec::new();
    let mut unruled = Vec::new();
    for adapter in adapters {
        match OperandDialect::from_adapter_type(&adapter.adapter_type) {
            Some(d) if !dialects.contains(&d) => dialects.push(d),
            Some(_) => {}
            None if !unruled.contains(&adapter.adapter_type) => {
                unruled.push(adapter.adapter_type.clone());
            }
            None => {}
        }
    }
    OperandTarget::Targets { dialects, unruled }
}

/// The names of the models a transformation pipeline loads, through the
/// same glob resolution `rocky run` uses. `None` when the set cannot be read.
fn pipeline_model_names(
    tx: &TransformationPipelineConfig,
    config_path: &Path,
) -> Option<Vec<String>> {
    match models_loader::locate_models_dir(&tx.models, config_path).ok()? {
        models_loader::ModelsDir::Absent(_) => Some(Vec::new()),
        models_loader::ModelsDir::Present(dir) => {
            let glob = models_loader::resolved_models_glob(&tx.models, config_path);
            let models = models_loader::load_project_models_matching(&dir, &glob, None).ok()?;
            Some(models.into_iter().map(|m| m.config.name).collect())
        }
    }
}

/// The `region` labels of the data each model writes, resolved from the
/// targets of the pipelines that load it, by the same rules as
/// [`ModelTargets`]:
///
/// - A model that a transformation pipeline's `models` glob loads takes
///   that pipeline's `target.region`. Several pipelines can load one model;
///   it then has several regions.
/// - A model no transformation pipeline loads can still run through
///   `rocky run --models` on any pipeline, so it takes every pipeline's
///   target region.
/// - A pipeline whose model set cannot be read adds its region to every
///   model (fail closed).
///
/// A table that is not a model takes its regions from the replication and
/// snapshot pipelines that write it ([`Self::for_table`]).
///
/// `None` in a list is a target with no label. Used to decide whether an AI
/// request about a model or table may go to the `[ai]` endpoint
/// ([`rocky_core::config::check_ai_residency`]).
pub struct ModelRegions {
    by_model: HashMap<String, Vec<Option<String>>>,
    unclaimed: Vec<Option<String>>,
    everywhere: Vec<Option<String>>,
    writers: Vec<TableWriter>,
}

/// The tables one replication or snapshot pipeline writes, and the regions
/// of their data.
struct TableWriter {
    /// Matches the schema part of a table name.
    schema: SchemaMatch,
    /// The exact table name, for a snapshot. `None` for a replication,
    /// whose table names come from the source at run time.
    table: Option<String>,
    /// The target's region, and the source's for data copied from it.
    regions: Vec<Option<String>>,
}

enum SchemaMatch {
    /// A snapshot's `target.schema`.
    Exact(String),
    /// A replication's `target.schema_template`, each `{placeholder}` a
    /// wildcard.
    Template(regex::Regex),
}

impl ModelRegions {
    /// Resolve the regions of every model from `config`, whose file is at
    /// `config_path` (pipeline `models` globs are relative to it). Reads
    /// every transformation pipeline's model files from disk.
    pub fn resolve(config: &RockyConfig, config_path: &Path) -> Self {
        let mut pipelines: Vec<(&String, &rocky_core::config::PipelineConfig)> =
            config.pipelines.iter().collect();
        pipelines.sort_by(|a, b| a.0.cmp(b.0));

        let mut by_model: HashMap<String, Vec<Option<String>>> = HashMap::new();
        let mut everywhere = Vec::new();
        let mut unclaimed = Vec::new();
        let mut writers = Vec::new();
        for (_, pipeline) in &pipelines {
            let region = pipeline.target_region().map(str::to_string);
            push_unique_region(&mut unclaimed, &region);
            if let Some(writer) = TableWriter::of(pipeline) {
                writers.push(writer);
            }
            let Some(tx) = pipeline.as_transformation() else {
                continue;
            };
            match pipeline_model_names(tx, config_path) {
                Some(names) => {
                    for name in names {
                        push_unique_region(by_model.entry(name).or_default(), &region);
                    }
                }
                None => push_unique_region(&mut everywhere, &region),
            }
        }
        Self {
            by_model,
            unclaimed,
            everywhere,
            writers,
        }
    }

    /// The regions of the data `model` writes. Never empty: a project with
    /// no pipeline gives `[None]`, a model of no known region.
    pub fn for_model(&self, model: &str) -> Vec<Option<String>> {
        let mut out = self
            .by_model
            .get(model)
            .cloned()
            .unwrap_or_else(|| self.unclaimed.clone());
        for region in &self.everywhere {
            push_unique_region(&mut out, region);
        }
        if out.is_empty() {
            out.push(None);
        }
        out
    }

    /// The regions of `table`, a table that is not a project model, named
    /// `<schema>.<table>` or `<catalog>.<schema>.<table>`.
    ///
    /// A replication pipeline writes every table in a schema its
    /// `target.schema_template` renders, each `{placeholder}` standing for
    /// any text. A snapshot pipeline writes its `target.schema` and
    /// `target.table`. Each pipeline that writes `table` adds its target
    /// region, and its source region, because the target holds data copied
    /// from the source. The catalog part is not compared, so a name can match
    /// more pipelines than really write it: that adds regions, which can only
    /// refuse more. Names compare without regard to case.
    ///
    /// `[None]` when no pipeline writes `table`, or for a name with no schema
    /// part: Rocky knows no region for it.
    pub fn for_table(&self, table: &str) -> Vec<Option<String>> {
        let mut out = Vec::new();
        let parts: Vec<&str> = table.split('.').collect();
        if let [.., schema, name] = parts.as_slice() {
            for writer in &self.writers {
                if writer.writes(schema, name) {
                    for region in &writer.regions {
                        push_unique_region(&mut out, region);
                    }
                }
            }
        }
        if out.is_empty() {
            out.push(None);
        }
        out
    }
}

impl TableWriter {
    /// The tables `pipeline` writes, or `None` for a pipeline type that
    /// writes models (transformation) or no table Rocky can name.
    fn of(pipeline: &rocky_core::config::PipelineConfig) -> Option<Self> {
        let (schema, table) = if let Some(r) = pipeline.as_replication() {
            (
                SchemaMatch::Template(schema_template_regex(&r.target.schema_template)?),
                None,
            )
        } else if let Some(s) = pipeline.as_snapshot() {
            (
                SchemaMatch::Exact(s.target.schema.clone()),
                Some(s.target.table.clone()),
            )
        } else {
            return None;
        };
        let mut regions = Vec::new();
        push_unique_region(&mut regions, &pipeline.target_region().map(str::to_string));
        if let Some(source) = pipeline.source_region() {
            push_unique_region(&mut regions, &source.map(str::to_string));
        }
        Some(Self {
            schema,
            table,
            regions,
        })
    }

    fn writes(&self, schema: &str, table: &str) -> bool {
        let schema_matches = match &self.schema {
            SchemaMatch::Exact(s) => s.eq_ignore_ascii_case(schema),
            SchemaMatch::Template(re) => re.is_match(schema),
        };
        schema_matches
            && self
                .table
                .as_deref()
                .is_none_or(|t| t.eq_ignore_ascii_case(table))
    }
}

/// A case-insensitive regex for the schema names `template` can render.
/// Each `{...}` placeholder matches any text with no `.`; the rest matches
/// as written. `None` only if the regex does not build.
fn schema_template_regex(template: &str) -> Option<regex::Regex> {
    let mut pattern = String::from("(?i)^");
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        let Some(close) = rest[open..].find('}') else {
            break;
        };
        pattern.push_str(&regex::escape(&rest[..open]));
        pattern.push_str("[^.]*");
        rest = &rest[open + close + 1..];
    }
    pattern.push_str(&regex::escape(rest));
    pattern.push('$');
    regex::Regex::new(&pattern).ok()
}

fn push_unique_region(list: &mut Vec<Option<String>>, region: &Option<String>) {
    if !list.contains(region) {
        list.push(region.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(path: &Path, contents: &str) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("mkdir");
        }
        std::fs::write(path, contents).expect("write");
    }

    /// A valid model with a target, so it loads cleanly.
    fn write_model(dir: &Path, name: &str) {
        write(&dir.join(format!("{name}.sql")), "SELECT 1 AS id\n");
        write(
            &dir.join(format!("{name}.toml")),
            &format!(
                "name = \"{name}\"\n[target]\ncatalog = \"w\"\nschema = \"s\"\ntable = \"{name}\"\n"
            ),
        );
    }

    /// Write `rocky.toml` (an adapter plus `pipelines`) under `root` and
    /// resolve its regions.
    fn resolve(root: &Path, pipelines: &str) -> ModelRegions {
        let path = root.join("rocky.toml");
        write(
            &path,
            &format!("[adapter]\ntype = \"duckdb\"\npath = \"w.duckdb\"\n\n{pipelines}"),
        );
        let config = rocky_core::config::load_rocky_config(&path).expect("config");
        ModelRegions::resolve(&config, &path)
    }

    fn label(region: &str) -> Option<String> {
        Some(region.to_string())
    }

    fn transformation(name: &str, glob: &str, region: Option<&str>) -> String {
        let region = region.map_or(String::new(), |r| format!("region = \"{r}\"\n"));
        format!(
            "[pipeline.{name}]\ntype = \"transformation\"\nmodels = \"{glob}\"\n\
             [pipeline.{name}.target]\nadapter = \"default\"\n{region}\n"
        )
    }

    fn replication(
        name: &str,
        schema_template: &str,
        target: Option<&str>,
        source: Option<&str>,
    ) -> String {
        let line = |r: Option<&str>| r.map_or(String::new(), |r| format!("region = \"{r}\"\n"));
        format!(
            "[pipeline.{name}]\ntype = \"replication\"\n\n\
             [pipeline.{name}.source]\n{}\n\
             [pipeline.{name}.source.schema_pattern]\nprefix = \"src__\"\n\
             separator = \"__\"\ncomponents = [\"source\"]\n\n\
             [pipeline.{name}.target]\ncatalog_template = \"wh\"\n\
             schema_template = \"{schema_template}\"\n{}\n",
            line(source),
            line(target)
        )
    }

    fn snapshot(name: &str, schema: &str, table: &str, region: &str) -> String {
        format!(
            "[pipeline.{name}]\ntype = \"snapshot\"\nunique_key = [\"id\"]\n\
             updated_at = \"updated_at\"\n\n\
             [pipeline.{name}.source]\ncatalog = \"wh\"\nschema = \"raw\"\ntable = \"c\"\n\n\
             [pipeline.{name}.target]\ncatalog = \"wh\"\nschema = \"{schema}\"\n\
             table = \"{table}\"\nregion = \"{region}\"\n\n"
        )
    }

    #[test]
    fn a_claimed_model_takes_its_pipeline_target_region() {
        let tmp = tempfile::tempdir().unwrap();
        write_model(&tmp.path().join("models"), "orders");
        let regions = resolve(
            tmp.path(),
            &transformation("xf", "models/**", Some("eu")),
        );
        assert_eq!(regions.for_model("orders"), vec![label("eu")]);
    }

    #[test]
    fn a_claimed_model_of_an_unlabelled_pipeline_has_no_region() {
        let tmp = tempfile::tempdir().unwrap();
        write_model(&tmp.path().join("models"), "orders");
        let regions = resolve(tmp.path(), &transformation("xf", "models/**", None));
        assert_eq!(regions.for_model("orders"), vec![None]);
    }

    /// Two pipelines load one model: it has both regions, so an AI request
    /// about it is judged against each.
    #[test]
    fn a_model_two_pipelines_load_has_both_regions() {
        let tmp = tempfile::tempdir().unwrap();
        write_model(&tmp.path().join("models"), "orders");
        let pipelines = format!(
            "{}{}",
            transformation("a", "models/**", Some("eu")),
            transformation("b", "models/**", Some("us"))
        );
        let regions = resolve(tmp.path(), &pipelines);
        assert_eq!(regions.for_model("orders"), vec![label("eu"), label("us")]);
    }

    /// A model no pipeline loads can run on any pipeline: it takes every
    /// pipeline's target region, the replication's included.
    #[test]
    fn an_unclaimed_model_takes_every_pipeline_region() {
        let tmp = tempfile::tempdir().unwrap();
        write_model(&tmp.path().join("models"), "orders");
        let pipelines = format!(
            "{}{}",
            transformation("xf", "models/**", Some("eu")),
            replication("bronze", "raw__{source}", Some("us"), None)
        );
        let regions = resolve(tmp.path(), &pipelines);
        assert_eq!(regions.for_model("orders"), vec![label("eu")]);
        assert_eq!(regions.for_model("adhoc"), vec![label("us"), label("eu")]);
    }

    /// A pipeline whose models do not load adds its region to every model
    /// (fail closed).
    #[test]
    fn a_pipeline_whose_models_do_not_load_adds_its_region_everywhere() {
        let tmp = tempfile::tempdir().unwrap();
        write_model(&tmp.path().join("models"), "orders");
        write(&tmp.path().join("broken").join("bad.sql"), "SELECT 1\n");
        write(&tmp.path().join("broken").join("bad.toml"), "name = [\n");
        let pipelines = format!(
            "{}{}",
            transformation("bad", "broken/**", Some("us")),
            transformation("good", "models/**", Some("eu"))
        );
        let regions = resolve(tmp.path(), &pipelines);
        assert_eq!(regions.for_model("orders"), vec![label("eu"), label("us")]);
    }

    #[test]
    fn no_pipeline_gives_no_known_region() {
        let tmp = tempfile::tempdir().unwrap();
        let regions = resolve(tmp.path(), "");
        assert_eq!(regions.for_model("orders"), vec![None]);
        assert_eq!(regions.for_table("raw.orders"), vec![None]);
    }

    /// A table a replication writes: its schema matches the target
    /// `schema_template`, with or without a catalog part, in any case.
    #[test]
    fn a_replicated_table_takes_the_replication_target_region() {
        let tmp = tempfile::tempdir().unwrap();
        let regions = resolve(
            tmp.path(),
            &replication("bronze", "raw__{source}", Some("eu"), None),
        );
        assert_eq!(regions.for_table("raw__crm.orders"), vec![label("eu"), None]);
        assert_eq!(
            regions.for_table("wh.raw__crm.orders"),
            vec![label("eu"), None]
        );
        assert_eq!(regions.for_table("RAW__CRM.orders"), vec![label("eu"), None]);
        // Another schema, and a bare name, are not this pipeline's.
        assert_eq!(regions.for_table("staging.orders"), vec![None]);
        assert_eq!(regions.for_table("orders"), vec![None]);
        // The literal text around the placeholder must match as written.
        assert_eq!(regions.for_table("rawXXcrm.orders"), vec![None]);
    }

    /// The target holds data copied from the source, so a replicated table
    /// carries the source region too: an unlabelled target does not hide an
    /// EU source.
    #[test]
    fn a_replicated_table_also_takes_the_source_region() {
        let tmp = tempfile::tempdir().unwrap();
        let regions = resolve(
            tmp.path(),
            &replication("bronze", "raw__{source}", None, Some("eu")),
        );
        assert_eq!(regions.for_table("raw__crm.orders"), vec![None, label("eu")]);
    }

    #[test]
    fn a_snapshot_table_matches_its_exact_target() {
        let tmp = tempfile::tempdir().unwrap();
        let regions = resolve(
            tmp.path(),
            &snapshot("hist", "silver", "customers_history", "eu"),
        );
        assert_eq!(
            regions.for_table("silver.customers_history"),
            vec![label("eu"), None]
        );
        assert_eq!(regions.for_table("silver.orders"), vec![None]);
    }

    /// Two writers match one table: it takes both regions (fail closed).
    #[test]
    fn a_table_two_pipelines_write_has_both_regions() {
        let tmp = tempfile::tempdir().unwrap();
        let pipelines = format!(
            "{}{}",
            replication("a", "raw__{source}", Some("eu"), Some("eu")),
            replication("b", "{tenant}__{source}", Some("us"), Some("us"))
        );
        let regions = resolve(tmp.path(), &pipelines);
        assert_eq!(
            regions.for_table("raw__crm.orders"),
            vec![label("eu"), label("us")]
        );
    }

    #[test]
    fn schema_template_regex_escapes_literals_and_wildcards_placeholders() {
        let re = schema_template_regex("raw.x__{source}").unwrap();
        assert!(!re.is_match("rawXx__crm"), "the `.` is a literal");
        let re = schema_template_regex("raw__{source}_{regions:_}").unwrap();
        assert!(re.is_match("raw__crm_eu_us"));
        assert!(!re.is_match("raw__crm"));
        assert!(!re.is_match("x_raw__crm_eu"));
    }
}
