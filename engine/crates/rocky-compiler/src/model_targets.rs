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

use std::collections::{HashMap, HashSet};
use std::path::Path;

use rocky_core::config::{AdapterConfig, AdapterKind, RockyConfig, TransformationPipelineConfig};
use rocky_core::schema::{PatternComponent, SchemaPattern};

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
/// A table that is not a model takes its regions from the pipelines that
/// write it or read it as their source ([`Self::for_table`]).
///
/// `None` in a list is a region block with no label. Used to decide whether
/// an AI request about a model or table may go to the `[ai]` endpoint
/// ([`rocky_core::config::check_ai_residency`]).
pub struct ModelRegions {
    by_model: HashMap<String, Vec<Option<String>>>,
    unclaimed: Vec<Option<String>>,
    everywhere: Vec<Option<String>>,
    /// The tables replication, snapshot and load pipelines write.
    targets: Vec<TableRule>,
    /// The tables replication and snapshot pipelines read as their source.
    sources: Vec<TableRule>,
}

/// The tables one side (target or source) of one pipeline covers, and the
/// regions of their data.
struct TableRule {
    /// Matches the schema part of a table name.
    schema: SchemaMatch,
    /// The exact table name, when the pipeline names one. `None` when the
    /// rule covers every table in a matching schema.
    table: Option<String>,
    /// The regions of the data in these tables.
    regions: Vec<Option<String>>,
}

enum SchemaMatch {
    /// A snapshot's or load's `target.schema`, or a snapshot's
    /// `source.schema`.
    Exact(String),
    /// A replication's `target.schema_template`.
    Template(SchemaTemplate),
    /// A replication's `source.schema_pattern`, matched the way discovery
    /// matches it ([`SchemaPattern::parse`]). Its prefix and separator are
    /// lowercased, and so is the schema it is tested against.
    SourcePattern(SchemaPattern),
    /// A replication `source.schema_pattern` whose `components` do not parse:
    /// it matches every schema (fail closed). Discovery refuses such a
    /// pipeline, so it reads no table, but Rocky cannot say which schemas it
    /// would have read.
    Any,
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
        let mut targets = Vec::new();
        let mut sources = Vec::new();
        for (_, pipeline) in &pipelines {
            let region = pipeline.target_region().map(str::to_string);
            push_unique_region(&mut unclaimed, &region);
            targets.extend(TableRule::target_of(pipeline));
            sources.extend(TableRule::source_of(pipeline));
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
            targets,
            sources,
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
    /// The last dot-separated part is the table. Every tail of the parts
    /// before it is tried as the schema (`a.b.c`, `b.c` and `c` for `a.b.c.t`), so a
    /// schema name that holds a `.` (a `source.schema_pattern` with
    /// `separator = "."`, for example) is matched too. A split that is not
    /// the real one can only add regions. The name is compared as given: no
    /// quote is removed. The names the AI
    /// commands pass come from the schema cache, whose keys are the
    /// warehouse's own names, lowercased and unquoted.
    ///
    /// Tables a pipeline writes (its target side):
    ///
    /// - A replication writes every table in a schema its
    ///   `target.schema_template` renders. Each `{placeholder}` stands for any
    ///   text with no `.` in it.
    /// - A snapshot writes its `target.schema` and `target.table`.
    /// - A load writes its `target.table` in its `target.schema`, or every
    ///   table in that schema when `table` is unset.
    ///
    /// Such a table takes the pipeline's target region, and its source region
    /// (replication and snapshot), because the target holds data copied from
    /// the source.
    ///
    /// Tables a pipeline reads (its source side):
    ///
    /// - A replication reads every table in a schema its
    ///   `source.schema_pattern` parses, with the parser discovery uses. The
    ///   match ignores case and catalog, so it is at least as wide as
    ///   discovery's.
    /// - A snapshot reads its `source.schema` and `source.table`.
    /// - A load reads files, not tables, so it has no source side.
    ///
    /// Such a table takes the pipeline's source region only.
    ///
    /// The catalog part is not compared, so a name can match more pipelines
    /// than really write or read it: that adds regions, which can only
    /// refuse more. Names compare without regard to ASCII case.
    ///
    /// `[None]` when no pipeline covers `table`, or for a name with no schema
    /// part: Rocky knows no region for it.
    pub fn for_table(&self, table: &str) -> Vec<Option<String>> {
        let mut out = Vec::new();
        let parts: Vec<&str> = table.split('.').collect();
        if let [front @ .., name] = parts.as_slice() {
            // Every run of parts that ends just before the table name is a
            // candidate schema, so a schema that holds a `.` is matched too.
            for start in 0..front.len() {
                let schema = front[start..].join(".");
                for rule in self.targets.iter().chain(&self.sources) {
                    if rule.covers(&schema, name) {
                        for region in &rule.regions {
                            push_unique_region(&mut out, region);
                        }
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

impl TableRule {
    /// The tables `pipeline` writes that are not models, or `None` for a
    /// pipeline type that writes only models (transformation) or no table
    /// (quality).
    fn target_of(pipeline: &rocky_core::config::PipelineConfig) -> Option<Self> {
        use rocky_core::config::PipelineConfig;
        let (schema, table) = match pipeline {
            PipelineConfig::Replication(r) => (
                SchemaMatch::Template(SchemaTemplate::new(&r.target.schema_template)),
                None,
            ),
            PipelineConfig::Snapshot(s) => (
                SchemaMatch::Exact(s.target.schema.clone()),
                Some(s.target.table.clone()),
            ),
            // A load with no `target.table` names each table after its file.
            PipelineConfig::Load(l) => (
                SchemaMatch::Exact(l.target.schema.clone()),
                l.target.table.clone(),
            ),
            PipelineConfig::Transformation(_) | PipelineConfig::Quality(_) => return None,
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

    /// The tables `pipeline` reads as its source, or `None` for a pipeline
    /// type with no source tables: a load reads files, and a transformation
    /// or quality pipeline has no source block.
    fn source_of(pipeline: &rocky_core::config::PipelineConfig) -> Option<Self> {
        use rocky_core::config::PipelineConfig;
        let (schema, table, region) = match pipeline {
            PipelineConfig::Replication(r) => {
                let schema = match r.source.schema_pattern.to_schema_pattern() {
                    Ok(pattern) => SchemaMatch::SourcePattern(lowercase_pattern(pattern)),
                    Err(_) => SchemaMatch::Any,
                };
                (schema, None, &r.source.region)
            }
            PipelineConfig::Snapshot(s) => (
                SchemaMatch::Exact(s.source.schema.clone()),
                Some(s.source.table.clone()),
                &s.source.region,
            ),
            PipelineConfig::Load(_)
            | PipelineConfig::Transformation(_)
            | PipelineConfig::Quality(_) => return None,
        };
        Some(Self {
            schema,
            table,
            regions: vec![region.clone()],
        })
    }

    fn covers(&self, schema: &str, table: &str) -> bool {
        let schema_matches = match &self.schema {
            SchemaMatch::Exact(s) => s.eq_ignore_ascii_case(schema),
            SchemaMatch::Template(t) => t.matches(schema),
            SchemaMatch::SourcePattern(p) => p.parse(&schema.to_ascii_lowercase()).is_ok(),
            SchemaMatch::Any => true,
        };
        schema_matches
            && self
                .table
                .as_deref()
                .is_none_or(|t| t.eq_ignore_ascii_case(table))
    }
}

/// `pattern` with its literal text lowercased, so it matches a lowercased
/// schema name the way the original matches the name as the source reports
/// it.
fn lowercase_pattern(pattern: SchemaPattern) -> SchemaPattern {
    SchemaPattern {
        prefix: pattern.prefix.to_ascii_lowercase(),
        separator: pattern.separator.to_ascii_lowercase(),
        components: pattern
            .components
            .into_iter()
            .map(|c| match c {
                PatternComponent::Fixed(text) => PatternComponent::Fixed(text.to_ascii_lowercase()),
                other @ (PatternComponent::Variable { .. }
                | PatternComponent::VariableLength { .. }
                | PatternComponent::Terminal { .. }) => other,
            })
            .collect(),
    }
}

/// The schema names a replication `target.schema_template` can render.
///
/// Each `{...}` placeholder matches any text with no `.` in it, and the text
/// between placeholders matches as written, ignoring ASCII case. A `{` with
/// no `}` after it is literal text, as rendering treats it.
///
/// A hand-written matcher, not a regex, so building one cannot fail: every
/// template has a pattern, and no fallback is needed. The search remembers
/// each `(literal, offset)` pair that failed, so it takes at most about
/// `k * n^2` steps for `k` placeholders over a name of `n` bytes, not an
/// exponential number.
struct SchemaTemplate {
    /// The literal text around the placeholders, lowercased. `n` pieces have
    /// `n - 1` placeholders between them. Never empty.
    literals: Vec<String>,
}

impl SchemaTemplate {
    fn new(template: &str) -> Self {
        let mut literals = Vec::new();
        let mut rest = template;
        while let Some(open) = rest.find('{') {
            let Some(close) = rest[open..].find('}') else {
                break;
            };
            literals.push(rest[..open].to_ascii_lowercase());
            rest = &rest[open + close + 1..];
        }
        literals.push(rest.to_ascii_lowercase());
        Self { literals }
    }

    fn matches(&self, schema: &str) -> bool {
        let text = schema.to_ascii_lowercase();
        let mut failed = HashSet::new();
        matches_from(&self.literals, 0, &text, 0, &mut failed)
    }
}

/// Whether `text[at..]` is `literals[index]`, then for each later literal a
/// run of text with no `.` followed by that literal. `index` is in bounds.
/// Records each `(index, at)` that fails in `failed`, so no pair is searched
/// twice.
fn matches_from(
    literals: &[String],
    index: usize,
    text: &str,
    at: usize,
    failed: &mut HashSet<(usize, usize)>,
) -> bool {
    if failed.contains(&(index, at)) {
        return false;
    }
    let found = match text[at..].strip_prefix(literals[index].as_str()) {
        None => false,
        Some(rest) if index + 1 == literals.len() => rest.is_empty(),
        Some(rest) => {
            let start = text.len() - rest.len();
            // The placeholder takes `rest[..end]`: any prefix with no `.`.
            let limit = rest.find('.').unwrap_or(rest.len());
            (0..=limit)
                .filter(|&end| rest.is_char_boundary(end))
                .any(|end| matches_from(literals, index + 1, text, start + end, failed))
        }
    };
    if !found {
        failed.insert((index, at));
    }
    found
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
        let regions = resolve(tmp.path(), &transformation("xf", "models/**", Some("eu")));
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
        // The `None` is deliberate: the source block has no `region`, and the
        // target holds data copied from that source. Under `[residency]
        // enforce`, this `None` refuses the table even though its target is
        // labelled.
        assert_eq!(
            regions.for_table("raw__crm.orders"),
            vec![label("eu"), None]
        );
        assert_eq!(
            regions.for_table("wh.raw__crm.orders"),
            vec![label("eu"), None]
        );
        assert_eq!(
            regions.for_table("RAW__CRM.orders"),
            vec![label("eu"), None]
        );
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
        assert_eq!(
            regions.for_table("raw__crm.orders"),
            vec![None, label("eu")]
        );
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

    /// A load pipeline writes its `target.table`, or with no `table`, every
    /// table in its `target.schema`. It has no source block.
    #[test]
    fn a_loaded_table_takes_the_load_target_region() {
        let tmp = tempfile::tempdir().unwrap();
        let load = |table: &str| {
            format!(
                "[pipeline.ingest]\ntype = \"load\"\nsource_dir = \"data/\"\n\n\
                 [pipeline.ingest.target]\ncatalog = \"wh\"\nschema = \"landing\"\n\
                 {table}region = \"eu\"\n"
            )
        };
        let regions = resolve(tmp.path(), &load(""));
        assert_eq!(regions.for_table("landing.orders"), vec![label("eu")]);
        assert_eq!(regions.for_table("staging.orders"), vec![None]);

        let regions = resolve(tmp.path(), &load("table = \"orders\"\n"));
        assert_eq!(regions.for_table("landing.orders"), vec![label("eu")]);
        assert_eq!(regions.for_table("landing.other"), vec![None]);
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
    fn a_schema_template_matches_literals_as_written_and_placeholders_without_a_dot() {
        // The matcher alone. `for_table` splits a name on `.`, so it never
        // passes a schema that holds a dot: such a schema is not matched.
        let t = SchemaTemplate::new("raw.x__{source}");
        assert!(t.matches("raw.x__crm"));
        assert!(!t.matches("rawXx__crm"), "the `.` is a literal");
        let t = SchemaTemplate::new("raw__{source}_{regions:_}");
        assert!(t.matches("raw__crm_eu_us"));
        assert!(t.matches("RAW__Crm_EU"), "ASCII case is ignored");
        assert!(!t.matches("raw__crm"));
        assert!(!t.matches("x_raw__crm_eu"));
        let t = SchemaTemplate::new("raw__{source}_v1");
        assert!(t.matches("raw__crm_v1"));
        assert!(!t.matches("raw__crm_v1x"), "the last literal ends the name");
        // A placeholder never takes a `.`.
        let t = SchemaTemplate::new("{tenant}__{source}");
        assert!(t.matches("acme__crm"));
        assert!(t.matches("__"), "a placeholder can be empty");
        assert!(!t.matches("a.b__crm"));
        assert!(!t.matches("acme__c.rm"));
        // A `{` with no `}` is literal text.
        let t = SchemaTemplate::new("raw__{source");
        assert!(t.matches("raw__{source"));
        assert!(!t.matches("raw__crm"));
        // No placeholder: the whole name is literal.
        assert!(SchemaTemplate::new("raw").matches("RAW"));
        assert!(!SchemaTemplate::new("raw").matches("raw2"));
    }

    /// Many placeholders over a long name that does not match. Without the
    /// memo of failed `(literal, offset)` pairs, this search tries every way
    /// to split the name and does not finish.
    #[test]
    fn a_schema_template_with_many_placeholders_fails_fast() {
        let t = SchemaTemplate::new(&format!("{}!", "{p}_".repeat(30)));
        assert!(!t.matches(&"_".repeat(200)));
        let t = SchemaTemplate::new(&"{p}".repeat(30));
        assert!(t.matches(&"x".repeat(200)));
        assert!(!t.matches(&format!("{}.", "x".repeat(200))));
    }

    /// The scenario of the red-team finding: a replication copies EU
    /// sources into a US target. `discover --with-schemas` caches the
    /// SOURCE tables, so a model's AI request can carry
    /// `src__acme__eu__shopify.orders`. That table is in the source region,
    /// not in no known region.
    #[test]
    fn a_replication_source_table_takes_the_source_region() {
        let tmp = tempfile::tempdir().unwrap();
        let pipeline = "[pipeline.bronze]\ntype = \"replication\"\n\n\
             [pipeline.bronze.source]\nregion = \"eu\"\n\n\
             [pipeline.bronze.source.schema_pattern]\nprefix = \"src__\"\n\
             separator = \"__\"\ncomponents = [\"tenant\", \"regions...\", \"source\"]\n\n\
             [pipeline.bronze.target]\ncatalog_template = \"wh\"\n\
             schema_template = \"raw__{source}\"\nregion = \"us\"\n\n[residency]\nallow = [{ from = \"eu\", to = \"us\" }]\n";
        let regions = resolve(tmp.path(), pipeline);
        // The source side takes only the source region, with or without a
        // catalog part.
        for name in [
            "src__acme__eu__shopify.orders",
            "src_cat.src__acme__eu__shopify.orders",
            "src__acme__eu_west__eu_north__shopify.orders",
        ] {
            assert_eq!(regions.for_table(name), vec![label("eu")], "{name}");
        }
        // Too few segments for the pattern, or another prefix: not a source.
        assert_eq!(regions.for_table("src__acme.orders"), vec![None]);
        assert_eq!(regions.for_table("other__acme__eu__x.orders"), vec![None]);
        // The target side is unchanged: target and source regions.
        assert_eq!(
            regions.for_table("raw__shopify.orders"),
            vec![label("us"), label("eu")]
        );
    }

    /// The schema cache lowercases its keys, and discovery matches the
    /// pattern against the name the source reports, in its own case. A
    /// mixed-case prefix must still match the lowercased cache key.
    #[test]
    fn a_source_pattern_matches_without_regard_to_case() {
        let tmp = tempfile::tempdir().unwrap();
        let pipeline = "[pipeline.bronze]\ntype = \"replication\"\n\n\
             [pipeline.bronze.source]\nregion = \"eu\"\n\n\
             [pipeline.bronze.source.schema_pattern]\nprefix = \"Src__\"\n\
             separator = \"__\"\ncomponents = [\"source\"]\n\n\
             [pipeline.bronze.target]\ncatalog_template = \"wh\"\n\
             schema_template = \"raw__{source}\"\n";
        let regions = resolve(tmp.path(), pipeline);
        assert_eq!(regions.for_table("src__shopify.orders"), vec![label("eu")]);
        assert_eq!(regions.for_table("SRC__Shopify.Orders"), vec![label("eu")]);
    }

    /// A source `schema_pattern` whose components do not parse matches every
    /// table (fail closed): Rocky cannot say which schemas it would read.
    #[test]
    fn a_source_pattern_that_does_not_parse_matches_every_table() {
        let tmp = tempfile::tempdir().unwrap();
        let pipeline = "[pipeline.bronze]\ntype = \"replication\"\n\n\
             [pipeline.bronze.source]\nregion = \"eu\"\n\n\
             [pipeline.bronze.source.schema_pattern]\nprefix = \"src__\"\n\
             separator = \"__\"\ncomponents = [\"\"]\n\n\
             [pipeline.bronze.target]\ncatalog_template = \"wh\"\n\
             schema_template = \"raw__{source}\"\nregion = \"eu\"\n";
        let regions = resolve(tmp.path(), pipeline);
        assert_eq!(regions.for_table("anything.orders"), vec![label("eu")]);
    }

    /// A snapshot reads its exact `source.schema` and `source.table`; that
    /// table takes the source region only.
    #[test]
    fn a_snapshot_source_table_takes_the_source_region() {
        let tmp = tempfile::tempdir().unwrap();
        let pipeline = "[pipeline.hist]\ntype = \"snapshot\"\nunique_key = [\"id\"]\n\
             updated_at = \"updated_at\"\n\n\
             [pipeline.hist.source]\ncatalog = \"wh\"\nschema = \"raw\"\ntable = \"c\"\n\
             region = \"eu\"\n\n\
             [pipeline.hist.target]\ncatalog = \"wh\"\nschema = \"silver\"\n\
             table = \"c_history\"\nregion = \"us\"\n\n[residency]\nallow = [{ from = \"eu\", to = \"us\" }]\n";
        let regions = resolve(tmp.path(), pipeline);
        assert_eq!(regions.for_table("RAW.C"), vec![label("eu")]);
        assert_eq!(regions.for_table("raw.other"), vec![None]);
        assert_eq!(
            regions.for_table("silver.c_history"),
            vec![label("us"), label("eu")]
        );
    }

    /// `separator = "."` is allowed, so a source schema can hold a dot:
    /// discovery caches `cat.acme.shopify.orders`, which reaches `for_table`
    /// as `acme.shopify.orders`. Taking only `shopify` as the schema missed
    /// it and gave no region (fail open).
    #[test]
    fn a_source_schema_that_holds_a_dot_takes_the_source_region() {
        let tmp = tempfile::tempdir().unwrap();
        let pipeline = "[pipeline.bronze]\ntype = \"replication\"\n\n\
             [pipeline.bronze.source]\nregion = \"eu\"\n\n\
             [pipeline.bronze.source.schema_pattern]\nprefix = \"\"\n\
             separator = \".\"\ncomponents = [\"tenant\", \"source\"]\n\n\
             [pipeline.bronze.target]\ncatalog_template = \"wh\"\n\
             schema_template = \"raw__{source}\"\nregion = \"eu\"\n";
        let regions = resolve(tmp.path(), pipeline);
        assert_eq!(regions.for_table("acme.shopify.orders"), vec![label("eu")]);
        assert_eq!(
            regions.for_table("cat.acme.shopify.orders"),
            vec![label("eu")]
        );
    }

    /// Two replications whose source patterns both accept one schema: the
    /// table takes both source regions.
    #[test]
    fn a_source_table_two_replications_read_has_both_regions() {
        let tmp = tempfile::tempdir().unwrap();
        let pipelines = format!(
            "{}{}",
            replication("a", "raw__{source}", Some("eu"), Some("eu")),
            replication("b", "stage__{source}", Some("us"), Some("us"))
        );
        let regions = resolve(tmp.path(), &pipelines);
        assert_eq!(
            regions.for_table("src__shopify.orders"),
            vec![label("eu"), label("us")]
        );
    }

    /// A load reads files, so no table is on its source side: only its
    /// target schema has its region.
    #[test]
    fn a_load_pipeline_has_no_source_side() {
        let tmp = tempfile::tempdir().unwrap();
        let regions = resolve(
            tmp.path(),
            "[pipeline.ingest]\ntype = \"load\"\nsource_dir = \"data/\"\n\n\
             [pipeline.ingest.target]\ncatalog = \"wh\"\nschema = \"landing\"\n\
             region = \"eu\"\n",
        );
        assert_eq!(regions.sources.len(), 0);
        assert_eq!(regions.for_table("data.orders"), vec![None]);
        assert_eq!(regions.for_table("landing.orders"), vec![label("eu")]);
    }
}
