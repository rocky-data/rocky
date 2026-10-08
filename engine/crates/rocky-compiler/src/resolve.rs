//! Automatic dependency resolution from SQL table references.
//!
//! Extracts table references from model SQL and classifies them as:
//! - **Model refs** — bare names bound to a model of the project
//! - **Source refs** — two-part qualified names (`schema.table`)
//! - **Raw refs** — three-part fully qualified names (`catalog.schema.table`)
//!
//! Model refs become DAG edges. Source and raw refs are external dependencies;
//! the run-time physical derivation (`rocky_core::physical_edges`) orders the
//! qualified ones that name a model's target.
//!
//! # How a bare read binds (#1354)
//!
//! A bare `FROM x` carries no catalog and no schema, so the warehouse resolves
//! it through the connection's search path to a PHYSICAL table called `x`.
//! Local execution (`rocky test`, `rocky ci`) materializes every model at its
//! configured `[target]` and binds a bare read the same way, so both paths
//! encode one semantics, and the compiler binds a bare read by the table a
//! model WRITES, not by the model's name:
//!
//! 1. An ephemeral model is read by its NAME: it writes nothing, and the
//!    inliner rewrites a bare read of its name into a CTE.
//! 2. A model reading a table with its own target's name reads itself or a
//!    same-named table elsewhere: the read is external.
//! 3. Otherwise the candidates are the other models whose target table is `x`
//!    (compared folded, [`rocky_core::physical_edges::fold_identifier`]).
//!    - none: the read is external;
//!    - one: the read binds to it;
//!    - several: the read binds to the one that is also NAMED `x`, else to the
//!      one the reader lists in `depends_on`; if neither picks exactly one,
//!      the read is ambiguous and refused as `E056`. Rocky cannot see the
//!      search path that would choose, so it does not guess.
//!
//! A binding by table alone — the model is not named `x`, and the reader did
//! not declare it — is the weakest evidence. If it would close a dependency
//! cycle it is dropped, the read is treated as external, and `D013` says so:
//! a guess about the search path must not refuse a project or reverse an edge
//! stronger evidence set.
//!
//! A bare read of a model's NAME that does not bind to that model — its target
//! table is spelled differently — is reported as `D012`: the read is external,
//! or binds to whichever model writes a table of that name.

use std::collections::{HashMap, HashSet};

use rocky_core::models::Model;
use rocky_core::physical_edges::fold_identifier;
use rocky_ir::dag::DagNode;
use rocky_sql::lineage;
use thiserror::Error;

use crate::diagnostic::Diagnostic;

/// Resolved output: DAG nodes, per-model lineage cache, and diagnostics.
pub type ResolveOutput = (
    Vec<DagNode>,
    HashMap<String, lineage::LineageResult>,
    Vec<Diagnostic>,
);

/// How a table reference in SQL maps to the project's dependency graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableRefKind {
    /// References another model in the project (bare name like `orders`).
    ModelRef(String),
    /// Two-part qualified reference (`schema.table`) — external source.
    SourceRef { schema: String, table: String },
    /// Three-part fully qualified reference (`catalog.schema.table`) — external.
    RawRef(String),
}

/// Errors during dependency resolution.
/// `#[non_exhaustive]` because this enum is public and adding a variant —
/// as #1224 just did — would otherwise break an external exhaustive match.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum ResolveError {
    #[error("failed to extract lineage from model '{model}': {reason}")]
    LineageExtraction { model: String, reason: String },

    /// Every model whose SQL could not be parsed, not just the first.
    ///
    /// Resolution still fails as a whole — a project Rocky cannot parse is a
    /// project it cannot reason about, and that contract is unchanged. What
    /// changed is that it stops reporting one model per compile. A single
    /// unsupported construct can account for most of a project's parse
    /// failures (#1224), and fixing them one recompile at a time hides the
    /// scale: the user sees "1 model failed" repeatedly instead of "112 models
    /// failed, all on the same syntax".
    #[error(
        "{} model(s) failed to parse:\n{}",
        failures.len(),
        failures
            .iter()
            .map(|(model, reason)| format!("  - {model}: {reason}"))
            .collect::<Vec<_>>()
            .join("\n")
    )]
    LineageExtractionMany { failures: Vec<(String, String)> },
}

/// Classify a table reference name based on its structure and known models.
///
/// Rules:
/// - Bare name matching a known model → `ModelRef`
/// - Two-part `schema.table` → `SourceRef`
/// - Three-part `catalog.schema.table` → `RawRef`
/// - Bare name NOT matching a model → `RawRef` (unknown external table)
///
/// This is a classification by NAME. Dependency derivation does not use it
/// for bare reads: [`resolve_dependencies`] binds a bare read by the table a
/// model writes (#1354; see the module docs).
pub fn classify_table_ref(name: &str, model_names: &HashSet<String>) -> TableRefKind {
    let parts: Vec<&str> = name.split('.').collect();
    match parts.len() {
        1 => {
            let bare_name = parts[0];
            if model_names.contains(bare_name) {
                TableRefKind::ModelRef(bare_name.to_string())
            } else {
                // Unknown bare name — treat as external
                TableRefKind::RawRef(name.to_string())
            }
        }
        2 => TableRefKind::SourceRef {
            schema: parts[0].to_string(),
            table: parts[1].to_string(),
        },
        _ => {
            // 3+ parts — fully qualified external reference
            TableRefKind::RawRef(name.to_string())
        }
    }
}

/// Resolve dependencies for all models by parsing their SQL.
///
/// Returns `DagNode` entries with `depends_on` auto-populated from the bare
/// SQL table refs that bind to other models (see the module docs), along with a cache of `LineageResult` per model
/// (keyed by model name) so downstream phases can reuse the parsed lineage
/// without re-parsing SQL.
///
/// If a model already has explicit `depends_on` in its config, those are
/// preserved and merged with auto-resolved dependencies.
pub fn resolve_dependencies(models: &[Model]) -> Result<ResolveOutput, ResolveError> {
    resolve_dependencies_with_externals(models, &std::collections::BTreeSet::new())
}

/// [`resolve_dependencies`], accepting `externals` as satisfied outside the
/// project.
///
/// A `depends_on` entry that names an external and no model of this project
/// is left out of the model's [`DagNode`], so the topological sort does not
/// refuse it as unknown. `rocky run --dag` passes the seed names its graph
/// resolved: the graph already ordered the model after the seed, and the
/// per-model sub-run compiles models only (#2138). The model's
/// `config.depends_on` itself is left as declared. A name that IS a model of
/// this project keeps its edge.
pub fn resolve_dependencies_with_externals(
    models: &[Model],
    externals: &std::collections::BTreeSet<String>,
) -> Result<ResolveOutput, ResolveError> {
    let model_names: HashSet<String> = models.iter().map(|m| m.config.name.clone()).collect();
    let index = BareReadIndex::new(models);
    let mut per_model = Vec::with_capacity(models.len());
    let mut lineage_cache = HashMap::with_capacity(models.len());
    let mut diagnostics = Vec::new();

    // Collect EVERY parse failure rather than propagating the first (#1224).
    //
    // Done in the SAME pass as the real work, not a pre-pass: a pre-pass costs
    // a second `extract_lineage` for every model on the success path — 2N
    // parses on a healthy project, which is the common case and the one that
    // must stay fast. Here a model that parses is parsed once and its result
    // used; only a model that FAILS is recorded, and after the first failure
    // the remaining work is skipped since the result is already an error.
    let mut parse_failures: Vec<(String, String)> = Vec::new();

    for model in models {
        let lineage_result = match lineage::extract_lineage(&model.sql) {
            Ok(result) => result,
            Err(reason) => {
                parse_failures.push((model.config.name.clone(), reason));
                continue;
            }
        };
        if !parse_failures.is_empty() {
            // Already failing: keep parsing to complete the report, but skip
            // the dependency/diagnostic work whose output is about to be
            // discarded.
            continue;
        }

        let (auto_deps, notes) = extract_deps_from_lineage(&lineage_result, model, &index);

        for note in notes {
            diagnostics.push(note.into_diagnostic(&model.config.name, &index));
        }

        // D011: warn when explicit depends_on is non-empty but misses auto-derived deps
        if !model.config.depends_on.is_empty() {
            let explicit: HashSet<&str> = model
                .config
                .depends_on
                .iter()
                .map(std::string::String::as_str)
                .collect();
            let missing: Vec<&String> = auto_deps
                .iter()
                .map(|d| &d.producer)
                .filter(|d| !explicit.contains(d.as_str()))
                .collect();
            if !missing.is_empty() {
                let missing_str = missing
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                diagnostics.push(
                    Diagnostic::warning(
                        "D011",
                        &model.config.name,
                        format!(
                            "depends_on declares [{}] but the SQL body also reads bare \
                             table names that bind to [{}] at compile time. Qualified \
                             reads are not checked here; they are ordered at run time. \
                             The bare-name dependencies will be merged, but consider \
                             updating depends_on or removing it to let auto-derivation \
                             handle everything.",
                            model.config.depends_on.join(", "),
                            missing_str,
                        ),
                    )
                    .with_suggestion(format!(
                        "Add '{}' to depends_on, or remove the depends_on field entirely",
                        missing_str,
                    )),
                );
            }
        }

        lineage_cache.insert(model.config.name.clone(), lineage_result);
        // An explicit entry satisfied outside the project is not a model edge.
        let declared: Vec<String> = model
            .config
            .depends_on
            .iter()
            .filter(|d| model_names.contains(*d) || !externals.contains(*d))
            .cloned()
            .collect();
        per_model.push((model.config.name.clone(), declared, auto_deps));
    }

    if !parse_failures.is_empty() {
        // Deterministic order: the same project must produce the same message
        // twice, and a set that changes order between runs is unreadable in CI.
        parse_failures.sort();
        return Err(ResolveError::LineageExtractionMany {
            failures: parse_failures,
        });
    }

    let (dag_nodes, dropped) = settle_dependencies(&per_model);
    for (consumer, producer) in dropped {
        diagnostics.push(dropped_binding_diagnostic(&consumer, &producer));
    }

    Ok((dag_nodes, lineage_cache, diagnostics))
}

/// The model edges the compiler derives from SQL, as `(consumer, producer)`
/// model names, over `models` — which may span several pipelines.
///
/// The same binding [`resolve_dependencies`] derives a model's dependencies
/// with, for a caller that schedules its own graph: `rocky run --dag` passes
/// these to `rocky_core::unified_dag::build_runtime_dag_with_model_edges`
/// instead of matching reads against node labels a second time (#1629).
/// Declared `depends_on` entries are not included; the graph adds those
/// itself. A model whose SQL does not parse derives no edges here — its own
/// compile refuses it. A model name that appears twice is indexed once.
#[must_use]
pub fn derived_model_edges(models: &[Model]) -> Vec<(String, String)> {
    let mut seen_names = HashSet::new();
    let unique: Vec<Model> = models
        .iter()
        .filter(|m| seen_names.insert(m.config.name.clone()))
        .cloned()
        .collect();
    let index = BareReadIndex::new(&unique);
    let mut per_model = Vec::with_capacity(unique.len());
    let mut auto: HashSet<(String, String)> = HashSet::new();
    for model in &unique {
        let deps = match lineage::extract_lineage(&model.sql) {
            Ok(lineage_result) => extract_deps_from_lineage(&lineage_result, model, &index).0,
            Err(_) => Vec::new(),
        };
        for d in &deps {
            auto.insert((model.config.name.clone(), d.producer.clone()));
        }
        per_model.push((
            model.config.name.clone(),
            model.config.depends_on.clone(),
            deps,
        ));
    }
    // Settled exactly as `resolve_dependencies` settles them, so a binding it
    // drops as a cycle-closer is dropped here too. Declared entries are left
    // to the graph.
    let (nodes, _dropped) = settle_dependencies(&per_model);
    nodes
        .into_iter()
        .flat_map(|n| {
            let name = n.name;
            n.depends_on
                .into_iter()
                .map(move |d| (name.clone(), d))
                .collect::<Vec<_>>()
        })
        .filter(|edge| auto.contains(edge))
        .collect()
}

/// Every model a bare read could bind to, indexed for [`BareReadIndex::bind`]
/// (#1354).
///
/// Public so local execution (`rocky_engine::executor`) binds a bare read
/// with the same rule the compile graph was derived with: one spelling, so the
/// two cannot disagree about which model a read reaches.
pub struct BareReadIndex<'a> {
    /// Ephemeral models, by exact name. A bare read of the name is inlined.
    ephemeral: HashSet<&'a str>,
    /// Models that write a table, by folded target table.
    by_table: HashMap<String, Vec<&'a Model>>,
    /// Models that write a table, by exact name. Used only to report `D012`.
    by_name: HashMap<&'a str, &'a Model>,
}

impl<'a> BareReadIndex<'a> {
    /// Index `models` (the whole project).
    #[must_use]
    pub fn new(models: &'a [Model]) -> Self {
        let mut index = BareReadIndex {
            ephemeral: HashSet::new(),
            by_table: HashMap::new(),
            by_name: HashMap::new(),
        };
        for m in models {
            if matches!(
                m.config.strategy,
                rocky_core::models::StrategyConfig::Ephemeral
            ) {
                index.ephemeral.insert(m.config.name.as_str());
            } else {
                index
                    .by_table
                    .entry(fold_identifier(&m.config.target.table))
                    .or_default()
                    .push(m);
                index.by_name.insert(m.config.name.as_str(), m);
            }
        }
        index
    }

    /// What a bare read spelled `read` in `reader` binds to. See the module
    /// docs for the rule.
    #[must_use]
    pub fn bind(&self, read: &str, reader: &Model) -> BareBinding {
        bind_bare_read(read, reader, self)
    }
}

/// What a bare read binds to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BareBinding {
    /// The read is this model's output.
    Model(String),
    /// No model writes a table of that name: the read is external.
    External,
    /// Several models write a table of that name and nothing picks one.
    Ambiguous(Vec<String>),
}

/// Bind a bare read spelled `read` in `reader` (#1354; rules in the module
/// docs). The reader never binds to itself: a model reading a table that has
/// its own target's name reads another schema's table.
fn bind_bare_read(read: &str, reader: &Model, index: &BareReadIndex<'_>) -> BareBinding {
    if index.ephemeral.contains(read) && read != reader.config.name {
        return BareBinding::Model(read.to_string());
    }
    let folded = fold_identifier(read);
    // A model reading a table with its OWN target's name reads itself (an
    // incremental self-read) or a same-named table in another schema the
    // search path decides. Neither is evidence for a sibling that happens to
    // write that name, so the read is external.
    if fold_identifier(&reader.config.target.table) == folded {
        return BareBinding::External;
    }
    let candidates: Vec<&Model> = index
        .by_table
        .get(&folded)
        .map(|ms| {
            ms.iter()
                .copied()
                .filter(|m| m.config.name != reader.config.name)
                .collect()
        })
        .unwrap_or_default();
    match candidates.as_slice() {
        [] => BareBinding::External,
        [only] => BareBinding::Model(only.config.name.clone()),
        several => {
            let named: Vec<&&Model> = several
                .iter()
                .filter(|m| fold_identifier(&m.config.name) == folded)
                .collect();
            if let [one] = named.as_slice() {
                return BareBinding::Model(one.config.name.clone());
            }
            let declared: Vec<&&Model> = several
                .iter()
                .filter(|m| reader.config.depends_on.contains(&m.config.name))
                .collect();
            if let [one] = declared.as_slice() {
                return BareBinding::Model(one.config.name.clone());
            }
            let mut names: Vec<String> = several.iter().map(|m| m.config.name.clone()).collect();
            names.sort();
            BareBinding::Ambiguous(names)
        }
    }
}

/// A finding about one bare read, reported once per reader and read.
#[derive(Debug, Clone, PartialEq, Eq)]
enum BareReadNote {
    /// `D012`: the read matches model `named` by name, but binds elsewhere.
    NameDoesNotBind {
        named: String,
        bound: Option<String>,
    },
    /// `E056`: several models write the table and nothing picks one.
    Ambiguous {
        read: String,
        candidates: Vec<String>,
    },
}

impl BareReadNote {
    fn into_diagnostic(self, reader: &str, index: &BareReadIndex<'_>) -> Diagnostic {
        let spell = |name: &str| {
            index.by_name.get(name).map_or_else(
                || name.to_string(),
                |m| {
                    let t = &m.config.target;
                    if t.catalog.is_empty() {
                        format!("{}.{}", t.schema, t.table)
                    } else {
                        format!("{}.{}.{}", t.catalog, t.schema, t.table)
                    }
                },
            )
        };
        match self {
            BareReadNote::NameDoesNotBind { named, bound } => {
                let target = spell(&named);
                let outcome = match &bound {
                    None => format!(
                        "No model writes a table called '{named}', so Rocky treats the read as \
                         an external table and derives no dependency on model '{named}'"
                    ),
                    Some(other) => format!(
                        "Model '{other}' writes '{}', so the read binds to '{other}' and \
                         derives no dependency on model '{named}'",
                        spell(other)
                    ),
                };
                Diagnostic::warning(
                    "D012",
                    reader,
                    format!(
                        "bare read of '{named}' matches model '{named}' by name, but that \
                         model writes '{target}'. A bare name carries no schema, so the \
                         warehouse resolves it through the search path to a table called \
                         '{named}', and `rocky test` / `rocky ci` bind it the same way. \
                         {outcome}."
                    ),
                )
                .with_suggestion(format!(
                    "Read '{target}' explicitly if you mean model '{named}'s output, or give \
                     model '{named}' a target table called '{named}'"
                ))
            }
            BareReadNote::Ambiguous { read, candidates } => {
                let listed = candidates
                    .iter()
                    .map(|c| format!("'{c}' ({})", spell(c)))
                    .collect::<Vec<_>>()
                    .join(", ");
                Diagnostic::error(
                    crate::diagnostic::E056,
                    reader,
                    format!(
                        "bare read of '{read}' is ambiguous: models {listed} all write a \
                         table called '{read}'. A bare name carries no schema, and Rocky \
                         cannot see the search path that would choose, so it binds none of \
                         them rather than guess"
                    ),
                )
                .with_suggestion(format!(
                    "Qualify the read with its schema (for example '{}'), or list the \
                     intended model in depends_on",
                    candidates
                        .first()
                        .map_or_else(|| read.clone(), |c| spell(c))
                ))
            }
        }
    }
}

/// Extract model dependencies from a pre-computed `LineageResult`.
///
/// Returns the names of the models the reader depends on, deduplicated and in
/// SQL order, excluding self-references, plus a note for each bare read that
/// binds somewhere a reader might not expect (`D012`) or nowhere because it
/// is ambiguous (`E056`). Only a single-part read can bind to a model; a
/// qualified read is external here (#1354; see the module docs).
fn extract_deps_from_lineage(
    lineage_result: &lineage::LineageResult,
    reader: &Model,
    index: &BareReadIndex<'_>,
) -> (Vec<Dep>, Vec<BareReadNote>) {
    let mut deps = Vec::new();
    let mut seen = HashSet::new();
    let mut notes = Vec::new();
    let mut noted = HashSet::new();

    // Reads that live inside a derived table or a `WITH` body (#1867) are
    // dependencies too, and derive edges exactly like a top-level read. They
    // are already lower-cased and stripped of `WITH`-bound names by
    // `extract_lineage`, so the CTE-shadowing rule (#1892) holds for them.
    let top_level = lineage_result.source_tables.iter().filter_map(|table_ref| {
        // A CTE is local to the query and names no object outside it, so a CTE
        // that happens to share a model's name must not derive an edge to it
        // — and two models with mutual local CTE names must not close a cycle
        // that does not exist (#1892).
        match table_ref.binding {
            lineage::TableBinding::Cte => None,
            lineage::TableBinding::Physical => Some(&table_ref.name),
        }
    });
    for name in top_level.chain(lineage_result.nested_sources.iter()) {
        if name.contains('.') {
            continue;
        }
        let binding = bind_bare_read(name, reader, index);
        let bound = match &binding {
            BareBinding::Model(m) => Some(m.clone()),
            BareBinding::External => None,
            BareBinding::Ambiguous(candidates) => {
                if noted.insert(name.clone()) {
                    notes.push(BareReadNote::Ambiguous {
                        read: name.clone(),
                        candidates: candidates.clone(),
                    });
                }
                None
            }
        };
        // D012: the read is a model's NAME, the model is not the binding.
        if let Some(named) = index.by_name.get(name.as_str())
            && named.config.name != reader.config.name
            && bound.as_deref() != Some(name.as_str())
            && !matches!(binding, BareBinding::Ambiguous(_))
            && noted.insert(name.clone())
        {
            notes.push(BareReadNote::NameDoesNotBind {
                named: name.clone(),
                bound: bound.clone(),
            });
        }
        if let Some(dep) = bound
            && dep != reader.config.name
            && seen.insert(dep.clone())
        {
            // Bound by the table alone: the model is not named after the read,
            // the reader did not declare it, and it is not an inlined
            // ephemeral. Weaker evidence than a name that agrees.
            let table_only = fold_identifier(&dep) != fold_identifier(name)
                && !reader.config.depends_on.contains(&dep)
                && !index.ephemeral.contains(dep.as_str());
            deps.push(Dep {
                producer: dep,
                table_only,
            });
        }
    }

    (deps, notes)
}

/// One auto-derived dependency of a reader.
#[derive(Debug, Clone)]
struct Dep {
    producer: String,
    /// Bound by target table alone (see [`extract_deps_from_lineage`]).
    table_only: bool,
}

/// Settle every model's dependencies (#1354).
///
/// `per_model` is `(model, declared depends_on, auto-derived deps)`. Declared
/// entries and auto deps whose name agrees go in first. A table-only binding
/// then joins only if it closes no cycle: a bare name has no schema, so a
/// binding by table alone is a guess about the search path, and a guess must
/// not turn a project that compiled into a refused cycle or reverse an edge
/// stronger evidence set. A dropped binding is returned as
/// `(consumer, producer)` so the caller can report it (D013).
fn settle_dependencies(
    per_model: &[(String, Vec<String>, Vec<Dep>)],
) -> (Vec<DagNode>, Vec<(String, String)>) {
    let mut deps: Vec<(String, Vec<String>)> = per_model
        .iter()
        .map(|(name, declared, auto)| {
            let mut all = declared.clone();
            for d in auto.iter().filter(|d| !d.table_only) {
                if !all.contains(&d.producer) {
                    all.push(d.producer.clone());
                }
            }
            (name.clone(), all)
        })
        .collect();
    let mut dropped = Vec::new();
    for (index, (consumer, _, auto)) in per_model.iter().enumerate() {
        for d in auto.iter().filter(|d| d.table_only) {
            if deps[index].1.contains(&d.producer) {
                continue;
            }
            if depends_transitively(&deps, &d.producer, consumer) {
                dropped.push((consumer.clone(), d.producer.clone()));
            } else {
                deps[index].1.push(d.producer.clone());
            }
        }
    }
    let nodes = deps
        .into_iter()
        .map(|(name, depends_on)| DagNode { name, depends_on })
        .collect();
    (nodes, dropped)
}

/// Whether `from` depends, directly or not, on `target` in `deps`.
fn depends_transitively(deps: &[(String, Vec<String>)], from: &str, target: &str) -> bool {
    let by_name: HashMap<&str, &Vec<String>> = deps.iter().map(|(n, d)| (n.as_str(), d)).collect();
    let mut stack = vec![from];
    let mut seen = HashSet::new();
    while let Some(current) = stack.pop() {
        if current == target {
            return true;
        }
        if !seen.insert(current) {
            continue;
        }
        if let Some(next) = by_name.get(current) {
            stack.extend(next.iter().map(String::as_str));
        }
    }
    false
}

/// The D013 warning for a table-only binding dropped because it closed a
/// cycle.
fn dropped_binding_diagnostic(consumer: &str, producer: &str) -> Diagnostic {
    Diagnostic::warning(
        "D013",
        consumer,
        format!(
            "a bare read in '{consumer}' names the table model '{producer}' writes, but '{producer}' \
             already depends on '{consumer}', so the binding would close a cycle. A bare name has \
             no schema, so Rocky treats this read as an external table and derives no dependency \
             on '{producer}'"
        ),
    )
    .with_suggestion(format!(
        "Qualify the read with its schema, or list '{producer}' in depends_on if '{consumer}' \
         really reads it"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_core::models::{ModelConfig, StrategyConfig, TargetConfig};

    fn make_model(name: &str, sql: &str) -> Model {
        Model {
            drop_existing_kind: None,
            config: ModelConfig {
                name: name.to_string(),
                depends_on: vec![],
                strategy: StrategyConfig::default(),
                target: TargetConfig {
                    catalog: "warehouse".to_string(),
                    schema: "silver".to_string(),
                    table: name.to_string(),
                },
                sources: vec![],
                adapter: None,
                intent: None,
                freshness: None,
                tests: vec![],
                format: None,
                format_options: None,
                classification: Default::default(),
                tags: Default::default(),
                governance: Default::default(),
                retention: None,
                budget: None,
                skip: None,
                name_declared: String::new(),
                target_table_declared: String::new(),
            },
            sql: sql.to_string(),
            file_path: format!("models/{name}.sql").into(),
            contract_path: None,
        }
    }

    /// #1224: every unparseable model is reported, not just the first.
    ///
    /// Resolution still fails as a whole — that contract is unchanged. But a
    /// single unsupported construct can account for most of a real project's
    /// parse failures, and reporting one per compile hides the scale: the user
    /// fixes one, recompiles, and meets the next.
    #[test]
    fn every_unparseable_model_is_reported_not_just_the_first() {
        // Deliberately malformed SQL, not a merely-unsupported construct.
        //
        // This fixture used to be `SELECT * EXCEPT (...)`, which the parser
        // rejected — until #1224 enabled `supports_select_wildcard_except` and
        // it started parsing, silently emptying this test. A test that proves
        // "every failure is reported" must not rest on a syntax gap someone
        // will eventually close; unbalanced parens cannot become valid.
        let models = vec![
            make_model("a", "SELECT ((( FROM raw.t"),
            make_model("ok", "SELECT 1 AS id"),
            make_model("b", "SELECT )))"),
        ];

        let err = resolve_dependencies(&models).expect_err("unparseable SQL must fail resolution");
        let ResolveError::LineageExtractionMany { failures } = &err else {
            panic!("expected the multi-failure variant, got {err:?}");
        };

        let names: Vec<&str> = failures.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(
            names,
            vec!["a", "b"],
            "both failures reported, sorted for a deterministic message; the \
             parseable model contributes nothing"
        );

        let rendered = err.to_string();
        assert!(
            rendered.contains("2 model(s) failed to parse"),
            "{rendered}"
        );
        assert!(
            rendered.contains("- a:") && rendered.contains("- b:"),
            "{rendered}"
        );
    }

    fn make_model_with_deps(name: &str, sql: &str, deps: Vec<&str>) -> Model {
        let mut m = make_model(name, sql);
        m.config.depends_on = deps.into_iter().map(String::from).collect();
        m
    }

    /// A bare name matching a model NAME is a model reference. #1354 asked
    /// whether it should instead match the model's TARGET; the answer is not
    /// this layer's to give (`rocky test` materializes by model name, so both
    /// answers are right on some path), so the rule is unchanged and the
    /// ambiguous case is reported as D012 instead.
    #[test]
    fn test_classify_bare_name_model() {
        let models: HashSet<String> = ["orders", "customers"]
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(
            classify_table_ref("orders", &models),
            TableRefKind::ModelRef("orders".to_string())
        );
    }

    #[test]
    fn test_classify_bare_name_unknown() {
        let models: HashSet<String> = ["orders"].iter().map(ToString::to_string).collect();
        assert_eq!(
            classify_table_ref("unknown_table", &models),
            TableRefKind::RawRef("unknown_table".to_string())
        );
    }

    #[test]
    fn test_classify_two_part() {
        let models: HashSet<String> = HashSet::new();
        assert_eq!(
            classify_table_ref("staging.orders", &models),
            TableRefKind::SourceRef {
                schema: "staging".to_string(),
                table: "orders".to_string(),
            }
        );
    }

    #[test]
    fn test_classify_three_part() {
        let models: HashSet<String> = HashSet::new();
        assert_eq!(
            classify_table_ref("catalog.schema.table", &models),
            TableRefKind::RawRef("catalog.schema.table".to_string())
        );
    }

    #[test]
    fn test_resolve_simple_dependency() {
        let models = vec![
            make_model("orders", "SELECT * FROM raw_orders"),
            make_model("raw_orders", "SELECT * FROM source.fivetran.orders"),
        ];

        let (dag_nodes, _lineage_cache, _diags) = resolve_dependencies(&models).unwrap();

        // orders depends on raw_orders
        let orders_node = dag_nodes.iter().find(|n| n.name == "orders").unwrap();
        assert_eq!(orders_node.depends_on, vec!["raw_orders"]);

        // raw_orders has no model dependencies (source is external)
        let raw_node = dag_nodes.iter().find(|n| n.name == "raw_orders").unwrap();
        assert!(raw_node.depends_on.is_empty());
    }

    #[test]
    fn test_resolve_join_dependencies() {
        let models = vec![
            make_model(
                "customer_orders",
                "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id",
            ),
            make_model("orders", "SELECT * FROM catalog.raw.orders"),
            make_model("customers", "SELECT * FROM catalog.raw.customers"),
        ];

        let (dag_nodes, _lineage_cache, _diags) = resolve_dependencies(&models).unwrap();
        let co_node = dag_nodes
            .iter()
            .find(|n| n.name == "customer_orders")
            .unwrap();

        assert!(co_node.depends_on.contains(&"orders".to_string()));
        assert!(co_node.depends_on.contains(&"customers".to_string()));
        assert_eq!(co_node.depends_on.len(), 2);
    }

    #[test]
    fn test_resolve_external_refs_not_dependencies() {
        let models = vec![make_model(
            "summary",
            "SELECT * FROM warehouse.staging.orders",
        )];

        let (dag_nodes, _lineage_cache, _diags) = resolve_dependencies(&models).unwrap();
        let node = dag_nodes.iter().find(|n| n.name == "summary").unwrap();
        assert!(node.depends_on.is_empty());
    }

    #[test]
    fn test_resolve_merges_explicit_and_auto() {
        let models = vec![
            make_model_with_deps("customer_orders", "SELECT * FROM orders", vec!["extra_dep"]),
            make_model("orders", "SELECT 1"),
            make_model("extra_dep", "SELECT 1"),
        ];

        let (dag_nodes, _lineage_cache, _diags) = resolve_dependencies(&models).unwrap();
        let co_node = dag_nodes
            .iter()
            .find(|n| n.name == "customer_orders")
            .unwrap();

        // Both explicit (extra_dep) and auto-resolved (orders) present
        assert!(co_node.depends_on.contains(&"extra_dep".to_string()));
        assert!(co_node.depends_on.contains(&"orders".to_string()));
    }

    #[test]
    fn test_resolve_no_self_reference() {
        // A model referencing itself should not create a self-dependency
        let models = vec![make_model(
            "orders",
            "SELECT * FROM orders WHERE status = 'active'",
        )];

        let (dag_nodes, _lineage_cache, _diags) = resolve_dependencies(&models).unwrap();
        let node = dag_nodes.iter().find(|n| n.name == "orders").unwrap();
        assert!(node.depends_on.is_empty());
    }

    /// #2138: a `depends_on` entry the caller names as external is left out
    /// of the model DAG, so the topological sort does not refuse it; a model
    /// of the same name keeps its edge; a name nobody vouched for is still an
    /// unknown dependency.
    #[test]
    fn external_depends_on_names_are_satisfied_outside_the_project() {
        let externals = std::collections::BTreeSet::from(["countries".to_string()]);
        let models = vec![make_model_with_deps(
            "stg",
            "SELECT 1 AS id",
            vec!["countries"],
        )];
        let (nodes, _, _) = resolve_dependencies_with_externals(&models, &externals).unwrap();
        assert!(nodes[0].depends_on.is_empty(), "{:?}", nodes[0].depends_on);
        rocky_ir::dag::topological_sort(&nodes).expect("no unknown dependency left");
        assert_eq!(
            models[0].config.depends_on,
            vec!["countries"],
            "the declared config is untouched"
        );

        // Without the external set the same project is refused, as before.
        let (nodes, _, _) = resolve_dependencies(&models).unwrap();
        assert!(matches!(
            rocky_ir::dag::topological_sort(&nodes),
            Err(rocky_ir::dag::DagError::UnknownDependency { .. })
        ));

        // A model carrying the external's name keeps its edge.
        let models = vec![
            make_model_with_deps("stg", "SELECT 1 AS id", vec!["countries"]),
            make_model("countries", "SELECT 1 AS code"),
        ];
        let (nodes, _, _) = resolve_dependencies_with_externals(&models, &externals).unwrap();
        let stg = nodes.iter().find(|n| n.name == "stg").unwrap();
        assert_eq!(stg.depends_on, vec!["countries"]);
    }

    fn make_model_with_target(name: &str, sql: &str, schema: &str, table: &str) -> Model {
        let mut m = make_model(name, sql);
        m.config.target.schema = schema.to_string();
        m.config.target.table = table.to_string();
        m
    }

    /// #1354 step 2: a bare read that matches a model by NAME while that model
    /// writes a differently-named table does not reach it — on a warehouse the
    /// search path finds a table called `customers`, and `rocky test` now
    /// materializes the model at `prod.customers_v2` and binds the same way.
    /// No edge; D012 says what happened.
    #[test]
    fn a_bare_read_of_a_renamed_target_models_name_derives_no_edge_and_warns() {
        let models = vec![
            make_model_with_target("customers", "SELECT 1 AS id", "prod", "customers_v2"),
            make_model("rollup", "SELECT id FROM customers"),
        ];

        let (dag_nodes, _lineage_cache, diags) = resolve_dependencies(&models).unwrap();
        let rollup = dag_nodes.iter().find(|n| n.name == "rollup").unwrap();
        assert!(
            rollup.depends_on.is_empty(),
            "a name match is not a binding: {:?}",
            rollup.depends_on
        );

        let d012: Vec<&Diagnostic> = diags.iter().filter(|d| &*d.code == "D012").collect();
        assert_eq!(d012.len(), 1, "{diags:?}");
        assert_eq!(d012[0].model, "rollup");
        assert!(!d012[0].is_error(), "{:?}", d012[0]);
        assert!(
            d012[0].message.contains("warehouse.prod.customers_v2")
                && d012[0].message.contains("external table"),
            "the warning must name the target and say the read is external: {}",
            d012[0].message
        );
        // A wrapped message literal that loses its `\` continuations reads as
        // one line of run-together spaces; the text is user-facing, so pin it.
        assert!(
            !d012[0].message.contains("  ")
                && !d012[0].suggestion.as_deref().unwrap().contains("  "),
            "{:?} / {:?}",
            d012[0].message,
            d012[0].suggestion
        );
    }

    /// The harm #1354 reported, now fixed: with no name edge, the TRUE
    /// physical-read edge is not a cycle-closer, so the run-time derivation
    /// keeps it and `rollup` runs before the model that reads it.
    ///
    /// ```text
    ///   before: rollup --(name match)--► customers;  customers ► rollup SKIPPED
    ///   after:  customers --(reads warehouse.silver.rollup)--► rollup  KEPT
    /// ```
    #[test]
    fn the_true_physical_read_edge_is_no_longer_suppressed() {
        let models = vec![
            make_model_with_target(
                "customers",
                "SELECT y FROM warehouse.silver.rollup",
                "prod",
                "customers_v2",
            ),
            make_model("rollup", "SELECT x FROM customers"),
        ];

        let (dag_nodes, _lineage_cache, _diags) = resolve_dependencies(&models).unwrap();
        let existing: Vec<(String, String)> = dag_nodes
            .iter()
            .flat_map(|n| {
                n.depends_on
                    .iter()
                    .map(|d| (n.name.clone(), d.clone()))
                    .collect::<Vec<_>>()
            })
            .collect();
        assert!(existing.is_empty(), "{existing:?}");

        let edge_models: Vec<rocky_core::physical_edges::PhysicalEdgeModel<'_>> = models
            .iter()
            .map(rocky_core::physical_edges::PhysicalEdgeModel::from_model)
            .collect();
        let derived = rocky_core::physical_edges::derive_physical_edges(&edge_models, &existing);
        assert_eq!(
            derived.edges,
            vec![("customers".to_string(), "rollup".to_string())],
            "{derived:?}"
        );
        assert!(derived.skipped_cycle_edges.is_empty(), "{derived:?}");
    }

    /// #1354 step 2: a bare read binds to the model that WRITES that table,
    /// whatever the model is called, and says nothing — that is the binding
    /// working, not an ambiguity.
    #[test]
    fn a_bare_read_binds_to_the_model_that_writes_the_table() {
        let models = vec![
            make_model_with_target("stg_events", "SELECT 2 AS id", "z", "events"),
            make_model("summary", "SELECT id FROM events"),
        ];
        let (dag_nodes, _, diags) = resolve_dependencies(&models).unwrap();
        let summary = dag_nodes.iter().find(|n| n.name == "summary").unwrap();
        assert_eq!(summary.depends_on, vec!["stg_events"]);
        assert!(diags.is_empty(), "{diags:?}");
    }

    /// #2045 shape 3: two models write a table called `events` in different
    /// schemas. The one also NAMED `events` is the binding, whatever order the
    /// schemas sort in.
    #[test]
    fn several_writers_of_one_table_bind_to_the_one_named_after_it() {
        for (first, second) in [("a", "z"), ("z", "a")] {
            let models = vec![
                make_model_with_target("events", "SELECT 2 AS id", first, "events"),
                make_model_with_target("other", "SELECT 1 AS id", second, "events"),
                make_model_with_target("summary", "SELECT id FROM events", "marts", "summary"),
            ];
            let (dag_nodes, _, diags) = resolve_dependencies(&models).unwrap();
            let summary = dag_nodes.iter().find(|n| n.name == "summary").unwrap();
            assert_eq!(summary.depends_on, vec!["events"], "{first}/{second}");
            assert!(diags.is_empty(), "{diags:?}");
        }
    }

    /// Several writers, none named after the table: `depends_on` picks one,
    /// and with nothing to pick the read is refused as E056 and binds to none.
    #[test]
    fn an_ambiguous_bare_read_is_refused_unless_depends_on_picks_one() {
        let writers = || {
            vec![
                make_model_with_target("stg_a", "SELECT 1 AS id", "a", "events"),
                make_model_with_target("stg_z", "SELECT 2 AS id", "z", "events"),
            ]
        };

        let mut models = writers();
        models.push(make_model("summary", "SELECT id FROM events"));
        let (dag_nodes, _, diags) = resolve_dependencies(&models).unwrap();
        let summary = dag_nodes.iter().find(|n| n.name == "summary").unwrap();
        assert!(summary.depends_on.is_empty(), "{:?}", summary.depends_on);
        let e056: Vec<&Diagnostic> = diags.iter().filter(|d| &*d.code == "E056").collect();
        assert_eq!(e056.len(), 1, "{diags:?}");
        assert!(e056[0].is_error(), "{:?}", e056[0]);
        assert_eq!(e056[0].model, "summary");
        assert!(
            e056[0].message.contains("'stg_a' (warehouse.a.events)")
                && e056[0].message.contains("'stg_z' (warehouse.z.events)")
                && !e056[0].message.contains("  "),
            "{}",
            e056[0].message
        );

        let mut models = writers();
        models.push(make_model_with_deps(
            "summary",
            "SELECT id FROM events",
            vec!["stg_z"],
        ));
        let (dag_nodes, _, diags) = resolve_dependencies(&models).unwrap();
        let summary = dag_nodes.iter().find(|n| n.name == "summary").unwrap();
        assert_eq!(summary.depends_on, vec!["stg_z"]);
        assert!(diags.iter().all(|d| &*d.code != "E056"), "{diags:?}");
    }

    /// A model that reads a table with its own target's name reads itself or
    /// a same-named table the search path picks — never evidence for a
    /// sibling that writes that name. The read is external.
    #[test]
    fn a_read_of_the_readers_own_table_name_is_external() {
        let models = vec![
            make_model_with_target("stg_orders", "SELECT id FROM orders", "staging", "orders"),
            make_model_with_target("raw_orders", "SELECT 1 AS id", "raw", "orders"),
        ];
        let (dag_nodes, _, diags) = resolve_dependencies(&models).unwrap();
        let stg = dag_nodes.iter().find(|n| n.name == "stg_orders").unwrap();
        assert!(stg.depends_on.is_empty(), "{:?}", stg.depends_on);
        assert!(diags.is_empty(), "{diags:?}");

        let models = vec![make_model_with_target(
            "stg_orders",
            "SELECT id FROM orders",
            "staging",
            "orders",
        )];
        let (dag_nodes, _, _) = resolve_dependencies(&models).unwrap();
        assert!(dag_nodes[0].depends_on.is_empty());
    }

    /// A binding by table alone that would close a cycle is dropped with
    /// D013, not refused: `stg_customers` reads the raw `customers` source,
    /// and `dim_customers` — which reads `stg_customers` by name — happens to
    /// write a table called `customers`. Compiled before; must still compile.
    #[test]
    fn a_table_only_binding_that_closes_a_cycle_is_dropped_with_d013() {
        let models = vec![
            make_model_with_target(
                "stg_customers",
                "SELECT id FROM customers",
                "staging",
                "stg_customers",
            ),
            make_model_with_target(
                "dim_customers",
                "SELECT id FROM stg_customers",
                "marts",
                "customers",
            ),
        ];
        let (dag_nodes, _, diags) = resolve_dependencies(&models).unwrap();
        rocky_ir::dag::topological_sort(&dag_nodes).expect("no cycle");
        let deps = |n: &str| {
            dag_nodes
                .iter()
                .find(|d| d.name == n)
                .unwrap()
                .depends_on
                .clone()
        };
        assert_eq!(deps("dim_customers"), vec!["stg_customers"]);
        assert!(deps("stg_customers").is_empty());
        let d013: Vec<&Diagnostic> = diags.iter().filter(|d| &*d.code == "D013").collect();
        assert_eq!(d013.len(), 1, "{diags:?}");
        assert_eq!(d013[0].model, "stg_customers");
        assert!(!d013[0].is_error() && !d013[0].message.contains("  "));
        assert_eq!(
            derived_model_edges(&models),
            vec![("dim_customers".to_string(), "stg_customers".to_string())],
            "`run --dag` settles the same way"
        );
    }

    /// An ephemeral model writes nothing; the inliner rewrites a bare read of
    /// its NAME. So it binds by name, and a model that merely shares its
    /// nominal target table is not a candidate for it.
    #[test]
    fn an_ephemeral_model_binds_by_name_and_never_by_table() {
        let mut eph = make_model_with_target("eph", "SELECT 2 AS v", "s", "t");
        eph.config.strategy = StrategyConfig::Ephemeral;
        let models = vec![
            make_model_with_target("real", "SELECT 1 AS v", "s", "t"),
            eph,
            make_model("reads_eph", "SELECT v FROM eph"),
            make_model("reads_t", "SELECT v FROM t"),
        ];
        let (dag_nodes, _, diags) = resolve_dependencies(&models).unwrap();
        let deps = |n: &str| {
            dag_nodes
                .iter()
                .find(|d| d.name == n)
                .unwrap()
                .depends_on
                .clone()
        };
        assert_eq!(deps("reads_eph"), vec!["eph"]);
        assert_eq!(deps("reads_t"), vec!["real"]);
        assert!(diags.is_empty(), "{diags:?}");
    }

    /// The common case — a model whose target table is its own name — must
    /// keep its edge and must NOT warn. This is the noise guard: D012 has to
    /// stay rare or it means nothing.
    #[test]
    fn the_common_case_binds_without_a_warning() {
        let models = vec![
            make_model("customers", "SELECT 1 AS id"),
            make_model("rollup", "SELECT id FROM customers"),
        ];

        let (dag_nodes, _lineage_cache, diags) = resolve_dependencies(&models).unwrap();
        let rollup = dag_nodes.iter().find(|n| n.name == "rollup").unwrap();
        assert_eq!(rollup.depends_on, vec!["customers"]);
        assert!(
            diags.iter().all(|d| &*d.code != "D012"),
            "the common case must not warn: {diags:?}"
        );
    }

    /// The comparison folds case on both sides. A project that spells its
    /// targets in upper case (`[target] table = "CUSTOMERS"` for model
    /// `customers` — the ordinary Snowflake shape) is the SAME name, not a
    /// renamed target, and must not warn. Without the fold, D012 would fire on
    /// every model in such a project.
    #[test]
    fn a_target_table_spelled_in_another_case_does_not_warn() {
        let models = vec![
            make_model_with_target("customers", "SELECT 1 AS id", "silver", "CUSTOMERS"),
            make_model("rollup", "SELECT id FROM customers"),
        ];

        let (dag_nodes, _lineage_cache, diags) = resolve_dependencies(&models).unwrap();
        let rollup = dag_nodes.iter().find(|n| n.name == "rollup").unwrap();
        assert_eq!(rollup.depends_on, vec!["customers"]);
        assert!(diags.iter().all(|d| &*d.code != "D012"), "{diags:?}");
    }

    /// A model reading its OWN name gets no edge (self-references are
    /// excluded), so there is no edge to qualify and nothing to warn about.
    #[test]
    fn a_self_read_of_a_renamed_target_name_does_not_warn() {
        let models = vec![make_model_with_target(
            "customers",
            "SELECT id FROM customers WHERE status = 'active'",
            "prod",
            "customers_v2",
        )];

        let (dag_nodes, _lineage_cache, diags) = resolve_dependencies(&models).unwrap();
        assert!(dag_nodes[0].depends_on.is_empty());
        assert!(
            diags.iter().all(|d| &*d.code != "D012"),
            "a self-read has no edge to qualify: {diags:?}"
        );
    }

    #[test]
    fn test_resolve_deduplicates() {
        // orders referenced twice in SQL (FROM + JOIN) should appear once
        let models = vec![
            make_model(
                "summary",
                "SELECT a.id, b.name FROM orders a JOIN orders b ON a.id = b.id",
            ),
            make_model("orders", "SELECT 1 AS id, 'test' AS name"),
        ];

        let (dag_nodes, _lineage_cache, _diags) = resolve_dependencies(&models).unwrap();
        let node = dag_nodes.iter().find(|n| n.name == "summary").unwrap();
        assert_eq!(node.depends_on, vec!["orders"]);
    }

    /// #1892: a CTE named after a model must derive no edge. The reader never
    /// reads the model — the `WITH` clause shadows the name for the whole
    /// query — so ordering it after the model is an invented dependency.
    #[test]
    fn a_cte_named_after_a_model_derives_no_edge() {
        let models = vec![
            make_model("orders", "SELECT 1 AS id"),
            make_model(
                "cte_reader",
                "WITH orders AS (SELECT 2 AS id) SELECT id FROM orders",
            ),
        ];

        let (dag_nodes, _lineage_cache, _diags) = resolve_dependencies(&models).unwrap();
        let reader = dag_nodes.iter().find(|n| n.name == "cte_reader").unwrap();
        assert!(
            reader.depends_on.is_empty(),
            "the CTE shadows the model name, so there is nothing to depend on: {:?}",
            reader.depends_on
        );
    }

    /// The worst form of the same defect, and the one that made it a refusal
    /// rather than a mis-ordering: two models with mutual local CTE names have
    /// no dependency in either direction, but the invented edges close a cycle
    /// and `topological_sort` refuses the project outright (#1892).
    #[test]
    fn mutual_cte_names_do_not_close_a_cycle() {
        let models = vec![
            make_model("alpha", "WITH beta AS (SELECT 1 AS id) SELECT id FROM beta"),
            make_model(
                "beta",
                "WITH alpha AS (SELECT 2 AS id) SELECT id FROM alpha",
            ),
        ];

        let (dag_nodes, _lineage_cache, _diags) = resolve_dependencies(&models).unwrap();
        for node in &dag_nodes {
            assert!(
                node.depends_on.is_empty(),
                "model '{}' has an invented dependency: {:?}",
                node.name,
                node.depends_on
            );
        }
        rocky_ir::dag::topological_sort(&dag_nodes)
            .expect("neither model reads the other, so the project must compile");
    }

    /// The guard against over-suppressing. A query that binds a CTE and ALSO
    /// reads a real model must keep the real edge — the fix shadows the bound
    /// name, not the whole `FROM` clause.
    #[test]
    fn a_query_with_a_cte_keeps_its_real_read_edge() {
        let models = vec![
            make_model("orders", "SELECT 1 AS id"),
            make_model(
                "mixed",
                "WITH c AS (SELECT 1 AS id) SELECT c.id FROM c JOIN orders USING (id)",
            ),
        ];

        let (dag_nodes, _lineage_cache, _diags) = resolve_dependencies(&models).unwrap();
        let mixed = dag_nodes.iter().find(|n| n.name == "mixed").unwrap();
        assert_eq!(mixed.depends_on, vec!["orders"]);
    }

    /// #1867: a read inside a derived table derives the edge. Before this the
    /// pair shared an execution layer, so `--parallel` raced them and a serial
    /// run got its order from the alphabetical root walk.
    ///
    /// The reader is named to sort FIRST, because that is what makes the
    /// assertion mean something: without the edge the walk would put it first.
    #[test]
    fn a_subquery_read_derives_the_edge() {
        let models = vec![
            make_model("orders", "SELECT 1 AS id"),
            make_model("a_reader", "SELECT id FROM (SELECT id FROM orders) AS s"),
        ];

        let (dag_nodes, _lineage_cache, _diags) = resolve_dependencies(&models).unwrap();
        let reader = dag_nodes.iter().find(|n| n.name == "a_reader").unwrap();
        assert_eq!(reader.depends_on, vec!["orders"]);

        let order = rocky_ir::dag::topological_sort(&dag_nodes).unwrap();
        let pos = |name: &str| order.iter().position(|n| n == name).unwrap();
        assert!(
            pos("orders") < pos("a_reader"),
            "the producer must run first: {order:?}"
        );
    }

    /// The same for a read inside a `WITH` body.
    #[test]
    fn a_cte_body_read_derives_the_edge() {
        let models = vec![
            make_model("orders", "SELECT 1 AS id"),
            make_model(
                "a_reader",
                "WITH c AS (SELECT id FROM orders) SELECT id FROM c",
            ),
        ];

        let (dag_nodes, _lineage_cache, _diags) = resolve_dependencies(&models).unwrap();
        let reader = dag_nodes.iter().find(|n| n.name == "a_reader").unwrap();
        assert_eq!(reader.depends_on, vec!["orders"]);
    }

    /// #1892's rule survives one level down: a name a `WITH` clause bound is
    /// not a table read wherever it appears, including inside another body.
    #[test]
    fn a_shadowed_name_inside_a_body_still_derives_no_edge() {
        let models = vec![
            make_model("orders", "SELECT 1 AS id"),
            make_model(
                "a_reader",
                "WITH orders AS (SELECT 2 AS id), \
                 c AS (SELECT id FROM orders) \
                 SELECT id FROM c",
            ),
        ];

        let (dag_nodes, _lineage_cache, _diags) = resolve_dependencies(&models).unwrap();
        let reader = dag_nodes.iter().find(|n| n.name == "a_reader").unwrap();
        assert!(
            reader.depends_on.is_empty(),
            "the body reads the CTE above it, not the model: {:?}",
            reader.depends_on
        );
    }

    /// The one user-visible REFUSAL this change introduces, decided
    /// deliberately rather than discovered later.
    ///
    /// Two models that genuinely read each other through subqueries compiled
    /// before #1867 — one execution layer, silently mis-ordered — because
    /// neither read derived an edge. Now both edges exist and they close a
    /// cycle, so `topological_sort` refuses the project.
    ///
    /// That is correct: it IS a cycle, and Rocky was running it in whatever
    /// order the alphabetical root walk produced. But it is a divergence worth
    /// naming — `augment_physical_read_edges` guarantees the opposite for its
    /// own derivation ("this recompute cannot turn a compiling project into a
    /// refused one"), because it skips cycle-closing candidates and warns.
    /// Compile-time has no such guard and does not want one: a compile-time
    /// cycle is a project error, not a scheduling hint.
    #[test]
    fn mutual_subquery_reads_are_refused_as_the_cycle_they_are() {
        let models = vec![
            make_model("alpha", "SELECT id FROM (SELECT id FROM beta) AS s"),
            make_model("beta", "SELECT id FROM (SELECT id FROM alpha) AS s"),
        ];

        let (dag_nodes, _lineage_cache, _diags) = resolve_dependencies(&models).unwrap();
        assert_eq!(
            dag_nodes
                .iter()
                .find(|n| n.name == "alpha")
                .unwrap()
                .depends_on,
            vec!["beta"]
        );
        let err = rocky_ir::dag::topological_sort(&dag_nodes)
            .expect_err("the two models really do read each other");
        assert!(
            format!("{err}").contains("circular"),
            "and it is reported as a cycle: {err}"
        );
    }

    /// A model reading its own name from inside a subquery is still a
    /// self-reference and still gets no edge — the nested path applies the
    /// same exclusion the top-level one does, or the model would depend on
    /// itself and `topological_sort` would refuse the project.
    #[test]
    fn a_nested_self_read_derives_no_edge() {
        let models = vec![make_model(
            "orders",
            "SELECT id FROM (SELECT id FROM orders) AS s",
        )];

        let (dag_nodes, _lineage_cache, _diags) = resolve_dependencies(&models).unwrap();
        assert!(dag_nodes[0].depends_on.is_empty());
        rocky_ir::dag::topological_sort(&dag_nodes).expect("a self-read must not close a cycle");
    }
}
