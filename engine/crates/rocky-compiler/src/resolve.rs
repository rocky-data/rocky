//! Automatic dependency resolution from SQL table references.
//!
//! Extracts table references from model SQL and classifies them as:
//! - **Model refs** — bare names matching another model in the project
//! - **Physical model refs** — qualified names matching a model's declared
//!   `[target]`, i.e. a read of an in-project model by the physical table it
//!   writes
//! - **Source refs** — two-part qualified names matching no model target
//! - **Raw refs** — three-part (or unresolvable) names matching no model target
//!
//! Model refs and physical model refs become DAG edges. Source and raw refs are
//! external dependencies.
//!
//! # Why physical reads must produce an edge
//!
//! Auto-derivation used to fire only on a **bare single-part** name matching a
//! model *name*, so `SELECT id FROM main.orders` produced no edge to the model
//! that writes `main.orders`. Producer and consumer then landed in the same
//! `execution_layers` layer and, under `--parallel N` on an adapter reporting
//! `supports_concurrent_execution()`, ran at the same time — the consumer
//! reading a table its producer had not written yet. Silent wrong data with a
//! green run, and nothing shadow-specific about it (#1275).
//!
//! # Boundary — one compiled model set, and one of two graphs
//!
//! [`resolve_dependencies`] sees exactly the models handed to one
//! [`crate::project::Project`], which is one pipeline's model set. That covers
//! a plain `rocky run`, `rocky compile`, `rocky plan` and the LSP, all of which
//! schedule from `Project::layers`.
//!
//! `rocky run --dag` does **not**. It schedules phases from a
//! `rocky_core::unified_dag::UnifiedDag` built across every pipeline, and each
//! node is a separate model-scoped sub-run — so a `Project` corrected here
//! cannot order a *different* concurrently dispatched node. That graph derives
//! the same edges through
//! `rocky_core::unified_dag::infer_runtime_dependencies`, which was taught the
//! same physical-target resolution (#1275) and, because it spans pipelines,
//! also orders a physical read *across* two transformation pipelines that this
//! resolver cannot see. The two are separate graphs on purpose; changing the
//! rule here without changing it there leaves `--dag` co-scheduling.
//!
//! # Known limit — the read set is only as complete as lineage extraction
//!
//! Edges are derived from `rocky_sql::lineage::extract_lineage`, which
//! enumerates the top-level `FROM`/`JOIN` relations. A physical read buried in
//! a CTE body, a derived table or an `EXISTS` sub-query is not in that list, so
//! it yields no edge — exactly as a *bare* upstream name in the same position
//! has never yielded one. This is a pre-existing limit of the read-set walk,
//! not of the resolution added for #1275, and it is why
//! `rocky_sql::lineage_complete::lineage_is_provably_complete` exists and why
//! `rocky_cli::commands::containment` fails closed on any model it rejects.
//! Declare such an upstream in `depends_on` for a hard ordering guarantee.

use std::collections::{HashMap, HashSet};

use rocky_core::models::Model;
use rocky_ir::dag::DagNode;
use rocky_sql::identifier::canonicalize_identifier;
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
    /// A qualified reference naming the physical `[target]` of one or more
    /// project models — a read of an in-project model by the table it writes
    /// (`main.orders`) rather than by its model name.
    ///
    /// More than one producer when the reference cannot distinguish between
    /// them; see [`ProducerIndex::producers_of`] for when that happens and why
    /// every one of them becomes an edge.
    PhysicalModelRef(Vec<ProducerMatch>),
    /// Two-part qualified reference (`schema.table`) matching no model target —
    /// external source.
    SourceRef { schema: String, table: String },
    /// Three-part fully qualified reference (`catalog.schema.table`) matching no
    /// model target — external.
    RawRef(String),
}

/// Maps a physical `[target]` back to the model(s) that write it, so a read
/// spelled as a warehouse table resolves to a producer.
///
/// Keyed on `(schema, table)` only, with the catalog kept per producer and
/// applied as a *filter* rather than part of the key, because a 2-part read
/// carries no catalog at all: `main.orders` binds to whatever catalog the
/// session defaults to at execution time, which the compiler cannot observe.
/// Refusing the edge on that uncertainty would reproduce exactly the bug this
/// index exists to fix, so an unqualified read matches every producer whose
/// `(schema, table)` matches, in any catalog.
///
/// # Identity folds case, unconditionally
///
/// Every key and the catalog filter are ASCII-lowercased by
/// [`canonicalize_identifier`], with no dialect [`IdentifierCaseRules`]
/// consulted. That is deliberate and matches
/// [`rocky_sql::defer::CollisionIdentity`], whose doc records the reasoning:
/// the question here is *"could this reference and that target be the same
/// warehouse object?"*, whether case separates two objects is **connection**
/// state Rocky cannot observe (`QUOTED_IDENTIFIERS_IGNORE_CASE`, a BigQuery
/// `is_case_insensitive` dataset), and answering "different" when they are in
/// fact one object is the unrecoverable direction. Read that doc before making
/// this case-aware.
///
/// [`IdentifierCaseRules`]: rocky_sql::defer::IdentifierCaseRules
pub struct ProducerIndex {
    /// `(schema, table)` → `[(model name, catalog)]`, all folded lowercase.
    by_schema_table: HashMap<(String, String), Vec<(String, String)>>,
}

impl ProducerIndex {
    /// Index every model that actually materializes a table.
    ///
    /// `Ephemeral` models are excluded, for the same reason #1291 excludes them
    /// from duplicate-target detection: they carry a fully populated but
    /// phantom `[target]` and materialize nothing, so there is no write for a
    /// read of that name to race with. (A bare *model-name* reference to an
    /// ephemeral model still produces an edge through the
    /// [`TableRefKind::ModelRef`] arm, which is unchanged.)
    #[must_use]
    pub fn build(models: &[Model]) -> Self {
        let mut by_schema_table: HashMap<(String, String), Vec<(String, String)>> = HashMap::new();
        for model in models {
            if matches!(
                model.config.strategy,
                rocky_core::models::StrategyConfig::Ephemeral
            ) {
                continue;
            }
            let t = &model.config.target;
            by_schema_table
                .entry((t.schema.to_lowercase(), t.table.to_lowercase()))
                .or_default()
                .push((model.config.name.clone(), t.catalog.to_lowercase()));
        }
        Self { by_schema_table }
    }

    /// Every model whose `[target]` the spelled reference `read` could name, or
    /// `None` when it names no producer.
    ///
    /// Matching is tail-aligned on `(schema, table)`, then filtered by catalog:
    ///
    /// * A **2-part** `schema.table` read matches producers in *any* catalog.
    ///   The read binds to the session default catalog at runtime, so this
    ///   cannot be narrowed statically.
    /// * A **3-part** `catalog.schema.table` read matches a producer whose
    ///   catalog is the same, **or is empty**. An empty configured catalog
    ///   means the model materializes at `schema.table` in the session default
    ///   catalog — again unobservable here — so it must be assumed to be the
    ///   named one.
    ///
    /// Returning several producers is not an error state to be refused. A
    /// reference that cannot be narrowed to one producer still has to be
    /// ordered after *all* of them: emitting no edge because the answer was
    /// plural is the same silent co-scheduling this exists to prevent. Two
    /// shapes produce a plural answer — several models declaring one target
    /// (which #1291 separately reports as an **E036** error and excludes from
    /// execution), and several models in *different* catalogs sharing a
    /// `(schema, table)` that a 2-part read cannot choose between (not an
    /// error: those are distinct objects). Because the second shape is
    /// legitimate and reaches past #1291's boundary, edge-to-all-producers is
    /// justified on its own and does **not** lean on E036.
    ///
    /// Returns `None` for an identity [`canonicalize_identifier`] refuses
    /// (unbalanced quotes, an empty segment, a quoted segment with an embedded
    /// dot) — such a read yields no edge here. That is a fail-*open* corner
    /// this cannot close: with no parse there is no producer to order against.
    /// `rocky_cli::commands::containment` resolves the same reads through the
    /// same canonicalizer and fails *closed* on `None`, which is where a
    /// stale-read hazard from an unparseable identity is actually caught.
    #[must_use]
    pub fn producers_of(&self, read: &str) -> Option<Vec<ProducerMatch>> {
        let parts = canonicalize_identifier(read)?;
        let (catalog, schema, table) = match parts.as_slice() {
            [schema, table] => (None, schema, table),
            [catalog, schema, table] => (Some(catalog), schema, table),
            _ => return None,
        };
        let candidates = self.by_schema_table.get(&(schema.clone(), table.clone()))?;
        let matched: Vec<ProducerMatch> = candidates
            .iter()
            .filter_map(|(name, producer_catalog)| {
                let exact = match catalog {
                    // A catalog-less read cannot name a catalog, so no producer
                    // is an exact match — every one of them is a guess.
                    None => false,
                    Some(read_catalog) => {
                        if producer_catalog == read_catalog {
                            true
                        } else if producer_catalog.is_empty() {
                            // Wildcard: the producer's catalog is the session
                            // default, which is unobservable here.
                            false
                        } else {
                            return None;
                        }
                    }
                };
                Some(ProducerMatch {
                    model: name.clone(),
                    catalog: producer_catalog.clone(),
                    exact,
                })
            })
            .collect();
        (!matched.is_empty()).then_some(matched)
    }
}

/// One model whose `[target]` a spelled reference could name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerMatch {
    /// The producing model's name.
    pub model: String,
    /// That model's configured catalog, folded lowercase. Empty when the model
    /// declares none — it then materializes in the session default catalog.
    ///
    /// Carried so a reader can tell a producer that shares its *exact* target
    /// identity (a #1291 duplicate-target collision) from one that merely
    /// tail-matches in a different catalog (a distinct object). Collapsing
    /// those two is what made an earlier revision drop a real producer.
    pub catalog: String,
    /// The reference named this producer's catalog explicitly and it matched.
    ///
    /// `false` for a catalog-less 2-part read and for the empty-catalog
    /// wildcard — both of which resolve against the session default catalog,
    /// which the compiler cannot observe, so the match is a deliberate
    /// over-approximation rather than a proven identity. Used only to rank
    /// edges when a cycle forces one to be dropped: a proven edge must never
    /// be sacrificed to keep a speculative one.
    pub exact: bool,
}

/// Errors during dependency resolution.
#[derive(Debug, Error)]
pub enum ResolveError {
    #[error("failed to extract lineage from model '{model}': {reason}")]
    LineageExtraction { model: String, reason: String },
}

/// Classify a table reference name based on its structure, the known model
/// names, and the physical targets those models write.
///
/// Rules:
/// - Bare name matching a known model → `ModelRef`
/// - Bare name NOT matching a model → `RawRef` (unknown external table)
/// - Qualified name matching one or more model `[target]`s →
///   `PhysicalModelRef`
/// - Two-part `schema.table` matching no target → `SourceRef`
/// - Three-part (or longer/unparseable) matching no target → `RawRef`
///
/// # Precedence: a model target wins over a source
///
/// A qualified reference is tested against the producer index *before* falling
/// back to `SourceRef`, so a `schema.table` that a project model writes is a
/// model dependency even if the project also declares it as a source. That
/// ordering is the safe one and not merely a tie-break: if a model writes that
/// table during this run, a reader of it must be ordered after the write
/// whatever else the table is also called. A `schema.table` matching no model
/// target is unaffected and still classifies as `SourceRef`.
///
/// # The bare-name arm deliberately does not consult the producer index
///
/// A 1-part name is *not* matched against producers' target **tables**, only
/// against model names, which is what it already did. Bare names share a
/// namespace with CTE aliases — `rocky_sql::lineage` does not filter `WITH`
/// names out of `source_tables` — so resolving them against target tables
/// would manufacture edges from CTE names that merely collide with some
/// model's output table. `rocky_cli::commands::containment` *does* take that
/// bare-name lookup, and correctly so: over-containing a reader of a failed
/// producer costs a withheld model, whereas a spurious edge here would perturb
/// scheduling for every healthy run and can close a cycle. The two differ on
/// purpose.
pub fn classify_table_ref(
    name: &str,
    model_names: &HashSet<String>,
    producers: &ProducerIndex,
) -> TableRefKind {
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
        2 => producers.producers_of(name).map_or_else(
            || TableRefKind::SourceRef {
                schema: parts[0].to_string(),
                table: parts[1].to_string(),
            },
            TableRefKind::PhysicalModelRef,
        ),
        _ => producers.producers_of(name).map_or_else(
            // 3+ parts naming no project target — fully qualified external
            || TableRefKind::RawRef(name.to_string()),
            TableRefKind::PhysicalModelRef,
        ),
    }
}

/// Resolve dependencies for all models by parsing their SQL.
///
/// Returns `DagNode` entries with `depends_on` auto-populated from SQL table
/// refs that match another model — by **model name** (`FROM orders`) or by the
/// **physical target** that model writes (`FROM main.orders`) — along with a
/// cache of `LineageResult` per model (keyed by model name) so downstream
/// phases can reuse the parsed lineage without re-parsing SQL.
///
/// If a model already has explicit `depends_on` in its config, those are
/// preserved and merged with auto-resolved dependencies.
///
/// # Physical edges never make a project unloadable
///
/// Derived physical edges are added only while the graph stays acyclic. The
/// resolution of a physical read is deliberately generous — a 2-part read
/// matches producers in any catalog — so it can pair two models into a cycle
/// that the declared graph does not have, and `Project::from_models` turns a
/// cycle into a hard `ProjectError`. #1291 recorded why that shape is wrong
/// here: a construction failure "is swallowed into silence by every tolerant
/// caller — `ci-diff` logs at `debug!` and falls back to filename-stem
/// classification, and the LSP's `Err` arm publishes no diagnostic at all."
/// So an edge that would close a cycle is dropped and reported as a **D012**
/// warning instead, mirroring `ContainmentLedger::augmented_layers`, which
/// reports unorderable models rather than failing. A project that loads today
/// still loads.
///
/// A dropped edge leaves that one pair co-schedulable — the #1275 hazard — which
/// is why it is reported rather than silently discarded. It is a warning and not
/// an error because the over-matching that can manufacture the cycle is
/// deliberate: making it an error would refuse legitimate multi-catalog
/// projects that run correctly today.
///
/// # Errors
///
/// Returns [`ResolveError::LineageExtraction`] when a model's SQL cannot be
/// parsed for lineage.
pub fn resolve_dependencies(models: &[Model]) -> Result<ResolveOutput, ResolveError> {
    let model_names: HashSet<String> = models.iter().map(|m| m.config.name.clone()).collect();
    let producers = ProducerIndex::build(models);
    let mut dag_nodes = Vec::with_capacity(models.len());
    let mut lineage_cache = HashMap::with_capacity(models.len());
    let mut diagnostics = Vec::new();
    // Every physical edge, applied after the loop so the acyclicity guard sees
    // the whole declared+bare graph first.
    let mut physical_edges: Vec<PhysicalEdge> = Vec::new();

    for model in models {
        let lineage_result = lineage::extract_lineage(&model.sql).map_err(|reason| {
            ResolveError::LineageExtraction {
                model: model.config.name.clone(),
                reason,
            }
        })?;

        let derived = extract_deps_from_lineage(&lineage_result, model, &model_names, &producers);

        // D011: warn when explicit depends_on is non-empty but misses auto-derived deps
        if !model.config.depends_on.is_empty() {
            let explicit: HashSet<&str> = model
                .config
                .depends_on
                .iter()
                .map(std::string::String::as_str)
                .collect();
            let missing: Vec<&str> = derived
                .by_name
                .iter()
                .map(std::string::String::as_str)
                .chain(derived.by_target.iter().map(|d| d.producer.as_str()))
                .filter(|d| !explicit.contains(d))
                .collect();
            if !missing.is_empty() {
                let missing_str = missing.join(", ");
                diagnostics.push(
                    Diagnostic::warning(
                        "D011",
                        &model.config.name,
                        format!(
                            "depends_on declares [{}] but SQL body also references [{}]. \
                             The auto-derived dependencies will be merged, but consider \
                             updating depends_on or removing it to let auto-derivation \
                             handle everything.",
                            model.config.depends_on.join(", "),
                            missing_str,
                        ),
                    )
                    // Auto-derivation covers a bare model name and a physical
                    // read of a model target within THIS compiled model set. It
                    // does not reach a target owned by another pipeline's model
                    // set (see the module docs), nor a reference
                    // `rocky_sql::identifier::canonicalize_identifier` cannot
                    // parse. Keep `depends_on` for those.
                    .with_suggestion(format!(
                        "Add '{missing_str}' to depends_on, or remove the depends_on field \
                         entirely — auto-derivation covers bare model names and physical \
                         reads of a model target within this pipeline's model set, so keep \
                         depends_on for a cross-pipeline upstream",
                    )),
                );
            }
        }

        // Merge: explicit depends_on + auto-resolved by name, deduplicated.
        // Physical edges are appended after the acyclicity guard below.
        let mut all_deps: Vec<String> = model.config.depends_on.clone();
        let mut seen: HashSet<String> = all_deps.iter().cloned().collect();
        for dep in derived.by_name {
            if seen.insert(dep.clone()) {
                all_deps.push(dep);
            }
        }
        for dep in derived.by_target {
            if !seen.contains(&dep.producer) {
                physical_edges.push(PhysicalEdge {
                    consumer: dag_nodes.len(),
                    producer: dep.producer,
                    exact: dep.exact,
                });
            }
        }

        lineage_cache.insert(model.config.name.clone(), lineage_result);

        dag_nodes.push(DagNode {
            name: model.config.name.clone(),
            depends_on: all_deps,
        });
    }

    diagnostics.extend(add_physical_edges_while_acyclic(
        &mut dag_nodes,
        physical_edges,
    ));

    Ok((dag_nodes, lineage_cache, diagnostics))
}

/// Auto-derived upstreams for one model, split by how they were derived.
///
/// The split exists so the acyclicity guard in [`resolve_dependencies`] can
/// drop a *physical* edge without touching a bare-name one: bare-name edges
/// reproduce the pre-#1275 graph exactly, so a project that loaded before must
/// still load with all of them applied.
struct DerivedDeps {
    /// Upstreams from a bare name matching a model name (`FROM orders`).
    by_name: Vec<String>,
    /// Upstreams from a qualified read of a model's `[target]`
    /// (`FROM main.orders`). Deduplicated against `by_name`.
    by_target: Vec<PhysicalDep>,
}

/// One derived physical edge, carrying how well-proven the match was.
struct PhysicalDep {
    producer: String,
    /// See [`ProducerMatch::exact`]. Ranks the edge when a cycle forces a drop.
    exact: bool,
}

/// A pending physical edge: `dag_nodes[consumer].depends_on += producer`.
struct PhysicalEdge {
    /// Index into `dag_nodes` of the reading model.
    consumer: usize,
    producer: String,
    /// See [`ProducerMatch::exact`].
    exact: bool,
}

/// Extract model dependencies from a pre-computed `LineageResult`.
///
/// Returns the names of referenced models — bare names matching a known model
/// name, and qualified names matching a model's declared `[target]` — excluding
/// self-references and duplicates.
///
/// # Self-reads drop only the reader and its exact-target twins
///
/// A reference resolving to a producer set that contains this model is a
/// self-read — the shape an incremental model's
/// `WHERE ts > (SELECT MAX(ts) FROM <own target>)` takes — and yields no edge
/// to itself. It also yields no edge to any *other* producer declaring the
/// **same target identity**, because that pair is a #1291 duplicate-target
/// collision: each one's self-read would otherwise become an edge to the other,
/// and if both self-read that is a two-node cycle whose D012 report would point
/// at the wrong problem (the collision is E036's business).
///
/// Producers that merely *tail-match* — same `schema.table`, a different
/// catalog — are a different object and keep their edge. Dropping the whole
/// candidate set instead was a fail-open: with `raw_orders` writing
/// `raw.main.orders`, `curated_orders` writing `analytics.main.orders`, and
/// `curated_orders` reading two-part `main.orders`, the candidate set is both
/// models, and discarding it because the reader appears in it silently
/// unordered the consumer from its real producer.
fn extract_deps_from_lineage(
    lineage_result: &lineage::LineageResult,
    model: &Model,
    model_names: &HashSet<String>,
    producers: &ProducerIndex,
) -> DerivedDeps {
    let model_name = model.config.name.as_str();
    let own_catalog = model.config.target.catalog.to_lowercase();
    let mut by_name = Vec::new();
    let mut by_target = Vec::new();
    let mut seen = HashSet::new();

    for table_ref in &lineage_result.source_tables {
        match classify_table_ref(&table_ref.name, model_names, producers) {
            TableRefKind::ModelRef(name) => {
                // Don't add self-references
                if name != model_name && seen.insert(name.clone()) {
                    by_name.push(name);
                }
            }
            TableRefKind::PhysicalModelRef(matches) => {
                let is_self_read = matches.iter().any(|m| m.model == model_name);
                for m in matches {
                    if m.model == model_name {
                        continue;
                    }
                    // Same physical object as the reader's own target ⇒ a
                    // duplicate-target collision, not an upstream.
                    if is_self_read && m.catalog == own_catalog {
                        continue;
                    }
                    if seen.insert(m.model.clone()) {
                        by_target.push(PhysicalDep {
                            producer: m.model,
                            exact: m.exact,
                        });
                    }
                }
            }
            TableRefKind::SourceRef { .. } | TableRefKind::RawRef(_) => {}
        }
    }

    DerivedDeps { by_name, by_target }
}

/// Apply `edges` to `dag_nodes`, skipping any that would close a cycle, and
/// return a **D012** warning for each one skipped.
///
/// Applies the whole set at once when that stays acyclic — the overwhelmingly
/// common case, one topological sort. Only when the full set cycles does it
/// fall back to adding edges one at a time in a deterministic order, keeping
/// each edge that leaves the graph orderable.
///
/// When the *declared* graph (explicit `depends_on` ∪ bare-name auto-derivation)
/// is already cyclic, no physical edge is applied and no warning is emitted:
/// `Project::from_models` is about to report that pre-existing cycle, and it
/// must report the same one it reported before physical edges existed.
fn add_physical_edges_while_acyclic(
    dag_nodes: &mut [DagNode],
    mut edges: Vec<PhysicalEdge>,
) -> Vec<Diagnostic> {
    if edges.is_empty() {
        return Vec::new();
    }
    let orderable = |nodes: &[DagNode]| rocky_ir::dag::topological_sort(nodes).is_ok();
    if !orderable(dag_nodes) {
        return Vec::new();
    }

    for edge in &edges {
        dag_nodes[edge.consumer]
            .depends_on
            .push(edge.producer.clone());
    }
    if orderable(dag_nodes) {
        return Vec::new();
    }

    // Roll back and re-add greedily, keeping only edges that stay orderable.
    for edge in edges.iter().rev() {
        dag_nodes[edge.consumer].depends_on.pop();
    }
    // Proven edges first, so a cycle never sacrifices an edge whose catalog was
    // named explicitly in order to keep one that only matched because a
    // catalog-less read or an empty configured catalog had to be assumed. Ties
    // keep insertion order — models arrive sorted from every loader, so the
    // surviving subset is deterministic for a given project.
    edges.sort_by_key(|e| !e.exact);

    let mut diagnostics = Vec::new();
    for edge in edges {
        let (node, producer) = (edge.consumer, edge.producer);
        dag_nodes[node].depends_on.push(producer.clone());
        if orderable(dag_nodes) {
            continue;
        }
        dag_nodes[node].depends_on.pop();
        let consumer = dag_nodes[node].name.clone();
        diagnostics.push(
            Diagnostic::warning(
                "D012",
                &consumer,
                format!(
                    "'{consumer}' reads the physical target of '{producer}', but ordering it \
                     after '{producer}' would make the dependency graph cyclic, so that edge \
                     was dropped. The two models can therefore run in the same execution \
                     layer, and under `--parallel` '{consumer}' may read '{producer}''s table \
                     before it is written."
                ),
            )
            .with_suggestion(format!(
                "Break the cycle — the two models read each other's output tables. If the \
                 reads only look circular because they share a `schema.table` across \
                 catalogs, declare the real upstream in '{consumer}''s depends_on"
            )),
        );
    }
    diagnostics
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_core::models::{ModelConfig, StrategyConfig, TargetConfig};

    fn make_model(name: &str, sql: &str) -> Model {
        Model {
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
            file_path: format!("models/{name}.sql"),
            contract_path: None,
        }
    }

    fn make_model_with_deps(name: &str, sql: &str, deps: Vec<&str>) -> Model {
        let mut m = make_model(name, sql);
        m.config.depends_on = deps.into_iter().map(String::from).collect();
        m
    }

    /// No project models ⇒ nothing to resolve a physical read against.
    fn no_producers() -> ProducerIndex {
        ProducerIndex::build(&[])
    }

    #[test]
    fn test_classify_bare_name_model() {
        let models: HashSet<String> = ["orders", "customers"]
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(
            classify_table_ref("orders", &models, &no_producers()),
            TableRefKind::ModelRef("orders".to_string())
        );
    }

    #[test]
    fn test_classify_bare_name_unknown() {
        let models: HashSet<String> = ["orders"].iter().map(ToString::to_string).collect();
        assert_eq!(
            classify_table_ref("unknown_table", &models, &no_producers()),
            TableRefKind::RawRef("unknown_table".to_string())
        );
    }

    #[test]
    fn test_classify_two_part() {
        let models: HashSet<String> = HashSet::new();
        assert_eq!(
            classify_table_ref("staging.orders", &models, &no_producers()),
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
            classify_table_ref("catalog.schema.table", &models, &no_producers()),
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

    // ---------------------------------------------------------------
    // #1275 — a physical `schema.table` read of a model's target must be a
    // DAG edge, so producer and consumer never share an execution layer.
    // ---------------------------------------------------------------

    /// Point a model at an explicit physical target. `make_model` defaults to
    /// `warehouse.silver.<name>`.
    fn targeting(mut m: Model, catalog: &str, schema: &str, table: &str) -> Model {
        m.config.target = TargetConfig {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
        };
        m
    }

    /// The zero-based execution layer `name` lands in — the structure
    /// `run.rs::execute_models` batches `--parallel` over, via
    /// `Project::layers`. Panics if the graph is cyclic or `name` is absent.
    fn layer_of(dag_nodes: &[DagNode], name: &str) -> usize {
        rocky_ir::dag::execution_layers(dag_nodes)
            .expect("graph must be orderable")
            .iter()
            .position(|layer| layer.iter().any(|n| n == name))
            .unwrap_or_else(|| panic!("'{name}' is in no layer"))
    }

    fn deps_of<'a>(dag_nodes: &'a [DagNode], name: &str) -> &'a [String] {
        &dag_nodes
            .iter()
            .find(|n| n.name == name)
            .unwrap_or_else(|| panic!("no node '{name}'"))
            .depends_on
    }

    /// The issue's exact reproduction: neither model declares `depends_on`,
    /// and the consumer reads the producer's target by physical name.
    ///
    /// Pre-fix `execution_layers` returned a single layer holding both, so
    /// under `--parallel N` the consumer could read `main.orders` before
    /// `orders` had written it. The assertion is on the LAYER, not merely on
    /// `depends_on`, because co-scheduling is the actual defect.
    #[test]
    fn two_part_physical_read_lands_the_consumer_in_a_later_layer() {
        let models = vec![
            targeting(make_model("orders", "SELECT 1 AS id"), "", "main", "orders"),
            targeting(
                make_model("mart_qualified", "SELECT id FROM main.orders"),
                "",
                "main",
                "mart_qualified",
            ),
        ];

        let (dag_nodes, _lineage, diags) = resolve_dependencies(&models).unwrap();

        assert_eq!(deps_of(&dag_nodes, "mart_qualified"), ["orders"]);
        assert!(
            layer_of(&dag_nodes, "orders") < layer_of(&dag_nodes, "mart_qualified"),
            "the producer must be scheduled strictly earlier: {:?}",
            rocky_ir::dag::execution_layers(&dag_nodes).unwrap()
        );
        assert!(
            diags.is_empty(),
            "a plain derived edge is not a diagnosis: {diags:?}"
        );
    }

    /// Trap 2: a model whose target declares a catalog is legitimately read as
    /// `catalog.schema.table`, which falls in the `RawRef` arm. Fixing only the
    /// 2-part arm would leave that spelling silently co-scheduled.
    #[test]
    fn three_part_physical_read_lands_the_consumer_in_a_later_layer() {
        let models = vec![
            targeting(
                make_model("orders", "SELECT 1 AS id"),
                "warehouse",
                "main",
                "orders",
            ),
            targeting(
                make_model("mart", "SELECT id FROM warehouse.main.orders"),
                "warehouse",
                "main",
                "mart",
            ),
        ];

        let (dag_nodes, _lineage, _diags) = resolve_dependencies(&models).unwrap();

        assert_eq!(deps_of(&dag_nodes, "mart"), ["orders"]);
        assert!(layer_of(&dag_nodes, "orders") < layer_of(&dag_nodes, "mart"));
    }

    /// A 3-part read names a catalog the producer leaves empty. An empty
    /// configured catalog means "the session default", which the compiler
    /// cannot observe — so the read must be assumed to name it.
    #[test]
    fn three_part_read_resolves_an_empty_catalog_producer() {
        let models = vec![
            targeting(make_model("orders", "SELECT 1 AS id"), "", "main", "orders"),
            targeting(
                make_model("mart", "SELECT id FROM memory.main.orders"),
                "",
                "main",
                "mart",
            ),
        ];

        let (dag_nodes, _lineage, _diags) = resolve_dependencies(&models).unwrap();
        assert_eq!(deps_of(&dag_nodes, "mart"), ["orders"]);
    }

    /// A 3-part read naming a DIFFERENT catalog than the producer's declared
    /// one is a different object — no edge. Guards against the empty-catalog
    /// wildcard degenerating into "any 3-part read matches".
    #[test]
    fn three_part_read_of_another_catalog_is_external() {
        let models = vec![
            targeting(
                make_model("orders", "SELECT 1 AS id"),
                "warehouse",
                "main",
                "orders",
            ),
            targeting(
                make_model("mart", "SELECT id FROM other_catalog.main.orders"),
                "warehouse",
                "main",
                "mart",
            ),
        ];

        let (dag_nodes, _lineage, _diags) = resolve_dependencies(&models).unwrap();
        assert!(
            deps_of(&dag_nodes, "mart").is_empty(),
            "a read of a different catalog names a different object"
        );
    }

    /// Trap 3: an incremental model reading its own target (the
    /// `WHERE ts > (SELECT MAX(ts) FROM <own target>)` shape) must not become
    /// its own upstream — a self-edge makes `topological_sort` report a cycle
    /// and `Project::from_models` fail.
    #[test]
    fn physical_read_of_own_target_is_not_a_self_edge() {
        let models = vec![targeting(
            make_model(
                "orders",
                "SELECT id FROM main.orders WHERE status = 'active'",
            ),
            "",
            "main",
            "orders",
        )];

        let (dag_nodes, _lineage, _diags) = resolve_dependencies(&models).unwrap();
        assert!(deps_of(&dag_nodes, "orders").is_empty());
        assert!(
            rocky_ir::dag::topological_sort(&dag_nodes).is_ok(),
            "a self-read must not make the project unorderable"
        );
    }

    /// The self-read exclusion drops the WHOLE producer set, not just this
    /// model. Two models declaring one target (#1291's E036 shape) that each
    /// self-read would otherwise become a mutual pair, and the D012 cycle
    /// report would point at the wrong problem.
    #[test]
    fn colliding_models_that_each_self_read_produce_no_edges() {
        let models = vec![
            targeting(
                make_model("a", "SELECT id FROM main.shared"),
                "",
                "main",
                "shared",
            ),
            targeting(
                make_model("b", "SELECT id FROM main.shared"),
                "",
                "main",
                "shared",
            ),
        ];

        let (dag_nodes, _lineage, diags) = resolve_dependencies(&models).unwrap();
        assert!(deps_of(&dag_nodes, "a").is_empty());
        assert!(deps_of(&dag_nodes, "b").is_empty());
        assert!(
            diags.is_empty(),
            "a target collision is E036's business, not D012's: {diags:?}"
        );
    }

    /// The self-read exclusion must drop only the reader and producers sharing
    /// its EXACT target — not every candidate the reference matched.
    ///
    /// `raw_orders` writes `raw.main.orders`; `curated_orders` writes
    /// `analytics.main.orders` and reads two-part `main.orders`. That read
    /// matches both (a catalog-less read cannot choose), so the candidate set
    /// contains the reader. Discarding the whole set on that basis silently
    /// unordered `curated_orders` from its real producer — the exact defect
    /// #1275 is about, reintroduced by the fix for it.
    ///
    /// Non-vacuous: with the whole-set drop, `deps` is empty and the two models
    /// share layer 0.
    #[test]
    fn a_self_read_still_orders_a_producer_in_another_catalog() {
        let models = vec![
            targeting(
                make_model("raw_orders", "SELECT 1 AS id"),
                "raw",
                "main",
                "orders",
            ),
            targeting(
                make_model("curated_orders", "SELECT id FROM main.orders"),
                "analytics",
                "main",
                "orders",
            ),
        ];

        let (dag_nodes, _lineage, _diags) = resolve_dependencies(&models).unwrap();

        assert_eq!(
            deps_of(&dag_nodes, "curated_orders"),
            ["raw_orders"],
            "a distinct object in another catalog is a real producer"
        );
        assert!(layer_of(&dag_nodes, "raw_orders") < layer_of(&dag_nodes, "curated_orders"));
    }

    /// The narrowing must not reopen the collision cycle it was written for.
    /// `colliding_models_that_each_self_read_produce_no_edges` covers the
    /// same-identity pair; this asserts the boundary from the other side — a
    /// third model with the same `schema.table` in a *different* catalog is
    /// still ordered ahead of both.
    #[test]
    fn a_collision_pair_still_orders_a_different_catalog_third_party() {
        let models = vec![
            targeting(
                make_model("a", "SELECT id FROM main.shared"),
                "cat",
                "main",
                "shared",
            ),
            targeting(
                make_model("b", "SELECT id FROM main.shared"),
                "cat",
                "main",
                "shared",
            ),
            targeting(
                make_model("elsewhere", "SELECT 1 AS id"),
                "other",
                "main",
                "shared",
            ),
        ];

        let (dag_nodes, _lineage, _diags) = resolve_dependencies(&models).unwrap();

        assert_eq!(
            deps_of(&dag_nodes, "a"),
            ["elsewhere"],
            "the same-identity twin is suppressed, the other-catalog one is not"
        );
        assert_eq!(deps_of(&dag_nodes, "b"), ["elsewhere"]);
    }

    /// Trap 4: a 2-part reference that matches no model target is still an
    /// external `SourceRef`. Model-target matching must not swallow every
    /// qualified read.
    #[test]
    fn two_part_read_matching_no_target_stays_a_source_ref() {
        let models = vec![targeting(
            make_model("orders", "SELECT 1 AS id"),
            "",
            "main",
            "orders",
        )];
        let producers = ProducerIndex::build(&models);
        let names: HashSet<String> = ["orders"].iter().map(ToString::to_string).collect();

        assert_eq!(
            classify_table_ref("fivetran_raw.orders", &names, &producers),
            TableRefKind::SourceRef {
                schema: "fivetran_raw".to_string(),
                table: "orders".to_string(),
            },
            "same table name, different schema — a genuine source"
        );
    }

    /// Trap 4, the other half: a source a model ALSO writes resolves to the
    /// model. Precedence is deliberate — the ordering hazard exists whatever
    /// else the table is called.
    #[test]
    fn a_target_that_is_also_a_declared_source_resolves_to_the_model() {
        let mut consumer = targeting(
            make_model("mart", "SELECT id FROM main.orders"),
            "",
            "main",
            "mart",
        );
        consumer.config.sources = vec![rocky_core::models::SourceConfig {
            catalog: String::new(),
            schema: "main".to_string(),
            table: "orders".to_string(),
        }];
        let models = vec![
            targeting(make_model("orders", "SELECT 1 AS id"), "", "main", "orders"),
            consumer,
        ];

        let (dag_nodes, _lineage, _diags) = resolve_dependencies(&models).unwrap();
        assert_eq!(deps_of(&dag_nodes, "mart"), ["orders"]);
    }

    /// Trap 5: case and quoting. `sqlparser` renders an `ObjectName` back to a
    /// string carrying its original quote characters, so an unstripped read
    /// identity misses the producer index entirely. Folding is unconditional,
    /// matching `rocky_sql::defer::CollisionIdentity`.
    #[test]
    fn quoted_and_case_differing_physical_reads_all_resolve() {
        for spelling in [
            "main.orders",
            "Main.Orders",
            "\"main\".\"orders\"",
            "\"MAIN\".\"Orders\"",
            "`main`.`orders`",
        ] {
            let models = vec![
                targeting(make_model("orders", "SELECT 1 AS id"), "", "main", "orders"),
                targeting(
                    make_model("mart", &format!("SELECT id FROM {spelling}")),
                    "",
                    "main",
                    "mart",
                ),
            ];

            let (dag_nodes, _lineage, _diags) = resolve_dependencies(&models).unwrap();
            assert_eq!(
                deps_of(&dag_nodes, "mart"),
                ["orders"],
                "spelling {spelling} must resolve to the producer"
            );
            assert!(layer_of(&dag_nodes, "orders") < layer_of(&dag_nodes, "mart"));
        }
    }

    /// A producer whose own target is spelled in a different case than the
    /// read still matches — folding applies to both sides.
    #[test]
    fn producer_target_case_is_folded_too() {
        let models = vec![
            targeting(make_model("orders", "SELECT 1 AS id"), "", "Main", "Orders"),
            targeting(
                make_model("mart", "SELECT id FROM main.orders"),
                "",
                "main",
                "mart",
            ),
        ];

        let (dag_nodes, _lineage, _diags) = resolve_dependencies(&models).unwrap();
        assert_eq!(deps_of(&dag_nodes, "mart"), ["orders"]);
    }

    /// Trap 1: two models declaring ONE target. Resolving that to "ambiguous"
    /// and emitting no edge would reproduce the very bug being fixed, so the
    /// read edges to EVERY producer. (#1291 separately reports the collision as
    /// an E036 error and excludes both from execution; this ordering does not
    /// depend on that, because the sibling shape below is legitimate.)
    #[test]
    fn a_read_of_a_duplicated_target_edges_to_every_producer() {
        let models = vec![
            targeting(make_model("a", "SELECT 1 AS id"), "", "main", "shared"),
            targeting(make_model("b", "SELECT 2 AS id"), "", "main", "shared"),
            targeting(
                make_model("mart", "SELECT id FROM main.shared"),
                "",
                "main",
                "mart",
            ),
        ];

        let (dag_nodes, _lineage, _diags) = resolve_dependencies(&models).unwrap();

        let mut deps = deps_of(&dag_nodes, "mart").to_vec();
        deps.sort();
        assert_eq!(deps, ["a", "b"], "no producer may be left unordered");
        assert!(layer_of(&dag_nodes, "a") < layer_of(&dag_nodes, "mart"));
        assert!(layer_of(&dag_nodes, "b") < layer_of(&dag_nodes, "mart"));
    }

    /// Trap 1, the legitimate shape: two models in DIFFERENT catalogs sharing a
    /// `(schema, table)`. These are distinct objects — no #1291 collision, no
    /// E036 — and a 2-part read binds to the session default catalog, which the
    /// compiler cannot observe. Both get an edge.
    #[test]
    fn a_catalog_ambiguous_two_part_read_edges_to_every_candidate() {
        let models = vec![
            targeting(make_model("a", "SELECT 1 AS id"), "cat_one", "main", "t"),
            targeting(make_model("b", "SELECT 2 AS id"), "cat_two", "main", "t"),
            targeting(
                make_model("mart", "SELECT id FROM main.t"),
                "cat_one",
                "main",
                "mart",
            ),
        ];

        let (dag_nodes, _lineage, _diags) = resolve_dependencies(&models).unwrap();

        let mut deps = deps_of(&dag_nodes, "mart").to_vec();
        deps.sort();
        assert_eq!(deps, ["a", "b"]);
    }

    /// An `Ephemeral` model carries a fully populated but phantom target and
    /// materializes nothing, so a read of that name races with no write —
    /// the same exclusion #1291 applies to duplicate-target detection.
    #[test]
    fn an_ephemeral_target_is_not_a_producer() {
        let mut eph = targeting(make_model("eph", "SELECT 1 AS id"), "", "main", "eph");
        eph.config.strategy = StrategyConfig::Ephemeral;
        let models = vec![
            eph,
            targeting(
                make_model("mart", "SELECT id FROM main.eph"),
                "",
                "main",
                "mart",
            ),
        ];

        let (dag_nodes, _lineage, _diags) = resolve_dependencies(&models).unwrap();
        assert!(deps_of(&dag_nodes, "mart").is_empty());
    }

    /// A bare name is matched against model NAMES only, never against producer
    /// target tables — `rocky_sql::lineage` does not strip `WITH` names out of
    /// `source_tables`, so a CTE alias colliding with some model's output table
    /// would otherwise manufacture an edge.
    #[test]
    fn a_bare_name_is_not_matched_against_target_tables() {
        let models = vec![
            targeting(
                make_model("stg_orders", "SELECT 1 AS id"),
                "",
                "main",
                "orders",
            ),
            targeting(
                make_model(
                    "mart",
                    "WITH orders AS (SELECT 1 AS id) SELECT id FROM orders",
                ),
                "",
                "main",
                "mart",
            ),
        ];

        let (dag_nodes, _lineage, _diags) = resolve_dependencies(&models).unwrap();
        assert!(
            deps_of(&dag_nodes, "mart").is_empty(),
            "a CTE alias matching a producer's target TABLE is not a dependency"
        );
    }

    /// A physical edge that would close a cycle is dropped and reported as
    /// D012 — never propagated as a `ProjectError`. #1291 recorded why a hard
    /// construction failure is the wrong shape: `ci-diff` and the LSP swallow
    /// it into silence, and a project that loads today must keep loading.
    #[test]
    fn a_cycle_closing_physical_edge_is_dropped_and_reported() {
        let models = vec![
            targeting(
                make_model("a", "SELECT id FROM main.b_out"),
                "",
                "main",
                "a_out",
            ),
            targeting(
                make_model("b", "SELECT id FROM main.a_out"),
                "",
                "main",
                "b_out",
            ),
        ];

        let (dag_nodes, _lineage, diags) = resolve_dependencies(&models).unwrap();

        assert!(
            rocky_ir::dag::topological_sort(&dag_nodes).is_ok(),
            "the graph must stay orderable — a ProjectError here makes the \
             project unloadable in ci-diff and the LSP"
        );
        assert_eq!(diags.len(), 1, "the dropped edge is reported: {diags:?}");
        assert_eq!(&*diags[0].code, "D012");
        assert!(
            !diags[0].is_error(),
            "a warning, not an error — the over-matching that can manufacture \
             the cycle is deliberate"
        );
        // One direction survives; the other is the reported drop.
        let kept = usize::from(!deps_of(&dag_nodes, "a").is_empty())
            + usize::from(!deps_of(&dag_nodes, "b").is_empty());
        assert_eq!(kept, 1, "exactly one of the mutual edges is kept");
    }

    /// A cycle in the DECLARED graph is reported by the existing
    /// `topological_sort` in `Project::from_models`, unchanged — no physical
    /// edge is applied on top and no D012 muddies the message.
    #[test]
    fn a_pre_existing_declared_cycle_is_left_exactly_as_it_was() {
        let models = vec![
            make_model_with_deps("a", "SELECT 1 AS id", vec!["b"]),
            make_model_with_deps("b", "SELECT 1 AS id", vec!["a"]),
        ];

        let (dag_nodes, _lineage, diags) = resolve_dependencies(&models).unwrap();
        assert!(rocky_ir::dag::topological_sort(&dag_nodes).is_err());
        assert!(
            diags.iter().all(|d| &*d.code != "D012"),
            "the declared cycle is not a dropped-physical-edge report: {diags:?}"
        );
    }

    /// D011 now also fires for a physical read the explicit `depends_on`
    /// misses, and its suggestion no longer tells the user that removing
    /// `depends_on` lets auto-derivation "handle everything" without saying
    /// where derivation stops.
    #[test]
    fn d011_covers_a_missing_physical_read_and_qualifies_its_advice() {
        let models = vec![
            targeting(make_model("orders", "SELECT 1 AS id"), "", "main", "orders"),
            targeting(make_model("other", "SELECT 1 AS id"), "", "main", "other"),
            targeting(
                make_model_with_deps("mart", "SELECT id FROM main.orders", vec!["other"]),
                "",
                "main",
                "mart",
            ),
        ];

        let (_dag_nodes, _lineage, diags) = resolve_dependencies(&models).unwrap();

        let d011 = diags
            .iter()
            .find(|d| &*d.code == "D011")
            .expect("a physical read absent from depends_on must be reported");
        assert!(d011.message.contains("orders"));
        let suggestion = d011.suggestion.as_deref().unwrap_or_default();
        assert!(
            suggestion.contains("cross-pipeline"),
            "the advice must say where auto-derivation stops: {suggestion}"
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
}
