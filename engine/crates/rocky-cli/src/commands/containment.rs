//! Model-failure containment — continue disjoint subgraphs on failure.
//!
//! When a model's build fails, the run loop can *contain* the failure instead
//! of aborting the whole run: the failed model and its downstream closure are
//! withheld, while disjoint subgraphs still materialize. The run then reports a
//! `PartialFailure` with a manifest naming what failed and its blast radius.
//!
//! # Naming (not row-quarantine)
//!
//! This is a different axis from the quality pipeline's *row*-quarantine
//! (`rocky_core::quarantine`), which splits bad *rows* out of one table.
//! Containment operates on the *model graph*: it withholds whole downstream
//! *models* when an upstream fails. The vocabulary here is deliberately
//! distinct — `Contained` / "blocked by" — so a reader never confuses a
//! withheld model with a quarantined row.
//!
//! # Soundness — the closure must be complete, not just the declared graph
//!
//! The invariant is absolute: **a downstream of a failed model must never build
//! on that failure's stale or missing output.** So the closure is *conservative
//! and fail-closed*, and it does **not** trust the declared dependency graph
//! (`dag_nodes`) as complete or authoritative. Two escape hatches make a naive
//! `dag_nodes`-only closure unsound, and both are closed here:
//!
//! 1. **Physical-table reads.** A raw-SQL model can read a producer's *target
//!    table by physical name* (`marts.orders_current`) instead of by model name
//!    (`ref(stage_orders)`). That read produces no `dag_nodes` edge, so a
//!    `dag_nodes`-only closure would let the model build on the failed
//!    producer's stale output. We therefore resolve every physical `FROM`/`JOIN`
//!    read against the set of **produced targets** ([`ProducerIndex`]) and add
//!    an implicit containment edge when a read matches a produced target — so a
//!    model reading a failed model's output, by `ref()` *or* by physical name,
//!    is withheld. A read that resolves to no produced target is a genuinely
//!    external table this run does not produce, so a run failure cannot make it
//!    stale — no edge, build allowed.
//! 2. **Unprovable read sets.** If a model's reads cannot be fully enumerated
//!    (`rocky_sql::lineage_complete::lineage_is_provably_complete` is `false` —
//!    CTEs, sub-queries, set operations, unparseable SQL) or a physical read is
//!    ambiguous (matches producers in more than one catalog), we cannot *prove*
//!    the model is disjoint from the failure set. Such a model **fails closed**:
//!    once any failure has occurred it is contained, never built.
//!
//! The closure is evaluated layer-by-layer in topological order, poisoning every
//! withheld model as it goes, so a one-hop upstream check is transitively
//! complete. When `contain_failures` is on, execution layers are recomputed over
//! this full edge set ([`ContainmentLedger::augmented_layers`]) so a reader is
//! scheduled strictly after every producer it reads — closing the same-layer
//! `--parallel` race for *resolvable* reads.
//!
//! # Guarantee scope
//!
//! Failure containment is **guaranteed** for dependencies declared via `ref()`
//! and for physical reads Rocky can statically resolve — 2-part `schema.table`,
//! 3-part `catalog.schema.table`, quoted or unquoted. Those are folded into both
//! the closure and the execution ordering, so a downstream of a failure is never
//! built on its stale/missing output.
//!
//! Reads Rocky **cannot enumerate** — a model built on a CTE, sub-query, or set
//! operation, anything `rocky_sql::lineage_complete::lineage_is_provably_complete`
//! rejects (an "uncertain" model) — are handled on a **best-effort** basis
//! *identical to a normal fail-fast run*. An uncertain model with a known failed
//! upstream is contained (the fail-closed belt in [`ContainmentLedger::evaluate`]),
//! but because its reads yield no ordering edge, under `--parallel` a same-layer
//! uncertain reader of a failing producer can still materialize on stale data —
//! **exactly as plain fail-fast does** in that case (verified by the
//! `containment_matches_fail_fast_for_same_layer_uncertain_read` parity test).
//! This is a documented boundary, not a regression: the achievable guarantee is
//! *containment never materializes anything fail-fast wouldn't*. Chasing the
//! absolute bar would mean containing every CTE model, which would false-fail
//! healthy projects. **Declare the dependency with `ref()` for a hard guarantee.**

use std::collections::{BTreeSet, HashMap, HashSet};

/// Index of every model's produced target, for resolving a physical `FROM` /
/// `JOIN` read back to the model that produces that table.
///
/// Three lookups mirror how a physical read can be written:
/// - `by_full` — an explicit 3-part `catalog.schema.table` read matches a
///   producer's full target exactly.
/// - `by_schema_table` — a 2-part `schema.table` read omits the catalog; it
///   matches producers on `(schema, table)`. More than one producer (across
///   catalogs) sharing that `(schema, table)` is *ambiguous* → fail closed.
/// - `by_table` — a bare 1-part read (`orders_current`) resolved via the
///   session default schema. We do NOT model per-dialect `search_path`
///   precisely (getting it wrong is the unsound direction); instead a bare read
///   colliding with a producer's target table **in any schema** is treated as a
///   *candidate* edge to each such producer, so the reader is contained iff at
///   least one candidate producer is poisoned. This over-contains only when a
///   bare read literally matches a failed producer's output table — the
///   acceptable direction; a bare read matching no producer table is external.
///
/// All keys are lowercased. A model appears once per lookup.
pub(crate) struct ProducerIndex {
    by_full: HashMap<(String, String, String), Vec<String>>,
    by_schema_table: HashMap<(String, String), Vec<String>>,
    by_table: HashMap<String, Vec<String>>,
}

/// How a single physical read resolves against the produced targets.
pub(crate) enum ReadResolution {
    /// Resolves to one or more candidate producers — each is added as a
    /// containment edge, so the reader is blocked iff at least one is poisoned.
    /// Exactly one candidate for an unambiguous 2/3-part read; possibly several
    /// for a bare 1-part read that collides with a producer target table across
    /// schemas.
    Edges(Vec<String>),
    /// A qualified (2/3-part) read matching more than one produced target
    /// (multi-catalog collision), or an un-canonicalizable identity — cannot be
    /// attributed, so the reader fails closed.
    Ambiguous,
    /// Matches no produced target — a genuinely external table this run does
    /// not produce, which a run failure cannot make stale.
    External,
}

impl ProducerIndex {
    /// Build from `(model_name, catalog, schema, table)` tuples.
    pub(crate) fn build<'a, I>(models: I) -> Self
    where
        I: IntoIterator<Item = (&'a str, &'a str, &'a str, &'a str)>,
    {
        let mut by_full: HashMap<(String, String, String), Vec<String>> = HashMap::new();
        let mut by_schema_table: HashMap<(String, String), Vec<String>> = HashMap::new();
        let mut by_table: HashMap<String, Vec<String>> = HashMap::new();
        for (name, catalog, schema, table) in models {
            let (c, s, t) = (
                catalog.to_lowercase(),
                schema.to_lowercase(),
                table.to_lowercase(),
            );
            by_full
                .entry((c, s.clone(), t.clone()))
                .or_default()
                .push(name.to_string());
            by_schema_table
                .entry((s, t.clone()))
                .or_default()
                .push(name.to_string());
            by_table.entry(t).or_default().push(name.to_string());
        }
        Self {
            by_full,
            by_schema_table,
            by_table,
        }
    }

    /// Resolve one physical read identity to a produced target.
    ///
    /// The identity is first canonicalized ([`canonicalize_identifier`]) so a
    /// quoted read (`"main"."orders_current"`, `` `main`.`orders_current` ``,
    /// mixed) matches the clean producer keys — the producer side is built from
    /// structured `config.target` parts and carries no quotes. A read that
    /// cannot be cleanly canonicalized (unbalanced quotes, an empty segment, or
    /// a quoted segment with an embedded dot that can't be slotted into the
    /// `schema.table` index) resolves to [`ReadResolution::Ambiguous`] so the
    /// reader **fails closed**.
    ///
    /// A bare (1-part) read is resolved against producer target-table names
    /// (`by_table`): a caller-known model name is skipped upstream (it is a
    /// `ref()` edge), so a bare read that reaches here and matches a producer's
    /// target table is a candidate edge to that producer. A qualified read
    /// (2/3-part) resolves uniquely or is [`ReadResolution::Ambiguous`]; a read
    /// with more than three parts is out of range → [`ReadResolution::External`].
    pub(crate) fn resolve(&self, read: &str) -> ReadResolution {
        let Some(parts) = canonicalize_identifier(read) else {
            // Un-canonicalizable — cannot attribute the read; fail closed.
            return ReadResolution::Ambiguous;
        };
        match parts.as_slice() {
            // Qualified reads must resolve to exactly one producer; a collision
            // (duplicate exact target, or multi-catalog `schema.table`) can't be
            // attributed, so it fails closed.
            [catalog, schema, table] => Self::qualified(self.by_full.get(&(
                catalog.clone(),
                schema.clone(),
                table.clone(),
            ))),
            [schema, table] => {
                Self::qualified(self.by_schema_table.get(&(schema.clone(), table.clone())))
            }
            // Bare read: every producer whose target table matches is a
            // candidate — contain iff at least one is poisoned. No match ⇒
            // genuinely external ⇒ build (no over-containment).
            [table] => match self.by_table.get(table) {
                None => ReadResolution::External,
                Some(candidates) => ReadResolution::Edges(candidates.clone()),
            },
            _ => ReadResolution::External,
        }
    }

    /// Resolve a qualified (2/3-part) read: exactly one producer ⇒ an edge; a
    /// collision ⇒ ambiguous (fail closed); none ⇒ external.
    fn qualified(candidates: Option<&Vec<String>>) -> ReadResolution {
        match candidates.map(Vec::as_slice) {
            None | Some([]) => ReadResolution::External,
            Some([one]) => ReadResolution::Edges(vec![one.clone()]),
            Some(_) => ReadResolution::Ambiguous,
        }
    }
}

/// Split a possibly-quoted SQL table identifier into its lowercased, unquoted
/// parts, or `None` when it can't be cleanly canonicalized.
///
/// The read identity arrives from the SQL lineage walk, where sqlparser renders
/// an `ObjectName` back to a string **with** its original quote characters
/// (`"main"."orders_current"`), while the producer index keys are the clean,
/// structured `config.target` parts. Matching therefore requires stripping the
/// quotes the same way for the read side. Quote styles recognized: double-quote
/// `"…"` (DuckDB / Snowflake / Postgres), backtick `` `…` `` (BigQuery /
/// Databricks), bracket `[…]` (T-SQL). Inside a quoted segment a `.` is a
/// literal, not a separator; a doubled closing quote (`""`, ` `` `) is an
/// escaped quote character.
///
/// Returns `None` — so the caller **fails closed** — on any identity that can't
/// be unambiguously slotted into the `schema.table` index: unbalanced quotes, an
/// empty segment (`a..b`, a trailing dot), or a segment that after unquoting
/// still contains a `.` (a quoted identifier with an embedded dot). Doing string
/// surgery here rather than a naive `replace('"', "")` is deliberate — the naive
/// form would mis-split a quoted identifier that legitimately contains a dot.
fn canonicalize_identifier(read: &str) -> Option<Vec<String>> {
    let mut parts: Vec<String> = Vec::new();
    let mut cur = String::new();
    let mut chars = read.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '"' | '`' | '[' => {
                let close = if ch == '[' { ']' } else { ch };
                loop {
                    match chars.next() {
                        // Unbalanced quote — cannot canonicalize.
                        None => return None,
                        Some(c) if c == close => {
                            // A doubled closing quote (`""` / ` `` `) is an
                            // escaped literal, not the end of the segment.
                            // Brackets have no doubling convention.
                            if close != ']' && chars.peek() == Some(&close) {
                                chars.next();
                                cur.push(close);
                            } else {
                                break;
                            }
                        }
                        Some(c) => cur.push(c),
                    }
                }
            }
            '.' => parts.push(std::mem::take(&mut cur)),
            c => cur.push(c),
        }
    }
    parts.push(cur);
    // An empty segment or an embedded dot (from a quoted identifier) can't be
    // slotted into the `schema.table` index — fail closed.
    if parts.iter().any(|p| p.is_empty() || p.contains('.')) {
        return None;
    }
    Some(parts.into_iter().map(|p| p.to_lowercase()).collect())
}

/// Conservative, fail-closed downstream-closure tracker for one run.
///
/// Holds, per model, its full containment-upstream set (`ref()` deps ∪ resolved
/// physical-target edges) plus an `uncertain` flag for models whose disjointness
/// from the failure set cannot be proven; and the *poison set* — the union of
/// failed and withheld models.
pub(crate) struct ContainmentLedger {
    /// `model name → its containment upstreams` (ref-deps ∪ physical edges).
    upstreams: HashMap<String, Vec<String>>,
    /// Models whose read set could not be proven complete / unambiguous — they
    /// fail closed once any failure has occurred.
    uncertain: HashSet<String>,
    /// Every model the ledger was told about (run-set coverage). A model queried
    /// but never added is treated as uncertain (fail closed).
    known: HashSet<String>,
    /// Failed ∪ withheld model names.
    poison: BTreeSet<String>,
}

/// The verdict for one model: whether to withhold it, and why.
pub(crate) struct ContainmentDecision {
    /// The poisoned models that make it unsafe to build — a proven upstream
    /// block names those upstreams; a fail-closed block names the current
    /// failure set.
    pub blocked_by: Vec<String>,
    /// `true` when contained by the conservative belt (unprovable disjointness),
    /// not by a proven poisoned upstream.
    pub fail_closed: bool,
}

/// Execution layers over the augmented containment graph, plus any models that
/// couldn't be ordered because a physical/bare edge formed a cycle.
pub(crate) struct LayerPlan {
    /// Topological execution layers (longest-path) over ref ∪ physical ∪ bare
    /// edges. A reader is strictly later than every producer it reads.
    pub layers: Vec<Vec<String>>,
    /// Models in — or downstream of — an augmented-graph cycle. They can't be
    /// safely ordered, so the caller contains them (fail closed). Empty in the
    /// common acyclic case.
    pub cyclic: Vec<String>,
}

impl ContainmentLedger {
    pub(crate) fn new() -> Self {
        Self {
            upstreams: HashMap::new(),
            uncertain: HashSet::new(),
            known: HashSet::new(),
            poison: BTreeSet::new(),
        }
    }

    /// Register one model with its `ref()` deps, its enumerated physical reads,
    /// whether that read set is provably complete, and the producer index.
    ///
    /// Computes the model's full containment-upstream set (ref-deps ∪ physical
    /// edges to produced targets) and marks it `uncertain` when the read set is
    /// not provably complete or any physical read is ambiguous.
    pub(crate) fn add_model(
        &mut self,
        name: &str,
        ref_deps: &[String],
        reads: &[String],
        reads_complete: bool,
        producers: &ProducerIndex,
    ) {
        self.known.insert(name.to_string());
        let mut ups: BTreeSet<String> = ref_deps.iter().cloned().collect();
        // A read set we cannot fully enumerate cannot prove disjointness.
        let mut uncertain = !reads_complete;
        for read in reads {
            // A bare read is "already covered" only when the resolver made the
            // SAME call — ref-edge derivation is case-sensitive on the
            // as-written identifier, so the test must be exact membership in
            // `ref_deps`, not membership in the model-name set: a
            // case-differing bare read (`FROM ORDERS` for model `orders`) has
            // NO ref edge, and skipping it here dropped the dependency
            // entirely (unquoted identifiers are case-insensitive in every
            // supported warehouse, so it IS a real data dependency). Anything
            // not exactly covered falls through to producer resolution, which
            // is set-deduped and fail-closed.
            if ref_deps.iter().any(|d| d == read) {
                continue;
            }
            match producers.resolve(read) {
                // A physical read of one-or-more producers' target(s). Each is a
                // candidate edge (self-reads excluded); the model is blocked iff
                // at least one candidate is poisoned.
                ReadResolution::Edges(producers) => {
                    for producer in producers {
                        if producer != name {
                            ups.insert(producer);
                        }
                    }
                }
                // Cannot attribute the read to a bounded producer set — fail closed.
                ReadResolution::Ambiguous => uncertain = true,
                // Genuinely external — a run failure cannot make it stale.
                ReadResolution::External => {}
            }
        }
        self.upstreams
            .insert(name.to_string(), ups.into_iter().collect());
        if uncertain {
            self.uncertain.insert(name.to_string());
        }
    }

    /// Seed the poison set with already-known failed models (e.g. compile
    /// failures excluded from execution). Their downstreams are withheld like a
    /// runtime failure's.
    pub(crate) fn seed_failed<I>(&mut self, models: I)
    where
        I: IntoIterator<Item = String>,
    {
        self.poison.extend(models);
    }

    /// Add a model to the poison set — a failed cause *or* a withheld
    /// downstream. Either way its descendants must be withheld, so both extend
    /// the closure identically.
    pub(crate) fn poison(&mut self, model: &str) {
        self.poison.insert(model.to_string());
    }

    /// Decide whether `model` must be withheld this run.
    ///
    /// - Any containment upstream (ref or physical) is poisoned ⇒ contained,
    ///   naming those upstreams (`fail_closed = false`).
    /// - Otherwise, if a failure has occurred (`poison` non-empty) and the model
    ///   is *uncertain* — unprovable read set, an ambiguous read, or unknown to
    ///   the ledger — ⇒ contained by the fail-closed belt, naming the current
    ///   failure set (`fail_closed = true`).
    /// - Otherwise ⇒ `None` (safe to build).
    pub(crate) fn evaluate(&self, model: &str) -> Option<ContainmentDecision> {
        let mut blockers: Vec<String> = self
            .upstreams
            .get(model)
            .map(|ups| {
                ups.iter()
                    .filter(|u| self.poison.contains(*u))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        if !blockers.is_empty() {
            blockers.sort();
            blockers.dedup();
            return Some(ContainmentDecision {
                blocked_by: blockers,
                fail_closed: false,
            });
        }
        let failure_occurred = !self.poison.is_empty();
        let is_uncertain = self.uncertain.contains(model) || !self.known.contains(model);
        if failure_occurred && is_uncertain {
            return Some(ContainmentDecision {
                blocked_by: self.poison.iter().cloned().collect(),
                fail_closed: true,
            });
        }
        None
    }

    /// Compute execution layers over the **full containment edge set** (ref()
    /// deps ∪ resolved physical/bare-read producer edges), so a reader is
    /// scheduled strictly after every producer it reads by any edge kind — the
    /// ordering guarantee that closes the same-layer `--parallel` race.
    ///
    /// Longest-path layering ([`rocky_ir::dag::execution_layers`]) places a node
    /// one layer after its deepest dependency, so no two models in the same
    /// layer share a containment edge; combined with the executor's per-layer
    /// barrier, every prior failure is poisoned before the next layer is
    /// evaluated.
    ///
    /// **Cycle → fail closed.** A physical/bare edge can introduce a dependency
    /// cycle the ref-graph lacked. Rather than let the topological sort silently
    /// drop the cyclic nodes, they are returned in [`LayerPlan::cyclic`] (the
    /// stuck set — the cycle plus everything downstream of it) for the caller to
    /// *contain*, and only the acyclic remainder is layered.
    pub(crate) fn augmented_layers(&self) -> LayerPlan {
        use rocky_ir::dag::{self, DagError, DagNode};

        // Augmented graph: each model's dependencies are its full containment
        // upstream set, restricted to known models.
        let node = |name: &str, ups: &[String]| DagNode {
            name: name.to_string(),
            depends_on: ups
                .iter()
                .filter(|u| self.known.contains(*u))
                .cloned()
                .collect(),
        };
        let nodes: Vec<DagNode> = self
            .known
            .iter()
            .map(|m| node(m, self.upstreams.get(m).map(Vec::as_slice).unwrap_or(&[])))
            .collect();

        match dag::execution_layers(&nodes) {
            Ok(layers) => LayerPlan {
                layers,
                cyclic: Vec::new(),
            },
            // The stuck set (cycle + downstream) can't be ordered — fail closed:
            // contain it, and layer only the acyclic remainder.
            Err(DagError::CyclicDependency { nodes: stuck }) => {
                let stuck_set: HashSet<&str> = stuck.iter().map(String::as_str).collect();
                let acyclic: Vec<DagNode> = nodes
                    .iter()
                    .filter(|n| !stuck_set.contains(n.name.as_str()))
                    .map(|n| DagNode {
                        name: n.name.clone(),
                        depends_on: n
                            .depends_on
                            .iter()
                            .filter(|d| !stuck_set.contains(d.as_str()))
                            .cloned()
                            .collect(),
                    })
                    .collect();
                let layers = dag::execution_layers(&acyclic).unwrap_or_default();
                let mut cyclic = stuck;
                cyclic.sort();
                LayerPlan { layers, cyclic }
            }
            // `UnknownDependency` cannot arise (deps are filtered to known
            // models), but fail closed if it somehow does: contain everything.
            Err(_) => {
                let mut cyclic: Vec<String> = self.known.iter().cloned().collect();
                cyclic.sort();
                LayerPlan {
                    layers: Vec::new(),
                    cyclic,
                }
            }
        }
    }
}

/// A human-readable hint telling the operator how to clear a withheld model.
pub(crate) fn unblock_hint(blocked_by: &[String], fail_closed: bool) -> String {
    if fail_closed {
        return match blocked_by {
            [] => "contained conservatively while a failure is unresolved; \
                   declare dependencies via ref()/model name and re-run"
                .to_string(),
            many => format!(
                "contained conservatively: cannot prove this model is independent of the \
                 failure(s) {}; declare dependencies via ref()/model name and re-run",
                many.join(", ")
            ),
        };
    }
    match blocked_by {
        [] => "re-run once the upstream failure is resolved".to_string(),
        [one] => format!("resolve upstream '{one}', then re-run"),
        many => format!("resolve upstream(s) {}, then re-run", many.join(", ")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a ledger from `(name, ref_deps, sql_reads, reads_complete)` tuples
    /// plus producer `(name, catalog, schema, table)` tuples, driving the same
    /// path `execute_models` uses.
    fn ledger(
        models: &[(&str, &[&str], &[&str], bool)],
        producers: &[(&str, &str, &str, &str)],
    ) -> ContainmentLedger {
        let index = ProducerIndex::build(producers.iter().copied());
        let mut led = ContainmentLedger::new();
        for (name, deps, reads, complete) in models {
            let deps: Vec<String> = deps.iter().copied().map(String::from).collect();
            let reads: Vec<String> = reads.iter().map(|s| s.to_lowercase()).collect();
            led.add_model(name, &deps, &reads, *complete, &index);
        }
        led
    }

    /// (a) Disjoint subtrees still run; the ref-declared downstream of a failure
    /// is contained.
    #[test]
    fn disjoint_subtree_builds_ref_downstream_contained() {
        let mut led = ledger(
            &[
                ("A", &[], &[], true),
                ("B", &["A"], &["a"], true),
                ("C", &[], &[], true),
                ("D", &["C"], &["c"], true),
            ],
            &[],
        );
        led.poison("A");
        assert!(led.evaluate("B").is_some(), "B depends on failed A");
        assert!(led.evaluate("C").is_none(), "C is disjoint");
        assert!(led.evaluate("D").is_none(), "D is disjoint");
    }

    /// FIX: a case-differing bare read of a model (`FROM ORDERS` for model
    /// `orders`) is a real data dependency — unquoted identifiers are
    /// case-insensitive in every supported warehouse — but ref-edge
    /// derivation is case-sensitive on the as-written identifier, so no
    /// `ref()` edge exists. The old "bare model-name read is already a ref
    /// edge" skip compared against the (lowercased) model-name set and
    /// dropped the dependency entirely: `rollup` built on failed `orders`'
    /// stale output. The skip now requires exact `ref_deps` membership, so
    /// the read falls through to producer resolution and yields the edge.
    #[test]
    fn case_differing_bare_read_of_failed_producer_is_contained() {
        // As collected by the run loop: reads arrive lowercased, so
        // `FROM ORDERS` in rollup's SQL is the read "orders"; model `orders`
        // produces `c.main.orders`. No ref edge (case-sensitive derivation
        // classified `ORDERS` as a raw ref).
        let mut led = ledger(
            &[
                ("orders", &[], &[], true),
                ("rollup", &[], &["orders"], true),
            ],
            &[("orders", "c", "main", "orders")],
        );
        led.poison("orders");
        assert!(
            led.evaluate("rollup").is_some(),
            "a case-differing bare read of a failed producer must be contained"
        );

        // The exact-match skip still dedups a true ref-edge bare read: with
        // the ref edge present the read is already covered, and the verdict
        // is identical (edge via ref_deps).
        let mut led2 = ledger(
            &[
                ("orders", &[], &[], true),
                ("rollup", &["orders"], &["orders"], true),
            ],
            &[("orders", "c", "main", "orders")],
        );
        led2.poison("orders");
        assert!(led2.evaluate("rollup").is_some());
    }

    /// (b) Physical-table read of a failed producer's TARGET is contained even
    /// with no `ref()` edge — the Finding-1 unit-level guard.
    #[test]
    fn physical_target_read_is_contained() {
        // `stage` produces `marts.orders_current`; `rollup` reads it by physical
        // name (2-part) with no ref edge to `stage`.
        let mut led = ledger(
            &[
                ("stage", &[], &["main.raw"], true),
                (
                    "rollup",
                    &["anchor"],
                    &["anchor", "marts.orders_current"],
                    true,
                ),
                ("anchor", &[], &[], true),
            ],
            &[
                ("stage", "", "marts", "orders_current"),
                ("rollup", "", "marts", "rollup"),
                ("anchor", "", "main", "anchor"),
            ],
        );
        led.poison("stage");
        let dec = led.evaluate("rollup").expect("rollup must be contained");
        assert!(
            !dec.fail_closed,
            "contained by a proven physical edge, not the belt"
        );
        assert!(
            dec.blocked_by.contains(&"stage".to_string()),
            "the physical edge must name the producer: {:?}",
            dec.blocked_by
        );
    }

    /// A physical read that matches NO produced target is external — never
    /// contained (a run failure can't make an external table stale).
    #[test]
    fn external_physical_read_is_not_contained() {
        let mut led = ledger(
            &[("m", &[], &["raw_db.orders"], true)],
            &[("other", "", "marts", "orders_current")],
        );
        led.poison("other");
        assert!(led.evaluate("m").is_none(), "external read must build");
    }

    /// A bare (1-part) read of a producer's target table (default-schema
    /// resolution) is contained when that producer is poisoned — via `by_table`,
    /// even though the producer's model name differs from its table name.
    #[test]
    fn bare_read_of_producer_table_is_contained() {
        let mut led = ledger(
            &[
                ("stage", &[], &[], true),
                ("rollup", &["anchor"], &["orders_current"], true),
                ("anchor", &[], &[], true),
            ],
            &[("stage", "", "main", "orders_current")],
        );
        led.poison("stage");
        let dec = led
            .evaluate("rollup")
            .expect("bare read of a failed producer's table ⇒ contained");
        assert!(
            !dec.fail_closed,
            "resolved via a producer edge, not the belt"
        );
        assert!(dec.blocked_by.contains(&"stage".to_string()));
    }

    /// A bare read of a genuine external table (no producer has that table name)
    /// builds — even when an unrelated failure has occurred. The bare-read
    /// resolution must not over-contain every bare read whenever a run fails.
    #[test]
    fn bare_read_of_external_table_builds() {
        let mut led = ledger(
            &[
                ("bad", &[], &[], true),
                ("reader", &["anchor"], &["some_external"], true),
                ("anchor", &[], &[], true),
            ],
            &[("stage", "", "main", "orders_current")], // no producer targets `some_external`
        );
        led.poison("bad");
        assert!(
            led.evaluate("reader").is_none(),
            "a bare external read must build even under an unrelated failure"
        );
    }

    /// Quoted physical reads (double-quote, backtick, and mixed) resolve to the
    /// producer just like the unquoted form and are contained.
    #[test]
    fn quoted_physical_reads_are_contained() {
        for read in [
            "\"marts\".\"orders_current\"",         // double-quoted
            "`marts`.`orders_current`",             // backtick-quoted
            "marts.\"orders_current\"",             // mixed
            "\"cat\".\"marts\".\"orders_current\"", // 3-part quoted
        ] {
            let mut led = ledger(
                &[
                    ("stage", &[], &[], true),
                    ("rollup", &["anchor"], &[read], true),
                    ("anchor", &[], &[], true),
                ],
                &[("stage", "cat", "marts", "orders_current")],
            );
            led.poison("stage");
            let dec = led
                .evaluate("rollup")
                .unwrap_or_else(|| panic!("quoted read {read:?} must be contained"));
            assert!(
                dec.blocked_by.contains(&"stage".to_string()),
                "quoted read {read:?} must name the producer: {:?}",
                dec.blocked_by
            );
        }
    }

    /// A quoted read of a genuinely EXTERNAL table (no producer) is external —
    /// it builds (not over-contained).
    #[test]
    fn quoted_external_read_builds() {
        let mut led = ledger(
            &[("m", &[], &["\"raw\".\"external\""], true)],
            &[("other", "", "marts", "orders_current")],
        );
        led.poison("other");
        assert!(
            led.evaluate("m").is_none(),
            "quoted external read must build"
        );
    }

    /// An un-canonicalizable read (unbalanced quote, or a quoted segment with an
    /// embedded dot) fails closed once a failure has occurred.
    #[test]
    fn uncanonicalizable_read_fails_closed() {
        for read in ["\"main\".\"orders", "\"weird.name\""] {
            let mut led = ledger(
                &[
                    ("bad", &[], &[], true),
                    ("reader", &["anchor"], &[read], true),
                    ("anchor", &[], &[], true),
                ],
                &[("p", "", "marts", "orders")],
            );
            led.poison("bad");
            let dec = led
                .evaluate("reader")
                .unwrap_or_else(|| panic!("un-canonicalizable read {read:?} must fail closed"));
            assert!(
                dec.fail_closed,
                "read {read:?} must trip the fail-closed belt"
            );
        }
    }

    /// Canonicalization unit cases: quote styles, mixed, escapes, and the
    /// fail-closed (`None`) cases.
    #[test]
    fn canonicalize_identifier_cases() {
        let parts = |v: &[&str]| Some(v.iter().copied().map(String::from).collect::<Vec<_>>());
        assert_eq!(
            canonicalize_identifier("main.orders"),
            parts(&["main", "orders"])
        );
        assert_eq!(
            canonicalize_identifier("\"MAIN\".\"Orders\""),
            parts(&["main", "orders"]),
            "quotes stripped and lowercased"
        );
        assert_eq!(
            canonicalize_identifier("`cat`.`marts`.`t`"),
            parts(&["cat", "marts", "t"])
        );
        assert_eq!(
            canonicalize_identifier("main.\"orders\""),
            parts(&["main", "orders"]),
            "mixed quoting"
        );
        // Doubled quote = escaped literal quote within the segment.
        assert_eq!(canonicalize_identifier("\"a\"\"b\""), parts(&["a\"b"]));
        // Fail-closed cases.
        assert_eq!(
            canonicalize_identifier("\"main\".\"orders"),
            None,
            "unbalanced"
        );
        assert_eq!(canonicalize_identifier("\"we.ird\""), None, "embedded dot");
        assert_eq!(canonicalize_identifier("a..b"), None, "empty segment");
        assert_eq!(canonicalize_identifier(".orders"), None, "leading dot");
    }

    /// (c) Fail-closed belt: an unenumerable read set (not provably complete) is
    /// contained once any failure has occurred, never built — Finding-2 /
    /// invariant-1 guard.
    #[test]
    fn unprovable_reads_fail_closed() {
        let mut led = ledger(
            &[
                ("bad", &[], &[], true),
                ("mystery", &["anchor"], &[], false), // reads NOT provably complete
                ("anchor", &[], &[], true),
            ],
            &[],
        );
        // No failure yet ⇒ even an uncertain model may build.
        assert!(led.evaluate("mystery").is_none(), "no failure ⇒ build");
        led.poison("bad");
        let dec = led
            .evaluate("mystery")
            .expect("uncertain ⇒ fail closed after a failure");
        assert!(dec.fail_closed, "contained by the fail-closed belt");
    }

    /// An ambiguous physical read (matches producers in two catalogs) fails
    /// closed once a failure has occurred.
    #[test]
    fn ambiguous_physical_read_fails_closed() {
        let mut led = ledger(
            &[
                ("bad", &[], &[], true),
                ("reader", &["anchor"], &["marts.orders"], true),
                ("anchor", &[], &[], true),
            ],
            &[
                ("p1", "cat_a", "marts", "orders"),
                ("p2", "cat_b", "marts", "orders"),
            ],
        );
        led.poison("bad");
        let dec = led
            .evaluate("reader")
            .expect("ambiguous read ⇒ fail closed");
        assert!(dec.fail_closed);
    }

    /// A model unknown to the ledger fails closed once a failure has occurred —
    /// the coverage guarantee (Finding 2: never trust the map as total).
    #[test]
    fn unknown_model_fails_closed() {
        let mut led = ledger(&[("known", &[], &[], true)], &[]);
        // No poison ⇒ an unknown model is not spuriously contained.
        assert!(led.evaluate("ghost").is_none());
        led.poison("something");
        let dec = led
            .evaluate("ghost")
            .expect("unknown model ⇒ fail closed after a failure");
        assert!(
            dec.fail_closed,
            "a model absent from the edge map must fail closed"
        );
    }

    /// The full downstream closure of a failure is contained transitively when
    /// evaluated in topological order (poisoning as we go).
    #[test]
    fn full_closure_contained_in_topo_order() {
        let mut led = ledger(
            &[
                ("A", &[], &[], true),
                ("B", &["A"], &["a"], true),
                ("C", &["B"], &["b"], true),
            ],
            &[],
        );
        led.poison("A");
        assert!(led.evaluate("B").is_some());
        led.poison("B"); // withheld B poisons its own downstream
        assert!(led.evaluate("C").is_some());
    }

    /// A compile-failed model seeded up front contains its downstream.
    #[test]
    fn seeded_failure_contains_downstream() {
        let mut led = ledger(
            &[("bad", &[], &[], true), ("dep", &["bad"], &["bad"], true)],
            &[],
        );
        led.seed_failed(["bad".to_string()]);
        assert!(led.evaluate("dep").is_some());
    }

    #[test]
    fn unblock_hint_wording() {
        assert!(unblock_hint(&["a".to_string()], false).contains("resolve upstream 'a'"));
        assert!(unblock_hint(&["a".to_string(), "b".to_string()], true).contains("cannot prove"));
        assert!(!unblock_hint(&[], true).is_empty());
    }

    fn layer_of(plan: &LayerPlan, model: &str) -> Option<usize> {
        plan.layers
            .iter()
            .position(|l| l.iter().any(|m| m == model))
    }

    /// Augmented layering orders a physical/bare reader strictly after its
    /// producer, even with NO `ref()` edge between them — the ordering fix for
    /// the same-layer `--parallel` race.
    #[test]
    fn augmented_layers_order_physical_reader_after_producer() {
        // `rollup` has no ref dep on `stage`; it only physically reads its
        // target `main.orders_current`. Ref-only layering would co-schedule
        // them; augmented layering must not.
        let led = ledger(
            &[
                ("stage", &[], &[], true),
                ("rollup", &[], &["main.orders_current"], true),
            ],
            &[
                ("stage", "", "main", "orders_current"),
                ("rollup", "", "main", "rollup"),
            ],
        );
        let plan = led.augmented_layers();
        assert!(plan.cyclic.is_empty());
        let (ls, lr) = (
            layer_of(&plan, "stage").unwrap(),
            layer_of(&plan, "rollup").unwrap(),
        );
        assert!(
            lr > ls,
            "reader (layer {lr}) must be strictly after producer (layer {ls})"
        );
    }

    /// Invariant: no two models in the same augmented layer share a containment
    /// edge (longest-path layering guarantees this; the executor relies on it so
    /// prior-layer poison is fully known before a layer dispatches).
    #[test]
    fn augmented_layers_no_same_layer_shared_edge() {
        let led = ledger(
            &[
                ("a", &[], &[], true),
                ("b", &["a"], &["a"], true),               // ref edge a→b
                ("c", &[], &["main.a_out"], true),         // physical edge a→c
                ("d", &["b"], &["b", "main.c_out"], true), // ref b→d, physical c→d
            ],
            &[("a", "", "main", "a_out"), ("c", "", "main", "c_out")],
        );
        let plan = led.augmented_layers();
        assert!(plan.cyclic.is_empty());
        // For every layer, no pair shares an edge (neither is the other's
        // upstream over the augmented graph).
        for layer in &plan.layers {
            for i in 0..layer.len() {
                for j in (i + 1)..layer.len() {
                    let (x, y) = (&layer[i], &layer[j]);
                    let x_ups = led.upstreams.get(x).cloned().unwrap_or_default();
                    let y_ups = led.upstreams.get(y).cloned().unwrap_or_default();
                    assert!(
                        !x_ups.contains(y) && !y_ups.contains(x),
                        "same-layer pair {x:?},{y:?} shares a containment edge"
                    );
                }
            }
        }
    }

    /// A physical/bare edge that forms a cycle the ref-graph lacked → fail
    /// closed: the stuck set is surfaced in `cyclic` (for the caller to
    /// contain), not silently dropped or mis-ordered.
    #[test]
    fn augmented_layers_cycle_is_contained() {
        // A refs B (edge B→A); B physically reads A's target `main.a` (edge
        // A→B) ⇒ cycle A↔B.
        let led = ledger(
            &[("A", &["B"], &["b"], true), ("B", &[], &["main.a"], true)],
            &[("A", "", "main", "a"), ("B", "", "main", "b")],
        );
        let plan = led.augmented_layers();
        assert_eq!(plan.cyclic, vec!["A".to_string(), "B".to_string()]);
        assert!(
            plan.layers
                .iter()
                .all(|l| l.is_empty() || !l.contains(&"A".to_string()))
        );
    }
}
