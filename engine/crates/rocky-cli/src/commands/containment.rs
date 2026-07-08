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
//! complete. Residual boundary: a physical read of a producer that is *not*
//! topologically earlier is a pre-existing undeclared-dependency ordering issue
//! (the read sees prior-run data regardless of containment) — declare the
//! dependency via `ref()`/model name.

use std::collections::{BTreeSet, HashMap, HashSet};

/// Index of every model's produced target, for resolving a physical `FROM` /
/// `JOIN` read back to the model that produces that table.
///
/// Two lookups mirror how a physical read can be written:
/// - `by_full` — an explicit 3-part `catalog.schema.table` read matches a
///   producer's full target exactly.
/// - `by_schema_table` — a 2-part `schema.table` read omits the catalog; it
///   matches producers on `(schema, table)`. If more than one producer (in
///   different catalogs) shares that `(schema, table)`, the read is *ambiguous*.
///
/// All keys are lowercased. A model appears once per lookup.
pub(crate) struct ProducerIndex {
    by_full: HashMap<(String, String, String), Vec<String>>,
    by_schema_table: HashMap<(String, String), Vec<String>>,
}

/// How a single physical read resolves against the produced targets.
enum ReadResolution {
    /// Resolves to exactly one produced target — add a containment edge.
    Edge(String),
    /// Matches more than one produced target (multi-catalog `schema.table`) —
    /// cannot be attributed, so the reader fails closed.
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
                .entry((s, t))
                .or_default()
                .push(name.to_string());
        }
        Self {
            by_full,
            by_schema_table,
        }
    }

    /// Resolve one lowercased physical read identity to a produced target.
    ///
    /// Only 2-part (`schema.table`) and 3-part (`catalog.schema.table`) reads
    /// are physical-target shapes. A bare (0-dot) read is either a model name
    /// (already a `ref()` edge, handled by the caller) or an external table; a
    /// read with more than three parts is out of range. Both resolve to
    /// [`ReadResolution::External`].
    fn resolve(&self, read: &str) -> ReadResolution {
        let parts: Vec<&str> = read.split('.').collect();
        let candidates: &[String] = match parts.as_slice() {
            [catalog, schema, table] => self
                .by_full
                .get(&(
                    (*catalog).to_string(),
                    (*schema).to_string(),
                    (*table).to_string(),
                ))
                .map(Vec::as_slice)
                .unwrap_or(&[]),
            [schema, table] => self
                .by_schema_table
                .get(&((*schema).to_string(), (*table).to_string()))
                .map(Vec::as_slice)
                .unwrap_or(&[]),
            _ => &[],
        };
        match candidates {
            [] => ReadResolution::External,
            [one] => ReadResolution::Edge(one.clone()),
            _ => ReadResolution::Ambiguous,
        }
    }
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
        model_names: &HashSet<String>,
        reads: &[String],
        reads_complete: bool,
        producers: &ProducerIndex,
    ) {
        self.known.insert(name.to_string());
        let mut ups: BTreeSet<String> = ref_deps.iter().cloned().collect();
        // A read set we cannot fully enumerate cannot prove disjointness.
        let mut uncertain = !reads_complete;
        for read in reads {
            // A bare model-name read is already a `ref()` edge (in `ref_deps`).
            if model_names.contains(read) {
                continue;
            }
            match producers.resolve(read) {
                // A physical read of another model's produced target.
                ReadResolution::Edge(producer) if producer != name => {
                    ups.insert(producer);
                }
                // A model reading its own target (e.g. incremental self-ref) is
                // not a cross-model edge.
                ReadResolution::Edge(_) => {}
                // Cannot attribute the read to a single producer — fail closed.
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
        let names: HashSet<String> = models.iter().map(|(n, ..)| n.to_string()).collect();
        let mut led = ContainmentLedger::new();
        for (name, deps, reads, complete) in models {
            let deps: Vec<String> = deps.iter().copied().map(String::from).collect();
            let reads: Vec<String> = reads.iter().map(|s| s.to_lowercase()).collect();
            led.add_model(name, &deps, &names, &reads, *complete, &index);
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
}
