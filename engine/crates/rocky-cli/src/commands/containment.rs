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
//! (`rocky_core::quarantine`, `QuarantineMode::{Split,Tag,Drop}`,
//! `__valid`/`__quarantine` tables), which splits bad *rows* out of one table.
//! Containment operates on the *model graph*: it withholds whole downstream
//! *models* when an upstream fails. The vocabulary here is deliberately
//! distinct — `Contained` / "blocked by" — so a reader never confuses a
//! withheld model with a quarantined row.
//!
//! # Soundness
//!
//! The closure is the soundness surface. The invariant: **a downstream of a
//! failed model must never build on stale or missing input.** So the ledger is
//! *conservative* — it withholds a model whenever any of its resolved upstreams
//! is failed-or-withheld, contain-more on any doubt.
//!
//! Two facts make a direct-upstream check transitively complete:
//!
//! - The edge set is the *resolved* dependency graph
//!   ([`rocky_ir::dag::DagNode::depends_on`] — explicit `depends_on` **merged
//!   with** the SQL-`ref()`-derived deps the executor's layering was built
//!   from). Using the executor's own edges means a model that lands in a later
//!   layer *because of* an edge is contained by that same edge — no
//!   SQL-only dependency slips through un-contained.
//! - The caller evaluates models in topological order (layer by layer), marking
//!   every withheld model into the poison set as it goes. So by the time a model
//!   is considered, every ancestor that failed or was withheld is already in the
//!   poison set, and `blocked_by` (a one-hop check against the poison set) sees
//!   it.

use std::collections::{BTreeSet, HashMap};

use rocky_ir::dag::DagNode;

/// Conservative downstream-closure tracker for one run.
///
/// Holds the resolved dependency edges (owned, cloned once from the project's
/// `dag_nodes`) plus the *poison set*: the union of failed models and models
/// withheld because an upstream was poisoned. A model is withheld iff any of
/// its resolved upstreams is in the poison set.
pub(crate) struct ContainmentLedger {
    /// `model name → its resolved upstream model names` (explicit + SQL-derived,
    /// the same edges the execution layering used).
    deps: HashMap<String, Vec<String>>,
    /// Failed ∪ withheld model names. A model whose upstream is in here is
    /// withheld; the model itself is then added, extending the closure.
    poison: BTreeSet<String>,
}

impl ContainmentLedger {
    /// Build a ledger from the project's resolved DAG nodes.
    ///
    /// The edges are cloned into an owned map so the ledger carries no borrow
    /// of the compile result — the run loop mutates `output` and other state
    /// alongside it without lifetime entanglement. The clone is one small pass
    /// over the node list, paid once per run.
    pub(crate) fn from_dag_nodes(nodes: &[DagNode]) -> Self {
        let deps = nodes
            .iter()
            .map(|n| (n.name.clone(), n.depends_on.clone()))
            .collect();
        Self {
            deps,
            poison: BTreeSet::new(),
        }
    }

    /// Seed the poison set with already-known failed models (e.g. models that
    /// failed to compile and were excluded from execution). Their downstreams
    /// will be withheld just like a runtime failure's.
    pub(crate) fn seed_failed<I>(&mut self, models: I)
    where
        I: IntoIterator<Item = String>,
    {
        self.poison.extend(models);
    }

    /// The poisoned upstreams that reach `model` — its *direct* failed-or-
    /// withheld dependencies. Empty ⇒ the model is safe to build. Non-empty ⇒
    /// the model must be withheld; the returned names are the operator's
    /// "blocked by" list (the run's `errors[]` name the root cause).
    ///
    /// Sorted + de-duplicated for deterministic output.
    pub(crate) fn blocked_by(&self, model: &str) -> Vec<String> {
        let Some(deps) = self.deps.get(model) else {
            return Vec::new();
        };
        let mut blockers: Vec<String> = deps
            .iter()
            .filter(|u| self.poison.contains(*u))
            .cloned()
            .collect();
        blockers.sort();
        blockers.dedup();
        blockers
    }

    /// Add a model to the poison set — a failed cause *or* a withheld
    /// downstream. Either way its own descendants must be withheld, so both
    /// extend the closure identically.
    pub(crate) fn poison(&mut self, model: &str) {
        self.poison.insert(model.to_string());
    }
}

/// A human-readable hint telling the operator how to clear a withheld model:
/// resolve the named upstream failure(s), then re-run.
pub(crate) fn unblock_hint(blocked_by: &[String]) -> String {
    match blocked_by {
        [] => "re-run once the upstream failure is resolved".to_string(),
        [one] => format!("resolve upstream '{one}', then re-run"),
        many => format!("resolve upstream(s) {}, then re-run", many.join(", ")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(name: &str, deps: &[&str]) -> DagNode {
        DagNode {
            name: name.to_string(),
            depends_on: deps.iter().copied().map(String::from).collect(),
        }
    }

    /// Drive the ledger over a topologically-ordered model list exactly as the
    /// run loop does: a model with a poisoned upstream is withheld (and itself
    /// poisoned); an already-failed model is seeded; everything else builds.
    /// Returns `(built, withheld)` model-name sets.
    fn simulate(
        nodes: &[DagNode],
        topo_order: &[&str],
        failed: &[&str],
    ) -> (Vec<String>, Vec<String>) {
        let mut ledger = ContainmentLedger::from_dag_nodes(nodes);
        let failed_set: BTreeSet<&str> = failed.iter().copied().collect();
        let mut built = Vec::new();
        let mut withheld = Vec::new();
        for &name in topo_order {
            // A model that is itself the failure: mark failed, don't build.
            if failed_set.contains(name) {
                ledger.poison(name);
                continue;
            }
            let blockers = ledger.blocked_by(name);
            if blockers.is_empty() {
                built.push(name.to_string());
            } else {
                ledger.poison(name);
                withheld.push(name.to_string());
            }
        }
        (built, withheld)
    }

    /// (a) Disjoint subtrees still run. Graph: A→B and C→D are two independent
    /// chains. A fails ⇒ B is withheld, but C and D (disjoint) still build.
    #[test]
    fn disjoint_subtree_still_builds() {
        let nodes = [
            node("A", &[]),
            node("B", &["A"]),
            node("C", &[]),
            node("D", &["C"]),
        ];
        let (built, withheld) = simulate(&nodes, &["A", "C", "B", "D"], &["A"]);
        assert_eq!(built, vec!["C", "D"], "the disjoint C→D subtree must build");
        assert_eq!(withheld, vec!["B"], "only A's downstream is withheld");
    }

    /// (b) The full downstream closure of a failure is contained. Chain
    /// A→B→C→D; A fails ⇒ B, C, and D are ALL withheld (transitive closure),
    /// even though only B depends on A directly — C is withheld because B is
    /// poisoned, D because C is.
    #[test]
    fn full_downstream_closure_is_contained() {
        let nodes = [
            node("A", &[]),
            node("B", &["A"]),
            node("C", &["B"]),
            node("D", &["C"]),
        ];
        let (built, withheld) = simulate(&nodes, &["A", "B", "C", "D"], &["A"]);
        assert!(built.is_empty(), "nothing survives A's failure in a chain");
        assert_eq!(
            withheld,
            vec!["B", "C", "D"],
            "the whole closure is withheld"
        );
    }

    /// (c) A diamond: A→{B,C}→D. A fails ⇒ B, C, and their shared join D are
    /// all withheld. D is blocked by BOTH B and C (both poisoned).
    #[test]
    fn diamond_closure_and_multi_blocker() {
        let nodes = [
            node("A", &[]),
            node("B", &["A"]),
            node("C", &["A"]),
            node("D", &["B", "C"]),
        ];
        let mut ledger = ContainmentLedger::from_dag_nodes(&nodes);
        ledger.poison("A"); // A failed
        assert_eq!(ledger.blocked_by("B"), vec!["A"]);
        ledger.poison("B");
        assert_eq!(ledger.blocked_by("C"), vec!["A"]);
        ledger.poison("C");
        // D is reachable from A through two paths — both blockers surface.
        assert_eq!(ledger.blocked_by("D"), vec!["B", "C"]);
    }

    /// A model whose upstreams are all healthy is never withheld.
    #[test]
    fn healthy_model_is_not_blocked() {
        let nodes = [node("A", &[]), node("B", &["A"])];
        let mut ledger = ContainmentLedger::from_dag_nodes(&nodes);
        ledger.poison("A");
        // B depends on A (poisoned) → blocked.
        assert!(!ledger.blocked_by("B").is_empty());
        // But a fresh sibling with no poisoned deps is clear.
        assert!(ledger.blocked_by("A").is_empty());
    }

    /// A compile-failed model seeded up front withholds its runtime downstream
    /// exactly like a runtime failure would — a compile error is a failure too.
    #[test]
    fn seeded_compile_failure_contains_downstream() {
        let nodes = [node("bad", &[]), node("dep", &["bad"])];
        let mut ledger = ContainmentLedger::from_dag_nodes(&nodes);
        ledger.seed_failed(["bad".to_string()]);
        assert_eq!(ledger.blocked_by("dep"), vec!["bad"]);
    }

    /// A poisoned *partitioned* cause blocks only its descendants, not a
    /// disjoint sibling — the model-level rule the partition path relies on
    /// (any failed partition poisons the whole model's downstream).
    #[test]
    fn partitioned_cause_blocks_only_descendants() {
        let nodes = [
            node("events", &[]), // partitioned model, one partition failed
            node("rollup", &["events"]),
            node("unrelated", &[]),
        ];
        let mut ledger = ContainmentLedger::from_dag_nodes(&nodes);
        ledger.poison("events");
        assert_eq!(ledger.blocked_by("rollup"), vec!["events"]);
        assert!(
            ledger.blocked_by("unrelated").is_empty(),
            "a disjoint model must be unaffected by a partition failure elsewhere"
        );
    }

    #[test]
    fn unblock_hint_wording() {
        assert!(unblock_hint(&["a".to_string()]).contains("resolve upstream 'a'"));
        assert!(unblock_hint(&["a".to_string(), "b".to_string()]).contains("a, b"));
        assert!(!unblock_hint(&[]).is_empty());
    }
}
