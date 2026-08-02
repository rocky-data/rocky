//! Target-aware physical-read edge derivation (#1275).
//!
//! A model that reads another model's target by its physical
//! `schema.table` / `catalog.schema.table` name gets no compile-time DAG
//! edge (`classify_table_ref` treats multi-part names as external), so the
//! two can be co-scheduled and race under concurrency. This module derives
//! the missing ordering edges at RUN time, from each model's rendered
//! target components matched against each model's SQL read set — leaving
//! the compile-time graph, the `E001` join-key surface, and the `rocky dag`
//! export untouched.
//!
//! One derivation, two consumers: the plain-run layer computation
//! (`compile_result.project.dag_nodes` → `execution_layers`) and the
//! `run --dag` [`crate::unified_dag::UnifiedDag`] (whose executor computes
//! its own phases). Both schedulers must see the same edges or the race
//! survives one flag away — the two-scheduler split is exactly how the
//! first fix attempt failed review.
//!
//! Comparison is componentwise and case-folded on BOTH sides (never fold
//! one side only). On a case-sensitive store two distinct tables differing
//! by case can therefore match and yield a spurious edge — that is the safe
//! error direction: an unnecessary ordering constraint, never a missed one.
//!
//! Cycle policy: a derived edge that would close a cycle (mutual physical
//! reads) is SKIPPED deterministically and reported, so projects that run
//! today keep running — serialized where possible, never refused. The
//! schedulers' own cycle detection stays the backstop for declared edges.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

/// One model's identity for the derivation: its name, its rendered target
/// components, and its compiled SQL.
#[derive(Debug, Clone)]
pub struct PhysicalEdgeModel<'a> {
    pub name: &'a str,
    pub catalog: &'a str,
    pub schema: &'a str,
    pub table: &'a str,
    pub sql: &'a str,
}

impl<'a> PhysicalEdgeModel<'a> {
    /// Both consumers build inputs from the same loaded-model shape; one
    /// constructor keeps them from drifting.
    #[must_use]
    pub fn from_model(m: &'a crate::models::Model) -> Self {
        Self {
            name: &m.config.name,
            catalog: &m.config.target.catalog,
            schema: &m.config.target.schema,
            table: &m.config.target.table,
            sql: &m.sql,
        }
    }
}

/// The derivation result. `edges` are `(consumer, producer)` pairs —
/// consumer depends on producer — already deduplicated against `existing`
/// and guaranteed not to close a cycle over `existing ∪ edges`.
#[derive(Debug, Default)]
pub struct DerivedPhysicalEdges {
    pub edges: Vec<(String, String)>,
    /// Candidate edges skipped because adding them would close a dependency
    /// cycle (mutual physical reads). The pair still gets ordered by the
    /// accepted direction; the skipped closer is surfaced as a warning.
    pub skipped_cycle_edges: Vec<(String, String)>,
    /// Models whose SQL could not be parsed for table references — their
    /// read set is unknown, so no edges could be derived for them. Surfaced
    /// as a warning under concurrency: co-scheduling them is unproven.
    pub unparsed: Vec<String>,
    /// Pairs of models whose canonical targets collide. Ordering between
    /// them is ill-defined; surfaced as a warning (writer refusal for the
    /// same-pipeline case is upstream's job).
    pub target_collisions: Vec<(String, String)>,
}

/// `referenced_tables` already lowercases and unquotes what the parser
/// returns, so folding the READ side here is defense-in-depth; the fold is
/// load-bearing for the TARGET side (configured `[target]` components come
/// through verbatim).
fn fold(s: &str) -> String {
    s.trim()
        .trim_matches('"')
        .trim_matches('`')
        .to_ascii_lowercase()
}

/// Derive physical-read ordering edges for `models`.
///
/// `existing` is the name-level dependency relation already present in the
/// consumer's graph (`(consumer, producer)` pairs — declared `depends_on`
/// plus compile-derived edges). Derived edges only ever connect two models
/// from `models`, so a name-level cycle guard over `existing ∪ accepted`
/// is sound: no derived cycle can pass through a non-model node.
#[must_use]
pub fn derive_physical_edges(
    models: &[PhysicalEdgeModel<'_>],
    existing: &[(String, String)],
) -> DerivedPhysicalEdges {
    let mut out = DerivedPhysicalEdges::default();

    // Producer index: canonical (catalog, schema, table) and (schema, table)
    // → model names. Multi-maps — a collision must not silently drop a
    // producer (the label-heuristic HashMap overwrite bug class).
    let mut by_three: BTreeMap<(String, String, String), Vec<&str>> = BTreeMap::new();
    let mut by_two: BTreeMap<(String, String), Vec<&str>> = BTreeMap::new();
    for m in models {
        let key3 = (fold(m.catalog), fold(m.schema), fold(m.table));
        let key2 = (key3.1.clone(), key3.2.clone());
        by_three.entry(key3).or_default().push(m.name);
        by_two.entry(key2).or_default().push(m.name);
    }
    for names in by_three.values() {
        if names.len() > 1 {
            for pair in names.windows(2) {
                out.target_collisions
                    .push((pair[0].to_string(), pair[1].to_string()));
            }
        }
    }

    // Existing relation as a set + adjacency for the cycle guard.
    let mut edge_set: HashSet<(String, String)> = existing.iter().cloned().collect();
    let mut depends_on: HashMap<String, HashSet<String>> = HashMap::new();
    for (c, p) in existing {
        depends_on.entry(c.clone()).or_default().insert(p.clone());
    }

    // `consumer` transitively depends on `target`?
    fn reaches(depends_on: &HashMap<String, HashSet<String>>, from: &str, to: &str) -> bool {
        let mut seen: HashSet<&str> = HashSet::new();
        let mut stack: Vec<&str> = vec![from];
        while let Some(cur) = stack.pop() {
            if cur == to {
                return true;
            }
            if !seen.insert(cur) {
                continue;
            }
            if let Some(deps) = depends_on.get(cur) {
                stack.extend(deps.iter().map(String::as_str));
            }
        }
        false
    }

    // Candidate edges in deterministic order (BTreeSet), so the accepted
    // direction of a mutual pair never depends on iteration order.
    let mut candidates: BTreeSet<(String, String)> = BTreeSet::new();
    for m in models {
        let refs = match rocky_sql::lineage::referenced_tables(m.sql) {
            Ok(refs) => refs,
            Err(_) => {
                out.unparsed.push(m.name.to_string());
                continue;
            }
        };
        for r in refs {
            let parts: Vec<String> = r.split('.').map(fold).collect();
            let producers: Option<&Vec<&str>> = match parts.len() {
                // Bare names are the compiler's business (ModelRef) — a bare
                // read matching a model name already has its edge; a bare
                // read of a physical table is external.
                3 => by_three.get(&(parts[0].clone(), parts[1].clone(), parts[2].clone())),
                2 => by_two.get(&(parts[0].clone(), parts[1].clone())),
                _ => None,
            };
            let Some(producers) = producers else { continue };
            for p in producers {
                if *p != m.name {
                    candidates.insert((m.name.to_string(), (*p).to_string()));
                }
            }
        }
    }

    for (consumer, producer) in candidates {
        if edge_set.contains(&(consumer.clone(), producer.clone())) {
            continue;
        }
        // Adding consumer→producer closes a cycle iff producer already
        // (transitively) depends on consumer.
        if reaches(&depends_on, &producer, &consumer) {
            out.skipped_cycle_edges.push((consumer, producer));
            continue;
        }
        edge_set.insert((consumer.clone(), producer.clone()));
        depends_on
            .entry(consumer.clone())
            .or_default()
            .insert(producer.clone());
        out.edges.push((consumer, producer));
    }

    out
}

/// Render the standard warnings for a derivation, shared by both consumers
/// so the operator-visible text stays identical across `run` and
/// `run --dag`.
#[must_use]
pub fn derivation_warnings(derived: &DerivedPhysicalEdges) -> Vec<String> {
    let mut w = Vec::new();
    for (a, b) in &derived.skipped_cycle_edges {
        w.push(format!(
            "mutual physical reads between '{a}' and '{b}': ordered by the derived edge that \
             was accepted first; declare depends_on to choose the order explicitly"
        ));
    }
    for m in &derived.unparsed {
        w.push(format!(
            "model '{m}': SQL could not be parsed for table references — physical-read \
             ordering could not be derived for it; concurrent execution against its upstreams \
             is unproven"
        ));
    }
    for (a, b) in &derived.target_collisions {
        w.push(format!(
            "models '{a}' and '{b}' render the same physical target — ordering between them \
             is ill-defined"
        ));
    }
    w
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m<'a>(
        name: &'a str,
        catalog: &'a str,
        schema: &'a str,
        table: &'a str,
        sql: &'a str,
    ) -> PhysicalEdgeModel<'a> {
        PhysicalEdgeModel {
            name,
            catalog,
            schema,
            table,
            sql,
        }
    }

    /// The issue's repro: a 2-part physical read derives the edge that
    /// compile-time resolution cannot.
    #[test]
    fn a_two_part_physical_read_derives_the_edge() {
        let models = [
            m("orders", "db", "main", "orders", "SELECT 1 AS id"),
            m(
                "mart_qualified",
                "db",
                "main",
                "mart_qualified",
                "SELECT id FROM main.orders",
            ),
        ];
        let d = derive_physical_edges(&models, &[]);
        assert_eq!(
            d.edges,
            vec![("mart_qualified".to_string(), "orders".to_string())]
        );
        assert!(d.skipped_cycle_edges.is_empty() && d.unparsed.is_empty());
    }

    /// A renamed target (model name ≠ table name) still matches — the case
    /// the label heuristic misses.
    #[test]
    fn a_renamed_target_still_matches_by_components() {
        let models = [
            m("orders_model", "db", "main", "orders_v2", "SELECT 1 AS id"),
            m(
                "mart",
                "db",
                "main",
                "mart",
                "SELECT id FROM db.main.orders_v2",
            ),
        ];
        let d = derive_physical_edges(&models, &[]);
        assert_eq!(
            d.edges,
            vec![("mart".to_string(), "orders_model".to_string())]
        );
    }

    /// Case and quoting fold on BOTH sides.
    #[test]
    fn comparison_folds_case_and_quotes_uniformly() {
        let models = [
            m("orders", "DB", "Main", "Orders", "SELECT 1 AS id"),
            m(
                "mart",
                "db",
                "main",
                "mart",
                "SELECT id FROM \"MAIN\".\"ORDERS\"",
            ),
        ];
        let d = derive_physical_edges(&models, &[]);
        assert_eq!(d.edges, vec![("mart".to_string(), "orders".to_string())]);
    }

    /// Mutual physical reads: one deterministic direction is accepted, the
    /// closer is skipped and reported — never a cycle, never a refusal.
    #[test]
    fn mutual_reads_serialize_deterministically_instead_of_cycling() {
        let models = [
            m("a", "db", "main", "a", "SELECT x FROM main.b"),
            m("b", "db", "main", "b", "SELECT y FROM main.a"),
        ];
        let d = derive_physical_edges(&models, &[]);
        // BTreeSet order: ("a","b") accepted first, ("b","a") closes → skipped.
        assert_eq!(d.edges, vec![("a".to_string(), "b".to_string())]);
        assert_eq!(
            d.skipped_cycle_edges,
            vec![("b".to_string(), "a".to_string())]
        );
    }

    /// A candidate that would close a cycle THROUGH an existing declared
    /// edge is skipped too.
    #[test]
    fn existing_declared_edges_participate_in_the_cycle_guard() {
        let models = [
            m("up", "db", "main", "up", "SELECT x FROM main.down"),
            m("down", "db", "main", "down", "SELECT 1 AS x"),
        ];
        // Declared: down depends on up.
        let existing = vec![("down".to_string(), "up".to_string())];
        let d = derive_physical_edges(&models, &existing);
        // Candidate up→down would close the cycle → skipped.
        assert!(d.edges.is_empty());
        assert_eq!(
            d.skipped_cycle_edges,
            vec![("up".to_string(), "down".to_string())]
        );
    }

    /// Unparseable SQL is reported, never silently skipped.
    #[test]
    fn unparseable_sql_is_reported_not_silent() {
        let models = [
            m("ok", "db", "main", "ok", "SELECT 1 AS x"),
            m("broken", "db", "main", "broken", "SELEC x FRM ("),
        ];
        let d = derive_physical_edges(&models, &[]);
        assert_eq!(d.unparsed, vec!["broken".to_string()]);
    }

    /// Target collisions surface both names instead of one overwriting the
    /// other in the producer index.
    #[test]
    fn target_collisions_are_reported_and_both_producers_match() {
        let models = [
            m("first", "db", "main", "shared", "SELECT 1 AS x"),
            m("second", "db", "main", "shared", "SELECT 2 AS x"),
            m(
                "reader",
                "db",
                "main",
                "reader",
                "SELECT x FROM main.shared",
            ),
        ];
        let d = derive_physical_edges(&models, &[]);
        assert_eq!(d.target_collisions.len(), 1);
        // The reader is ordered after BOTH claimants.
        let mut producers: Vec<&str> = d
            .edges
            .iter()
            .filter(|(c, _)| c == "reader")
            .map(|(_, p)| p.as_str())
            .collect();
        producers.sort_unstable();
        assert_eq!(producers, vec!["first", "second"]);
    }

    /// A bare single-part read never derives an edge here — that is the
    /// compiler's surface, and matching table-only would be far too loose.
    #[test]
    fn bare_reads_are_left_to_the_compiler() {
        let models = [
            m("orders", "db", "main", "orders", "SELECT 1 AS id"),
            m("mart", "db", "main", "mart", "SELECT id FROM orders"),
        ];
        let d = derive_physical_edges(&models, &[]);
        assert!(d.edges.is_empty());
    }
}
