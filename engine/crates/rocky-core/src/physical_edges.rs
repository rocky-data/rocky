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
    /// Whether this model actually writes its configured target. An
    /// ephemeral model (inlined as a CTE, no table created) must never be
    /// indexed as a PRODUCER: its configured target does not exist, so a
    /// physical read of that name is a read of something else — deriving an
    /// edge to the phantom would order the reader after a producer that
    /// produces nothing, and could suppress the reader's true edges via the
    /// cycle guard.
    pub materializes: bool,
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
            materializes: !matches!(m.config.strategy, crate::models::StrategyConfig::Ephemeral),
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
    /// Mutual candidates where BOTH directions come from the loosest (bare
    /// table-component) tier. Bare matches are guesses — accepting one
    /// direction by name order could deterministically REVERSE a real
    /// producer edge, which is worse than today's undefined order. Neither
    /// edge is derived; the pair is surfaced for an explicit `depends_on`.
    pub ambiguous_bare_pairs: Vec<(String, String)>,
}

/// `referenced_tables` already lowercases and unquotes what the parser
/// returns, so folding the READ side here is defense-in-depth; the fold is
/// load-bearing for the TARGET side (configured `[target]` components come
/// through verbatim).
///
/// Public because the compile-time resolver asks the same question about the
/// same two strings (#1354) and a second spelling of this fold is a second
/// matcher that can disagree with this one.
#[must_use]
pub fn fold_identifier(s: &str) -> String {
    s.trim()
        .trim_matches('"')
        .trim_matches('`')
        .to_ascii_lowercase()
}

/// Whether a bare reference spelled with `model_name` names the table this
/// model actually writes.
///
/// A bare `FROM x` carries no catalog and no schema, so the warehouse
/// resolves it through connection state (search path / current schema) to a
/// PHYSICAL table called `x`. That table can only be this model's output when
/// the model's target table component is itself `x`. A model named
/// `customers` whose configured target is `prod.customers_v2` therefore does
/// not answer to a bare `FROM customers`: the name matches, the object does
/// not (#1354).
///
/// This is a question about the WAREHOUSE. `rocky test` / `rocky ci` go
/// through `rocky_engine::executor::execute_locally`, which materializes every
/// model as `CREATE OR REPLACE TABLE <model name>` and ignores the configured
/// target — there a bare read of the name always reaches the model, whatever
/// this returns. Callers must know which execution they are reasoning about.
///
/// The comparison folds both sides through [`fold_identifier`], so a project
/// that spells its targets in upper case (`[target] table = "CUSTOMERS"` for
/// model `customers` — the common Snowflake shape) still binds.
///
/// Two callers, one spelling: `rocky_compiler::resolve::resolve_dependencies`
/// (which reports the mismatch as D012 and keeps the edge — see #1354 for why
/// dropping it is not that layer's call) and the content-reuse read resolver
/// in `rocky-cli` (which refuses to bind the read, failing closed to a build).
#[must_use]
pub fn bare_name_binds(model_name: &str, target_table: &str) -> bool {
    fold_identifier(model_name) == fold_identifier(target_table)
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
    let mut by_table: BTreeMap<String, Vec<&str>> = BTreeMap::new();
    for m in models {
        if !m.materializes {
            continue;
        }
        let key3 = (
            fold_identifier(m.catalog),
            fold_identifier(m.schema),
            fold_identifier(m.table),
        );
        let key2 = (key3.1.clone(), key3.2.clone());
        by_table.entry(key3.2.clone()).or_default().push(m.name);
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

    // Candidate edges in deterministic order, MOST SPECIFIC FIRST: an exact
    // three-part match is accepted before a two-part match, which is
    // accepted before a bare table-component match. A loose (possibly
    // spurious) candidate must never occupy the graph first and cause the
    // cycle guard to discard a more specific true edge. Within a
    // specificity tier, name order keeps mutual pairs deterministic.
    let mut candidates: BTreeSet<(u8, String, String)> = BTreeSet::new();
    for m in models {
        let refs = match rocky_sql::lineage::referenced_tables(m.sql) {
            Ok(refs) => refs,
            Err(_) => {
                out.unparsed.push(m.name.to_string());
                continue;
            }
        };
        for r in refs {
            let parts: Vec<String> = r.split('.').map(fold_identifier).collect();
            let (tier, producers): (u8, Option<&Vec<&str>>) = match parts.len() {
                3 => (
                    0,
                    by_three.get(&(parts[0].clone(), parts[1].clone(), parts[2].clone())),
                ),
                2 => (1, by_two.get(&(parts[0].clone(), parts[1].clone()))),
                // A bare read resolves through connection state (search path /
                // current schema) Rocky cannot observe. A bare MODEL-name
                // read already has its compile-time edge; a bare read that
                // matches an in-run model's TABLE component may be that very
                // table — match on the table alone, in the loosest tier.
                1 => (2, by_table.get(&parts[0])),
                _ => (3, None),
            };
            let Some(producers) = producers else { continue };
            for p in producers {
                if *p != m.name {
                    candidates.insert((tier, m.name.to_string(), (*p).to_string()));
                }
            }
        }
    }

    // Decision loop, run to a FIXPOINT. A bare↔bare contradiction WITHDRAWS
    // an already-accepted edge, which can invalidate every skip decided
    // while that edge was in the graph (a three-way bare chain skips a safe
    // edge through the soon-withdrawn one). Each withdrawal moves one pair
    // into the ambiguous set — monotone, so restarting the pass terminates
    // in at most one restart per contradicting pair. Restart-from-scratch
    // keeps every decision derived from a consistent graph.
    let mut ambiguous: HashSet<(String, String)> = HashSet::new();
    'fixpoint: loop {
        out.edges.clear();
        out.skipped_cycle_edges.clear();
        let mut edge_set: HashSet<(String, String)> = existing.iter().cloned().collect();
        let mut depends_on: HashMap<String, HashSet<String>> = HashMap::new();
        for (c, p) in existing {
            depends_on.entry(c.clone()).or_default().insert(p.clone());
        }
        let mut seen_pairs: HashSet<(String, String)> = HashSet::new();
        let mut accepted_tier: HashMap<(String, String), u8> = HashMap::new();
        for (tier, consumer, producer) in &candidates {
            let (tier, consumer, producer) = (*tier, consumer.clone(), producer.clone());
            if ambiguous.contains(&(consumer.clone(), producer.clone()))
                || ambiguous.contains(&(producer.clone(), consumer.clone()))
            {
                continue;
            }
            if !seen_pairs.insert((consumer.clone(), producer.clone())) {
                continue;
            }
            if edge_set.contains(&(consumer.clone(), producer.clone())) {
                continue;
            }
            // Adding consumer→producer closes a cycle iff producer already
            // (transitively) depends on consumer.
            if reaches(&depends_on, &producer, &consumer) {
                // A bare candidate blocked by an ACCEPTED bare opposite is
                // not a cycle to serialize — it is two guesses contradicting
                // each other. Name order must not pick the winner: a wrong
                // pick deterministically reverses the real producer edge,
                // WORSE than the undefined order it replaces. Mark the pair
                // ambiguous and restart so no decision keeps depending on
                // the withdrawn guess.
                if tier == 2 && accepted_tier.get(&(producer.clone(), consumer.clone())) == Some(&2)
                {
                    ambiguous.insert((consumer.clone(), producer.clone()));
                    out.ambiguous_bare_pairs.push((consumer, producer));
                    continue 'fixpoint;
                }
                out.skipped_cycle_edges.push((consumer, producer));
                continue;
            }
            edge_set.insert((consumer.clone(), producer.clone()));
            depends_on
                .entry(consumer.clone())
                .or_default()
                .insert(producer.clone());
            accepted_tier.insert((consumer.clone(), producer.clone()), tier);
            out.edges.push((consumer, producer));
        }
        break;
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
            "physical-read ordering: the derived edge '{a}' -> '{b}' would close a dependency \
             cycle and was skipped; the pair executes in the already-established order. \
             Declare depends_on to choose the order explicitly"
        ));
    }
    for m in &derived.unparsed {
        w.push(format!(
            "model '{m}': SQL could not be parsed for table references — physical-read \
             ordering could not be derived for it; concurrent execution against its upstreams \
             is unproven"
        ));
    }
    for (a, b) in &derived.ambiguous_bare_pairs {
        w.push(format!(
            "models '{a}' and '{b}' each bare-read a name matching the other's table — both \
             matches are search-path guesses and contradict, so neither ordering was derived; \
             declare depends_on to state the real direction"
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
            materializes: true,
        }
    }

    /// The bare-name rule shared by the compile-time resolver (which reports
    /// it) and the content-reuse read resolver (which refuses to bind on it) —
    /// #1354. A bare read can only be a model's output when the model's
    /// configured target table is spelled that way; case and quoting fold on
    /// both sides so an upper-cased target is the SAME name, not a rename.
    #[test]
    fn bare_name_binds_only_when_the_model_writes_that_name() {
        assert!(bare_name_binds("customers", "customers"));
        assert!(bare_name_binds("customers", "CUSTOMERS"));
        assert!(bare_name_binds("Customers", "customers"));
        assert!(bare_name_binds("customers", "\"customers\""));
        assert!(!bare_name_binds("customers", "customers_v2"));
        // The shared fold trims, exactly as the physical producer index does
        // — one fold, one answer.
        assert!(bare_name_binds("customers", " customers "));
        assert!(!bare_name_binds("stg_customers", "customers"));
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

    /// A bare read matching an in-run model's TABLE component derives the
    /// edge: bare names resolve through connection search-path state Rocky
    /// cannot observe, and `FROM orders_v2` may be exactly the producer's
    /// table. (A bare read matching a MODEL name dedups against the
    /// compile-time edge.)
    #[test]
    fn a_bare_read_of_a_renamed_target_table_derives_the_edge() {
        let models = [
            m("orders_model", "db", "main", "orders_v2", "SELECT 1 AS id"),
            m("mart", "db", "main", "mart", "SELECT id FROM orders_v2"),
        ];
        let d = derive_physical_edges(&models, &[]);
        assert_eq!(
            d.edges,
            vec![("mart".to_string(), "orders_model".to_string())]
        );
    }

    /// A bare read matching a model NAME whose compile-time edge already
    /// exists adds nothing (dedup against `existing`).
    #[test]
    fn bare_model_name_reads_dedup_against_compile_edges() {
        let models = [
            m("orders", "db", "main", "orders", "SELECT 1 AS id"),
            m("mart", "db", "main", "mart", "SELECT id FROM orders"),
        ];
        let existing = vec![("mart".to_string(), "orders".to_string())];
        let d = derive_physical_edges(&models, &existing);
        assert!(d.edges.is_empty(), "{:?}", d.edges);
    }

    /// An ephemeral model produces no table: it must never be indexed as a
    /// producer, or a read of an EXTERNAL table sharing its configured name
    /// would derive an edge to a phantom — and could suppress a true edge
    /// through the cycle guard.
    #[test]
    fn ephemeral_models_are_not_physical_producers() {
        let mut phantom = m("phantom", "db", "main", "phantom_out", "SELECT 1 AS x");
        phantom.materializes = false;
        let models = [
            phantom,
            m(
                "reader",
                "db",
                "main",
                "reader",
                "SELECT x FROM main.phantom_out",
            ),
        ];
        let d = derive_physical_edges(&models, &[]);
        assert!(d.edges.is_empty(), "{:?}", d.edges);
    }

    /// The reviewer's three-way chain: a bare candidate skipped BECAUSE OF
    /// an edge that a later bare-bare withdrawal removes must be
    /// re-evaluated — the fixpoint restart derives it after the ambiguous
    /// pair is excluded.
    #[test]
    fn a_withdrawal_reopens_decisions_that_depended_on_the_withdrawn_edge() {
        // Candidate order (name-sorted within the bare tier):
        //   ("a","b")   accepted first (a depends on b),
        //   ("ab","d")  skipped through d→a→b→ab (uses the accepted edge),
        //   ("b","a")   contradicts → withdrawal → restart.
        // After the restart with (a,b)/(b,a) ambiguous, ("ab","d") is safe.
        let models = [
            m("a", "db", "main", "t_a", "SELECT x FROM t_b"),
            m("b", "db", "main", "t_b", "SELECT y FROM t_a"),
            m("ab", "db", "main", "t_ab", "SELECT z FROM t_d"),
            m("d", "db", "main", "t_d", "SELECT 1 AS q"),
        ];
        let existing = vec![
            ("d".to_string(), "a".to_string()),
            ("b".to_string(), "ab".to_string()),
        ];
        let d = derive_physical_edges(&models, &existing);
        assert_eq!(d.ambiguous_bare_pairs.len(), 1, "{d:?}");
        assert!(
            d.edges.contains(&("ab".to_string(), "d".to_string())),
            "the stale skip must be re-evaluated after the withdrawal: {d:?}"
        );
        assert!(
            d.skipped_cycle_edges.is_empty(),
            "nothing blocks ab→d once the guess is gone: {d:?}"
        );
    }

    /// Reciprocal BARE reads are two contradicting guesses: name order must
    /// not pick a winner (a wrong pick deterministically reverses the real
    /// producer edge). Neither direction derives; the pair is surfaced.
    #[test]
    fn reciprocal_bare_reads_derive_nothing_and_are_surfaced() {
        let models = [
            m("alpha", "db", "main", "t_alpha", "SELECT x FROM t_beta"),
            m("beta", "db", "main", "t_beta", "SELECT y FROM t_alpha"),
        ];
        let d = derive_physical_edges(&models, &[]);
        assert!(d.edges.is_empty(), "{:?}", d.edges);
        assert!(
            d.skipped_cycle_edges.is_empty(),
            "{:?}",
            d.skipped_cycle_edges
        );
        assert_eq!(d.ambiguous_bare_pairs.len(), 1);
    }

    /// A bare guess contradicted by an EXACT read is not ambiguous — the
    /// exact edge stands (specificity), the bare closer is skipped.
    #[test]
    fn an_exact_edge_survives_a_contradicting_bare_guess() {
        let models = [
            m("alpha", "db", "main", "t_alpha", "SELECT x FROM t_beta"),
            m(
                "beta",
                "db",
                "main",
                "t_beta",
                "SELECT y FROM db.main.t_alpha",
            ),
        ];
        let d = derive_physical_edges(&models, &[]);
        assert_eq!(d.edges, vec![("beta".to_string(), "alpha".to_string())]);
        assert!(d.ambiguous_bare_pairs.is_empty());
        assert_eq!(d.skipped_cycle_edges.len(), 1);
    }

    /// Specificity ordering: when a loose bare-name candidate and an exact
    /// three-part candidate form a mutual pair, the EXACT edge is accepted
    /// and the loose one is the skipped closer — never the reverse.
    #[test]
    fn exact_matches_win_over_loose_ones_in_cycle_resolution() {
        // z bare-reads "a_out" (loose match to model a's table) while a
        // exactly reads z's full target — mutual candidates at different
        // tiers. Sorted purely by name, ("a","z") would be accepted and the
        // loose ("z","a") skipped — which happens to agree; so construct the
        // adversarial order: name order favors the LOOSE edge.
        let models = [
            m("a_reader", "db", "main", "a_out", "SELECT 1 AS x"),
            m(
                "z_exact",
                "db",
                "main",
                "z_out",
                "SELECT x FROM db.main.a_out",
            ),
        ];
        // Manufacture mutuality: a_reader bare-reads z_out.
        let models = [
            m("a_reader", "db", "main", "a_out", "SELECT x FROM z_out"),
            models[1].clone(),
        ];
        let d = derive_physical_edges(&models, &[]);
        // Exact tier processed first: z_exact→a_reader accepted; the loose
        // bare closer a_reader→z_exact is skipped.
        assert_eq!(
            d.edges,
            vec![("z_exact".to_string(), "a_reader".to_string())]
        );
        assert_eq!(
            d.skipped_cycle_edges,
            vec![("a_reader".to_string(), "z_exact".to_string())]
        );
    }
}
