//! Qualify deferred upstream model references in a model's SQL body.
//!
//! Rocky's transformation models reference upstream models by **bare name**
//! (`FROM orders`). That bare name resolves against the warehouse's current
//! schema at execution time — so a development run that builds only a subset
//! of the DAG has no table to read for the upstreams it didn't build.
//!
//! [`qualify_deferred_refs`] rewrites those bare references to fully qualified
//! `schema.table` (or `catalog.schema.table`) names pointing at an existing
//! ("defer target") location — typically the production schema where the
//! unbuilt upstream already lives. This is the engine half of the
//! `rocky run --defer` developer convenience: build your changed models
//! locally, read every unchanged upstream from production.
//!
//! The rewrite is purely additive: only **single-part bare names** that match
//! a supplied deferred-model name are touched. Already-qualified references
//! (`schema.table`, `catalog.schema.table`), names that don't match a deferred
//! model, and CTE names defined inside the statement are all left untouched.

use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;

use sqlparser::ast::{Ident, ObjectName, ObjectNamePart, Query, VisitMut, VisitorMut};

use crate::parser::{ParseError, parse_single_statement};

/// A fully resolved target for a deferred upstream reference.
///
/// Built from the upstream model's configured `target`, mirroring the
/// warehouse dialect's empty-catalog rule: an empty `catalog` yields a
/// two-part `schema.table` reference, a non-empty one yields the three-part
/// `catalog.schema.table` form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeferTarget {
    /// Warehouse catalog. Empty string ⇒ omit the catalog part.
    pub catalog: String,
    /// Schema the deferred upstream table lives in.
    pub schema: String,
    /// Table name of the deferred upstream.
    pub table: String,
    /// Optional identifier quote character to wrap each part in when the
    /// reference is re-serialized.
    ///
    /// `None` renders bare identifiers (`catalog.schema.table`) — the default
    /// for warehouses whose qualified parts always match the strict
    /// SQL-identifier rule (Databricks, Snowflake, DuckDB). Dialects that
    /// permit characters bare identifiers reject — notably BigQuery, whose
    /// project IDs allow hyphens (`my-gcp-project-123`) — set this so the
    /// rewritten reference is quoted exactly as the warehouse expects
    /// (`` `my-gcp-project-123`.`schema`.`table` ``) instead of parsing the
    /// hyphen as subtraction.
    pub quote_style: Option<char>,
}

impl DeferTarget {
    /// Build the qualified `ObjectName` for this target, applying the
    /// empty-catalog rule (`catalog.is_empty()` ⇒ `[schema, table]`, else
    /// `[catalog, schema, table]`) and the configured [`quote_style`].
    ///
    /// [`quote_style`]: DeferTarget::quote_style
    fn to_object_name(&self) -> ObjectName {
        let ident = |value: &str| -> ObjectNamePart {
            let part = match self.quote_style {
                Some(q) => Ident::with_quote(q, value.to_string()),
                None => Ident::new(value.to_string()),
            };
            ObjectNamePart::Identifier(part)
        };
        let mut parts: Vec<ObjectNamePart> = Vec::with_capacity(3);
        if !self.catalog.is_empty() {
            parts.push(ident(&self.catalog));
        }
        parts.push(ident(&self.schema));
        parts.push(ident(&self.table));
        ObjectName(parts)
    }
}

/// Rewrite bare upstream model references in `sql` to their qualified
/// [`DeferTarget`].
///
/// `deferred` maps a deferred model's bare name to the fully qualified
/// location its reference should resolve to. Only single-part relations whose
/// identifier matches a key in `deferred` — and that are not shadowed by a CTE
/// in scope at that point — are rewritten; everything else is left as parsed.
///
/// CTE shadowing is resolved with lexical scope: the rewrite walks the AST
/// maintaining a stack of the CTE names in scope (each query's `WITH` names
/// cover its own body and every CTE body nested under it), so a `WITH orders
/// AS (…)` inside a derived-table / `IN` / `UNION` subquery only shadows the
/// deferred `orders` *within that subquery*, while a genuine top-level `FROM
/// orders` with no shadowing CTE is still qualified.
///
/// `case_rules` governs CTE-alias comparison ONLY. The `deferred` lookup itself
/// stays exact — see the note in `pre_visit_relation`.
///
/// ‼️ There is NO near-miss report on this path and no way to refuse, so the
/// scope answer is ACTED ON rather than checked — and on a dialect that folds
/// unquoted identifiers it rests on an assumption. Under Snowflake's DEFAULT
/// `QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE` the stack matches the warehouse
/// exactly: a quoted lowercase alias does not hide an unquoted reference, so the
/// reference is a table reference, which `--defer` qualifies to the deferred
/// model's target because a bare name matching a model name IS that model
/// (`resolve::classify_table_ref`). Pinned by
/// `defer_cte_alias_comparison_follows_dialect_identity`.
///
/// Under `QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE` that parameter folds a
/// DOUBLE-QUOTED identifier to upper case too, so the CTE does bind and this
/// qualification is wrong — silently, because nothing here can refuse. Clearing
/// the axis does NOT close that: the same pair is then mis-bound under the
/// DEFAULT instead, which is what the rules before #1282 did. The change moves
/// the error from the default configuration to an opt-out one; it does not
/// create it. Closing it properly needs a three-valued binding — bound, free, or
/// setting-dependent, refusing the third — or #1281's live probe. Tracked in
/// #1622; deliberately not built here on an unverified reading of how that
/// parameter treats a CTE alias.
///
/// Unlike [`rewrite_upstream_refs`] this reports no case near-misses, and that
/// asymmetry is deliberate rather than an omission. That matcher compares a
/// *spelled qualified reference* against a *configured warehouse target* — two
/// independently authored spellings that can genuinely disagree about case, so
/// a near-match is ambiguous and has to be refused. Here both sides come from
/// one source of truth: a bare identifier is matched against a Rocky model name,
/// exactly as `resolve::classify_table_ref` matches it, and the replacement is
/// rendered by `rewrite_quote_style`, which tracks the same `format_table_ref`
/// the producing model's DDL used. There is no second spelling to mismatch.
///
/// Re-serialization preserves the statement's semantics, but the AST round
/// trip may strip comments and trailing semicolons and normalize incidental
/// whitespace — callers only invoke this in `--defer` mode, never on the
/// default path.
///
/// # Errors
///
/// Returns [`ParseError`] when `sql` is not a single parseable statement.
pub fn qualify_deferred_refs(
    sql: &str,
    deferred: &HashMap<String, DeferTarget>,
    case_rules: IdentifierCaseRules,
    recursive_visibility: RecursiveCteVisibility,
) -> Result<String, ParseError> {
    if deferred.is_empty() {
        return Ok(sql.to_string());
    }

    let mut statement = parse_single_statement(sql)?;

    // Walk the whole AST with a scope-aware visitor. `visit_relations_mut`
    // reaches every relation (including those inside derived-table / scalar /
    // `IN` subqueries and set-op branches), but it is scope-blind — so the
    // visitor tracks the CTE names in scope on a stack and only rewrites a
    // bare reference that is *not* shadowed at that point. The visitor never
    // breaks, so the `ControlFlow` is always `Continue`.
    let mut rewriter = DeferRewriter {
        deferred,
        scopes: CteScopeStack::new(case_rules, recursive_visibility),
    };
    let _: ControlFlow<()> = statement.visit(&mut rewriter);

    Ok(statement.to_string())
}

/// Whether each component of a qualified name carries case as part of object
/// identity on the dialect a rewrite is being performed for.
///
/// [`rewrite_upstream_refs`] compares each component of a reference against each
/// rename key under the rule for that component: exactly where case is part of
/// identity, case-insensitively where it is not.
///
/// Comparing case-insensitively everywhere — as this matcher used to — is not
/// merely imprecise, it redirects reads to the wrong table. On a case-sensitive
/// dialect `raw.Orders` and `raw.orders` are two different objects, so a model
/// reading the first must not be rewritten to the replacement registered for the
/// second. Conversely, comparing exactly on a dialect that folds would strand
/// references that legitimately name the renamed object.
///
/// Per component rather than one boolean because a warehouse may apply different
/// rules to the catalog than to the schema and table. **No dialect Rocky
/// supports exercises that today** — duckdb, databricks and trino are uniformly
/// case-insensitive, bigquery and snowflake uniformly case-sensitive. The shape
/// is kept because the one live divergence is per-object, not per-dialect:
/// BigQuery datasets carry an `is_case_insensitive` flag that Rocky cannot
/// observe and must assume unset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdentifierCaseRules {
    /// Case distinguishes two catalogs.
    pub catalog: bool,
    /// Case distinguishes two schemas.
    pub schema: bool,
    /// Case distinguishes two tables.
    pub table: bool,
    /// The dialect resolves an UNQUOTED identifier by upper-casing it, so its
    /// written spelling is not its identity.
    ///
    /// Quoting is a second identity axis, independent of case. Snowflake stores
    /// an unquoted `orders` as `ORDERS` while a quoted `"orders"` stays exactly
    /// as written — and Rocky renders Snowflake targets QUOTED, so a configured
    /// target `orders` is the object `orders`, which an unquoted `FROM orders`
    /// does NOT name. Ignoring this makes two texts that look identical refer to
    /// different tables.
    ///
    /// Only meaningful where `catalog`/`schema`/`table` are case-sensitive; a
    /// dialect that folds every identifier already treats the two forms alike.
    pub unquoted_uppercases: bool,
}

impl IdentifierCaseRules {
    /// Every component case-insensitive: two spellings differing only by case
    /// name one object. Callers whose rename keys are already case-folded want
    /// this — disambiguation can never separate keys that were folded together.
    #[must_use]
    pub const fn all_insensitive() -> Self {
        Self {
            catalog: false,
            schema: false,
            table: false,
            unquoted_uppercases: false,
        }
    }

    /// The same rule for every component, which is what all five in-repo
    /// dialects need.
    #[must_use]
    pub const fn uniform(case_sensitive: bool) -> Self {
        Self {
            catalog: case_sensitive,
            schema: case_sensitive,
            table: case_sensitive,
            unquoted_uppercases: false,
        }
    }

    /// [`uniform`] plus the unquoted-uppercasing rule — Snowflake's shape.
    ///
    /// [`uniform`]: IdentifierCaseRules::uniform
    #[must_use]
    pub const fn uniform_uppercasing(case_sensitive: bool) -> Self {
        Self {
            catalog: case_sensitive,
            schema: case_sensitive,
            table: case_sensitive,
            unquoted_uppercases: true,
        }
    }

    /// The rule for the component at `index` of a `catalog.schema.table` key.
    const fn at(self, index: usize) -> bool {
        match index {
            0 => self.catalog,
            1 => self.schema,
            _ => self.table,
        }
    }
}

/// The identity of a warehouse object, for answering **"is this reference
/// definitely THIS object?"**
///
/// Folded per component per dialect: verbatim where case is part of identity,
/// lowercased where it is not. Answering "same" when unsure would redirect a
/// read to a table the model never named, so this demands proof.
///
/// Deliberately a distinct type from [`CollisionIdentity`], which answers the
/// opposite question and fails closed in the opposite direction. The two were
/// both `String`, built fourteen lines apart and consumed in the same loop, and
/// every defect in this area lived in or beside that pair. Keeping them apart is
/// now the compiler's job rather than the reader's.
///
/// Holds the three parts rather than a joined string: a component containing a
/// dot used to be re-split wrongly on the way into the matcher.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TargetIdentity([String; 3]);

impl TargetIdentity {
    /// Build from a target's parts under this dialect's rules.
    #[must_use]
    pub fn of(catalog: &str, schema: &str, table: &str, rules: IdentifierCaseRules) -> Self {
        let fold = |value: &str, sensitive: bool| {
            if sensitive {
                value.to_string()
            } else {
                value.to_ascii_lowercase()
            }
        };
        Self([
            fold(catalog, rules.catalog),
            fold(schema, rules.schema),
            fold(table, rules.table),
        ])
    }

    /// The `[catalog, schema, table]` parts, already folded.
    #[must_use]
    pub fn parts(&self) -> &[String; 3] {
        &self.0
    }
}

impl std::fmt::Display for TargetIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.0[0], self.0[1], self.0[2])
    }
}

/// The identity of a warehouse object, for answering **"could these two be the
/// SAME object?"**
///
/// **Always** folds, on every dialect, and takes no [`IdentifierCaseRules`] *by
/// construction* so it cannot accidentally be made case-aware. Whether case
/// separates two objects is connection state Rocky cannot observe — a Snowflake
/// account may set `QUOTED_IDENTIFIERS_IGNORE_CASE`, a BigQuery dataset may be
/// `is_case_insensitive` — and answering "different" when they are in fact one
/// object lets two models write the same table with no error at all. That is
/// unrecoverable, while the remedy for an over-strict answer (rename a target)
/// is trivial. This asymmetry is permanent; do not "unify" it with
/// [`TargetIdentity`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CollisionIdentity(String);

impl CollisionIdentity {
    /// Build from a target's parts. No rules parameter, deliberately.
    ///
    /// Folds with Unicode [`str::to_lowercase`], not `to_ascii_lowercase`.
    /// An ASCII-only fold answers "different" for `TÄBLE` and `täble`, which
    /// a case-insensitive warehouse resolves to one object — precisely the
    /// unrecoverable direction this type exists to avoid.
    ///
    /// Widening is safe here in the sense that matters — it is monotone. It
    /// can only make more things equal, never fewer, so no collision detected
    /// before goes undetected now and no consumer (they all refuse on a match)
    /// is un-gated. `unicode_fold_never_splits_an_ascii_equality` proves the
    /// monotonicity over every Unicode scalar rather than asserting it.
    ///
    /// It is *not* merely additive at the output, and three distinct effects
    /// are worth naming because "only adds refusals" glosses all of them:
    ///
    /// 1. **New groups.** Two singleton targets `TÄBLE` and `täble` produced
    ///    no E036 at all before; they now merge into a group and both models
    ///    are newly reported. This is the point of the change, not a side
    ///    effect — but it is a project that used to compile and no longer
    ///    does.
    /// 2. **Absorbed singletons.** A singleton can join an existing group,
    ///    enlarging a diagnostic that was already firing.
    /// 3. **Reordering without merging.** `rocky-compiler` groups into a
    ///    `BTreeMap` keyed on the folded string and emits in that order, so a
    ///    fold that changes a key changes group order even when nothing
    ///    merges: `İ` sorts after `z` folded to ASCII, and before it folded to
    ///    `i` + U+0307.
    ///
    /// No participant is ever lost — `compile` emits one hard error per model
    /// in a group, merged or not.
    #[must_use]
    pub fn of(catalog: &str, schema: &str, table: &str) -> Self {
        Self(format!("{catalog}.{schema}.{table}").to_lowercase())
    }

    /// Build from an already-qualified `catalog.schema.table` string.
    ///
    /// For callers holding a target joined upstream — a persisted plan step,
    /// where the components were flattened at plan time and cannot be
    /// recovered. Equivalent to [`Self::of`] on the same components, so the
    /// fold stays defined in exactly one place.
    #[must_use]
    pub fn of_qualified(qualified: &str) -> Self {
        Self(qualified.to_lowercase())
    }
}

impl std::fmt::Display for CollisionIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Whether `spelled` matches the tail of `key_exact`, comparing each component
/// case-sensitively only where `rules` says case is part of identity.
///
/// Alignment is by tail, so a two-part reference is compared against the key's
/// schema and table — and therefore against the *schema* and *table* rules, not
/// the catalog's.
fn tail_matches_with_case(
    key_exact: &[String],
    spelled: &[&str],
    rules: IdentifierCaseRules,
) -> bool {
    if key_exact.len() < spelled.len() {
        return false;
    }
    let offset = key_exact.len() - spelled.len();
    spelled.iter().enumerate().all(|(i, part)| {
        let key_part = &key_exact[offset + i];
        if rules.at(offset + i) {
            key_part.as_str() == *part
        } else {
            key_part.eq_ignore_ascii_case(part)
        }
    })
}

/// Outcome of [`rewrite_upstream_refs`].
#[derive(Debug)]
pub struct UpstreamRewriteOutcome {
    /// The statement re-serialized after rewriting.
    pub sql: String,
    /// The rename keys that matched at least one relation and were rewritten,
    /// returned **verbatim** — exactly the `String` the caller supplied in
    /// `renames`, never re-derived or folded. (It used to say "lowercase",
    /// which stopped being true once keys became case-preserving.)
    ///
    /// That is a contract two callers depend on: shadow execution looks these
    /// up in an `owner_by_key` map built from the same key space to attribute
    /// derived DAG edges, and replay compares them against its own key set to
    /// require that *every* upstream was redirected. A key normalised on the
    /// way out would miss both lookups silently.
    pub rewritten_keys: HashSet<TargetIdentity>,
    /// Display strings of relations that matched **more than one** rename
    /// key (a partially-qualified reference whose tail is shared by several
    /// upstreams). Ambiguous references are left untouched — callers must
    /// fail closed rather than guess.
    pub ambiguous_refs: Vec<String>,
    /// Display strings of relations that match NO key under the supplied
    /// [`IdentifierCaseRules`] but WOULD match one if case were ignored.
    ///
    /// Whether such a reference names the routed upstream depends on connection
    /// state (`QUOTED_IDENTIFIERS_IGNORE_CASE`, a dataset's
    /// `is_case_insensitive`) that Rocky cannot observe, and both guesses are
    /// unsafe — rewriting reads the wrong table, not rewriting reads production.
    /// Callers that require isolation must fail closed on any. Always empty when
    /// `case_rules` is [`IdentifierCaseRules::all_insensitive`].
    pub case_fold_only_refs: Vec<String>,
}

/// Rewrite references to known upstream tables — bare, `schema.table`, or
/// `catalog.schema.table` — to a replacement [`DeferTarget`].
///
/// `renames` is keyed by the upstream's fully-qualified `catalog.schema.table`
/// identity, spelled as the dialect stores it — callers on a case-sensitive
/// dialect must NOT pre-fold their keys, or two distinct objects collapse into
/// one entry. A relation matches a key when every part it *does* spell agrees
/// with the key's tail under `case_rules`: a three-part
/// reference must match catalog, schema, and table; a two-part reference
/// schema and table; a bare name just the table (subject to CTE shadowing,
/// exactly as [`qualify_deferred_refs`] resolves it). A relation whose tail
/// matches several keys is ambiguous: it is left as parsed and reported in
/// [`UpstreamRewriteOutcome::ambiguous_refs`] so the caller can refuse the
/// rewrite instead of redirecting a reference to the wrong upstream.
///
/// # Errors
///
/// Returns [`ParseError`] when `sql` is not a single parseable statement.
pub fn rewrite_upstream_refs(
    sql: &str,
    renames: &HashMap<TargetIdentity, DeferTarget>,
    case_rules: IdentifierCaseRules,
    recursive_visibility: RecursiveCteVisibility,
) -> Result<UpstreamRewriteOutcome, ParseError> {
    let mut statement = parse_single_statement(sql)?;

    // The parts come straight off the key — no re-splitting on `.`, which used
    // to mis-handle a component containing one.
    let split: Vec<(&[String; 3], &TargetIdentity, &DeferTarget)> = renames
        .iter()
        .map(|(key, target)| (key.parts(), key, target))
        .collect();

    let mut rewriter = UpstreamRewriter {
        renames: &split,
        case_rules,
        scopes: CteScopeStack::new(case_rules, recursive_visibility),
        rewritten_keys: HashSet::new(),
        ambiguous_refs: Vec::new(),
        case_fold_only_refs: Vec::new(),
    };
    let _: ControlFlow<()> = statement.visit(&mut rewriter);

    let UpstreamRewriter {
        rewritten_keys,
        ambiguous_refs,
        case_fold_only_refs,
        ..
    } = rewriter;
    Ok(UpstreamRewriteOutcome {
        sql: statement.to_string(),
        rewritten_keys,
        ambiguous_refs,
        case_fold_only_refs,
    })
}

/// Scope-aware visitor behind [`rewrite_upstream_refs`]. Shares the CTE
/// shadowing discipline with [`DeferRewriter`]: a bare name shadowed by a CTE
/// in scope is a reference to that CTE, never to the upstream.
struct UpstreamRewriter<'a> {
    /// `(key parts, original key, replacement)` triples, key parts as spelled.
    renames: &'a [(&'a [String; 3], &'a TargetIdentity, &'a DeferTarget)],
    /// Per-component case semantics for this dialect.
    case_rules: IdentifierCaseRules,
    scopes: CteScopeStack,
    rewritten_keys: HashSet<TargetIdentity>,
    ambiguous_refs: Vec<String>,
    case_fold_only_refs: Vec<String>,
}

/// Whether a dialect lets a CTE inside a `WITH RECURSIVE` clause reference an
/// alias declared AFTER it.
///
/// A separate axis from [`IdentifierCaseRules`] — case rules say which spellings
/// name one object, this says which names are in scope at all — and the two
/// dialects Rocky has grounded disagree, so it cannot be inferred.
///
/// **The default is the conservative one, and the direction matters.** Guessing
/// [`Forward`](Self::Forward) where the dialect does not allow it leaves a
/// reference that actually binds to a physical table unrewritten, so a shadow
/// run reads PRODUCTION and reports success. Guessing
/// [`PrecedingAndSelf`](Self::PrecedingAndSelf) where the dialect does allow it
/// rewrites a reference that binds to a CTE, so the query reads a shadow table
/// instead of its own CTE — wrong, but isolated. The second failure is
/// recoverable and the first is not, so unverified dialects get the second.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecursiveCteVisibility {
    /// A CTE body sees the aliases before it, plus its own. Verified against
    /// DuckDB: in `WITH RECURSIVE first AS (SELECT * FROM orders), orders AS
    /// (SELECT 999 AS id) SELECT * FROM first`, `first` returns the PHYSICAL
    /// orders row, not 999.
    PrecedingAndSelf,
    /// A CTE body additionally sees aliases declared after it. BigQuery: "A
    /// recursive CTE can reference itself, a preceding CTE, or a subsequent
    /// CTE." — docs.cloud.google.com/bigquery/docs/recursive-ctes
    Forward,
}

/// Which part of a query's `WITH` structure the walk is currently inside.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Region {
    /// The query's own body, or a subquery of it: every alias is visible.
    Body,
    /// The body of the CTE at this index: only the aliases before it are.
    Cte(usize),
}

/// One query's CTE aliases, plus where in that query the walk currently is.
///
/// A non-recursive CTE is visible to the CTEs declared AFTER it and to the
/// query body — never to the ones before it.
///
/// `body_addr` maps the heap address of each CTE's body `Query` to that CTE's
/// index. `Cte::query` is a `Box<Query>`, so the address is stable for the
/// whole walk and the visitor is handed that same allocation when it descends.
/// Keying on identity rather than on arrival order is deliberate: the previous
/// design counted completed child queries and so depended on `with` being
/// visited before `body` and on CTE bodies arriving in declaration order.
/// Neither is guaranteed by `sqlparser`'s `VisitorMut` contract, which promises
/// only "before/after visiting children". Nothing here assumes either.
///
/// The one residual assumption: a CTE body is visited AS a direct child
/// `Query`. If a future `sqlparser` flattens that or introduces its own CTE
/// hook, the address lookup finds nothing, `region` stays [`Region::Body`],
/// every alias is visible everywhere, and an upstream reference inside an
/// earlier CTE body is left unrewritten — reading production while the model
/// writes its shadow. `cte_visibility_is_positional` case 1 is the detector;
/// `sqlparser_visits_with_before_body` is the canary that names the cause.
struct CteScope {
    /// Lookup-form aliases in declaration order.
    aliases: Vec<String>,
    /// Address of each CTE body `Query` → its index in `aliases`.
    body_addr: HashMap<usize, usize>,
    /// `WITH RECURSIVE`: a CTE body additionally sees its OWN alias, so it can
    /// reference itself. Nothing more — later aliases stay invisible to earlier
    /// bodies exactly as in the non-recursive case.
    recursive: bool,
    region: Region,
}

impl CteScope {
    /// The aliases visible at the walk's current position in this query.
    ///
    /// `RECURSIVE` adds exactly one alias to a CTE body's scope — its OWN, so it
    /// can reference itself. It does NOT make later aliases visible to earlier
    /// bodies, and treating it as if it did is a production read: in
    /// `WITH RECURSIVE first AS (SELECT * FROM orders), orders AS (…)`, `first`
    /// reads the physical `orders` (verified against DuckDB), so a rewriter that
    /// thinks the later CTE shadows it leaves the reference pointing at
    /// production while the model writes its shadow target.
    fn visible(&self, policy: RecursiveCteVisibility) -> &[String] {
        match self.region {
            Region::Body => &self.aliases,
            Region::Cte(_) if self.recursive && policy == RecursiveCteVisibility::Forward => {
                &self.aliases
            }
            Region::Cte(i) if self.recursive => &self.aliases[..=i],
            Region::Cte(i) => &self.aliases[..i],
        }
    }
}

/// The CTE scope stack shared by both rewriters in this module.
///
/// Both need the identical alias rule, so this is one struct rather than a
/// trait. Owning it in both visitors is what keeps them from drifting apart:
/// every alias that enters a frame goes through [`Self::lookup_form`], and the
/// reference tested against it goes through the same function, so the frame and
/// the test cannot disagree about case or quoting. They did — frames stored the
/// alias verbatim and compared exactly while relation matching folded, so on a
/// case-insensitive dialect `WITH Orders … FROM orders` was not treated as
/// shadowed (those are ONE CTE there) and the reference was rewritten to a
/// table the author never named.
struct CteScopeStack {
    frames: Vec<CteScope>,
    case_rules: IdentifierCaseRules,
    recursive_visibility: RecursiveCteVisibility,
}

impl CteScopeStack {
    fn new(case_rules: IdentifierCaseRules, recursive_visibility: RecursiveCteVisibility) -> Self {
        Self {
            frames: Vec::new(),
            case_rules,
            recursive_visibility,
        }
    }

    /// The lookup form of an identifier: resolved the way the warehouse
    /// resolves it, then folded unless case is part of identity.
    ///
    /// CTE names live in the table namespace, so the `table` rule governs.
    fn lookup_form(&self, value: &str, quoted: bool) -> String {
        let resolved = if self.case_rules.unquoted_uppercases && !quoted {
            value.to_uppercase()
        } else {
            value.to_string()
        };
        if self.case_rules.table {
            resolved
        } else {
            resolved.to_ascii_lowercase()
        }
    }

    fn addr_of(query: &Query) -> usize {
        std::ptr::from_ref(query).addr()
    }

    /// Call from `pre_visit_query`. Positions the PARENT frame — this query is
    /// either one of its CTE bodies or part of its body — then pushes this
    /// query's own frame.
    fn enter_query(&mut self, query: &Query) {
        let addr = Self::addr_of(query);
        if let Some(parent) = self.frames.last_mut() {
            parent.region = parent
                .body_addr
                .get(&addr)
                .copied()
                .map_or(Region::Body, Region::Cte);
        }
        let mut frame = CteScope {
            aliases: Vec::new(),
            body_addr: HashMap::new(),
            recursive: query.with.as_ref().is_some_and(|w| w.recursive),
            region: Region::Body,
        };
        if let Some(with) = &query.with {
            for (index, cte) in with.cte_tables.iter().enumerate() {
                let alias = &cte.alias.name;
                frame
                    .aliases
                    .push(self.lookup_form(&alias.value, alias.quote_style.is_some()));
                frame.body_addr.insert(Self::addr_of(&cte.query), index);
            }
        }
        self.frames.push(frame);
    }

    /// Call from `post_visit_query`. Leaving a child returns the parent to its
    /// own body, where every alias is visible again.
    fn exit_query(&mut self) {
        self.frames.pop();
        if let Some(parent) = self.frames.last_mut() {
            parent.region = Region::Body;
        }
    }

    /// Whether `value` names a CTE in scope at this point of the walk.
    fn is_shadowed(&self, value: &str, quoted: bool) -> bool {
        let name = self.lookup_form(value, quoted);
        self.frames
            .iter()
            .any(|frame| frame.visible(self.recursive_visibility).contains(&name))
    }
}

impl VisitorMut for UpstreamRewriter<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        self.scopes.enter_query(query);
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &mut Query) -> ControlFlow<Self::Break> {
        self.scopes.exit_query();
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, relation: &mut ObjectName) -> ControlFlow<Self::Break> {
        // Extract the spelled parts — bail on anything that is not a plain
        // identifier chain (e.g. a table function).
        // `(value, was_quoted)`. Quotedness is part of identity on dialects that
        // fold unquoted identifiers, so it must survive to the comparison.
        let mut spelled_parts: Vec<(&str, bool)> = Vec::with_capacity(relation.0.len());
        for part in &relation.0 {
            let Some(ident) = part.as_ident() else {
                return ControlFlow::Continue(());
            };
            spelled_parts.push((ident.value.as_str(), ident.quote_style.is_some()));
        }
        if spelled_parts.is_empty() || spelled_parts.len() > 3 {
            return ControlFlow::Continue(());
        }
        // A bare name shadowed by a CTE in scope refers to the CTE.
        if spelled_parts.len() == 1
            && self
                .scopes
                .is_shadowed(spelled_parts[0].0, spelled_parts[0].1)
        {
            return ControlFlow::Continue(());
        }
        // Resolve each part to the name the warehouse will actually look up.
        let resolved: Vec<String> = spelled_parts
            .iter()
            .map(|(value, quoted)| {
                if self.case_rules.unquoted_uppercases && !quoted {
                    value.to_uppercase()
                } else {
                    (*value).to_string()
                }
            })
            .collect();
        let original: Vec<&str> = resolved.iter().map(String::as_str).collect();
        // Tail-match each rename key, comparing every component under the
        // dialect's own rule for that component. Where case is part of identity
        // this is an exact comparison, because `raw.Orders` and `raw.orders` are
        // then two DIFFERENT objects and redirecting a read of one to the
        // other's replacement would silently read the wrong table. Where it is
        // not, the comparison folds, exactly as it always has.
        let candidates: Vec<&(&[String; 3], &TargetIdentity, &DeferTarget)> = self
            .renames
            .iter()
            .filter(|(key_exact, _, _)| {
                tail_matches_with_case(*key_exact, &original, self.case_rules)
            })
            .collect();
        match candidates.as_slice() {
            [] => {
                // No key matches under the dialect's declared rules — but if one
                // matches when case is IGNORED, the right answer depends on
                // state this process cannot see.
                //
                // Neither guess is safe here, which is why this is reported
                // rather than decided. Rewriting a reference that names a
                // genuinely different object reads the wrong table; NOT
                // rewriting one that does name the routed upstream reads
                // PRODUCTION while the model writes its shadow — an isolation
                // break that reports success and then shows up as a clean
                // `rocky compare`. Case-sensitivity is connection state (a
                // Snowflake account may set QUOTED_IDENTIFIERS_IGNORE_CASE; a
                // BigQuery dataset may be `is_case_insensitive`), so the caller
                // is handed the near-miss and fails closed.
                //
                // Inert wherever `case_rules` is already all-insensitive: folded
                // and rule-based matching coincide, so this can never fire.
                if self.renames.iter().any(|(key_exact, _, _)| {
                    tail_matches_with_case(
                        *key_exact,
                        &original,
                        IdentifierCaseRules::all_insensitive(),
                    )
                }) {
                    self.case_fold_only_refs.push(relation.to_string());
                }
            }
            [(_, key, target)] => {
                *relation = target.to_object_name();
                self.rewritten_keys.insert((*key).clone());
            }
            // Several upstreams are indistinguishable from this reference under
            // the dialect's own rules, so it genuinely names more than one of
            // them.
            _ => self.ambiguous_refs.push(relation.to_string()),
        }
        ControlFlow::Continue(())
    }
}

/// Scope-aware visitor that qualifies bare deferred-model references while
/// honouring lexical CTE shadowing.
///
/// Shares [`CteScopeStack`] with [`UpstreamRewriter`], so CTE visibility is
/// positional and alias comparison follows the dialect's own case and quoting
/// rules. Before that was shared, this visitor installed every alias up front
/// and compared them verbatim, which cost it two defects at once: a later CTE
/// shadowed a reference in an earlier CTE's body (leaving that upstream
/// unqualified, to resolve against whatever the working schema happens to
/// hold), and on a folding dialect `WITH Orders … FROM orders` was not treated
/// as shadowed at all, so the reference was qualified to the production table
/// instead of reading the CTE.
struct DeferRewriter<'a> {
    deferred: &'a HashMap<String, DeferTarget>,
    scopes: CteScopeStack,
}

impl VisitorMut for DeferRewriter<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        self.scopes.enter_query(query);
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &mut Query) -> ControlFlow<Self::Break> {
        self.scopes.exit_query();
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, relation: &mut ObjectName) -> ControlFlow<Self::Break> {
        // Only single-part bare names are candidates; anything already
        // qualified (`schema.table`, `catalog.schema.table`) is left alone.
        if relation.0.len() != 1 {
            return ControlFlow::Continue(());
        }
        let Some(ident) = relation.0[0].as_ident() else {
            return ControlFlow::Continue(());
        };
        if self
            .scopes
            .is_shadowed(&ident.value, ident.quote_style.is_some())
        {
            return ControlFlow::Continue(());
        }
        // ‼️ EXACT, and deliberately not routed through `case_rules`. These keys
        // are Rocky MODEL names, not warehouse identifiers, and
        // `resolve::classify_table_ref` resolves a bare name to a model with an
        // exact `HashSet::contains`. Folding here would qualify a reference the
        // compiler gave no dependency edge to, so `--defer` would disagree with
        // the DAG about what a name means. The case axis reaches the SCOPE STACK
        // above and nothing else.
        if let Some(target) = self.deferred.get(&ident.value) {
            *relation = target.to_object_name();
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    /// Pins the equivalence a probe-driven `case_rules` will depend on (#1282).
    ///
    /// Nothing routes through the probe today — #1281 shipped the capability
    /// only. But the moment a consumer maps an `Insignificant` observation to
    /// `all_insensitive()`, DuckDB, Databricks and Trino change constructor:
    /// `dialect_case_rules` hands them `uniform(false)` today. "No behaviour
    /// change for the three folding dialects" is then a claim about these two
    /// values being equal, which is a test rather than a PR-body assertion.
    ///
    /// If this ever fails, the consumer is the thing to fix — not this test.
    #[test]
    fn uniform_false_and_all_insensitive_are_the_same_rules() {
        assert_eq!(
            super::IdentifierCaseRules::uniform(false),
            super::IdentifierCaseRules::all_insensitive(),
            "a folding dialect's rules must not change depending on which \
             constructor produced them"
        );
    }

    use super::*;

    fn target(catalog: &str, schema: &str, table: &str) -> DeferTarget {
        DeferTarget {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            quote_style: None,
        }
    }

    fn quoted_target(catalog: &str, schema: &str, table: &str, quote: char) -> DeferTarget {
        DeferTarget {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            quote_style: Some(quote),
        }
    }

    fn deferred_map(entries: &[(&str, DeferTarget)]) -> HashMap<String, DeferTarget> {
        entries
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn qualifies_a_bare_upstream_ref() {
        let deferred = deferred_map(&[("orders", target("", "prod", "orders"))]);
        let out = qualify_deferred_refs(
            "SELECT * FROM orders",
            &deferred,
            IdentifierCaseRules::all_insensitive(),
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert_eq!(out, "SELECT * FROM prod.orders");
    }

    #[test]
    fn qualifies_with_catalog_when_present() {
        let deferred = deferred_map(&[("orders", target("warehouse", "prod", "orders"))]);
        let out = qualify_deferred_refs(
            "SELECT * FROM orders",
            &deferred,
            IdentifierCaseRules::all_insensitive(),
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert_eq!(out, "SELECT * FROM warehouse.prod.orders");
    }

    #[test]
    fn leaves_aliased_ref_table_qualified_alias_intact() {
        // `FROM orders o` — the relation is `orders`; the alias `o` and any
        // `o.col` column refs are never in relation position, so only the
        // table name is rewritten.
        let deferred = deferred_map(&[("orders", target("", "prod", "orders"))]);
        let out = qualify_deferred_refs(
            "SELECT o.id FROM orders o WHERE o.id > 0",
            &deferred,
            IdentifierCaseRules::all_insensitive(),
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        // The table name is qualified; the alias `o` and the `o.col` column
        // refs are untouched (they were never in relation position).
        assert!(out.contains("prod.orders"), "got: {out}");
        assert!(
            out.contains("o.id"),
            "alias-qualified columns intact: {out}"
        );
    }

    #[test]
    fn rewrites_ref_inside_join() {
        let deferred = deferred_map(&[
            ("orders", target("", "prod", "orders")),
            ("customers", target("", "prod", "customers")),
        ]);
        let out = qualify_deferred_refs(
            "SELECT o.id, c.name FROM orders o JOIN customers c ON o.cid = c.id",
            &deferred,
            IdentifierCaseRules::all_insensitive(),
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert!(out.contains("FROM prod.orders"), "got: {out}");
        assert!(out.contains("JOIN prod.customers"), "got: {out}");
    }

    #[test]
    fn rewrites_ref_inside_subquery() {
        // extract_lineage only walks top-level FROM; the AST visitor reaches
        // subquery relations too. This is the case that motivates visiting
        // the whole AST.
        let deferred = deferred_map(&[("orders", target("", "prod", "orders"))]);
        let out = qualify_deferred_refs(
            "SELECT * FROM (SELECT id FROM orders) sub",
            &deferred,
            IdentifierCaseRules::all_insensitive(),
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert!(
            out.contains("prod.orders"),
            "subquery ref must be qualified: {out}"
        );
    }

    #[test]
    fn does_not_rewrite_cte_that_shadows_a_model_name() {
        // `orders` is both a deferred model AND a CTE here. The CTE wins — the
        // bare `FROM orders` after the WITH must read the CTE, not the
        // deferred production table.
        let deferred = deferred_map(&[("orders", target("", "prod", "orders"))]);
        let out = qualify_deferred_refs(
            "WITH orders AS (SELECT 1 AS id) SELECT * FROM orders",
            &deferred,
            IdentifierCaseRules::all_insensitive(),
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert!(
            !out.contains("prod.orders"),
            "CTE shadowing a model name must not be qualified: {out}"
        );
    }

    #[test]
    fn does_not_rewrite_cte_shadow_inside_derived_table_subquery() {
        // Case A: the shadowing `WITH orders` lives inside a derived-table
        // subquery (`FROM (... ) sub`). Its inner `FROM orders` must read the
        // CTE, not the deferred production table — the scope-aware walk must
        // not flatten the nested CTE name into a global shadow set either, but
        // here we only assert the inner ref stays unqualified.
        let deferred = deferred_map(&[("orders", target("", "prod", "orders"))]);
        let out = qualify_deferred_refs(
            "SELECT * FROM (WITH orders AS (SELECT 1 AS id) SELECT * FROM orders) sub",
            &deferred,
            IdentifierCaseRules::all_insensitive(),
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert!(
            !out.contains("prod.orders"),
            "CTE inside a derived-table subquery must shadow the deferred ref: {out}"
        );
    }

    #[test]
    fn does_not_rewrite_cte_shadow_inside_in_subquery() {
        // Case B: the shadowing `WITH orders` lives inside an `IN (...)`
        // scalar subquery — an `Expr`, not a `TableFactor`. The whole-AST
        // visitor still reaches it, and the scope stack keeps the inner
        // `FROM orders` reading the CTE.
        let deferred = deferred_map(&[("orders", target("", "prod", "orders"))]);
        let out = qualify_deferred_refs(
            "SELECT id FROM events WHERE id IN \
             (WITH orders AS (SELECT 1 AS id) SELECT id FROM orders)",
            &deferred,
            IdentifierCaseRules::all_insensitive(),
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert!(
            !out.contains("prod.orders"),
            "CTE inside an IN-subquery must shadow the deferred ref: {out}"
        );
    }

    #[test]
    fn does_not_rewrite_cte_shadow_inside_union_branch() {
        // Case C: the shadowing `WITH orders` lives inside a set-op (UNION)
        // branch. The visitor descends into both branches; the inner branch's
        // CTE shadows its own `FROM orders`.
        let deferred = deferred_map(&[("orders", target("", "prod", "orders"))]);
        let out = qualify_deferred_refs(
            "SELECT 1 AS id UNION ALL \
             (WITH orders AS (SELECT 2 AS id) SELECT id FROM orders)",
            &deferred,
            IdentifierCaseRules::all_insensitive(),
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert!(
            !out.contains("prod.orders"),
            "CTE inside a UNION branch must shadow the deferred ref: {out}"
        );
    }

    #[test]
    fn qualifies_top_level_ref_with_no_shadowing_cte() {
        // Case D (positive control): a genuine top-level `FROM orders` with no
        // shadowing CTE anywhere is still qualified.
        let deferred = deferred_map(&[("orders", target("", "prod", "orders"))]);
        let out = qualify_deferred_refs(
            "SELECT * FROM orders",
            &deferred,
            IdentifierCaseRules::all_insensitive(),
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert!(
            out.contains("prod.orders"),
            "top-level deferred ref with no shadow must be qualified: {out}"
        );
    }

    #[test]
    fn qualifies_outer_ref_but_not_nested_cte_shadow() {
        // Case E (the discriminating case): a flat global shadow set would
        // wrongly skip the top-level `FROM orders`, and a scope-blind walk
        // would wrongly qualify the inner one. Only a scope-aware stack gets
        // both: the top-level ref IS qualified, the nested CTE-shadowed ref is
        // NOT.
        let deferred = deferred_map(&[("orders", target("", "prod", "orders"))]);
        let out = qualify_deferred_refs(
            "SELECT * FROM orders WHERE id IN \
             (WITH orders AS (SELECT 1 AS id) SELECT id FROM orders)",
            &deferred,
            IdentifierCaseRules::all_insensitive(),
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        // Exactly one occurrence of the qualified production ref (the outer
        // FROM); the inner CTE ref stays bare.
        assert_eq!(
            out.matches("prod.orders").count(),
            1,
            "outer ref qualified, nested CTE-shadowed ref left bare: {out}"
        );
        assert!(
            out.contains("FROM prod.orders WHERE"),
            "the outer FROM must be the qualified one: {out}"
        );
    }

    #[test]
    fn quotes_hyphenated_catalog_for_bigquery_style_target() {
        // A BigQuery-style defer target: the project (catalog) contains
        // hyphens, so the rewritten reference must be backtick-quoted to match
        // what the BigQuery dialect emits — bare hyphens would parse as
        // subtraction and the warehouse would reject the SQL.
        let deferred = deferred_map(&[(
            "orders",
            quoted_target("my-gcp-project-123", "prod", "orders", '`'),
        )]);
        let out = qualify_deferred_refs(
            "SELECT * FROM orders",
            &deferred,
            IdentifierCaseRules::all_insensitive(),
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert_eq!(out, "SELECT * FROM `my-gcp-project-123`.`prod`.`orders`");
    }

    #[test]
    fn leaves_already_qualified_ref_untouched() {
        // A two-part `staging.orders` is an external source ref, not a model
        // ref — it must not be rewritten even if `orders` is deferred.
        let deferred = deferred_map(&[("orders", target("", "prod", "orders"))]);
        let out = qualify_deferred_refs(
            "SELECT * FROM staging.orders",
            &deferred,
            IdentifierCaseRules::all_insensitive(),
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert!(out.contains("staging.orders"), "got: {out}");
        assert!(!out.contains("prod.orders"), "got: {out}");
    }

    #[test]
    fn leaves_non_deferred_bare_ref_untouched() {
        // `customers` is selected (built locally), not deferred — its bare
        // ref stays bare so it resolves against the working schema.
        let deferred = deferred_map(&[("orders", target("", "prod", "orders"))]);
        let out = qualify_deferred_refs(
            "SELECT * FROM orders JOIN customers ON orders.cid = customers.id",
            &deferred,
            IdentifierCaseRules::all_insensitive(),
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert!(out.contains("prod.orders"), "got: {out}");
        // `customers` (not in the deferred set) stays bare.
        assert!(
            out.contains("JOIN customers"),
            "non-deferred ref must stay bare: {out}"
        );
    }

    #[test]
    fn empty_deferred_map_is_a_noop() {
        let deferred: HashMap<String, DeferTarget> = HashMap::new();
        let sql = "SELECT * FROM orders";
        let out = qualify_deferred_refs(
            sql,
            &deferred,
            IdentifierCaseRules::all_insensitive(),
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert_eq!(out, sql);
    }

    #[test]
    fn rewrites_repeated_refs() {
        let deferred = deferred_map(&[("orders", target("", "prod", "orders"))]);
        let out = qualify_deferred_refs(
            "SELECT a.id FROM orders a JOIN orders b ON a.id = b.id",
            &deferred,
            IdentifierCaseRules::all_insensitive(),
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        // Both occurrences qualified; aliases `a`/`b` preserved.
        assert!(out.contains("FROM prod.orders a"), "got: {out}");
        assert!(out.contains("JOIN prod.orders b"), "got: {out}");
    }

    #[test]
    fn unparseable_sql_returns_err() {
        let deferred = deferred_map(&[("orders", target("", "prod", "orders"))]);
        let err = qualify_deferred_refs(
            "NOT VALID SQL ;;;",
            &deferred,
            IdentifierCaseRules::all_insensitive(),
            RecursiveCteVisibility::PrecedingAndSelf,
        );
        assert!(err.is_err());
    }

    /// DEPENDENCY CANARY for `CteScopeStack`'s one residual assumption.
    ///
    /// The scope stack keys CTE bodies by the heap address of their `Query`, so
    /// it no longer depends on `with` preceding `body` or on CTE bodies arriving
    /// in declaration order. What it DOES still require is that each CTE body is
    /// visited **as a direct child `Query`**, so `pre_visit_query` is handed that
    /// same allocation.
    ///
    /// If a future `sqlparser` flattens that traversal or adds its own CTE hook,
    /// the address lookup finds nothing, `region` stays `Body`, every alias
    /// becomes visible everywhere, and an upstream reference inside an earlier
    /// CTE body is silently left unrewritten — reading production while the
    /// model writes its shadow.
    ///
    /// This test has no source mutation by design: it guards against a
    /// `cargo update` of sqlparser, not against an edit here. Its source-mutation
    /// partner is `cte_visibility_is_positional` case 1, which fails the moment
    /// the address lookup stops resolving. This one names the cause; that one
    /// proves it matters.
    #[test]
    fn sqlparser_visits_each_cte_body_as_a_direct_child_query() {
        use sqlparser::ast::Statement;

        #[derive(Default)]
        struct Recorder {
            events: Vec<String>,
            depth: usize,
        }
        impl VisitorMut for Recorder {
            type Break = ();
            fn pre_visit_query(&mut self, q: &mut Query) -> ControlFlow<()> {
                let with = q.with.as_ref().map_or(0, |w| w.cte_tables.len());
                self.events.push(format!("query-pre(ctes={with})"));
                self.depth += 1;
                ControlFlow::Continue(())
            }
            fn post_visit_query(&mut self, _q: &mut Query) -> ControlFlow<()> {
                self.depth -= 1;
                self.events.push("query-post".to_string());
                ControlFlow::Continue(())
            }
            fn pre_visit_relation(&mut self, r: &mut ObjectName) -> ControlFlow<()> {
                self.events.push(format!("rel({r})"));
                ControlFlow::Continue(())
            }
        }

        let mut stmt: Statement = parse_single_statement(
            "WITH a AS (SELECT * FROM x), b AS (SELECT * FROM y) SELECT * FROM z",
        )
        .unwrap();
        let mut rec = Recorder::default();
        let _: ControlFlow<()> = stmt.visit(&mut rec);

        assert_eq!(
            rec.events,
            vec![
                "query-pre(ctes=2)",
                "query-pre(ctes=0)",
                "rel(x)",
                "query-post",
                "query-pre(ctes=0)",
                "rel(y)",
                "query-post",
                "rel(z)",
                "query-post",
            ],
            "sqlparser's traversal changed. The load-bearing part is that each \
             CTE body appears as its own query-pre/query-post pair — if those \
             disappeared, CteScopeStack's address keying silently stops working. \
             Order within the walk is NOT load-bearing and this assertion may be \
             relaxed for an ordering change alone."
        );
    }

    /// E1/E5 — `--defer` CTE visibility is positional, and RECURSIVE still
    /// shadows its own body.
    ///
    /// Before the shared scope stack, every alias was installed before any body
    /// was walked, so a later CTE shadowed a reference in an EARLIER CTE's body
    /// and that upstream was left unqualified — to resolve against whatever the
    /// working schema happens to hold. `defer_resolves_unbuilt_upstream_to_prod_schema`
    /// in `run.rs` seeds a decoy table expressly to show that such a read can
    /// succeed silently rather than fail loudly.
    ///
    /// Position 4 (recursive) previously worked *by accident* — the up-front
    /// install covered the self-reference. A naive positional port would have
    /// regressed it, which is why it is pinned here.
    #[test]
    fn defer_cte_visibility_is_positional() {
        let deferred = deferred_map(&[("up", target("", "prod", "up"))]);
        let rules = IdentifierCaseRules::all_insensitive();

        // 1. Earlier CTE body, later CTE of the same name: NOT shadowed.
        let out = qualify_deferred_refs(
            "WITH first AS (SELECT * FROM up), up AS (SELECT 1 AS id) SELECT * FROM first",
            &deferred,
            rules,
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert!(
            out.contains("FROM prod.up"),
            "a later CTE must not shadow an earlier body's upstream read: {out}"
        );

        // 2. Later CTE body referencing an EARLIER CTE: shadowed.
        let out = qualify_deferred_refs(
            "WITH up AS (SELECT 1 AS id), second AS (SELECT * FROM up) SELECT * FROM second",
            &deferred,
            rules,
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert!(
            !out.contains("prod.up"),
            "an earlier CTE must shadow a later body's reference: {out}"
        );

        // 3. Query body sees every CTE.
        let out = qualify_deferred_refs(
            "WITH a AS (SELECT 1 AS id), up AS (SELECT 2 AS id) SELECT * FROM up",
            &deferred,
            rules,
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert!(!out.contains("prod.up"), "the body sees every CTE: {out}");

        // 4. RECURSIVE: in scope for its own body.
        let out = qualify_deferred_refs(
            "WITH RECURSIVE up AS (SELECT 1 AS id UNION ALL SELECT id FROM up) \
             SELECT * FROM up",
            &deferred,
            rules,
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert!(
            !out.contains("prod.up"),
            "a recursive CTE shadows its own self-reference: {out}"
        );

        // 5. RECURSIVE does not expose a LATER alias to an EARLIER body.
        let out = qualify_deferred_refs(
            "WITH RECURSIVE first AS (SELECT * FROM up), up AS (SELECT 1 AS id) \
             SELECT * FROM first",
            &deferred,
            rules,
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert!(
            out.contains("FROM prod.up"),
            "a later recursive alias must not shadow an earlier body's read: {out}"
        );
    }

    /// E2/E3 — `--defer` compares CTE aliases by resolved identity.
    ///
    /// On a folding dialect `WITH Orders … FROM orders` is ONE CTE, so the
    /// reference must read it. Comparing aliases verbatim (as this did) left it
    /// unshadowed and qualified it to the PRODUCTION table instead.
    ///
    /// The quoted half pins the second axis: under Snowflake's shape a quoted
    /// alias keeps its case while an unquoted reference resolves upper, so
    /// `WITH "orders"` must NOT shadow a bare `orders`.
    #[test]
    fn defer_cte_alias_comparison_follows_dialect_identity() {
        let deferred = deferred_map(&[("orders", target("", "prod", "orders"))]);

        for (cte, reference) in [("Orders", "orders"), ("orders", "ORDERS")] {
            let out = qualify_deferred_refs(
                &format!("WITH {cte} AS (SELECT 1 AS id) SELECT * FROM {reference}"),
                &deferred,
                IdentifierCaseRules::all_insensitive(),
                RecursiveCteVisibility::PrecedingAndSelf,
            )
            .unwrap();
            assert!(
                !out.contains("prod.orders"),
                "CTE {cte} must shadow {reference} on a folding dialect: {out}"
            );
        }

        // Snowflake shape: quoted alias, unquoted reference -> different names.
        let out = qualify_deferred_refs(
            "WITH \"orders\" AS (SELECT 1 AS id) SELECT * FROM orders",
            &deferred,
            IdentifierCaseRules::uniform_uppercasing(true),
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert!(
            out.contains("prod.orders"),
            "a quoted alias must not shadow an unquoted reference there: {out}"
        );
    }

    /// E4 — the `deferred` lookup itself stays EXACT, on every dialect.
    ///
    /// Its keys are Rocky model names, and `resolve::classify_table_ref` matches
    /// a bare name to a model with an exact `HashSet::contains`. Folding this
    /// lookup would qualify a reference the compiler gave no dependency edge to,
    /// so `--defer` would disagree with the DAG about what a name means. The
    /// case axis reaches the scope stack only.
    #[test]
    fn defer_model_name_lookup_stays_exact() {
        let deferred = deferred_map(&[("Orders", target("", "prod", "orders"))]);
        for rules in [
            IdentifierCaseRules::all_insensitive(),
            IdentifierCaseRules::uniform(true),
        ] {
            let out = qualify_deferred_refs(
                "SELECT * FROM orders",
                &deferred,
                rules,
                RecursiveCteVisibility::PrecedingAndSelf,
            )
            .unwrap();
            assert_eq!(
                out, "SELECT * FROM orders",
                "a differently-cased bare name is not a ModelRef, so it is not qualified"
            );
        }
    }

    // -- rewrite_upstream_refs ---------------------------------------------

    /// A `catalog.schema.table` string as a `TargetIdentity`, spelling
    /// preserved. `uniform(true)` folds nothing, so the parts are exactly as
    /// written — which is what these tests mean when they write a key.
    fn tid(key: &str) -> TargetIdentity {
        let parts: Vec<&str> = key.split('.').collect();
        assert_eq!(parts.len(), 3, "test keys are catalog.schema.table");
        TargetIdentity::of(
            parts[0],
            parts[1],
            parts[2],
            IdentifierCaseRules::uniform(true),
        )
    }

    fn renames_map(entries: &[(&str, DeferTarget)]) -> HashMap<TargetIdentity, DeferTarget> {
        entries.iter().map(|(k, v)| (tid(k), v.clone())).collect()
    }

    #[test]
    fn rewrites_fully_qualified_ref() {
        let renames = renames_map(&[("cat.raw.orders", target("cat", "replay_ns", "raw__orders"))]);
        let out = rewrite_upstream_refs(
            "SELECT * FROM cat.raw.orders",
            &renames,
            IdentifierCaseRules::all_insensitive(),
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert_eq!(out.sql, "SELECT * FROM cat.replay_ns.raw__orders");
        assert!(out.rewritten_keys.contains(&tid("cat.raw.orders")));
        assert!(out.ambiguous_refs.is_empty());
    }

    #[test]
    fn rewrites_two_part_and_bare_tails() {
        let renames = renames_map(&[("cat.raw.orders", target("cat", "replay_ns", "raw__orders"))]);
        let two = rewrite_upstream_refs(
            "SELECT * FROM raw.orders",
            &renames,
            IdentifierCaseRules::all_insensitive(),
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert_eq!(two.sql, "SELECT * FROM cat.replay_ns.raw__orders");
        let bare = rewrite_upstream_refs(
            "SELECT * FROM orders",
            &renames,
            IdentifierCaseRules::all_insensitive(),
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert_eq!(bare.sql, "SELECT * FROM cat.replay_ns.raw__orders");
        assert!(bare.rewritten_keys.contains(&tid("cat.raw.orders")));
    }

    #[test]
    fn matches_case_insensitively() {
        let renames = renames_map(&[("cat.raw.orders", target("cat", "replay_ns", "raw__orders"))]);
        let out = rewrite_upstream_refs(
            "SELECT * FROM CAT.Raw.ORDERS",
            &renames,
            IdentifierCaseRules::all_insensitive(),
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert_eq!(out.sql, "SELECT * FROM cat.replay_ns.raw__orders");
    }

    #[test]
    fn ambiguous_tail_is_reported_not_rewritten() {
        // Two upstreams share the `orders` table segment: a bare reference
        // cannot be attributed to either — it must be reported, not guessed.
        let renames = renames_map(&[
            ("cat.raw.orders", target("cat", "replay_ns", "raw__orders")),
            (
                "cat.staging.orders",
                target("cat", "replay_ns", "staging__orders"),
            ),
        ]);
        let out = rewrite_upstream_refs(
            "SELECT * FROM orders",
            &renames,
            IdentifierCaseRules::all_insensitive(),
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert_eq!(out.sql, "SELECT * FROM orders", "ambiguous ref untouched");
        assert!(out.rewritten_keys.is_empty());
        assert_eq!(out.ambiguous_refs, vec!["orders".to_string()]);
        // A fully qualified reference disambiguates.
        let fq = rewrite_upstream_refs(
            "SELECT * FROM cat.staging.orders",
            &renames,
            IdentifierCaseRules::all_insensitive(),
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert_eq!(fq.sql, "SELECT * FROM cat.replay_ns.staging__orders");
        assert!(fq.ambiguous_refs.is_empty());
    }

    #[test]
    fn cte_shadowing_protects_bare_names_only_in_scope() {
        let renames = renames_map(&[("cat.raw.orders", target("cat", "replay_ns", "raw__orders"))]);
        let out = rewrite_upstream_refs(
            "WITH orders AS (SELECT 1 AS id) SELECT * FROM orders",
            &renames,
            IdentifierCaseRules::all_insensitive(),
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert!(
            out.sql.contains("SELECT * FROM orders"),
            "CTE-shadowed bare name untouched: {}",
            out.sql
        );
        assert!(out.rewritten_keys.is_empty());
    }

    #[test]
    fn unmatched_key_reported_via_rewritten_keys_absence() {
        let renames = renames_map(&[("cat.raw.orders", target("cat", "replay_ns", "raw__orders"))]);
        let out = rewrite_upstream_refs(
            "SELECT * FROM cat.raw.customers",
            &renames,
            IdentifierCaseRules::all_insensitive(),
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert!(out.rewritten_keys.is_empty());
        assert_eq!(out.sql, "SELECT * FROM cat.raw.customers");
    }

    // -- case-aware disambiguation ------------------------------------------

    /// Case-INsensitive dialect: any spelling reaches the one upstream.
    #[test]
    fn a_folding_dialect_matches_every_spelling() {
        let renames = renames_map(&[("cat.raw.orders", target("cat", "shadow", "orders"))]);
        for spelling in ["Orders", "raw.ORDERS", "CAT.Raw.Orders"] {
            let out = rewrite_upstream_refs(
                &format!("SELECT * FROM {spelling}"),
                &renames,
                IdentifierCaseRules::all_insensitive(),
                RecursiveCteVisibility::PrecedingAndSelf,
            )
            .unwrap();
            assert_eq!(
                out.sql, "SELECT * FROM cat.shadow.orders",
                "case-insensitive dialect must still match {spelling}"
            );
        }
    }

    /// Case-SENSITIVE dialect: a differently-cased reference names a DIFFERENT
    /// object, so it must be left alone.
    ///
    /// This is the correction that the `apply_shadow_rewrite` guard tests forced.
    /// An earlier revision of this matcher gathered candidates by folding and
    /// only compared case when several matched — which meant a lone key
    /// `raw.orders` captured a read of `raw.Orders`, redirecting it to a table it
    /// never named. Matching on identity, not resemblance, is the whole point.
    #[test]
    fn a_case_sensitive_dialect_leaves_a_differently_cased_reference_alone() {
        let renames = renames_map(&[("cat.raw.orders", target("cat", "shadow", "orders"))]);
        for spelling in ["cat.raw.Orders", "raw.ORDERS", "Orders"] {
            let out = rewrite_upstream_refs(
                &format!("SELECT * FROM {spelling}"),
                &renames,
                IdentifierCaseRules::uniform(true),
                RecursiveCteVisibility::PrecedingAndSelf,
            )
            .unwrap();
            assert_eq!(
                out.sql,
                format!("SELECT * FROM {spelling}"),
                "{spelling} names a different object than cat.raw.orders"
            );
            assert!(out.rewritten_keys.is_empty());
            assert!(
                out.ambiguous_refs.is_empty(),
                "not ambiguous — it matches no key under these rules"
            );
            // But it is NOT silently fine: it matches when case is ignored, and
            // whether that means it names the routed upstream depends on
            // connection state, so it is reported for the caller to refuse on.
            assert_eq!(
                out.case_fold_only_refs,
                vec![spelling.to_string()],
                "a case-only near-miss must be reported, never silently skipped"
            );
        }
        // The ordinary spellings still match and report nothing. This is the
        // usability floor for the refusal above: a model that spells its
        // upstream the way the target is configured — including the bare-name
        // idiom Rocky models normally use — is unaffected on a case-sensitive
        // dialect that does not fold unquoted identifiers (BigQuery). Snowflake,
        // which does fold them, is covered separately below.
        for spelling in ["cat.raw.orders", "raw.orders", "orders"] {
            let out = rewrite_upstream_refs(
                &format!("SELECT * FROM {spelling}"),
                &renames,
                IdentifierCaseRules::uniform(true),
                RecursiveCteVisibility::PrecedingAndSelf,
            )
            .unwrap();
            assert_eq!(
                out.sql, "SELECT * FROM cat.shadow.orders",
                "matching-case spelling {spelling} must route normally"
            );
            assert!(
                out.case_fold_only_refs.is_empty(),
                "{spelling} matches exactly, so nothing is reported"
            );
        }
    }

    /// Two upstreams differing only by case are two objects on a case-sensitive
    /// dialect. The reference's own spelling picks one.
    ///
    /// This is the case that used to force `apply_shadow_rewrite` to refuse the
    /// entire run. Resolving it here is what allows that guard to be deleted.
    #[test]
    fn case_separates_two_keys_that_fold_together() {
        let renames = renames_map(&[
            ("cat.raw.Orders", target("cat", "shadow", "upper__orders")),
            ("cat.raw.orders", target("cat", "shadow", "lower__orders")),
        ]);
        let rules = IdentifierCaseRules::uniform(true);

        let upper = rewrite_upstream_refs(
            "SELECT * FROM cat.raw.Orders",
            &renames,
            rules,
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert_eq!(upper.sql, "SELECT * FROM cat.shadow.upper__orders");
        assert!(upper.rewritten_keys.contains(&tid("cat.raw.Orders")));
        assert!(upper.ambiguous_refs.is_empty());

        let lower = rewrite_upstream_refs(
            "SELECT * FROM cat.raw.orders",
            &renames,
            rules,
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert_eq!(lower.sql, "SELECT * FROM cat.shadow.lower__orders");
        assert!(lower.rewritten_keys.contains(&tid("cat.raw.orders")));
        assert!(lower.ambiguous_refs.is_empty());
    }

    /// A third spelling matches neither key, so it names neither object and is
    /// left as parsed. Not ambiguous — unmatched.
    #[test]
    fn a_third_spelling_matches_neither_key() {
        let renames = renames_map(&[
            ("cat.raw.Orders", target("cat", "shadow", "upper__orders")),
            ("cat.raw.orders", target("cat", "shadow", "lower__orders")),
        ]);
        let out = rewrite_upstream_refs(
            "SELECT * FROM cat.raw.ORDERS",
            &renames,
            IdentifierCaseRules::uniform(true),
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert_eq!(out.sql, "SELECT * FROM cat.raw.ORDERS", "left as parsed");
        assert!(out.rewritten_keys.is_empty());
        assert!(out.ambiguous_refs.is_empty());
    }

    /// On a case-INsensitive dialect the same two keys name ONE object, so no
    /// spelling can separate them and every reference is ambiguous. The caller's
    /// duplicate-target checks are what should catch this first; the matcher
    /// still refuses rather than guessing.
    #[test]
    fn folding_keys_stay_ambiguous_on_a_case_insensitive_dialect() {
        let renames = renames_map(&[
            ("cat.raw.Orders", target("cat", "shadow", "upper__orders")),
            ("cat.raw.orders", target("cat", "shadow", "lower__orders")),
        ]);
        let out = rewrite_upstream_refs(
            "SELECT * FROM cat.raw.Orders",
            &renames,
            IdentifierCaseRules::all_insensitive(),
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert!(out.rewritten_keys.is_empty());
        assert_eq!(out.ambiguous_refs, vec!["cat.raw.Orders".to_string()]);
    }

    /// Disambiguation aligns by TAIL, so a two-part reference is judged by the
    /// schema and table rules — never the catalog's. With only the catalog
    /// case-sensitive, two keys differing in the schema's case still fold
    /// together and stay ambiguous.
    #[test]
    fn per_component_rules_align_to_the_tail() {
        let renames = renames_map(&[
            ("cat.Raw.orders", target("cat", "shadow", "upper__raw")),
            ("cat.raw.orders", target("cat", "shadow", "lower__raw")),
        ]);
        let catalog_only = IdentifierCaseRules {
            catalog: true,
            schema: false,
            table: false,
            unquoted_uppercases: false,
        };
        let out = rewrite_upstream_refs(
            "SELECT * FROM Raw.orders",
            &renames,
            catalog_only,
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert_eq!(
            out.ambiguous_refs,
            vec!["Raw.orders".to_string()],
            "the schema rule governs a two-part ref, and it is insensitive here"
        );

        // Turning the schema rule on separates them.
        let schema_sensitive = IdentifierCaseRules {
            catalog: true,
            schema: true,
            table: false,
            unquoted_uppercases: false,
        };
        let out = rewrite_upstream_refs(
            "SELECT * FROM Raw.orders",
            &renames,
            schema_sensitive,
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert_eq!(out.sql, "SELECT * FROM cat.shadow.upper__raw");
        assert!(out.ambiguous_refs.is_empty());
    }

    /// Snowflake's quoting axis: identical TEXT can name different objects.
    ///
    /// Rocky renders Snowflake targets quoted, so a configured target `orders`
    /// is the object `orders`. Snowflake resolves an unquoted `FROM orders` to
    /// `ORDERS` — a different table. Matching on spelled text alone (which this
    /// matcher did before, and on `main` still does) rewrites that read to the
    /// shadow and silently changes its source.
    ///
    /// The matrix below is the whole point: with an UPPERCASE configured target
    /// — the Snowflake-idiomatic choice — an unquoted reference resolves onto it
    /// and routes normally, so the common case is unaffected. It is only the
    /// lowercase/mixed-case configured target that becomes a near-miss, and such
    /// a model could not have read its upstream in production either.
    #[test]
    fn snowflake_resolves_unquoted_identifiers_before_matching() {
        let rules = IdentifierCaseRules::uniform_uppercasing(true);

        // Upper-case target: the idiomatic Snowflake shape. Unquoted references
        // resolve onto it, quoted ones must spell it exactly.
        let upper = renames_map(&[("CAT.RAW.ORDERS", target("CAT", "SHADOW", "ORDERS"))]);
        for spelling in ["cat.raw.orders", "CAT.RAW.ORDERS", "raw.orders", "orders"] {
            let out = rewrite_upstream_refs(
                &format!("SELECT * FROM {spelling}"),
                &upper,
                rules,
                RecursiveCteVisibility::PrecedingAndSelf,
            )
            .unwrap();
            assert_eq!(
                out.sql, "SELECT * FROM CAT.SHADOW.ORDERS",
                "unquoted {spelling} resolves upper and routes"
            );
            assert!(out.case_fold_only_refs.is_empty(), "{spelling}");
        }

        // Lower-case target: Rocky created `"orders"`, which an UNQUOTED
        // reference cannot name. Reported, not silently rewritten.
        let lower = renames_map(&[("cat.raw.orders", target("cat", "shadow", "orders"))]);
        let out = rewrite_upstream_refs(
            "SELECT * FROM cat.raw.orders",
            &lower,
            rules,
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert!(
            out.rewritten_keys.is_empty(),
            "an unquoted ref resolves to CAT.RAW.ORDERS, not cat.raw.orders: {}",
            out.sql
        );
        assert_eq!(
            out.case_fold_only_refs,
            vec!["cat.raw.orders".to_string()],
            "the near-miss must be reported so the caller refuses"
        );

        // Quoted, spelled exactly: that IS the object, so it routes.
        let out = rewrite_upstream_refs(
            "SELECT * FROM \"cat\".\"raw\".\"orders\"",
            &lower,
            rules,
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert_eq!(out.sql, "SELECT * FROM cat.shadow.orders");
        assert!(out.case_fold_only_refs.is_empty());
    }

    /// #1282 makes the CTE-scope axis REACHABLE on `--defer`, which
    /// `defer_cte_alias_comparison_follows_dialect_identity` had only pinned
    /// latently. This states the delta the flag causes, in one place.
    ///
    /// `uniform(true)` — Snowflake's rules before this change — treats a quoted
    /// lowercase alias as hiding an unquoted reference. Snowflake does not bind
    /// those two names, so that was Rocky modelling the SQL wrongly.
    /// `uniform_uppercasing(true)` leaves the reference free, and `--defer`
    /// qualifies it to the deferred model's target, because a bare name matching
    /// a model name is that model.
    #[test]
    fn snowflake_cte_shadowing_honours_the_quoting_axis() {
        let deferred = deferred_map(&[("orders", target("cat", "prod", "orders"))]);

        // An unquoted alias folds exactly the way the reference does, so it
        // still hides it. That is the ordinary shape and it does not change.
        let unquoted_alias = qualify_deferred_refs(
            "WITH orders AS (SELECT 1 AS id) SELECT * FROM orders",
            &deferred,
            IdentifierCaseRules::uniform_uppercasing(true),
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert!(
            !unquoted_alias.contains("prod"),
            "an unquoted alias still shadows: {unquoted_alias}"
        );

        // The delta, both halves side by side.
        let quoted_alias_sql = "WITH \"orders\" AS (SELECT 1 AS id) SELECT * FROM orders";
        let before = qualify_deferred_refs(
            quoted_alias_sql,
            &deferred,
            IdentifierCaseRules::uniform(true),
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert!(
            !before.contains("prod"),
            "premise: the old rules treated the quoted alias as hiding the reference: {before}"
        );
        let after = qualify_deferred_refs(
            quoted_alias_sql,
            &deferred,
            IdentifierCaseRules::uniform_uppercasing(true),
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert!(
            after.contains("prod"),
            "Snowflake does not bind a quoted alias to a bare reference, so the reference is \
             the deferred model: {after}"
        );
    }

    /// The shadow/replay matcher DOES honour the axis in its CTE scope, and it
    /// can, because it fails closed.
    ///
    /// On Snowflake a quoted lowercase CTE alias does not hide an unquoted
    /// reference — the warehouse does not bind those two names. So the reference
    /// reaches the matcher. Against an UPPERCASE routed target it resolves onto
    /// it and routes, which is the fix: before, the CTE was treated as hiding it
    /// and the read stayed on production while the model wrote its shadow.
    /// Against a lowercase target it is reported as a near-miss and the caller
    /// refuses. Neither outcome is silent.
    #[test]
    fn shadow_cte_scope_honours_the_quoting_axis_and_fails_closed() {
        let rules = IdentifierCaseRules::uniform_uppercasing(true);
        let sql = "WITH \"orders\" AS (SELECT 1 AS id) SELECT * FROM orders";

        let upper = renames_map(&[("CAT.RAW.ORDERS", target("CAT", "SHADOW", "ORDERS"))]);
        let out =
            rewrite_upstream_refs(sql, &upper, rules, RecursiveCteVisibility::PrecedingAndSelf)
                .unwrap();
        assert!(
            out.sql.contains("CAT.SHADOW.ORDERS"),
            "the unquoted reference names the routed upstream, so it must route: {}",
            out.sql
        );

        let lower = renames_map(&[("cat.raw.orders", target("cat", "shadow", "orders"))]);
        let out =
            rewrite_upstream_refs(sql, &lower, rules, RecursiveCteVisibility::PrecedingAndSelf)
                .unwrap();
        assert!(out.rewritten_keys.is_empty());
        assert_eq!(
            out.case_fold_only_refs,
            vec!["orders".to_string()],
            "a near-miss must be reported so the caller refuses, never rewritten silently"
        );
    }

    /// A CTE still shadows a bare name before any of this runs.
    #[test]
    fn cte_shadowing_wins_over_case_disambiguation() {
        let renames = renames_map(&[
            ("cat.raw.Orders", target("cat", "shadow", "upper__orders")),
            ("cat.raw.orders", target("cat", "shadow", "lower__orders")),
        ]);
        let out = rewrite_upstream_refs(
            "WITH Orders AS (SELECT 1 AS id) SELECT * FROM Orders",
            &renames,
            IdentifierCaseRules::uniform(true),
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert!(out.rewritten_keys.is_empty());
        assert!(out.ambiguous_refs.is_empty(), "a CTE ref is not ambiguous");
    }

    /// The recursive forward-reference rule is a per-DIALECT fact, and the two
    /// grounded dialects disagree.
    ///
    /// DuckDB, verified empirically: in
    /// `WITH RECURSIVE first AS (SELECT * FROM orders), orders AS (SELECT 999
    /// AS id) SELECT * FROM first`, `first` returns the PHYSICAL orders row —
    /// the later alias does not shadow it, so the upstream read must be
    /// redirected or the shadow run reads production.
    ///
    /// BigQuery, per vendor documentation: "A recursive CTE can reference
    /// itself, a preceding CTE, or a subsequent CTE." There the same reference
    /// binds to the later CTE, so redirecting it would change what the query
    /// reads.
    ///
    /// One rule cannot serve both, which is why this is a parameter rather than
    /// a constant. Unverified dialects take `PrecedingAndSelf`, because the
    /// failure it risks (reading a shadow table instead of a CTE) is isolated
    /// while the failure `Forward` risks (reading production) is not.
    #[test]
    fn recursive_forward_visibility_is_per_dialect() {
        let renames = renames_map(&[("cat.raw.orders", target("cat", "shadow", "orders"))]);
        let sql = "WITH RECURSIVE first AS (SELECT * FROM orders), orders AS (SELECT 1 AS id) \
                   SELECT * FROM first";

        let conservative = rewrite_upstream_refs(
            sql,
            &renames,
            IdentifierCaseRules::all_insensitive(),
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert!(
            conservative.sql.contains("FROM cat.shadow.orders"),
            "DuckDB binds this to the physical table, so it must be redirected: {}",
            conservative.sql
        );

        let forward = rewrite_upstream_refs(
            sql,
            &renames,
            IdentifierCaseRules::all_insensitive(),
            RecursiveCteVisibility::Forward,
        )
        .unwrap();
        assert!(
            !forward.sql.contains("cat.shadow.orders"),
            "BigQuery binds this to the later CTE, so it must be left alone: {}",
            forward.sql
        );

        // The self-reference is shadowed under BOTH policies.
        for policy in [
            RecursiveCteVisibility::PrecedingAndSelf,
            RecursiveCteVisibility::Forward,
        ] {
            let out = rewrite_upstream_refs(
                "WITH RECURSIVE orders AS (SELECT 1 AS id UNION ALL SELECT id FROM orders) \
                 SELECT * FROM orders",
                &renames,
                IdentifierCaseRules::all_insensitive(),
                policy,
            )
            .unwrap();
            assert!(
                !out.sql.contains("cat.shadow.orders"),
                "a recursive CTE always shadows its own self-reference: {}",
                out.sql
            );
        }
    }

    /// A CTE shadows a reference that resolves to the SAME name, not merely one
    /// spelled identically.
    ///
    /// Live defect before this fix, on every case-insensitive dialect and on the
    /// pre-existing matcher: frames stored the alias verbatim and `is_shadowed`
    /// compared exactly, while relation matching folded. `WITH Orders … FROM
    /// orders` — one CTE on DuckDB, Databricks and Trino — was therefore NOT
    /// treated as shadowed, and the reference was rewritten to
    /// `cat.shadow.orders`. The query silently read a table instead of its own
    /// CTE.
    ///
    /// Mutation: reverting `pre_visit_query` to insert `alias.value` verbatim
    /// makes the first case rewrite and fails this.
    #[test]
    fn a_cte_shadows_a_case_equivalent_reference() {
        let renames = renames_map(&[("cat.raw.orders", target("cat", "shadow", "orders"))]);

        // Case-insensitive dialect: the CTE and the reference are one name.
        for (cte, reference) in [
            ("Orders", "orders"),
            ("orders", "ORDERS"),
            ("ORDERS", "Orders"),
        ] {
            let out = rewrite_upstream_refs(
                &format!("WITH {cte} AS (SELECT 1 AS id) SELECT * FROM {reference}"),
                &renames,
                IdentifierCaseRules::all_insensitive(),
                RecursiveCteVisibility::PrecedingAndSelf,
            )
            .unwrap();
            assert!(
                !out.sql.contains("cat.shadow.orders"),
                "CTE {cte} must shadow the reference {reference}: {}",
                out.sql
            );
            assert!(out.rewritten_keys.is_empty());
            assert!(
                out.case_fold_only_refs.is_empty(),
                "shadowed, not a near-miss"
            );
        }

        // Snowflake's shape: both unquoted names resolve to ORDERS, so the CTE
        // shadows the reference there too — the case Codex flagged.
        let upper = renames_map(&[("CAT.RAW.ORDERS", target("CAT", "SHADOW", "ORDERS"))]);
        let out = rewrite_upstream_refs(
            "WITH orders AS (SELECT 1 AS id) SELECT * FROM Orders",
            &upper,
            IdentifierCaseRules::uniform_uppercasing(true),
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert!(
            !out.sql.contains("SHADOW"),
            "unquoted CTE `orders` and unquoted ref `Orders` both resolve to ORDERS: {}",
            out.sql
        );

        // Control: on a case-SENSITIVE dialect that does not fold unquoted
        // names, a differently-cased CTE genuinely does not shadow the
        // reference, so the reference is judged on its own merits.
        let out = rewrite_upstream_refs(
            "WITH Orders AS (SELECT 1 AS id) SELECT * FROM orders",
            &renames,
            IdentifierCaseRules::uniform(true),
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert_eq!(
            out.sql, "WITH Orders AS (SELECT 1 AS id) SELECT * FROM cat.shadow.orders",
            "a distinct CTE name must not shadow an exactly-matching reference"
        );
    }

    /// CTE visibility is positional: a CTE is in scope for the CTEs declared
    /// AFTER it and for the query body, never for the ones before it.
    ///
    /// Installing every alias up front made a later CTE shadow a reference in an
    /// earlier CTE's body, so an upstream read there was left unrewritten and
    /// read PRODUCTION while the run routed that upstream to a shadow target —
    /// an isolation break that exits 0.
    ///
    /// Measured before the fix, on a case-insensitive dialect:
    ///   WITH first AS (SELECT * FROM orders), Orders AS (…) SELECT * FROM first
    ///   -> `FROM orders` left as-is, rewritten_keys empty
    ///
    /// The four positions below are the whole contract. Mutation: promoting
    /// every alias in `pre_visit_query` instead of on child completion fails
    /// the first case; never promoting fails the second and third.
    #[test]
    fn cte_visibility_is_positional() {
        let renames = renames_map(&[("cat.raw.orders", target("cat", "shadow", "orders"))]);
        let rules = IdentifierCaseRules::all_insensitive();

        // 1. Earlier CTE body, later CTE of the same name: NOT shadowed, so the
        //    upstream read is redirected.
        let out = rewrite_upstream_refs(
            "WITH first AS (SELECT * FROM orders), Orders AS (SELECT 1 AS id) \
             SELECT * FROM first",
            &renames,
            rules,
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert!(
            out.sql.contains("FROM cat.shadow.orders"),
            "a later CTE must not shadow an earlier body's upstream read: {}",
            out.sql
        );
        assert!(out.rewritten_keys.contains(&tid("cat.raw.orders")));

        // 2. Later CTE body referencing an EARLIER CTE: shadowed.
        let out = rewrite_upstream_refs(
            "WITH orders AS (SELECT 1 AS id), second AS (SELECT * FROM orders) \
             SELECT * FROM second",
            &renames,
            rules,
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert!(
            !out.sql.contains("cat.shadow.orders"),
            "an earlier CTE must shadow a later body's reference: {}",
            out.sql
        );

        // 3. Query body referencing a CTE declared anywhere: shadowed.
        let out = rewrite_upstream_refs(
            "WITH a AS (SELECT 1 AS id), orders AS (SELECT 2 AS id) SELECT * FROM orders",
            &renames,
            rules,
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert!(
            !out.sql.contains("cat.shadow.orders"),
            "the body sees every CTE: {}",
            out.sql
        );

        // 4. RECURSIVE: a CTE is in scope for its OWN body.
        let out = rewrite_upstream_refs(
            "WITH RECURSIVE orders AS (SELECT 1 AS id UNION ALL SELECT id FROM orders) \
             SELECT * FROM orders",
            &renames,
            rules,
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert!(
            !out.sql.contains("cat.shadow.orders"),
            "a recursive CTE shadows its own self-reference: {}",
            out.sql
        );

        // 5. RECURSIVE does NOT make a LATER alias visible to an EARLIER body.
        //    Verified against DuckDB: `first` here reads the physical `orders`,
        //    so failing to redirect it reads PRODUCTION during a shadow run.
        //    An earlier revision exposed every alias throughout a recursive
        //    query on the theory that over-broad shadowing was harmless; it is
        //    not, and this is the case that shows it.
        let out = rewrite_upstream_refs(
            "WITH RECURSIVE first AS (SELECT * FROM orders), orders AS (SELECT 1 AS id) \
             SELECT * FROM first",
            &renames,
            rules,
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert!(
            out.sql.contains("FROM cat.shadow.orders"),
            "a later recursive alias must not shadow an earlier body's upstream read: {}",
            out.sql
        );
    }

    #[test]
    fn non_matching_relations_are_left_alone() {
        let renames = renames_map(&[("cat.raw.orders", target("cat", "replay_ns", "raw__orders"))]);
        let out = rewrite_upstream_refs(
            "SELECT o.id FROM cat.raw.orders o JOIN other.schema.customers c ON o.id = c.id",
            &renames,
            IdentifierCaseRules::all_insensitive(),
            RecursiveCteVisibility::PrecedingAndSelf,
        )
        .unwrap();
        assert!(
            out.sql.contains("cat.replay_ns.raw__orders o"),
            "{}",
            out.sql
        );
        assert!(out.sql.contains("other.schema.customers c"), "{}", out.sql);
    }

    /// **The widening's load-bearing safety property**, proven over the whole
    /// of Unicode rather than argued.
    ///
    /// The justification for replacing `to_ascii_lowercase` with Unicode
    /// `to_lowercase` is that the new fold is strictly *coarser* — it can
    /// merge keys that were distinct, never split keys that were equal. If
    /// some pair folded equal under ASCII but differs under Unicode, a
    /// collision detected today would silently stop being detected: a
    /// fail-open introduced by a change justified as fail-closed.
    ///
    /// Several shapes per scalar because `to_lowercase` is context-sensitive
    /// (Greek final sigma folds differently at the end of a word), so the
    /// position of the scalar in the string matters.
    #[test]
    fn unicode_fold_never_splits_an_ascii_equality() {
        for cp in 0u32..=0x0010_FFFF {
            let Some(c) = char::from_u32(cp) else {
                continue;
            };
            for shape in [
                format!("A{c}z"),
                format!("A{c}"),
                format!("{c}z"),
                c.to_string(),
            ] {
                let upper = shape.to_ascii_uppercase();
                let lower = shape.to_ascii_lowercase();
                // The two are equal under the OLD fold by construction.
                assert_eq!(upper.to_ascii_lowercase(), lower.to_ascii_lowercase());
                // They must remain equal under the NEW one.
                assert_eq!(
                    upper.to_lowercase(),
                    lower.to_lowercase(),
                    "U+{cp:04X} in {shape:?} splits an equality the ASCII fold established"
                );
            }
        }
    }

    /// The key is a single `catalog.schema.table` string, so a scalar that
    /// folded *into* a `.` would let one component's contents manufacture a
    /// component boundary. No scalar does; asserted rather than assumed,
    /// since it is what makes the joined form safe.
    #[test]
    fn no_scalar_lowercases_into_the_component_separator() {
        for cp in 0u32..=0x0010_FFFF {
            let Some(c) = char::from_u32(cp) else {
                continue;
            };
            if c == '.' {
                continue;
            }
            let folded: String = c.to_lowercase().collect();
            assert!(
                !folded.contains('.'),
                "U+{cp:04X} folds to {folded:?}, which contains the component separator"
            );
        }
    }

    /// Case-variant spellings are one identity — the property every collision
    /// gate leans on. The non-ASCII pair is the one an `to_ascii_lowercase`
    /// fold gets wrong: `Ä` stays uppercase, so the two keys differ and two
    /// models writing one table on a case-insensitive warehouse pass every
    /// gate silently.
    #[test]
    fn collision_identity_folds_case_including_non_ascii() {
        for (a, b) in [
            (("cat", "raw", "ORDERS"), ("cat", "raw", "orders")),
            (("CAT", "RAW", "orders"), ("cat", "raw", "orders")),
            (("cat", "raw", "TÄBLE"), ("cat", "raw", "täble")),
            (("cat", "SCHÉMA", "t"), ("cat", "schéma", "t")),
        ] {
            assert_eq!(
                CollisionIdentity::of(a.0, a.1, a.2),
                CollisionIdentity::of(b.0, b.1, b.2),
                "{a:?} and {b:?} name one object on a case-insensitive warehouse"
            );
        }
    }

    /// The fold must not go so far that genuinely distinct targets merge —
    /// an over-strict identity refuses a legitimate project.
    #[test]
    fn collision_identity_keeps_distinct_targets_distinct() {
        assert_ne!(
            CollisionIdentity::of("cat", "raw", "orders"),
            CollisionIdentity::of("cat", "raw", "order")
        );
        assert_ne!(
            CollisionIdentity::of("cat", "raw", "täble"),
            CollisionIdentity::of("cat", "raw", "table")
        );
    }

    /// `of_qualified` is documented as equivalent to `of` on the same
    /// components. Pinned, because the seam that uses it keys on a *string*
    /// built elsewhere, and a doc claim of equivalence is worth exactly as
    /// much as the test behind it.
    #[test]
    fn collision_identity_of_qualified_agrees_with_of() {
        for (c, s, t) in [
            ("cat", "raw", "orders"),
            ("CAT", "Raw", "ORDERS"),
            ("cat", "schéma", "TÄBLE"),
            ("", "raw", "orders"),
        ] {
            assert_eq!(
                CollisionIdentity::of(c, s, t),
                CollisionIdentity::of_qualified(&format!("{c}.{s}.{t}")),
                "constructors must agree for {c}.{s}.{t}"
            );
        }
    }
}
