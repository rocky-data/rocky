//! dbt-style node selection (`--select` / `--exclude`).
//!
//! A small, pure selector engine: it parses selector strings and resolves them
//! against a model graph that the caller builds. It does no I/O. The caller
//! supplies the `state:` change sets (computed from git by the CLI) when a
//! selector asks for them.
//!
//! The grammar follows dbt's node-selection syntax:
//!
//! - Syntax overview: <https://docs.getdbt.com/reference/node-selection/syntax>
//! - Graph operators (`+`, `n+`, `+n`, `@`):
//!   <https://docs.getdbt.com/reference/node-selection/graph-operators>
//! - Set operators (space = union, comma = intersection):
//!   <https://docs.getdbt.com/reference/node-selection/set-operators>
//! - Methods (`tag:`, `path:`, `config.`, `state:`, `source:`, ...):
//!   <https://docs.getdbt.com/reference/node-selection/methods>
//!
//! ```text
//! values   := value ( value )*        each `--select` value; values union
//! value    := term ( ' ' term )*      space = union
//! term     := atom ( ',' atom )*      comma = intersection
//! atom     := '@' criteria
//!           | [N] '+' criteria [ '+' [N] ]
//!           | criteria [ '+' [N] ]
//! criteria := method ':' pattern | pattern
//! ```
//!
//! A bare pattern selects by model name, unless it looks like a path (contains
//! `/` or ends in `.sql` / `.rocky`), in which case it selects by path — the
//! same inference dbt makes. Patterns accept `*` and `?` globs.
//!
//! `--exclude` uses the same grammar and is subtracted from the selection
//! after graph operators have been applied.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::models::StrategyConfig;

/// Errors raised while parsing or resolving a selector.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SelectorError {
    /// The selector string is syntactically invalid.
    #[error("invalid selector '{selector}': {reason}")]
    Syntax { selector: String, reason: String },
    /// The selector names a method Rocky does not support.
    #[error(
        "unknown selector method '{method}' in '{selector}'; supported methods: \
         name, tag, path, file, config.<key>, state, source"
    )]
    UnknownMethod { selector: String, method: String },
    /// `config.<key>` names a key Rocky cannot select on.
    #[error(
        "unsupported config key '{key}' in '{selector}'; supported keys: \
         materialized (alias: strategy, type), schema, catalog (alias: database), \
         table (alias: alias)"
    )]
    UnknownConfigKey { selector: String, key: String },
    /// `state:<x>` names a state other than `modified` / `new`.
    #[error("unsupported state selector '{selector}'; supported: state:modified, state:new")]
    UnknownState { selector: String },
    /// A `state:` selector was used but the caller provided no state.
    #[error("selector '{selector}' needs change state, but none was provided")]
    StateUnavailable { selector: String },
}

/// One model as seen by the selector.
#[derive(Debug, Clone, Default)]
pub struct SelectorNode {
    /// Model name.
    pub name: String,
    /// Resolved upstream model names (edges to unknown names are ignored).
    pub depends_on: Vec<String>,
    /// Spellings of the model's file path that `path:` may match, `/`
    /// separated — typically relative to the project root
    /// (`models/staging/stg_orders.sql`) and to the models directory
    /// (`staging/stg_orders.sql`).
    pub paths: Vec<String>,
    /// Model-level `[tags]` from the sidecar.
    pub tags: BTreeMap<String, String>,
    /// Materialization strategy name (see [`strategy_kind`]).
    pub materialization: String,
    /// Target catalog.
    pub catalog: String,
    /// Target schema.
    pub schema: String,
    /// Target table.
    pub table: String,
    /// External relations this model reads (lowercased, dotted).
    pub sources: Vec<String>,
}

/// The model graph a selector resolves against.
#[derive(Debug, Clone, Default)]
pub struct SelectorGraph {
    nodes: BTreeMap<String, SelectorNode>,
    parents: BTreeMap<String, BTreeSet<String>>,
    children: BTreeMap<String, BTreeSet<String>>,
}

impl SelectorGraph {
    /// Build a graph from nodes. Dependencies on names that are not nodes are
    /// dropped (they are external relations, not models).
    pub fn new(nodes: impl IntoIterator<Item = SelectorNode>) -> Self {
        let nodes: BTreeMap<String, SelectorNode> =
            nodes.into_iter().map(|n| (n.name.clone(), n)).collect();
        let mut parents: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        let mut children: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for node in nodes.values() {
            for dep in &node.depends_on {
                if dep != &node.name && nodes.contains_key(dep) {
                    parents
                        .entry(node.name.clone())
                        .or_default()
                        .insert(dep.clone());
                    children
                        .entry(dep.clone())
                        .or_default()
                        .insert(node.name.clone());
                }
            }
        }
        Self {
            nodes,
            parents,
            children,
        }
    }

    /// Every node name, sorted.
    pub fn names(&self) -> BTreeSet<String> {
        self.nodes.keys().cloned().collect()
    }

    /// Whether the graph holds a node with this name.
    pub fn contains(&self, name: &str) -> bool {
        self.nodes.contains_key(name)
    }

    /// Walk `edges` from `start` up to `depth` hops (`None` = unbounded).
    /// The start nodes themselves are not included.
    fn walk(
        edges: &BTreeMap<String, BTreeSet<String>>,
        start: &BTreeSet<String>,
        depth: Option<u32>,
    ) -> BTreeSet<String> {
        let mut seen: BTreeSet<String> = BTreeSet::new();
        let mut queue: VecDeque<(String, u32)> = start.iter().map(|n| (n.clone(), 0)).collect();
        while let Some((name, d)) = queue.pop_front() {
            if depth.is_some_and(|max| d >= max) {
                continue;
            }
            if let Some(next) = edges.get(&name) {
                for n in next {
                    if seen.insert(n.clone()) {
                        queue.push_back((n.clone(), d + 1));
                    }
                }
            }
        }
        seen
    }

    /// Ancestors of `start` within `depth` hops (`None` = all).
    pub fn ancestors(&self, start: &BTreeSet<String>, depth: Option<u32>) -> BTreeSet<String> {
        Self::walk(&self.parents, start, depth)
    }

    /// Descendants of `start` within `depth` hops (`None` = all).
    pub fn descendants(&self, start: &BTreeSet<String>, depth: Option<u32>) -> BTreeSet<String> {
        Self::walk(&self.children, start, depth)
    }
}

/// Model sets for the `state:` method, computed by the caller against a base
/// git ref.
#[derive(Debug, Clone, Default)]
pub struct StateSets {
    /// Models whose files changed (existing on both sides).
    pub modified: BTreeSet<String>,
    /// Models that exist only on the current side.
    pub new: BTreeSet<String>,
}

/// The `config.<key>` keys Rocky can select on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigKey {
    /// `config.materialized` (aliases `strategy`, `type`).
    Materialized,
    /// `config.schema`.
    Schema,
    /// `config.catalog` (alias `database`).
    Catalog,
    /// `config.table` (alias `alias`).
    Table,
}

/// Which `state:` set a selector asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateKind {
    /// `state:modified` — changed or new models (dbt includes new nodes).
    Modified,
    /// `state:new` — models that do not exist at the base ref.
    New,
}

/// A selector method plus its pattern.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Method {
    /// Model name (glob).
    Name(String),
    /// `[tags]` key, value, or `key=value` pair (glob).
    Tag(String),
    /// File path or directory prefix (glob).
    Path(String),
    /// File name, with or without extension (glob).
    File(String),
    /// `config.<key>:<value>`.
    Config(ConfigKey, String),
    /// `state:modified` / `state:new`.
    State(StateKind),
    /// External relation read by the model (glob, segment-aligned).
    Source(String),
}

/// One selector atom: a method with optional graph operators.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Atom {
    /// The atom as written, for messages.
    pub raw: String,
    /// The method.
    pub method: Method,
    /// `+m` / `N+m`: `Some(None)` = all ancestors, `Some(Some(n))` = n hops.
    pub parents: Option<Option<u32>>,
    /// `m+` / `m+N`: `Some(None)` = all descendants, `Some(Some(n))` = n hops.
    pub children: Option<Option<u32>>,
    /// `@m`: descendants plus all ancestors of those descendants.
    pub at: bool,
}

/// A parsed selector: a union of intersections of atoms.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Selector {
    /// Union members; each is an intersection of atoms.
    pub terms: Vec<Vec<Atom>>,
}

/// Result of resolving a selector.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Resolution {
    /// The selected model names.
    pub selected: BTreeSet<String>,
    /// Atoms (as written) whose method matched no model.
    pub unmatched: Vec<String>,
}

/// Map a strategy to the name `config.materialized` matches against. These
/// are the sidecar `type` spellings.
pub fn strategy_kind(strategy: &StrategyConfig) -> &'static str {
    match strategy {
        StrategyConfig::FullRefresh => "full_refresh",
        StrategyConfig::Incremental { .. } => "incremental",
        StrategyConfig::Merge { .. } => "merge",
        StrategyConfig::TimeInterval { .. } => "time_interval",
        StrategyConfig::DeleteInsert { .. } => "delete_insert",
        StrategyConfig::Ephemeral => "ephemeral",
        StrategyConfig::Microbatch { .. } => "microbatch",
        StrategyConfig::ContentAddressed { .. } => "content_addressed",
        StrategyConfig::View => "view",
        StrategyConfig::MaterializedView => "materialized_view",
        StrategyConfig::Snapshot { .. } => "snapshot",
        StrategyConfig::DynamicTable { .. } => "dynamic_table",
    }
}

/// Parse `--select` / `--exclude` values. Each value may itself hold several
/// space-separated terms (`--select "a b"` = `--select a --select b`).
pub fn parse(values: &[String]) -> Result<Selector, SelectorError> {
    let mut terms = Vec::new();
    for value in values {
        for term in value.split_whitespace() {
            let mut atoms = Vec::new();
            for raw in term.split(',') {
                if raw.is_empty() {
                    return Err(SelectorError::Syntax {
                        selector: term.to_string(),
                        reason: "empty selector between commas".to_string(),
                    });
                }
                atoms.push(parse_atom(raw)?);
            }
            terms.push(atoms);
        }
    }
    Ok(Selector { terms })
}

fn syntax(raw: &str, reason: &str) -> SelectorError {
    SelectorError::Syntax {
        selector: raw.to_string(),
        reason: reason.to_string(),
    }
}

fn parse_depth(raw: &str, digits: &str) -> Result<Option<u32>, SelectorError> {
    if digits.is_empty() {
        Ok(None)
    } else {
        digits
            .parse::<u32>()
            .map(Some)
            .map_err(|_| syntax(raw, "graph operator depth is not a valid number"))
    }
}

fn parse_atom(raw: &str) -> Result<Atom, SelectorError> {
    let mut rest = raw;
    let mut at = false;
    let mut parents = None;

    if let Some(stripped) = rest.strip_prefix('@') {
        at = true;
        rest = stripped;
        // dbt rejects mixing `@` with `+` graph operators.
        if rest.starts_with('+') || rest.ends_with('+') {
            return Err(syntax(raw, "'@' cannot be combined with '+'"));
        }
    } else {
        // Leading `[N]+`. Digits not followed by `+` belong to the name.
        let digits_len = rest.bytes().take_while(u8::is_ascii_digit).count();
        if rest[digits_len..].starts_with('+') {
            parents = Some(parse_depth(raw, &rest[..digits_len])?);
            rest = &rest[digits_len + 1..];
        }
    }

    // Trailing `+[N]`.
    let mut children = None;
    let trailing_digits = rest.bytes().rev().take_while(u8::is_ascii_digit).count();
    let before_digits = &rest[..rest.len() - trailing_digits];
    if let Some(body) = before_digits.strip_suffix('+') {
        if at {
            return Err(syntax(raw, "'@' cannot be combined with '+'"));
        }
        children = Some(parse_depth(raw, &rest[before_digits.len()..])?);
        rest = body;
    }

    if rest.is_empty() {
        return Err(syntax(raw, "missing a model name or method"));
    }
    if rest.contains('+') || rest.contains('@') {
        return Err(syntax(raw, "unexpected '+' or '@' inside the selector"));
    }

    let method = parse_method(raw, rest)?;
    Ok(Atom {
        raw: raw.to_string(),
        method,
        parents,
        children,
        at,
    })
}

fn parse_method(raw: &str, criteria: &str) -> Result<Method, SelectorError> {
    let Some((name, value)) = criteria.split_once(':') else {
        // dbt's inference: a value with a separator is a path; a bare file
        // name (`stg_orders.sql`) is the `file` method; anything else is a
        // name.
        let has_separator = criteria.contains('/') || criteria.contains('\\');
        let is_file = criteria.ends_with(".sql") || criteria.ends_with(".rocky");
        return Ok(if has_separator {
            Method::Path(criteria.to_string())
        } else if is_file {
            Method::File(criteria.to_string())
        } else {
            Method::Name(criteria.to_string())
        });
    };
    if value.is_empty() {
        return Err(syntax(raw, "method has an empty value"));
    }
    let value = value.to_string();
    if let Some(key) = name.strip_prefix("config.") {
        let key = match key {
            "materialized" | "strategy" | "type" => ConfigKey::Materialized,
            "schema" => ConfigKey::Schema,
            "catalog" | "database" => ConfigKey::Catalog,
            "table" | "alias" => ConfigKey::Table,
            other => {
                return Err(SelectorError::UnknownConfigKey {
                    selector: raw.to_string(),
                    key: other.to_string(),
                });
            }
        };
        return Ok(Method::Config(key, value));
    }
    match name {
        "name" | "fqn" => Ok(Method::Name(value)),
        "tag" => Ok(Method::Tag(value)),
        "path" => Ok(Method::Path(value)),
        "file" => Ok(Method::File(value)),
        "source" => Ok(Method::Source(value.to_ascii_lowercase())),
        "state" => match value.as_str() {
            "modified" => Ok(Method::State(StateKind::Modified)),
            "new" => Ok(Method::State(StateKind::New)),
            _ => Err(SelectorError::UnknownState {
                selector: raw.to_string(),
            }),
        },
        other => Err(SelectorError::UnknownMethod {
            selector: raw.to_string(),
            method: other.to_string(),
        }),
    }
}

impl Selector {
    /// Whether any atom uses the `state:` method, so the caller knows to
    /// compute [`StateSets`].
    pub fn uses_state(&self) -> bool {
        self.terms
            .iter()
            .flatten()
            .any(|a| matches!(a.method, Method::State(_)))
    }

    /// Whether the selector has no terms.
    pub fn is_empty(&self) -> bool {
        self.terms.is_empty()
    }

    /// Resolve against `graph`. `state` must be `Some` when
    /// [`Self::uses_state`] is true.
    pub fn resolve(
        &self,
        graph: &SelectorGraph,
        state: Option<&StateSets>,
    ) -> Result<Resolution, SelectorError> {
        let mut out = Resolution::default();
        for term in &self.terms {
            let mut acc: Option<BTreeSet<String>> = None;
            for atom in term {
                let matched = match_method(atom, graph, state)?;
                if matched.is_empty() {
                    out.unmatched.push(atom.raw.clone());
                }
                let expanded = apply_graph_ops(atom, graph, matched);
                acc = Some(match acc {
                    None => expanded,
                    Some(prev) => prev.intersection(&expanded).cloned().collect(),
                });
            }
            out.selected.extend(acc.unwrap_or_default());
        }
        Ok(out)
    }
}

fn apply_graph_ops(atom: &Atom, graph: &SelectorGraph, base: BTreeSet<String>) -> BTreeSet<String> {
    if atom.at {
        let mut set = base;
        set.extend(graph.descendants(&set, None));
        let ancestors = graph.ancestors(&set, None);
        set.extend(ancestors);
        return set;
    }
    let mut set = base.clone();
    if let Some(depth) = atom.parents {
        set.extend(graph.ancestors(&base, depth));
    }
    if let Some(depth) = atom.children {
        set.extend(graph.descendants(&base, depth));
    }
    set
}

fn match_method(
    atom: &Atom,
    graph: &SelectorGraph,
    state: Option<&StateSets>,
) -> Result<BTreeSet<String>, SelectorError> {
    if let Method::State(kind) = atom.method {
        let state = state.ok_or_else(|| SelectorError::StateUnavailable {
            selector: atom.raw.clone(),
        })?;
        let mut names: BTreeSet<String> = state.new.clone();
        if kind == StateKind::Modified {
            names.extend(state.modified.iter().cloned());
        }
        return Ok(names.into_iter().filter(|n| graph.contains(n)).collect());
    }
    Ok(graph
        .nodes
        .values()
        .filter(|node| node_matches(&atom.method, node))
        .map(|node| node.name.clone())
        .collect())
}

fn node_matches(method: &Method, node: &SelectorNode) -> bool {
    match method {
        Method::Name(p) => glob_match(p, &node.name),
        Method::Tag(p) => match p.split_once('=') {
            Some((k, v)) => node
                .tags
                .iter()
                .any(|(tk, tv)| glob_match(k, tk) && glob_match(v, tv)),
            None => node
                .tags
                .iter()
                .any(|(tk, tv)| glob_match(p, tk) || glob_match(p, tv)),
        },
        Method::Path(p) => {
            let pattern = normalize_path(p);
            // `path:.` / `path:./` is the project root: every model.
            if pattern.is_empty() || pattern == "." {
                return true;
            }
            node.paths.iter().any(|path| {
                glob_match(&pattern, path)
                    || path
                        .strip_prefix(pattern.as_str())
                        .is_some_and(|rest| rest.starts_with('/'))
                    || glob_match(&format!("{pattern}/*"), path)
            })
        }
        Method::File(p) => node.paths.iter().any(|path| {
            let file = path.rsplit('/').next().unwrap_or(path);
            let stem = file.rsplit_once('.').map_or(file, |(s, _)| s);
            glob_match(p, file) || glob_match(p, stem)
        }),
        Method::Config(key, p) => match key {
            ConfigKey::Materialized => {
                glob_match(p, &node.materialization)
                    || (p == "table" && node.materialization == "full_refresh")
            }
            ConfigKey::Schema => glob_match(p, &node.schema),
            ConfigKey::Catalog => glob_match(p, &node.catalog),
            ConfigKey::Table => glob_match(p, &node.table),
        },
        Method::Source(p) => node.sources.iter().any(|s| source_matches(p, s)),
        Method::State(_) => false,
    }
}

/// Strip `./`, trailing `/`, and normalise separators to `/`.
fn normalize_path(p: &str) -> String {
    let mut s = p.replace('\\', "/");
    while let Some(rest) = s.strip_prefix("./") {
        s = rest.to_string();
    }
    while s.len() > 1 && s.ends_with('/') {
        s.pop();
    }
    s
}

/// A `source:` pattern matches a relation when its dotted segments equal a
/// prefix (`raw` → `raw.orders`) or a suffix (`raw.orders` →
/// `warehouse.raw.orders`) of the relation's segments. Each segment may glob.
fn source_matches(pattern: &str, relation: &str) -> bool {
    let p: Vec<&str> = pattern.split('.').collect();
    let r: Vec<&str> = relation.split('.').collect();
    if p.len() > r.len() {
        return false;
    }
    let seg_eq = |a: &[&str], b: &[&str]| a.iter().zip(b).all(|(x, y)| glob_match(x, y));
    seg_eq(&p, &r[..p.len()]) || seg_eq(&p, &r[r.len() - p.len()..])
}

/// Minimal glob: `*` matches any run (including `/`), `?` one character.
pub fn glob_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    let (mut pi, mut ti) = (0usize, 0usize);
    let mut star: Option<(usize, usize)> = None;
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some((pi, ti));
            pi += 1;
        } else if let Some((sp, st)) = star {
            pi = sp + 1;
            ti = st + 1;
            star = Some((sp, st + 1));
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

/// The final result of `--select` minus `--exclude`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Selection {
    /// Selected model names.
    pub models: BTreeSet<String>,
    /// Human-readable warnings (unmatched criteria), dbt-worded.
    pub warnings: Vec<String>,
}

/// Resolve `select` (all models when empty) minus `exclude`.
pub fn select(
    graph: &SelectorGraph,
    select: &Selector,
    exclude: &Selector,
    state: Option<&StateSets>,
) -> Result<Selection, SelectorError> {
    let mut warnings = Vec::new();
    let mut models = if select.is_empty() {
        graph.names()
    } else {
        let r = select.resolve(graph, state)?;
        warnings.extend(r.unmatched.iter().map(|raw| {
            format!("The selection criterion '{raw}' does not match any enabled nodes")
        }));
        r.selected
    };
    if !exclude.is_empty() {
        let r = exclude.resolve(graph, state)?;
        warnings.extend(r.unmatched.iter().map(|raw| {
            format!("The exclusion criterion '{raw}' does not match any enabled nodes")
        }));
        models.retain(|m| !r.selected.contains(m));
    }
    Ok(Selection { models, warnings })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(name: &str, deps: &[&str]) -> SelectorNode {
        SelectorNode {
            name: name.to_string(),
            depends_on: deps.iter().map(ToString::to_string).collect(),
            paths: vec![format!("models/{name}.sql"), format!("{name}.sql")],
            materialization: "full_refresh".to_string(),
            schema: "analytics".to_string(),
            catalog: "warehouse".to_string(),
            table: name.to_string(),
            ..Default::default()
        }
    }

    /// Diamond with a tail and an unrelated island:
    ///
    /// ```text
    ///        a
    ///       / \
    ///      b   c
    ///       \ /
    ///        d
    ///        |
    ///        e        x (island)
    /// ```
    fn diamond() -> SelectorGraph {
        SelectorGraph::new(vec![
            node("a", &[]),
            node("b", &["a"]),
            node("c", &["a"]),
            node("d", &["b", "c"]),
            node("e", &["d"]),
            node("x", &[]),
        ])
    }

    fn sel(graph: &SelectorGraph, s: &str) -> Vec<String> {
        parse(&[s.to_string()])
            .unwrap()
            .resolve(graph, None)
            .unwrap()
            .selected
            .into_iter()
            .collect()
    }

    fn names(v: &[&str]) -> Vec<String> {
        v.iter().map(ToString::to_string).collect()
    }

    #[test]
    fn parses_graph_operators() {
        let a = parse_atom("2+m+3").unwrap();
        assert_eq!(a.parents, Some(Some(2)));
        assert_eq!(a.children, Some(Some(3)));
        assert_eq!(a.method, Method::Name("m".into()));
        let a = parse_atom("+m").unwrap();
        assert_eq!(a.parents, Some(None));
        assert_eq!(a.children, None);
        let a = parse_atom("@m").unwrap();
        assert!(a.at);
        // Leading digits not followed by '+' are part of the name.
        let a = parse_atom("2024_orders").unwrap();
        assert_eq!(a.method, Method::Name("2024_orders".into()));
        assert_eq!(a.parents, None);
        // Trailing digits without '+' are part of the name too.
        let a = parse_atom("orders_v2").unwrap();
        assert_eq!(a.method, Method::Name("orders_v2".into()));
        assert_eq!(a.children, None);
    }

    #[test]
    fn rejects_bad_syntax() {
        assert!(parse_atom("@+m").is_err());
        assert!(parse_atom("@m+").is_err());
        assert!(parse_atom("+").is_err());
        assert!(parse_atom("a+b").is_err());
        assert!(parse(&["a,,b".into()]).is_err());
        assert!(parse_atom("tag:").is_err());
    }

    #[test]
    fn unknown_method_and_keys_are_errors() {
        assert!(matches!(
            parse_atom("bogus:x"),
            Err(SelectorError::UnknownMethod { .. })
        ));
        assert!(matches!(
            parse_atom("test_type:unit"),
            Err(SelectorError::UnknownMethod { .. })
        ));
        assert!(matches!(
            parse_atom("config.owner:x"),
            Err(SelectorError::UnknownConfigKey { .. })
        ));
        assert!(matches!(
            parse_atom("state:modified.body"),
            Err(SelectorError::UnknownState { .. })
        ));
    }

    #[test]
    fn bare_path_like_values_select_by_path() {
        assert_eq!(
            parse_atom("models/staging").unwrap().method,
            Method::Path("models/staging".into())
        );
        assert_eq!(
            parse_atom("stg.sql").unwrap().method,
            Method::File("stg.sql".into())
        );
    }

    #[test]
    fn diamond_ancestors_and_descendants() {
        let g = diamond();
        assert_eq!(sel(&g, "+d"), names(&["a", "b", "c", "d"]));
        assert_eq!(sel(&g, "a+"), names(&["a", "b", "c", "d", "e"]));
        assert_eq!(sel(&g, "+d+"), names(&["a", "b", "c", "d", "e"]));
        assert_eq!(sel(&g, "d"), names(&["d"]));
    }

    #[test]
    fn depth_limits() {
        let g = diamond();
        assert_eq!(sel(&g, "1+d"), names(&["b", "c", "d"]));
        assert_eq!(sel(&g, "2+e"), names(&["b", "c", "d", "e"]));
        assert_eq!(sel(&g, "a+1"), names(&["a", "b", "c"]));
        assert_eq!(sel(&g, "a+2"), names(&["a", "b", "c", "d"]));
        assert_eq!(sel(&g, "0+d"), names(&["d"]));
    }

    #[test]
    fn at_operator_adds_ancestors_of_descendants() {
        let g = diamond();
        // @b = b, its descendants (d, e), and every ancestor of those (a, c).
        assert_eq!(sel(&g, "@b"), names(&["a", "b", "c", "d", "e"]));
        assert_eq!(sel(&g, "@x"), names(&["x"]));
    }

    #[test]
    fn globs_union_and_intersection() {
        let g = diamond();
        assert_eq!(sel(&g, "a x"), names(&["a", "x"]));
        // `+d,c+` = ancestors-of-d ∩ descendants-of-c.
        assert_eq!(sel(&g, "+d,c+"), names(&["c", "d"]));
        assert_eq!(sel(&g, "*"), names(&["a", "b", "c", "d", "e", "x"]));
        assert!(glob_match("stg_*", "stg_orders"));
        assert!(!glob_match("stg_*", "fct_orders"));
        assert!(glob_match("s?g_*s", "stg_orders"));
    }

    #[test]
    fn exclude_subtracts_after_graph_ops() {
        let g = diamond();
        let s = parse(&["a+".into()]).unwrap();
        let x = parse(&["d+".into()]).unwrap();
        let r = select(&g, &s, &x, None).unwrap();
        assert_eq!(
            r.models.into_iter().collect::<Vec<_>>(),
            names(&["a", "b", "c"])
        );
        // Exclude alone applies to every model.
        let r = select(&g, &Selector::default(), &x, None).unwrap();
        assert_eq!(
            r.models.into_iter().collect::<Vec<_>>(),
            names(&["a", "b", "c", "x"])
        );
    }

    #[test]
    fn unmatched_criteria_warn() {
        let g = diamond();
        let s = parse(&["nope a".into()]).unwrap();
        let r = select(&g, &s, &Selector::default(), None).unwrap();
        assert_eq!(r.models.len(), 1);
        assert_eq!(r.warnings.len(), 1);
        assert!(r.warnings[0].contains("'nope'"));
    }

    #[test]
    fn methods_tag_path_config_source_file() {
        let mut stg = node("stg_orders", &[]);
        stg.paths = vec![
            "models/staging/stg_orders.sql".into(),
            "staging/stg_orders.sql".into(),
        ];
        stg.tags.insert("domain".into(), "finance".into());
        stg.sources = vec!["raw.orders".into()];
        stg.materialization = "view".into();
        let mut fct = node("fct_revenue", &["stg_orders"]);
        fct.paths = vec![
            "models/marts/fct_revenue.sql".into(),
            "marts/fct_revenue.sql".into(),
        ];
        fct.tags.insert("tier".into(), "gold".into());
        fct.sources = vec!["warehouse.raw.customers".into()];
        fct.schema = "marts".into();
        let g = SelectorGraph::new(vec![stg, fct]);

        assert_eq!(sel(&g, "tag:finance"), names(&["stg_orders"]));
        assert_eq!(sel(&g, "tag:tier"), names(&["fct_revenue"]));
        assert_eq!(sel(&g, "tag:tier=gold"), names(&["fct_revenue"]));
        assert!(sel(&g, "tag:tier=silver").is_empty());
        assert_eq!(sel(&g, "path:models/staging"), names(&["stg_orders"]));
        assert_eq!(sel(&g, "path:./models/staging/"), names(&["stg_orders"]));
        assert_eq!(sel(&g, "path:./").len(), 2, "the root selects every model");
        assert_eq!(sel(&g, "stg_orders.sql"), names(&["stg_orders"]));
        assert_eq!(sel(&g, "path:marts"), names(&["fct_revenue"]));
        assert_eq!(sel(&g, "marts/fct_revenue.sql"), names(&["fct_revenue"]));
        assert!(
            sel(&g, "path:mart").is_empty(),
            "directory prefix is segment-aligned"
        );
        assert_eq!(sel(&g, "file:stg_orders"), names(&["stg_orders"]));
        assert_eq!(sel(&g, "config.materialized:view"), names(&["stg_orders"]));
        assert_eq!(
            sel(&g, "config.materialized:table"),
            names(&["fct_revenue"])
        );
        assert_eq!(sel(&g, "config.schema:marts"), names(&["fct_revenue"]));
        assert_eq!(sel(&g, "source:raw"), names(&["stg_orders"]));
        assert_eq!(sel(&g, "source:raw.customers"), names(&["fct_revenue"]));
        // Case-insensitive; the suffix rule lets `raw.*` match the
        // three-part `warehouse.raw.customers` too.
        assert_eq!(
            sel(&g, "source:RAW.*"),
            names(&["fct_revenue", "stg_orders"])
        );
        assert!(sel(&g, "source:warehouse").len() == 1);
        assert_eq!(
            sel(&g, "source:raw+"),
            names(&["fct_revenue", "stg_orders"])
        );
        assert_eq!(
            sel(&g, "tag:finance+,config.schema:marts"),
            names(&["fct_revenue"])
        );
    }

    #[test]
    fn state_methods() {
        let g = diamond();
        let state = StateSets {
            modified: ["b".to_string()].into(),
            new: ["x".to_string(), "gone".to_string()].into(),
        };
        let resolve = |s: &str| -> Vec<String> {
            parse(&[s.to_string()])
                .unwrap()
                .resolve(&g, Some(&state))
                .unwrap()
                .selected
                .into_iter()
                .collect()
        };
        assert_eq!(resolve("state:modified"), names(&["b", "x"]));
        assert_eq!(resolve("state:new"), names(&["x"]));
        assert_eq!(resolve("state:modified+"), names(&["b", "d", "e", "x"]));
        let p = parse(&["state:new".into()]).unwrap();
        assert!(p.uses_state());
        assert!(matches!(
            p.resolve(&g, None),
            Err(SelectorError::StateUnavailable { .. })
        ));
    }

    #[test]
    fn cycle_safe_walk() {
        // A self-edge and a back-edge must not loop forever.
        let g = SelectorGraph::new(vec![node("p", &["q", "p"]), node("q", &["p"])]);
        assert_eq!(sel(&g, "+p"), names(&["p", "q"]));
        assert_eq!(sel(&g, "@q"), names(&["p", "q"]));
    }
}
