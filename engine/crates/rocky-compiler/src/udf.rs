//! Compile-time support for user-defined functions (`functions/`).
//!
//! [`rocky_core::functions`] loads each function's sidecar and body and owns
//! the per-dialect DDL. This module turns those definitions into a
//! [`FunctionRegistry`] the compiler consults for four things:
//!
//! 1. **Validation** — a definition that cannot be created (Python, a bad
//!    identifier or type, a missing body, a duplicate name, a call cycle) is an
//!    `E051` attributed to the function.
//! 2. **Dependencies** — which functions each model calls
//!    ([`function_usage`]), and the order functions must be created in
//!    ([`FunctionRegistry::creation_order`]).
//! 3. **Call checking** — a call with the wrong number of arguments, or a call
//!    to a function that failed validation, is an `E051` on the calling model.
//!    An argument whose type is certainly incompatible with the declared
//!    parameter is an `E051`; one Rocky cannot verify, or that relies on an
//!    implicit warehouse coercion, is a `W051`.
//! 4. **Return types** — a projection that is a direct UDF call takes the
//!    declared return type instead of `Unknown`, so downstream models and
//!    contracts see it.
//!
//! Argument types need the expression type scope that lives inside
//! `typecheck.rs`. Rather than thread a registry through every inference
//! helper, the typecheck of one model installs a [`TypecheckScope`] on the
//! current thread for its duration; `infer_function_type` consults it for
//! names it does not know. Each model's typecheck runs synchronously on one
//! rayon worker with no nested parallelism, and scopes nest as a stack, so a
//! scope is never observed by another model's inference.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::Path;
use std::sync::Arc;

use sqlparser::ast::{self, Expr, SelectItem, SetExpr, Statement, Visit};
use sqlparser::parser::Parser;

use rocky_core::functions::{FunctionDef, LoadedFunctions, functions_dir_for};

use crate::diagnostic::{Diagnostic, E051, SourceSpan, W051};
use crate::types::{RockyType, TypedColumn};

/// One declared parameter, with its type resolved for checking.
#[derive(Debug, Clone, PartialEq)]
pub struct UdfParam {
    pub name: String,
    /// The declared type, as written.
    pub sql_type: String,
    /// [`udf_declared_type`] of `sql_type`.
    pub data_type: RockyType,
}

/// A function that passed validation.
#[derive(Debug, Clone, PartialEq)]
pub struct UdfSignature {
    pub def: FunctionDef,
    pub params: Vec<UdfParam>,
    /// [`udf_declared_type`] of the declared return type.
    pub returns: RockyType,
    /// Other project functions this function's body calls (display names).
    pub calls: BTreeSet<String>,
}

impl UdfSignature {
    /// `name(arg TYPE, ...) RETURNS TYPE`.
    #[must_use]
    pub fn signature(&self) -> String {
        self.def.signature()
    }

    /// Markdown for an editor hover: the signature, then the description.
    #[must_use]
    pub fn hover_markdown(&self) -> String {
        let mut md = format!("**Function:** `{}`", self.signature());
        if let Some(description) = self
            .def
            .config
            .description
            .as_deref()
            .filter(|d| !d.trim().is_empty())
        {
            md.push_str(&format!("\n\n> {}", description.trim()));
        }
        md
    }
}

/// Every function a project declares, keyed case-insensitively by name.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FunctionRegistry {
    valid: BTreeMap<String, UdfSignature>,
    /// Declared but unusable (failed validation): lowercased name → name.
    invalid: BTreeMap<String, String>,
}

impl FunctionRegistry {
    /// No function declared at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.valid.is_empty() && self.invalid.is_empty()
    }

    /// Valid functions, in name order.
    pub fn functions(&self) -> impl Iterator<Item = &UdfSignature> {
        self.valid.values()
    }

    /// Whether the project declares a function called `name`, valid or not.
    #[must_use]
    pub fn declares(&self, name: &str) -> bool {
        let key = name.to_ascii_lowercase();
        self.valid.contains_key(&key) || self.invalid.contains_key(&key)
    }

    /// The valid function `name` refers to (case-insensitive), if any.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&UdfSignature> {
        self.valid.get(&name.to_ascii_lowercase())
    }

    /// Resolve a call's (possibly qualified) name.
    fn resolve_call(&self, name: &ast::ObjectName) -> Option<Resolved<'_>> {
        let parts: Vec<String> = name
            .0
            .iter()
            .filter_map(|p| p.as_ident().map(|i| i.value.clone()))
            .collect();
        let (last, qualifier) = parts.split_last()?;
        let key = last.to_ascii_lowercase();
        if let Some(sig) = self.valid.get(&key) {
            // A qualified call resolves only when every qualifier part
            // matches the declared target, so `other.f(x)` or
            // `governance.f(x, y)` is never mistaken for this project's `f`.
            if !qualifier.is_empty() {
                let target = &sig.def.config.target;
                let declared: Vec<&str> = [target.catalog.as_deref(), target.schema.as_deref()]
                    .into_iter()
                    .flatten()
                    .collect();
                if qualifier.len() > declared.len()
                    || !qualifier
                        .iter()
                        .rev()
                        .zip(declared.iter().rev())
                        .all(|(call, decl)| call.eq_ignore_ascii_case(decl))
                {
                    return None;
                }
            }
            return Some(Resolved::Valid(sig));
        }
        if qualifier.is_empty() {
            return self
                .invalid
                .get(&key)
                .map(|n| Resolved::Invalid(n.as_str()));
        }
        None
    }

    /// The functions needed to run `needed` (display or lowercase names),
    /// including the functions they call, in creation order (callees first).
    #[must_use]
    pub fn creation_order<'a, I>(&self, needed: I) -> Vec<&UdfSignature>
    where
        I: IntoIterator<Item = &'a str>,
    {
        let mut order = Vec::new();
        let mut visited = HashSet::new();
        for name in needed {
            self.visit_creation(&name.to_ascii_lowercase(), &mut visited, &mut order);
        }
        order
    }

    fn visit_creation<'s>(
        &'s self,
        key: &str,
        visited: &mut HashSet<String>,
        order: &mut Vec<&'s UdfSignature>,
    ) {
        if !visited.insert(key.to_string()) {
            return;
        }
        let Some(sig) = self.valid.get(key) else {
            return;
        };
        for callee in &sig.calls {
            self.visit_creation(&callee.to_ascii_lowercase(), visited, order);
        }
        order.push(sig);
    }
}

enum Resolved<'a> {
    Valid(&'a UdfSignature),
    Invalid(&'a str),
}

/// Map a declared UDF type to a [`RockyType`].
///
/// Stricter than the source-schema mappers on purpose: a declared return type
/// flows straight into contracts, so a name that means different types on
/// different warehouses must not become a concrete type here. `FLOAT` / `REAL`
/// are 32-bit on DuckDB and Databricks but 64-bit on Snowflake, and
/// `INT` / `INTEGER` / `SMALLINT` / `TINYINT` are `NUMBER(38,0)` on Snowflake
/// and `INT64` on BigQuery, so all of them stay [`RockyType::Unknown`].
/// Everything else goes through
/// [`rocky_core::contracts::warehouse_type_to_rocky`], which also knows the
/// BigQuery scalar names (`INT64`, `FLOAT64`, …).
#[must_use]
pub fn udf_declared_type(sql_type: &str) -> RockyType {
    let upper = sql_type.trim().to_ascii_uppercase();
    match upper.as_str() {
        "FLOAT" | "FLOAT4" | "FLOAT8" | "REAL" | "INT" | "INTEGER" | "SMALLINT" | "TINYINT"
        | "BYTEINT" | "BIGINT" | "TIMESTAMP" => RockyType::Unknown,
        _ => rocky_core::contracts::warehouse_type_to_rocky(&upper),
    }
}

/// Map a declared *parameter* type for argument checking.
///
/// Looser than [`udf_declared_type`]: a parameter type never reaches a
/// contract, it only decides whether a call is flagged. `BIGINT` reads as
/// `Int64` here (it is on DuckDB, Databricks and BigQuery; Snowflake's
/// `NUMBER(38,0)` is still integral), so typed calls verify without a
/// warning. The checks that refuse are narrow category mismatches, which a
/// width difference never triggers.
#[must_use]
pub fn udf_param_type(sql_type: &str) -> RockyType {
    match sql_type.trim().to_ascii_uppercase().as_str() {
        "BIGINT" => RockyType::Int64,
        _ => udf_declared_type(sql_type),
    }
}

/// Built-in function names the type checker models, plus common ones it
/// does not. A project function with one of these names is ignored.
const BUILTIN_NAMES: &[&str] = &[
    "abs",
    "avg",
    "cast",
    "ceil",
    "ceiling",
    "char_length",
    "character_length",
    "coalesce",
    "concat",
    "concat_ws",
    "cos",
    "count",
    "cume_dist",
    "current_date",
    "current_timestamp",
    "date",
    "date_add",
    "date_sub",
    "date_trunc",
    "dateadd",
    "datediff",
    "datesub",
    "day",
    "dayofweek",
    "dayofyear",
    "dense_rank",
    "exp",
    "first_value",
    "floor",
    "greatest",
    "hour",
    "if",
    "iff",
    "ifnull",
    "initcap",
    "instr",
    "lag",
    "last_value",
    "lead",
    "least",
    "left",
    "length",
    "ln",
    "log",
    "log10",
    "log2",
    "lower",
    "lpad",
    "ltrim",
    "max",
    "md5",
    "min",
    "minute",
    "month",
    "months_between",
    "now",
    "nth_value",
    "ntile",
    "nullif",
    "nvl",
    "octet_length",
    "percent_rank",
    "position",
    "pow",
    "power",
    "quarter",
    "rank",
    "replace",
    "reverse",
    "right",
    "round",
    "row_number",
    "rpad",
    "rtrim",
    "second",
    "sha1",
    "sha2",
    "sha256",
    "sign",
    "sin",
    "sqrt",
    "strpos",
    "substr",
    "substring",
    "sum",
    "tan",
    "timestamp",
    "timestampdiff",
    "to_date",
    "to_timestamp",
    "today",
    "trim",
    "trunc",
    "truncate",
    "try_cast",
    "upper",
    "weekofyear",
    "year",
];

fn is_builtin_name(lower: &str) -> bool {
    BUILTIN_NAMES.contains(&lower)
}

fn function_span(def: &FunctionDef) -> SourceSpan {
    SourceSpan {
        file: def.file_path.display().to_string(),
        line: 1,
        col: 1,
    }
}

/// Load the `functions/` directory that belongs to `models_dir`.
#[must_use]
pub fn load_for_models_dir(models_dir: &Path) -> (FunctionRegistry, Vec<Diagnostic>) {
    match functions_dir_for(models_dir) {
        Some(dir) => build_registry(rocky_core::functions::load_functions_from_dir(&dir)),
        None => (FunctionRegistry::default(), Vec::new()),
    }
}

/// Validate loaded definitions and build the registry. Every problem is an
/// `E051` (or `W051` for a body Rocky cannot parse) attributed to the
/// function's name.
#[must_use]
pub fn build_registry(loaded: LoadedFunctions) -> (FunctionRegistry, Vec<Diagnostic>) {
    let mut diagnostics = Vec::new();
    let mut registry = FunctionRegistry::default();

    for err in loaded.errors {
        diagnostics.push(
            Diagnostic::error(E051, &err.name, err.message).with_span(SourceSpan {
                file: err.file_path.display().to_string(),
                line: 1,
                col: 1,
            }),
        );
        registry
            .invalid
            .insert(err.name.to_ascii_lowercase(), err.name.clone());
    }

    // Duplicate names (case-insensitive) make every holder invalid: there is
    // no rule that says which definition a call means.
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for def in &loaded.functions {
        *counts.entry(def.name.to_ascii_lowercase()).or_default() += 1;
    }

    let mut candidates: Vec<UdfSignature> = Vec::new();
    for def in loaded.functions {
        let key = def.name.to_ascii_lowercase();
        // A name the warehouse already defines: the builtin wins on the
        // warehouse, so treating calls as this function would refuse valid
        // SQL. Report it and leave the function out entirely.
        if is_builtin_name(&key) {
            diagnostics.push(
                Diagnostic::warning(
                    W051,
                    &def.name,
                    format!(
                        "function `{}` has the name of a built-in SQL function; Rocky ignores it \
                         (calls resolve to the built-in) — rename it",
                        def.name
                    ),
                )
                .with_span(function_span(&def)),
            );
            continue;
        }
        let mut problems = def.validation_problems();
        if counts.get(&key).copied().unwrap_or(0) > 1 {
            problems.push(format!(
                "function `{}` is declared more than once (names are case-insensitive)",
                def.name
            ));
        }
        if !problems.is_empty() {
            for problem in problems {
                diagnostics.push(
                    Diagnostic::error(E051, &def.name, problem)
                        .with_span(function_span(&def))
                        .with_suggestion(
                            "fix the function's .toml/.sql pair under functions/ — \
                             models that call it are not built until it is valid",
                        ),
                );
            }
            registry.invalid.insert(key, def.name.clone());
            continue;
        }
        let params = def
            .config
            .arguments
            .iter()
            .map(|a| UdfParam {
                name: a.name.clone(),
                sql_type: a.data_type.trim().to_string(),
                data_type: udf_param_type(&a.data_type),
            })
            .collect();
        let returns = udf_declared_type(&def.config.returns);
        candidates.push(UdfSignature {
            def,
            params,
            returns,
            calls: BTreeSet::new(),
        });
    }

    // Body calls to other project functions → creation-order edges. A body
    // Rocky cannot parse is not refused (it may use dialect syntax the
    // compiler's parser lacks); it is reported and passed through unchanged.
    let names: BTreeMap<String, String> = candidates
        .iter()
        .map(|c| (c.def.name.to_ascii_lowercase(), c.def.name.clone()))
        .collect();
    for cand in &mut candidates {
        let body = cand.def.body.clone().unwrap_or_default();
        let dialect = rocky_sql::dialect::DatabricksDialect;
        match Parser::parse_sql(&dialect, &format!("SELECT {body}")) {
            Ok(stmts) => {
                let mut calls = BTreeSet::new();
                let _ = stmts.visit(&mut FunctionCallVisitor(|f: &ast::Function| {
                    if let Some(last) = f.name.0.last().and_then(ast::ObjectNamePart::as_ident)
                        && let Some(name) = names.get(&last.value.to_ascii_lowercase())
                    {
                        calls.insert(name.clone());
                    }
                }));
                cand.calls = calls;
            }
            Err(e) => diagnostics.push(
                Diagnostic::warning(
                    W051,
                    &cand.def.name,
                    format!(
                        "could not parse the function body ({e}); it is sent to the \
                         warehouse unchanged and calls inside it are not tracked"
                    ),
                )
                .with_span(function_span(&cand.def)),
            ),
        }
    }

    // A call cycle cannot be created in any order.
    let by_key: BTreeMap<String, &UdfSignature> = candidates
        .iter()
        .map(|c| (c.def.name.to_ascii_lowercase(), c))
        .collect();
    let mut cyclic: BTreeSet<String> = BTreeSet::new();
    for key in by_key.keys() {
        if reaches(key, key, &by_key, &mut HashSet::new()) {
            cyclic.insert(key.clone());
        }
    }

    for cand in candidates {
        let key = cand.def.name.to_ascii_lowercase();
        if cyclic.contains(&key) {
            diagnostics.push(
                Diagnostic::error(
                    E051,
                    &cand.def.name,
                    format!(
                        "function `{}` is part of a call cycle between functions",
                        cand.def.name
                    ),
                )
                .with_span(function_span(&cand.def)),
            );
            registry.invalid.insert(key, cand.def.name.clone());
            continue;
        }
        registry.valid.insert(key, cand);
    }

    // A valid function whose body calls an invalid one cannot be created.
    loop {
        let broken: Vec<String> = registry
            .valid
            .iter()
            .filter(|(_, sig)| {
                sig.calls
                    .iter()
                    .any(|c| !registry.valid.contains_key(&c.to_ascii_lowercase()))
            })
            .map(|(k, _)| k.clone())
            .collect();
        if broken.is_empty() {
            break;
        }
        for key in broken {
            if let Some(sig) = registry.valid.remove(&key) {
                diagnostics.push(
                    Diagnostic::error(
                        E051,
                        &sig.def.name,
                        format!(
                            "function `{}` calls a function that is not valid: {}",
                            sig.def.name,
                            sig.calls.iter().cloned().collect::<Vec<_>>().join(", ")
                        ),
                    )
                    .with_span(function_span(&sig.def)),
                );
                registry.invalid.insert(key, sig.def.name.clone());
            }
        }
    }

    (registry, diagnostics)
}

fn reaches(
    from: &str,
    target: &str,
    by_key: &BTreeMap<String, &UdfSignature>,
    seen: &mut HashSet<String>,
) -> bool {
    let Some(sig) = by_key.get(from) else {
        return false;
    };
    for callee in &sig.calls {
        let key = callee.to_ascii_lowercase();
        if key == target {
            return true;
        }
        if seen.insert(key.clone()) && reaches(&key, target, by_key, seen) {
            return true;
        }
    }
    false
}

/// Visits every function call in a statement, nested ones included.
struct FunctionCallVisitor<F: FnMut(&ast::Function)>(F);

impl<F: FnMut(&ast::Function)> ast::Visitor for FunctionCallVisitor<F> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> std::ops::ControlFlow<()> {
        if let Expr::Function(f) = expr {
            (self.0)(f);
        }
        std::ops::ControlFlow::Continue(())
    }
}

/// Positional argument expressions of a call, or `None` when the argument
/// list is not a plain positional list (named arguments, `*`, a subquery),
/// where Rocky cannot be certain how arguments bind.
fn positional_args(func: &ast::Function) -> Option<Vec<&Expr>> {
    match &func.args {
        ast::FunctionArguments::List(list) => {
            if !list.clauses.is_empty() || list.duplicate_treatment.is_some() {
                return None;
            }
            list.args
                .iter()
                .map(|arg| match arg {
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => Some(e),
                    _ => None,
                })
                .collect()
        }
        ast::FunctionArguments::None => Some(Vec::new()),
        ast::FunctionArguments::Subquery(_) => None,
    }
}

fn parse_query(sql: &str) -> Option<Vec<Statement>> {
    let dialect = rocky_sql::dialect::DatabricksDialect;
    Parser::parse_sql(&dialect, sql).ok()
}

/// Which project functions each model calls: function name → calling models.
///
/// Only valid functions appear. A model whose SQL Rocky cannot parse is
/// skipped (the typecheck reports it).
#[must_use]
pub fn function_usage(
    models: &[rocky_core::models::Model],
    registry: &FunctionRegistry,
) -> BTreeMap<String, BTreeSet<String>> {
    let mut usage: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    if registry.valid.is_empty() {
        return usage;
    }
    for model in models {
        for name in model_calls(&model.sql, registry) {
            usage
                .entry(name)
                .or_default()
                .insert(model.config.name.clone());
        }
    }
    usage
}

/// The valid project functions `sql` calls (display names).
#[must_use]
pub fn model_calls(sql: &str, registry: &FunctionRegistry) -> BTreeSet<String> {
    let mut calls = BTreeSet::new();
    if registry.valid.is_empty() {
        return calls;
    }
    if let Some(stmts) = parse_query(sql) {
        let _ = stmts.visit(&mut FunctionCallVisitor(|f: &ast::Function| {
            if let Some(Resolved::Valid(sig)) = registry.resolve_call(&f.name) {
                calls.insert(sig.def.name.clone());
            }
        }));
    } else {
        // SQL the compiler's parser rejects (dialect syntax) still runs on
        // the warehouse. Fall back to a word match so its functions are still
        // created and its skip/reuse exclusion still holds: creating one
        // function too many is harmless, missing one is not.
        for sig in registry.valid.values() {
            if mentions_call(sql, &sig.def.name) {
                calls.insert(sig.def.name.clone());
            }
        }
    }
    calls
}

/// Whether `sql` contains `name` as a whole word followed by `(`
/// (case-insensitive, whitespace allowed before the paren).
fn mentions_call(sql: &str, name: &str) -> bool {
    let hay = sql.to_ascii_lowercase();
    let needle = name.to_ascii_lowercase();
    let is_word = |c: char| c.is_ascii_alphanumeric() || c == '_';
    let mut from = 0;
    while let Some(pos) = hay[from..].find(&needle) {
        let start = from + pos;
        let end = start + needle.len();
        let before_ok = hay[..start].chars().next_back().is_none_or(|c| !is_word(c));
        let after = hay[end..].trim_start();
        if before_ok && after.starts_with('(') {
            return true;
        }
        from = end;
    }
    false
}

/// `E051` for every call in every model that names an invalid function or
/// passes the wrong number of arguments. Both are certain from the SQL text
/// alone, so they are checked everywhere in the statement (WHERE, JOIN,
/// GROUP BY, …), not just in the projection.
#[must_use]
pub fn check_model_calls(
    models: &[rocky_core::models::Model],
    registry: &FunctionRegistry,
) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    if registry.is_empty() {
        return diagnostics;
    }
    for model in models {
        let Some(stmts) = parse_query(&model.sql) else {
            continue;
        };
        let model_name = model.config.name.as_str();
        let span = SourceSpan {
            file: model.file_path.display().to_string(),
            line: 1,
            col: 1,
        };
        let mut seen: HashSet<String> = HashSet::new();
        let _ = stmts.visit(&mut FunctionCallVisitor(|f: &ast::Function| {
            let message = match registry.resolve_call(&f.name) {
                Some(Resolved::Invalid(name)) => format!(
                    "calls function `{name}`, which failed validation (see its E051) and \
                     cannot be created"
                ),
                Some(Resolved::Valid(sig))
                    if f.name.0.len() == 1 && sig.def.config.target.schema.is_some() =>
                {
                    // Created as `schema.f`; an unqualified call resolves in
                    // the session's schema, which may not be that one.
                    let message = format!(
                        "calls `{}` unqualified, but it is created in schema `{}`; the \
                         warehouse resolves the call in the session's current schema",
                        sig.def.name,
                        sig.def.config.target.schema.as_deref().unwrap_or_default()
                    );
                    if seen.insert(message.clone()) {
                        diagnostics.push(
                            Diagnostic::warning(W051, model_name, message).with_span(span.clone()),
                        );
                    }
                    match positional_args(f) {
                        Some(args) if args.len() != sig.params.len() => format!(
                            "calls `{}` with {} argument(s) but it declares {}: {}",
                            sig.def.name,
                            args.len(),
                            sig.params.len(),
                            sig.signature()
                        ),
                        _ => return,
                    }
                }
                Some(Resolved::Valid(sig)) => match positional_args(f) {
                    Some(args) if args.len() != sig.params.len() => format!(
                        "calls `{}` with {} argument(s) but it declares {}: {}",
                        sig.def.name,
                        args.len(),
                        sig.params.len(),
                        sig.signature()
                    ),
                    _ => return,
                },
                None => return,
            };
            if seen.insert(message.clone()) {
                diagnostics
                    .push(Diagnostic::error(E051, model_name, message).with_span(span.clone()));
            }
        }));
    }
    diagnostics
}

// ---------------------------------------------------------------------------
// Typecheck integration
// ---------------------------------------------------------------------------

struct ActiveScope {
    registry: Arc<FunctionRegistry>,
    model: String,
    diagnostics: Vec<Diagnostic>,
    seen: HashSet<String>,
}

thread_local! {
    static ACTIVE: RefCell<Vec<ActiveScope>> = const { RefCell::new(Vec::new()) };
}

/// The function registry, installed for the duration of one model's
/// typecheck. See the module docs for why this is thread-scoped.
pub(crate) struct TypecheckScope {
    pushed: bool,
}

impl TypecheckScope {
    /// Install `registry` for `model` when its SQL may call a project
    /// function. A no-op (nothing installed) for every other model.
    pub(crate) fn enter(
        registry: &Arc<FunctionRegistry>,
        model: &str,
        sql: Option<&str>,
    ) -> TypecheckScope {
        let mentions_udf = sql.is_some_and(|sql| {
            let lower = sql.to_ascii_lowercase();
            registry
                .valid
                .keys()
                .any(|name| lower.contains(name.as_str()))
        });
        if !mentions_udf {
            return TypecheckScope { pushed: false };
        }
        ACTIVE.with(|stack| {
            stack.borrow_mut().push(ActiveScope {
                registry: Arc::clone(registry),
                model: model.to_string(),
                diagnostics: Vec::new(),
                seen: HashSet::new(),
            });
        });
        TypecheckScope { pushed: true }
    }

    /// Whether a registry is installed (the model may call a UDF), so the
    /// caller should run expression inference.
    pub(crate) fn is_active(&self) -> bool {
        self.pushed
    }

    /// Uninstall and return the argument diagnostics collected.
    pub(crate) fn finish(mut self) -> Vec<Diagnostic> {
        self.pop().map(|s| s.diagnostics).unwrap_or_default()
    }

    fn pop(&mut self) -> Option<ActiveScope> {
        if !self.pushed {
            return None;
        }
        self.pushed = false;
        ACTIVE.with(|stack| stack.borrow_mut().pop())
    }
}

impl Drop for TypecheckScope {
    fn drop(&mut self) {
        let _ = self.pop();
    }
}

/// Is `ty` one of the temporal types?
fn is_temporal(ty: &RockyType) -> bool {
    matches!(
        ty,
        RockyType::Date | RockyType::Timestamp | RockyType::TimestampNtz
    )
}

fn is_numeric(ty: &RockyType) -> bool {
    matches!(
        ty,
        RockyType::Int32
            | RockyType::Int64
            | RockyType::Float32
            | RockyType::Float64
            | RockyType::Decimal { .. }
    )
}

fn is_complex(ty: &RockyType) -> bool {
    matches!(
        ty,
        RockyType::Array(_) | RockyType::Map(_, _) | RockyType::Struct(_)
    )
}

/// Pairs no supported warehouse converts implicitly. Deliberately narrow:
/// strings, booleans and `VARIANT` are left out because some warehouse
/// coerces them, and a false refusal is worse than a miss.
fn certainly_incompatible(arg: &RockyType, param: &RockyType) -> bool {
    let scalar_mismatch = |a: &RockyType, b: &RockyType| {
        (is_temporal(a) && (is_numeric(b) || matches!(b, RockyType::Boolean)))
            || (matches!(a, RockyType::Binary)
                && (is_numeric(b) || is_temporal(b) || matches!(b, RockyType::Boolean)))
    };
    let complex_mismatch = |a: &RockyType, b: &RockyType| {
        is_complex(a)
            && !is_complex(b)
            && !matches!(
                b,
                RockyType::Variant | RockyType::Unknown | RockyType::String
            )
    };
    scalar_mismatch(arg, param)
        || scalar_mismatch(param, arg)
        || complex_mismatch(arg, param)
        || complex_mismatch(param, arg)
}

/// Type a call to a project function during expression inference.
///
/// Returns `None` when no scope is installed or `func` is not a valid project
/// function — the caller keeps its own fallback. Otherwise records argument
/// diagnostics on the active scope and returns the declared return type
/// (always nullable: a SQL function over a NULL argument can return NULL).
pub(crate) fn infer_active_call(
    func: &ast::Function,
    arg_type: &dyn Fn(&Expr) -> (RockyType, bool),
) -> Option<(RockyType, bool)> {
    // Take what we need and release the borrow: inferring an argument
    // re-enters this function for a nested call (`f(g(x))`), and a borrow
    // held across that recursion would panic.
    let registry = ACTIVE.with(|stack| stack.borrow().last().map(|s| Arc::clone(&s.registry)))?;
    let Some(Resolved::Valid(sig)) = registry.resolve_call(&func.name) else {
        return None;
    };
    let result = Some((sig.returns.clone(), true));
    // Arity is `check_model_calls`' certain E051; argument checks only
    // make sense once arguments line up with parameters.
    let Some(args) = positional_args(func) else {
        return result;
    };
    if args.len() != sig.params.len() {
        return result;
    }
    let mut found: Vec<(&'static str, String)> = Vec::new();
    let mut unverified = Vec::new();
    for (index, (arg, param)) in args.iter().zip(&sig.params).enumerate() {
        if matches!(arg, Expr::Value(v) if matches!(v.value, ast::Value::Null)) {
            continue; // NULL binds to any parameter type
        }
        let (arg_ty, _) = arg_type(arg);
        let position = index + 1;
        if arg_ty == RockyType::Unknown || param.data_type == RockyType::Unknown {
            unverified.push(format!("{position} (`{}`)", param.name));
            continue;
        }
        if crate::types::is_assignable(&arg_ty, &param.data_type) {
            continue;
        }
        found.push(if certainly_incompatible(&arg_ty, &param.data_type) {
            (
                E051,
                format!(
                    "argument {position} of `{}` is {arg_ty} but parameter `{}` is declared \
                     {}: {}",
                    sig.def.name,
                    param.name,
                    param.sql_type,
                    sig.signature()
                ),
            )
        } else {
            (
                W051,
                format!(
                    "argument {position} of `{}` is {arg_ty} but parameter `{}` is declared \
                     {}; the call relies on the warehouse converting it implicitly",
                    sig.def.name, param.name, param.sql_type
                ),
            )
        });
    }
    if !unverified.is_empty() {
        found.push((
            W051,
            format!(
                "cannot verify argument {} of `{}` against its declared type (Rocky could not \
                 infer one of the two types): {}",
                unverified.join(", "),
                sig.def.name,
                sig.signature()
            ),
        ));
    }
    ACTIVE.with(|stack| {
        if let Some(scope) = stack.borrow_mut().last_mut() {
            for (code, message) in found {
                push_scope_diagnostic(scope, code, message);
            }
        }
    });
    result
}

fn push_scope_diagnostic(scope: &mut ActiveScope, code: &str, message: String) {
    if !scope.seen.insert(format!("{code}:{message}")) {
        return;
    }
    let diagnostic = if code == E051 {
        Diagnostic::error(code, &scope.model, message)
    } else {
        Diagnostic::warning(code, &scope.model, message)
    };
    scope.diagnostics.push(diagnostic);
}

/// Give each output column that is a direct call to a project function the
/// function's declared return type.
///
/// Only the outermost `SELECT` list is read, and only items that are exactly
/// a call (optionally parenthesised) with an alias, so the column name is
/// certain. Columns that already have a concrete type are left alone. A call
/// with the wrong arity keeps `Unknown` (it is an `E051` anyway).
pub(crate) fn apply_direct_call_types(
    sql: &str,
    registry: &FunctionRegistry,
    typed_cols: &mut [TypedColumn],
) {
    if registry.valid.is_empty() {
        return;
    }
    let Some(stmts) = parse_query(sql) else {
        return;
    };
    let Some(Statement::Query(query)) = stmts.first() else {
        return;
    };
    let mut body = query.body.as_ref();
    while let SetExpr::Query(inner) = body {
        body = inner.body.as_ref();
    }
    let SetExpr::Select(select) = body else {
        return;
    };
    for item in &select.projection {
        let SelectItem::ExprWithAlias { expr, alias } = item else {
            continue;
        };
        let mut expr = expr;
        while let Expr::Nested(inner) = expr {
            expr = inner;
        }
        let Expr::Function(func) = expr else {
            continue;
        };
        let Some(Resolved::Valid(sig)) = registry.resolve_call(&func.name) else {
            continue;
        };
        if func.over.is_some()
            || positional_args(func).is_none_or(|args| args.len() != sig.params.len())
            || sig.returns == RockyType::Unknown
        {
            continue;
        }
        for col in typed_cols.iter_mut() {
            if col.name.eq_ignore_ascii_case(&alias.value) && col.data_type == RockyType::Unknown {
                col.data_type = sig.returns.clone();
                col.nullable = true;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_core::functions::{FunctionArgument, FunctionConfig, FunctionTarget};
    use std::path::PathBuf;

    fn def(name: &str, args: &[(&str, &str)], returns: &str, body: &str) -> FunctionDef {
        FunctionDef {
            name: name.to_string(),
            config: FunctionConfig {
                name: None,
                description: None,
                language: "sql".to_string(),
                returns: returns.to_string(),
                arguments: args
                    .iter()
                    .map(|(n, t)| FunctionArgument {
                        name: n.to_string(),
                        data_type: t.to_string(),
                    })
                    .collect(),
                deterministic: None,
                target: FunctionTarget::default(),
            },
            body: Some(body.to_string()),
            file_path: PathBuf::from(format!("functions/{name}.toml")),
        }
    }

    fn registry(defs: Vec<FunctionDef>) -> (FunctionRegistry, Vec<Diagnostic>) {
        build_registry(LoadedFunctions {
            functions: defs,
            errors: Vec::new(),
        })
    }

    #[test]
    fn declared_types_map_conservatively() {
        assert_eq!(udf_declared_type("DOUBLE"), RockyType::Float64);
        assert_eq!(udf_param_type("bigint"), RockyType::Int64);
        // A return type that means different things per warehouse stays
        // Unknown: Snowflake BIGINT is NUMBER(38,0); DuckDB TIMESTAMP is naive.
        assert_eq!(udf_declared_type("BIGINT"), RockyType::Unknown);
        assert_eq!(udf_declared_type("TIMESTAMP"), RockyType::Unknown);
        assert_eq!(udf_declared_type("FLOAT64"), RockyType::Float64);
        assert_eq!(
            udf_declared_type("DECIMAL(10, 2)"),
            RockyType::Decimal {
                precision: 10,
                scale: 2
            }
        );
        // Names whose width differs between warehouses stay Unknown.
        for ambiguous in ["FLOAT", "REAL", "INT", "INTEGER"] {
            assert_eq!(udf_declared_type(ambiguous), RockyType::Unknown);
        }
    }

    #[test]
    fn python_and_duplicates_are_e051_and_invalid() {
        let mut py = def("py_fn", &[], "DOUBLE", "1");
        py.config.language = "python".to_string();
        let (reg, diags) = registry(vec![
            py,
            def("dup", &[], "DOUBLE", "1"),
            def("DUP", &[], "DOUBLE", "2"),
            def("ok", &[("x", "BIGINT")], "DOUBLE", "x / 100.0"),
        ]);
        let e051: Vec<_> = diags.iter().filter(|d| &*d.code == E051).collect();
        assert!(
            e051.iter()
                .any(|d| d.model == "py_fn" && d.message.contains("Python UDFs are not supported"))
        );
        assert_eq!(
            e051.iter()
                .filter(|d| d.model.eq_ignore_ascii_case("dup"))
                .count(),
            2
        );
        assert!(reg.get("ok").is_some());
        assert!(reg.get("py_fn").is_none() && reg.get("dup").is_none());
    }

    #[test]
    fn creation_order_puts_callees_first_and_cycles_are_refused() {
        let (reg, diags) = registry(vec![
            def("outer_fn", &[("x", "BIGINT")], "DOUBLE", "inner_fn(x) * 2"),
            def("inner_fn", &[("x", "BIGINT")], "DOUBLE", "x / 100.0"),
        ]);
        assert!(diags.is_empty(), "{diags:?}");
        let order: Vec<_> = reg
            .creation_order(["outer_fn"])
            .iter()
            .map(|s| s.def.name.clone())
            .collect();
        assert_eq!(order, ["inner_fn", "outer_fn"]);

        let (reg, diags) = registry(vec![
            def("a", &[("x", "BIGINT")], "BIGINT", "b(x)"),
            def("b", &[("x", "BIGINT")], "BIGINT", "a(x)"),
        ]);
        assert!(reg.get("a").is_none() && reg.get("b").is_none());
        assert_eq!(
            diags
                .iter()
                .filter(|d| &*d.code == E051 && d.message.contains("call cycle"))
                .count(),
            2
        );
    }

    fn model(name: &str, sql: &str) -> rocky_core::models::Model {
        use rocky_core::models::{Model, ModelConfig, StrategyConfig, TargetConfig};
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

    #[test]
    fn arity_mismatch_and_invalid_function_calls_are_e051_on_the_model() {
        let mut bad = def("bad_fn", &[], "DOUBLE", "1");
        bad.config.language = "python".to_string();
        let (reg, _) = registry(vec![
            def(
                "cents_to_dollars",
                &[("cents", "BIGINT")],
                "DOUBLE",
                "cents / 100.0",
            ),
            bad,
        ]);
        let models = vec![
            model(
                "too_many",
                "SELECT cents_to_dollars(amount, 2) AS usd FROM raw.orders",
            ),
            model(
                "in_where",
                "SELECT order_id FROM raw.orders WHERE cents_to_dollars() > 1",
            ),
            model("calls_bad", "SELECT bad_fn() AS x FROM raw.orders"),
            model(
                "fine",
                "SELECT CENTS_TO_DOLLARS(amount) AS usd FROM raw.orders",
            ),
            model(
                "other_schema",
                "SELECT elsewhere.unrelated(amount) AS usd FROM raw.orders",
            ),
        ];
        let diags = check_model_calls(&models, &reg);
        let by_model = |m: &str| diags.iter().filter(|d| d.model == m).count();
        assert_eq!(by_model("too_many"), 1);
        assert_eq!(by_model("in_where"), 1);
        assert_eq!(by_model("calls_bad"), 1);
        assert_eq!(by_model("fine"), 0);
        assert_eq!(by_model("other_schema"), 0);
        assert!(diags.iter().all(|d| &*d.code == E051 && d.is_error()));

        let usage = function_usage(&models, &reg);
        assert_eq!(
            usage["cents_to_dollars"],
            ["fine", "in_where", "too_many"]
                .into_iter()
                .map(String::from)
                .collect()
        );
    }

    #[test]
    fn qualified_calls_must_match_a_declared_schema() {
        let mut d = def("f", &[("x", "BIGINT")], "BIGINT", "x");
        d.config.target.schema = Some("util".to_string());
        let (reg, _) = registry(vec![d]);
        assert_eq!(
            model_calls("SELECT util.f(a) AS b FROM t", &reg).len(),
            1,
            "matching schema resolves"
        );
        assert!(model_calls("SELECT other.f(a) AS b FROM t", &reg).is_empty());
        assert_eq!(model_calls("SELECT f(a) AS b FROM t", &reg).len(), 1);
        assert!(model_calls("SELECT cat.other.f(a) AS b FROM t", &reg).is_empty());

        // No declared target: any qualified call is someone else's function,
        // so a different arity there is not this project's E051.
        let (reg, _) = registry(vec![def("mask_email", &[("s", "VARCHAR")], "VARCHAR", "s")]);
        assert!(model_calls("SELECT governance.mask_email(s, '*') AS m FROM t", &reg).is_empty());
        let models = vec![model(
            "m",
            "SELECT governance.mask_email(s, '*') AS m FROM t",
        )];
        assert!(check_model_calls(&models, &reg).is_empty());
    }

    #[test]
    fn builtin_names_are_ignored_not_refused() {
        let (reg, diags) = registry(vec![def("round", &[("x", "DOUBLE")], "DOUBLE", "x")]);
        assert!(reg.get("round").is_none() && !reg.declares("round"));
        assert!(diags.iter().all(|d| !d.is_error()));
        assert!(
            diags
                .iter()
                .any(|d| &*d.code == W051 && d.message.contains("built-in"))
        );
        let models = vec![model("m", "SELECT ROUND(x, 2) AS r FROM t")];
        assert!(check_model_calls(&models, &reg).is_empty());
    }

    #[test]
    fn unparseable_model_sql_still_records_its_calls() {
        let (reg, _) = registry(vec![def("f", &[("x", "BIGINT")], "BIGINT", "x")]);
        let sql = "SELECT f (x) AS y, [i FOR i IN xs IF ] FROM t WHERE ((";
        assert!(parse_query(sql).is_none());
        assert_eq!(model_calls(sql, &reg).len(), 1);
        assert!(model_calls("SELECT xf(x), f_other(1) FROM ((", &reg).is_empty());
    }

    #[test]
    fn direct_calls_take_the_declared_return_type() {
        let (reg, _) = registry(vec![def(
            "cents_to_dollars",
            &[("cents", "BIGINT")],
            "DOUBLE",
            "cents / 100.0",
        )]);
        let mut cols = vec![
            TypedColumn {
                name: "usd".to_string(),
                data_type: RockyType::Unknown,
                nullable: true,
            },
            TypedColumn {
                name: "wrapped".to_string(),
                data_type: RockyType::Unknown,
                nullable: true,
            },
            TypedColumn {
                name: "arith".to_string(),
                data_type: RockyType::Unknown,
                nullable: true,
            },
        ];
        apply_direct_call_types(
            "SELECT cents_to_dollars(amount) AS usd, (cents_to_dollars(1)) AS wrapped, \
             cents_to_dollars(amount) + 1 AS arith FROM raw.orders",
            &reg,
            &mut cols,
        );
        assert_eq!(cols[0].data_type, RockyType::Float64);
        assert_eq!(cols[1].data_type, RockyType::Float64);
        // An expression over a call is not a direct call: arithmetic result
        // types are dialect-dependent, so it stays Unknown.
        assert_eq!(cols[2].data_type, RockyType::Unknown);
    }

    #[test]
    fn hover_shows_the_signature_and_description() {
        let mut d = def(
            "cents_to_dollars",
            &[("cents", "BIGINT")],
            "DOUBLE",
            "cents / 100.0",
        );
        d.config.description = Some("Integer cents to dollars".to_string());
        let (reg, _) = registry(vec![d]);
        assert_eq!(
            reg.get("CENTS_TO_DOLLARS").unwrap().hover_markdown(),
            "**Function:** `cents_to_dollars(cents BIGINT) RETURNS DOUBLE`\n\n\
             > Integer cents to dollars"
        );
    }

    #[test]
    fn incompatibility_is_narrow() {
        assert!(certainly_incompatible(&RockyType::Date, &RockyType::Int64));
        assert!(certainly_incompatible(
            &RockyType::Float64,
            &RockyType::Timestamp
        ));
        assert!(!certainly_incompatible(
            &RockyType::String,
            &RockyType::Int64
        ));
        assert!(!certainly_incompatible(
            &RockyType::Int64,
            &RockyType::String
        ));
        assert!(!certainly_incompatible(
            &RockyType::Boolean,
            &RockyType::Int64
        ));
        assert!(!certainly_incompatible(
            &RockyType::Variant,
            &RockyType::Date
        ));
    }
}
