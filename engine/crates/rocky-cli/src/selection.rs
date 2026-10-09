//! CLI glue for dbt-style node selection (`--select` / `--exclude`).
//!
//! The selector grammar and graph resolution live in
//! [`rocky_core::selector`], which is pure. This module builds the selector
//! graph from a loaded project, computes `state:` sets from git (reusing the
//! `rocky ci-diff` change detection), and reports dbt-style warnings.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rocky_compiler::project::Project;
use rocky_core::selector::{self, SelectorGraph, SelectorNode, StateSets};
use tracing::warn;

/// Default git ref `state:` selectors diff against — the same default
/// `rocky ci-diff` uses.
pub const DEFAULT_STATE_REF: &str = "main";

/// The raw `--select` / `--exclude` / `--state-ref` flag values.
#[derive(Debug, Clone, Default)]
pub struct SelectionArgs {
    /// `--select` / `-s` values. Empty = every model.
    pub select: Vec<String>,
    /// `--exclude` values, subtracted after graph operators.
    pub exclude: Vec<String>,
    /// `--state-ref`: base git ref for `state:modified` / `state:new`.
    /// `None` = [`DEFAULT_STATE_REF`].
    pub state_ref: Option<String>,
    /// `--state-working-tree`: `state:` selectors compare the working tree
    /// (staged, unstaged and untracked files) with the merge base of
    /// `state_ref`, as `rocky ci-diff --working-tree` does. Default: the
    /// committed `state_ref...HEAD` diff only.
    pub state_working_tree: bool,
    /// A legacy `--model <name>` folded in by [`Self::with_model`]. It must
    /// name a real model, as `--model` alone requires.
    pub required_model: Option<String>,
    /// `--var` values. The selector graph substitutes `@var(...)` markers
    /// (supplied value, else inline default, else `NULL`) before it parses
    /// model SQL, exactly as `rocky compile` does. Empty = inline defaults.
    pub run_vars: rocky_core::run_vars::RunVars,
}

/// Build the dependency graph's [`Project`] from loaded models, after
/// substituting `@var(...)` markers the way `rocky compile` does. The raw
/// SQL does not parse (`@var(k, 2)` is not SQL), so a graph built without
/// this step fails for any project that uses `@var` (#2315).
///
/// Substitution diagnostics (a required var with no value) are not errors
/// here: the graph only needs the SQL to parse, and `compile`/`run` report
/// E028. They are added to the error only when the graph then fails to build.
pub fn project_from_models(
    mut models: Vec<rocky_core::models::Model>,
    run_vars: &rocky_core::run_vars::RunVars,
) -> Result<Project> {
    let diagnostics =
        rocky_compiler::compile::substitute_run_vars_into_models(&mut models, run_vars);
    Project::from_models(models).map_err(|e| {
        // A required `@var` with no value became `NULL`, which can break the
        // parse (`FROM NULL`). Name the real cause rather than only the parser.
        let causes: Vec<String> = diagnostics
            .iter()
            .map(|d| format!("{}: {} ({})", d.code, d.message, d.model))
            .collect();
        if causes.is_empty() {
            anyhow::anyhow!("{e}")
        } else {
            anyhow::anyhow!("{e}\n  likely cause:\n  {}", causes.join("\n  "))
        }
    })
}

impl SelectionArgs {
    /// Whether any selection flag was passed.
    pub fn is_active(&self) -> bool {
        !self.select.is_empty() || !self.exclude.is_empty()
    }

    /// Fold the legacy `--model <name>` flag in. `--model` selects one model
    /// by exact name; it cannot be combined with `--select`.
    pub fn with_model(mut self, model: Option<&str>) -> Result<Self> {
        if let Some(name) = model {
            anyhow::ensure!(
                self.select.is_empty(),
                "--model and --select cannot be combined; use --select {name} (plus any \
                 other selectors) instead"
            );
            // `name:` keeps it an exact-name match even when the name looks
            // like a path or carries glob characters.
            self.select.push(format!("name:{name}"));
            self.required_model = Some(name.to_string());
        }
        Ok(self)
    }
}

/// Where `state:` selectors find the project, the state store, and the
/// schema cache.
#[derive(Debug, Clone, Copy)]
pub struct StateContext<'a> {
    pub config_path: &'a Path,
    pub state_path: &'a Path,
    pub cache_ttl_override: Option<u64>,
}

/// Build the selector graph from a resolved project.
///
/// Dependencies come from the compiler's resolved DAG (sidecar `depends_on`
/// plus SQL-inferred references). Sources are the external relations each
/// model's SQL reads, plus any sidecar `[[sources]]`.
pub fn build_graph(project: &Project, models_dir: &Path) -> SelectorGraph {
    let model_names: BTreeSet<String> = project
        .models
        .iter()
        .map(|m| m.config.name.to_ascii_lowercase())
        .collect();
    let model_targets: BTreeSet<String> = project
        .models
        .iter()
        .flat_map(|m| {
            let t = &m.config.target;
            [
                format!("{}.{}", t.schema, t.table).to_ascii_lowercase(),
                format!("{}.{}.{}", t.catalog, t.schema, t.table).to_ascii_lowercase(),
            ]
        })
        .collect();
    let models_root_name = models_dir.file_name().map(PathBuf::from);

    let nodes = project.models.iter().map(|model| {
        let depends_on = project
            .dag_nodes
            .iter()
            .find(|n| n.name == model.config.name)
            .map(|n| n.depends_on.clone())
            .unwrap_or_else(|| model.config.depends_on.clone());

        let mut paths = Vec::new();
        let to_slash = |p: &Path| p.to_string_lossy().replace('\\', "/");
        if let Ok(in_models) = model.file_path.strip_prefix(models_dir) {
            if let Some(root) = &models_root_name {
                paths.push(to_slash(&root.join(in_models)));
            }
            paths.push(to_slash(in_models));
        }
        let raw = to_slash(&model.file_path);
        paths.push(raw.trim_start_matches("./").to_string());

        let mut sources: Vec<String> = rocky_sql::lineage::referenced_tables(&model.sql)
            .unwrap_or_default()
            .into_iter()
            .filter(|t| !model_names.contains(t) && !model_targets.contains(t))
            .collect();
        sources.extend(model.config.sources.iter().map(|s| {
            [s.catalog.as_str(), s.schema.as_str(), s.table.as_str()]
                .iter()
                .filter(|p| !p.is_empty())
                .copied()
                .collect::<Vec<_>>()
                .join(".")
                .to_ascii_lowercase()
        }));
        sources.sort();
        sources.dedup();

        let target = &model.config.target;
        SelectorNode {
            name: model.config.name.clone(),
            depends_on,
            paths,
            tags: model.config.tags.clone(),
            materialization: selector::strategy_kind(&model.config.strategy).to_string(),
            catalog: target.catalog.clone(),
            schema: target.schema.clone(),
            table: target.table.clone(),
            sources,
        }
    });
    SelectorGraph::new(nodes)
}

/// Compute `state:` sets by diffing `state_ref...HEAD` exactly as
/// `rocky ci-diff` does (committed changes only), or the working tree when
/// `working_tree` (`--state-working-tree`, as `rocky ci-diff --working-tree`).
///
/// `ci-diff` classifies only files directly inside the models directory, but
/// models also load one directory down (`models/staging/*.sql`). So the git
/// change list is also mapped onto each model's own files: its source, its
/// `.toml` sidecar, its `.contract.toml`, and any `_defaults.toml` in its
/// directory or the models root. Both answers are unioned, so a change is
/// never missed.
fn compute_state(
    state_ref: &str,
    working_tree: bool,
    project: &Project,
    models_dir: &Path,
    ctx: &StateContext<'_>,
) -> Result<StateSets> {
    let mut state = compute_ci_diff_state(state_ref, working_tree, models_dir, ctx)?;
    let changed = if working_tree {
        crate::commands::ci_diff::changed_paths_worktree(state_ref)?
    } else {
        crate::commands::ci_diff::changed_paths(state_ref)?
    };
    mark_changed_models(&changed, project, models_dir, &mut state);
    if !working_tree {
        warn_uncommitted(project, models_dir, &state);
    }
    Ok(state)
}

/// Warn about models whose files have uncommitted edits that the committed
/// `state:` diff leaves out. Best effort: a git failure here only skips the
/// warning.
fn warn_uncommitted(project: &Project, models_dir: &Path, state: &StateSets) {
    let Ok(uncommitted) = crate::commands::ci_diff::uncommitted_paths() else {
        return;
    };
    let mut touched = StateSets::default();
    mark_changed_models(&uncommitted, project, models_dir, &mut touched);
    let excluded: BTreeSet<&String> = touched
        .modified
        .iter()
        .chain(&touched.new)
        .filter(|m| !state.modified.contains(*m) && !state.new.contains(*m))
        .collect();
    if let Some(message) = uncommitted_warning(&excluded) {
        warn!("{message}");
    }
}

/// The warning text for models with uncommitted edits that `state:` left out.
fn uncommitted_warning(excluded: &BTreeSet<&String>) -> Option<String> {
    if excluded.is_empty() {
        return None;
    }
    Some(format!(
        "state: selectors compare committed changes only (<ref>...HEAD); {} model(s) with \
         uncommitted edits are not selected by them: {}. Commit the edits, or pass \
         --state-working-tree to compare the working tree",
        excluded.len(),
        excluded
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    ))
}

/// Map git's changed paths (repo-root-relative) onto each model's own files —
/// its source, `.toml` sidecar, `.contract.toml`, and any `_defaults.toml`
/// in its directory or the models root — and record the model as new or
/// modified in `state`.
fn mark_changed_models(
    changed: &[(char, String)],
    project: &Project,
    models_dir: &Path,
    state: &mut StateSets,
) {
    if changed.is_empty() {
        return;
    }
    let Some(repo_root) = git_toplevel() else {
        return;
    };
    let changed: Vec<(char, PathBuf)> = changed
        .iter()
        .map(|(status, path)| (*status, repo_root.join(path)))
        .collect();
    let models_root = std::fs::canonicalize(models_dir).ok();
    for model in &project.models {
        let Ok(source) = std::fs::canonicalize(&model.file_path) else {
            continue;
        };
        let dir = source.parent().map(Path::to_path_buf).unwrap_or_default();
        let stem = source
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or_default()
            .to_string();
        let owned = [
            source.clone(),
            dir.join(format!("{stem}.toml")),
            dir.join(format!("{stem}.contract.toml")),
            dir.join("_defaults.toml"),
        ];
        let root_defaults = models_root.as_ref().map(|r| r.join("_defaults.toml"));
        for (status, path) in &changed {
            if *path == source && *status == 'A' {
                state.new.insert(model.config.name.clone());
            } else if owned.contains(path) || root_defaults.as_ref() == Some(path) {
                state.modified.insert(model.config.name.clone());
            }
        }
    }
}

/// The repository root, canonicalized, or `None` outside a git work tree.
fn git_toplevel() -> Option<PathBuf> {
    let out = std::process::Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let root = String::from_utf8(out.stdout).ok()?;
    std::fs::canonicalize(root.trim()).ok()
}

/// The `rocky ci-diff` classification of `state_ref...HEAD`.
fn compute_ci_diff_state(
    state_ref: &str,
    working_tree: bool,
    models_dir: &Path,
    ctx: &StateContext<'_>,
) -> Result<StateSets> {
    let data = crate::commands::ci_diff::compute_ci_diff(
        ctx.config_path,
        ctx.state_path,
        state_ref,
        models_dir,
        ctx.cache_ttl_override,
        // Committed `HEAD`, the same snapshot `changed_paths` diffs, so the
        // file selection and the compiled models agree (G7). With
        // `--state-working-tree`, the working tree for both.
        crate::commands::ci_diff_mode(working_tree),
    )
    .with_context(|| format!("failed to compute `state:` selection against '{state_ref}'"))?;
    let mut state = StateSets::default();
    for r in data.results {
        match r.status {
            rocky_core::ci_diff::ModelDiffStatus::Modified => {
                state.modified.insert(r.model_name);
            }
            rocky_core::ci_diff::ModelDiffStatus::Added => {
                state.new.insert(r.model_name);
            }
            rocky_core::ci_diff::ModelDiffStatus::Removed
            | rocky_core::ci_diff::ModelDiffStatus::Unchanged => {}
        }
    }
    Ok(state)
}

/// Read the `[selectors]` table from `rocky.toml`, only when a selector value
/// uses the `selector:` method. A project without a config file has none.
fn load_saved_selectors(
    config_path: &Path,
    args: &SelectionArgs,
) -> Result<selector::SavedSelectors> {
    let uses_saved = args
        .select
        .iter()
        .chain(&args.exclude)
        .any(|value| value.contains("selector:"));
    if !uses_saved {
        return Ok(selector::SavedSelectors::new());
    }
    let raw = match rocky_core::config::parse_rocky_config_raw(config_path) {
        Ok(raw) => raw,
        Err(rocky_core::config::ConfigError::FileNotFound { .. }) => {
            return Ok(selector::SavedSelectors::new());
        }
        Err(e) => {
            return Err(anyhow::Error::new(e).context(format!(
                "failed to read [selectors] from {}",
                config_path.display()
            )));
        }
    };
    let Some(table) = raw.get("selectors") else {
        return Ok(selector::SavedSelectors::new());
    };
    let table = table
        .as_table()
        .context("[selectors] in rocky.toml must be a table of name = \"expression\"")?;
    table
        .iter()
        .map(|(name, value)| {
            let expression = value.as_str().with_context(|| {
                format!("[selectors] {name} in rocky.toml must be a string expression")
            })?;
            Ok((name.clone(), expression.to_string()))
        })
        .collect()
}

/// Resolve the selection against an already-loaded project. Logs a warning
/// for each criterion that matches nothing, and a dbt-style "Nothing to do"
/// warning when the final selection is empty.
pub fn resolve(
    args: &SelectionArgs,
    project: &Project,
    models_dir: &Path,
    ctx: &StateContext<'_>,
) -> Result<BTreeSet<String>> {
    let saved = load_saved_selectors(ctx.config_path, args)?;
    let select = selector::parse_with(&args.select, &saved)?;
    let exclude = selector::parse_with(&args.exclude, &saved)?;
    let state = if select.uses_state() || exclude.uses_state() {
        let state_ref = args.state_ref.as_deref().unwrap_or(DEFAULT_STATE_REF);
        Some(compute_state(
            state_ref,
            args.state_working_tree,
            project,
            models_dir,
            ctx,
        )?)
    } else {
        None
    };
    let graph = build_graph(project, models_dir);
    if let Some(name) = &args.required_model {
        anyhow::ensure!(
            graph.contains(name),
            "model '{name}' not found (no transformation model with that name)"
        );
    }
    let selection = selector::select(&graph, &select, &exclude, state.as_ref())?;
    // A term that names one model, tag, path, file or source that does not
    // exist is a typo, not an empty selection: refuse it rather than run
    // nothing and report success. Globs and computed sets (`state:`,
    // `config.`) that match nothing stay a warning.
    if !selection.unmatched_named.is_empty() {
        anyhow::bail!(
            "--select: selector term(s) that match nothing in this project: {}. Each names a \
             model, tag, path, file or source that does not exist. Check the spelling; \
             `rocky list models` shows the model names",
            selection
                .unmatched_named
                .iter()
                .map(|t| format!("'{t}'"))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    for w in &selection.warnings {
        warn!("{w}");
    }
    if selection.models.is_empty() {
        warn!("Nothing to do. Try checking your model configs and model specification args");
    }
    Ok(selection.models)
}

/// Load the models under `models_dir` (optionally narrowed to `models_glob`),
/// resolve their DAG, and resolve the selection.
pub fn resolve_in_dir(
    args: &SelectionArgs,
    models_dir: &Path,
    models_glob: Option<&str>,
    ctx: &StateContext<'_>,
) -> Result<BTreeSet<String>> {
    let project = load_project(models_dir, models_glob, &args.run_vars)?;
    resolve(args, &project, models_dir, ctx)
}

/// [`resolve_in_dir`] for commands that build models (`rocky run`): drop
/// every `ephemeral` model from the selection. An ephemeral model is never
/// materialized — its consumers inline it — so selecting one (for example
/// through `state:modified`) builds nothing. A model named by the literal
/// `--model` flag (`args.required_model`) stays, so the runner refuses it
/// with E038 rather than silently doing nothing.
pub fn resolve_buildable_in_dir(
    args: &SelectionArgs,
    models_dir: &Path,
    models_glob: Option<&str>,
    ctx: &StateContext<'_>,
) -> Result<BTreeSet<String>> {
    let project = load_project(models_dir, models_glob, &args.run_vars)?;
    let mut selected = resolve(args, &project, models_dir, ctx)?;
    let had_models = !selected.is_empty();
    selected.retain(|name| {
        args.required_model.as_deref() == Some(name.as_str())
            || !project.model(name).is_some_and(|m| {
                matches!(
                    m.config.strategy,
                    rocky_core::models::StrategyConfig::Ephemeral
                )
            })
    });
    if had_models && selected.is_empty() {
        warn!(
            "Nothing to do: the selection matched only ephemeral models, which are inlined \
             into their consumers and never built on their own"
        );
    }
    Ok(selected)
}

fn load_project(
    models_dir: &Path,
    models_glob: Option<&str>,
    run_vars: &rocky_core::run_vars::RunVars,
) -> Result<Project> {
    let models = match models_glob {
        Some(glob) => crate::models_loader::load_project_models_matching(models_dir, glob, None)?,
        None => crate::models_loader::load_project_models(models_dir, None)?,
    };
    project_from_models(models, run_vars).context("failed to resolve the model graph for --select")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two models; `m` reads `base` and carries `@var(k, 2)` (#2315).
    fn write_var_project(dir: &Path) {
        std::fs::write(dir.join("base.sql"), "SELECT 1 AS id, 10 AS x\n").unwrap();
        std::fs::write(
            dir.join("m.sql"),
            "SELECT id, x * @var(k, 2) AS v FROM base\n",
        )
        .unwrap();
        for name in ["base", "m"] {
            std::fs::write(
                dir.join(format!("{name}.toml")),
                format!(
                    "name = \"{name}\"\n[strategy]\ntype = \"full_refresh\"\n\
                     [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"{name}\"\n"
                ),
            )
            .unwrap();
        }
    }

    fn ctx(dir: &Path) -> StateContext<'_> {
        StateContext {
            config_path: dir,
            state_path: dir,
            cache_ttl_override: None,
        }
    }

    #[test]
    fn graph_selection_works_when_a_model_uses_var_with_a_default() {
        let dir = tempfile::tempdir().unwrap();
        write_var_project(dir.path());
        let args = SelectionArgs {
            select: vec!["base+".into()],
            ..Default::default()
        };
        let got = resolve_in_dir(&args, dir.path(), None, &ctx(dir.path()))
            .expect("@var(k, 2) must not break the selection graph");
        assert_eq!(
            got.into_iter().collect::<Vec<_>>(),
            vec!["base".to_string(), "m".to_string()]
        );
    }

    /// A supplied `--var` that names a table changes the graph edge. This
    /// tells "run_vars honored" from "run_vars ignored", which a test whose
    /// result does not depend on the value cannot.
    #[test]
    fn graph_selection_follows_a_supplied_var_that_names_a_table() {
        let dir = tempfile::tempdir().unwrap();
        for (name, sql) in [
            ("base", "SELECT 1 AS id"),
            ("other", "SELECT 2 AS id"),
            ("m", "SELECT id FROM @var(src, base)"),
        ] {
            std::fs::write(dir.path().join(format!("{name}.sql")), format!("{sql}\n")).unwrap();
            std::fs::write(
                dir.path().join(format!("{name}.toml")),
                format!(
                    "name = \"{name}\"\n[strategy]\ntype = \"full_refresh\"\n\
                     [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"{name}\"\n"
                ),
            )
            .unwrap();
        }
        let select = |vars: rocky_core::run_vars::RunVars| {
            let args = SelectionArgs {
                select: vec!["+m".into()],
                run_vars: vars,
                ..Default::default()
            };
            resolve_in_dir(&args, dir.path(), None, &ctx(dir.path()))
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>()
        };
        assert_eq!(select(Default::default()), vec!["base", "m"]);
        let mut vars = rocky_core::run_vars::RunVars::new();
        vars.insert("src", "other");
        assert_eq!(select(vars), vec!["m", "other"]);
    }

    /// A required `@var` with no value becomes `NULL`; if that breaks the
    /// parse, the error must name the variable, not only the parser (#2315).
    #[test]
    fn graph_failure_from_a_missing_required_var_names_the_variable() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("m.sql"),
            "SELECT id FROM @var(src) x WHERE (\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("m.toml"),
            "name = \"m\"\n[strategy]\ntype = \"full_refresh\"\n\
             [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"m\"\n",
        )
        .unwrap();
        let args = SelectionArgs {
            select: vec!["m+".into()],
            ..Default::default()
        };
        let outcome = resolve_in_dir(&args, dir.path(), None, &ctx(dir.path()));
        {
            let e = outcome.expect_err("FROM NULL does not resolve");
            let text = format!("{e:#}");
            assert!(text.contains("E028") && text.contains("src"), "{text}");
        }
    }

    #[test]
    fn graph_selection_substitutes_supplied_vars_before_parsing() {
        let dir = tempfile::tempdir().unwrap();
        write_var_project(dir.path());
        let mut run_vars = rocky_core::run_vars::RunVars::new();
        run_vars.insert("k", "3");
        let args = SelectionArgs {
            select: vec!["m".into()],
            run_vars,
            ..Default::default()
        };
        let got = resolve_buildable_in_dir(&args, dir.path(), None, &ctx(dir.path())).unwrap();
        assert_eq!(got.into_iter().collect::<Vec<_>>(), vec!["m".to_string()]);
    }

    /// `selector:<name>` reads the `[selectors]` table of the config file.
    #[test]
    fn saved_selector_expands_from_rocky_toml() {
        let dir = tempfile::tempdir().unwrap();
        write_var_project(dir.path());
        let config = dir.path().join("rocky.toml");
        std::fs::write(
            &config,
            "[selectors]\ndownstream = \"base+\"\nonly_m = \"selector:downstream,m\"\n",
        )
        .unwrap();
        let ctx = StateContext {
            config_path: &config,
            state_path: dir.path(),
            cache_ttl_override: None,
        };
        let run = |select: &str| {
            let args = SelectionArgs {
                select: vec![select.into()],
                ..Default::default()
            };
            resolve_in_dir(&args, dir.path(), None, &ctx)
                .map(|s| s.into_iter().collect::<Vec<_>>())
        };
        assert_eq!(run("selector:downstream").unwrap(), vec!["base", "m"]);
        assert_eq!(run("selector:only_m").unwrap(), vec!["m"]);
        assert_eq!(run("+selector:only_m").unwrap(), vec!["base", "m"]);
        let err = run("selector:missing").unwrap_err();
        assert!(format!("{err:#}").contains("unknown saved selector"), "{err:#}");
    }

    #[test]
    fn saved_selector_without_a_config_file_is_an_unknown_selector() {
        let dir = tempfile::tempdir().unwrap();
        write_var_project(dir.path());
        let args = SelectionArgs {
            select: vec!["selector:nightly".into()],
            ..Default::default()
        };
        let missing = dir.path().join("absent.toml");
        let ctx = StateContext {
            config_path: &missing,
            state_path: dir.path(),
            cache_ttl_override: None,
        };
        let err = resolve_in_dir(&args, dir.path(), None, &ctx).unwrap_err();
        assert!(format!("{err:#}").contains("[selectors]"), "{err:#}");
    }
}
