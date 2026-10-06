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

/// Resolve the selection against an already-loaded project. Logs a warning
/// for each criterion that matches nothing, and a dbt-style "Nothing to do"
/// warning when the final selection is empty.
pub fn resolve(
    args: &SelectionArgs,
    project: &Project,
    models_dir: &Path,
    ctx: &StateContext<'_>,
) -> Result<BTreeSet<String>> {
    let select = selector::parse(&args.select)?;
    let exclude = selector::parse(&args.exclude)?;
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
    let project = load_project(models_dir, models_glob)?;
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
    let project = load_project(models_dir, models_glob)?;
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

fn load_project(models_dir: &Path, models_glob: Option<&str>) -> Result<Project> {
    let models = match models_glob {
        Some(glob) => crate::models_loader::load_project_models_matching(models_dir, glob, None)?,
        None => crate::models_loader::load_project_models(models_dir, None)?,
    };
    Project::from_models(models)
        .map_err(|e| anyhow::anyhow!("{e}"))
        .context("failed to resolve the model graph for --select")
}
