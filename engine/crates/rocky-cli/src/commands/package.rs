//! `rocky package` — vendor dbt Hub packages as Rocky models.
//!
//! ```text
//! rocky package add fivetran/stripe@>=1.0.0,<2.0.0 --vars stripe_schema=raw_stripe
//! rocky package update [stripe]
//! rocky package list
//! rocky package remove stripe
//! ```
//!
//! `add` and `update` run dbt once, in a throwaway project under a temp
//! directory: `dbt deps`, then `dbt compile --full-refresh`. Nothing is
//! written to the warehouse by default. With the opt-in `--build-empty`, dbt
//! first runs `dbt run --empty --full-refresh` so compile-time introspection
//! macros see real columns; without it, a package whose compiled SQL shows
//! such a macro found nothing (all-NULL columns, a placeholder `*`) is
//! refused with E055 rather than vendored wrong. The mode is recorded in the
//! lockfile and replayed by `update`. The compiled
//! manifest is imported for that one package and written to
//! `models/packages/<package>/`, with `rocky-packages.lock` recording what was
//! written. From then on Rocky owns the models; dbt is not needed to compile,
//! test or run them.
//!
//! The pure logic (selection, namespacing, test mapping, lockfile, three-way
//! update plan) lives in `rocky_compiler::import::dbt_package`; this file is
//! the process and filesystem shell around it.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, anyhow, bail};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use rocky_compiler::import::dbt_manifest;
use rocky_compiler::import::dbt_package::{
    self, BuildMode, INCOMING_SUFFIX, LOCKFILE_NAME, LockedPackage, PackageImport, PackagesLock,
};
use rocky_core::config::{AdapterConfig, RockyConfig};
use rocky_core::models::TargetConfig;

use crate::output::print_json;

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Name of the throwaway dbt project and its profile.
const BUILD_PROJECT: &str = "rocky_package_build";

/// Default dbt target schema for the throwaway build. `dbt run --empty`
/// creates empty relations under it (and `<it>_<custom schema>`); Rocky never
/// builds there.
const DEFAULT_DBT_SCHEMA: &str = "rocky_package_build";

// ---------------------------------------------------------------------------
// JSON output
// ---------------------------------------------------------------------------

/// A finding from `rocky package`: `E055` (refused) or `W055` (vendored, but
/// something needs a look).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PackageDiagnostic {
    pub code: String,
    /// `error` or `warning`.
    pub severity: String,
    pub message: String,
    /// The vendored file the finding is about, when there is one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// The model the finding is about, when there is one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

/// A raw source table a vendored package reads.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PackageSourceOutput {
    /// `<source_name>.<table>` as the package declares it.
    pub name: String,
    pub catalog: String,
    pub schema: String,
    pub table: String,
}

/// A dbt test on a package model that was not mapped to Rocky.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PackageDroppedTest {
    pub test: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attached_to: Option<String>,
    pub reason: String,
}

/// A package model that could not be vendored.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PackageFailedModel {
    pub name: String,
    pub reason: String,
}

/// What `add` / `update` did for one package.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PackageVendorReport {
    /// dbt project name of the package; its directory under `models/packages/`.
    pub name: String,
    /// dbt Hub name (`fivetran/stripe`).
    pub hub: String,
    /// Version `dbt deps` resolved.
    pub version: String,
    /// Version requirement the package was added with; empty means latest.
    pub version_spec: String,
    pub dbt_version: String,
    /// Rocky adapter type the package was compiled against.
    pub adapter: String,
    /// Schema the vendored models build into.
    pub target_schema: String,
    pub vars_hash: String,
    /// How the SQL was compiled: `compile-only` (`dbt compile` alone; nothing
    /// written to the warehouse), `build-empty` (`dbt run --empty` first) or
    /// `compiled` (`--compiled <dir>`).
    pub mode: String,
    /// Dependency packages whose models were vendored alongside.
    pub includes: Vec<String>,
    /// Every vendored model, sorted.
    pub models: Vec<String>,
    /// Models new in this run (all of them on `add`).
    pub models_added: Vec<String>,
    /// Models the previous version had and this one does not.
    pub models_removed: Vec<String>,
    pub sources: Vec<PackageSourceOutput>,
    /// dbt generic tests mapped to Rocky `[[tests]]`.
    pub tests_mapped: usize,
    pub tests_dropped: Vec<PackageDroppedTest>,
    /// dbt `incremental` models that did not stay incremental.
    pub incremental_fallbacks: Vec<String>,
    pub failed_models: Vec<PackageFailedModel>,
    /// Files written in place.
    pub files_written: Vec<String>,
    /// Files you edited that changed upstream: the new version is at
    /// `<path>.incoming`.
    pub files_incoming: Vec<String>,
    /// Vendored files deleted because upstream removed them.
    pub files_deleted: Vec<String>,
    /// Files you edited that were left as they are.
    pub files_kept_edited: Vec<String>,
    pub files_unchanged: usize,
}

/// JSON output of `rocky package add`.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct PackageAddOutput {
    pub version: String,
    pub command: String,
    /// Path of `rocky-packages.lock`.
    pub lockfile: String,
    pub package: PackageVendorReport,
    pub diagnostics: Vec<PackageDiagnostic>,
}

/// JSON output of `rocky package update`.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct PackageUpdateOutput {
    pub version: String,
    pub command: String,
    pub lockfile: String,
    pub packages: Vec<PackageVendorReport>,
    pub diagnostics: Vec<PackageDiagnostic>,
}

/// One vendored package, as `rocky package list` reports it.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PackageListEntry {
    pub name: String,
    pub hub: String,
    pub version: String,
    pub version_spec: String,
    pub dbt_version: String,
    pub adapter: String,
    pub target_schema: String,
    /// `compile-only`, `build-empty` or `compiled`; replayed by `update`.
    pub mode: String,
    pub compiled_at: String,
    pub includes: Vec<String>,
    pub models: Vec<String>,
    pub sources: Vec<PackageSourceOutput>,
    /// Vendored files whose content differs from what Rocky wrote.
    pub files_modified: Vec<String>,
    /// Vendored files that are gone.
    pub files_missing: Vec<String>,
    /// Pending `<path>.incoming` files from an update.
    pub files_incoming: Vec<String>,
}

/// JSON output of `rocky package list`.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct PackageListOutput {
    pub version: String,
    pub command: String,
    pub lockfile: String,
    pub packages: Vec<PackageListEntry>,
    pub count: usize,
}

/// JSON output of `rocky package remove`.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct PackageRemoveOutput {
    pub version: String,
    pub command: String,
    pub lockfile: String,
    pub name: String,
    pub files_deleted: Vec<String>,
}

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

/// Flags shared by `add` and `update`.
#[derive(Debug, Clone, Default)]
pub struct PackageBuildOptions {
    /// `--vars key=value`, merged over any vars recorded in the lockfile.
    pub vars: Vec<String>,
    /// `--adapter <name>`: the `rocky.toml` adapter dbt compiles against.
    pub adapter: Option<String>,
    /// `--target-schema`: where the vendored models build.
    pub target_schema: Option<String>,
    /// `--dbt <path>`; else `dbt` on `PATH`.
    pub dbt: Option<PathBuf>,
    /// `--compiled <dir>`: import an already-compiled dbt project instead of
    /// running dbt (`<dir>/target/manifest.json` + `<dir>/package-lock.yml`).
    pub compiled: Option<PathBuf>,
    /// `--build-empty[=BOOL]`: run `dbt run --empty` before compiling. Unset
    /// means compile-only on `add` and the recorded mode on `update`.
    pub build_empty: Option<bool>,
}

/// Pick the build mode from the flags and, on `update`, the lock entry.
fn resolve_mode(
    opts: &PackageBuildOptions,
    previous: Option<&LockedPackage>,
    hub: &str,
) -> Result<BuildMode> {
    if opts.compiled.is_some() {
        if opts.build_empty == Some(true) {
            bail!(e055(
                "--build-empty and --compiled conflict: --compiled imports SQL dbt already \
                 compiled elsewhere"
            ));
        }
        return Ok(BuildMode::Compiled);
    }
    Ok(match (opts.build_empty, previous.map(|p| p.mode)) {
        (Some(true), _) => BuildMode::BuildEmpty,
        (Some(false), _) => BuildMode::CompileOnly,
        (None, Some(BuildMode::Compiled)) => bail!(e055(format!(
            "`{hub}` was vendored from --compiled; pass --compiled <dir> with a fresh compile, \
             or --build-empty to let rocky package run dbt"
        ))),
        (None, Some(mode)) => mode,
        (None, None) => BuildMode::CompileOnly,
    })
}

/// A refusal carrying `E055`.
fn e055(msg: impl std::fmt::Display) -> anyhow::Error {
    anyhow!("{}: {msg}", rocky_compiler::diagnostic::E055)
}

fn w055(message: String, path: Option<String>, model: Option<String>) -> PackageDiagnostic {
    PackageDiagnostic {
        code: rocky_compiler::diagnostic::W055.to_string(),
        severity: "warning".to_string(),
        message,
        path,
        model,
    }
}

/// Project root: the directory holding `rocky.toml`.
fn project_root(config_path: &Path) -> PathBuf {
    match config_path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
    }
}

// ---------------------------------------------------------------------------
// add / update
// ---------------------------------------------------------------------------

/// Execute `rocky package add <hub>[@<version>]`.
pub fn run_package_add(
    config_path: &Path,
    spec: &str,
    opts: &PackageBuildOptions,
    output_json: bool,
) -> Result<()> {
    let (hub, version_spec) = dbt_package::parse_package_spec(spec).map_err(e055)?;
    let root = project_root(config_path);
    let lock_path = root.join(LOCKFILE_NAME);
    let lock = PackagesLock::read(&lock_path).map_err(e055)?;
    if let Some(existing) = lock
        .packages
        .iter()
        .find(|p| p.hub.eq_ignore_ascii_case(&hub))
    {
        bail!(e055(format!(
            "`{hub}` is already vendored as `{}` ({}); use `rocky package update {}`",
            existing.name, existing.version, existing.name
        )));
    }
    let vars = dbt_package::parse_vars(&opts.vars).map_err(e055)?;
    let request = BuildRequest {
        hub,
        version_spec,
        vars,
        adapter_name: opts.adapter.clone(),
        target_schema: opts.target_schema.clone(),
    };
    let (report, diagnostics) = vendor(config_path, &root, &lock_path, &request, opts, None)?;

    if output_json {
        print_json(&PackageAddOutput {
            version: VERSION.to_string(),
            command: "package_add".to_string(),
            lockfile: lock_path.display().to_string(),
            package: report,
            diagnostics,
        })?;
    } else {
        print_report(&report, &diagnostics);
    }
    Ok(())
}

/// Execute `rocky package update [<name>]`.
pub fn run_package_update(
    config_path: &Path,
    name: Option<&str>,
    opts: &PackageBuildOptions,
    output_json: bool,
) -> Result<()> {
    let root = project_root(config_path);
    let lock_path = root.join(LOCKFILE_NAME);
    let lock = PackagesLock::read(&lock_path).map_err(e055)?;
    let targets: Vec<LockedPackage> = match name {
        Some(n) => vec![lock.get(n).cloned().ok_or_else(|| {
            e055(format!(
                "no vendored package `{n}` in {}; `rocky package list` shows what is vendored",
                lock_path.display()
            ))
        })?],
        None => lock.packages.clone(),
    };
    if targets.is_empty() {
        bail!(e055(format!(
            "{} lists no packages; add one with `rocky package add <namespace>/<name>`",
            lock_path.display()
        )));
    }
    if targets.len() > 1 && opts.compiled.is_some() {
        bail!(e055(
            "`--compiled` imports one compiled project; name the package to update"
        ));
    }
    let flag_vars = dbt_package::parse_vars(&opts.vars).map_err(e055)?;

    let mut reports = Vec::new();
    let mut diagnostics = Vec::new();
    for previous in targets {
        let mut vars = previous.vars.clone();
        vars.extend(flag_vars.clone());
        let request = BuildRequest {
            hub: previous.hub.clone(),
            version_spec: previous.version_spec.clone(),
            vars,
            adapter_name: opts
                .adapter
                .clone()
                .or_else(|| Some(previous.adapter_name.clone()).filter(|s| !s.is_empty())),
            target_schema: opts
                .target_schema
                .clone()
                .or_else(|| Some(previous.target_schema.clone()).filter(|s| !s.is_empty())),
        };
        let (report, diags) = vendor(
            config_path,
            &root,
            &lock_path,
            &request,
            opts,
            Some(&previous),
        )?;
        reports.push(report);
        diagnostics.extend(diags);
    }

    if output_json {
        print_json(&PackageUpdateOutput {
            version: VERSION.to_string(),
            command: "package_update".to_string(),
            lockfile: lock_path.display().to_string(),
            packages: reports,
            diagnostics,
        })?;
    } else {
        for r in &reports {
            print_report(r, &[]);
        }
        print_diagnostics(&diagnostics);
    }
    Ok(())
}

struct BuildRequest {
    hub: String,
    version_spec: String,
    vars: BTreeMap<String, String>,
    adapter_name: Option<String>,
    target_schema: Option<String>,
}

/// Compile (or read) the package, import it, apply the three-way plan, and
/// record the lock entry. Shared by `add` (`previous = None`) and `update`.
fn vendor(
    config_path: &Path,
    root: &Path,
    lock_path: &Path,
    request: &BuildRequest,
    opts: &PackageBuildOptions,
    previous: Option<&LockedPackage>,
) -> Result<(PackageVendorReport, Vec<PackageDiagnostic>)> {
    let config = rocky_core::config::load_rocky_config(config_path)
        .with_context(|| format!("failed to load config from {}", config_path.display()))?;
    let (adapter_name, adapter) = select_adapter(&config, request.adapter_name.as_deref())?;
    if opts.compiled.is_none()
        && !dbt_package::PROFILE_ADAPTERS.contains(&adapter.adapter_type.as_str())
    {
        bail!(e055(format!(
            "adapter `{adapter_name}`: no dbt profile mapping for adapter type `{}`; \
             `rocky package` supports {}. Compile the package yourself and pass \
             `--compiled <dbt project dir>`",
            adapter.adapter_type,
            dbt_package::PROFILE_ADAPTERS.join(", ")
        )));
    }
    let target_schema = match &request.target_schema {
        Some(s) => s.clone(),
        None => dbt_package::default_target_schema(&adapter.adapter_type)
            .map(str::to_string)
            .ok_or_else(|| {
                e055(format!(
                    "adapter `{adapter_name}` ({}) has no default schema; pass --target-schema",
                    adapter.adapter_type
                ))
            })?,
    };
    if !target_schema
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        bail!(e055(format!(
            "--target-schema `{target_schema}` must be letters, digits and `_`"
        )));
    }

    let mut diagnostics = Vec::new();
    let mode = resolve_mode(opts, previous, &request.hub)?;
    let build_empty = mode == BuildMode::BuildEmpty;
    let _tmp; // keeps the throwaway project alive until the import is done
    let project_dir = match &opts.compiled {
        Some(dir) => dir.clone(),
        None => {
            _tmp = tempfile::Builder::new()
                .prefix("rocky-package-")
                .tempdir()
                .context("failed to create a temp dir for the dbt build")?;
            run_dbt(
                _tmp.path(),
                request,
                &adapter_name,
                adapter,
                build_empty,
                opts.dbt.as_deref(),
                &mut diagnostics,
            )?;
            _tmp.path().to_path_buf()
        }
    };

    let lock_text = std::fs::read_to_string(project_dir.join("package-lock.yml")).map_err(|e| {
        e055(format!(
            "cannot read package-lock.yml from the dbt project: {e}"
        ))
    })?;
    let (pkg_name, resolved_version) =
        dbt_package::resolve_from_package_lock(&lock_text, &request.hub).map_err(e055)?;
    if !dbt_package::is_safe_package_name(&pkg_name) {
        bail!(e055(format!(
            "package name `{pkg_name}` is not a safe directory name"
        )));
    }
    if previous.is_none()
        && let Some(other) = PackagesLock::read(lock_path)
            .map_err(e055)?
            .get(&pkg_name)
            .filter(|o| !o.hub.eq_ignore_ascii_case(&request.hub))
    {
        bail!(e055(format!(
            "`{}` is dbt project `{pkg_name}`, the same name as the vendored `{}`; both would \
             write models/packages/{pkg_name}/. Remove `{pkg_name}` first",
            request.hub, other.hub
        )));
    }
    if let Some(prev) = previous
        && prev.name != pkg_name
    {
        bail!(e055(format!(
            "`{}` now resolves to dbt project `{pkg_name}`, not `{}`; remove and re-add it",
            request.hub, prev.name
        )));
    }

    let manifest_path = project_dir.join("target").join("manifest.json");
    let manifest = dbt_manifest::parse_manifest(&manifest_path).map_err(e055)?;
    let info = dbt_package::parse_package_info(&manifest_path).map_err(e055)?;
    let default_target = TargetConfig {
        catalog: String::new(),
        schema: target_schema.clone(),
        table: String::new(),
    };
    let import =
        dbt_package::import_package(&manifest, &info, &pkg_name, &default_target).map_err(e055)?;
    // Without empty upstream relations, an introspecting macro compiles to
    // wrong SQL (all-NULL columns, a placeholder `*`). Never vendor that.
    if mode != BuildMode::BuildEmpty && !import.introspection_refused.is_empty() {
        let shown: Vec<&str> = import
            .introspection_refused
            .iter()
            .take(5)
            .map(String::as_str)
            .collect();
        let more = import
            .introspection_refused
            .len()
            .saturating_sub(shown.len());
        let source = if mode == BuildMode::Compiled {
            "The project in --compiled was compiled"
        } else {
            "dbt compiled the package"
        };
        bail!(e055(format!(
            "{source} before the relations its macros read existed, so {} model(s) came out \
             wrong (all-NULL columns or a placeholder `*`): {}{}. Nothing was written. Choose \
             one:\n  --build-empty     run `dbt run --empty` first; this writes empty \
             `rocky_package_build*` schemas to the warehouse and runs the package's \
             on-run-start/on-run-end hooks\n  --compiled <dir>  import a dbt project compiled \
             elsewhere after `dbt run --empty --full-refresh`",
            import.introspection_refused.len(),
            shown.join(", "),
            if more > 0 {
                format!(" and {more} more")
            } else {
                String::new()
            }
        )));
    }

    // Never leave a project `rocky compile` rejects: a vendored model must not
    // read something that was not vendored, and its SQL must parse.
    if !import.blocking.is_empty() {
        bail!(e055(format!(
            "package `{pkg_name}` cannot be vendored as compiled; nothing was written:\n  {}",
            import.blocking.join("\n  ")
        )));
    }
    // A model vendored before that fails now would look "removed upstream"
    // and its files would be deleted. Refuse instead.
    if let Some(prev) = previous {
        let locked = dbt_package::locked_model_names(prev);
        let lost: Vec<String> = import
            .failed
            .iter()
            .filter(|f| locked.contains(&f.name))
            .map(|f| format!("`{}`: {}", f.name, f.reason))
            .collect();
        if !lost.is_empty() {
            bail!(e055(format!(
                "{} vendored model(s) of `{pkg_name}` no longer import, so updating would delete \
                 them; nothing was written:\n  {}",
                lost.len(),
                lost.join("\n  ")
            )));
        }
    }

    let planned = dbt_package::render_package_files(&import).map_err(e055)?;

    // Namespacing: refuse a model name the project or another package owns.
    let lock = PackagesLock::read(lock_path).map_err(e055)?;
    let existing = existing_models(&config, config_path, root, &lock, &planned)?;
    let replacing: BTreeSet<String> = std::iter::once(pkg_name.clone()).collect();
    let twins = dbt_package::case_duplicates(&import);
    if !twins.is_empty() {
        let list: Vec<String> = twins
            .iter()
            .map(|(a, b)| format!("`{a}` and `{b}`"))
            .collect();
        bail!(e055(format!(
            "package `{pkg_name}` has models whose names differ only by case: {}. Warehouses \
             fold unquoted names and case-insensitive file systems cannot hold both, so Rocky \
             cannot vendor them",
            list.join(", ")
        )));
    }
    let collisions = dbt_package::find_collisions(&import, &existing.names, &replacing);
    if !collisions.is_empty() {
        let list: Vec<String> = collisions
            .iter()
            .map(|c| {
                if c.existing == c.model {
                    format!("`{}` (owned by {})", c.model, c.owner)
                } else {
                    format!("`{}` (as `{}`, owned by {})", c.model, c.existing, c.owner)
                }
            })
            .collect();
        bail!(e055(format!(
            "package `{pkg_name}` would define model names that already exist: {}. Rocky \
             resolves models by bare name, so it does not rename package models; rename or \
             remove the existing model first",
            list.join(", ")
        )));
    }

    let target_collisions =
        dbt_package::find_target_collisions(&import, &existing.targets, &replacing);
    if !target_collisions.is_empty() {
        let list: Vec<String> = target_collisions
            .iter()
            .map(|c| {
                format!(
                    "`{}` and `{}` ({}) both write `{}`",
                    c.model, c.existing, c.owner, c.target
                )
            })
            .collect();
        bail!(e055(format!(
            "package `{pkg_name}` would write tables another model already writes: {}. Change \
             one model's [target] (or --target-schema) first; nothing was written",
            list.join("; ")
        )));
    }
    for e in &existing.load_errors {
        diagnostics.push(w055(
            format!(
                "could not load every project model, so target-table collisions with it were \
                 not checked: {e}"
            ),
            None,
            None,
        ));
    }

    let locked_files = previous.map(|p| p.files.clone()).unwrap_or_default();
    let disk = |rel: &str| dbt_package::read_disk(&root.join(rel));
    let plan = dbt_package::plan_update(&planned, &locked_files, &disk);
    apply_plan(root, &plan)?;

    let models: Vec<String> = import.models.iter().map(|m| m.model.name.clone()).collect();
    let previous_models = previous
        .map(dbt_package::locked_model_names)
        .unwrap_or_default();
    let now: BTreeSet<String> = models.iter().cloned().collect();
    let models_added: Vec<String> = now.difference(&previous_models).cloned().collect();
    let models_removed: Vec<String> = previous_models.difference(&now).cloned().collect();
    let includes: Vec<String> = import
        .models
        .iter()
        .map(|m| m.owner_package.clone())
        .filter(|p| *p != pkg_name)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    collect_import_diagnostics(&import, &mut diagnostics);
    for path in plan.incoming.keys() {
        diagnostics.push(w055(
            format!(
                "you edited `{path}` and the package changed it; your file is kept and the new \
                 version is at `{path}{INCOMING_SUFFIX}`. Merge by hand, then delete the \
                 `.incoming` file"
            ),
            Some(path.clone()),
            None,
        ));
    }
    for path in &plan.kept_edited {
        if !planned.contains_key(path) {
            diagnostics.push(w055(
                format!(
                    "the package no longer produces `{path}`, but you edited it, so it was kept"
                ),
                Some(path.clone()),
                None,
            ));
        }
    }

    let vars_hash = dbt_package::vars_hash(&request.vars);
    let dbt_version = import.dbt_version.clone().unwrap_or_default();
    let mut lock = lock;
    lock.upsert(LockedPackage {
        name: pkg_name.clone(),
        hub: request.hub.clone(),
        version_spec: request.version_spec.clone(),
        version: resolved_version.clone(),
        dbt_version: dbt_version.clone(),
        adapter: adapter.adapter_type.clone(),
        adapter_name: adapter_name.clone(),
        target_schema: target_schema.clone(),
        compiled_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        vars_hash: vars_hash.clone(),
        vars: request.vars.clone(),
        mode,
        includes: includes.clone(),
        sources: import.sources.clone(),
        files: plan.lock_files.clone(),
    });
    lock.write(lock_path).map_err(e055)?;

    let report = PackageVendorReport {
        name: pkg_name,
        hub: request.hub.clone(),
        version: resolved_version,
        version_spec: request.version_spec.clone(),
        dbt_version,
        adapter: adapter.adapter_type.clone(),
        target_schema,
        vars_hash,
        mode: mode.as_str().to_string(),
        includes,
        models,
        models_added,
        models_removed,
        sources: import.sources.iter().map(source_output).collect(),
        tests_mapped: import.tests_mapped,
        tests_dropped: import
            .tests_dropped
            .iter()
            .map(|d| PackageDroppedTest {
                test: d.test.clone(),
                attached_to: d.attached_to.clone(),
                reason: d.reason.clone(),
            })
            .collect(),
        incremental_fallbacks: import.incremental_fallbacks.clone(),
        failed_models: import
            .failed
            .iter()
            .map(|f| PackageFailedModel {
                name: f.name.clone(),
                reason: f.reason.clone(),
            })
            .collect(),
        files_written: plan.write.keys().cloned().collect(),
        files_incoming: plan.incoming.keys().cloned().collect(),
        files_deleted: plan.delete.clone(),
        files_kept_edited: plan.kept_edited.clone(),
        files_unchanged: plan.unchanged.len(),
    };
    Ok((report, diagnostics))
}

fn source_output(s: &dbt_package::PackageSource) -> PackageSourceOutput {
    PackageSourceOutput {
        name: s.name.clone(),
        catalog: s.catalog.clone(),
        schema: s.schema.clone(),
        table: s.table.clone(),
    }
}

/// W055 findings from the import itself.
fn collect_import_diagnostics(import: &PackageImport, out: &mut Vec<PackageDiagnostic>) {
    for f in &import.failed {
        out.push(w055(
            format!("model `{}` was not vendored: {}", f.name, f.reason),
            None,
            Some(f.name.clone()),
        ));
    }
    for m in &import.incremental_fallbacks {
        out.push(w055(
            format!(
                "dbt incremental model `{m}` did not map to a Rocky incremental strategy; it \
                 rebuilds in full on every run, or failed to import (see failed_models)"
            ),
            None,
            Some(m.clone()),
        ));
    }
    if !import.tests_dropped.is_empty() {
        out.push(w055(
            format!(
                "{} dbt test(s) were not mapped to Rocky [[tests]] (only not_null, unique, \
                 accepted_values and relationships map); see tests_dropped",
                import.tests_dropped.len()
            ),
            None,
            None,
        ));
    }
}

/// Pick the adapter dbt compiles against: the named one, else the only
/// adapter that can hold data.
fn select_adapter<'a>(
    config: &'a RockyConfig,
    name: Option<&str>,
) -> Result<(String, &'a AdapterConfig)> {
    if let Some(name) = name {
        let adapter = config.adapters.get(name).ok_or_else(|| {
            e055(format!(
                "no adapter `{name}` in rocky.toml (have: {})",
                config
                    .adapters
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        })?;
        return Ok((name.to_string(), adapter));
    }
    let data: Vec<(&String, &AdapterConfig)> = config
        .adapters
        .iter()
        .filter(|(_, a)| {
            rocky_core::adapter_capability::capability_for(&a.adapter_type)
                .is_none_or(|c| c.supports_data)
        })
        .collect();
    match data.as_slice() {
        [(name, adapter)] => Ok(((*name).clone(), adapter)),
        [] => Err(e055(
            "rocky.toml has no warehouse adapter for dbt to compile the package against",
        )),
        many => Err(e055(format!(
            "rocky.toml has several warehouse adapters ({}); pick one with --adapter",
            many.iter()
                .map(|(n, _)| n.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ))),
    }
}

/// The models already in the project, from every transformation pipeline's
/// models directory (`models/` when no pipeline declares one).
struct ProjectModels {
    /// Lowercased resolved name (sidecar `name =` override, else the file
    /// stem) → definitions, each with its owner: the lock entry that vendored
    /// it, or `project`.
    names: BTreeMap<String, BTreeSet<dbt_package::ExistingModel>>,
    /// [`dbt_package::target_key`] → the models writing that table.
    targets: BTreeMap<String, BTreeSet<dbt_package::ExistingModel>>,
    /// Directories whose models could not all be loaded, so the target check
    /// is incomplete there.
    load_errors: Vec<String>,
}

/// The directories `rocky compile` reads models from.
fn project_model_dirs(config: &RockyConfig, config_path: &Path, root: &Path) -> Vec<PathBuf> {
    let mut dirs: Vec<PathBuf> = Vec::new();
    for pipeline in config.pipelines.values() {
        let Some(tx) = pipeline.as_transformation() else {
            continue;
        };
        if let Ok(crate::models_loader::ModelsDir::Present(dir)) =
            crate::models_loader::locate_models_dir(&tx.models, config_path)
            && !dirs.contains(&dir)
        {
            dirs.push(dir);
        }
    }
    if dirs.is_empty() {
        dirs.push(root.join("models"));
    }
    dirs
}

/// Scan the project's models.
///
/// A file at a path this run is about to write, with exactly the content it
/// would write, is skipped: it is the leftover of an earlier run that wrote
/// files but failed before recording them, not a competing model.
fn existing_models(
    config: &RockyConfig,
    config_path: &Path,
    root: &Path,
    lock: &PackagesLock,
    planned: &BTreeMap<String, String>,
) -> Result<ProjectModels> {
    let owner_of = |path: &Path| -> Option<String> {
        let rel = relative_slash(root, path);
        let owner = lock.owner_of(&rel).unwrap_or("project").to_string();
        let leftover = owner == "project"
            && planned.get(&rel).is_some_and(|content| {
                dbt_package::read_disk(path)
                    == dbt_package::DiskFile::Hash(dbt_package::content_hash(content))
            });
        (!leftover).then_some(owner)
    };
    let mut out = ProjectModels {
        names: BTreeMap::new(),
        targets: BTreeMap::new(),
        load_errors: Vec::new(),
    };
    for models_dir in project_model_dirs(config, config_path, root) {
        let (dirs, errors) = rocky_core::model_walk::walk_model_dirs(&models_dir);
        if let Some(e) = errors.into_iter().next() {
            return Err(anyhow!("{e}"));
        }
        for dir in dirs {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                let ext = path.extension().and_then(|e| e.to_str());
                if !matches!(ext, Some("sql" | "rocky")) {
                    continue;
                }
                let Some(name) = dbt_package::resolved_model_name(&path) else {
                    continue;
                };
                let Some(owner) = owner_of(&path) else {
                    continue;
                };
                out.names
                    .entry(name.to_lowercase())
                    .or_insert_with(BTreeSet::new)
                    .insert(dbt_package::ExistingModel { name, owner });
            }
        }
        let (models, errors) = crate::models_loader::load_project_models_partial(&models_dir, None);
        for e in errors {
            out.load_errors
                .push(format!("{}: {e}", models_dir.display()));
        }
        for model in models {
            let Some(owner) = owner_of(&model.file_path) else {
                continue;
            };
            out.targets
                .entry(dbt_package::target_key(
                    &model.config.target,
                    &model.config.name,
                ))
                .or_insert_with(BTreeSet::new)
                .insert(dbt_package::ExistingModel {
                    name: model.config.name.clone(),
                    owner,
                });
        }
    }
    Ok(out)
}

fn relative_slash(root: &Path, path: &Path) -> String {
    let rel = path.strip_prefix(root).unwrap_or(path);
    rel.components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

/// Write, stage `.incoming`, and delete per the plan; prune emptied dirs.
fn apply_plan(root: &Path, plan: &dbt_package::UpdatePlan) -> Result<()> {
    let write = |rel: &str, content: &str| -> Result<()> {
        let path = root.join(rel);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        std::fs::write(&path, content)
            .with_context(|| format!("failed to write {}", path.display()))
    };
    for (rel, content) in &plan.write {
        write(rel, content)?;
        // A stale `.incoming` for a file now written cleanly is obsolete.
        let _ = std::fs::remove_file(root.join(format!("{rel}{INCOMING_SUFFIX}")));
    }
    for (rel, content) in &plan.incoming {
        write(&format!("{rel}{INCOMING_SUFFIX}"), content)?;
    }
    for rel in &plan.delete {
        let path = root.join(rel);
        std::fs::remove_file(&path)
            .with_context(|| format!("failed to delete {}", path.display()))?;
        let _ = std::fs::remove_file(root.join(format!("{rel}{INCOMING_SUFFIX}")));
        prune_empty_dirs(root, path.parent());
    }
    Ok(())
}

/// Remove now-empty directories up to (not including) `models/packages`.
fn prune_empty_dirs(root: &Path, mut dir: Option<&Path>) {
    let stop = root.join(dbt_package::PACKAGES_DIR);
    while let Some(d) = dir {
        if d == stop || !d.starts_with(&stop) || std::fs::remove_dir(d).is_err() {
            break;
        }
        dir = d.parent();
    }
}

// ---------------------------------------------------------------------------
// dbt process
// ---------------------------------------------------------------------------

/// Locate `dbt`: `--dbt`, else the first `dbt` on `PATH`.
fn locate_dbt(flag: Option<&Path>) -> Result<PathBuf> {
    if let Some(p) = flag {
        if p.is_file() {
            return Ok(p.to_path_buf());
        }
        bail!(e055(format!("--dbt {} is not a file", p.display())));
    }
    let names: &[&str] = if cfg!(windows) {
        &["dbt.exe", "dbt.cmd", "dbt"]
    } else {
        &["dbt"]
    };
    if let Some(path) = std::env::var_os("PATH") {
        for dir in std::env::split_paths(&path) {
            for n in names {
                let candidate = dir.join(n);
                if candidate.is_file() {
                    return Ok(candidate);
                }
            }
        }
    }
    Err(e055(
        "dbt is not on PATH. `rocky package` runs dbt once to compile the package. Install \
         dbt-core 1.8+ with the adapter for your warehouse, e.g. `uv tool install dbt-core \
         --with dbt-duckdb` (or `pip install dbt-core dbt-duckdb`), or pass --dbt <path>. To \
         skip dbt entirely, compile the package elsewhere and pass --compiled <dbt project dir>",
    ))
}

/// Write the throwaway project and run `dbt deps`, `dbt run --empty`,
/// `dbt compile` in it.
fn run_dbt(
    dir: &Path,
    request: &BuildRequest,
    adapter_name: &str,
    adapter: &AdapterConfig,
    build_empty: bool,
    dbt_flag: Option<&Path>,
    diagnostics: &mut Vec<PackageDiagnostic>,
) -> Result<()> {
    let dbt = locate_dbt(dbt_flag)?;
    let duckdb_path = match (adapter.adapter_type.as_str(), adapter.path.as_deref()) {
        ("duckdb", Some(p)) => Some(
            std::path::absolute(p).with_context(|| format!("cannot resolve DuckDB path {p}"))?,
        ),
        _ => None,
    };
    let profile = dbt_package::render_profiles_yml(
        BUILD_PROJECT,
        adapter,
        DEFAULT_DBT_SCHEMA,
        duckdb_path.as_deref(),
    )
    .map_err(|e| e055(format!("adapter `{adapter_name}`: {e}")))?;
    std::fs::write(
        dir.join("packages.yml"),
        dbt_package::render_packages_yml(&request.hub, &request.version_spec),
    )?;
    std::fs::write(
        dir.join("dbt_project.yml"),
        dbt_package::render_dbt_project_yml(BUILD_PROJECT, &request.vars).map_err(e055)?,
    )?;
    let profiles = dir.join("profiles.yml");
    std::fs::write(&profiles, &profile.yaml)?;

    let run = |args: &[&str]| -> Result<(bool, String)> {
        tracing::info!(dbt = %dbt.display(), ?args, "running dbt");
        let out = Command::new(&dbt)
            .args(args)
            .arg("--project-dir")
            .arg(dir)
            .arg("--profiles-dir")
            .arg(dir)
            .current_dir(dir)
            .envs(profile.env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
            .output()
            .with_context(|| format!("failed to start {}", dbt.display()))?;
        let mut text = String::from_utf8_lossy(&out.stdout).into_owned();
        text.push_str(&String::from_utf8_lossy(&out.stderr));
        tracing::debug!(output = %text, "dbt output");
        Ok((out.status.success(), text))
    };
    let tail = |text: &str| -> String {
        let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
        lines[lines.len().saturating_sub(15)..].join("\n")
    };

    let (ok, text) = run(&["deps"])?;
    if !ok {
        bail!(e055(format!(
            "`dbt deps` failed for `{}`:\n{}",
            request.hub,
            tail(&text)
        )));
    }
    if build_empty {
        let (ok, text) = run(&["run", "--empty", "--full-refresh"])?;
        if !ok {
            diagnostics.push(w055(
                format!(
                    "`dbt run --empty` did not build every model, so models compiled after a \
                     failed one may carry introspection placeholders (those are refused):\n{}",
                    tail(&text)
                ),
                None,
                None,
            ));
        }
    }
    let (ok, text) = run(&["compile", "--full-refresh"])?;
    if !ok {
        bail!(e055(format!(
            "`dbt compile` failed for `{}`:\n{}",
            request.hub,
            tail(&text)
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// list / remove
// ---------------------------------------------------------------------------

/// Execute `rocky package list`.
pub fn run_package_list(config_path: &Path, output_json: bool) -> Result<()> {
    let root = project_root(config_path);
    let lock_path = root.join(LOCKFILE_NAME);
    let lock = PackagesLock::read(&lock_path).map_err(e055)?;
    let mut entries = Vec::new();
    let mut packages = lock.packages.clone();
    packages.sort_by(|a, b| a.name.cmp(&b.name));
    for p in &packages {
        let mut modified = Vec::new();
        let mut missing = Vec::new();
        let mut incoming = Vec::new();
        for (rel, hash) in &p.files {
            match dbt_package::read_disk(&root.join(rel)) {
                dbt_package::DiskFile::Hash(h) if h == *hash => {}
                dbt_package::DiskFile::Absent => missing.push(rel.clone()),
                _ => modified.push(rel.clone()),
            }
            if root.join(format!("{rel}{INCOMING_SUFFIX}")).exists() {
                incoming.push(format!("{rel}{INCOMING_SUFFIX}"));
            }
        }
        entries.push(PackageListEntry {
            name: p.name.clone(),
            hub: p.hub.clone(),
            version: p.version.clone(),
            version_spec: p.version_spec.clone(),
            dbt_version: p.dbt_version.clone(),
            adapter: p.adapter.clone(),
            target_schema: p.target_schema.clone(),
            mode: p.mode.as_str().to_string(),
            compiled_at: p.compiled_at.clone(),
            includes: p.includes.clone(),
            models: dbt_package::locked_model_names(p).into_iter().collect(),
            sources: p.sources.iter().map(source_output).collect(),
            files_modified: modified,
            files_missing: missing,
            files_incoming: incoming,
        });
    }
    if output_json {
        let count = entries.len();
        print_json(&PackageListOutput {
            version: VERSION.to_string(),
            command: "package_list".to_string(),
            lockfile: lock_path.display().to_string(),
            packages: entries,
            count,
        })?;
    } else if entries.is_empty() {
        println!(
            "No vendored packages ({} is empty or absent).",
            lock_path.display()
        );
    } else {
        for e in &entries {
            println!(
                "{:<24} {:<28} {:<10} {} models, {} modified, {} missing, {} incoming",
                e.name,
                e.hub,
                e.version,
                e.models.len(),
                e.files_modified.len(),
                e.files_missing.len(),
                e.files_incoming.len()
            );
        }
    }
    Ok(())
}

/// Execute `rocky package remove <name>`.
pub fn run_package_remove(
    config_path: &Path,
    name: &str,
    force: bool,
    output_json: bool,
) -> Result<()> {
    let root = project_root(config_path);
    let lock_path = root.join(LOCKFILE_NAME);
    let mut lock = PackagesLock::read(&lock_path).map_err(e055)?;
    let pkg = lock.get(name).cloned().ok_or_else(|| {
        e055(format!(
            "no vendored package `{name}` in {}",
            lock_path.display()
        ))
    })?;
    let edited: Vec<&String> = pkg
        .files
        .iter()
        .filter(
            |(rel, hash)| match dbt_package::read_disk(&root.join(rel)) {
                dbt_package::DiskFile::Absent => false,
                dbt_package::DiskFile::Hash(h) => h != **hash,
                dbt_package::DiskFile::Unreadable => true,
            },
        )
        .map(|(rel, _)| rel)
        .collect();
    if !edited.is_empty() && !force {
        bail!(e055(format!(
            "you edited {} vendored file(s) of `{name}` ({}); pass --force to delete them too",
            edited.len(),
            edited
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }
    let mut deleted = Vec::new();
    for rel in pkg.files.keys() {
        let path = root.join(rel);
        if path.exists() {
            std::fs::remove_file(&path)
                .with_context(|| format!("failed to delete {}", path.display()))?;
            deleted.push(rel.clone());
        }
        let _ = std::fs::remove_file(root.join(format!("{rel}{INCOMING_SUFFIX}")));
        prune_empty_dirs(&root, path.parent());
    }
    lock.packages.retain(|p| p.name != name);
    lock.write(&lock_path).map_err(e055)?;

    if output_json {
        print_json(&PackageRemoveOutput {
            version: VERSION.to_string(),
            command: "package_remove".to_string(),
            lockfile: lock_path.display().to_string(),
            name: name.to_string(),
            files_deleted: deleted,
        })?;
    } else {
        println!("Removed `{name}`: {} file(s) deleted.", deleted.len());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Table output
// ---------------------------------------------------------------------------

fn print_report(r: &PackageVendorReport, diagnostics: &[PackageDiagnostic]) {
    println!(
        "{} {} ({}) → models/packages/{}/",
        r.hub, r.version, r.name, r.name
    );
    println!(
        "  {} models ({} added, {} removed), {} sources, {} tests mapped, {} tests dropped",
        r.models.len(),
        r.models_added.len(),
        r.models_removed.len(),
        r.sources.len(),
        r.tests_mapped,
        r.tests_dropped.len()
    );
    println!(
        "  files: {} written, {} unchanged, {} deleted, {} kept (edited), {} .incoming",
        r.files_written.len(),
        r.files_unchanged,
        r.files_deleted.len(),
        r.files_kept_edited.len(),
        r.files_incoming.len()
    );
    print_diagnostics(diagnostics);
}

fn print_diagnostics(diagnostics: &[PackageDiagnostic]) {
    for d in diagnostics {
        println!("  {}: {}", d.code, d.message);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn project_root_of_a_bare_config_name_is_cwd() {
        assert_eq!(project_root(Path::new("rocky.toml")), PathBuf::from("."));
        assert_eq!(
            project_root(Path::new("/a/b/rocky.toml")),
            PathBuf::from("/a/b")
        );
    }

    #[test]
    fn missing_dbt_flag_path_is_e055() {
        let err = locate_dbt(Some(Path::new("/definitely/not/dbt"))).unwrap_err();
        assert!(err.to_string().starts_with("E055:"), "{err}");
    }

    #[test]
    fn adapter_selection_needs_a_name_when_ambiguous() {
        let cfg: RockyConfig = toml::from_str(
            "[adapter.a]\ntype = \"duckdb\"\npath = \"a.duckdb\"\n\
             [adapter.b]\ntype = \"duckdb\"\npath = \"b.duckdb\"\n\
             [adapter.f]\ntype = \"fivetran\"\ndestination_id = \"d\"\napi_key = \"k\"\napi_secret = \"s\"\n",
        )
        .unwrap();
        let err = select_adapter(&cfg, None).unwrap_err().to_string();
        assert!(err.contains("--adapter"), "{err}");
        assert_eq!(select_adapter(&cfg, Some("b")).unwrap().0, "b");
        assert!(select_adapter(&cfg, Some("zzz")).is_err());
    }
}
